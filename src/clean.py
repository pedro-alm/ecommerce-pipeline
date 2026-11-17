"""Etapa 3 - Validação e limpeza (PySpark) -> camada CURATED em Parquet.

Regras (conforme a especificação da N2):
  - remover linhas duplicadas;
  - converter tipos (data/hora, decimal, inteiro) e normalizar texto e país;
  - cancelado  = InvoiceNo começa com "C";
  - cliente_identificado = CustomerID presente (nulo => cliente não identificado);
  - eh_servico = StockCode é frete/desconto/ajuste (não é produto);
  - suspeito   = preço zero/negativo ou valores inválidos (sinalizado, não apagado);
  - valor_item_gbp = quantidade x preço unitário;
  - gravar em Parquet particionado por ano e mês da fatura.
"""
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config

log = config.get_logger("clean")


def read_raw(spark: SparkSession) -> DataFrame:
    """Lê o CSV bruto: tudo como string (os tipos são tratados na limpeza)."""
    return (
        spark.read.option("header", True)
        .option("encoding", config.CSV_ENCODING)
        .option("quote", '"').option("escape", '"')
        .option("multiLine", False)
        .csv(str(config.RAW_FILE))
    )


def _padroniza_texto(df: DataFrame) -> DataFrame:
    """Trim em todas as colunas e texto vazio -> nulo."""
    return df.select([
        F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c)
        for c in df.columns
    ])


def limpar(df_raw: DataFrame):
    """Aplica as regras de limpeza. Retorna (DataFrame limpo, contadores)."""
    base = _padroniza_texto(df_raw).cache()
    n_lidas = base.count()

    dedup = base.dropDuplicates()
    n_dedup = dedup.count()

    pais = F.trim(F.col("Country"))
    for original, novo in config.COUNTRY_MAP.items():
        pais = F.when(pais == original, novo).otherwise(pais)

    data_hora = F.coalesce(
        F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"),
        F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("InvoiceDate", "d-M-yyyy H:mm"),
    )
    preco = F.round(F.col("UnitPrice").cast("double"), 2)
    qtd = F.col("Quantity").cast("double").cast("int")
    codigo = F.upper(F.col("StockCode"))

    tipado = dedup.select(
        F.col("InvoiceNo").alias("invoice_no"),
        codigo.alias("codigo_produto"),
        F.regexp_replace(F.col("Description"), r"\s+", " ").alias("descricao"),
        qtd.alias("quantidade"),
        data_hora.alias("data_hora"),
        preco.alias("preco_unitario_gbp"),
        F.col("CustomerID").cast("double").cast("long").alias("cliente_id"),
        pais.alias("pais"),
    )

    invalidas = tipado.filter(F.col("invoice_no").isNull() | F.col("data_hora").isNull())
    n_invalidas = invalidas.count()
    validas = tipado.filter(F.col("invoice_no").isNotNull() & F.col("data_hora").isNotNull())

    limpo = (
        validas
        .withColumn("cancelado", F.col("invoice_no").startswith("C"))
        .withColumn("cliente_identificado", F.col("cliente_id").isNotNull())
        .withColumn("eh_servico", F.col("codigo_produto").isin(config.SERVICE_CODES))
        .withColumn(
            "motivo_suspeito",
            F.when(F.col("preco_unitario_gbp").isNull(), "preco_invalido")
            .when(F.col("preco_unitario_gbp") <= 0, "preco_zero_ou_negativo")
            .when(F.col("quantidade").isNull(), "quantidade_invalida"),
        )
        .withColumn("suspeito", F.col("motivo_suspeito").isNotNull())
        .withColumn("valor_item_gbp", F.round(F.col("quantidade") * F.col("preco_unitario_gbp"), 2))
        .withColumn("ano", F.year("data_hora"))
        .withColumn("mes", F.month("data_hora"))
    )

    contadores = {"linhas_lidas": n_lidas,
                  "duplicadas_removidas": n_lidas - n_dedup,
                  "descartadas_data_ou_fatura_invalida": n_invalidas}
    return limpo, contadores


def run(spark: SparkSession):
    df_raw = read_raw(spark)
    limpo, cont = limpar(df_raw)
    limpo = limpo.cache()

    s = limpo.agg(
        F.count("*").alias("linhas_curated"),
        F.sum(F.col("cancelado").cast("int")).alias("linhas_cancelamento"),
        F.countDistinct(F.when(F.col("cancelado"), F.col("invoice_no"))).alias("faturas_canceladas"),
        F.sum((~F.col("cliente_identificado")).cast("int")).alias("linhas_sem_cliente"),
        F.sum(F.col("descricao").isNull().cast("int")).alias("linhas_sem_descricao"),
        F.sum(F.col("eh_servico").cast("int")).alias("linhas_servico"),
        F.sum(F.col("suspeito").cast("int")).alias("linhas_suspeitas_preco"),
        F.countDistinct("invoice_no").alias("faturas_distintas"),
        F.countDistinct("cliente_id").alias("clientes_distintos"),
        F.countDistinct("codigo_produto").alias("codigos_distintos"),
        F.countDistinct("pais").alias("paises_distintos"),
        F.min("data_hora").alias("data_min"),
        F.max("data_hora").alias("data_max"),
    ).first().asDict()
    s["data_min"] = s["data_min"].strftime("%Y-%m-%d %H:%M")
    s["data_max"] = s["data_max"].strftime("%Y-%m-%d %H:%M")

    config.CURATED_DIR.parent.mkdir(parents=True, exist_ok=True)
    (limpo.repartition("ano", "mes")
          .write.mode("overwrite").partitionBy("ano", "mes")
          .parquet(str(config.CURATED_DIR)))

    relatorio = {**cont, **s, "camada_curated": str(config.CURATED_DIR),
                 "particionamento": ["ano", "mes"], "formato": "parquet"}
    config.OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.OUT_DIR / "relatorio_qualidade.json", "w", encoding="utf-8") as f:
        json.dump(relatorio, f, indent=2, ensure_ascii=False)

    log.info("Limpeza concluída: %s linhas lidas -> %s duplicadas removidas -> %s linhas no Parquet",
             f"{cont['linhas_lidas']:,}", f"{cont['duplicadas_removidas']:,}", f"{s['linhas_curated']:,}")
    log.info("Relatório de qualidade: %s", config.OUT_DIR / "relatorio_qualidade.json")
    return relatorio
