"""Etapa 4 - Transformação para agregados (modelo de documentos da N2).

Lê a camada CURATED (Parquet) e gera três conjuntos prontos para o MongoDB:
  - pedidos  : um documento por fatura, com os itens EMBUTIDOS;
  - clientes : métricas por cliente identificado;
  - produtos : métricas por código de produto.

Só entram nos agregados as linhas de produto válidas: ficam de fora as linhas de serviço
(frete, desconto, ajuste) e as sinalizadas como suspeitas (preço zero/negativo). Elas
continuam disponíveis, com as flags, na camada curated.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config

log = config.get_logger("transform")


def run(spark: SparkSession):
    curated = spark.read.parquet(str(config.CURATED_DIR))
    itens_validos = curated.filter(~F.col("eh_servico") & ~F.col("suspeito")).cache()

    # ---------------- pedidos (agregado principal) ----------------
    item = F.struct(
        F.col("codigo_produto"), F.col("descricao"), F.col("quantidade"),
        F.col("preco_unitario_gbp"), F.col("valor_item_gbp"),
    )
    pedidos = (
        itens_validos.groupBy("invoice_no").agg(
            F.min("data_hora").alias("data_hora"),
            F.first("cliente_id", ignorenulls=True).alias("cliente_id"),
            F.first("pais").alias("pais"),
            F.max("cancelado").alias("cancelado"),
            F.sort_array(F.collect_list(item)).alias("itens"),
            F.count("*").alias("total_itens"),
            F.round(F.sum("valor_item_gbp"), 2).alias("valor_total_gbp"),
        ).withColumnRenamed("invoice_no", "_id")
    )

    # ---------------- clientes ----------------
    nao_canc = ~F.col("cancelado")
    clientes = (
        itens_validos.filter(F.col("cliente_id").isNotNull())
        .groupBy("cliente_id").agg(
            F.max_by("pais", "data_hora").alias("pais"),
            F.countDistinct(F.when(nao_canc, F.col("invoice_no"))).alias("total_pedidos"),
            F.round(F.coalesce(F.sum(F.when(nao_canc, F.col("valor_item_gbp"))), F.lit(0.0)), 2).alias("valor_total_gbp"),
            F.date_format(F.min(F.when(nao_canc, F.col("data_hora"))), "yyyy-MM-dd").alias("primeira_compra"),
            F.date_format(F.max(F.when(nao_canc, F.col("data_hora"))), "yyyy-MM-dd").alias("ultima_compra"),
            F.countDistinct(F.when(F.col("cancelado"), F.col("invoice_no"))).alias("total_cancelamentos"),
        ).withColumnRenamed("cliente_id", "_id")
    )

    # ---------------- produtos ----------------
    com_desc = F.col("descricao").isNotNull()
    produtos = (
        itens_validos.groupBy("codigo_produto").agg(
            F.max_by(F.col("descricao"), F.when(com_desc, F.col("data_hora"))).alias("descricao"),
            F.coalesce(F.sum(F.when(nao_canc & (F.col("quantidade") > 0), F.col("quantidade"))), F.lit(0)).alias("quantidade_vendida"),
            F.round(F.coalesce(F.sum(F.when(nao_canc, F.col("valor_item_gbp"))), F.lit(0.0)), 2).alias("receita_gbp"),
            F.coalesce(F.sum(F.when(F.col("cancelado"), -F.col("quantidade"))), F.lit(0)).alias("total_devolvido"),
        ).withColumnRenamed("codigo_produto", "_id")
    )

    config.AGG_DIR.mkdir(parents=True, exist_ok=True)
    cont = {}
    for nome, df in (("pedidos", pedidos), ("clientes", clientes), ("produtos", produtos)):
        destino = config.AGG_DIR / nome
        df.write.mode("overwrite").parquet(str(destino))
        cont[nome] = spark.read.parquet(str(destino)).count()
        log.info("Agregado '%s': %s documentos -> %s", nome, f"{cont[nome]:,}", destino)
    itens_validos.unpersist()
    return cont
