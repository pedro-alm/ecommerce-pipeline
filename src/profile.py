"""Etapa 2 - Exploração inicial (perfilamento do CSV bruto com PySpark).

Confirma colunas, tipos, nulos, preços zerados, quantidades negativas, cancelamentos e
duplicados ANTES de limpar. A saída é impressa e salva em outputs/perfil_exploratorio.txt.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config
from src.clean import read_raw

log = config.get_logger("profile")


def run(spark: SparkSession):
    df = read_raw(spark).cache()
    linhas = []

    def p(txt=""):
        linhas.append(txt)

    total = df.count()
    p("=" * 70)
    p("PERFIL EXPLORATÓRIO DO CSV BRUTO")
    p("=" * 70)
    p(f"Arquivo ........: {config.RAW_FILE.name}")
    p(f"Linhas .........: {total:,}")
    p(f"Colunas ........: {', '.join(df.columns)}")
    p("")
    p("Nulos por coluna (quantidade e %):")
    nulos = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict()
    for c, n in nulos.items():
        p(f"  {c:<12} {n:>9,}  ({100 * n / total:5.2f}%)")

    num = df.select(
        F.col("Quantity").cast("double").alias("q"),
        F.col("UnitPrice").cast("double").alias("p"),
        F.col("InvoiceNo").alias("inv"),
        F.col("StockCode").alias("sc"),
    )
    r = num.agg(
        F.sum((F.col("q") < 0).cast("int")).alias("qtd_negativa"),
        F.sum((F.col("p") == 0).cast("int")).alias("preco_zero"),
        F.sum((F.col("p") < 0).cast("int")).alias("preco_negativo"),
        F.sum(F.col("inv").startswith("C").cast("int")).alias("linhas_cancelamento"),
        F.sum(F.upper(F.col("sc")).isin(config.SERVICE_CODES).cast("int")).alias("linhas_servico"),
        F.countDistinct("inv").alias("faturas"),
    ).first().asDict()
    dup = total - df.dropDuplicates().count()

    p("")
    p("Pontos de qualidade esperados:")
    p(f"  Quantidades negativas (cancelamentos/devoluções) : {r['qtd_negativa']:,}")
    p(f"  Linhas de fatura cancelada (prefixo C)            : {r['linhas_cancelamento']:,}")
    p(f"  Preço unitário zero                               : {r['preco_zero']:,}")
    p(f"  Preço unitário negativo                           : {r['preco_negativo']:,}")
    p(f"  Linhas de serviço (frete, desconto, ajuste)       : {r['linhas_servico']:,}")
    p(f"  Linhas duplicadas (idênticas)                     : {dup:,}")
    p(f"  Faturas distintas                                 : {r['faturas']:,}")

    p("")
    p("Top 5 países por número de linhas:")
    for row in df.groupBy("Country").count().orderBy(F.desc("count")).limit(5).collect():
        p(f"  {row['Country']:<20} {row['count']:>9,}")

    p("")
    p("Amostra (5 linhas):")
    for row in df.limit(5).collect():
        p("  " + " | ".join(str(v) for v in row))

    texto = "\n".join(linhas)
    print(texto)
    config.OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.OUT_DIR / "perfil_exploratorio.txt", "w", encoding="utf-8") as f:
        f.write(texto + "\n")
    df.unpersist()
    log.info("Perfil salvo em %s", config.OUT_DIR / "perfil_exploratorio.txt")
