"""Etapa 5 - Carga no MongoDB (Atlas).

Lê os agregados em Parquet e grava nas coleções `pedidos`, `clientes` e `produtos`
usando UPSERT pela chave natural (_id), em lotes. Pode ser reexecutada sem duplicar dados.
Também cria os índices recomendados na N2 e confere as contagens (Parquet x MongoDB).
"""
import json
from datetime import datetime
from itertools import islice

from pymongo import ASCENDING, DESCENDING, ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config, mongo

log = config.get_logger("load")

TAMANHO_LOTE = 1000

INDICES = {
    "pedidos": [("data_hora", ASCENDING), ("cliente_id", ASCENDING), ("pais", ASCENDING),
                ("itens.codigo_produto", ASCENDING)],
    "clientes": [("valor_total_gbp", DESCENDING)],
    "produtos": [("quantidade_vendida", DESCENDING)],
}


def _docs(df):
    """Itera as linhas do DataFrame como dicionários prontos para o BSON."""
    if "data_hora" in df.columns:
        # string ISO em UTC -> datetime sem depender do fuso da máquina
        df = df.withColumn("data_hora", F.date_format("data_hora", "yyyy-MM-dd'T'HH:mm:ss"))
    for row in df.toLocalIterator():
        doc = row.asDict(recursive=True)
        if isinstance(doc.get("data_hora"), str):
            doc["data_hora"] = datetime.fromisoformat(doc["data_hora"])
        yield doc


def _carregar(db, spark, nome, recriar):
    df = spark.read.parquet(str(config.AGG_DIR / nome))
    esperado = df.count()
    colecao = db[nome]
    if recriar:
        colecao.drop()

    inseridos = atualizados = 0
    it = _docs(df)
    while lote := list(islice(it, TAMANHO_LOTE)):
        ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in lote]
        r = colecao.bulk_write(ops, ordered=False)
        inseridos += r.upserted_count
        atualizados += r.modified_count
    for campo, ordem in INDICES[nome]:
        colecao.create_index([(campo, ordem)])
    no_banco = colecao.count_documents({})
    log.info("%-9s esperado=%s no_mongo=%s (novos=%s, atualizados=%s)",
             nome, f"{esperado:,}", f"{no_banco:,}", f"{inseridos:,}", f"{atualizados:,}")
    return {"parquet": esperado, "mongodb": no_banco, "novos": inseridos,
            "atualizados": atualizados, "confere": esperado == no_banco,
            "indices": [f"{c}" for c, _ in INDICES[nome]]}


def run(spark: SparkSession, recriar: bool = False):
    db = mongo.get_db()
    log.info("Conectado ao banco '%s'", db.name)
    resultado = {nome: _carregar(db, spark, nome, recriar) for nome in ("pedidos", "clientes", "produtos")}

    config.OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.OUT_DIR / "relatorio_carga.json", "w", encoding="utf-8") as f:
        json.dump({"banco": db.name, "colecoes": resultado}, f, indent=2, ensure_ascii=False)
    if not all(v["confere"] for v in resultado.values()):
        log.warning("Há divergência entre Parquet e MongoDB - veja outputs/relatorio_carga.json")
    return resultado
