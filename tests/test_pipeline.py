"""Teste de ponta a ponta com dados sintéticos e MongoDB simulado (mongomock).

Roda o pipeline inteiro (ingest -> profile -> clean -> transform -> load -> analytics)
sem precisar de internet nem do Atlas:   pytest -s
"""
import mongomock
import pytest
from mongomock import collection as _mm_collection

# mongomock ainda não conhece o parâmetro `sort` que o pymongo 4.x passa no ReplaceOne;
# este ajuste vale só para o teste (no Atlas real o pymongo funciona normalmente).
_orig_add_replace = _mm_collection.BulkOperationBuilder.add_replace


def _add_replace(self, *args, sort=None, **kwargs):
    return _orig_add_replace(self, *args, **kwargs)


_mm_collection.BulkOperationBuilder.add_replace = _add_replace

from src import config, mongo


@pytest.fixture(scope="module")
def ambiente(tmp_path_factory):
    base = tmp_path_factory.mktemp("pipeline")
    config.usar_amostra()
    config.RAW_FILE = base / "raw" / "amostra.csv"
    config.CURATED_DIR = base / "curated" / "transacoes"
    config.AGG_DIR = base / "aggregates"
    config.OUT_DIR = base / "outputs"

    from scripts.gerar_amostra import gerar
    gerar(800, config.RAW_FILE)

    cliente = mongomock.MongoClient()
    mongo.get_client = lambda: cliente  # troca o Atlas pelo mock
    return cliente[config.MONGO_DB]


def test_pipeline_completo(ambiente):
    import run_pipeline
    run_pipeline.main([])  # caminhos e banco já configurados na fixture
    db = ambiente

    assert db.pedidos.count_documents({}) > 0
    assert db.clientes.count_documents({}) > 0
    assert db.produtos.count_documents({}) > 0

    ped = db.pedidos.find_one({"cancelado": False})
    assert ped["total_itens"] == len(ped["itens"])
    assert round(sum(i["valor_item_gbp"] for i in ped["itens"]), 2) == pytest.approx(ped["valor_total_gbp"])
    assert ped["data_hora"].year in (2010, 2011)

    # serviços (POST, D, M, BANK CHARGES) não podem aparecer como produtos
    assert db.produtos.count_documents({"_id": {"$in": ["POST", "D", "M", "BANK CHARGES"]}}) == 0
    # preço zero não entra nos pedidos
    assert db.pedidos.count_documents({"itens.preco_unitario_gbp": {"$lte": 0}}) == 0
    # idempotência: nova carga não duplica
    antes = db.pedidos.count_documents({})
    from src import load
    from src.spark_session import get_spark
    load.run(get_spark())
    assert db.pedidos.count_documents({}) == antes
