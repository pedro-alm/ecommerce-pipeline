"""Etapa 6 - Consultas analíticas no MongoDB e geração das saídas esperadas.

Executa as consultas de exemplo da N2 (aggregation pipeline), imprime as tabelas e salva
CSVs + um resumo em Markdown em outputs/. Esses arquivos são a evidência do funcionamento.
"""
from datetime import datetime

import pandas as pd

from src import config, mongo

log = config.get_logger("analytics")


def _df(rows, colunas=None):
    df = pd.DataFrame(rows)
    if colunas is not None and not df.empty:
        df = df.rename(columns=colunas)
        chave = [c for c in colunas.values() if c in df.columns]
        df = df[chave + [c for c in df.columns if c not in chave]]  # chave primeiro
    return df


def consultas(db):
    r = {}

    # 1) Receita e nº de pedidos por mês (somente pedidos não cancelados)
    r["01_receita_por_mes"] = _df(db.pedidos.aggregate([
        {"$match": {"cancelado": False}},
        {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$data_hora"}},
                    "pedidos": {"$sum": 1}, "receita_gbp": {"$sum": "$valor_total_gbp"}}},
        {"$sort": {"_id": 1}},
    ]), {"_id": "mes"})

    # 2) Top 10 países por receita
    r["02_top_paises_receita"] = _df(db.pedidos.aggregate([
        {"$match": {"cancelado": False}},
        {"$group": {"_id": "$pais", "pedidos": {"$sum": 1}, "receita_gbp": {"$sum": "$valor_total_gbp"}}},
        {"$sort": {"receita_gbp": -1}}, {"$limit": 10},
    ]), {"_id": "pais"})

    # 3) Top 10 produtos mais vendidos em dez/2010 e dez/2011
    for rotulo, ini, fim in (("dez2010", datetime(2010, 12, 1), datetime(2011, 1, 1)),
                             ("dez2011", datetime(2011, 12, 1), datetime(2012, 1, 1))):
        r[f"03_top_produtos_{rotulo}"] = _df(db.pedidos.aggregate([
            {"$match": {"cancelado": False, "data_hora": {"$gte": ini, "$lt": fim}}},
            {"$unwind": "$itens"},
            {"$group": {"_id": "$itens.codigo_produto", "descricao": {"$first": "$itens.descricao"},
                        "quantidade": {"$sum": "$itens.quantidade"},
                        "receita_gbp": {"$sum": "$itens.valor_item_gbp"}}},
            {"$sort": {"quantidade": -1}}, {"$limit": 10},
        ]), {"_id": "codigo_produto"})

    # 4) Clientes com mais de 20 pedidos e valor total alto (coleção clientes)
    r["04_clientes_vip"] = _df(db.clientes.find(
        {"total_pedidos": {"$gt": 20}, "valor_total_gbp": {"$gt": 5000}},
        {"pais": 1, "total_pedidos": 1, "valor_total_gbp": 1, "ultima_compra": 1},
    ).sort("valor_total_gbp", -1).limit(10), {"_id": "cliente_id"})

    # 5) Taxa de cancelamento por país
    canc = _df(db.pedidos.aggregate([
        {"$group": {"_id": "$pais", "pedidos": {"$sum": 1},
                    "cancelados": {"$sum": {"$cond": ["$cancelado", 1, 0]}}}},
        {"$match": {"pedidos": {"$gte": 20}}},
    ]), {"_id": "pais"})
    if not canc.empty:
        canc["taxa_cancelamento_pct"] = (100 * canc["cancelados"] / canc["pedidos"]).round(2)
        canc = canc.sort_values("taxa_cancelamento_pct", ascending=False).head(10)
    r["05_taxa_cancelamento_pais"] = canc

    # 6) Produtos mais devolvidos
    r["06_produtos_mais_devolvidos"] = _df(db.produtos.find(
        {}, {"descricao": 1, "total_devolvido": 1, "quantidade_vendida": 1},
    ).sort("total_devolvido", -1).limit(10), {"_id": "codigo_produto"})

    return r


def run():
    db = mongo.get_db()
    resumo = {
        "pedidos": db.pedidos.count_documents({}),
        "clientes": db.clientes.count_documents({}),
        "produtos": db.produtos.count_documents({}),
    }
    exemplo = db.pedidos.find_one({"cancelado": False}, sort=[("data_hora", 1)])

    resultados = consultas(db)
    config.OUT_DIR.mkdir(parents=True, exist_ok=True)

    md = [f"# Saídas das consultas — banco `{db.name}`\n",
          "## Documentos por coleção\n",
          *[f"- `{k}`: {v:,}" for k, v in resumo.items()], ""]
    if exemplo:
        md += ["## Exemplo de documento da coleção `pedidos`\n", "```json",
               _json_legivel(exemplo), "```\n"]

    for nome, df in resultados.items():
        titulo = nome.split("_", 1)[1].replace("_", " ")
        print(f"\n=== {titulo.upper()} ===")
        print(df.to_string(index=False) if not df.empty else "(sem resultados)")
        df.to_csv(config.OUT_DIR / f"{nome}.csv", index=False, encoding="utf-8")
        md += [f"## {titulo.capitalize()}\n", "```", df.to_string(index=False) if not df.empty else "(sem resultados)", "```\n"]

    with open(config.OUT_DIR / "resultado_consultas.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    log.info("Saídas salvas em %s", config.OUT_DIR)
    return resultados


def _json_legivel(doc):
    import json
    itens = doc.get("itens", [])
    doc = {**doc, "data_hora": doc["data_hora"].isoformat(), "itens": itens[:2]}
    txt = json.dumps(doc, indent=2, ensure_ascii=False, default=str)
    return txt + ("\n// ... (itens truncados para exibição)" if len(itens) > 2 else "")
