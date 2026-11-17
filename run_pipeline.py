"""Orquestrador do pipeline: Transações de E-commerce (UK) -> MongoDB Atlas.

Exemplos:
    python run_pipeline.py                       # todas as etapas, dados reais
    python run_pipeline.py --amostra             # demo com dados sintéticos (banco *_amostra)
    python run_pipeline.py --etapas clean transform   # só algumas etapas
    python run_pipeline.py --recriar             # apaga e recria as coleções antes de carregar
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src import config  # noqa: E402

ETAPAS = ["ingest", "profile", "clean", "transform", "load", "analytics"]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--etapas", nargs="+", choices=ETAPAS, default=ETAPAS,
                    help="etapas a executar (padrão: todas, na ordem do pipeline)")
    ap.add_argument("--amostra", action="store_true",
                    help="usa dados sintéticos (gera o CSV se não existir) e um banco separado")
    ap.add_argument("--recriar", action="store_true", help="apaga as coleções antes da carga")
    args = ap.parse_args(argv)

    if args.amostra:
        config.usar_amostra()
        if not config.RAW_FILE.exists():
            from scripts.gerar_amostra import gerar
            n = gerar(3000, config.RAW_FILE)
            print(f"[amostra] CSV sintético gerado com {n} linhas")

    log = config.get_logger("pipeline")
    etapas = [e for e in ETAPAS if e in args.etapas]  # mantém a ordem lógica
    log.info("Etapas: %s | banco: %s", " > ".join(etapas), config.MONGO_DB)

    spark = None
    if set(etapas) & {"profile", "clean", "transform", "load"}:
        from src.spark_session import get_spark
        spark = get_spark()

    inicio = time.time()
    for etapa in etapas:
        t0 = time.time()
        log.info(">>> ETAPA: %s", etapa.upper())
        if etapa == "ingest":
            from src import ingest
            ingest.run()
        elif etapa == "profile":
            from src import profile
            profile.run(spark)
        elif etapa == "clean":
            from src import clean
            clean.run(spark)
        elif etapa == "transform":
            from src import transform
            transform.run(spark)
        elif etapa == "load":
            from src import load
            load.run(spark, recriar=args.recriar)
        elif etapa == "analytics":
            from src import analytics
            analytics.run()
        log.info("<<< %s concluída em %.1fs", etapa.upper(), time.time() - t0)

    if spark:
        spark.stop()
    log.info("PIPELINE CONCLUÍDO em %.1fs", time.time() - inicio)


if __name__ == "__main__":
    main()
