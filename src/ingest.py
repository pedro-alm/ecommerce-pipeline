"""Etapa 1 - Captura (ingestão em lote).

Coloca o CSV original do Kaggle na landing zone (camada RAW), SEM alterá-lo, e registra
um manifesto com tamanho e hash SHA-256 para rastreabilidade.

O arquivo é obtido de uma destas formas (nesta ordem):
  1. Já existe em data/raw/data.csv  -> apenas registra o manifesto;
  2. Kaggle CLI configurada (~/.kaggle/kaggle.json) -> baixa e descompacta;
  3. Caso contrário, mostra como baixar manualmente.
"""
import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone

from src import config

log = config.get_logger("ingest")


def _sha256(caminho, bloco=1024 * 1024):
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        while chunk := f.read(bloco):
            h.update(chunk)
    return h.hexdigest()


def _baixar_kaggle(destino_dir):
    if shutil.which("kaggle") is None:
        return False
    log.info("Baixando %s pela Kaggle CLI...", config.KAGGLE_DATASET)
    r = subprocess.run(["kaggle", "datasets", "download", "-d", config.KAGGLE_DATASET,
                        "-p", str(destino_dir), "--unzip"], capture_output=True, text=True)
    if r.returncode != 0:
        log.warning("Kaggle CLI falhou: %s", r.stderr.strip())
        return False
    return True


def run():
    arquivo = config.RAW_FILE
    arquivo.parent.mkdir(parents=True, exist_ok=True)

    if not arquivo.exists():
        if not _baixar_kaggle(arquivo.parent):
            raise SystemExit(
                f"\nCSV não encontrado em {arquivo}.\n"
                f"Baixe manualmente em https://www.kaggle.com/datasets/{config.KAGGLE_DATASET}\n"
                f"e salve como {arquivo}\n"
                "(ou rode com --amostra para usar dados sintéticos de demonstração)."
            )

    manifesto = {
        "arquivo": arquivo.name,
        "tamanho_bytes": arquivo.stat().st_size,
        "sha256": _sha256(arquivo),
        "capturado_em_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fonte": ("amostra_sintetica" if arquivo == config.SAMPLE_FILE
                  else f"kaggle:{config.KAGGLE_DATASET}"),
        "codificacao": config.CSV_ENCODING,
    }
    with open(arquivo.parent / "_manifesto.json", "w", encoding="utf-8") as f:
        json.dump(manifesto, f, indent=2, ensure_ascii=False)
    log.info("Camada RAW pronta: %s (%.1f MB, sha256 %s...)", arquivo.name,
             manifesto["tamanho_bytes"] / 1e6, manifesto["sha256"][:12])
    return manifesto
