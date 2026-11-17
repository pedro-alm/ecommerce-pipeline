"""Configurações centrais do pipeline (caminhos, conexão e log)."""
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

DATA_DIR = ROOT / "data"
RAW_FILE = Path(os.getenv("RAW_FILE", DATA_DIR / "raw" / "data.csv"))
CURATED_DIR = DATA_DIR / "curated" / "transacoes"
AGG_DIR = DATA_DIR / "aggregates"
OUT_DIR = ROOT / "outputs"
SAMPLE_FILE = DATA_DIR / "sample" / "ecommerce_amostra.csv"

MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB = os.getenv("MONGO_DB", "ecommerce_uk")

# O CSV do Kaggle (carrie1/ecommerce-data) está em Latin-1, não em UTF-8
CSV_ENCODING = "ISO-8859-1"
KAGGLE_DATASET = "carrie1/ecommerce-data"

# Códigos de StockCode que são serviços/ajustes e não produtos de verdade
SERVICE_CODES = ["POST", "D", "DOT", "M", "S", "B", "BANK CHARGES",
                 "AMAZONFEE", "CRUK", "C2", "PADS"]

# Normalização de nomes de países
COUNTRY_MAP = {"EIRE": "Ireland", "RSA": "South Africa", "USA": "United States"}


def usar_amostra():
    """Troca caminhos e banco para o modo de demonstração com dados sintéticos,
    sem tocar nos dados reais nem no banco real."""
    global RAW_FILE, CURATED_DIR, AGG_DIR, OUT_DIR, MONGO_DB
    RAW_FILE = SAMPLE_FILE
    CURATED_DIR = DATA_DIR / "curated_amostra" / "transacoes"
    AGG_DIR = DATA_DIR / "aggregates_amostra"
    OUT_DIR = ROOT / "outputs_amostra"
    MONGO_DB = MONGO_DB + "_amostra"


def get_logger(nome: str) -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                            datefmt="%H:%M:%S")
    return logging.getLogger(nome)
