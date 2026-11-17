"""Conexão com o MongoDB Atlas."""
import certifi
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from src import config


def get_client() -> MongoClient:
    if not config.MONGO_URI:
        raise SystemExit(
            "MONGO_URI não definido. Copie .env.example para .env e informe a "
            "string de conexão do MongoDB Atlas."
        )
    kwargs = {"serverSelectionTimeoutMS": 15000}
    if config.MONGO_URI.startswith("mongodb+srv"):
        kwargs["tlsCAFile"] = certifi.where()
    client = MongoClient(config.MONGO_URI, **kwargs)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError as exc:
        raise SystemExit(
            "Não foi possível conectar ao MongoDB Atlas. Verifique: (1) usuário/senha "
            "na MONGO_URI; (2) Network Access do Atlas liberando o seu IP; "
            f"(3) sua conexão com a internet.\nDetalhe: {exc}"
        )
    return client


def get_db():
    return get_client()[config.MONGO_DB]
