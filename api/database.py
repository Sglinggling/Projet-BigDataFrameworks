"""
database.py — Connexion SQLAlchemy à PostgreSQL

Charge la configuration depuis database_config.yaml via CONFIG_DIR (variable
d'environnement) ou config/ à la racine du projet en local.
Pool de connexions paramétré depuis le fichier YAML.
get_db() est une dépendance FastAPI injectée via Depends().
"""

import os
import yaml
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# Répertoire config : CONFIG_DIR (Docker) ou config/ relatif au projet (local)
_CONFIG_DIR = Path(os.environ.get("CONFIG_DIR", Path(__file__).parent.parent / "config"))


def _load_db_config() -> dict:
    with open(_CONFIG_DIR / "database_config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


_cfg = _load_db_config()
_pg  = _cfg["postgresql"]
_sa  = _cfg.get("sqlalchemy", {})

DATABASE_URL = (
    f"postgresql+psycopg2://{_pg['user']}:{_pg['password']}"
    f"@{_pg['host']}:{_pg['port']}/{_pg['database']}"
)

engine = create_engine(
    DATABASE_URL,
    pool_size=_sa.get("pool_size", 5),
    max_overflow=_sa.get("max_overflow", 10),
    pool_timeout=_sa.get("pool_timeout", 30),
    echo=_sa.get("echo", False),
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db() -> Generator:
    """Dépendance FastAPI : ouvre une session SQLAlchemy et la ferme après la requête."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
