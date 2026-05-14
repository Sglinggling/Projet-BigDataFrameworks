"""
database.py — Connexion SQLAlchemy à PostgreSQL

Rôle  : Fournir le moteur SQLAlchemy et la session de base de données
         utilisés par les modèles et les endpoints FastAPI.

TODO Phase 5 :
    - Construire l'URL de connexion depuis database_config.yaml
    - Configurer le pool de connexions
    - Implémenter get_db() comme dépendance FastAPI (contextmanager)
"""

# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker, declarative_base
# import yaml

# TODO Phase 5 : charger database_config.yaml et construire DATABASE_URL
DATABASE_URL = "postgresql://foodnutrition:foodnutrition123@postgres:5432/food_nutrition"

# engine = create_engine(DATABASE_URL)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Base = declarative_base()


def get_db():
    """Dépendance FastAPI : fournit une session SQLAlchemy et la ferme après la requête. TODO Phase 5."""
    pass
