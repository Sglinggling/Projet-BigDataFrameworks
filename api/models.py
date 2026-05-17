"""
models.py — Modèles ORM SQLAlchemy et schémas Pydantic

ORM : uniquement la table `users` (gérée par l'API, créée au démarrage).
Pydantic : schémas de réponse pour les 4 datamarts + pagination générique + auth.
Les tables datamarts sont lues en SQL brut (text()) — pas d'ORM pour elles.
"""

from __future__ import annotations
from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel
from sqlalchemy import Column, Integer, String

from api.database import Base


# ─── ORM ─────────────────────────────────────────────────────────────────────

class UserORM(Base):
    """Table PostgreSQL des comptes utilisateurs de l'API (créée par lifespan)."""
    __tablename__ = "users"

    id              = Column(Integer, primary_key=True, index=True)
    username        = Column(String(100), unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)


# ─── Pagination générique ─────────────────────────────────────────────────────

T = TypeVar("T")

class Page(BaseModel, Generic[T]):
    page:  int
    limit: int
    total: int
    items: List[T]


# ─── Auth ─────────────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type:   str = "bearer"

class TokenData(BaseModel):
    username: Optional[str] = None


# ─── Sugar — dm_sugar_by_category ────────────────────────────────────────────

class SugarItem(BaseModel):
    category:               Optional[str]   = None
    product_name:           Optional[str]   = None
    sugars_100g:            Optional[float] = None
    nutrition_grade_fr:     Optional[str]   = None
    sugar_rank_in_category: Optional[int]   = None


# ─── Nutriscore — dm_nutriscore_by_country ────────────────────────────────────

class NutriscoreItem(BaseModel):
    country:              Optional[str]   = None
    region:               Optional[str]   = None
    nutrition_grade_fr:   Optional[str]   = None
    nb_products:          Optional[int]   = None
    avg_nutriscore_score: Optional[float] = None


# ─── Additives — dm_additives_analysis ───────────────────────────────────────

class AdditiveItem(BaseModel):
    additive_tag:           Optional[str]   = None
    total_occurrences:      Optional[int]   = None
    nb_distinct_categories: Optional[int]   = None
    rank_overall:           Optional[int]   = None
    pct_of_total:           Optional[float] = None


# ─── ML Datamart — dm_ml_nutriscore_prediction ───────────────────────────────

class MLItem(BaseModel):
    # Colonnes exactes produites par datamart.py build_dm_ml_nutriscore_prediction()
    code:               Optional[str]   = None
    sugars_100g:        Optional[float] = None
    fat_100g:           Optional[float] = None
    saturated_fat_100g: Optional[float] = None
    salt_100g:          Optional[float] = None
    energy_100g:        Optional[float] = None
    proteins_100g:      Optional[float] = None
    fiber_100g:         Optional[float] = None
    additives_n:        Optional[int]   = None
    main_category:      Optional[str]   = None
    country_normalized: Optional[str]   = None
    population:         Optional[float] = None
    area_sq_mi:         Optional[float] = None
    nutrition_grade_fr: Optional[str]   = None

class MLStats(BaseModel):
    grade:         Optional[str]   = None
    count:         int
    avg_energy:    Optional[float] = None
    avg_fat:       Optional[float] = None
    avg_sugars:    Optional[float] = None
    avg_proteins:  Optional[float] = None
    avg_fiber:     Optional[float] = None
    avg_salt:      Optional[float] = None
    avg_additives: Optional[float] = None
