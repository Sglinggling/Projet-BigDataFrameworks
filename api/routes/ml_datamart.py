"""
routes/ml_datamart.py — Endpoints du datamart dm_ml_nutriscore_prediction

Dataset ML-ready : une ligne par produit, features numériques + target nutrition_grade_fr.
JWT Bearer requis sur tous les endpoints (Depends(get_current_user)).
"""

import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.auth import get_current_user
from api.database import get_db
from api.models import MLItem, MLStats, Page, UserORM

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ml", tags=["ML Datamart"])

_TABLE = "dm_ml_nutriscore_prediction"
# Colonnes dans l'ordre exact de datamart.py build_dm_ml_nutriscore_prediction()
_COLS = (
    "code, sugars_100g, fat_100g, saturated_fat_100g, salt_100g, "
    "energy_100g, proteins_100g, fiber_100g, additives_n, "
    "main_category, country_normalized, population, area_sq_mi, nutrition_grade_fr"
)


@router.get(
    "",
    response_model=Page[MLItem],
    summary="Dataset ML paginé",
)
def list_ml_dataset(
    page:  int = Query(1,   ge=1),
    limit: int = Query(100, ge=1, le=1000, description="Résultats par page (max 1000)"),
    _:  UserORM = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Dataset ML-ready complet, paginé.
    Une ligne = un produit unique (code) avec features numériques + target.
    Trié par code produit pour une pagination reproductible.
    """
    total = db.execute(text(f"SELECT COUNT(*) FROM {_TABLE}")).scalar()

    rows = db.execute(
        text(
            f"SELECT {_COLS} FROM {_TABLE} "
            f"ORDER BY code "
            f"LIMIT :limit OFFSET :offset"
        ),
        {"limit": limit, "offset": (page - 1) * limit},
    ).mappings().all()

    return Page(page=page, limit=limit, total=total, items=[dict(r) for r in rows])


@router.get(
    "/stats",
    response_model=list[MLStats],
    summary="Statistiques descriptives par grade Nutri-Score",
)
def ml_stats(
    _:  UserORM = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Agrégats par grade (A à E) : effectif et moyennes de toutes les features numériques.
    Utile pour vérifier l'équilibre des classes et les distributions avant entraînement ML.
    """
    rows = db.execute(
        text(
            f"SELECT "
            f"  nutrition_grade_fr        AS grade, "
            f"  COUNT(*)                  AS count, "
            f"  ROUND(AVG(energy_100g)::numeric,      2) AS avg_energy, "
            f"  ROUND(AVG(fat_100g)::numeric,         2) AS avg_fat, "
            f"  ROUND(AVG(sugars_100g)::numeric,      2) AS avg_sugars, "
            f"  ROUND(AVG(proteins_100g)::numeric,    2) AS avg_proteins, "
            f"  ROUND(AVG(fiber_100g)::numeric,       2) AS avg_fiber, "
            f"  ROUND(AVG(salt_100g)::numeric,        2) AS avg_salt, "
            f"  ROUND(AVG(additives_n)::numeric,      2) AS avg_additives "
            f"FROM {_TABLE} "
            f"WHERE nutrition_grade_fr IS NOT NULL "
            f"GROUP BY nutrition_grade_fr "
            f"ORDER BY nutrition_grade_fr"
        )
    ).mappings().all()
    return [dict(r) for r in rows]


@router.get(
    "/sample",
    response_model=list[MLItem],
    summary="Échantillon aléatoire du dataset ML",
)
def ml_sample(
    n: int = Query(10, ge=1, le=100, description="Taille de l'échantillon (max 100)"),
    _:  UserORM = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retourne N lignes aléatoires du dataset ML.
    Utile pour debug et aperçu rapide des données. Non reproductible (ORDER BY RANDOM()).
    """
    rows = db.execute(
        text(f"SELECT {_COLS} FROM {_TABLE} ORDER BY RANDOM() LIMIT :n"),
        {"n": n},
    ).mappings().all()
    return [dict(r) for r in rows]
