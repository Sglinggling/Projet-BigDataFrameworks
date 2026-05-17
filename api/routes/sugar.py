"""
routes/sugar.py — Endpoints du datamart dm_sugar_by_category

Top produits les plus sucrés par catégorie, avec rang calculé par window function.
JWT Bearer requis sur tous les endpoints (Depends(get_current_user)).
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.auth import get_current_user
from api.database import get_db
from api.models import Page, SugarItem, UserORM

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sugar", tags=["Sugar"])

_TABLE = "dm_sugar_by_category"


def _build_where(
    category:   Optional[str],
    min_sugars: Optional[float],
    grade:      Optional[str],
) -> tuple[str, dict]:
    """Construit la clause WHERE et les paramètres nommés selon les filtres actifs."""
    clauses: list[str] = []
    params:  dict      = {}
    if category:
        clauses.append("category = :category")
        params["category"] = category
    if min_sugars is not None:
        clauses.append("sugars_100g >= :min_sugars")
        params["min_sugars"] = min_sugars
    if grade:
        clauses.append("LOWER(nutrition_grade_fr) = :grade")
        params["grade"] = grade.lower()
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


@router.get(
    "",
    response_model=Page[SugarItem],
    summary="Liste paginée des produits sucrés par catégorie",
)
def list_sugar(
    page:             int            = Query(1,    ge=1,        description="Numéro de page (commence à 1)"),
    limit:            int            = Query(50,   ge=1, le=500, description="Résultats par page"),
    category:         Optional[str]  = Query(None,              description="Filtrer par catégorie"),
    min_sugars:       Optional[float]= Query(None, ge=0,        description="Teneur en sucre minimale (g/100g)"),
    nutriscore_grade: Optional[str]  = Query(None,              description="Grade Nutri-Score : a / b / c / d / e"),
    _:  UserORM = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Produits triés par catégorie puis rang de teneur en sucre.
    Les filtres `category`, `min_sugars` et `nutriscore_grade` sont cumulables.
    """
    where, params = _build_where(category, min_sugars, nutriscore_grade)

    total = db.execute(
        text(f"SELECT COUNT(*) FROM {_TABLE} {where}"), params
    ).scalar()

    rows = db.execute(
        text(
            f"SELECT category, product_name, sugars_100g, nutrition_grade_fr, sugar_rank_in_category "
            f"FROM {_TABLE} {where} "
            f"ORDER BY category, sugar_rank_in_category "
            f"LIMIT :limit OFFSET :offset"
        ),
        {**params, "limit": limit, "offset": (page - 1) * limit},
    ).mappings().all()

    return Page(page=page, limit=limit, total=total, items=[dict(r) for r in rows])


@router.get(
    "/categories",
    summary="Liste des catégories disponibles",
)
def list_categories(
    _:  UserORM = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Retourne les catégories distinctes — utile pour alimenter des filtres côté client."""
    rows = db.execute(
        text(f"SELECT DISTINCT category FROM {_TABLE} WHERE category IS NOT NULL ORDER BY category")
    ).scalars().all()
    return {"categories": list(rows), "count": len(rows)}
