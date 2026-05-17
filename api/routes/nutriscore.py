"""
routes/nutriscore.py — Endpoints du datamart dm_nutriscore_by_country

Nutri-Score moyen par pays et région (jointure produits × pays Phase 3).
JWT Bearer requis sur tous les endpoints (Depends(get_current_user)).
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.auth import get_current_user
from api.database import get_db
from api.models import NutriscoreItem, Page, UserORM

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/nutriscore", tags=["Nutriscore"])

_TABLE = "dm_nutriscore_by_country"


def _build_where(
    country: Optional[str],
    region:  Optional[str],
    grade:   Optional[str],
) -> tuple[str, dict]:
    clauses: list[str] = []
    params:  dict      = {}
    if country:
        clauses.append("LOWER(country) = LOWER(:country)")
        params["country"] = country
    if region:
        clauses.append("LOWER(region) = LOWER(:region)")
        params["region"] = region
    if grade:
        clauses.append("LOWER(nutrition_grade_fr) = :grade")
        params["grade"] = grade.lower()
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


@router.get(
    "",
    response_model=Page[NutriscoreItem],
    summary="Liste paginée Nutri-Score par pays",
)
def list_nutriscore(
    page:             int            = Query(1,  ge=1),
    limit:            int            = Query(50, ge=1, le=500),
    country:          Optional[str]  = Query(None, description="Filtrer par pays (insensible à la casse)"),
    region:           Optional[str]  = Query(None, description="Filtrer par région"),
    nutriscore_grade: Optional[str]  = Query(None, description="Grade Nutri-Score : a / b / c / d / e"),
    _:  UserORM = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Nutri-Score moyen par combinaison (pays, région, grade).
    Trié par score moyen croissant (meilleurs pays en premier).
    """
    where, params = _build_where(country, region, nutriscore_grade)

    total = db.execute(text(f"SELECT COUNT(*) FROM {_TABLE} {where}"), params).scalar()

    rows = db.execute(
        text(
            f"SELECT country, region, nutrition_grade_fr, nb_products, avg_nutriscore_score "
            f"FROM {_TABLE} {where} "
            f"ORDER BY avg_nutriscore_score ASC NULLS LAST "
            f"LIMIT :limit OFFSET :offset"
        ),
        {**params, "limit": limit, "offset": (page - 1) * limit},
    ).mappings().all()

    return Page(page=page, limit=limit, total=total, items=[dict(r) for r in rows])


@router.get("/summary", summary="Agrégats par grade Nutri-Score")
def nutriscore_summary(
    _:  UserORM = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Nombre de lignes (pays × grade) et score moyen global pour chaque grade A–E."""
    rows = db.execute(
        text(
            f"SELECT "
            f"  nutrition_grade_fr AS grade, "
            f"  COUNT(*) AS count, "
            f"  ROUND(AVG(avg_nutriscore_score)::numeric, 2) AS global_avg_score "
            f"FROM {_TABLE} "
            f"WHERE nutrition_grade_fr IS NOT NULL "
            f"GROUP BY nutrition_grade_fr ORDER BY nutrition_grade_fr"
        )
    ).mappings().all()
    return {"summary": [dict(r) for r in rows]}
