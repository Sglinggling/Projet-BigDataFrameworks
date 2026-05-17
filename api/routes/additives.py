"""
routes/additives.py — Endpoints du datamart dm_additives_analysis

Additifs alimentaires les plus fréquents, rang global calculé par window function.
JWT Bearer requis sur tous les endpoints (Depends(get_current_user)).
"""

import logging

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.auth import get_current_user
from api.database import get_db
from api.models import AdditiveItem, Page, UserORM

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/additives", tags=["Additives"])

_TABLE = "dm_additives_analysis"
_COLS  = "additive_tag, total_occurrences, nb_distinct_categories, rank_overall, pct_of_total"


@router.get(
    "",
    response_model=Page[AdditiveItem],
    summary="Liste paginée des additifs par rang global",
)
def list_additives(
    page:  int = Query(1,  ge=1),
    limit: int = Query(50, ge=1, le=500),
    _:  UserORM = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Additifs triés par fréquence décroissante (rank_overall = 1 → additif le plus fréquent)."""
    total = db.execute(text(f"SELECT COUNT(*) FROM {_TABLE}")).scalar()

    rows = db.execute(
        text(
            f"SELECT {_COLS} FROM {_TABLE} "
            f"ORDER BY rank_overall "
            f"LIMIT :limit OFFSET :offset"
        ),
        {"limit": limit, "offset": (page - 1) * limit},
    ).mappings().all()

    return Page(page=page, limit=limit, total=total, items=[dict(r) for r in rows])


@router.get(
    "/top/{n}",
    response_model=list[AdditiveItem],
    summary="Top N additifs les plus fréquents",
)
def top_additives(
    n:  int = Path(ge=1, le=100, description="Nombre d'additifs à retourner (1–100)"),
    _:  UserORM = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Retourne les N additifs de rang 1 à N (fréquence décroissante)."""
    rows = db.execute(
        text(f"SELECT {_COLS} FROM {_TABLE} ORDER BY rank_overall LIMIT :n"),
        {"n": n},
    ).mappings().all()
    return [dict(r) for r in rows]
