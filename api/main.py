"""
main.py — Point d'entrée de l'API Food Nutrition

Configure :
  - Logging vers logs/api_YYYYMMDD.txt (LOG_DIR env ou logs/ local)
  - CORS ouvert (Power BI + tests navigateur)
  - Création de la table `users` au démarrage (lifespan)
  - Routers : /auth, /datamart/sugar, /datamart/nutriscore,
              /datamart/additives, /datamart/ml

Démarrage local :
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

Démarrage Docker :
    docker compose up -d api
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from api.database import engine
from api.models import Base

# ─── Logging fichier horodaté ─────────────────────────────────────────────────

_LOG_DIR = Path(os.environ.get("LOG_DIR", Path(__file__).parent.parent / "logs"))
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_log_file = _LOG_DIR / f"api_{datetime.now().strftime('%Y%m%d')}.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


# ─── Lifecycle ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Crée la table `users` si absente au démarrage, la laisse intacte sinon."""
    logger.info("Démarrage API Food Nutrition — log : %s", _log_file)
    Base.metadata.create_all(bind=engine)
    logger.info("Tables Postgres initialisées — API prête sur http://0.0.0.0:8000")
    yield
    logger.info("Arrêt de l'API")


# ─── Application ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Food Nutrition API",
    description=(
        "API REST sécurisée JWT pour les datamarts Big Data alimentaires (EFREI M1).\n\n"
        "**Workflow d'authentification :**\n"
        "1. `POST /auth/register` — créer un compte (JSON)\n"
        "2. `POST /auth/token` — obtenir un JWT (form-data)\n"
        "3. Cliquer sur **Authorize** dans Swagger UI et coller le token\n"
        "4. Tous les endpoints `/datamart/...` sont maintenant accessibles"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS ouvert — permet les appels depuis Power BI et les outils de test
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Routers ─────────────────────────────────────────────────────────────────

from api.auth                import router as auth_router
from api.routes.sugar        import router as sugar_router
from api.routes.nutriscore   import router as nutriscore_router
from api.routes.additives    import router as additives_router
from api.routes.ml_datamart  import router as ml_router

app.include_router(auth_router)                                 # /auth/token, /auth/register
app.include_router(sugar_router,      prefix="/datamart")       # /datamart/sugar/...
app.include_router(nutriscore_router, prefix="/datamart")       # /datamart/nutriscore/...
app.include_router(additives_router,  prefix="/datamart")       # /datamart/additives/...
app.include_router(ml_router,         prefix="/datamart")       # /datamart/ml/...


# ─── Endpoints racine ─────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    """Message de bienvenue avec liens utiles."""
    return {
        "message": "Food Nutrition API v1.0.0",
        "docs":    "http://localhost:8000/docs",
        "health":  "http://localhost:8000/health",
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Vérifie la connexion PostgreSQL — utile pour les health checks Docker."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok", "database": "connected"}
    except Exception as exc:
        logger.error("Health check PostgreSQL échoué : %s", exc)
        return {"status": "error", "database": str(exc)}
