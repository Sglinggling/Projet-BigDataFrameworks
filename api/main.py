"""
main.py — Point d'entrée de l'API FastAPI

Rôle  : Initialiser l'application FastAPI, enregistrer les routers
         de chaque datamart, configurer CORS et la documentation Swagger.

Démarrage :
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

TODO Phase 5 :
    - Enregistrer les routers (sugar, nutriscore, additives, ml_datamart)
    - Configurer CORS pour Power BI
    - Ajouter les middlewares de logging des requêtes
"""

from fastapi import FastAPI

# from api.routes import sugar, nutriscore, additives, ml_datamart  # TODO Phase 5

app = FastAPI(
    title="Food Nutrition API",
    description="API REST sécurisée JWT pour les datamarts Big Data alimentaires",
    version="1.0.0",
)

# TODO Phase 5 : app.include_router(sugar.router, prefix="/api/v1/sugar", tags=["Sugar"])
# TODO Phase 5 : app.include_router(nutriscore.router, prefix="/api/v1/nutriscore", tags=["Nutriscore"])
# TODO Phase 5 : app.include_router(additives.router, prefix="/api/v1/additives", tags=["Additives"])
# TODO Phase 5 : app.include_router(ml_datamart.router, prefix="/api/v1/ml", tags=["ML"])


@app.get("/health")
def health_check():
    return {"status": "ok"}
