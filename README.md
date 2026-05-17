# Food Nutrition Big Data — Lakehouse Platform

Plateforme Big Data en architecture médaillon (Bronze → Silver → Gold) pour analyser
les habitudes alimentaires mondiales à partir de données open source (Open Food Facts, ~2,5 M
de produits). Le pipeline complet ingère, nettoie, agrège et expose les données via une
API REST sécurisée JWT et des dashboards Power BI.

Projet Master 1 Data Engineering — EFREI

[SCREENSHOT_SWAGGER : Swagger UI de l'API FastAPI avec les 11 endpoints]

---

## Architecture

```
Open Food Facts (CSV)  ──┐
                          ├──► feeder.py ──► MinIO raw/ (Parquet partitionné)
Countries of the World ──┘          │
                                     ▼
                              processor.py ──► Hive silver/ (tables internes)
                                     │
                                     ▼
                              datamart.py ──► PostgreSQL (4 datamarts)
                                     │
                          ┌──────────┴──────────┐
                          ▼                     ▼
                   API FastAPI (JWT)    export_for_powerbi.py
                   :8000/docs           → CSV UTF-8 BOM
                                         → Power BI Service
```

Documentation complète : [docs/architecture.md](docs/architecture.md)

---

## Stack technique

| Composant       | Technologie                      |
|-----------------|----------------------------------|
| Traitement      | PySpark 3.5.3 (spark-submit)     |
| Data Lake       | MinIO (compatible S3)            |
| Metastore       | Apache Hive 4.0 (Derby embedded) |
| Datamarts       | PostgreSQL 15                    |
| API             | FastAPI + JWT (python-jose)      |
| Orchestration   | Docker Compose                   |
| Export BI       | psycopg2 + CSV UTF-8 BOM         |
| Visualisation   | Power BI Service (web)           |

---

## Prérequis

- Docker Desktop >= 24.0
- Docker Compose >= 2.20
- Python >= 3.10 (pour l'export CSV et l'exécution locale hors Docker)
- RAM disponible recommandée : **8 Go minimum**

---

## Démarrage rapide

```bash
# 1. Cloner le dépôt
git clone <url-du-repo>
cd food-nutrition-bigdata-project

# 2. Télécharger les données sources
#    - Open Food Facts : https://world.openfoodfacts.org/data
#      → Placer en.openfoodfacts.org.products.csv dans data/input/openfoodfacts/
#    - Countries of the World : https://www.kaggle.com/datasets/fernandol/countries-of-the-world
#      → Placer countries of the world.csv dans data/input/countries/

# 3. Démarrer tous les services
./start_project.sh
# ou manuellement :
docker compose up -d

# 4. Vérifier que tous les services sont UP
docker compose ps
```

---

## Interfaces web

| Service          | URL                        | Identifiants               |
|------------------|----------------------------|----------------------------|
| MinIO Console    | http://localhost:9001      | minioadmin / minioadmin123 |
| Spark Master UI  | http://localhost:8080      | —                          |
| Spark Worker UI  | http://localhost:8082      | —                          |
| API FastAPI docs | http://localhost:8000/docs | —                          |

---

## Exécution du pipeline

```bash
# Phase 2 — Ingestion (feeder.py)
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    scripts/feeder.py --config config

# Phase 3 — Transformation silver (processor.py)
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    scripts/processor.py --config config

# Phase 4 — Datamarts PostgreSQL (datamart.py)
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    scripts/datamart.py --config config

# Phase 5 — Démarrage de l'API FastAPI
docker exec spark-master uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# Phase 6 — Export CSV pour Power BI (depuis l'hôte, pas dans Docker)
python3 scripts/export_for_powerbi.py --config config --output data/output/powerbi
```

---

## Export Power BI (Phase 6)

Les 4 datamarts PostgreSQL sont exportés en CSV (UTF-8 BOM, séparateur virgule) pour
import dans Power BI Service sur macOS.

```bash
# Générer les 4 fichiers CSV
python3 scripts/export_for_powerbi.py

# Fichiers produits dans data/output/powerbi/ :
#   dm_sugar_by_category.csv
#   dm_nutriscore_by_country.csv
#   dm_additives_analysis.csv
#   dm_ml_nutriscore_prediction.csv
```

Importer sur https://app.powerbi.com — voir le guide complet : [docs/powerbi_setup.md](docs/powerbi_setup.md)

[SCREENSHOT_POWERBI : Dashboard Power BI — carte choroplèthe Nutri-Score par pays]

---

## Structure du projet

```
├── config/                        # Fichiers de configuration YAML
│   ├── app_config.yaml            # Chemins, tables Hive, logging
│   ├── spark_config.yaml          # SparkSession, S3A/MinIO, Hive metastore
│   ├── database_config.yaml       # PostgreSQL JDBC + SQLAlchemy
│   └── auth_config.yaml           # Secret JWT, algorithme, expiration
├── data/
│   ├── input/openfoodfacts/       # Fichier CSV Open Food Facts (à déposer ici)
│   ├── input/countries/           # Fichier CSV Countries (à déposer ici)
│   ├── output/powerbi/            # CSV exportés pour Power BI (générés)
│   └── sample/                    # Échantillons pour tests rapides
├── scripts/
│   ├── feeder.py                  # Ingestion → couche raw Parquet (MinIO)
│   ├── processor.py               # Nettoyage + jointures → couche silver (Hive)
│   ├── datamart.py                # Agrégats finaux → PostgreSQL (4 datamarts)
│   └── export_for_powerbi.py      # Export CSV depuis PostgreSQL → Power BI
├── api/                           # API REST FastAPI avec JWT
│   ├── main.py                    # Point d'entrée, CORS, routers
│   ├── auth.py                    # JWT + endpoints /auth/token et /auth/register
│   ├── database.py                # SQLAlchemy engine + session
│   ├── models.py                  # Pydantic models + ORM UserORM
│   └── routes/                    # Un router par datamart
│       ├── sugar.py               # /datamart/sugar (2 endpoints)
│       ├── nutriscore.py          # /datamart/nutriscore (2 endpoints)
│       ├── additives.py           # /datamart/additives (2 endpoints)
│       └── ml_datamart.py         # /datamart/ml (3 endpoints)
├── docs/
│   ├── architecture.md            # Architecture médaillon + justifications
│   ├── api_documentation.md       # Documentation des 11 endpoints + exemples curl
│   └── powerbi_setup.md           # Guide import CSV Power BI Service (macOS)
├── sql/                           # Scripts SQL de création et indexation
├── logs/                          # Fichiers de logs .txt générés à l'exécution
├── notebooks/                     # Analyses exploratoires Jupyter
├── powerbi/                       # Fichiers Power BI (.pbix si disponibles)
├── docker-compose.yml
├── start_project.sh               # Script de démarrage rapide
└── requirements.txt
```

---

## Documentation

| Document | Contenu |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Architecture médaillon, flux de données, justifications techniques |
| [docs/api_documentation.md](docs/api_documentation.md) | 11 endpoints, workflow JWT, exemples curl, pagination |
| [docs/powerbi_setup.md](docs/powerbi_setup.md) | Import CSV dans Power BI Service, 4 dashboards suggérés |

---

## Arrêt et reset

```bash
# Arrêter les services (données conservées)
docker compose down

# Arrêter ET supprimer toutes les données (volumes)
docker compose down -v
```

---

## Crédits

Projet réalisé par :
- Halit Salih — [s.halit@federation-eben.com](mailto:s.halit@federation-eben.com)
- [Nom de la binôme]

Encadrement : M1 Data Engineering — EFREI Paris
