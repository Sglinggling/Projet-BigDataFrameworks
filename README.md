# Food Nutrition Big Data — Lakehouse Platform

Plateforme Big Data en architecture médaillon (Bronze → Silver → Gold) pour analyser
les habitudes alimentaires mondiales à partir de données open source.

Projet Master 1 Data Engineering — EFREI

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
                                     ▼
                              API FastAPI (JWT) ──► Power BI
```

## Stack technique

| Composant       | Technologie              |
|-----------------|--------------------------|
| Traitement      | PySpark 3.5 (spark-submit) |
| Data Lake       | MinIO (compatible S3)    |
| Metastore       | Apache Hive 4.0          |
| Datamarts       | PostgreSQL 15            |
| API             | FastAPI + JWT            |
| Orchestration   | Docker Compose           |

## Prérequis

- Docker Desktop >= 24.0
- Docker Compose >= 2.20
- Python >= 3.10 (pour exécution locale hors Docker)
- RAM disponible recommandée : **8 Go minimum**

## Lancement de la plateforme

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
docker compose up -d

# 4. Vérifier que tous les services sont UP
docker compose ps
```

## Interfaces web

| Service          | URL                       | Identifiants               |
|------------------|---------------------------|----------------------------|
| MinIO Console    | http://localhost:9001     | minioadmin / minioadmin123 |
| Spark Master UI  | http://localhost:8080     | —                          |
| Spark Worker UI  | http://localhost:8081     | —                          |
| API FastAPI docs | http://localhost:8000/docs | —                         |

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
```

## Structure du projet

```
├── config/                     # Fichiers de configuration YAML
│   ├── app_config.yaml         # Chemins, tables Hive, logging
│   ├── spark_config.yaml       # SparkSession, S3A/MinIO, Hive metastore
│   └── database_config.yaml    # PostgreSQL JDBC + SQLAlchemy
├── data/
│   ├── input/openfoodfacts/    # Fichier CSV Open Food Facts (à déposer ici)
│   ├── input/countries/        # Fichier CSV Countries (à déposer ici)
│   └── sample/                 # Échantillons pour tests rapides
├── scripts/
│   ├── feeder.py               # Ingestion → couche raw Parquet (MinIO)
│   ├── processor.py            # Nettoyage + jointures → couche silver (Hive)
│   └── datamart.py             # Agrégats finaux → PostgreSQL
├── api/                        # API REST FastAPI avec JWT
│   ├── main.py
│   ├── auth.py
│   ├── database.py
│   ├── models.py
│   └── routes/                 # Un fichier par datamart
├── sql/                        # Scripts SQL de création et indexation
├── logs/                       # Fichiers de logs .txt générés à l'exécution
├── notebooks/                  # Analyses exploratoires Jupyter
├── powerbi/                    # Fichiers .pbix Power BI
├── docs/                       # Documentation technique
├── docker-compose.yml
└── requirements.txt
```

## Arrêt et reset

```bash
# Arrêter les services (données conservées)
docker compose down

# Arrêter ET supprimer toutes les données (volumes)
docker compose down -v
```
