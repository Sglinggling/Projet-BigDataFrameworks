# Architecture — Food Nutrition Big Data Lakehouse

Projet M1 Data Engineering — EFREI  
Architecture médaillon Bronze → Silver → Gold appliquée à l'analyse alimentaire mondiale.

---

## Vue d'ensemble du pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│  SOURCES DE DONNÉES                                                     │
│                                                                         │
│  Open Food Facts (TSV ~3 Go)  ──┐                                       │
│  en.openfoodfacts.org (~2,5M    │   feeder.py (PySpark spark-submit)    │
│  produits alimentaires)         ├──► Ingestion + validation + écriture  │
│                                 │   Parquet partitionné                 │
│  Countries of the World (~230   │   year=YYYY/month=MM/day=DD           │
│  pays, données socio-économ.)  ──┘                                      │
└────────────────────────────────────────┬────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  COUCHE BRONZE — Raw (MinIO / S3A)                                      │
│                                                                         │
│  Bucket : raw/                                                          │
│  ├── openfoodfacts/year=2024/month=01/day=15/*.parquet                  │
│  └── countries/year=2024/month=01/day=15/*.parquet                      │
│                                                                         │
│  Contenu brut, 16 colonnes OFF sélectionnées, aucune transformation.   │
└────────────────────────────────────────┬────────────────────────────────┘
                                         │
                                         ▼ processor.py (PySpark spark-submit)
                                         │ • Nettoyage, cast de types
                                         │ • Validation (5 règles)
                                         │ • Jointure produits × pays
                                         │ • Window functions (rang sucre)
                                         │ • cache() / persist() actifs
┌─────────────────────────────────────────────────────────────────────────┐
│  COUCHE SILVER — Tables Hive (Derby embedded + MinIO warehouse)         │
│                                                                         │
│  Base Hive : food_nutrition (silver_database)                           │
│  ├── silver_products       — produits OFF nettoyés (~200 000 lignes)    │
│  ├── silver_countries      — pays nettoyés (colonnes snake_case)        │
│  └── silver_food_by_country — jointure produits × pays + agrégats       │
│                                                                         │
│  Stockage physique : s3a://warehouse/ (MinIO)                           │
└────────────────────────────────────────┬────────────────────────────────┘
                                         │
                                         ▼ datamart.py (PySpark spark-submit)
                                         │ • Agrégations finales
                                         │ • Window functions (rangs)
                                         │ • Écriture JDBC PostgreSQL
┌─────────────────────────────────────────────────────────────────────────┐
│  COUCHE GOLD — Datamarts PostgreSQL 15                                  │
│                                                                         │
│  Base : food_nutrition                                                  │
│  ├── dm_sugar_by_category         — Top 10 produits sucrés / catégorie  │
│  ├── dm_nutriscore_by_country     — Nutri-Score moyen par pays/région   │
│  ├── dm_additives_analysis        — Additifs fréquents + rang global    │
│  └── dm_ml_nutriscore_prediction  — Dataset ML-ready (features + target)│
└──────────────┬──────────────────────────────┬───────────────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────┐          ┌───────────────────────────────────────┐
│  API REST FastAPI    │          │  Export CSV (export_for_powerbi.py)   │
│  11 endpoints + JWT  │          │  data/output/powerbi/*.csv            │
│  :8000/docs          │          │  UTF-8 BOM, séparateur virgule        │
└──────────────┬───────┘          └────────────────────┬──────────────────┘
               │                                       │
               └───────────────────┬───────────────────┘
                                   ▼
                        ┌──────────────────────┐
                        │  Power BI Service    │
                        │  app.powerbi.com     │
                        │  Import CSV          │
                        └──────────────────────┘
```

---

## Description des 4 couches

### Couche Bronze — Raw (MinIO)

Ingestion brute depuis les fichiers sources. Aucune transformation métier. Le rôle de cette couche est de persister les données d'origine dans un format optimisé (Parquet) et de constituer un point de reprise fiable en cas d'erreur dans les étapes suivantes.

- Format : Parquet (columnar, compression Snappy)
- Partitionnement : `year/month/day` (permet les lectures filtrées par date)
- 16 colonnes d'Open Food Facts sélectionnées sur ~184 disponibles
- 5 règles de validation avant écriture (volume minimum, types, valeurs nulles critiques)

### Couche Silver — Tables Hive

Nettoyage, enrichissement et jointure. Cette couche produit des tables fiables et normalisées, directement exploitables par les analyses.

- Nettoyage des chaînes (strip, lowercase sur les colonnes catégorielles)
- Cast explicite de types (energies, sucres, sel → Double)
- Jointure `silver_products × silver_countries` sur la colonne `countries_en`
- Window function `ROW_NUMBER() OVER (PARTITION BY main_category ORDER BY sugars_100g DESC)`
- `cache()` et `persist(MEMORY_AND_DISK)` sur les DataFrames centraux pour éviter les relectures S3A

### Couche Gold — Datamarts PostgreSQL

Agrégats finaux orientés usage métier (BI) et machine learning. Chaque datamart répond à une question analytique précise et est optimisé pour les requêtes de lecture.

| Datamart | Question analytique | Window function |
|---|---|---|
| `dm_sugar_by_category` | Quels produits sont les plus sucrés dans chaque catégorie ? | `ROW_NUMBER() OVER (PARTITION BY category)` |
| `dm_nutriscore_by_country` | Quel est le Nutri-Score moyen par pays/région ? | Agrégation simple |
| `dm_additives_analysis` | Quels additifs reviennent le plus souvent ? | `ROW_NUMBER() OVER (ORDER BY occurrences DESC)` |
| `dm_ml_nutriscore_prediction` | Quelles features numériques prédisent le grade ? | Déduplication `dropDuplicates(["code"])` |

### Couche Exposition

- **API REST FastAPI** : accès JWT sécurisé aux datamarts, pagination, filtres dynamiques
- **Export CSV** : fichiers consommables par Power BI Service (web) sans connecteur natif PostgreSQL

---

## Justification des choix techniques

### PySpark 3.5 (spark-submit)

La volumétrie d'Open Food Facts (~3 Go, ~2,5 millions de produits) dépasse la capacité d'un traitement pandas mono-thread. PySpark distribue les transformations sur plusieurs workers et offre les primitives nécessaires au projet (window functions, `saveAsTable`, JDBC write). L'utilisation exclusive de `spark-submit` garantit que la SparkSession est correctement attachée au master cluster.

### MinIO (compatible S3)

MinIO offre une compatibilité totale avec l'API S3 d'Amazon (protocole S3A), ce qui permet d'utiliser les JARs `hadoop-aws` standard sans modification. Déployé on-premise dans Docker, il évite toute dépendance à un cloud payant tout en simulant fidèlement un environnement de production.

### Apache Hive (Derby embedded)

Le metastore Hive centralise le catalogue des tables silver (schéma, localisation S3A, partitions). `enableHiveSupport()` dans la SparkSession permet d'utiliser `df.write.saveAsTable()` qui enregistre les métadonnées automatiquement. Derby embedded simplifie le déploiement (pas de service Hive externe) au prix d'une limitation : pas d'accès concurrent depuis plusieurs processus JVM simultanés.

### PostgreSQL 15

PostgreSQL est le choix standard pour les datamarts BI. Son support JDBC natif par Spark, ses index B-tree, et sa compatibilité avec Power BI (connecteur natif) en font un choix robuste. Les datamarts sont de taille raisonnable (quelques dizaines de milliers de lignes) — PostgreSQL est dimensionné pour cet usage.

### FastAPI + JWT

FastAPI génère automatiquement une documentation Swagger UI (`/docs`) et une documentation ReDoc (`/redoc`). L'authentification JWT (HS256, python-jose) est stateless, compatible avec Power BI Service. La validation des schémas d'entrée/sortie est assurée par Pydantic.

### Docker Compose

Docker Compose garantit la reproductibilité de l'environnement sur toutes les machines de développement. Un seul fichier `docker-compose.yml` suffit à démarrer les 4 services (Spark master, Spark worker, MinIO, PostgreSQL) avec leurs volumes persistants et leurs configurations réseau.

---

## Flux de données détaillé

```
Open Food Facts CSV (TSV)
        │
        │  feeder.py — lecture en streaming Spark
        │  → sélection de 16 colonnes sur ~184
        │  → cast de types (StructType OFF_SCHEMA)
        │  → remplacement des colonnes absentes par lit(None)
        │  → validation : volume > 200 000 lignes
        │  → partitionBy("year", "month", "day")
        ▼
  MinIO s3a://raw/openfoodfacts/
        │
        │  processor.py — lecture Parquet
        │  → nettoyage / normalisation
        │  → validation (5 règles : nulls, types, plages)
        │  → JOIN silver_products × silver_countries
        │  → window function sugar_rank_in_category
        │  → persist(MEMORY_AND_DISK)
        │  → saveAsTable("food_nutrition.silver_*")
        ▼
  Hive silver_food_by_country (~N×M lignes, une par produit×pays)
        │
        │  datamart.py — lecture table Hive
        │  → dropDuplicates selon le datamart
        │  → agrégations (groupBy, count, avg, explode)
        │  → window functions (ROW_NUMBER)
        │  → write.jdbc() mode overwrite → PostgreSQL
        ▼
  PostgreSQL dm_* (4 tables datamarts)
        │
        ├──► API FastAPI (lecture JDBC via SQLAlchemy)
        │    → filtres dynamiques, pagination, JWT
        │
        └──► export_for_powerbi.py (lecture psycopg2)
             → CSV UTF-8 BOM → data/output/powerbi/
             → import dans Power BI Service
```

---

## Conventions de nommage

| Préfixe | Couche | Exemples |
|---|---|---|
| *(pas de préfixe)* | Raw Parquet (MinIO) | `s3a://raw/openfoodfacts/` |
| `silver_` | Tables Hive nettoyées | `silver_products`, `silver_food_by_country` |
| `dm_` | Datamarts PostgreSQL | `dm_sugar_by_category`, `dm_additives_analysis` |
| `export_` | Scripts d'export/utilitaires | `export_for_powerbi.py` |

Colonnes avec tiret dans les noms OFF d'origine (`energy-kcal_100g`, `saturated-fat_100g`) sont aliasées en snake_case dans les datamarts PostgreSQL (`energy_100g`, `saturated_fat_100g`).
