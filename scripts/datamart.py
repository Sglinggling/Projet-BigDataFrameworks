"""
datamart.py — Production des 4 datamarts dans PostgreSQL

Rôle  : Lire les tables Hive silver, calculer les agrégats finaux
         et écrire chaque datamart dans PostgreSQL via JDBC.

Datamarts produits :
    - dm_sugar_by_category         : produits les plus sucrés par catégorie
    - dm_nutriscore_by_country      : Nutri-Score moyen par pays/région
    - dm_additives_analysis         : additifs les plus fréquents
    - dm_ml_nutriscore_prediction   : dataset ML-ready (features + target)

Exécution via spark-submit :
    spark-submit \\
        --master spark://spark-master:7077 \\
        scripts/datamart.py --config config
"""

import argparse
import logging
import os
import sys
from datetime import datetime

import yaml
from pyspark.sql import DataFrame, SparkSession


# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging(log_dir: str, script_name: str) -> logging.Logger:
    """
    Configure le logger pour écrire simultanément dans un fichier .txt et stdout.

    Args:
        log_dir:     Répertoire de sortie des logs (depuis app_config.yaml).
        script_name: Nom du script, utilisé pour nommer le fichier de log.

    Returns:
        Logger configuré prêt à l'emploi.
    """
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.txt")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logger = logging.getLogger(script_name)
    logger.info("Logging initialisé — fichier : %s", log_file)
    return logger


# ─── Configuration ────────────────────────────────────────────────────────────

def load_config(config_dir: str) -> dict:
    """
    Charge les trois fichiers YAML depuis config_dir.

    Args:
        config_dir: Chemin vers le répertoire contenant les fichiers .yaml.

    Returns:
        Dictionnaire imbriqué avec les clés 'app', 'spark', 'database'.
    """
    config = {}
    for name in ("app_config", "spark_config", "database_config"):
        path = os.path.join(config_dir, f"{name}.yaml")
        with open(path, "r", encoding="utf-8") as fh:
            config[name.replace("_config", "")] = yaml.safe_load(fh)
    return config


# ─── SparkSession ─────────────────────────────────────────────────────────────

def create_spark_session(config: dict, logger: logging.Logger) -> SparkSession:
    """
    Crée la SparkSession avec support Hive (lecture tables silver) et JDBC PostgreSQL.

    Args:
        config: Configuration fusionnée depuis load_config().
        logger: Logger actif.

    Returns:
        SparkSession prête à l'emploi.
    """
    spark_cfg = config["spark"]["spark"]
    s3a_cfg = config["spark"]["s3a"]
    hive_cfg = config["spark"]["hive"]
    packages = ",".join(config["spark"].get("packages", []))

    spark = (
        SparkSession.builder
        .appName(f"{spark_cfg['app_name']}_datamart")
        .master(spark_cfg["master"])
        .config("spark.executor.memory", spark_cfg["executor_memory"])
        .config("spark.driver.memory", spark_cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions", spark_cfg["sql_shuffle_partitions"])
        # Hive embedded (Derby local) — pas de service hive-metastore externe
        .config("spark.sql.warehouse.dir",               hive_cfg["warehouse_dir"])
        .config("javax.jdo.option.ConnectionURL",         f"jdbc:derby:;databaseName={hive_cfg['derby_db_path']};create=true")
        .config("javax.jdo.option.ConnectionDriverName",  "org.apache.derby.jdbc.EmbeddedDriver")
        .config("datanucleus.schema.autoCreateAll",       "true")
        .config("datanucleus.autoCreateSchema",           "true")
        .config("spark.hadoop.fs.s3a.endpoint", s3a_cfg["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", s3a_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", s3a_cfg["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", str(s3a_cfg["path_style_access"]).lower())
        .config("spark.hadoop.fs.s3a.impl", s3a_cfg["impl"])
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(s3a_cfg["connection_ssl_enabled"]).lower())
        .config("spark.jars.packages", packages)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession créée — master : %s", spark.sparkContext.master)
    return spark


# ─── Lecture des tables Hive silver ───────────────────────────────────────────

def read_silver_table(spark: SparkSession, table_name: str, config: dict, logger: logging.Logger) -> DataFrame:
    """
    Lit une table Hive de la couche silver.

    Étapes prévues (Phase 4) :
    1. Construction du nom complet : <database>.<table_name>
    2. spark.table() pour lire depuis le metastore Hive
    3. Cache() du DataFrame + action count() pour matérialiser
    4. Log du nombre de lignes

    Args:
        spark:      SparkSession active.
        table_name: Nom de la table (clé dans app_config.yaml hive.tables).
        config:     Configuration chargée.
        logger:     Logger actif.

    Returns:
        DataFrame lu depuis Hive, persisté en mémoire.
    """
    pass  # TODO Phase 4


# ─── Construction des datamarts ───────────────────────────────────────────────

def build_dm_sugar_by_category(df_silver: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Construit le datamart dm_sugar_by_category.

    Calculs prévus (Phase 4) :
    - GroupBy catégorie → moyenne et max de sugars_100g
    - Window function : rang de chaque produit dans sa catégorie (par sucre décroissant)
    - Filtre sur les catégories avec au moins N produits
    - Colonnes : category, product_name, sugars_100g, avg_sugars_category, rank_in_category

    Args:
        df_silver: DataFrame silver_food_by_country.
        logger:    Logger actif.

    Returns:
        DataFrame du datamart prêt à être écrit en PostgreSQL.
    """
    pass  # TODO Phase 4


def build_dm_nutriscore_by_country(df_silver: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Construit le datamart dm_nutriscore_by_country.

    Calculs prévus (Phase 4) :
    - GroupBy pays et région → Nutri-Score moyen (A=1, B=2, … E=5)
    - Window function : rang des pays par Nutri-Score moyen par région
    - Colonnes : country, region, avg_nutriscore_numeric, nutriscore_rank_in_region,
                 total_products, population

    Args:
        df_silver: DataFrame silver_food_by_country.
        logger:    Logger actif.

    Returns:
        DataFrame du datamart prêt à être écrit en PostgreSQL.
    """
    pass  # TODO Phase 4


def build_dm_additives_analysis(df_silver: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Construit le datamart dm_additives_analysis.

    Calculs prévus (Phase 4) :
    - Explode de la colonne additives (liste → lignes individuelles)
    - GroupBy additif → count, catégories concernées
    - Window function : rang de l'additif dans sa catégorie
    - Colonnes : additive_code, additive_name, total_products, top_category,
                 rank_global, pct_of_total

    Args:
        df_silver: DataFrame silver_products avec colonne additives_list.
        logger:    Logger actif.

    Returns:
        DataFrame du datamart prêt à être écrit en PostgreSQL.
    """
    pass  # TODO Phase 4


def build_dm_ml_nutriscore_prediction(df_silver: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Construit le dataset ML-ready dm_ml_nutriscore_prediction.

    Préparation prévue (Phase 4) :
    - Sélection des features numériques : energy_100g, fat_100g, saturated_fat_100g,
      sugars_100g, fiber_100g, proteins_100g, salt_100g, additives_count
    - Target : nutriscore_grade encodé numériquement (A=0, B=1, C=2, D=3, E=4)
    - Suppression des lignes avec nulls sur les features
    - Ajout d'un identifiant unique (product_id)
    - Colonnes finales : product_id, <features>, nutriscore_target

    Args:
        df_silver: DataFrame silver_products.
        logger:    Logger actif.

    Returns:
        DataFrame ML-ready prêt à être écrit en PostgreSQL.
    """
    pass  # TODO Phase 4


# ─── Écriture PostgreSQL ──────────────────────────────────────────────────────

def write_to_postgres(df: DataFrame, table_name: str, config: dict, logger: logging.Logger) -> None:
    """
    Écrit un DataFrame dans une table PostgreSQL via JDBC.

    Étapes prévues (Phase 4) :
    1. Construction de l'URL JDBC depuis database_config.yaml
    2. df.write.jdbc() en mode overwrite (configurable)
    3. Log du nombre de lignes écrites et du temps d'exécution

    Args:
        df:         DataFrame à écrire.
        table_name: Nom de la table cible dans PostgreSQL.
        config:     Configuration chargée.
        logger:     Logger actif.
    """
    pass  # TODO Phase 4


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="datamart.py — Production des datamarts PostgreSQL")
    parser.add_argument(
        "--config",
        default="config",
        help="Chemin vers le répertoire de configuration YAML (défaut : config/)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config["app"]["logging"]["log_dir"], "datamart")

    dm_names = config["database"]["datamarts"]
    hive_tables = config["app"]["hive"]["tables"]

    logger.info("=== Démarrage datamart.py ===")
    spark = None
    try:
        spark = create_spark_session(config, logger)

        df_silver = read_silver_table(spark, hive_tables["silver_joined"], config, logger)

        dm_sugar = build_dm_sugar_by_category(df_silver, logger)
        write_to_postgres(dm_sugar, dm_names["sugar_by_category"], config, logger)

        dm_nutriscore = build_dm_nutriscore_by_country(df_silver, logger)
        write_to_postgres(dm_nutriscore, dm_names["nutriscore_by_country"], config, logger)

        dm_additives = build_dm_additives_analysis(df_silver, logger)
        write_to_postgres(dm_additives, dm_names["additives_analysis"], config, logger)

        dm_ml = build_dm_ml_nutriscore_prediction(df_silver, logger)
        write_to_postgres(dm_ml, dm_names["ml_nutriscore_prediction"], config, logger)

        logger.info("=== 4 datamarts écrits avec succès dans PostgreSQL ===")

    except Exception as exc:
        logger.error("Erreur fatale dans datamart.py : %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession arrêtée.")
