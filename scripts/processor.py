"""
processor.py — Transformation raw → silver (couche Hive)

Rôle  : Lire les Parquet bruts depuis MinIO, nettoyer et enrichir les données,
         effectuer des jointures + agrégations + window functions,
         puis sauvegarder en tables Hive internes (saveAsTable).

Utilisation de cache() / persist() documentée et visible dans Spark UI.

Exécution via spark-submit :
    spark-submit \\
        --master spark://spark-master:7077 \\
        scripts/processor.py --config config
"""

import argparse
import logging
import os
import sys
from datetime import datetime

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel


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
    Crée la SparkSession avec support Hive et connecteur S3A pour MinIO.

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
        .appName(f"{spark_cfg['app_name']}_processor")
        .master(spark_cfg["master"])
        .config("spark.executor.memory", spark_cfg["executor_memory"])
        .config("spark.driver.memory", spark_cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions", spark_cfg["sql_shuffle_partitions"])
        .config("spark.sql.warehouse.dir", hive_cfg["warehouse_dir"])
        .config("spark.hadoop.hive.metastore.uris", hive_cfg["metastore_uris"])
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


# ─── Chargement des données raw ───────────────────────────────────────────────

def load_raw_openfoodfacts(spark: SparkSession, config: dict, logger: logging.Logger) -> DataFrame:
    """
    Lit la dernière partition disponible du Parquet Open Food Facts depuis MinIO.

    Étapes prévues (Phase 3) :
    1. Construction du chemin s3a://raw/openfoodfacts/ depuis la config
    2. Lecture du Parquet avec inférence de schéma
    3. Log du nombre de lignes et de colonnes lues

    Args:
        spark:  SparkSession active.
        config: Configuration chargée.
        logger: Logger actif.

    Returns:
        DataFrame brut Open Food Facts.
    """
    pass  # TODO Phase 3


def load_raw_countries(spark: SparkSession, config: dict, logger: logging.Logger) -> DataFrame:
    """
    Lit la dernière partition disponible du Parquet Countries depuis MinIO.

    Étapes prévues (Phase 3) :
    1. Construction du chemin s3a://raw/countries/ depuis la config
    2. Lecture du Parquet
    3. Log du nombre de lignes lues

    Args:
        spark:  SparkSession active.
        config: Configuration chargée.
        logger: Logger actif.

    Returns:
        DataFrame brut Countries of the World.
    """
    pass  # TODO Phase 3


# ─── Nettoyage ────────────────────────────────────────────────────────────────

def clean_openfoodfacts(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Nettoie et normalise le DataFrame Open Food Facts.

    Traitements prévus (Phase 3) :
    - Suppression des colonnes inutiles (>80% de nulls)
    - Cast des colonnes numériques (sugars_100g, energy_100g, etc.)
    - Normalisation des colonnes texte (strip, lowercase)
    - Filtrage des lignes sans Nutri-Score ni catégorie
    - Extraction des additifs depuis la colonne additives_tags
    - Persist MEMORY_AND_DISK + action count() pour matérialiser en mémoire

    Args:
        df:     DataFrame brut Open Food Facts.
        logger: Logger actif.

    Returns:
        DataFrame nettoyé et persisté.
    """
    pass  # TODO Phase 3


def clean_countries(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Nettoie et normalise le DataFrame Countries of the World.

    Traitements prévus (Phase 3) :
    - Renommage des colonnes (suppression des espaces, snake_case)
    - Cast des colonnes numériques (population, gdp, etc.)
    - Normalisation du nom de pays pour la jointure
    - Suppression des doublons

    Args:
        df:     DataFrame brut Countries.
        logger: Logger actif.

    Returns:
        DataFrame nettoyé.
    """
    pass  # TODO Phase 3


# ─── Jointure ─────────────────────────────────────────────────────────────────

def join_food_countries(
    df_food: DataFrame,
    df_countries: DataFrame,
    logger: logging.Logger,
) -> DataFrame:
    """
    Effectue une jointure entre les produits alimentaires et les données pays.

    Logique prévue (Phase 3) :
    - Left join sur la colonne pays du produit (countries_en) vs nom pays normalisé
    - Ajout des colonnes démographiques (région, population, PIB)
    - Broadcast join sur df_countries (petite table)
    - Cache() du résultat + log du taux de correspondance

    Args:
        df_food:      DataFrame Open Food Facts nettoyé.
        df_countries: DataFrame Countries nettoyé.
        logger:       Logger actif.

    Returns:
        DataFrame joint produits × pays.
    """
    pass  # TODO Phase 3


# ─── Agrégations et window functions ──────────────────────────────────────────

def compute_aggregations(df_joined: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Calcule des agrégations et window functions sur le DataFrame joint.

    Calculs prévus (Phase 3) :
    - Moyenne de sugars_100g par catégorie (groupBy + agg)
    - Rang des produits par taux de sucre dans leur catégorie (Window + rank())
    - Nutri-Score moyen par pays et par région (groupBy)
    - Pourcentage d'additifs par rapport au total de la catégorie (Window + sum() over)
    - Dense rank des pays par score moyen (Window + dense_rank())

    Args:
        df_joined: DataFrame joint produits × pays.
        logger:    Logger actif.

    Returns:
        DataFrame enrichi avec les colonnes agrégées.
    """
    pass  # TODO Phase 3


# ─── Écriture silver (Hive) ───────────────────────────────────────────────────

def persist_silver_tables(spark: SparkSession, df_joined: DataFrame, config: dict, logger: logging.Logger) -> None:
    """
    Écrit les DataFrames nettoyés et enrichis en tables Hive internes (silver).

    Étapes prévues (Phase 3) :
    1. Création de la base Hive si absente (spark.sql CREATE DATABASE IF NOT EXISTS)
    2. Écriture de silver_products via df.write.mode("overwrite").saveAsTable()
    3. Écriture de silver_food_by_country via saveAsTable()
    4. Vérification avec spark.catalog.tableExists()
    5. Log du nombre de lignes de chaque table

    Args:
        spark:     SparkSession active (nécessaire pour spark.sql).
        df_joined: DataFrame joint et enrichi.
        config:    Configuration chargée.
        logger:    Logger actif.
    """
    pass  # TODO Phase 3


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="processor.py — Transformation raw → silver")
    parser.add_argument(
        "--config",
        default="config",
        help="Chemin vers le répertoire de configuration YAML (défaut : config/)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config["app"]["logging"]["log_dir"], "processor")

    logger.info("=== Démarrage processor.py ===")
    spark = None
    try:
        spark = create_spark_session(config, logger)

        df_food = load_raw_openfoodfacts(spark, config, logger)
        df_countries = load_raw_countries(spark, config, logger)

        df_food_clean = clean_openfoodfacts(df_food, logger)
        df_countries_clean = clean_countries(df_countries, logger)

        df_joined = join_food_countries(df_food_clean, df_countries_clean, logger)
        df_enriched = compute_aggregations(df_joined, logger)

        persist_silver_tables(spark, df_enriched, config, logger)

        logger.info("=== Traitement silver terminé avec succès ===")

    except Exception as exc:
        logger.error("Erreur fatale dans processor.py : %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession arrêtée.")
