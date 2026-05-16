"""
feeder.py — Ingestion des sources open data vers la couche raw (MinIO/Parquet)

Rôle  : Lire les fichiers CSV bruts (Open Food Facts + Countries of the World)
         et les écrire en Parquet partitionné par date d'ingestion dans MinIO.

Partitionnement : s3a://raw/<source>/year=YYYY/month=MM/day=DD/

Exécution via spark-submit :
    spark-submit \\
        --master spark://spark-master:7077 \\
        scripts/feeder.py [--source all|openfoodfacts|countries] [--config config] [--sample]
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ─── Schéma des colonnes critiques d'Open Food Facts ─────────────────────────
# Le fichier CSV source contient ~200 colonnes ; on extrait seulement celles
# nécessaires pour les phases 3 et 4. Toutes les autres sont ignorées.
#
# Choix de conception : on lit le CSV entier en StringType (inferSchema=false)
# puis on caste manuellement les colonnes sélectionnées. Cette approche est plus
# rapide que inferSchema=true sur un fichier de plusieurs Go, et plus robuste
# qu'un schéma positionnel (les colonnes OFF peuvent varier selon la version).
#
# Colonnes à noms spéciaux (tirets) : nutrition-score-fr_100g, saturated-fat_100g.
# Le tiret est un opérateur arithmétique dans Spark SQL — ces colonnes nécessitent
# des backticks dans col() : col("`nutrition-score-fr_100g`").
# Dans processor.py, nutrition-score-fr_100g est renommé dès la lecture pour éviter
# de propager les backticks partout dans le code aval.
OFF_SCHEMA = StructType([
    StructField("code",                    StringType(),  True),  # Code-barres EAN
    StructField("product_name",            StringType(),  True),  # Nom du produit
    StructField("countries_en",            StringType(),  True),  # Pays de vente (liste CSV interne)
    StructField("categories_en",           StringType(),  True),  # Catégories taxonomiques
    StructField("main_category",           StringType(),  True),  # Catégorie principale
    # Nutri-Score : renommés dans le dump récent (anciens noms : nutriscore_grade / nutriscore_score)
    StructField("nutrition_grade_fr",      StringType(),  True),  # Grade Nutri-Score : a, b, c, d ou e
    StructField("nutrition-score-fr_100g", DoubleType(),  True),  # Score numérique (backtick requis dans col())
    # Énergie : le dump récent expose energy_100g (en kJ/100g), pas energy-kcal_100g
    StructField("energy_100g",             DoubleType(),  True),  # Énergie en kJ/100g
    StructField("fat_100g",                DoubleType(),  True),  # Graisses totales (g/100g)
    StructField("saturated-fat_100g",      DoubleType(),  True),  # Graisses saturées (g/100g)
    StructField("sugars_100g",             DoubleType(),  True),  # Sucres (g/100g)
    StructField("fiber_100g",              DoubleType(),  True),  # Fibres alimentaires (g/100g)
    StructField("proteins_100g",           DoubleType(),  True),  # Protéines (g/100g)
    StructField("salt_100g",               DoubleType(),  True),  # Sel (g/100g)
    StructField("additives_n",             IntegerType(), True),  # Nombre d'additifs déclarés
    StructField("additives_tags",          StringType(),  True),  # Tags additifs pour Phase 3
])


# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging(log_dir: str, script_name: str) -> logging.Logger:
    """
    Configure le logger pour écrire simultanément dans un fichier .txt et stdout.
    Le fichier est horodaté : <script_name>_YYYYMMDD_HHMMSS.txt

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
    Charge les trois fichiers YAML depuis config_dir et les fusionne.

    Args:
        config_dir: Chemin vers le répertoire contenant les fichiers .yaml.

    Returns:
        Dictionnaire imbriqué avec les clés 'app', 'spark', 'database'.

    Raises:
        FileNotFoundError: Si un fichier YAML est absent.
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

    Configuration clé : partitionOverwriteMode=dynamic
    → En mode overwrite, seule la partition correspondant aux données du DataFrame
      est écrasée. Les partitions des jours précédents sont préservées.
    → Sans ce réglage, mode=overwrite effacerait TOUT le répertoire s3a://raw/source/.

    Args:
        config: Configuration fusionnée depuis load_config().
        logger: Logger actif.

    Returns:
        SparkSession prête à l'emploi.
    """
    spark_cfg = config["spark"]["spark"]
    s3a_cfg   = config["spark"]["s3a"]
    hive_cfg  = config["spark"]["hive"]
    packages  = ",".join(config["spark"].get("packages", []))

    spark = (
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        .master(spark_cfg["master"])
        .config("spark.executor.memory", spark_cfg["executor_memory"])
        .config("spark.driver.memory",   spark_cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions", spark_cfg["sql_shuffle_partitions"])
        # Hive embedded (Derby local) — pas de service hive-metastore externe
        .config("spark.sql.warehouse.dir",               hive_cfg["warehouse_dir"])
        .config("javax.jdo.option.ConnectionURL",         f"jdbc:derby:;databaseName={hive_cfg['derby_db_path']};create=true")
        .config("javax.jdo.option.ConnectionDriverName",  "org.apache.derby.jdbc.EmbeddedDriver")
        .config("datanucleus.schema.autoCreateAll",       "true")
        .config("datanucleus.autoCreateSchema",           "true")
        # MinIO via le protocole S3A (compatible AWS S3)
        .config("spark.hadoop.fs.s3a.endpoint",               s3a_cfg["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key",             s3a_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key",             s3a_cfg["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access",      str(s3a_cfg["path_style_access"]).lower())
        .config("spark.hadoop.fs.s3a.impl",                   s3a_cfg["impl"])
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(s3a_cfg["connection_ssl_enabled"]).lower())
        # Packages Maven (AWS connector + PostgreSQL JDBC)
        .config("spark.jars.packages", packages)
        # Overwrite partitionnel : préserve les partitions hors de la fenêtre courante
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession créée — master : %s", spark.sparkContext.master)
    return spark


# ─── Helpers privés ───────────────────────────────────────────────────────────

def _build_partition_path(base_path: str, source_name: str, now: datetime) -> str:
    """
    Retourne le chemin S3A complet de la partition du jour.
    Format : <base_path>/<source>/year=YYYY/month=MM/day=DD

    Le zéro-padding sur le mois et le jour garantit un tri lexicographique correct
    dans les explorateurs de fichiers (MinIO console, spark.read.parquet).
    """
    return (
        f"{base_path}/{source_name}"
        f"/year={now.year}"
        f"/month={now.month:02d}"
        f"/day={now.day:02d}"
    )


def _add_partition_columns(df: DataFrame, now: datetime) -> DataFrame:
    """
    Ajoute les colonnes year, month, day en type entier au DataFrame.

    On utilise des entiers plutôt que des strings pour un tri numérique correct
    (sans zéro-padding) dans les requêtes Hive/Spark SQL du type
    WHERE year = 2024 AND month = 5 AND day = 14.
    """
    return (
        df
        .withColumn("year",  lit(now.year))
        .withColumn("month", lit(now.month))
        .withColumn("day",   lit(now.day))
    )


def _apply_off_schema(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Sélectionne et caste les colonnes critiques depuis le DataFrame brut
    (toutes les colonnes sont en StringType après lecture CSV avec inferSchema=false).

    Stratégie par colonne :
    - Présente dans le fichier → col("`nom`").cast(type_cible)
    - Absente dans le fichier  → lit(None).cast(type_cible)  (null typé, pas d'erreur)

    Les backticks sont obligatoires pour les noms contenant un tiret
    (energy-kcal_100g, saturated-fat_100g) : sans eux, PySpark interprète
    le tiret comme une soustraction et lève une AnalysisException.

    Args:
        df:     DataFrame brut (toutes colonnes en StringType).
        logger: Logger actif.

    Returns:
        DataFrame avec uniquement les colonnes de OFF_SCHEMA, castées.
    """
    available = set(df.columns)
    selections = []

    for field in OFF_SCHEMA.fields:
        if field.name in available:
            selections.append(
                col(f"`{field.name}`").cast(field.dataType).alias(field.name)
            )
        else:
            # Certaines colonnes OFF varient selon la version du dump mensuel
            logger.warning(
                "[OFF] Colonne absente dans le fichier source : '%s' → remplacée par null",
                field.name,
            )
            selections.append(lit(None).cast(field.dataType).alias(field.name))

    return df.select(selections)


# ─── Ingestion Open Food Facts ────────────────────────────────────────────────

def ingest_openfoodfacts(
    spark: SparkSession,
    config: dict,
    logger: logging.Logger,
    sample: bool = False,
) -> dict:
    """
    Lit le fichier CSV Open Food Facts (TSV ~3 Go, séparateur tabulation) et
    l'écrit en Parquet partitionné dans la couche raw de MinIO.

    Métriques retournées (dict) :
        rows_read        — nombre de lignes lues dans le CSV source
        rows_written     — nombre de lignes écrites en Parquet (= rows_read hors sample)
        output_path      — chemin S3A de la partition écrite
        duration_seconds — durée totale de la fonction en secondes

    Args:
        spark:  SparkSession active.
        config: Configuration fusionnée depuis les YAML.
        logger: Logger actif.
        sample: Si True, limite à 10 000 lignes (usage en développement sans les données complètes).
    """
    t_start     = time.time()
    src_cfg     = config["app"]["sources"]["openfoodfacts"]
    storage_cfg = config["app"]["storage"]

    input_path     = os.path.join(src_cfg["path"], src_cfg["filename"])
    now            = datetime.now()
    base_output    = f"{storage_cfg['raw_base_path']}/openfoodfacts"
    partition_path = _build_partition_path(storage_cfg["raw_base_path"], "openfoodfacts", now)

    logger.info("[OFF] ─── Démarrage ingestion Open Food Facts ───")
    logger.info("[OFF] Source      : %s", input_path)
    logger.info("[OFF] Destination : %s", partition_path)

    # ── Lecture CSV ──────────────────────────────────────────────────────────
    # inferSchema=false : on ne laisse pas Spark scanner une deuxième fois le fichier
    # pour deviner les types — sur ~3 Go, cela doublerait le temps de lecture.
    # mode=PERMISSIVE : les lignes corrompues (champs malformés, encoding cassé)
    # produisent des nulls au lieu de lever une exception, ce qui est préférable
    # sur un dataset communautaire où la qualité des saisies est hétérogène.
    # multiLine=false : les champs OFF ne contiennent pas de sauts de ligne
    # (noms de produits sur une seule ligne), ce réglage améliore les performances.
    df_raw = (
        spark.read
        .option("header",      "true")
        .option("sep",         src_cfg["delimiter"])    # Tabulation \t
        .option("encoding",    src_cfg["encoding"])
        .option("inferSchema", "false")
        .option("multiLine",   "false")
        .option("quote",       '"')
        .option("escape",      '"')
        .option("mode",        "PERMISSIVE")
        .csv(input_path)
    )

    # count() déclenche une lecture complète du fichier (action Spark).
    # Coûteux sur ~3 Go, mais requis pour loguer le nombre de lignes lues.
    # Visible dans la Spark UI sous "Jobs" > "count at feeder.py".
    rows_read = df_raw.count()
    logger.info("[OFF] Lignes brutes lues depuis le CSV : %d", rows_read)

    if sample:
        df_raw = df_raw.limit(10_000)
        rows_written = min(10_000, rows_read)
        logger.info("[OFF] Mode --sample actif : traitement limité à 10 000 lignes")
    else:
        rows_written = rows_read  # Pas de filtrage dans feeder.py, les lignes sont conservées

    # ── Sélection + cast des colonnes critiques via OFF_SCHEMA ───────────────
    df_typed = _apply_off_schema(df_raw, logger)
    logger.info("[OFF] %d colonnes sélectionnées et castées depuis le schéma OFF", len(OFF_SCHEMA.fields))

    # ── Ajout des colonnes de partition (year, month, day) ───────────────────
    df_final = _add_partition_columns(df_typed, now)

    # ── Écriture Parquet partitionné dans MinIO ───────────────────────────────
    # partitionBy("year", "month", "day") → Spark crée les sous-répertoires automatiquement.
    # mode("overwrite") + partitionOverwriteMode=dynamic (configuré dans SparkSession) →
    # seule la partition du jour est écrasée en cas de re-run.
    df_final.write.mode("overwrite").partitionBy("year", "month", "day").parquet(base_output)

    duration = time.time() - t_start
    logger.info(
        "[OFF] ✓ Écriture terminée | lignes : %d | sortie : %s | durée : %.1f s",
        rows_written, partition_path, duration,
    )

    return {
        "rows_read":        rows_read,
        "rows_written":     rows_written,
        "output_path":      partition_path,
        "duration_seconds": round(duration, 1),
    }


# ─── Ingestion Countries of the World ─────────────────────────────────────────

def ingest_countries(
    spark: SparkSession,
    config: dict,
    logger: logging.Logger,
    sample: bool = False,
) -> dict:
    """
    Lit le fichier CSV Countries of the World et l'écrit en Parquet partitionné
    dans la couche raw de MinIO.

    Contrairement à Open Food Facts, inferSchema=true est utilisé ici :
    le fichier contient ~230 lignes, le coût du double-scan est donc négligeable.
    Les noms de colonnes (avec espaces et parenthèses) sont conservés tels quels
    et seront nettoyés dans processor.py (Phase 3).

    Métriques retournées : rows_read, rows_written, output_path, duration_seconds.

    Args:
        spark:  SparkSession active.
        config: Configuration fusionnée depuis les YAML.
        logger: Logger actif.
        sample: Flag ignoré pour cette source (le fichier est déjà petit).
    """
    t_start     = time.time()
    src_cfg     = config["app"]["sources"]["countries"]
    storage_cfg = config["app"]["storage"]

    input_path     = os.path.join(src_cfg["path"], src_cfg["filename"])
    now            = datetime.now()
    base_output    = f"{storage_cfg['raw_base_path']}/countries"
    partition_path = _build_partition_path(storage_cfg["raw_base_path"], "countries", now)

    logger.info("[CTR] ─── Démarrage ingestion Countries of the World ───")
    logger.info("[CTR] Source      : %s", input_path)
    logger.info("[CTR] Destination : %s", partition_path)

    if sample:
        logger.info("[CTR] Mode --sample ignoré : fichier Countries déjà petit (~230 lignes)")

    # inferSchema=true acceptable sur ~230 lignes (scan quasi-instantané).
    # Les noms de colonnes contiennent des espaces et parenthèses ; ils seront
    # renommés en snake_case dans processor.py.
    df_raw = (
        spark.read
        .option("header",      "true")
        .option("sep",         src_cfg["delimiter"])
        .option("encoding",    src_cfg["encoding"])
        .option("inferSchema", "true")
        .option("mode",        "PERMISSIVE")
        .csv(input_path)
    )

    rows_read = df_raw.count()
    logger.info("[CTR] Lignes brutes lues depuis le CSV : %d", rows_read)

    df_final = _add_partition_columns(df_raw, now)

    df_final.write.mode("overwrite").partitionBy("year", "month", "day").parquet(base_output)

    duration = time.time() - t_start
    logger.info(
        "[CTR] ✓ Écriture terminée | lignes : %d | sortie : %s | durée : %.1f s",
        rows_read, partition_path, duration,
    )

    return {
        "rows_read":        rows_read,
        "rows_written":     rows_read,
        "output_path":      partition_path,
        "duration_seconds": round(duration, 1),
    }


# ─── Validation post-ingestion ────────────────────────────────────────────────

def validate_ingestion(
    spark: SparkSession,
    config: dict,
    source: str,
    logger: logging.Logger,
) -> bool:
    """
    Valide l'ingestion en relisant les données du jour depuis MinIO et en comparant
    les comptages aux seuils définis dans app_config.yaml.

    Stratégie de lecture — chemin parent + filtre sur colonnes de partition :
    On lit depuis le répertoire parent (ex : s3a://raw/openfoodfacts/) et on filtre
    sur year/month/day plutôt que de construire le chemin de partition exact.
    Cette approche corrige deux bugs simultanément :
      (1) Inconsistance S3A : MinIO peut ne pas lister immédiatement un répertoire
          venant d'être créé. Lire le parent (déjà connu) évite ce délai.
      (2) Mismatch de zero-padding : _build_partition_path génère month=05 mais
          Spark écrit les entiers sans padding (month=5). La lecture par chemin exact
          levait donc systématiquement PATH_NOT_FOUND.

    Args:
        spark:  SparkSession active.
        config: Configuration chargée.
        source: "openfoodfacts", "countries" ou "all".
        logger: Logger actif.

    Returns:
        True si tous les comptages dépassent les seuils configurés, False sinon.
    """
    storage_cfg    = config["app"]["storage"]
    validation_cfg = config["app"]["validation"]
    now            = datetime.now()
    all_ok         = True

    checks = []
    if source in ("openfoodfacts", "all"):
        checks.append({
            "name":      "openfoodfacts",
            "base_path": f"{storage_cfg['raw_base_path']}/openfoodfacts",
            "min_rows":  validation_cfg["min_rows_openfoodfacts"],
        })
    if source in ("countries", "all"):
        checks.append({
            "name":      "countries",
            "base_path": f"{storage_cfg['raw_base_path']}/countries",
            "min_rows":  validation_cfg["min_rows_countries"],
        })

    for check in checks:
        logger.info(
            "[VALID] Vérification %s | year=%d month=%d day=%d depuis %s",
            check["name"], now.year, now.month, now.day, check["base_path"],
        )
        try:
            # Lecture depuis le chemin parent : Spark découvre toutes les partitions
            # disponibles, puis le filtre pousse le prédicat sur les colonnes de partition
            # (partition pruning) pour ne lire que les fichiers du jour.
            count = (
                spark.read.parquet(check["base_path"])
                .filter(
                    (col("year")  == now.year)
                    & (col("month") == now.month)
                    & (col("day")   == now.day)
                )
                .count()
            )
            if count >= check["min_rows"]:
                logger.info(
                    "[VALID] ✓ %-20s | %d lignes ≥ seuil %d",
                    check["name"], count, check["min_rows"],
                )
            else:
                logger.error(
                    "[VALID] ✗ %-20s | %d lignes < seuil minimum %d — ingestion incomplète ?",
                    check["name"], count, check["min_rows"],
                )
                all_ok = False
        except Exception as exc:
            logger.error(
                "[VALID] ✗ Impossible de lire '%s' : %s",
                check["base_path"], exc, exc_info=True,
            )
            all_ok = False

    return all_ok


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="feeder.py — Ingestion des sources open data vers la couche raw (MinIO/Parquet)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  # Ingérer les deux sources (par défaut)
  spark-submit scripts/feeder.py

  # Ingérer uniquement Open Food Facts
  spark-submit scripts/feeder.py --source openfoodfacts

  # Mode développement : 10 000 lignes pour Open Food Facts
  spark-submit scripts/feeder.py --sample

  # Spécifier un répertoire de config alternatif
  spark-submit scripts/feeder.py --config /opt/config
        """,
    )
    parser.add_argument(
        "--source",
        choices=["openfoodfacts", "countries", "all"],
        default="all",
        help="Source à ingérer (défaut : all)",
    )
    parser.add_argument(
        "--config",
        default="config",
        help="Répertoire des fichiers de configuration YAML (défaut : config/)",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Mode développement : limite Open Food Facts à 10 000 lignes",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config["app"]["logging"]["log_dir"], "feeder")

    logger.info("=== Démarrage feeder.py | source=%s | sample=%s ===", args.source, args.sample)

    spark   = None
    results = {}   # Métriques par source
    errors  = {}   # Erreurs par source (pour rapport final)

    try:
        spark = create_spark_session(config, logger)

        # Chaque source est encapsulée dans son propre try/except :
        # un échec sur Open Food Facts n'empêche pas l'ingestion des Countries, et inversement.
        if args.source in ("openfoodfacts", "all"):
            try:
                results["openfoodfacts"] = ingest_openfoodfacts(
                    spark, config, logger, sample=args.sample
                )
            except Exception as exc:
                logger.error("[OFF] Échec de l'ingestion : %s", exc, exc_info=True)
                errors["openfoodfacts"] = str(exc)

        if args.source in ("countries", "all"):
            try:
                results["countries"] = ingest_countries(
                    spark, config, logger, sample=args.sample
                )
            except Exception as exc:
                logger.error("[CTR] Échec de l'ingestion : %s", exc, exc_info=True)
                errors["countries"] = str(exc)

        # ── Rapport de synthèse ───────────────────────────────────────────────
        logger.info("─── Résumé de l'ingestion ───────────────────────────────")
        for src, m in results.items():
            logger.info(
                "  %-20s | lues : %8d | écrites : %8d | durée : %.1f s | %s",
                src, m["rows_read"], m["rows_written"], m["duration_seconds"], m["output_path"],
            )

        # ── Validation post-écriture ──────────────────────────────────────────
        if results:
            all_ok = validate_ingestion(spark, config, args.source, logger)
            if all_ok:
                logger.info("=== Ingestion et validation terminées avec succès ===")
            else:
                logger.error("=== Validation échouée — vérifier les seuils dans app_config.yaml ===")
                sys.exit(1)

        # Sortie en erreur si au moins une source a échoué
        if errors:
            logger.error(
                "=== %d source(s) en erreur : %s ===", len(errors), list(errors.keys())
            )
            sys.exit(1)

    except Exception as exc:
        logger.error("Erreur fatale dans feeder.py : %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession arrêtée.")
