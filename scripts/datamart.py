"""
datamart.py — Production des 4 datamarts dans PostgreSQL

Rôle  : Lire les tables Hive silver, calculer les agrégats finaux
         et écrire chaque datamart dans PostgreSQL via JDBC.
         Crée ensuite les index PostgreSQL via le gateway JVM Spark.

Datamarts produits :
    1. dm_sugar_by_category         — top 10 produits les plus sucrés par catégorie
    2. dm_nutriscore_by_country      — répartition Nutri-Score par pays et région
    3. dm_additives_analysis         — additifs les plus fréquents (explode + rang)
    4. dm_ml_nutriscore_prediction   — dataset ML-ready (features numériques + target)

Exécution via spark-submit :
    spark-submit \\
        --master spark://spark-master:7077 \\
        scripts/datamart.py [--config config] [--datamarts all|1|2|3|4]
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel


# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging(log_dir: str, script_name: str) -> logging.Logger:
    """Configure le logger fichier .txt horodaté + stdout."""
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
    """Charge app_config, spark_config et database_config depuis config_dir."""
    config = {}
    for name in ("app_config", "spark_config", "database_config"):
        path = os.path.join(config_dir, f"{name}.yaml")
        with open(path, "r", encoding="utf-8") as fh:
            config[name.replace("_config", "")] = yaml.safe_load(fh)
    return config


# ─── SparkSession ─────────────────────────────────────────────────────────────

def create_spark_session(config: dict, logger: logging.Logger) -> SparkSession:
    """
    Crée la SparkSession avec Hive embedded (Derby) et S3A.
    Le driver PostgreSQL est dans /opt/spark/jars/ (téléchargé par docker-compose).
    """
    spark_cfg = config["spark"]["spark"]
    s3a_cfg   = config["spark"]["s3a"]
    hive_cfg  = config["spark"]["hive"]
    packages  = ",".join(config["spark"].get("packages", []))

    spark = (
        SparkSession.builder
        .appName(f"{spark_cfg['app_name']}_datamart")
        .master(spark_cfg["master"])
        .config("spark.executor.memory",          spark_cfg["executor_memory"])
        .config("spark.driver.memory",            spark_cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions",   spark_cfg["sql_shuffle_partitions"])
        # Hive embedded (Derby local)
        .config("spark.sql.warehouse.dir",               hive_cfg["warehouse_dir"])
        .config("javax.jdo.option.ConnectionURL",         f"jdbc:derby:;databaseName={hive_cfg['derby_db_path']};create=true")
        .config("javax.jdo.option.ConnectionDriverName",  "org.apache.derby.jdbc.EmbeddedDriver")
        .config("datanucleus.schema.autoCreateAll",       "true")
        .config("datanucleus.autoCreateSchema",           "true")
        # S3A — nécessaire pour lire les Parquet silver stockés dans MinIO
        .config("spark.hadoop.fs.s3a.endpoint",               s3a_cfg["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key",             s3a_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key",             s3a_cfg["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access",      str(s3a_cfg["path_style_access"]).lower())
        .config("spark.hadoop.fs.s3a.impl",                   s3a_cfg["impl"])
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(s3a_cfg["connection_ssl_enabled"]).lower())
        .config("spark.jars.packages", packages)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession créée — master : %s", spark.sparkContext.master)
    return spark


# ─── Construction de l'URL JDBC ───────────────────────────────────────────────

def _jdbc_url(config: dict) -> str:
    """Construit l'URL JDBC PostgreSQL depuis database_config.yaml."""
    db = config["database"]["postgresql"]
    return f"jdbc:postgresql://{db['host']}:{db['port']}/{db['database']}"


def _jdbc_properties(config: dict) -> dict:
    """Retourne les propriétés JDBC (driver, user, password, batchsize)."""
    db  = config["database"]["postgresql"]
    jdb = config["database"]["jdbc"]
    return {
        "driver":    jdb["driver"],
        "user":      db["user"],
        "password":  db["password"],
        "batchsize": str(jdb.get("batch_size", 10000)),
    }


# ─── Lecture des tables Hive silver ───────────────────────────────────────────

def read_silver_tables(
    spark: SparkSession,
    config: dict,
    logger: logging.Logger,
) -> tuple:
    """
    Lit les 3 tables Hive silver et les cache en mémoire.

    products_enriched est la table centrale — elle contient le résultat de la
    jointure produits × pays + le rang sucre calculé en Phase 3. Elle est lue
    une fois et partagée entre les 4 builders de datamarts.

    Retourne : (df_enriched, df_products_clean, df_countries_clean)
    """
    silver_db  = config["app"]["hive"].get("silver_database", "silver")
    tables_cfg = config["app"]["hive"]["tables"]

    tables = {
        "enriched":  f"{silver_db}.{tables_cfg['silver_joined']}",
        "products":  f"{silver_db}.{tables_cfg['silver_products']}",
        "countries": f"{silver_db}.{tables_cfg['silver_countries']}",
    }

    results = {}
    for key, fqn in tables.items():
        logger.info("[READ] Lecture Hive : %s", fqn)
        df = spark.table(fqn)
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        count = df.count()
        logger.info("[READ] ✓ %s | %d lignes | %d colonnes", fqn, count, len(df.columns))
        results[key] = df

    return results["enriched"], results["products"], results["countries"]


# ─── Écriture PostgreSQL ──────────────────────────────────────────────────────

def write_to_postgres(
    df: DataFrame,
    table_name: str,
    config: dict,
    logger: logging.Logger,
) -> int:
    """
    Écrit un DataFrame dans PostgreSQL via JDBC et retourne le nombre de lignes.

    Mode overwrite : DROP + CREATE + INSERT à chaque run.
    Le mode overwrite JDBC recrée la table entière — schéma inclus. C'est voulu :
    cela garantit la cohérence colonnes/types entre le DataFrame Spark et la table PG.
    Les indexes sont recrées après l'écriture (CREATE INDEX IF NOT EXISTS est idempotent).
    """
    t0         = time.time()
    jdbc_url   = _jdbc_url(config)
    properties = _jdbc_properties(config)

    row_count = df.count()
    logger.info("[PG] Écriture → %s | %d lignes", table_name, row_count)

    (
        df.write
        .mode(config["database"]["jdbc"].get("write_mode", "overwrite"))
        .jdbc(url=jdbc_url, table=table_name, properties=properties)
    )

    duration = time.time() - t0
    logger.info("[PG] ✓ %s | %d lignes | %.1f s", table_name, row_count, duration)
    return row_count


# ─── Création des indexes PostgreSQL ─────────────────────────────────────────

def create_indexes(spark: SparkSession, config: dict, logger: logging.Logger) -> None:
    """
    Crée les indexes PostgreSQL via le gateway JVM Spark.

    On réutilise le driver PostgreSQL déjà chargé dans la JVM Spark pour éviter
    d'avoir besoin de psycopg2 (non installé dans l'image apache/spark:3.5.3).
    DriverManager.getConnection() ouvre une connexion JDBC native depuis Python.
    CREATE INDEX IF NOT EXISTS est idempotent — safe à rejouer.
    """
    jdbc_url = _jdbc_url(config)
    db       = config["database"]["postgresql"]

    indexes = [
        ("idx_sugar_category", "dm_sugar_by_category",          "category"),
        ("idx_sugar_rank",     "dm_sugar_by_category",          "sugar_rank_in_category"),
        ("idx_sugar_grade",    "dm_sugar_by_category",          "nutrition_grade_fr"),
        ("idx_ns_country",     "dm_nutriscore_by_country",      "country"),
        ("idx_ns_region",      "dm_nutriscore_by_country",      "region"),
        ("idx_ns_grade",       "dm_nutriscore_by_country",      "nutrition_grade_fr"),
        ("idx_add_rank",       "dm_additives_analysis",         "rank_overall"),
        ("idx_add_tag",        "dm_additives_analysis",         "additive_tag"),
        ("idx_ml_grade",       "dm_ml_nutriscore_prediction",   "nutrition_grade_fr"),
        ("idx_ml_category",    "dm_ml_nutriscore_prediction",   "main_category"),
    ]

    try:
        # Chargement explicite du driver avant getConnection()
        spark._jvm.java.lang.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(
            jdbc_url, db["user"], db["password"]
        )
        stmt = conn.createStatement()
        for idx_name, table, column in indexes:
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({column})"
            stmt.execute(sql)
            logger.info("[INDEX] ✓ %s ON %s(%s)", idx_name, table, column)
        stmt.close()
        conn.close()
        logger.info("[INDEX] %d indexes créés/vérifiés", len(indexes))
    except Exception as exc:
        # Les indexes sont une optimisation — leur échec ne doit pas bloquer le pipeline
        logger.error("[INDEX] Échec (non bloquant) : %s", exc, exc_info=True)


# ─── Datamart 1 : dm_sugar_by_category ───────────────────────────────────────

def build_dm_sugar_by_category(
    df_enriched: DataFrame,
    config: dict,
    logger: logging.Logger,
) -> DataFrame:
    """
    Top N produits les plus sucrés par catégorie.

    products_enriched contient une ligne par (produit × pays) à cause de l'explode
    fait en Phase 3. On déduplique d'abord sur (code, main_category) pour obtenir
    un produit unique par catégorie, puis on recompute le rang proprement.
    Utiliser le sugar_rank_in_category de la Phase 3 directement produirait des
    doublons (un produit vendu dans 3 pays aurait 3 rangs différents).
    """
    top_n = config["app"].get("processor", {}).get("top_n_sugary_by_category", 10)

    logger.info("[DM1] Construction dm_sugar_by_category | top_n=%d", top_n)

    # Déduplication (produit × catégorie) avant le classement
    df_unique = (
        df_enriched
        .filter(F.col("main_category").isNotNull() & (F.col("main_category") != ""))
        .select("code", "main_category", "product_name", "sugars_100g", "nutrition_grade_fr")
        .dropDuplicates(["code", "main_category"])
    )

    # ROW_NUMBER() OVER (PARTITION BY main_category ORDER BY sugars_100g DESC)
    window_spec = Window.partitionBy("main_category").orderBy(F.desc("sugars_100g"))
    dm = (
        df_unique
        .withColumn("sugar_rank_in_category", F.row_number().over(window_spec))
        .filter(F.col("sugar_rank_in_category") <= top_n)
        .select(
            F.col("main_category").alias("category"),
            "product_name",
            "sugars_100g",
            "nutrition_grade_fr",
            "sugar_rank_in_category",
        )
        .orderBy("category", "sugar_rank_in_category")
    )

    count = dm.count()
    logger.info("[DM1] ✓ %d lignes | colonnes : category, product_name, sugars_100g, nutrition_grade_fr, sugar_rank_in_category", count)
    return dm


# ─── Datamart 2 : dm_nutriscore_by_country ───────────────────────────────────

def build_dm_nutriscore_by_country(
    df_enriched: DataFrame,
    logger: logging.Logger,
) -> DataFrame:
    """
    Répartition du Nutri-Score par pays et région.

    GroupBy (country, region, nutrition_grade_fr) → nb_products + avg_nutriscore_score.
    Une ligne = nombre de produits d'un grade donné dans un pays donné.
    region vient de countries_clean, jointée dans products_enriched en Phase 3.
    nutriscore_score est le score numérique (colonne renommée depuis nutrition-score-fr_100g).
    """
    logger.info("[DM2] Construction dm_nutriscore_by_country")

    dm = (
        df_enriched
        .filter(
            F.col("country").isNotNull()
            & F.col("nutrition_grade_fr").isNotNull()
        )
        .groupBy("country", "region", "nutrition_grade_fr")
        .agg(
            F.count("code").alias("nb_products"),
            F.avg("nutriscore_score").alias("avg_nutriscore_score"),
        )
        .orderBy("country", "nutrition_grade_fr")
    )

    count = dm.count()
    logger.info("[DM2] ✓ %d lignes | colonnes : country, region, nutrition_grade_fr, nb_products, avg_nutriscore_score", count)
    return dm


# ─── Datamart 3 : dm_additives_analysis ──────────────────────────────────────

def build_dm_additives_analysis(
    df_enriched: DataFrame,
    logger: logging.Logger,
) -> DataFrame:
    """
    Additifs les plus fréquents avec rang global.

    Explode de additives_tags (séparateur virgule) → une ligne par additif.
    GroupBy additif → total_occurrences + nb_distinct_categories.
    ROW_NUMBER() OVER (ORDER BY total_occurrences DESC) → rang global.
    pct_of_total = occurrences / total lignes du dataset × 100.
    """
    logger.info("[DM3] Construction dm_additives_analysis")

    # Total de lignes pour le calcul du pourcentage
    total_rows = df_enriched.count()

    df_exploded = (
        df_enriched
        .filter(F.col("additives_tags").isNotNull() & (F.col("additives_tags") != ""))
        .withColumn("additive_tag", F.explode(F.split(F.trim(F.col("additives_tags")), ",")))
        .withColumn("additive_tag", F.trim(F.col("additive_tag")))
        .filter(F.col("additive_tag") != "")
    )

    df_agg = (
        df_exploded
        .groupBy("additive_tag")
        .agg(
            F.count("*").alias("total_occurrences"),
            F.countDistinct("main_category").alias("nb_distinct_categories"),
        )
    )

    # Rang global par fréquence décroissante
    window_spec = Window.orderBy(F.desc("total_occurrences"))
    dm = (
        df_agg
        .withColumn("rank_overall", F.row_number().over(window_spec))
        .withColumn(
            "pct_of_total",
            F.round(F.col("total_occurrences") / F.lit(total_rows) * 100, 4),
        )
        .select("additive_tag", "total_occurrences", "nb_distinct_categories", "rank_overall", "pct_of_total")
        .orderBy("rank_overall")
    )

    count = dm.count()
    logger.info("[DM3] ✓ %d additifs distincts", count)

    # Aperçu des 5 additifs les plus fréquents dans les logs
    for row in dm.limit(5).collect():
        logger.info(
            "[DM3]   [#%d] %-35s → %d occurrences (%.2f%%)",
            row["rank_overall"], row["additive_tag"], row["total_occurrences"], row["pct_of_total"],
        )

    return dm


# ─── Datamart 4 : dm_ml_nutriscore_prediction ────────────────────────────────

def build_dm_ml_nutriscore_prediction(
    df_enriched: DataFrame,
    logger: logging.Logger,
) -> DataFrame:
    """
    Dataset ML-ready : une ligne par produit unique avec features numériques + target.

    Déduplication sur code (products_enriched a N lignes par produit en raison
    de l'explode par pays en Phase 3). On garde la première occurrence.
    La colonne saturated-fat_100g (tiret dans le nom) est aliasée en
    saturated_fat_100g pour respecter les conventions de nommage PostgreSQL.
    nutrition_grade_fr est la variable cible (A/B/C/D/E).
    Les features sont laissées telles quelles (encodage catégoriel délégué au ML).
    """
    logger.info("[DM4] Construction dm_ml_nutriscore_prediction")

    dm = (
        df_enriched
        .filter(F.col("nutrition_grade_fr").isNotNull())
        .select(
            "code",
            "sugars_100g",
            "fat_100g",
            # colonne à tiret — aliasée pour éviter les problèmes de nommage en PG
            F.col("saturated-fat_100g").alias("saturated_fat_100g"),
            "salt_100g",
            "energy_100g",
            "proteins_100g",
            "fiber_100g",
            F.col("additives_n").cast("integer").alias("additives_n"),
            "main_category",
            "country_normalized",
            "population",
            "area_sq_mi",
            "nutrition_grade_fr",
        )
        .dropDuplicates(["code"])  # une ligne par produit
    )

    count = dm.count()
    logger.info("[DM4] ✓ %d produits ML-ready | target : nutrition_grade_fr", count)

    # Distribution des grades dans les logs (validation rapide)
    grade_dist = (
        dm.groupBy("nutrition_grade_fr")
        .count()
        .orderBy("nutrition_grade_fr")
        .collect()
    )
    for row in grade_dist:
        logger.info("[DM4]   Grade %-2s → %d produits (%.1f%%)",
                    row["nutrition_grade_fr"], row["count"],
                    row["count"] / count * 100 if count > 0 else 0)

    return dm


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="datamart.py — Production des datamarts PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  # Tous les datamarts
  spark-submit scripts/datamart.py

  # Un seul datamart (dev / debug)
  spark-submit scripts/datamart.py --datamarts 1

  # Config alternative
  spark-submit scripts/datamart.py --config /opt/config
        """,
    )
    parser.add_argument(
        "--config",
        default="config",
        help="Répertoire des fichiers de configuration YAML (défaut : config/)",
    )
    parser.add_argument(
        "--datamarts",
        choices=["all", "1", "2", "3", "4"],
        default="all",
        help="Datamart(s) à construire : all (défaut), 1=sugar, 2=nutriscore, 3=additives, 4=ml",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config["app"]["logging"]["log_dir"], "datamart")

    logger.info("=== Démarrage datamart.py | datamarts=%s ===", args.datamarts)
    t_global = time.time()
    spark    = None
    metrics  = {}  # {nom_table: nb_lignes}
    errors   = []  # noms des datamarts en échec

    try:
        spark = create_spark_session(config, logger)

        # ── Lecture des tables silver ──────────────────────────────────────────
        logger.info("─── Étape : Lecture tables Hive silver ───")
        df_enriched, df_products, df_countries = read_silver_tables(spark, config, logger)

        dm_cfg = config["database"]["datamarts"]

        # ── Datamart 1 : sugar by category ────────────────────────────────────
        if args.datamarts in ("all", "1"):
            logger.info("─── Étape : DM1 — dm_sugar_by_category ───")
            try:
                dm1 = build_dm_sugar_by_category(df_enriched, config, logger)
                metrics[dm_cfg["sugar_by_category"]] = write_to_postgres(
                    dm1, dm_cfg["sugar_by_category"], config, logger
                )
            except Exception as exc:
                logger.error("[DM1] Échec : %s", exc, exc_info=True)
                errors.append("dm_sugar_by_category")

        # ── Datamart 2 : nutriscore by country ────────────────────────────────
        if args.datamarts in ("all", "2"):
            logger.info("─── Étape : DM2 — dm_nutriscore_by_country ───")
            try:
                dm2 = build_dm_nutriscore_by_country(df_enriched, logger)
                metrics[dm_cfg["nutriscore_by_country"]] = write_to_postgres(
                    dm2, dm_cfg["nutriscore_by_country"], config, logger
                )
            except Exception as exc:
                logger.error("[DM2] Échec : %s", exc, exc_info=True)
                errors.append("dm_nutriscore_by_country")

        # ── Datamart 3 : additives analysis ───────────────────────────────────
        if args.datamarts in ("all", "3"):
            logger.info("─── Étape : DM3 — dm_additives_analysis ───")
            try:
                dm3 = build_dm_additives_analysis(df_enriched, logger)
                metrics[dm_cfg["additives_analysis"]] = write_to_postgres(
                    dm3, dm_cfg["additives_analysis"], config, logger
                )
            except Exception as exc:
                logger.error("[DM3] Échec : %s", exc, exc_info=True)
                errors.append("dm_additives_analysis")

        # ── Datamart 4 : ML dataset ────────────────────────────────────────────
        if args.datamarts in ("all", "4"):
            logger.info("─── Étape : DM4 — dm_ml_nutriscore_prediction ───")
            try:
                dm4 = build_dm_ml_nutriscore_prediction(df_enriched, logger)
                metrics[dm_cfg["ml_nutriscore_prediction"]] = write_to_postgres(
                    dm4, dm_cfg["ml_nutriscore_prediction"], config, logger
                )
            except Exception as exc:
                logger.error("[DM4] Échec : %s", exc, exc_info=True)
                errors.append("dm_ml_nutriscore_prediction")

        # ── Création des indexes ───────────────────────────────────────────────
        if args.datamarts == "all":
            logger.info("─── Étape : Création des indexes PostgreSQL ───")
            create_indexes(spark, config, logger)

        # ── Libération mémoire ─────────────────────────────────────────────────
        df_enriched.unpersist()
        df_products.unpersist()
        df_countries.unpersist()

        # ── Récap final ────────────────────────────────────────────────────────
        duration_total = time.time() - t_global
        logger.info("─── Récapitulatif datamart.py ───────────────────────────────")
        for table_name, row_count in metrics.items():
            logger.info("  %-40s → %d lignes", table_name, row_count)
        if errors:
            logger.error("  Datamarts en ÉCHEC : %s", errors)
        logger.info("  Durée totale : %.1f s", duration_total)

        if errors:
            logger.error("=== %d datamart(s) en erreur ===", len(errors))
            sys.exit(1)
        else:
            logger.info("=== %d datamart(s) écrits avec succès dans PostgreSQL ===", len(metrics))

    except Exception as exc:
        logger.error("Erreur fatale dans datamart.py : %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession arrêtée.")
