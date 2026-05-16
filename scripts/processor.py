"""
processor.py — Transformation raw → silver (couche Hive + MinIO)

Rôle  : Lire les Parquet bruts depuis MinIO, appliquer 6 règles de validation,
         effectuer une jointure produits × pays (split + explode), calculer 3
         agrégations et une window function, puis sauvegarder en double écriture :
         (a) Parquet partitionné dans MinIO s3a://silver/
         (b) Tables Hive internes via saveAsTable() dans le schéma 'silver'

Exécution via spark-submit :
    spark-submit \\
        --master spark://spark-master:7077 \\
        scripts/processor.py [--config config] [--sample]
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
    """
    Configure le logger pour écrire simultanément dans un fichier .txt et stdout.
    Le fichier est horodaté : <script_name>_YYYYMMDD_HHMMSS.txt
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
    """Charge les trois fichiers YAML depuis config_dir et les fusionne."""
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

    partitionOverwriteMode=dynamic : en mode overwrite, seule la partition
    courante est écrasée. Nécessaire pour les re-runs sans écraser les
    partitions des jours précédents dans s3a://silver/.
    """
    spark_cfg = config["spark"]["spark"]
    s3a_cfg   = config["spark"]["s3a"]
    hive_cfg  = config["spark"]["hive"]
    packages  = ",".join(config["spark"].get("packages", []))

    spark = (
        SparkSession.builder
        .appName(f"{spark_cfg['app_name']}_processor")
        .master(spark_cfg["master"])
        .config("spark.executor.memory",           spark_cfg["executor_memory"])
        .config("spark.driver.memory",             spark_cfg["driver_memory"])
        .config("spark.executor.cores",            spark_cfg.get("executor_cores", 2))
        .config("spark.sql.shuffle.partitions",    spark_cfg["sql_shuffle_partitions"])
        # Hive embedded (Derby local) — pas de service hive-metastore externe
        .config("spark.sql.warehouse.dir",               hive_cfg["warehouse_dir"])
        .config("javax.jdo.option.ConnectionURL",         f"jdbc:derby:;databaseName={hive_cfg['derby_db_path']};create=true")
        .config("javax.jdo.option.ConnectionDriverName",  "org.apache.derby.jdbc.EmbeddedDriver")
        .config("datanucleus.schema.autoCreateAll",       "true")
        .config("datanucleus.autoCreateSchema",           "true")
        .config("spark.hadoop.fs.s3a.endpoint",               s3a_cfg["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key",             s3a_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key",             s3a_cfg["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access",      str(s3a_cfg["path_style_access"]).lower())
        .config("spark.hadoop.fs.s3a.impl",                   s3a_cfg["impl"])
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(s3a_cfg["connection_ssl_enabled"]).lower())
        .config("spark.jars.packages",                        packages)
        # Overwrite partitionnel : préserve les autres partitions lors d'un re-run
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Nécessaire pour que Spark écrive correctement dans les tables Hive partitionnées
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession créée — master : %s", spark.sparkContext.master)
    return spark


# ─── Chargement des données raw ───────────────────────────────────────────────

def load_raw_openfoodfacts(
    spark: SparkSession,
    config: dict,
    logger: logging.Logger,
    ingest_date: datetime,
    sample: bool = False,
) -> DataFrame:
    """
    Lit les Parquet Open Food Facts du jour d'ingestion spécifié.

    On lit depuis le chemin parent et on filtre sur year/month/day (partition pruning).
    Cela permet de rejouer le processor sur n'importe quelle date d'ingestion passée
    via --date YYYY-MM-DD, et évite de retraiter toutes les partitions historiques.
    """
    raw_path = f"{config['app']['storage']['raw_base_path']}/openfoodfacts"
    logger.info(
        "[RAW-OFF] Lecture depuis : %s | filtre year=%d month=%d day=%d",
        raw_path, ingest_date.year, ingest_date.month, ingest_date.day,
    )

    df = (
        spark.read.parquet(raw_path)
        .filter(
            (F.col("year")  == ingest_date.year)
            & (F.col("month") == ingest_date.month)
            & (F.col("day")   == ingest_date.day)
        )
    )
    rows = df.count()
    logger.info("[RAW-OFF] %d lignes lues | %d colonnes", rows, len(df.columns))

    if sample:
        df = df.limit(50_000)
        logger.info("[RAW-OFF] --sample actif : limité à 50 000 lignes")

    return df


def load_raw_countries(
    spark: SparkSession,
    config: dict,
    logger: logging.Logger,
    ingest_date: datetime,
) -> DataFrame:
    """Lit le Parquet Countries du jour d'ingestion spécifié."""
    raw_path = f"{config['app']['storage']['raw_base_path']}/countries"
    logger.info(
        "[RAW-CTR] Lecture depuis : %s | filtre year=%d month=%d day=%d",
        raw_path, ingest_date.year, ingest_date.month, ingest_date.day,
    )

    df = (
        spark.read.parquet(raw_path)
        .filter(
            (F.col("year")  == ingest_date.year)
            & (F.col("month") == ingest_date.month)
            & (F.col("day")   == ingest_date.day)
        )
    )
    rows = df.count()
    logger.info("[RAW-CTR] %d lignes lues | %d colonnes", rows, len(df.columns))
    return df


# ─── Nettoyage Open Food Facts ────────────────────────────────────────────────

def clean_openfoodfacts(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Applique 6 règles de validation et normalise le DataFrame OFF.

    La colonne 'nutrition-score-fr_100g' (tiret = caractère spécial dans Spark SQL)
    est renommée en 'nutriscore_score' dès l'entrée pour éviter l'usage systématique
    des backticks dans toutes les expressions col() en aval.

    Le persist MEMORY_AND_DISK final est déclenché via un count() (action Spark) pour
    matérialiser le DataFrame nettoyé en mémoire/disque avant les multiples passes
    de la jointure + agrégations. Sans ce persist, chaque action en aval recalculerait
    l'intégralité du pipeline raw → nettoyage depuis S3A.
    Visible dans Spark UI → onglet Storage après la première action.
    """
    # Renommage immédiat des colonnes à noms spéciaux pour simplifier les expressions
    # 'nutrition-score-fr_100g' contient des tirets → Spark SQL les interprète comme
    # des soustractions sans backticks. On renomme une fois pour toutes.
    df = df.withColumnRenamed("nutrition-score-fr_100g", "nutriscore_score")

    total_initial = df.count()
    logger.info("[CLEAN-OFF] Début nettoyage | %d lignes en entrée", total_initial)

    # ── Règle 1 : produit_name non null et non vide ───────────────────────────
    before = total_initial
    df = df.filter(
        F.col("product_name").isNotNull() & (F.trim(F.col("product_name")) != "")
    )
    after = df.count()
    logger.info("[VALID-1] product_name nul/vide → %d lignes supprimées", before - after)

    # ── Règle 2 : Nutri-Score présent + normalisation + valeurs légales ───────
    # On filtre d'abord les nulls/vides, normalise en majuscules, puis filtre les
    # grades hors de {A,B,C,D,E} (ex : "z", "unknown", données corrompues).
    before = after
    df = df.filter(
        F.col("nutrition_grade_fr").isNotNull() & (F.trim(F.col("nutrition_grade_fr")) != "")
    )
    df = df.withColumn("nutrition_grade_fr", F.upper(F.trim(F.col("nutrition_grade_fr"))))
    df = df.filter(F.col("nutrition_grade_fr").isin("A", "B", "C", "D", "E"))
    after = df.count()
    logger.info("[VALID-2] Nutri-Score absent/invalide → %d lignes supprimées", before - after)

    # ── Règle 3 : valeurs nutritionnelles physiquement cohérentes ─────────────
    # Un produit peut avoir des nulls (données non renseignées) — on tolère les nulls.
    # On rejette uniquement les valeurs impossibles physiquement :
    #   • sucre > 100 g/100 g (impossible même pour le sucre pur)
    #   • valeurs négatives sur sugars / fat / salt / energy
    before = after
    df = df.filter(
        F.col("sugars_100g").isNull() | ((F.col("sugars_100g") >= 0) & (F.col("sugars_100g") <= 100))
    ).filter(
        F.col("fat_100g").isNull() | (F.col("fat_100g") >= 0)
    ).filter(
        F.col("salt_100g").isNull() | (F.col("salt_100g") >= 0)
    ).filter(
        F.col("energy_100g").isNull() | (F.col("energy_100g") >= 0)
    )
    after = df.count()
    logger.info("[VALID-3] Valeurs nutritionnelles incohérentes → %d lignes supprimées", before - after)

    # ── Règle 4 : doublons sur code-barres (code EAN) ─────────────────────────
    # dropDuplicates conserve la première occurrence dans l'ordre du DataFrame.
    # Sur un dataset communautaire (OFF), plusieurs entrées peuvent partager le même
    # code (corrections successives). On garde une seule.
    before = after
    df = df.dropDuplicates(["code"])
    after = df.count()
    logger.info("[VALID-4] Doublons sur code EAN → %d lignes supprimées", before - after)

    # ── Règle 5 : normalisation countries_en (lowercase + trim) ──────────────
    # La colonne countries_en est multi-valuée ("France,Spain,Italy").
    # Le lowercase + trim prépare le split + explode + join de la Phase de jointure.
    df = df.withColumn("countries_en", F.lower(F.trim(F.col("countries_en"))))
    logger.info("[VALID-5] countries_en normalisé (lowercase + trim)")

    # ── Règle 6 : normalisation main_category (lowercase + trim) ─────────────
    # Harmonisation pour les groupBy de la phase d'agrégation
    df = df.withColumn("main_category", F.lower(F.trim(F.col("main_category"))))
    logger.info("[VALID-6] main_category normalisée (lowercase + trim)")

    total_final = df.count()
    rejected = total_initial - total_final
    logger.info(
        "[CLEAN-OFF] Nettoyage terminé | %d lignes retenues | %d rejetées (%.1f%%)",
        total_final, rejected, (rejected / total_initial * 100) if total_initial > 0 else 0,
    )

    # Persist MEMORY_AND_DISK : ce DataFrame est consommé plusieurs fois en aval
    # (jointure + 3 agrégations distinctes). Sans persist, Spark relit S3A + rejoue
    # les 6 règles de validation à chaque action. Le persist coûte une seule passe
    # en échange de lectures disque/réseau nulles pour les passes suivantes.
    # Spark UI → Storage tab : apparaît après le count() ci-dessous.
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    _ = df.count()  # action de matérialisation
    logger.info("[CLEAN-OFF] DataFrame persisté en MEMORY_AND_DISK")

    return df


# ─── Nettoyage Countries of the World ─────────────────────────────────────────

def clean_countries(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Renomme les colonnes en snake_case, gère le séparateur décimal virgule
    (format européen : "48,0" → 48.0), caste les types numériques et normalise
    le nom de pays pour la jointure.

    Les colonnes numériques du fichier source utilisent la virgule comme séparateur
    décimal (ex : "48,0", "163,07"). Ce format non standard pour les CSV nécessite
    un regexp_replace avant le cast en double.
    """
    # Renommage : suppression des espaces, parenthèses et caractères spéciaux
    rename_map = {
        "Country":                          "country",
        "Region":                           "region",
        "Population":                       "population",
        "Area (sq. mi.)":                   "area_sq_mi",
        "Pop. Density (per sq. mi.)":       "pop_density",
        "Coastline (coast/area ratio)":     "coastline_ratio",
        "Net migration":                    "net_migration",
        "Infant mortality (per 1000 births)": "infant_mortality",
        "GDP ($ per capita)":               "gdp_per_capita",
        "Literacy (%)":                     "literacy_pct",
        "Phones (per 1000)":               "phones_per_1000",
        "Arable (%)":                       "arable_pct",
        "Crops (%)":                        "crops_pct",
        "Other (%)":                        "other_pct",
        "Climate":                          "climate",
        "Birthrate":                        "birthrate",
        "Deathrate":                        "deathrate",
        "Agriculture":                      "agriculture",
        "Industry":                         "industry",
        "Service":                          "service",
    }
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # Colonnes décimales avec virgule comme séparateur (format source européen)
    # regexp_replace remplace ',' par '.' avant le cast en double
    float_cols = [
        "pop_density", "coastline_ratio", "net_migration", "infant_mortality",
        "literacy_pct", "phones_per_1000", "arable_pct", "crops_pct", "other_pct",
        "birthrate", "deathrate", "agriculture", "industry", "service",
    ]
    for col_name in float_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.regexp_replace(F.col(col_name).cast("string"), ",", ".").cast("double"),
            )

    # Colonnes entières (pas de virgule décimale dans la source)
    for col_name in ["population", "area_sq_mi", "gdp_per_capita"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("long"))

    # Normalisation du nom de pays pour la jointure avec OFF
    df = df.withColumn("country_normalized", F.lower(F.trim(F.col("country"))))
    df = df.dropDuplicates(["country_normalized"])

    count = df.count()
    logger.info("[CLEAN-CTR] %d pays après nettoyage et dédoublonnage", count)
    return df


# ─── Jointure produits × pays ──────────────────────────────────────────────────

def join_food_countries(
    df_food: DataFrame,
    df_countries: DataFrame,
    logger: logging.Logger,
) -> DataFrame:
    """
    Jointure produits × pays avec explode de la colonne multi-valuée countries_en.

    La colonne countries_en contient des listes de pays séparés par une virgule
    (ex : "france,spain,italy"). On la split puis on explode pour créer une ligne
    par (produit, pays). Résultat : chaque produit apparaît autant de fois qu'il
    est commercialisé dans de pays différents.

    Left join : les produits sans pays matché dans Countries conservent une ligne
    avec les colonnes pays à null — ils ne sont pas perdus.

    Broadcast join : df_countries ne contient que ~230 lignes. Le hint broadcast
    évite un shuffle de tout df_food et réduit considérablement le temps de jointure.
    Visible dans Spark UI → onglet SQL → plan d'exécution (BroadcastHashJoin).

    Persist MEMORY_AND_DISK post-jointure : ce DataFrame est la base de toutes les
    agrégations + window function + 2 écritures silver qui suivent. Sans persist,
    Spark rejouerait lecture raw + nettoyage + jointure à chaque action. Le persist
    matérialise le résultat une seule fois.
    Spark UI → Storage tab : taille du DataFrame en RAM/disque visible après count().
    """
    # Les colonnes year/month/day existent des deux côtés (ajoutées par feeder.py).
    # On les supprime du côté countries avant la jointure pour éviter les doublons
    # de colonnes qui provoqueraient une AnalysisException lors des sélections aval.
    partition_cols = [c for c in ["year", "month", "day"] if c in df_countries.columns]
    df_countries_join = df_countries.drop(*partition_cols)

    # Split de countries_en (ex: "france,spain") → array, puis explode → une ligne par pays
    # explode_outer (vs explode) : conserve les produits avec countries_en null
    # (une ligne avec country_single = null) plutôt que de les supprimer.
    df_exploded = (
        df_food
        .withColumn(
            "country_single",
            F.explode_outer(F.split(F.col("countries_en"), ",")),
        )
        .withColumn("country_single", F.trim(F.col("country_single")))
    )

    count_exploded = df_exploded.count()
    logger.info("[JOIN] Après explode countries_en : %d lignes (produits × pays)", count_exploded)

    # Left join sur le nom de pays normalisé
    df_joined = df_exploded.join(
        F.broadcast(df_countries_join),
        df_exploded["country_single"] == df_countries_join["country_normalized"],
        how="left",
    )

    total    = df_joined.count()
    matched  = df_joined.filter(F.col("country_normalized").isNotNull()).count()
    unmatched = total - matched
    logger.info(
        "[JOIN] %d lignes | %d avec pays matché (%.1f%%) | %d sans correspondance",
        total, matched,
        (matched / total * 100) if total > 0 else 0,
        unmatched,
    )

    df_joined = df_joined.persist(StorageLevel.MEMORY_AND_DISK)
    _ = df_joined.count()  # matérialisation
    logger.info("[JOIN] DataFrame joint persisté en MEMORY_AND_DISK")

    return df_joined


# ─── Agrégations et window function ───────────────────────────────────────────

def compute_aggregations(
    df_joined: DataFrame,
    config: dict,
    logger: logging.Logger,
) -> DataFrame:
    """
    3 agrégations + 1 window function obligatoire (ROW_NUMBER par catégorie).

    Les agrégations sont calculées sur df_joined déjà persisté : pas de relecture S3A.
    Elles servent à valider la qualité des données et enrichissent les logs.
    Le DataFrame enrichi (avec sugar_rank_in_category) est retourné pour la couche silver.

    Agrégation 1 — statistiques nutritionnelles par catégorie :
        avg(sugars, fat, salt, energy) GROUP BY main_category
    Agrégation 2 — produits par Nutri-Score et par pays :
        count(*) GROUP BY country × nutrition_grade_fr
    Agrégation 3 — fréquence des additifs :
        explode(additives_tags) → count(*) GROUP BY additive
    Window function :
        ROW_NUMBER() OVER (PARTITION BY main_category ORDER BY sugars_100g DESC)
        → classe chaque produit dans sa catégorie par taux de sucre décroissant
        → permet au datamart dm_sugar_by_category (Phase 4) de filtrer le top N
    """
    top_n = config["app"].get("processor", {}).get("top_n_sugary_by_category", 10)

    # ── Agrégation 1 : moyennes nutritionnelles par catégorie ─────────────────
    logger.info("[AGG-1] Calcul des moyennes nutritionnelles par catégorie...")
    df_agg_cat = (
        df_joined
        .filter(F.col("main_category").isNotNull() & (F.col("main_category") != ""))
        .groupBy("main_category")
        .agg(
            F.avg("sugars_100g").alias("avg_sugars_100g"),
            F.avg("fat_100g").alias("avg_fat_100g"),
            F.avg("salt_100g").alias("avg_salt_100g"),
            F.avg("energy_100g").alias("avg_energy_kj_100g"),
            F.count("code").alias("product_count"),
        )
        .orderBy(F.desc("product_count"))
    )
    nb_cat = df_agg_cat.count()
    top_cat = df_agg_cat.select("main_category", "product_count").limit(5).collect()
    logger.info("[AGG-1] %d catégories trouvées | Top 5 :", nb_cat)
    for row in top_cat:
        logger.info("[AGG-1]   %-50s → %d produits", row["main_category"], row["product_count"])

    # ── Agrégation 2 : nombre de produits par Nutri-Score et par pays ─────────
    logger.info("[AGG-2] Calcul du nb de produits par Nutri-Score et par pays...")
    df_agg_ns = (
        df_joined
        .filter(F.col("country_normalized").isNotNull())
        .groupBy("country", "nutrition_grade_fr")
        .agg(
            F.count("code").alias("product_count"),
            F.avg("nutriscore_score").alias("avg_nutriscore_score"),
        )
        .orderBy("country", "nutrition_grade_fr")
    )
    logger.info("[AGG-2] %d groupes (pays × Nutri-Score)", df_agg_ns.count())

    # ── Agrégation 3 : fréquence des additifs (explode de additives_tags) ─────
    logger.info("[AGG-3] Calcul de la fréquence des additifs...")
    df_additives_freq = (
        df_joined
        .filter(F.col("additives_tags").isNotNull() & (F.col("additives_tags") != ""))
        .withColumn("additive", F.explode(F.split(F.trim(F.col("additives_tags")), ",")))
        .withColumn("additive", F.trim(F.col("additive")))
        .filter(F.col("additive") != "")
        .groupBy("additive")
        .agg(
            F.count("*").alias("occurrence"),
            F.countDistinct("main_category").alias("nb_categories"),
        )
        .orderBy(F.desc("occurrence"))
    )
    nb_additives = df_additives_freq.count()
    logger.info("[AGG-3] %d additifs distincts détectés | Top 10 :", nb_additives)
    for row in df_additives_freq.limit(10).collect():
        logger.info(
            "[AGG-3]   %-30s → %6d occurrences (%d catégories)",
            row["additive"], row["occurrence"], row["nb_categories"],
        )

    # ── Window function : rang par taux de sucre dans la catégorie ────────────
    # ROW_NUMBER() OVER (PARTITION BY main_category ORDER BY sugars_100g DESC)
    # Classe chaque produit de 1 (le plus sucré) à N dans sa catégorie.
    # Produits avec sugars_100g null reçoivent un rang après tous les non-null
    # (ORDER BY DESC NULLS LAST dans Spark par défaut).
    logger.info("[WINDOW] Calcul ROW_NUMBER() par catégorie (ORDER BY sugars_100g DESC)...")
    window_spec = Window.partitionBy("main_category").orderBy(F.desc("sugars_100g"))
    df_enriched = df_joined.withColumn("sugar_rank_in_category", F.row_number().over(window_spec))

    # Vérification : top N produits sucrés par catégorie (preview dans les logs)
    df_top = df_enriched.filter(F.col("sugar_rank_in_category") <= top_n)
    nb_top = df_top.count()
    logger.info("[WINDOW] Top %d par catégorie → %d lignes", top_n, nb_top)
    preview = (
        df_top
        .filter(F.col("sugars_100g").isNotNull())
        .select("main_category", "product_name", "sugars_100g", "sugar_rank_in_category")
        .orderBy("main_category", "sugar_rank_in_category")
        .limit(10)
        .collect()
    )
    for row in preview:
        logger.info(
            "[WINDOW]   [rank %d] %-30s | %-40s | %.1f g/100g",
            row["sugar_rank_in_category"],
            str(row["main_category"])[:30],
            str(row["product_name"])[:40],
            row["sugars_100g"] or 0.0,
        )

    return df_enriched


# ─── Écriture silver — double écriture MinIO + Hive ───────────────────────────

def _write_silver_table(
    df: DataFrame,
    s3_path: str,
    hive_table_fqn: str,
    label: str,
    logger: logging.Logger,
) -> int:
    """
    Double écriture d'un DataFrame silver :
    (a) Parquet partitionné par year/month/day dans MinIO (s3a://silver/...)
    (b) Table Hive interne via saveAsTable() (données dans s3a://warehouse/)

    Retourne le nombre de lignes écrites.
    """
    t0 = time.time()

    # (a) Parquet partitionné dans MinIO
    logger.info("[SILVER] (a) Parquet MinIO → %s", s3_path)
    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(s3_path)
    )
    logger.info("[SILVER] ✓ Parquet MinIO écrit : %s", label)

    # (b) Table Hive interne
    # format("parquet") explicite : Spark utilise le writer Parquet natif même avec
    # Hive support activé, ce qui est plus fiable que le format Hive ORC par défaut.
    # Les données sont stockées dans s3a://warehouse/<schema>.db/<table>/
    logger.info("[SILVER] (b) Table Hive → %s", hive_table_fqn)
    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(hive_table_fqn)
    )

    row_count = df.count()
    duration  = time.time() - t0
    logger.info(
        "[SILVER] ✓ '%s' | %d lignes | %.1f s | Hive: %s | MinIO: %s",
        label, row_count, duration, hive_table_fqn, s3_path,
    )
    return row_count


def persist_silver_tables(
    spark: SparkSession,
    df_food_clean: DataFrame,
    df_countries_clean: DataFrame,
    df_enriched: DataFrame,
    config: dict,
    logger: logging.Logger,
) -> dict:
    """
    Orchestre la double écriture des 3 tables silver et retourne les métriques.

    Tables écrites :
      • silver.products_clean    — produits OFF nettoyés (6 règles de validation)
      • silver.countries_clean   — données pays nettoyées (snake_case + cast)
      • silver.products_enriched — jointure produits × pays + sugar_rank_in_category
    """
    hive_cfg    = config["app"]["hive"]
    silver_db   = hive_cfg.get("silver_database", "silver")
    silver_base = config["app"]["storage"]["silver_base_path"]
    tables_cfg  = hive_cfg["tables"]

    # Création du schéma Hive 'silver' s'il n'existe pas déjà
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{silver_db}`")
    dbs = [r[0] for r in spark.sql("SHOW DATABASES").collect()]
    logger.info("[SILVER] Schéma Hive '%s' prêt | Bases disponibles : %s", silver_db, dbs)

    metrics = {}

    # Table 1 : products_clean
    # df_food_clean conserve les colonnes year/month/day du raw (partition du jour)
    metrics["products_clean"] = _write_silver_table(
        df=df_food_clean,
        s3_path=f"{silver_base}/products_clean",
        hive_table_fqn=f"{silver_db}.{tables_cfg['silver_products']}",
        label="products_clean",
        logger=logger,
    )

    # Table 2 : countries_clean
    # df_countries_clean conserve les colonnes year/month/day du raw countries
    metrics["countries_clean"] = _write_silver_table(
        df=df_countries_clean,
        s3_path=f"{silver_base}/countries_clean",
        hive_table_fqn=f"{silver_db}.{tables_cfg['silver_countries']}",
        label="countries_clean",
        logger=logger,
    )

    # Table 3 : products_enriched (jointure + rang sucre)
    # Contient year/month/day hérités du côté food de la jointure
    metrics["products_enriched"] = _write_silver_table(
        df=df_enriched,
        s3_path=f"{silver_base}/products_enriched",
        hive_table_fqn=f"{silver_db}.{tables_cfg['silver_joined']}",
        label="products_enriched",
        logger=logger,
    )

    # Vérification post-écriture via le catalog Hive
    for key, fqn in [
        ("products_clean",   f"{silver_db}.{tables_cfg['silver_products']}"),
        ("countries_clean",  f"{silver_db}.{tables_cfg['silver_countries']}"),
        ("products_enriched", f"{silver_db}.{tables_cfg['silver_joined']}"),
    ]:
        exists = spark.catalog.tableExists(fqn)
        logger.info("[SILVER] Vérif catalog Hive %-30s → %s", fqn, "✓ OK" if exists else "✗ ABSENT")

    return metrics


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="processor.py — Transformation raw → silver (Hive + MinIO)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  # Exécution complète (date du jour par défaut)
  spark-submit scripts/processor.py

  # Rejouer sur une ingestion passée
  spark-submit scripts/processor.py --date 2026-05-16

  # Mode développement (50 000 lignes OFF)
  spark-submit scripts/processor.py --sample

  # Config alternative
  spark-submit scripts/processor.py --config /opt/config
        """,
    )
    parser.add_argument(
        "--config",
        default="config",
        help="Répertoire des fichiers de configuration YAML (défaut : config/)",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Mode développement : limite Open Food Facts à 50 000 lignes",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date d'ingestion raw à traiter, format YYYY-MM-DD (défaut : aujourd'hui)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config["app"]["logging"]["log_dir"], "processor")

    # Résolution de la date : paramètre CLI ou datetime.now()
    if args.date:
        try:
            ingest_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"[ERREUR] --date invalide : '{args.date}'. Format attendu : YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
    else:
        ingest_date = datetime.now()

    logger.info(
        "=== Démarrage processor.py | date=%s | sample=%s ===",
        ingest_date.strftime("%Y-%m-%d"), args.sample,
    )
    t_global = time.time()
    spark    = None

    try:
        spark = create_spark_session(config, logger)

        # ── Chargement raw ────────────────────────────────────────────────────
        df_food      = load_raw_openfoodfacts(spark, config, logger, ingest_date, sample=args.sample)
        df_countries = load_raw_countries(spark, config, logger, ingest_date)

        rows_raw_food     = df_food.count()
        rows_raw_countries = df_countries.count()

        # ── Nettoyage ─────────────────────────────────────────────────────────
        # Try/except par étape : un échec ici est bloquant (les étapes suivantes
        # dépendent des DataFrames nettoyés). On laisse remonter l'exception.
        logger.info("─── Étape : Nettoyage Open Food Facts ───")
        df_food_clean = clean_openfoodfacts(df_food, logger)

        logger.info("─── Étape : Nettoyage Countries ───")
        df_countries_clean = clean_countries(df_countries, logger)

        # ── Jointure ──────────────────────────────────────────────────────────
        logger.info("─── Étape : Jointure produits × pays ───")
        df_joined = join_food_countries(df_food_clean, df_countries_clean, logger)

        # ── Agrégations + window function ─────────────────────────────────────
        # Try/except isolé : un échec sur les agrégations ne doit pas empêcher
        # l'écriture silver des tables de base (food_clean, countries_clean, joined).
        logger.info("─── Étape : Agrégations + window function ───")
        df_enriched = None
        try:
            df_enriched = compute_aggregations(df_joined, config, logger)
        except Exception as exc:
            logger.error(
                "[AGG] Échec des agrégations : %s — on utilisera df_joined sans sugar_rank",
                exc, exc_info=True,
            )
            # Fallback : on écrit le joined sans le rang plutôt que de tout perdre
            df_enriched = df_joined.withColumn("sugar_rank_in_category", F.lit(None).cast("int"))

        # ── Écriture silver ───────────────────────────────────────────────────
        logger.info("─── Étape : Écriture silver (MinIO + Hive) ───")
        metrics = persist_silver_tables(
            spark, df_food_clean, df_countries_clean, df_enriched, config, logger,
        )

        # ── Libération des persists (bonne pratique) ──────────────────────────
        df_food_clean.unpersist()
        df_joined.unpersist()
        logger.info("[CLEANUP] DataFrames persistés libérés")

        # ── Récap final ───────────────────────────────────────────────────────
        duration_total = time.time() - t_global
        logger.info("─── Récapitulatif processor.py ─────────────────────────────")
        logger.info("  Lignes raw OFF lues         : %d", rows_raw_food)
        logger.info("  Lignes raw Countries lues   : %d", rows_raw_countries)
        logger.info("  Lignes OFF après nettoyage  : %d", df_food_clean.count() if False else metrics.get("products_clean", 0))
        logger.info("  ─────────────────────────────────────────────────────────")
        for table_name, row_count in metrics.items():
            logger.info("  %-25s → %d lignes écrites", table_name, row_count)
        logger.info("  ─────────────────────────────────────────────────────────")
        logger.info("  Durée totale                : %.1f s", duration_total)
        logger.info("=== Traitement silver terminé avec succès ===")

    except Exception as exc:
        logger.error("Erreur fatale dans processor.py : %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession arrêtée.")
