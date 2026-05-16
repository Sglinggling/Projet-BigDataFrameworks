-- create_datamarts.sql
-- Schémas des tables datamarts dans PostgreSQL.
-- Ces tables sont créées/écrasées automatiquement par datamart.py (JDBC overwrite).
-- Ce fichier sert à la documentation et à l'initialisation manuelle si nécessaire.

CREATE TABLE IF NOT EXISTS dm_sugar_by_category (
    category               TEXT,
    product_name           TEXT,
    sugars_100g            DOUBLE PRECISION,
    nutrition_grade_fr     TEXT,
    sugar_rank_in_category INTEGER
);

CREATE TABLE IF NOT EXISTS dm_nutriscore_by_country (
    country                TEXT,
    region                 TEXT,
    nutrition_grade_fr     TEXT,
    nb_products            BIGINT,
    avg_nutriscore_score   DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dm_additives_analysis (
    additive_tag           TEXT,
    total_occurrences      BIGINT,
    nb_distinct_categories BIGINT,
    rank_overall           INTEGER,
    pct_of_total           DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dm_ml_nutriscore_prediction (
    code                   TEXT PRIMARY KEY,
    sugars_100g            DOUBLE PRECISION,
    fat_100g               DOUBLE PRECISION,
    saturated_fat_100g     DOUBLE PRECISION,
    salt_100g              DOUBLE PRECISION,
    energy_100g            DOUBLE PRECISION,
    proteins_100g          DOUBLE PRECISION,
    fiber_100g             DOUBLE PRECISION,
    additives_n            INTEGER,
    main_category          TEXT,
    country_normalized     TEXT,
    population             BIGINT,
    area_sq_mi             BIGINT,
    nutrition_grade_fr     TEXT
);
