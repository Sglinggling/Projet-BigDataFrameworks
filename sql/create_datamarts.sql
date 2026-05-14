-- create_datamarts.sql
-- Création des tables datamarts dans PostgreSQL.
-- Ces tables sont également créées automatiquement par datamart.py (JDBC overwrite),
-- mais ce fichier permet une initialisation manuelle ou une vérification de schéma.

-- TODO Phase 4 : définir les colonnes exactes après implémentation de datamart.py

CREATE TABLE IF NOT EXISTS dm_sugar_by_category (
    -- category              TEXT,
    -- product_name          TEXT,
    -- sugars_100g           DOUBLE PRECISION,
    -- avg_sugars_category   DOUBLE PRECISION,
    -- rank_in_category      INTEGER
);

CREATE TABLE IF NOT EXISTS dm_nutriscore_by_country (
    -- country                    TEXT,
    -- region                     TEXT,
    -- avg_nutriscore_numeric     DOUBLE PRECISION,
    -- nutriscore_rank_in_region  INTEGER,
    -- total_products             BIGINT,
    -- population                 BIGINT
);

CREATE TABLE IF NOT EXISTS dm_additives_analysis (
    -- additive_code    TEXT,
    -- additive_name    TEXT,
    -- total_products   BIGINT,
    -- top_category     TEXT,
    -- rank_global      INTEGER,
    -- pct_of_total     DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dm_ml_nutriscore_prediction (
    -- product_id           TEXT,
    -- energy_100g          DOUBLE PRECISION,
    -- fat_100g             DOUBLE PRECISION,
    -- saturated_fat_100g   DOUBLE PRECISION,
    -- sugars_100g          DOUBLE PRECISION,
    -- fiber_100g           DOUBLE PRECISION,
    -- proteins_100g        DOUBLE PRECISION,
    -- salt_100g            DOUBLE PRECISION,
    -- additives_count      INTEGER,
    -- nutriscore_target    INTEGER
);
