-- indexes.sql
-- Index PostgreSQL pour optimiser les requêtes de l'API FastAPI (Phase 5).
-- Exécutés automatiquement par datamart.py après chaque écriture JDBC.

-- dm_sugar_by_category
CREATE INDEX IF NOT EXISTS idx_sugar_category ON dm_sugar_by_category (category);
CREATE INDEX IF NOT EXISTS idx_sugar_rank     ON dm_sugar_by_category (sugar_rank_in_category);
CREATE INDEX IF NOT EXISTS idx_sugar_grade    ON dm_sugar_by_category (nutrition_grade_fr);

-- dm_nutriscore_by_country
CREATE INDEX IF NOT EXISTS idx_ns_country ON dm_nutriscore_by_country (country);
CREATE INDEX IF NOT EXISTS idx_ns_region  ON dm_nutriscore_by_country (region);
CREATE INDEX IF NOT EXISTS idx_ns_grade   ON dm_nutriscore_by_country (nutrition_grade_fr);

-- dm_additives_analysis
CREATE INDEX IF NOT EXISTS idx_add_rank ON dm_additives_analysis (rank_overall);
CREATE INDEX IF NOT EXISTS idx_add_tag  ON dm_additives_analysis (additive_tag);

-- dm_ml_nutriscore_prediction
CREATE INDEX IF NOT EXISTS idx_ml_grade    ON dm_ml_nutriscore_prediction (nutrition_grade_fr);
CREATE INDEX IF NOT EXISTS idx_ml_category ON dm_ml_nutriscore_prediction (main_category);
