-- indexes.sql
-- Index PostgreSQL pour optimiser les requêtes de l'API FastAPI.
-- À exécuter après la première écriture des datamarts par datamart.py.

-- TODO Phase 4 : créer les index après validation des schémas finaux

-- dm_sugar_by_category
-- CREATE INDEX IF NOT EXISTS idx_sugar_category ON dm_sugar_by_category (category);
-- CREATE INDEX IF NOT EXISTS idx_sugar_rank ON dm_sugar_by_category (rank_in_category);

-- dm_nutriscore_by_country
-- CREATE INDEX IF NOT EXISTS idx_nutriscore_country ON dm_nutriscore_by_country (country);
-- CREATE INDEX IF NOT EXISTS idx_nutriscore_region ON dm_nutriscore_by_country (region);

-- dm_additives_analysis
-- CREATE INDEX IF NOT EXISTS idx_additives_rank ON dm_additives_analysis (rank_global);
-- CREATE INDEX IF NOT EXISTS idx_additives_code ON dm_additives_analysis (additive_code);

-- dm_ml_nutriscore_prediction
-- CREATE INDEX IF NOT EXISTS idx_ml_target ON dm_ml_nutriscore_prediction (nutriscore_target);
