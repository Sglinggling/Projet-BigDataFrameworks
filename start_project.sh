#!/bin/bash
# Demarrage complet du projet Food Nutrition Lakehouse
# Usage: ./start_project.sh

echo "=== 1. Démarrage des conteneurs Docker ==="
docker compose up -d
sleep 30

echo "=== 2. Vérification des services ==="
docker compose ps

echo "=== 3. Installation pyyaml dans Spark (bug connu) ==="
docker exec spark-master pip install pyyaml --quiet
docker exec spark-worker pip install pyyaml --quiet

echo "=== 4. Sync du JAR PostgreSQL master->worker ==="
docker cp spark-master:/opt/spark/jars/postgresql-42.7.1.jar /tmp/pg-driver.jar
docker cp /tmp/pg-driver.jar spark-worker:/opt/spark/jars/postgresql-42.7.1.jar

echo "=== 5. Nettoyage MinIO ==="
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin123 2>/dev/null || true
docker exec minio mc rm --recursive --force local/warehouse/ 2>/dev/null || true
docker exec minio mc rm --recursive --force local/silver/ 2>/dev/null || true
docker exec minio mc rm --recursive --force local/raw/ 2>/dev/null || true

echo "=== 6. Pipeline FEEDER (raw Parquet) ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/feeder.py --config /opt/spark/config --sample

echo "=== 7. Pipeline PROCESSOR (silver + Hive) ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/processor.py --config /opt/spark/config --sample

echo "=== 8. Pipeline DATAMART (PostgreSQL) ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/datamart.py --config /opt/spark/config

echo "=== 9. Vérification finale Postgres ==="
docker exec postgres psql -U foodnutrition -d food_nutrition -c "
SELECT 'dm_sugar' AS dm, COUNT(*) FROM dm_sugar_by_category
UNION ALL SELECT 'dm_nutri', COUNT(*) FROM dm_nutriscore_by_country
UNION ALL SELECT 'dm_addit', COUNT(*) FROM dm_additives_analysis
UNION ALL SELECT 'dm_ml', COUNT(*) FROM dm_ml_nutriscore_prediction;
"

echo ""
echo "=== 🎉 PROJET PRÊT ==="
echo "Spark UI       : http://localhost:8080"
echo "MinIO Console  : http://localhost:9001 (minioadmin / minioadmin123)"
echo "API Swagger    : http://localhost:8000/docs"
