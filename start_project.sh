#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# start_project.sh — Démarrage complet du projet Food Nutrition Lakehouse
#
# Usage:
#   ./start_project.sh           → mode COMPLET (356k lignes, ~15-25 min)
#   ./start_project.sh --sample  → mode SAMPLE (10k lignes, ~1 min, pour debug)
#
# Le script télécharge automatiquement les dépendances JARs si manquantes,
# pour que `git clone` + `./start_project.sh` suffise à tout lancer.
# ─────────────────────────────────────────────────────────────────────────────

# Détection du flag --sample
SAMPLE_FLAG=""
MODE_LABEL="COMPLET (356k lignes, ~15-25 min)"
if [[ "$1" == "--sample" ]]; then
  SAMPLE_FLAG="--sample"
  MODE_LABEL="SAMPLE (10k lignes, ~1 min)"
fi

echo "🚀 Démarrage en mode : $MODE_LABEL"
echo ""

echo "=== 1. Démarrage des conteneurs Docker ==="
docker compose up -d
sleep 30

echo "=== 2. Vérification des services ==="
docker compose ps

echo "=== 3. Installation pyyaml dans Spark (bug connu) ==="
docker exec spark-master pip install pyyaml --quiet 2>/dev/null
docker exec spark-worker pip install pyyaml --quiet 2>/dev/null

echo "=== 4. Téléchargement automatique + copie des JARs S3A/PostgreSQL ==="

# JARs nécessaires avec leurs URLs de téléchargement et tailles attendues
declare -A JAR_URLS=(
  ["hadoop-aws-3.3.4.jar"]="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  ["aws-java-sdk-bundle-1.12.262.jar"]="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
  ["postgresql-42.7.1.jar"]="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar"
)

# Tailles minimales attendues (en bytes) pour détecter les téléchargements tronqués
declare -A JAR_MIN_SIZES=(
  ["hadoop-aws-3.3.4.jar"]="900000"
  ["aws-java-sdk-bundle-1.12.262.jar"]="270000000"
  ["postgresql-42.7.1.jar"]="1000000"
)

mkdir -p jars

for jar in "hadoop-aws-3.3.4.jar" "aws-java-sdk-bundle-1.12.262.jar" "postgresql-42.7.1.jar"; do
  # Vérifier si le fichier existe ET fait la bonne taille
  needs_download=false
  
  if [ ! -f "jars/$jar" ]; then
    echo "   → JAR manquant : $jar"
    needs_download=true
  else
    actual_size=$(stat -f%z "jars/$jar" 2>/dev/null || stat -c%s "jars/$jar" 2>/dev/null)
    min_size=${JAR_MIN_SIZES[$jar]}
    if [ "$actual_size" -lt "$min_size" ]; then
      echo "   ⚠️  JAR tronqué : $jar (${actual_size} bytes < ${min_size} attendu)"
      needs_download=true
    fi
  fi

  # Télécharger si nécessaire
  if [ "$needs_download" = true ]; then
    echo "   → Téléchargement automatique de $jar depuis Maven Central..."
    curl -L --fail --retry 3 -o "jars/$jar" "${JAR_URLS[$jar]}"
    
    # Re-vérifier la taille après download
    actual_size=$(stat -f%z "jars/$jar" 2>/dev/null || stat -c%s "jars/$jar" 2>/dev/null)
    min_size=${JAR_MIN_SIZES[$jar]}
    if [ "$actual_size" -lt "$min_size" ]; then
      echo "   ❌ Échec du téléchargement (taille ${actual_size} < ${min_size})"
      echo "   Vérifier votre connexion réseau (proxy, VPN, Zscaler...) et relancer."
      exit 1
    fi
    echo "   ✓ $jar téléchargé (${actual_size} bytes)"
  fi

  # Copier dans les conteneurs Spark
  echo "   → Copie $jar vers spark-master et spark-worker"
  docker cp "jars/$jar" "spark-master:/opt/spark/jars/$jar"
  docker cp "jars/$jar" "spark-worker:/opt/spark/jars/$jar"
done

# Vérification finale des tailles dans les conteneurs
echo "   → Vérification des tailles dans les conteneurs :"
docker exec spark-master ls -lh /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar | awk '{print "     master: "$5"  "$NF}'
docker exec spark-worker ls -lh /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar | awk '{print "     worker: "$5"  "$NF}'

echo "=== 5. Nettoyage MinIO ==="
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin123 2>/dev/null || true
docker exec minio mc rm --recursive --force local/warehouse/ 2>/dev/null || true
docker exec minio mc rm --recursive --force local/silver/ 2>/dev/null || true
docker exec minio mc rm --recursive --force local/raw/ 2>/dev/null || true

echo "=== 6. Pipeline FEEDER (raw Parquet) ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/feeder.py --config /opt/spark/config $SAMPLE_FLAG

echo "=== 7. Pipeline PROCESSOR (silver + Hive) ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/processor.py --config /opt/spark/config $SAMPLE_FLAG

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
echo "=== 🎉 PROJET PRÊT (mode : $MODE_LABEL) ==="
echo "Spark UI       : http://localhost:8080"
echo "MinIO Console  : http://localhost:9001 (minioadmin / minioadmin123)"
echo "API Swagger    : http://localhost:8000/docs"
echo ""