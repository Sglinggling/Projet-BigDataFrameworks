"""
export_for_powerbi.py — Export CSV des 4 datamarts PostgreSQL vers Power BI

Rôle  : Lire les 4 tables datamarts dans PostgreSQL (via psycopg2, sans Spark)
         et les exporter en CSV (UTF-8 BOM, séparateur virgule) dans data/output/powerbi/.

Exécution depuis l'hôte (pas dans Docker) :
    python3 scripts/export_for_powerbi.py --config config

Options :
    --config  DIR      Répertoire des fichiers YAML (défaut : config)
    --output  DIR      Répertoire de sortie CSV    (défaut : data/output/powerbi)
    --host    HOST     Override du host PostgreSQL  (défaut : localhost)
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import yaml


# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging(log_dir: str) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"export_powerbi_{timestamp}.txt")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logger = logging.getLogger("export_powerbi")
    logger.info("Log : %s", log_file)
    return logger


# ─── Configuration ────────────────────────────────────────────────────────────

def load_config(config_dir: str) -> dict:
    path = os.path.join(config_dir, "database_config.yaml")
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ─── Connexion PostgreSQL ─────────────────────────────────────────────────────

def connect_postgres(
    config: dict,
    host_override: str | None,
    logger: logging.Logger,
) -> psycopg2.extensions.connection:
    db  = config["postgresql"]
    host = host_override or "localhost"
    logger.info(
        "Connexion PostgreSQL → %s:%s/%s (user: %s)",
        host, db["port"], db["database"], db["user"],
    )
    conn = psycopg2.connect(
        host=host,
        port=db["port"],
        dbname=db["database"],
        user=db["user"],
        password=db["password"],
        connect_timeout=10,
    )
    conn.set_session(readonly=True, autocommit=True)
    logger.info("Connexion etablie.")
    return conn


# ─── Export d'une table ───────────────────────────────────────────────────────

def export_table(
    conn,
    table_name: str,
    order_by: str,
    output_dir: Path,
    logger: logging.Logger,
) -> int:
    """
    Exporte une table PostgreSQL en CSV (UTF-8 BOM, virgule, header inclus).
    Retourne le nombre de lignes écrites.
    """
    csv_path = output_dir / f"{table_name}.csv"
    query    = f"SELECT * FROM {table_name} ORDER BY {order_by}"

    logger.info("[EXPORT] %s → %s", table_name, csv_path)

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows    = cur.fetchall()

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(columns)
        writer.writerows(rows)

    row_count = len(rows)
    size_kb   = csv_path.stat().st_size / 1024
    logger.info(
        "[EXPORT] ✓ %s | %d lignes | %.1f Ko | colonnes : %s",
        table_name, row_count, size_kb, ", ".join(columns),
    )
    return row_count


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export CSV des datamarts PostgreSQL pour Power BI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  # Export standard (hôte : localhost)
  python3 scripts/export_for_powerbi.py

  # Config et sortie personnalisées
  python3 scripts/export_for_powerbi.py --config config --output data/output/powerbi

  # Override du host PostgreSQL (ex. depuis Docker)
  python3 scripts/export_for_powerbi.py --host postgres
        """,
    )
    parser.add_argument(
        "--config",
        default="config",
        help="Répertoire des fichiers YAML (défaut : config/)",
    )
    parser.add_argument(
        "--output",
        default="data/output/powerbi",
        help="Répertoire de sortie CSV (défaut : data/output/powerbi/)",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="Override du host PostgreSQL (défaut : localhost)",
    )
    args = parser.parse_args()

    # ── Setup ──────────────────────────────────────────────────────────────────
    log_dir = "logs"
    logger  = setup_logging(log_dir)
    logger.info("=== Démarrage export_for_powerbi.py ===")

    config     = load_config(args.config)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Répertoire de sortie : %s", output_dir.resolve())

    # ── Tables à exporter : (nom_table, ORDER BY) ──────────────────────────────
    datamarts = config["datamarts"]
    exports = [
        (datamarts["sugar_by_category"],       "category, sugar_rank_in_category"),
        (datamarts["nutriscore_by_country"],    "country, nutrition_grade_fr"),
        (datamarts["additives_analysis"],       "rank_overall"),
        (datamarts["ml_nutriscore_prediction"], "code"),
    ]

    # ── Connexion et export ────────────────────────────────────────────────────
    conn    = None
    metrics = {}
    errors  = []

    try:
        conn = connect_postgres(config, args.host, logger)

        for table_name, order_by in exports:
            try:
                count = export_table(conn, table_name, order_by, output_dir, logger)
                metrics[table_name] = count
            except Exception as exc:
                logger.error("[EXPORT] Échec %s : %s", table_name, exc, exc_info=True)
                errors.append(table_name)

    except psycopg2.OperationalError as exc:
        logger.error(
            "Impossible de se connecter à PostgreSQL : %s\n"
            "  → Vérifiez que le container postgres est démarré (docker compose up -d)\n"
            "  → Utilisez --host postgres si vous exécutez depuis l'intérieur du réseau Docker",
            exc,
        )
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            logger.info("Connexion PostgreSQL fermée.")

    # ── Récapitulatif ──────────────────────────────────────────────────────────
    logger.info("─── Récapitulatif ───────────────────────────────────────────")
    for table, count in metrics.items():
        logger.info("  %-42s → %d lignes", table, count)
    if errors:
        logger.error("  Tables en ÉCHEC : %s", errors)
    logger.info(
        "  Fichiers CSV dans : %s",
        output_dir.resolve(),
    )

    if errors:
        logger.error("=== %d table(s) en erreur ===", len(errors))
        sys.exit(1)
    else:
        logger.info(
            "=== %d CSV exportés avec succès ===",
            len(metrics),
        )
