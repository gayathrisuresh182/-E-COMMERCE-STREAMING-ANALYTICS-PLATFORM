#!/usr/bin/env python3
"""
Check if BigQuery tables are populated for Looker Studio dashboards.

Usage:
  python scripts/check_bigquery_data.py [--project ecommerce-analytics-prod]

Requires: GOOGLE_APPLICATION_CREDENTIALS, google-cloud-bigquery
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
load_dotenv(Path(__file__).resolve().parent.parent.parent / "docs" / ".env")
except ImportError:
    pass

try:
    from google.cloud import bigquery
except ImportError:
    print("pip install google-cloud-bigquery")
    sys.exit(1)


# Tables that Looker Studio typically needs
TABLES_TO_CHECK = [
    ("marts", "fct_orders", "Orders fact - primary for dashboards"),
    ("marts", "fct_daily_metrics", "Daily aggregated metrics"),
    ("marts", "fct_experiment_results", "A/B test results"),
    ("staging", "stg_orders", "Staging orders (source)"),
    ("realtime", "realtime_orders", "Streaming orders"),
    ("realtime", "realtime_metrics", "5-min window metrics"),
    ("experiments", "experiment_results", "Experiment outcomes"),
]


def main() -> None:
    ap = argparse.ArgumentParser(description="Check BigQuery table row counts")
    ap.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    args = ap.parse_args()

    project = args.project or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project")
        sys.exit(1)

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_APPLICATION_CREDENTIALS")
        sys.exit(1)

    client = bigquery.Client(project=project)
    print(f"Project: {project}\n")
    print(f"{'Dataset':<15} {'Table':<25} {'Rows':>12} {'Status':<10} Description")
    print("-" * 85)

    for dataset, table, desc in TABLES_TO_CHECK:
        table_id = f"`{project}.{dataset}.{table}`"
        try:
            query = f"SELECT COUNT(*) AS n FROM {table_id}"
            result = list(client.query(query).result())
            n = result[0].n if result else 0
            status = "OK" if n > 0 else "EMPTY"
            print(f"{dataset:<15} {table:<25} {n:>12,} {status:<10} {desc}")
        except Exception as e:
            err = str(e)[:40] + "..." if len(str(e)) > 40 else str(e)
            print(f"{dataset:<15} {table:<25} {'N/A':>12} {'MISSING':<10} {desc} ({err})")

    print("\nIf tables are EMPTY or MISSING:")
    print("  1. Create tables: python scripts/bigquery_create_datasets.py && python scripts/bigquery_create_tables.py")
    print("  2. Load data: Run Dagster with BigQuery I/O, or use bq load / custom ETL")
    print("  3. For Olist data: Load raw CSVs to staging, then run Dagster batch jobs")


if __name__ == "__main__":
    main()
