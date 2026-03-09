#!/usr/bin/env python3
"""
Create optional BigQuery views for Phase 5B dashboard (realtime orders last 24h, GMV by 5-min).
Run after: bigquery_create_datasets.py and bigquery_create_tables.py.
Requires: GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_CLOUD_PROJECT or --project.

Usage: python scripts/bigquery_create_realtime_views.py [--project YOUR_PROJECT]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    load_dotenv(PROJECT_ROOT / "docs" / ".env")
except ImportError:
    pass

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=PROJECT, help="GCP project ID")
    args = ap.parse_args()
    if not args.project:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project")
        sys.exit(1)
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_APPLICATION_CREDENTIALS")
        sys.exit(1)

    from google.cloud import bigquery
    client = bigquery.Client(project=args.project)
    project = args.project

    views = [
        (
            f"realtime.realtime_orders_last_24h",
            f"""
            CREATE OR REPLACE VIEW `{project}.realtime.realtime_orders_last_24h` AS
            SELECT *
            FROM `{project}.realtime.realtime_orders`
            WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            """,
        ),
        (
            f"realtime.gmv_5min",
            f"""
            CREATE OR REPLACE VIEW `{project}.realtime.gmv_5min` AS
            SELECT window_start, metric_value AS gmv
            FROM `{project}.realtime.realtime_metrics`
            WHERE metric_name = 'total_gmv'
              AND window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            """,
        ),
    ]
    for name, sql in views:
        try:
            client.query(sql).result()
            print(f"Created/updated: {name}")
        except Exception as e:
            print(f"Failed {name}: {e}")
    print("Done.")


if __name__ == "__main__":
    main()
