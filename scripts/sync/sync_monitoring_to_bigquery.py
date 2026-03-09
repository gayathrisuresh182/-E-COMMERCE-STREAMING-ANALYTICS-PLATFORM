#!/usr/bin/env python3
"""
Sync pipeline health metrics to BigQuery for Phase 5F Data Quality dashboard.

Queries Kafka consumer lag and MongoDB collection stats, loads to monitoring.consumer_health_metrics.
Can be run on a schedule (e.g. every 5-15 min) for near real-time monitoring.

Requires: Kafka + MongoDB running, GOOGLE_APPLICATION_CREDENTIALS.

Usage: python scripts/sync_monitoring_to_bigquery.py [--project YOUR_PROJECT]
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


def _query_kafka_lag() -> list[dict]:
    from ecommerce_analytics.assets.stream_ops_assets import _query_kafka_lag as q
    return q()


def _query_mongo_stats() -> list[dict]:
    from ecommerce_analytics.assets.stream_ops_assets import _query_mongo_stats as q
    return q()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    args = ap.parse_args()
    if not args.project:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project")
        sys.exit(1)
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_APPLICATION_CREDENTIALS")
        sys.exit(1)

    import pandas as pd
    from google.cloud import bigquery

    kafka_rows = _query_kafka_lag()
    mongo_rows = _query_mongo_stats()

    kafka_df = pd.DataFrame(kafka_rows)
    mongo_df = pd.DataFrame(mongo_rows)

    # Normalize for single table: kafka has consumer_group, topics, total_lag, etc.; mongo has collection, document_count, etc.
    kafka_df["source"] = "kafka"
    kafka_df["collection"] = None
    kafka_df["label"] = None
    kafka_df["document_count"] = None
    kafka_df["newest_timestamp"] = None

    mongo_df["source"] = "mongodb"
    mongo_df["consumer_group"] = None
    mongo_df["topics"] = None
    mongo_df["total_lag"] = None
    mongo_df["partitions"] = None
    mongo_df["status"] = None

    combined = pd.concat([kafka_df, mongo_df], ignore_index=True)

    # Align columns for BQ table
    cols = ["consumer_group", "topics", "total_lag", "partitions", "status", "measured_at",
            "source", "collection", "label", "document_count", "newest_timestamp", "error"]
    for c in cols:
        if c not in combined.columns:
            combined[c] = None
    combined["measured_at"] = pd.to_datetime(combined.get("measured_at", pd.NaT), errors="coerce")
    combined = combined[[c for c in cols if c in combined.columns]]

    table_id = f"{args.project}.monitoring.consumer_health_metrics"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    client = bigquery.Client(project=args.project)
    job = client.load_table_from_dataframe(combined, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(combined)} rows into {table_id}")


if __name__ == "__main__":
    main()
