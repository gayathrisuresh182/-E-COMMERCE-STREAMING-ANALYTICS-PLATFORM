#!/usr/bin/env python3
"""
Sync realtime data from MongoDB to BigQuery for the Phase 5B dashboard.

Reads MongoDB collections fct_orders_realtime and realtime_metrics (populated
by Kafka consumers), then loads them into BigQuery realtime.realtime_orders
and realtime.realtime_metrics. Run this after Steps 1–3 (Kafka + producers +
consumers) so MongoDB has data.

Prerequisites:
  - BigQuery datasets and tables exist (python scripts/bigquery_create_datasets.py
    and scripts/bigquery_create_tables.py)
  - GOOGLE_APPLICATION_CREDENTIALS and (optional) GOOGLE_CLOUD_PROJECT set
  - MongoDB has been populated by run_consumers.py

Usage:
  python scripts/sync_realtime_to_bigquery.py [--project YOUR_GCP_PROJECT]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import pymongo

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load env (MongoDB and optional GCP project)
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    load_dotenv(PROJECT_ROOT / "docs" / ".env")
except ImportError:
    pass

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce_analytics")


def _customer_state_lookup(client, db_name: str) -> dict[str, str]:
    """Build customer_id -> state from dim_customers (field 'state')."""
    coll = client[db_name]["dim_customers"]
    cursor = coll.find({}, {"_id": 0, "customer_id": 1, "state": 1})
    return {str(d["customer_id"]).strip(): str(d.get("state") or "").strip() for d in cursor if d.get("customer_id")}


def _mongo_realtime_orders() -> pd.DataFrame:
    """Read fct_orders_realtime from MongoDB and map to BigQuery schema. Backfill customer_state from dim_customers when blank."""
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    coll = client[MONGO_DB]["fct_orders_realtime"]
    cursor = coll.find({}, {"_id": 0})
    rows = list(cursor)
    if not rows:
        client.close()
        return pd.DataFrame(columns=[
            "event_id", "order_id", "customer_id", "customer_state",
            "event_timestamp", "order_total", "event_type", "created_at",
        ])
    df = pd.DataFrame(rows)
    # Backfill customer_state from dim_customers when missing/blank (e.g. docs written before consumer fix)
    state_lookup = _customer_state_lookup(client, MONGO_DB)
    client.close()
    if state_lookup:
        missing = (df.get("customer_state", pd.Series(dtype=object)).isna()) | (df.get("customer_state", "").astype(str).str.strip() == "")
        if missing.any():
            cids = df.loc[missing, "customer_id"].astype(str).str.strip()
            df.loc[missing, "customer_state"] = cids.map(lambda x: state_lookup.get(x, "") or "").values
    # Map to BigQuery realtime.realtime_orders schema
    out = pd.DataFrame()
    out["event_id"] = df.get("order_id", pd.Series(dtype=str)).astype(str)
    out["order_id"] = df.get("order_id", "").astype(str)
    out["customer_id"] = df.get("customer_id", "").astype(str)
    customer_state_col = df["customer_state"] if "customer_state" in df.columns else pd.Series([""] * len(df), index=df.index)
    out["customer_state"] = customer_state_col.fillna("").astype(str).str.strip().replace("", "unknown")
    out["event_timestamp"] = pd.to_datetime(df.get("event_timestamp", df.get("processed_at")), errors="coerce")
    out["order_total"] = pd.to_numeric(df.get("order_total", 0), errors="coerce").fillna(0)
    event_type_col = df["event_type"] if "event_type" in df.columns else pd.Series(["order_placed"] * len(df), index=df.index)
    out["event_type"] = event_type_col.fillna("order_placed").astype(str).str.strip().replace("", "order_placed")
    out["created_at"] = pd.to_datetime(df.get("processed_at"), errors="coerce")
    return out.dropna(subset=["event_timestamp"])


def _mongo_realtime_metrics() -> pd.DataFrame:
    """Read realtime_metrics from MongoDB and map to BigQuery schema."""
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    coll = client[MONGO_DB]["realtime_metrics"]
    cursor = coll.find({}, {"_id": 0})
    rows = list(cursor)
    client.close()
    if not rows:
        return pd.DataFrame(columns=[
            "window_start", "window_end", "metric_name", "metric_value", "_ingested_at",
        ])
    df = pd.DataFrame(rows)
    out = pd.DataFrame()
    out["window_start"] = pd.to_datetime(df["window_start"], errors="coerce")
    out["window_end"] = pd.to_datetime(df["window_end"], errors="coerce")
    out["metric_name"] = df.get("metric_name", "").astype(str)
    out["metric_value"] = pd.to_numeric(df.get("metric_value", 0), errors="coerce").fillna(0)
    out["_ingested_at"] = pd.Timestamp.utcnow()
    return out.dropna(subset=["window_start", "metric_name"])


def main() -> None:
    ap = argparse.ArgumentParser(description="Sync MongoDB realtime collections to BigQuery")
    ap.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"), help="GCP project ID")
    ap.add_argument("--dry-run", action="store_true", help="Only read MongoDB, do not write BigQuery")
    args = ap.parse_args()

    if not args.project and not args.dry_run:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project (or use --dry-run to only read MongoDB)")
        sys.exit(1)
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and not args.dry_run:
        print("Set GOOGLE_APPLICATION_CREDENTIALS for BigQuery")
        sys.exit(1)

    print("Reading MongoDB...")
    orders_df = _mongo_realtime_orders()
    metrics_df = _mongo_realtime_metrics()
    print(f"  fct_orders_realtime -> {len(orders_df)} rows")
    print(f"  realtime_metrics    -> {len(metrics_df)} rows")

    if args.dry_run:
        print("Dry run: skipping BigQuery load.")
        return

    from google.cloud import bigquery
    client = bigquery.Client(project=args.project)

    # Load realtime_orders (keep timestamp columns as datetime for partition column type)
    if not orders_df.empty:
        table_id = f"{args.project}.realtime.realtime_orders"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        job = client.load_table_from_dataframe(orders_df, table_id, job_config=job_config)
        job.result()
        print(f"  Loaded {len(orders_df)} rows -> {table_id}")
    else:
        print("  No realtime orders to load.")

    # Load realtime_metrics (keep timestamp columns as datetime for partition column type)
    if not metrics_df.empty:
        table_id = f"{args.project}.realtime.realtime_metrics"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        job = client.load_table_from_dataframe(metrics_df, table_id, job_config=job_config)
        job.result()
        print(f"  Loaded {len(metrics_df)} rows -> {table_id}")
    else:
        print("  No realtime metrics to load.")

    print("Done.")


if __name__ == "__main__":
    main()
