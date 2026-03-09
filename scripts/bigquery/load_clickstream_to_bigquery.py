#!/usr/bin/env python3
"""
Load clickstream events from events.ndjson into BigQuery for Phase 5D Customer Journey dashboard.

Source: output/events.ndjson (from generate_clickstream_events.py)
Target: marts.clickstream_events

Usage: python scripts/load_clickstream_to_bigquery.py [--project YOUR_PROJECT] [--limit N]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EVENTS_PATH = PROJECT_ROOT / "output" / "events.ndjson"

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    load_dotenv(PROJECT_ROOT / "docs" / ".env")
except ImportError:
    pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    ap.add_argument("--limit", type=int, default=0, help="Limit rows (0=all)")
    args = ap.parse_args()
    if not args.project:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project")
        sys.exit(1)
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_APPLICATION_CREDENTIALS")
        sys.exit(1)
    if not EVENTS_PATH.exists():
        print(f"Missing {EVENTS_PATH}. Run: python scripts/generate_clickstream_events.py --limit 10000")
        sys.exit(1)

    rows = []
    with open(EVENTS_PATH, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if args.limit and i >= args.limit:
                break
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            props = ev.get("properties") or {}
            ts = ev.get("timestamp")
            if not ts:
                continue
            rows.append({
                "event_id": str(ev.get("event_id", "")),
                "event_type": str(ev.get("event_type", "")),
                "event_timestamp": pd.to_datetime(ts, errors="coerce"),
                "session_id": str(ev.get("session_id", "")),
                "customer_id": str(ev.get("customer_id", "")),
                "experiment_id": str(ev.get("experiment_id", "")),
                "variant": str(ev.get("variant", "")),
                "device": str(props.get("device", "")),
                "referrer": str(props.get("referrer", "")),
                "page_url": str(props.get("page_url", "")),
                "order_id": str(props.get("order_id", "")),
            })

    if not rows:
        print("No valid events to load.")
        sys.exit(0)

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["event_timestamp"])
    df["_loaded_at"] = pd.Timestamp.utcnow()

    from google.cloud import bigquery
    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.marts.clickstream_events"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df):,} rows into {table_id}")


if __name__ == "__main__":
    main()
