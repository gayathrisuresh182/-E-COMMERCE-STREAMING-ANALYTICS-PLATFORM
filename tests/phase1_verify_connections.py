#!/usr/bin/env python3
"""
Phase 1 Verification: Test all infrastructure connections.
Usage: python scripts/phase1_verify_connections.py [--skip-s3] [--skip-bigquery]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
load_dotenv(Path(__file__).resolve().parent.parent / "docs" / ".env")

CHECKS: list[tuple[str, bool | None]] = []


def check(name: str, ok: bool | None, msg: str = "") -> None:
    status = "OK" if ok is True else ("FAIL" if ok is False else "SKIP")
    CHECKS.append((name, ok))
    print(f"  [{status}] {name}" + (f" - {msg}" if msg else ""))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-s3", action="store_true", help="Skip S3 (no bucket configured)")
    ap.add_argument("--skip-bigquery", action="store_true", help="Skip BigQuery")
    args = ap.parse_args()

    print("Phase 1 connection verification\n")

    # Kafka
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092").split(","))
        admin.list_topics()
        admin.close()
        check("Kafka (Python)", True)
    except Exception as e:
        check("Kafka (Python)", False, str(e))

    # MongoDB
    try:
        import pymongo
        uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        client = pymongo.MongoClient(uri)
        client.admin.command("ping")
        client.close()
        check("MongoDB (Python)", True)
    except Exception as e:
        check("MongoDB (Python)", False, str(e))

    # BigQuery
    if not args.skip_bigquery:
        try:
            from google.cloud import bigquery
            if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                check("BigQuery (Python)", False, "GOOGLE_APPLICATION_CREDENTIALS not set")
            else:
                project = os.environ.get("GOOGLE_CLOUD_PROJECT")
                client = bigquery.Client(project=project)
                client.query("SELECT 1").result()
                check("BigQuery (Python)", True)
        except Exception as e:
            check("BigQuery (Python)", False, str(e))
    else:
        check("BigQuery (Python)", None, "skipped")

    # S3 (optional: skip when bucket not configured)
    if args.skip_s3:
        check("S3 (Python)", None, "skipped")
    else:
        bucket = os.environ.get("S3_BUCKET", "").strip()
        if not bucket:
            check("S3 (Python)", None, "S3_BUCKET not set (optional)")
        else:
            try:
                import boto3
                s3 = boto3.client("s3")
                s3.head_bucket(Bucket=bucket)
                check("S3 (Python)", True)
            except Exception as e:
                check("S3 (Python)", False, str(e))

    # Summary
    passed = sum(1 for _, r in CHECKS if r is True)
    failed = sum(1 for _, r in CHECKS if r is False)
    skipped = sum(1 for _, r in CHECKS if r is None)
    parts = [f"{passed} passed", f"{failed} failed"]
    if skipped:
        parts.append(f"{skipped} skipped")
    print(f"\nResult: {', '.join(parts)}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
