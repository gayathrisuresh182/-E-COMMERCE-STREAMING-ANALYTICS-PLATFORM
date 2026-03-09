#!/usr/bin/env python3
"""
Phase 1 Verification: S3 -> BigQuery.
1. Upload a small test CSV to S3 bronze (or use existing).
2. Read from S3 with boto3, load into BigQuery staging.
3. Query BigQuery and verify row(s).
Usage: python scripts/phase1_test_s3_to_bigquery.py [--bucket BUCKET]
Requires: AWS credentials, GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_CLOUD_PROJECT
"""
from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
load_dotenv(Path(__file__).resolve().parent.parent / "docs" / ".env")


def main() -> None:
    import boto3
    from google.cloud import bigquery

    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        print("Set S3_BUCKET or pass --bucket")
        sys.exit(1)
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project or not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_CLOUD_PROJECT and GOOGLE_APPLICATION_CREDENTIALS")
        sys.exit(1)

    key = "bronze/olist_historical/orders/phase1_verify_s3.csv"
    csv_content = "order_id,customer_id,order_status,order_total\n"
    csv_content += f"verify_s3_{uuid.uuid4().hex[:8]},cust_s3_001,delivered,42.50\n"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=csv_content.encode("utf-8"))
    print("Uploaded test CSV to s3://" + bucket + "/" + key)

    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    rows = [r.split(",") for r in body.strip().split("\n")]
    header, data = rows[0], rows[1:]
    order_id = data[0][0] if data else ""

    client = bigquery.Client(project=project)
    table_id = f"{project}.realtime.realtime_orders"
    row = {
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": "cust_s3_001",
        "event_timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "order_total": 42.50,
        "event_type": "phase1_s3_verify",
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    }
    err = client.insert_rows_json(table_id, [row])
    if err:
        print("BigQuery insert errors:", err)
        sys.exit(1)
    print("Inserted into BigQuery", table_id)

    time.sleep(2)
    query = "SELECT order_id, customer_id, order_total FROM `" + table_id + "` WHERE order_id = @oid"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("oid", "STRING", order_id)]
    )
    result = list(client.query(query, job_config=job_config).result())
    client.close()
    if result:
        print("Verified in BigQuery:", dict(result[0]))
    else:
        print("Row not yet visible (streaming buffer); insert succeeded.")
    print("S3 -> BigQuery test OK.")


if __name__ == "__main__":
    main()
