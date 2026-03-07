#!/usr/bin/env python3
"""
Phase 1B: Upload Olist CSVs from raw/olist/ to S3 bronze/olist_historical/<table>/.
Adds metadata: source, upload_date.

Usage:
  python scripts/s3_upload_olist_bronze.py --bucket ecommerce-streaming-analytics-myid
  python scripts/s3_upload_olist_bronze.py  # uses S3_BUCKET env or prompts

Requires: pip install boto3, AWS credentials, raw/olist/ populated
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"

# CSV file -> S3 prefix (table folder)
CSV_TO_PREFIX = {
    "olist_orders_dataset.csv": "bronze/olist_historical/orders/",
    "olist_order_items_dataset.csv": "bronze/olist_historical/order_items/",
    "olist_order_payments_dataset.csv": "bronze/olist_historical/order_payments/",
    "olist_order_reviews_dataset.csv": "bronze/olist_historical/order_reviews/",
    "olist_products_dataset.csv": "bronze/olist_historical/products/",
    "olist_customers_dataset.csv": "bronze/olist_historical/customers/",
    "olist_sellers_dataset.csv": "bronze/olist_historical/sellers/",
    "olist_geolocation_dataset.csv": "bronze/olist_historical/geolocation/",
}


def main() -> None:
    ap = argparse.ArgumentParser(description="Upload Olist CSVs to S3 bronze layer")
    ap.add_argument("--bucket", default=os.environ.get("S3_BUCKET"), help="S3 bucket name")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"), help="AWS region")
    args = ap.parse_args()

    bucket = args.bucket
    if not bucket:
        bucket = input("S3 bucket name (e.g. ecommerce-streaming-analytics-xyz): ").strip()
    if not bucket:
        print("Bucket name required.")
        sys.exit(1)

    if not RAW_OLIST.exists():
        print(f"Not found: {RAW_OLIST}")
        print("Run: python scripts/download_olist.py")
        sys.exit(1)

    s3 = boto3.client("s3", region_name=args.region)
    upload_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    uploaded = 0
    for csv_name, prefix in CSV_TO_PREFIX.items():
        path = RAW_OLIST / csv_name
        if not path.exists():
            print(f"  Skip (not found): {csv_name}")
            continue
        key = f"{prefix}{csv_name}"
        metadata = {
            "source": "kaggle-olist-brazilian-ecommerce",
            "upload_date": upload_date,
            "table": csv_name.replace(".csv", ""),
        }
        try:
            s3.upload_file(
                str(path),
                bucket,
                key,
                ExtraArgs={
                    "Metadata": metadata,
                    "ServerSideEncryption": "AES256",
                },
            )
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"  Uploaded: s3://{bucket}/{key} ({size_mb:.2f} MB)")
            uploaded += 1
        except ClientError as e:
            print(f"  Error {csv_name}: {e}")
            sys.exit(1)

    print(f"\nDone. {uploaded} files uploaded to bronze.")
    print("Verify: aws s3 ls s3://{}/bronze/olist_historical/ --recursive".format(bucket))


if __name__ == "__main__":
    main()
