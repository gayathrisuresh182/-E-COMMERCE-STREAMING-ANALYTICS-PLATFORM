#!/usr/bin/env python3
"""
Phase 1B: Create S3 bucket with data lake structure (bronze/silver/gold).
- Enable versioning
- Enable SSE-S3 encryption
- Set lifecycle rules (bronze retain, silver→Glacier 90d, gold expire 90d)
- Block public access

Usage:
  python scripts/s3_setup_bucket.py
  python scripts/s3_setup_bucket.py --bucket ecommerce-streaming-analytics-myid --region us-east-1

Requires: pip install boto3, AWS credentials configured
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

DEFAULT_REGION = "us-east-1"
BUCKET_PREFIX = "ecommerce-streaming-analytics"

# Folder prefixes (folders are created implicitly when objects are uploaded)
BRONZE_PREFIXES = [
    "bronze/olist_historical/orders/",
    "bronze/olist_historical/order_items/",
    "bronze/olist_historical/order_payments/",
    "bronze/olist_historical/order_reviews/",
    "bronze/olist_historical/products/",
    "bronze/olist_historical/customers/",
    "bronze/olist_historical/sellers/",
    "bronze/olist_historical/geolocation/",
    "bronze/kafka_archive/",
    "bronze/experiments/",
]
SILVER_PREFIXES = ["silver/orders_cleaned/", "silver/events_processed/"]
GOLD_PREFIXES = ["gold/daily_metrics/", "gold/experiment_results/"]


def get_lifecycle_rules() -> list[dict]:
    return [
        {
            "ID": "BronzeRetainForever",
            "Status": "Enabled",
            "Filter": {"Prefix": "bronze/"},
            "NoncurrentVersionExpiration": {"NoncurrentDays": 90},
        },
        {
            "ID": "SilverToGlacier",
            "Status": "Enabled",
            "Filter": {"Prefix": "silver/"},
            "Transitions": [{"Days": 90, "StorageClass": "GLACIER"}],
            "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
        },
        {
            "ID": "GoldExpire90d",
            "Status": "Enabled",
            "Filter": {"Prefix": "gold/"},
            "Expiration": {"Days": 90},
            "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
        },
    ]


def main() -> None:
    ap = argparse.ArgumentParser(description="Create S3 data lake bucket")
    ap.add_argument("--bucket", default=None, help=f"Bucket name (default: {BUCKET_PREFIX}-<timestamp>)")
    ap.add_argument("--region", default=DEFAULT_REGION, help="AWS region")
    ap.add_argument("--skip-lifecycle", action="store_true", help="Skip lifecycle rules")
    args = ap.parse_args()

    bucket = args.bucket or f"{BUCKET_PREFIX}-{datetime.utcnow().strftime('%Y%m%d%H%M')}"
    region = args.region

    s3 = boto3.client("s3", region_name=region)

    # 1. Create bucket
    print(f"Creating bucket: {bucket} in {region}")
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print("  Created.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print("  Bucket already exists.")
        else:
            print(e)
            sys.exit(1)

    # 2. Block public access
    print("Blocking public access...")
    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    print("  Done.")

    # 3. Versioning
    print("Enabling versioning...")
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    print("  Done.")

    # 4. Default encryption (SSE-S3)
    print("Enabling SSE-S3 encryption...")
    try:
        s3.put_bucket_encryption(
            Bucket=bucket,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                        "BucketKeyEnabled": True,
                    }
                ]
            },
        )
        print("  Done.")
    except ClientError as e:
        print(f"  Warning: {e}")

    # 5. Lifecycle
    if not args.skip_lifecycle:
        print("Applying lifecycle rules...")
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={"Rules": get_lifecycle_rules()},
        )
        print("  Done.")

    # 6. Create folder placeholders (empty .placeholder objects)
    print("Creating folder structure...")
    prefixes = BRONZE_PREFIXES + SILVER_PREFIXES + GOLD_PREFIXES
    for prefix in prefixes:
        key = f"{prefix}.placeholder"
        s3.put_object(Bucket=bucket, Key=key, Body=b"", ServerSideEncryption="AES256")
    print(f"  {len(prefixes)} prefixes created.")

    print("\nBucket ready:", bucket)
    print("S3 URI: s3://" + bucket)


if __name__ == "__main__":
    main()
