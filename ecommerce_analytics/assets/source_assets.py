"""
Phase 3A: Dagster source assets for Olist historical data.

Source assets represent external data (S3 CSV files). They load on materialization
and provide lineage for downstream staging/marts. No transformations—load as-is.

S3 path: s3://{bucket}/bronze/olist_historical/{table}/olist_{table}_dataset.csv
Local fallback: raw/olist/olist_{table}_dataset.csv (when S3_BUCKET not set)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import asset, get_dagster_logger

logger = get_dagster_logger()

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"

# Table name -> (S3 prefix, local filename)
OLIST_SOURCES = {
    "orders": ("bronze/olist_historical/orders/", "olist_orders_dataset.csv"),
    "customers": ("bronze/olist_historical/customers/", "olist_customers_dataset.csv"),
    "products": ("bronze/olist_historical/products/", "olist_products_dataset.csv"),
    "order_items": ("bronze/olist_historical/order_items/", "olist_order_items_dataset.csv"),
    "order_payments": ("bronze/olist_historical/order_payments/", "olist_order_payments_dataset.csv"),
    "order_reviews": ("bronze/olist_historical/order_reviews/", "olist_order_reviews_dataset.csv"),
    "sellers": ("bronze/olist_historical/sellers/", "olist_sellers_dataset.csv"),
    "geolocation": ("bronze/olist_historical/geolocation/", "olist_geolocation_dataset.csv"),
}


def _load_csv(context, table: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load CSV from S3 or local. Returns (DataFrame, metadata dict)."""
    s3_prefix, filename = OLIST_SOURCES[table]
    bucket = os.environ.get("S3_BUCKET")
    local_path = RAW_OLIST / filename

    if bucket:
        try:
            import boto3
            s3 = boto3.client("s3")
            key = f"{s3_prefix}{filename}"
            obj = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(obj["Body"])
            # Metadata from S3
            head = s3.head_object(Bucket=bucket, Key=key)
            metadata = {
                "source": f"s3://{bucket}/{key}",
                "file_size_bytes": head.get("ContentLength", 0),
                "last_modified": str(head.get("LastModified", "")),
            }
        except Exception as e:
            logger.warning("S3 load failed (%s), falling back to local: %s", table, e)
            if local_path.exists():
                df = pd.read_csv(local_path)
                metadata = {"source": str(local_path), "file_size_bytes": local_path.stat().st_size}
            else:
                raise FileNotFoundError(f"Neither S3 nor local file found for {table}") from e
    else:
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}. Set S3_BUCKET or run download_olist.py")
        df = pd.read_csv(local_path)
        metadata = {"source": str(local_path), "file_size_bytes": local_path.stat().st_size}

    metadata["rows"] = len(df)
    metadata["columns"] = len(df.columns)
    metadata["column_names"] = list(df.columns)
    metadata["dtypes"] = {k: str(v) for k, v in df.dtypes.items()}

    return df, metadata


def _make_source_asset(table: str, description: str):
    """Factory for source asset definitions."""

    @asset(
        name=f"raw_{table}",
        description=description,
        compute_kind="pandas",
    )
    def _asset(context) -> pd.DataFrame:
        df, meta = _load_csv(context, table)
        context.add_output_metadata({
            "rows": meta["rows"],
            "columns": meta["columns"],
            "column_names": meta["column_names"],
            "source": meta["source"],
            "file_size_bytes": meta.get("file_size_bytes"),
            "last_modified": meta.get("last_modified", ""),
        })
        return df

    return _asset


# Define all 8 source assets
raw_orders = _make_source_asset(
    "orders",
    "Raw orders from Olist CSV (S3 bronze or local). s3://bucket/bronze/olist_historical/orders/",
)
raw_customers = _make_source_asset(
    "customers",
    "Raw customers from Olist CSV. s3://bucket/bronze/olist_historical/customers/",
)
raw_products = _make_source_asset(
    "products",
    "Raw products from Olist CSV. s3://bucket/bronze/olist_historical/products/",
)
raw_order_items = _make_source_asset(
    "order_items",
    "Raw order items from Olist CSV. s3://bucket/bronze/olist_historical/order_items/",
)
raw_order_payments = _make_source_asset(
    "order_payments",
    "Raw order payments from Olist CSV. s3://bucket/bronze/olist_historical/order_payments/",
)
raw_order_reviews = _make_source_asset(
    "order_reviews",
    "Raw order reviews from Olist CSV. s3://bucket/bronze/olist_historical/order_reviews/",
)
raw_sellers = _make_source_asset(
    "sellers",
    "Raw sellers from Olist CSV. s3://bucket/bronze/olist_historical/sellers/",
)
raw_geolocation = _make_source_asset(
    "geolocation",
    "Raw geolocation from Olist CSV. s3://bucket/bronze/olist_historical/geolocation/",
)

SOURCE_ASSETS = [
    raw_orders,
    raw_customers,
    raw_products,
    raw_order_items,
    raw_order_payments,
    raw_order_reviews,
    raw_sellers,
    raw_geolocation,
]
