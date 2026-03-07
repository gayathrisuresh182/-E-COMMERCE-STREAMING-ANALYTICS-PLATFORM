"""
Phase 2E LOAD 5: Dimension tables for consumer lookups.
- dim_customers: from olist_customers + geography (denormalized for fast lookup)
- dim_sellers: from olist_sellers
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pymongo
from pymongo import ReplaceOne

from .config import BATCH_SIZE, RAW_OLIST


def _load_geo_lookup() -> dict[str, tuple[float, float]]:
    """Build zip_code_prefix -> (lat, lng) from first occurrence."""
    geo_path = RAW_OLIST / "olist_geolocation_dataset.csv"
    if not geo_path.exists():
        return {}
    df = pd.read_csv(geo_path).drop_duplicates(subset=["geolocation_zip_code_prefix"], keep="first")
    return dict(
        zip(
            df["geolocation_zip_code_prefix"].astype(str),
            zip(df["geolocation_lat"], df["geolocation_lng"]),
        )
    )


def load_dim_customers(db: pymongo.database.Database, drop: bool = False) -> int:
    """Load dim_customers. Returns count loaded."""
    customers_path = RAW_OLIST / "olist_customers_dataset.csv"
    if not customers_path.exists():
        raise FileNotFoundError(f"Missing {customers_path}")

    geo = _load_geo_lookup()

    coll = db["dim_customers"]
    if drop:
        coll.drop()

    df = pd.read_csv(customers_path).drop_duplicates(subset=["customer_id"], keep="first")
    docs = []
    for _, row in df.iterrows():
        cid = str(row["customer_id"]).strip()
        zip_prefix = str(row.get("customer_zip_code_prefix", "")).strip()
        lat_lng = geo.get(zip_prefix, (None, None))
        docs.append({
            "_id": cid,
            "customer_id": cid,
            "customer_unique_id": str(row.get("customer_unique_id", "")).strip(),
            "zip_code_prefix": zip_prefix,
            "city": str(row.get("customer_city", "")).strip() if pd.notna(row.get("customer_city")) else None,
            "state": str(row.get("customer_state", "")).strip() if pd.notna(row.get("customer_state")) else None,
            "latitude": lat_lng[0],
            "longitude": lat_lng[1],
        })

    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i : i + BATCH_SIZE]
        ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in batch]
        coll.bulk_write(ops, ordered=False)
    return len(docs)


def load_dim_sellers(db: pymongo.database.Database, drop: bool = False) -> int:
    """Load dim_sellers. Returns count loaded."""
    sellers_path = RAW_OLIST / "olist_sellers_dataset.csv"
    if not sellers_path.exists():
        raise FileNotFoundError(f"Missing {sellers_path}")

    coll = db["dim_sellers"]
    if drop:
        coll.drop()

    df = pd.read_csv(sellers_path).drop_duplicates(subset=["seller_id"], keep="first")
    docs = []
    for _, row in df.iterrows():
        sid = str(row["seller_id"]).strip()
        docs.append({
            "_id": sid,
            "seller_id": sid,
            "zip_code_prefix": str(row.get("seller_zip_code_prefix", "")).strip(),
            "city": str(row.get("seller_city", "")).strip() if pd.notna(row.get("seller_city")) else None,
            "state": str(row.get("seller_state", "")).strip() if pd.notna(row.get("seller_state")) else None,
        })

    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i : i + BATCH_SIZE]
        ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in batch]
        coll.bulk_write(ops, ordered=False)
    return len(docs)


def load_dimensions(db: pymongo.database.Database, drop: bool = False) -> tuple[int, int]:
    """Load dim_customers and dim_sellers. Returns (customers_count, sellers_count)."""
    n_cust = load_dim_customers(db, drop)
    n_sell = load_dim_sellers(db, drop)
    return n_cust, n_sell
