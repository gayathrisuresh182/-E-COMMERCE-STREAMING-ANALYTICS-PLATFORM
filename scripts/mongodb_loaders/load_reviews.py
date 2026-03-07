"""
Phase 2E LOAD 2: Reviews collection.
Input: olist_order_reviews_dataset.csv + olist_order_items_dataset.csv (for product_id lookup)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pymongo
from pymongo import ReplaceOne

from .config import BATCH_SIZE, RAW_OLIST


def _parse_date(s) -> str | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    try:
        dt = pd.to_datetime(s)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def _build_order_to_product() -> dict[str, str]:
    """Build order_id -> product_id (first product in order)."""
    items_path = RAW_OLIST / "olist_order_items_dataset.csv"
    if not items_path.exists():
        return {}
    df = pd.read_csv(items_path)
    # First product per order
    first = df.drop_duplicates(subset=["order_id"], keep="first")
    return dict(zip(first["order_id"].astype(str), first["product_id"].astype(str)))


def transform_row(row: pd.Series, order_to_product: dict[str, str]) -> dict:
    """Transform CSV row to Phase 2E review document."""
    review_id = str(row["review_id"]).strip()
    order_id = str(row["order_id"]).strip()
    product_id = order_to_product.get(order_id, order_id)

    doc = {
        "_id": review_id,
        "review_id": review_id,
        "order_id": order_id,
        "product_id": product_id,
        "rating": int(row["review_score"]) if pd.notna(row.get("review_score")) else None,
        "comment": {
            "title": str(row["review_comment_title"]).strip() if pd.notna(row.get("review_comment_title")) and str(row["review_comment_title"]).strip() else None,
            "message": str(row["review_comment_message"]).strip() if pd.notna(row.get("review_comment_message")) and str(row["review_comment_message"]).strip() else None,
            "language": "pt",
        },
        "timestamps": {
            "created_at": _parse_date(row.get("review_creation_date")),
            "answered_at": _parse_date(row.get("review_answer_timestamp")),
        },
        "metadata": {
            "helpful_votes": 0,
            "verified_purchase": True,
        },
    }
    return doc


def load_reviews(db: pymongo.database.Database, drop: bool = False) -> int:
    """Load reviews into MongoDB. Returns count loaded."""
    reviews_path = RAW_OLIST / "olist_order_reviews_dataset.csv"
    if not reviews_path.exists():
        raise FileNotFoundError(f"Missing {reviews_path}")

    order_to_product = _build_order_to_product()

    coll = db["reviews"]
    if drop:
        coll.drop()

    df = pd.read_csv(reviews_path).drop_duplicates(subset=["review_id"], keep="first")
    docs = [transform_row(row, order_to_product) for _, row in df.iterrows()]

    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i : i + BATCH_SIZE]
        ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in batch]
        coll.bulk_write(ops, ordered=False)
    return len(docs)
