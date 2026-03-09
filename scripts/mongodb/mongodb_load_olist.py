#!/usr/bin/env python3
"""
Phase 1C: Load products and reviews from Olist CSVs into MongoDB.
Usage: python scripts/mongodb_load_olist.py [--uri mongodb://localhost:27017] [--drop]
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pymongo
from pymongo import ReplaceOne

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"
DB = "ecommerce_streaming"

# Portuguese category -> English (subset for demo; expand as needed)
CATEGORY_TRANSLATION = {
    "beleza_saude": "beauty_health",
    "informatica_acessorios": "computers_accessories",
    "perfumaria": "perfumery",
    "casa_conforto": "home_comfort",
    "esporte_lazer": "sports_leisure",
    "moveis_decoracao": "furniture_decoration",
    "utilidades_domesticas": "household_utilities",
    "brinquedos": "toys",
    "eletrodomesticos": "appliances",
    "automotivo": "automotive",
}


def parse_date(s: str | float):
    if pd.isna(s) or s == "":
        return None
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=os.environ.get("MONGODB_URI", "mongodb://localhost:27017"))
    ap.add_argument("--drop", action="store_true", help="Drop collections before load")
    args = ap.parse_args()

    if not RAW_OLIST.exists():
        print(f"Not found: {RAW_OLIST}")
        print("Run: python scripts/download_olist.py")
        sys.exit(1)

    try:
        client = pymongo.MongoClient(args.uri)
        client.admin.command("ping")
    except Exception as e:
        print(f"Cannot connect: {e}")
        sys.exit(1)

    db = client[DB]
    now = datetime.now(timezone.utc)

    # Products
    products_path = RAW_OLIST / "olist_products_dataset.csv"
    if not products_path.exists():
        print("Skip products: file not found")
    else:
        coll = db["products"]
        if args.drop:
            coll.drop()
        df = pd.read_csv(products_path).drop_duplicates(subset=["product_id"], keep="first")
        docs = []
        for _, row in df.iterrows():
            cat_pt = row.get("product_category_name")
            cat_en = CATEGORY_TRANSLATION.get(str(cat_pt), cat_pt) if pd.notna(cat_pt) else None
            docs.append({
                "product_id": str(row["product_id"]),
                "product_category_name": str(cat_pt) if pd.notna(cat_pt) else None,
                "product_category_name_english": cat_en,
                "product_name_length": int(row["product_name_length"]) if pd.notna(row.get("product_name_length")) else None,
                "product_description_length": int(row["product_description_length"]) if pd.notna(row.get("product_description_length")) else None,
                "product_photos_qty": int(row["product_photos_qty"]) if pd.notna(row.get("product_photos_qty")) else None,
                "product_weight_g": float(row["product_weight_g"]) if pd.notna(row.get("product_weight_g")) else None,
                "product_length_cm": float(row["product_length_cm"]) if pd.notna(row.get("product_length_cm")) else None,
                "product_height_cm": float(row["product_height_cm"]) if pd.notna(row.get("product_height_cm")) else None,
                "product_width_cm": float(row["product_width_cm"]) if pd.notna(row.get("product_width_cm")) else None,
                "created_at": now,
            })
        ops = [ReplaceOne({"product_id": d["product_id"]}, d, upsert=True) for d in docs]
        coll.bulk_write(ops, ordered=False)
        print(f"  products: {len(docs)} upserted")

    # Reviews
    reviews_path = RAW_OLIST / "olist_order_reviews_dataset.csv"
    if not reviews_path.exists():
        print("Skip reviews: file not found")
    else:
        coll = db["reviews"]
        if args.drop:
            coll.drop()
        df = pd.read_csv(reviews_path).drop_duplicates(subset=["review_id"], keep="first")
        docs = []
        for _, row in df.iterrows():
            docs.append({
                "review_id": str(row["review_id"]),
                "order_id": str(row["order_id"]),
                "review_score": int(row["review_score"]) if pd.notna(row.get("review_score")) else None,
                "review_comment_title": str(row["review_comment_title"]) if pd.notna(row.get("review_comment_title")) else None,
                "review_comment_message": str(row["review_comment_message"]) if pd.notna(row.get("review_comment_message")) else None,
                "review_creation_date": parse_date(row.get("review_creation_date")),
                "review_answer_timestamp": parse_date(row.get("review_answer_timestamp")),
                "created_at": now,
            })
        ops = [ReplaceOne({"review_id": d["review_id"]}, d, upsert=True) for d in docs]
        coll.bulk_write(ops, ordered=False)
        print(f"  reviews: {len(docs)} upserted")

    print("Done. Verify: mongosh --eval 'db.products.countDocuments()' ecommerce_streaming")


if __name__ == "__main__":
    main()
