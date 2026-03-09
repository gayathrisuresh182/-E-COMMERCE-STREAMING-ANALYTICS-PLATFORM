#!/usr/bin/env python3
"""
Phase 1C: Create MongoDB collections and indexes.
Usage: python scripts/mongodb_create_collections.py [--uri mongodb://localhost:27017]
"""
from __future__ import annotations

import argparse
import os
import sys

import pymongo

DB = "ecommerce_streaming"

# (keys, opts) - keys: str or list of (field, direction)
INDEXES = {
    "products": [
        ([("product_id", 1)], {"unique": True}),
        ([("product_category_name", 1)], {}),
    ],
    "reviews": [
        ([("review_id", 1)], {"unique": True}),
        ([("order_id", 1)], {}),
        ([("review_comment_message", "text"), ("review_comment_title", "text")], {}),
    ],
    "orders": [
        ([("order_id", 1)], {"unique": True}),
        ([("customer_id", 1)], {}),
        ([("order_status", 1)], {}),
    ],
    "user_events": [
        ([("event_id", 1)], {"unique": True}),
        ([("user_id", 1), ("timestamp", -1)], {}),
        ([("session_id", 1), ("timestamp", -1)], {}),
    ],
    "experiments": [
        ([("experiment_id", 1)], {"unique": True}),
    ],
    "experiment_assignments": [
        ([("user_id", 1), ("experiment_id", 1)], {"unique": True}),
        ([("user_id", 1)], {}),
        ([("experiment_id", 1)], {}),
    ],
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=os.environ.get("MONGODB_URI", "mongodb://localhost:27017"))
    args = ap.parse_args()

    try:
        client = pymongo.MongoClient(args.uri)
        client.admin.command("ping")
    except Exception as e:
        print(f"Cannot connect to MongoDB: {e}")
        print("Is MongoDB running? (docker-compose up -d)")
        sys.exit(1)

    db = client[DB]

    for coll_name, index_specs in INDEXES.items():
        coll = db[coll_name]
        for keys, opts in index_specs:
            coll.create_index(keys, **opts)
        print(f"  {coll_name}: {len(index_specs)} indexes")

    print("Done. Collections and indexes created.")


if __name__ == "__main__":
    main()
