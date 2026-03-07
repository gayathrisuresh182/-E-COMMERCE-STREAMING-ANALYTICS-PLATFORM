#!/usr/bin/env python3
"""
Phase 2E: Validate MongoDB load and run performance tests.

Usage:
  python scripts/validate_mongodb_phase2e.py [--uri URI]

- Count documents per collection
- Sample query each collection
- Check indexes (db.collection.getIndexes())
- Performance: query by product_id (<10ms target), aggregate count by rating
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    load_dotenv(PROJECT_ROOT / "docs" / ".env")
except Exception:
    pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=os.environ.get("MONGODB_URI", "mongodb://localhost:27017"))
    args = ap.parse_args()

    import pymongo

    try:
        client = pymongo.MongoClient(args.uri)
        client.admin.command("ping")
    except Exception as e:
        print(f"Cannot connect: {e}")
        sys.exit(1)

    db = client["ecommerce_streaming"]

    print("=== Document counts ===")
    for coll_name in ["products", "reviews", "experiments", "experiment_assignments", "dim_customers", "dim_sellers"]:
        coll = db[coll_name]
        n = coll.count_documents({})
        print(f"  {coll_name}: {n}")

    print("\n=== Sample documents ===")
    for coll_name in ["products", "reviews", "experiments"]:
        coll = db[coll_name]
        doc = coll.find_one()
        if doc:
            # Truncate for display
            keys = list(doc.keys())[:6]
            sample = {k: doc[k] for k in keys}
            print(f"  {coll_name}: {sample}")
        else:
            print(f"  {coll_name}: (empty)")

    print("\n=== Indexes ===")
    for coll_name in ["products", "reviews", "experiment_assignments"]:
        coll = db[coll_name]
        idxs = list(coll.list_indexes())
        names = [idx.get("name", "?") for idx in idxs]
        print(f"  {coll_name}: {names}")

    print("\n=== Performance tests ===")
    # 1. Query by product_id
    products = db["products"]
    first = products.find_one()
    if first and "product_id" in first:
        pid = first["product_id"]
        t0 = time.perf_counter()
        for _ in range(100):
            products.find_one({"product_id": pid})
        elapsed_ms = (time.perf_counter() - t0) * 10  # 100 queries -> ms per query
        print(f"  product_id lookup (100x): {elapsed_ms:.2f} ms/query (target <10ms)")

    # 2. Aggregate: count reviews by rating
    reviews = db["reviews"]
    t0 = time.perf_counter()
    agg = list(reviews.aggregate([{"$group": {"_id": "$rating", "count": {"$sum": 1}}}]))
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"  reviews by rating aggregation: {elapsed_ms:.1f} ms")
    for r in sorted(agg, key=lambda x: x["_id"] or 0):
        print(f"    rating {r['_id']}: {r['count']}")

    # 3. Explain to verify index use
    if first and "product_id" in first:
        explain = products.find({"product_id": first["product_id"]}).explain()
        stage = explain.get("queryPlanner", {}).get("winningPlan", {}).get("stage", "?")
        print(f"  product_id query plan: {stage}")

    print("\nValidation complete.")


if __name__ == "__main__":
    main()
