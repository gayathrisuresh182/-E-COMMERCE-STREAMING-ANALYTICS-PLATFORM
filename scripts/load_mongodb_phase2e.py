#!/usr/bin/env python3
"""
Phase 2E: Load operational data to MongoDB.

Loads: products, reviews, experiments, experiment_assignments, dim_customers, dim_sellers.

Usage:
  python scripts/load_mongodb_phase2e.py [--uri URI] [--drop] [--load NAME]
  python scripts/load_mongodb_phase2e.py --load all   # load all (default)
  python scripts/load_mongodb_phase2e.py --load products --load reviews

Creates indexes after load. Uses MONGODB_URI from env or docs/.env.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

# Load env before importing loaders
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    load_dotenv(PROJECT_ROOT / "docs" / ".env")
except Exception:
    pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=os.environ.get("MONGODB_URI", "mongodb://localhost:27017"))
    ap.add_argument("--drop", action="store_true", help="Drop collections before load")
    ap.add_argument(
        "--load",
        action="append",
        choices=["products", "reviews", "experiments", "assignments", "dimensions", "all"],
        default=[],
        help="Which to load (default: all)",
    )
    args = ap.parse_args()

    loads = args.load if args.load else ["all"]
    if "all" in loads:
        loads = ["products", "reviews", "experiments", "assignments", "dimensions"]

    import pymongo
    from mongodb_loaders.config import DB_NAME, get_mongodb_uri
    from mongodb_loaders.load_products import load_products
    from mongodb_loaders.load_reviews import load_reviews
    from mongodb_loaders.load_experiments import load_experiments
    from mongodb_loaders.load_assignments import load_assignments
    from mongodb_loaders.load_dimensions import load_dim_customers, load_dim_sellers

    uri = args.uri or get_mongodb_uri()
    try:
        client = pymongo.MongoClient(uri)
        client.admin.command("ping")
    except Exception as e:
        print(f"Cannot connect to MongoDB: {e}")
        sys.exit(1)

    db = client[DB_NAME]
    results = {}

    if "products" in loads:
        try:
            n = load_products(db, drop=args.drop)
            results["products"] = n
            print(f"  products: {n} documents")
        except FileNotFoundError as e:
            print(f"  products: SKIP - {e}")
        except Exception as e:
            print(f"  products: FAILED - {e}")
            raise

    if "reviews" in loads:
        try:
            n = load_reviews(db, drop=args.drop)
            results["reviews"] = n
            print(f"  reviews: {n} documents")
        except FileNotFoundError as e:
            print(f"  reviews: SKIP - {e}")
        except Exception as e:
            print(f"  reviews: FAILED - {e}")
            raise

    if "experiments" in loads:
        try:
            n = load_experiments(db, drop=args.drop)
            results["experiments"] = n
            print(f"  experiments: {n} documents")
        except FileNotFoundError as e:
            print(f"  experiments: SKIP - {e}")
        except Exception as e:
            print(f"  experiments: FAILED - {e}")
            raise

    if "assignments" in loads:
        try:
            n = load_assignments(db, drop=args.drop)
            results["assignments"] = n
            print(f"  experiment_assignments: {n} documents")
        except FileNotFoundError as e:
            print(f"  experiment_assignments: SKIP - {e}")
        except Exception as e:
            print(f"  experiment_assignments: FAILED - {e}")
            raise

    if "dimensions" in loads:
        try:
            nc = load_dim_customers(db, drop=args.drop)
            ns = load_dim_sellers(db, drop=args.drop)
            results["dim_customers"] = nc
            results["dim_sellers"] = ns
            print(f"  dim_customers: {nc} documents")
            print(f"  dim_sellers: {ns} documents")
        except FileNotFoundError as e:
            print(f"  dimensions: SKIP - {e}")
        except Exception as e:
            print(f"  dimensions: FAILED - {e}")
            raise

    # Create indexes
    print("Creating indexes...")
    _create_indexes(db)

    print("Done. Verify: python scripts/validate_mongodb_phase2e.py")


def _create_indexes(db) -> None:
    """Create Phase 2E indexes."""
    idx = [
        ("products", [("product_id", 1)], {"unique": True}),
        ("products", [("category", 1)], {}),
        ("products", [("attributes.weight_g", 1)], {}),
        ("reviews", [("review_id", 1)], {"unique": True}),
        ("reviews", [("order_id", 1), ("product_id", 1)], {}),
        ("reviews", [("rating", 1)], {}),
        ("reviews", [("timestamps.created_at", 1)], {}),
        ("experiments", [("experiment_id", 1)], {"unique": True}),
        ("experiment_assignments", [("customer_id", 1), ("experiment_id", 1)], {"unique": True}),
        ("experiment_assignments", [("experiment_id", 1)], {}),
        ("dim_customers", [("customer_id", 1)], {"unique": True}),
        ("dim_customers", [("state", 1)], {}),
        ("dim_sellers", [("seller_id", 1)], {"unique": True}),
        ("dim_sellers", [("state", 1)], {}),
    ]
    for coll_name, keys, opts in idx:
        try:
            db[coll_name].create_index(keys, **opts)
        except Exception as e:
            print(f"    {coll_name}: index exists or error - {e}")


if __name__ == "__main__":
    main()
