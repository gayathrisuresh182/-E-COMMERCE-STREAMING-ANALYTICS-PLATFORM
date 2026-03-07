#!/usr/bin/env python3
"""
Phase 2B: Generate experiment assignments from Olist customers.

Assigns each customer to all 10 experiments using hash-based deterministic
randomization. Ensures ~50/50 control/treatment per experiment.

Input:
  - raw/olist/olist_customers_dataset.csv
  - docs/experiment_catalog.json

Output:
  - output/experiment_assignments.csv
  - MongoDB: experiment_assignments, experiments (metadata)

Usage:
  python scripts/generate_experiment_assignments.py [--uri mongodb://localhost:27017] [--skip-mongo] [--out path]

Loads MONGODB_URI from docs/.env if available (via python-dotenv).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"
DOCS = PROJECT_ROOT / "docs"
OUTPUT_DIR = PROJECT_ROOT / "output"
CATALOG_PATH = DOCS / "experiment_catalog.json"
CUSTOMERS_PATH = RAW_OLIST / "olist_customers_dataset.csv"
ASSIGNMENTS_CSV = OUTPUT_DIR / "experiment_assignments.csv"
DB = "ecommerce_streaming"
ASSIGNED_AT = "2024-02-01T00:00:00Z"


def _load_env() -> None:
    """Load .env so MONGODB_URI from docs/.env is available."""
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env")
        load_dotenv(DOCS / ".env")
    except Exception:
        pass


def _hash_assign(customer_id: str, experiment_id: str) -> int:
    """Hash(customer_id + experiment_id) % 2 -> 0=control, 1=treatment."""
    key = f"{customer_id}|{experiment_id}"
    h = int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16)
    return h % 2


def _slug(name: str) -> str:
    return name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").replace("/", "_")


def load_catalog(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("experiments", [])


def generate_assignments(customers_df: pd.DataFrame, experiments: list[dict]) -> pd.DataFrame:
    customers_df = customers_df.copy()
    customers_df["customer_id"] = customers_df["customer_id"].astype(str).str.strip()
    customers_df["customer_state"] = customers_df["customer_state"].fillna("").astype(str).str.strip()

    dfs = []
    for exp in experiments:
        exp_id = exp["id"]
        slug = _slug(exp.get("name", exp_id))
        df_exp = customers_df[["customer_id", "customer_state"]].copy()
        df_exp["experiment_id"] = exp_id
        df_exp["experiment_name"] = slug
        df_exp["variant"] = df_exp["customer_id"].apply(
            lambda cid: "control" if _hash_assign(cid, exp_id) == 0 else "treatment"
        )
        df_exp["assigned_at"] = ASSIGNED_AT
        df_exp["assignment_method"] = "hash_based"
        dfs.append(df_exp)

    out = pd.concat(dfs, ignore_index=True)
    out["assignment_id"] = [str(uuid.uuid4()) for _ in range(len(out))]
    return out[["assignment_id", "customer_id", "customer_state", "experiment_id", "experiment_name", "variant", "assigned_at", "assignment_method"]]


def validate_assignments(df: pd.DataFrame, n_customers: int, experiments: list[dict]) -> dict:
    results = {"ok": True, "errors": [], "warnings": []}
    n_exp = len(experiments)
    expected_total = n_customers * n_exp
    actual_total = len(df)
    if actual_total != expected_total:
        results["errors"].append(f"Total {actual_total} != expected {expected_total}")
        results["ok"] = False
    dupes = df.duplicated(subset=["customer_id", "experiment_id"]).sum()
    if dupes > 0:
        results["errors"].append(f"Duplicates: {dupes}")
        results["ok"] = False
    for exp in experiments:
        sub = df[df["experiment_id"] == exp["id"]]
        control_pct = (sub["variant"] == "control").mean() * 100
        if control_pct < 45 or control_pct > 55:
            results["warnings"].append(f"{exp['id']}: control {control_pct:.1f}%")
            if control_pct < 40 or control_pct > 60:
                results["ok"] = False
    return results


def build_experiment_metadata(experiments: list[dict]) -> list[dict]:
    docs = []
    for exp in experiments:
        variants_raw = exp.get("variants", ["control", "treatment"])
        variants = [
            {"variant_id": "control", "name": variants_raw[0] if variants_raw else "Control", "allocation": 0.5},
            {"variant_id": "treatment", "name": variants_raw[1] if len(variants_raw) > 1 else "Treatment", "allocation": 0.5},
        ]
        guardrail = exp.get("guardrail_metrics", [])
        if isinstance(guardrail, str):
            guardrail = [guardrail]
        docs.append({
            "experiment_id": exp["id"],
            "experiment_name": _slug(exp.get("name", exp["id"])),
            "description": exp.get("name", exp["id"]),
            "start_date": "2024-02-01",
            "end_date": "2024-03-01",
            "status": "active",
            "variants": variants,
            "metrics": {
                "primary": exp.get("primary_metric", "conversion_rate"),
                "secondary": ["aov", "items_per_order", "revenue_per_user"],
                "guardrail": guardrail,
            },
            "mde_percent": exp.get("mde_percent"),
            "test_type": exp.get("test_type"),
        })
    return docs


def main() -> None:
    _load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=os.environ.get("MONGODB_URI", "mongodb://localhost:27017"))
    ap.add_argument("--skip-mongo", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    out_path = Path(args.out) if args.out else ASSIGNMENTS_CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not CUSTOMERS_PATH.exists():
        print(f"Not found: {CUSTOMERS_PATH}")
        sys.exit(1)
    if not CATALOG_PATH.exists():
        print(f"Not found: {CATALOG_PATH}")
        sys.exit(1)

    customers_df = pd.read_csv(CUSTOMERS_PATH).drop_duplicates(subset=["customer_id"], keep="first")
    n_customers = len(customers_df)
    print(f"Loaded {n_customers:,} customers")

    experiments = load_catalog(CATALOG_PATH)
    print(f"Loaded {len(experiments)} experiments")

    df = generate_assignments(customers_df, experiments)
    print(f"Generated {len(df):,} assignments")

    val = validate_assignments(df, n_customers, experiments)
    for e in val["errors"]:
        print(f"  [ERROR] {e}")
    for w in val["warnings"]:
        print(f"  [WARN] {w}")
    if val["ok"]:
        print("  Validation: OK")
    else:
        print("  Validation: FAILED")
        if val["errors"]:
            sys.exit(1)

    print("\nVariant split per experiment:")
    for exp_id in df["experiment_id"].unique():
        sub = df[df["experiment_id"] == exp_id]
        c = (sub["variant"] == "control").sum()
        t = (sub["variant"] == "treatment").sum()
        print(f"  {exp_id}: control {c:,} ({c/len(sub)*100:.1f}%), treatment {t:,} ({t/len(sub)*100:.1f}%)")

    df.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}")

    if not args.skip_mongo:
        print("  Connecting to MongoDB...", flush=True)
        try:
            import pymongo
            client = pymongo.MongoClient(args.uri)
            client.admin.command("ping")
        except Exception as e:
            print(f"MongoDB connect failed: {e}")
            sys.exit(1)

        db = client[DB]
        now = datetime.now(timezone.utc)
        coll_assign = db["experiment_assignments"]
        coll_assign.drop()
        print("  Preparing records (to_dict)...", flush=True)
        df["created_at"] = now
        records = df.to_dict("records")
        print("  Loading to MongoDB (batches of 20k)...", flush=True)
        BATCH_SIZE = 20_000
        n_inserted = 0
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            coll_assign.insert_many(batch, ordered=False)
            n_inserted += len(batch)
            print(f"  Inserted {n_inserted:,} / {len(records):,}", flush=True)
        print(f"  MongoDB experiment_assignments: {coll_assign.count_documents({}):,} docs")
        coll_assign.create_index("customer_id")
        coll_assign.create_index("experiment_id")
        coll_assign.create_index([("customer_id", 1), ("experiment_id", 1)], unique=True)
        print("  Indexes created.")

        meta = build_experiment_metadata(experiments)
        coll_exp = db["experiments"]
        coll_exp.drop()
        for m in meta:
            m["created_at"] = now
        coll_exp.insert_many(meta)
        print(f"  MongoDB experiments (metadata): {coll_exp.count_documents({}):,} docs")
        print("Done.")
    else:
        print("Skipped MongoDB (--skip-mongo)")


if __name__ == "__main__":
    main()
