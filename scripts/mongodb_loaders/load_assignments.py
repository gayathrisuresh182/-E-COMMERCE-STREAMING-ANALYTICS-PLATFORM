"""
Phase 2E LOAD 4: Experiment Assignments collection.
Input: output/experiment_assignments.csv from Phase 2B
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pymongo
from pymongo import ReplaceOne

from .config import BATCH_SIZE, OUTPUT_DIR

ASSIGNMENTS_CSV = OUTPUT_DIR / "experiment_assignments.csv"


def _assignment_hash(customer_id: str, experiment_id: str) -> str:
    """Deterministic hash for consistency verification."""
    key = f"{customer_id}|{experiment_id}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def transform_row(row: pd.Series) -> dict:
    """Transform CSV row to Phase 2E assignment document."""
    customer_id = str(row["customer_id"]).strip()
    experiment_id = str(row["experiment_id"]).strip()
    variant = str(row["variant"]).strip()
    assigned_at = str(row.get("assigned_at", "2024-02-01T00:00:00Z"))

    composite_id = f"{customer_id}|{experiment_id}"
    doc = {
        "_id": composite_id,
        "customer_id": customer_id,
        "experiment_id": experiment_id,
        "variant": variant,
        "assigned_at": assigned_at,
        "assignment_hash": _assignment_hash(customer_id, experiment_id),
    }
    return doc


def load_assignments(db: pymongo.database.Database, drop: bool = False) -> int:
    """Load experiment assignments into MongoDB. Returns count loaded."""
    if not ASSIGNMENTS_CSV.exists():
        raise FileNotFoundError(f"Missing {ASSIGNMENTS_CSV}. Run generate_experiment_assignments.py first.")

    coll = db["experiment_assignments"]
    if drop:
        coll.drop()

    df = pd.read_csv(ASSIGNMENTS_CSV)
    docs = [transform_row(row) for _, row in df.iterrows()]

    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i : i + BATCH_SIZE]
        ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in batch]
        coll.bulk_write(ops, ordered=False)
    return len(docs)
