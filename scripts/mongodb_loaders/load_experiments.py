"""
Phase 2E LOAD 3: Experiments collection.
Input: docs/experiment_catalog.json (Phase 2A)
"""
from __future__ import annotations

import json
from pathlib import Path

import pymongo
from pymongo import ReplaceOne

from .config import DOCS

CATALOG_PATH = DOCS / "experiment_catalog.json"


def _parse_variants(variant_strings: list[str], split: str = "50/50") -> list[dict]:
    """Parse variant strings like 'control_50', 'treatment_35' into variant docs."""
    variants = []
    parts = split.split("/")
    default_pct = 50 if len(parts) < 2 else int(parts[0].strip())
    for i, vs in enumerate(variant_strings or ["control", "treatment"]):
        pct = default_pct if i == 0 else (100 - default_pct)
        if "_" in str(vs):
            tok = str(vs).split("_", 1)
            vid = tok[0]
            if len(tok) > 1 and tok[1].isdigit():
                pct = int(tok[1])
        else:
            vid = str(vs).lower()
        params = {}
        if "50" in str(vs) or "35" in str(vs):
            for x in str(vs).split("_"):
                if x.isdigit():
                    params["threshold"] = int(x)
                    break
        variants.append({
            "variant_id": vid,
            "name": vs.replace("_", " ").title(),
            "description": f"Variant {vid}",
            "allocation_percent": pct,
            "parameters": params or {},
        })
    return variants


def transform_experiment(exp: dict) -> dict:
    """Transform catalog experiment to Phase 2E document."""
    exp_id = exp.get("id", "unknown")
    variants_raw = exp.get("variants", ["control", "treatment"])
    split = exp.get("split", "50/50")
    sample_per = exp.get("sample_per_variant", 2500)

    doc = {
        "_id": exp_id,
        "experiment_id": exp_id,
        "name": exp.get("name", exp_id),
        "description": exp.get("description", f"Experiment {exp.get('name', exp_id)}"),
        "hypothesis": f"Test impact on {exp.get('primary_metric', 'metric')}",
        "status": "active",
        "dates": {
            "start_date": "2024-02-01",
            "end_date": "2024-03-01",
            "created_at": "2024-01-15",
        },
        "variants": _parse_variants(variants_raw, split),
        "metrics": {
            "primary": exp.get("primary_metric", "conversion_rate"),
            "secondary": ["aov", "items_per_order"],
            "guardrail": exp.get("guardrail_metrics", []),
        },
        "sample_size": {
            "required_per_variant": sample_per,
            "achieved_control": 0,
            "achieved_treatment": 0,
        },
    }
    return doc


def load_experiments(db: pymongo.database.Database, drop: bool = False) -> int:
    """Load experiments into MongoDB. Returns count loaded."""
    if not CATALOG_PATH.exists():
        raise FileNotFoundError(f"Missing {CATALOG_PATH}")

    with open(CATALOG_PATH, encoding="utf-8") as f:
        data = json.load(f)
    experiments = data.get("experiments", [])

    coll = db["experiments"]
    if drop:
        coll.drop()

    docs = [transform_experiment(exp) for exp in experiments]

    for doc in docs:
        coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)
    return len(docs)
