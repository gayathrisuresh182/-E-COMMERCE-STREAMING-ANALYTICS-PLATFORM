"""
Phase 3G – Dagster assets for real-time stream data.

Architecture (Hybrid Sensor + Micro-Batch):
  Kafka consumers run as standalone processes, writing to MongoDB.
  Dagster assets **snapshot** MongoDB collections on a schedule or
  when triggered by sensors, producing DataFrames for downstream use.

This mirrors real production patterns: streaming infra (Flink, Spark
Streaming, standalone consumers) is decoupled from the orchestration
layer (Dagster / Airflow), which observes, validates, and tracks lineage.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
import pymongo
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset,
    asset_check,
)

from ecommerce_analytics.consumers.config import MONGO_DB, MONGO_URI

LOG = logging.getLogger("realtime_assets")


def _mongo_collection(collection: str):
    """Return a pymongo Collection handle (short-lived per materialization)."""
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return client, client[MONGO_DB][collection]


# ── Asset 1: realtime_orders ─────────────────────────────────────────

@asset(
    group_name="realtime",
    description=(
        "Snapshot of real-time orders written by the Order Stream Processor. "
        "Reads from MongoDB fct_orders_realtime collection."
    ),
    compute_kind="mongodb",
    metadata={"schema": "marts", "table": "fct_orders_realtime", "freshness_target_minutes": 30},
)
def realtime_orders(context) -> pd.DataFrame:
    client, coll = _mongo_collection("fct_orders_realtime")
    try:
        cursor = coll.find({}, {"_id": 0}).sort("processed_at", -1)
        rows = list(cursor)
        df = pd.DataFrame(rows) if rows else pd.DataFrame()

        context.add_output_metadata({
            "row_count": len(df),
            "source_collection": "fct_orders_realtime",
            "snapshot_ts": datetime.now(timezone.utc).isoformat(),
            "unique_orders": int(df["order_id"].nunique()) if "order_id" in df.columns else 0,
        })
        LOG.info("realtime_orders: %d rows snapshotted", len(df))
        return df
    finally:
        client.close()


# ── Asset 2: realtime_metrics_5min ───────────────────────────────────

@asset(
    group_name="realtime",
    description=(
        "5-minute windowed business metrics written by the Metrics Aggregator. "
        "Reads from MongoDB realtime_metrics collection."
    ),
    compute_kind="mongodb",
    metadata={"schema": "marts", "table": "realtime_metrics", "freshness_target_minutes": 15},
)
def realtime_metrics_5min(context) -> pd.DataFrame:
    client, coll = _mongo_collection("realtime_metrics")
    try:
        cursor = coll.find({}, {"_id": 0}).sort("window_start", -1)
        rows = list(cursor)
        df = pd.DataFrame(rows) if rows else pd.DataFrame()

        n_windows = int(df["window_start"].nunique()) if "window_start" in df.columns else 0
        context.add_output_metadata({
            "row_count": len(df),
            "unique_windows": n_windows,
            "source_collection": "realtime_metrics",
            "snapshot_ts": datetime.now(timezone.utc).isoformat(),
        })
        LOG.info("realtime_metrics_5min: %d rows, %d windows", len(df), n_windows)
        return df
    finally:
        client.close()


# ── Asset 3: realtime_experiment_results ─────────────────────────────

@asset(
    group_name="realtime",
    description=(
        "Live A/B test metrics written by the Experiment Tracker. "
        "Reads from MongoDB experiments_realtime collection."
    ),
    compute_kind="mongodb",
    metadata={"schema": "marts", "table": "experiments_realtime", "freshness_target_minutes": 30},
)
def realtime_experiment_results(context) -> pd.DataFrame:
    client, coll = _mongo_collection("experiments_realtime")
    try:
        cursor = coll.find({}, {"_id": 0})
        rows = list(cursor)
        df = pd.DataFrame(rows) if rows else pd.DataFrame()

        n_experiments = int(df["experiment_id"].nunique()) if "experiment_id" in df.columns else 0
        context.add_output_metadata({
            "row_count": len(df),
            "unique_experiments": n_experiments,
            "source_collection": "experiments_realtime",
            "snapshot_ts": datetime.now(timezone.utc).isoformat(),
        })
        LOG.info("realtime_experiment_results: %d rows, %d experiments", len(df), n_experiments)
        return df
    finally:
        client.close()


# ── Asset Checks ─────────────────────────────────────────────────────

@asset_check(asset=realtime_orders, description="Verify realtime_orders has rows")
def check_realtime_orders_not_empty(realtime_orders: pd.DataFrame) -> AssetCheckResult:
    n = len(realtime_orders)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"row_count": n},
        description=f"realtime_orders has {n} rows" if n else "Collection is empty — consumers may not be running",
    )


@asset_check(asset=realtime_orders, description="Check order totals are non-negative")
def check_realtime_orders_positive_totals(realtime_orders: pd.DataFrame) -> AssetCheckResult:
    if realtime_orders.empty or "order_total" not in realtime_orders.columns:
        return AssetCheckResult(passed=True, metadata={"skipped": True})
    neg = int((realtime_orders["order_total"].astype(float) < 0).sum())
    return AssetCheckResult(
        passed=neg == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"negative_totals": neg, "total_rows": len(realtime_orders)},
    )


@asset_check(
    asset=realtime_orders,
    description="Verify MongoDB fct_orders_realtime count matches snapshot",
)
def check_realtime_orders_consistency(realtime_orders: pd.DataFrame) -> AssetCheckResult:
    client, coll = _mongo_collection("fct_orders_realtime")
    try:
        mongo_count = coll.count_documents({})
    finally:
        client.close()
    df_count = len(realtime_orders)
    diff_pct = abs(mongo_count - df_count) / max(mongo_count, 1) * 100
    return AssetCheckResult(
        passed=diff_pct <= 5,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "mongo_count": mongo_count,
            "snapshot_count": df_count,
            "diff_pct": round(diff_pct, 2),
        },
        description=f"MongoDB={mongo_count} vs snapshot={df_count} (diff {diff_pct:.1f}%)",
    )


@asset_check(asset=realtime_metrics_5min, description="Verify metrics collection has data")
def check_realtime_metrics_not_empty(realtime_metrics_5min: pd.DataFrame) -> AssetCheckResult:
    n = len(realtime_metrics_5min)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"row_count": n},
    )


@asset_check(
    asset=realtime_experiment_results,
    description="Conversion rates should be between 0–100%",
)
def check_experiment_rates_valid(realtime_experiment_results: pd.DataFrame) -> AssetCheckResult:
    df = realtime_experiment_results
    if df.empty or "conversion_rate" not in df.columns:
        return AssetCheckResult(passed=True, metadata={"skipped": True})
    invalid = int(((df["conversion_rate"] < 0) | (df["conversion_rate"] > 100)).sum())
    return AssetCheckResult(
        passed=invalid == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"invalid_rates": invalid, "total_rows": len(df)},
    )


# ── Collect for registration ─────────────────────────────────────────

REALTIME_ASSETS = [
    realtime_orders,
    realtime_metrics_5min,
    realtime_experiment_results,
]

REALTIME_ASSET_CHECKS = [
    check_realtime_orders_not_empty,
    check_realtime_orders_positive_totals,
    check_realtime_orders_consistency,
    check_realtime_metrics_not_empty,
    check_experiment_rates_valid,
]
