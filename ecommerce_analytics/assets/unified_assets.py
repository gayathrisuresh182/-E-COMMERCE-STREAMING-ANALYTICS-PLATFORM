"""
Phase 3L – Unified Lambda assets.

Three serving-layer assets that combine batch (historical) and stream
(real-time) data, implementing the Lambda architecture's serving layer.

In production BigQuery these would be views (`marts.fct_orders_unified`,
`marts.daily_metrics_unified`, `marts.experiment_results_unified`) defined
in scripts/bigquery/04_create_views.sql. Here they are materialized as
Dagster assets so the lineage graph shows the full batch+stream merge.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset,
    asset_check,
)

LOG = logging.getLogger("unified_assets")


# ── Asset 1: unified_orders ──────────────────────────────────────────

@asset(
    group_name="unified",
    description=(
        "Lambda serving layer: combines batch fct_order_items (historical "
        "Olist orders) with realtime_orders (Kafka stream). Deduplicates "
        "by order_id, preferring the most recent record."
    ),
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "fct_orders_unified"},
)
def unified_orders(
    context,
    stg_orders: pd.DataFrame,
    realtime_orders: pd.DataFrame,
) -> pd.DataFrame:
    batch_cols = ["order_id", "order_status"]
    batch = pd.DataFrame()
    if not stg_orders.empty and "order_id" in stg_orders.columns:
        available = [c for c in batch_cols if c in stg_orders.columns]
        batch = stg_orders[available].copy()
        batch["_source"] = "batch"

    stream = pd.DataFrame()
    if not realtime_orders.empty and "order_id" in realtime_orders.columns:
        stream_cols = [c for c in ["order_id", "order_status"] if c in realtime_orders.columns]
        stream = realtime_orders[stream_cols].copy()
        stream["_source"] = "stream"

    if batch.empty and stream.empty:
        context.add_output_metadata({"row_count": 0, "batch": 0, "stream": 0})
        return pd.DataFrame()

    combined = pd.concat([batch, stream], ignore_index=True)

    combined["_rank"] = combined["_source"].map({"stream": 0, "batch": 1})
    combined = combined.sort_values("_rank").drop_duplicates(
        subset=["order_id"], keep="first"
    ).drop(columns=["_rank"])

    batch_count = int((combined["_source"] == "batch").sum())
    stream_count = int((combined["_source"] == "stream").sum())

    context.add_output_metadata({
        "row_count": len(combined),
        "batch_rows": batch_count,
        "stream_rows": stream_count,
        "deduplicated_at": datetime.now(timezone.utc).isoformat(),
    })
    LOG.info("unified_orders: %d rows (batch=%d stream=%d)",
             len(combined), batch_count, stream_count)
    return combined


# ── Asset 2: unified_daily_metrics ───────────────────────────────────

@asset(
    group_name="unified",
    description=(
        "Lambda serving layer: combines batch fct_daily_metrics (historical "
        "daily KPIs) with realtime_metrics_5min (windowed stream metrics). "
        "Batch provides day-level granularity; stream adds sub-day windows."
    ),
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "daily_metrics_unified"},
)
def unified_daily_metrics(
    context,
    fct_seller_performance: pd.DataFrame,
    realtime_metrics_5min: pd.DataFrame,
) -> pd.DataFrame:
    batch = pd.DataFrame()
    if not fct_seller_performance.empty:
        batch = fct_seller_performance.copy()
        batch["_source"] = "batch"
        batch["_grain"] = "seller_summary"

    stream = pd.DataFrame()
    if not realtime_metrics_5min.empty:
        stream = realtime_metrics_5min.copy()
        stream["_source"] = "stream"
        stream["_grain"] = "5min_window"

    combined = pd.concat([batch, stream], ignore_index=True)

    context.add_output_metadata({
        "row_count": len(combined),
        "batch_rows": len(batch),
        "stream_rows": len(stream),
        "unified_at": datetime.now(timezone.utc).isoformat(),
    })
    LOG.info("unified_daily_metrics: %d rows (batch=%d stream=%d)",
             len(combined), len(batch), len(stream))
    return combined


# ── Asset 3: unified_experiment_results ──────────────────────────────

@asset(
    group_name="unified",
    description=(
        "Lambda serving layer: combines batch fct_experiment_results "
        "(full historical A/B test analysis) with realtime_experiment_results "
        "(live streaming metrics). Deduplicates by experiment_id + variant, "
        "preferring stream for fresher metrics."
    ),
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "experiment_results_unified"},
)
def unified_experiment_results(
    context,
    fct_experiment_results: pd.DataFrame,
    realtime_experiment_results: pd.DataFrame,
) -> pd.DataFrame:
    batch = pd.DataFrame()
    if not fct_experiment_results.empty:
        batch = fct_experiment_results.copy()
        batch["_source"] = "batch"

    stream = pd.DataFrame()
    if not realtime_experiment_results.empty:
        stream = realtime_experiment_results.copy()
        stream["_source"] = "stream"

    if batch.empty and stream.empty:
        context.add_output_metadata({"row_count": 0})
        return pd.DataFrame()

    combined = pd.concat([batch, stream], ignore_index=True)

    dedup_cols = []
    for col in ["experiment_id", "variant"]:
        if col in combined.columns:
            dedup_cols.append(col)

    if dedup_cols:
        combined["_rank"] = combined["_source"].map({"stream": 0, "batch": 1})
        combined = combined.sort_values("_rank").drop_duplicates(
            subset=dedup_cols, keep="first"
        ).drop(columns=["_rank"])

    batch_count = int((combined["_source"] == "batch").sum())
    stream_count = int((combined["_source"] == "stream").sum())

    context.add_output_metadata({
        "row_count": len(combined),
        "batch_rows": batch_count,
        "stream_rows": stream_count,
        "unified_at": datetime.now(timezone.utc).isoformat(),
    })
    LOG.info("unified_experiment_results: %d rows (batch=%d stream=%d)",
             len(combined), batch_count, stream_count)
    return combined


# ── Asset Checks ─────────────────────────────────────────────────────

@asset_check(asset=unified_orders, description="Unified orders should have rows")
def check_unified_orders_not_empty(unified_orders: pd.DataFrame) -> AssetCheckResult:
    n = len(unified_orders)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"row_count": n},
    )


@asset_check(
    asset=unified_orders,
    description="No duplicate order_ids in unified view",
)
def check_unified_orders_no_duplicates(unified_orders: pd.DataFrame) -> AssetCheckResult:
    if unified_orders.empty or "order_id" not in unified_orders.columns:
        return AssetCheckResult(passed=True, metadata={"skipped": True})
    dupes = int(unified_orders["order_id"].duplicated().sum())
    return AssetCheckResult(
        passed=dupes == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_order_ids": dupes, "total_rows": len(unified_orders)},
    )


@asset_check(
    asset=unified_orders,
    description="Unified orders should include both batch and stream sources",
)
def check_unified_orders_has_both_sources(unified_orders: pd.DataFrame) -> AssetCheckResult:
    if unified_orders.empty or "_source" not in unified_orders.columns:
        return AssetCheckResult(passed=True, metadata={"skipped": True})
    sources = set(unified_orders["_source"].unique())
    return AssetCheckResult(
        passed="batch" in sources and "stream" in sources,
        severity=AssetCheckSeverity.WARN,
        metadata={"sources_present": sorted(sources)},
    )


@asset_check(
    asset=unified_experiment_results,
    description="Unified experiments should have rows",
)
def check_unified_experiments_not_empty(
    unified_experiment_results: pd.DataFrame,
) -> AssetCheckResult:
    n = len(unified_experiment_results)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"row_count": n},
    )


@asset_check(
    asset=unified_daily_metrics,
    description="Unified metrics should have rows",
)
def check_unified_metrics_not_empty(
    unified_daily_metrics: pd.DataFrame,
) -> AssetCheckResult:
    n = len(unified_daily_metrics)
    return AssetCheckResult(
        passed=n > 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"row_count": n},
    )


# ── Collect for registration ─────────────────────────────────────────

UNIFIED_ASSETS = [
    unified_orders,
    unified_daily_metrics,
    unified_experiment_results,
]

UNIFIED_ASSET_CHECKS = [
    check_unified_orders_not_empty,
    check_unified_orders_no_duplicates,
    check_unified_orders_has_both_sources,
    check_unified_experiments_not_empty,
    check_unified_metrics_not_empty,
]
