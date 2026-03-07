"""
Phase 3K – Stream operations assets.

Two new assets that bridge the stream and batch layers:

1. consumer_health_metrics: Observable Dagster asset that queries Kafka
   consumer group lag and MongoDB collection stats, producing a trackable
   health report. Materialized hourly alongside realtime snapshots.

2. batch_stream_reconciliation: Nightly job (03:00, after batch) that
   reads stream data from MongoDB, compares with the batch layer's
   staging tables, and reports overlap/gaps. In a production BigQuery
   deployment this would MERGE stream rows into the batch table and
   archive processed stream data. Here we produce the reconciliation
   report as a DataFrame.
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

from ecommerce_analytics.consumers.config import (
    GROUP_DELIVERY_MONITOR,
    GROUP_EXPERIMENT_TRACKER,
    GROUP_METRICS_AGGREGATOR,
    GROUP_ORDER_PROCESSORS,
    KAFKA_BOOTSTRAP,
    MONGO_DB,
    MONGO_URI,
)

LOG = logging.getLogger("stream_ops")

CONSUMER_GROUPS = {
    GROUP_ORDER_PROCESSORS: "orders-stream",
    GROUP_METRICS_AGGREGATOR: "orders-stream,clickstream",
    GROUP_EXPERIMENT_TRACKER: "orders-stream,clickstream",
    GROUP_DELIVERY_MONITOR: "deliveries-stream",
}


def _query_kafka_lag() -> list[dict]:
    """Query Kafka for per-group consumer lag."""
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    try:
        from kafka import KafkaAdminClient, KafkaConsumer

        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=5000
        )
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)

        for group, topics in CONSUMER_GROUPS.items():
            try:
                offsets = admin.list_consumer_group_offsets(group)
                total_lag = 0
                partitions_assigned = 0
                for tp, offset_meta in offsets.items():
                    end_offsets = consumer.end_offsets([tp])
                    head = end_offsets.get(tp, 0)
                    total_lag += max(0, head - offset_meta.offset)
                    partitions_assigned += 1
                rows.append({
                    "consumer_group": group,
                    "topics": topics,
                    "total_lag": total_lag,
                    "partitions": partitions_assigned,
                    "status": "critical" if total_lag > 5000
                             else "warning" if total_lag > 1000
                             else "healthy",
                    "measured_at": now,
                })
            except Exception as exc:
                rows.append({
                    "consumer_group": group,
                    "topics": topics,
                    "total_lag": -1,
                    "partitions": 0,
                    "status": "unreachable",
                    "measured_at": now,
                    "error": str(exc),
                })
        consumer.close()
        admin.close()
    except Exception as exc:
        LOG.warning("Kafka lag query failed: %s", exc)
        for group, topics in CONSUMER_GROUPS.items():
            rows.append({
                "consumer_group": group,
                "topics": topics,
                "total_lag": -1,
                "partitions": 0,
                "status": "kafka_unavailable",
                "measured_at": now,
                "error": str(exc),
            })
    return rows


def _query_mongo_stats() -> list[dict]:
    """Query MongoDB realtime collections for document counts and freshness."""
    now = datetime.now(timezone.utc).isoformat()
    collections_ts = {
        "orders": ("fct_orders_realtime", "processed_at"),
        "metrics": ("realtime_metrics", "window_start"),
        "experiments": ("experiments_realtime", "updated_at"),
        "delivery_alerts": ("delivery_alerts", "created_at"),
        "delivery_events": ("delivery_events", "delivered_at"),
    }
    rows = []
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]
        for label, (coll_name, ts_field) in collections_ts.items():
            coll = db[coll_name]
            count = coll.count_documents({})
            newest = None
            if count > 0:
                doc = coll.find_one(sort=[(ts_field, pymongo.DESCENDING)])
                if doc:
                    newest = str(doc.get(ts_field, ""))
            rows.append({
                "collection": coll_name,
                "label": label,
                "document_count": count,
                "newest_timestamp": newest,
                "measured_at": now,
            })
        client.close()
    except Exception as exc:
        LOG.warning("MongoDB stats query failed: %s", exc)
        for label, (coll_name, _) in collections_ts.items():
            rows.append({
                "collection": coll_name,
                "label": label,
                "document_count": -1,
                "newest_timestamp": None,
                "measured_at": now,
                "error": str(exc),
            })
    return rows


# ── Asset 1: consumer_health_metrics ─────────────────────────────────

@asset(
    group_name="stream_ops",
    description=(
        "Kafka consumer health report: per-group lag, partition assignments, "
        "and MongoDB collection stats. Materialized hourly to track stream "
        "infrastructure health over time."
    ),
    compute_kind="kafka",
    metadata={"schema": "monitoring", "table": "consumer_health_metrics"},
)
def consumer_health_metrics(context) -> pd.DataFrame:
    kafka_rows = _query_kafka_lag()
    mongo_rows = _query_mongo_stats()

    kafka_df = pd.DataFrame(kafka_rows)
    mongo_df = pd.DataFrame(mongo_rows)

    total_lag = sum(r["total_lag"] for r in kafka_rows if r["total_lag"] >= 0)
    unhealthy = sum(1 for r in kafka_rows if r["status"] in ("critical", "unreachable"))
    total_docs = sum(r["document_count"] for r in mongo_rows if r["document_count"] >= 0)

    context.add_output_metadata({
        "kafka_total_lag": total_lag,
        "kafka_unhealthy_groups": unhealthy,
        "kafka_groups_checked": len(kafka_rows),
        "mongo_total_documents": total_docs,
        "mongo_collections_checked": len(mongo_rows),
        "snapshot_ts": datetime.now(timezone.utc).isoformat(),
    })

    if unhealthy > 0:
        LOG.warning("ALERT: %d consumer groups unhealthy (total lag: %d)", unhealthy, total_lag)

    combined = pd.concat(
        [kafka_df.assign(source="kafka"), mongo_df.assign(source="mongodb")],
        ignore_index=True,
    )
    return combined


# ── Asset 2: batch_stream_reconciliation ─────────────────────────────

@asset(
    group_name="stream_ops",
    description=(
        "Nightly reconciliation between stream layer (MongoDB) and batch "
        "layer (staging). Compares order IDs in fct_orders_realtime against "
        "stg_orders to find: orders only in stream (new, not yet in batch), "
        "orders in both (overlap, candidates for dedup), orders only in "
        "batch (historical, not in stream). In production BigQuery this "
        "would MERGE stream into batch and archive."
    ),
    compute_kind="pandas",
    metadata={"schema": "monitoring", "table": "batch_stream_reconciliation"},
)
def batch_stream_reconciliation(
    context,
    stg_orders: pd.DataFrame,
    realtime_orders: pd.DataFrame,
) -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()

    batch_ids = set()
    if not stg_orders.empty and "order_id" in stg_orders.columns:
        batch_ids = set(stg_orders["order_id"].astype(str).str.strip())

    stream_ids = set()
    if not realtime_orders.empty and "order_id" in realtime_orders.columns:
        stream_ids = set(realtime_orders["order_id"].astype(str).str.strip())

    only_stream = stream_ids - batch_ids
    only_batch = batch_ids - stream_ids
    overlap = stream_ids & batch_ids

    report = pd.DataFrame([
        {
            "category": "stream_only",
            "description": "Orders in stream but not batch (new, pending daily load)",
            "order_count": len(only_stream),
            "sample_ids": ", ".join(sorted(only_stream)[:5]),
            "reconciled_at": now,
        },
        {
            "category": "batch_only",
            "description": "Orders in batch but not stream (historical)",
            "order_count": len(only_batch),
            "sample_ids": "",
            "reconciled_at": now,
        },
        {
            "category": "overlap",
            "description": "Orders in both layers (candidates for dedup on merge)",
            "order_count": len(overlap),
            "sample_ids": ", ".join(sorted(overlap)[:5]),
            "reconciled_at": now,
        },
        {
            "category": "totals",
            "description": "Summary",
            "order_count": len(batch_ids | stream_ids),
            "sample_ids": f"batch={len(batch_ids)} stream={len(stream_ids)} union={len(batch_ids | stream_ids)}",
            "reconciled_at": now,
        },
    ])

    context.add_output_metadata({
        "batch_orders": len(batch_ids),
        "stream_orders": len(stream_ids),
        "stream_only": len(only_stream),
        "overlap": len(overlap),
        "batch_only": len(only_batch),
        "union_total": len(batch_ids | stream_ids),
        "reconciled_at": now,
    })

    LOG.info(
        "Reconciliation: batch=%d stream=%d overlap=%d stream_only=%d",
        len(batch_ids), len(stream_ids), len(overlap), len(only_stream),
    )
    return report


# ── Asset Checks ─────────────────────────────────────────────────────

@asset_check(
    asset=consumer_health_metrics,
    description="All Kafka consumer groups should be healthy",
)
def check_consumer_health_no_critical(
    consumer_health_metrics: pd.DataFrame,
) -> AssetCheckResult:
    kafka = consumer_health_metrics[consumer_health_metrics["source"] == "kafka"]
    if kafka.empty:
        return AssetCheckResult(passed=True, metadata={"skipped": True})
    critical = kafka[kafka["status"].isin(["critical", "unreachable"])]
    return AssetCheckResult(
        passed=len(critical) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "critical_groups": len(critical),
            "total_groups": len(kafka),
            "groups": critical["consumer_group"].tolist() if len(critical) > 0 else [],
        },
    )


@asset_check(
    asset=batch_stream_reconciliation,
    description="Stream-only orders should be reasonable (< 50% of stream)",
)
def check_reconciliation_stream_drift(
    batch_stream_reconciliation: pd.DataFrame,
) -> AssetCheckResult:
    totals = batch_stream_reconciliation[
        batch_stream_reconciliation["category"] == "totals"
    ]
    stream_only_row = batch_stream_reconciliation[
        batch_stream_reconciliation["category"] == "stream_only"
    ]
    if totals.empty or stream_only_row.empty:
        return AssetCheckResult(passed=True, metadata={"skipped": True})

    stream_only_count = int(stream_only_row["order_count"].iloc[0])
    sample = totals["sample_ids"].iloc[0]
    stream_total = 0
    for part in str(sample).split():
        if part.startswith("stream="):
            stream_total = int(part.split("=")[1])
    drift_pct = (stream_only_count / max(stream_total, 1)) * 100

    return AssetCheckResult(
        passed=drift_pct < 50,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "stream_only_orders": stream_only_count,
            "stream_total": stream_total,
            "drift_pct": round(drift_pct, 1),
        },
    )


# ── Collect for registration ─────────────────────────────────────────

STREAM_OPS_ASSETS = [
    consumer_health_metrics,
    batch_stream_reconciliation,
]

STREAM_OPS_ASSET_CHECKS = [
    check_consumer_health_no_critical,
    check_reconciliation_stream_drift,
]
