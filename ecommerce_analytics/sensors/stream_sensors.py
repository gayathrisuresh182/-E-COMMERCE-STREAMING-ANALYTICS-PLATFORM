"""
Dagster sensors for Kafka consumer health and real-time data freshness.

1. kafka_consumer_health_sensor
   - Queries Kafka for consumer group lag across all consumer groups
   - If total lag exceeds threshold → triggers realtime asset materialization
   - Logs lag metrics as sensor metadata

2. realtime_freshness_sensor
   - Checks MongoDB collections for stale data (no recent writes)
   - Triggers materialization when new data detected
   - Acts as a change-data-capture trigger for the Dagster asset graph
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import pymongo
from dagster import (
    AssetKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
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

LOG = logging.getLogger("sensors")

CONSUMER_GROUPS = [
    GROUP_ORDER_PROCESSORS,
    GROUP_METRICS_AGGREGATOR,
    GROUP_EXPERIMENT_TRACKER,
    GROUP_DELIVERY_MONITOR,
]

COLLECTION_ASSET_MAP = {
    "fct_orders_realtime": AssetKey("realtime_orders"),
    "realtime_metrics": AssetKey("realtime_metrics_5min"),
    "experiments_realtime": AssetKey("realtime_experiment_results"),
}

LAG_THRESHOLD = 5000


def _get_kafka_lag() -> dict[str, int]:
    """
    Query Kafka consumer group lag using kafka-python admin client.
    Returns {group_id: total_lag} for each consumer group.
    """
    try:
        from kafka import KafkaAdminClient, KafkaConsumer
        from kafka.structs import TopicPartition

        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
        )
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
        lag_by_group: dict[str, int] = {}

        for group in CONSUMER_GROUPS:
            try:
                offsets = admin.list_consumer_group_offsets(group)
                total_lag = 0
                for tp, offset_meta in offsets.items():
                    end_offsets = consumer.end_offsets([tp])
                    head = end_offsets.get(tp, 0)
                    committed = offset_meta.offset
                    total_lag += max(0, head - committed)
                lag_by_group[group] = total_lag
            except Exception:
                lag_by_group[group] = -1

        consumer.close()
        admin.close()
        return lag_by_group
    except Exception as exc:
        LOG.warning("Kafka lag check failed: %s", exc)
        return {g: -1 for g in CONSUMER_GROUPS}


def _get_mongo_freshness() -> dict[str, dict]:
    """
    Check MongoDB collections for document count and newest timestamp.
    Returns {collection: {"count": N, "newest": iso_str or None}}.
    """
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]
    result = {}

    ts_fields = {
        "fct_orders_realtime": "processed_at",
        "realtime_metrics": "window_start",
        "experiments_realtime": "updated_at",
    }

    for coll_name, ts_field in ts_fields.items():
        coll = db[coll_name]
        count = coll.count_documents({})
        newest = None
        if count > 0:
            doc = coll.find_one(sort=[(ts_field, pymongo.DESCENDING)])
            if doc:
                newest = str(doc.get(ts_field, ""))
        result[coll_name] = {"count": count, "newest": newest}

    client.close()
    return result


# ── Sensor 1: Kafka Consumer Lag ─────────────────────────────────────

@sensor(
    name="kafka_consumer_health_sensor",
    description=(
        "Monitors Kafka consumer group lag. "
        "Triggers realtime asset materialization when lag exceeds threshold."
    ),
    minimum_interval_seconds=120,
)
def kafka_consumer_health_sensor(
    context: SensorEvaluationContext,
) -> SensorResult | SkipReason:
    lag = _get_kafka_lag()
    total_lag = sum(v for v in lag.values() if v >= 0)
    stale_groups = [g for g, v in lag.items() if v > LAG_THRESHOLD]

    context.log.info("Kafka lag: %s (total=%d)", lag, total_lag)

    if not stale_groups:
        return SkipReason(
            f"All consumer groups healthy. Total lag: {total_lag}"
        )

    run_requests = [
        RunRequest(
            run_key=f"kafka-lag-{int(time.time())}",
            asset_selection=[
                AssetKey("realtime_orders"),
                AssetKey("realtime_metrics_5min"),
                AssetKey("realtime_experiment_results"),
            ],
        )
    ]
    return SensorResult(
        run_requests=run_requests,
        cursor=str(int(time.time())),
    )


# ── Sensor 2: MongoDB Freshness (Change-Data-Capture style) ─────────

@sensor(
    name="realtime_freshness_sensor",
    description=(
        "Polls MongoDB realtime collections for new data. "
        "Triggers snapshot materialization when fresh documents detected."
    ),
    minimum_interval_seconds=300,
)
def realtime_freshness_sensor(
    context: SensorEvaluationContext,
) -> SensorResult | SkipReason:
    freshness = _get_mongo_freshness()

    prev_cursor = context.cursor or ""
    prev_counts: dict[str, int] = {}
    if prev_cursor:
        for pair in prev_cursor.split(";"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                prev_counts[k] = int(v)

    changed_assets: list[AssetKey] = []
    for coll_name, info in freshness.items():
        current_count = info["count"]
        previous_count = prev_counts.get(coll_name, 0)
        if current_count > previous_count:
            asset_key = COLLECTION_ASSET_MAP.get(coll_name)
            if asset_key:
                changed_assets.append(asset_key)
                context.log.info(
                    "%s: %d → %d docs (newest: %s)",
                    coll_name, previous_count, current_count, info["newest"],
                )

    new_cursor = ";".join(
        f"{c}={info['count']}" for c, info in freshness.items()
    )

    if not changed_assets:
        return SkipReason(
            f"No new data in realtime collections. Counts: "
            + ", ".join(f"{c}={info['count']}" for c, info in freshness.items())
        )

    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"freshness-{int(time.time())}",
                asset_selection=changed_assets,
            )
        ],
        cursor=new_cursor,
    )



# ── Sensor 3: Stream Health Alert (critical lag → reconciliation) ────

@sensor(
    name="stream_health_alert_sensor",
    description=(
        "Monitors consumer health metrics asset for critical status. "
        "When any consumer group is 'critical' or 'unreachable', triggers "
        "consumer_health_metrics refresh for immediate visibility."
    ),
    minimum_interval_seconds=600,
)
def stream_health_alert_sensor(
    context: SensorEvaluationContext,
) -> SensorResult | SkipReason:
    lag = _get_kafka_lag()
    critical = {g: v for g, v in lag.items() if v > 10000 or v == -1}

    if not critical:
        total = sum(v for v in lag.values() if v >= 0)
        return SkipReason(f"All groups within SLA. Total lag: {total}")

    context.log.warning("CRITICAL consumer groups: %s", critical)
    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"health-alert-{int(time.time())}",
                asset_selection=[
                    AssetKey("consumer_health_metrics"),
                    AssetKey("realtime_orders"),
                    AssetKey("realtime_metrics_5min"),
                    AssetKey("realtime_experiment_results"),
                ],
            )
        ],
        cursor=str(int(time.time())),
    )


SENSORS = [
    kafka_consumer_health_sensor,
    realtime_freshness_sensor,
    stream_health_alert_sensor,
]
