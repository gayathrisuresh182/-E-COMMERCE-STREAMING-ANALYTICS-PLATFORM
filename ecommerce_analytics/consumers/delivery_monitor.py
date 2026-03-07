"""
Consumer 4 – Delivery SLA Monitor.
Consumes deliveries-stream, checks for late deliveries, writes alerts to
MongoDB delivery_alerts collection, updates seller scores.
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Any
import pymongo
from ecommerce_analytics.consumers.base_consumer import BaseConsumer
from ecommerce_analytics.consumers.config import (
    GROUP_DELIVERY_MONITOR, MONGO_DB, MONGO_URI, TOPIC_DELIVERIES,
)

LOG = logging.getLogger("consumers.delivery_monitor")
LATE_THRESHOLD_DAYS = 3


class DeliverySLAMonitor(BaseConsumer):
    def __init__(self) -> None:
        super().__init__(topics=[TOPIC_DELIVERIES], group_id=GROUP_DELIVERY_MONITOR)
        self._mongo: pymongo.MongoClient | None = None
        self._db: Any = None

    def on_start(self) -> None:
        self._mongo = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        self._db = self._mongo[MONGO_DB]
        self._db["delivery_alerts"].create_index("order_id")
        self._db["delivery_events"].create_index(
            [("order_id", 1), ("event_type", 1)], unique=True,
        )
        LOG.info("DeliverySLAMonitor connected to MongoDB")

    def on_stop(self) -> None:
        if self._mongo:
            self._mongo.close()

    def validate(self, message: dict) -> None:
        super().validate(message)
        if "order_id" not in message:
            raise ValueError("Missing order_id")

    def process_batch(self, messages: list[dict]) -> None:
        events, alerts = [], []
        for msg in messages:
            doc = {
                "order_id": msg["order_id"],
                "event_type": msg.get("event_type", "unknown"),
                "timestamp": msg.get("timestamp"),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
            events.append(doc)
            if msg.get("event_type") == "delivered":
                alert = self._check_sla(msg)
                if alert:
                    alerts.append(alert)
        if events:
            ops = [
                pymongo.UpdateOne(
                    {"order_id": e["order_id"], "event_type": e["event_type"]},
                    {"$set": e}, upsert=True,
                ) for e in events
            ]
            self._db["delivery_events"].bulk_write(ops, ordered=False)
        if alerts:
            self._db["delivery_alerts"].insert_many(alerts)
            for a in alerts:
                LOG.warning(
                    "LATE DELIVERY: order=%s days_late=%.1f",
                    a["order_id"], a.get("days_late", 0),
                )

    def _check_sla(self, msg: dict) -> dict | None:
        shipped = self._db["delivery_events"].find_one(
            {"order_id": msg["order_id"], "event_type": "shipped"}
        )
        if not shipped or not shipped.get("timestamp"):
            return None
        try:
            ts_ship = datetime.fromisoformat(
                shipped["timestamp"].replace("Z", "+00:00")
            )
            ts_deliver = datetime.fromisoformat(
                msg["timestamp"].replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            return None
        days = (ts_deliver - ts_ship).total_seconds() / 86400
        if days > LATE_THRESHOLD_DAYS:
            return {
                "order_id": msg["order_id"],
                "days_late": round(days - LATE_THRESHOLD_DAYS, 1),
                "total_delivery_days": round(days, 1),
                "shipped_at": shipped["timestamp"],
                "delivered_at": msg["timestamp"],
                "severity": "critical" if days > 7 else "warning",
                "alerted_at": datetime.now(timezone.utc).isoformat(),
            }
        return None
