"""
Consumer 1 – Order Stream Processor.
Consumes orders-stream, enriches with dimension lookups (customer cache),
dual-writes to MongoDB operational + analytics collections.
Idempotent upserts by order_id; manual Kafka offset commit after writes.
"""
from __future__ import annotations
import logging, time
from datetime import datetime, timezone
from typing import Any
import pymongo
from ecommerce_analytics.consumers.base_consumer import BaseConsumer
from ecommerce_analytics.consumers.config import (
    CACHE_REFRESH_SECONDS, GROUP_ORDER_PROCESSORS, MONGO_DB, MONGO_URI, TOPIC_ORDERS,
)

LOG = logging.getLogger("consumers.order_processor")


class OrderStreamProcessor(BaseConsumer):
    def __init__(self) -> None:
        super().__init__(topics=[TOPIC_ORDERS], group_id=GROUP_ORDER_PROCESSORS)
        self._mongo: pymongo.MongoClient | None = None
        self._db: Any = None
        self._cust_cache: dict[str, dict] = {}
        self._cache_ts = 0.0

    def on_start(self) -> None:
        self._mongo = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        self._db = self._mongo[MONGO_DB]
        self._db["orders"].create_index("order_id", unique=True)
        self._db["fct_orders_realtime"].create_index("order_id", unique=True)
        self._refresh_cache()
        LOG.info("Connected to MongoDB")

    def on_stop(self) -> None:
        if self._mongo:
            self._mongo.close()

    def validate(self, message: dict) -> None:
        super().validate(message)
        for key in ("order_id", "customer_id"):
            if key not in message:
                raise ValueError(f"Missing {key}")

    def process_batch(self, messages: list[dict]) -> None:
        if time.monotonic() - self._cache_ts > CACHE_REFRESH_SECONDS:
            self._refresh_cache()
        ops_docs, ana_docs = [], []
        for msg in messages:
            o, a = self._enrich(msg)
            ops_docs.append(o)
            ana_docs.append(a)
        self._upsert(self._db["orders"], ops_docs)
        self._upsert(self._db["fct_orders_realtime"], ana_docs)

    def _enrich(self, msg: dict) -> tuple[dict, dict]:
        cid = msg["customer_id"]
        now = datetime.now(timezone.utc).isoformat()
        c = self._cust_cache.get(cid, {})
        total = float(msg.get("order_total", 0))
        ops = {
            "order_id": msg["order_id"], "customer_id": cid,
            "customer_city": c.get("city", "unknown"),
            "customer_state": c.get("state", "unknown"),
            "order_total": total,
            "order_status": msg.get("order_status", "unknown"),
            "event_type": msg.get("event_type", "order_placed"),
            "experiment_id": msg.get("experiment_id"),
            "variant": msg.get("variant"),
            "event_timestamp": msg.get("timestamp", now),
            "processing_timestamp": now,
            "needs_enrichment": not bool(c),
        }
        ana = {
            "order_id": msg["order_id"], "customer_id": cid,
            "customer_state": c.get("state"),
            "customer_tier": c.get("customer_tier"),
            "order_total": total, "order_status": msg.get("order_status"),
            "event_type": msg.get("event_type", "order_placed"),
            "experiment_id": msg.get("experiment_id"),
            "variant": msg.get("variant"),
            "event_timestamp": msg.get("timestamp"), "processed_at": now,
        }
        return ops, ana

    @staticmethod
    def _upsert(coll: Any, docs: list[dict]) -> None:
        if not docs:
            return
        coll.bulk_write(
            [pymongo.UpdateOne({"order_id": d["order_id"]}, {"$set": d}, upsert=True) for d in docs],
            ordered=False,
        )

    def _refresh_cache(self) -> None:
        try:
            # dim_customers uses "state" and "city" (from load_dimensions.py), not customer_state/customer_city
            self._cust_cache = {
                d["customer_id"]: d
                for d in self._db["dim_customers"].find(
                    {}, {"_id": 0, "customer_id": 1, "city": 1, "state": 1})
                if d.get("customer_id")
            }
            LOG.info("Customer cache: %d entries", len(self._cust_cache))
        except Exception as e:
            LOG.warning("Cache refresh failed: %s", e)
        self._cache_ts = time.monotonic()
