"""
Consumer 3 – Experiment Tracker.
Tracks A/B test conversions in real-time from clickstream + orders-stream.
Running totals per experiment x variant; z-test when sample >= 5000.
"""
from __future__ import annotations
import logging, math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
import pymongo
from ecommerce_analytics.consumers.base_consumer import BaseConsumer
from ecommerce_analytics.consumers.config import (
    GROUP_EXPERIMENT_TRACKER, MONGO_DB, MONGO_URI,
    TOPIC_CLICKSTREAM, TOPIC_ORDERS,
)

LOG = logging.getLogger("consumers.experiment_tracker")
MIN_SAMPLE = 5000
Z_CRITICAL = 1.96  # 95% confidence


class ExperimentTracker(BaseConsumer):
    def __init__(self) -> None:
        super().__init__(
            topics=[TOPIC_ORDERS, TOPIC_CLICKSTREAM],
            group_id=GROUP_EXPERIMENT_TRACKER,
        )
        self._mongo: pymongo.MongoClient | None = None
        self._db: Any = None
        self._counters: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"impressions": 0, "clicks": 0, "conversions": 0, "revenue": 0.0}
        )

    def on_start(self) -> None:
        self._mongo = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        self._db = self._mongo[MONGO_DB]
        self._db["experiments_realtime"].create_index(
            [("experiment_id", 1), ("variant", 1)], unique=True,
        )
        LOG.info("ExperimentTracker connected to MongoDB")

    def on_stop(self) -> None:
        self._flush()
        if self._mongo:
            self._mongo.close()

    def process_batch(self, messages: list[dict]) -> None:
        for msg in messages:
            exp_id = msg.get("experiment_id")
            variant = msg.get("variant")
            if not exp_id or not variant:
                continue
            key = f"{exp_id}|{variant}"
            evt = msg.get("event_type", "")
            if evt in ("session_start", "page_view", "product_view"):
                self._counters[key]["impressions"] += 1
            elif evt in ("add_to_cart", "checkout_start"):
                self._counters[key]["clicks"] += 1
            elif evt in ("order_placed", "checkout_complete"):
                self._counters[key]["conversions"] += 1
                self._counters[key]["revenue"] += float(msg.get("order_total", 0))
            self._counters[key]["experiment_id"] = exp_id
            self._counters[key]["variant"] = variant
        self._flush()

    def _flush(self) -> None:
        if not self._counters:
            return
        now = datetime.now(timezone.utc).isoformat()
        ops = []
        for key, c in self._counters.items():
            imp = c["impressions"]
            conv = c["conversions"]
            rate = round(conv / imp * 100, 4) if imp else 0.0
            doc = {
                "experiment_id": c["experiment_id"],
                "variant": c["variant"],
                "impressions": imp, "clicks": c["clicks"],
                "conversions": conv, "revenue": round(c["revenue"], 2),
                "conversion_rate": rate, "updated_at": now,
            }
            ops.append(pymongo.UpdateOne(
                {"experiment_id": c["experiment_id"], "variant": c["variant"]},
                {"$set": doc}, upsert=True,
            ))
        if ops:
            self._db["experiments_realtime"].bulk_write(ops, ordered=False)
        self._check_significance()

    def _check_significance(self) -> None:
        experiments: dict[str, list] = defaultdict(list)
        for c in self._counters.values():
            experiments[c["experiment_id"]].append(c)
        for exp_id, variants in experiments.items():
            if len(variants) < 2:
                continue
            ctrl = next((v for v in variants if v["variant"] == "control"), None)
            treat = next((v for v in variants if v["variant"] == "treatment"), None)
            if not ctrl or not treat:
                continue
            n_c, n_t = ctrl["impressions"], treat["impressions"]
            if n_c < MIN_SAMPLE or n_t < MIN_SAMPLE:
                continue
            p_c = ctrl["conversions"] / n_c if n_c else 0
            p_t = treat["conversions"] / n_t if n_t else 0
            p_pool = (ctrl["conversions"] + treat["conversions"]) / (n_c + n_t)
            se = math.sqrt(p_pool * (1 - p_pool) * (1/n_c + 1/n_t)) if p_pool > 0 else 1
            z = (p_t - p_c) / se if se > 0 else 0
            if abs(z) > Z_CRITICAL:
                winner = "treatment" if z > 0 else "control"
                LOG.info(
                    "SIGNIFICANT: %s — z=%.3f p_c=%.4f p_t=%.4f winner=%s",
                    exp_id, z, p_c, p_t, winner,
                )
