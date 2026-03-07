"""
Consumer 2 – Real-Time Metrics Aggregator (5-min tumbling windows).
Consumes orders-stream and clickstream, buffers messages into time windows,
aggregates on window close, writes results to MongoDB realtime_metrics.
"""
from __future__ import annotations
import logging, time, math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
import pymongo
from ecommerce_analytics.consumers.base_consumer import BaseConsumer
from ecommerce_analytics.consumers.config import (
    GROUP_METRICS_AGGREGATOR, MONGO_DB, MONGO_URI,
    TOPIC_CLICKSTREAM, TOPIC_ORDERS,
    WINDOW_GRACE_SECONDS, WINDOW_SIZE_SECONDS,
)

LOG = logging.getLogger("consumers.metrics_aggregator")


class WindowBuffer:
    """Accumulates messages for a single tumbling window."""
    def __init__(self, start: float, size: float) -> None:
        self.start = start
        self.end = start + size
        self.orders: list[dict] = []
        self.clicks: list[dict] = []
        self.last_msg_time = time.monotonic()

    def accepts(self, ts: float) -> bool:
        return self.start <= ts < self.end

    def is_closed(self, now_wall: float, grace: float) -> bool:
        return now_wall >= self.end + grace


class MetricsAggregator(BaseConsumer):
    def __init__(self) -> None:
        super().__init__(
            topics=[TOPIC_ORDERS, TOPIC_CLICKSTREAM],
            group_id=GROUP_METRICS_AGGREGATOR,
        )
        self._mongo: pymongo.MongoClient | None = None
        self._db: Any = None
        self._windows: dict[float, WindowBuffer] = {}
        self._win_size = WINDOW_SIZE_SECONDS
        self._grace = WINDOW_GRACE_SECONDS

    def on_start(self) -> None:
        self._mongo = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        self._db = self._mongo[MONGO_DB]
        self._db["realtime_metrics"].create_index(
            [("window_start", 1), ("metric_name", 1)], unique=True,
        )
        LOG.info("MetricsAggregator connected to MongoDB")

    def on_stop(self) -> None:
        self._flush_all_windows()
        if self._mongo:
            self._mongo.close()

    def process_batch(self, messages: list[dict]) -> None:
        for msg in messages:
            self._route_to_window(msg)
        self._close_expired_windows()

    def _route_to_window(self, msg: dict) -> None:
        ts_str = msg.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        except (ValueError, AttributeError):
            ts = time.time()
        win_start = math.floor(ts / self._win_size) * self._win_size
        if win_start not in self._windows:
            self._windows[win_start] = WindowBuffer(win_start, self._win_size)
        buf = self._windows[win_start]
        evt = msg.get("event_type", "")
        if evt == "order_placed":
            buf.orders.append(msg)
        else:
            buf.clicks.append(msg)
        buf.last_msg_time = time.monotonic()

    def _close_expired_windows(self) -> None:
        now = time.time()
        closed = [k for k, w in self._windows.items() if w.is_closed(now, self._grace)]
        for k in closed:
            self._aggregate_and_write(self._windows.pop(k))

    def _flush_all_windows(self) -> None:
        for w in list(self._windows.values()):
            self._aggregate_and_write(w)
        self._windows.clear()

    def _aggregate_and_write(self, w: WindowBuffer) -> None:
        ws = datetime.fromtimestamp(w.start, tz=timezone.utc).isoformat()
        we = datetime.fromtimestamp(w.end, tz=timezone.utc).isoformat()
        n_orders = len(w.orders)
        gmv = sum(float(o.get("order_total", 0)) for o in w.orders)
        sessions = len({c.get("session_id") for c in w.clicks if c.get("session_id")})
        conv = round(n_orders / sessions * 100, 2) if sessions else 0.0
        aov = round(gmv / n_orders, 2) if n_orders else 0.0
        metrics = {
            "total_orders": n_orders, "total_gmv": round(gmv, 2),
            "unique_sessions": sessions, "conversion_rate": conv,
            "avg_order_value": aov, "total_clicks": len(w.clicks),
        }
        docs = [
            {"window_start": ws, "window_end": we,
             "metric_name": k, "metric_value": v}
            for k, v in metrics.items()
        ]
        if docs:
            ops = [
                pymongo.UpdateOne(
                    {"window_start": d["window_start"], "metric_name": d["metric_name"]},
                    {"$set": d}, upsert=True,
                ) for d in docs
            ]
            self._db["realtime_metrics"].bulk_write(ops, ordered=False)
            LOG.info("Window %s: orders=%d gmv=%.2f sessions=%d", ws, n_orders, gmv, sessions)
