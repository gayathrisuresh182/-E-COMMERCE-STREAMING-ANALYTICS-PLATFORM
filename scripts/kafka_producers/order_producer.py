"""
Phase 2D: Order event producer.
Replays historical Olist orders as order_placed events with compressed timing.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .base import close_producer, create_producer, send_with_retry
from .replay_clock import ReplayClock

LOG = logging.getLogger("order_producer")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW = PROJECT_ROOT / "raw" / "olist"
OUTPUT = PROJECT_ROOT / "output"


def parse_ts(s):
    if pd.isna(s):
        return None
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def run_order_producer(
    replay_hours: float = 48.0,
    limit: int = 0,
    dry_run: bool = False,
) -> int:
    """Produce order_placed events. Returns count sent."""
    orders_path = RAW / "olist_orders_dataset.csv"
    items_path = RAW / "olist_order_items_dataset.csv"
    assignments_path = OUTPUT / "experiment_assignments.csv"

    if not orders_path.exists():
        LOG.error("Missing %s", orders_path)
        return 0

    nrows = (limit + 5000) if limit > 0 else None
    orders = pd.read_csv(orders_path, nrows=nrows)
    orders["order_purchase_timestamp"] = orders["order_purchase_timestamp"].apply(parse_ts)
    orders = orders.dropna(subset=["order_purchase_timestamp"]).sort_values("order_purchase_timestamp")
    if limit > 0:
        orders = orders.head(limit)

    items_nrows = (limit * 50 + 1000) if limit > 0 else None
    items = pd.read_csv(items_path, nrows=items_nrows)
    order_totals = items.groupby("order_id")["price"].sum().to_dict()

    assignments = {}
    if assignments_path.exists():
        assign_nrows = 50000 if limit > 0 and limit <= 100 else None
        a = pd.read_csv(assignments_path, nrows=assign_nrows)
        a = a[a["experiment_id"] == "exp_001"][["customer_id", "variant"]]
        assignments = dict(zip(a["customer_id"].astype(str), a["variant"]))

    first_ts = orders["order_purchase_timestamp"].iloc[0]
    last_ts = orders["order_purchase_timestamp"].iloc[-1]
    clock = ReplayClock(first_ts, last_ts, replay_hours)

    producer = create_producer() if not dry_run else None
    sent = 0
    start_wall = time.time()

    try:
        for i, row in orders.iterrows():
            order_id = str(row["order_id"])
            customer_id = str(row["customer_id"]).strip()
            order_ts = row["order_purchase_timestamp"]
            replay_at = clock.replay_time(order_ts)
            now = datetime.now(timezone.utc)
            if replay_at > now:
                sleep_secs = (replay_at - now).total_seconds()
                if sleep_secs > 0:
                    time.sleep(sleep_secs)

            total = order_totals.get(order_id, 0.0)
            variant = assignments.get(customer_id, "control")
            order_status = str(row.get("order_status", "delivered")).strip().lower()
            # Map order_status -> event_type (delivered -> order_delivered, canceled -> order_canceled, etc.)
            event_type = f"order_{order_status}" if order_status else "order_placed"
            msg = {
                "event_type": event_type,
                "order_id": order_id,
                "customer_id": customer_id,
                "order_total": round(float(total), 2),
                "order_status": order_status,
                "experiment_id": "exp_001",
                "variant": variant,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

            if producer and send_with_retry(producer, "orders-stream", msg, key=customer_id.encode()):
                sent += 1
            elif dry_run:
                sent += 1

            if sent > 0 and sent % 500 == 0:
                elapsed = time.time() - start_wall
                rate = sent / (elapsed / 3600) if elapsed > 0 else 0
                LOG.info("  Orders sent: %d (%.0f/hour)", sent, rate)
    finally:
        close_producer(producer)

    return sent
