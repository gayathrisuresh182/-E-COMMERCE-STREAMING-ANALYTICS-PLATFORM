"""
Phase 2D: Delivery event producer.
Emits shipped, in_transit, out_for_delivery, delivered with compressed timing.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from .base import close_producer, create_producer, send_with_retry
from .replay_clock import ReplayClock

LOG = logging.getLogger("delivery_producer")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW = PROJECT_ROOT / "raw" / "olist"


def parse_ts(s):
    if pd.isna(s):
        return None
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def run_delivery_producer(
    replay_hours: float = 48.0,
    limit: int = 0,
    dry_run: bool = False,
) -> int:
    """Produce delivery events. Returns count sent."""
    orders_path = RAW / "olist_orders_dataset.csv"

    if not orders_path.exists():
        LOG.error("Missing %s", orders_path)
        return 0

    nrows = (limit + 5000) if limit > 0 else None
    orders = pd.read_csv(orders_path, nrows=nrows)
    orders["order_purchase_timestamp"] = orders["order_purchase_timestamp"].apply(parse_ts)
    orders["order_delivered_customer_date"] = orders["order_delivered_customer_date"].apply(parse_ts)
    orders = orders.dropna(subset=["order_purchase_timestamp"])
    if limit > 0:
        orders = orders.head(limit)

    first_ts = orders["order_purchase_timestamp"].min()
    last_ts = orders["order_purchase_timestamp"].max()
    clock = ReplayClock(first_ts, last_ts, replay_hours)

    producer = create_producer() if not dry_run else None
    sent = 0
    start_wall = time.time()
    events_list = []

    for _, row in orders.iterrows():
        order_id = str(row["order_id"])
        purchase_ts = row["order_purchase_timestamp"]
        delivered_ts = row.get("order_delivered_customer_date")
        if pd.isna(delivered_ts):
            delivered_ts = purchase_ts + timedelta(days=10)

        shipped_ts = purchase_ts + timedelta(days=2)
        in_transit_ts = purchase_ts + timedelta(days=4)
        out_for_delivery_ts = delivered_ts - timedelta(days=1)

        for ev_type, hist_ts in [
            ("shipped", shipped_ts),
            ("in_transit", in_transit_ts),
            ("out_for_delivery", out_for_delivery_ts),
            ("delivered", delivered_ts),
        ]:
            replay_at = clock.replay_time(hist_ts)
            events_list.append((replay_at, order_id, ev_type))

    events_list.sort(key=lambda x: x[0])

    try:
        for replay_at, order_id, ev_type in events_list:
            now = datetime.now(timezone.utc)
            if replay_at > now:
                sleep_secs = (replay_at - now).total_seconds()
                if sleep_secs > 0:
                    time.sleep(sleep_secs)

            msg = {
                "event_type": ev_type,
                "order_id": order_id,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

            if producer and send_with_retry(producer, "deliveries-stream", msg, key=order_id.encode()):
                sent += 1
            elif dry_run:
                sent += 1

            if sent > 0 and sent % 1000 == 0:
                elapsed = time.time() - start_wall
                rate = sent / (elapsed / 3600) if elapsed > 0 else 0
                LOG.info("  Delivery events sent: %d (%.0f/hour)", sent, rate)
    finally:
        close_producer(producer)

    return sent
