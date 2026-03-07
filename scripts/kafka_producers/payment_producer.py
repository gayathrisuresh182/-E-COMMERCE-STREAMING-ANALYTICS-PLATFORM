"""
Phase 2D: Payment event producer.
Sends payment_completed 30 seconds after each order_placed.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from .base import close_producer, create_producer, send_with_retry
from .replay_clock import ReplayClock

LOG = logging.getLogger("payment_producer")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW = PROJECT_ROOT / "raw" / "olist"


def parse_ts(s):
    if pd.isna(s):
        return None
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def run_payment_producer(
    replay_hours: float = 48.0,
    delay_after_order_seconds: int = 30,
    limit: int = 0,
    dry_run: bool = False,
) -> int:
    """Produce payment_completed events. Returns count sent."""
    orders_path = RAW / "olist_orders_dataset.csv"
    payments_path = RAW / "olist_order_payments_dataset.csv"

    if not orders_path.exists() or not payments_path.exists():
        LOG.error("Missing orders or payments CSV")
        return 0

    nrows = (limit + 5000) if limit > 0 else None
    orders = pd.read_csv(orders_path, nrows=nrows)
    orders["order_purchase_timestamp"] = orders["order_purchase_timestamp"].apply(parse_ts)
    orders = orders.dropna(subset=["order_purchase_timestamp"]).sort_values("order_purchase_timestamp")
    if limit > 0:
        orders = orders.head(limit)

    # Payments file isn't ordered by order_id; read enough rows to find payments for our orders
    payments_nrows = 50000 if (limit > 0 and limit <= 100) else None
    payments = pd.read_csv(payments_path, nrows=payments_nrows)
    order_ids = set(orders["order_id"].astype(str))
    payments = payments[payments["order_id"].astype(str).isin(order_ids)]

    first_ts = orders["order_purchase_timestamp"].iloc[0]
    last_ts = orders["order_purchase_timestamp"].iloc[-1]
    clock = ReplayClock(first_ts, last_ts, replay_hours)
    order_replay = {str(r["order_id"]): clock.replay_time(r["order_purchase_timestamp"]) for _, r in orders.iterrows()}

    producer = create_producer() if not dry_run else None
    sent = 0
    start_wall = time.time()
    PAYMENT_DELAY = timedelta(seconds=delay_after_order_seconds)

    try:
        for _, row in payments.iterrows():
            order_id = str(row["order_id"])
            if order_id not in order_replay:
                continue
            replay_at = order_replay[order_id] + PAYMENT_DELAY
            now = datetime.now(timezone.utc)
            if replay_at > now:
                sleep_secs = (replay_at - now).total_seconds()
                if sleep_secs > 0:
                    time.sleep(sleep_secs)

            msg = {
                "event_type": "payment_completed",
                "order_id": order_id,
                "payment_type": str(row.get("payment_type", "credit_card")),
                "payment_value": float(row.get("payment_value", 0)),
                "payment_installments": int(row.get("payment_installments", 1)),
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

            if producer and send_with_retry(producer, "payments-stream", msg, key=order_id.encode()):
                sent += 1
            elif dry_run:
                sent += 1

            if sent > 0 and sent % 500 == 0:
                elapsed = time.time() - start_wall
                rate = sent / (elapsed / 3600) if elapsed > 0 else 0
                LOG.info("  Payments sent: %d (%.0f/hour)", sent, rate)
    finally:
        close_producer(producer)

    return sent
