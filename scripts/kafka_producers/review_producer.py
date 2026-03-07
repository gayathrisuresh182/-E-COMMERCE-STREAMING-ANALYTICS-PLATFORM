"""
Phase 2D: Review event producer.
Emits review_posted 3-7 days after delivery (uses review_creation_date from Olist).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .base import close_producer, create_producer, send_with_retry
from .replay_clock import ReplayClock

LOG = logging.getLogger("review_producer")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW = PROJECT_ROOT / "raw" / "olist"


def parse_ts(s):
    if pd.isna(s):
        return None
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def run_review_producer(
    replay_hours: float = 48.0,
    limit: int = 0,
    dry_run: bool = False,
) -> int:
    """Produce review_posted events. Returns count sent."""
    reviews_path = RAW / "olist_order_reviews_dataset.csv"
    items_path = RAW / "olist_order_items_dataset.csv"

    if not reviews_path.exists():
        LOG.error("Missing %s", reviews_path)
        return 0

    nrows = (limit + 2000) if limit > 0 else None
    reviews = pd.read_csv(reviews_path, nrows=nrows)
    reviews["review_creation_date"] = reviews["review_creation_date"].apply(parse_ts)
    reviews = reviews.dropna(subset=["review_creation_date"])
    if limit > 0:
        reviews = reviews.head(limit)

    order_to_product = {}
    if items_path.exists():
        items_nrows = 20000 if limit > 0 and limit <= 100 else None
        items = pd.read_csv(items_path, nrows=items_nrows)
        for order_id, grp in items.groupby("order_id"):
            first_prod = grp["product_id"].iloc[0]
            order_to_product[str(order_id)] = str(first_prod)

    first_ts = reviews["review_creation_date"].min()
    last_ts = reviews["review_creation_date"].max()
    clock = ReplayClock(first_ts, last_ts, replay_hours)

    producer = create_producer() if not dry_run else None
    sent = 0
    start_wall = time.time()
    events_list = []

    for _, row in reviews.iterrows():
        order_id = str(row["order_id"])
        product_id = order_to_product.get(order_id, order_id)
        ts = row["review_creation_date"]
        replay_at = clock.replay_time(ts)
        events_list.append((replay_at, order_id, product_id, row))

    events_list.sort(key=lambda x: x[0])

    try:
        for replay_at, order_id, product_id, row in events_list:
            now = datetime.now(timezone.utc)
            if replay_at > now:
                sleep_secs = (replay_at - now).total_seconds()
                if sleep_secs > 0:
                    time.sleep(sleep_secs)

            msg = {
                "event_type": "review_posted",
                "review_id": str(row.get("review_id", "")),
                "order_id": order_id,
                "product_id": product_id,
                "review_score": int(row.get("review_score", 5)),
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

            if producer and send_with_retry(producer, "reviews-stream", msg, key=product_id.encode()):
                sent += 1
            elif dry_run:
                sent += 1

            if sent > 0 and sent % 500 == 0:
                elapsed = time.time() - start_wall
                rate = sent / (elapsed / 3600) if elapsed > 0 else 0
                LOG.info("  Reviews sent: %d (%.0f/hour)", sent, rate)
    finally:
        close_producer(producer)

    return sent
