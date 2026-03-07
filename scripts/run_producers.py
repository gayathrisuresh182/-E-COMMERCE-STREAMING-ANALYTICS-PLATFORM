#!/usr/bin/env python3
"""
Phase 2D: Run all Kafka producers in parallel.

Replays Olist data as real-time events with compressed timing (48h default).
Graceful shutdown on Ctrl+C.

Usage:
  python scripts/run_producers.py [--duration 48] [--limit 1000] [--test]
  python scripts/run_producers.py --producer orders   # run only orders
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("run_producers")

# Lazy imports to avoid loading all producers at once
def _run_orders(args):
    from scripts.kafka_producers.order_producer import run_order_producer
    return run_order_producer(args.duration, args.limit, args.dry_run)

def _run_clickstream(args):
    from scripts.kafka_producers.clickstream_producer import run_clickstream_producer
    return run_clickstream_producer(args.duration, args.limit, args.dry_run)

def _run_payments(args):
    from scripts.kafka_producers.payment_producer import run_payment_producer
    return run_payment_producer(args.duration, args.limit, args.dry_run)

def _run_deliveries(args):
    from scripts.kafka_producers.delivery_producer import run_delivery_producer
    return run_delivery_producer(args.duration, args.limit, args.dry_run)

def _run_reviews(args):
    from scripts.kafka_producers.review_producer import run_review_producer
    return run_review_producer(args.duration, args.limit, args.dry_run)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=48.0, help="Replay window in hours")
    ap.add_argument("--limit", type=int, default=0, help="Limit events per producer (0=all)")
    ap.add_argument("--dry-run", action="store_true", help="No Kafka, just count")
    ap.add_argument("--test", action="store_true", help="Short test (limit 10 per producer)")
    ap.add_argument("--producer", choices=["orders", "clickstream", "payments", "deliveries", "reviews", "all"], default="all")
    args = ap.parse_args()

    if args.test:
        args.limit = 50  # enough to populate all topics; 10 was too few for payments/deliveries
        args.duration = 0.0  # no replay delay: send all events immediately

    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
    from kafka_producers import order_producer, clickstream_producer, payment_producer, delivery_producer, review_producer

    producers = []
    dur, lim, dry = args.duration, args.limit, args.dry_run
    if args.producer in ("orders", "all"):
        producers.append(("orders", lambda: order_producer.run_order_producer(dur, lim, dry)))
    if args.producer in ("clickstream", "all"):
        producers.append(("clickstream", lambda: clickstream_producer.run_clickstream_producer(dur, lim, dry)))
    if args.producer in ("payments", "all"):
        pay_delay = 0 if args.test else 30
        producers.append(("payments", lambda: payment_producer.run_payment_producer(dur, pay_delay, lim, dry)))
    if args.producer in ("deliveries", "all"):
        producers.append(("deliveries", lambda: delivery_producer.run_delivery_producer(dur, lim, dry)))
    if args.producer in ("reviews", "all"):
        producers.append(("reviews", lambda: review_producer.run_review_producer(dur, lim, dry)))

    if not producers:
        LOG.error("No producers selected")
        sys.exit(1)

    results = {}
    errors = []

    def run_one(name, fn):
        try:
            n = fn()
            results[name] = n
        except Exception as e:
            errors.append((name, str(e)))
            results[name] = -1

    LOG.info("Starting %d producer(s), duration=%.1fh, limit=%s", len(producers), args.duration, args.limit or "all")

    if len(producers) == 1:
        name, fn = producers[0]
        run_one(name, fn)
    else:
        threads = []
        for name, fn in producers:
            t = threading.Thread(target=run_one, args=(name, fn))
            t.start()
            threads.append(t)
            time.sleep(1.5)  # Stagger starts so Kafka isn't hit by all connections at once
        for t in threads:
            t.join()

    for name, n in results.items():
        if n >= 0:
            LOG.info("  %s: %d events", name, n)
        else:
            LOG.error("  %s: FAILED", name)
    for name, err in errors:
        LOG.error("  %s error: %s", name, err)

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
