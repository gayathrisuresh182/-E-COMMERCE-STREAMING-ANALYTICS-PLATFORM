#!/usr/bin/env python3
"""
Phase 2D: Quick test - send 10 messages per producer to verify Kafka connectivity.
Usage: python scripts/test_producers.py
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

def main():
    print("Testing Kafka producers (10 messages each)...")
    from kafka_producers.order_producer import run_order_producer
    from kafka_producers.payment_producer import run_payment_producer
    from kafka_producers.clickstream_producer import run_clickstream_producer
    from kafka_producers.delivery_producer import run_delivery_producer
    from kafka_producers.review_producer import run_review_producer

    n = run_order_producer(replay_hours=0.001, limit=10, dry_run=False)
    print(f"  orders: {n}")
    n = run_payment_producer(replay_hours=0.001, limit=10, dry_run=False)
    print(f"  payments: {n}")
    n = run_clickstream_producer(replay_hours=0.001, limit=10, dry_run=False)
    print(f"  clickstream: {n}")
    n = run_delivery_producer(replay_hours=0.001, limit=10, dry_run=False)
    print(f"  deliveries: {n}")
    n = run_review_producer(replay_hours=0.001, limit=10, dry_run=False)
    print(f"  reviews: {n}")
    print("Done. Verify with: kafka-console-consumer --topic orders-stream --from-beginning --max-messages 5")


if __name__ == "__main__":
    main()
