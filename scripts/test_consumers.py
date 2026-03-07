#!/usr/bin/env python3
"""
Quick integration test: produce 10 messages to orders-stream, then consume
them with OrderStreamProcessor for 10 seconds. Verifies end-to-end flow.

Uses an isolated consumer group so it won't conflict with running consumers.

Prerequisites: docker-compose up -d  (Kafka + MongoDB running)
Usage: python scripts/test_consumers.py
"""
from __future__ import annotations
import json, sys, time, threading, logging, uuid

sys.path.insert(0, ".")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
LOG = logging.getLogger("test_consumers")

TEST_GROUP = f"test-order-processors-{uuid.uuid4().hex[:8]}"


def produce_test_messages(n: int = 10) -> int:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=["127.0.0.1:9092"],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all", retries=2, request_timeout_ms=10000,
    )
    sent = 0
    for i in range(n):
        msg = {
            "event_type": "order_placed",
            "order_id": f"test_order_{i:04d}",
            "customer_id": f"test_customer_{i:04d}",
            "order_total": 100.0 + i * 10,
            "order_status": "delivered",
            "timestamp": "2026-01-15T12:00:00Z",
        }
        try:
            producer.send("orders-stream", value=msg, partition=0)
            sent += 1
        except Exception as e:
            LOG.error("Send failed: %s", e)
    producer.flush(timeout=10)
    producer.close(timeout=5)
    return sent


def run_consumer_briefly(seconds: float = 10) -> dict:
    from ecommerce_analytics.consumers.order_processor import OrderStreamProcessor
    consumer = OrderStreamProcessor()
    consumer.group_id = TEST_GROUP
    LOG.info("Using isolated consumer group: %s", TEST_GROUP)

    def stop_after():
        time.sleep(seconds)
        consumer._running = False

    stopper = threading.Thread(target=stop_after, daemon=True)
    stopper.start()

    try:
        consumer.run()
    except Exception as e:
        LOG.error("Consumer error: %s", e)

    return consumer.metrics.snapshot()


def main() -> None:
    LOG.info("=== Kafka Consumer Integration Test ===")

    LOG.info("Step 1: Producing 10 test messages to orders-stream...")
    try:
        sent = produce_test_messages(10)
        LOG.info("Produced %d messages", sent)
    except Exception as e:
        LOG.error("Producer failed (is Kafka running?): %s", e)
        sys.exit(1)

    LOG.info("Step 2: Running OrderStreamProcessor for 10 seconds...")
    try:
        metrics = run_consumer_briefly(10)
        LOG.info("Consumer metrics: %s", metrics)
    except Exception as e:
        LOG.error("Consumer failed: %s", e)
        sys.exit(1)

    processed = metrics.get("total_processed", 0)
    failed = metrics.get("total_failed", 0)
    LOG.info("=== Results: processed=%d failed=%d ===", processed, failed)

    if processed > 0:
        LOG.info("SUCCESS: Consumer processed messages end-to-end")
    else:
        LOG.warning("No messages processed (check Kafka/MongoDB connectivity)")


if __name__ == "__main__":
    main()
