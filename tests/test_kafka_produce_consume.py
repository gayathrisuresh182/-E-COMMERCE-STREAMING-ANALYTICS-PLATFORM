#!/usr/bin/env python3
"""
Test Kafka produce and consume (Phase 1E).
Run after: docker-compose up -d and create_kafka_topics.
Usage: python scripts/test_kafka_produce_consume.py
"""
from __future__ import annotations

import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = "localhost:9092"
TOPIC = "orders-stream"


def main() -> None:
    print("Connecting to Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except NoBrokersAvailable as e:
        raise SystemExit(f"Cannot connect to Kafka at {BOOTSTRAP}. Is docker-compose up? {e}")

    msg = {
        "event_type": "order_placed",
        "order_id": "test_order_001",
        "customer_id": "test_customer_001",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    producer.send(TOPIC, value=msg, key=b"test_customer_001")  # Partition key for ordering
    producer.flush()
    print(f"Produced to {TOPIC}: {msg}")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    for m in consumer:
        print(f"Consumed from {TOPIC}: {m.value}")
        break
    consumer.close()
    producer.close()
    print("Done. Kafka produce/consume OK.")


if __name__ == "__main__":
    main()
