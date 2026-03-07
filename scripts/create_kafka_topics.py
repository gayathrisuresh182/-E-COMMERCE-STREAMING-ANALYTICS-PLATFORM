#!/usr/bin/env python3
"""
Create Kafka topics for E-Commerce Streaming Analytics (Phase 1E).
Run after: docker-compose up -d
Usage: python scripts/create_kafka_topics.py
"""
from __future__ import annotations

import time
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = ["127.0.0.1:9092"]

TOPICS = [
    {"name": "orders-stream", "partitions": 3, "replication_factor": 1, "retention_ms": 604800000},   # 7 days
    {"name": "clickstream", "partitions": 6, "replication_factor": 1, "retention_ms": 259200000},    # 3 days
    {"name": "payments-stream", "partitions": 3, "replication_factor": 1, "retention_ms": 604800000},
    {"name": "shipments-stream", "partitions": 3, "replication_factor": 1, "retention_ms": 604800000},
    {"name": "deliveries-stream", "partitions": 3, "replication_factor": 1, "retention_ms": 604800000},
    {"name": "reviews-stream", "partitions": 3, "replication_factor": 1, "retention_ms": 604800000},
    {"name": "experiments-stream", "partitions": 3, "replication_factor": 1, "retention_ms": 2592000000},  # 30 days
    # Dead-letter queues (Phase 3F consumers)
    {"name": "dlq_orders-stream", "partitions": 1, "replication_factor": 1, "retention_ms": 2592000000},
    {"name": "dlq_clickstream", "partitions": 1, "replication_factor": 1, "retention_ms": 2592000000},
    {"name": "dlq_deliveries-stream", "partitions": 1, "replication_factor": 1, "retention_ms": 2592000000},
]


def main() -> None:
    print("Connecting to Kafka...")
    for attempt in range(6):
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            admin.list_topics()
            break
        except Exception as e:
            print(f"  Attempt {attempt + 1}/6: {e}")
            time.sleep(5)
    else:
        raise SystemExit("Could not connect to Kafka. Is docker-compose up?")

    for t in TOPICS:
        topic = NewTopic(
            name=t["name"],
            num_partitions=t["partitions"],
            replication_factor=t["replication_factor"],
            topic_configs={
            "retention.ms": str(t["retention_ms"]),
            "cleanup.policy": "delete",
            # Omit compression.type to avoid python-snappy dependency (tricky on Windows)
        },
        )
        try:
            admin.create_topics([topic])
            print(f"Created: {t['name']}")
        except TopicAlreadyExistsError:
            print(f"Exists:  {t['name']}")

    print("Listing topics:", admin.list_topics())
    admin.close()
    print("Done.")


if __name__ == "__main__":
    main()
