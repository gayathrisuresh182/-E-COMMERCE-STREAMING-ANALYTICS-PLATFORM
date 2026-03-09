#!/usr/bin/env python3
"""
Test Kafka partition ordering: same partition key → same partition → ordering preserved.
Run after: docker-compose up -d and create_kafka_topics.
Usage: python scripts/test_kafka_partition_ordering.py
"""
from __future__ import annotations

import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = "localhost:9092"
TOPIC = "orders-stream"
GROUP_ID = "test-ordering-group"


def main() -> None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except NoBrokersAvailable as e:
        raise SystemExit(f"Cannot connect to {BOOTSTRAP}. Is docker-compose up? {e}")

    # Send 3 messages with same partition key (customer_id) - they go to same partition, ordered
    customer_id = b"cust_order_test_001"
    for i in range(3):
        msg = {
            "event_type": "order_placed",
            "order_id": f"order_{i}",
            "customer_id": customer_id.decode(),
            "seq": i,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        future = producer.send(TOPIC, value=msg, key=customer_id)
        future.get()
        print(f"  Sent seq={i} (key={customer_id.decode()})")
    producer.flush()
    producer.close()

    # Consume - messages should arrive in order (seq 0, 1, 2)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    seen = []
    for m in consumer:
        seen.append(m.value.get("seq", -1))
        if len(seen) >= 3:
            break
    consumer.close()

    ordered = seen == [0, 1, 2] or seen == sorted(seen)
    print(f"Consumed seqs: {seen} | Ordering preserved: {ordered}")
    print("Done. Partition ordering test OK." if ordered else "Check: messages may be out of order.")


if __name__ == "__main__":
    main()
