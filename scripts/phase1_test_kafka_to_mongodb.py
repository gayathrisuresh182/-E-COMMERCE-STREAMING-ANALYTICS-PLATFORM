#!/usr/bin/env python3
"""
Phase 1 Verification: Kafka -> Consumer -> MongoDB.
1. Send test order to orders-stream
2. Consumer reads and writes to MongoDB orders collection
3. Verify document exists and print it.
Usage: python scripts/phase1_test_kafka_to_mongodb.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
load_dotenv(Path(__file__).resolve().parent.parent / "docs" / ".env")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "orders-stream"
DB = "ecommerce_streaming"
COLLECTION = "orders"


def main() -> None:
    import pymongo
    from kafka import KafkaConsumer, KafkaProducer

    order_id = "verify_" + uuid.uuid4().hex[:8]
    customer_id = "verify_customer_001"
    msg = {
        "event_type": "order_placed",
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": "delivered",
        "order_total": 99.99,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    producer.send(TOPIC, value=msg, key=customer_id.encode())
    producer.flush()
    producer.close()
    print("Produced to", TOPIC, "order_id=", order_id)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
    )
    doc = None
    for m in consumer:
        if m.value.get("order_id") == order_id:
            doc = {
                "order_id": m.value["order_id"],
                "customer_id": m.value["customer_id"],
                "order_status": m.value.get("order_status"),
                "order_total": m.value.get("order_total"),
                "event_type": m.value.get("event_type"),
                "source": "phase1_verify",
            }
            break
    consumer.close()

    if not doc:
        print("Consumer did not receive the message.")
        sys.exit(1)

    uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
    client = pymongo.MongoClient(uri)
    coll = client[DB][COLLECTION]
    coll.insert_one(doc)
    print("Inserted into MongoDB", DB + "." + COLLECTION)

    found = coll.find_one({"order_id": order_id})
    client.close()
    if not found:
        print("Verification FAIL: document not found in MongoDB")
        sys.exit(1)
    out = {k: str(v) for k, v in found.items() if k != "_id"}
    print("Verified in MongoDB:", json.dumps(out, default=str))
    print("Kafka -> MongoDB test OK.")


if __name__ == "__main__":
    main()
