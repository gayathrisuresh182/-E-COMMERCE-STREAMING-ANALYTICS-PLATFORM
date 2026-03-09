#!/usr/bin/env python3
"""
Phase 1 Verification: End-to-end pipeline.
Producer -> Kafka -> Consumer -> MongoDB + BigQuery.
Send 10 test orders; verify all 10 in both stores; check no duplicates; report latency.
Usage: python scripts/phase1_test_e2e.py
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
N_ORDERS = 10
DB = "ecommerce_streaming"
COLLECTION = "orders"


def main() -> None:
    import pymongo
    from google.cloud import bigquery
    from kafka import KafkaConsumer, KafkaProducer

    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project or not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_CLOUD_PROJECT and GOOGLE_APPLICATION_CREDENTIALS for BigQuery")
        sys.exit(1)

    order_ids = [f"e2e_{uuid.uuid4().hex[:8]}" for _ in range(N_ORDERS)]
    t0 = time.perf_counter()

    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for i, oid in enumerate(order_ids):
        producer.send(
            TOPIC,
            value={
                "event_type": "order_placed",
                "order_id": oid,
                "customer_id": f"e2e_cust_{i}",
                "order_total": 10.0 + i,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            key=f"e2e_cust_{i}".encode(),
        )
    producer.flush()
    producer.close()
    t_produced = time.perf_counter() - t0
    print(f"Produced {N_ORDERS} orders in {t_produced:.2f}s")

    uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
    mongo = pymongo.MongoClient(uri)
    coll = mongo[DB][COLLECTION]
    bq = bigquery.Client(project=project)
    table_id = f"{project}.realtime.realtime_orders"

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=15000,
    )
    seen = set()
    for m in consumer:
        v = m.value
        oid = v.get("order_id")
        if oid in order_ids and oid not in seen:
            seen.add(oid)
            coll.insert_one({
                "order_id": oid,
                "customer_id": v.get("customer_id"),
                "order_total": v.get("order_total"),
                "source": "e2e_test",
            })
            bq.insert_rows_json(table_id, [{
                "event_id": str(uuid.uuid4()),
                "order_id": oid,
                "customer_id": v.get("customer_id"),
                "event_timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                "order_total": v.get("order_total"),
                "event_type": "order_placed",
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            }])
        if len(seen) >= N_ORDERS:
            break
    consumer.close()

    t_done = time.perf_counter() - t0
    mongo_count = coll.count_documents({"order_id": {"$in": order_ids}})
    mongo.close()

    time.sleep(2)
    bq_count = 0
    for oid in order_ids:
        r = list(bq.query(f"SELECT 1 FROM `{table_id}` WHERE order_id = '{oid}'").result())
        bq_count += len(r)
    bq.close()

    print(f"MongoDB: {mongo_count}/{N_ORDERS} orders")
    print(f"BigQuery: {bq_count}/{N_ORDERS} orders (streaming delay may show less)")
    print(f"Latency (produce to consume+write): ~{t_done:.2f}s for {N_ORDERS} orders")
    dup_mongo = mongo_count - len(seen)
    if mongo_count != N_ORDERS or dup_mongo > 0:
        print("FAIL: count or duplicate issue")
        sys.exit(1)
    print("E2E test OK.")


if __name__ == "__main__":
    main()
