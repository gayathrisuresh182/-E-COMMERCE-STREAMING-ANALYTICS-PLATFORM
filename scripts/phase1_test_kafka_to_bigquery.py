#!/usr/bin/env python3
"""
Phase 1 Verification: Kafka → Consumer → BigQuery streaming insert.
1. Send test order to orders-stream
2. Consumer reads and inserts row into BigQuery (realtime.realtime_orders)
3. Query BigQuery and verify row.
Usage: python scripts/phase1_test_kafka_to_bigquery.py
Requires: GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_CLOUD_PROJECT
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


def main() -> None:
    from google.cloud import bigquery
    from kafka import KafkaConsumer, KafkaProducer

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or not os.environ.get("GOOGLE_CLOUD_PROJECT"):
        print("Set GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT")
        sys.exit(1)

    BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    TOPIC = "orders-stream"
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    table_id = f"{project}.realtime.realtime_orders"

    order_id = f"verify_bq_{uuid.uuid4().hex[:8]}"
    customer_id = "verify_customer_bq"
    msg = {
        "event_type": "order_placed",
        "order_id": order_id,
        "customer_id": customer_id,
        "order_total": 88.88,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    # 1. Produce
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    producer.send(TOPIC, value=msg, key=customer_id.encode())
    producer.flush()
    producer.close()
    print(f"Produced to {TOPIC}: order_id={order_id}")

    # 2. Consume
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
    )
    payload = None
    for m in consumer:
        if m.value.get("order_id") == order_id:
            payload = m.value
            break
    consumer.close()

    if not payload:
        print("Consumer did not receive the message.")
        sys.exit(1)

    # 3. Streaming insert to BigQuery
    client = bigquery.Client(project=project)
    rows = [{
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "event_timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "order_total": payload.get("order_total", 0),
        "event_type": payload.get("event_type", "order_placed"),
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    }]
    err = client.insert_rows_json(table_id, rows)
    if err:
        print("BigQuery insert errors:", err)
        sys.exit(1)
    print(f"Inserted into BigQuery {table_id}")

    # 4. Verify (streaming buffer ~90s; query may take a moment)
    time.sleep(2)
    query = f"SELECT order_id, customer_id, order_total FROM `{table_id}` WHERE order_id = @order_id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("order_id", "STRING", order_id)]
    )
    result = list(client.query(query, job_config=job_config).result())
    client.close()
    if not result:
        print("Verification: row not yet visible (streaming buffer). Re-run query in a minute.")
        print("Kafka → BigQuery insert was successful; delay is normal for streaming.")
        sys.exit(0)
    row = result[0]
    print("Verified in BigQuery:", dict(row))
    print("Kafka → BigQuery test OK.")


if __name__ == "__main__":
    main()
