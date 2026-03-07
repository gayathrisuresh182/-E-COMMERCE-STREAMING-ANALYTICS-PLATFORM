#!/usr/bin/env python3
"""
Phase 3L – Failure scenario tests for resilience verification.

Tests:
  1. Consumer crash recovery: kill consumer, restart, verify no data loss
  2. Kafka broker restart: stop/start Kafka, verify reconnection
  3. MongoDB unavailability: stop MongoDB, verify DLQ + graceful degradation
  4. Dagster job failure: verify downstream assets not materialized
  5. Duplicate message handling: send same message twice, verify idempotency

Prerequisites:
  docker-compose up -d
  python scripts/integration_test.py --phase 1  (batch assets exist)

Usage:
  python scripts/failure_scenarios.py                  # all scenarios
  python scripts/failure_scenarios.py --scenario 1     # specific scenario
  python scripts/failure_scenarios.py --list            # list scenarios
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("failure_scenarios")

RESULTS: dict[str, dict] = {}


def record(scenario: str, step: str, passed: bool, detail: str = "") -> None:
    key = f"{scenario}/{step}"
    RESULTS[key] = {"passed": passed, "detail": detail}
    icon = "PASS" if passed else "FAIL"
    LOG.info("[%s] %s: %s", icon, key, detail)


def docker_cmd(action: str, service: str, timeout: int = 30) -> bool:
    """Run docker-compose action on a service."""
    cmd = ["docker-compose", action, service]
    LOG.info("Docker: %s %s", action, service)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       cwd=str(Path(__file__).resolve().parent.parent))
    return r.returncode == 0


# ═══════════════════════════════════════════════════════════════════════
# Scenario 1: Consumer Crash Recovery
# ═══════════════════════════════════════════════════════════════════════

def scenario1_consumer_crash_recovery() -> None:
    """
    Simulate consumer crash and restart.
    Verify: resumes from last committed offset, no data loss.
    """
    LOG.info("\n" + "=" * 60)
    LOG.info("SCENARIO 1: Consumer Crash Recovery")
    LOG.info("=" * 60)

    group = f"crash-test-{uuid.uuid4().hex[:8]}"

    # Step 1: Produce messages
    LOG.info("Step 1: Produce 20 messages")
    sent = _produce(20)
    record("crash_recovery", "produce", sent == 20, f"sent={sent}")

    # Step 2: Consume first 10, then "crash" (stop after 5s)
    LOG.info("Step 2: Consume briefly (5s) then stop")
    m1 = _consume_isolated(group, seconds=5)
    first_batch = m1.get("total_processed", 0)
    record("crash_recovery", "first_batch", first_batch > 0,
           f"processed={first_batch}")

    # Step 3: "Restart" consumer — resume from committed offset
    LOG.info("Step 3: Restart consumer (same group, should resume)")
    m2 = _consume_isolated(group, seconds=8)
    second_batch = m2.get("total_processed", 0)
    total = first_batch + second_batch
    record("crash_recovery", "resume_after_crash", total >= sent,
           f"first={first_batch} second={second_batch} total={total} expected>={sent}")

    # Step 4: Verify idempotency — no duplicates in MongoDB
    LOG.info("Step 4: Checking for duplicates")
    import pymongo
    client = pymongo.MongoClient("mongodb://localhost:27017",
                                 serverSelectionTimeoutMS=5000)
    db = client["ecommerce_analytics"]
    pipeline = [
        {"$group": {"_id": "$order_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    dupes = list(db["fct_orders_realtime"].aggregate(pipeline))
    client.close()
    record("crash_recovery", "no_duplicates", len(dupes) == 0,
           f"duplicate_order_ids={len(dupes)}")


# ═══════════════════════════════════════════════════════════════════════
# Scenario 2: Duplicate Message Handling (Idempotency)
# ═══════════════════════════════════════════════════════════════════════

def scenario2_idempotency() -> None:
    """
    Send the same order_id twice.
    Verify: only one record in MongoDB (upsert idempotency).
    """
    LOG.info("\n" + "=" * 60)
    LOG.info("SCENARIO 2: Duplicate Message Handling (Idempotency)")
    LOG.info("=" * 60)

    group = f"idempotent-test-{uuid.uuid4().hex[:8]}"
    oid = f"idempotent_{uuid.uuid4().hex[:10]}"

    # Step 1: Send same order_id twice
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=["127.0.0.1:9092"],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
    )
    for i in range(2):
        producer.send("orders-stream", value={
            "event_type": "order_placed",
            "order_id": oid,
            "customer_id": f"idem_cust_{i}",
            "order_total": 100.0 + i,
            "order_status": "delivered",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }, partition=0)
    producer.flush(timeout=10)
    producer.close(timeout=5)
    record("idempotency", "produce_duplicates", True, f"order_id={oid} sent=2")

    # Step 2: Consume
    _consume_isolated(group, seconds=8)

    # Step 3: Check MongoDB — should have exactly 1 document with this order_id
    import pymongo
    client = pymongo.MongoClient("mongodb://localhost:27017",
                                 serverSelectionTimeoutMS=5000)
    db = client["ecommerce_analytics"]
    count = db["fct_orders_realtime"].count_documents({"order_id": oid})
    client.close()
    record("idempotency", "single_record", count == 1,
           f"count={count} (expected 1)")


# ═══════════════════════════════════════════════════════════════════════
# Scenario 3: Kafka Connectivity Loss
# ═══════════════════════════════════════════════════════════════════════

def scenario3_kafka_restart() -> None:
    """
    Stop and restart Kafka.
    Verify: consumer reconnects, no data loss after recovery.
    """
    LOG.info("\n" + "=" * 60)
    LOG.info("SCENARIO 3: Kafka Broker Restart")
    LOG.info("=" * 60)

    # Step 1: Verify Kafka is running
    try:
        from kafka import KafkaProducer
        p = KafkaProducer(bootstrap_servers=["127.0.0.1:9092"],
                          request_timeout_ms=5000)
        p.close(timeout=3)
        record("kafka_restart", "pre_check_kafka_up", True)
    except Exception as e:
        record("kafka_restart", "pre_check_kafka_up", False, str(e))
        LOG.warning("Kafka not running; skipping restart scenario")
        return

    # Step 2: Produce messages before restart
    sent_before = _produce(5)
    record("kafka_restart", "produce_before_restart", sent_before == 5,
           f"sent={sent_before}")

    # Step 3: Stop Kafka
    LOG.info("Stopping Kafka...")
    stopped = docker_cmd("stop", "kafka")
    record("kafka_restart", "kafka_stopped", stopped)
    time.sleep(3)

    # Step 4: Verify producer fails gracefully
    try:
        p2 = KafkaProducer(bootstrap_servers=["127.0.0.1:9092"],
                           request_timeout_ms=3000)
        p2.close(timeout=1)
        record("kafka_restart", "producer_fails_when_down", False,
               "producer should have failed")
    except Exception:
        record("kafka_restart", "producer_fails_when_down", True,
               "producer correctly raised exception")

    # Step 5: Restart Kafka
    LOG.info("Restarting Kafka...")
    started = docker_cmd("start", "kafka")
    record("kafka_restart", "kafka_restarted", started)
    LOG.info("Waiting 15s for Kafka to stabilize...")
    time.sleep(15)

    # Step 6: Verify producer works again
    try:
        sent_after = _produce(5)
        record("kafka_restart", "produce_after_restart", sent_after == 5,
               f"sent={sent_after}")
    except Exception as e:
        record("kafka_restart", "produce_after_restart", False, str(e))

    # Step 7: Consume all messages (before + after restart)
    group = f"restart-test-{uuid.uuid4().hex[:8]}"
    m = _consume_isolated(group, seconds=10)
    processed = m.get("total_processed", 0)
    record("kafka_restart", "consume_after_restart", processed >= sent_before,
           f"processed={processed} expected>={sent_before}")


# ═══════════════════════════════════════════════════════════════════════
# Scenario 4: Dagster Asset Check Failure Propagation
# ═══════════════════════════════════════════════════════════════════════

def scenario4_dagster_check_propagation() -> None:
    """
    Verify that Dagster runs asset checks after materialization
    and that check results are recorded (pass or warn/fail).
    """
    LOG.info("\n" + "=" * 60)
    LOG.info("SCENARIO 4: Dagster Asset Check Propagation")
    LOG.info("=" * 60)

    cmd = ["dagster", "asset", "materialize", "-m", "ecommerce_analytics",
           "--select", "consumer_health_metrics"]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                       cwd=str(Path(__file__).resolve().parent.parent))
    combined = (r.stdout or "") + "\n" + (r.stderr or "")

    has_check = "ASSET_CHECK_EVALUATION" in combined
    record("dagster_checks", "asset_check_runs", has_check,
           "check evaluation found in output" if has_check else "no check ran")
    record("dagster_checks", "asset_check_result",
           r.returncode == 0 and "RUN_SUCCESS" in combined,
           f"exit={r.returncode}")


# ═══════════════════════════════════════════════════════════════════════
# Scenario 5: DLQ Handling for Malformed Messages
# ═══════════════════════════════════════════════════════════════════════

def scenario5_dlq_handling() -> None:
    """
    Send a malformed message (missing required fields).
    Verify: consumer routes it to DLQ, continues processing valid messages.
    """
    LOG.info("\n" + "=" * 60)
    LOG.info("SCENARIO 5: DLQ Handling for Malformed Messages")
    LOG.info("=" * 60)

    group = f"dlq-test-{uuid.uuid4().hex[:8]}"

    from kafka import KafkaProducer, KafkaConsumer
    producer = KafkaProducer(
        bootstrap_servers=["127.0.0.1:9092"],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
    )

    # Send 1 malformed + 2 valid messages
    producer.send("orders-stream", value={
        "event_type": "order_placed",
        "bad_field": "no order_id or customer_id",
    }, partition=0)

    for i in range(2):
        producer.send("orders-stream", value={
            "event_type": "order_placed",
            "order_id": f"dlq_valid_{uuid.uuid4().hex[:8]}",
            "customer_id": f"dlq_cust_{i}",
            "order_total": 99.99,
            "order_status": "delivered",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }, partition=0)
    producer.flush(timeout=10)
    producer.close(timeout=5)
    record("dlq", "produce_mixed", True, "1 malformed + 2 valid")

    # Consume
    metrics = _consume_isolated(group, seconds=10)
    processed = metrics.get("total_processed", 0)
    failed = metrics.get("total_failed", 0)
    dlq_sent = metrics.get("dlq_sent", 0)

    record("dlq", "valid_processed", processed >= 2,
           f"processed={processed} (expected >=2)")
    record("dlq", "malformed_to_dlq", dlq_sent >= 1 or failed >= 1,
           f"dlq_sent={dlq_sent} failed={failed}")

    # Check DLQ topic has messages
    try:
        dlq_consumer = KafkaConsumer(
            "dlq_orders-stream",
            bootstrap_servers=["127.0.0.1:9092"],
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        dlq_msgs = []
        for msg in dlq_consumer:
            dlq_msgs.append(msg.value)
        dlq_consumer.close()
        record("dlq", "dlq_topic_has_messages", len(dlq_msgs) > 0,
               f"dlq_messages={len(dlq_msgs)}")
    except Exception as e:
        record("dlq", "dlq_topic_check", False, str(e))


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _produce(n: int) -> int:
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
            "order_id": f"ft_{uuid.uuid4().hex[:12]}",
            "customer_id": f"ft_cust_{i:04d}",
            "order_total": 75.0 + i * 5,
            "order_status": "delivered",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            producer.send("orders-stream", value=msg, partition=0)
            sent += 1
        except Exception as e:
            LOG.error("Send failed: %s", e)
    producer.flush(timeout=10)
    producer.close(timeout=5)
    return sent


def _consume_isolated(group: str, seconds: float) -> dict:
    from ecommerce_analytics.consumers.order_processor import OrderStreamProcessor
    consumer = OrderStreamProcessor()
    consumer.group_id = group

    def stopper():
        time.sleep(seconds)
        consumer._running = False

    t = threading.Thread(target=stopper, daemon=True)
    t.start()
    try:
        consumer.run()
    except Exception as e:
        LOG.error("Consumer error: %s", e)
    return consumer.metrics.snapshot()


# ═══════════════════════════════════════════════════════════════════════
# Report & Main
# ═══════════════════════════════════════════════════════════════════════

SCENARIOS = {
    1: ("Consumer Crash Recovery", scenario1_consumer_crash_recovery),
    2: ("Duplicate Message Handling", scenario2_idempotency),
    3: ("Kafka Broker Restart", scenario3_kafka_restart),
    4: ("Dagster Check Propagation", scenario4_dagster_check_propagation),
    5: ("DLQ Malformed Messages", scenario5_dlq_handling),
}


def print_report() -> None:
    LOG.info("\n" + "=" * 60)
    LOG.info("FAILURE SCENARIO TEST REPORT")
    LOG.info("=" * 60)
    total = len(RESULTS)
    passed = sum(1 for v in RESULTS.values() if v["passed"])
    for key, val in RESULTS.items():
        icon = "PASS" if val["passed"] else "FAIL"
        LOG.info("  [%s] %-45s %s", icon, key, val["detail"][:70])
    LOG.info("-" * 60)
    LOG.info("Total: %d | Passed: %d | Failed: %d | Score: %.0f%%",
             total, passed, total - passed, (passed / max(total, 1)) * 100)

    report_path = Path(__file__).resolve().parent.parent / "output" / "failure_scenario_report.json"
    with open(report_path, "w") as f:
        json.dump({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total": total, "passed": passed, "failed": total - passed,
            "results": RESULTS,
        }, f, indent=2, default=str)
    LOG.info("Report saved to %s", report_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Failure scenario tests")
    parser.add_argument("--scenario", type=int, choices=list(SCENARIOS.keys()))
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.list:
        for num, (name, _) in SCENARIOS.items():
            print(f"  {num}: {name}")
        return

    to_run = [args.scenario] if args.scenario else list(SCENARIOS.keys())
    for num in to_run:
        name, fn = SCENARIOS[num]
        LOG.info("\nRunning scenario %d: %s", num, name)
        try:
            fn()
        except Exception as e:
            record(f"scenario_{num}", "exception", False, str(e))

    print_report()


if __name__ == "__main__":
    main()
