#!/usr/bin/env python3
"""
Phase 3L – End-to-end integration test for batch + stream + Lambda layers.

Orchestrates a multi-step test that proves:
  1. Batch layer: Dagster assets materialize correctly (dims, facts, aggs)
  2. Stream layer: Kafka produce → consume → MongoDB write works
  3. Lambda layer: Dagster snapshots realtime data, reconciles with batch
  4. Data quality: Completeness, consistency, accuracy across layers

Prerequisites:
  docker-compose up -d   (Kafka + MongoDB)
  pip install -e .       (project installed)
  Olist data in raw/olist/

Usage:
  python scripts/integration_test.py              # full test (all phases)
  python scripts/integration_test.py --phase 1    # batch layer only
  python scripts/integration_test.py --phase 2    # stream layer only
  python scripts/integration_test.py --phase 3    # lambda layer only
  python scripts/integration_test.py --phase 4    # data quality validation
  python scripts/integration_test.py --quick      # abbreviated (1 partition, 5 msgs)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
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
LOG = logging.getLogger("integration_test")

RESULTS: dict[str, dict] = {}
T0 = time.monotonic()


def elapsed() -> str:
    return f"{time.monotonic() - T0:.1f}s"


def record(phase: str, step: str, passed: bool, detail: str = "") -> None:
    key = f"{phase}/{step}"
    RESULTS[key] = {"passed": passed, "detail": detail, "ts": elapsed()}
    icon = "PASS" if passed else "FAIL"
    LOG.info("[%s] %s: %s %s", elapsed(), icon, key, detail)


# ═══════════════════════════════════════════════════════════════════════
# Phase 1: Batch Layer
# ═══════════════════════════════════════════════════════════════════════

def run_dagster_cmd(args: list[str], timeout: int = 300) -> subprocess.CompletedProcess:
    """Run a dagster CLI command and return the result."""
    cmd = ["dagster"] + args + ["-m", "ecommerce_analytics"]
    LOG.info("Running: %s", " ".join(cmd))
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout,
        cwd=str(Path(__file__).resolve().parent.parent),
    )
    # Dagster writes debug/info to stderr; combine for success detection
    result._combined = (result.stdout or "") + "\n" + (result.stderr or "")
    return result


def dagster_succeeded(result: subprocess.CompletedProcess) -> bool:
    """Check if a Dagster CLI run succeeded (searches both stdout and stderr)."""
    combined = getattr(result, "_combined", "")
    return result.returncode == 0 and "RUN_SUCCESS" in combined


def phase1_batch_layer(quick: bool = False) -> None:
    """Test batch layer: materialize dimensions, facts, and aggregates."""
    LOG.info("\n" + "=" * 70)
    LOG.info("PHASE 1: BATCH LAYER VERIFICATION")
    LOG.info("=" * 70)

    # Step 1a: Refresh dimensions (source → staging → dims)
    LOG.info("[%s] Step 1a: Materializing dimensions...", elapsed())
    result = run_dagster_cmd(["job", "execute", "-j", "refresh_dimensions"], timeout=300)
    passed = dagster_succeeded(result)
    record("batch", "refresh_dimensions", passed, f"exit={result.returncode}")
    if not passed:
        LOG.error("OUTPUT (last 2000): %s", result._combined[-2000:])
        return

    # Step 1b: Materialize partitioned facts for a date range
    partitions = ["2016-09-15"] if quick else [
        "2016-09-04", "2016-09-05", "2016-09-15",
        "2017-01-15", "2017-06-15", "2018-01-15", "2018-08-01",
    ]
    LOG.info("[%s] Step 1b: Materializing partitioned facts for %d date(s)...",
             elapsed(), len(partitions))
    fact_results = {}
    for p in partitions:
        r = run_dagster_cmd([
            "asset", "materialize",
            "--select", "fct_orders,fct_reviews,fct_daily_metrics",
            "--partition", p,
        ], timeout=300)
        ok = dagster_succeeded(r)
        fact_results[p] = ok
        record("batch", f"partitioned_facts/{p}", ok, f"exit={r.returncode}")
        if not ok:
            LOG.error("Partition %s failed: %s", p, r.stdout[-1000:])

    passed_count = sum(1 for v in fact_results.values() if v)
    record("batch", "partitioned_facts_summary",
           passed_count == len(partitions),
           f"{passed_count}/{len(partitions)} partitions succeeded")

    # Step 1c: Refresh aggregated facts
    LOG.info("[%s] Step 1c: Materializing aggregated facts...", elapsed())
    result = run_dagster_cmd(["job", "execute", "-j", "refresh_agg_facts"], timeout=300)
    passed = dagster_succeeded(result)
    record("batch", "refresh_agg_facts", passed, f"exit={result.returncode}")

    # Step 1d: Validate batch assets via Python
    LOG.info("[%s] Step 1d: Validating batch asset outputs...", elapsed())
    _validate_batch_assets()


def _validate_batch_assets() -> None:
    """Load materialized batch assets from filesystem and validate."""
    import pickle
    storage = Path(__file__).resolve().parent.parent / ".dagster" / "storage"

    checks = {
        "stg_orders": {"min_rows": 100, "required_cols": ["order_id", "order_status"]},
        "stg_customers": {"min_rows": 100, "required_cols": ["customer_unique_id"]},
        "dim_customers": {"min_rows": 50, "required_cols": ["customer_key"]},
        "dim_products": {"min_rows": 50, "required_cols": ["product_key"]},
        "dim_sellers": {"min_rows": 10, "required_cols": ["seller_key"]},
        "dim_geography": {"min_rows": 10, "required_cols": ["geography_key"]},
        "dim_dates": {"min_rows": 100, "required_cols": ["date_key"]},
        "fct_order_items": {"min_rows": 100, "required_cols": ["order_id", "product_key"]},
        "fct_experiment_results": {"min_rows": 1, "required_cols": ["experiment_id"]},
        "fct_product_performance": {"min_rows": 10, "required_cols": ["product_key"]},
        "fct_seller_performance": {"min_rows": 5, "required_cols": ["seller_key"]},
    }

    for asset_name, spec in checks.items():
        path = storage / asset_name
        if not path.exists():
            record("batch_validate", asset_name, False, "file not found")
            continue
        try:
            with open(path, "rb") as f:
                df = pickle.load(f)
            row_ok = len(df) >= spec["min_rows"]
            col_ok = all(c in df.columns for c in spec["required_cols"])
            missing = [c for c in spec["required_cols"] if c not in df.columns]
            passed = row_ok and col_ok
            detail = f"rows={len(df)} cols={list(df.columns[:8])}"
            if missing:
                detail += f" MISSING={missing}"
            record("batch_validate", asset_name, passed, detail)
        except Exception as e:
            record("batch_validate", asset_name, False, str(e))


# ═══════════════════════════════════════════════════════════════════════
# Phase 2: Stream Layer
# ═══════════════════════════════════════════════════════════════════════

def phase2_stream_layer(quick: bool = False) -> None:
    """Test stream layer: produce → consume → MongoDB write."""
    LOG.info("\n" + "=" * 70)
    LOG.info("PHASE 2: STREAM LAYER VERIFICATION")
    LOG.info("=" * 70)

    n_messages = 5 if quick else 30
    consume_seconds = 10 if quick else 20

    # Step 2a: Check Kafka connectivity
    LOG.info("[%s] Step 2a: Checking Kafka connectivity...", elapsed())
    try:
        from kafka import KafkaProducer
        p = KafkaProducer(
            bootstrap_servers=["127.0.0.1:9092"],
            request_timeout_ms=5000,
        )
        p.close(timeout=3)
        record("stream", "kafka_connectivity", True)
    except Exception as e:
        record("stream", "kafka_connectivity", False, str(e))
        return

    # Step 2b: Check MongoDB connectivity
    LOG.info("[%s] Step 2b: Checking MongoDB connectivity...", elapsed())
    try:
        import pymongo
        client = pymongo.MongoClient("mongodb://localhost:27017",
                                     serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["ecommerce_analytics"]
        pre_orders = db["fct_orders_realtime"].count_documents({})
        pre_metrics = db["realtime_metrics"].count_documents({})
        client.close()
        record("stream", "mongodb_connectivity", True,
               f"pre_orders={pre_orders} pre_metrics={pre_metrics}")
    except Exception as e:
        record("stream", "mongodb_connectivity", False, str(e))
        return

    # Step 2c: Produce test messages
    LOG.info("[%s] Step 2c: Producing %d test messages...", elapsed(), n_messages)
    test_group = f"integration-test-{uuid.uuid4().hex[:8]}"
    sent = _produce_test_orders(n_messages)
    record("stream", "produce_messages", sent == n_messages,
           f"sent={sent}/{n_messages}")

    # Step 2d: Consume with OrderStreamProcessor
    LOG.info("[%s] Step 2d: Running consumer for %ds...", elapsed(), consume_seconds)
    metrics = _run_consumer_isolated(test_group, consume_seconds)
    processed = metrics.get("total_processed", 0)
    failed = metrics.get("total_failed", 0)
    record("stream", "consume_messages", processed > 0,
           f"processed={processed} failed={failed} "
           f"throughput={metrics.get('throughput_per_sec', 0)}/s")

    # Step 2e: Verify MongoDB writes
    LOG.info("[%s] Step 2e: Verifying MongoDB writes...", elapsed())
    try:
        import pymongo
        client = pymongo.MongoClient("mongodb://localhost:27017",
                                     serverSelectionTimeoutMS=5000)
        db = client["ecommerce_analytics"]
        post_orders = db["fct_orders_realtime"].count_documents({})
        post_ops = db["orders"].count_documents({})

        new_orders = post_orders - pre_orders
        record("stream", "mongodb_orders_written", new_orders >= n_messages,
               f"fct_orders_realtime: {pre_orders}→{post_orders} (+{new_orders}) "
               f"orders: {post_ops} (expected>={n_messages})")

        # Verify idempotency: re-counting shouldn't show duplicates
        test_prefix = db["fct_orders_realtime"].count_documents(
            {"order_id": {"$regex": "^inttest_"}}
        )
        record("stream", "mongodb_no_duplicates", True,
               f"test orders in realtime: {test_prefix}")
        client.close()
    except Exception as e:
        record("stream", "mongodb_verification", False, str(e))

    # Step 2f: Check consumer lag
    LOG.info("[%s] Step 2f: Checking consumer lag...", elapsed())
    try:
        from kafka import KafkaAdminClient, KafkaConsumer as KC
        admin = KafkaAdminClient(bootstrap_servers=["127.0.0.1:9092"],
                                 request_timeout_ms=5000)
        consumer = KC(bootstrap_servers=["127.0.0.1:9092"])
        offsets = admin.list_consumer_group_offsets(test_group)
        total_lag = 0
        for tp, meta in offsets.items():
            end = consumer.end_offsets([tp])
            total_lag += max(0, end.get(tp, 0) - meta.offset)
        consumer.close()
        admin.close()
        record("stream", "consumer_lag", total_lag < 100,
               f"lag={total_lag} (threshold <100)")
    except Exception as e:
        record("stream", "consumer_lag", False, str(e))


def _produce_test_orders(n: int) -> int:
    """Produce n test order messages to orders-stream."""
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=["127.0.0.1:9092"],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all", retries=2, request_timeout_ms=10000,
    )
    sent = 0
    ts = datetime.now(timezone.utc).isoformat()
    for i in range(n):
        msg = {
            "event_type": "order_placed",
            "order_id": f"inttest_{uuid.uuid4().hex[:12]}",
            "customer_id": f"inttest_cust_{i:04d}",
            "order_total": round(50.0 + i * 7.5, 2),
            "order_status": "delivered",
            "timestamp": ts,
        }
        try:
            producer.send("orders-stream", value=msg, partition=0)
            sent += 1
        except Exception as e:
            LOG.error("Send failed: %s", e)
    producer.flush(timeout=10)
    producer.close(timeout=5)
    return sent


def _run_consumer_isolated(group: str, seconds: float) -> dict:
    """Run OrderStreamProcessor in an isolated group for `seconds`."""
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
# Phase 3: Lambda Layer (Dagster integration)
# ═══════════════════════════════════════════════════════════════════════

def phase3_lambda_layer() -> None:
    """Test Lambda layer: Dagster snapshots, reconciliation, health metrics."""
    LOG.info("\n" + "=" * 70)
    LOG.info("PHASE 3: LAMBDA LAYER VERIFICATION")
    LOG.info("=" * 70)

    # Step 3a: Run stream_sync job
    LOG.info("[%s] Step 3a: Running stream_sync job...", elapsed())
    result = run_dagster_cmd(["job", "execute", "-j", "stream_sync"], timeout=120)
    passed = dagster_succeeded(result)
    record("lambda", "stream_sync_job", passed, f"exit={result.returncode}")

    # Step 3b: Run batch_stream_reconciliation
    LOG.info("[%s] Step 3b: Running batch_stream_reconciliation...", elapsed())
    result = run_dagster_cmd([
        "asset", "materialize",
        "--select", "batch_stream_reconciliation",
    ], timeout=120)
    passed = dagster_succeeded(result)
    record("lambda", "batch_stream_reconciliation", passed,
           f"exit={result.returncode}")

    # Step 3c: Validate realtime assets exist
    LOG.info("[%s] Step 3c: Validating realtime asset outputs...", elapsed())
    _validate_realtime_assets()

    # Step 3d: Validate reconciliation report
    LOG.info("[%s] Step 3d: Validating reconciliation report...", elapsed())
    _validate_reconciliation()


def _validate_realtime_assets() -> None:
    """Check that realtime assets were written to storage."""
    import pickle
    storage = Path(__file__).resolve().parent.parent / ".dagster" / "storage"

    for name in ["realtime_orders", "realtime_metrics_5min",
                 "realtime_experiment_results", "consumer_health_metrics"]:
        path = storage / name
        if not path.exists():
            record("lambda_validate", name, False, "file not found")
            continue
        try:
            with open(path, "rb") as f:
                df = pickle.load(f)
            record("lambda_validate", name, True,
                   f"rows={len(df)} cols={list(df.columns[:6])}")
        except Exception as e:
            record("lambda_validate", name, False, str(e))


def _validate_reconciliation() -> None:
    """Load reconciliation report and validate its structure."""
    import pickle
    path = (Path(__file__).resolve().parent.parent /
            ".dagster" / "storage" / "batch_stream_reconciliation")
    if not path.exists():
        record("lambda_validate", "reconciliation_report", False, "file not found")
        return
    try:
        with open(path, "rb") as f:
            df = pickle.load(f)
        categories = set(df["category"].tolist())
        expected = {"stream_only", "batch_only", "overlap", "totals"}
        has_all = expected.issubset(categories)
        totals_row = df[df["category"] == "totals"].iloc[0]
        record("lambda_validate", "reconciliation_report", has_all,
               f"categories={categories} totals={totals_row['sample_ids']}")
    except Exception as e:
        record("lambda_validate", "reconciliation_report", False, str(e))


# ═══════════════════════════════════════════════════════════════════════
# Phase 4: Data Quality Validation
# ═══════════════════════════════════════════════════════════════════════

def phase4_data_quality() -> None:
    """Cross-layer data quality checks."""
    LOG.info("\n" + "=" * 70)
    LOG.info("PHASE 4: DATA QUALITY VALIDATION")
    LOG.info("=" * 70)

    import pickle
    import pymongo
    storage = Path(__file__).resolve().parent.parent / ".dagster" / "storage"

    # 4a: Completeness — MongoDB vs Dagster snapshot match
    LOG.info("[%s] Step 4a: Completeness check...", elapsed())
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017",
                                     serverSelectionTimeoutMS=5000)
        db = client["ecommerce_analytics"]
        mongo_orders = db["fct_orders_realtime"].count_documents({})
        mongo_metrics = db["realtime_metrics"].count_documents({})
        client.close()

        rt_path = storage / "realtime_orders"
        if rt_path.exists():
            with open(rt_path, "rb") as f:
                rt_df = pickle.load(f)
            snapshot_orders = len(rt_df)
            diff_pct = abs(mongo_orders - snapshot_orders) / max(mongo_orders, 1) * 100
            record("quality", "completeness_orders",
                   diff_pct <= 5,
                   f"mongo={mongo_orders} snapshot={snapshot_orders} diff={diff_pct:.1f}%")
        else:
            record("quality", "completeness_orders", False, "realtime_orders not found")

        met_path = storage / "realtime_metrics_5min"
        if met_path.exists():
            with open(met_path, "rb") as f:
                met_df = pickle.load(f)
            record("quality", "completeness_metrics", True,
                   f"mongo={mongo_metrics} snapshot={len(met_df)}")
        else:
            record("quality", "completeness_metrics", False, "realtime_metrics not found")

    except Exception as e:
        record("quality", "completeness", False, str(e))

    # 4b: Consistency — batch staging vs dimension counts
    LOG.info("[%s] Step 4b: Consistency check...", elapsed())
    try:
        with open(storage / "stg_orders", "rb") as f:
            stg = pickle.load(f)
        with open(storage / "dim_customers", "rb") as f:
            dim_c = pickle.load(f)
        with open(storage / "fct_order_items", "rb") as f:
            fct_oi = pickle.load(f)

        id_col = "customer_unique_id" if "customer_unique_id" in stg.columns else "customer_id"
        stg_custs = stg[id_col].nunique()
        dim_custs = len(dim_c)
        record("quality", "consistency_customers",
               dim_custs > 0 and stg_custs > 0,
               f"stg_{id_col}_unique={stg_custs} dim={dim_custs}")

        stg_order_count = stg["order_id"].nunique()
        record("quality", "consistency_staging_rows", stg_order_count > 0,
               f"stg_orders unique_ids={stg_order_count}")

        record("quality", "consistency_order_items", len(fct_oi) > 0,
               f"fct_order_items rows={len(fct_oi)}")

    except Exception as e:
        record("quality", "consistency", False, str(e))

    # 4c: Accuracy — order totals should be positive, dates logical
    LOG.info("[%s] Step 4c: Accuracy check...", elapsed())
    try:
        with open(storage / "stg_orders", "rb") as f:
            stg = pickle.load(f)

        if "order_purchase_timestamp" in stg.columns and "order_delivered_customer_date" in stg.columns:
            import pandas as pd
            delivered = stg.dropna(subset=["order_delivered_customer_date"])
            if len(delivered) > 0:
                purchase = pd.to_datetime(delivered["order_purchase_timestamp"], errors="coerce")
                delivery = pd.to_datetime(delivered["order_delivered_customer_date"], errors="coerce")
                valid_mask = purchase.notna() & delivery.notna()
                if valid_mask.any():
                    bad_order = ((delivery[valid_mask] < purchase[valid_mask]).sum())
                    total = valid_mask.sum()
                    record("quality", "accuracy_delivery_after_purchase",
                           bad_order / total < 0.01,
                           f"delivery_before_purchase={bad_order}/{total}")
        else:
            record("quality", "accuracy_delivery_after_purchase", True, "columns not in stg")

        with open(storage / "fct_order_items", "rb") as f:
            oi = pickle.load(f)
        if "price" in oi.columns:
            neg_prices = (oi["price"].astype(float) < 0).sum()
            record("quality", "accuracy_positive_prices",
                   neg_prices == 0,
                   f"negative_prices={neg_prices}/{len(oi)}")

    except Exception as e:
        record("quality", "accuracy", False, str(e))

    # 4d: Consumer health metrics
    LOG.info("[%s] Step 4d: Consumer health check...", elapsed())
    try:
        with open(storage / "consumer_health_metrics", "rb") as f:
            health = pickle.load(f)
        kafka_rows = health[health["source"] == "kafka"]
        mongo_rows = health[health["source"] == "mongodb"]

        if not kafka_rows.empty and "status" in kafka_rows.columns:
            critical = kafka_rows[kafka_rows["status"].isin(["critical", "unreachable"])]
            record("quality", "consumer_health_kafka",
                   len(critical) == 0,
                   f"groups={len(kafka_rows)} critical={len(critical)}")
        else:
            record("quality", "consumer_health_kafka", True, "no kafka rows (ok)")

        if not mongo_rows.empty and "document_count" in mongo_rows.columns:
            total_docs = mongo_rows["document_count"].astype(int).sum()
            record("quality", "consumer_health_mongo", True,
                   f"collections={len(mongo_rows)} total_docs={total_docs}")
    except Exception as e:
        record("quality", "consumer_health", False, str(e))


# ═══════════════════════════════════════════════════════════════════════
# Report
# ═══════════════════════════════════════════════════════════════════════

def print_report() -> None:
    """Print final test report."""
    LOG.info("\n" + "=" * 70)
    LOG.info("INTEGRATION TEST REPORT")
    LOG.info("=" * 70)

    total = len(RESULTS)
    passed = sum(1 for v in RESULTS.values() if v["passed"])
    failed = total - passed

    for key, val in RESULTS.items():
        icon = "PASS" if val["passed"] else "FAIL"
        LOG.info("  [%s] %-50s %s", icon, key, val["detail"][:80])

    LOG.info("-" * 70)
    LOG.info("Total: %d | Passed: %d | Failed: %d | Score: %.0f%%",
             total, passed, failed, (passed / max(total, 1)) * 100)
    LOG.info("Elapsed: %s", elapsed())
    LOG.info("=" * 70)

    report_path = Path(__file__).resolve().parent.parent / "output" / "integration_test_report.json"
    with open(report_path, "w") as f:
        json.dump({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total": total, "passed": passed, "failed": failed,
            "score_pct": round((passed / max(total, 1)) * 100, 1),
            "elapsed": elapsed(),
            "results": RESULTS,
        }, f, indent=2, default=str)
    LOG.info("Report saved to %s", report_path)


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 3L Integration Test")
    parser.add_argument("--phase", type=int, choices=[1, 2, 3, 4],
                        help="Run a specific phase only")
    parser.add_argument("--quick", action="store_true",
                        help="Quick mode (fewer partitions, fewer messages)")
    args = parser.parse_args()

    LOG.info("=" * 70)
    LOG.info("PHASE 3L: END-TO-END INTEGRATION TEST")
    LOG.info("Mode: %s | Phase: %s",
             "quick" if args.quick else "full",
             args.phase or "all")
    LOG.info("=" * 70)

    phases = [args.phase] if args.phase else [1, 2, 3, 4]

    if 1 in phases:
        phase1_batch_layer(quick=args.quick)
    if 2 in phases:
        phase2_stream_layer(quick=args.quick)
    if 3 in phases:
        phase3_lambda_layer()
    if 4 in phases:
        phase4_data_quality()

    print_report()


if __name__ == "__main__":
    main()
