# Phase 3F: Kafka Consumer Architecture — Stream Processing with Idempotent Writes

> Four purpose-built Kafka consumers process order events, compute windowed metrics, track A/B experiments, and monitor delivery SLAs — all writing to MongoDB with idempotent upserts, manual offset commits, and dead-letter queue routing for fault tolerance.

---

## Overview

The streaming layer runs independently from Dagster. Kafka consumers are long-running Python processes that continuously poll topics, process messages, and write results to MongoDB. Dagster's role is to observe, snapshot, and validate this data (Phase 3G) — not to manage consumer lifecycles.

This separation of concerns mirrors production architectures at companies running streaming alongside orchestration: Flink or standalone consumers handle the hot path, while Dagster or Airflow handles scheduling, validation, and lineage.

---

## Architecture

```
Kafka Topics                   Consumers (Python)              MongoDB Collections
─────────────                  ──────────────────              ───────────────────
orders-stream    ────────────> OrderStreamProcessor ─────────> fct_orders_realtime
                                                    ├───────> orders (operational)

orders-stream ──┐
                ├────────────> MetricsAggregator ───────────> realtime_metrics
clickstream ────┘              (5-min tumbling windows)

orders-stream ──┐
                ├────────────> ExperimentTracker ───────────> experiments_realtime
clickstream ────┘              (funnel tracking + z-test)

deliveries-stream ──────────> DeliverySLAMonitor ───────────> delivery_events
                                                  ├─────────> delivery_alerts
```

---

## Consumer Details

### 1. OrderStreamProcessor

The primary consumer. Processes every order event into both an operational store and an analytics-ready collection.

| Property | Value |
|----------|-------|
| **Topic** | `orders-stream` |
| **Consumer Group** | `order-processors` |
| **Sinks** | MongoDB `orders` (operational) + `fct_orders_realtime` (analytics) |
| **Enrichment** | Customer lookup from `dim_customers` collection (cached 1 hour) |
| **Idempotency** | Upsert by `order_id` — duplicates overwrite, not accumulate |

> **Why dual writes:** The `orders` collection serves the API layer (current order status, customer-facing). The `fct_orders_realtime` collection feeds the analytics pipeline (Dagster realtime assets, BigQuery stream tables). Different schemas, different access patterns, different retention policies.

### 2. MetricsAggregator

Computes real-time business metrics using 5-minute tumbling windows.

| Property | Value |
|----------|-------|
| **Topics** | `orders-stream`, `clickstream` |
| **Consumer Group** | `metrics-aggregator` |
| **Sink** | MongoDB `realtime_metrics` |
| **Windowing** | 5-minute tumbling windows |
| **Metrics** | `total_orders`, `total_gmv`, `unique_sessions`, `conversion_rate`, `avg_order_value` |

> **Why 5-minute windows:** This balances freshness against computation cost. One-minute windows produce 12x more documents with marginal freshness improvement. Five minutes aligns with typical dashboard refresh intervals.

### 3. ExperimentTracker

Tracks the impression-to-click-to-conversion funnel for A/B tests with real-time statistical significance.

| Property | Value |
|----------|-------|
| **Topics** | `orders-stream`, `clickstream` |
| **Consumer Group** | `experiment-tracker` |
| **Sink** | MongoDB `experiments_realtime` |
| **Processing** | Joins clickstream impressions to order conversions |
| **Statistics** | Z-test for proportions; alerts when p < 0.05 and sample > 5,000 |

### 4. DeliverySLAMonitor

Watches delivery events and generates alerts when shipments exceed SLA thresholds.

| Property | Value |
|----------|-------|
| **Topic** | `deliveries-stream` |
| **Consumer Group** | `delivery-monitor` |
| **Sinks** | MongoDB `delivery_events` + `delivery_alerts` |
| **Alert threshold** | Actual delivery > estimated + 3 days |

---

## Shared Patterns (BaseConsumer)

All four consumers extend `BaseConsumer`, which encapsulates the production-grade patterns that differentiate a portfolio project from a tutorial:

### Manual Offset Commits

```python
# Offsets committed ONLY after successful processing + MongoDB write
consumer.commit()
```

**Why not auto-commit:** With `enable_auto_commit=True`, Kafka commits offsets on a timer regardless of processing success. If the consumer crashes after commit but before MongoDB write, messages are lost. Manual commits implement **at-least-once** semantics.

### Dead-Letter Queue (DLQ)

Malformed messages are routed to `dlq_{topic}` topics instead of blocking the consumer.

| Scenario | Action |
|----------|--------|
| Malformed JSON | Route to DLQ, log error, continue |
| Missing required fields | Route to DLQ, log error, continue |
| MongoDB write failure | Retry 3x with backoff, then DLQ |

> **Why DLQ over skip-and-log:** DLQ messages can be reprocessed after the root cause is fixed. Simply logging and skipping means the data is gone unless someone manually replays from Kafka.

### Retry with Exponential Backoff

Failed operations retry 3 times with delays of 1s, 2s, and 4s. This handles transient MongoDB connection issues and network hiccups without overwhelming the database during an outage.

### Graceful Shutdown

SIGINT/SIGTERM signals set `_running = False`, allowing the current batch to complete before the consumer exits. This prevents partial writes and uncommitted offsets.

### Metrics Tracking

`ConsumerMetrics` tracks per-consumer:
- Messages processed (total and per-second throughput)
- Failed message count
- DLQ routed count
- Processing latency (mean, p95, p99)

---

## Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Local Kafka broker |
| `MONGO_URI` | `mongodb://localhost:27017` | Local MongoDB |
| `MAX_POLL_RECORDS` | 100 | Batch size per poll; balances throughput and latency |
| `POLL_TIMEOUT_MS` | 10,000 | 10s poll timeout; prevents tight loops on empty topics |
| `MAX_RETRIES` | 3 | Retry budget before DLQ routing |
| `WINDOW_SIZE_SECONDS` | 300 | 5-minute tumbling windows for MetricsAggregator |
| `CACHE_REFRESH` | 3,600 | 1-hour cache for dimension lookups (OrderStreamProcessor) |

---

## Error Handling Matrix

| Failure Mode | Detection | Response | Recovery |
|-------------|-----------|----------|----------|
| Malformed message | JSON parse / schema validation | Route to DLQ | Fix producer, replay DLQ |
| MongoDB write failure | Write exception | Retry 3x with backoff | Auto-recovers on transient failures |
| Consumer crash | Process exit | OS/Docker restarts process | Resumes from last committed offset |
| Kafka broker down | Poll timeout | Log warning, keep polling | Auto-reconnects when broker returns |
| Duplicate messages | N/A (expected) | Upsert by `order_id` | Idempotent; no data corruption |
| Consumer lag buildup | Kafka admin API | Dagster sensor triggers snapshot | Increased parallelism or new consumer instances |

> **Why idempotent writes are non-negotiable:** In distributed systems, messages can be delivered more than once (network retries, consumer restarts, rebalancing). Upsert-by-key ensures that processing the same message twice produces the same result. Without idempotency, duplicate messages cause inflated metrics — a silent, hard-to-debug failure mode.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Consumer runtime | Standalone Python processes | Kafka consumers are long-running; Dagster ops are finite |
| Offset management | Manual commit after processing | At-least-once delivery; no message loss |
| Error handling | DLQ + retry with backoff | Fault isolation; bad messages don't block good ones |
| Idempotency | Upsert by natural key | Handles duplicates from rebalancing and retries |
| Dimension caching | 1-hour TTL cache | Avoids per-message MongoDB lookups; acceptable staleness |
| Windowing | 5-minute tumbling | Balances freshness and computation cost |

---

## Production Considerations

- **Scaling consumers:** Each consumer group can have multiple instances (one per Kafka partition). With 3 partitions on `orders-stream`, up to 3 `OrderStreamProcessor` instances can run in parallel.
- **Exactly-once semantics:** The current design achieves at-least-once with idempotent writes (effectively exactly-once for upserts). True exactly-once requires Kafka transactions, which add latency and complexity.
- **Consumer lag monitoring:** Kafka consumer group lag is the primary health metric. Lag > 5,000 messages triggers Dagster's `kafka_consumer_health_sensor` (Phase 3G). Lag > 10,000 triggers the critical alert sensor (Phase 3K).
- **Schema evolution:** Consumers validate against expected fields and drop unknowns with logging. Adding new fields requires: (1) add nullable column to MongoDB/BigQuery, (2) update consumer, (3) deploy.
- **Cost at scale:** MongoDB write throughput is the bottleneck. At 10,000 msg/sec, consider batched writes (bulk upsert) or switching to BigQuery's Storage Write API for the analytics sink.
- **Observability:** Consumer metrics should be exported to a monitoring system (Prometheus/Grafana) for alerting on throughput drops, latency spikes, and DLQ growth.

---

*Last updated: March 2026*
