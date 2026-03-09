# Phase 3G: Realtime Assets — Hybrid Sensor-Driven Stream Observability

> Dagster realtime assets bridge the gap between continuously-running Kafka consumers and the batch-oriented asset graph by snapshotting MongoDB stream data on demand, triggered by sensors that detect new data or consumer lag.

---

## Overview

Kafka consumers run forever. Dagster assets materialize on demand. These paradigms do not naturally align, and how you bridge them reveals architectural maturity.

This phase implements a **hybrid sensor + micro-batch snapshot** pattern that mirrors how production data platforms at companies like Spotify, Shopify, and Datadog integrate streaming infrastructure with orchestration layers. Consumers own the hot path; Dagster observes, snapshots, and validates.

---

## The Core Design Challenge

| Paradigm | Characteristic | Tool |
|----------|---------------|------|
| **Streaming** | Continuous, infinite, event-driven | Kafka consumers |
| **Orchestration** | Finite, scheduled, batch-oriented | Dagster assets |

Three approaches were evaluated:

| Criteria | Consumer as Dagster Op | Scheduled Micro-Batch | Sensor-Triggered Snapshot |
|----------|----------------------|----------------------|--------------------------|
| **Dagster fit** | Poor — long-running ops fight the asset model | Good — clean asset contract | Good — event-driven |
| **Complexity** | High — lifecycle management inside Dagster | Low — simple cron | Medium — sensor logic |
| **Freshness** | True real-time | Fixed lag (e.g., hourly) | Minutes (configurable) |
| **Portfolio signal** | Shows misuse of Dagster | Shows scheduling | Shows sensors + observability |

### Chosen: Hybrid of Scheduled + Sensor-Triggered

> **Why this matters:** Running a Kafka consumer as a Dagster op is a common anti-pattern. It turns a finite-compute orchestrator into a process manager, fighting against Dagster's design. The hybrid approach demonstrates understanding of where each tool's responsibilities begin and end.

---

## Architecture

```
                   STREAMING LAYER                    ORCHESTRATION LAYER
                   (runs continuously)                (runs on-demand)

Kafka Topics ──> Consumers ──> MongoDB ──> Sensors ──> Dagster Assets
                 (Phase 3F)   Collections    │         (snapshots)
                                             │
                              ┌──────────────┘
                              │
                    Trigger materialization when:
                    - New documents detected (CDC-style)
                    - Consumer lag > 5,000 messages
```

**Separation of concerns:**
- Consumers own their processing state (Kafka offsets, retry logic, DLQ)
- Dagster owns observability (lineage, freshness tracking, quality checks)
- MongoDB serves as the integration boundary between the two systems

---

## Realtime Assets

### realtime_orders

| Property | Value |
|----------|-------|
| **Source** | MongoDB `fct_orders_realtime` collection |
| **Writer** | `OrderStreamProcessor` (Consumer 1) |
| **Grain** | One row per order |
| **Group** | `realtime` |
| **Freshness target** | 30 minutes |

Snapshots all orders written by the order stream processor. Each materialization reads the full collection and produces a DataFrame with the same schema as the batch `fct_orders`, enabling direct comparison and reconciliation.

### realtime_metrics_5min

| Property | Value |
|----------|-------|
| **Source** | MongoDB `realtime_metrics` collection |
| **Writer** | `MetricsAggregator` (Consumer 2) |
| **Grain** | One row per metric per 5-minute window |
| **Group** | `realtime` |
| **Freshness target** | 15 minutes |

Contains windowed aggregations: `total_orders`, `total_gmv`, `unique_sessions`, `conversion_rate`, `avg_order_value`, `total_clicks`. These feed the real-time dashboard panels.

### realtime_experiment_results

| Property | Value |
|----------|-------|
| **Source** | MongoDB `experiments_realtime` collection |
| **Writer** | `ExperimentTracker` (Consumer 3) |
| **Grain** | One row per experiment per variant |
| **Group** | `realtime` |
| **Freshness target** | 30 minutes |

Live A/B test running totals with conversion rates and statistical significance tracking. Compared against batch `fct_experiment_results` for validation.

---

## Sensor Design

### kafka_consumer_health_sensor

| Property | Value |
|----------|-------|
| **Interval** | Every 2 minutes |
| **Logic** | Queries Kafka admin API for consumer group lag across all 4 groups |
| **Trigger condition** | Any group's total lag exceeds 5,000 messages |
| **Action** | Materializes all realtime assets |

> **Why lag-based triggering:** Consumer lag is the single best indicator of stream health. When lag grows, consumers are falling behind — either due to increased traffic, slow processing, or partial failures. Triggering a snapshot captures the current state for downstream analysis and alerting.

### realtime_freshness_sensor

| Property | Value |
|----------|-------|
| **Interval** | Every 5 minutes |
| **Logic** | Polls MongoDB collections for document count changes vs. previous cursor |
| **Trigger condition** | New documents detected in any monitored collection |
| **Action** | Materializes the corresponding asset(s) |

This is a **change-data-capture (CDC) style trigger**: only materialize when there is genuinely new data, avoiding unnecessary computation when streams are idle.

---

## Asset Checks

| Check | Asset | Severity | Validates |
|-------|-------|----------|-----------|
| `check_realtime_orders_not_empty` | `realtime_orders` | WARN | Collection has > 0 rows |
| `check_realtime_orders_positive_totals` | `realtime_orders` | ERROR | No negative `order_total` values |
| `check_realtime_orders_consistency` | `realtime_orders` | WARN | Snapshot count matches MongoDB count within 5% |
| `check_realtime_metrics_not_empty` | `realtime_metrics_5min` | WARN | Metrics collection has data |
| `check_experiment_rates_valid` | `realtime_experiment_results` | ERROR | Conversion rates between 0-100% |

> **Why the consistency check matters:** If the Dagster snapshot has significantly fewer rows than MongoDB, it indicates either a failed read or a race condition during materialization. The 5% tolerance accounts for new documents arriving during the snapshot.

---

## Kafka Offset Management

Offsets are managed by **Kafka itself**, not Dagster:

1. Consumers use `enable_auto_commit=False` (manual commits)
2. After successful processing + MongoDB write, the consumer calls `consumer.commit()`
3. Offsets stored in Kafka's internal `__consumer_offsets` topic
4. On restart, consumers resume from the last committed offset

**Dagster does not manage Kafka offsets.** This is intentional. The consumers own their processing state. Dagster observes the results. Mixing offset management across systems creates a distributed coordination problem that adds complexity without benefit.

---

## Data Flow: End-to-End

```
Kafka Topics               Consumers              MongoDB                 Dagster
────────────               ─────────              ───────                 ──────

orders-stream ───────────> OrderStreamProcessor ──> fct_orders_realtime ──> realtime_orders
                                                 └> orders (operational)

orders-stream ──┐
                ├────────> MetricsAggregator ─────> realtime_metrics ─────> realtime_metrics_5min
clickstream ────┘          (5-min windows)

orders-stream ──┐
                ├────────> ExperimentTracker ─────> experiments_realtime ──> realtime_experiment_results
clickstream ────┘          (running z-test)

deliveries-stream ──────> DeliverySLAMonitor ────> delivery_alerts
                                                 └> delivery_events

                                                          ^
              Sensors (poll MongoDB + Kafka lag) ─────────┘
```

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Consumer lifecycle | Outside Dagster | Kafka consumers are infinite; Dagster ops are finite |
| Trigger mechanism | Sensors (CDC + lag) | Event-driven; avoids unnecessary materialization |
| Snapshot strategy | Full collection read | Simple; MongoDB collections are small (<100K docs) |
| Freshness targets | 15-30 minutes | Appropriate for analytics; not a real-time serving layer |
| Asset grouping | `realtime` group | Isolates from batch assets in Dagit UI and job selection |

---

## Production Considerations

- **Snapshot size limits:** Full collection reads work at current scale. At 10M+ documents, switch to incremental reads with a high-water-mark timestamp filter.
- **Sensor resource usage:** Sensors poll MongoDB and Kafka on short intervals. Ensure connection pooling to avoid exhausting database connections.
- **Freshness SLA mismatch:** If the dashboard requires sub-second freshness, it should query MongoDB directly. Dagster realtime assets provide minutes-level freshness, suitable for analytics, not real-time serving.
- **Multi-environment sensors:** In production, sensor configuration (MongoDB URI, Kafka bootstrap) should be injected via Dagster resources, not environment variables, for testability.
- **Failure isolation:** A sensor failure should not affect consumer processing. Consumers continue writing to MongoDB regardless of Dagster's state. This is a key benefit of the decoupled architecture.
- **Lambda architecture alignment:** Batch assets (`fct_orders`) and realtime assets (`realtime_orders`) cover the same domain with different freshness guarantees. Phase 3I unifies them through BigQuery views.

---

*Last updated: March 2026*
