# Phase 3K: Stream Processing Integration — Consumer Health, Reconciliation, and Lifecycle Management

> Completes the Lambda architecture by adding hourly stream sync, nightly batch-stream reconciliation, and critical health alerting — ensuring the streaming and batch layers stay consistent and consumer failures are detected within minutes.

---

## Overview

Phases 3F and 3G established Kafka consumers and Dagster's ability to snapshot their output. Phase 3K closes the loop by answering three operational questions that production streaming systems must address:

1. **Are the consumers healthy?** The `consumer_health_metrics` asset captures lag, partition assignments, and status for every consumer group.
2. **Do the stream and batch layers agree?** The `batch_stream_reconciliation` asset compares order IDs across layers to quantify drift and flag merge candidates.
3. **What happens when something breaks?** The `stream_health_alert_sensor` triggers immediate materialization when any consumer reaches critical lag.

---

## Consumer Lifecycle Management

### The Decision: Consumers Run Outside Dagster

| Concern | Approach |
|---------|----------|
| **Consumer processes** | Standalone Python scripts (`scripts/run_consumers.py`) |
| **Process management** | Docker containers, systemd, or supervisord |
| **Dagster's role** | Monitor health, snapshot data, validate quality |
| **Start/stop consumers** | Outside Dagster (Docker restart, manual) |
| **Health visibility** | `consumer_health_metrics` asset + sensors |

> **Why not manage consumers inside Dagster:** Kafka consumers are infinite loops. Dagster ops are designed for finite tasks that produce outputs and complete. Running a consumer as an op turns the orchestrator into a process manager — fighting against its design, complicating error handling, and coupling consumer availability to Dagster's uptime. This is the same reason Airflow does not run Flink jobs inline; it triggers and monitors them.

---

## New Assets

### consumer_health_metrics

| Property | Value |
|----------|-------|
| **Group** | `stream_ops` |
| **Grain** | One row per Kafka consumer group + one row per MongoDB collection |
| **Dependencies** | None (queries Kafka + MongoDB directly) |
| **Schedule** | Hourly via `stream_sync` job |
| **Check** | `check_consumer_health_no_critical` |

This asset queries all four consumer groups for:

- **Lag:** Total messages pending per group (the primary health indicator)
- **Partition assignments:** How many Kafka partitions are assigned to each consumer
- **Status classification:** `healthy` (lag < 1,000), `warning` (1,000-10,000), `critical` (> 10,000), `unreachable`

It also queries five MongoDB collections for document counts and newest document timestamps, providing a complete health snapshot.

> **Why health metrics as a Dagster asset:** By making consumer health a materialized asset, it gets full lineage, versioning, and check integration. Historical health snapshots enable trend analysis (is lag growing over time?) and incident investigation (when did this consumer group fall behind?).

### batch_stream_reconciliation

| Property | Value |
|----------|-------|
| **Group** | `stream_ops` |
| **Grain** | 4-row report (stream_only, batch_only, overlap, totals) |
| **Dependencies** | `stg_orders`, `realtime_orders` |
| **Schedule** | Nightly at 03:00 via `batch_stream_reconciliation_job` |
| **Check** | `check_reconciliation_stream_drift` |

Compares order IDs between the stream layer (MongoDB `fct_orders_realtime`) and the batch layer (`stg_orders`) to produce a reconciliation report:

| Category | Meaning | Expected Behavior |
|----------|---------|-------------------|
| **stream_only** | New orders not yet in batch | Should be small; will be picked up by next batch run |
| **overlap** | Orders in both layers | Dedup candidates for BigQuery unified view |
| **batch_only** | Historical orders not in stream window | Normal; stream only has recent data |

> **Why reconciliation matters:** In a Lambda architecture, the batch and stream layers process the same data independently. Without reconciliation, silent divergence can cause dashboards to show different numbers depending on which layer is queried. The nightly reconciliation job catches this drift early.

---

## Complete Job Inventory (7 Jobs)

| Job | Type | Schedule | Assets | Duration |
|-----|------|----------|--------|----------|
| `refresh_dimensions` | Batch | Manual | Source, staging, dimensions (23) | ~2.5 min |
| `daily_partitioned_facts` | Batch | Manual (requires `--partition`) | `fct_orders`, `fct_reviews`, `fct_daily_metrics` | ~30s/partition |
| `refresh_agg_facts` | Batch | Manual | Aggregate facts (5) | ~1.5 min |
| `daily_batch_processing` | Batch | **02:00 daily** | All non-partitioned batch (28) | ~5 min |
| `realtime_snapshot` | Stream | Sensor-triggered | Realtime assets (3) | ~3s |
| `stream_sync` | Stream | **Every hour** | Realtime assets (3) + `consumer_health_metrics` | ~4s |
| `batch_stream_reconciliation_job` | Reconciliation | **03:00 daily** | `batch_stream_reconciliation` | ~1s |

### Daily Timeline

```
00:00 ── stream_sync (hourly) ──────────────────────────────────────────
01:00 ── stream_sync (hourly) ──────────────────────────────────────────
02:00 ── daily_batch_processing (dims + agg facts) ~5 min ─────────────
03:00 ── batch_stream_reconciliation (compare layers) ─────────────────
04:00 ── stream_sync (hourly) ──────────────────────────────────────────
  ...    (stream_sync continues every hour; sensors run continuously)
```

> **Why reconciliation runs at 03:00:** One hour after the daily batch job completes at 02:00. This ensures the batch layer has been refreshed before comparing it against the stream layer. Running them simultaneously would compare stale batch data against fresh stream data, producing misleading drift numbers.

---

## Complete Schedule Inventory (3 Schedules)

| Schedule | Cron | Timezone | Job | Purpose |
|----------|------|----------|-----|---------|
| `daily_batch_schedule` | `0 2 * * *` | America/Sao_Paulo | `daily_batch_processing` | Nightly dimension + aggregate refresh |
| `hourly_stream_schedule` | `0 * * * *` | America/Sao_Paulo | `stream_sync` | Hourly health check + realtime snapshot |
| `nightly_reconciliation_schedule` | `0 3 * * *` | America/Sao_Paulo | `batch_stream_reconciliation_job` | Stream-batch consistency check |

---

## Complete Sensor Inventory (3 Sensors)

| Sensor | Interval | Trigger Condition | Action |
|--------|----------|-------------------|--------|
| `kafka_consumer_health_sensor` | 2 min | Total lag > 5,000 messages | Materialize all realtime assets |
| `realtime_freshness_sensor` | 5 min | New documents in MongoDB (CDC-style) | Materialize changed realtime assets |
| `stream_health_alert_sensor` | 10 min | Any consumer group critical (lag > 10,000) or unreachable | Materialize `consumer_health_metrics` + all realtime assets |

### Sensor Escalation Ladder

```
Normal operation:
  realtime_freshness_sensor triggers snapshots on new data (every few minutes)

Moderate backlog:
  kafka_consumer_health_sensor fires when lag > 5,000
  Snapshots all realtime assets to capture current state

Critical failure:
  stream_health_alert_sensor fires when lag > 10,000 or consumer unreachable
  Captures health metrics + snapshots for incident investigation
```

> **Why three sensors with different thresholds:** A single sensor that triggers on any lag change would fire constantly during normal operation. The tiered approach (freshness for routine, health for moderate issues, alert for critical) mirrors production alerting patterns with different severity levels.

---

## Asset Checks

| Check | Asset | Severity | Condition |
|-------|-------|----------|-----------|
| `check_consumer_health_no_critical` | `consumer_health_metrics` | WARN | No consumer groups in `critical` or `unreachable` status |
| `check_reconciliation_stream_drift` | `batch_stream_reconciliation` | WARN | Stream-only orders < 50% of total stream count |

The drift check catches scenarios where the stream layer has significantly diverged from batch. A stream_only percentage above 50% suggests either the batch job is failing to pick up recent data, or the stream is processing data that batch never receives.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Consumer management | Outside Dagster | Infinite loops vs. finite ops; separation of concerns |
| Health monitoring | Dagster asset with hourly schedule | Full lineage and history; integrates with asset checks |
| Reconciliation | Nightly job comparing order IDs | Catches stream-batch drift before it affects dashboards |
| Alert escalation | Three sensors with increasing thresholds | Prevents alert fatigue; mirrors production severity levels |
| Reconciliation timing | 03:00 (1 hour after batch) | Ensures batch is fresh before comparison |
| Stream sync frequency | Hourly | Balances freshness against materialization overhead |

---

## Production Considerations

- **Consumer restart automation:** In production, use Kubernetes `Deployment` resources with `restartPolicy: Always` or systemd units with `Restart=always`. The consumer resumes from the last committed Kafka offset after restart.
- **Reconciliation at scale:** The current order ID comparison loads both datasets into memory. At 10M+ orders, push the comparison to BigQuery with a SQL `FULL OUTER JOIN` between batch and stream tables.
- **Alert routing:** The `stream_health_alert_sensor` currently logs warnings. In production, integrate with PagerDuty or Slack via Dagster's `make_slack_on_run_failure_sensor` for on-call notification.
- **Backpressure:** If consumers cannot keep up with Kafka throughput, lag grows indefinitely. Consider auto-scaling consumer instances based on lag metrics, or implementing Kafka consumer group rebalancing.
- **Stream sync cost:** Hourly materialization of 4 assets (24 runs/day) is lightweight. But if each materialization triggers expensive downstream processing, consider reducing frequency or adding change detection to skip no-op runs.
- **Reconciliation false positives:** During periods of high order volume, the stream layer naturally has more stream_only orders (not yet processed by batch). The 50% threshold should be tuned based on observed traffic patterns.
- **Dagster instance sizing:** With 35 assets, 7 jobs, 3 schedules, and 3 sensors, the Dagster daemon requires modest resources (~256 MB RAM). At 100x assets, consider dedicated scheduler and sensor daemon processes.

---

*Last updated: March 2026*
