# Phase 3J: Batch Job Architecture — Scheduling, Partitioning, and Pipeline Orchestration

> Five purpose-built Dagster jobs split the pipeline into independently schedulable, retryable units — separating dimension refreshes, partitioned fact materialization, aggregate computation, and realtime snapshots with clear dependency ordering and monitoring targets.

---

## Overview

A single monolithic job that materializes every asset on every run is the first approach most engineers try. It works for small pipelines but fails at scale: a dimension table failure blocks fact processing, partitioned and non-partitioned assets cannot share a run configuration, and retry granularity is all-or-nothing.

This project splits the pipeline into five jobs, each with a clear responsibility, expected duration, and failure boundary. This architecture enables independent scheduling, targeted retries, and granular monitoring.

---

## Job Architecture

```
                  daily_batch_schedule (02:00 Sao Paulo)
                               |
                               v
                  +────────────────────────────+
                  |   daily_batch_processing   |   Non-partitioned only
                  |   (source > stg > dim > agg)|   ~5 min
                  +────────────────────────────+
                               |
        +──────────────────────+──────────────────────+
        v                      v                       v
  refresh_dimensions     refresh_agg_facts     daily_partitioned_facts
  (23 assets, ~2.5 min)  (5 assets, ~1.5 min)  (3 assets, per partition)
                                                       ^
                                                CLI / Dagit backfill

                  +────────────────────────────+
                  |     realtime_snapshot      |   Sensor-triggered
                  |     (3 realtime assets)    |   (not scheduled)
                  +────────────────────────────+
                               ^
                   kafka_consumer_health_sensor
                   realtime_freshness_sensor
```

---

## Job Definitions

### 1. refresh_dimensions — Foundation Layer

| Property | Value |
|----------|-------|
| **Assets** | 23 (all source, staging, and dimension assets + verification) |
| **Partitioned** | No |
| **Duration** | ~2.5 minutes |
| **Purpose** | Ensure dimension tables are current before fact processing |

This job materializes the entire upstream dependency tree: raw sources, staging transformations, and dimension tables. It runs first because fact tables depend on current dimension values for FK lookups.

> **Why dimensions refresh fully:** Dimension tables are small (total <15 MB). Full refresh is faster than implementing change detection logic. At 100x scale with large dimensions, consider incremental refresh with change data capture.

### 2. daily_partitioned_facts — Incremental Fact Processing

| Property | Value |
|----------|-------|
| **Assets** | 3 (`fct_orders`, `fct_reviews`, `fct_daily_metrics`) |
| **Partitioned** | Yes (daily, 2016-09-01 to 2018-08-31) |
| **Duration** | ~30 seconds per partition |
| **Purpose** | Materialize one day's transactional facts |

Partitioned facts are materialized per-day, either through CLI backfill or Dagit's partition UI. This job is not scheduled because Dagster schedules cannot easily pass dynamic partition keys.

**Backfill pattern:**
```bash
# Process one month at a time to control memory usage
dagster asset materialize -m ecommerce_analytics \
    --select "fct_orders,fct_reviews,fct_daily_metrics" \
    --partition-range 2017-06-01:2017-06-30
```

### 3. refresh_agg_facts — Aggregate Layer

| Property | Value |
|----------|-------|
| **Assets** | 5 (`fct_order_items`, `fct_experiment_results`, `fct_product_performance`, `fct_seller_performance`, `raw_experiment_assignments`) |
| **Partitioned** | No |
| **Duration** | ~1.5 minutes |
| **Asset checks** | 7 checks, all validated |
| **Purpose** | Rebuild summary tables from detail data |

Aggregate facts are rebuilt fully on each run. This is acceptable because the source data (detail facts and staging) is already computed, and aggregation over ~100K rows is fast.

### 4. daily_batch_processing — Combined Scheduled Pipeline

| Property | Value |
|----------|-------|
| **Assets** | 28 (all non-partitioned batch assets) |
| **Partitioned** | No |
| **Duration** | ~5 minutes |
| **Schedule** | Daily at 02:00 Sao Paulo (BRT, UTC-3) |
| **Purpose** | Scheduled full refresh of dimensions + aggregates |

This is the primary scheduled job. It combines `refresh_dimensions` and `refresh_agg_facts` into a single run for operational simplicity.

> **Why 02:00 BRT:** After business hours in Brazil (the primary market for Olist data). Late enough to capture all of yesterday's data, early enough to complete before morning dashboards are viewed. This follows the standard pattern for nightly batch jobs in e-commerce.

### 5. realtime_snapshot — Sensor-Triggered

| Property | Value |
|----------|-------|
| **Assets** | 3 (`realtime_orders`, `realtime_metrics_5min`, `realtime_experiment_results`) |
| **Partitioned** | No |
| **Trigger** | Sensors only (not scheduled) |
| **Purpose** | Snapshot MongoDB realtime collections into Dagster's asset graph |

Triggered by `kafka_consumer_health_sensor` (lag > 5,000) or `realtime_freshness_sensor` (new documents). This job bridges the streaming and batch layers.

---

## Asset Selection Strategy

```python
NON_PARTITIONED_SELECTION = (
    AssetSelection.key_substring("raw_")
    | AssetSelection.key_substring("stg_")
    | AssetSelection.key_substring("dim_")
    | AssetSelection.keys("phase1_verification_asset")
)

PARTITIONED_FACT_SELECTION = AssetSelection.keys(
    "fct_orders", "fct_reviews", "fct_daily_metrics"
)

AGG_FACT_SELECTION = AssetSelection.keys(
    "raw_experiment_assignments", "fct_order_items",
    "fct_experiment_results", "fct_product_performance",
    "fct_seller_performance",
)

REALTIME_SELECTION = AssetSelection.groups("realtime")
```

> **Why explicit selection over `AssetSelection.all()`:** Mixing partitioned and non-partitioned assets in a single job causes Dagster to require partition keys for assets that do not support them. Explicit selection ensures each job contains only compatible assets.

---

## Schedule Configuration

```python
ScheduleDefinition(
    name="daily_batch_schedule",
    job=daily_batch_processing,
    cron_schedule="0 2 * * *",
    execution_timezone="America/Sao_Paulo",
)
```

**Why timezone matters:** Without `execution_timezone`, Dagster uses UTC. A job scheduled at `0 2 * * *` UTC runs at 23:00 BRT (previous day) — before all of yesterday's data has been processed. Specifying the timezone ensures the job runs after the business day ends in the data's primary market.

---

## Operational Runbooks

### First-Time Setup (Full Backfill)

```bash
# Step 1: Refresh all dimensions (~2.5 min)
dagster job execute -m ecommerce_analytics -j refresh_dimensions

# Step 2: Backfill partitioned facts (hours total; run in monthly batches)
dagster asset materialize -m ecommerce_analytics \
    --select "fct_orders,fct_reviews,fct_daily_metrics" \
    --partition-range 2016-09-01:2016-09-30
# Repeat for each month through 2018-08-31

# Step 3: Refresh aggregate facts (~1.5 min)
dagster job execute -m ecommerce_analytics -j refresh_agg_facts
```

### Daily Operations

The `daily_batch_schedule` handles non-partitioned assets automatically. For partitioned facts in production with BigQuery, a partition-aware schedule would materialize yesterday's partition. In local development, use Dagit's partition view.

### Targeted Re-Materialization

```bash
# Single dimension refresh
dagster asset materialize -m ecommerce_analytics --select dim_customers

# Single partition re-processing
dagster asset materialize -m ecommerce_analytics \
    --select fct_orders --partition 2017-06-15

# Manual realtime snapshot
dagster job execute -m ecommerce_analytics -j realtime_snapshot
```

---

## Monitoring Targets

| Metric | Expected | Alert Threshold |
|--------|----------|----------------|
| `refresh_dimensions` duration | < 5 min | > 10 min |
| `refresh_agg_facts` duration | < 3 min | > 10 min |
| Per-partition fact materialization | < 1 min | > 5 min |
| Asset checks pass rate | 100% | Any ERROR severity failure |
| Partition coverage | All dates materialized | Missing partitions |
| Schedule execution | Daily at 02:00 BRT | Missed execution |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Job granularity | 5 separate jobs | Independent scheduling, retry, and monitoring |
| Schedule time | 02:00 Sao Paulo | After business hours; before morning dashboards |
| Partitioned facts | Not scheduled (CLI/Dagit backfill) | Dagster schedules cannot pass dynamic partition keys |
| Realtime job | Sensor-triggered only | Event-driven; avoids unnecessary polling |
| Asset selection | Explicit, not `all()` | Prevents partition/non-partition mixing |
| Combined daily job | `daily_batch_processing` | Operational simplicity for the common case |

---

## Production Considerations

- **Retry policy:** In production, add `RetryPolicy(max_retries=2, delay=60)` to jobs. A failed dimension refresh should retry before alerting, as transient network issues are the most common failure mode.
- **Concurrency control:** Dagster's default executor runs assets sequentially. For the 23-asset `refresh_dimensions` job, switching to `multiprocess_executor` with `max_concurrent=4` would cut runtime by ~60%.
- **Backfill management:** Two years of daily partitions (729 days x 3 facts = 2,187 partition materializations) should be run in monthly batches with monitoring. A full backfill takes approximately 3-4 hours.
- **Schedule drift:** If the daily job takes longer than expected and overlaps with the next day's run, Dagster's schedule guarantees prevent double-execution. Configure `default_status=DefaultScheduleStatus.RUNNING` for production.
- **Cross-job dependencies:** `refresh_agg_facts` implicitly depends on `refresh_dimensions` (shared staging data). The combined `daily_batch_processing` job handles this by materializing both in dependency order.
- **Cost of full refresh:** Rebuilding all non-partitioned assets daily is cheap at current scale. At 100x, consider selective refresh based on upstream change detection.

---

*Last updated: March 2026*
