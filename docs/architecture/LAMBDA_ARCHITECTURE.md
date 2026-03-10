# Lambda Architecture — Batch + Stream Unification

> How batch-processed historical data and real-time streaming events converge into unified BigQuery views, giving consumers a single queryable interface regardless of when an event occurred.

---

## Overview

The platform follows a **Lambda architecture** pattern: two parallel data paths (batch and stream) that merge at a serving layer. This design exists because batch processing gives completeness and correctness (full Olist dataset, validated joins, aggregated metrics) while streaming gives freshness (orders arriving via Kafka in near real-time).

```
                    ┌───────────────────────────────────────┐
                    │          BigQuery Warehouse            │
                    │                                       │
Olist CSV ──► Dagster ──► staging ──► marts                │
                    │       │          │                     │
                    │       │    fct_orders (99,441 rows)    │    ◄── BATCH PATH
                    │       │    fct_daily_metrics (634)     │
                    │       │    dim_customers (99,441)      │
                    │       │    dim_products (32,951)       │
                    │       │    dim_sellers (3,095)         │
                    │       │                               │
Kafka ──► Consumers ──► realtime                           │
                    │       │                               │
                    │    realtime_orders (500 rows)         │    ◄── STREAM PATH
                    │    realtime_metrics (66 rows)         │
                    │       │                               │
                    │       ▼                               │
                    │    fct_orders_unified ◄── VIEW         │    ◄── SERVING LAYER
                    │    daily_metrics_unified ◄── VIEW      │
                    │                                       │
                    └───────────────────────────────────────┘
```

---

## Batch Path

### Source

The Olist Brazilian E-Commerce dataset: ~100K orders across 8 CSV files covering 2016-2018.

### Pipeline

1. **Raw CSVs** in `raw/olist/` (or S3 bucket)
2. **Dagster source assets** read CSVs into DataFrames
3. **Staging assets** clean, validate, and write to BigQuery `staging` dataset (`stg_orders`, `stg_order_items`, `stg_customers`, `stg_products`, `stg_reviews`)
4. **Dimension assets** derive enriched dimension tables from staging joins
5. **Fact assets** build `fct_orders` (one row per order) and `fct_daily_metrics` (one row per day) in `marts`
6. **Analysis assets** produce experiment summaries, comparison views, clickstream aggregates

### Characteristics

- Complete historical record (every order from 2016-2018)
- Validated joins across all tables
- Derived fields (customer tier, product category group, seller tier)
- Updated by `daily_batch_processing` Dagster job (scheduled or manual)

---

## Stream Path

### Source

Kafka producers sample from the Olist dataset and publish events to 7 topics (`orders-stream`, `clickstream`, `payments-stream`, `shipments-stream`, `deliveries-stream`, `reviews-stream`, `experiments-stream`).

### Pipeline

1. **Kafka producers** generate events with current timestamps
2. **Kafka consumers** (`run_consumers.py`) read from topics
3. **MongoDB** receives real-time documents (`ecommerce_streaming` database)
4. **BigQuery** receives streaming inserts into `realtime` dataset (`realtime_orders`, `realtime_metrics`)

### Characteristics

- Near real-time (seconds of latency)
- Simulated from Olist data but with live timestamps
- Smaller volume (hundreds of records vs batch's ~100K)
- Updated continuously while consumers are running

---

## Unified Views (Serving Layer)

The serving layer is implemented as BigQuery views that `UNION ALL` batch and stream tables.

### `marts.fct_orders_unified`

Combines `marts.fct_orders` (99,441 batch orders) with `realtime.realtime_orders` (500 stream orders).

**Columns:** `order_id`, `order_date`, `order_status`, `order_total`, `customer_id`, `customer_state`, `_source_layer`, `_timestamp`

The `_source_layer` column (`batch` or `stream`) identifies the origin of each row, allowing downstream queries to filter by source if needed.

### `marts.daily_metrics_unified`

Combines `marts.fct_daily_metrics` (634 batch days) with `realtime.realtime_metrics` (4 stream windows).

**Columns:** `metric_date`, `total_orders`, `total_gmv`, `avg_order_value`, `unique_customers`, `late_delivery_rate`, `_source_layer`

### Why No Deduplication

In a production Lambda system, the overlap window between batch and stream requires deduplication (the same order could exist in both layers during the transition period). In this platform:

- Batch data covers 2016-2018 (the original Olist dataset)
- Stream data is generated with current timestamps (2026)
- There is no temporal overlap between the two, so `UNION ALL` without deduplication is correct

The `_source_layer` column is included as a safety mechanism: if overlap ever occurs, queries can filter or deduplicate on it.

---

## Dimension Tables

Three dimension tables are derived from staging data to support analytical queries:

### `marts.dim_customers`

Created from `stg_customers` joined with `stg_orders`:

- **Enriched fields:** `customer_region` (mapped from state), `total_orders`, `total_spent`, `avg_order_value`, `first_order_date`, `last_order_date`
- **Derived tiers:** `customer_tier` (VIP / Regular / New based on order count), `customer_value` (based on total spend)
- **Row count:** 99,441

### `marts.dim_products`

Created from `stg_products` joined with `stg_order_items`:

- **Enriched fields:** `product_category_group` (mapped from category), `product_volume_cm3` (calculated from dimensions)
- **Derived fields:** `product_size_category` (Small / Medium / Large / Extra Large based on volume)
- **Row count:** 32,951

### `marts.dim_sellers`

Created from `stg_order_items` joined with `stg_orders` and `stg_reviews`:

- **Enriched fields:** `total_orders_fulfilled`, `total_revenue`, `avg_review_score`, `avg_delivery_time_days`
- **Derived tiers:** `seller_tier` (Platinum / Gold / Silver / Bronze based on order volume and revenue)
- **Row count:** 3,095

### Creation Script

`scripts/bigquery/bigquery_create_dimension_tables.py` generates these tables using SQL `CREATE OR REPLACE TABLE ... AS SELECT` statements. It can be rerun safely (idempotent) whenever staging data is refreshed.

---

## How the Dashboard Uses Unified Data

The React dashboard and AI agent both query unified views transparently:

- **Business Performance dashboard** — queries `fct_orders_unified` for total GMV, order counts, and trends. The user sees one continuous dataset without knowing it comes from two sources.
- **AI Agent** — when asked "What's the total GMV?", the agent can query `marts.fct_orders_unified` and get the combined batch + stream number.
- **Data Quality dashboard** — monitors freshness of both batch and stream tables separately, alerting if either path falls behind.

---

## Production Considerations

### Scaling to Real Data

In a production environment with actual real-time orders:

1. **Overlap window** — Batch jobs run daily. Stream data covers the current day. The unified view would need a deduplication strategy: either `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _timestamp DESC)` or a cutoff date filter.
2. **Schema contract** — Batch and stream schemas must stay aligned. A shared Protobuf or JSON Schema definition would enforce this.
3. **Materialized views** — At high volume, BigQuery materialized views would replace the current `CREATE VIEW` for better query performance.
4. **Partition expiration** — Stream data in the `realtime` dataset should have partition expiration (e.g., 7 days) to control storage costs.

### What Could Go Wrong

- **Schema drift:** If a Kafka consumer adds a new field to `realtime_orders` that doesn't exist in `fct_orders`, the `UNION ALL` view breaks. Fix: enforce column-explicit `SELECT` lists in the view definitions (already done).
- **Stale batch data:** If the daily Dagster job fails silently, the batch layer falls behind. The Data Quality dashboard and `check_data_freshness` agent tool detect this.
- **Stale stream data:** If Kafka consumers stop, the realtime tables go stale. The `run_anomaly_scan` agent tool checks for this (threshold: 24 hours for `realtime_orders`, 6 hours for `consumer_health_metrics`).

---

*Last updated: February 2026*
