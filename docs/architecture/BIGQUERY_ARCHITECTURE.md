# BigQuery Architecture — Warehouse Design for Lambda Analytics

> How the analytical warehouse is structured to serve both batch-processed historical data and real-time streaming events through unified views.

---

## Overview

BigQuery sits at the center of this platform's analytical layer. It is the single system where batch data (dimensional model built from historical Olist transactions) and streaming data (live events from Kafka consumers) converge into queryable views. The warehouse is organized into four datasets that reflect the data's lifecycle — from raw staging through curated marts to real-time and experimental layers.

The core design challenge is **serving two query patterns from one warehouse**: slow-changing analytical queries over months of historical data, and low-latency lookups of events that arrived seconds ago. BigQuery's serverless architecture and native streaming insert API make this possible without managing separate infrastructure for batch and real-time workloads.

---

## Dataset Architecture

The warehouse uses four datasets, each with a distinct purpose and retention policy. This separation enforces access boundaries (analysts query marts, not staging) and allows independent lifecycle management.

| Dataset | Purpose | Contents | Retention | Write Pattern |
|---------|---------|----------|-----------|---------------|
| **staging** | Cleaned, validated source data | `stg_orders`, `stg_customers`, `stg_products`, `stg_order_items`, `stg_reviews` | 2 years | Batch load via Dagster |
| **marts** | Dimensional model (star schema) | Fact tables, dimension tables, experiment views, unified views | 2 years | Batch load via Dagster |
| **realtime** | Streaming events from Kafka | `realtime_orders`, `realtime_metrics` | 7 days | Streaming insert API |
| **monitoring** | Pipeline health metrics | `consumer_health_metrics` | 30 days | Kafka consumer writes |

> **Why four datasets instead of one?** Dataset-level separation in BigQuery maps to IAM boundaries. In production, data engineers have write access to staging, analysts have read-only access to marts, and the streaming consumer service account only touches realtime. This is defense-in-depth — a misconfigured query against staging cannot accidentally corrupt mart tables.

---

## Dimensional Model

The marts dataset follows a **star schema** pattern — fact tables at the center, dimension tables around them. This is the standard modeling approach for analytical workloads because it optimizes for the queries analysts actually run: aggregations, filters, and group-bys across business dimensions.

### Fact Tables

| Table | Grain | Key Metrics | Partitioned By | Clustered By | Approx. Rows |
|-------|-------|-------------|----------------|--------------|--------------|
| `fct_orders` | One row per order | GMV, item count, delivery time, review score | `order_purchase_date` | `order_status`, `customer_state` | ~99K |
| `fct_daily_metrics` | One row per day | Total orders, revenue, avg delivery time, avg review score | `metric_date` | — | ~730 |
| `fct_order_items` | One row per order line item | Price, freight, seller metrics | `order_purchase_date` | `product_category`, `seller_id` | ~113K |
| `fct_experiment_results` | One row per experiment | Sample size, conversion rates, p-value, lift | — | `experiment_name` | ~20 |

### Dimension Tables

| Table | Grain | Key Attributes | Derived Fields | Approx. Rows |
|-------|-------|----------------|----------------|--------------|
| `dim_customers` | One row per customer | State, city, zip prefix, region | `customer_tier` (VIP/Regular/New), `customer_value`, `total_orders`, `total_spent`, `avg_order_value` | ~99K |
| `dim_products` | One row per product | Category, weight, dimensions | `product_category_group`, `product_volume_cm3`, `product_size_category` (S/M/L/XL) | ~33K |
| `dim_sellers` | One row per seller | Total orders, revenue, review score | `seller_tier` (Platinum/Gold/Silver/Bronze), `avg_delivery_time_days` | ~3K |

These dimension tables are created by `scripts/bigquery/bigquery_create_dimension_tables.py`, which derives enriched fields from staging table joins. The script is idempotent (uses `CREATE OR REPLACE TABLE ... AS SELECT`).

> **Why star schema over OBT (One Big Table)?** An OBT (pre-joined, fully denormalized table) is simpler to query but expensive to maintain — every dimension change requires a full table rebuild. Star schema lets dimension tables update independently, reduces storage through normalization, and aligns with how BI tools (Looker, Tableau) generate SQL.

---

## Partitioning and Clustering Strategy

BigQuery's cost model charges per byte scanned. Partitioning and clustering are the primary levers for controlling both cost and query performance.

### Partitioning

All time-series fact tables are **partitioned by date** (ingestion-time or a date column). This means a query filtering on `WHERE order_date BETWEEN '2018-01-01' AND '2018-03-31'` only scans 3 months of data instead of the entire table.

| Table | Partition Column | Partition Type | Rationale |
|-------|-----------------|----------------|-----------|
| `fct_orders` | `order_purchase_date` | DAY | Most queries filter by purchase date |
| `fct_order_items` | `order_purchase_date` | DAY | Aligns with fact table for efficient joins |
| `realtime_orders` | `_PARTITIONTIME` | DAY (ingestion) | Streaming inserts use ingestion-time partitioning |
| `fct_daily_metrics` | `metric_date` | DAY | One partition per day, naturally aligned |

### Clustering

Clustering sorts data within partitions for additional scan reduction. BigQuery supports up to 4 clustering columns per table.

- `fct_orders` clustered by `(order_status, customer_state)` — dashboards frequently filter by status and drill down by geography
- `fct_order_items` clustered by `(product_category, seller_id)` — product and seller analysis queries benefit from co-location

> **Cost impact:** On the Olist dataset (~100K orders), partitioning alone reduces typical query scans from ~17 MB to ~2 MB. At production scale (millions of orders), this difference becomes the difference between staying in the free tier and paying hundreds per month.

---

## Unified Views — Lambda Merge Layer

The unified views are the implementation of the Lambda architecture's **serving layer**. They combine batch mart data with real-time streaming data, giving consumers a single queryable interface regardless of when an event occurred.

Two views are currently materialized:

| View | Batch Source | Stream Source | Total Rows |
|------|-------------|--------------|------------|
| `marts.fct_orders_unified` | `marts.fct_orders` (99,441) | `realtime.realtime_orders` (500) | ~99,941 |
| `marts.daily_metrics_unified` | `marts.fct_daily_metrics` (634) | `realtime.realtime_metrics` (4 windows) | ~638 |

### How It Works

```sql
CREATE OR REPLACE VIEW marts.fct_orders_unified AS
SELECT
    order_id,
    DATE(order_purchase_timestamp) AS order_date,
    order_status,
    order_total,
    customer_id,
    customer_state,
    'batch' AS _source_layer,
    order_purchase_timestamp AS _timestamp
FROM marts.fct_orders

UNION ALL

SELECT
    order_id,
    DATE(event_timestamp) AS order_date,
    COALESCE(order_status, event_type) AS order_status,
    order_total,
    customer_id,
    customer_state,
    'stream' AS _source_layer,
    event_timestamp AS _timestamp
FROM realtime.realtime_orders
```

### Critical Design Considerations

- **No date filtering:** The batch data (2016-2018) and stream data (current timestamps) occupy different time ranges in this platform, so no overlap window or deduplication is needed. A production system with real concurrent data would add `WHERE` clauses or `ROW_NUMBER()` deduplication.
- **Source layer column:** `_source_layer` (`batch` or `stream`) identifies the origin of each row, enabling downstream filtering or debugging.
- **Schema alignment:** Batch and realtime tables must expose the same columns with the same types. Schema drift between the two is the most common source of unified view failures. The view definitions use explicit `SELECT` lists with `COALESCE` to handle column name differences.
- **Creation script:** `scripts/bigquery/bigquery_create_unified_views.py` creates both views and runs verification queries. It is idempotent.

> For a deeper discussion of the Lambda architecture design, batch/stream paths, and dimension table derivation, see [LAMBDA_ARCHITECTURE.md](LAMBDA_ARCHITECTURE.md).

---

## Data Population Paths

### Production Path (Dagster-Managed)

```
Dagster assets → BigQueryIOManager → staging/marts datasets
Kafka consumers → BigQuery streaming insert API → realtime dataset
```

Dagster's `BigQueryIOManager` handles table creation, schema management, and partitioned writes. Assets define the schema in Python; the IO manager translates to BigQuery DDL.

### Development Path (Script-Based)

```
load_olist_to_bigquery.py → CSV upload → staging/marts datasets
load_clickstream_to_bigquery.py → DataFrame upload → realtime dataset
```

The dev scripts use `load_table_from_dataframe` or CSV upload for non-partitioned tables and `load_table_from_uri` for partitioned tables (DataFrame-based partitioned loads are unreliable in the BigQuery Python client).

> **Why two paths?** The Dagster path requires a running Dagster instance and all resources configured. The script path lets you populate BigQuery independently for dashboard development, testing, or demos without spinning up the full orchestration stack.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Dataset separation | 4 datasets (staging, marts, realtime, experiments) | IAM boundaries, independent lifecycle, clear data lineage |
| Modeling approach | Star schema (facts + dimensions) | Optimal for analytical queries; BI tool compatible; independent dimension updates |
| Partitioning | Date-based on all time-series tables | Reduces scan cost proportionally to query time range; critical for staying in free tier |
| Clustering | Status + geography on fact tables | Matches the most common dashboard filter patterns |
| Streaming insert | BigQuery streaming API (not micro-batch) | Sub-10-second latency for real-time dashboards; acceptable cost at current volume |
| Unified views | SQL UNION with time boundary | Simple, transparent Lambda merge; no additional infrastructure required |
| Experiment data | Dedicated dataset | Isolates experimental data from production metrics; prevents accidental inclusion in business dashboards |

---

## Production Considerations

### Scaling

- **Query cost at volume:** BigQuery charges ~$5 per TB scanned. At 10M orders, `fct_orders` grows to ~2 GB. Without partitioning, a full scan costs ~$0.01 — trivial. At 1B orders (~200 GB), the same scan costs ~$1 per query. Partitioning and clustering become essential, not optional.
- **Streaming insert throughput:** BigQuery supports up to 100K rows/second per table via the streaming API. The current ~1K events/hour is far below this limit, but burst scenarios (flash sales) could spike to 10K events/minute. The streaming buffer handles this without intervention.
- **Slot contention:** BigQuery's on-demand pricing provides up to 2,000 slots. Concurrent dashboard users running heavy queries can exhaust slots, causing slowdowns. Flat-rate reservations or BI Engine caching would be the production mitigation.

### What Could Go Wrong

- **Schema drift between batch and realtime:** If the Kafka event schema changes but the batch pipeline has not been updated (or vice versa), the unified view breaks. Mitigation: enforce a shared schema contract (Protobuf or JSON Schema) and validate at write time.
- **Streaming buffer delay:** Data in BigQuery's streaming buffer is not immediately available for DML operations or table copies. This can cause confusion during debugging — rows appear in queries but not in exports.
- **Partition expiration misconfigured:** If the realtime dataset's 7-day expiration is accidentally set on marts, historical data disappears silently. Mitigation: apply partition expiration only at the dataset level, not the table level, and verify with automated checks.
- **Cost surprise from `SELECT *`:** A single unfiltered query on a large table can scan the entire dataset. Set up BigQuery cost controls (maximum bytes billed per query) and educate dashboard developers to always include partition filters.

### Cost Profile

| Component | Free Tier Allowance | Current Usage | Break-even Point |
|-----------|-------------------|---------------|------------------|
| Query processing | 1 TB/month | ~5 GB/month | 200x current usage |
| Active storage | 10 GB | ~500 MB | 20x current data |
| Streaming inserts | — | ~$0.01/month | Always paid, but negligible at this scale |
| Long-term storage | 10 GB (after 90 days) | ~200 MB | Auto-transitions after 90 days of no edits |

---

*Last updated: March 2026*
