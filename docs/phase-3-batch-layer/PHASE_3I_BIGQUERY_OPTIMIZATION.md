# Phase 3I: BigQuery Optimization — Lambda Architecture, Partitioning, and Cost Control

> This is the architectural centerpiece of the platform's storage layer. It implements a Lambda architecture in BigQuery with separate batch and stream tables unified through views, daily partitioning with expiration policies, multi-column clustering, and append-only streaming with query-time deduplication — all designed for cost efficiency within BigQuery's free tier.

---

## Overview

The Lambda architecture solves a fundamental tension in data systems: batch processing produces accurate, complete results but with high latency, while stream processing provides low-latency results that may be incomplete or approximate. Rather than choosing one, Lambda uses both and merges them.

This project implements Lambda in BigQuery with a clean boundary rule: **batch owns everything before today; stream owns today.** A unified view presents both layers as a single queryable surface, and partition expiration automatically garbage-collects stale stream data.

---

## Lambda Architecture: Three Approaches Evaluated

This is the most consequential design decision in the storage layer. The choice affects query complexity, write costs, data correctness, and operational burden.

| Criteria | Option A: Separate Tables | Option B: Single Table with MERGE | Option C: Append-Only with View Dedup |
|----------|--------------------------|----------------------------------|--------------------------------------|
| **Batch table** | `fct_orders_batch` | `fct_orders` (shared) | `fct_orders_raw` (shared) |
| **Stream table** | `fct_orders_stream` | Same table | Same table |
| **Query surface** | UNION ALL view | Direct query | Deduplicated view |
| **Dedup required?** | No (layers isolated) | Yes (MERGE DML at write time) | Yes (ROW_NUMBER at query time) |
| **Write complexity** | Append-only (simplest) | MERGE statement (complex) | Append-only (simplest) |
| **Write cost** | $0 (free inserts) | $5/TB (DML pricing) | $0 (free inserts) |
| **Failure isolation** | High (layers independent) | Low (bad merge corrupts both) | Medium |
| **Explainability** | Maps directly to Lambda theory | Harder to reason about | Moderate |

### Chosen: Option A — Separate Tables + Unified View

> **Why Option A wins:** Clean separation means batch and stream layers cannot corrupt each other. Streaming writes are append-only (the cheapest pattern in BigQuery). The unified view provides a transparent query surface at zero additional cost. And the architecture maps directly to Lambda theory, making it straightforward to explain in interviews and documentation.

**Why not Option B (MERGE):** BigQuery's MERGE DML costs $5/TB processed. For a streaming pipeline writing continuously, this adds up. More critically, a failed MERGE can leave the table in an inconsistent state, violating the "batch is always correct" principle.

**Why not Option C (Append-Only single table):** While cost-efficient, having batch and stream data in the same table with different correctness guarantees creates confusion. Partition expiration cannot selectively remove only stream rows.

---

## The Boundary Rule

```
Timeline:  ──────────[yesterday]──────────[today]──────────[future]──────>

Batch layer:   Covers all data before CURRENT_DATE()
               Written by Dagster daily job (WRITE_TRUNCATE per partition)
               Source of truth for historical data

Stream layer:  Covers data from CURRENT_DATE() onwards
               Written by Kafka consumers (append-only)
               Near-real-time, may contain duplicates

Unified view:  UNION ALL of both layers
               Single query surface for dashboards
```

**What happens at midnight:** Tonight's batch job processes today's data and writes it to the batch table (WRITE_TRUNCATE replaces the partition). Tomorrow, the stream table only needs to cover the new day. Stream data older than 7 days auto-expires via partition expiration.

---

## Partitioning Strategy

### Batch Tables

| Table | Partition Column | Type | Expiration | Require Filter |
|-------|-----------------|------|-----------|----------------|
| `fct_orders_batch` | `order_date` | DAY | 730 days | YES |
| `fct_reviews` | `review_creation_date` | DAY | 730 days | YES |
| `fct_daily_metrics` | `metric_date` | DAY | 730 days | NO |

**Why daily partitions:**
- E-commerce query patterns are date-range driven (daily dashboards, monthly reports, seasonal analysis)
- Dagster writes one partition per daily job run using `WRITE_TRUNCATE` (complete partition replacement)
- `require_partition_filter = TRUE` prevents accidental full-table scans that could consume the entire monthly free tier in a single query

> **Why `require_partition_filter`:** A single unfiltered `SELECT *` on a large partitioned table scans every partition. In BigQuery's on-demand pricing ($5/TB), this is expensive and usually unintentional. Requiring a partition filter forces query authors to specify a date range.

### Stream Tables

| Table | Partition Column | Type | Expiration |
|-------|-----------------|------|-----------|
| `fct_orders_stream` | `processing_timestamp` | DAY | 7 days |
| `realtime_metrics` | `window_start` | DAY | 30 days |
| `delivery_alerts` | `created_at` | DAY | 90 days |

**Why short expiration on stream tables:**
- Stream data is ephemeral — batch takes over after the daily job runs
- 7-day retention provides a safety buffer if the batch job fails for several consecutive days
- Auto-deletion via partition expiration eliminates the need for manual cleanup jobs
- Storage costs are controlled automatically

### Unpartitioned Tables

`fct_order_items`, `dim_customers`, `dim_products`, `dim_sellers`, `experiments_realtime` are all under 15 MB total. Partitioning tables this small adds metadata overhead without query performance benefit. BigQuery's minimum partition size for cost savings is approximately 1 GB.

---

## Clustering Strategy

Clustering sorts data within partitions by specified columns, enabling BigQuery to skip irrelevant storage blocks during queries.

| Table | Clustering Columns | Query Pattern |
|-------|-------------------|---------------|
| `fct_orders_batch` | `order_status`, `payment_type` | `WHERE order_status = 'delivered'` |
| `fct_reviews` | `sentiment`, `review_score` | Dashboard sentiment filters |
| `fct_order_items` | `product_key`, `seller_key` | JOIN acceleration on dimension keys |
| `fct_orders_stream` | `order_status`, `event_type` | Stream event filtering |
| `realtime_metrics` | `metric_name` | `WHERE metric_name = 'total_gmv'` |
| `dim_customers` | `customer_state` | Geographic segmentation queries |
| `dim_products` | `product_category_name` | Category-level analysis |

> **Why column order matters in clustering:** BigQuery clusters by columns in the specified order. The first column provides the coarsest sort. Put the most frequently filtered column first. With `order_status` first, a query filtering only on `payment_type` benefits less from clustering.

---

## Deduplication Strategy

### Stream Table: Append-Only + View-Time Dedup

Kafka consumers write events in append-only mode. The same order may produce multiple events (created, updated, delivered). A deduplication view surfaces only the latest version:

```sql
CREATE VIEW realtime.fct_orders_stream_deduped AS
SELECT * EXCEPT (_row_num)
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY processing_timestamp DESC
    ) AS _row_num
    FROM realtime.fct_orders_stream
)
WHERE _row_num = 1;
```

**Why this pattern over MERGE:**

| Approach | Write Cost | Query Cost | Complexity | Data Loss Risk |
|----------|-----------|-----------|-----------|----------------|
| MERGE DML at write time | $5/TB | Lowest | High | Medium (failed MERGE) |
| **ROW_NUMBER at query time** | **$0** | **Slight overhead** | **Low** | **None (append-only)** |
| Delete + re-insert | $5/TB | Lowest | Medium | High (race conditions) |

The ROW_NUMBER approach keeps a full audit trail (all versions visible in the raw table), costs nothing to write, and the dedup overhead at query time is negligible for tables with 7-day expiration windows.

### Batch Table: No Dedup Needed

Dagster uses `WRITE_TRUNCATE` per partition. Each daily run completely replaces the partition's contents. Duplicates are structurally impossible.

---

## Unified Lambda Views

### marts.fct_orders_unified

```sql
-- Batch: complete, accurate historical data
SELECT * FROM batch.fct_orders_batch
WHERE order_date < CURRENT_DATE()

UNION ALL

-- Stream: today's near-real-time data (deduplicated)
SELECT * FROM realtime.fct_orders_stream_deduped
WHERE processing_timestamp >= CURRENT_DATE()
```

**Boundary rule:** Batch owns everything up to yesterday. Stream owns today. When tonight's batch job runs, it overwrites today's batch partition, and stream data gradually expires.

### marts.daily_metrics_unified

Same UNION ALL pattern: batch daily metrics for historical dates, windowed stream metrics for today.

### marts.experiment_results_unified

Uses `FULL OUTER JOIN` to show both batch (complete statistical analysis) and stream (live counters) side by side, enabling comparison of final vs. in-progress experiment results.

---

## Streaming Insert Configuration

### Storage Write API vs. Legacy Streaming

| Feature | Legacy Streaming API | Storage Write API |
|---------|---------------------|-------------------|
| **Cost** | $0.05/GB | Free |
| **Delivery semantics** | At-least-once | Exactly-once |
| **Throughput** | 100K rows/sec | 3 GB/sec |
| **Buffer time** | ~90 minutes (data in streaming buffer) | Committed immediately |
| **Recommended** | No (legacy) | **Yes** |

> **Why Storage Write API:** It is free, faster, and provides exactly-once semantics. The legacy streaming API's 90-minute buffer delay and per-GB cost make it strictly inferior for new projects. The only reason to use legacy is backward compatibility.

### Consumer Write Pattern

```python
buffer_size = 100        # batch 100 rows before flush (aligns with Kafka MAX_POLL_RECORDS)
flush_interval = 5       # or flush every 5 seconds, whichever comes first
max_retries = 3          # retry failed writes with exponential backoff
use_storage_write_api = True
```

---

## Schema Evolution Strategy

| Strategy | Pros | Cons | Used Here? |
|----------|------|------|-----------|
| Schema-on-read (accept anything) | Maximum flexibility | Unpredictable query results, silent breakage | No |
| Schema Registry (Confluent/Avro) | Strict contracts, compatibility checks | Complex infrastructure dependency | No |
| **Drop unknown fields + log** | **Simple, predictable, auditable** | Might miss new data until schema update | **Yes** |

**Implementation:** The `BaseConsumer.validate()` method checks incoming messages against expected fields. Unknown fields are logged and dropped. Schema changes follow a controlled process:

1. Add nullable column to BigQuery table
2. Update consumer validation to accept new field
3. Deploy updated consumer
4. Backfill if needed

> **Why not Schema Registry:** For a portfolio project with four consumers, a Schema Registry adds operational complexity (another service to run, version management, compatibility rules) without proportional benefit. In a 50-consumer production environment, Schema Registry becomes essential.

---

## Cost Estimation

### Storage (Monthly)

| Component | Size | Cost |
|-----------|------|------|
| Batch tables (all) | ~35 MB | Free (within 10 GB free tier) |
| Stream tables (with 7-day retention) | ~7 MB/week | Free |
| **Total storage** | **< 100 MB** | **$0.00** |

### Queries (Monthly, Moderate Dashboard Usage)

| Query Type | Estimated Bytes/Month | Cost |
|-----------|----------------------|------|
| Batch analytics (dashboards, reports) | ~50 GB | $0.25 |
| Stream dashboards (real-time panels) | ~5 GB | $0.03 |
| **Total queries** | **~55 GB** | **$0.28** |

### Streaming Inserts (Monthly)

| Method | Volume | Cost |
|--------|--------|------|
| Legacy Streaming API | ~3 GB | $0.15 |
| **Storage Write API** | **~3 GB** | **$0.00** |

### Total Monthly Cost: $0.28

Well within BigQuery's free tier (1 TB/month queries, 10 GB active storage). Even at 10x scale, monthly costs would remain under $3.

> **Why cost matters in a portfolio project:** Demonstrating awareness of cloud cost optimization shows production readiness. Choosing Storage Write API over legacy streaming, using partition expiration instead of manual cleanup, and requiring partition filters are all cost-conscious decisions that translate directly to real-world budget conversations.

---

## BigQuery DDL Files

| File | Purpose |
|------|---------|
| `scripts/bigquery/01_create_datasets.sql` | Dataset (schema) creation: `batch`, `realtime`, `marts`, `staging` |
| `scripts/bigquery/02_create_batch_tables.sql` | Batch table DDL with partitioning + clustering |
| `scripts/bigquery/03_create_stream_tables.sql` | Stream table DDL with short expiration policies |
| `scripts/bigquery/04_create_views.sql` | Deduplication views + unified Lambda views |
| `scripts/bigquery/05_performance_queries.sql` | Annotated queries demonstrating partition pruning |
| `scripts/bigquery/06_optimization.sql` | Materialized views, expiration tuning, best practices |
| `ecommerce_analytics/resources/bigquery_config.py` | Python config mapping Dagster assets to BigQuery tables |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Lambda implementation | Separate tables + UNION ALL view | Clean isolation; cheapest write pattern |
| Batch write mode | WRITE_TRUNCATE per partition | Idempotent; no dedup needed |
| Stream write mode | Append-only + view dedup | $0 write cost; full audit trail |
| Partition granularity | Daily | Matches query patterns and Dagster job frequency |
| Stream table expiration | 7 days | Safety buffer with automatic cleanup |
| Streaming API | Storage Write API | Free, exactly-once, higher throughput |
| Schema evolution | Drop unknown + log | Simple; appropriate for project scale |
| Partition filter | Required on large tables | Prevents accidental full scans |

---

## Production Considerations

- **Lambda vs. Kappa debate:** Kappa architecture eliminates the batch layer entirely, processing everything through the stream. It is simpler but requires stream reprocessing for corrections. Lambda is chosen here because BigQuery is naturally batch-oriented, and the Olist historical data does not change — making batch processing the natural fit for the bulk of the data.
- **View performance at scale:** UNION ALL views are zero-cost in BigQuery (views are just saved queries). At very high query volume, consider a materialized view for `fct_orders_unified` to avoid recomputing the union on every query.
- **Partition pruning verification:** Use `INFORMATION_SCHEMA.JOBS_BY_PROJECT` to verify that queries are actually pruning partitions. A query scanning all partitions despite a date filter usually indicates a type mismatch (string vs. DATE) in the WHERE clause.
- **Clustering maintenance:** BigQuery auto-reclusters in the background. No manual maintenance required, but heavily updated tables may temporarily have suboptimal clustering.
- **Cost scaling:** At 100x data volume (~10 GB storage, ~5 TB queries/month), monthly costs would be approximately $25-30. The free tier covers the portfolio project entirely.
- **Disaster recovery:** Batch tables can be fully reconstructed from source data via Dagster backfill. Stream tables are ephemeral by design. The combination provides durability without backup complexity.

---

*Last updated: March 2026*
