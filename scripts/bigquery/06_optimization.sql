-- ============================================================================
-- Phase 3I: BigQuery Cost & Performance Optimization
--
-- Applied optimizations:
--   1. Partition expiration (auto-delete old data)
--   2. Materialized views (pre-compute expensive aggregations)
--   3. BI Engine reservation (sub-second dashboard queries)
--   4. Slot reservations (predictable cost for production)
-- ============================================================================


-- ── 1. Partition Expiration ──────────────────────────────────────────
-- Automatically drop partitions older than N days to control storage costs.
-- Already set in CREATE TABLE, but can be updated:

ALTER TABLE marts.fct_orders_batch
SET OPTIONS (partition_expiration_days = 730);  -- 2 years

ALTER TABLE realtime.fct_orders_stream
SET OPTIONS (partition_expiration_days = 7);    -- 7 days (batch takes over)

ALTER TABLE realtime.realtime_metrics
SET OPTIONS (partition_expiration_days = 30);   -- 30 days

ALTER TABLE realtime.delivery_alerts
SET OPTIONS (partition_expiration_days = 90);   -- 90 days


-- ── 2. Materialized Views ────────────────────────────────────────────
-- Pre-compute expensive aggregations. BigQuery auto-refreshes when base
-- tables change. Queries that match the MV pattern are auto-rewritten.

CREATE MATERIALIZED VIEW IF NOT EXISTS marts.mv_monthly_revenue AS
SELECT
    DATE_TRUNC(order_date, MONTH) AS month,
    order_status,
    COUNT(*) AS total_orders,
    SUM(order_total) AS total_revenue,
    AVG(order_total) AS avg_order_value,
    SUM(freight_value) AS total_freight
FROM marts.fct_orders_batch
GROUP BY month, order_status;

CREATE MATERIALIZED VIEW IF NOT EXISTS marts.mv_seller_performance AS
SELECT
    seller_key,
    COUNT(*) AS total_orders,
    SUM(price) AS total_revenue,
    AVG(price) AS avg_item_price
FROM marts.fct_order_items
GROUP BY seller_key;


-- ── 3. Query Optimization Techniques ─────────────────────────────────

-- 3a. Always use partition filters (enforced by require_partition_filter)
-- BAD:  SELECT * FROM marts.fct_orders_batch;  -- full scan, rejected
-- GOOD: SELECT * FROM marts.fct_orders_batch WHERE order_date = '2017-06-15';

-- 3b. SELECT only needed columns (columnar storage = pay per column)
-- BAD:  SELECT * FROM marts.fct_orders_batch WHERE order_date = '2017-06-15';
-- GOOD: SELECT order_id, order_total FROM marts.fct_orders_batch WHERE order_date = '2017-06-15';

-- 3c. Use approximate functions for large-scale analytics
-- BAD:  SELECT COUNT(DISTINCT customer_key) FROM marts.fct_orders_batch WHERE ...;
-- GOOD: SELECT APPROX_COUNT_DISTINCT(customer_key) FROM marts.fct_orders_batch WHERE ...;

-- 3d. Avoid SELECT DISTINCT on large tables; use GROUP BY instead
-- 3e. Push filters before JOINs (BigQuery optimizer usually handles this)


-- ── 4. Table-level settings for cost control ─────────────────────────

-- Enable query caching (default, but explicit for documentation)
-- BigQuery caches identical query results for 24 hours at no cost.

-- Set maximum bytes billed per query (safety net):
-- ALTER PROJECT SET OPTIONS (default_query_max_bytes_billed = 10737418240);  -- 10 GB

-- In Dagster, set per-job:
-- job_config = {"query": {"maximum_bytes_billed": 10 * 1024**3}}


-- ── 5. Streaming Insert Best Practices ───────────────────────────────

-- 5a. Use the Storage Write API (not legacy streaming API)
--     - Lower cost: free (vs $0.05/GB for legacy)
--     - Exactly-once semantics with committed streams
--     - Higher throughput: 3 GB/s per project

-- 5b. Batch streaming writes (buffer 100 rows, flush every 5 seconds)
--     - Reduces API calls and cost
--     - Our consumers already batch via MAX_POLL_RECORDS=100

-- 5c. Stream to append-only table, deduplicate via view
--     - Avoids UPDATE/MERGE DML cost ($5/TB)
--     - ROW_NUMBER view handles dedup at query time (free)

-- 5d. Streaming buffer is queryable immediately but not in exports
--     - Data in buffer for ~90 minutes before committed to columnar
--     - Can query buffer: results are eventually consistent

-- 5e. Schema evolution: use the Storage Write API's schema update
--     - New fields added to messages? Add nullable columns to table
--     - Consumer drops unknown fields with logging (our pattern)
