-- ============================================================================
-- Phase 3I: Deduplication Views + Unified Lambda Views
--
-- Lambda architecture: batch layer (historical, daily refresh) UNION
-- speed layer (real-time, streaming inserts). Views provide a single
-- query surface that transparently combines both.
-- ============================================================================

-- ── Deduplication view for streaming orders ──────────────────────────
-- The stream table is append-only; consumer retries can insert duplicates.
-- ROW_NUMBER deduplication keeps only the latest version per order_id.

CREATE OR REPLACE VIEW realtime.fct_orders_stream_deduped AS
SELECT * EXCEPT (_row_num)
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY processing_timestamp DESC
        ) AS _row_num
    FROM realtime.fct_orders_stream
)
WHERE _row_num = 1;


-- ── Unified Lambda View: fct_orders_unified ──────────────────────────
-- Combines batch (historical, SoR) with stream (recent, near-real-time).
--
-- Strategy: batch owns all data up to yesterday; stream owns today only.
-- This avoids double-counting when the daily batch job runs and overwrites
-- today's data from staging into the batch table.
--
-- Query pattern:
--   SELECT * FROM marts.fct_orders_unified
--   WHERE order_date >= '2024-01-01'
--   → Scans batch partitions for historical + stream for today

CREATE OR REPLACE VIEW marts.fct_orders_unified AS

-- Batch layer: all historical data (up to yesterday)
SELECT
    order_id,
    order_date,
    order_status,
    order_total,
    item_count,
    customer_key,
    seller_key,
    payment_type,
    delivery_time_days,
    is_late_delivery,
    'batch' AS _source_layer,
    _loaded_at AS _timestamp
FROM marts.fct_orders_batch
WHERE order_date < CURRENT_DATE()

UNION ALL

-- Speed layer: today only (deduplicated stream)
SELECT
    order_id,
    DATE(event_timestamp) AS order_date,
    order_status,
    order_total,
    CAST(NULL AS INT64) AS item_count,
    CAST(NULL AS STRING) AS customer_key,
    CAST(NULL AS STRING) AS seller_key,
    CAST(NULL AS STRING) AS payment_type,
    CAST(NULL AS INT64) AS delivery_time_days,
    CAST(NULL AS BOOL) AS is_late_delivery,
    'stream' AS _source_layer,
    processing_timestamp AS _timestamp
FROM realtime.fct_orders_stream_deduped
WHERE DATE(processing_timestamp) >= CURRENT_DATE();


-- ── Unified daily metrics: batch + stream windows ────────────────────
-- For dashboards: historical daily metrics from batch, plus today's
-- running totals from the 5-minute windowed stream.

CREATE OR REPLACE VIEW marts.daily_metrics_unified AS

-- Batch: historical daily metrics
SELECT
    metric_date,
    total_orders,
    total_gmv,
    avg_order_value,
    unique_customers,
    late_delivery_rate,
    'batch' AS _source_layer
FROM marts.fct_daily_metrics
WHERE metric_date < CURRENT_DATE()

UNION ALL

-- Stream: today's running totals (sum of 5-minute windows)
SELECT
    DATE(window_start) AS metric_date,
    SUM(CASE WHEN metric_name = 'total_orders' THEN metric_value END) AS total_orders,
    SUM(CASE WHEN metric_name = 'total_gmv' THEN metric_value END) AS total_gmv,
    SUM(CASE WHEN metric_name = 'avg_order_value' THEN metric_value END)
        / NULLIF(COUNT(CASE WHEN metric_name = 'avg_order_value' THEN 1 END), 0) AS avg_order_value,
    SUM(CASE WHEN metric_name = 'unique_sessions' THEN metric_value END) AS unique_customers,
    CAST(NULL AS FLOAT64) AS late_delivery_rate,
    'stream' AS _source_layer
FROM realtime.realtime_metrics
WHERE DATE(window_start) >= CURRENT_DATE()
GROUP BY DATE(window_start);


-- ── Unified experiment results ───────────────────────────────────────
-- Combines batch experiment analysis with live streaming counters.
-- Prefer stream data when available (more recent), fall back to batch.

CREATE OR REPLACE VIEW marts.experiment_results_unified AS
SELECT
    COALESCE(s.experiment_id, b.experiment_id) AS experiment_id,
    COALESCE(s.variant, b.variant) AS variant,
    COALESCE(s.impressions, 0) AS live_impressions,
    COALESCE(s.conversions, 0) AS live_conversions,
    COALESCE(s.conversion_rate, 0) AS live_conversion_rate,
    COALESCE(s.revenue, 0) AS live_revenue,
    b.unique_users AS batch_unique_users,
    b.total_orders AS batch_total_orders,
    b.conversion_rate AS batch_conversion_rate,
    b.total_revenue AS batch_total_revenue,
    CASE
        WHEN s.experiment_id IS NOT NULL THEN 'stream'
        ELSE 'batch'
    END AS _primary_source
FROM realtime.experiments_realtime s
FULL OUTER JOIN marts.fct_experiment_results b
    ON s.experiment_id = b.experiment_id
    AND s.variant = b.variant;
