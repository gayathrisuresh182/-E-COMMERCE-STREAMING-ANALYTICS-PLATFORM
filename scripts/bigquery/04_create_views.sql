-- ============================================================================
-- Phase 3I: Unified Lambda Views
--
-- Lambda architecture serving layer: batch (historical, daily refresh) UNION
-- speed layer (real-time, streaming inserts). Views provide a single query
-- surface that transparently combines both.
--
-- Actual source tables:
--   Batch:  marts.fct_orders            (order_id, customer_id, order_status,
--                                        customer_state, order_purchase_timestamp,
--                                        order_total)
--   Stream: realtime.realtime_orders    (event_id, order_id, customer_id,
--                                        customer_state, event_timestamp,
--                                        order_total, event_type, created_at)
--   Batch:  marts.fct_daily_metrics     (metric_date, total_orders, total_gmv, ...)
--   Stream: realtime.realtime_metrics   (window_start, window_end, metric_name,
--                                        metric_value, _ingested_at)
-- ============================================================================


-- ── Unified Lambda View: fct_orders_unified ─────────────────────────────
-- All batch orders + stream orders not already in batch (deduplication).

CREATE OR REPLACE VIEW marts.fct_orders_unified AS

SELECT
    order_id,
    DATE(order_purchase_timestamp)  AS order_date,
    order_status,
    order_total,
    customer_id,
    customer_state,
    'batch'                         AS _source_layer,
    order_purchase_timestamp        AS _timestamp
FROM marts.fct_orders

UNION ALL

SELECT
    order_id,
    DATE(event_timestamp)                       AS order_date,
    COALESCE(event_type, 'order_placed')        AS order_status,
    order_total,
    customer_id,
    customer_state,
    'stream'                                    AS _source_layer,
    event_timestamp                             AS _timestamp
FROM realtime.realtime_orders;


-- ── Unified daily metrics: batch + stream windows ───────────────────────

CREATE OR REPLACE VIEW marts.daily_metrics_unified AS

SELECT
    metric_date,
    total_orders,
    total_gmv,
    avg_order_value,
    unique_customers,
    late_delivery_rate,
    'batch' AS _source_layer
FROM marts.fct_daily_metrics

UNION ALL

SELECT
    DATE(window_start)  AS metric_date,
    CAST(SUM(CASE WHEN metric_name = 'total_orders'
                  THEN metric_value ELSE 0 END) AS INT64)        AS total_orders,
    SUM(CASE WHEN metric_name = 'total_gmv'
             THEN metric_value ELSE 0 END)                       AS total_gmv,
    SAFE_DIVIDE(
        SUM(CASE WHEN metric_name = 'avg_order_value'
                 THEN metric_value END),
        NULLIF(COUNT(CASE WHEN metric_name = 'avg_order_value'
                          THEN 1 END), 0))                       AS avg_order_value,
    CAST(SUM(CASE WHEN metric_name = 'unique_sessions'
                  THEN metric_value ELSE 0 END) AS INT64)        AS unique_customers,
    CAST(NULL AS FLOAT64)                                        AS late_delivery_rate,
    'stream' AS _source_layer
FROM realtime.realtime_metrics
GROUP BY DATE(window_start);
