-- ============================================================================
-- Phase 3I: Performance Test Queries
--
-- Each query annotated with:
--   - Use case (stream vs batch vs Lambda)
--   - Expected partitions scanned
--   - Cost optimization notes
--   - Estimated bytes processed (for ~100K orders dataset)
-- ============================================================================

-- ─────────────────────────────────────────────────────────────────────
-- Q1: Today's real-time metrics (STREAM use case)
-- Scans: 1 partition of fct_orders_stream (today)
-- Cost: ~0.5 MB × $5/TB = negligible
-- ─────────────────────────────────────────────────────────────────────

SELECT
    COUNT(DISTINCT order_id) AS orders_today,
    SUM(order_total) AS gmv_today,
    AVG(order_total) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM realtime.fct_orders_stream_deduped
WHERE DATE(processing_timestamp) = CURRENT_DATE();


-- ─────────────────────────────────────────────────────────────────────
-- Q2: Monthly revenue trend (BATCH use case)
-- Scans: 12 daily partitions per month, only 2017 year
-- Cost: ~365 partitions × ~2KB each = ~0.7 MB → negligible
-- Clustering on order_status prunes further if WHERE added
-- ─────────────────────────────────────────────────────────────────────

SELECT
    DATE_TRUNC(order_date, MONTH) AS month,
    COUNT(*) AS total_orders,
    SUM(order_total) AS total_revenue,
    AVG(order_total) AS avg_order_value,
    SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) AS late_deliveries
FROM marts.fct_orders_batch
WHERE order_date BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY month
ORDER BY month;


-- ─────────────────────────────────────────────────────────────────────
-- Q3: Last 30 days unified (LAMBDA use case)
-- Scans: ~30 batch partitions + 1 stream partition
-- Cost: ~30 × ~2KB + stream buffer = ~0.1 MB → negligible
-- Demonstrates Lambda: batch history + stream "today" in one query
-- ─────────────────────────────────────────────────────────────────────

SELECT
    order_date,
    _source_layer,
    COUNT(*) AS orders,
    SUM(order_total) AS revenue
FROM marts.fct_orders_unified
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY order_date, _source_layer
ORDER BY order_date DESC;


-- ─────────────────────────────────────────────────────────────────────
-- Q4: Order status breakdown with clustering benefit
-- Scans: all partitions BUT clustering on order_status prunes blocks
-- Without clustering: full table scan (~10 MB)
-- With clustering: ~2 MB (only reads 'delivered' blocks)
-- ─────────────────────────────────────────────────────────────────────

SELECT
    order_status,
    COUNT(*) AS order_count,
    SUM(order_total) AS total_revenue,
    AVG(delivery_time_days) AS avg_delivery_days
FROM marts.fct_orders_batch
WHERE order_date BETWEEN '2017-01-01' AND '2018-08-31'
    AND order_status = 'delivered'
GROUP BY order_status;


-- ─────────────────────────────────────────────────────────────────────
-- Q5: Real-time windowed metrics (STREAM use case)
-- Scans: last 24 hours of realtime_metrics
-- Clustered by metric_name → only reads relevant metric blocks
-- ─────────────────────────────────────────────────────────────────────

SELECT
    window_start,
    window_end,
    metric_value
FROM realtime.realtime_metrics
WHERE DATE(window_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND metric_name = 'total_orders'
ORDER BY window_start DESC;


-- ─────────────────────────────────────────────────────────────────────
-- Q6: A/B test comparison — batch vs live (LAMBDA use case)
-- Shows how batch (complete, daily) and stream (live, partial)
-- complement each other for experiment analysis
-- ─────────────────────────────────────────────────────────────────────

SELECT
    experiment_id,
    variant,
    live_impressions,
    live_conversions,
    live_conversion_rate,
    batch_total_orders,
    batch_conversion_rate,
    _primary_source
FROM marts.experiment_results_unified
ORDER BY experiment_id, variant;


-- ─────────────────────────────────────────────────────────────────────
-- Q7: Top products by revenue (BATCH use case with JOIN)
-- Uses fct_order_items + dim_products
-- Clustering on product_key speeds the join
-- ─────────────────────────────────────────────────────────────────────

SELECT
    p.product_category_name,
    COUNT(*) AS items_sold,
    SUM(oi.price) AS total_revenue,
    AVG(oi.price) AS avg_price
FROM marts.fct_order_items oi
JOIN marts.dim_products p ON oi.product_key = p.product_key
GROUP BY p.product_category_name
ORDER BY total_revenue DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────
-- Q8: Delivery SLA violations in last 7 days (STREAM use case)
-- Partitioned by created_at → only scans 7 partitions
-- ─────────────────────────────────────────────────────────────────────

SELECT
    DATE(created_at) AS alert_date,
    COUNT(*) AS total_alerts,
    AVG(days_to_deliver) AS avg_late_days,
    COUNT(DISTINCT seller_id) AS sellers_affected
FROM realtime.delivery_alerts
WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND is_late = TRUE
GROUP BY alert_date
ORDER BY alert_date DESC;


-- ============================================================================
-- COST ESTIMATION SUMMARY (for ~100K orders, ~112K items, ~100K reviews)
-- ============================================================================
--
-- Table                        Est. Size    Partitions
-- ──────────────────────────── ────────── ──────────
-- fct_orders_batch             ~10 MB       ~730 daily
-- fct_order_items              ~12 MB       none (small)
-- fct_reviews                  ~8 MB        ~730 daily
-- fct_daily_metrics            ~0.5 MB      ~730 daily
-- fct_orders_stream            ~1 MB/day    rolling daily
-- realtime_metrics             ~0.1 MB      rolling daily
-- experiments_realtime         ~0.001 MB    none (tiny)
-- dim_customers                ~5 MB        none
-- dim_products                 ~2 MB        none
-- dim_sellers                  ~0.5 MB      none
--
-- Monthly query cost estimate (moderate dashboard usage):
--   Batch queries: ~50 GB/month × $5/TB = $0.25/month
--   Stream queries: ~5 GB/month × $5/TB = $0.03/month
--   Total: ~$0.28/month (well within free tier 1 TB/month)
--
-- Streaming insert cost:
--   ~100K events/day × 1KB avg = ~100 MB/day
--   $0.05/GB → ~$0.005/day → ~$0.15/month
-- ============================================================================
