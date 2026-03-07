-- ============================================================================
-- Phase 3I: Stream Layer Tables (populated by Kafka consumers)
--
-- These tables receive streaming inserts via the BigQuery Storage Write API.
-- Designed for append-only writes; deduplication handled by views.
--
-- Key differences from batch:
--   - No require_partition_filter (real-time queries need latest data fast)
--   - processing_timestamp for write-time ordering
--   - Partition by _PARTITIONTIME or ingestion time for streaming
-- ============================================================================

-- ── fct_orders_stream ────────────────────────────────────────────────
-- Append-only table receiving streaming inserts from OrderStreamProcessor.
-- May contain duplicates if consumer retries; deduplicated via view.

CREATE OR REPLACE TABLE realtime.fct_orders_stream (
    order_id                STRING      NOT NULL,
    customer_id             STRING,
    customer_state          STRING,
    customer_tier           STRING,
    order_total             FLOAT64,
    order_status            STRING,
    event_type              STRING,
    experiment_id           STRING,
    variant                 STRING,
    event_timestamp         TIMESTAMP,
    processing_timestamp    TIMESTAMP   NOT NULL,
    _ingested_at            TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(processing_timestamp)
CLUSTER BY order_status, event_type
OPTIONS (
    description = 'Streaming orders from Kafka OrderStreamProcessor (append-only)',
    labels = [("layer", "stream"), ("grain", "order_event")]
);


-- ── realtime_metrics ─────────────────────────────────────────────────
-- 5-minute windowed aggregations from MetricsAggregator consumer.
-- Upserted by window_start + metric_name (handled at application layer).

CREATE OR REPLACE TABLE realtime.realtime_metrics (
    window_start            TIMESTAMP   NOT NULL,
    window_end              TIMESTAMP   NOT NULL,
    metric_name             STRING      NOT NULL,
    metric_value            FLOAT64,
    _ingested_at            TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(window_start)
CLUSTER BY metric_name
OPTIONS (
    description = '5-minute tumbling window metrics from Kafka MetricsAggregator',
    labels = [("layer", "stream"), ("grain", "window_metric")],
    partition_expiration_days = 30
);


-- ── experiments_realtime ─────────────────────────────────────────────
-- Running A/B test totals from ExperimentTracker consumer.
-- Small table (one row per experiment x variant), no partitioning needed.

CREATE OR REPLACE TABLE realtime.experiments_realtime (
    experiment_id           STRING      NOT NULL,
    variant                 STRING      NOT NULL,
    impressions             INT64,
    clicks                  INT64,
    conversions             INT64,
    revenue                 FLOAT64,
    conversion_rate         FLOAT64,
    updated_at              TIMESTAMP,
    _ingested_at            TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY experiment_id
OPTIONS (
    description = 'Live A/B test metrics from Kafka ExperimentTracker',
    labels = [("layer", "stream"), ("grain", "experiment_variant")]
);


-- ── delivery_alerts ──────────────────────────────────────────────────
-- SLA violation alerts from DeliverySLAMonitor consumer.

CREATE OR REPLACE TABLE realtime.delivery_alerts (
    order_id                STRING      NOT NULL,
    seller_id               STRING,
    shipped_at              TIMESTAMP,
    delivered_at            TIMESTAMP,
    days_to_deliver         FLOAT64,
    is_late                 BOOL,
    alert_severity          STRING,
    created_at              TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
OPTIONS (
    description = 'Delivery SLA alerts from Kafka DeliverySLAMonitor',
    labels = [("layer", "stream"), ("grain", "delivery_alert")],
    partition_expiration_days = 90
);
