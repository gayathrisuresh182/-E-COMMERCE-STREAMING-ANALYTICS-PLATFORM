-- ============================================================================
-- Phase 3I: Batch Layer Tables (populated by Dagster daily)
--
-- Design: Option A — Separate batch + stream tables with unified views.
-- Batch tables use WRITE_TRUNCATE per partition (Dagster overwrites daily).
-- ============================================================================

-- ── fct_orders_batch ─────────────────────────────────────────────────
-- Grain: one row per order
-- Partitioned by order_date (daily), clustered for common query patterns

CREATE OR REPLACE TABLE marts.fct_orders_batch (
    fact_key            STRING      NOT NULL,
    order_id            STRING      NOT NULL,
    customer_key        STRING,
    seller_key          STRING,
    geography_key       STRING,
    date_key            STRING,
    order_date          DATE        NOT NULL,
    order_status        STRING,
    order_total         FLOAT64,
    item_count          INT64,
    payment_value       FLOAT64,
    freight_value       FLOAT64,
    profit_estimate     FLOAT64,
    avg_item_price      FLOAT64,
    delivery_time_days  INT64,
    is_late_delivery    BOOL,
    payment_type        STRING,
    payment_installments INT64,
    _loaded_at          TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY order_date
CLUSTER BY order_status, payment_type
OPTIONS (
    description = 'Batch fact table: one row per order, loaded daily by Dagster',
    labels = [("layer", "batch"), ("grain", "order")],
    partition_expiration_days = 730,
    require_partition_filter = TRUE
);


-- ── fct_order_items ──────────────────────────────────────────────────
-- Grain: one row per line item per order

CREATE OR REPLACE TABLE marts.fct_order_items (
    order_item_fact_key STRING      NOT NULL,
    order_id            STRING      NOT NULL,
    fact_key            STRING,
    order_item_id       INT64,
    product_key         STRING,
    seller_key          STRING,
    quantity            INT64,
    price               FLOAT64,
    freight_value       FLOAT64,
    item_total          FLOAT64,
    _loaded_at          TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY product_key, seller_key
OPTIONS (
    description = 'Order line items fact (batch), one row per item per order',
    labels = [("layer", "batch"), ("grain", "order_item")]
);


-- ── fct_reviews ──────────────────────────────────────────────────────
-- Grain: one row per review, partitioned by review creation date

CREATE OR REPLACE TABLE marts.fct_reviews (
    review_fact_key         STRING      NOT NULL,
    review_id               STRING      NOT NULL,
    order_id                STRING,
    fact_key                STRING,
    review_score            INT64,
    review_comment_length   INT64,
    response_time_hours     FLOAT64,
    sentiment               STRING,
    has_comment             BOOL,
    review_creation_date    DATE,
    _loaded_at              TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY review_creation_date
CLUSTER BY sentiment, review_score
OPTIONS (
    description = 'Reviews fact table, partitioned by review date',
    labels = [("layer", "batch"), ("grain", "review")],
    partition_expiration_days = 730,
    require_partition_filter = TRUE
);


-- ── fct_daily_metrics ────────────────────────────────────────────────
-- Grain: one row per date (aggregated from fct_orders_batch)

CREATE OR REPLACE TABLE marts.fct_daily_metrics (
    date_key                STRING,
    metric_date             DATE        NOT NULL,
    total_orders            INT64,
    total_gmv               FLOAT64,
    total_items             INT64,
    avg_order_value         FLOAT64,
    unique_customers        INT64,
    orders_delivered         INT64,
    orders_canceled          INT64,
    avg_delivery_time_days  FLOAT64,
    late_delivery_rate      FLOAT64,
    _loaded_at              TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY metric_date
OPTIONS (
    description = 'Daily aggregated business metrics, one row per date',
    labels = [("layer", "batch"), ("grain", "daily")],
    partition_expiration_days = 730
);


-- ── Dimension tables (no partitioning — small, full refresh) ─────────

CREATE OR REPLACE TABLE marts.dim_customers (
    customer_key            STRING      NOT NULL,
    customer_id             STRING,
    customer_unique_id      STRING,
    customer_zip_code_prefix STRING,
    customer_city           STRING,
    customer_state          STRING,
    _loaded_at              TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY customer_state
OPTIONS (
    description = 'Customer dimension (SCD Type 1, full refresh)',
    labels = [("layer", "batch"), ("grain", "customer")]
);

CREATE OR REPLACE TABLE marts.dim_products (
    product_key             STRING      NOT NULL,
    product_id              STRING,
    product_category_name   STRING,
    product_weight_g        FLOAT64,
    product_length_cm       FLOAT64,
    product_height_cm       FLOAT64,
    product_width_cm        FLOAT64,
    _loaded_at              TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY product_category_name
OPTIONS (
    description = 'Product dimension',
    labels = [("layer", "batch"), ("grain", "product")]
);

CREATE OR REPLACE TABLE marts.dim_sellers (
    seller_key              STRING      NOT NULL,
    seller_id               STRING,
    seller_zip_code_prefix  STRING,
    seller_city             STRING,
    seller_state            STRING,
    _loaded_at              TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY seller_state
OPTIONS (
    description = 'Seller dimension',
    labels = [("layer", "batch"), ("grain", "seller")]
);
