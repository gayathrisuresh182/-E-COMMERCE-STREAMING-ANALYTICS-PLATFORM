-- Phase 1D: BigQuery table DDL (run after creating datasets)
-- Replace YOUR_PROJECT_ID with your GCP project ID before running
-- Run: bq query --use_legacy_sql=false --project_id=YOUR_PROJECT_ID "$(cat scripts/bigquery_create_tables.sql | sed 's/YOUR_PROJECT_ID/your-actual-project/g')"
-- Or use: python scripts/bigquery_create_tables.py --project YOUR_PROJECT_ID

-- ============= STAGING =============
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.staging.stg_orders` (
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  order_status STRING,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP,
  order_total FLOAT64,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.staging.stg_customers` (
  customer_id STRING NOT NULL,
  customer_unique_id STRING,
  customer_zip_code_prefix STRING,
  customer_city STRING,
  customer_state STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.staging.stg_products` (
  product_id STRING NOT NULL,
  product_category_name STRING,
  product_category_name_english STRING,
  product_name_length INT64,
  product_description_length INT64,
  product_photos_qty INT64,
  product_weight_g FLOAT64,
  product_length_cm FLOAT64,
  product_height_cm FLOAT64,
  product_width_cm FLOAT64,
  created_at TIMESTAMP
);

-- ============= MARTS (partitioned + clustered) =============
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.marts.fct_orders` (
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  order_status STRING,
  customer_state STRING,
  order_purchase_timestamp TIMESTAMP,
  order_total FLOAT64
)
PARTITION BY DATE(order_purchase_timestamp)
CLUSTER BY order_status, customer_state
OPTIONS (
  partition_expiration_days = 730,
  description = 'Order fact table - partitioned by order date'
);

-- ============= REALTIME =============
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.realtime.realtime_orders` (
  event_id STRING NOT NULL,
  order_id STRING,
  customer_id STRING,
  event_timestamp TIMESTAMP,
  order_total FLOAT64,
  event_type STRING,
  created_at TIMESTAMP
)
PARTITION BY DATE(event_timestamp)
OPTIONS (
  partition_expiration_days = 7,
  description = 'Real-time orders from Kafka - 7 day retention'
);

-- ============= EXPERIMENTS =============
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.experiments.experiment_results` (
  experiment_id STRING NOT NULL,
  variant STRING NOT NULL,
  n_users INT64,
  conversions INT64,
  conversion_rate FLOAT64,
  revenue FLOAT64,
  p_value FLOAT64,
  created_at TIMESTAMP
);
