-- Phase 1E: Redshift schema and table DDL (optional)
-- Run after creating Redshift cluster. Replace YOUR_CLUSTER_ENDPOINT, DB, USER.
-- Use: psql -h YOUR_CLUSTER -U admin -d dev -f scripts/redshift_create_schema.sql

-- Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Staging (ALL distribution for even spread)
CREATE TABLE IF NOT EXISTS staging.stg_orders (
  order_id VARCHAR(32) NOT NULL,
  customer_id VARCHAR(32) NOT NULL,
  order_status VARCHAR(32),
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP,
  order_total DECIMAL(12,2),
  created_at TIMESTAMP
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS staging.stg_customers (
  customer_id VARCHAR(32) NOT NULL,
  customer_unique_id VARCHAR(32),
  customer_zip_code_prefix VARCHAR(8),
  customer_city VARCHAR(64),
  customer_state VARCHAR(4),
  created_at TIMESTAMP
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS staging.stg_products (
  product_id VARCHAR(32) NOT NULL,
  product_category_name VARCHAR(64),
  product_category_name_english VARCHAR(64),
  product_name_length INT,
  product_description_length INT,
  product_photos_qty INT,
  product_weight_g DECIMAL(10,2),
  product_length_cm DECIMAL(10,2),
  product_height_cm DECIMAL(10,2),
  product_width_cm DECIMAL(10,2),
  created_at TIMESTAMP
) DISTSTYLE ALL;

-- Marts: distkey + sortkey
CREATE TABLE IF NOT EXISTS marts.fct_orders (
  order_id VARCHAR(32) NOT NULL,
  customer_id VARCHAR(32) NOT NULL,
  order_status VARCHAR(32),
  customer_state VARCHAR(4),
  order_purchase_timestamp TIMESTAMP,
  order_total DECIMAL(12,2)
)
DISTKEY(customer_id)
SORTKEY(order_purchase_timestamp);
