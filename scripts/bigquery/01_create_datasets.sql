-- ============================================================================
-- Phase 3I: BigQuery Dataset Creation
-- Separate datasets for logical isolation: staging, marts, realtime
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging
OPTIONS (
    description = 'Raw and staged data from source systems',
    default_table_expiration_days = 90,
    location = 'US'
);

CREATE SCHEMA IF NOT EXISTS marts
OPTIONS (
    description = 'Curated fact and dimension tables for analytics',
    location = 'US'
);

CREATE SCHEMA IF NOT EXISTS realtime
OPTIONS (
    description = 'Real-time streaming tables populated by Kafka consumers',
    location = 'US'
);
