# Phase 3B: Staging Layer — Data Cleaning and Standardization

> Staging assets transform raw Olist data into clean, typed, and enriched DataFrames at the same grain as the source. This is where data quality problems are fixed so downstream dimension and fact tables receive trustworthy inputs.

---

## Overview

Raw e-commerce data is messy. Timestamps arrive as strings, categories are in Portuguese, geolocation has duplicate ZIP codes, and order statuses contain inconsistent casing. The staging layer exists to absorb this complexity so that every downstream asset can assume clean, well-typed data.

Staging follows a critical principle: **same grain, no aggregation**. Each staging asset produces one row per source record (after deduplication). Aggregation is deferred to fact and dimension layers where business logic determines the correct grain.

---

## Staging Asset Inventory

| Asset | Dependencies | Key Transformations |
|-------|-------------|---------------------|
| **stg_geolocation** | `raw_geolocation` | Deduplicate by ZIP prefix; median lat/lng per ZIP |
| **stg_orders** | `raw_orders`, `raw_order_items` | Datetime parsing, delivery metrics, order totals, status normalization |
| **stg_customers** | `raw_customers`, `stg_geolocation` | State name expansion (SP to Sao Paulo), lat/lng enrichment |
| **stg_products** | `raw_products` | Portuguese-to-English category translation, volume/weight derivation |
| **stg_order_items** | `raw_order_items` | Type coercion, `item_total` calculation |
| **stg_payments** | `raw_order_payments` | Null removal, numeric coercion, payment type standardization |
| **stg_reviews** | `raw_order_reviews`, `stg_orders` | Sentiment classification, response time calculation |
| **stg_sellers** | `raw_sellers`, `stg_geolocation` | Lat/lng enrichment from geolocation |

> **Why stg_geolocation runs first:** It is a dependency for both `stg_customers` and `stg_sellers`. By deduplicating geolocation data (1M+ rows down to ~19K unique ZIPs) early, downstream joins are fast and deterministic.

---

## Transformation Details

### stg_orders — The Central Staging Asset

The orders table is the spine of the entire warehouse. It receives the most transformations:

- **Type casting:** Five timestamp columns parsed from strings to `datetime64`
- **Data quality:** Null `order_id` rows dropped; `order_status` stripped and lowercased
- **Derived columns:**
  - `order_date` / `order_hour` — extracted from purchase timestamp
  - `delivery_time_days` — actual delivery minus purchase date
  - `is_late` — delivered after estimated delivery date
  - `order_status_category` — grouped into `completed`, `in_progress`, `canceled`, `other`
  - `order_total` — sum of item prices per order (joined from `raw_order_items`)
  - `order_size_category` — `small` (<$50), `medium` ($50-$200), `large` (>$200)

### stg_geolocation — Deduplication at Scale

The raw geolocation dataset contains multiple lat/lng readings per ZIP code. Staging reduces this to one canonical row per ZIP using median coordinates and first-seen city/state. This is a classic deduplication pattern for geographic reference data.

### stg_products — Cross-Language Standardization

Product categories arrive in Portuguese. Staging joins against a translation table (`product_category_name_translation.csv`) and derives physical attributes:

- `product_volume_cm3` = length x height x width
- `product_weight_category` — `light` (<500g), `medium` (500-2000g), `heavy` (>2000g)

### stg_reviews — Sentiment Engineering

Reviews are enriched with:

- `review_sentiment` — `positive` (score 4-5), `neutral` (3), `negative` (1-2)
- `days_since_delivery` — review date minus delivery date (joined from `stg_orders`)

---

## Data Quality Strategy

### The Philosophy

> **Why fix problems in staging:** Staging is the single place where raw data quirks are absorbed. Downstream assets should never need defensive null checks or type coercion. This follows the "clean once, use many" principle from Kimball's data warehousing methodology.

### Sanitization Rules

| Issue | Source | Fix Applied |
|-------|--------|-------------|
| Null `order_id` | `raw_orders`, `raw_order_payments` | Rows dropped |
| Unknown `order_status` values | `raw_orders` | Normalized to `"unknown"` |
| `order_approved_at` < `order_purchase_timestamp` | `raw_orders` (data entry errors) | Clamped: approved >= purchase |
| `order_delivered_date` < `order_approved_at` | `raw_orders` | Clamped: delivered >= approved |
| Mixed-case `payment_type` | `raw_order_payments` | Trimmed and lowercased |

### Asset Checks

| Check | Asset | What It Validates |
|-------|-------|-------------------|
| `check_stg_orders_no_null_ids` | `stg_orders` | Zero null `order_id` values |
| `check_stg_orders_valid_status` | `stg_orders` | All statuses in canonical set |
| `check_stg_orders_timestamps` | `stg_orders` | Temporal ordering: approved >= purchased, delivered >= approved |
| `check_stg_payments_no_null_order_id` | `stg_payments` | Zero null `order_id` values |

> **Why checks stay strict:** Rather than relaxing validations to accommodate dirty data, staging fixes the data so strict checks always pass. This means a failing check genuinely signals a new, unexpected data quality issue rather than a known quirk.

---

## Dependency Graph

```
raw_geolocation ──────────────────> stg_geolocation ──┬──> stg_customers
                                                      └──> stg_sellers

raw_orders ──────┬──> stg_orders ──> stg_reviews
raw_order_items ─┘                     ^
                                       |
raw_order_reviews ─────────────────────┘

raw_customers ──> stg_customers
raw_products ───> stg_products
raw_order_items ──> stg_order_items
raw_order_payments ──> stg_payments
raw_sellers ──> stg_sellers
```

---

## Persistence Strategy

| Environment | I/O Manager | Storage Location | Format |
|-------------|-------------|-----------------|--------|
| **Development** | `FilesystemIOManager` | `.dagster/storage/` | Pickle |
| **Production** | `BigQueryIOManager` | `staging.stg_*` tables | BigQuery native |

> **Why two I/O managers:** Local development should not require BigQuery credentials. The `FilesystemIOManager` enables rapid iteration. In production, switching to `BigQueryIOManager` only requires changing the `io_manager_key` on assets — no transformation logic changes.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Grain preservation | Same as source (no aggregation) | Keeps staging predictable; aggregation belongs in facts |
| Timestamp sanitization | Clamp to logical order | Strict checks pass; downstream calculations are valid |
| Category translation | Join to translation CSV | Avoids hardcoded mappings; extensible to new categories |
| Geolocation dedup | Median lat/lng per ZIP | Statistically robust; handles GPS jitter in source data |
| Status normalization | Map unknowns explicitly | Prevents silent data loss from unexpected enum values |

---

## Production Considerations

- **Scaling to 100x volume:** Replace pandas with PySpark or BigQuery SQL transforms. The staging contract (same grain, typed columns, quality checks) remains unchanged.
- **Schema evolution:** If Olist adds columns, staging assets should log and drop unknown columns rather than failing. Add explicit column selection lists.
- **Monitoring:** Track row counts across materializations. A sudden drop in `stg_orders` row count (vs. `raw_orders`) indicates a new data quality issue in the source.
- **Incremental staging:** For live e-commerce data, staging would process only new/changed records using Dagster's partition-aware dependencies. The Olist dataset is static, so full refresh is appropriate.
- **Cost in BigQuery:** Staging tables are intermediate. Consider using BigQuery `WRITE_TRUNCATE` to avoid accumulating historical staging snapshots that consume storage.

---

*Last updated: March 2026*
