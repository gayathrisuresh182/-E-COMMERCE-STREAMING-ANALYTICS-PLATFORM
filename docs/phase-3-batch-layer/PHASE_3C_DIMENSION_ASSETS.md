# Phase 3C: Dimension Tables — Star Schema Entity Modeling

> Dimension assets model the "who, what, where, when" of e-commerce transactions using a star schema with hash-based surrogate keys and Type 1 (overwrite) SCD, optimized for analytical query patterns.

---

## Overview

Fact tables answer "how much" and "how many," but without dimensions they lack context. A row in `fct_orders` with `customer_key = cust_a1b2c3` is meaningless until joined to `dim_customers` to reveal the customer's city, tier, and lifetime value.

This project implements five dimension tables following Kimball's dimensional modeling methodology. Each dimension has a single row per entity, enriched with derived attributes that eliminate the need for complex joins in analytical queries.

---

## Dimension Inventory

| Dimension | Grain | Source Dependencies | Row Count | Surrogate Key |
|-----------|-------|--------------------|-----------|---------------|
| **dim_dates** | One row per calendar date | None (generated) | ~730 | `date_key` (date string) |
| **dim_geography** | One row per ZIP prefix | `stg_geolocation` | ~19,000 | `geography_key` |
| **dim_products** | One row per product | `stg_products` | ~32,951 | `product_key` |
| **dim_customers** | One row per customer | `stg_customers`, `stg_orders` | ~99,441 | `customer_key` |
| **dim_sellers** | One row per seller | `stg_sellers`, `stg_order_items`, `stg_orders`, `stg_reviews` | ~3,095 | `seller_key` |

---

## Dimension Design

### dim_dates — The Calendar Spine

**Dependencies:** None. Generated programmatically for the Olist analysis window (2016-09-01 to 2018-08-31).

Every data warehouse needs a date dimension. It enables time-based analysis without date function calls in every query.

| Attribute | Example | Purpose |
|-----------|---------|---------|
| `date` | 2017-06-15 | Join key from fact tables |
| `year`, `quarter`, `month` | 2017, Q2, 6 | Hierarchical drill-down |
| `day_of_week`, `day_name` | 4, Thursday | Day-of-week analysis |
| `is_weekend` | FALSE | Weekend vs. weekday segmentation |
| `is_holiday` | FALSE | Brazilian holiday flag (New Year, Christmas) |
| `fiscal_year`, `fiscal_quarter` | 2017, Q2 | Financial reporting (calendar-aligned) |

> **Why a date dimension instead of date functions:** Pre-computed attributes like `is_holiday` and `fiscal_quarter` avoid repeated logic in queries. BI tools (Looker, Metabase) can use the dimension directly for drill-downs without custom SQL.

### dim_geography — Brazilian Regional Analysis

**Dependencies:** `stg_geolocation`

Transforms ZIP-level geolocation into a geography dimension with regional groupings based on IBGE (Brazilian Institute of Geography and Statistics) regions.

| Attribute | Derivation |
|-----------|-----------|
| `geography_key` | Hash of `zip_code_prefix` |
| `zip_code_prefix` | Natural key |
| `city`, `state` | From geolocation data |
| `latitude`, `longitude` | Median per ZIP (from staging dedup) |
| `region` | IBGE region: Southeast, South, Northeast, North, Center-West |

> **Why regions matter for e-commerce:** Brazilian e-commerce has stark regional patterns. Southeast (Sao Paulo, Rio) accounts for ~60% of orders. Shipping times and costs vary dramatically by region, making geographic segmentation essential for logistics analysis.

### dim_products — Category Hierarchies

**Dependencies:** `stg_products`

| Attribute | Derivation |
|-----------|-----------|
| `product_key` | Hash of `product_id` |
| `product_category_name_english` | Translated from Portuguese in staging |
| `product_category_group` | Grouped: `health_beauty` to `lifestyle`, `computers_accessories` to `electronics` |
| `product_size_category` | By volume: `small` (<5K cm3), `medium` (5K-50K), `large` (>50K) |
| `product_weight_category` | From staging: `light`, `medium`, `heavy` |
| Physical attributes | `length`, `height`, `width`, `weight` |

### dim_customers — Behavioral Segmentation

**Dependencies:** `stg_customers`, `stg_orders`

This is one of the richest dimensions, combining identity data with behavioral aggregates computed from delivered orders.

| Attribute | Derivation |
|-----------|-----------|
| `customer_key` | Hash of `customer_id` |
| `customer_unique_id` | Olist's cross-order identifier |
| `total_orders` | Count of delivered orders |
| `total_spent` | Sum of order totals (delivered only) |
| `avg_order_value` | Mean order value |
| `customer_tier` | `VIP` (>10 orders), `regular` (2-10), `new` (1) |
| `customer_value` | `high` (>$1000 LTV), `medium` ($100-$1000), `low` (<$100) |
| `customer_region` | IBGE region from state |
| `customer_lat`, `customer_lng` | From geolocation join |

> **Why behavioral attributes in the dimension:** Pre-computing `customer_tier` and `customer_value` in the dimension avoids expensive aggregations at query time. Dashboards can filter by tier instantly rather than re-scanning the entire orders fact table.

### dim_sellers — Performance Profiling

**Dependencies:** `stg_sellers`, `stg_order_items`, `stg_orders`, `stg_reviews`

The most dependency-heavy dimension, combining seller identity with fulfillment performance metrics.

| Attribute | Derivation |
|-----------|-----------|
| `seller_key` | Hash of `seller_id` |
| `total_orders_fulfilled` | Count distinct orders from order items |
| `avg_review_score` | Mean review score via order items to reviews join |
| `avg_delivery_time_days` | Mean delivery time via order items to orders join |
| `seller_tier` | `top` (>=4.5 avg review), `good` (4-4.5), `average` (3-4), `poor` (<3) |
| `seller_region` | IBGE region from state |

---

## Surrogate Key Strategy

| Aspect | Design Choice | Rationale |
|--------|--------------|-----------|
| **Method** | SHA-256 hash of natural key, first 16 hex chars | Deterministic; same input always produces same key |
| **Prefix** | `cust_`, `prd_`, `sell_`, `geo_` | Human-readable; easy to identify key type in joins |
| **No auto-increment** | Hash-based, not sequential | No database dependency; works with filesystem I/O manager |
| **Reproducibility** | Identical across pipeline runs | Safe for incremental loads; no key collisions across environments |

> **Why hashed surrogate keys over auto-increment:** Auto-increment requires a stateful database to guarantee uniqueness across runs. Hash-based keys are deterministic — the same `customer_id` always maps to the same `customer_key`, whether running locally or in production. This is essential for a pipeline that may run in different environments.

---

## Slowly Changing Dimensions (SCD)

| SCD Type | Behavior | Used Here? | Rationale |
|----------|----------|-----------|-----------|
| **Type 0** | Never changes | No | Too restrictive |
| **Type 1** | Overwrite current value | **Yes** | Simplicity; Olist data is static |
| **Type 2** | Track history with effective dates | No (future) | Adds complexity without benefit for static data |
| **Type 3** | Store previous + current | No | Limited history (only one prior value) |

**Why Type 1 for this project:** The Olist dataset is a historical snapshot. Customer addresses and seller ratings do not change between materializations. For a live e-commerce platform, `dim_customers` would likely use Type 2 SCD to track address changes for shipping analytics.

> **Portfolio extension point:** Adding SCD Type 2 to `dim_customers` (tracking address history with `effective_from` / `effective_to` dates) would demonstrate advanced dimensional modeling skills. The current Type 1 design keeps the scope manageable.

---

## Asset Checks

| Check | Asset | Validates |
|-------|-------|-----------|
| `check_dim_customers_unique_customer_id` | `dim_customers` | No duplicate `customer_id` (grain integrity) |
| `check_dim_sellers_unique_seller_id` | `dim_sellers` | No duplicate `seller_id` (grain integrity) |

Grain integrity checks are the most critical validation for dimensions. A duplicate row in a dimension table causes fan-out in fact table joins, silently inflating metrics.

---

## Dependency Graph

```
(generated)           ──> dim_dates

stg_geolocation       ──> dim_geography

stg_products          ──> dim_products

stg_customers    ──┐
stg_orders       ──┴──> dim_customers

stg_sellers      ──┐
stg_order_items  ──┤
stg_orders       ──┤──> dim_sellers
stg_reviews      ──┘
```

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Schema style | Star schema (denormalized dims) | Optimized for analytical queries; fewer joins |
| Surrogate keys | SHA-256 hash with prefix | Deterministic, portable, human-readable |
| SCD type | Type 1 (overwrite) | Static dataset; no change tracking needed |
| Behavioral attributes | Pre-computed in dimensions | Eliminates expensive aggregations at query time |
| Normalization | Denormalized (e.g., `product_category_group` in `dim_products`) | Star schema convention; snowflaking deferred |

---

## Production Considerations

- **Dimension table size:** All five dimensions total under 15 MB. No partitioning needed. At 100x scale (10M customers), consider BigQuery clustering on `customer_state` or `customer_tier`.
- **Refresh frequency:** Dimensions refresh daily as part of the `refresh_dimensions` job (Phase 3J). Full refresh is fast given the small table sizes.
- **Late-arriving dimensions:** If a fact record references a `customer_id` not yet in `dim_customers`, the FK join produces a null surrogate key. Monitor null key rates in fact table metadata.
- **Conformed dimensions:** `dim_dates` and `dim_geography` are shared across all fact tables (conformed dimensions in Kimball terminology). Changes to these dimensions affect every downstream fact.
- **Testing strategy:** Materialize dimensions independently before facts. `dim_dates` has no dependencies and serves as a quick smoke test for the pipeline.

---

*Last updated: March 2026*
