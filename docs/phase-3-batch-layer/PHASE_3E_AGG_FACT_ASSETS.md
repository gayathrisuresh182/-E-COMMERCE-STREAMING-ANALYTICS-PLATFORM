# Phase 3E: Aggregated Fact Tables — Multi-Grain Summary Analytics

> Four aggregated fact tables roll up detail-grain data into daily metrics, product performance, seller performance, and A/B experiment results — demonstrating the data warehouse pattern of deriving higher-level analytics from a single source of truth.

---

## Overview

Detail-grain fact tables (Phase 3D) capture every transaction, but dashboards and reports rarely need row-level data. Aggregated facts pre-compute the metrics that analysts and product managers actually query: daily GMV, seller fulfillment rates, product review scores, and experiment conversion rates.

The critical design principle: **aggregated facts are built from detail facts, not from staging.** This ensures all FK lookups, calculated measures, and data quality fixes flow through a single source of truth. Changing aggregation logic requires rematerializing only the aggregate, not reprocessing raw data.

---

## Aggregated Fact Inventory

| Asset | Grain | Rows | Partitioned | Dependencies |
|-------|-------|------|-------------|-------------|
| `fct_daily_metrics` | One row per date | 1 per partition | Yes (daily) | `fct_orders` |
| `fct_experiment_results` | One row per experiment x variant | 20 | No | `raw_experiment_assignments`, `stg_orders` |
| `fct_product_performance` | One row per product | ~32,951 | No | `fct_order_items`, `stg_reviews`, `dim_products` |
| `fct_seller_performance` | One row per seller | ~3,095 | No | `fct_order_items`, `stg_orders`, `stg_reviews`, `dim_sellers` |

---

## fct_daily_metrics — Operational Dashboard Foundation

### Purpose

This is the table behind the daily operations dashboard. It answers questions like: How many orders did we process yesterday? What was the GMV? Are deliveries running late?

### Measures

| Column | Aggregation | Business Use |
|--------|-------------|-------------|
| `total_orders` | COUNT | Volume tracking |
| `total_gmv` | SUM(`order_total`) | Gross Merchandise Value |
| `total_items` | SUM(`item_count`) | Basket size trends |
| `avg_order_value` | MEAN(`order_total`) | AOV monitoring |
| `unique_customers` | COUNT DISTINCT `customer_key` | Customer acquisition |
| `orders_delivered` | COUNT WHERE `delivered` | Fulfillment rate |
| `orders_canceled` | COUNT WHERE `canceled` | Cancellation monitoring |
| `avg_delivery_time_days` | MEAN(`delivery_time_days`) | Logistics SLA tracking |
| `late_delivery_rate` | late / total x 100 | SLA compliance percentage |
| `total_revenue` | SUM(`payment_value`) | Revenue recognition |
| `total_freight` | SUM(`freight_value`) | Shipping cost analysis |
| `avg_items_per_order` | MEAN(`item_count`) | Cross-sell effectiveness |

### Partition Strategy

Uses the same `DailyPartitionsDefinition` as `fct_orders`. Dagster applies **identity partition mapping** — partition `2017-01-15` of `fct_daily_metrics` reads exactly partition `2017-01-15` of `fct_orders`. No configuration required.

> **Why partition daily metrics:** Incremental processing means only yesterday's partition is recomputed each night. Full rematerialization of 729 daily partitions is only needed if the aggregation logic changes.

---

## fct_experiment_results — A/B Test Analysis

### Purpose

Provides a pre-computed summary of all A/B experiments with conversion rates and revenue metrics per variant, ready for statistical analysis.

### Design

- **Grain:** One row per experiment x variant (10 experiments x 2 variants = 20 rows)
- **Not partitioned:** Experiments span multiple dates; splitting by date would fragment the statistical analysis
- **Source:** `raw_experiment_assignments` joined to `stg_orders` on `customer_id`

### Measures

| Column | Aggregation |
|--------|-------------|
| `unique_users` | COUNT DISTINCT `customer_id` per variant |
| `total_orders` | COUNT of matched orders |
| `converting_users` | COUNT DISTINCT customers with at least one order |
| `conversion_rate` | converting / unique x 100 |
| `total_revenue` | SUM(`order_total`) |
| `avg_order_value` | MEAN(`order_total`) |
| `revenue_per_user` | total_revenue / unique_users |

> **Why A/B testing in a data warehouse:** Experiment analysis requires joining assignment data with transactional outcomes. Pre-computing these joins in the warehouse avoids analysts writing complex ad-hoc queries and ensures everyone uses the same conversion definitions.

---

## fct_product_performance — Catalog Analytics

### Purpose

One-row-per-product summary enabling product managers to identify top sellers, underperformers, and review sentiment patterns across the catalog.

### Measures

| Column | Source | Aggregation |
|--------|--------|-------------|
| `units_sold` | `fct_order_items` | SUM(`quantity`) |
| `total_revenue` | `fct_order_items` | SUM(`price`) |
| `total_freight` | `fct_order_items` | SUM(`freight_value`) |
| `avg_price` | `fct_order_items` | MEAN(`price`) |
| `order_count` | `fct_order_items` | COUNT DISTINCT `order_id` |
| `num_reviews` | `stg_reviews` | COUNT per product (via order items link) |
| `avg_review_score` | `stg_reviews` | MEAN(`review_score`) |
| `review_rate` | Derived | num_reviews / units_sold x 100 |

**Enrichment:** Product category and category group from `dim_products` for segmented analysis.

### Asset Checks

- `check_fct_product_performance_row_count` — at least one product row exists
- `check_fct_product_performance_no_neg_revenue` — no negative `total_revenue` values

---

## fct_seller_performance — Marketplace Quality

### Purpose

Seller performance metrics drive marketplace quality programs. This table enables ranking sellers by fulfillment speed, review scores, and late delivery rates.

### Measures

| Column | Source | Aggregation |
|--------|--------|-------------|
| `total_orders_fulfilled` | `fct_order_items` | COUNT DISTINCT `order_id` |
| `total_revenue` | `fct_order_items` | SUM(`price`) |
| `total_items_sold` | `fct_order_items` | SUM(`quantity`) |
| `avg_delivery_time_days` | `stg_orders` | MEAN(`delivery_time_days`) |
| `late_delivery_rate` | `stg_orders` | late / total x 100 |
| `avg_review_score` | `stg_reviews` | MEAN(`review_score`) |
| `num_reviews` | `stg_reviews` | COUNT per seller |

**Enrichment:** Seller city, state, region, and tier from `dim_sellers`.

### Asset Checks

- `check_fct_seller_performance_row_count` — at least one seller row exists
- `check_fct_seller_performance_late_rate` — rate between 0-100%

---

## Dependency Strategy: Partitioned vs. Non-Partitioned

A key architectural challenge: non-partitioned aggregates cannot easily consume partitioned detail facts in Dagster's filesystem I/O manager (loading all 729 partitions requires all to be materialized).

| Downstream | Upstream | Strategy | Production Equivalent |
|------------|----------|----------|----------------------|
| `fct_daily_metrics` (partitioned) | `fct_orders` (partitioned) | Identity partition mapping | Same |
| `fct_product_performance` (non-partitioned) | `fct_order_items` (non-partitioned) | Direct dependency | Same |
| `fct_product_performance` | Review data | Uses `stg_reviews` (non-partitioned) | Would query `fct_reviews` via SQL |
| `fct_seller_performance` | Order data | Uses `stg_orders` (non-partitioned) | Would query `fct_orders` via SQL |

> **Why staging as a workaround:** Dagster's `FilesystemIOManager` cannot load all partitions of a partitioned asset into a non-partitioned downstream. In BigQuery production, the equivalent queries would read directly from partitioned fact tables via SQL (`SELECT * FROM fct_orders`). This is a local-dev constraint, not an architectural limitation.

---

## Partition Mapping Patterns

### Identity Mapping (Daily to Daily)

```
fct_orders[2017-01-15] ──> fct_daily_metrics[2017-01-15]
fct_orders[2017-01-16] ──> fct_daily_metrics[2017-01-16]
```

Both assets share the same `DailyPartitionsDefinition`. Dagster automatically applies `IdentityPartitionMapping`.

### When to Partition Aggregates

| Scenario | Partition? | Reason |
|----------|-----------|--------|
| Daily dashboard metrics | Yes (daily) | Incremental; same grain as source |
| Monthly summaries | Yes (monthly) | Incremental; use `TimeWindowPartitionMapping` |
| Product performance | No | Full catalog snapshot; not time-sliced |
| Seller performance | No | Full roster snapshot |
| Experiment results | No | Spans multiple dates; needs complete data |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Source of truth | Detail facts, not staging | All quality fixes and FK lookups already applied |
| Daily metrics partitioning | Identity mapping from `fct_orders` | Incremental processing; one partition per run |
| Experiment results | Non-partitioned, full refresh | Cross-date analysis requires complete dataset |
| Staging fallback for some deps | `stg_reviews` instead of `fct_reviews` | Filesystem I/O constraint in local dev |
| Inline validation for partitioned | `context.add_output_metadata()` | Handles partial materialization |

---

## Production Considerations

- **Materialized views in BigQuery:** `fct_daily_metrics` is an excellent candidate for a BigQuery materialized view. It would auto-refresh as `fct_orders` partitions update, eliminating the need for a separate Dagster asset.
- **Aggregation lag:** `fct_product_performance` and `fct_seller_performance` are full-refresh. At 100x scale, consider incremental aggregation or BigQuery SQL-based computation to avoid reprocessing the entire catalog.
- **Experiment significance:** `fct_experiment_results` computes descriptive statistics. Production A/B test platforms (Statsig, Eppo) add Bayesian or frequentist significance testing. The pre-computed `conversion_rate` and `revenue_per_user` columns are inputs to such tests.
- **Dashboard coupling:** These aggregated tables are purpose-built for specific dashboards. Adding new dashboard requirements typically means adding new aggregated facts rather than modifying existing ones.
- **Cost:** Non-partitioned full-refresh tables are cheap for small datasets but expensive at scale. Monitor query volume against these tables to decide when to optimize.

---

*Last updated: March 2026*
