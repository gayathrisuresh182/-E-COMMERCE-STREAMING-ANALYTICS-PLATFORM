# Phase 3D: Fact Tables — Transactional Grain Modeling

> Three detail-grain fact tables capture every order, line item, and review in the Olist dataset, connected to dimension tables via surrogate keys and partitioned daily for incremental processing and BigQuery partition pruning.

---

## Overview

Fact tables are the heart of a star schema. They store measurable events at the lowest useful grain and reference dimensions via foreign keys. This project implements three fact tables that together cover the full e-commerce transaction lifecycle: order placement, item-level detail, and post-purchase reviews.

The design follows Kimball's fact table taxonomy: **transaction facts** (one row per event) with additive measures that can be summed, averaged, and counted across any dimension.

---

## Fact Table Inventory

| Fact Table | Grain | Row Count | Partitioned | Partition Key |
|------------|-------|-----------|-------------|---------------|
| **fct_orders** | One row per order | ~99,441 | Yes (daily) | `order_date` |
| **fct_order_items** | One row per item per order | ~112,650 | No | N/A |
| **fct_reviews** | One row per review | ~99,224 | Yes (daily) | `review_creation_date` |

---

## fct_orders — The Primary Transaction Fact

### Grain and Keys

Each row represents a single customer order, connected to four dimension tables:

| Foreign Key | Joined From | Lookup Logic |
|-------------|------------|--------------|
| `customer_key` | `dim_customers` | Join on `customer_id` |
| `seller_key` | `dim_sellers` | First `seller_id` per order from `stg_order_items` |
| `geography_key` | `dim_geography` | Customer's ZIP prefix |
| `date_key` | `dim_dates` | `order_date` to `dim_dates.date` |

### Measures

| Measure | Type | Source |
|---------|------|--------|
| `order_total` | Additive | SUM(item price + freight) from order items |
| `item_count` | Additive | COUNT of items in order |
| `payment_value` | Additive | SUM of payment values |
| `freight_value` | Additive | SUM of freight from order items |
| `profit_estimate` | Derived | `order_total * 0.15` (assumed 15% margin) |
| `avg_item_price` | Non-additive | `order_total / item_count` |
| `delivery_time_days` | Semi-additive | Days between purchase and delivery |
| `is_late_delivery` | Boolean | Delivered after estimated date |

> **Why `profit_estimate` uses a fixed margin:** Real profit calculations require cost-of-goods data that Olist does not provide. The 15% margin is a reasonable e-commerce benchmark that enables profitability analysis without introducing fictional cost data.

### Degenerate Dimensions

These are dimension-like attributes stored directly in the fact table because they lack enough attributes to justify a separate dimension:

- `order_status` — delivered, canceled, shipped, etc.
- `payment_type` — credit_card, boleto, debit_card (first payment per order)
- `payment_installments` — number of installments

### Partitioning

- **Definition:** `DailyPartitionsDefinition(start_date="2016-09-01", end_date="2018-08-31")`
- **729 partitions** covering the full Olist analysis window
- **Incremental pattern:** Each partition processes one day's orders independently

> **Why daily partitioning matters:** In BigQuery, a query filtering on `WHERE order_date = '2017-06-15'` scans only that day's partition (~136 orders) instead of the full 99K-row table. At scale, this is the difference between scanning gigabytes and scanning megabytes.

---

## fct_order_items — Line-Item Detail

### Grain and Keys

Each row represents one item within an order. This finer grain enables product-level and seller-level analysis that `fct_orders` (order grain) cannot support.

| Foreign Key | Joined From |
|-------------|------------|
| `product_key` | `dim_products` on `product_id` |
| `seller_key` | `dim_sellers` on `seller_id` |
| `fact_key` | Derived from `order_id` (links to `fct_orders`) |

### Measures

| Measure | Description |
|---------|-------------|
| `quantity` | Always 1 (Olist uses one row per item) |
| `price` | Item unit price |
| `freight_value` | Shipping cost allocated to this item |
| `item_total` | `price + freight_value` |

### Why Not Partitioned

The order items table is small (~112K rows, <15 MB) and does not have a natural date column. Partitioning would add overhead without benefit. If this table grew to millions of rows, partitioning by order date (via a join) would be warranted.

---

## fct_reviews — Post-Purchase Feedback

### Grain and Keys

Each row represents one customer review, linked back to orders via `fact_key`.

### Measures

| Measure | Description |
|---------|-------------|
| `review_score` | 1 to 5 rating |
| `review_comment_length` | Character count of comment text |
| `response_time_hours` | Hours between review submission and platform response |
| `sentiment` | Derived: `positive` (4-5), `neutral` (3), `negative` (1-2) |
| `has_comment` | Boolean: whether customer left text feedback |

### Partitioning

Partitioned by `review_creation_date` using the same `DailyPartitionsDefinition` as `fct_orders`. This enables time-based analysis of review trends and sentiment shifts.

---

## Surrogate Key Design

| Fact Table | Surrogate Key | Source | Purpose |
|------------|--------------|--------|---------|
| `fct_orders` | `fact_key` | Hash of `order_id` | Deterministic; links to `fct_order_items` and `fct_reviews` |
| `fct_order_items` | `order_item_fact_key` | Hash of `order_id` + `order_item_id` | Unique per line item |
| `fct_reviews` | `review_fact_key` | Hash of `review_id` | Unique per review |

All fact tables retain their natural keys (`order_id`, `review_id`) alongside surrogate keys for debugging and data reconciliation.

---

## Validation Strategy

### The Partitioned Asset Check Problem

Dagster's `@asset_check` decorator loads the full asset before validating. For partitioned assets with 729 daily partitions, this means loading all partitions — which fails if any partition is unmaterialized.

| Asset | Validation Method | Rationale |
|-------|-------------------|-----------|
| `fct_orders` (partitioned) | Inline via `context.add_output_metadata()` | Validates each partition independently |
| `fct_order_items` (unpartitioned) | `@asset_check` decorators | Single materialization; standard checks work |
| `fct_reviews` (partitioned) | Inline via `context.add_output_metadata()` | Same constraint as `fct_orders` |

### Inline Validation Metrics

For partitioned facts, each partition reports:

| Metric | Validates |
|--------|-----------|
| `row_count` | Partition is not empty |
| `null_customer_key` | Dimension join integrity |
| `null_date_key` | Date dimension join integrity |
| `negative_order_total_delivered` | No delivered orders with zero/negative revenue |

> **Why inline over asset checks:** This is a practical trade-off. In production with BigQuery, `@asset_check` could query the partitioned table directly via SQL. With Dagster's `FilesystemIOManager`, inline validation is the reliable approach.

---

## Incremental Processing Pattern

```
Daily Batch Job (02:00 BRT)
    |
    v
Materialize yesterday's partition only
    |
    ├── fct_orders[2017-06-15]
    ├── fct_reviews[2017-06-15]
    └── fct_daily_metrics[2017-06-15]
    
DO NOT rematerialize all 729 historical partitions every run.
Dagster tracks which partitions are materialized.
```

For initial backfill, partitions are materialized in monthly batches to avoid memory pressure from processing two years of data in a single run.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Fact grain | Transaction-level (one row per event) | Maximum analytical flexibility; aggregations built on top |
| Partitioning | Daily by event date | Incremental processing; BigQuery partition pruning |
| Surrogate keys | Hash-based, deterministic | Portable across environments; no database dependency |
| Validation | Inline for partitioned, `@asset_check` for unpartitioned | Works with partial materialization |
| Degenerate dimensions | Stored in fact table | `order_status`, `payment_type` lack enough attributes for separate dims |
| Natural key retention | Kept alongside surrogate keys | Debugging and data reconciliation |

---

## Production Considerations

- **Backfill strategy:** Two years of daily partitions (729 days) should be backfilled in batches (monthly or weekly) to control memory usage and provide checkpoints for failure recovery.
- **Late-arriving facts:** Orders placed at 23:59 may not appear in today's partition until tomorrow's run. A one-day lookback window handles this edge case.
- **Null foreign keys:** Monitor `null_customer_key` and `null_date_key` rates. A spike indicates a dimension table is missing records (late-arriving dimension problem).
- **Fact table growth:** At 100x scale (10M orders/year), daily partitions average ~27K rows each. BigQuery handles this effortlessly, but the pandas-based local pipeline would need chunked processing or a Spark backend.
- **Cost in BigQuery:** Fact tables are the largest tables in the warehouse. Partition pruning and clustering (by `order_status`, `payment_type`) are essential to control query costs at scale.

---

*Last updated: March 2026*
