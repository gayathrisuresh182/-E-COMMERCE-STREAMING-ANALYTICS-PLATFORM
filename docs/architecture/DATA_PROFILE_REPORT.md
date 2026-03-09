# Data Profile Report — Olist Brazilian E-Commerce Dataset

> Source data quality assessment covering 8 tables, ~1.5M rows, and 120 MB of Brazilian e-commerce transaction data used as the foundation for the analytics platform.

---

## Overview

Every analytics platform is only as reliable as its source data. This report profiles the Olist Brazilian E-Commerce dataset (Kaggle) — the raw material that feeds the batch pipeline, populates the dimensional model in BigQuery, and simulates streaming events through Kafka. Understanding the dataset's shape, quality issues, and join relationships before building pipelines prevents the most common failure mode in data engineering: **garbage in, garbage out**.

The Olist dataset contains real (anonymized) transaction data from a Brazilian e-commerce marketplace, covering orders placed between 2016 and 2018. It includes the full order lifecycle: customer purchases, payments, seller fulfillment, shipping, delivery, and post-delivery reviews. This breadth makes it unusually well-suited for building a realistic analytics platform — most public datasets cover only one slice of the e-commerce funnel.

---

## Dataset Summary

| Table | Rows | File Size | Primary Key | Grain | Role in Pipeline |
|-------|------|-----------|-------------|-------|------------------|
| `olist_orders_dataset` | 99,441 | 16.84 MB | `order_id` | One row per order | Central fact table; drives `fct_orders` |
| `olist_order_items_dataset` | 112,650 | 14.72 MB | Composite | One row per order line item | Drives `fct_order_items`; links orders to products and sellers |
| `olist_order_payments_dataset` | 103,886 | 5.51 MB | Composite | One row per payment installment | Payment method analysis; multiple payments per order |
| `olist_order_reviews_dataset` | 99,224 | 13.78 MB | `review_id` | One row per review | Customer satisfaction metrics; NLP-ready text fields |
| `olist_products_dataset` | 32,951 | 2.27 MB | `product_id` | One row per product | Product dimension; category analysis |
| `olist_customers_dataset` | 99,441 | 8.62 MB | `customer_id` | One row per customer | Customer dimension; geographic analysis |
| `olist_sellers_dataset` | 3,095 | 170.6 KB | `seller_id` | One row per seller | Seller dimension; marketplace analysis |
| `olist_geolocation_dataset` | 1,000,163 | 58.44 MB | None | One row per zip code + lat/long | Geographic enrichment; distance calculations |

**Total:** ~1.55M rows across 8 tables, ~120 MB uncompressed.

---

## Data Quality Assessment

### Null Analysis

Nulls in source data propagate through the pipeline unless explicitly handled. The staging layer must decide for each null: impute a default, filter the row, or preserve the null with documentation.

| Table | Column | Null Rate | Impact | Handling Strategy |
|-------|--------|-----------|--------|-------------------|
| `orders` | `order_approved_at` | 0.2% | Orders that were never approved (cancelled before approval) | Preserve null; filter in `fct_orders` WHERE `order_status != 'cancelled'` |
| `orders` | `order_delivered_carrier_date` | 1.8% | Orders not yet handed to carrier (in-process or cancelled) | Preserve null; delivery time calculations must handle NULL gracefully |
| `orders` | `order_delivered_customer_date` | 3.0% | Orders not yet delivered to customer | Preserve null; exclude from delivery time metrics |
| `reviews` | `review_comment_title` | 88.3% | Most reviewers skip the title field | Drop column from analytics; not useful at 88% null |
| `reviews` | `review_comment_message` | 58.7% | Many reviews are rating-only (no text) | Preserve for NLP analysis; filter to non-null for text mining |
| `products` | `product_category_name` | 1.9% | Uncategorized products | Impute as `'unknown'` in staging to avoid NULL group-by issues |
| `products` | Physical dimensions (weight, length, height, width) | <0.1% | Missing product specs | Impute median values per category for freight cost estimation |

> **Why 3% null on delivery dates matters:** Delivery time is a core e-commerce KPI. If the pipeline calculates average delivery time by simply subtracting dates, the 3% of orders with NULL `order_delivered_customer_date` will either throw errors (if not handled) or silently skew the average (if coalesced to a default). The staging layer explicitly filters these rows from delivery time calculations while keeping them in order count metrics.

### Duplicate Analysis

| Table | Primary Key | Duplicates | Severity | Impact |
|-------|-------------|------------|----------|--------|
| `orders` | `order_id` | 0 | None | Clean — safe to use as-is |
| `customers` | `customer_id` | 0 | None | Clean — safe to use as-is |
| `products` | `product_id` | 0 | None | Clean — safe to use as-is |
| `sellers` | `seller_id` | 0 | None | Clean — safe to use as-is |
| `reviews` | `review_id` | 814 | Medium | 0.8% of reviews are duplicated; must deduplicate in staging before aggregation |
| `order_items` | No single PK | N/A | N/A | Composite key (`order_id` + `order_item_id`); no duplicates on composite |
| `payments` | No single PK | N/A | N/A | Composite key (`order_id` + `payment_sequential`); no duplicates on composite |
| `geolocation` | No single PK | N/A | N/A | Multiple lat/long entries per zip prefix (intentional) |

> **The review duplicates problem:** 814 duplicate `review_id` values mean the same review appears multiple times. If not deduplicated, these inflate review counts and skew average ratings. The staging layer deduplicates by keeping the most recent version (latest `review_creation_date`) of each `review_id`. This is a common pattern in event-sourced systems where the source may emit the same event multiple times.

---

## Entity Relationships

Understanding how tables join is essential for building the dimensional model. The Olist dataset follows a classic e-commerce schema centered on the `orders` table.

```
                                ┌──────────────┐
                                │  customers   │
                                │ (customer_id)│
                                └──────┬───────┘
                                       │ 1
                                       │
                                       │ *
                              ┌────────┴────────┐
                              │     orders      │
                              │   (order_id)    │
                              └──┬────┬────┬────┘
                                 │    │    │
                          1:*    │    │    │   1:*
                    ┌────────────┘    │    └────────────┐
                    │                 │                 │
              ┌─────┴──────┐  ┌──────┴──────┐  ┌──────┴───────┐
              │ order_items │  │  payments   │  │   reviews    │
              │             │  │             │  │ (review_id)  │
              └──┬─────┬────┘  └─────────────┘  └──────────────┘
                 │     │
           *:1   │     │  *:1
          ┌──────┘     └──────┐
          │                   │
   ┌──────┴──────┐    ┌──────┴──────┐
   │  products   │    │   sellers   │
   │(product_id) │    │ (seller_id) │
   └─────────────┘    └─────────────┘
```

### Join Paths

| From | To | Join Key | Cardinality | Notes |
|------|----|----------|-------------|-------|
| `orders` | `customers` | `customer_id` | Many-to-one | Every order has exactly one customer |
| `orders` | `order_items` | `order_id` | One-to-many | An order contains 1+ line items |
| `orders` | `payments` | `order_id` | One-to-many | An order can have multiple payment methods/installments |
| `orders` | `reviews` | `order_id` | One-to-one (mostly) | Most orders have 0 or 1 review; a few have multiple |
| `order_items` | `products` | `product_id` | Many-to-one | Each line item references one product |
| `order_items` | `sellers` | `seller_id` | Many-to-one | Each line item is fulfilled by one seller |
| `customers`/`sellers` | `geolocation` | `zip_code_prefix` | Many-to-many | Multiple lat/long entries per zip prefix |

### Referential Integrity

Join validation confirms **zero orphan foreign keys** across all relationships. Every `order_id` in `order_items` exists in `orders`; every `product_id` in `order_items` exists in `products`; every `customer_id` in `orders` exists in `customers`. This is unusually clean for a real-world dataset and simplifies the staging layer — no need for LEFT JOINs to handle missing references.

> **Why this matters for the pipeline:** Clean referential integrity means the dimensional model can use INNER JOINs throughout. If orphan FKs existed, the pipeline would need to either reject rows (losing data) or use LEFT JOINs with COALESCE (adding complexity and potentially masking real issues). The Olist dataset's cleanliness here is a significant advantage.

---

## Dataset Characteristics for Pipeline Design

### Volume Implications

| Metric | Value | Pipeline Impact |
|--------|-------|----------------|
| Total rows across all tables | ~1.55M | Fits comfortably in memory for batch processing; no need for chunked reads |
| Largest table (geolocation) | 1M rows, 58 MB | The only table that might benefit from chunked processing in memory-constrained environments |
| Order table size | 99K rows, 17 MB | Small enough for full-table BigQuery scans within free tier |
| Date range | Sept 2016 – Oct 2018 | ~2 years of data; daily partitioning creates ~730 partitions in BigQuery |

### Temporal Distribution

The dataset is not uniformly distributed over time. Order volume ramps up significantly from 2016 to 2018, reflecting the marketplace's growth.

- **2016:** ~300 orders/month (sparse; early marketplace)
- **2017:** ~4,000 orders/month (growth phase)
- **2018:** ~7,000 orders/month (mature marketplace)

This non-uniform distribution affects:
- **BigQuery partitioning:** Early partitions (2016) contain very few rows; later partitions (2018) contain 10–20x more. Partition pruning is still effective, but storage efficiency varies.
- **Streaming simulation:** Event producers should model this growth curve rather than generating uniform traffic, to create realistic streaming patterns.
- **A/B testing:** Experiments should run against the high-volume 2018 data to ensure statistical significance; 2016 data is too sparse for meaningful test results.

### Data Types and Encoding

- **Timestamps:** ISO 8601 format (`2018-06-01 12:00:00`), UTC-3 (Brazil). The pipeline converts to UTC in the staging layer.
- **Currency:** Brazilian Real (BRL). All monetary values are in BRL with 2 decimal places.
- **Text:** Portuguese language (product categories, review text, city names). Category name translation table is available separately.
- **Geographic:** Latitude/longitude as floats; zip code prefix as 5-digit string (not integer — leading zeros matter).

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Null handling | Per-column strategy (preserve, impute, or filter) | Blanket null removal would lose valid data (e.g., undelivered orders are real, not errors) |
| Review deduplication | Keep latest by `review_creation_date` | Preserves the most recent version of edited reviews; matches event-sourcing semantics |
| Geolocation join | By zip code prefix, not exact zip | Olist anonymizes exact addresses; prefix-level is the finest granularity available |
| Timezone conversion | Convert BRT (UTC-3) to UTC in staging | All downstream systems (Kafka, BigQuery, dashboards) operate in UTC to avoid timezone bugs |
| Category nulls | Impute as `'unknown'` | Prevents NULL group-by issues in dimensional model; clearly signals missing data vs. empty string |
| Primary key validation | Assert zero duplicates in staging | Catches data corruption early; duplicate PKs would cause fan-out in joins and inflated metrics |

---

## Production Considerations

### If This Were Real Production Data

- **PII handling:** Customer IDs, geographic data, and review text would require anonymization or encryption at rest. The Olist dataset is pre-anonymized, but a production pipeline would need a PII classification and masking layer in the staging step.
- **Data freshness SLA:** The batch pipeline assumes data arrives as daily CSV dumps. In production, real-time ingestion via Kafka would replace CSV uploads, and the pipeline would need freshness monitoring (alert if no new orders in the last hour).
- **Schema evolution:** If Olist adds new columns (e.g., `delivery_rating`), the pipeline must handle the schema change gracefully — new columns should pass through staging without breaking existing transformations.
- **Data volume at scale:** At 100x volume (~10M orders), the geolocation table would grow to ~100M rows. The current approach of loading the entire table into memory for joins would need to be replaced with database-side joins or partitioned processing.

### Data Quality Monitoring

In a production system, the following checks would run automatically after each batch load:

| Check | Threshold | Action on Failure |
|-------|-----------|-------------------|
| Primary key uniqueness | 0 duplicates | Block pipeline; investigate source |
| Null rate per column | Within documented tolerance (see above) | Alert if null rate exceeds 2x historical baseline |
| Row count variance | Within 20% of previous load | Alert on significant volume changes (could indicate missing data or duplicates) |
| Referential integrity | 0 orphan FKs | Block pipeline; log orphan records for investigation |
| Freshness | Data timestamp within 24 hours of current time | Alert; indicates stale source data |

---

*Last updated: March 2026*
