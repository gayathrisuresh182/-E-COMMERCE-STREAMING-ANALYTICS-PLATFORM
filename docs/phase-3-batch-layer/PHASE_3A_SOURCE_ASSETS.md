# Phase 3A: Source Asset Design — Ingesting Raw E-Commerce Data

> Dagster source assets form the foundation of the entire pipeline, loading eight Olist CSV datasets from S3 (or local fallback) and establishing full data lineage from raw ingestion through to analytics-ready fact tables.

---

## Overview

Every data warehouse begins with reliable ingestion. Source assets solve the problem of bringing external data into Dagster's asset graph with full observability: row counts, schema metadata, and provenance tracking are captured on every materialization.

The design prioritizes **environment portability** — the same asset code runs against S3 in production and local files in development, with zero code changes. This pattern is common in production data platforms where engineers need to iterate locally without AWS credentials.

---

## Source Asset Architecture

### What a Source Asset Does

A source asset is a Dagster `@asset` that loads external data without transforming it. It establishes the entry point for downstream lineage and captures metadata for observability.

| Aspect | Behavior |
|--------|----------|
| **Definition** | `@asset` that loads from S3 or local filesystem |
| **Computation** | Read CSV into pandas DataFrame; no transformations applied |
| **Lineage** | All downstream assets (`stg_*`, `dim_*`, `fct_*`) trace back to these |
| **Persistence** | `mem_io_manager` keeps DataFrames in memory for the run |
| **Observability** | Row count, column count, source URI, file size reported to Dagit |

> **Why no transformations here:** Source assets should be a faithful representation of the external data. Any cleaning, type casting, or enrichment happens in the staging layer (Phase 3B). This separation makes it trivial to diagnose whether a data quality issue originated in the source or was introduced during transformation.

---

## The Eight Olist Datasets

The Olist Brazilian E-Commerce dataset from Kaggle contains eight interrelated CSVs covering the full order lifecycle: customers, sellers, products, orders, payments, reviews, and geolocation.

| Asset | Records | S3 Path | Local Fallback |
|-------|---------|---------|----------------|
| `raw_orders` | ~99,441 | `s3://{bucket}/bronze/olist_historical/orders/` | `raw/olist/olist_orders_dataset.csv` |
| `raw_customers` | ~99,441 | `.../customers/` | `raw/olist/olist_customers_dataset.csv` |
| `raw_products` | ~32,951 | `.../products/` | `raw/olist/olist_products_dataset.csv` |
| `raw_order_items` | ~112,650 | `.../order_items/` | `raw/olist/olist_order_items_dataset.csv` |
| `raw_order_payments` | ~103,886 | `.../order_payments/` | `raw/olist/olist_order_payments_dataset.csv` |
| `raw_order_reviews` | ~99,224 | `.../order_reviews/` | `raw/olist/olist_order_reviews_dataset.csv` |
| `raw_sellers` | ~3,095 | `.../sellers/` | `raw/olist/olist_sellers_dataset.csv` |
| `raw_geolocation` | ~1,000,163 | `.../geolocation/` | `raw/olist/olist_geolocation_dataset.csv` |

---

## I/O Strategy: S3 with Local Fallback

### The Decision

| Option | Approach | Trade-off |
|--------|----------|-----------|
| A | S3-only with Dagster's built-in S3 I/O manager | Requires AWS credentials everywhere, including CI |
| **B (Chosen)** | **Custom load with `boto3` + local fallback** | **Portable; works with or without AWS** |
| C | Dagster `SourceAsset` (external, no computation) | No metadata capture; opaque to lineage graph |

**Why Option B:** A single environment variable (`S3_BUCKET`) toggles between cloud and local. This is the same pattern used at companies like dbt Labs and Dagster's own examples — infrastructure configuration should not leak into asset logic.

### How It Works

- **When `S3_BUCKET` is set:** Uses `boto3` to read CSV directly from S3. Captures `LastModified` timestamp for staleness tracking.
- **When `S3_BUCKET` is unset:** Falls back to the `raw/olist/` directory. No AWS credentials required.
- **I/O Manager:** `mem_io_manager` keeps DataFrames in memory. Source data is not persisted by Dagster — it exists in S3/local and flows through the pipeline.

> **Why `mem_io_manager` for sources:** Persisting raw CSVs as pickled DataFrames in `.dagster/storage/` would double storage with no benefit. The raw data already lives in S3 or on disk. Downstream staging assets use `FilesystemIOManager` (dev) or BigQuery I/O manager (production) for actual persistence.

---

## Asset Metadata and Observability

Each source asset reports structured metadata on every materialization, visible in the Dagit UI:

| Metadata Key | Example Value | Purpose |
|--------------|---------------|---------|
| `rows` | `99441` | Volume validation; detect truncated loads |
| `columns` | `8` | Schema drift detection |
| `column_names` | `["order_id", "customer_id", ...]` | Quick schema inspection |
| `source` | `s3://bucket/bronze/.../orders.csv` | Provenance; which environment served data |
| `file_size_bytes` | `12,345,678` | Data volume monitoring |
| `last_modified` | `2026-02-15T10:30:00Z` | Staleness detection (S3 only) |

> **Why this matters:** In production, a monitoring alert on `rows` dropping below a threshold catches upstream data delivery failures before they propagate to dashboards.

---

## Partitioning Strategy

| Layer | Partitioned? | Rationale |
|-------|-------------|-----------|
| **Source (`raw_*`)** | No | Olist is a single historical snapshot; no incremental delivery |
| **Staging (`stg_*`)** | No | Full refresh; Olist data is static |
| **Facts (`fct_*`)** | Yes (daily) | Enables incremental processing and BigQuery partition pruning |

**Why sources are unpartitioned:** The Olist dataset is a point-in-time export (2016-2018). There is no ongoing data delivery to partition by. For a production e-commerce platform with daily data drops, source assets would use `DailyPartitionsDefinition` keyed on delivery date.

For streaming data ingested via Kafka (Phase 3F+), partitioning by event date is applied at the fact layer, not at source ingestion.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Load strategy | Custom `boto3` + pandas | S3/local portability without Dagster S3 dependency |
| I/O manager | `mem_io_manager` | Avoid redundant persistence of raw data |
| Metadata capture | Row/column counts, source URI | Observability without external monitoring tools |
| Partitioning | None for sources | Historical snapshot; no incremental pattern needed |
| Transformation | None (deferred to staging) | Clean separation of ingestion and cleaning responsibilities |

---

## Production Considerations

- **Scaling to 100x data volume:** Replace pandas `read_csv` with chunked reading or Spark. The asset contract (DataFrame out, metadata reported) stays the same.
- **S3 access failures:** The current implementation raises on S3 errors. In production, add retry logic with exponential backoff and dead-letter alerting.
- **Schema drift:** If Olist adds columns, the source asset loads them silently. The staging layer should validate expected columns and reject or log unexpected ones.
- **Cost:** S3 GET requests cost $0.0004 per 1,000. Eight CSVs at ~50 MB total is negligible. At scale, consider S3 Select or Parquet format to reduce transfer volume.
- **Concurrency:** All eight source assets are independent and can materialize in parallel, bounded only by Dagster's executor concurrency limits.

---

*Last updated: March 2026*
