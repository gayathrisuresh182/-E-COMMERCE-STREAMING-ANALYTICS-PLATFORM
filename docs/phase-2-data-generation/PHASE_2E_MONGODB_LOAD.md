# MongoDB Data Load — Document Schemas, Indexes, and Bulk Loading Strategy

> Loads five collections into MongoDB with denormalized document schemas, targeted indexes, and idempotent bulk operations — building the operational data store that powers real-time dashboard queries and API lookups.

---

## Overview

The MongoDB operational store serves a different purpose than BigQuery (the analytical warehouse) or S3 (the data lake). It handles **low-latency, point lookups** that the dashboard and API need in real time: "What products are in category X?", "What variant is customer Y assigned to?", "Show me reviews for order Z." These queries need sub-10ms responses, which requires purpose-built document schemas and carefully planned indexes.

This phase loads five collections from three data sources: Olist CSVs (products, reviews, dimension tables), generated experiment data (assignments, experiment metadata), and establishes the schema patterns that Kafka consumers will follow when writing streaming data (orders, events).

---

## Collection Architecture

### Data Sources and Flow

```
Olist CSVs (raw/olist/)
    ├── products CSV ──────────► products collection
    ├── reviews CSV + items CSV ► reviews collection (enriched with product_id)
    ├── customers CSV ─────────► dim_customers collection
    └── sellers CSV ───────────► dim_sellers collection

Generated Data (Phase 2A/2B)
    ├── experiment_catalog.json ► experiments collection
    └── assignments CSV ────────► experiment_assignments collection
```

### Collection Summary

| Collection | Documents | Avg Doc Size | Source | Primary Access Pattern |
|------------|----------|-------------|--------|----------------------|
| **products** | ~33,000 | ~300 bytes | Olist CSV | Lookup by product_id; filter by category |
| **reviews** | ~100,000 | ~400 bytes | Olist CSV (enriched) | Lookup by order_id; text search on comments |
| **experiments** | 10 | ~500 bytes | Configuration JSON | Lookup by experiment_id; list active |
| **experiment_assignments** | ~994,000 | ~200 bytes | Generated CSV | Compound lookup (customer_id, experiment_id) |
| **dim_customers** | ~99,000 | ~250 bytes | Olist CSV + geolocation | Lookup by customer_id; filter by state |
| **dim_sellers** | ~3,000 | ~200 bytes | Olist CSV | Lookup by seller_id |

---

## Document Schema Design

### Products

```json
{
  "_id": "ObjectId",
  "product_id": "4244733e06e7ecb4970a6e2683c13e61",
  "category": "perfumery",
  "attributes": {
    "weight_g": 650,
    "dimensions": {
      "length_cm": 20,
      "height_cm": 14,
      "width_cm": 10
    },
    "photos_qty": 6,
    "name_length": 54,
    "description_length": 1280
  },
  "metadata": {
    "created_at": "2024-02-20T10:00:00Z",
    "last_updated": "2024-02-20T10:00:00Z"
  }
}
```

> **Design rationale:** Physical attributes are nested under `attributes` to separate business identity (product_id, category) from descriptive properties. This pattern makes it straightforward to add new attribute types (color, material, brand) without restructuring the document. The `metadata` block provides audit timestamps for every document.

### Reviews (Enriched)

```json
{
  "_id": "ObjectId",
  "review_id": "a1b2c3d4e5f6",
  "order_id": "e481f51cbdc5dee999eceb98",
  "product_id": "lookup_from_order_items",
  "rating": 5,
  "comment": {
    "title": "Excelente",
    "message": "Produto muito bom, chegou antes do prazo...",
    "language": "pt"
  },
  "timestamps": {
    "created_at": "2018-01-15T00:00:00Z",
    "answered_at": "2018-01-16T00:00:00Z"
  },
  "metadata": {
    "helpful_votes": 0,
    "verified_purchase": true
  }
}
```

> **Enrichment detail:** The raw Olist reviews dataset does not include `product_id` directly. During load, `product_id` is resolved by joining `order_reviews` with `order_items` via `order_id`. This denormalization enables "reviews for product X" queries without a runtime join.

### Experiment Assignments

```json
{
  "_id": "ObjectId",
  "customer_id": "9ef432eb6251297304e0ea311e75e2f1",
  "experiment_id": "exp_001",
  "variant": "treatment",
  "assigned_at": "2024-02-01T00:00:00Z",
  "assignment_hash": "a3f2b7c9e1d4..."
}
```

### Dimension Tables

**dim_customers:**
```json
{
  "_id": "ObjectId",
  "customer_id": "abc123",
  "state": "SP",
  "city": "sao paulo",
  "geolocation": {
    "lat": -23.5505,
    "lng": -46.6333
  }
}
```

**dim_sellers:**
```json
{
  "_id": "ObjectId",
  "seller_id": "def456",
  "state": "RJ",
  "city": "rio de janeiro"
}
```

---

## Index Strategy

Every index must justify its existence with a specific, frequent query pattern. Unnecessary indexes slow writes and consume memory.

### Products Indexes

| Index | Type | Query Pattern | Expected Latency |
|-------|------|--------------|-----------------|
| `product_id` | Unique | Product detail page lookup | <5ms |
| `category` | Single | Category browsing, filter by category | <10ms |
| `attributes.weight_g` | Single | Shipping cost estimation queries | <10ms |

### Reviews Indexes

| Index | Type | Query Pattern | Expected Latency |
|-------|------|--------------|-----------------|
| `review_id` | Unique | Deduplication during upsert | <5ms |
| `{order_id, product_id}` | Compound | "Show reviews for this order's products" | <10ms |
| `rating` | Single | Rating distribution aggregation | <50ms |
| `timestamps.created_at` | Single | Time-range queries for review trends | <50ms |

### Experiment Assignments Indexes

| Index | Type | Query Pattern | Expected Latency |
|-------|------|--------------|-----------------|
| `{customer_id, experiment_id}` | Compound unique | "What variant is this customer in?" (most critical query) | <5ms |
| `experiment_id` | Single | "All assignments for experiment X" (aggregation) | <100ms |

### Dimension Table Indexes

| Index | Type | Query Pattern |
|-------|------|--------------|
| `customer_id` | Unique | Customer detail lookups |
| `seller_id` | Unique | Seller detail lookups |
| `state` | Single | Geographic filtering |

---

## Bulk Loading Strategy

### Why Bulk Upserts

The loading strategy uses `bulk_write` with `ReplaceOne` operations and `upsert=True`:

```python
operations = [
    ReplaceOne(
        {"product_id": doc["product_id"]},
        doc,
        upsert=True
    )
    for doc in batch
]
collection.bulk_write(operations, ordered=False)
```

| Property | Benefit |
|----------|---------|
| **Idempotent** | Running the script twice produces the same result — no duplicates |
| **Resumable** | If the script crashes mid-load, re-running picks up where it left off |
| **Atomic per batch** | Each batch of 1,000 documents is a single network round-trip |
| **Unordered** | `ordered=False` allows MongoDB to execute operations in parallel within a batch |

### Batch Size: 1,000 Documents

- **Why not larger?** MongoDB has a 48MB limit on `bulk_write` payloads. At ~300 bytes/doc average, 1,000 documents is ~300 KB — well within limits and balances network efficiency with memory usage.
- **Why not smaller?** Each `bulk_write` call has network overhead (~5ms round-trip to Atlas). 100-document batches would turn a 33K product load into 330 network calls instead of 33.

### Load Order

Collections are loaded in dependency order:

1. **products** (no dependencies)
2. **experiments** (no dependencies)
3. **reviews** (needs order_items for product_id enrichment)
4. **experiment_assignments** (needs experiments collection for validation)
5. **dim_customers, dim_sellers** (no dependencies, can load in parallel)

---

## Performance Targets

| Query | Target Latency | Index Used |
|-------|---------------|------------|
| Find product by `product_id` | <10ms | `product_id` unique index |
| Aggregate reviews by rating | <100ms | `rating` single index |
| Lookup assignment by `(customer_id, experiment_id)` | <5ms | Compound unique index |
| Count documents per collection | <50ms | Collection metadata (no index scan) |
| Text search on review comments | <200ms | Text index on `comment.message` |

> **Benchmark context:** These targets assume MongoDB running locally (Docker) or on Atlas M0 free tier. Production M10+ clusters with dedicated resources would achieve 2-5x better latencies.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Load method | Bulk upsert (`ReplaceOne` + `upsert=True`) | Idempotent re-runs; safe for both initial load and incremental updates |
| Batch size | 1,000 documents | Balances network efficiency with memory usage; well within 48MB wire protocol limit |
| Schema validation | Application-level (Python) | MongoDB schema validation is optional. Python validators provide clearer error messages and can transform data during validation. |
| Denormalization | Reviews enriched with product_id at load time | Avoids runtime joins; reviews are queried by product more often than by order |
| `--drop` flag | Available for clean reloads | Useful during development; drops and recreates collections with fresh indexes |
| Timestamp format | ISO 8601 strings (not BSON Date) | Consistent with Kafka event format and BigQuery imports; avoids timezone conversion issues |

---

## Production Considerations

- **Change data capture (CDC):** In production, MongoDB changes would be captured via Change Streams and forwarded to Kafka or BigQuery for downstream sync. This project uses batch loads, but the document schemas are designed to support CDC (every document has `last_updated`).
- **Scaling writes:** At the current load volumes (33K products, 994K assignments), a single-threaded Python loader completes in under 60 seconds. At 10x volume, consider multi-threaded batch writers or MongoDB's `mongoimport` for raw CSV ingestion.
- **Collection growth:** The `experiment_assignments` collection (994K docs, ~200 MB) is the largest. With 100 experiments and 1M customers, it would reach 100M documents (~20 GB). At that scale, shard by `experiment_id` to distribute load.
- **Index memory:** All indexes should fit in RAM for optimal performance. The current index set for all collections requires approximately 50-100 MB. Atlas M0 (512 MB) accommodates this comfortably. Monitor with `db.collection.stats()`.
- **Data freshness:** The `--drop` flag destroys existing data. In production, use rolling updates with upserts and maintain a separate "last successful load" checkpoint for recovery.

---

*Last updated: March 2026*
