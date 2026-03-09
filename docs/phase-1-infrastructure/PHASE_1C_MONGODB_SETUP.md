# MongoDB — Real-Time Operational Data Store

> MongoDB serves as the low-latency operational store for product catalogs, user events, reviews, and experiment assignments, enabling sub-10ms lookups that power the real-time dashboard and API layer.

---

## Overview

In a Lambda architecture, the speed layer needs a database optimized for fast reads on semi-structured data with flexible schemas. MongoDB fills this role by storing operational data that the React dashboard and FastAPI backend query in real time — product details, live experiment assignments, clickstream events, and customer reviews.

Relational databases could serve this purpose, but MongoDB's document model is a natural fit for e-commerce data where products have variable attributes, events carry arbitrary properties, and experiment configurations evolve without migrations. The schema-on-read approach also means Kafka consumers can write documents directly without ETL overhead.

---

## Why MongoDB for This Architecture

| Requirement | Why MongoDB Fits | Alternative Considered |
|-------------|-----------------|----------------------|
| Variable product attributes | Document model handles arbitrary nested fields without ALTER TABLE | PostgreSQL JSONB (viable, but less idiomatic for document-heavy workloads) |
| Real-time event ingestion | Write-optimized with configurable write concern | Redis (faster but no persistence/querying depth) |
| Flexible experiment schemas | Each experiment can have different config shapes | PostgreSQL (requires schema migrations per experiment) |
| Text search on reviews | Native text indexes with language-aware stemming | Elasticsearch (overkill for this scale; additional infrastructure) |
| Dashboard API queries | Rich query language with aggregation pipeline | DynamoDB (limited query flexibility without GSIs) |

> **Why not just use BigQuery for everything?** BigQuery excels at analytical queries over large datasets but has query latency in the 1-5 second range. The dashboard needs sub-100ms responses for product lookups, experiment status checks, and live event feeds. MongoDB handles these operational queries while BigQuery handles the heavy analytical workload.

---

## Database and Collection Design

**Database:** `ecommerce_streaming`

| Collection | Record Count | Source | Access Pattern |
|------------|-------------|--------|----------------|
| **products** | ~33,000 | Olist CSV (batch load) | Lookup by product_id; filter by category; weight-based queries |
| **reviews** | ~100,000 | Olist CSV + streaming | Lookup by order_id; filter by rating; full-text search on comments |
| **orders** | ~99,000 | Kafka orders-stream | Lookup by order_id or customer_id; filter by status |
| **user_events** | ~1.3M | Kafka clickstream | User timeline queries; session replay; time-range scans |
| **experiments** | 10 | Configuration | Lookup by experiment_id; list active experiments |
| **experiment_assignments** | ~994,000 | Generated (Phase 2B) | Lookup by (customer_id, experiment_id); variant distribution queries |

### Document Schema Design Principles

- **Denormalize for read performance:** Orders embed their items and payments as nested arrays. One query returns the full order — no joins needed.
- **Use meaningful _id fields sparingly:** Let MongoDB generate ObjectIds for _id; use business keys (product_id, order_id) as indexed unique fields. This avoids key collision issues during upserts.
- **Nest related data, reference distant data:** Product attributes are nested (always queried together). Reviews reference order_id rather than embedding (queried independently).
- **Include metadata timestamps:** Every document carries `created_at` and `last_updated` for audit trails and incremental sync.

---

## Document Schemas

### products

```json
{
  "_id": "ObjectId",
  "product_id": "4244733e06e7ecb4970a6e2683c13e61",
  "category": "perfumery",
  "attributes": {
    "weight_g": 650,
    "dimensions": {"length_cm": 20, "height_cm": 14, "width_cm": 10},
    "photos_qty": 6,
    "name_length": 54,
    "description_length": 1280
  },
  "metadata": {
    "created_at": "2024-01-01T00:00:00Z",
    "last_updated": "2024-01-01T00:00:00Z"
  }
}
```

> **Design choice:** Physical attributes are nested under `attributes` rather than stored flat. This keeps the top-level document clean and makes it easy to extend with new attribute types (e.g., color, material) without touching the core schema.

### user_events (clickstream)

```json
{
  "_id": "ObjectId",
  "event_id": "evt-uuid",
  "event_type": "product_view",
  "user_id": "cust-123",
  "session_id": "sess-456",
  "timestamp": "2024-02-19T12:00:00Z",
  "properties": {
    "product_id": "abc",
    "page": "/p/123",
    "device": "mobile",
    "time_spent_seconds": 45
  }
}
```

### orders (denormalized operational view)

```json
{
  "_id": "ObjectId",
  "order_id": "e481f51cbdc5dee999eceb98",
  "customer_id": "9ef432eb6251297304e0ea311e75e2f1",
  "order_status": "delivered",
  "order_total": 89.90,
  "items": [
    {"product_id": "abc", "quantity": 1, "price": 49.90}
  ],
  "payments": [
    {"type": "credit_card", "value": 89.90}
  ],
  "created_at": "2024-02-19T12:00:00Z"
}
```

### experiment_assignments

```json
{
  "_id": "ObjectId",
  "customer_id": "cust-123",
  "experiment_id": "exp_001",
  "variant": "treatment",
  "assigned_at": "2024-02-01T00:00:00Z",
  "assignment_hash": "sha256_hex_digest"
}
```

---

## Index Strategy

Indexes are the most critical performance lever in MongoDB. Every index adds write overhead, so each one must justify its existence with a specific query pattern.

| Collection | Index | Type | Query It Serves |
|------------|-------|------|----------------|
| products | `product_id` | Unique | Product detail page lookups |
| products | `category` | Single | Category browsing and filtering |
| reviews | `review_id` | Unique | Deduplication on upsert |
| reviews | `order_id` | Single | "Show reviews for this order" |
| reviews | `review_comment_message` | Text | Full-text search ("search reviews for 'quality'") |
| reviews | `rating` | Single | Rating distribution aggregations |
| orders | `order_id` | Unique | Order detail lookups |
| orders | `customer_id` | Single | "Show all orders for this customer" |
| orders | `order_status` | Single | Filter active/delivered/cancelled |
| user_events | `event_id` | Unique | Deduplication on upsert |
| user_events | `{user_id, timestamp}` | Compound | User activity timeline (sorted) |
| user_events | `{session_id, timestamp}` | Compound | Session replay (all events in a session, ordered) |
| experiment_assignments | `{customer_id, experiment_id}` | Compound unique | "What variant is this customer in for this experiment?" |

> **Why compound indexes on user_events?** The two most common query patterns are "show me everything user X did" and "replay session Y." Both need results sorted by timestamp. A compound index on `{user_id, timestamp}` supports the query and the sort in a single index scan — no in-memory sort needed.

### Indexes Not Created (and Why)

- **No index on `user_events.event_type`:** Filtering by event type alone is rare; it is always combined with user_id or session_id, which the compound indexes cover.
- **No index on `orders.created_at`:** Time-range queries on orders go to BigQuery, not MongoDB. MongoDB handles point lookups.
- **No TTL index on user_events:** Events are retained indefinitely for session replay. In production, a TTL index (e.g., 90-day expiry) would control collection growth.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Database engine | MongoDB (Atlas or local Docker) | Document model matches semi-structured e-commerce data; rich query language for API layer |
| Deployment | Docker for development, Atlas for cloud demos | Docker provides zero-dependency local development; Atlas demonstrates cloud deployment skills |
| Schema approach | Schema-on-read with application-level validation | Flexibility for evolving experiment configs; Python validators enforce structure at write time |
| Denormalization | Orders embed items and payments | Eliminates joins for the most common API query (order detail); accepts write amplification trade-off |
| Write strategy | Bulk upserts with `ReplaceOne` | Idempotent re-runs; safe to replay data without duplicates |
| Text search | Native MongoDB text index on reviews | Avoids Elasticsearch dependency; sufficient for review search at this scale |
| Connection pooling | PyMongo default pool (100 connections) | Adequate for single-application access; increase for multi-service production |

---

## Production Considerations

- **Scaling to 1M+ events:** The `user_events` collection grows fastest. At 1.3M documents, queries remain fast with proper indexes. At 10M+, consider **time-based sharding** (shard key: `{timestamp, user_id}`) or TTL indexes to cap collection size.
- **What breaks first:** Unindexed queries. A single aggregation pipeline without index support can lock the collection and spike CPU. Always use `explain()` before deploying new query patterns.
- **Write throughput:** Bulk inserts at 1,000 documents per batch achieve ~10,000 writes/second on a single node. This comfortably handles the Kafka consumer write rate (~27K events/hour peak).
- **Atlas free tier limits:** 512 MB storage, shared vCPU, connection limits. Sufficient for this project's data volume (~200 MB total). Production would require M10+ dedicated clusters.
- **Backup and recovery:** Atlas provides automated backups. For Docker, use `mongodump` before destructive operations. The `--drop` flag on the load script wipes collections — always back up first.
- **Connection string security:** Never commit connection strings with credentials. Use environment variables (`MONGODB_URI`) and `.env` files excluded from version control.

---

*Last updated: March 2026*
