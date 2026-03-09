# Kafka Topics — Streaming Event Design

> Topic topology, partitioning strategy, and retention policies for the platform's event streaming layer.

---

## Overview

Kafka serves as the central nervous system for all real-time data in this platform. Every user action, order state change, payment confirmation, and experiment assignment flows through Kafka before reaching MongoDB (operational store) or BigQuery (analytical warehouse). The topic design determines how events are partitioned, how long they are retained, and what ordering guarantees downstream consumers can rely on.

Getting topic design wrong is expensive to fix — changing partition keys or counts requires creating new topics and migrating consumers. The decisions documented here were made with e-commerce semantics in mind: orders must be processed in sequence per customer, clickstream events must be sessionized, and experiment assignments must be durable enough for multi-week A/B analysis.

---

## Cluster Configuration

| Parameter | Development | Production (Recommended) |
|-----------|-------------|--------------------------|
| **Brokers** | 1 (Docker) | 3+ (managed or self-hosted) |
| **Replication factor** | 1 | 3 |
| **Min in-sync replicas** | 1 | 2 |
| **Coordination** | ZooKeeper | KRaft (ZooKeeper-free, Kafka 3.3+) |
| **Schema Registry** | Not deployed | Required (Confluent or Apicurio) |
| **Compression** | Snappy (all topics) | Snappy (all topics) |

> **Why Snappy over LZ4 or Zstd?** Snappy offers the best balance of compression ratio and CPU overhead for JSON-encoded events. LZ4 is faster but compresses less; Zstd compresses more but uses significantly more CPU. For the small JSON payloads in this platform (~500 bytes per event), the difference is marginal — Snappy is the Kafka ecosystem default and avoids unnecessary optimization.

---

## Topic Catalog

| Topic | Partitions | Retention | Partition Key | Purpose |
|-------|------------|-----------|---------------|---------|
| `orders-stream` | 3 | 7 days | `customer_id` | Order lifecycle events (created, approved, shipped, delivered, cancelled) |
| `clickstream` | 6 | 3 days | `session_id` | Page views, product clicks, cart actions, search queries |
| `payments-stream` | 3 | 7 days | `order_id` | Payment confirmations, refunds, chargebacks |
| `shipments-stream` | 3 | 7 days | `order_id` | Shipping label created, in-transit updates |
| `deliveries-stream` | 3 | 7 days | `order_id` | Delivery confirmations, failed delivery attempts |
| `reviews-stream` | 3 | 7 days | `product_id` | Review submissions, ratings |
| `experiments-stream` | 3 | 30 days | `user_id` | A/B test variant assignments and outcome events |

---

## Partitioning Strategy

Partition key selection is the most consequential decision in Kafka topic design. The key determines which partition a message lands in, and **messages within a partition are strictly ordered**. Choosing the wrong key either breaks ordering guarantees or creates hot partitions.

### Partition Key Rationale

| Topic | Key | Why This Key |
|-------|-----|--------------|
| `orders-stream` | `customer_id` | All events for a single customer's orders land in the same partition, preserving the sequence: created → paid → shipped → delivered. This is critical for building accurate customer order histories. |
| `clickstream` | `session_id` | Sessionization (grouping clicks into browsing sessions) requires all events from one session to arrive in order. Keying by `session_id` guarantees this without buffering or reordering in the consumer. |
| `payments-stream` | `order_id` | A single order's payment events (authorized → captured → settled) must be processed in sequence. Keying by `order_id` ensures this. |
| `deliveries-stream` | `order_id` | Delivery status updates for a single order must be ordered (shipped → in-transit → delivered). Same rationale as payments. |
| `reviews-stream` | `product_id` | Keying by `product_id` co-locates all reviews for a product, enabling efficient real-time rating aggregation per product in the consumer. |
| `experiments-stream` | `user_id` | A user's assignment and outcome events must be processed together and in order to correctly attribute conversions to variants. |

> **Hot partition risk:** If a small number of customers generate a disproportionate share of orders, keying `orders-stream` by `customer_id` can create hot partitions. In the Olist dataset, customer order frequency is relatively uniform (most customers have 1–2 orders), so this is not an issue. In a production system with power-law order distributions, a compound key (`customer_id + order_id`) or a separate high-volume topic would be necessary.

### Partition Count Rationale

- **3 partitions** (most topics): Sufficient for the current throughput (~500–1K events/hour per topic). Provides 3x consumer parallelism — enough for a consumer group with 3 instances.
- **6 partitions** (`clickstream`): Clickstream generates ~5x the volume of transactional topics. More partitions provide higher throughput and finer-grained parallelism. At 6 partitions, the consumer group can scale to 6 instances before partitions become idle.

> **Why not more partitions?** Each partition has overhead: a file descriptor per broker, increased replication traffic, and longer leader election times during broker failures. Over-partitioning a low-volume topic wastes resources. The rule of thumb is **at least as many partitions as peak consumer instances**, with headroom for 2–3x growth.

---

## Retention Policies

Retention determines how long Kafka stores messages before deleting them. The right retention depends on the downstream recovery model — how long does a consumer need to be able to "rewind" and reprocess?

| Retention | Topics | Rationale |
|-----------|--------|-----------|
| **3 days** | `clickstream` | High volume, low individual value. Consumers process in real-time; replay beyond 3 days is better served from the data lake. Shorter retention reduces disk usage significantly. |
| **7 days** | `orders-stream`, `payments-stream`, `shipments-stream`, `deliveries-stream`, `reviews-stream` | Covers a full business week. If a consumer goes down on Friday evening, it can recover all weekend events on Monday morning. |
| **30 days** | `experiments-stream` | A/B tests run for 2–4 weeks. Experiment events must be retained long enough to reprocess the full test window if the analysis pipeline needs a rerun. |

> **Why not infinite retention?** Kafka can retain messages indefinitely (log compaction or `retention.ms=-1`), effectively becoming a database. For this platform, S3 serves as the long-term archive. Kafka is optimized for recent, high-throughput access — not cheap, durable storage. Archiving older events to S3 and reading from there for historical reprocessing is more cost-effective and operationally simpler.

---

## Event Schema Design

All events follow a common envelope pattern with topic-specific payloads. Events are serialized as JSON (no Schema Registry in development).

```json
{
  "event_id": "uuid-v4",
  "event_type": "order_created",
  "event_timestamp": "2026-03-08T14:30:00Z",
  "source": "order-service",
  "payload": {
    "order_id": "abc123",
    "customer_id": "cust_456",
    "order_total": 149.99,
    "items": [
      {"product_id": "prod_789", "quantity": 2, "price": 74.99}
    ]
  }
}
```

### Schema Conventions

- **`event_id`** — UUID v4, globally unique. Used for deduplication in consumers.
- **`event_type`** — Discriminator for polymorphic topics (e.g., `orders-stream` carries `order_created`, `order_shipped`, `order_delivered`).
- **`event_timestamp`** — ISO 8601, UTC. This is the business timestamp, not the Kafka ingestion timestamp. Consumers should use this for windowing and time-based logic.
- **`source`** — Identifies the producing service. Useful for debugging and routing in multi-service architectures.
- **`payload`** — Topic-specific data. Schema varies by `event_type`.

> **Why JSON over Avro or Protobuf?** For development and portfolio purposes, JSON is human-readable, debuggable, and requires no additional infrastructure (Schema Registry). In production, Avro with Schema Registry would be strongly preferred — it enforces schema compatibility (backward/forward), reduces payload size by ~60%, and catches breaking changes before they reach consumers.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Partition key model | Business-entity keys (customer, session, order, product, user) | Preserves ordering guarantees per entity; matches downstream processing semantics |
| Partition count | 3 (standard) / 6 (clickstream) | Matches current throughput with room for 2–3x growth; avoids over-partitioning overhead |
| Retention model | Time-based (3/7/30 days) per topic | Balances replay capability with disk cost; long-term archive lives in S3 |
| Serialization | JSON (dev) / Avro (production recommended) | JSON for readability and zero-infrastructure dev; Avro for schema enforcement and size reduction |
| Compression | Snappy | Best CPU/compression trade-off for small JSON payloads; Kafka ecosystem default |
| Topic granularity | One topic per event domain (orders, payments, clickstream, etc.) | Clear ownership boundaries; independent scaling and retention per domain |
| Replication | 1 (dev) / 3 (production) | Dev simplicity vs. production fault tolerance |

---

## Production Considerations

### Scaling to 100x Volume

- **Clickstream at 500K events/hour:** Increase to 12–24 partitions. Deploy a 6-instance consumer group. Consider switching to Avro to reduce network bandwidth by ~60%.
- **Order volume spikes (flash sales):** Pre-scale partitions before known events. Kafka does not support partition count reduction — over-provisioning temporarily is acceptable.
- **Cross-datacenter replication:** Use MirrorMaker 2 or Confluent Replicator to replicate critical topics (orders, payments) to a secondary cluster for disaster recovery.

### Failure Modes

- **Consumer lag accumulation:** If consumers process slower than producers, lag grows until retention expires and messages are lost. Monitor consumer lag per partition; alert when lag exceeds 5 minutes of wall-clock time.
- **Broker disk full:** Kafka stops accepting writes. Monitor disk usage; set up alerts at 70% capacity. Reduce retention on high-volume topics if disk is constrained.
- **Partition leader election storm:** If all 3 brokers restart simultaneously (e.g., bad deployment), leader election for all partitions happens at once, causing a brief outage. Roll restarts one broker at a time.
- **Poison pill messages:** A malformed message that crashes the consumer blocks the entire partition. Implement a dead-letter queue (DLQ) pattern — after N retries, route the bad message to a `<topic>-dlq` topic and continue processing.
- **Key skew:** If a single `customer_id` generates millions of events (e.g., a bot), one partition becomes a bottleneck. Implement key-based rate limiting at the producer level.

### Monitoring Checklist

| Metric | Threshold | Action |
|--------|-----------|--------|
| Consumer lag (messages) | > 1,000 | Investigate consumer performance; add instances |
| Consumer lag (time) | > 5 minutes | Critical — consumers may lose data at retention boundary |
| Broker disk usage | > 70% | Reduce retention or add disk |
| Under-replicated partitions | > 0 | Broker health issue; check broker logs |
| Request latency (p99) | > 100ms | Network or broker overload |

---

*Last updated: March 2026*
