# Kafka — Event Streaming Infrastructure

> Apache Kafka provides the real-time event backbone for this platform, streaming orders, clickstream events, payments, deliveries, and reviews through a partitioned, durable log that feeds both the speed layer (MongoDB) and the batch layer (S3 archive to BigQuery).

---

## Overview

In a Lambda architecture, the streaming layer must durably capture every event in real time while enabling multiple consumers to process the same data independently. Kafka serves this role as a distributed commit log — producers write events once, and any number of consumers (MongoDB writer, S3 archiver, real-time aggregator) read from the log at their own pace.

This project uses Kafka to replay ~2 years of Brazilian e-commerce history as a compressed real-time stream: orders, clickstream interactions, payments, shipment updates, reviews, and experiment assignments flow through 7 topics at rates simulating a mid-size e-commerce platform.

---

## Deployment Decision

| Option | Strengths | Limitations | Best For |
|--------|-----------|-------------|----------|
| **Docker Compose (chosen)** | Free, reproducible, includes Kafka UI, single-command setup | Single broker, no true HA | Development, portfolio demos, local testing |
| Confluent Cloud | Fully managed, production-grade, schema registry included | Costs money after free trial | Production workloads, team environments |
| AWS MSK | AWS-native, IAM integration, VPC-level security | Complex setup (VPC, security groups, multi-AZ) | AWS-centric production deployments |

> **Why Docker Compose for this project:** The goal is demonstrating streaming architecture patterns — topic design, partition key strategy, consumer groups, exactly-once semantics — not operating a production cluster. Docker Compose provides all of this with zero cloud cost and full reproducibility. Anyone cloning the repo can `docker-compose up` and have a working Kafka cluster in 30 seconds.

### Docker Compose Services

| Service | Role | Port |
|---------|------|------|
| **Zookeeper** | Cluster coordination and metadata management | 2181 |
| **Kafka Broker** | Single-broker message log | 9092 |
| **Kafka UI** | Web-based topic/consumer inspection | 8080 |

**Why Zookeeper over KRaft:** KRaft (Kafka's newer Zookeeper-free mode) is production-ready as of Kafka 3.3+, but Zookeeper mode remains better documented and more widely supported in Docker images. For a single-broker development setup, the difference is negligible.

---

## Topic Design

### Topic Specifications

| Topic | Partitions | Retention | Partition Key | Compression | Events/Hour (est.) |
|-------|-----------|-----------|---------------|-------------|-------------------|
| `orders-stream` | 3 | 7 days | `customer_id` | Snappy | ~2,000 |
| `clickstream` | 6 | 3 days | `session_id` | Snappy | ~27,000 |
| `payments-stream` | 3 | 7 days | `order_id` | Snappy | ~2,200 |
| `shipments-stream` | 3 | 7 days | `order_id` | Snappy | ~2,000 |
| `deliveries-stream` | 3 | 7 days | `order_id` | Snappy | ~8,300 |
| `reviews-stream` | 3 | 7 days | `product_id` | Snappy | ~2,000 |
| `experiments-stream` | 3 | 30 days | `user_id` | Snappy | ~1,000 |

### Partition Key Strategy

Partition keys determine ordering guarantees and consumer parallelism. Each key was chosen to match the primary query and ordering requirement:

- **`customer_id` for orders:** All orders from the same customer land on the same partition, preserving order sequence per customer. Critical for detecting repeat purchases and computing customer lifetime value in order.
- **`session_id` for clickstream:** All events within a user session stay on the same partition. This guarantees session-level event ordering (page_view before add_to_cart before checkout_complete) — essential for funnel analysis.
- **`order_id` for payments/shipments/deliveries:** All lifecycle events for an order stay together. A consumer processing delivery status updates sees `shipped → in_transit → out_for_delivery → delivered` in sequence.
- **`product_id` for reviews:** Enables per-product review aggregation without cross-partition reads.
- **`user_id` for experiments:** All experiment assignments for a user on one partition, enabling consistent variant resolution.

> **Why 6 partitions for clickstream but 3 for everything else?** Clickstream generates 10x more events than any other topic. More partitions allow more consumer instances to process clickstream in parallel. The other topics are low enough throughput that 3 partitions (matching 3 potential consumer instances) provide sufficient parallelism.

### Partition Count Rationale

The formula for minimum partitions: `max(target_throughput / per_partition_throughput, consumer_instances)`. At ~27K events/hour for clickstream, even a single partition handles the throughput. But partitions also determine **consumer parallelism** — 6 partitions means up to 6 consumers can process clickstream events in parallel. This headroom is valuable if processing per event becomes expensive (e.g., ML inference on each event).

---

## Compression and Retention

### Compression: Snappy

| Algorithm | Speed | Ratio | CPU Cost |
|-----------|-------|-------|----------|
| None | N/A | 1:1 | None |
| **Snappy (chosen)** | Fast | ~1.5:1 | Very low |
| LZ4 | Fastest | ~1.4:1 | Lowest |
| GZIP | Slow | ~2.5:1 | High |
| Zstd | Moderate | ~2.8:1 | Moderate |

Snappy provides a good compression ratio with negligible CPU impact. For JSON-serialized e-commerce events (~500 bytes each), Snappy typically reduces message size by 40-50%. At this project's throughput, compression is more about establishing good practices than saving meaningful bandwidth.

### Retention Decisions

- **7-day retention for transactional topics:** Provides a full week for consumers to process events and for debugging. Events are also archived to S3 bronze, so Kafka is not the long-term store.
- **3-day retention for clickstream:** Higher volume, less need for replay. Clickstream is archived to S3 and loaded to BigQuery; Kafka retention is only for consumer catch-up.
- **30-day retention for experiments:** Experiment assignments change infrequently and are small. Longer retention allows late-joining consumers to bootstrap state.

---

## Producer and Consumer Configuration

### Producer Settings

| Setting | Value | Why |
|---------|-------|-----|
| `acks` | `all` | Wait for all in-sync replicas to acknowledge. Prevents data loss on broker failure. With a single broker this is equivalent to `acks=1`, but the setting is correct for multi-broker production. |
| `retries` | 3 | Retry transient failures (network blips, leader election). Combined with exponential backoff. |
| `batch_size` | 16 KB (default) | Batches multiple messages per network call. Improves throughput by 5-10x over single-message sends. |
| `linger_ms` | 10 | Wait up to 10ms to accumulate more messages in a batch. Small delay for significant throughput gain. |
| `compression_type` | `snappy` | Compresses each batch before sending. |

### Consumer Settings

| Setting | Value | Why |
|---------|-------|-----|
| `auto_offset_reset` | `earliest` | New consumer groups start from the beginning of the topic. Ensures no data is missed on first run. |
| `enable_auto_commit` | `False` | Manual offset commits after processing. Prevents marking messages as consumed before they are actually processed (at-least-once guarantee). |
| `session_timeout_ms` | 10000 | If a consumer fails to heartbeat within 10 seconds, the broker reassigns its partitions. Balances fast failure detection with avoiding false positives. |
| `max_poll_records` | 500 | Process up to 500 messages per poll cycle. Limits memory usage and ensures timely commits. |

> **Why manual commit?** Auto-commit marks messages as processed on a timer, regardless of whether processing succeeded. If the consumer crashes between auto-commit and actual processing, those messages are lost. Manual commit after successful processing provides at-least-once delivery guarantees.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Message format | JSON | Human-readable, easy to debug, sufficient for this throughput. Avro/Protobuf warranted at 10x+ volume for schema enforcement and 3-5x size reduction. |
| Topic auto-creation | Disabled | Explicit topic creation prevents typos from silently creating rogue topics. All topics defined in `create_kafka_topics.py`. |
| Serialization | JSON string (UTF-8) | Avoids Avro/Schema Registry complexity. Trade-off: no schema enforcement at the broker level. |
| Consumer group model | One group per application | MongoDB writer, S3 archiver, and real-time aggregator each get independent consumer groups. Each reads the full stream independently. |
| Replication factor | 1 (single broker) | Development environment. Production would use replication factor 3 across 3+ brokers for fault tolerance. |
| Idempotent producer | Not enabled | Requires `enable.idempotence=true` and `max.in.flight.requests.per.connection=5`. Valuable for production but unnecessary overhead for deterministic replay. |

---

## Production Considerations

- **Scaling to production:** Move from 1 broker to 3+ brokers with replication factor 3. This provides fault tolerance (survive 1 broker failure) and increases throughput through partition distribution across brokers.
- **What breaks first:** Consumer lag. If a consumer (e.g., MongoDB writer) processes events slower than producers emit them, lag grows unboundedly. Monitor consumer lag via Kafka UI or JMX metrics. Solutions: increase partitions (more consumer parallelism), optimize consumer processing, or add consumer instances.
- **Schema evolution:** JSON without schema registry means producers can silently add/remove fields, breaking consumers. At scale, add Confluent Schema Registry with Avro or JSON Schema to enforce backward/forward compatibility.
- **Exactly-once semantics:** This project uses at-least-once delivery (manual commit + consumer-side deduplication via event_id). True exactly-once requires Kafka Transactions (producer) + idempotent consumer patterns — significant complexity for marginal benefit at this scale.
- **Monitoring:** Kafka UI provides topic inspection, consumer group lag, and message browsing. In production, export JMX metrics to Prometheus/Grafana for alerting on lag thresholds, broker health, and throughput anomalies.
- **Disk pressure:** At 27K events/hour with 3-day retention, clickstream accumulates ~2M messages (~1 GB). Well within single-disk capacity. At 100x scale, plan for dedicated disks and monitor `log.retention.bytes` alongside time-based retention.

---

*Last updated: March 2026*
