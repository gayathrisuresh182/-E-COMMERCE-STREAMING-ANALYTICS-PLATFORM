# Kafka Producer Architecture — Real-Time E-Commerce Event Replay

> Five parallel Kafka producers replay two years of Olist e-commerce history as a compressed real-time stream, simulating the event throughput of a mid-size online marketplace with orders, clickstream, payments, deliveries, and reviews.

---

## Overview

Static datasets are useful for batch analytics, but a streaming analytics platform needs to demonstrate real-time processing. These producers bridge the gap by replaying historical Olist data through Kafka topics with compressed timing — two years of history streams through in 48 hours, preserving the relative ordering and temporal patterns of the original data.

This approach is a common pattern in streaming system development known as **event replay** or **historical replay**. It allows the entire downstream pipeline (consumers, MongoDB writers, S3 archivers, BigQuery loaders) to be tested against realistic data volumes and timing patterns without waiting for live production traffic.

---

## Producer Architecture

### Five Parallel Producers

| Producer | Input Source | Kafka Topic | Partition Key | Event Types | Est. Rate |
|----------|------------|-------------|---------------|-------------|-----------|
| **Orders** | Olist orders + order_items | `orders-stream` | `customer_id` | `order_placed` | ~2,000/hr |
| **Clickstream** | `events.ndjson` (Phase 2C) | `clickstream` | `session_id` | session_start, page_view, product_view, add_to_cart, cart_view, checkout_start, checkout_complete | ~27,000/hr |
| **Payments** | Olist order_payments | `payments-stream` | `order_id` | `payment_completed` | ~2,200/hr |
| **Deliveries** | Olist orders (delivery dates) | `deliveries-stream` | `order_id` | shipped, in_transit, out_for_delivery, delivered | ~8,300/hr |
| **Reviews** | Olist order_reviews | `reviews-stream` | `product_id` | `review_posted` | ~2,000/hr |

> **Why five separate producers instead of one?** Each producer models a different bounded context in the e-commerce domain. In production, orders come from the checkout service, payments from the payment gateway, deliveries from the logistics system, and reviews from the review service. Separate producers mirror this microservice reality and allow independent scaling, error handling, and replay.

### Data Flow

```
Olist Historical Data (CSV/NDJSON)
    |
    v
[Time Compression Engine]
    - Maps 2-year historical span to 48-hour replay window
    - Preserves relative event ordering and gaps
    |
    v
[5 Parallel Producer Threads]
    - Each thread reads its data source
    - Sleeps until replay timestamp for each event
    - Sends to assigned Kafka topic with partition key
    |
    v
[Kafka Cluster]
    - 7 topics, 3-6 partitions each
    - Snappy compression
    - 3-30 day retention
```

---

## Time Compression

### The Problem

Olist data spans approximately 2 years (2016-2018). Streaming this in real time would take 2 years. The platform needs to demonstrate real-time processing in hours, not years.

### The Solution: Linear Time Compression

```
replay_timestamp = replay_start + (historical_ts - first_historical_ts) * compression_factor

where compression_factor = replay_duration / historical_span
```

| Parameter | Value |
|-----------|-------|
| Historical span | ~730 days (2 years) |
| Replay window | 48 hours (configurable) |
| Compression factor | ~0.0027 (each historical day = ~3.9 replay minutes) |

### What Time Compression Preserves

- **Relative ordering:** Events that happened in sequence historically maintain the same sequence during replay.
- **Temporal patterns:** Weekday/weekend patterns, time-of-day distributions, and seasonal trends are compressed but proportionally preserved.
- **Inter-event gaps:** The relative spacing between events is maintained. A payment that followed an order by 30 minutes historically might follow by ~2.4 seconds during replay.

### What It Does Not Preserve

- **Absolute timing:** All timestamps are shifted to the replay window. Consumers should use event timestamps (event time), not processing timestamps (wall time).
- **Burst patterns:** Extreme spikes (e.g., Black Friday) are compressed into very short bursts, which may exceed single-consumer processing capacity. This is actually a useful stress test.

---

## Rate Limiting and Backpressure

### Sleep-Based Rate Control

Each producer thread uses a `ReplayClock` that computes the wall-clock time for each event's replay timestamp. The producer sleeps until that time before sending:

```python
replay_time = replay_clock.to_wall_time(event.historical_timestamp)
sleep_duration = replay_time - now()
if sleep_duration > 0:
    time.sleep(sleep_duration)
producer.send(topic, value=event, key=partition_key)
```

### Resulting Throughput

| Producer | Total Events | Over 48 Hours | Per Hour | Per Second |
|----------|-------------|---------------|----------|------------|
| Orders | ~99,000 | 48h | ~2,000 | ~0.6 |
| Clickstream | ~1,300,000 | 48h | ~27,000 | ~7.5 |
| Payments | ~104,000 | 48h | ~2,200 | ~0.6 |
| Deliveries | ~400,000 | 48h | ~8,300 | ~2.3 |
| Reviews | ~98,000 | 48h | ~2,000 | ~0.6 |
| **Total** | **~2,000,000** | | **~41,500** | **~11.5** |

> **Context:** 11.5 events/second is modest by production standards (Shopify processes ~80,000 orders/minute at peak). This throughput is designed to be comfortable for a single-broker Kafka cluster while producing enough volume to demonstrate meaningful streaming analytics.

---

## Producer Configuration

| Setting | Value | Rationale |
|---------|-------|-----------|
| `bootstrap_servers` | `localhost:9092` (or `KAFKA_BOOTSTRAP` env) | Single broker for development |
| `acks` | `all` | Durability guarantee; wait for all in-sync replicas |
| `retries` | 3 | Recover from transient network/broker failures |
| `retry_backoff_ms` | Exponential (100ms, 200ms, 400ms) | Avoid overwhelming a recovering broker |
| `value_serializer` | JSON (UTF-8) | Human-readable; compatible with Kafka UI inspection |
| `compression_type` | `snappy` | Reduces network I/O with minimal CPU overhead |
| `batch_size` | 16 KB (default) | Batches messages per partition for efficient network usage |
| `linger_ms` | 10 | Small delay to accumulate batches |

### Why JSON Instead of Avro

JSON serialization was chosen for transparency and debugging ease. Trade-offs:

| Factor | JSON | Avro |
|--------|------|------|
| Message size | ~500 bytes/event | ~150 bytes/event (3x smaller) |
| Schema enforcement | None (consumer validates) | Broker-level via Schema Registry |
| Debugging | Human-readable in Kafka UI | Requires Avro deserializer |
| Setup complexity | Zero additional infrastructure | Requires Schema Registry service |
| Recommendation at scale | Replace at >100K events/hour | Standard for production |

---

## Error Handling Strategy

### Per-Message Failure

```
Send attempt
    |-- Success -> log, continue
    |-- Failure -> retry 1 (100ms backoff)
        |-- Failure -> retry 2 (200ms backoff)
            |-- Failure -> retry 3 (400ms backoff)
                |-- Failure -> log error, skip message, continue
```

- **Philosophy:** A single failed message should not stop the entire replay. The error is logged with full context (topic, key, error type) for post-run diagnosis.
- **Idempotency:** Consumers use `event_id` for deduplication, so a retried message that was actually delivered (network timeout on ACK) does not create duplicates.

### Graceful Shutdown

On `Ctrl+C` or `SIGTERM`:

1. Stop all producer threads (set shutdown flag)
2. `flush()` remaining buffered messages (up to 30-second timeout)
3. `close()` producer connections
4. Log final statistics (messages sent, errors, duration)

---

## Orchestration

### Parallel Execution

All five producers run as parallel threads within a single Python process:

- **Shared replay clock:** All producers use the same `ReplayClock` instance to maintain temporal consistency across topics.
- **Independent error handling:** A failure in the deliveries producer does not affect orders or clickstream.
- **Thread safety:** Each producer has its own `KafkaProducer` instance. No shared mutable state between threads.

### Execution Modes

| Mode | Command | Behavior |
|------|---------|----------|
| Full replay | `python scripts/run_producers.py` | All producers, 48h compressed replay |
| Test mode | `python scripts/run_producers.py --test` | 10 events per producer, instant completion |
| Single producer | `python scripts/run_producers.py --producer orders` | Run only the orders producer |
| Limited replay | `python scripts/run_producers.py --limit 1000` | Cap each producer at 1000 events |
| Dry run | `python scripts/run_producers.py --dry-run` | Compute timestamps and log, do not send to Kafka |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Replay model | Time-compressed linear replay | Preserves temporal patterns; tests consumer behavior under realistic ordering |
| Threading model | 5 threads, 1 process | Simple concurrency; GIL is not a bottleneck since threads are I/O-bound (sleeping + network) |
| Partition key strategy | Domain-specific keys | Same keys as production would use; exercises Kafka's ordering guarantees |
| Error tolerance | Skip-and-continue | Replay is not transactional; a few missed messages are acceptable for testing |
| Serialization | JSON | Debugging and inspection priority over message size |
| Replay duration | 48 hours (configurable) | Long enough to test sustained consumer processing; short enough for a demo cycle |

---

## Production Considerations

- **Scaling producers:** In production, each bounded context (orders, payments, etc.) would have its own producer service. This project's single-process multi-thread model is appropriate for replay but would be replaced by independent microservices in production.
- **Exactly-once delivery:** The current producers use at-least-once delivery (retries without idempotent producer). For production financial data (payments), enable Kafka idempotent producer (`enable.idempotence=true`) and transactional writes.
- **Schema evolution:** Adding new fields to events is safe with JSON (consumers ignore unknown fields). Removing or renaming fields breaks consumers silently. In production, Schema Registry with compatibility checks prevents breaking changes.
- **Monitoring replay progress:** Each producer logs its progress (events sent, current historical timestamp, lag). For longer replays, expose these metrics via Prometheus or a simple HTTP health endpoint.
- **Backpressure handling:** If Kafka is slow (broker overloaded, disk full), the producer's `send()` blocks. The sleep-based rate limiter naturally adapts — a delayed send shifts all subsequent sends later. At production scale, implement explicit backpressure (reduce batch size, pause ingestion).

---

*Last updated: March 2026*
