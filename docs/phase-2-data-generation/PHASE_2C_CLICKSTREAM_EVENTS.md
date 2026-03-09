# Clickstream Event Generation — Simulating Realistic User Behavior

> Generates 1.2-1.8 million realistic clickstream events from Olist order data, simulating complete user funnels with session context, device metadata, and experiment variant tagging — ready for Kafka streaming and funnel analysis.

---

## Overview

Raw order data tells you *what* customers bought, but not *how* they got there. Clickstream data fills this gap by capturing the user's journey: landing on the site, browsing pages, viewing products, adding items to cart, starting checkout, and completing the purchase. This behavioral data is essential for funnel analysis, session-based A/B testing, and real-time personalization.

Since the Olist dataset contains only completed orders (no raw clickstream), this generator synthesizes realistic user event streams for each order. Every event follows a plausible timing pattern, references real products from the order, and carries the customer's experiment variant assignment. The output is NDJSON (one JSON event per line) — the standard format for streaming into Kafka.

---

## Event Generation Architecture

### From Orders to Event Streams

For each of Olist's ~99,000 orders, the generator produces a complete user session:

```
Order Record (Olist)
    |
    v
[Session Context Generation]
    - session_id (UUID)
    - device (mobile/desktop/tablet, weighted distribution)
    - referrer (direct/search/social/email)
    |
    v
[Experiment Assignment Lookup]
    - customer_id -> experiment_id -> variant
    |
    v
[Funnel Event Emission]
    session_start -> page_view(s) -> product_view(s) -> add_to_cart(s)
    -> cart_view(s) -> checkout_start -> checkout_complete
```

### Event Types and Counts Per Session

| Event Type | Count Per Session | Timing Relative to Order |
|------------|------------------|-------------------------|
| `session_start` | 1 | 10-60 minutes before order |
| `page_view` | 3-10 (random) | Spread evenly between session start and first product view |
| `product_view` | 1-5 (based on order items) | After page views; references actual products from the order |
| `add_to_cart` | 1 per order item | After corresponding product view |
| `cart_view` | 0-3 (random) | After add-to-cart events |
| `checkout_start` | 1 | 2-10 minutes before order timestamp |
| `checkout_complete` | 1 | Matches `order_purchase_timestamp` exactly |

> **Why these specific counts?** They mirror industry benchmarks for e-commerce session behavior. A typical converting session involves 5-12 page interactions before purchase. The ranges introduce natural variance — not every session looks identical.

---

## Timing Model

### Session Duration

Sessions span 10-60 minutes from `session_start` to `checkout_complete`. The duration is drawn from a truncated normal distribution centered at 25 minutes, matching typical e-commerce session lengths.

### Intra-Session Timing

| Transition | Delay |
|-----------|-------|
| `session_start` to first `page_view` | 5-30 seconds |
| Between `page_view` events | 15-90 seconds |
| `page_view` to `product_view` | 10-60 seconds |
| `product_view` dwell time (`time_spent_seconds`) | 10-120 seconds |
| `product_view` to `add_to_cart` | 15-90 seconds |
| Between `add_to_cart` events | 15-90 seconds |
| Last `add_to_cart` to `checkout_start` | 30-300 seconds |
| `checkout_start` to `checkout_complete` | 60-600 seconds |

### Chronological Guarantee

All events within a session are strictly ordered by timestamp. The generator enforces:

```
session_start < page_view[0] < ... < page_view[n] < product_view[0]
< ... < add_to_cart[0] < ... < checkout_start < checkout_complete
```

This ordering is critical for Kafka partitioning (all session events on the same partition) and downstream funnel analysis.

---

## Abandoned Sessions

The generator optionally produces incomplete funnels simulating users who did not convert:

- **Partial funnels:** `session_start` + 2-6 `page_view` events, sometimes `product_view`, sometimes `add_to_cart` — but no `checkout_start` or `checkout_complete`
- **Purpose:** Creates realistic funnel drop-off ratios for conversion analysis

### Target Funnel Shape

```
1000 sessions
 └── 800 view a product (80%)
      └── 500 add to cart (50%)
           └── 400 start checkout (40%)
                └── 350 complete purchase (35%)
```

This matches industry-average e-commerce conversion funnels where roughly 2-4% of total sessions result in a purchase.

---

## Event Schema

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "product_view",
  "timestamp": "2024-02-20T14:25:00Z",
  "session_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "customer_id": "9ef432eb6251297304e0ea311e75e2f1",
  "experiment_id": "exp_001",
  "variant": "control",
  "properties": {
    "product_id": "4244733e06e7ecb4970a6e2683c13e61",
    "page_url": "/product/4244733e06e7ecb4970a6e2683c13e61",
    "device": "mobile",
    "referrer": "google_search",
    "time_spent_seconds": 45
  },
  "metadata": {
    "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)",
    "ip_address": "sanitized"
  }
}
```

### Field Descriptions

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | UUID | Globally unique; used for deduplication in consumers |
| `event_type` | enum | One of the 7 defined event types |
| `timestamp` | ISO 8601 UTC | When the event occurred; derived from the timing model |
| `session_id` | UUID | Groups all events in a single user visit; used as Kafka partition key |
| `customer_id` | string | Links to Olist customer; joins to experiment assignments |
| `experiment_id` | string | Which experiment this event participates in |
| `variant` | string | `"control"` or `"treatment"` — from assignment engine |
| `properties` | object | Event-type-specific data (product_id, page_url, device, etc.) |
| `metadata` | object | Request-level context (user agent, sanitized IP) |

---

## Data Integrity Constraints

| Constraint | Validation Rule | Why It Matters |
|-----------|----------------|---------------|
| Order coverage | Every Olist order has exactly one `checkout_complete` event | Ensures 1:1 mapping between orders and completed sessions |
| Timestamp ordering | Events within a session are strictly chronological | Funnel analysis and session replay depend on correct ordering |
| Referential integrity (customers) | Every `customer_id` exists in Olist customers table | Prevents orphaned events that cannot be joined to customer data |
| Referential integrity (products) | Every `product_id` in `product_view`/`add_to_cart` exists in that order's items | Product-level funnel analysis requires valid product references |
| Experiment consistency | `variant` matches the assignment from Phase 2B | Incorrect variant tagging invalidates all experiment analysis |

---

## Output Format and Volume

| Property | Value |
|----------|-------|
| Format | NDJSON (newline-delimited JSON) |
| File | `output/events.ndjson` |
| Estimated events | 1.2-1.8 million (99K orders x 12-18 events/order average) |
| Estimated file size | 100-150 MB |
| Encoding | UTF-8 |

### Why NDJSON

- **Streaming-friendly:** Each line is independently parseable. Kafka producers read line-by-line without loading the entire file into memory.
- **Append-friendly:** New events can be appended without modifying existing content.
- **Tool-compatible:** Works with `jq`, `wc -l`, `head`, and standard Unix text processing tools for quick inspection.

---

## Kafka Integration

The generated NDJSON feeds directly into the Kafka producer (Phase 2D):

- **Topic:** `clickstream`
- **Partition key:** `session_id` — ensures all events in a session land on the same partition, preserving intra-session ordering
- **Serialization:** JSON string (UTF-8 encoded)

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

with open("output/events.ndjson") as f:
    for line in f:
        event = json.loads(line)
        producer.send(
            "clickstream",
            value=event,
            key=event["session_id"]
        )
producer.flush()
```

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Generation approach | Synthesize from orders | Olist has no clickstream data; synthesis creates realistic funnels grounded in real purchase data |
| Output format | NDJSON | Streaming-compatible; line-by-line processing; no memory overhead |
| Session-to-order mapping | 1:1 for converted sessions | Each order represents one converting session; simplifies validation |
| Timing distribution | Truncated normal with realistic ranges | Produces natural variance without unrealistic outliers |
| Abandoned sessions | Optional (`--abandoned N` flag) | Useful for funnel analysis but increases file size significantly |
| Experiment tagging | Embedded in each event | Eliminates join overhead in streaming consumers; enables real-time variant-aware processing |
| Reproducibility | `--seed` flag for deterministic output | Same seed produces identical events across runs; critical for regression testing |

---

## Production Considerations

- **Volume at scale:** At 1M orders/month (10x Olist), the generator would produce ~15M events/month. NDJSON at this volume (~1.5 GB/month) remains manageable. At 100x, switch to Parquet for 10:1 compression and columnar efficiency.
- **Real vs. synthetic:** In production, clickstream comes from real user interactions via JavaScript tracking (Segment, Snowplow, or custom). This generator simulates the shape of that data for pipeline development and testing.
- **Bot filtering:** Real clickstream includes bot traffic (scrapers, crawlers). Production pipelines need bot detection (user-agent filtering, rate-based heuristics). Synthetic data is bot-free by construction.
- **Session stitching:** This generator assigns one session per order. Real users have multiple sessions before purchasing (cross-device, return visits). Production systems need session stitching logic using cookies or identity resolution.
- **Late-arriving events:** In production, events can arrive out of order due to network delays. The pipeline should handle late arrivals with watermarking (event-time processing) rather than assuming ordered delivery.

---

*Last updated: March 2026*
