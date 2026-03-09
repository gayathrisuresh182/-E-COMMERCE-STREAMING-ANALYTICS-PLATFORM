# Data Validation Framework — Ensuring Pipeline Integrity Before Streaming

> A 14-check validation suite that verifies completeness, consistency, accuracy, schema conformance, and statistical properties of generated event data and experiment assignments before they enter the Kafka streaming pipeline.

---

## Overview

Data validation is the gate between data generation and data streaming. If malformed events enter Kafka, they propagate through every downstream system — MongoDB, S3, BigQuery, and the dashboard — creating cascading data quality issues that are exponentially harder to fix after the fact. This validation framework catches problems at the source.

The framework validates two primary artifacts: the clickstream event file (`events.ndjson`, ~1.3M events) and the experiment assignment file (`experiment_assignments.csv`, ~994K assignments). It cross-references both against the source Olist dataset to verify referential integrity, temporal correctness, and statistical properties.

> **Why validate before streaming?** In production, bad data in a streaming pipeline requires replaying corrected data through every consumer. For Kafka with 7 topics, MongoDB, S3, and BigQuery, that means coordinating correction across 4+ systems. Catching issues before they enter the pipeline reduces remediation cost by an order of magnitude.

---

## Validation Check Catalog

### Completeness Checks

| # | Check | Pass Criteria | What Failure Indicates |
|---|-------|--------------|----------------------|
| 1 | All orders have events | Every Olist order_id has a `checkout_complete` event in `events.ndjson` | Event generator skipped orders (likely `--limit` was used) |
| 2 | All customer_ids in Olist | No event references a `customer_id` absent from Olist customers | Event generator created synthetic customers instead of using Olist data |
| 3 | All product_ids in Olist | No `product_view`/`add_to_cart` references a product absent from Olist products | Product ID mapping error in event generator |
| 4 | All customers have assignments | Every customer in `events.ndjson` has an `exp_001` assignment | Assignment generator missed customers or used a different customer set |

### Consistency Checks

| # | Check | Pass Criteria | What Failure Indicates |
|---|-------|--------------|----------------------|
| 5 | Timestamps chronological | Within each session, events are strictly time-ordered | Bug in the event generator's timing model |
| 6 | Order totals match | `checkout_complete.total` equals `sum(order_items.price)` for that order | Rounding error or cart/order mismatch in generator |

### Accuracy Checks

| # | Check | Pass Criteria | What Failure Indicates |
|---|-------|--------------|----------------------|
| 7 | Events per order | Average 8-15 events per order (median in this range) | Generator producing too few events (lost steps) or too many (runaway loop) |
| 8 | Session duration | Average session is 5-90 minutes | Timing model producing unrealistic sessions |
| 9 | No future timestamps | All event timestamps are at or before the current time | Clock skew, timezone bug, or incorrect timestamp generation |

### Schema Checks

| # | Check | Pass Criteria | What Failure Indicates |
|---|-------|--------------|----------------------|
| 10 | Required fields present | Every event has: `event_id`, `event_type`, `timestamp`, `customer_id`, `session_id` | Schema drift in event generator; missing field serialization |
| 11 | Valid event types | `event_type` is one of: `session_start`, `page_view`, `product_view`, `add_to_cart`, `cart_view`, `checkout_start`, `checkout_complete` | Typo in generator or undocumented event type added |

### Statistical Checks

| # | Check | Pass Criteria | What Failure Indicates |
|---|-------|--------------|----------------------|
| 12 | Funnel coherence | `page_view` count >= `product_view` >= `add_to_cart` >= `checkout_complete` | Funnel logic error — users skipping steps |
| 13 | 50/50 experiment split | Control/treatment ratio within 49-51% per experiment | Hash function issue or biased input data |
| 14 | No cross-contamination | No customer appears in both control and treatment for the same experiment | Assignment logic bug — determinism violation |

---

## Validation Architecture

### How Checks Are Structured

Each check follows a consistent pattern:

```python
class ValidationCheck:
    name: str           # Human-readable check name
    category: str       # completeness | consistency | accuracy | schema | statistical
    pass_criteria: str  # What "passing" means
    
    def run(self, events, assignments, olist_data) -> CheckResult:
        # Returns: passed (bool), details (dict), recommendations (list[str])
```

### Data Loading Strategy

The validator loads data efficiently for cross-referencing:

| Dataset | Load Strategy | Memory Footprint |
|---------|--------------|-----------------|
| `events.ndjson` | Stream line-by-line; build aggregates in memory | ~200 MB (session maps, event counts) |
| `experiment_assignments.csv` | Full load via pandas | ~80 MB |
| Olist CSVs | Load only needed columns | ~50 MB |

> **Why stream events?** The full `events.ndjson` file is 100-150 MB. Loading all events into memory as Python dicts would require ~1 GB. Streaming with incremental aggregation keeps memory bounded while still computing all needed statistics.

---

## Validation Report

### Output Format

The validator produces a Markdown report at `output/validation_report.md`:

```markdown
# Data Quality Validation Report

Generated: 2024-02-20T10:00:00Z
Events file: output/events.ndjson
Assignments file: output/experiment_assignments.csv

## Summary
- Checks passed: 13/14
- Checks failed: 1/14
- Status: WARN

## Results

| # | Category | Check | Status | Details |
|---|----------|-------|--------|---------|
| 1 | Completeness | All orders have events | PASS | 99,441/99,441 orders covered |
| ...
| 6 | Consistency | Order totals match | FAIL | 23 orders with >$0.01 discrepancy |

## Failed Check Details

### Check 6: Order totals match
- **Finding:** 23 orders have checkout_complete.total != sum(order_items.price)
- **Max discrepancy:** $0.03
- **Likely cause:** Floating-point rounding during aggregation
- **Recommendation:** Use Decimal type or round to 2 decimal places in generator
```

### Severity Levels

| Status | Meaning | Action |
|--------|---------|--------|
| **PASS** | All 14 checks pass | Safe to proceed to Kafka streaming |
| **WARN** | Minor issues (rounding, small count deviations) | Review warnings; usually safe to proceed |
| **FAIL** | Critical issues (missing data, schema violations, split imbalance) | Fix generator and re-run before streaming |

---

## Common Issues and Resolutions

| Issue | Root Cause | Resolution |
|-------|-----------|------------|
| Missing events for some orders | Generator run with `--limit` flag | Re-run without `--limit`: `python scripts/generate_clickstream_events.py` |
| Orphan customer_ids | Events reference customers not in Olist dataset | Verify generator reads from correct Olist orders file |
| Orphan product_ids | product_view/add_to_cart references invalid products | Ensure generator only uses product_ids from the order's `order_items` |
| Non-chronological timestamps | Bug in timing model | Check `generate_clickstream_events.py` — ensure events are emitted in sorted order within each session |
| Order total mismatch | Floating-point arithmetic | Use `round(total, 2)` or `Decimal` in generator |
| Future timestamps | Timezone handling error | Ensure all timestamps use UTC; check system clock |
| Experiment split outside 49-51% | Hash function or filtered input | Re-run `generate_experiment_assignments.py`; verify all customers are included |
| Customer in both variants | Non-deterministic assignment | Verify hash function uses `customer_id|experiment_id` consistently |

---

## MongoDB Post-Load Validation

After data is loaded to MongoDB (Phase 2E), additional validation queries confirm data integrity in the operational store:

### Event Distribution by Type

```javascript
db.user_events.aggregate([
  { $group: { _id: "$event_type", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```

Expected: `page_view` has the highest count, `checkout_complete` matches order count (~99K).

### Experiment Split Verification

```javascript
db.experiment_assignments.aggregate([
  { $group: {
      _id: { experiment: "$experiment_id", variant: "$variant" },
      count: { $sum: 1 }
  }},
  { $sort: { "_id.experiment": 1, "_id.variant": 1 } }
])
```

Expected: Each experiment shows approximately equal control/treatment counts.

### Cross-Contamination Check

```javascript
db.experiment_assignments.aggregate([
  { $group: {
      _id: { customer_id: "$customer_id", experiment_id: "$experiment_id" },
      variants: { $addToSet: "$variant" },
      count: { $sum: 1 }
  }},
  { $match: { count: { $gt: 1 } } }
])
```

Expected result: **zero documents**. Any results indicate duplicate assignments.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Validation timing | Before Kafka streaming (pre-pipeline gate) | Prevents bad data from propagating through 4+ downstream systems |
| Check categories | 5 categories (completeness, consistency, accuracy, schema, statistical) | Covers the full spectrum of data quality dimensions |
| Report format | Markdown | Human-readable, version-controllable, renderable in GitHub/documentation |
| Partial run support | `--allow-partial` flag | Enables validation during development when using `--limit` on the generator |
| Threshold-based pass/fail | Fixed thresholds (e.g., 49-51% for split) | Simple, deterministic, no ambiguity about pass/fail status |
| Cross-dataset validation | Events validated against Olist source data | Catches referential integrity issues that single-file validation would miss |

---

## Production Considerations

- **Continuous validation:** In production, validation runs continuously on incoming streams, not just as a one-time pre-flight check. Tools like Great Expectations, Soda, or dbt tests provide this capability.
- **Data contracts:** The validation checks implicitly define a **data contract** between the generator (producer) and the pipeline (consumer). In a microservice architecture, these contracts would be formalized and enforced at API boundaries.
- **Alerting on degradation:** Production validation should alert (PagerDuty, Slack) when metrics drift outside acceptable ranges, not just fail silently. A 48/52 split is acceptable; a 40/60 split requires immediate investigation.
- **Sampling at scale:** Validating 100M events line-by-line is expensive. Production validators use statistical sampling (validate 1% of events) with probabilistic guarantees on error detection rates.
- **Schema registry integration:** In production Kafka deployments, schema validation shifts from the validation script to the Schema Registry. Producers cannot emit events that violate the registered schema. This eliminates checks 10 and 11 from the manual validation suite.
- **Cost of missed validation:** In a real e-commerce pipeline, a referential integrity failure (orphan product_ids) can cause null product names in dashboards, broken recommendation models, and incorrect revenue attribution. The cost of a 30-second validation run is trivial compared to the cost of debugging a corrupted analytics pipeline.

---

*Last updated: March 2026*
