# Experiment Assignment Engine — Hash-Based Deterministic Randomization

> A deterministic, reproducible assignment system that maps every customer to a variant for each of 10 concurrent experiments using SHA-256 hashing — producing 994,410 stable assignments with guaranteed 50/50 splits.

---

## Overview

Experiment assignment is the mechanism that decides which variant (control or treatment) each customer experiences. The assignment must satisfy three critical properties: it must be **deterministic** (same customer always gets the same variant), **uniform** (approximately 50/50 split), and **independent across experiments** (a customer's variant in experiment 1 should not predict their variant in experiment 2).

This system uses hash-based randomization rather than random number generation. The key advantage is reproducibility — assignments can be recomputed from customer IDs at any time, on any machine, without storing state or managing random seeds.

---

## Randomization Algorithm

### Core Logic

```python
import hashlib

def assign_variant(customer_id: str, experiment_id: str) -> str:
    key = f"{customer_id}|{experiment_id}"
    hash_value = int(hashlib.sha256(key.encode()).hexdigest(), 16)
    return "control" if hash_value % 2 == 0 else "treatment"
```

### Why This Works

- **SHA-256 produces uniform distribution:** Cryptographic hash functions distribute outputs uniformly across their output space. Taking `mod 2` of a uniformly distributed integer yields 0 or 1 with equal probability.
- **Deterministic:** Given the same `customer_id` and `experiment_id`, the hash always produces the same variant. No random seed to manage, no state to persist.
- **Experiment independence:** Including `experiment_id` in the hash input means the same customer gets an independently-drawn variant for each experiment. A customer in the control group for experiment 1 has a 50% chance of being in control or treatment for experiment 2.
- **Collision-free:** SHA-256 has a 256-bit output space (~10^77 possible values). For 99,441 customers across 10 experiments, collision probability is effectively zero.

> **Why not just use `random.random()` with a seed?** Random seeds produce reproducible results only when the same sequence of calls is made in the same order. Adding or removing a customer changes all subsequent assignments. Hash-based assignment is stable under additions and removals — each assignment is independently computed.

---

## Assignment at Scale

### Volume

| Dimension | Count |
|-----------|-------|
| Unique customers (Olist) | 99,441 |
| Experiments | 10 |
| Total assignments | 994,410 |
| Expected per variant per experiment | ~49,720 |

### Expected Split Distribution

For each experiment, the hash function produces a near-perfect 50/50 split:

| Experiment | Expected Control | Expected Treatment | Acceptable Range |
|------------|-----------------|-------------------|-----------------|
| Each of 10 | ~49,720 (50.0%) | ~49,721 (50.0%) | 45%-55% (warn outside this) |

> **Statistical guarantee:** For 99,441 customers, the standard deviation of the split is `sqrt(n * 0.5 * 0.5) = ~158`. A 50/50 split will fluctuate by roughly +/- 0.3%, well within the 45-55% acceptable range.

### Validation Checks

| Check | Pass Criteria | What Failure Means |
|-------|--------------|-------------------|
| Total assignment count | Exactly 994,410 | Missing customers or experiments |
| No duplicate (customer_id, experiment_id) pairs | 0 duplicates | Assignment logic bug |
| Control/treatment split per experiment | Within 45-55% | Hash function issue or data corruption |
| All Olist customer_ids present | 100% coverage | Input data filtering error |
| Cross-experiment independence | Chi-square p > 0.05 for any pair | Hash input not properly combining experiment_id |

---

## Assignment Schema

### CSV Output (`output/experiment_assignments.csv`)

| Field | Type | Description |
|-------|------|-------------|
| `assignment_id` | UUID | Unique identifier per assignment row |
| `customer_id` | string | Olist customer identifier |
| `customer_state` | string | Customer's state (for stratification analysis) |
| `experiment_id` | string | `exp_001` through `exp_010` |
| `experiment_name` | string | Snake_case name (e.g., `free_shipping_threshold`) |
| `variant` | string | `"control"` or `"treatment"` |
| `assigned_at` | ISO 8601 | Assignment timestamp |
| `assignment_method` | string | Always `"hash_based"` |

### MongoDB Documents (`ecommerce_streaming.experiment_assignments`)

Same fields as CSV, plus MongoDB-generated `_id`. Indexed on `(customer_id, experiment_id)` as a unique compound index for fast variant lookups during event processing.

### Experiment Metadata (`ecommerce_streaming.experiments`)

```json
{
  "experiment_id": "exp_001",
  "experiment_name": "free_shipping_threshold",
  "description": "Free Shipping Threshold",
  "start_date": "2024-02-01",
  "end_date": "2024-03-01",
  "status": "active",
  "variants": [
    {"variant_id": "control", "name": "control_50", "allocation": 0.5},
    {"variant_id": "treatment", "name": "treatment_35", "allocation": 0.5}
  ],
  "metrics": {
    "primary": "conversion_rate",
    "secondary": ["aov", "items_per_order", "revenue_per_user"],
    "guardrail": ["profit_margin", "return_rate", "review_score"]
  },
  "mde_percent": 10,
  "test_type": "chi_square"
}
```

---

## Alternatives Considered

| Approach | Pros | Cons | Why Not Chosen |
|----------|------|------|---------------|
| **Hash-based (chosen)** | Deterministic, reproducible, no state needed, stable under population changes | Requires hash computation | Best balance of properties for offline simulation |
| Random with seed | Simple to implement | Fragile — adding/removing customers changes all subsequent assignments | Not stable for evolving customer populations |
| Pre-generated lookup table | Fast lookups | Requires storage; must regenerate for new customers | Unnecessary overhead; hash computes on the fly |
| Modular hashing (CRC32) | Fast | Poor uniformity for small populations; not cryptographically uniform | SHA-256 uniformity is proven; speed difference is negligible |
| Stratified random sampling | Guarantees exact 50/50 per stratum | Complex, stateful, not deterministic without persistent storage | Overkill when hash produces near-perfect splits naturally |

---

## Stratification Analysis

While the hash function naturally produces near-50/50 splits overall, within-state balance should be verified:

- **Large states** (Sao Paulo, Rio de Janeiro with 10,000+ customers): Splits will be very close to 50/50 by the law of large numbers.
- **Small states** (under 100 customers): Larger variance is expected. A state with 50 customers could see a 40/60 split by chance — this is statistically normal, not a bug.
- **Mitigation for analysis:** Use stratified statistical tests (stratify by state) rather than trying to force exact balance at the state level. The hash function's simplicity and determinism outweigh the minor imbalance in small strata.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Hash algorithm | SHA-256 | Cryptographic uniformity guarantee; widely available in every language |
| Hash input | `"{customer_id}\|{experiment_id}"` | Pipe separator prevents ambiguity (customer "ab" + experiment "cd" vs. customer "a" + experiment "bcd") |
| Split method | `hash_int % 2` | Simplest possible uniform binary split |
| Assignment persistence | CSV + MongoDB | CSV for batch analytics (BigQuery load); MongoDB for real-time lookups (API, Kafka consumers) |
| Stratification | Post-hoc analysis only | Hash produces naturally balanced splits; forced stratification adds complexity without meaningful benefit at n=99K |
| Multi-variant support | Binary only (control/treatment) | All 10 experiments are two-variant. Extending to N variants requires `hash_int % N`. |

---

## Production Considerations

- **Scaling to millions of customers:** The hash computation is O(1) per customer-experiment pair. At 1M customers and 50 experiments, generating 50M assignments takes ~2 minutes on a single core. No algorithmic scaling concerns.
- **Real-time assignment:** In production, variants are computed on-the-fly at request time (hash the user ID + experiment ID), not pre-computed and stored. This eliminates assignment storage and enables instant experiment activation for new users. Pre-computation in this project is for batch analytics.
- **Experiment ramp-up:** To start with 10% treatment and gradually increase, modify the split threshold: `hash_int % 100 < 10` for 10% treatment. The hash remains deterministic — the same 10% of users always qualify. Expanding to 20% adds new users without reassigning existing ones.
- **Assignment logging:** Every assignment should be logged with a timestamp for auditability. If a bug in the assignment logic is discovered, the log enables identifying affected users and time windows.
- **Feature flag integration:** In production systems, experiment assignments typically integrate with feature flag services (LaunchDarkly, Unleash, Flagsmith). The hash-based approach used here mirrors how these services implement deterministic bucketing internally.

---

*Last updated: March 2026*
