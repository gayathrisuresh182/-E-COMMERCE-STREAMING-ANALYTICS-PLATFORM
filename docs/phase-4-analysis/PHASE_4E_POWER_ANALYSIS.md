# Power Analysis — Sample Size Planning and the Cost of Underpowered Tests

> Power analysis determines how many users an experiment needs to detect a meaningful effect. Without it, you're flying blind — a non-significant result could mean the treatment doesn't work, or it could mean you didn't run the test long enough to find out.

---

## Overview

Statistical power is the probability of detecting a real effect when one exists (1 - beta, where beta is the Type II error rate). An experiment with 50% power is a coin flip — half the time, a genuinely effective treatment will be declared "not significant" and killed.

The business consequence of underpowered tests is asymmetric and severe. A false negative doesn't just waste the experiment's runtime — it kills a potentially valuable feature and consumes the organizational willpower needed to re-test it. Teams rarely re-run "failed" experiments.

This module provides prospective sample size calculation (before an experiment), retrospective power auditing (after), and sensitivity analysis (what's the smallest effect we could have detected?).

---

## Why Power Analysis Matters for Business Decisions

### The Cost of Getting It Wrong

| Error | Statistical Term | Business Consequence |
|-------|-----------------|---------------------|
| Ship a feature that doesn't work | Type I (false positive) | Wasted engineering effort; possible harm to metrics |
| Kill a feature that works | Type II (false negative) | **Lost revenue forever**; team loses trust in experimentation |

> **The asymmetry:** False positives are eventually caught — metrics don't improve and the feature gets reverted. False negatives are invisible — the feature is killed, and no one knows revenue was left on the table.

### Real-World Examples

- **Booking.com** found that most "failed" experiments from their early, underpowered tests actually had positive effects when re-run with adequate sample sizes. They now require mandatory power analysis before any experiment launches.
- **Airbnb** discovered that their experimentation platform was systematically killing features that needed 2x the traffic to reach significance. They rebuilt their sample size calculator and added minimum runtime requirements.
- A common e-commerce failure: testing a checkout change with 1,000 users per variant when the MDE requires 15,000. The test comes back non-significant, the PM says "checkout doesn't matter," and a real improvement is abandoned.

---

## Key Concepts

| Term | Definition | Why It Matters |
|------|-----------|----------------|
| **Power (1 - beta)** | Probability of detecting a real effect | Target 80% minimum; 90% for high-stakes tests |
| **Type II error (beta)** | Probability of missing a real effect | 20% at 80% power — 1 in 5 real effects are missed |
| **MDE** | Minimum Detectable Effect — smallest lift worth detecting | Drives sample size; smaller MDE = exponentially more users |
| **Alpha** | Type I error rate (false positive threshold) | Usually 0.05; lowering it increases required sample size |
| **Baseline rate** | Control group's metric value | Lower baselines require larger samples for the same relative MDE |
| **Cohen's h** | Effect size for proportion comparisons | Standardized measure; enables comparison across experiments |

---

## The Power-Sample Size-MDE Triangle

Power, sample size, and MDE are locked in a three-way relationship. Fix any two and the third is determined:

| You Want | You Need | Trade-off |
|----------|----------|-----------|
| Detect smaller effects (lower MDE) | More users | Longer runtime, higher cost |
| Higher power (more certainty) | More users | Longer runtime, higher cost |
| Fewer users (faster test) | Accept larger MDE or lower power | Risk missing small but real effects |

### The Quadrupling Rule

> **Halving the MDE roughly quadruples the required sample size.** This is the most important rule of thumb in experiment design.

For a 12% baseline conversion rate:

| Relative MDE | Absolute MDE | Required n per variant | Runtime at 500/day |
|--------------|-------------|----------------------|-------------------|
| 20% | 2.40 pp | ~3,200 | ~6 days |
| 10% | 1.20 pp | ~12,000 | ~24 days |
| 5% | 0.60 pp | ~47,000 | ~94 days |
| 2% | 0.24 pp | ~295,000 | ~590 days |

Detecting a 2% relative lift on a 12% baseline requires nearly 300K users per variant. This is why most e-commerce experiments target 5-15% MDE — smaller effects are real but impractical to detect.

---

## Prospective Planning (Before the Experiment)

### Sample Size Calculator

```python
from ecommerce_analytics.analysis import sample_size_calculator

result = sample_size_calculator(
    baseline_rate=0.12,
    mde_relative=0.10,     # 10% relative lift
    alpha=0.05,
    power=0.80,
    daily_traffic=500,
)

# result["n_per_variant"]  = 11,999
# result["n_total"]        = 23,998
# result["runtime_days"]   = 48.0
```

### For Continuous Metrics

```python
from ecommerce_analytics.analysis import sample_size_continuous

result = sample_size_continuous(
    baseline_mean=140.0,
    baseline_std=85.0,
    mde_relative=0.05,    # 5% lift in AOV
    alpha=0.05,
    power=0.80,
)
```

---

## Retrospective Auditing (After the Experiment)

Retrospective power answers: **"Given the sample we collected and the effect we observed, what was our probability of detecting it?"**

| Achieved Power | Interpretation |
|----------------|----------------|
| > 90% | Highly trustworthy result (significant or not) |
| 80-90% | Adequate; meets industry standard |
| 50-80% | Borderline; non-significant results are inconclusive |
| < 50% | **Underpowered** — a non-significant result tells you nothing |

> **Critical:** A non-significant result from an underpowered test (< 80% power) should never be interpreted as "the treatment doesn't work." The correct conclusion is "we don't know." This distinction is routinely ignored in industry.

### Sensitivity Analysis

Given the sample size collected, what's the smallest effect the test could have detected at 80% power?

```python
from ecommerce_analytics.analysis import sensitivity_analysis

result = sensitivity_analysis(
    n_per_variant=49_782,
    baseline_rate=0.12,
    alpha=0.05,
)
# At 80% power, minimum detectable relative lift = 4.9%
```

This is useful for post-hoc interpretation: if the test was non-significant and the MDE sensitivity is 5%, then effects smaller than 5% relative lift cannot be ruled out.

---

## Verified Results — All 10 Experiments

| Experiment | Baseline | MDE | Required n | Actual n | Achieved Power | Adequate |
|------------|----------|-----|-----------|---------|----------------|----------|
| Free Shipping Threshold | 12.0% | 10% | 11,999 | 49,782 | 100% | Yes |
| Payment Installments | 8.0% | 8% | 8,559 | 49,741 | 100% | Yes |
| One-Page Checkout | 15.0% | 10% | 14,349 | 49,734 | 100% | Yes |
| Review Solicitation | 22.0% | 15% | 1,485 | 49,844 | 100% | Yes |
| Product Recommendations | 10.0% | 10% | 10,323 | 49,656 | 100% | Yes |
| Delivery Speed Options | 14.0% | 5% | 39,370 | 49,746 | 100% | Yes |
| Seller Ratings Display | 11.0% | 8% | 13,247 | 49,713 | 100% | Yes |
| Bundle Discounts | 25.0% | 15% | 2,192 | 49,671 | 100% | Yes |
| Guest Checkout | 9.0% | 10% | 7,519 | 49,720 | 100% | Yes |
| Dynamic Pricing | 13.0% | 5% | **42,906** | 49,833 | **72%** | **No** |

### Audit Summary

- **9 of 10 experiments** are adequately powered at the 80% threshold
- **8 experiments** are substantially over-powered (could have run with fewer users)
- **Dynamic Pricing** is borderline at 72% power — a 5% MDE on a 13% baseline requires ~43K users per variant, and the actual sample barely exceeds this

> **Dynamic Pricing finding:** This experiment detected a +7.5% lift (p = 0.013) despite being slightly underpowered. The result is real but less certain than the other experiments. If this had come back non-significant, the correct action would be to extend the test, not abandon the feature.

### Over-Powering Analysis

Most experiments are over-powered by 3-30x. This means they could have been stopped earlier, freeing traffic for other experiments. In a production setting with limited traffic, this represents an opportunity cost.

| Experiment | Over-sampling Factor | Could Have Stopped At |
|------------|---------------------|-----------------------|
| Review Solicitation | 33.5x | ~1,500 users/variant |
| Bundle Discounts | 22.7x | ~2,200 users/variant |
| Guest Checkout | 6.6x | ~7,500 users/variant |
| Free Shipping Threshold | 4.1x | ~12,000 users/variant |

---

## Design Rules of Thumb

| Rule | Detail |
|------|--------|
| Target 80% power minimum | Industry standard; balances sample size with detection ability |
| Use 90% for high-stakes tests | Revenue-critical experiments, pricing changes, checkout flows |
| Halving MDE quadruples sample | The most impactful planning parameter |
| Lower baselines need larger samples | A 3% baseline needs 4x the sample of a 12% baseline for the same relative MDE |
| Over-powering wastes traffic | >99% power means you could have tested something else with the extra users |
| Underpowered non-significant is NOT "no effect" | It means "we don't know" — a critical distinction |
| Plan for the MDE you care about | Don't set MDE to match what you hope the effect is; set it to the smallest effect worth shipping |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| 80% power target | Industry standard | 90% is better but often impractical with limited traffic |
| Two-tailed tests | No directional assumption | One-tailed increases power but masks potential harm |
| Prospective + retrospective | Both required | Prospective plans the test; retrospective validates conclusions |
| Sensitivity analysis included | Standard practice | Tells stakeholders the detection limits of the test |
| Audit all experiments | Portfolio-level review | Catches underpowered tests before results are interpreted |

---

## Production Considerations

- **Traffic allocation:** In a production system with limited traffic, power analysis drives experiment prioritization. High-MDE experiments (needing fewer users) can run concurrently; low-MDE experiments may need exclusive traffic allocation.
- **Sequential testing:** For experiments where early stopping is valuable, combine power analysis with group sequential methods (O'Brien-Fleming boundaries) or Bayesian monitoring to stop early without inflating error.
- **Runtime estimation:** Power analysis combined with daily traffic estimates produces a runtime forecast. If the estimated runtime exceeds the business window (e.g., seasonal promotion), either increase MDE or delay the experiment.
- **Cost modeling:** Each user in an experiment has an opportunity cost — they could be in a different experiment. Power analysis quantifies the minimum investment needed for each test.
- **Re-test policy:** If an underpowered experiment returns non-significant results, the recommendation should be "extend or re-run" rather than "abandon." Document this policy before analysis begins.

---

*Last updated: March 2026*
