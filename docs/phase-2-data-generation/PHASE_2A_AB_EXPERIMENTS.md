# A/B Experiment Design — 10 Experiments for E-Commerce Optimization

> Ten rigorously designed A/B experiments covering pricing, checkout UX, delivery, recommendations, and dynamic pricing — each with formal hypotheses, power analysis, and simulation strategies using Olist Brazilian e-commerce data.

---

## Overview

A/B testing is the gold standard for causal inference in product development. These 10 experiments simulate the kinds of decisions a real e-commerce team faces: Should we lower the free shipping threshold? Does offering more payment installments convert high-ticket buyers? Can we increase review volume by changing solicitation timing?

Each experiment follows a structured design framework: business context, formal hypotheses, variant definitions, randomization strategy, primary/secondary/guardrail metrics, statistical power calculations, and a concrete plan for simulating outcomes on the Olist dataset. The designs feed directly into the experiment assignment engine (Phase 2B) and clickstream generator (Phase 2C).

> **Why 10 experiments?** A single experiment demonstrates the concept; 10 experiments demonstrate a system. Multiple concurrent experiments test the assignment engine's ability to maintain independence, the analytics pipeline's ability to slice by experiment, and the dashboard's ability to surface actionable results across different metric types.

---

## Experiment Design Framework

Every experiment in this catalog follows a consistent structure:

| Component | Purpose |
|-----------|---------|
| **Business Context** | What problem exists and what revenue/UX opportunity it represents |
| **Hypothesis** | Formal null (H0) and alternative (H1) with directionality |
| **Variants** | Concrete control and treatment definitions with implementation logic |
| **Randomization** | Assignment unit, split ratio, stratification if needed |
| **Metrics** | Primary (decision metric), secondary (supporting), guardrail (must not degrade) |
| **Power Analysis** | Baseline rate, MDE, alpha, power, required sample size per variant |
| **Olist Simulation** | How to approximate the experiment using available historical data |

---

## Experiment 1: Free Shipping Threshold

### Business Context

- **Current state:** Free shipping on orders above $50
- **Problem:** Cart abandonment spikes for orders in the $35-50 range where shipping cost tips the decision
- **Opportunity:** Lowering the threshold to $35 could capture these marginal conversions

### Hypothesis

- **H0:** Lowering the free shipping threshold from $50 to $35 has no effect on conversion rate
- **H1:** The lower threshold increases conversion rate by more than 10% (relative lift)
- **Direction:** One-tailed (expecting improvement)

### Variants

| Variant | Free Shipping Rule | Implementation |
|---------|-------------------|----------------|
| **Control** | Free shipping when `order_total >= $50` | `freight_value = 0` for qualifying orders |
| **Treatment** | Free shipping when `order_total >= $35` | Same logic, lower threshold |

### Randomization

- **Unit:** `customer_id` (consistent experience across sessions)
- **Split:** 50/50, stratified by `customer_state`
- **Method:** Deterministic hash-based assignment (Phase 2B)

### Metrics

| Type | Metric | Definition |
|------|--------|------------|
| **Primary** | Conversion rate | Orders placed / unique sessions |
| **Secondary** | AOV, items per order, revenue per user, 7-day repeat rate | Standard e-commerce KPIs |
| **Guardrail** | Profit margin, return rate, review score | Must not degrade more than 5% |

### Power Analysis

| Parameter | Value |
|-----------|-------|
| Baseline conversion | 3.2% (historical) |
| Minimum detectable effect | 10% relative (3.2% to 3.52%) |
| Alpha | 0.05 |
| Power | 0.80 |
| Required per variant | ~2,450 |
| Estimated runtime | ~10 days at 500 orders/hour |
| Test type | Chi-square for proportions |

### Risks and Mitigations

- **Margin erosion:** More orders qualify for free shipping, increasing fulfillment cost. **Mitigation:** Guardrail metric on profit margin with automated alerting.
- **Selection bias:** Only price-sensitive customers respond. **Mitigation:** Segment analysis by customer tier (new vs. repeat, high-value vs. low-value).

### Olist Simulation Strategy

Use `orders` + `order_items` tables. Aggregate `price` and `freight_value` per order. Assign customers to variants via hash. Apply threshold logic: zero out `freight_value` for qualifying orders. Measure conversion using `order_status = 'delivered'` as the success signal.

---

## Experiment 2: Payment Installments — 6 vs. 12 Months

### Business Context

- **Current state:** Maximum 6 installments for credit card payments
- **Problem:** High-ticket purchases ($200+) abandoned when monthly payments are too large at 6 installments
- **Opportunity:** 12-installment option reduces per-month cost, potentially converting hesitant buyers

### Hypothesis

- **H0:** Offering 12 installments has no effect on conversion for orders above $200
- **H1:** 12-installment option increases high-ticket conversion by more than 15% (relative)
- **Direction:** One-tailed

### Variants

| Variant | Installment Cap | Target Segment |
|---------|----------------|----------------|
| **Control** | Maximum 6 installments | Orders > $200 with credit card payment |
| **Treatment** | Maximum 12 installments | Same segment |

### Metrics

| Type | Metric |
|------|--------|
| **Primary** | Conversion rate for orders above $200 |
| **Secondary** | AOV, payment approval rate, default/late payment rate |
| **Guardrail** | Default rate must not increase significantly; approval rate stable |

### Power Analysis

- **Baseline:** ~2% conversion for high-ticket segment
- **MDE:** 15% relative | **Alpha:** 0.05 | **Power:** 0.80
- **Required per variant:** ~3,200

### Risks and Mitigations

- **Default risk:** Longer payment terms increase credit risk. **Mitigation:** Guardrail on late payment rate; segment by credit-card type.

### Olist Simulation Strategy

Filter `order_payments` for `payment_type = 'credit_card'`. Aggregate order values from `order_items`. Cap installments at 6 (control) vs. 12 (treatment). Olist lacks default data — use review score or delivery timeliness as satisfaction proxies.

---

## Experiment 3: One-Page Checkout

### Business Context

- **Current state:** Multi-step checkout flow (address, payment, review, confirm)
- **Problem:** Each step introduces drop-off. Industry data shows 20-30% abandonment across multi-step checkouts.
- **Opportunity:** Consolidating into a single page reduces friction

### Hypothesis

- **H0:** One-page checkout has no effect on checkout completion rate
- **H1:** One-page checkout increases completion rate by more than 8% (relative)
- **Direction:** One-tailed

### Variants

| Variant | Checkout Experience | Simulation Proxy |
|---------|-------------------|-----------------|
| **Control** | Multi-step checkout | Orders with 2+ items (complex carts) |
| **Treatment** | Single-page checkout | Same segment, modeled with uplift factor |

> **Simulation note:** Olist contains only completed orders — no abandoned carts. The simulation assigns treatment and models uplift by bootstrapping a subset of "would-have-abandoned" orders as converted under treatment.

### Metrics

| Type | Metric |
|------|--------|
| **Primary** | Checkout completion rate (orders / carts initiated) |
| **Secondary** | Time to checkout, items per order |
| **Guardrail** | Payment error rate, address validation errors |

### Power Analysis

- **Baseline:** 4% completion | **MDE:** 8% relative
- **Required per variant:** ~3,500

---

## Experiment 4: Review Solicitation Timing — 3 Days vs. 7 Days

### Business Context

- **Current state:** Review requests sent 7 days after delivery
- **Problem:** Customer recall fades with time; response rates drop after day 5
- **Opportunity:** Earlier solicitation (3 days) captures fresher impressions and higher response rates

### Hypothesis

- **H0:** Soliciting reviews at 3 days post-delivery has no effect on review submission rate
- **H1:** 3-day solicitation increases review rate by more than 20% (relative)
- **Direction:** One-tailed

### Variants

| Variant | Solicitation Timing | Measurement |
|---------|-------------------|-------------|
| **Control** | 7 days post-delivery | `review_creation_date - order_delivered_customer_date ~ 7 days` |
| **Treatment** | 3 days post-delivery | Same metric, filtered for ~3-day response window |

### Metrics

| Type | Metric |
|------|--------|
| **Primary** | Review submission rate (orders with review / delivered orders) |
| **Secondary** | Average review score, score variance (earlier reviews may be more polarized) |
| **Guardrail** | Review score must not systematically decrease |

### Power Analysis

- **Baseline:** 80% review rate | **MDE:** 20% relative
- **Required per variant:** ~1,800

### Olist Simulation Strategy

Join `orders` with `order_reviews` on delivery and review dates. Bin responses into "early" (1-4 days) vs. "late" (5-10 days). Compare review rates and scores between the two natural cohorts.

---

## Experiment 5: Product Recommendations — Personalized vs. Popular

### Business Context

- **Current state:** "Most popular products" recommendations shown to all users
- **Problem:** Popular items are irrelevant for niche buyers (e.g., showing electronics to someone buying garden supplies)
- **Opportunity:** Category-aware personalization could increase recommendation relevance and add-to-cart rates

### Hypothesis

- **H0:** Personalized recommendations have no effect on add-to-cart rate
- **H1:** Personalized recommendations increase add-to-cart rate by more than 12% (relative)
- **Direction:** One-tailed

### Variants

| Variant | Recommendation Logic | Olist Proxy |
|---------|---------------------|-------------|
| **Control** | Top products by global order volume | Products in the top 20% by total orders |
| **Treatment** | Products in same category as user's purchase history | Category match between prior and current purchases |

### Metrics

| Type | Metric |
|------|--------|
| **Primary** | Add-to-cart rate (items from recommendations / impressions) |
| **Secondary** | CTR, AOV, items per order |
| **Guardrail** | Recommendation diversity score (avoid filter bubble) |

### Power Analysis

- **MDE:** 12% relative | **Required per variant:** ~2,700
- **Stratification:** By prior purchase count (new vs. repeat customers)

### Olist Simulation Strategy

Build a "popular" product list from `order_items` frequency. Build "personalized" lists from each customer's prior `product_category_name`. Tag items as from-popular or from-personalized. Olist has no impression data — use "bought from recommended category" as a proxy for add-to-cart.

---

## Experiment 6: Delivery Speed — Standard vs. Express Option

### Business Context

- **Current state:** Standard delivery only (estimated 5-15 days depending on region)
- **Problem:** Some customers are willing to pay a premium for faster delivery
- **Opportunity:** Offering an express option (3 days faster, with surcharge) could increase conversion and capture willingness to pay

### Hypothesis

- **H0:** Offering express delivery has no effect on conversion rate
- **H1:** Express delivery option increases conversion by more than 5% (relative)
- **Direction:** One-tailed

### Variants

| Variant | Delivery Options | Simulation |
|---------|-----------------|------------|
| **Control** | Standard delivery only | Observed delivery times |
| **Treatment** | Standard + express (3 days faster, $5 surcharge) | 50% of treatment orders "use express"; delivery reduced by 3 days |

### Metrics

| Type | Metric |
|------|--------|
| **Primary** | Conversion rate |
| **Secondary** | AOV, express adoption rate, delivery satisfaction (review score) |
| **Guardrail** | On-time delivery rate for standard must not degrade |

### Power Analysis

- **Baseline:** 5% | **MDE:** 5% relative
- **Required per variant:** ~6,200

### Olist Simulation Strategy

Use `orders` with delivery timestamps. Treatment group: reduce delivery days by 3 for a subset; add express fee to order total. Compare conversion and AOV.

---

## Experiment 7: Seller Rating Visibility — Show vs. Hide

### Business Context

- **Current state:** Seller ratings displayed prominently on product pages
- **Problem:** Low-rated sellers (below 4.0) see reduced conversion even when their products are competitive
- **Opportunity:** Testing whether hiding ratings for low-rated sellers increases sales without degrading post-purchase satisfaction

### Hypothesis

- **H0:** Hiding seller ratings has no effect on conversion for low-rated sellers
- **H1:** Hiding ratings increases conversion for sellers with rating below 4.0 by more than 10%
- **Direction:** One-tailed

### Variants

| Variant | Rating Display | Target Segment |
|---------|---------------|----------------|
| **Control** | Seller rating visible | Orders from sellers with avg rating < 4.0 |
| **Treatment** | Seller rating hidden | Same seller segment |

### Metrics

| Type | Metric |
|------|--------|
| **Primary** | Conversion rate for low-rated seller products |
| **Secondary** | Post-purchase review score, return rate |
| **Guardrail** | Post-purchase satisfaction must not decrease (hiding bad ratings should not increase buyer regret) |

### Power Analysis

- **MDE:** 10% relative | **Required per variant:** ~2,500

### Olist Simulation Strategy

Compute seller ratings from `order_reviews` (via `order_items` join). Filter to orders from sellers with avg rating below 4.0. Assign control/treatment. Olist has no page-view data, so conversion is measured as order completion within the segment.

---

## Experiment 8: Bundle Discount — Buy 3 for 10% Off

### Business Context

- **Current state:** No volume discounts
- **Problem:** Median cart size is 1-2 items
- **Opportunity:** A bundle incentive ("Buy 3+ items, get 10% off") could increase items per order and overall basket value

### Hypothesis

- **H0:** Bundle discount has no effect on items per order
- **H1:** Bundle discount increases items per order by more than 15% (relative)
- **Direction:** One-tailed

### Variants

| Variant | Pricing Rule | Implementation |
|---------|-------------|----------------|
| **Control** | Standard pricing, no volume discount | Actual `order_items` counts and prices |
| **Treatment** | 10% discount when cart has 3+ items | Apply 10% reduction to `order_total` when item count >= 3 |

### Metrics

| Type | Metric |
|------|--------|
| **Primary** | Items per order (continuous metric) |
| **Secondary** | AOV, conversion rate, gross margin |
| **Guardrail** | Margin impact (discount cost must not exceed revenue lift) |

### Power Analysis

- **MDE:** 15% relative on items/order | **Test type:** t-test (continuous metric)
- **Required per variant:** ~350 (smaller sample due to continuous metric with lower variance)

### Olist Simulation Strategy

Aggregate `order_items` per order. Control uses observed counts/prices. Treatment applies 10% discount to 3+ item orders. For 2-item carts, bootstrap an uplift factor simulating customers who "would have added a third item."

---

## Experiment 9: Guest Checkout vs. Forced Registration

### Business Context

- **Current state:** Account creation required before checkout
- **Problem:** Registration friction causes abandonment, especially for first-time buyers
- **Opportunity:** Guest checkout removes friction but trades off future re-engagement (no account = harder to retarget)

### Hypothesis

- **H0:** Guest checkout has no effect on first-time buyer conversion
- **H1:** Guest checkout increases first-time conversion by more than 15% (relative)
- **Direction:** One-tailed

### Variants

| Variant | Checkout Requirement | Olist Proxy |
|---------|---------------------|-------------|
| **Control** | Mandatory registration | First-time buyers (`customer_id` appears once in orders) |
| **Treatment** | Guest checkout available | Same segment with simulated uplift |

> **Simulation note:** Olist has no guest checkout flag or abandoned cart data. The simulation uses first-order customers as a baseline and models treatment uplift by assuming 15% of "would-have-abandoned" first-time visitors convert under guest checkout.

### Metrics

| Type | Metric |
|------|--------|
| **Primary** | First-time buyer conversion rate |
| **Secondary** | Registration rate (how many guests later create accounts), 30-day LTV |
| **Guardrail** | Guest-converted user LTV must be acceptable vs. registered user LTV |

### Power Analysis

- **Baseline:** 6% | **MDE:** 15% relative
- **Required per variant:** ~1,900

---

## Experiment 10: Dynamic Pricing — Demand-Based Adjustments

### Business Context

- **Current state:** Static pricing across all time periods
- **Problem:** Revenue is left on the table during high-demand periods when willingness to pay is elevated
- **Opportunity:** A modest price increase (+3%) during peak demand windows could increase revenue without meaningfully reducing volume

### Hypothesis

- **H0:** Dynamic pricing has no effect on revenue per order
- **H1:** Dynamic pricing increases revenue per order by more than 5% with less than 3% volume drop
- **Direction:** One-tailed for revenue; two-tailed guardrail for volume

### Variants

| Variant | Pricing Strategy | Implementation |
|---------|-----------------|----------------|
| **Control** | Static pricing | Actual prices from `order_items` |
| **Treatment** | +3% during peak demand windows | Identify peak periods (weekends, high-volume hours); add 3% to treatment prices |

### Metrics

| Type | Metric |
|------|--------|
| **Primary** | Revenue per order |
| **Secondary** | Conversion rate, order volume |
| **Guardrail** | Volume drop must stay below 3% |

### Power Analysis

- **MDE:** 5% relative on revenue | **Test type:** t-test (continuous metric)
- **Required per variant:** ~1,200
- **Stratification:** By demand window (peak vs. off-peak)

### Olist Simulation Strategy

Define peak periods from `orders.order_purchase_timestamp` (weekends, top 20% of hourly volume). Apply +3% price adjustment to treatment orders during peak. Measure revenue per order and volume changes.

---

## Experiment Catalog Summary

| # | Experiment | Primary Metric | MDE | Test Type | Sample/Variant |
|---|-----------|---------------|-----|-----------|----------------|
| 1 | Free Shipping Threshold | Conversion rate | 10% rel | Chi-square | ~2,450 |
| 2 | Payment Installments (6 vs 12) | Conversion (orders > $200) | 15% rel | Chi-square | ~3,200 |
| 3 | One-Page Checkout | Checkout completion | 8% rel | Chi-square | ~3,500 |
| 4 | Review Solicitation (3d vs 7d) | Review submission rate | 20% rel | Chi-square | ~1,800 |
| 5 | Product Recommendations | Add-to-cart rate | 12% rel | Chi-square | ~2,700 |
| 6 | Delivery Speed Options | Conversion rate | 5% rel | Chi-square | ~6,200 |
| 7 | Seller Rating Visibility | Conversion (low-rated sellers) | 10% rel | Chi-square | ~2,500 |
| 8 | Bundle Discount (3 for 10%) | Items per order | 15% rel | t-test | ~350 |
| 9 | Guest Checkout | First-time conversion | 15% rel | Chi-square | ~1,900 |
| 10 | Dynamic Pricing | Revenue per order | 5% rel | t-test | ~1,200 |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Number of experiments | 10 concurrent | Demonstrates multi-experiment management, assignment independence, and cross-experiment analysis |
| Assignment unit | `customer_id` for most; `order_id` for checkout/pricing | Customer-level ensures consistent experience; order-level needed when the intervention happens per-transaction |
| Split ratio | 50/50 for all | Maximizes statistical power; no need for conservative allocation (simulation, not production traffic) |
| Statistical framework | Frequentist (Chi-square, t-test) | Industry standard for A/B testing; interpretable p-values and confidence intervals |
| MDE range | 5-20% relative | Realistic for e-commerce interventions; smaller effects need larger samples |
| Guardrail metrics | Included for all experiments | Prevents shipping a "winning" variant that improves the primary metric but degrades something else (e.g., margin, satisfaction) |

---

## Olist Dataset Reference

| Table | Key Columns Used |
|-------|-----------------|
| `orders` | order_id, customer_id, order_status, order_purchase_timestamp, order_delivered_customer_date |
| `order_items` | order_id, product_id, seller_id, price, freight_value |
| `order_payments` | order_id, payment_type, payment_installments, payment_value |
| `order_reviews` | order_id, review_id, review_score, review_creation_date |
| `products` | product_id, product_category_name |
| `customers` | customer_id, customer_state |

---

## Production Considerations

- **Interaction effects:** With 10 concurrent experiments, interaction effects are possible (e.g., free shipping + bundle discount may compound). The hash-based assignment ensures independence at the assignment level, but metric interactions require post-hoc analysis. In production, use an experimentation platform (e.g., Optimizely, Eppo) that models interactions.
- **Multiple testing correction:** Running 10 experiments with alpha=0.05 each means ~40% chance of at least one false positive. Apply Bonferroni correction (alpha/10 = 0.005) or control false discovery rate with Benjamini-Hochberg when interpreting results across experiments.
- **Sample ratio mismatch (SRM):** If observed split deviates significantly from 50/50 (Chi-square test on assignment counts), the experiment is invalid. The validation framework (Phase 2F) checks for SRM.
- **Novelty and primacy effects:** Users initially react strongly to changes (novelty) or resist them (primacy). In production, exclude the first 7 days of data to mitigate these effects.
- **Simpson's paradox:** Aggregate results can mask segment-level differences. Always check results by key segments (geography, device, customer tenure).

---

*Last updated: March 2026*
