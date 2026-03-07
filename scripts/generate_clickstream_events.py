#!/usr/bin/env python3
"""
Phase 2C: Generate realistic user event streams from Olist orders.

For each order, generates session_start, page_view, product_view, add_to_cart,
cart_view, checkout_start, checkout_complete events with realistic timing.

Output: NDJSON (newline-delimited JSON) for Kafka clickstream producer.

Usage:
  python scripts/generate_clickstream_events.py [--out output/events.ndjson] [--limit N] [--abandoned N] [--seed 42]
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"
OUTPUT_DIR = PROJECT_ROOT / "output"
ASSIGNMENTS_CSV = OUTPUT_DIR / "experiment_assignments.csv"
EVENTS_OUT = OUTPUT_DIR / "events.ndjson"

# Event types and funnel order
DEVICES = ["mobile", "desktop", "tablet"]
REFERRERS = ["google", "direct", "facebook", "email", "organic"]
PAGE_URLS = ["/home", "/search", "/category/electronics", "/category/home", "/category/sports", "/deals"]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
]


def parse_ts(s: str):
    if pd.isna(s):
        return None
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def ts_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_assignments_lookup(assignments_path: Path, exp_id: str = "exp_001") -> dict[str, str]:
    """(customer_id) -> variant"""
    df = pd.read_csv(assignments_path)
    df = df[df["experiment_id"] == exp_id][["customer_id", "variant"]]
    return dict(zip(df["customer_id"].astype(str), df["variant"]))


def emit_event(out, event: dict) -> None:
    out.write(json.dumps(event, default=str) + "\n")


def generate_session_events(
    order_id: str,
    customer_id: str,
    order_ts: datetime,
    items: list[dict],
    payment_type: str,
    variant: str,
    exp_id: str,
    rng: random.Random,
    out,
) -> int:
    """
    Generate full funnel events for one order. Returns event count.
    """
    n_events = 0
    session_id = str(uuid.uuid4())
    device = rng.choice(DEVICES)
    referrer = rng.choice(REFERRERS)

    # Session start: 10-60 min before order
    session_start_min = rng.randint(10, 60)
    session_start = order_ts - timedelta(minutes=session_start_min)

    # 1. session_start
    emit_event(out, {
        "event_id": str(uuid.uuid4()),
        "event_type": "session_start",
        "timestamp": ts_iso(session_start),
        "session_id": session_id,
        "customer_id": customer_id,
        "experiment_id": exp_id,
        "variant": variant,
        "properties": {"device": device, "referrer": referrer},
        "metadata": {"user_agent": rng.choice(USER_AGENTS), "ip_address": "10.0.0.1"},
    })
    n_events += 1

    # 2. page_view: 3-10 events, spread between session_start and order
    n_page_views = rng.randint(3, 10)
    for i in range(n_page_views):
        frac = (i + 1) / (n_page_views + 1)
        t = session_start + (order_ts - session_start) * frac
        emit_event(out, {
            "event_id": str(uuid.uuid4()),
            "event_type": "page_view",
            "timestamp": ts_iso(t),
            "session_id": session_id,
            "customer_id": customer_id,
            "experiment_id": exp_id,
            "variant": variant,
            "properties": {"page_url": rng.choice(PAGE_URLS), "device": device},
            "metadata": {},
        })
        n_events += 1

    # 3. product_view: 1-5 events for products from order
    n_product_views = min(rng.randint(1, 5), len(items))
    viewed_products = rng.sample(items, n_product_views)
    product_view_ts = session_start + (order_ts - session_start) * rng.uniform(0.2, 0.6)
    for p in viewed_products:
        time_spent = rng.randint(10, 120)
        emit_event(out, {
            "event_id": str(uuid.uuid4()),
            "event_type": "product_view",
            "timestamp": ts_iso(product_view_ts),
            "session_id": session_id,
            "customer_id": customer_id,
            "experiment_id": exp_id,
            "variant": variant,
            "properties": {
                "product_id": p["product_id"],
                "page_url": f"/product/{p['product_id']}",
                "time_spent_seconds": time_spent,
                "device": device,
            },
            "metadata": {},
        })
        n_events += 1
        product_view_ts += timedelta(seconds=time_spent + rng.randint(5, 30))

    # 4. add_to_cart: 1 per item
    add_to_cart_ts = product_view_ts
    cart_total = 0.0
    for item in items:
        add_to_cart_ts += timedelta(seconds=rng.randint(15, 90))
        cart_total += item["price"] * item["quantity"]
        emit_event(out, {
            "event_id": str(uuid.uuid4()),
            "event_type": "add_to_cart",
            "timestamp": ts_iso(add_to_cart_ts),
            "session_id": session_id,
            "customer_id": customer_id,
            "experiment_id": exp_id,
            "variant": variant,
            "properties": {
                "product_id": item["product_id"],
                "quantity": item["quantity"],
                "price": item["price"],
                "device": device,
            },
            "metadata": {},
        })
        n_events += 1

    # 5. cart_view: 0-3 events
    n_cart_views = rng.randint(0, 3)
    for _ in range(n_cart_views):
        add_to_cart_ts += timedelta(seconds=rng.randint(30, 120))
        emit_event(out, {
            "event_id": str(uuid.uuid4()),
            "event_type": "cart_view",
            "timestamp": ts_iso(add_to_cart_ts),
            "session_id": session_id,
            "customer_id": customer_id,
            "experiment_id": exp_id,
            "variant": variant,
            "properties": {"cart_total": round(cart_total, 2), "item_count": len(items), "device": device},
            "metadata": {},
        })
        n_events += 1

    # 6. checkout_start: 2-10 min before order
    checkout_start_min = rng.uniform(2, 10)
    checkout_start = order_ts - timedelta(minutes=checkout_start_min)
    emit_event(out, {
        "event_id": str(uuid.uuid4()),
        "event_type": "checkout_start",
        "timestamp": ts_iso(checkout_start),
        "session_id": session_id,
        "customer_id": customer_id,
        "experiment_id": exp_id,
        "variant": variant,
        "properties": {"payment_method_selected": payment_type, "device": device},
        "metadata": {},
    })
    n_events += 1

    # 7. checkout_complete
    emit_event(out, {
        "event_id": str(uuid.uuid4()),
        "event_type": "checkout_complete",
        "timestamp": ts_iso(order_ts),
        "session_id": session_id,
        "customer_id": customer_id,
        "experiment_id": exp_id,
        "variant": variant,
        "properties": {
            "order_id": order_id,
            "total": round(cart_total, 2),
            "payment_method": payment_type,
        },
        "metadata": {},
    })
    n_events += 1

    return n_events


def generate_abandoned_session(
    customer_id: str,
    base_ts: datetime,
    items_sample: list[dict],
    variant: str,
    exp_id: str,
    rng: random.Random,
    out,
) -> int:
    """Generate partial funnel for abandoned session (no checkout_complete)."""
    n_events = 0
    session_id = str(uuid.uuid4())
    device = rng.choice(DEVICES)
    session_start = base_ts - timedelta(minutes=rng.randint(10, 60))

    emit_event(out, {
        "event_id": str(uuid.uuid4()),
        "event_type": "session_start",
        "timestamp": ts_iso(session_start),
        "session_id": session_id,
        "customer_id": customer_id,
        "experiment_id": exp_id,
        "variant": variant,
        "properties": {"device": device, "referrer": rng.choice(REFERRERS)},
        "metadata": {},
    })
    n_events += 1

    for _ in range(rng.randint(2, 6)):
        session_start += timedelta(seconds=rng.randint(10, 180))
        emit_event(out, {
            "event_id": str(uuid.uuid4()),
            "event_type": "page_view",
            "timestamp": ts_iso(session_start),
            "session_id": session_id,
            "customer_id": customer_id,
            "experiment_id": exp_id,
            "variant": variant,
            "properties": {"page_url": rng.choice(PAGE_URLS), "device": device},
            "metadata": {},
        })
        n_events += 1

    if items_sample and rng.random() < 0.5:
        p = rng.choice(items_sample)
        emit_event(out, {
            "event_id": str(uuid.uuid4()),
            "event_type": "product_view",
            "timestamp": ts_iso(session_start + timedelta(seconds=rng.randint(30, 120))),
            "session_id": session_id,
            "customer_id": customer_id,
            "experiment_id": exp_id,
            "variant": variant,
            "properties": {"product_id": p["product_id"], "time_spent_seconds": rng.randint(10, 90)},
            "metadata": {},
        })
        n_events += 1
        if rng.random() < 0.6:
            emit_event(out, {
                "event_id": str(uuid.uuid4()),
                "event_type": "add_to_cart",
                "timestamp": ts_iso(session_start + timedelta(seconds=rng.randint(60, 300))),
                "session_id": session_id,
                "customer_id": customer_id,
                "experiment_id": exp_id,
                "variant": variant,
                "properties": {"product_id": p["product_id"], "quantity": 1, "price": p["price"]},
                "metadata": {},
            })
            n_events += 1

    return n_events


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(EVENTS_OUT))
    ap.add_argument("--limit", type=int, default=0, help="Limit orders (0=all)")
    ap.add_argument("--abandoned", type=int, default=0, help="Number of abandoned sessions to generate")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    RAW_OLIST.mkdir(parents=True, exist_ok=True)

    orders_path = RAW_OLIST / "olist_orders_dataset.csv"
    items_path = RAW_OLIST / "olist_order_items_dataset.csv"
    payments_path = RAW_OLIST / "olist_order_payments_dataset.csv"

    if not orders_path.exists() or not items_path.exists():
        print("Run download_olist.py first.")
        sys.exit(1)

    if not ASSIGNMENTS_CSV.exists():
        print(f"Run generate_experiment_assignments.py first. Missing: {ASSIGNMENTS_CSV}")
        sys.exit(1)

    print("Loading data...")
    orders = pd.read_csv(orders_path)
    orders["order_purchase_timestamp"] = orders["order_purchase_timestamp"].apply(parse_ts)
    orders = orders.dropna(subset=["order_purchase_timestamp"])
    if args.limit > 0:
        orders = orders.head(args.limit)

    items = pd.read_csv(items_path)
    items_per_order = items.groupby("order_id").apply(
        lambda g: [{"product_id": r["product_id"], "quantity": 1, "price": r["price"]} for _, r in g.iterrows()],
        include_groups=False,
    ).to_dict()

    payments = pd.read_csv(payments_path)
    payment_per_order = payments.groupby("order_id")["payment_type"].first().to_dict()

    assignments = build_assignments_lookup(ASSIGNMENTS_CSV)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    total_events = 0
    missing_assignments = 0

    print(f"Generating events for {len(orders):,} orders...")
    with open(args.out, "w", encoding="utf-8") as out:
        for _, row in orders.iterrows():
            order_id = str(row["order_id"])
            customer_id = str(row["customer_id"]).strip()
            order_ts = row["order_purchase_timestamp"]
            if pd.isna(order_ts):
                continue
            variant = assignments.get(customer_id, "control")
            if customer_id not in assignments:
                missing_assignments += 1
            order_items = items_per_order.get(order_id, [])
            payment_type = str(payment_per_order.get(order_id, "credit_card"))
            n = generate_session_events(
                order_id, customer_id, order_ts, order_items, payment_type,
                variant, "exp_001", rng, out
            )
            total_events += n
            if total_events % 100000 == 0 and total_events > 0:
                print(f"  {total_events:,} events...")

        if args.abandoned > 0:
            print(f"Generating {args.abandoned:,} abandoned sessions...")
            customers = orders["customer_id"].drop_duplicates().tolist()
            items_flat = items[["product_id", "price"]].drop_duplicates()
            items_sample = items_flat.to_dict("records")
            for i in range(args.abandoned):
                cid = rng.choice(customers)
                variant = assignments.get(str(cid), "control")
                base_ts = pd.Timestamp("2018-01-01") + timedelta(days=rng.randint(0, 365))
                total_events += generate_abandoned_session(
                    str(cid), base_ts, items_sample, variant, "exp_001", rng, out
                )
                if (i + 1) % 5000 == 0:
                    print(f"  Abandoned: {i+1:,}")

    print(f"\nDone. {total_events:,} events written to {args.out}")
    if missing_assignments:
        print(f"  (Warning: {missing_assignments} orders had customers not in assignments, used 'control')")


if __name__ == "__main__":
    main()
