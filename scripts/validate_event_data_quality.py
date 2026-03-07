#!/usr/bin/env python3
"""
Phase 2F: Validate generated event data quality.

Validates events.ndjson (Phase 2C) and experiment_assignments before streaming to Kafka.

Usage:
  python scripts/validate_event_data_quality.py [--events output/events.ndjson] [--out output/validation_report.md]
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"
OUTPUT_DIR = PROJECT_ROOT / "output"
EVENTS_PATH = OUTPUT_DIR / "events.ndjson"
ASSIGNMENTS_CSV = OUTPUT_DIR / "experiment_assignments.csv"

REQUIRED_FIELDS = {"event_id", "event_type", "timestamp", "customer_id"}
EVENT_TYPES = {"session_start", "page_view", "product_view", "add_to_cart", "cart_view", "checkout_start", "checkout_complete"}
VALID_VARIANTS = {"control", "treatment"}


def load_events(path: Path) -> list[dict]:
    """Load events from NDJSON."""
    events = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def parse_ts(s: str):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def run_validation(events_path: Path, assignments_path: Path, report_path: Path | None, allow_partial: bool = False) -> dict:
    """Run all validation checks. Returns results dict."""
    results = {
        "checks": {},
        "stats": {},
        "issues": [],
        "passed": 0,
        "failed": 0,
    }

    print("Loading data...")
    events = load_events(events_path)
    orders_df = pd.read_csv(RAW_OLIST / "olist_orders_dataset.csv")
    customers_df = pd.read_csv(RAW_OLIST / "olist_customers_dataset.csv")
    products_df = pd.read_csv(RAW_OLIST / "olist_products_dataset.csv")
    items_df = pd.read_csv(RAW_OLIST / "olist_order_items_dataset.csv")
    assignments_df = pd.read_csv(assignments_path)

    orders_set = set(orders_df["order_id"].astype(str))
    customers_set = set(customers_df["customer_id"].astype(str))
    products_set = set(products_df["product_id"].astype(str))
    order_items_map = {}
    for oid, grp in items_df.groupby("order_id"):
        order_items_map[str(oid)] = grp[["product_id", "price"]].to_dict("records")

    # Build assignment lookup
    assign_exp = assignments_df[assignments_df["experiment_id"] == "exp_001"][["customer_id", "variant"]]
    assignments_lookup = dict(zip(assign_exp["customer_id"].astype(str), assign_exp["variant"].astype(str)))

    now = datetime.now(timezone.utc)
    order_ids_in_events = set()
    customer_ids_in_events = set()
    product_ids_in_events = set()
    event_counts_by_type = defaultdict(int)
    events_per_session = defaultdict(list)
    session_durations = []
    timestamps_all = []
    funnel_counts = defaultdict(int)
    schema_issues = []
    order_totals_check = []

    for ev in events:
        et = ev.get("event_type", "")
        event_counts_by_type[et] += 1
        customer_ids_in_events.add(str(ev.get("customer_id", "")))
        sid = ev.get("session_id", "")
        if sid:
            events_per_session[sid].append(ev)

        if et == "checkout_complete":
            oid = ev.get("properties", {}).get("order_id")
            if oid:
                order_ids_in_events.add(str(oid))
            total = ev.get("properties", {}).get("total")
            if total is not None and oid:
                items_list = order_items_map.get(str(oid), [])
                expected = sum(r.get("price", 0) for r in items_list)
                order_totals_check.append((oid, total, round(expected, 2)))

        if et == "add_to_cart":
            pid = ev.get("properties", {}).get("product_id")
            if pid:
                product_ids_in_events.add(str(pid))
        if et == "product_view":
            pid = ev.get("properties", {}).get("product_id")
            if pid:
                product_ids_in_events.add(str(pid))

        ts = parse_ts(ev.get("timestamp", ""))
        if ts:
            timestamps_all.append(ts)
            if ts > now:
                results["issues"].append(f"Future timestamp: event_id={ev.get('event_id')} ts={ev.get('timestamp')}")

        for f in REQUIRED_FIELDS:
            if f not in ev or ev[f] is None:
                schema_issues.append(f"Missing {f}: event_id={ev.get('event_id')}")
        if et and et not in EVENT_TYPES:
            schema_issues.append(f"Invalid event_type '{et}': event_id={ev.get('event_id')}")

        variant = ev.get("variant", "")
        if variant and variant not in VALID_VARIANTS:
            schema_issues.append(f"Invalid variant '{variant}': event_id={ev.get('event_id')}")

    # Session-level checks
    for sid, evs in events_per_session.items():
        funnel_counts[len(evs)] += 1
        evs_sorted = sorted(evs, key=lambda e: parse_ts(e.get("timestamp", "")) or datetime.min.replace(tzinfo=timezone.utc))
        ts_first = parse_ts(evs_sorted[0].get("timestamp", ""))
        ts_last = parse_ts(evs_sorted[-1].get("timestamp", ""))
        if ts_first and ts_last:
            dur_min = (ts_last - ts_first).total_seconds() / 60
            session_durations.append(dur_min)

    # --- CHECK 1: Completeness ---
    missing_orders = orders_set - order_ids_in_events
    results["checks"]["all_orders_have_events"] = len(missing_orders) == 0 or allow_partial
    if missing_orders and not allow_partial:
        results["issues"].append(f"Missing events for {len(missing_orders)} orders (sample: {list(missing_orders)[:5]})")
        results["failed"] += 1
    else:
        results["passed"] += 1

    orphan_customers = customer_ids_in_events - customers_set
    results["checks"]["all_customers_in_olist"] = len(orphan_customers) == 0
    if orphan_customers:
        results["issues"].append(f"{len(orphan_customers)} customer_ids not in Olist (sample: {list(orphan_customers)[:5]})")
        results["failed"] += 1
    else:
        results["passed"] += 1

    orphan_products = product_ids_in_events - products_set
    results["checks"]["all_products_in_olist"] = len(orphan_products) == 0
    if orphan_products:
        results["issues"].append(f"{len(orphan_products)} product_ids not in Olist (sample: {list(orphan_products)[:5]})")
        results["failed"] += 1
    else:
        results["passed"] += 1

    customers_with_events = customer_ids_in_events & customers_set
    missing_assign = [c for c in customers_with_events if c not in assignments_lookup]
    results["checks"]["all_customers_have_assignments"] = len(missing_assign) == 0
    if missing_assign:
        results["issues"].append(f"{len(missing_assign)} customers without experiment assignment")
        results["failed"] += 1
    else:
        results["passed"] += 1

    # --- CHECK 2: Consistency (timestamps chronological per session) ---
    ts_chrono_fail = 0
    for sid, evs in events_per_session.items():
        evs_sorted = sorted(evs, key=lambda e: parse_ts(e.get("timestamp", "")) or datetime.min.replace(tzinfo=timezone.utc))
        prev = None
        for e in evs_sorted:
            t = parse_ts(e.get("timestamp", ""))
            if t and prev and t < prev:
                ts_chrono_fail += 1
                break
            prev = t
    results["checks"]["timestamps_chronological"] = ts_chrono_fail == 0
    if ts_chrono_fail:
        results["issues"].append(f"{ts_chrono_fail} sessions with non-chronological timestamps")
        results["failed"] += 1
    else:
        results["passed"] += 1

    # Order totals (within 0.01 tolerance)
    total_mismatch = sum(1 for oid, total, exp in order_totals_check if abs(total - exp) > 0.01)
    results["checks"]["order_totals_match"] = total_mismatch == 0
    if total_mismatch:
        results["issues"].append(f"{total_mismatch} checkout_complete totals don't match order_items sum")
        results["failed"] += 1
    else:
        results["passed"] += 1

    # --- CHECK 3: Accuracy ---
    events_per_order = len(events) / max(len(order_ids_in_events), 1)
    results["stats"]["events_per_order_avg"] = round(events_per_order, 1)
    results["checks"]["events_per_order_reasonable"] = 8 <= events_per_order <= 15
    if not results["checks"]["events_per_order_reasonable"]:
        results["issues"].append(f"Avg events/order {events_per_order:.1f} outside 8-15 range")
        results["failed"] += 1
    else:
        results["passed"] += 1

    avg_dur = sum(session_durations) / len(session_durations) if session_durations else 0
    results["stats"]["session_duration_avg_min"] = round(avg_dur, 1)
    results["checks"]["session_duration_realistic"] = 5 <= avg_dur <= 90
    if session_durations and not results["checks"]["session_duration_realistic"]:
        results["issues"].append(f"Avg session duration {avg_dur:.1f} min outside 5-60 range")
        results["failed"] += 1
    else:
        results["passed"] += 1

    future_ts = sum(1 for t in timestamps_all if t > now)
    results["checks"]["no_future_timestamps"] = future_ts == 0
    if future_ts:
        results["issues"].append(f"{future_ts} events with future timestamps")
        results["failed"] += 1
    else:
        results["passed"] += 1

    # --- CHECK 4: Schema ---
    results["checks"]["schema_valid"] = len(schema_issues) == 0
    if schema_issues:
        results["issues"].extend(schema_issues[:20])
        if len(schema_issues) > 20:
            results["issues"].append(f"... and {len(schema_issues)-20} more schema issues")
        results["failed"] += 1
    else:
        results["passed"] += 1

    # --- CHECK 5: Statistical ---
    results["stats"]["event_distribution"] = dict(event_counts_by_type)
    pv = event_counts_by_type.get("page_view", 0)
    cc = event_counts_by_type.get("checkout_complete", 0)
    results["checks"]["funnel_makes_sense"] = pv >= cc and event_counts_by_type.get("session_start", 0) >= cc
    if not results["checks"]["funnel_makes_sense"]:
        results["failed"] += 1
    else:
        results["passed"] += 1

    # --- CHECK 6: Experiment split ---
    exp_assign = assignments_df[assignments_df["experiment_id"] == "exp_001"]
    split = exp_assign.groupby("variant").size()
    control_n = split.get("control", 0)
    treatment_n = split.get("treatment", 0)
    total_a = control_n + treatment_n
    control_pct = (control_n / total_a * 100) if total_a else 0
    treatment_pct = (treatment_n / total_a * 100) if total_a else 0
    results["stats"]["experiment_split"] = {"control": control_n, "treatment": treatment_n, "control_pct": control_pct, "treatment_pct": treatment_pct}
    results["checks"]["experiment_50_50_split"] = 49 <= control_pct <= 51 and 49 <= treatment_pct <= 51
    if not results["checks"]["experiment_50_50_split"]:
        results["issues"].append(f"Split not 50/50: control={control_pct:.1f}% treatment={treatment_pct:.1f}%")
        results["failed"] += 1
    else:
        results["passed"] += 1

    # No customer in both control and treatment
    cust_variants = assignments_df[assignments_df["experiment_id"] == "exp_001"].groupby("customer_id")["variant"].nunique()
    both = (cust_variants > 1).sum()
    results["checks"]["no_customer_in_both_variants"] = both == 0
    if both:
        results["issues"].append(f"{both} customers in both control and treatment for same experiment")
        results["failed"] += 1
    else:
        results["passed"] += 1

    results["stats"]["total_events"] = len(events)
    results["stats"]["total_orders"] = len(orders_set)
    results["stats"]["orders_with_events"] = len(order_ids_in_events)
    results["stats"]["sessions"] = len(events_per_session)

    return results


def write_report(results: dict, path: Path) -> None:
    """Write Markdown validation report."""
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Phase 2F: Event Data Quality Validation Report\n\n")
        f.write(f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n")
        f.write("## Summary\n\n")
        total = results["passed"] + results["failed"]
        f.write(f"- **Passed:** {results['passed']}/{total} checks\n")
        f.write(f"- **Failed:** {results['failed']}/{total} checks\n")
        f.write(f"- **Status:** {'PASS' if results['failed'] == 0 else 'FAIL'}\n\n")

        f.write("## Statistics\n\n")
        for k, v in results["stats"].items():
            if isinstance(v, dict):
                f.write(f"- **{k}:**\n")
                for k2, v2 in v.items():
                    f.write(f"  - {k2}: {v2}\n")
            else:
                f.write(f"- **{k}:** {v}\n")
        f.write("\n")

        f.write("## Check Results\n\n")
        f.write("| Check | Result |\n|-------|--------|\n")
        for check, passed in results["checks"].items():
            status = "PASS" if passed else "FAIL"
            f.write(f"| {check} | {status} |\n")
        f.write("\n")

        if results["issues"]:
            f.write("## Issues\n\n")
            for issue in results["issues"][:50]:
                f.write(f"- {issue}\n")
            if len(results["issues"]) > 50:
                f.write(f"\n... and {len(results['issues'])-50} more.\n")
        f.write("\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", default=str(EVENTS_PATH))
    ap.add_argument("--assignments", default=str(ASSIGNMENTS_CSV))
    ap.add_argument("--out", default=str(OUTPUT_DIR / "validation_report.md"))
    ap.add_argument("--allow-partial", action="store_true", help="Pass even if not all orders have events (e.g. --limit run)")
    args = ap.parse_args()

    events_path = Path(args.events)
    assignments_path = Path(args.assignments)
    report_path = Path(args.out)

    if not events_path.exists():
        print(f"Missing events file: {events_path}")
        print("Run: python scripts/generate_clickstream_events.py")
        raise SystemExit(1)
    if not assignments_path.exists():
        print(f"Missing assignments: {assignments_path}")
        print("Run: python scripts/generate_experiment_assignments.py")
        raise SystemExit(1)

    results = run_validation(events_path, assignments_path, report_path, allow_partial=args.allow_partial)

    report_path.parent.mkdir(parents=True, exist_ok=True)
    write_report(results, report_path)

    print("\n=== Validation Results ===")
    print(f"Passed: {results['passed']}, Failed: {results['failed']}")
    for check, passed in results["checks"].items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"\nReport: {report_path}")

    if results["failed"] > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
