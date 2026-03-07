#!/usr/bin/env python3
"""
Phase 1A: Verify Olist download – list 8 CSVs, file sizes, row counts.
Usage: python scripts/verify_olist_download.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"

EXPECTED_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
]

EXPECTED_ROWS = {
    "olist_orders_dataset.csv": 99_441,
    "olist_order_items_dataset.csv": 112_650,
    "olist_order_payments_dataset.csv": 103_886,
    "olist_order_reviews_dataset.csv": 99_224,
    "olist_products_dataset.csv": 32_951,
    "olist_customers_dataset.csv": 99_441,
    "olist_sellers_dataset.csv": 3_095,
    "olist_geolocation_dataset.csv": 1_000_462,
}


def format_size(bytes: int) -> str:
    if bytes < 1024:
        return f"{bytes} B"
    if bytes < 1024 * 1024:
        return f"{bytes / 1024:.1f} KB"
    return f"{bytes / (1024 * 1024):.2f} MB"


def main() -> None:
    if not RAW_OLIST.exists():
        print(f"Folder not found: {RAW_OLIST}")
        print("Run: python scripts/download_olist.py")
        raise SystemExit(1)

    print("Olist dataset verification")
    print("=" * 60)
    missing = []
    for f in EXPECTED_FILES:
        path = RAW_OLIST / f
        if not path.exists():
            missing.append(f)
            continue
        size = path.stat().st_size
        try:
            n = len(pd.read_csv(path))
        except Exception as e:
            n = f"Error: {e}"
        expected = EXPECTED_ROWS.get(f, "?")
        ok = "OK" if (isinstance(n, int) and n == expected) else "check"
        print(f"  {f}")
        print(f"    Size: {format_size(size)}  Rows: {n}  (expected ~{expected})  [{ok}]")

    if missing:
        print("\nMissing files:", missing)
        raise SystemExit(1)
    print("\nAll 8 files present.")


if __name__ == "__main__":
    main()
