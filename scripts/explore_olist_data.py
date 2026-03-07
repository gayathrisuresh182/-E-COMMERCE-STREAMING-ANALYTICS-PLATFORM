#!/usr/bin/env python3
"""
Phase 1A: Initial data exploration for Olist CSVs.
- Load each CSV, show first 5 rows, dtypes, null %, PK/FK checks.
Usage: python scripts/explore_olist_data.py
       python scripts/explore_olist_data.py --report  # also write report to stdout/file
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"

# Table -> (primary_key, [foreign_keys])
TABLE_KEYS = {
    "olist_orders_dataset.csv": ("order_id", ["customer_id"]),
    "olist_order_items_dataset.csv": (None, ["order_id", "product_id", "seller_id"]),
    "olist_order_payments_dataset.csv": (None, ["order_id"]),
    "olist_order_reviews_dataset.csv": ("review_id", ["order_id"]),
    "olist_products_dataset.csv": ("product_id", []),
    "olist_customers_dataset.csv": ("customer_id", []),
    "olist_sellers_dataset.csv": ("seller_id", []),
    "olist_geolocation_dataset.csv": (None, ["geolocation_zip_code_prefix"]),
}

FILE_LIST = list(TABLE_KEYS.keys())


def format_size(path: Path) -> str:
    n = path.stat().st_size
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.2f} MB"


def explore_table(path: Path, pk: str | None, fks: list[str], verbose: bool = True) -> dict:
    """Load CSV, compute basic stats. Return dict for report."""
    name = path.name
    df = pd.read_csv(path)
    n_rows = len(df)
    n_cols = len(df.columns)

    # Nulls
    null_pct = (df.isnull().sum() / n_rows * 100).round(2)
    null_cols = null_pct[null_pct > 0]

    # Duplicates on PK
    dup_pk = ""
    if pk and pk in df.columns:
        dup_count = df.duplicated(subset=[pk]).sum()
        dup_pk = f"{dup_count} duplicate {pk}" if dup_count else "0 duplicates"

    out = {
        "file": name,
        "rows": n_rows,
        "size": format_size(path),
        "columns": n_cols,
        "pk": pk,
        "fks": fks,
        "null_cols": dict(null_cols),
        "dup_pk": dup_pk,
    }

    if verbose:
        print("\n" + "=" * 70)
        print(name, f"({n_rows:,} rows, {format_size(path)})")
        print("=" * 70)
        print("First 5 rows:")
        print(df.head().to_string())
        print("\nDtypes:")
        print(df.dtypes.to_string())
        print("\nNull % (non-zero only):")
        if null_cols.empty:
            print("  None")
        else:
            print(null_cols.to_string())
        print("\nKeys:", f"PK={pk}", f"FKs={fks}")
        if dup_pk:
            print("Duplicates:", dup_pk)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Explore Olist CSVs")
    ap.add_argument("--report", action="store_true", help="Print summary table only (for report)")
    ap.add_argument("--out", type=Path, default=None, help="Write report markdown to file")
    args = ap.parse_args()

    if not RAW_OLIST.exists():
        print(f"Folder not found: {RAW_OLIST}", file=sys.stderr)
        raise SystemExit(1)

    results = []
    for f in FILE_LIST:
        path = RAW_OLIST / f
        if not path.exists():
            print(f"Skip (not found): {f}", file=sys.stderr)
            continue
        pk, fks = TABLE_KEYS[f]
        r = explore_table(path, pk, fks, verbose=not args.report)
        results.append(r)

    if args.report or args.out:
        lines = [
            "# Olist Data Profile (Phase 1A)",
            "",
            "| Table | Rows | Size | PK | FKs | Null columns | Duplicates (PK) |",
            "|-------|------|------|-----|-----|--------------|------------------|",
        ]
        for r in results:
            null_str = "; ".join(f"{k}:{v}%" for k, v in list(r["null_cols"].items())[:5])
            if not null_str:
                null_str = "—"
            lines.append(
                f"| {r['file']} | {r['rows']:,} | {r['size']} | {r['pk'] or '—'} | "
                f"{', '.join(r['fks']) or '—'} | {null_str} | {r['dup_pk'] or '—'} |"
            )
        lines.extend(["", "## Key columns", ""])
        for r in results:
            lines.append(f"- **{r['file']}**: PK={r['pk']}, FKs={r['fks']}")
        lines.extend(["", "## Data quality", ""])
        for r in results:
            if r["null_cols"]:
                lines.append(f"- **{r['file']}**: nulls in {list(r['null_cols'].keys())}")
        report_text = "\n".join(lines)
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(report_text, encoding="utf-8")
            print("Wrote", args.out)
        if args.report:
            print(report_text)


if __name__ == "__main__":
    main()
