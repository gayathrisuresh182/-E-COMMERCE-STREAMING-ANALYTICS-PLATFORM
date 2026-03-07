#!/usr/bin/env python3
"""
Export experiment results to CSV for Power Query / Excel import.

Usage:
  python scripts/export_experiment_csv.py

Output:
  output/analysis/experiment_results.csv
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.excel_data import gather_experiment_data

if __name__ == "__main__":
    variant_df, summary_df = gather_experiment_data()
    out_dir = ROOT / "output" / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    variant_path = out_dir / "experiment_results.csv"
    summary_path = out_dir / "experiment_summary.csv"
    variant_df.to_csv(variant_path, index=False)
    summary_df.to_csv(summary_path, index=False)
    print(f"Exported: {variant_path}")
    print(f"Exported: {summary_path}")
