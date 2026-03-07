#!/usr/bin/env python3
"""
Phase 1A: Download Olist Brazilian E-Commerce dataset from Kaggle and extract to raw/olist/.
Requires: pip install kaggle
Configure: ~/.kaggle/kaggle.json (Windows: %USERPROFILE%\\.kaggle\\kaggle.json)

Usage: python scripts/download_olist.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

# Project root (parent of scripts/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"
DATASET = "olistbr/brazilian-ecommerce"
ZIP_NAME = "brazilian-ecommerce.zip"


def run_cmd(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    """Run command; raise on failure."""
    r = subprocess.run(cmd, cwd=cwd or PROJECT_ROOT, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stderr or r.stdout)
        raise SystemExit(r.returncode)
    return r


def main() -> None:
    RAW_OLIST.mkdir(parents=True, exist_ok=True)
    os.chdir(PROJECT_ROOT)

    # 1. Download via Kaggle API
    print("Downloading dataset from Kaggle...")
    try:
        run_cmd(["kaggle", "datasets", "download", "-d", DATASET])
    except FileNotFoundError:
        print("Kaggle CLI not found. Install: pip install kaggle")
        print("Then configure: https://www.kaggle.com/settings -> Create New Token -> save as ~/.kaggle/kaggle.json")
        raise SystemExit(1)

    zip_path = PROJECT_ROOT / ZIP_NAME
    if not zip_path.exists():
        raise SystemExit("Download did not create zip file.")

    # 2. Extract to raw/olist/
    print("Extracting to raw/olist/...")
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.endswith("/"):
                continue
            # Preserve filename only (some zips have a folder prefix)
            base_name = os.path.basename(name)
            if not base_name.lower().endswith(".csv"):
                continue
            target = RAW_OLIST / base_name
            with z.open(name) as src, open(target, "wb") as dst:
                dst.write(src.read())
            print(f"  {target.name}")

    # 3. Remove zip (optional)
    zip_path.unlink()
    print("Done. Files are in:", RAW_OLIST)


if __name__ == "__main__":
    main()
