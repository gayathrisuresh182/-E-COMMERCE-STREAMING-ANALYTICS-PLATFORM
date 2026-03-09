#!/usr/bin/env python3
"""
Phase 1D: Create BigQuery datasets (staging, marts, realtime, experiments).
Usage: python scripts/bigquery_create_datasets.py [--project my-gcp-project]
Requires: GOOGLE_APPLICATION_CREDENTIALS set, pip install google-cloud-bigquery
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (or docs/ if present)
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
load_dotenv(Path(__file__).resolve().parent.parent.parent / "docs" / ".env")

from google.cloud import bigquery

DATASETS = [
    ("staging", "Cleaned source data from Olist and Kafka"),
    ("marts", "Dimensional model (facts, dimensions)"),
    ("realtime", "Stream data from Kafka consumers"),
    ("experiments", "A/B test results and metadata"),
    ("monitoring", "Pipeline health, consumer lag, data quality (PHASE_5F)"),
]

DEFAULT_LOCATION = "US"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    ap.add_argument("--location", default=DEFAULT_LOCATION)
    args = ap.parse_args()

    if not args.project:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project")
        sys.exit(1)

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_APPLICATION_CREDENTIALS to service account JSON path")
        sys.exit(1)

    client = bigquery.Client(project=args.project, location=args.location)

    for dataset_id, desc in DATASETS:
        dataset_ref = f"{args.project}.{dataset_id}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = args.location
        dataset.description = desc
        try:
            client.create_dataset(dataset, exists_ok=True)
            print(f"  {dataset_id}: created or exists")
        except Exception as e:
            print(f"  {dataset_id}: {e}")
            sys.exit(1)

    print("Done.")


if __name__ == "__main__":
    main()
