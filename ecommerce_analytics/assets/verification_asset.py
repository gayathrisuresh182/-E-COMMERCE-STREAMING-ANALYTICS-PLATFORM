"""
Phase 1 Verification: Dagster asset that writes to MongoDB and BigQuery.
Materialize this asset to confirm Dagster can use both resources.
"""
from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

from dagster import asset


def _load_env() -> None:
    """Load project .env so GCP/MongoDB credentials are available when Dagster runs the asset."""
    try:
        from dotenv import load_dotenv
        root = Path(__file__).resolve().parent.parent.parent
        load_dotenv(root / ".env")
        load_dotenv(root / "docs" / ".env")
    except Exception:
        pass


@asset(
    description="Phase 1 verification: write one doc to MongoDB and one row to BigQuery.",
    required_resource_keys={"mongodb", "bigquery"},
)
def phase1_verification_asset(context) -> dict:
    """Uses mongodb and bigquery resources; verifies Dagster can talk to both."""
    _load_env()
    mongodb = context.resources.mongodb
    bigquery_config = context.resources.bigquery
    order_id = "dagster_verify_" + uuid.uuid4().hex[:8]

    # MongoDB
    import pymongo
    client = pymongo.MongoClient(mongodb["connection_string"])
    coll = client[mongodb["database"]]["orders"]
    coll.insert_one({
        "order_id": order_id,
        "customer_id": "dagster_verify",
        "order_total": 1.0,
        "source": "phase1_dagster_asset",
    })
    client.close()

    # BigQuery (streaming insert); use env project if config has placeholder/empty
    project = (bigquery_config.get("project") or "").strip()
    if not project or project == "your-gcp-project":
        project = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
    if project:
        from google.cloud import bigquery
        bq = bigquery.Client(project=project)
        table_id = f"{project}.{bigquery_config.get('dataset_realtime', 'realtime')}.realtime_orders"
        bq.insert_rows_json(table_id, [{
            "event_id": str(uuid.uuid4()),
            "order_id": order_id,
            "customer_id": "dagster_verify",
            "event_timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            "order_total": 1.0,
            "event_type": "phase1_dagster_verify",
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        }])
        bq.close()

    return {"order_id": order_id, "mongodb": True, "bigquery": bool(project)}
