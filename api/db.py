"""
Database clients for BigQuery and MongoDB.

All dashboard data flows through here — no mock data.
BigQuery: historical batch + synced stream data.
MongoDB: live real-time collections from Kafka consumers.
"""
from __future__ import annotations

import decimal
import logging
import os
from datetime import date, datetime
from functools import lru_cache

from google.cloud import bigquery
from pymongo import MongoClient

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_bq_client() -> bigquery.Client:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "ecommerce-analytics-prod")
    return bigquery.Client(project=project)


@lru_cache(maxsize=1)
def get_mongo_client() -> MongoClient:
    uri = os.environ.get("MONGODB_URI", "")
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def get_mongo_db(name: str = "ecommerce_analytics"):
    return get_mongo_client()[name]


def _serialize(val):
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, decimal.Decimal):
        return float(val)
    return val


def bq_query(sql: str) -> list[dict]:
    """Run a BigQuery SQL query and return rows as list[dict]."""
    client = get_bq_client()
    try:
        rows = client.query(sql).result()
        return [{k: _serialize(v) for k, v in dict(row).items()} for row in rows]
    except Exception as e:
        log.error("BigQuery query failed: %s\nSQL: %s", e, sql[:200])
        return []


def bq_scalar(sql: str):
    """Run a query that returns a single value."""
    rows = bq_query(sql)
    if rows:
        first_val = list(rows[0].values())
        return first_val[0] if first_val else None
    return None
