"""MongoDB loader configuration."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"
OUTPUT_DIR = PROJECT_ROOT / "output"
DOCS = PROJECT_ROOT / "docs"
DB_NAME = "ecommerce_streaming"
BATCH_SIZE = 1000


def get_mongodb_uri() -> str:
    """Load .env and return MONGODB_URI."""
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env")
        load_dotenv(DOCS / ".env")
    except Exception:
        pass
    return os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
