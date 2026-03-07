"""
E-Commerce Streaming Analytics Platform.
Asset-oriented Dagster definitions (batch + stream, Lambda architecture).
"""
from pathlib import Path

# Load .env before any config is read (for EMAIL_*, AWS_*, etc.)
try:
    from dotenv import load_dotenv
    _root = Path(__file__).resolve().parent.parent
    load_dotenv(_root / ".env")
    load_dotenv(_root / "docs" / ".env")
except ImportError:
    pass

from dagster import Definitions, FilesystemIOManager

from ecommerce_analytics.assets import core_assets, core_asset_checks
from ecommerce_analytics.jobs import core_jobs, core_schedules
from ecommerce_analytics.resources import get_resources
from ecommerce_analytics.sensors import SENSORS

definitions = Definitions(
    assets=core_assets,
    asset_checks=core_asset_checks,
    jobs=core_jobs,
    schedules=core_schedules,
    sensors=SENSORS,
    resources={
        **get_resources(),
        "io_manager": FilesystemIOManager(base_dir=".dagster/storage"),
    },
)
