"""
E-Commerce Analytics Platform — FastAPI Backend

Serves dashboard data from BigQuery (production) or mock data (demo mode).
Auto-detects BigQuery credentials; falls back to mock data when unavailable.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

from api.routers import agent, business, experiments, funnel, monitoring, realtime  # noqa: E402

app = FastAPI(
    title="E-Commerce Analytics API",
    version="1.0.0",
    description="Dashboard API for the E-Commerce Streaming Analytics Platform",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(realtime.router, prefix="/api/realtime", tags=["Real-Time Operations"])
app.include_router(experiments.router, prefix="/api/experiments", tags=["A/B Testing"])
app.include_router(funnel.router, prefix="/api/funnel", tags=["Customer Journey"])
app.include_router(business.router, prefix="/api/business", tags=["Business Performance"])
app.include_router(monitoring.router, prefix="/api/monitoring", tags=["Data Quality"])
app.include_router(agent.router, prefix="/api/agent", tags=["AI Agent"])

DASHBOARD_BUILD = ROOT / "dashboard" / "dist"
if DASHBOARD_BUILD.exists():
    app.mount("/", StaticFiles(directory=str(DASHBOARD_BUILD), html=True), name="spa")


@app.get("/api/health")
def health():
    bq_available = bool(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    return {
        "status": "ok",
        "mode": "bigquery" if bq_available else "demo",
        "project": os.environ.get("GOOGLE_CLOUD_PROJECT", ""),
    }
