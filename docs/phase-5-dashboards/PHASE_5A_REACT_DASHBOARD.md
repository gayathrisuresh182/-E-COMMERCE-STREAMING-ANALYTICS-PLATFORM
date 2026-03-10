# Phase 5A: React + FastAPI Dashboard

> Production-grade analytics dashboard replacing Looker Studio. Built with React, TypeScript, Recharts, and Tailwind CSS, served by a FastAPI backend connected to BigQuery and MongoDB.

---

## Why React Over Looker Studio

Looker Studio works for quick prototypes, but it falls short when the goal is a portfolio piece that demonstrates production engineering:

| Concern | Looker Studio | React + FastAPI |
|---------|--------------|-----------------|
| Customization | Limited to built-in chart types and themes | Full control over layout, interactions, and styling |
| Data sources | Direct BigQuery connector only | Backend can merge BigQuery, MongoDB, Kafka metrics in one API call |
| Real-time | Polling with manual refresh | SSE streaming, auto-refresh, live WebSocket potential |
| Portfolio signal | "I connected a BI tool" | "I built a full-stack analytics application" |
| Deployment | Hosted on Google, can't self-host | `npm run build` produces a static SPA that FastAPI serves |

---

## Architecture

```
Browser (React SPA)
   │
   ├── GET /api/realtime/*       ──► BigQuery: realtime + marts tables
   ├── GET /api/experiments/*    ──► BigQuery: experiment_comparison_view, fct_experiment_results
   ├── GET /api/funnel/*         ──► BigQuery: clickstream_events, fct_orders
   ├── GET /api/business/*       ──► BigQuery: fct_orders_unified, fct_daily_metrics, dim_*
   ├── GET /api/monitoring/*     ──► BigQuery + MongoDB: consumer_health_metrics, pipeline status
   ├── POST /api/agent/chat      ──► LangGraph AI Agent (SSE streaming)
   └── GET /api/health           ──► Credential check
```

The frontend never talks to BigQuery or MongoDB directly. Every data request goes through FastAPI, which handles authentication, query execution, serialization, and error handling.

---

## Backend Structure

### Entry Point: `api/main.py`

Registers six routers under `/api/` prefixes with CORS enabled for local development. If `dashboard/dist/` exists (production build), it mounts the SPA as a static file handler at `/`.

### Database Layer: `api/db.py`

Two singleton clients, cached with `@lru_cache`:

- **`get_bq_client()`** — Google BigQuery client using `GOOGLE_APPLICATION_CREDENTIALS` and `GOOGLE_CLOUD_PROJECT` from environment
- **`get_mongo_client()`** — PyMongo client using `MONGODB_URI` from environment
- **`bq_query(sql)`** — Runs any SQL, returns `list[dict]` with automatic type serialization (datetime, Decimal)
- **`bq_scalar(sql)`** — Runs a query that returns a single value

### Routers

| Router | Prefix | Data Sources | Key Endpoints |
|--------|--------|-------------|---------------|
| `realtime.py` | `/api/realtime` | BigQuery realtime + marts | Live orders, GMV sparklines, state heatmap |
| `experiments.py` | `/api/experiments` | BigQuery marts | Experiment portfolio, cumulative conversion, CI bars |
| `funnel.py` | `/api/funnel` | BigQuery marts (clickstream) | Conversion funnel, device/source breakdown |
| `business.py` | `/api/business` | BigQuery marts (unified views) | Revenue trends, cohort retention, AOV distribution |
| `monitoring.py` | `/api/monitoring` | BigQuery monitoring + MongoDB | Pipeline health, Kafka lag, SLA compliance |
| `agent.py` | `/api/agent` | All (via LangGraph tools) | AI agent chat with SSE streaming |

---

## Frontend Structure

### Tech Stack

- **React 18** with TypeScript — component-based UI
- **Vite** — fast dev server with HMR
- **Tailwind CSS** — utility-first styling with custom light theme
- **Recharts** — composable chart library (LineChart, BarChart, AreaChart, PieChart, Treemap)
- **React Router** — client-side routing between dashboard pages

### Pages

| Page | Route | Purpose |
|------|-------|---------|
| `Home.tsx` | `/` | Landing page with navigation to all dashboards |
| `RealtimeOps.tsx` | `/realtime` | Live order feed, GMV trends, geographic heatmap |
| `ABTesting.tsx` | `/experiments` | Experiment portfolio, statistical significance, revenue impact |
| `CustomerJourney.tsx` | `/funnel` | Conversion funnel, device analysis, time-to-convert |
| `BusinessPerformance.tsx` | `/business` | Executive KPIs, cohort retention, category revenue |
| `DataQuality.tsx` | `/monitoring` | Pipeline health gauge, Kafka consumer lag, SLA tracking |
| `Agent.tsx` | `/agent` | AI agent chat interface with reasoning display and inline charts |

### Design System

The dashboard uses a **light theme with blue accents**:

- **Font:** Inter (400, 500, 600, 700, 800 weights from Google Fonts)
- **Primary color:** `#4C6FFF` (blue)
- **Background:** `#F8FAFC` (slate-50)
- **Cards:** White with subtle `box-shadow`, 12px border-radius
- **KPI cards:** Larger shadow, bold metric value, muted label
- **Charts:** Consistent color palette across all pages

---

## Running Locally

### Prerequisites

- Python 3.10+ with `pip install -e .` (installs FastAPI, uvicorn, google-cloud-bigquery, pymongo)
- Node.js 18+ for the React frontend
- `.env` file with `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_CLOUD_PROJECT`, `MONGODB_URI`

### Development

```bash
# Terminal 1: Start FastAPI backend
uvicorn api.main:app --port 8000 --reload

# Terminal 2: Start React dev server
cd dashboard
npm install
npm run dev
```

The React dev server runs on `http://localhost:5173` and proxies API calls to `http://localhost:8000`.

### Production Build

```bash
cd dashboard
npm run build
# The built SPA is at dashboard/dist/
# FastAPI auto-serves it when the directory exists
uvicorn api.main:app --port 8000
```

---

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `GOOGLE_APPLICATION_CREDENTIALS` | Yes | Path to GCP service account JSON |
| `GOOGLE_CLOUD_PROJECT` | Yes | GCP project ID for BigQuery |
| `MONGODB_URI` | Yes | MongoDB Atlas connection string |
| `ANTHROPIC_API_KEY` | Yes (for AI agent) | Claude API key for the LangGraph agent |

---

*Last updated: February 2026*
