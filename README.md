# E-Commerce Streaming Analytics Platform

Production-grade e-commerce analytics combining **real-time event streaming** (Apache Kafka), **asset-oriented orchestration** (Dagster), **Lambda architecture** (batch + stream), **statistical A/B testing**, and a custom **React + FastAPI dashboard** — built on the Olist Brazilian E-Commerce dataset.

## Architecture

```
Olist CSV ──► Dagster batch assets ──► BigQuery (staging → dims → facts → marts)
    │                                         │
    └── Kafka producers ──► Kafka topics ──► Consumers ──► BigQuery / MongoDB
                                                                  │
React Dashboard ◄── FastAPI ◄─────────────────────────────────────┘
```

- **Batch layer:** Historical Olist data → Dagster jobs → BigQuery (partitioned facts, dimensions, marts).
- **Stream layer:** Kafka topics (orders, clickstream, payments, deliveries, reviews, experiments) → consumers → BigQuery + MongoDB.
- **Serving layer:** Unified views (batch + stream) for the React dashboard via FastAPI.

## Tech Stack

| Area           | Technology                              |
|----------------|-----------------------------------------|
| Orchestration  | Dagster (asset-oriented)                |
| Streaming      | Apache Kafka (Confluent)                |
| Analytics DB   | Google BigQuery                         |
| NoSQL          | MongoDB Atlas                           |
| API            | FastAPI + Uvicorn                       |
| Dashboard      | React + TypeScript + Recharts + Tailwind|
| Statistics     | Python (scipy, statsmodels, pandas)     |
| Infrastructure | Docker Compose, GCP                     |

## Dashboards

| Dashboard | Key Features |
|-----------|-------------|
| **Real-Time Operations** | Live orders, GMV sparklines, heatmap, cancellation trends |
| **A/B Testing Hub** | Experiment portfolio, cumulative conversion, CI bars, revenue impact |
| **Customer Journey** | Conversion funnel, device/source breakdown, cart abandonment, time-to-convert |
| **Business Performance** | 7-day MA trends, cohort retention heatmap, AOV distribution, WoW comparison |
| **Data Quality** | Health gauge, Kafka consumer lag, SLA compliance calendar, pipeline throughput |

## Prerequisites

- **Python** 3.10+
- **Node.js** 18+ (for dashboard)
- **Docker** and **Docker Compose** (for Kafka + MongoDB)
- **GCP** project with BigQuery enabled
- **Kaggle** account (for the Olist dataset)

## Quick Start

```bash
# 1. Clone and install
pip install -e .

# 2. Start Kafka + MongoDB
docker-compose up -d

# 3. Create Kafka topics
python scripts/kafka/create_kafka_topics.py

# 4. Start Dagster
export DAGSTER_HOME=$(pwd)
dagster dev -m ecommerce_analytics

# 5. Start API
uvicorn api.main:app --port 8000

# 6. Start Dashboard
cd dashboard && npm install && npm run dev
```

Or use the Makefile: `make all`

## Project Structure

```
├── api/                          # FastAPI backend
│   ├── main.py                   #   App entry point
│   ├── db.py                     #   BigQuery + MongoDB clients
│   └── routers/                  #   Route handlers per dashboard
│
├── dashboard/                    # React + TypeScript frontend
│   └── src/
│       ├── api.ts                #   API client
│       ├── components/           #   Layout, shared components
│       └── pages/                #   One page per dashboard
│
├── ecommerce_analytics/          # Dagster code location
│   ├── assets/                   #   Data assets (staging, dims, facts, marts)
│   ├── consumers/                #   Kafka consumers
│   ├── analysis/                 #   Statistical frameworks (Bayesian, power)
│   ├── jobs/                     #   Dagster jobs and schedules
│   ├── sensors/                  #   Stream and report sensors
│   └── resources/                #   BigQuery, Kafka, MongoDB configs
│
├── scripts/                      # Operational scripts
│   ├── data/                     #   Data download, generation, loading
│   ├── bigquery/                 #   BigQuery DDL and data loading
│   ├── kafka/                    #   Topic creation, producers, consumers
│   ├── mongodb/                  #   Collection setup and data loading
│   ├── sync/                     #   Cross-system sync (realtime, monitoring)
│   ├── analysis/                 #   Excel workbook, CSV export
│   └── infra/                    #   S3 bucket setup
│
├── tests/                        # Integration and verification tests
├── docs/                         # Documentation by phase
│   ├── architecture/
│   ├── phase-1-infrastructure/
│   ├── phase-2-data-generation/
│   ├── phase-3-batch-layer/
│   ├── phase-4-analysis/
│   └── phase-5-dashboards/
│
├── raw/olist/                    # Raw Olist CSVs (gitignored)
├── output/                       # Generated artifacts (gitignored)
├── docker-compose.yml            # Kafka, Zookeeper, MongoDB, Kafka UI
├── dagster.yaml                  # Dagster instance config
└── pyproject.toml                # Python package and dependencies
```

## Kafka Topics

| Topic             | Partitions | Retention | Purpose              |
|-------------------|------------|-----------|----------------------|
| orders-stream     | 3          | 7 days    | Order placed events  |
| clickstream       | 6          | 3 days    | Browsing/session     |
| payments-stream   | 3          | 7 days    | Payment events       |
| shipments-stream  | 3          | 7 days    | Shipping updates     |
| deliveries-stream | 3          | 7 days    | Delivery events      |
| reviews-stream    | 3          | 7 days    | Review events        |
| experiments-stream| 3          | 30 days   | Experiment assigns   |

## Key Design Decisions

- **Dagster over Airflow** — Asset-oriented, not task-oriented. Dependencies come from data, not manual DAG wiring.
- **Lambda architecture** — Batch (historical completeness) + stream (real-time freshness) with unified BigQuery views.
- **React over Looker** — Full control over UX, professional portfolio presentation, no vendor lock-in.
- **Statistical rigor** — A/B tests include p-values, confidence intervals, power analysis, and Bayesian framework.

## Dataset

- **Source:** [Olist Brazilian E-Commerce (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Scale:** ~100K orders, 8 CSV tables
- **Use:** Batch assets from CSV; Kafka producers simulate real-time events from this data.

## License

Portfolio/learning project. Olist dataset is subject to Kaggle/Olist terms.
