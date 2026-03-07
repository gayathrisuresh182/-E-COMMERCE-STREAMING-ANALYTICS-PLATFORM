# E-Commerce Streaming Analytics Platform

Production-grade e-commerce analytics demonstrating **real-time event streaming** (Apache Kafka), **asset-oriented orchestration** (Dagster), **Lambda architecture** (batch + stream), and **statistical A/B testing** on the Olist Brazilian E-Commerce dataset.

## What This Project Demonstrates

- **Dagster** – Asset-centric data orchestration (not task-centric)
- **Apache Kafka** – Event streaming with multiple topics and retention
- **Lambda architecture** – Batch (historical) + speed (real-time) layers
- **A/B testing** – Experiment design and statistical analysis (scipy, statsmodels)
- **Multi-cloud analytics** – BigQuery (primary), Redshift (comparison)
- **NoSQL** – MongoDB for operational/semi-structured data
- **Data quality** – Dagster asset checks and monitoring

## Architecture (High Level)

```
Olist CSV (S3) ──► Dagster batch assets ──► BigQuery (staging → marts)
       │
       └── Simulated events ──► Kafka topics ──► Consumers ──► BigQuery / MongoDB
                                                                      │
Looker / Excel ◄──────────────────────────────────────────────────────┘
```

- **Batch layer:** Historical Olist data → Dagster jobs → BigQuery (partitioned facts, dimensions).
- **Stream layer:** Kafka topics (orders, clickstream, payments, deliveries, reviews, experiments) → consumers → BigQuery + MongoDB.
- **Serving layer:** Unified views (batch + stream) for dashboards and reporting.

## Tech Stack

| Area           | Technology                          |
|----------------|-------------------------------------|
| Orchestration  | Dagster (asset-oriented)           |
| Streaming      | Apache Kafka                        |
| Analytics DB   | BigQuery, Redshift (optional)        |
| NoSQL          | MongoDB                             |
| Storage        | S3 (data lake)                      |
| Statistics     | Python (scipy, statsmodels, pandas) |
| BI             | Looker, Excel                        |
| DevOps         | Docker Compose, GitHub Actions      |

## Prerequisites

- **Python** 3.10+
- **Docker** and **Docker Compose** (for Kafka)
- **Kaggle account** (for Olist dataset – Phase 1A)
- **GCP** (BigQuery) and optionally **MongoDB** when you enable those layers

## Quick Start

### 0. (Phase 1A) Download Olist dataset

```bash
pip install kaggle
# Configure: Kaggle → Settings → API → Create New Token → save as ~/.kaggle/kaggle.json
python scripts/download_olist.py
python scripts/verify_olist_download.py
```

See [docs/PHASE_1A_KAGGLE_SETUP.md](docs/PHASE_1A_KAGGLE_SETUP.md) for credentials and folder layout. CSVs go to `raw/olist/`.

### 1. Clone and install Python dependencies

```bash
cd -E-COMMERCE-STREAMING-ANALYTICS-PLATFORM
pip install -r requirements.txt
# or: pip install -e .
```

### 2. Start Kafka (Phase 1E)

```bash
docker-compose up -d
```

Wait ~30 seconds, then create topics:

- **Windows (PowerShell):** `.\scripts\create_kafka_topics.ps1`
- **Bash:** `bash scripts/create_kafka_topics.sh`
- **Python:** `python scripts/create_kafka_topics.py`

- **Kafka UI:** http://localhost:8080  
- **Broker:** `localhost:9092`

### 3. Start Dagster (Phase 1F)

```bash
# Optional: set DAGSTER_HOME to current directory so dagster.yaml is used
set DAGSTER_HOME=%CD%        # Windows CMD
$env:DAGSTER_HOME = (Get-Location).Path   # PowerShell
export DAGSTER_HOME=$(pwd)   # Bash

dagster dev -m ecommerce_analytics
```

- **Dagit UI:** http://localhost:3000  
- You should see the asset graph (`raw_orders_csv` → `stg_orders` → `fct_orders`) and the `daily_batch_processing` job/schedule.

### 4. Test Kafka (optional)

Produce a test message:

```bash
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic orders-stream
# Type a line and press Enter
```

Consume:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-stream --from-beginning
```

## Project Structure

```
├── docker-compose.yml          # Kafka, Zookeeper, Kafka UI
├── dagster.yaml                # Dagster instance config
├── pyproject.toml              # Python package and deps
├── requirements.txt
├── scripts/
│   ├── create_kafka_topics.sh  # Create Kafka topics (Bash)
│   ├── create_kafka_topics.ps1 # Create Kafka topics (PowerShell)
│   └── create_kafka_topics.py  # Create Kafka topics (Python)
├── docs/
│   └── KAFKA_TOPICS.md         # Topic design and retention
└── ecommerce_analytics/        # Dagster code location
    ├── __init__.py             # Definitions (assets, jobs, schedules, resources)
    ├── assets/
    │   └── __init__.py         # Placeholder assets (expand in Phase 3)
    ├── jobs/
    │   └── __init__.py         # daily_batch_processing job and schedule
    └── resources/
        ├── __init__.py
        └── config.py           # Kafka, BigQuery, MongoDB, Slack resources
```

## Datasets: Olist Brazilian E-Commerce

- **Source:** [Kaggle – Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)  
- **Scale:** ~100k orders, 8 CSV tables (orders, order_items, payments, reviews, products, customers, sellers, geolocation).  
- **Use:** Batch assets from S3/CSV; Kafka producers will simulate real-time events from this data.

## Execution Plan (Phases)

| Phase | Focus |
|-------|--------|
| 1 | Infrastructure (S3, MongoDB, BigQuery, **Kafka**, **Dagster**) |
| 2 | Data generation (experiments, event streams) |
| 3 | Batch layer (Dagster assets: staging → dimensions → facts) |
| 4 | Streaming (Kafka producers/consumers, stream assets) |
| 5 | Statistical analysis (A/B tests, notebooks) |
| 6 | BI dashboards, monitoring, quality checks |
| 7 | Testing and CI/CD |
| 8 | Documentation and runbooks |

**Current:** Phase 1E (Kafka) and 1F (Dagster) are set up. Next: load Olist data, define real assets, then add producers/consumers.

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

See [docs/KAFKA_TOPICS.md](docs/KAFKA_TOPICS.md) for partition and retention rationale.

## Dagster vs Airflow

- **Asset-oriented:** You define *data products* (assets) and dependencies; Dagster infers execution order.  
- **No manual DAG edges:** Dependencies come from asset inputs/outputs.  
- **Asset checks:** Data quality lives next to assets and can block downstream materialization.  
- **Partitioning:** First-class support for daily (or custom) partitions and backfills.

## License and Use

This is a portfolio/learning project. Olist dataset is subject to Kaggle/Olist terms.
