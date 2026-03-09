# S3 Data Lake — Medallion Architecture for E-Commerce Analytics

> A three-zone data lake on AWS S3 implementing the medallion architecture pattern (bronze/silver/gold) to provide scalable, cost-efficient storage for both batch and streaming data in a Lambda architecture.

---

## Overview

Every analytics platform needs a durable, schema-agnostic storage layer that can absorb data at any velocity without schema-on-write constraints. S3 serves as the foundational storage tier in this architecture, sitting between raw data ingestion (Kafka archives, Olist CSV imports) and the analytical warehouse (BigQuery).

The medallion architecture organizes data into progressive refinement zones. Raw data lands in bronze untouched, transformations produce cleaned data in silver, and business-ready aggregates live in gold. This separation ensures raw data is never lost, transformations are always reproducible, and downstream consumers get pre-validated datasets.

---

## Medallion Architecture Design

### Zone Definitions

| Zone | Purpose | Data Characteristics | Mutability |
|------|---------|---------------------|------------|
| **Bronze** | Raw source of truth | Exact copies of source data; no transformations applied | Append-only, immutable |
| **Silver** | Cleaned and standardized | Deduped, typed, null-handled, schema-enforced | Overwrite on re-processing |
| **Gold** | Business aggregates | Pre-computed metrics, experiment results, dashboard feeds | Rebuilt from silver |

> **Why this matters:** In production e-commerce, a data quality issue discovered weeks later can be reprocessed from bronze without any data loss. The medallion pattern makes every transformation auditable and reproducible.

### Folder Structure

```
s3://ecommerce-streaming-analytics-{id}/
├── bronze/
│   ├── olist_historical/        # Source CSVs partitioned by table
│   │   ├── orders/
│   │   ├── order_items/
│   │   ├── order_payments/
│   │   ├── order_reviews/
│   │   ├── products/
│   │   ├── customers/
│   │   ├── sellers/
│   │   └── geolocation/
│   ├── kafka_archive/           # Streaming data archived by date/topic
│   └── experiments/             # Experiment assignment snapshots
├── silver/
│   ├── orders_cleaned/          # Type-cast, null-handled, deduped
│   ├── events_processed/        # Validated clickstream events
│   └── stg_*/                   # Staging models (dbt convention)
└── gold/
    ├── daily_metrics/           # Pre-aggregated KPIs
    └── experiment_results/      # Statistical test outputs
```

### Why Three Zones Instead of Two

A simpler raw/processed split is common but creates problems at scale:

- **Debugging:** When gold metrics look wrong, silver provides a checkpoint. Without it, you re-process from raw every time.
- **Multiple consumers:** The dashboard team needs gold aggregates; the data science team needs silver-level cleaned records. One zone cannot serve both efficiently.
- **Cost management:** Silver can be archived to Glacier after 90 days because it is always reproducible from bronze. Gold can expire even sooner.

---

## Data Flow Through Zones

### Bronze Ingestion

Bronze receives data from two primary paths:

- **Batch path:** Olist historical CSVs uploaded via Dagster pipeline or manual script. Files land in `bronze/olist_historical/{table}/` preserving original column names and types.
- **Streaming path:** Kafka consumers archive topic data to `bronze/kafka_archive/{topic}/{date}/` in NDJSON format. This provides a durable backup of all streaming events independent of Kafka retention.

### Silver Transformation

Dagster orchestrates bronze-to-silver transformations:

- **Type casting:** String dates to ISO 8601, numeric strings to proper types
- **Deduplication:** Remove duplicate records using natural keys (order_id, event_id)
- **Null handling:** Apply business rules for missing values (e.g., null freight_value defaults to 0 for digital products)
- **Schema enforcement:** Validate against expected column sets; quarantine malformed records

### Gold Aggregation

Gold tables are purpose-built for specific consumers:

- **Daily metrics:** Revenue, order count, AOV, conversion rate by day
- **Experiment results:** Per-variant aggregates with statistical test outputs
- **Dashboard feeds:** Pre-joined, pre-filtered datasets optimized for the React dashboard

---

## Lifecycle and Cost Management

| Zone | Retention Policy | Storage Class | Rationale |
|------|-----------------|---------------|-----------|
| **Bronze** | Indefinite (10-year expiry) | S3 Standard, then Infrequent Access after 180 days | Raw data is the only non-reproducible asset |
| **Silver** | 90 days, then Glacier | S3 Standard → Glacier transition | Reproducible from bronze; Glacier reduces cost by ~80% |
| **Gold** | 90 days, then expire | S3 Standard | Fully reproducible from silver; no archival needed |

> **Cost perspective:** For this project's data volume (~500 MB bronze, ~300 MB silver, ~50 MB gold), S3 costs are negligible. At 100x scale (~50 GB bronze), lifecycle policies save approximately $15/month by moving silver to Glacier and expiring gold. The real value is establishing the pattern for production use.

### Lifecycle Policy Configuration

```json
{
  "Rules": [
    {
      "ID": "BronzeRetainForever",
      "Status": "Enabled",
      "Filter": {"Prefix": "bronze/"},
      "Expiration": {"Days": 3650},
      "NoncurrentVersionExpiration": {"NoncurrentDays": 90}
    },
    {
      "ID": "SilverToGlacier",
      "Status": "Enabled",
      "Filter": {"Prefix": "silver/"},
      "Transitions": [
        {"Days": 90, "StorageClass": "GLACIER"}
      ],
      "NoncurrentVersionExpiration": {"NoncurrentDays": 30}
    },
    {
      "ID": "GoldShortRetention",
      "Status": "Enabled",
      "Filter": {"Prefix": "gold/"},
      "Expiration": {"Days": 90},
      "NoncurrentVersionExpiration": {"NoncurrentDays": 30}
    }
  ]
}
```

---

## Security Model

### Access Control Strategy

The data lake uses a **role-based access model** aligned with the principle of least privilege:

| Role | Bronze | Silver | Gold | Use Case |
|------|--------|--------|------|----------|
| **Dagster Pipeline** | Read | Read/Write | Read/Write | Orchestrates all transformations |
| **Kafka Archiver** | Write (kafka_archive/ only) | None | None | Streams backup to S3 |
| **BI / Dashboard** | None | Read | Read | Consumes analytics-ready data |
| **Data Science** | Read | Read | Read | Ad-hoc analysis from any zone |

### Critical Security Decisions

- **All public access blocked:** S3 Block Public Access enabled on all four settings. Data lake buckets should never be publicly accessible.
- **Versioning enabled:** Protects against accidental overwrites and deletes. Critical for bronze where data is irreplaceable.
- **SSE-S3 encryption:** Server-side encryption with S3-managed keys (AES-256). No key management overhead; sufficient for non-regulated data.
- **Prefix-scoped policies:** IAM policies restrict write access by prefix. The pipeline role cannot write to bronze (preventing accidental raw data corruption).

### Cross-Cloud Access (S3 to BigQuery)

Since the warehouse (BigQuery) runs on GCP while the data lake runs on AWS, data transfer follows this path:

- **Dagster acts as the bridge:** Dagster holds credentials for both AWS and GCP, reads from S3 and loads to BigQuery via the BigQuery Python client.
- **No direct S3-to-BigQuery connection:** Avoids the complexity of GCP-AWS federation (Storage Transfer Service, external connections). Dagster provides full control over transformation and error handling during the transfer.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage service | AWS S3 | Industry standard for data lakes; integrates with every tool in the ecosystem |
| Architecture pattern | Medallion (bronze/silver/gold) | Proven pattern from Databricks/Delta Lake; clear separation of concerns |
| File format (bronze) | CSV / NDJSON | Preserves source format exactly; no transformation at ingestion |
| File format (silver/gold) | Parquet (future) / CSV | Parquet planned for columnar efficiency; CSV used initially for simplicity |
| Partitioning strategy | By table (bronze), by date (kafka_archive) | Matches query patterns; date partitioning enables efficient time-range scans |
| Cross-cloud transfer | Dagster-mediated | Simpler than GCP-AWS federation; full control over error handling |
| Encryption | SSE-S3 (AES-256) | Zero management overhead; sufficient for portfolio/non-PII data |
| Versioning | Enabled | Protects irreplaceable bronze data; enables audit trail |

---

## Production Considerations

- **Scaling to 100x volume:** S3 scales infinitely, but costs shift. At ~50 GB bronze, consider Parquet + columnar compression (10:1 ratio typical) and S3 Select for server-side filtering to reduce data transfer costs.
- **What breaks first:** Not S3 itself, but the Dagster-mediated cross-cloud transfer. At high volume, consider direct S3-to-BigQuery integration via GCP Storage Transfer Service or BigQuery external tables with S3 connector.
- **Partition explosion:** The `kafka_archive/{topic}/{date}/` structure creates one prefix per topic per day. At 7 topics over a year, that is ~2,500 prefixes — well within S3 limits. At 100 topics, consider adding hour-level partitioning selectively.
- **Cost monitoring:** Set S3 billing alerts. The primary cost driver shifts from storage to API calls (PUT/GET) at high event volumes. Batch uploads (multi-part) reduce PUT costs significantly.
- **Data consistency:** S3 provides strong read-after-write consistency (since December 2020). No special handling needed for read-after-write scenarios in the pipeline.
- **Disaster recovery:** Bronze is the critical recovery point. Consider cross-region replication for bronze in a production deployment. Silver and gold are always reproducible.

---

*Last updated: March 2026*
