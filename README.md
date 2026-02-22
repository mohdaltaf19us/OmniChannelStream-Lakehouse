# OmniOC Lakehouse

A production-grade Azure data engineering platform built from scratch — covering everything from raw event ingestion to analytical gold layer, with proper CI/CD, data quality contracts, and four distinct modelling approaches under one roof.

This isn't a tutorial project or a proof-of-concept. It's the kind of platform you'd actually run in a mid-sized e-commerce company, built the way a proper data engineering team would build it.

---

## Why I Built This

I wanted a single project that covered the full breadth of modern Azure data engineering without cutting corners on any layer. Most portfolio projects pick one thing — usually a simple ETL pipeline — and call it done. This one starts at the raw event producer and doesn't stop until you have dimensional models, data vault, big table aggregates, and domain-separated data mesh catalogs all running in production.

The design decisions throughout — medallion architecture, metadata-driven ADF pipelines, Unity Catalog governance, streaming with late arrival handling, four modelling paradigms in one platform — are the kinds of decisions you make at work, not in a tutorial.

---

## What the Platform Does

Three business domains feed into a unified lakehouse:

**Supply Chain** — suppliers, products, warehouses, shipments, contracts. Batch-loaded from CSV/Parquet via Autoloader. SCD Type 1 and Type 2 handled by DLT APPLY CHANGES.

**Finance / Transactions** — customers, orders, order lines, payments, refunds, exchange rates. 100k orders, 150k order lines. CDC change capture tracks customer and product mutations over time.

**Clickstream / Real-time** — user sessions, page views, cart events, purchases. Streams from Azure Event Hubs via the Kafka-compatible endpoint into a continuous DLT pipeline. Late arrivals and poison pills injected deliberately to test quarantine logic.

Everything lands in Bronze as-is, gets cleaned and conformed in Silver, and surfaces as business-ready analytical tables in Gold.

---

## Architecture

```
Local Kafka / Event Producers
          │
          ▼
  Azure Event Hubs (eh-clickstream, device-telemetry, order-events)
          │
          ├────────────────────────────────────────────┐
          │                                            │
          ▼                                            ▼
   ADLS Gen2 landing/                        Event Hubs Kafka endpoint
   (batch files via ADF)                     (streaming into Databricks)
          │                                            │
          └──────────────┬─────────────────────────────┘
                         │
                         ▼
              Databricks Bronze Layer
              (Autoloader + Structured Streaming)
                         │
                         ▼
              Delta Live Tables (Silver)
              SCD1/2, dedup, quarantine, DQ expectations
                         │
                         ▼
              Gold Layer — 4 modelling approaches
              Dimensional │ Data Vault │ Big Table │ Data Mesh
                         │
                         ▼
              Unity Catalog (omnioc_dev)
              Domain catalogs, row-level security, access controls
```

**Azure resources:**
- Resource group: `omnioc-dev-eus-rg-01` (East US)
- Databricks workspace: `omnioc-dev-eus-dbw-01` with Unity Catalog
- Event Hubs namespace: `omnioc-dev-eus-ehns-std-01` (Standard tier)
- ADLS Gen2: `omniocdeveusst01` (landing / bronze / silver / gold / checkpoint / ucroot)
- ADF: `omnioc-dev-eus-adf-01`
- Key Vault: `omnioc-dev-eus-kv-01`

---

## Repository Structure

```
Lakehouse-Structured-Streaming/
│
├── databricks.yml                        # DABs root — multi-env bundle config
│
├── resources/
│   ├── jobs/
│   │   ├── batch_ingestion_job.yml       # Daily batch job (9 tasks, autoscale 2-6)
│   │   └── streaming_job.yml             # Always-on streaming job
│   ├── pipelines/
│   │   ├── supply_chain_dlt.yml          # Batch DLT pipeline config
│   │   └── clickstream_dlt.yml           # Continuous DLT pipeline config
│   └── warehouses/
│       └── reporting_warehouse.yml       # Serverless SQL warehouse
│
├── src/
│   ├── ingestion/
│   │   ├── 01_bronze_clickstream_eventhub.py   # Event Hubs → Bronze Delta
│   │   ├── 02_bronze_autoloader_csv.py          # CSV files → Bronze
│   │   ├── 03_bronze_autoloader_json.py         # JSON files → Bronze
│   │   ├── 04_bronze_autoloader_parquet.py      # Parquet → Bronze
│   │   └── 05_bronze_verification.py            # Row counts + DQ gate
│   │
│   ├── dlt_pipelines/
│   │   ├── supply_chain_dlt.sql          # Declarative SQL DLT — 15 tables
│   │   └── clickstream_dlt.py            # Python DLT — continuous streaming
│   │
│   ├── streaming/
│   │   ├── clickstream_stream.py         # Structured streaming with MERGE upserts
│   │   └── telemetry_stream.py           # Device telemetry + alert thresholds
│   │
│   └── data_models/
│       ├── dimensional/
│       │   └── gold_dimensional_model.py  # Star schema — SCD2 dims, fact tables
│       ├── data_vault/
│       │   └── gold_data_vault.py         # DV2.0 — hubs, links, satellites, PIT
│       ├── big_table/
│       │   └── gold_big_table.py          # Wide tables — 30+ KPIs, no joins
│       └── data_mesh/
│           └── gold_data_mesh_setup.py    # UC domain catalogs + row-level security
│
├── src/data_quality/
│   └── great_expectations/
│       └── dq_validation.py              # DQ suite — 8 expectation types
│
├── data_contracts/
│   ├── silver/orders.yml                 # Contract v1.2 — 17 columns, 3 quality rules
│   └── gold/fact_orders.yml              # Contract v1.0 — surrogate keys, breaking changes
│
├── adf/
│   ├── linkedServices/                   # ADLS, SQL Server, Databricks, KV, HTTP
│   ├── datasets/                         # Parameterized SQL Server + ADLS datasets
│   ├── pipelines/
│   │   ├── pl_master_orchestrator.json   # Daily sequence with audit logging
│   │   ├── pl_ingest_sqlserver_to_adls.json  # Metadata-driven ForEach copy
│   │   ├── pl_cdc_extract.json           # SQL Server CDC via LSN watermarks
│   │   ├── pl_trigger_databricks_batch.json  # REST polling loop
│   │   ├── pl_monitor_bronze_freshness.json  # Teams alert on stale tables
│   │   └── pl_exchange_rate_api.json     # HTTP → ADLS FX rates
│   ├── triggers/                         # Daily schedule, tumbling window, storage event
│   └── sql_server_support_objects.sql    # Config tables + stored procedures
│
├── tests/
│   └── unit/
│       └── test_transformations.py       # 20 pytest tests — transforms, DV hashing, DQ
│
└── .github/
    ├── workflows/
    │   ├── ci.yml                        # PR gate — lint, test, bundle validate
    │   ├── deploy_dev.yml                # Auto-deploy on main push
    │   ├── deploy_prod.yml               # Manual prod deploy with approval gate
    │   └── deploy_adf.yml                # ADF pipeline deployment
    └── scripts/
        └── validate_contracts.py         # CI data contract schema checker
```

---

## The Four Gold Models

One thing I wanted to get right was demonstrating that different modelling approaches aren't competing philosophies — they solve different problems and you'd use all of them in the same platform.

**Dimensional (Star Schema)** — `gold_dimensional_model.py`
The standard. `dim_customer` and `dim_product` are SCD Type 2 with hash diffs for change detection. `dim_date` is a 10-year spine. `fact_orders` and `fact_payments` (accumulating snapshot with four milestones) give you clean join paths for BI tools.

**Data Vault 2.0** — `gold_data_vault.py`
Five hubs, four links, five satellites, one PIT table. All insert-only. Hub hash keys are MD5 of business keys. The PIT table does 365-day daily snapshots by cross-joining hubs against a date range, then point-in-time joining the satellites. Expensive to build, worth it when you need full audit history.

**Big Table (Wide Aggregates)** — `gold_big_table.py`
The anti-normalisation approach. `wide_customer_order_summary` computes 30+ KPIs in a single pass — lifetime value, churn risk score, cancellation rate, channel breakdown — all pre-joined so analysts never touch a JOIN button. `wide_product_performance` and `agg_daily_sales_by_region` follow the same pattern.

**Data Mesh** — `gold_data_mesh_setup.py`
Four Unity Catalog domain catalogs: `omnioc_supply_chain`, `omnioc_finance`, `omnioc_cx`, `omnioc_platform`. Each domain exposes published views, internal schemas, and controlled access via UC role grants. The CX team's customer view SHA2-hashes the customer ID and strips all PII — they get segment, country, loyalty tier, and churn score but nothing identifiable.

---

## ADF Orchestration

ADF handles the four jobs that are better done outside Databricks:

**Metadata-driven SQL Server copy** — a config table in SQL Server (`adf_copy_config`) drives the entire ingestion. Adding a new source table is one INSERT row, no code change. ForEach runs four tables in parallel; watermarks update via stored procedure after each successful copy.

**CDC extraction** — reads `sys.fn_cdc_get_all_changes_*` using LSN boundaries tracked in `adf_cdc_watermark`. A tumbling window trigger fires every 15 minutes, guaranteeing exactly-once processing even after retries. Databricks picks up the JSON files with APPLY CHANGES INTO.

**HTTP → ADLS FX rates** — open.er-api.com for three base currencies daily. ADF's native HTTP connector handles this in zero Spark code.

**Bronze freshness monitoring** — queries Databricks SQL API post-load to check row counts against thresholds. If any table is stale, posts a Teams adaptive card with the run ID and ADF link. This is the ops visibility layer.

---

## Data Quality

DQ runs at two levels. DLT pipeline expectations (`@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`) enforce quality inline — failed rows go to a quarantine table rather than being silently dropped. This is visible in the DLT event log.

The `DQSuite` class in `dq_validation.py` runs after load as a standalone validation pass — eight expectation types including referential integrity and freshness checks. Results write to `bronze.dq_validation_results`. Any CRITICAL failures raise a `ValueError` that fails the CI gate.

Data contracts in `/data_contracts/` define the schema, allowed values, quality rules, and breaking change policy for Silver and Gold tables as YAML. The `validate_contracts.py` CI script checks every contract on every PR — nobody merges schema changes without updating the contract first.

---

## Data Generation

The platform ships with realistic synthetic data across all three domains:

| Domain | Files | Volume |
|---|---|---|
| Supply Chain | suppliers, products, warehouses, shipments, contracts | 5k products, 30k shipments |
| Finance | customers, orders, order lines, payments, refunds, exchange rates | 10k customers, 100k orders |
| CDC | customer changes, product changes | 2k + 500 change events |
| Bad Data | poison pills — null IDs, type mismatches, negative amounts, schema drift, duplicates | 500 records |

The Kafka producer (`data_generators/omnioc_kafka_producer.py`) runs four topics simultaneously in separate threads with configurable event counts, timing intervals, late arrival injection (8%), and poison pill injection (2%). Sessions maintain continuity across events — add-to-cart followed by purchase on the same session ID.

---

## CI/CD

Three GitHub Actions workflows:

`ci.yml` — runs on every PR. Flake8 lint, pytest with coverage, contract validation, `databricks bundle validate --target dev`. All four jobs must pass.

`deploy_dev.yml` — auto-triggers on push to main. Deploys the DABs bundle to dev, runs a smoke test Bronze verification job, optionally triggers DLT pipeline refresh.

`deploy_prod.yml` — manual `workflow_dispatch` only. Requires `confirm_prod = "DEPLOY-PROD"` as input. Full CI, `production` environment approval gate in GitHub, deploy, smoke test, tag the release. On failure: Slack notification with rollback instructions.

ADF gets its own `deploy_adf.yml` — validates all JSON files, deploys linked services then datasets then pipelines in dependency order (child pipelines before master), deploys triggers in stopped state so nothing fires automatically on deploy.

---

## Getting Started

**Prerequisites:**
- Azure subscription with Contributor on the resource group
- Databricks workspace with Unity Catalog enabled
- Event Hubs Standard namespace
- Python 3.10+, Docker (for local Kafka)
- Databricks CLI v0.200+

**Quick setup:**

```bash
# Clone the repo
git clone https://github.com/mohdaltaf19us/Lakehouse-Structured-Streaming
cd Lakehouse-Structured-Streaming

# Install Python deps
pip install kafka-python faker pandas pyarrow azure-storage-blob

# Start local Kafka stack
docker compose -f docker/docker-compose.yml up -d

# Generate and upload sample data
jupyter notebook data_generators/generate_sample_data_v2.ipynb
# Edit OUTPUT_DIR and STORAGE_CONNECTION_STRING in Cell 2, then Run All

# Produce streaming events
jupyter notebook data_generators/omnioc_kafka_producer.ipynb
# Mirror to Event Hubs
jupyter notebook data_consumer/omnioc_kafka_to_eventhub.ipynb

# Deploy to Databricks dev
databricks auth login --host https://adb-7405609624004172.12.azuredatabricks.net
databricks bundle deploy --target dev
```

**Key Vault secrets needed before running notebooks:**

| Secret name | Where to get it |
|---|---|
| `eventhub-clickstream-connection-string` | Event Hubs → Shared access policies → Connection string |
| `databricks-adf-access-token` | Databricks → Settings → Developer → Access tokens |
| `eventhub-telemetry-connection-string` | Same namespace, same connection string |

**Databricks secret scope:**

```
https://{workspace-url}/#secrets/createScope
Scope name: omnioc-kv-scope
DNS Name: https://omnioc-dev-eus-kv-01.vault.azure.net/
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud | Microsoft Azure (East US) |
| Compute | Azure Databricks (Unity Catalog) |
| Orchestration | Azure Data Factory |
| Streaming | Azure Event Hubs (Kafka protocol) |
| Storage | ADLS Gen2 |
| Table format | Delta Lake |
| Pipeline framework | Delta Live Tables |
| Secrets | Azure Key Vault |
| CI/CD | GitHub Actions + Databricks Asset Bundles |
| Data quality | Great Expectations patterns + DLT expectations |
| Testing | pytest + chispa (DataFrame assertions) |
| Local dev | Docker Kafka + Kafka UI |
| Data generation | Python + Faker |

---

## What I'd Add With More Time

A few things I left out that a production platform would have: proper Databricks SQL dashboards querying the Gold layer, alerting via Azure Monitor in addition to Teams webhooks, schema registry integration for the Kafka producers, and a proper slowly-changing dimension backfill strategy for when historical data arrives late. The Data Mesh setup also needs proper data sharing agreements configured — right now it's catalog isolation without actual cross-workspace sharing.

The SQL Server CDC path is designed but not fully wired end-to-end since it requires a running SQL Server instance. The DDL and stored procedures are all there in `adf/sql_server_support_objects.sql` — it just needs a server.

---

## Author

**Mohammed Altaf** — [@mohdaltaf19us](https://github.com/mohdaltaf19us)

Built as a comprehensive portfolio demonstration of Azure data engineering at senior level. If you're looking at this for hiring purposes: the architectural decisions, the choice to put four modelling approaches in one platform rather than four separate repos, and the decision to wire up proper CI/CD with approval gates rather than just pushing notebooks — those were all deliberate.
