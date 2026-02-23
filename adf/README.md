# OmniOC ADF Pipelines — Architecture Guide

## What ADF Does in This Project

ADF handles the **orchestration layer** — specifically the jobs that are
better suited to ADF than Databricks Jobs: SQL Server extraction, metadata-
driven looping, scheduling, and cross-system monitoring.

Databricks handles all compute-heavy transformation work (DLT, Spark jobs).
ADF calls Databricks, it does not replace it.

```
                         ┌──────────────────────────────────────────────────────┐
                         │         AZURE DATA FACTORY (omnioc-dev-eus-adf-01)   │
                         │                                                        │
  SQL Server ────────────►  pl_master_orchestrator                               │
  (on-prem / Azure VM)  │       │                                                │
                         │       ├─► pl_ingest_sqlserver_to_adls  (Copy Data)   │
  ADLS landing/ ─────────►      │       └── metadata-driven ForEach over tables  │
  (file arrival)         │       │                                                │
                         │       ├─► pl_cdc_extract               (CDC)          │
                         │       │       └── incremental change capture          │
                         │       │                                                │
                         │       ├─► pl_trigger_databricks_batch  (Databricks)  │
                         │       │       └── calls Databricks Job via REST       │
                         │       │                                                │
                         │       └─► pl_monitor_bronze_freshness  (Validation)   │
                         │               └── checks row counts, sends alerts     │
                         │                                                        │
                         │  Triggers:                                             │
                         │    tr_daily_batch          — 01:00 UTC daily          │
                         │    tr_tumbling_window_cdc  — every 15 min CDC         │
                         │    tr_storage_event        — file arrives in landing/ │
                         └──────────────────────────────────────────────────────┘
                                          │
                                          ▼
                         Databricks (DLT + Jobs handle all transformation)
```

## Pipeline Inventory

| Pipeline | Type | Trigger | Purpose |
|---|---|---|---|
| pl_master_orchestrator | Execute Pipeline | tr_daily_batch | Sequences all daily pipelines |
| pl_ingest_sqlserver_to_adls | Copy Data (metadata-driven) | Master | Copies 8 SQL Server tables → ADLS Parquet |
| pl_cdc_extract | Copy Data (incremental) | tr_tumbling_window_cdc | CDC incremental rows from SQL Server |
| pl_trigger_databricks_batch | Databricks Notebook Activity | Master | Triggers Databricks batch ingestion job |
| pl_monitor_bronze_freshness | Web Activity + If Condition | Master | Validates Bronze freshness, sends Teams alert |
| pl_exchange_rate_api | Copy Data (HTTP) | tr_daily_batch | Pulls FX rates from external API → ADLS |

## Linked Services

| Linked Service | Type | Auth | Used By |
|---|---|---|---|
| ls_adls_omniocdeveusst01 | Azure Data Lake Storage Gen2 | Managed Identity | All datasets |
| ls_sqlserver_omnioc | SQL Server | Key Vault secret | SQL Server datasets |
| ls_databricks_omnioc | Azure Databricks | Access Token (Key Vault) | Databricks activities |
| ls_keyvault_omnioc | Azure Key Vault | Managed Identity | Secret resolution |
| ls_http_fxrates | HTTP | Anonymous | FX rate API |

## Import Instructions

1. In ADF Studio: **Manage → Source Control** or use ARM template
2. Or use ADF CLI:
   ```bash
   az datafactory pipeline create \
     --factory-name omnioc-dev-eus-adf-01 \
     --resource-group omnioc-dev-eus-rg-01 \
     --name pl_master_orchestrator \
     --pipeline @pipelines/pl_master_orchestrator.json
   ```
3. Or use the ADF Deployment action in `.github/workflows/deploy_adf.yml`

## Key Design Decisions

- **No nested ForEach** — ADF does not support ForEach inside ForEach.
  Metadata lookup tables flatten the table list; one ForEach copies all tables in parallel.
- **SetVariable is thread-unsafe inside ForEach** — all per-table results written to
  an output array using `@concat()` pattern, not SetVariable.
- **All secrets in Key Vault** — no passwords in linked service JSON. Linked services
  reference Key Vault secret references.
- **Managed Identity** — ADF MSI used for ADLS and Key Vault access. No service principal
  passwords stored anywhere.
