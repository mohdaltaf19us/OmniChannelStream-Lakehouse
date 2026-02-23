# ADF Pipeline Architecture — OmniOC Lakehouse
# ================================================
#
# WHEN TO USE ADF vs DATABRICKS:
#
# USE ADF FOR:                           USE DATABRICKS FOR:
# ─────────────────────────────────────  ──────────────────────────────────────
# JDBC copy from SQL Server              All transformations (Bronze→Silver→Gold)
# CDC change capture (native activity)   DLT pipelines
# File copy (SFTP, HTTP, Blob→ADLS)     Streaming (Event Hubs)
# REST API polling → landing             Autoloader (cloudFiles)
# Master orchestration (retry/alert)    ML / feature engineering
# Watermark state management             Data Vault loads
# SAP / Oracle / Snowflake connectors    Big Table builds
# Email/Teams alerts (natively)          Data quality expectations
# Trigger Databricks jobs on schedule    Unity Catalog governance
# Parameter passing across environments  Data Mesh setup
#
# PIPELINE MAP:
#
# PL_00_Master_Daily_Orchestrator      ← Daily at 02:00 UTC (trigger)
#   │
#   ├─► PL_01_JDBC_Full_Load           ← warehouses, exchange_rates (full reload small dims)
#   │       └─► Copy Activity: SQL Server → ADLS landing/
#   │
#   ├─► PL_02_JDBC_Incremental         ← orders, shipments, payments, inventory
#   │       └─► LookupActivity(watermark) → Copy(filter by updated_at) → SP(update watermark)
#   │
#   ├─► PL_03_CDC_Capture              ← customers, products (CDC-enabled tables)
#   │       └─► CDC Activity → ADLS landing/cdc/  (Parquet change files)
#   │
#   ├─► PL_04_Metadata_Driven_Copy     ← reads from config table, loops N sources
#   │       └─► LookupActivity(config) → ForEach → Copy Activity
#   │
#   ├─► PL_05_Trigger_Databricks_Batch ← triggers the Databricks batch job
#   │       └─► DatabricksNotebook/Job → wait → assert success
#   │
#   ├─► PL_06_DQ_Gate                  ← calls DQ validation notebook, fails if critical issues
#   │       └─► DatabricksNotebook → IfCondition(pass/fail) → alert or proceed
#   │
#   └─► PL_07_Alerting                 ← Teams / email notification on any failure
#
# SUPPORTING PIPELINES:
#   PL_08_Landing_Cleanup              ← Archive/delete processed landing files
#   PL_09_Delta_Optimize               ← OPTIMIZE + VACUUM via Databricks
#   PL_10_Backfill                     ← Manual backfill with date range params
#
# LINKED SERVICES:
#   LS_SqlServer_OnPrem     — SQL Server via Self-hosted IR (simulated with Docker)
#   LS_ADLS_Gen2            — Storage Account (managed identity)
#   LS_Databricks           — Databricks workspace (PAT from KV)
#   LS_KeyVault             — Key Vault reference
#   LS_AzureSqlDb_Config    — Azure SQL DB for watermark/config table (optional)
#
# DATASETS:
#   DS_SqlServer_Generic    — parameterized SQL Server table dataset
#   DS_ADLS_Parquet         — parameterized ADLS Parquet dataset
#   DS_ADLS_CSV             — parameterized ADLS CSV dataset
#   DS_ADLS_Json            — parameterized ADLS JSON dataset
#
# TRIGGERS:
#   TR_Daily_02UTC          — ScheduleTrigger: daily 02:00 UTC → PL_00_Master
#   TR_Landing_FileArrival  — StorageEventTrigger: new file in landing/ → PL_05_Databricks
#   TR_Weekly_Optimize      — ScheduleTrigger: Sunday 03:00 UTC → PL_09_Delta_Optimize
