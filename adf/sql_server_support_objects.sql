-- ============================================================
-- OmniOC SQL Server — ADF Support Objects
-- Run on the SQL Server instance that ADF connects to.
-- These tables drive the metadata-driven copy pipeline and
-- store CDC watermarks and pipeline audit logs.
-- ============================================================

USE [OmniOC_OLTP];
GO

-- ══════════════════════════════════════════════════════════
--  1. ADF COPY CONFIG — metadata-driven pipeline table list
-- ══════════════════════════════════════════════════════════

CREATE TABLE [dbo].[adf_copy_config] (
    config_id          INT IDENTITY(1,1) PRIMARY KEY,
    source_schema      NVARCHAR(128) NOT NULL DEFAULT 'dbo',
    source_table       NVARCHAR(256) NOT NULL,
    watermark_column   NVARCHAR(128) NOT NULL DEFAULT 'updated_at',
    last_watermark_value  NVARCHAR(50) NOT NULL DEFAULT '1900-01-01 00:00:00',
    load_type          NVARCHAR(20)  NOT NULL DEFAULT 'incremental',  -- 'full' | 'incremental'
    target_folder      NVARCHAR(512) NOT NULL,  -- e.g. 'supply_chain/suppliers'
    target_file_prefix NVARCHAR(256) NOT NULL,  -- e.g. 'suppliers'
    query_hint         NVARCHAR(1000) NULL,      -- optional WHERE clause suffix or NOLOCK hint
    is_active          BIT NOT NULL DEFAULT 1,
    copy_order         INT NOT NULL DEFAULT 100,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Seed with all 8 source tables
INSERT INTO [dbo].[adf_copy_config]
    (source_schema, source_table,  watermark_column, load_type, target_folder,             target_file_prefix, copy_order, query_hint)
VALUES
    ('dbo', 'suppliers',       'updated_at', 'incremental', 'supply_chain/suppliers',       'suppliers',       10,  'WITH (NOLOCK)'),
    ('dbo', 'products',        'updated_at', 'incremental', 'supply_chain/products',         'products',        20,  'WITH (NOLOCK)'),
    ('dbo', 'warehouses',      'updated_at', 'full',        'supply_chain/warehouses',       'warehouses',      30,  'WITH (NOLOCK)'),
    ('dbo', 'shipments',       'updated_at', 'incremental', 'supply_chain/shipments',        'shipments',       40,  'WITH (NOLOCK)'),
    ('dbo', 'customers',       'updated_at', 'incremental', 'finance/customers',             'customers',       50,  'WITH (NOLOCK)'),
    ('dbo', 'orders',          'updated_at', 'incremental', 'finance/orders',               'orders',          60,  'WITH (NOLOCK)'),
    ('dbo', 'order_line_items','updated_at', 'incremental', 'finance/order_lines',          'order_lines',     70,  'WITH (NOLOCK)'),
    ('dbo', 'payments',        'updated_at', 'incremental', 'finance/payments',             'payments',        80,  'WITH (NOLOCK)');
GO

-- ══════════════════════════════════════════════════════════
--  2. CDC WATERMARK — tracks LSN per CDC-enabled table
-- ══════════════════════════════════════════════════════════

CREATE TABLE [dbo].[adf_cdc_watermark] (
    watermark_id          INT IDENTITY(1,1) PRIMARY KEY,
    table_name            NVARCHAR(256) NOT NULL UNIQUE,
    last_processed_lsn    NVARCHAR(100) NOT NULL DEFAULT '0x00000000000000000000',
    last_processed_time   DATETIME2 NULL,
    rows_extracted_last   INT NULL,
    is_active             BIT NOT NULL DEFAULT 1,
    created_at            DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at            DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Seed CDC watermarks for tables where CDC is enabled
INSERT INTO [dbo].[adf_cdc_watermark] (table_name, last_processed_lsn)
VALUES
    ('customers', '0x00000000000000000000'),
    ('orders',    '0x00000000000000000000'),
    ('products',  '0x00000000000000000000');
GO

-- ══════════════════════════════════════════════════════════
--  3. PIPELINE AUDIT LOG — run history for every ADF run
-- ══════════════════════════════════════════════════════════

CREATE TABLE [dbo].[adf_pipeline_audit] (
    audit_id       INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name  NVARCHAR(256) NOT NULL,
    run_id         NVARCHAR(100) NOT NULL,
    status         NVARCHAR(50)  NOT NULL,  -- Running | Success | Failed
    triggered_by   NVARCHAR(256) NULL,
    rows_copied    BIGINT NULL,
    run_date       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    error_message  NVARCHAR(MAX) NULL,
    INDEX IX_audit_pipeline_date (pipeline_name, run_date DESC)
);
GO

-- ══════════════════════════════════════════════════════════
--  4. STORED PROCEDURES
-- ══════════════════════════════════════════════════════════

-- usp_update_watermark — called after each successful Copy activity
CREATE OR ALTER PROCEDURE [dbo].[usp_update_watermark]
    @tableName     NVARCHAR(256),
    @newWatermark  NVARCHAR(50),
    @rowsCopied    NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[adf_copy_config]
    SET    last_watermark_value = @newWatermark,
           updated_at           = SYSUTCDATETIME()
    WHERE  source_table = @tableName
      AND  load_type    = 'incremental';

    -- Log the update
    INSERT INTO [dbo].[adf_pipeline_audit]
        (pipeline_name, run_id, status, rows_copied)
    VALUES
        ('pl_ingest_sqlserver_to_adls', NEWID(), 'TableCopied', TRY_CAST(@rowsCopied AS BIGINT));
END;
GO

-- usp_update_cdc_watermark — called after each CDC extraction
CREATE OR ALTER PROCEDURE [dbo].[usp_update_cdc_watermark]
    @tableName     NVARCHAR(256),
    @newLSN        NVARCHAR(100),
    @rowsExtracted NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[adf_cdc_watermark]
    SET    last_processed_lsn  = @newLSN,
           last_processed_time = SYSUTCDATETIME(),
           rows_extracted_last = TRY_CAST(@rowsExtracted AS INT),
           updated_at          = SYSUTCDATETIME()
    WHERE  table_name = @tableName;
END;
GO

-- usp_adf_log_run — called at start and end of master orchestrator
CREATE OR ALTER PROCEDURE [dbo].[usp_adf_log_run]
    @pipelineName NVARCHAR(256),
    @runId        NVARCHAR(100),
    @status       NVARCHAR(50),
    @triggeredBy  NVARCHAR(256) = NULL,
    @errorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM [dbo].[adf_pipeline_audit] WHERE run_id = @runId)
        UPDATE [dbo].[adf_pipeline_audit]
        SET    status        = @status,
               error_message = @errorMessage,
               run_date      = SYSUTCDATETIME()
        WHERE  run_id = @runId;
    ELSE
        INSERT INTO [dbo].[adf_pipeline_audit]
            (pipeline_name, run_id, status, triggered_by, error_message)
        VALUES
            (@pipelineName, @runId, @status, @triggeredBy, @errorMessage);
END;
GO

-- ══════════════════════════════════════════════════════════
--  5. ENABLE CDC ON REQUIRED TABLES
--     Run this as sysadmin. CDC must be enabled on the DB
--     first, then per-table.
-- ══════════════════════════════════════════════════════════

-- Step 1: Enable CDC on database
EXEC sys.sp_cdc_enable_db;
GO

-- Step 2: Enable CDC on each table
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'customers',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'orders',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'products',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

-- Step 3: Grant CDC read permissions to ADF reader account
GRANT SELECT ON SCHEMA::cdc TO [omnioc_adf_reader];
GO

-- Step 4: Verify CDC is active
SELECT
    ct.name      AS table_name,
    cc.is_tracked_by_cdc,
    cc.column_name
FROM sys.tables ct
JOIN sys.change_tracking_columns cc ON ct.object_id = cc.object_id
WHERE ct.name IN ('customers','orders','products');

-- Or check cdc tables directly:
SELECT object_name(source_object_id) AS table_name, *
FROM cdc.change_tables;
GO

-- ══════════════════════════════════════════════════════════
--  6. USEFUL DIAGNOSTIC QUERIES
-- ══════════════════════════════════════════════════════════

-- Check current watermarks
-- SELECT * FROM [dbo].[adf_copy_config] ORDER BY copy_order;

-- Check CDC watermarks
-- SELECT * FROM [dbo].[adf_cdc_watermark] ORDER BY table_name;

-- Check pipeline run history
-- SELECT TOP 20 * FROM [dbo].[adf_pipeline_audit] ORDER BY run_date DESC;

-- Count CDC changes in window
-- SELECT table_name, COUNT(*) AS change_count
-- FROM cdc.dbo_customers_CT
-- GROUP BY table_name;
