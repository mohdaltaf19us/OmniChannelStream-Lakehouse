-- ============================================================
-- OmniOC ADF Control Tables + Stored Procedures
-- Run this on SQL Server BEFORE deploying ADF pipelines
-- Database: OmniOC
-- ============================================================

USE OmniOC;
GO

-- ============================================================
-- 1. WATERMARK CONTROL TABLE
--    Tracks last_loaded_at per source table for PL_02_JDBC_Incremental
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'adf_watermark_control')
BEGIN
    CREATE TABLE dbo.adf_watermark_control (
        source_schema   VARCHAR(50)   NOT NULL,
        source_table    VARCHAR(100)  NOT NULL,
        watermark_col   VARCHAR(100)  NOT NULL,
        last_loaded_at  DATETIME2(7)  NOT NULL DEFAULT '2020-01-01T00:00:00',
        last_run_at     DATETIME2(7)  NULL,
        rows_loaded     BIGINT        NOT NULL DEFAULT 0,
        status          VARCHAR(20)   NOT NULL DEFAULT 'ACTIVE',
        notes           NVARCHAR(500) NULL,
        CONSTRAINT PK_adf_watermark PRIMARY KEY (source_schema, source_table)
    );
    PRINT 'Created: dbo.adf_watermark_control';
END
GO

-- Seed watermark entries for all incremental tables
MERGE dbo.adf_watermark_control AS target
USING (VALUES
    ('dbo', 'orders',          'updated_at', '2020-01-01T00:00:00', 'Finance domain — incremental by updated_at'),
    ('dbo', 'payments',        'updated_at', '2020-01-01T00:00:00', 'Finance domain — incremental by updated_at'),
    ('dbo', 'order_line_items','updated_at', '2020-01-01T00:00:00', 'Finance domain — incremental by updated_at'),
    ('dbo', 'inventory',       'updated_at', '2020-01-01T00:00:00', 'Supply chain — stock level changes'),
    ('dbo', 'shipments',       'created_at', '2020-01-01T00:00:00', 'Supply chain — append only (no updates)')
) AS source (source_schema, source_table, watermark_col, last_loaded_at, notes)
ON target.source_schema = source.source_schema
   AND target.source_table = source.source_table
WHEN NOT MATCHED THEN
    INSERT (source_schema, source_table, watermark_col, last_loaded_at, notes)
    VALUES (source.source_schema, source.source_table, source.watermark_col, source.last_loaded_at, source.notes);
GO

-- ============================================================
-- 2. COPY CONFIG TABLE (Metadata-driven pipeline PL_04)
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'adf_copy_config')
BEGIN
    CREATE TABLE dbo.adf_copy_config (
        config_id       INT IDENTITY(1,1) PRIMARY KEY,
        source_type     VARCHAR(20)   NOT NULL DEFAULT 'sqlserver',
        source_schema   VARCHAR(50)   NOT NULL DEFAULT 'dbo',
        source_table    VARCHAR(100)  NOT NULL,
        load_type       VARCHAR(20)   NOT NULL DEFAULT 'full',
        watermark_col   VARCHAR(100)  NULL,
        target_domain   VARCHAR(50)   NOT NULL,
        target_format   VARCHAR(20)   NOT NULL DEFAULT 'parquet',
        is_active       BIT           NOT NULL DEFAULT 1,
        priority        INT           NOT NULL DEFAULT 100,
        last_run_at     DATETIME2(7)  NULL,
        last_run_status VARCHAR(20)   NULL,
        rows_last_run   BIGINT        NULL,
        notes           NVARCHAR(500) NULL
    );
    PRINT 'Created: dbo.adf_copy_config';
END
GO

-- Seed config rows — one row per source table
MERGE dbo.adf_copy_config AS target
USING (VALUES
    -- Full loads (small dimension tables)
    ('sqlserver','dbo','warehouses',    'full',        NULL,         'supply_chain','parquet', 1, 10, 'Static dim — full reload daily'),
    ('sqlserver','dbo','suppliers',     'full',        NULL,         'supply_chain','parquet', 1, 20, 'Rarely changes — full reload'),
    ('sqlserver','dbo','products',      'full',        NULL,         'supply_chain','parquet', 1, 30, 'SCD2 handled in DLT — send full snapshot'),
    -- Incremental loads (high-volume transactional tables)
    ('sqlserver','dbo','orders',        'incremental', 'updated_at', 'finance',     'parquet', 1, 40, 'High volume — watermark on updated_at'),
    ('sqlserver','dbo','order_line_items','incremental','updated_at','finance',     'parquet', 1, 50, 'High volume — watermark on updated_at'),
    ('sqlserver','dbo','payments',      'incremental', 'updated_at', 'finance',     'parquet', 1, 60, 'Payment status changes'),
    ('sqlserver','dbo','inventory',     'incremental', 'updated_at', 'supply_chain','parquet', 1, 70, 'Stock level changes'),
    ('sqlserver','dbo','shipments',     'incremental', 'created_at', 'supply_chain','parquet', 1, 80, 'Append only — no updates')
) AS source (source_type, source_schema, source_table, load_type, watermark_col, target_domain, target_format, is_active, priority, notes)
ON target.source_schema = source.source_schema
   AND target.source_table = source.source_table
WHEN NOT MATCHED THEN
    INSERT (source_type, source_schema, source_table, load_type, watermark_col,
            target_domain, target_format, is_active, priority, notes)
    VALUES (source.source_type, source.source_schema, source.source_table,
            source.load_type, source.watermark_col, source.target_domain,
            source.target_format, source.is_active, source.priority, source.notes);
GO

-- ============================================================
-- 3. STORED PROCEDURE: usp_update_watermark
--    Called by PL_02_JDBC_Incremental after each successful copy
-- ============================================================

CREATE OR ALTER PROCEDURE dbo.usp_update_watermark
    @source_schema  VARCHAR(50),
    @source_table   VARCHAR(100),
    @new_watermark  DATETIME2(7),
    @rows_loaded    BIGINT = 0
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.adf_watermark_control
    SET
        last_loaded_at = @new_watermark,
        last_run_at    = SYSUTCDATETIME(),
        rows_loaded    = @rows_loaded,
        status         = 'ACTIVE'
    WHERE
        source_schema = @source_schema
        AND source_table = @source_table;

    IF @@ROWCOUNT = 0
    BEGIN
        RAISERROR('Watermark row not found for %s.%s — run setup script first.', 16, 1, @source_schema, @source_table);
        RETURN;
    END

    PRINT CONCAT('Watermark updated: ', @source_schema, '.', @source_table,
                 ' → ', CONVERT(VARCHAR, @new_watermark, 126),
                 ' (', @rows_loaded, ' rows)');
END;
GO

-- ============================================================
-- 4. STORED PROCEDURE: usp_update_copy_config_status
--    Called by PL_04_Metadata_Driven_Copy after each table copy
-- ============================================================

CREATE OR ALTER PROCEDURE dbo.usp_update_copy_config_status
    @config_id  INT,
    @status     VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.adf_copy_config
    SET
        last_run_at     = SYSUTCDATETIME(),
        last_run_status = @status
    WHERE config_id = @config_id;

    PRINT CONCAT('Config status updated: config_id=', @config_id, ' status=', @status);
END;
GO

-- ============================================================
-- 5. ENABLE CDC ON REQUIRED TABLES
--    CDC must be enabled at database level first, then per table
-- ============================================================

-- Enable CDC on the database (requires sysadmin or db_owner)
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT 'CDC enabled on database: ' + DB_NAME();
END
GO

-- Enable CDC on dbo.customers (tracked: all columns)
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_customers'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema         = 'dbo',
        @source_name           = 'customers',
        @capture_instance      = 'dbo_customers',
        @role_name             = NULL,
        @supports_net_changes  = 1,
        @captured_column_list  = NULL;   -- NULL = capture all columns
    PRINT 'CDC enabled: dbo.customers';
END
GO

-- Enable CDC on dbo.products (price changes are critical for DV satellite)
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_products'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema         = 'dbo',
        @source_name           = 'products',
        @capture_instance      = 'dbo_products',
        @role_name             = NULL,
        @supports_net_changes  = 1,
        @captured_column_list  = NULL;
    PRINT 'CDC enabled: dbo.products';
END
GO

-- Enable CDC on dbo.payments (status lifecycle: pending→completed→refunded)
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_payments'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema         = 'dbo',
        @source_name           = 'payments',
        @capture_instance      = 'dbo_payments',
        @role_name             = NULL,
        @supports_net_changes  = 1,
        @captured_column_list  = NULL;
    PRINT 'CDC enabled: dbo.payments';
END
GO

-- ============================================================
-- 6. VERIFY SETUP
-- ============================================================

SELECT 'Watermark Control' AS table_name, COUNT(*) AS rows FROM dbo.adf_watermark_control
UNION ALL
SELECT 'Copy Config',       COUNT(*) FROM dbo.adf_copy_config
UNION ALL
SELECT 'CDC Tables',        COUNT(*) FROM cdc.change_tables;

SELECT
    cc.name    AS capture_instance,
    t.name     AS source_table,
    cc.is_net_changes_enabled,
    cc.create_date
FROM cdc.change_tables cc
JOIN sys.tables t ON cc.source_object_id = t.object_id
ORDER BY cc.create_date;
GO

PRINT '=== ADF Control Tables Setup Complete ===';
