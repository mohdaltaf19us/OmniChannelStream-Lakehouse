# Databricks notebook source
# MAGIC %md
# MAGIC # BRONZE DDL - SETUP NOTEBOOK

# COMMAND ----------

# MAGIC %md
# MAGIC # All Tables
# MAGIC Creates all bronze Delta tables with explicit schemas, partitioning, and Delta properties.
# MAGIC
# MAGIC **Run this ONCE before any ingestion notebook. Tables are idempotent (CREATE IF NOT EXISTS).**
# MAGIC
# MAGIC | Layer | Domain | Tables |
# MAGIC |-------|--------|--------|
# MAGIC | Batch | Supply Chain | suppliers_raw, products_raw, warehouses_raw, shipments_raw, supplier_contracts_raw |
# MAGIC | Batch | Finance | customers_raw, orders_raw, order_lines_raw, payments_raw, refunds_raw, exchange_rates_raw |
# MAGIC | Batch | CDC | cdc_customers_raw, cdc_products_raw |
# MAGIC | Batch | DQ Testing | poison_pills_raw |
# MAGIC | Streaming | Clickstream | clickstream_raw_eh, clickstream_raw_kafka |
# MAGIC | Streaming | IoT | device_telemetry_raw_eh, device_telemetry_raw_kafka |
# MAGIC | Streaming | Finance | order_events_raw_eh, order_events_raw_kafka |
# MAGIC | Streaming | Monitoring | dq_alerts_raw_eh, dq_alerts_raw_kafka |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Summary
# MAGIC
# MAGIC | Property | Value | Why |
# MAGIC |---|---|---|
# MAGIC | `autoOptimize.optimizeWrite` | true | Coalesces small files during write — avoids small file problem |
# MAGIC | `autoOptimize.autoCompact` | true | Auto-runs OPTIMIZE after writes — keeps file sizes healthy |
# MAGIC | `enableChangeDataFeed` | true | Enables CDF for Silver MERGE efficiency — reads only changed rows |
# MAGIC | `columnMapping.mode` = name | name | Allows column rename/drop without full rewrite |
# MAGIC | `dataSkippingNumIndexedCols` | 5-10 | More columns in min/max stats = better file pruning |
# MAGIC | `zOrderCols` | Per table | Co-locates related data — reduces files scanned per query |
# MAGIC | `PARTITIONED BY (_ingest_date)` | Date | Enables partition pruning for time-range queries |
# MAGIC | CDC tables: + operation | operation | Silver MERGE can filter DELETE partition directly |
# MAGIC | `GENERATED ALWAYS AS` | _ingest_date | Auto-populated from _ingest_time — no manual partition key |
# MAGIC

# COMMAND ----------

# spark.sql("DROP CATALOG IF EXISTS omnioc_dev CASCADE")

# COMMAND ----------

# DBTITLE 1, Create Catalog with Managed Location
# Check if catalog exists
catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
if 'omnioc_dev' not in catalogs:
    spark.sql("CREATE CATALOG omnioc_dev")
    print("✅ Catalog omnioc_dev created")
else:
    print("✅ Catalog omnioc_dev already exists")

# COMMAND ----------

# DBTITLE 1, Create Schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS omnioc_dev.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS omnioc_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS omnioc_dev.gold")
print("✅ Schemas created: bronze, silver, gold")

# COMMAND ----------

# DBTITLE 1, Verify
spark.sql("SHOW SCHEMAS IN omnioc_dev").show()

# COMMAND ----------

# DBTITLE 1, Verify ADLS Access
files = dbutils.fs.ls("abfss://landing@omniocdeveusst01.dfs.core.windows.net/")
print("✅ ADLS landing/ accessible — folders found:")
for f in files:
    print(f"   {f.path}")

# COMMAND ----------

# DBTITLE 1, Verify Secret Scope
conn = dbutils.secrets.get('omnioc-kv-scope', 'eventhub-clickstream-connection-string')
print("✅ Secret scope working — eventhub-clickstream-connection-string resolved")
print("\n🎯 All checks passed — ready to run notebooks 01 → 05")

# COMMAND ----------

# DBTITLE 1,Use Catalog
spark.sql("USE CATALOG omnioc_dev")
spark.sql("USE SCHEMA bronze")

# BASE = "abfss://bronze@omniocdeveusst01.dfs.core.windows.net"
print(f"✅ Target location: {BASE}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Supply Chain CSV Table

# COMMAND ----------

# DBTITLE 1,suppliers_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.suppliers_raw (
    supplier_id         STRING    COMMENT 'Unique supplier identifier (SUP-XXXXX)',
    supplier_name       STRING    COMMENT 'Legal supplier name',
    country             STRING    COMMENT 'Supplier country of origin',
    city                STRING    COMMENT 'Supplier city',
    contact_email       STRING    COMMENT 'Primary contact email',
    contact_phone       STRING    COMMENT 'Primary contact phone',
    rating              DOUBLE    COMMENT 'Supplier performance rating 0-5',
    lead_time_days      INT       COMMENT 'Average lead time in days',
    payment_terms_days  INT       COMMENT 'Payment terms in days (net 30/60/90)',
    is_active           BOOLEAN   COMMENT 'Whether supplier is currently active',
    contract_start_date STRING    COMMENT 'Active contract start date',
    created_at          STRING    COMMENT 'Record creation timestamp from source',
    updated_at          STRING    COMMENT 'Record last update timestamp from source',
    _ingest_time        TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file        STRING    COMMENT 'Source ADLS file path',
    _source_system      STRING    COMMENT 'Source: adls_csv',
    _ingest_date        DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                  COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/suppliers_raw'
COMMENT 'Bronze supplier master data from ADLS CSV. Full reload pattern.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'supplier_id,country',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'supply_chain',
    'quality.source'                    = 'adls_csv'
)
""")
print("✅ suppliers_raw")

# COMMAND ----------

# DBTITLE 1,products_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.products_raw (
    product_id          STRING    COMMENT 'Unique product identifier (PRD-XXXXX)',
    product_name        STRING    COMMENT 'Product display name',
    sku                 STRING    COMMENT 'Stock keeping unit code',
    category            STRING    COMMENT 'Top-level product category',
    sub_category        STRING    COMMENT 'Sub-category within category',
    unit_cost           DOUBLE    COMMENT 'Cost price per unit in USD',
    unit_price          DOUBLE    COMMENT 'Retail price per unit in USD',
    weight_kg           DOUBLE    COMMENT 'Product weight in kilograms',
    is_active           BOOLEAN   COMMENT 'Whether product is currently active',
    supplier_id         STRING    COMMENT 'FK to suppliers_raw.supplier_id',
    reorder_point       INT       COMMENT 'Inventory level triggering reorder',
    reorder_quantity    INT       COMMENT 'Quantity to order when reorder triggered',
    created_at          STRING    COMMENT 'Record creation timestamp from source',
    updated_at          STRING    COMMENT 'Record last update timestamp from source',
    _ingest_time        TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file        STRING    COMMENT 'Source ADLS file path',
    _source_system      STRING    COMMENT 'Source: adls_csv',
    _ingest_date        DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                  COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/products_raw'
COMMENT 'Bronze product catalog from ADLS CSV. 5000+ active SKUs.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '6',
    'pipelines.autoOptimize.zOrderCols' = 'product_id,category,supplier_id',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'supply_chain',
    'quality.source'                    = 'adls_csv'
)
""")
print("✅ products_raw")

# COMMAND ----------

# DBTITLE 1,warehouses_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.warehouses_raw (
    warehouse_id        STRING    COMMENT 'Unique warehouse identifier (WH-XX)',
    warehouse_name      STRING    COMMENT 'Warehouse display name',
    region              STRING    COMMENT 'Geographic region (NA, EMEA, APAC)',
    country             STRING    COMMENT 'Country of warehouse location',
    city                STRING    COMMENT 'City of warehouse location',
    state               STRING    COMMENT 'State or province',
    zip_code            STRING    COMMENT 'Postal or ZIP code',
    address             STRING    COMMENT 'Street address',
    capacity_sqft       INT       COMMENT 'Warehouse capacity in square feet',
    max_sku_capacity    INT       COMMENT 'Maximum number of distinct SKUs',
    manager_name        STRING    COMMENT 'Warehouse manager full name',
    manager_email       STRING    COMMENT 'Warehouse manager email',
    is_active           BOOLEAN   COMMENT 'Whether warehouse is operational',
    opened_date         STRING    COMMENT 'Warehouse opening date',
    _ingest_time        TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file        STRING    COMMENT 'Source ADLS file path',
    _source_system      STRING    COMMENT 'Source: adls_csv',
    _ingest_date        DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                  COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/warehouses_raw'
COMMENT 'Bronze warehouse master data from ADLS CSV. Small dimension table 20 rows.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '4',
    'pipelines.autoOptimize.zOrderCols' = 'warehouse_id,region,country',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'supply_chain',
    'quality.source'                    = 'adls_csv'
)
""")
print("✅ warehouses_raw")

# COMMAND ----------

# DBTITLE 1,exchange_rates_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.exchange_rates_raw (
    rate_date           STRING    COMMENT 'Date the rate applies to (YYYY-MM-DD)',
    base_currency       STRING    COMMENT 'Base currency code (USD)',
    target_currency     STRING    COMMENT 'Target currency code (CAD, EUR, GBP...)',
    exchange_rate       DOUBLE    COMMENT 'Exchange rate: 1 base = X target',
    source              STRING    COMMENT 'Rate source (ECB, FX_API)',
    created_at          STRING    COMMENT 'Record creation timestamp',
    _ingest_time        TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file        STRING    COMMENT 'Source ADLS file path',
    _source_system      STRING    COMMENT 'Source: adls_csv',
    _ingest_date        DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                  COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/exchange_rates_raw'
COMMENT 'Bronze FX exchange rates from ADLS CSV. Daily frequency.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '4',
    'pipelines.autoOptimize.zOrderCols' = 'rate_date,base_currency,target_currency',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'finance',
    'quality.source'                    = 'adls_csv'
)
""")
print("✅ exchange_rates_raw")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Finance JSON Tables

# COMMAND ----------

# DBTITLE 1,customers_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.customers_raw (
    customer_id             STRING    COMMENT 'Unique customer identifier (CUST-XXXXX)',
    first_name              STRING    COMMENT 'Customer first name',
    last_name               STRING    COMMENT 'Customer last name',
    email                   STRING    COMMENT 'Customer email address',
    phone                   STRING    COMMENT 'Customer phone number',
    date_of_birth           STRING    COMMENT 'Customer date of birth',
    gender                  STRING    COMMENT 'Customer gender',
    segment                 STRING    COMMENT 'Customer segment (VIP, Standard, New)',
    country                 STRING    COMMENT 'Country of residence',
    city                    STRING    COMMENT 'City of residence',
    state                   STRING    COMMENT 'State or province',
    zip_code                STRING    COMMENT 'Postal code',
    address_line1           STRING    COMMENT 'Street address',
    signup_date             STRING    COMMENT 'Account creation date',
    loyalty_points          INT       COMMENT 'Current loyalty points balance',
    preferred_currency      STRING    COMMENT 'Preferred currency code',
    is_email_verified       BOOLEAN   COMMENT 'Email verification status',
    is_active               BOOLEAN   COMMENT 'Account active status',
    created_at              STRING    COMMENT 'Record creation timestamp',
    updated_at              STRING    COMMENT 'Record last update timestamp',
    _ingest_time            TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file            STRING    COMMENT 'Source ADLS file path',
    _source_system          STRING    COMMENT 'Source: adls_json',
    _rescued_data           STRING    COMMENT 'Autoloader rescued columns (schema evolution)',
    _ingest_date            DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                      COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/customers_raw'
COMMENT 'Bronze customer master data from ADLS JSON. 10k+ customers.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'customer_id,country,segment',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'finance',
    'quality.source'                    = 'adls_json'
)
""")
print("✅ customers_raw")

# COMMAND ----------

# DBTITLE 1,orders_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.orders_raw (
    order_id                    STRING    COMMENT 'Unique order identifier (ORD-XXXXXXXX)',
    customer_id                 STRING    COMMENT 'FK to customers_raw.customer_id',
    warehouse_id                STRING    COMMENT 'FK to warehouses_raw.warehouse_id',
    order_date                  STRING    COMMENT 'Order placement date',
    status                      STRING    COMMENT 'Order status: pending/shipped/delivered/cancelled',
    channel                     STRING    COMMENT 'Sales channel: web/mobile/store',
    total_amount                DOUBLE    COMMENT 'Total order amount before discounts',
    discount_amount             DOUBLE    COMMENT 'Total discount applied',
    tax_amount                  DOUBLE    COMMENT 'Tax amount',
    shipping_amount             DOUBLE    COMMENT 'Shipping cost',
    currency                    STRING    COMMENT 'Order currency code',
    shipping_address_city       STRING    COMMENT 'Delivery city',
    shipping_address_country    STRING    COMMENT 'Delivery country',
    ship_date                   STRING    COMMENT 'Actual ship date',
    delivery_date               STRING    COMMENT 'Actual or estimated delivery date',
    promo_code                  STRING    COMMENT 'Promotional code applied (nullable)',
    created_at                  STRING    COMMENT 'Record creation timestamp',
    updated_at                  STRING    COMMENT 'Record last update timestamp',
    _ingest_time                TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file                STRING    COMMENT 'Source ADLS file path',
    _source_system              STRING    COMMENT 'Source: adls_json',
    _rescued_data               STRING    COMMENT 'Autoloader rescued columns',
    _ingest_date                DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                          COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/orders_raw'
COMMENT 'Bronze order transactions from ADLS JSON. 100k+ orders. Largest bronze table.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '10',
    'pipelines.autoOptimize.zOrderCols' = 'customer_id,order_date,status,channel',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'finance',
    'quality.source'                    = 'adls_json'
)
""")
print("✅ orders_raw")

# COMMAND ----------

# DBTITLE 1,order_lines_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.order_lines_raw (
    line_id         STRING    COMMENT 'Unique order line identifier',
    order_id        STRING    COMMENT 'FK to orders_raw.order_id',
    product_id      STRING    COMMENT 'FK to products_raw.product_id',
    quantity        INT       COMMENT 'Quantity ordered',
    unit_price      DOUBLE    COMMENT 'Unit price at time of order',
    discount_pct    DOUBLE    COMMENT 'Discount percentage on this line',
    line_total      DOUBLE    COMMENT 'Line total after discount',
    is_gift         BOOLEAN   COMMENT 'Whether item is a gift',
    gift_message    STRING    COMMENT 'Gift message if applicable',
    created_at      STRING    COMMENT 'Record creation timestamp',
    updated_at      STRING    COMMENT 'Record last update timestamp',
    _ingest_time    TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file    STRING    COMMENT 'Source ADLS file path',
    _source_system  STRING    COMMENT 'Source: adls_json',
    _rescued_data   STRING    COMMENT 'Autoloader rescued columns',
    _ingest_date    DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                              COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/order_lines_raw'
COMMENT 'Bronze order line items from ADLS JSON. 150k+ rows. Child of orders_raw.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '6',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,product_id',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'finance',
    'quality.source'                    = 'adls_json'
)
""")
print("✅ order_lines_raw")

# COMMAND ----------

# DBTITLE 1,payments_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.payments_raw (
    payment_id      STRING    COMMENT 'Unique payment identifier',
    order_id        STRING    COMMENT 'FK to orders_raw.order_id',
    amount          DOUBLE    COMMENT 'Payment amount',
    currency        STRING    COMMENT 'Payment currency code',
    method          STRING    COMMENT 'Payment method: card/paypal/bank_transfer',
    status          STRING    COMMENT 'Payment status: pending/completed/failed/refunded',
    gateway         STRING    COMMENT 'Payment gateway: Stripe/Adyen/Braintree',
    transaction_id  STRING    COMMENT 'Gateway transaction reference',
    card_last_four  STRING    COMMENT 'Last 4 digits of card (masked)',
    billing_country STRING    COMMENT 'Billing country',
    created_at      STRING    COMMENT 'Payment initiation timestamp',
    paid_at         STRING    COMMENT 'Payment completion timestamp',
    updated_at      STRING    COMMENT 'Record last update timestamp',
    _ingest_time    TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file    STRING    COMMENT 'Source ADLS file path',
    _source_system  STRING    COMMENT 'Source: adls_json',
    _rescued_data   STRING    COMMENT 'Autoloader rescued columns',
    _ingest_date    DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                              COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/payments_raw'
COMMENT 'Bronze payment transactions from ADLS JSON. 48k+ payments.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '7',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,status,method',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'finance',
    'quality.source'                    = 'adls_json'
)
""")
print("✅ payments_raw")


# COMMAND ----------

# DBTITLE 1,refunds_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.refunds_raw (
    refund_id       STRING    COMMENT 'Unique refund identifier',
    order_id        STRING    COMMENT 'FK to orders_raw.order_id',
    refund_amount   DOUBLE    COMMENT 'Refund amount',
    currency        STRING    COMMENT 'Refund currency code',
    reason          STRING    COMMENT 'Refund reason category',
    refund_type     STRING    COMMENT 'full or partial',
    status          STRING    COMMENT 'Refund status: pending/processed/rejected',
    requested_at    STRING    COMMENT 'Refund request timestamp',
    processed_at    STRING    COMMENT 'Refund processing timestamp',
    notes           STRING    COMMENT 'Additional notes',
    _ingest_time    TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file    STRING    COMMENT 'Source ADLS file path',
    _source_system  STRING    COMMENT 'Source: adls_json',
    _rescued_data   STRING    COMMENT 'Autoloader rescued columns',
    _ingest_date    DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                              COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/refunds_raw'
COMMENT 'Bronze refund records from ADLS JSON.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,status,reason',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'finance',
    'quality.source'                    = 'adls_json'
)
""")
print("✅ refunds_raw")


# COMMAND ----------

# DBTITLE 1,supplier_contracts_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.supplier_contracts_raw (
    contract_id         STRING    COMMENT 'Unique contract identifier',
    supplier_id         STRING    COMMENT 'FK to suppliers_raw.supplier_id',
    contract_type       STRING    COMMENT 'Contract type: annual/multi-year/spot',
    start_date          STRING    COMMENT 'Contract start date',
    end_date            STRING    COMMENT 'Contract end date',
    total_value_usd     DOUBLE    COMMENT 'Total contract value in USD',
    payment_currency    STRING    COMMENT 'Payment currency code',
    sla_terms           STRING    COMMENT 'SLA terms description',
    auto_renewal        BOOLEAN   COMMENT 'Whether contract auto-renews',
    is_active           BOOLEAN   COMMENT 'Whether contract is currently active',
    created_at          STRING    COMMENT 'Record creation timestamp',
    _ingest_time        TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file        STRING    COMMENT 'Source ADLS file path',
    _source_system      STRING    COMMENT 'Source: adls_json',
    _rescued_data       STRING    COMMENT 'Autoloader rescued columns',
    _ingest_date        DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                  COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/supplier_contracts_raw'
COMMENT 'Bronze supplier contracts from ADLS JSON.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'supplier_id,is_active',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'supply_chain',
    'quality.source'                    = 'adls_json'
)
""")
print("✅ supplier_contracts_raw")


# COMMAND ----------

# MAGIC %md 
# MAGIC ## CDC Tables

# COMMAND ----------

# DBTITLE 1,cdc_customers_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.cdc_customers_raw (
    cdc_event_id        STRING    COMMENT 'Unique CDC event identifier',
    operation           STRING    COMMENT 'CDC operation: INSERT, UPDATE, DELETE',
    table_name          STRING    COMMENT 'Source table name',
    commit_timestamp    STRING    COMMENT 'Source DB commit timestamp',
    lsn                 STRING    COMMENT 'Log Sequence Number from source DB',
    before              STRING    COMMENT 'JSON payload: row state before change',
    after               STRING    COMMENT 'JSON payload: row state after change',
    _ingest_time        TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file        STRING    COMMENT 'Source ADLS file path',
    _source_system      STRING    COMMENT 'Source: adls_cdc',
    _rescued_data       STRING    COMMENT 'Autoloader rescued columns',
    _ingest_date        DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                  COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date, operation)
-- LOCATION '{BASE}/cdc_customers_raw'
COMMENT 'Bronze CDC customer change events. Partitioned by date AND operation for Silver MERGE efficiency.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'operation,commit_timestamp',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'cdc',
    'quality.source'                    = 'adls_cdc'
)
""")
print("✅ cdc_customers_raw")


# COMMAND ----------

# DBTITLE 1,cdc_products_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.cdc_products_raw (
    cdc_event_id        STRING    COMMENT 'Unique CDC event identifier',
    operation           STRING    COMMENT 'CDC operation: INSERT, UPDATE, DELETE',
    table_name          STRING    COMMENT 'Source table name',
    commit_timestamp    STRING    COMMENT 'Source DB commit timestamp',
    lsn                 STRING    COMMENT 'Log Sequence Number from source DB',
    before              STRING    COMMENT 'JSON payload: row state before change',
    after               STRING    COMMENT 'JSON payload: row state after change',
    _ingest_time        TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file        STRING    COMMENT 'Source ADLS file path',
    _source_system      STRING    COMMENT 'Source: adls_cdc',
    _rescued_data       STRING    COMMENT 'Autoloader rescued columns',
    _ingest_date        DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                  COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date, operation)
-- LOCATION '{BASE}/cdc_products_raw'
COMMENT 'Bronze CDC product change events. Partitioned by date AND operation for Silver MERGE efficiency.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'operation,commit_timestamp',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'cdc',
    'quality.source'                    = 'adls_cdc'
)
""")
print("✅ cdc_products_raw")

# COMMAND ----------

# DBTITLE 1,poison_pills_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.poison_pills_raw (
    order_id                STRING    COMMENT 'Order ID (may be malformed)',
    customer_id             STRING    COMMENT 'Customer ID (may be null/invalid)',
    total_amount            STRING    COMMENT 'String type: may contain non-numeric values',
    order_date              STRING    COMMENT 'Date field (may be invalid format)',
    _dq_issue               STRING    COMMENT 'Description of intentional DQ issue injected',
    new_field_not_in_schema STRING    COMMENT 'Schema evolution test field',
    another_new_field       STRING    COMMENT 'Additional schema evolution test field',
    _ingest_time            TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file            STRING    COMMENT 'Source ADLS file path',
    _source_system          STRING    COMMENT 'Source: adls_bad_data',
    _rescued_data           STRING    COMMENT 'Autoloader rescued columns',
    _ingest_date            DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                      COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/poison_pills_raw'
COMMENT 'Bronze intentionally malformed records for DQ pipeline testing. CDF disabled intentionally.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'false',
    'delta.columnMapping.mode'          = 'name',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'dq_testing',
    'quality.source'                    = 'adls_bad_data'
)
""")
print("✅ poison_pills_raw")

# COMMAND ----------

# DBTITLE 1,shipments_raw
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.shipments_raw (
    shipment_id                 STRING    COMMENT 'Unique shipment identifier (SHIP-XXXXXXXX)',
    supplier_id                 STRING    COMMENT 'FK to suppliers_raw.supplier_id',
    destination_warehouse_id    STRING    COMMENT 'FK to warehouses_raw.warehouse_id',
    product_id                  STRING    COMMENT 'FK to products_raw.product_id',
    quantity                    INT       COMMENT 'Units shipped',
    unit_cost                   DOUBLE    COMMENT 'Unit cost at time of shipment',
    total_cost                  DOUBLE    COMMENT 'Total shipment cost',
    ship_date                   STRING    COMMENT 'Actual ship date',
    eta_date                    STRING    COMMENT 'Estimated arrival date',
    actual_arrival_date         STRING    COMMENT 'Actual arrival date (nullable if in transit)',
    carrier                     STRING    COMMENT 'Shipping carrier name',
    tracking_number             STRING    COMMENT 'Carrier tracking number',
    status                      STRING    COMMENT 'in_transit/delivered/delayed/lost',
    created_at                  STRING    COMMENT 'Record creation timestamp',
    _ingest_time                TIMESTAMP COMMENT 'Databricks ingestion timestamp',
    _source_file                STRING    COMMENT 'Source ADLS file path',
    _source_system              STRING    COMMENT 'Source: adls_parquet',
    _ingest_date                DATE      GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                          COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/shipments_raw'
COMMENT 'Bronze shipment records from ADLS Parquet. 30k+ shipments.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'supplier_id,destination_warehouse_id,status',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'supply_chain',
    'quality.source'                    = 'adls_parquet'
)
""")
print("✅ shipments_raw")

# COMMAND ----------

# MAGIC %md ## Streaming Tables

# COMMAND ----------

# DBTITLE 1,clickstream_raw_eh
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.clickstream_raw_eh (
    event_id            STRING        COMMENT 'Unique event identifier',
    event_type          STRING        COMMENT 'Type: page_view, click, purchase, search',
    session_id          STRING        COMMENT 'User session identifier',
    user_id             STRING        COMMENT 'User identifier (nullable for anonymous)',
    device_type         STRING        COMMENT 'desktop, mobile, tablet',
    event_time          STRING        COMMENT 'Event timestamp from source',
    processing_time     STRING        COMMENT 'Processing timestamp from producer',
    page_url            STRING        COMMENT 'Page URL where event occurred',
    referrer_url        STRING        COMMENT 'Referring URL',
    product_id          STRING        COMMENT 'Product ID if applicable',
    category            STRING        COMMENT 'Product category',
    search_query        STRING        COMMENT 'Search query if search event',
    cart_value          DOUBLE        COMMENT 'Cart value at time of event',
    purchase_amount     DOUBLE        COMMENT 'Purchase amount if purchase event',
    ip_address          STRING        COMMENT 'Masked client IP address',
    user_agent          STRING        COMMENT 'Browser user agent string',
    geo_country         STRING        COMMENT 'Country derived from IP',
    geo_city            STRING        COMMENT 'City derived from IP',
    session_page_count  INT           COMMENT 'Page count within session',
    _is_late_arrival    BOOLEAN       COMMENT 'DQ flag: event arrived late',
    _is_poison_pill     BOOLEAN       COMMENT 'DQ flag: intentionally bad record',
    _eh_enqueued_time   TIMESTAMP     COMMENT 'Event Hubs enqueue timestamp',
    _eh_partition       STRING        COMMENT 'Event Hubs partition ID',
    _eh_offset          STRING        COMMENT 'Event Hubs offset',
    _eh_sequence        LONG          COMMENT 'Event Hubs sequence number',
    _eh_publisher       STRING        COMMENT 'Event Hubs publisher',
    _ingest_time        TIMESTAMP     COMMENT 'Databricks ingestion timestamp',
    _source_system      STRING        COMMENT 'Source: eventhubs_native',
    _ingest_date        DATE          GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                      COMMENT 'Partition column derived from ingest time'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/clickstream_raw_eh'
COMMENT 'Bronze clickstream events from Azure Event Hubs native connector.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'event_type,user_id,geo_country',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'clickstream',
    'quality.source'                    = 'eventhubs_native'
)
""")
print("✅ clickstream_raw_eh")

# COMMAND ----------

# DBTITLE 1,clickstream_raw_kafka
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.clickstream_raw_kafka (
    event_id            STRING        COMMENT 'Unique event identifier',
    event_type          STRING        COMMENT 'Type: page_view, click, purchase, search',
    session_id          STRING        COMMENT 'User session identifier',
    user_id             STRING        COMMENT 'User identifier',
    device_type         STRING        COMMENT 'desktop, mobile, tablet',
    event_time          STRING        COMMENT 'Event timestamp from source',
    processing_time     STRING        COMMENT 'Processing timestamp from producer',
    page_url            STRING        COMMENT 'Page URL',
    referrer_url        STRING        COMMENT 'Referring URL',
    product_id          STRING        COMMENT 'Product ID if applicable',
    category            STRING        COMMENT 'Product category',
    search_query        STRING        COMMENT 'Search query if search event',
    cart_value          DOUBLE        COMMENT 'Cart value at time of event',
    purchase_amount     DOUBLE        COMMENT 'Purchase amount if purchase event',
    ip_address          STRING        COMMENT 'Masked client IP address',
    user_agent          STRING        COMMENT 'Browser user agent string',
    geo_country         STRING        COMMENT 'Country derived from IP',
    geo_city            STRING        COMMENT 'City derived from IP',
    session_page_count  INT           COMMENT 'Page count within session',
    _is_late_arrival    BOOLEAN       COMMENT 'DQ flag: event arrived late',
    _is_poison_pill     BOOLEAN       COMMENT 'DQ flag: intentionally bad record',
    _eh_enqueued_time   TIMESTAMP     COMMENT 'Kafka message timestamp',
    _eh_partition       STRING        COMMENT 'Kafka partition ID',
    _eh_offset          STRING        COMMENT 'Kafka offset',
    _ingest_time        TIMESTAMP     COMMENT 'Databricks ingestion timestamp',
    _source_system      STRING        COMMENT 'Source: eventhubs_kafka',
    _ingest_date        DATE          GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                      COMMENT 'Partition column derived from ingest time'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/clickstream_raw_kafka'
COMMENT 'Bronze clickstream events from Azure Event Hubs via Kafka connector. No Maven library required.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'event_type,user_id,geo_country',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'clickstream',
    'quality.source'                    = 'eventhubs_kafka'
)
""")
print("✅ clickstream_raw_kafka")

# COMMAND ----------

# DBTITLE 1,device_telemetry_raw_eh
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.device_telemetry_raw_eh (
    telemetry_id           STRING        COMMENT 'Unique telemetry event identifier (UUID)',
    device_id              STRING        COMMENT 'Device identifier DEV-XXXXX — NULL in poison pill rows',
    device_type            STRING        COMMENT 'router | sensor | gateway | edge_node | pos_terminal',
    firmware_version       STRING        COMMENT 'Device firmware version string (e.g. v2.1.4)',
    location_id            STRING        COMMENT 'FK → warehouses_raw.warehouse_id',
    event_time             STRING        COMMENT 'Sensor event timestamp — may be backdated for late arrivals',
    processing_time        STRING        COMMENT 'Producer wall-clock time at message creation',
    cpu_pct                DOUBLE        COMMENT 'CPU utilisation 0–100 — may be -999 in poison pill rows',
    memory_pct             DOUBLE        COMMENT 'Memory utilisation percentage 0–100',
    disk_pct               DOUBLE        COMMENT 'Disk utilisation percentage 0–100',
    temperature_c          DOUBLE        COMMENT 'Device temperature in Celsius — NULL when source has non-numeric value',
    network_in_kbps        DOUBLE        COMMENT 'Inbound network throughput in kbps',
    network_out_kbps       DOUBLE        COMMENT 'Outbound network throughput in kbps',
    error_count            INT           COMMENT 'Error count since last heartbeat',
    uptime_seconds         INT           COMMENT 'Device uptime since last reboot in seconds',
    status                 STRING        COMMENT 'online | degraded | offline',
    alert_triggered        BOOLEAN       COMMENT 'Whether device triggered a threshold alert this cycle',
    _is_late_arrival       BOOLEAN       COMMENT 'DQ flag: event_time is >10 min before processing_time',
    _is_poison_pill        BOOLEAN       COMMENT 'DQ flag: intentionally corrupt record for pipeline testing',
    metrics_raw            STRING        COMMENT 'Original nested metrics JSON preserved for audit and reprocessing',
    _eh_enqueued_time      TIMESTAMP     COMMENT 'Event Hubs server-side enqueue timestamp',
    _eh_partition          STRING        COMMENT 'Event Hubs partition ID',
    _eh_offset             STRING        COMMENT 'Event Hubs offset within partition',
    _eh_sequence           LONG          COMMENT 'Event Hubs sequence number within partition',
    _eh_publisher          STRING        COMMENT 'Event Hubs publisher name (nullable)',
    _ingest_time           TIMESTAMP     COMMENT 'Databricks ingestion timestamp',
    _source_system         STRING        COMMENT 'Source: eventhubs_native',
    _ingest_date           DATE          GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                         COMMENT 'Partition column derived from ingest time'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/device_telemetry_raw_eh'
COMMENT 'Bronze IoT device telemetry from Azure Event Hubs native connector. Nested metrics struct flattened; raw JSON preserved.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'device_id,status,device_type',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'iot',
    'quality.source'                    = 'eventhubs_native'
)
""")
print("✅ device_telemetry_raw_eh")

# COMMAND ----------

# DBTITLE 1,order_events_raw_eh
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.order_events_raw_eh (
    event_id               STRING        COMMENT 'Unique event identifier (UUID)',
    event_type             STRING        COMMENT 'order_placed | order_confirmed | order_shipped | order_delivered | order_cancelled | order_updated',
    order_id               STRING        COMMENT 'FK → orders_raw.order_id — NULL in poison pill rows',
    customer_id            STRING        COMMENT 'FK → customers_raw.customer_id — NULL in poison pill rows',
    warehouse_id           STRING        COMMENT 'FK → warehouses_raw.warehouse_id',
    event_time             STRING        COMMENT 'Timestamp when order event occurred',
    order_date             STRING        COMMENT 'Date the original order was placed',
    status                 STRING        COMMENT 'placed | confirmed | shipped | delivered | cancelled | updated',
    channel                STRING        COMMENT 'web | mobile_app | marketplace | in_store',
    currency               STRING        COMMENT 'ISO 4217 transaction currency',
    total_amount           DOUBLE        COMMENT 'Gross order total — may be negative in poison pill rows',
    discount_amount        DOUBLE        COMMENT 'Total discount applied',
    tax_amount             DOUBLE        COMMENT 'Tax charged on order',
    shipping_amount        DOUBLE        COMMENT 'Shipping and handling charge',
    line_items             STRING        COMMENT 'Array of line item objects serialised as JSON string — parsed in Silver',
    shipping_country       STRING        COMMENT 'Shipping destination country',
    carrier                STRING        COMMENT 'Logistics carrier — populated for order_shipped events only',
    tracking_number        STRING        COMMENT 'Carrier tracking reference — populated for order_shipped events only',
    cancellation_reason    STRING        COMMENT 'Cancellation reason — populated for order_cancelled events only',
    _is_poison_pill        BOOLEAN       COMMENT 'DQ flag: intentionally corrupt record for pipeline testing',
    _eh_enqueued_time      TIMESTAMP     COMMENT 'Event Hubs server-side enqueue timestamp',
    _eh_partition          STRING        COMMENT 'Event Hubs partition ID',
    _eh_offset             STRING        COMMENT 'Event Hubs offset within partition',
    _eh_sequence           LONG          COMMENT 'Event Hubs sequence number within partition',
    _eh_publisher          STRING        COMMENT 'Event Hubs publisher name (nullable)',
    _ingest_time           TIMESTAMP     COMMENT 'Databricks ingestion timestamp',
    _source_system         STRING        COMMENT 'Source: eventhubs_native',
    _ingest_date           DATE          GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                         COMMENT 'Partition column derived from ingest time'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/order_events_raw_eh'
COMMENT 'Bronze order lifecycle events from Azure Event Hubs native connector. line_items stored as JSON string for Silver parsing.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,customer_id,event_type',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'finance',
    'quality.source'                    = 'eventhubs_native'
)
""")
print("✅ order_events_raw_eh")

# COMMAND ----------

# DBTITLE 1,dq_alerts_raw_eh
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.dq_alerts_raw_eh (
    alert_id               STRING        COMMENT 'Unique alert identifier (UUID)',
    alert_time             STRING        COMMENT 'Timestamp when the alert was generated',
    severity               STRING        COMMENT 'LOW | MEDIUM | HIGH | CRITICAL',
    rule_type              STRING        COMMENT 'null_check | range_check | referential_integrity | schema_drift | duplicate_key | type_mismatch | freshness_check',
    rule_name              STRING        COMMENT 'Composite name: ruletype__tablename',
    pipeline_name          STRING        COMMENT 'supply_chain_dlt | clickstream_dlt | finance_batch | telemetry_stream',
    table_name             STRING        COMMENT 'Fully-qualified table name that triggered the alert',
    failed_record_count    INT           COMMENT 'Number of records that failed the DQ rule',
    total_record_count     INT           COMMENT 'Total records evaluated in the batch',
    failure_rate_pct       DOUBLE        COMMENT 'Failure rate as a percentage (failed / total × 100)',
    sample_failed_values   STRING        COMMENT 'Array of up to 3 sample failed values serialised as JSON string',
    action_taken           STRING        COMMENT 'quarantined | dropped | warned | failed_pipeline',
    resolution_required    BOOLEAN       COMMENT 'True for HIGH and CRITICAL severity alerts',
    runbook_url            STRING        COMMENT 'Link to remediation runbook in internal wiki',
    correlation_id         STRING        COMMENT 'UUID linking related alerts across pipelines',
    _eh_enqueued_time      TIMESTAMP     COMMENT 'Event Hubs server-side enqueue timestamp',
    _eh_partition          STRING        COMMENT 'Event Hubs partition ID',
    _eh_offset             STRING        COMMENT 'Event Hubs offset within partition',
    _eh_sequence           LONG          COMMENT 'Event Hubs sequence number within partition',
    _eh_publisher          STRING        COMMENT 'Event Hubs publisher name (nullable)',
    _ingest_time           TIMESTAMP     COMMENT 'Databricks ingestion timestamp',
    _source_system         STRING        COMMENT 'Source: eventhubs_native',
    _ingest_date           DATE          GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                         COMMENT 'Partition column derived from ingest time'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/dq_alerts_raw_eh'
COMMENT 'Bronze DQ pipeline alert notifications from Azure Event Hubs native connector.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'severity,table_name,rule_type',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'monitoring',
    'quality.source'                    = 'eventhubs_native'
)
""")
print("✅ dq_alerts_raw_eh")

# COMMAND ----------

# DBTITLE 1,clickstream_raw_kafka
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.clickstream_raw_kafka (
    event_id               STRING        COMMENT 'Unique event identifier (UUID)',
    event_type             STRING        COMMENT 'page_view | search | click | add_to_cart | remove_from_cart | purchase',
    session_id             STRING        COMMENT 'User session identifier',
    user_id                STRING        COMMENT 'Customer ID — NULL for anonymous sessions (30%)',
    device_type            STRING        COMMENT 'mobile | desktop | tablet',
    event_time             STRING        COMMENT 'Actual event timestamp — backdated for late arrivals',
    processing_time        STRING        COMMENT 'Producer wall-clock time at message creation',
    page_url               STRING        COMMENT 'Page URL where event was triggered',
    referrer_url           STRING        COMMENT 'Referring URL (nullable)',
    product_id             STRING        COMMENT 'Product ID for product-related events (nullable)',
    category               STRING        COMMENT 'Product category for product-related events',
    search_query           STRING        COMMENT 'Search query text for search events (nullable)',
    cart_value             DOUBLE        COMMENT 'Running cart value at time of event',
    purchase_amount        DOUBLE        COMMENT 'Final purchase amount for purchase events (nullable)',
    ip_address             STRING        COMMENT 'Masked client IP address',
    user_agent             STRING        COMMENT 'Browser user agent string',
    geo_country            STRING        COMMENT 'Country derived from IP geolocation',
    geo_city               STRING        COMMENT 'City derived from IP geolocation',
    session_page_count     INT           COMMENT 'Cumulative page views within this session',
    _is_late_arrival       BOOLEAN       COMMENT 'DQ flag: event_time is >15 min before processing_time',
    _is_poison_pill        BOOLEAN       COMMENT 'DQ flag: intentionally corrupt record for pipeline testing',
    _eh_enqueued_time      TIMESTAMP     COMMENT 'Kafka message timestamp (maps to EH enqueue time)',
    _eh_partition          STRING        COMMENT 'Kafka partition ID',
    _eh_offset             STRING        COMMENT 'Kafka offset within partition',
    _ingest_time           TIMESTAMP     COMMENT 'Databricks ingestion timestamp',
    _source_system         STRING        COMMENT 'Source: eventhubs_kafka',
    _ingest_date           DATE          GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                         COMMENT 'Partition column derived from ingest time'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/clickstream_raw_kafka'
COMMENT 'Bronze clickstream events via Event Hubs Kafka endpoint. Alternative path to clickstream_raw_eh.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'event_type,user_id,geo_country',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'clickstream',
    'quality.source'                    = 'eventhubs_kafka'
)
""")
print("✅ clickstream_raw_kafka")

# COMMAND ----------

# DBTITLE 1,device_telemetry_raw_kafka
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.device_telemetry_raw_kafka (
    telemetry_id           STRING        COMMENT 'Unique telemetry event identifier (UUID)',
    device_id              STRING        COMMENT 'Device identifier DEV-XXXXX — NULL in poison pill rows',
    device_type            STRING        COMMENT 'router | sensor | gateway | edge_node | pos_terminal',
    firmware_version       STRING        COMMENT 'Device firmware version string (e.g. v2.1.4)',
    location_id            STRING        COMMENT 'FK → warehouses_raw.warehouse_id',
    event_time             STRING        COMMENT 'Sensor event timestamp — may be backdated for late arrivals',
    processing_time        STRING        COMMENT 'Producer wall-clock time at message creation',
    cpu_pct                DOUBLE        COMMENT 'CPU utilisation 0–100 — may be -999 in poison pill rows',
    memory_pct             DOUBLE        COMMENT 'Memory utilisation percentage 0–100',
    disk_pct               DOUBLE        COMMENT 'Disk utilisation percentage 0–100',
    temperature_c          DOUBLE        COMMENT 'Device temperature in Celsius — NULL when source has non-numeric value',
    network_in_kbps        DOUBLE        COMMENT 'Inbound network throughput in kbps',
    network_out_kbps       DOUBLE        COMMENT 'Outbound network throughput in kbps',
    error_count            INT           COMMENT 'Error count since last heartbeat',
    uptime_seconds         INT           COMMENT 'Device uptime since last reboot in seconds',
    status                 STRING        COMMENT 'online | degraded | offline',
    alert_triggered        BOOLEAN       COMMENT 'Whether device triggered a threshold alert this cycle',
    _is_late_arrival       BOOLEAN       COMMENT 'DQ flag: event_time is >10 min before processing_time',
    _is_poison_pill        BOOLEAN       COMMENT 'DQ flag: intentionally corrupt record for pipeline testing',
    metrics_raw            STRING        COMMENT 'Original nested metrics JSON preserved for audit and reprocessing',
    _eh_enqueued_time      TIMESTAMP     COMMENT 'Kafka message timestamp (maps to EH enqueue time)',
    _eh_partition          STRING        COMMENT 'Kafka partition ID',
    _eh_offset             STRING        COMMENT 'Kafka offset within partition',
    _ingest_time           TIMESTAMP     COMMENT 'Databricks ingestion timestamp',
    _source_system         STRING        COMMENT 'Source: eventhubs_kafka',
    _ingest_date           DATE          GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                         COMMENT 'Partition column derived from ingest time'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/device_telemetry_raw_kafka'
COMMENT 'Bronze IoT device telemetry via Event Hubs Kafka endpoint. Alternative path to device_telemetry_raw_eh.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'device_id,status,device_type',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'iot',
    'quality.source'                    = 'eventhubs_kafka'
)
""")
print("✅ device_telemetry_raw_kafka")

# COMMAND ----------

# DBTITLE 1,order_events_raw_kafka
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.order_events_raw_kafka (
    event_id               STRING        COMMENT 'Unique event identifier (UUID)',
    event_type             STRING        COMMENT 'order_placed | order_confirmed | order_shipped | order_delivered | order_cancelled | order_updated',
    order_id               STRING        COMMENT 'FK → orders_raw.order_id — NULL in poison pill rows',
    customer_id            STRING        COMMENT 'FK → customers_raw.customer_id — NULL in poison pill rows',
    warehouse_id           STRING        COMMENT 'FK → warehouses_raw.warehouse_id',
    event_time             STRING        COMMENT 'Timestamp when order event occurred',
    order_date             STRING        COMMENT 'Date the original order was placed',
    status                 STRING        COMMENT 'placed | confirmed | shipped | delivered | cancelled | updated',
    channel                STRING        COMMENT 'web | mobile_app | marketplace | in_store',
    currency               STRING        COMMENT 'ISO 4217 transaction currency',
    total_amount           DOUBLE        COMMENT 'Gross order total — may be negative in poison pill rows',
    discount_amount        DOUBLE        COMMENT 'Total discount applied',
    tax_amount             DOUBLE        COMMENT 'Tax charged on order',
    shipping_amount        DOUBLE        COMMENT 'Shipping and handling charge',
    line_items             STRING        COMMENT 'Array of line item objects serialised as JSON string — parsed in Silver',
    shipping_country       STRING        COMMENT 'Shipping destination country',
    carrier                STRING        COMMENT 'Logistics carrier — populated for order_shipped events only',
    tracking_number        STRING        COMMENT 'Carrier tracking reference — populated for order_shipped events only',
    cancellation_reason    STRING        COMMENT 'Cancellation reason — populated for order_cancelled events only',
    _is_poison_pill        BOOLEAN       COMMENT 'DQ flag: intentionally corrupt record for pipeline testing',
    _eh_enqueued_time      TIMESTAMP     COMMENT 'Kafka message timestamp (maps to EH enqueue time)',
    _eh_partition          STRING        COMMENT 'Kafka partition ID',
    _eh_offset             STRING        COMMENT 'Kafka offset within partition',
    _ingest_time           TIMESTAMP     COMMENT 'Databricks ingestion timestamp',
    _source_system         STRING        COMMENT 'Source: eventhubs_kafka',
    _ingest_date           DATE          GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                         COMMENT 'Partition column derived from ingest time'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/order_events_raw_kafka'
COMMENT 'Bronze order lifecycle events via Event Hubs Kafka endpoint. Alternative path to order_events_raw_eh.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,customer_id,event_type',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'finance',
    'quality.source'                    = 'eventhubs_kafka'
)
""")
print("✅ order_events_raw_kafka")

# COMMAND ----------

# DBTITLE 1,dq_alerts_raw_kafka
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.bronze.dq_alerts_raw_kafka (
    alert_id               STRING        COMMENT 'Unique alert identifier (UUID)',
    alert_time             STRING        COMMENT 'Timestamp when the alert was generated',
    severity               STRING        COMMENT 'LOW | MEDIUM | HIGH | CRITICAL',
    rule_type              STRING        COMMENT 'null_check | range_check | referential_integrity | schema_drift | duplicate_key | type_mismatch | freshness_check',
    rule_name              STRING        COMMENT 'Composite name: ruletype__tablename',
    pipeline_name          STRING        COMMENT 'supply_chain_dlt | clickstream_dlt | finance_batch | telemetry_stream',
    table_name             STRING        COMMENT 'Fully-qualified table name that triggered the alert',
    failed_record_count    INT           COMMENT 'Number of records that failed the DQ rule',
    total_record_count     INT           COMMENT 'Total records evaluated in the batch',
    failure_rate_pct       DOUBLE        COMMENT 'Failure rate as a percentage (failed / total × 100)',
    sample_failed_values   STRING        COMMENT 'Array of up to 3 sample failed values serialised as JSON string',
    action_taken           STRING        COMMENT 'quarantined | dropped | warned | failed_pipeline',
    resolution_required    BOOLEAN       COMMENT 'True for HIGH and CRITICAL severity alerts',
    runbook_url            STRING        COMMENT 'Link to remediation runbook in internal wiki',
    correlation_id         STRING        COMMENT 'UUID linking related alerts across pipelines',
    _eh_enqueued_time      TIMESTAMP     COMMENT 'Kafka message timestamp (maps to EH enqueue time)',
    _eh_partition          STRING        COMMENT 'Kafka partition ID',
    _eh_offset             STRING        COMMENT 'Kafka offset within partition',
    _ingest_time           TIMESTAMP     COMMENT 'Databricks ingestion timestamp',
    _source_system         STRING        COMMENT 'Source: eventhubs_kafka',
    _ingest_date           DATE          GENERATED ALWAYS AS (CAST(_ingest_time AS DATE))
                                         COMMENT 'Partition column derived from ingest time'
)
USING DELTA
PARTITIONED BY (_ingest_date)
-- LOCATION '{BASE}/dq_alerts_raw_kafka'
COMMENT 'Bronze DQ pipeline alert notifications via Event Hubs Kafka endpoint. Alternative path to dq_alerts_raw_eh.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.minReaderVersion'            = '2',
    'delta.minWriterVersion'            = '5',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'severity,table_name,rule_type',
    'quality.layer'                     = 'bronze',
    'quality.domain'                    = 'monitoring',
    'quality.source'                    = 'eventhubs_kafka'
)
""")
print("✅ dq_alerts_raw_kafka")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Final Verification

# COMMAND ----------

# DBTITLE 1,All tables in omnioc_dev.bronze
tables = spark.sql("SHOW TABLES IN omnioc_dev.bronze").collect()

batch_t    = sorted([t.tableName for t in tables if not any(x in t.tableName for x in ['_eh','_kafka'])])
eh_t       = sorted([t.tableName for t in tables if '_eh'    in t.tableName])
kafka_t    = sorted([t.tableName for t in tables if '_kafka' in t.tableName])

print("="*75)
print("  00b Bronze DDL — Verification")
print("="*75)
print(f"\n  BATCH ({len(batch_t)} tables)")
print("  " + "─"*55)
for t in batch_t:
    print(f"  ✅  {t}")

print(f"\n  STREAMING — EH NATIVE ({len(eh_t)} tables)")
print("  " + "─"*55)
for t in eh_t:
    print(f"  ✅  {t}")

print(f"\n  STREAMING — KAFKA ({len(kafka_t)} tables)")
print("  " + "─"*55)
for t in kafka_t:
    print(f"  ✅  {t}")

print("\n" + "="*75)
print(f"  Batch     : {len(batch_t)}")
print(f"  EH        : {len(eh_t)}")
print(f"  Kafka     : {len(kafka_t)}")
print(f"  Total     : {len(tables)}")
print("="*75)
print("\n  ✅ All tables created — proceed to ingestion notebooks")
