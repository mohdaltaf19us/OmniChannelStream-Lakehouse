# Databricks notebook source
# MAGIC %md
# MAGIC # Silver DDL — Setup Notebook
# MAGIC
# MAGIC Creates all silver Delta tables with proper types, deduplication tracking,
# MAGIC SCD Type 2 support for CDC tables, and exploded arrays for nested structures.
# MAGIC
# MAGIC **Run ONCE before any silver transform notebook. All tables are idempotent.**
# MAGIC
# MAGIC | Domain | Silver Tables | Key Changes from Bronze |
# MAGIC |--------|--------------|------------------------|
# MAGIC | Supply Chain | suppliers, products, warehouses, shipments, supplier_contracts | STRING → proper types |
# MAGIC | Finance | customers, orders, order_lines, payments, refunds, exchange_rates | STRING → proper types |
# MAGIC | CDC | customers_scd, products_scd | SCD Type 2 with valid_from/to |
# MAGIC | Streaming | clickstream, device_telemetry, order_events, dq_alerts | Deduped, typed, arrays |
# MAGIC | Cross-domain | order_event_line_items | Exploded from order_events.line_items |
# MAGIC | Quarantine | poison_pills | All poison pill rows from all streaming topics |

# COMMAND ----------
# DBTITLE 1, Use Catalog
spark.sql("USE CATALOG omnioc_dev")
spark.sql("USE SCHEMA silver")

BASE = "abfss://silver@omniocdeveusst01.dfs.core.windows.net"
print(f"✅ Target schema : omnioc_dev.silver")
print(f"✅ Target location: {BASE}")

# COMMAND ----------
# MAGIC %md ## Supply Chain Domain

# COMMAND ----------
# DBTITLE 1, suppliers
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.suppliers (
    supplier_id          STRING    COMMENT 'Unique supplier identifier (SUP-XXXXX)',
    supplier_name        STRING    COMMENT 'Legal supplier name',
    country              STRING    COMMENT 'Supplier country of origin',
    city                 STRING    COMMENT 'Supplier city',
    contact_email        STRING    COMMENT 'Primary contact email',
    contact_phone        STRING    COMMENT 'Primary contact phone',
    rating               DOUBLE    COMMENT 'Supplier performance rating 0-5',
    lead_time_days       INT       COMMENT 'Average lead time in days',
    payment_terms_days   INT       COMMENT 'Payment terms in days (net 30/60/90)',
    is_active            BOOLEAN   COMMENT 'Whether supplier is currently active',
    contract_start_date  DATE      COMMENT 'Active contract start date — cast from STRING in bronze',
    created_at           TIMESTAMP COMMENT 'Record creation timestamp — cast from STRING in bronze',
    updated_at           TIMESTAMP COMMENT 'Record last update timestamp — cast from STRING in bronze',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _silver_date         DATE      GENERATED ALWAYS AS (CAST(_silver_ingest_time AS DATE))
                                   COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_silver_date)
COMMENT 'Silver supplier master — type-cast from bronze, deduped on supplier_id.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'supplier_id,country',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'supply_chain'
)
""")
print("✅ suppliers")

# COMMAND ----------
# DBTITLE 1, products
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.products (
    product_id           STRING    COMMENT 'Unique product identifier (PRD-XXXXX)',
    product_name         STRING    COMMENT 'Product display name',
    sku                  STRING    COMMENT 'Stock keeping unit code',
    category             STRING    COMMENT 'Top-level product category',
    sub_category         STRING    COMMENT 'Sub-category within category',
    unit_cost            DOUBLE    COMMENT 'Cost price per unit in USD',
    unit_price           DOUBLE    COMMENT 'Retail price per unit in USD',
    weight_kg            DOUBLE    COMMENT 'Product weight in kilograms',
    is_active            BOOLEAN   COMMENT 'Whether product is currently active',
    supplier_id          STRING    COMMENT 'FK → silver.suppliers.supplier_id',
    reorder_point        INT       COMMENT 'Inventory level triggering reorder',
    reorder_quantity     INT       COMMENT 'Quantity to order when reorder triggered',
    created_at           TIMESTAMP COMMENT 'Record creation timestamp — cast from STRING in bronze',
    updated_at           TIMESTAMP COMMENT 'Record last update timestamp — cast from STRING in bronze',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _silver_date         DATE      GENERATED ALWAYS AS (CAST(_silver_ingest_time AS DATE))
                                   COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_silver_date)
COMMENT 'Silver product master — type-cast from bronze, deduped on product_id.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'product_id,category,supplier_id',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'supply_chain'
)
""")
print("✅ products")

# COMMAND ----------
# DBTITLE 1, warehouses
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.warehouses (
    warehouse_id         STRING    COMMENT 'Unique warehouse identifier (WH-XXXXX)',
    warehouse_name       STRING    COMMENT 'Warehouse display name',
    country              STRING    COMMENT 'Warehouse country',
    city                 STRING    COMMENT 'Warehouse city',
    region               STRING    COMMENT 'Geographic region',
    capacity_sqft        INT       COMMENT 'Warehouse capacity in square feet',
    is_active            BOOLEAN   COMMENT 'Whether warehouse is currently operational',
    manager_name         STRING    COMMENT 'Warehouse manager full name',
    contact_email        STRING    COMMENT 'Warehouse contact email',
    created_at           TIMESTAMP COMMENT 'Record creation timestamp — cast from STRING in bronze',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _silver_date         DATE      GENERATED ALWAYS AS (CAST(_silver_ingest_time AS DATE))
                                   COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_silver_date)
COMMENT 'Silver warehouse master — type-cast from bronze, deduped on warehouse_id.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '4',
    'pipelines.autoOptimize.zOrderCols' = 'warehouse_id,country,region',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'supply_chain'
)
""")
print("✅ warehouses")

# COMMAND ----------
# DBTITLE 1, shipments
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.shipments (
    shipment_id          STRING    COMMENT 'Unique shipment identifier (SHP-XXXXXXXX)',
    order_id             STRING    COMMENT 'FK → silver.orders.order_id',
    supplier_id          STRING    COMMENT 'FK → silver.suppliers.supplier_id',
    warehouse_id         STRING    COMMENT 'FK → silver.warehouses.warehouse_id',
    product_id           STRING    COMMENT 'FK → silver.products.product_id',
    quantity             INT       COMMENT 'Quantity shipped',
    shipment_date        DATE      COMMENT 'Date shipment dispatched — cast from STRING in bronze',
    expected_arrival     DATE      COMMENT 'Expected arrival date — cast from STRING in bronze',
    actual_arrival       DATE      COMMENT 'Actual arrival date — NULL until arrived',
    carrier              STRING    COMMENT 'Logistics carrier name',
    tracking_number      STRING    COMMENT 'Carrier tracking reference',
    status               STRING    COMMENT 'in_transit | delivered | delayed | cancelled',
    freight_cost         DOUBLE    COMMENT 'Total freight cost in USD',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _event_date          DATE      GENERATED ALWAYS AS (shipment_date)
                                   COMMENT 'Partition column — business date of shipment'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Silver shipments — type-cast, deduped on shipment_id. Partitioned by shipment_date.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'shipment_id,order_id,status',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'supply_chain'
)
""")
print("✅ shipments")

# COMMAND ----------
# DBTITLE 1, supplier_contracts
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.supplier_contracts (
    contract_id          STRING    COMMENT 'Unique contract identifier (CON-XXXXXXXX)',
    supplier_id          STRING    COMMENT 'FK → silver.suppliers.supplier_id',
    contract_type        STRING    COMMENT 'exclusive | preferred | standard',
    start_date           DATE      COMMENT 'Contract start date — cast from STRING in bronze',
    end_date             DATE      COMMENT 'Contract end date — cast from STRING in bronze',
    annual_value_usd     DOUBLE    COMMENT 'Total annual contract value in USD',
    payment_terms_days   INT       COMMENT 'Payment terms in days',
    auto_renew           BOOLEAN   COMMENT 'Whether contract auto-renews on expiry',
    is_active            BOOLEAN   COMMENT 'Whether contract is currently active',
    signed_by            STRING    COMMENT 'Name of signatory',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _silver_date         DATE      GENERATED ALWAYS AS (CAST(_silver_ingest_time AS DATE))
                                   COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_silver_date)
COMMENT 'Silver supplier contracts — type-cast, deduped on contract_id.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'supplier_id,contract_type,is_active',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'supply_chain'
)
""")
print("✅ supplier_contracts")

# COMMAND ----------
# MAGIC %md ## Finance Domain

# COMMAND ----------
# DBTITLE 1, customers
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.customers (
    customer_id          STRING    COMMENT 'Unique customer identifier (CUST-XXXXXXX)',
    first_name           STRING    COMMENT 'Customer first name',
    last_name            STRING    COMMENT 'Customer last name',
    email                STRING    COMMENT 'Customer email address',
    phone                STRING    COMMENT 'Customer phone number',
    country              STRING    COMMENT 'Customer country',
    city                 STRING    COMMENT 'Customer city',
    segment              STRING    COMMENT 'retail | wholesale | enterprise',
    registration_date    DATE      COMMENT 'Account registration date — cast from STRING in bronze',
    is_active            BOOLEAN   COMMENT 'Whether customer account is active',
    lifetime_value       DOUBLE    COMMENT 'Calculated customer lifetime value in USD',
    created_at           TIMESTAMP COMMENT 'Record creation timestamp — cast from STRING in bronze',
    updated_at           TIMESTAMP COMMENT 'Record last update timestamp — cast from STRING in bronze',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _silver_date         DATE      GENERATED ALWAYS AS (CAST(_silver_ingest_time AS DATE))
                                   COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_silver_date)
COMMENT 'Silver customer master — type-cast, deduped on customer_id.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'customer_id,country,segment',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'finance'
)
""")
print("✅ customers")

# COMMAND ----------
# DBTITLE 1, orders
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.orders (
    order_id             STRING    COMMENT 'Unique order identifier (ORD-XXXXXXXX)',
    customer_id          STRING    COMMENT 'FK → silver.customers.customer_id',
    warehouse_id         STRING    COMMENT 'FK → silver.warehouses.warehouse_id',
    order_date           DATE      COMMENT 'Date order was placed — cast from STRING in bronze',
    status               STRING    COMMENT 'placed | confirmed | shipped | delivered | cancelled',
    channel              STRING    COMMENT 'web | mobile_app | marketplace | in_store',
    currency             STRING    COMMENT 'ISO 4217 transaction currency',
    total_amount         DOUBLE    COMMENT 'Gross order total in transaction currency',
    discount_amount      DOUBLE    COMMENT 'Total discount applied',
    tax_amount           DOUBLE    COMMENT 'Tax charged on order',
    shipping_amount      DOUBLE    COMMENT 'Shipping and handling charge',
    shipping_country     STRING    COMMENT 'Shipping destination country',
    created_at           TIMESTAMP COMMENT 'Record creation timestamp — cast from STRING in bronze',
    updated_at           TIMESTAMP COMMENT 'Record last update timestamp — cast from STRING in bronze',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _event_date          DATE      GENERATED ALWAYS AS (order_date)
                                   COMMENT 'Partition column — business date of order'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Silver orders — type-cast, deduped on order_id. Partitioned by order_date.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,customer_id,status',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'finance'
)
""")
print("✅ orders")

# COMMAND ----------
# DBTITLE 1, order_lines
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.order_lines (
    line_id              STRING    COMMENT 'Unique order line identifier',
    order_id             STRING    COMMENT 'FK → silver.orders.order_id',
    product_id           STRING    COMMENT 'FK → silver.products.product_id',
    quantity             INT       COMMENT 'Quantity ordered',
    unit_price           DOUBLE    COMMENT 'Price per unit at time of order',
    discount_pct         DOUBLE    COMMENT 'Discount percentage applied to this line',
    line_total           DOUBLE    COMMENT 'quantity × unit_price × (1 - discount_pct/100)',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _silver_date         DATE      GENERATED ALWAYS AS (CAST(_silver_ingest_time AS DATE))
                                   COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_silver_date)
COMMENT 'Silver order lines — type-cast, deduped on line_id.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,product_id',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'finance'
)
""")
print("✅ order_lines")

# COMMAND ----------
# DBTITLE 1, payments
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.payments (
    payment_id           STRING    COMMENT 'Unique payment identifier (PAY-XXXXXXXX)',
    order_id             STRING    COMMENT 'FK → silver.orders.order_id',
    customer_id          STRING    COMMENT 'FK → silver.customers.customer_id',
    payment_date         DATE      COMMENT 'Date payment was processed — cast from STRING in bronze',
    payment_method       STRING    COMMENT 'credit_card | debit_card | paypal | bank_transfer | crypto',
    amount               DOUBLE    COMMENT 'Payment amount in transaction currency',
    currency             STRING    COMMENT 'ISO 4217 payment currency',
    status               STRING    COMMENT 'pending | completed | failed | refunded',
    gateway              STRING    COMMENT 'Payment gateway used',
    transaction_ref      STRING    COMMENT 'Gateway transaction reference',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _event_date          DATE      GENERATED ALWAYS AS (payment_date)
                                   COMMENT 'Partition column — business date of payment'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Silver payments — type-cast, deduped on payment_id. Partitioned by payment_date.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'payment_id,order_id,status',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'finance'
)
""")
print("✅ payments")

# COMMAND ----------
# DBTITLE 1, refunds
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.refunds (
    refund_id            STRING    COMMENT 'Unique refund identifier (REF-XXXXXXXX)',
    order_id             STRING    COMMENT 'FK → silver.orders.order_id',
    payment_id           STRING    COMMENT 'FK → silver.payments.payment_id',
    customer_id          STRING    COMMENT 'FK → silver.customers.customer_id',
    refund_date          DATE      COMMENT 'Date refund was issued — cast from STRING in bronze',
    refund_amount        DOUBLE    COMMENT 'Amount refunded in transaction currency',
    currency             STRING    COMMENT 'ISO 4217 refund currency',
    reason               STRING    COMMENT 'defective | wrong_item | not_delivered | customer_request | other',
    status               STRING    COMMENT 'pending | completed | rejected',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _event_date          DATE      GENERATED ALWAYS AS (refund_date)
                                   COMMENT 'Partition column — business date of refund'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Silver refunds — type-cast, deduped on refund_id. Partitioned by refund_date.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '5',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,customer_id,status',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'finance'
)
""")
print("✅ refunds")

# COMMAND ----------
# DBTITLE 1, exchange_rates
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.exchange_rates (
    rate_id              STRING    COMMENT 'Unique rate record identifier',
    from_currency        STRING    COMMENT 'Source ISO 4217 currency code',
    to_currency          STRING    COMMENT 'Target ISO 4217 currency code',
    rate_date            DATE      COMMENT 'Date rate was effective — cast from STRING in bronze',
    exchange_rate        DOUBLE    COMMENT 'Exchange rate: 1 unit of from_currency in to_currency',
    source               STRING    COMMENT 'Rate source: ecb | xe | internal',
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table name',
    _event_date          DATE      GENERATED ALWAYS AS (rate_date)
                                   COMMENT 'Partition column — effective date of rate'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Silver exchange rates — type-cast, deduped on rate_id + rate_date.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '4',
    'pipelines.autoOptimize.zOrderCols' = 'from_currency,to_currency,rate_date',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'finance'
)
""")
print("✅ exchange_rates")

# COMMAND ----------
# MAGIC %md ## CDC Domain — SCD Type 2

# COMMAND ----------
# DBTITLE 1, customers_scd (SCD Type 2 from cdc_customers_raw)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.customers_scd (
    -- Business key
    customer_id          STRING    COMMENT 'Unique customer identifier (CUST-XXXXXXX)',
    -- Tracked attributes — changes create new row
    first_name           STRING    COMMENT 'Customer first name',
    last_name            STRING    COMMENT 'Customer last name',
    email                STRING    COMMENT 'Customer email address',
    phone                STRING    COMMENT 'Customer phone number',
    country              STRING    COMMENT 'Customer country',
    city                 STRING    COMMENT 'Customer city',
    segment              STRING    COMMENT 'retail | wholesale | enterprise',
    is_active            BOOLEAN   COMMENT 'Whether customer account is active',
    lifetime_value       DOUBLE    COMMENT 'Customer lifetime value in USD at time of change',
    -- CDC metadata
    cdc_operation        STRING    COMMENT 'INSERT | UPDATE | DELETE — from source CDC stream',
    -- SCD Type 2 columns
    _valid_from          TIMESTAMP COMMENT 'Timestamp this version became effective',
    _valid_to            TIMESTAMP COMMENT 'Timestamp this version was superseded — NULL if current',
    _is_current          BOOLEAN   COMMENT 'True if this is the current active version',
    _record_hash         STRING    COMMENT 'MD5 hash of tracked columns — used to detect actual changes',
    -- Silver metadata
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table: cdc_customers_raw',
    _valid_from_date     DATE      GENERATED ALWAYS AS (CAST(_valid_from AS DATE))
                                   COMMENT 'Partition column — date version became effective'
)
USING DELTA
PARTITIONED BY (_valid_from_date)
COMMENT 'SCD Type 2 customer history — each row is one version of a customer record. Use _is_current = true for latest.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'customer_id,_is_current,_valid_from',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'cdc'
)
""")
print("✅ customers_scd")

# COMMAND ----------
# DBTITLE 1, products_scd (SCD Type 2 from cdc_products_raw)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.products_scd (
    -- Business key
    product_id           STRING    COMMENT 'Unique product identifier (PRD-XXXXX)',
    -- Tracked attributes
    product_name         STRING    COMMENT 'Product display name',
    sku                  STRING    COMMENT 'Stock keeping unit code',
    category             STRING    COMMENT 'Top-level product category',
    sub_category         STRING    COMMENT 'Sub-category within category',
    unit_cost            DOUBLE    COMMENT 'Cost price per unit at time of change',
    unit_price           DOUBLE    COMMENT 'Retail price per unit at time of change',
    is_active            BOOLEAN   COMMENT 'Whether product is active at time of change',
    supplier_id          STRING    COMMENT 'FK → silver.suppliers.supplier_id',
    -- CDC metadata
    cdc_operation        STRING    COMMENT 'INSERT | UPDATE | DELETE — from source CDC stream',
    -- SCD Type 2 columns
    _valid_from          TIMESTAMP COMMENT 'Timestamp this version became effective',
    _valid_to            TIMESTAMP COMMENT 'Timestamp this version was superseded — NULL if current',
    _is_current          BOOLEAN   COMMENT 'True if this is the current active version',
    _record_hash         STRING    COMMENT 'MD5 hash of tracked columns — used to detect actual changes',
    -- Silver metadata
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table: cdc_products_raw',
    _valid_from_date     DATE      GENERATED ALWAYS AS (CAST(_valid_from AS DATE))
                                   COMMENT 'Partition column — date version became effective'
)
USING DELTA
PARTITIONED BY (_valid_from_date)
COMMENT 'SCD Type 2 product history — each row is one version of a product record. Use _is_current = true for latest.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'product_id,_is_current,_valid_from',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'cdc'
)
""")
print("✅ products_scd")

# COMMAND ----------
# MAGIC %md ## Streaming Domain — Clickstream

# COMMAND ----------
# DBTITLE 1, clickstream
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.clickstream (
    event_id             STRING    COMMENT 'Unique event identifier (UUID) — dedup key',
    event_type           STRING    COMMENT 'page_view | search | click | add_to_cart | remove_from_cart | purchase',
    session_id           STRING    COMMENT 'User session identifier',
    user_id              STRING    COMMENT 'Authenticated user ID — NULL for anonymous sessions',
    device_type          STRING    COMMENT 'desktop | mobile | tablet',
    event_time           TIMESTAMP COMMENT 'Timestamp when event occurred — cast from STRING in bronze',
    processing_time      TIMESTAMP COMMENT 'Timestamp when event was processed — cast from STRING in bronze',
    page_url             STRING    COMMENT 'Full URL of page where event occurred',
    referrer_url         STRING    COMMENT 'Referring URL — NULL for direct traffic',
    product_id           STRING    COMMENT 'FK → silver.products.product_id — populated for product events',
    category             STRING    COMMENT 'Product category context',
    search_query         STRING    COMMENT 'Search term — populated for search events only',
    cart_value           DOUBLE    COMMENT 'Cart total at time of event',
    purchase_amount      DOUBLE    COMMENT 'Purchase amount — populated for purchase events only',
    ip_address           STRING    COMMENT 'Client IP address',
    user_agent           STRING    COMMENT 'Browser/client user agent string',
    geo_country          STRING    COMMENT 'Geolocation country derived from IP',
    geo_city             STRING    COMMENT 'Geolocation city derived from IP',
    session_page_count   INT       COMMENT 'Number of pages viewed in this session so far',
    -- Silver metadata
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table (clickstream_raw_eh or clickstream_raw_kafka)',
    _event_date          DATE      GENERATED ALWAYS AS (CAST(event_time AS DATE))
                                   COMMENT 'Partition column — business date of event'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Silver clickstream — deduped on event_id, poison pills excluded, event_time cast to TIMESTAMP.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'event_type,user_id,geo_country',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'clickstream'
)
""")
print("✅ clickstream")

# COMMAND ----------
# MAGIC %md ## Streaming Domain — IoT Device Telemetry

# COMMAND ----------
# DBTITLE 1, device_telemetry
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.device_telemetry (
    telemetry_id         STRING    COMMENT 'Unique telemetry record identifier — dedup key',
    device_id            STRING    COMMENT 'Unique device identifier (DEV-XXXXX)',
    device_type          STRING    COMMENT 'router | sensor | gateway | edge_node | pos_terminal',
    firmware_version     STRING    COMMENT 'Device firmware version string',
    location_id          STRING    COMMENT 'FK → silver.warehouses.warehouse_id',
    event_time           TIMESTAMP COMMENT 'Timestamp of telemetry reading — cast from STRING in bronze',
    processing_time      TIMESTAMP COMMENT 'Timestamp when event was processed — cast from STRING in bronze',
    -- Flattened metrics (already flat in bronze, validated ranges in silver)
    cpu_pct              DOUBLE    COMMENT 'CPU utilisation percentage 0-100',
    memory_pct           DOUBLE    COMMENT 'Memory utilisation percentage 0-100',
    disk_pct             DOUBLE    COMMENT 'Disk utilisation percentage 0-100',
    temperature_c        DOUBLE    COMMENT 'Device temperature in Celsius — validated: 0-120°C in silver',
    network_in_kbps      DOUBLE    COMMENT 'Inbound network throughput in kbps',
    network_out_kbps     DOUBLE    COMMENT 'Outbound network throughput in kbps',
    error_count          INT       COMMENT 'Error count since last reading',
    uptime_seconds       INT       COMMENT 'Device uptime in seconds since last reboot',
    status               STRING    COMMENT 'online | degraded | offline',
    alert_triggered      BOOLEAN   COMMENT 'Whether this reading triggered an alert',
    -- Silver metadata
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table (device_telemetry_raw_eh or _kafka)',
    _event_date          DATE      GENERATED ALWAYS AS (CAST(event_time AS DATE))
                                   COMMENT 'Partition column — business date of reading'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Silver device telemetry — deduped on telemetry_id, poison pills excluded, metric ranges validated.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'device_id,status,device_type',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'iot'
)
""")
print("✅ device_telemetry")

# COMMAND ----------
# MAGIC %md ## Streaming Domain — Order Events

# COMMAND ----------
# DBTITLE 1, order_events
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.order_events (
    event_id             STRING    COMMENT 'Unique event identifier (UUID) — dedup key',
    event_type           STRING    COMMENT 'order_placed | confirmed | shipped | delivered | cancelled | updated',
    order_id             STRING    COMMENT 'FK → silver.orders.order_id',
    customer_id          STRING    COMMENT 'FK → silver.customers.customer_id',
    warehouse_id         STRING    COMMENT 'FK → silver.warehouses.warehouse_id',
    event_time           TIMESTAMP COMMENT 'Timestamp when order event occurred — cast from STRING in bronze',
    order_date           DATE      COMMENT 'Date the original order was placed — cast from STRING in bronze',
    status               STRING    COMMENT 'placed | confirmed | shipped | delivered | cancelled | updated',
    channel              STRING    COMMENT 'web | mobile_app | marketplace | in_store',
    currency             STRING    COMMENT 'ISO 4217 transaction currency',
    total_amount         DOUBLE    COMMENT 'Gross order total in transaction currency',
    discount_amount      DOUBLE    COMMENT 'Total discount applied',
    tax_amount           DOUBLE    COMMENT 'Tax charged on order',
    shipping_amount      DOUBLE    COMMENT 'Shipping and handling charge',
    shipping_country     STRING    COMMENT 'Shipping destination country',
    carrier              STRING    COMMENT 'Logistics carrier — populated for shipped events',
    tracking_number      STRING    COMMENT 'Carrier tracking reference — populated for shipped events',
    cancellation_reason  STRING    COMMENT 'Cancellation reason — populated for cancelled events',
    line_item_count      INT       COMMENT 'Number of line items — derived from line_items array',
    -- Silver metadata
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table (order_events_raw_eh or _kafka)',
    _event_date          DATE      GENERATED ALWAYS AS (CAST(event_time AS DATE))
                                   COMMENT 'Partition column — business date of event'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Silver order events — deduped on event_id, poison pills excluded. line_items exploded to order_event_line_items.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,customer_id,event_type',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'finance'
)
""")
print("✅ order_events")

# COMMAND ----------
# DBTITLE 1, order_event_line_items (exploded from order_events.line_items)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.order_event_line_items (
    event_id             STRING    COMMENT 'FK → silver.order_events.event_id',
    order_id             STRING    COMMENT 'FK → silver.orders.order_id',
    line_index           INT       COMMENT 'Position of this line item within the order (0-based)',
    product_id           STRING    COMMENT 'FK → silver.products.product_id',
    quantity             INT       COMMENT 'Quantity ordered for this line',
    unit_price           DOUBLE    COMMENT 'Price per unit at time of event',
    discount_pct         DOUBLE    COMMENT 'Discount percentage applied to this line',
    line_total           DOUBLE    COMMENT 'Derived: quantity × unit_price × (1 - discount_pct/100)',
    -- Silver metadata
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table (order_events_raw_eh or _kafka)',
    _event_date          DATE      COMMENT 'Partition column — copied from parent order_events._event_date'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Exploded order event line items — one row per product per order event. Join to order_events on event_id.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'event_id,order_id,product_id',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'finance'
)
""")
print("✅ order_event_line_items")

# COMMAND ----------
# MAGIC %md ## Streaming Domain — DQ Alerts

# COMMAND ----------
# DBTITLE 1, dq_alerts
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.dq_alerts (
    alert_id             STRING    COMMENT 'Unique alert identifier (UUID) — dedup key',
    alert_time           TIMESTAMP COMMENT 'Timestamp when alert was generated — cast from STRING in bronze',
    severity             STRING    COMMENT 'LOW | MEDIUM | HIGH | CRITICAL',
    rule_type            STRING    COMMENT 'null_check | range_check | referential_integrity | schema_drift | duplicate_key | type_mismatch | freshness_check',
    rule_name            STRING    COMMENT 'Composite name: ruletype__tablename',
    pipeline_name        STRING    COMMENT 'supply_chain_dlt | clickstream_dlt | finance_batch | telemetry_stream',
    table_name           STRING    COMMENT 'Fully-qualified table name that triggered the alert',
    failed_record_count  INT       COMMENT 'Number of records that failed the DQ rule',
    total_record_count   INT       COMMENT 'Total records evaluated in the batch',
    failure_rate_pct     DOUBLE    COMMENT 'Failure rate as percentage',
    sample_failed_values ARRAY<STRING> COMMENT 'Up to 3 sample failed values — parsed from JSON string in bronze',
    action_taken         STRING    COMMENT 'quarantined | dropped | warned | failed_pipeline',
    resolution_required  BOOLEAN   COMMENT 'True for HIGH and CRITICAL severity',
    runbook_url          STRING    COMMENT 'Link to remediation runbook',
    correlation_id       STRING    COMMENT 'UUID linking related alerts across pipelines',
    -- Silver metadata
    _silver_ingest_time  TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_source       STRING    COMMENT 'Source bronze table (dq_alerts_raw_eh or _kafka)',
    _event_date          DATE      GENERATED ALWAYS AS (CAST(alert_time AS DATE))
                                   COMMENT 'Partition column — business date of alert'
)
USING DELTA
PARTITIONED BY (_event_date)
COMMENT 'Silver DQ alerts — deduped on alert_id, sample_failed_values parsed from JSON string to ARRAY.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '8',
    'pipelines.autoOptimize.zOrderCols' = 'severity,table_name,rule_type',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'monitoring'
)
""")
print("✅ dq_alerts")

# COMMAND ----------
# MAGIC %md ## Cross-Domain — Quarantine

# COMMAND ----------
# DBTITLE 1, poison_pills (quarantine for all streaming topics)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS omnioc_dev.silver.poison_pills (
    record_id            STRING    COMMENT 'Original primary key from source topic (event_id / telemetry_id / alert_id)',
    source_topic         STRING    COMMENT 'Origin topic: clickstream | device_telemetry | order_events | dq_alerts',
    source_table         STRING    COMMENT 'Origin bronze table name',
    raw_payload          STRING    COMMENT 'Full original JSON payload preserved as STRING',
    poison_reason        STRING    COMMENT 'Inferred reason: null_key | negative_amount | wrong_type | null_customer',
    _eh_enqueued_time    TIMESTAMP COMMENT 'Original EH enqueue timestamp from bronze',
    _eh_partition        STRING    COMMENT 'Original EH partition from bronze',
    _silver_ingest_time  TIMESTAMP COMMENT 'Timestamp record was routed to quarantine',
    _quarantine_date     DATE      GENERATED ALWAYS AS (CAST(_silver_ingest_time AS DATE))
                                   COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (_quarantine_date)
COMMENT 'Quarantine table for all poison pill records across streaming topics. Preserved for investigation and reprocessing.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'false',
    'delta.columnMapping.mode'          = 'name',
    'delta.dataSkippingNumIndexedCols'  = '4',
    'pipelines.autoOptimize.zOrderCols' = 'source_topic,poison_reason',
    'quality.layer'                     = 'silver',
    'quality.domain'                    = 'quarantine'
)
""")
print("✅ poison_pills")

# COMMAND ----------
# MAGIC %md ## Final Verification

# COMMAND ----------
# DBTITLE 1, All tables in omnioc_dev.silver
tables = spark.sql("SHOW TABLES IN omnioc_dev.silver").collect()

supply_chain = sorted([t.tableName for t in tables if t.tableName in
    ["suppliers","products","warehouses","shipments","supplier_contracts"]])
finance      = sorted([t.tableName for t in tables if t.tableName in
    ["customers","orders","order_lines","payments","refunds","exchange_rates"]])
cdc          = sorted([t.tableName for t in tables if "_scd" in t.tableName])
streaming    = sorted([t.tableName for t in tables if t.tableName in
    ["clickstream","device_telemetry","order_events","order_event_line_items","dq_alerts"]])
quarantine   = sorted([t.tableName for t in tables if t.tableName == "poison_pills"])

print("=" * 70)
print("  01 Silver DDL — Verification")
print("=" * 70)

for label, group in [
    ("SUPPLY CHAIN", supply_chain),
    ("FINANCE",      finance),
    ("CDC / SCD2",   cdc),
    ("STREAMING",    streaming),
    ("QUARANTINE",   quarantine),
]:
    print(f"\n  {label} ({len(group)} tables)")
    print("  " + "─" * 50)
    for t in group:
        print(f"  ✅  {t}")

print("\n" + "=" * 70)
print(f"  Total : {len(tables)} silver tables")
print("=" * 70)
print("\n  ✅ Silver DDL complete — proceed to silver transform notebooks")
