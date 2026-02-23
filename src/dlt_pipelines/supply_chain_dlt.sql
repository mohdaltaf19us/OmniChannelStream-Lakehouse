-- ============================================================
-- OmniOC Lakehouse — Supply Chain DLT Pipeline (SQL Declarative)
-- Reads from: omnioc_dev.bronze.*
-- Writes to:  omnioc_dev.silver.*  and  omnioc_dev.gold.*
--
-- Patterns demonstrated:
--   STREAMING LIVE TABLE         — stateful streaming ingest from Autoloader
--   LIVE TABLE                   — batch transformation
--   APPLY CHANGES INTO           — CDC merge, SCD Type 1 & 2
--   CONSTRAINT ON VIOLATION DROP — DQ expectations with quarantine routing
--   CONSTRAINT ON VIOLATION WARN — Soft alerts via DLT event log
--   CONSTRAINT ON VIOLATION FAIL — Hard pipeline stop for critical tables
--   MATERIALIZED VIEW            — pre-computed Gold aggregates
-- ============================================================

-- Pipeline config passed from DABs as Spark conf
-- ${catalog}        = omnioc_dev | omnioc_staging | omnioc_prod
-- ${storage_account}= omniocdeveusst01

-- ================================================================
--  LAYER 1: BRONZE STREAMING LIVE TABLES (entry into DLT graph)
-- ================================================================

-- Read from Bronze Delta tables using readStream
-- These act as the DLT graph's source — Autoloader wrote to Bronze,
-- DLT reads from Bronze via CDF (change data feed) for incremental processing

CREATE OR REFRESH STREAMING LIVE TABLE bronze_suppliers_stream
COMMENT "Streaming entry: suppliers from Bronze (Autoloader CSV)"
TBLPROPERTIES ("quality" = "bronze", "pipelines.reset.allowed" = "true")
AS SELECT
    supplier_id,
    supplier_name,
    country,
    city,
    contact_email,
    contact_phone,
    CAST(rating AS DOUBLE)                AS rating,
    CAST(lead_time_days AS INT)           AS lead_time_days,
    CAST(payment_terms_days AS INT)       AS payment_terms_days,
    CAST(is_active AS BOOLEAN)            AS is_active,
    TRY_CAST(contract_start_date AS DATE) AS contract_start_date,
    TRY_TO_TIMESTAMP(created_at)          AS created_at,
    TRY_TO_TIMESTAMP(updated_at)          AS updated_at,
    _ingest_time,
    _source_file,
    _rescued_data
FROM STREAM(${catalog}.bronze.suppliers_raw);

-- ────────────────────────────────────────────────────────────────
CREATE OR REFRESH STREAMING LIVE TABLE bronze_products_stream
COMMENT "Streaming entry: products from Bronze (Autoloader CSV)"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
    product_id,
    product_name,
    sku,
    category,
    sub_category,
    CAST(unit_cost  AS DOUBLE)  AS unit_cost,
    CAST(unit_price AS DOUBLE)  AS unit_price,
    CAST(weight_kg  AS DOUBLE)  AS weight_kg,
    CAST(is_active  AS BOOLEAN) AS is_active,
    supplier_id,
    CAST(reorder_point    AS INT) AS reorder_point,
    CAST(reorder_quantity AS INT) AS reorder_quantity,
    TRY_TO_TIMESTAMP(created_at) AS created_at,
    TRY_TO_TIMESTAMP(updated_at) AS updated_at,
    _ingest_time,
    _source_file,
    _rescued_data
FROM STREAM(${catalog}.bronze.products_raw);

-- ────────────────────────────────────────────────────────────────
CREATE OR REFRESH STREAMING LIVE TABLE bronze_orders_stream
COMMENT "Streaming entry: orders from Bronze (Autoloader JSON)"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
    order_id,
    customer_id,
    warehouse_id,
    channel,
    CAST(status AS STRING)              AS status,
    TRY_CAST(total_amount  AS DOUBLE)   AS total_amount,
    TRY_CAST(discount_amount AS DOUBLE) AS discount_amount,
    TRY_CAST(tax_amount    AS DOUBLE)   AS tax_amount,
    TRY_CAST(shipping_amount AS DOUBLE) AS shipping_amount,
    currency,
    shipping_address_city,
    shipping_address_country,
    promo_code,
    TRY_TO_TIMESTAMP(order_date)    AS order_date,
    TRY_TO_TIMESTAMP(ship_date)     AS ship_date,
    TRY_TO_TIMESTAMP(delivery_date) AS delivery_date,
    TRY_TO_TIMESTAMP(created_at)    AS created_at,
    TRY_TO_TIMESTAMP(updated_at)    AS updated_at,
    _ingest_time,
    _source_file,
    _rescued_data
FROM STREAM(${catalog}.bronze.orders_raw);

-- ────────────────────────────────────────────────────────────────
CREATE OR REFRESH STREAMING LIVE TABLE bronze_shipments_stream
COMMENT "Streaming entry: shipments from Bronze (Autoloader Parquet)"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM STREAM(${catalog}.bronze.shipments_raw);

-- ================================================================
--  LAYER 2: SILVER — VALIDATED + CLEANSED (with APPLY CHANGES)
-- ================================================================

-- ── SILVER: SUPPLIERS (SCD Type 1 — overwrite, no history needed) ────────
CREATE OR REFRESH STREAMING LIVE TABLE silver_suppliers_clean
COMMENT "Validated suppliers — must-have constraints before APPLY CHANGES"
(
  CONSTRAINT supplier_id_not_null    EXPECT (supplier_id IS NOT NULL)   ON VIOLATION DROP ROW,
  CONSTRAINT supplier_name_not_null  EXPECT (supplier_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_rating            EXPECT (rating BETWEEN 0 AND 5)    ON VIOLATION WARN,
  CONSTRAINT valid_lead_time         EXPECT (lead_time_days > 0)        ON VIOLATION WARN
)
AS SELECT * FROM STREAM(LIVE.bronze_suppliers_stream);

-- SCD Type 1 — APPLY CHANGES INTO replaces current record (no history kept)
CREATE OR REFRESH STREAMING LIVE TABLE silver_suppliers
COMMENT "Silver suppliers — SCD Type 1 (current state only)"
TBLPROPERTIES (
    "quality" = "silver",
    "delta.enableChangeDataFeed" = "true"
);
APPLY CHANGES INTO LIVE.silver_suppliers
FROM STREAM(LIVE.silver_suppliers_clean)
KEYS (supplier_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;

-- ── SILVER: PRODUCTS (SCD Type 2 — tracks price & status history) ─────────
CREATE OR REFRESH STREAMING LIVE TABLE silver_products_clean
COMMENT "Validated products"
(
  CONSTRAINT product_id_not_null   EXPECT (product_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT sku_not_null          EXPECT (sku IS NOT NULL)          ON VIOLATION DROP ROW,
  CONSTRAINT positive_unit_cost    EXPECT (unit_cost > 0)            ON VIOLATION DROP ROW,
  CONSTRAINT positive_unit_price   EXPECT (unit_price > 0)           ON VIOLATION DROP ROW,
  CONSTRAINT category_not_null     EXPECT (category IS NOT NULL)     ON VIOLATION WARN
)
AS SELECT * FROM STREAM(LIVE.bronze_products_stream);

-- SCD Type 2 — preserves full price/status history
CREATE OR REFRESH STREAMING LIVE TABLE silver_products
COMMENT "Silver products — SCD Type 2 (full history of price and status changes)"
TBLPROPERTIES (
    "quality" = "silver",
    "delta.enableChangeDataFeed" = "true"
);
APPLY CHANGES INTO LIVE.silver_products
FROM STREAM(LIVE.silver_products_clean)
KEYS (product_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (_ingest_time, _source_file, _rescued_data)
STORED AS SCD TYPE 2;

-- ── SILVER: ORDERS (Incremental merge — no CDC, watermark dedup) ───────────
CREATE OR REFRESH STREAMING LIVE TABLE silver_orders_validated
COMMENT "Orders — validated, nulls handled, amounts checked"
(
  CONSTRAINT order_id_not_null      EXPECT (order_id IS NOT NULL)     ON VIOLATION FAIL UPDATE,
  CONSTRAINT customer_id_not_null   EXPECT (customer_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT positive_total_amount  EXPECT (total_amount > 0)         ON VIOLATION DROP ROW,
  CONSTRAINT valid_status           EXPECT (status IN (
      'placed','confirmed','processing','shipped','delivered','cancelled','returned'
  )) ON VIOLATION WARN,
  CONSTRAINT valid_currency         EXPECT (currency IN ('USD','EUR','GBP','AUD','CAD','JPY','SGD'))
                                                                       ON VIOLATION WARN
)
AS SELECT * FROM STREAM(LIVE.bronze_orders_stream);

CREATE OR REFRESH STREAMING LIVE TABLE silver_orders_quarantine
COMMENT "Orders that failed DROP ROW constraints — for ops investigation"
AS SELECT
    order_id,
    customer_id,
    total_amount,
    status,
    _ingest_time,
    "failed_bronze_constraints" AS _quarantine_reason
FROM STREAM(LIVE.bronze_orders_stream)
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR total_amount <= 0;

-- Deduped + cleaned orders (use APPLY CHANGES for upsert by updated_at)
CREATE OR REFRESH STREAMING LIVE TABLE silver_orders
COMMENT "Silver orders — deduplicated, cleansed"
TBLPROPERTIES (
    "quality" = "silver",
    "delta.enableChangeDataFeed" = "true"
);
APPLY CHANGES INTO LIVE.silver_orders
FROM STREAM(LIVE.silver_orders_validated)
KEYS (order_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;

-- ── SILVER: SHIPMENTS ─────────────────────────────────────────────────────
CREATE OR REFRESH STREAMING LIVE TABLE silver_shipments
COMMENT "Silver shipments — validated, typed"
TBLPROPERTIES ("quality" = "silver", "delta.enableChangeDataFeed" = "true")
(
  CONSTRAINT shipment_id_not_null EXPECT (shipment_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity       EXPECT (quantity > 0)             ON VIOLATION WARN,
  CONSTRAINT valid_total_cost     EXPECT (total_cost > 0)           ON VIOLATION WARN
)
AS SELECT
    shipment_id,
    supplier_id,
    destination_warehouse_id AS warehouse_id,
    product_id,
    CAST(quantity   AS INT)    AS quantity,
    CAST(unit_cost  AS DOUBLE) AS unit_cost,
    CAST(total_cost AS DOUBLE) AS total_cost,
    TRY_CAST(ship_date          AS DATE) AS ship_date,
    TRY_CAST(eta_date           AS DATE) AS eta_date,
    TRY_CAST(actual_arrival_date AS DATE) AS actual_arrival_date,
    carrier,
    tracking_number,
    status,
    DATEDIFF(
        TRY_CAST(actual_arrival_date AS DATE),
        TRY_CAST(eta_date AS DATE)
    ) AS delivery_delay_days,
    _ingest_time
FROM STREAM(LIVE.bronze_shipments_stream);

-- ── SILVER: CDC PRODUCTS (from cdc_products_raw) ──────────────────────────
-- Applies CDC change records on top of silver_products using APPLY CHANGES
-- This simulates the ADF CDC activity output being processed by DLT
CREATE OR REFRESH STREAMING LIVE TABLE silver_cdc_products_incoming
COMMENT "CDC product change events from SQL Server"
AS SELECT
    after.product_id  AS product_id,
    after.unit_price  AS unit_price,
    after.is_active   AS is_active,
    after.updated_at  AS updated_at,
    operation,
    commit_timestamp  AS cdc_timestamp,
    lsn
FROM STREAM(${catalog}.bronze.cdc_products_raw)
WHERE operation IN ('INSERT', 'UPDATE')
  AND after IS NOT NULL;

-- ================================================================
--  LAYER 3: GOLD — SERVING LAYER (Materialized Views)
-- ================================================================

-- ── GOLD: dim_warehouse (full load — static, rarely changes) ──────────────
CREATE OR REFRESH LIVE TABLE gold_dim_warehouse
COMMENT "Gold dimension: warehouses — Type 1 (current state)"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
    warehouse_id,
    warehouse_name,
    region,
    country,
    city,
    state,
    zip_code,
    CAST(capacity_sqft  AS INT)  AS capacity_sqft,
    CAST(is_active AS BOOLEAN)   AS is_active,
    TRY_CAST(opened_date AS DATE) AS opened_date,
    current_timestamp()          AS _gold_updated_at
FROM ${catalog}.bronze.warehouses_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY _ingest_time DESC) = 1;

-- ── GOLD: dim_supplier (Type 1 — latest state from Silver) ────────────────
CREATE OR REFRESH LIVE TABLE gold_dim_supplier
COMMENT "Gold dimension: suppliers — Type 1 current"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
    supplier_id,
    supplier_name,
    country,
    city,
    CAST(rating AS DOUBLE)          AS rating,
    CAST(lead_time_days AS INT)     AS lead_time_days,
    CAST(payment_terms_days AS INT) AS payment_terms_days,
    CAST(is_active AS BOOLEAN)      AS is_active,
    contract_start_date,
    current_timestamp()             AS _gold_updated_at
FROM LIVE.silver_suppliers
WHERE __END_AT IS NULL;   -- SCD1 APPLY CHANGES: only current row

-- ── GOLD: fact_shipments ───────────────────────────────────────────────────
CREATE OR REFRESH LIVE TABLE gold_fact_shipments
COMMENT "Gold fact: shipments — joins Silver shipments with dims"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
    s.shipment_id,
    s.supplier_id,
    s.warehouse_id,
    s.product_id,
    s.quantity,
    s.unit_cost,
    s.total_cost,
    s.ship_date,
    s.eta_date,
    s.actual_arrival_date,
    s.carrier,
    s.status,
    s.delivery_delay_days,
    -- Late delivery flag
    CASE WHEN s.delivery_delay_days > 0 THEN TRUE ELSE FALSE END AS is_late,
    -- SLA breach (supplier lead time)
    CASE WHEN s.delivery_delay_days > sup.lead_time_days THEN TRUE ELSE FALSE END AS sla_breached,
    sup.supplier_name,
    sup.country      AS supplier_country,
    sup.rating       AS supplier_rating,
    wh.warehouse_name,
    wh.region        AS warehouse_region,
    current_timestamp() AS _gold_updated_at
FROM LIVE.silver_shipments s
LEFT JOIN LIVE.gold_dim_supplier  sup ON s.supplier_id  = sup.supplier_id
LEFT JOIN LIVE.gold_dim_warehouse wh  ON s.warehouse_id = wh.warehouse_id;

-- ── GOLD: agg_supplier_performance ────────────────────────────────────────
CREATE OR REFRESH LIVE TABLE gold_agg_supplier_performance
COMMENT "Gold aggregate: supplier performance KPIs — feeds dashboard"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
    supplier_id,
    supplier_name,
    supplier_country,
    supplier_rating,
    COUNT(*)                                          AS total_shipments,
    SUM(quantity)                                     AS total_units_shipped,
    SUM(total_cost)                                   AS total_spend_usd,
    AVG(total_cost)                                   AS avg_shipment_value_usd,
    SUM(CASE WHEN is_late    THEN 1 ELSE 0 END)       AS late_shipments,
    SUM(CASE WHEN sla_breached THEN 1 ELSE 0 END)     AS sla_breaches,
    AVG(delivery_delay_days)                          AS avg_delay_days,
    ROUND(
        SUM(CASE WHEN NOT is_late THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                 AS on_time_delivery_pct,
    MIN(ship_date)                                    AS first_shipment_date,
    MAX(ship_date)                                    AS last_shipment_date,
    current_timestamp()                               AS _computed_at
FROM LIVE.gold_fact_shipments
GROUP BY supplier_id, supplier_name, supplier_country, supplier_rating;

-- ── GOLD: agg_daily_order_summary ─────────────────────────────────────────
CREATE OR REFRESH LIVE TABLE gold_agg_daily_order_summary
COMMENT "Gold aggregate: daily order metrics — rolling window for finance dashboard"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
    CAST(order_date AS DATE)             AS order_date,
    currency,
    shipping_address_country             AS country,
    channel,
    status,
    COUNT(*)                             AS order_count,
    SUM(total_amount)                    AS gross_revenue,
    SUM(discount_amount)                 AS total_discounts,
    SUM(tax_amount)                      AS total_tax,
    SUM(shipping_amount)                 AS total_shipping,
    SUM(total_amount - discount_amount)  AS net_revenue,
    AVG(total_amount)                    AS avg_order_value,
    COUNT(DISTINCT customer_id)          AS unique_customers,
    COUNT(DISTINCT warehouse_id)         AS fulfilling_warehouses,
    current_timestamp()                  AS _computed_at
FROM LIVE.silver_orders
WHERE order_date IS NOT NULL
GROUP BY
    CAST(order_date AS DATE), currency,
    shipping_address_country, channel, status;
