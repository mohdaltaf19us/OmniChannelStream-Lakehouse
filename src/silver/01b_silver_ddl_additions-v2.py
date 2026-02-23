# Databricks notebook source
# MAGIC %md
# MAGIC # 01b Silver DDL — Additions
# MAGIC
# MAGIC Adds ALL new derived columns required by notebooks 02–05.
# MAGIC Run ONCE before any silver transform notebook.
# MAGIC Safe to rerun — ALTER TABLE ADD COLUMN is idempotent if column already exists.
# MAGIC
# MAGIC Generated columns (_silver_date, _event_date, _valid_from_date, _quarantine_date)
# MAGIC are already defined in 01_silver_ddl.py and are NEVER written explicitly.

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
SILVER = "omnioc_dev.silver"

def add_col(table, col_def):
    """Add column — swallow error if already exists."""
    try:
        spark.sql(f"ALTER TABLE {SILVER}.{table} ADD COLUMN {col_def}")
        print(f"  ✅ {table}.{col_def.split()[0]}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  ⏭  {table}.{col_def.split()[0]} (already exists)")
        else:
            raise

print(f"Target: {SILVER}")

# COMMAND ----------
# DBTITLE 1, Cell 2 — suppliers additions
# New derived columns not in original DDL
add_col("suppliers", "phone_normalized         STRING  COMMENT 'Phone stripped to digits only — regexp_replace'")
add_col("suppliers", "payment_tier             STRING  COMMENT 'Gold/Silver/Bronze — based on payment_terms_days'")
add_col("suppliers", "lead_time_category       STRING  COMMENT 'fast/standard/slow — based on lead_time_days'")
add_col("suppliers", "is_high_rating           BOOLEAN COMMENT 'True when rating >= 4.0'")
add_col("suppliers", "supplier_rank_in_country INT     COMMENT 'Rank by rating within country — window rank()'")
add_col("suppliers", "country_avg_rating       DOUBLE  COMMENT 'Running avg rating for country — avg() over window'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 3 — products additions
add_col("products", "category_l1      STRING  COMMENT 'Top-level category — split(category,\"/\")[0]'")
add_col("products", "category_l2      STRING  COMMENT 'Top-level sub_category — split(sub_category,\"/\")[0]'")
add_col("products", "margin_pct       DOUBLE  COMMENT 'Gross margin % — (unit_price-unit_cost)/unit_price*100'")
add_col("products", "markup_pct       DOUBLE  COMMENT 'Markup % over cost — (unit_price-unit_cost)/unit_cost*100'")
add_col("products", "price_tier       STRING  COMMENT 'budget/mid/premium — based on unit_price'")
add_col("products", "is_high_margin   BOOLEAN COMMENT 'True when margin_pct >= 40'")
add_col("products", "unit_margin_usd  DOUBLE  COMMENT 'Absolute margin per unit in USD'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 4 — warehouses additions
add_col("warehouses", "capacity_tier  STRING  COMMENT 'small/medium/large — based on capacity_sqft'")
add_col("warehouses", "country_code   STRING  COMMENT 'Normalised uppercase country code'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 5 — shipments additions
add_col("shipments", "is_delayed              BOOLEAN COMMENT 'True when actual_arrival > expected_arrival'")
add_col("shipments", "delay_days              INT     COMMENT 'Positive = late, negative = early, null = in_transit'")
add_col("shipments", "transit_days            INT     COMMENT 'Actual transit days from shipment_date to actual_arrival'")
add_col("shipments", "expected_transit_days   INT     COMMENT 'Planned transit days from shipment_date to expected_arrival'")
add_col("shipments", "delivery_status         STRING  COMMENT 'in_transit / on_time_or_early / delayed'")
add_col("shipments", "freight_cost_per_unit   DOUBLE  COMMENT 'freight_cost / quantity'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 6 — supplier_contracts additions
add_col("supplier_contracts", "contract_duration_months  DOUBLE  COMMENT 'Contract length in months — months_between(end_date, start_date)'")
add_col("supplier_contracts", "start_quarter             STRING  COMMENT 'Quarter bucket for start_date — date_trunc quarter'")
add_col("supplier_contracts", "days_until_expiry         INT     COMMENT 'Days from today to end_date'")
add_col("supplier_contracts", "is_expiring_soon          BOOLEAN COMMENT 'True when days_until_expiry between 0 and 90'")
add_col("supplier_contracts", "is_expired                BOOLEAN COMMENT 'True when end_date < today'")
add_col("supplier_contracts", "contract_duration_days    INT     COMMENT 'Total contract length in days'")
add_col("supplier_contracts", "contract_value_tier       STRING  COMMENT 'low/mid/high — based on annual_value_usd'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 7 — customers additions
add_col("customers", "full_name                  STRING  COMMENT 'first_name + last_name concatenated'")
add_col("customers", "full_name_ascii             STRING  COMMENT 'ASCII-safe name — translate accent chars'")
add_col("customers", "email_domain               STRING  COMMENT 'Domain extracted from email — regexp_extract'")
add_col("customers", "account_age_months         INT     COMMENT 'Account tenure in months — months_between'")
add_col("customers", "account_age_days           INT     COMMENT 'Account tenure in days — datediff'")
add_col("customers", "customer_tier              STRING  COMMENT 'VIP/Premium/Standard — based on lifetime_value'")
add_col("customers", "is_high_value              BOOLEAN COMMENT 'True when lifetime_value >= 10000'")
add_col("customers", "country_code               STRING  COMMENT 'Normalised uppercase country code'")
add_col("customers", "customer_rank_in_segment   INT     COMMENT 'Dense rank by LTV within segment — dense_rank()'")
add_col("customers", "ltv_score                  DOUBLE  COMMENT 'Composite LTV score 0-100 — pandas_udf'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 8 — orders additions
add_col("orders", "month_key             STRING  COMMENT 'Year-month reporting key — date_trunc month'")
add_col("orders", "net_amount            DOUBLE  COMMENT 'total_amount - discount_amount'")
add_col("orders", "gross_revenue         DOUBLE  COMMENT 'total_amount + tax_amount + shipping_amount'")
add_col("orders", "discount_rate_pct     DOUBLE  COMMENT 'discount_amount / total_amount * 100'")
add_col("orders", "is_discounted         BOOLEAN COMMENT 'True when discount_amount > 0'")
add_col("orders", "order_value_tier      STRING  COMMENT 'micro/small/mid/large — based on total_amount'")
add_col("orders", "is_marketplace_order  BOOLEAN COMMENT 'True when channel = marketplace'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 9 — order_lines additions
# line_total already exists in DDL — only adding new derived cols
add_col("order_lines", "effective_unit_price  DOUBLE  COMMENT 'unit_price after discount'")
add_col("order_lines", "discount_amount       DOUBLE  COMMENT 'Absolute discount value on this line'")
add_col("order_lines", "is_discounted_line    BOOLEAN COMMENT 'True when discount_pct > 0'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 10 — payments additions
add_col("payments", "is_successful     BOOLEAN COMMENT 'True when status = completed'")
add_col("payments", "is_failed         BOOLEAN COMMENT 'True when status = failed'")
add_col("payments", "is_refunded       BOOLEAN COMMENT 'True when status = refunded'")
add_col("payments", "is_card_payment   BOOLEAN COMMENT 'True when credit_card or debit_card'")
add_col("payments", "is_digital_payment BOOLEAN COMMENT 'True when card, paypal, or crypto'")
add_col("payments", "payment_category  STRING  COMMENT 'card/digital_wallet/bank/crypto/other'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 11 — refunds additions
add_col("refunds", "is_high_value_refund      BOOLEAN COMMENT 'True when refund_amount >= 500'")
add_col("refunds", "is_processed              BOOLEAN COMMENT 'True when status = completed'")
add_col("refunds", "is_rejected               BOOLEAN COMMENT 'True when status = rejected'")
add_col("refunds", "refund_reason_category    STRING  COMMENT 'product_issue/fulfillment_issue/customer_initiated/other'")
add_col("refunds", "next_refund_date          DATE    COMMENT 'Next refund date for same customer — lead()'")
add_col("refunds", "days_to_next_refund       INT     COMMENT 'Days until next refund — repeat refunder signal'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 12 — exchange_rates additions
add_col("exchange_rates", "inverse_rate    DOUBLE  COMMENT '1 / exchange_rate — reverse direction rate'")
add_col("exchange_rates", "pair_key        STRING  COMMENT 'FROM_TO pair key e.g. USD_EUR'")
add_col("exchange_rates", "is_major_pair   BOOLEAN COMMENT 'Both currencies in G7 major list'")
add_col("exchange_rates", "rate_age_days   INT     COMMENT 'Days since rate_date — freshness indicator'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 13 — customers_scd additions
add_col("customers_scd", "full_name                  STRING  COMMENT 'first_name + last_name'")
add_col("customers_scd", "full_name_ascii             STRING  COMMENT 'ASCII-safe name — translate'")
add_col("customers_scd", "email_domain               STRING  COMMENT 'Domain from email'")
add_col("customers_scd", "account_age_months         INT     COMMENT 'Tenure in months at time of version'")
add_col("customers_scd", "customer_tier              STRING  COMMENT 'VIP/Premium/Standard at this version'")
add_col("customers_scd", "is_high_value              BOOLEAN COMMENT 'High value flag at this version'")
add_col("customers_scd", "country_code               STRING  COMMENT 'Normalised country code'")
add_col("customers_scd", "customer_rank_in_segment   INT     COMMENT 'Segment rank at this version'")
add_col("customers_scd", "ltv_score                  DOUBLE  COMMENT 'LTV score at this version'")
add_col("customers_scd", "_prior_hash                STRING  COMMENT 'Previous _record_hash — lag() for audit trail'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 14 — products_scd additions
add_col("products_scd", "category_l1      STRING  COMMENT 'Top-level category'")
add_col("products_scd", "category_l2      STRING  COMMENT 'Top-level sub_category'")
add_col("products_scd", "margin_pct       DOUBLE  COMMENT 'Gross margin % at this version'")
add_col("products_scd", "markup_pct       DOUBLE  COMMENT 'Markup % at this version'")
add_col("products_scd", "price_tier       STRING  COMMENT 'Price tier at this version'")
add_col("products_scd", "is_high_margin   BOOLEAN COMMENT 'High margin flag at this version'")
add_col("products_scd", "unit_margin_usd  DOUBLE  COMMENT 'Absolute margin per unit at this version'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 15 — clickstream additions
add_col("clickstream", "utm_source           STRING  COMMENT 'UTM source extracted from page_url — regexp_extract'")
add_col("clickstream", "event_hour           INT     COMMENT 'Hour of event (0-23) — hour(event_time)'")
add_col("clickstream", "device_category      STRING  COMMENT 'touch/desktop/unknown — coarser device grouping'")
add_col("clickstream", "product_name         STRING  COMMENT 'Product name from stream-static join'")
add_col("clickstream", "product_category_l1  STRING  COMMENT 'Product category L1 from stream-static join'")
add_col("clickstream", "product_price_tier   STRING  COMMENT 'Product price tier from stream-static join'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 16 — device_telemetry additions
add_col("device_telemetry", "health_score  DOUBLE  COMMENT 'Composite device health 0-100 — weighted cpu/mem/disk/temp score'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 17 — order_events additions
add_col("order_events", "event_hour_bucket  TIMESTAMP  COMMENT 'event_time truncated to hour — date_trunc'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 18 — dq_alerts additions
add_col("dq_alerts", "sample_failed_cleaned        ARRAY<STRING>  COMMENT 'sample_failed_values with NULLs removed — array_filter'")
add_col("dq_alerts", "unique_failure_sample_count  INT            COMMENT 'Count of distinct failure samples — array_distinct + size'")
print()

# COMMAND ----------
# DBTITLE 1, Cell 19 — Verification
print("── DDL Additions Verification ──────────────────────────────────────")
checks = {
    "suppliers":          ["phone_normalized", "payment_tier", "lead_time_category", "is_high_rating", "supplier_rank_in_country", "country_avg_rating"],
    "products":           ["category_l1", "category_l2", "margin_pct", "markup_pct", "price_tier", "is_high_margin", "unit_margin_usd"],
    "warehouses":         ["capacity_tier", "country_code"],
    "shipments":          ["is_delayed", "delay_days", "delivery_status", "freight_cost_per_unit"],
    "supplier_contracts": ["contract_duration_months", "days_until_expiry", "contract_value_tier"],
    "customers":          ["full_name", "email_domain", "customer_tier", "customer_rank_in_segment", "ltv_score"],
    "orders":             ["month_key", "net_amount", "order_value_tier"],
    "order_lines":        ["effective_unit_price", "discount_amount", "is_discounted_line"],
    "payments":           ["is_successful", "payment_category"],
    "refunds":            ["refund_reason_category", "days_to_next_refund"],
    "exchange_rates":     ["inverse_rate", "pair_key", "is_major_pair"],
    "customers_scd":      ["full_name", "customer_tier", "ltv_score", "_prior_hash"],
    "products_scd":       ["margin_pct", "price_tier", "category_l1"],
    "clickstream":        ["utm_source", "event_hour", "device_category"],
    "device_telemetry":   ["health_score"],
    "order_events":       ["event_hour_bucket"],
    "dq_alerts":          ["unique_failure_sample_count"],
}
for tbl, cols in checks.items():
    schema_cols = [f.name for f in spark.table(f"{SILVER}.{tbl}").schema.fields]
    all_ok = all(c in schema_cols for c in cols)
    print(f"  {'✅' if all_ok else '❌'}  {tbl}")
    for c in cols:
        if c not in schema_cols:
            print(f"       ❌ missing: {c}")

print("\n✅ DDL additions complete — proceed to silver transforms")
