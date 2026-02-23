# Databricks notebook source
# MAGIC %md
# MAGIC # 01b Silver DDL — Additions
# MAGIC
# MAGIC Adds new derived columns to existing silver tables to support
# MAGIC the expanded transformation suite in notebooks 02–05.
# MAGIC Run ONCE before re-running any silver transform notebook.
# MAGIC
# MAGIC | Table | New Columns | Transformation Technique |
# MAGIC |-------|-------------|--------------------------|
# MAGIC | suppliers | phone_normalized, supplier_rank_in_country | regexp_replace, window rank |
# MAGIC | products | category_l1, category_l2 | split array indexing |
# MAGIC | supplier_contracts | contract_duration_months | months_between |
# MAGIC | customers | email_domain, account_age_months, customer_rank_in_segment | regexp_extract, months_between, window dense_rank |
# MAGIC | orders | month_key | date_trunc |
# MAGIC | clickstream | event_hour, device_category | hour(), when/otherwise |
# MAGIC | device_telemetry | health_score | composite formula |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
SILVER = "omnioc_dev.silver"
print(f"✅ Target: {SILVER}")
print("   Running ALTER TABLE ADD COLUMN — safe to rerun (IF NOT EXISTS)")

# COMMAND ----------
# DBTITLE 1, Cell 2 — suppliers additions
spark.sql(f"ALTER TABLE {SILVER}.suppliers ADD COLUMNS (phone_normalized STRING COMMENT 'Phone stripped to digits only via regexp_replace')")
spark.sql(f"ALTER TABLE {SILVER}.suppliers ADD COLUMNS (supplier_rank_in_country INT COMMENT 'Rank of supplier by rating within country — window rank()')")
print("✅ suppliers — phone_normalized, supplier_rank_in_country")

# COMMAND ----------
# DBTITLE 1, Cell 3 — products additions
spark.sql(f"ALTER TABLE {SILVER}.products ADD COLUMNS (category_l1 STRING COMMENT 'Top-level category extracted via split() on category path')")
spark.sql(f"ALTER TABLE {SILVER}.products ADD COLUMNS (category_l2 STRING COMMENT 'Second-level category extracted via split() on sub_category path')")
print("✅ products — category_l1, category_l2")

# COMMAND ----------
# DBTITLE 1, Cell 4 — supplier_contracts additions
spark.sql(f"ALTER TABLE {SILVER}.supplier_contracts ADD COLUMNS (contract_duration_months DOUBLE COMMENT 'Contract length in months — months_between(end_date, start_date)')")
print("✅ supplier_contracts — contract_duration_months")

# COMMAND ----------
# DBTITLE 1, Cell 5 — customers additions
spark.sql(f"ALTER TABLE {SILVER}.customers ADD COLUMNS (email_domain STRING COMMENT 'Email domain extracted via regexp_extract')")
spark.sql(f"ALTER TABLE {SILVER}.customers ADD COLUMNS (account_age_months INT COMMENT 'Account tenure in months — months_between')")
spark.sql(f"ALTER TABLE {SILVER}.customers ADD COLUMNS (customer_rank_in_segment INT COMMENT 'Customer rank by lifetime_value within segment — window dense_rank()')")
print("✅ customers — email_domain, account_age_months, customer_rank_in_segment")

# COMMAND ----------
# DBTITLE 1, Cell 6 — orders additions
spark.sql(f"ALTER TABLE {SILVER}.orders ADD COLUMNS (month_key STRING COMMENT 'Year-month bucket for partitioned reporting — date_trunc month')")
print("✅ orders — month_key")

# COMMAND ----------
# DBTITLE 1, Cell 7 — clickstream additions
spark.sql(f"ALTER TABLE {SILVER}.clickstream ADD COLUMNS (event_hour INT COMMENT 'Hour of event (0-23) — for intraday analysis')")
spark.sql(f"ALTER TABLE {SILVER}.clickstream ADD COLUMNS (device_category STRING COMMENT 'Coarser device grouping: touch | desktop | unknown')")
print("✅ clickstream — event_hour, device_category")

# COMMAND ----------
# DBTITLE 1, Cell 8 — device_telemetry additions
spark.sql(f"ALTER TABLE {SILVER}.device_telemetry ADD COLUMNS (health_score DOUBLE COMMENT 'Composite device health 0-100: weighted average of inverted cpu/mem/disk/error metrics')")
print("✅ device_telemetry — health_score")

# COMMAND ----------
# DBTITLE 1, Cell 9 — Verify additions
print("── New Column Verification ───────────────────────────────────────────")
checks = {
    "suppliers":          ["phone_normalized", "supplier_rank_in_country"],
    "products":           ["category_l1", "category_l2"],
    "supplier_contracts": ["contract_duration_months"],
    "customers":          ["email_domain", "account_age_months", "customer_rank_in_segment"],
    "orders":             ["month_key"],
    "clickstream":        ["event_hour", "device_category"],
    "device_telemetry":   ["health_score"],
}
for tbl, cols in checks.items():
    schema_cols = [f.name for f in spark.table(f"{SILVER}.{tbl}").schema.fields]
    for c in cols:
        found = c in schema_cols
        print(f"  {'✅' if found else '❌'}  {tbl}.{c}")
print("\n  ✅ DDL additions complete — proceed to silver transforms")
