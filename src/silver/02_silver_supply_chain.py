# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver — Supply Chain Transforms
# MAGIC
# MAGIC Reads bronze batch tables, casts types, deduplicates, and MERGEs into silver using Spark SQL.
# MAGIC
# MAGIC | Cell | Bronze Source | Silver Target |
# MAGIC |------|--------------|---------------|
# MAGIC | 2 | suppliers_raw | suppliers |
# MAGIC | 3 | products_raw | products |
# MAGIC | 4 | warehouses_raw | warehouses |
# MAGIC | 5 | shipments_raw | shipments |
# MAGIC | 6 | supplier_contracts_raw | supplier_contracts |
# MAGIC | 7 | Verification |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import col, to_timestamp, to_date, current_timestamp, lit, row_number
from pyspark.sql.window import Window

BRONZE = "omnioc_dev.bronze"
SILVER = "omnioc_dev.silver"

def dedup(df, pk):
    """Keep latest row per primary key based on _ingest_time."""
    w = Window.partitionBy(pk).orderBy(col("_ingest_time").desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

print(f"✅ Config ready — Bronze: {BRONZE}  Silver: {SILVER}")

# COMMAND ----------
# DBTITLE 1, Cell 2 — suppliers
dedup(spark.table(f"{BRONZE}.suppliers_raw"), "supplier_id").select(
    col("supplier_id"),
    col("supplier_name"),
    col("country"),
    col("city"),
    col("contact_email"),
    col("contact_phone"),
    col("rating").cast("double"),
    col("lead_time_days").cast("int"),
    col("payment_terms_days").cast("int"),
    col("is_active").cast("boolean"),
    to_date(col("contract_start_date")).alias("contract_start_date"),
    to_timestamp(col("created_at")).alias("created_at"),
    to_timestamp(col("updated_at")).alias("updated_at"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("suppliers_raw").alias("_bronze_source"),
).createOrReplaceTempView("suppliers_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.suppliers AS t
    USING suppliers_stage AS s
    ON t.supplier_id = s.supplier_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ suppliers — {spark.table(f'{SILVER}.suppliers').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 3 — products
dedup(spark.table(f"{BRONZE}.products_raw"), "product_id").select(
    col("product_id"),
    col("product_name"),
    col("sku"),
    col("category"),
    col("sub_category"),
    col("unit_cost").cast("double"),
    col("unit_price").cast("double"),
    col("weight_kg").cast("double"),
    col("is_active").cast("boolean"),
    col("supplier_id"),
    col("reorder_point").cast("int"),
    col("reorder_quantity").cast("int"),
    to_timestamp(col("created_at")).alias("created_at"),
    to_timestamp(col("updated_at")).alias("updated_at"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("products_raw").alias("_bronze_source"),
).createOrReplaceTempView("products_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.products AS t
    USING products_stage AS s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ products — {spark.table(f'{SILVER}.products').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 4 — warehouses
dedup(spark.table(f"{BRONZE}.warehouses_raw"), "warehouse_id").select(
    col("warehouse_id"),
    col("warehouse_name"),
    col("country"),
    col("city"),
    col("region"),
    col("capacity_sqft").cast("int"),
    col("is_active").cast("boolean"),
    col("manager_name"),
    col("contact_email"),
    to_timestamp(col("created_at")).alias("created_at"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("warehouses_raw").alias("_bronze_source"),
).createOrReplaceTempView("warehouses_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.warehouses AS t
    USING warehouses_stage AS s
    ON t.warehouse_id = s.warehouse_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ warehouses — {spark.table(f'{SILVER}.warehouses').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 5 — shipments
dedup(spark.table(f"{BRONZE}.shipments_raw"), "shipment_id").select(
    col("shipment_id"),
    col("order_id"),
    col("supplier_id"),
    col("warehouse_id"),
    col("product_id"),
    col("quantity").cast("int"),
    to_date(col("shipment_date")).alias("shipment_date"),
    to_date(col("expected_arrival")).alias("expected_arrival"),
    to_date(col("actual_arrival")).alias("actual_arrival"),
    col("carrier"),
    col("tracking_number"),
    col("status"),
    col("freight_cost").cast("double"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("shipments_raw").alias("_bronze_source"),
).createOrReplaceTempView("shipments_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.shipments AS t
    USING shipments_stage AS s
    ON t.shipment_id = s.shipment_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ shipments — {spark.table(f'{SILVER}.shipments').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 6 — supplier_contracts
dedup(spark.table(f"{BRONZE}.supplier_contracts_raw"), "contract_id").select(
    col("contract_id"),
    col("supplier_id"),
    col("contract_type"),
    to_date(col("start_date")).alias("start_date"),
    to_date(col("end_date")).alias("end_date"),
    col("annual_value_usd").cast("double"),
    col("payment_terms_days").cast("int"),
    col("auto_renew").cast("boolean"),
    col("is_active").cast("boolean"),
    col("signed_by"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("supplier_contracts_raw").alias("_bronze_source"),
).createOrReplaceTempView("supplier_contracts_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.supplier_contracts AS t
    USING supplier_contracts_stage AS s
    ON t.contract_id = s.contract_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ supplier_contracts — {spark.table(f'{SILVER}.supplier_contracts').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 7 — Verification
print("── Supply Chain Silver — Row Counts ────────────────────────────────")
for tbl, pk in [
    ("suppliers",          "supplier_id"),
    ("products",           "product_id"),
    ("warehouses",         "warehouse_id"),
    ("shipments",          "shipment_id"),
    ("supplier_contracts", "contract_id"),
]:
    df    = spark.table(f"{SILVER}.{tbl}")
    cnt   = df.count()
    nulls = df.filter(col(pk).isNull()).count()
    print(f"  {'✅' if cnt > 0 else '❌'}  {tbl:<25}  {cnt:>6,} rows  |  PK nulls: {nulls}")
