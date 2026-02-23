# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver — Supply Chain Transforms
# MAGIC
# MAGIC Reads from bronze batch tables, casts types, deduplicates, and MERGEs into silver.
# MAGIC
# MAGIC | Cell | Bronze Source | Silver Target | Key Cast |
# MAGIC |------|--------------|---------------|----------|
# MAGIC | 2 | suppliers_raw | suppliers | contract_start_date, created_at, updated_at → DATE/TIMESTAMP |
# MAGIC | 3 | products_raw | products | created_at, updated_at → TIMESTAMP |
# MAGIC | 4 | warehouses_raw | warehouses | created_at → TIMESTAMP |
# MAGIC | 5 | shipments_raw | shipments | shipment_date, expected_arrival, actual_arrival → DATE |
# MAGIC | 6 | supplier_contracts_raw | supplier_contracts | start_date, end_date → DATE |
# MAGIC | 7 | Verification |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, lit,
    row_number, md5, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE  = "omnioc_dev.bronze"
SILVER  = "omnioc_dev.silver"
SOURCE  = "adls_csv"

def dedup(df, pk):
    """Keep latest row per primary key based on _ingest_time."""
    w = Window.partitionBy(pk).orderBy(col("_ingest_time").desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

def merge_into(df, target_table, pk):
    """MERGE df into silver target on primary key."""
    dt = DeltaTable.forName(spark, f"{SILVER}.{target_table}")
    (
        dt.alias("t")
        .merge(df.alias("s"), f"t.{pk} = s.{pk}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"✅ Config ready — Bronze: {BRONZE}  Silver: {SILVER}")

# COMMAND ----------
# DBTITLE 1, Cell 2 — suppliers
df = spark.table(f"{BRONZE}.suppliers_raw")

df_clean = dedup(df, "supplier_id").select(
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
)

merge_into(df_clean, "suppliers", "supplier_id")
print(f"✅ suppliers — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 3 — products
df = spark.table(f"{BRONZE}.products_raw")

df_clean = dedup(df, "product_id").select(
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
)

merge_into(df_clean, "products", "product_id")
print(f"✅ products — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 4 — warehouses
df = spark.table(f"{BRONZE}.warehouses_raw")

df_clean = dedup(df, "warehouse_id").select(
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
)

merge_into(df_clean, "warehouses", "warehouse_id")
print(f"✅ warehouses — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 5 — shipments
df = spark.table(f"{BRONZE}.shipments_raw")

df_clean = dedup(df, "shipment_id").select(
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
)

merge_into(df_clean, "shipments", "shipment_id")
print(f"✅ shipments — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 6 — supplier_contracts
df = spark.table(f"{BRONZE}.supplier_contracts_raw")

df_clean = dedup(df, "contract_id").select(
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
)

merge_into(df_clean, "supplier_contracts", "contract_id")
print(f"✅ supplier_contracts — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 7 — Verification
print("── Supply Chain Silver — Row Counts ────────────────────────────────")
for tbl in ["suppliers", "products", "warehouses", "shipments", "supplier_contracts"]:
    df  = spark.table(f"{SILVER}.{tbl}")
    cnt = df.count()
    nulls = df.filter(col(list(df.columns)[0]).isNull()).count()
    status = "✅" if cnt > 0 else "❌"
    print(f"  {status}  {tbl:<25}  {cnt:>6,} rows  |  PK nulls: {nulls}")
