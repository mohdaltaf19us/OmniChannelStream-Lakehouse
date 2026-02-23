# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver — Finance Transforms
# MAGIC
# MAGIC | Cell | Bronze Source | Silver Target | Key Cast |
# MAGIC |------|--------------|---------------|----------|
# MAGIC | 2 | customers_raw | customers | registration_date → DATE, created_at/updated_at → TIMESTAMP |
# MAGIC | 3 | orders_raw | orders | order_date → DATE, created_at/updated_at → TIMESTAMP |
# MAGIC | 4 | order_lines_raw | order_lines | line_total derived |
# MAGIC | 5 | payments_raw | payments | payment_date → DATE |
# MAGIC | 6 | refunds_raw | refunds | refund_date → DATE |
# MAGIC | 7 | exchange_rates_raw | exchange_rates | rate_date → DATE |
# MAGIC | 8 | Verification |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, lit,
    row_number, round as spark_round, expr
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE = "omnioc_dev.bronze"
SILVER = "omnioc_dev.silver"

def dedup(df, pk):
    w = Window.partitionBy(pk).orderBy(col("_ingest_time").desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

def merge_into(df, target_table, pk):
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
# DBTITLE 1, Cell 2 — customers
df = spark.table(f"{BRONZE}.customers_raw")

df_clean = dedup(df, "customer_id").select(
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("phone"),
    col("country"),
    col("city"),
    col("segment"),
    to_date(col("registration_date")).alias("registration_date"),
    col("is_active").cast("boolean"),
    col("lifetime_value").cast("double"),
    to_timestamp(col("created_at")).alias("created_at"),
    to_timestamp(col("updated_at")).alias("updated_at"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("customers_raw").alias("_bronze_source"),
)

merge_into(df_clean, "customers", "customer_id")
print(f"✅ customers — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 3 — orders
df = spark.table(f"{BRONZE}.orders_raw")

df_clean = dedup(df, "order_id").select(
    col("order_id"),
    col("customer_id"),
    col("warehouse_id"),
    to_date(col("order_date")).alias("order_date"),
    col("status"),
    col("channel"),
    col("currency"),
    col("total_amount").cast("double"),
    col("discount_amount").cast("double"),
    col("tax_amount").cast("double"),
    col("shipping_amount").cast("double"),
    col("shipping_country"),
    to_timestamp(col("created_at")).alias("created_at"),
    to_timestamp(col("updated_at")).alias("updated_at"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("orders_raw").alias("_bronze_source"),
)

merge_into(df_clean, "orders", "order_id")
print(f"✅ orders — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 4 — order_lines
df = spark.table(f"{BRONZE}.order_lines_raw")

df_clean = dedup(df, "line_id").select(
    col("line_id"),
    col("order_id"),
    col("product_id"),
    col("quantity").cast("int"),
    col("unit_price").cast("double"),
    col("discount_pct").cast("double"),
    # Derive line_total — quantity × unit_price × (1 - discount_pct/100)
    spark_round(
        col("quantity").cast("double") *
        col("unit_price").cast("double") *
        (1 - col("discount_pct").cast("double") / 100),
        2
    ).alias("line_total"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("order_lines_raw").alias("_bronze_source"),
)

merge_into(df_clean, "order_lines", "line_id")
print(f"✅ order_lines — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 5 — payments
df = spark.table(f"{BRONZE}.payments_raw")

df_clean = dedup(df, "payment_id").select(
    col("payment_id"),
    col("order_id"),
    col("customer_id"),
    to_date(col("payment_date")).alias("payment_date"),
    col("payment_method"),
    col("amount").cast("double"),
    col("currency"),
    col("status"),
    col("gateway"),
    col("transaction_ref"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("payments_raw").alias("_bronze_source"),
)

merge_into(df_clean, "payments", "payment_id")
print(f"✅ payments — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 6 — refunds
df = spark.table(f"{BRONZE}.refunds_raw")

df_clean = dedup(df, "refund_id").select(
    col("refund_id"),
    col("order_id"),
    col("payment_id"),
    col("customer_id"),
    to_date(col("refund_date")).alias("refund_date"),
    col("refund_amount").cast("double"),
    col("currency"),
    col("reason"),
    col("status"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("refunds_raw").alias("_bronze_source"),
)

merge_into(df_clean, "refunds", "refund_id")
print(f"✅ refunds — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 7 — exchange_rates
df = spark.table(f"{BRONZE}.exchange_rates_raw")

# Dedup on composite key — same currency pair same date
df_clean = dedup(df, "rate_id").select(
    col("rate_id"),
    col("from_currency"),
    col("to_currency"),
    to_date(col("rate_date")).alias("rate_date"),
    col("exchange_rate").cast("double"),
    col("source"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("exchange_rates_raw").alias("_bronze_source"),
)

merge_into(df_clean, "exchange_rates", "rate_id")
print(f"✅ exchange_rates — {df_clean.count():,} rows merged")

# COMMAND ----------
# DBTITLE 1, Cell 8 — Verification
print("── Finance Silver — Row Counts ─────────────────────────────────────")
for tbl in ["customers", "orders", "order_lines", "payments", "refunds", "exchange_rates"]:
    df  = spark.table(f"{SILVER}.{tbl}")
    cnt = df.count()
    nulls = df.filter(col(list(df.columns)[0]).isNull()).count()
    status = "✅" if cnt > 0 else "❌"
    print(f"  {status}  {tbl:<20}  {cnt:>6,} rows  |  PK nulls: {nulls}")
