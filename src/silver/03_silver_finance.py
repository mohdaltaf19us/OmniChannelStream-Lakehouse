# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver — Finance Transforms
# MAGIC
# MAGIC Reads bronze batch tables, casts types, deduplicates, and MERGEs into silver using Spark SQL.
# MAGIC
# MAGIC | Cell | Bronze Source | Silver Target |
# MAGIC |------|--------------|---------------|
# MAGIC | 2 | customers_raw | customers |
# MAGIC | 3 | orders_raw | orders |
# MAGIC | 4 | order_lines_raw | order_lines |
# MAGIC | 5 | payments_raw | payments |
# MAGIC | 6 | refunds_raw | refunds |
# MAGIC | 7 | exchange_rates_raw | exchange_rates |
# MAGIC | 8 | Verification |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, lit,
    row_number, round as spark_round
)
from pyspark.sql.window import Window

BRONZE = "omnioc_dev.bronze"
SILVER = "omnioc_dev.silver"

def dedup(df, pk):
    """Keep latest row per primary key based on _ingest_time."""
    w = Window.partitionBy(pk).orderBy(col("_ingest_time").desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

print(f"✅ Config ready — Bronze: {BRONZE}  Silver: {SILVER}")

# COMMAND ----------
# DBTITLE 1, Cell 2 — customers
dedup(spark.table(f"{BRONZE}.customers_raw"), "customer_id").select(
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
).createOrReplaceTempView("customers_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.customers AS t
    USING customers_stage AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ customers — {spark.table(f'{SILVER}.customers').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 3 — orders
dedup(spark.table(f"{BRONZE}.orders_raw"), "order_id").select(
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
).createOrReplaceTempView("orders_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.orders AS t
    USING orders_stage AS s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ orders — {spark.table(f'{SILVER}.orders').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 4 — order_lines
dedup(spark.table(f"{BRONZE}.order_lines_raw"), "line_id").select(
    col("line_id"),
    col("order_id"),
    col("product_id"),
    col("quantity").cast("int"),
    col("unit_price").cast("double"),
    col("discount_pct").cast("double"),
    # line_total derived: quantity × unit_price × (1 - discount_pct/100)
    spark_round(
        col("quantity").cast("double") *
        col("unit_price").cast("double") *
        (1 - col("discount_pct").cast("double") / 100),
        2
    ).alias("line_total"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("order_lines_raw").alias("_bronze_source"),
).createOrReplaceTempView("order_lines_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.order_lines AS t
    USING order_lines_stage AS s
    ON t.line_id = s.line_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ order_lines — {spark.table(f'{SILVER}.order_lines').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 5 — payments
dedup(spark.table(f"{BRONZE}.payments_raw"), "payment_id").select(
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
).createOrReplaceTempView("payments_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.payments AS t
    USING payments_stage AS s
    ON t.payment_id = s.payment_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ payments — {spark.table(f'{SILVER}.payments').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 6 — refunds
dedup(spark.table(f"{BRONZE}.refunds_raw"), "refund_id").select(
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
).createOrReplaceTempView("refunds_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.refunds AS t
    USING refunds_stage AS s
    ON t.refund_id = s.refund_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ refunds — {spark.table(f'{SILVER}.refunds').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 7 — exchange_rates
dedup(spark.table(f"{BRONZE}.exchange_rates_raw"), "rate_id").select(
    col("rate_id"),
    col("from_currency"),
    col("to_currency"),
    to_date(col("rate_date")).alias("rate_date"),
    col("exchange_rate").cast("double"),
    col("source"),
    current_timestamp().alias("_silver_ingest_time"),
    lit("exchange_rates_raw").alias("_bronze_source"),
).createOrReplaceTempView("exchange_rates_stage")

spark.sql(f"""
    MERGE INTO {SILVER}.exchange_rates AS t
    USING exchange_rates_stage AS s
    ON t.rate_id = s.rate_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ exchange_rates — {spark.table(f'{SILVER}.exchange_rates').count():,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 8 — Verification
print("── Finance Silver — Row Counts ─────────────────────────────────────")
for tbl, pk in [
    ("customers",     "customer_id"),
    ("orders",        "order_id"),
    ("order_lines",   "line_id"),
    ("payments",      "payment_id"),
    ("refunds",       "refund_id"),
    ("exchange_rates","rate_id"),
]:
    df    = spark.table(f"{SILVER}.{tbl}")
    cnt   = df.count()
    nulls = df.filter(col(pk).isNull()).count()
    print(f"  {'✅' if cnt > 0 else '❌'}  {tbl:<20}  {cnt:>6,} rows  |  PK nulls: {nulls}")
