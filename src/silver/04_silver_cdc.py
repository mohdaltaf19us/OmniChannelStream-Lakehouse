# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver — CDC SCD Type 2 Transforms
# MAGIC
# MAGIC Reads bronze CDC tables and maintains full version history using SCD Type 2.
# MAGIC
# MAGIC **SCD Type 2 logic:**
# MAGIC - New key → INSERT with `_valid_from=now`, `_valid_to=9999-12-31`, `_is_current=true`
# MAGIC - Changed key (hash differs) → close old row, INSERT new version
# MAGIC - DELETE CDC op → close current row, no new version inserted
# MAGIC - Same hash → no action, skip
# MAGIC
# MAGIC | Cell | Bronze Source | Silver Target |
# MAGIC |------|--------------|---------------|
# MAGIC | 2 | cdc_customers_raw | customers_scd |
# MAGIC | 3 | cdc_products_raw | products_scd |
# MAGIC | 4 | Verification |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, current_timestamp, lit, md5, concat_ws,
    row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

BRONZE   = "omnioc_dev.bronze"
SILVER   = "omnioc_dev.silver"
OPEN_END = lit("9999-12-31 00:00:00").cast(TimestampType())

def dedup_cdc(df, pk):
    """Keep latest CDC operation per key based on ingest time."""
    w = Window.partitionBy(pk).orderBy(col("_ingest_time").desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

print("✅ Config ready")
print(f"   Bronze   : {BRONZE}")
print(f"   Silver   : {SILVER}")
print(f"   Open-end : 9999-12-31 00:00:00 (sentinel for current rows)")

# COMMAND ----------
# DBTITLE 1, Cell 2 — customers_scd

CUSTOMER_TRACKED = [
    "first_name", "last_name", "email", "phone",
    "country", "city", "segment", "is_active", "lifetime_value"
]

# ── Step 1: read + dedup + compute hash ───────────────────────────────────────
df_incoming = (
    dedup_cdc(spark.table(f"{BRONZE}.cdc_customers_raw"), "customer_id")
    .withColumn(
        "_record_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in CUSTOMER_TRACKED]))
    )
    .select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("phone"),
        col("country"),
        col("city"),
        col("segment"),
        col("is_active").cast("boolean"),
        col("lifetime_value").cast("double"),
        col("cdc_operation"),
        col("_record_hash"),
    )
)

target = DeltaTable.forName(spark, f"{SILVER}.customers_scd")

# ── Step 2: close current rows where hash changed or DELETE received ───────────
(
    target.alias("t")
    .merge(
        df_incoming.alias("s"),
        "t.customer_id = s.customer_id AND t._is_current = true"
    )
    .whenMatchedUpdate(
        condition = "s._record_hash <> t._record_hash OR s.cdc_operation = 'DELETE'",
        set = {
            "_valid_to"   : current_timestamp(),
            "_is_current" : lit(False),
        }
    )
    .execute()
)

# ── Step 3: insert new version for changed rows (not DELETEs) ─────────────────
# Anti-join: rows in incoming that are NOT already current in silver
#            OR are brand new (not in silver at all)
df_existing_current = (
    spark.table(f"{SILVER}.customers_scd")
    .filter(col("_is_current") == True)
    .select("customer_id", col("_record_hash").alias("existing_hash"))
)

df_to_insert = (
    df_incoming
    .filter(col("cdc_operation") != "DELETE")
    .join(df_existing_current, on="customer_id", how="left")
    # Insert if: new customer (no existing row) OR hash changed (different from current)
    .filter(
        col("existing_hash").isNull() |
        (col("_record_hash") != col("existing_hash"))
    )
    .select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("phone"),
        col("country"),
        col("city"),
        col("segment"),
        col("is_active"),
        col("lifetime_value"),
        col("cdc_operation"),
        col("_record_hash"),
        current_timestamp().alias("_valid_from"),
        OPEN_END.alias("_valid_to"),
        lit(True).alias("_is_current"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("cdc_customers_raw").alias("_bronze_source"),
    )
)

if df_to_insert.count() > 0:
    df_to_insert.write.format("delta").mode("append").saveAsTable(f"{SILVER}.customers_scd")

total   = spark.table(f"{SILVER}.customers_scd").count()
current = spark.table(f"{SILVER}.customers_scd").filter(col("_is_current") == True).count()
print(f"✅ customers_scd — {total:,} total versions  |  {current:,} current")

# COMMAND ----------
# DBTITLE 1, Cell 3 — products_scd

PRODUCT_TRACKED = [
    "product_name", "sku", "category", "sub_category",
    "unit_cost", "unit_price", "is_active", "supplier_id"
]

df_incoming = (
    dedup_cdc(spark.table(f"{BRONZE}.cdc_products_raw"), "product_id")
    .withColumn(
        "_record_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in PRODUCT_TRACKED]))
    )
    .select(
        col("product_id"),
        col("product_name"),
        col("sku"),
        col("category"),
        col("sub_category"),
        col("unit_cost").cast("double"),
        col("unit_price").cast("double"),
        col("is_active").cast("boolean"),
        col("supplier_id"),
        col("cdc_operation"),
        col("_record_hash"),
    )
)

target = DeltaTable.forName(spark, f"{SILVER}.products_scd")

(
    target.alias("t")
    .merge(
        df_incoming.alias("s"),
        "t.product_id = s.product_id AND t._is_current = true"
    )
    .whenMatchedUpdate(
        condition = "s._record_hash <> t._record_hash OR s.cdc_operation = 'DELETE'",
        set = {
            "_valid_to"   : current_timestamp(),
            "_is_current" : lit(False),
        }
    )
    .execute()
)

df_existing_current = (
    spark.table(f"{SILVER}.products_scd")
    .filter(col("_is_current") == True)
    .select("product_id", col("_record_hash").alias("existing_hash"))
)

df_to_insert = (
    df_incoming
    .filter(col("cdc_operation") != "DELETE")
    .join(df_existing_current, on="product_id", how="left")
    .filter(
        col("existing_hash").isNull() |
        (col("_record_hash") != col("existing_hash"))
    )
    .select(
        col("product_id"),
        col("product_name"),
        col("sku"),
        col("category"),
        col("sub_category"),
        col("unit_cost"),
        col("unit_price"),
        col("is_active"),
        col("supplier_id"),
        col("cdc_operation"),
        col("_record_hash"),
        current_timestamp().alias("_valid_from"),
        OPEN_END.alias("_valid_to"),
        lit(True).alias("_is_current"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("cdc_products_raw").alias("_bronze_source"),
    )
)

if df_to_insert.count() > 0:
    df_to_insert.write.format("delta").mode("append").saveAsTable(f"{SILVER}.products_scd")

total   = spark.table(f"{SILVER}.products_scd").count()
current = spark.table(f"{SILVER}.products_scd").filter(col("_is_current") == True).count()
print(f"✅ products_scd — {total:,} total versions  |  {current:,} current")

# COMMAND ----------
# DBTITLE 1, Cell 4 — Verification
print("── CDC Silver — SCD Type 2 Verification ─────────────────────────────")

for tbl, pk in [("customers_scd", "customer_id"), ("products_scd", "product_id")]:
    df      = spark.table(f"{SILVER}.{tbl}")
    total   = df.count()
    current = df.filter(col("_is_current") == True).count()
    history = total - current

    # No PK should have more than one current row
    dupes = (
        df.filter(col("_is_current") == True)
        .groupBy(pk).count()
        .filter(col("count") > 1)
        .count()
    )

    # All closed rows must have _valid_to set
    open_closed = df.filter(
        (col("_is_current") == False) & col("_valid_to").isNull()
    ).count()

    print(f"\n  {tbl}")
    print(f"    Total versions           : {total:,}")
    print(f"    Current (_is_current=T)  : {current:,}")
    print(f"    Historical               : {history:,}")
    print(f"    Duplicate current rows   : {dupes}   {'✅' if dupes == 0 else '❌'}")
    print(f"    Closed rows missing _valid_to : {open_closed}   {'✅' if open_closed == 0 else '❌'}")

print("\n  ✅ SCD Type 2 complete")
