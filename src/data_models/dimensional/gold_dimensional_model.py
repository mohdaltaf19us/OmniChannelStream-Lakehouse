# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Dimensional Model — Star Schema
# MAGIC **Pattern:** SCD Type 1 (overwrite) · SCD Type 2 (history) · Surrogate keys
# MAGIC **Tables:** dim_date · dim_customer (SCD2) · dim_product (SCD2) · dim_supplier (SCD1)
# MAGIC            dim_warehouse (SCD1) · fact_orders · fact_order_lines · fact_payments (accumulating)

# COMMAND ----------
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import hashlib

dbutils.widgets.text("catalog", "omnioc_dev", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")

def gold(t): return f"{CATALOG}.gold.{t}"
def silver(t): return f"{CATALOG}.silver.{t}"
def bronze(t): return f"{CATALOG}.bronze.{t}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

def hash_key(*cols):
    """Generate MD5 surrogate key from natural keys."""
    return F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in cols]))

def hash_diff(*cols):
    """Hash of attribute columns — detect row changes for SCD."""
    return F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in cols]))

# COMMAND ----------
# MAGIC %md ## 1 — dim_date (static date spine — 10 years)

print("Building dim_date...")
dates = spark.sql("""
    SELECT
        sequence(to_date('2020-01-01'), to_date('2030-12-31'), INTERVAL 1 DAY) AS date_seq
""").select(F.explode("date_seq").alias("date_key"))

dim_date = (
    dates
    .withColumn("date_key",       F.date_format("date_key", "yyyyMMdd").cast("int"))
    .withColumn("full_date",      F.to_date(F.col("date_key").cast("string"), "yyyyMMdd"))
    .withColumn("year",           F.year("full_date"))
    .withColumn("quarter",        F.quarter("full_date"))
    .withColumn("month",          F.month("full_date"))
    .withColumn("month_name",     F.date_format("full_date", "MMMM"))
    .withColumn("week_of_year",   F.weekofyear("full_date"))
    .withColumn("day_of_month",   F.dayofmonth("full_date"))
    .withColumn("day_of_week",    F.dayofweek("full_date"))
    .withColumn("day_name",       F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend",     F.dayofweek("full_date").isin([1, 7]))
    .withColumn("fiscal_year",    F.when(F.month("full_date") >= 4, F.year("full_date"))
                                   .otherwise(F.year("full_date") - 1))
    .withColumn("fiscal_quarter", F.when(F.month("full_date").isin(4,5,6), 1)
                                   .when(F.month("full_date").isin(7,8,9), 2)
                                   .when(F.month("full_date").isin(10,11,12), 3)
                                   .otherwise(4))
    .withColumn("quarter_name",   F.concat(F.lit("Q"), F.quarter("full_date"),
                                           F.lit("-"), F.year("full_date")))
    .withColumn("_updated_at",    F.current_timestamp())
)

dim_date.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(gold("dim_date"))
print(f"✅ dim_date: {spark.table(gold('dim_date')).count():,} rows")

# COMMAND ----------
# MAGIC %md ## 2 — dim_customer (SCD Type 2 — tracks address, segment, loyalty changes)

print("Building dim_customer (SCD Type 2)...")

# Source: Silver customers (APPLY CHANGES already manages versions)
src = spark.table(silver("customers")).filter(F.col("__END_AT").isNull()) \
    if spark.catalog.tableExists(silver("customers")) \
    else spark.table(bronze("customers_raw"))

dim_customer_new = (
    src
    .withColumn("customer_sk",  hash_key("customer_id"))
    .withColumn("full_name",    F.concat_ws(" ", "first_name", "last_name"))
    .withColumn("age_band",
        F.when(F.months_between(F.current_date(), F.to_date("date_of_birth")) / 12 < 25, "18-24")
         .when(F.months_between(F.current_date(), F.to_date("date_of_birth")) / 12 < 35, "25-34")
         .when(F.months_between(F.current_date(), F.to_date("date_of_birth")) / 12 < 50, "35-49")
         .when(F.months_between(F.current_date(), F.to_date("date_of_birth")) / 12 < 65, "50-64")
         .otherwise("65+"))
    .withColumn("loyalty_tier",
        F.when(F.col("loyalty_points") >= 10000, "Platinum")
         .when(F.col("loyalty_points") >= 5000,  "Gold")
         .when(F.col("loyalty_points") >= 1000,  "Silver")
         .otherwise("Bronze"))
    .withColumn("_hash_diff", hash_diff("email","segment","country","city","state",
                                        "loyalty_points","is_active","preferred_currency"))
    .withColumn("_effective_from", F.current_date())
    .withColumn("_effective_to",   F.lit("9999-12-31").cast("date"))
    .withColumn("_is_current",     F.lit(True))
    .withColumn("_row_type",       F.lit("current"))
    .select(
        "customer_sk", "customer_id", "full_name", "first_name", "last_name",
        "email", "phone", "date_of_birth", "age_band", "gender",
        "segment", "loyalty_tier", "loyalty_points", "preferred_currency",
        "country", "city", "state", "zip_code",
        F.to_date("signup_date").alias("signup_date"),
        "is_email_verified", "is_active",
        "_hash_diff", "_effective_from", "_effective_to", "_is_current",
        F.current_timestamp().alias("_updated_at"),
    )
)

if spark.catalog.tableExists(gold("dim_customer")):
    existing = spark.table(gold("dim_customer")).filter(F.col("_is_current"))
    # Find changed rows
    changed = (
        dim_customer_new.alias("new")
        .join(existing.alias("old"), "customer_id")
        .filter(F.col("new._hash_diff") != F.col("old._hash_diff"))
        .select("new.*")
    )
    # Expire old records
    if changed.count() > 0:
        DeltaTable.forName(spark, gold("dim_customer")) \
            .alias("t") \
            .merge(changed.alias("s"), "t.customer_id = s.customer_id AND t._is_current = true") \
            .whenMatchedUpdate(set={
                "_is_current": F.lit(False),
                "_effective_to": F.current_date(),
                "_updated_at": F.current_timestamp(),
            }).execute()
    # Insert new versions
    new_inserts = (
        dim_customer_new.alias("new")
        .join(existing.alias("old"), "customer_id", "left_anti")
    )
    changed.union(new_inserts) \
        .write.format("delta").mode("append").saveAsTable(gold("dim_customer"))
else:
    dim_customer_new.write.format("delta").mode("overwrite").saveAsTable(gold("dim_customer"))

print(f"✅ dim_customer: {spark.table(gold('dim_customer')).count():,} rows (all versions)")

# COMMAND ----------
# MAGIC %md ## 3 — dim_product (SCD Type 2 — tracks price changes)

print("Building dim_product (SCD Type 2)...")

src_prod = spark.table(silver("products")).filter(F.col("__END_AT").isNull()) \
    if spark.catalog.tableExists(silver("products")) \
    else spark.table(bronze("products_raw"))

dim_product_new = (
    src_prod
    .withColumn("product_sk",       hash_key("product_id"))
    .withColumn("price_band",
        F.when(F.col("unit_price") < 10,   "Under $10")
         .when(F.col("unit_price") < 50,   "$10-$50")
         .when(F.col("unit_price") < 200,  "$50-$200")
         .when(F.col("unit_price") < 500,  "$200-$500")
         .otherwise("$500+"))
    .withColumn("margin_pct",
        F.when(F.col("unit_cost") > 0,
            (F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price") * 100
        ).otherwise(F.lit(None)))
    .withColumn("_hash_diff", hash_diff("product_name","unit_price","unit_cost",
                                        "category","sub_category","is_active"))
    .withColumn("_effective_from", F.current_date())
    .withColumn("_effective_to",   F.lit("9999-12-31").cast("date"))
    .withColumn("_is_current",     F.lit(True))
    .select(
        "product_sk", "product_id", "product_name", "sku",
        "category", "sub_category", "supplier_id",
        "unit_cost", "unit_price", "price_band", "margin_pct",
        "weight_kg", "is_active", "reorder_point", "reorder_quantity",
        "_hash_diff", "_effective_from", "_effective_to", "_is_current",
        F.current_timestamp().alias("_updated_at"),
    )
)

if spark.catalog.tableExists(gold("dim_product")):
    existing_p = spark.table(gold("dim_product")).filter("_is_current")
    changed_p  = dim_product_new.alias("n") \
        .join(existing_p.alias("o"), "product_id") \
        .filter(F.col("n._hash_diff") != F.col("o._hash_diff")) \
        .select("n.*")
    if changed_p.count() > 0:
        DeltaTable.forName(spark, gold("dim_product")).alias("t") \
            .merge(changed_p.alias("s"), "t.product_id=s.product_id AND t._is_current=true") \
            .whenMatchedUpdate(set={"_is_current": F.lit(False),
                                    "_effective_to": F.current_date()}).execute()
    new_p = dim_product_new.alias("n").join(existing_p.alias("o"), "product_id", "left_anti")
    changed_p.union(new_p).write.format("delta").mode("append").saveAsTable(gold("dim_product"))
else:
    dim_product_new.write.format("delta").mode("overwrite").saveAsTable(gold("dim_product"))

print(f"✅ dim_product: {spark.table(gold('dim_product')).count():,} rows")

# COMMAND ----------
# MAGIC %md ## 4 — fact_orders (Transactional — append + surrogate key lookup)

print("Building fact_orders...")

orders_src = spark.table(silver("orders")) if spark.catalog.tableExists(silver("orders")) \
    else spark.table(bronze("orders_raw"))

dim_cust_curr = spark.table(gold("dim_customer")).filter("_is_current") \
    .select("customer_id", "customer_sk", "segment", "loyalty_tier", "country")

dim_prod_curr = spark.table(gold("dim_product")).filter("_is_current") \
    .select("product_id", "product_sk", "category", "price_band")

dim_wh        = spark.table(gold("dim_warehouse")) \
    .select("warehouse_id", F.lit(None).cast("string").alias("warehouse_sk"), "region")

fact_orders = (
    orders_src
    .join(dim_cust_curr, "customer_id", "left")
    .join(dim_wh,        "warehouse_id", "left")
    .withColumn("order_date_key",    F.date_format("order_date", "yyyyMMdd").cast("int"))
    .withColumn("ship_date_key",     F.date_format("ship_date", "yyyyMMdd").cast("int"))
    .withColumn("delivery_date_key", F.date_format("delivery_date", "yyyyMMdd").cast("int"))
    .withColumn("net_revenue",
        F.col("total_amount") - F.coalesce("discount_amount", F.lit(0)))
    .withColumn("days_to_ship",
        F.when(F.col("ship_date").isNotNull(),
            F.datediff("ship_date", "order_date")).otherwise(F.lit(None)))
    .withColumn("days_to_deliver",
        F.when(F.col("delivery_date").isNotNull(),
            F.datediff("delivery_date", "order_date")).otherwise(F.lit(None)))
    .withColumn("_gold_updated_at", F.current_timestamp())
    .select(
        "order_id",                                  # degenerate dimension
        F.coalesce("customer_sk", hash_key("customer_id")).alias("customer_sk"),
        "warehouse_id",
        "order_date_key", "ship_date_key", "delivery_date_key",
        "status", "channel", "currency",
        "segment",                                   # from dim_customer (denorm)
        "loyalty_tier",                              # from dim_customer (denorm)
        "region",                                    # from dim_warehouse (denorm)
        "shipping_address_country",
        "promo_code",
        "total_amount", "discount_amount", "tax_amount",
        "shipping_amount", "net_revenue",
        "days_to_ship", "days_to_deliver",
        "_gold_updated_at",
    )
)

fact_orders.write.format("delta").mode("overwrite").option("mergeSchema","true") \
    .saveAsTable(gold("fact_orders"))
print(f"✅ fact_orders: {spark.table(gold('fact_orders')).count():,} rows")

# COMMAND ----------
# MAGIC %md ## 5 — fact_payments (Accumulating Snapshot — 4 milestones)

print("Building fact_payments (accumulating snapshot)...")

payments_src = spark.table(bronze("payments_raw")) \
    .withColumn("payment_sk",    hash_key("payment_id")) \
    .withColumn("order_date_key", F.date_format(F.to_timestamp("created_at"), "yyyyMMdd").cast("int"))

acc_snapshot = (
    payments_src
    .withColumn("placed_at",    F.to_timestamp("created_at"))
    .withColumn("paid_at",      F.to_timestamp("paid_at"))
    # Future milestones (settle, refund) — NULLs updated as they occur
    .withColumn("settled_at",   F.lit(None).cast("timestamp"))
    .withColumn("refunded_at",  F.lit(None).cast("timestamp"))
    .withColumn("is_completed", F.col("status") == "completed")
    .withColumn("is_failed",    F.col("status") == "failed")
    .withColumn("is_refunded",  F.col("status") == "refunded")
    .withColumn("days_to_pay",  F.datediff(F.to_date("paid_at"), F.to_date("created_at")))
    .withColumn("_gold_updated_at", F.current_timestamp())
    .select(
        "payment_id", "payment_sk", "order_id", "amount", "currency",
        "method", "status", "gateway",
        "placed_at", "paid_at", "settled_at", "refunded_at",
        "is_completed", "is_failed", "is_refunded",
        "days_to_pay", "billing_country", "_gold_updated_at",
    )
)

acc_snapshot.write.format("delta").mode("overwrite").saveAsTable(gold("fact_payments"))
print(f"✅ fact_payments: {spark.table(gold('fact_payments')).count():,} rows")

# COMMAND ----------
# MAGIC %md ## Summary

for t in ["dim_date","dim_customer","dim_product","dim_warehouse",
          "dim_supplier","fact_orders","fact_payments"]:
    try:
        n = spark.table(gold(t)).count()
        print(f"  ✅ {gold(t):<50} {n:>10,}")
    except Exception as e:
        print(f"  ❌ {gold(t):<50} {e}")
