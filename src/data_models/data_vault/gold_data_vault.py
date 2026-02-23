# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Data Vault 2.0
# MAGIC **Tables:** Hubs · Links · Satellites · PIT Table
# MAGIC **Rules:** Business key-driven · Hash diff for change detection · Load date stamped · Source system tagged

# COMMAND ----------
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "omnioc_dev", "Unity Catalog")
CATALOG    = dbutils.widgets.get("catalog")
DV_SCHEMA  = f"{CATALOG}.gold_dv"
RECORD_SRC = "omnioc-lakehouse"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DV_SCHEMA}")

def dv(t): return f"{DV_SCHEMA}.{t}"
def bronze(t): return f"{CATALOG}.bronze.{t}"
def silver(t): return f"{CATALOG}.silver.{t}"

def hub_hash(bk): return F.md5(F.upper(F.trim(bk.cast("string"))))
def sat_hash(*cols):
    return F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in cols]))

LOAD_DATE = F.current_timestamp()

# ════════════════════════════════════════════════════════════════════════════════
#  HUBS — Business key + load metadata only (no descriptive attributes)
# ════════════════════════════════════════════════════════════════════════════════

def load_hub(hub_name, src_df, bk_col, hub_bk_col):
    """Generic hub load — insert new business keys only (hubs never update)."""
    new_hub = (
        src_df
        .select(F.col(bk_col).alias(hub_bk_col))
        .filter(F.col(hub_bk_col).isNotNull())
        .distinct()
        .withColumn("hub_hash_key",   hub_hash(F.col(hub_bk_col)))
        .withColumn("load_date",      LOAD_DATE)
        .withColumn("record_source",  F.lit(RECORD_SRC))
    )
    if spark.catalog.tableExists(dv(hub_name)):
        existing = spark.table(dv(hub_name)).select(hub_bk_col)
        inserts  = new_hub.join(existing, hub_bk_col, "left_anti")
        if inserts.count() > 0:
            inserts.write.format("delta").mode("append").saveAsTable(dv(hub_name))
    else:
        new_hub.write.format("delta").mode("overwrite").saveAsTable(dv(hub_name))
    n = spark.table(dv(hub_name)).count()
    print(f"  ✅ {hub_name:<35} {n:>8,} BKs")
    return n

# COMMAND ----------
print("── HUBS ─────────────────────────────────────────")

src_customers = spark.table(bronze("customers_raw"))
src_orders    = spark.table(bronze("orders_raw"))
src_products  = spark.table(bronze("products_raw"))
src_suppliers = spark.table(bronze("suppliers_raw"))
src_payments  = spark.table(bronze("payments_raw"))

load_hub("hub_customer", src_customers, "customer_id", "customer_bk")
load_hub("hub_order",    src_orders,    "order_id",    "order_bk")
load_hub("hub_product",  src_products,  "product_id",  "product_bk")
load_hub("hub_supplier", src_suppliers, "supplier_id", "supplier_bk")
load_hub("hub_payment",  src_payments,  "payment_id",  "payment_bk")

# ════════════════════════════════════════════════════════════════════════════════
#  LINKS — Relationship between hubs (foreign key associations)
# ════════════════════════════════════════════════════════════════════════════════

print("\n── LINKS ────────────────────────────────────────")

def load_link(link_name, src_df, cols_map):
    """
    cols_map: {hub_hash_col: source_bk_col, ...}
    Generates composite link hash from all hub hash keys.
    """
    select_cols = []
    for hash_col, src_col in cols_map.items():
        select_cols.append(hub_hash(F.col(src_col)).alias(hash_col))

    new_link = (
        src_df
        .filter(F.col(list(cols_map.values())[0]).isNotNull())
        .select(*[F.col(c) for c in cols_map.values()])
        .dropDuplicates()
        .select(*select_cols)
        .withColumn("link_hash_key", F.md5(F.concat(*[F.col(c) for c in cols_map.keys()])))
        .withColumn("load_date",     LOAD_DATE)
        .withColumn("record_source", F.lit(RECORD_SRC))
    )
    if spark.catalog.tableExists(dv(link_name)):
        existing = spark.table(dv(link_name)).select("link_hash_key")
        inserts  = new_link.join(existing, "link_hash_key", "left_anti")
        if inserts.count() > 0:
            inserts.write.format("delta").mode("append").saveAsTable(dv(link_name))
    else:
        new_link.write.format("delta").mode("overwrite").saveAsTable(dv(link_name))
    n = spark.table(dv(link_name)).count()
    print(f"  ✅ {link_name:<35} {n:>8,} relationships")

load_link("link_order_customer",
    src_orders,
    {"customer_hash_key": "customer_id", "order_hash_key": "order_id"})

load_link("link_order_product",
    spark.table(bronze("order_lines_raw")) if spark.catalog.tableExists(bronze("order_lines_raw"))
    else spark.table(bronze("orders_raw")),
    {"order_hash_key": "order_id", "product_hash_key": "product_id"})

load_link("link_order_payment",
    src_payments,
    {"order_hash_key": "order_id", "payment_hash_key": "payment_id"})

load_link("link_supplier_product",
    src_products,
    {"supplier_hash_key": "supplier_id", "product_hash_key": "product_id"})

# ════════════════════════════════════════════════════════════════════════════════
#  SATELLITES — Descriptive attributes (full history via hash_diff)
# ════════════════════════════════════════════════════════════════════════════════

print("\n── SATELLITES ───────────────────────────────────")

def load_satellite(sat_name, src_df, bk_col, attribute_cols):
    """
    Insert-only satellite — new row added only when hash_diff changes.
    Older rows remain for full history.
    """
    new_sat = (
        src_df
        .filter(F.col(bk_col).isNotNull())
        .withColumn("hub_hash_key",  hub_hash(F.col(bk_col)))
        .withColumn("hash_diff",     sat_hash(*attribute_cols))
        .withColumn("load_date",     LOAD_DATE)
        .withColumn("load_end_date", F.lit(None).cast("timestamp"))
        .withColumn("record_source", F.lit(RECORD_SRC))
        .select("hub_hash_key", "hash_diff", "load_date", "load_end_date",
                "record_source", *attribute_cols)
    )
    if spark.catalog.tableExists(dv(sat_name)):
        # Only insert rows where hash_diff doesn't exist yet for this hub_hash_key
        existing = spark.table(dv(sat_name)).select("hub_hash_key", "hash_diff")
        inserts  = new_sat.join(existing, ["hub_hash_key", "hash_diff"], "left_anti")
        if inserts.count() > 0:
            inserts.write.format("delta").mode("append").saveAsTable(dv(sat_name))
    else:
        new_sat.write.format("delta").mode("overwrite").saveAsTable(dv(sat_name))
    n = spark.table(dv(sat_name)).count()
    print(f"  ✅ {sat_name:<40} {n:>8,} history rows")

# Satellite: customer details (tracks all customer attribute changes)
load_satellite(
    "sat_customer_details",
    src_customers,
    "customer_id",
    ["first_name","last_name","email","phone","gender","segment",
     "country","city","state","zip_code","is_email_verified","is_active"],
)

# Satellite: customer loyalty (tracks loyalty points changes separately)
load_satellite(
    "sat_customer_loyalty",
    src_customers,
    "customer_id",
    ["loyalty_points","preferred_currency"],
)

# Satellite: product pricing (tracks price changes — most important DV satellite)
load_satellite(
    "sat_product_pricing",
    src_products,
    "product_id",
    ["product_name","sku","category","sub_category",
     "unit_cost","unit_price","is_active"],
)

# Satellite: supplier details
load_satellite(
    "sat_supplier_details",
    src_suppliers,
    "supplier_id",
    ["supplier_name","country","city","contact_email","contact_phone",
     "rating","lead_time_days","payment_terms_days","is_active"],
)

# Satellite: order status (tracks order lifecycle changes)
load_satellite(
    "sat_order_status",
    src_orders,
    "order_id",
    ["status","channel","total_amount","currency",
     "ship_date","delivery_date","promo_code"],
)

# COMMAND ----------
# MAGIC %md ## PIT Table — Point-in-Time (query accelerator for customer satellites)

print("\n── PIT TABLE ────────────────────────────────────")
print("Building pit_customer (daily snapshots joining all customer sats)...")

# Generate daily PIT snapshot for last 365 days
pit_dates = spark.sql("""
    SELECT EXPLODE(SEQUENCE(
        DATE_SUB(CURRENT_DATE(), 365),
        CURRENT_DATE(),
        INTERVAL 1 DAY
    )) AS snapshot_date
""")

hubs = spark.table(dv("hub_customer")).select("customer_bk", "hub_hash_key")

sat_details = spark.table(dv("sat_customer_details")) \
    .select("hub_hash_key", "load_date", "hash_diff",
            F.col("load_date").alias("det_load_date"),
            F.col("hash_diff").alias("det_hash_diff"))

sat_loyalty = spark.table(dv("sat_customer_loyalty")) \
    .select("hub_hash_key", "load_date",
            F.col("load_date").alias("loy_load_date"),
            F.col("hash_diff").alias("loy_hash_diff"))

# For each customer × date, find the most recent satellite row as-of that date
pit = (
    hubs.crossJoin(pit_dates)
    .join(
        sat_details.withColumnRenamed("hub_hash_key","hk_d"),
        (F.col("hub_hash_key") == F.col("hk_d")) &
        (F.col("det_load_date") <= F.col("snapshot_date")),
        "left"
    )
    .join(
        sat_loyalty.withColumnRenamed("hub_hash_key","hk_l"),
        (F.col("hub_hash_key") == F.col("hk_l")) &
        (F.col("loy_load_date") <= F.col("snapshot_date")),
        "left"
    )
    .withColumn("det_rn", F.row_number().over(
        Window.partitionBy("hub_hash_key","snapshot_date")
              .orderBy(F.col("det_load_date").desc())))
    .withColumn("loy_rn", F.row_number().over(
        Window.partitionBy("hub_hash_key","snapshot_date")
              .orderBy(F.col("loy_load_date").desc())))
    .filter((F.col("det_rn") == 1) & (F.col("loy_rn") == 1))
    .select(
        "hub_hash_key", "customer_bk", "snapshot_date",
        "det_load_date", "det_hash_diff",
        "loy_load_date", "loy_hash_diff",
        F.current_timestamp().alias("pit_load_date"),
    )
)

pit.write.format("delta").mode("overwrite").saveAsTable(dv("pit_customer"))
print(f"  ✅ pit_customer: {spark.table(dv('pit_customer')).count():,} rows (customer × date)")

# COMMAND ----------
# MAGIC %md ## Verify DV structure

print("\n── DATA VAULT INVENTORY ────────────────────────────────────")
for t in ["hub_customer","hub_order","hub_product","hub_supplier","hub_payment",
          "link_order_customer","link_order_product","link_order_payment","link_supplier_product",
          "sat_customer_details","sat_customer_loyalty","sat_product_pricing",
          "sat_supplier_details","sat_order_status","pit_customer"]:
    try:
        n = spark.table(dv(t)).count()
        print(f"  ✅ {dv(t):<60} {n:>10,}")
    except Exception as e:
        print(f"  ❌ {t:<60} {e}")
