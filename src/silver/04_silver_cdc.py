# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver — CDC SCD Type 2 Transforms
# MAGIC
# MAGIC No SELECT *, UPDATE SET *, or INSERT * anywhere.
# MAGIC Generated columns EXCLUDED from all writes:
# MAGIC   customers_scd → _valid_from_date (GENERATED from _valid_from)
# MAGIC   products_scd  → _valid_from_date (GENERATED from _valid_from)
# MAGIC
# MAGIC SCD2 two-step pattern per table:
# MAGIC   Step 1 MERGE: close current rows where hash differs or cdc_operation = DELETE
# MAGIC     → only updates t._valid_to and t._is_current — nothing else touched
# MAGIC   Step 2 INSERT: add new version for changed or new keys (not DELETEs)
# MAGIC     → explicit 27-column INSERT for customers_scd, 23-column for products_scd
# MAGIC
# MAGIC Transformation techniques:
# MAGIC   intersectAll  — validate CDC operation set before processing
# MAGIC   exceptAll     — isolate DELETE-only batch for fast close path
# MAGIC   lag()         — capture prior _record_hash for audit trail
# MAGIC   md5+concat_ws — hash tracked source columns for change detection
# MAGIC   translate     — ASCII-safe full name
# MAGIC   regexp_extract — email domain
# MAGIC   months_between — account tenure in months
# MAGIC   dense_rank()  — rank within segment
# MAGIC   pandas_udf    — vectorised LTV score
# MAGIC   selectExpr    — products: all casts as SQL strings

# COMMAND ----------

# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, current_timestamp, lit, md5, concat_ws,
    when, datediff, round as spark_round,
    lower, trim, upper, translate, regexp_extract,
    months_between, dense_rank, lag,
    split,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import pandas_udf
import pandas as pd

BRONZE   = "omnioc_dev.bronze"
SILVER   = "omnioc_dev.silver"
OPEN_END = "9999-12-31 00:00:00"
VALID_CDC_OPS = ["INSERT", "UPDATE", "DELETE"]

# ── Writable column lists (GENERATED columns excluded) ───────────────────────
CUSTOMERS_SCD_COLS = [
    "customer_id", "first_name", "last_name", "email", "phone",
    "country", "city", "segment", "is_active", "lifetime_value",
    "cdc_operation", "_valid_from", "_valid_to", "_is_current", "_record_hash",
    "_silver_ingest_time", "_bronze_source",
    # ── derived (added by 01b) ────────────────────────────────────────────────
    "full_name", "full_name_ascii", "email_domain", "account_age_months",
    "customer_tier", "is_high_value", "country_code",
    "customer_rank_in_segment", "ltv_score", "_prior_hash",
]   # _valid_from_date excluded — GENERATED from _valid_from

PRODUCTS_SCD_COLS = [
    "product_id", "product_name", "sku", "category", "sub_category",
    "unit_cost", "unit_price", "is_active", "supplier_id",
    "cdc_operation", "_valid_from", "_valid_to", "_is_current", "_record_hash",
    "_silver_ingest_time", "_bronze_source",
    # ── derived (added by 01b) ────────────────────────────────────────────────
    "category_l1", "category_l2", "margin_pct", "markup_pct",
    "price_tier", "is_high_margin", "unit_margin_usd",
]   # _valid_from_date excluded — GENERATED from _valid_from

print("✅ Config ready")
print(f"   Bronze         : {BRONZE}")
print(f"   Silver         : {SILVER}")
print(f"   Open-end stamp : {OPEN_END}")

# COMMAND ----------

# DBTITLE 1, Cell 2 — customers_scd
from pyspark.sql.functions import from_json, coalesce
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

CUSTOMER_TRACKED = [
    "first_name", "last_name", "email", "phone",
    "country", "city", "segment", "is_active", "lifetime_value",
]

@pandas_udf(DoubleType())
def ltv_score(lifetime_value: pd.Series, age_months: pd.Series) -> pd.Series:
    """Composite LTV score 0-100: 60% LTV weight + 40% tenure weight."""
    return ((lifetime_value.clip(0, 100_000) / 100_000 * 60)
          + (age_months.clip(0, 60) / 60 * 40)).round(2)

w_segment_rank  = Window.partitionBy("segment").orderBy(col("lifetime_value").cast("double").desc())
w_customer_hist = Window.partitionBy("customer_id").orderBy(col("_ingest_time"))

# ── Parse CDC JSON format ─────────────────────────────────────────────────────
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("lifetime_value", StringType(), True),
])

df_cdc_raw = spark.table(f"{BRONZE}.cdc_customers_raw")

# Parse JSON and flatten, using 'after' for INSERT/UPDATE, 'before' for DELETE
df_customers_raw = (
    df_cdc_raw
    .withColumn("after_parsed", from_json(col("after"), customer_schema))
    .withColumn("before_parsed", from_json(col("before"), customer_schema))
    .select(
        coalesce(col("after_parsed.customer_id"), col("before_parsed.customer_id")).alias("customer_id"),
        coalesce(col("after_parsed.first_name"), col("before_parsed.first_name")).alias("first_name"),
        coalesce(col("after_parsed.last_name"), col("before_parsed.last_name")).alias("last_name"),
        coalesce(col("after_parsed.email"), col("before_parsed.email")).alias("email"),
        coalesce(col("after_parsed.phone"), col("before_parsed.phone")).alias("phone"),
        coalesce(col("after_parsed.country"), col("before_parsed.country")).alias("country"),
        coalesce(col("after_parsed.city"), col("before_parsed.city")).alias("city"),
        coalesce(col("after_parsed.segment"), col("before_parsed.segment")).alias("segment"),
        coalesce(col("after_parsed.is_active"), col("before_parsed.is_active")).alias("is_active"),
        coalesce(col("after_parsed.lifetime_value"), col("before_parsed.lifetime_value")).alias("lifetime_value"),
        col("operation").alias("cdc_operation"),
        col("_ingest_time")
    )
)

# ── intersectAll — confirm only known CDC operations arrive ───────────────────
from pyspark.sql import Row
df_valid_ops    = spark.createDataFrame([Row(cdc_operation=op) for op in VALID_CDC_OPS])
df_incoming_ops = df_customers_raw.select("cdc_operation").distinct()
confirmed_ops   = df_incoming_ops.intersectAll(df_valid_ops).count()
print(f"CDC op validation: {confirmed_ops}/{df_incoming_ops.count()} distinct ops are known values")

# ── exceptAll — separate DELETE-only batch from upserts ──────────────────────
df_deletes_only = df_customers_raw.filter(col("cdc_operation") == "DELETE")
df_upserts      = df_customers_raw.exceptAll(df_deletes_only)
print(f"Deletes: {df_deletes_only.count()}  |  Upserts: {df_upserts.count()}")

df_customers_cdc = (
    df_customers_raw
    .select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        lower(trim(col("email"))).alias("email"),
        col("phone"),
        col("country"),
        col("city"),
        col("segment"),
        col("is_active").cast("boolean"),
        col("lifetime_value").cast("double"),
        col("cdc_operation"),
        col("_ingest_time"),
        # ── md5 — hash only tracked source columns ────────────────────────────
        md5(concat_ws("||", *[col(c).cast("string") for c in CUSTOMER_TRACKED]))
            .alias("_record_hash"),
        # ── lag() — prior hash for audit trail ────────────────────────────────
        lag(md5(concat_ws("||", *[col(c).cast("string") for c in CUSTOMER_TRACKED])), 1)
        .over(w_customer_hist).alias("_prior_hash"),
        # ── concat_ws + translate — ASCII-safe display name ───────────────────
        concat_ws(" ", trim(col("first_name")), trim(col("last_name"))).alias("full_name"),
        translate(
            concat_ws(" ", trim(col("first_name")), trim(col("last_name"))),
            "àáâãäåèéêëìíîïòóôõöùúûüý",
            "aaaaaaeeeeiiiioooouuuuy"
        ).alias("full_name_ascii"),
        # ── regexp_extract — email domain ─────────────────────────────────────
        regexp_extract(lower(trim(col("email"))), r"@(.+)$", 1).alias("email_domain"),
        # ── months_between — account tenure in months ─────────────────────────
        spark_round(months_between(current_timestamp(), col("_ingest_time")), 1)
            .cast("int").alias("account_age_months"),
        # ── Business derivations ──────────────────────────────────────────────
        when(col("lifetime_value").cast("double") >= 50000, "VIP")
        .when(col("lifetime_value").cast("double") >= 10000, "Premium")
        .otherwise("Standard").alias("customer_tier"),
        (col("lifetime_value").cast("double") >= 10000).alias("is_high_value"),
        upper(trim(col("country"))).alias("country_code"),
        # ── dense_rank() — rank within segment by LTV ─────────────────────────
        dense_rank().over(w_segment_rank).alias("customer_rank_in_segment"),
    )
    .withColumn("ltv_score",
        ltv_score(col("lifetime_value"), col("account_age_months").cast("double"))
    )
    .withColumn("_silver_ingest_time", current_timestamp())
    .withColumn("_bronze_source", lit("cdc_customers_raw"))
)

df_customers_cdc.createOrReplaceTempView("customers_cdc_typed")

# ── Dedup — latest CDC op per customer_id ─────────────────────────────────────
CUST_STAGE_COLS = ", ".join(
    [c for c in CUSTOMERS_SCD_COLS if c not in ("_valid_from","_valid_to","_is_current")] +
    ["_ingest_time"]
)
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW customers_cdc_stage AS
    SELECT {CUST_STAGE_COLS}
    FROM customers_cdc_typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingest_time DESC) = 1
""")

# ── Step 1: Close current rows — hash mismatch OR DELETE ─────────────────────
# Only _valid_to and _is_current are updated; all other columns left unchanged
spark.sql(f"""
    MERGE INTO {SILVER}.customers_scd AS t
    USING customers_cdc_stage AS s
    ON t.customer_id = s.customer_id
       AND t._is_current = true
    WHEN MATCHED
         AND (t._record_hash <> s._record_hash OR s.cdc_operation = 'DELETE')
    THEN UPDATE SET
        t._valid_to   = current_timestamp(),
        t._is_current = false
""")

# ── Step 2: INSERT new version for changed/new keys (not DELETEs) ─────────────
# Explicit 27-column INSERT — _valid_from_date excluded (GENERATED)
INS_COLS  = ", ".join(CUSTOMERS_SCD_COLS)
INS_VALS  = ", ".join([
    "s.customer_id", "s.first_name", "s.last_name", "s.email", "s.phone",
    "s.country", "s.city", "s.segment", "s.is_active", "s.lifetime_value",
    "s.cdc_operation",
    "current_timestamp()",                    # _valid_from
    f"CAST('{OPEN_END}' AS TIMESTAMP)",       # _valid_to
    "true",                                   # _is_current
    "s._record_hash",
    "current_timestamp()",                    # _silver_ingest_time
    "'cdc_customers_raw'",                    # _bronze_source
    "s.full_name", "s.full_name_ascii", "s.email_domain", "s.account_age_months",
    "s.customer_tier", "s.is_high_value", "s.country_code",
    "s.customer_rank_in_segment", "s.ltv_score", "s._prior_hash",
])

spark.sql(f"""
    INSERT INTO {SILVER}.customers_scd ({INS_COLS})
    SELECT {INS_VALS}
    FROM customers_cdc_stage s
    LEFT JOIN {SILVER}.customers_scd t
        ON  s.customer_id = t.customer_id
        AND t._is_current = true
    WHERE s.cdc_operation != 'DELETE'
      AND (t.customer_id IS NULL OR s._record_hash <> t._record_hash)
""")

total   = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.customers_scd").collect()[0][0]
current = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.customers_scd WHERE _is_current = true").collect()[0][0]
print(f"✅ customers_scd — {total:,} total versions  |  {current:,} current")
spark.sql(f"""
    SELECT customer_tier, COUNT(*) versions, ROUND(AVG(ltv_score),1) avg_score
    FROM {SILVER}.customers_scd WHERE _is_current = true GROUP BY 1 ORDER BY 1
""").show()

# COMMAND ----------

# DBTITLE 1, Cell 3 — products_scd
from pyspark.sql.functions import from_json, coalesce
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType

PRODUCT_TRACKED = [
    "product_name", "sku", "category", "sub_category",
    "unit_cost", "unit_price", "is_active", "supplier_id",
]

# ── Parse CDC JSON format ─────────────────────────────────────────────────────
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("supplier_id", StringType(), True),
])

df_cdc_raw = spark.table(f"{BRONZE}.cdc_products_raw")

# Parse JSON and flatten, using 'after' for INSERT/UPDATE, 'before' for DELETE
df_products_raw = (
    df_cdc_raw
    .withColumn("after_parsed", from_json(col("after"), product_schema))
    .withColumn("before_parsed", from_json(col("before"), product_schema))
    .select(
        coalesce(col("after_parsed.product_id"), col("before_parsed.product_id")).alias("product_id"),
        coalesce(col("after_parsed.product_name"), col("before_parsed.product_name")).alias("product_name"),
        coalesce(col("after_parsed.sku"), col("before_parsed.sku")).alias("sku"),
        coalesce(col("after_parsed.category"), col("before_parsed.category")).alias("category"),
        coalesce(col("after_parsed.sub_category"), col("before_parsed.sub_category")).alias("sub_category"),
        coalesce(col("after_parsed.unit_cost"), col("before_parsed.unit_cost")).alias("unit_cost"),
        coalesce(col("after_parsed.unit_price"), col("before_parsed.unit_price")).alias("unit_price"),
        coalesce(col("after_parsed.is_active"), col("before_parsed.is_active")).alias("is_active"),
        coalesce(col("after_parsed.supplier_id"), col("before_parsed.supplier_id")).alias("supplier_id"),
        col("operation").alias("cdc_operation"),
        col("_ingest_time")
    )
)

df_products_cdc = (
    # ── selectExpr — all casts + derivations as SQL string expressions ────────
    df_products_raw.selectExpr(
        "product_id", "product_name", "sku", "category", "sub_category",
        "CAST(unit_cost  AS DOUBLE) AS unit_cost",
        "CAST(unit_price AS DOUBLE) AS unit_price",
        "CAST(is_active  AS BOOLEAN) AS is_active",
        "supplier_id", "cdc_operation", "_ingest_time",
        # ── split() — decompose category path ────────────────────────────────
        "split(category, '/')[0]     AS category_l1",
        "split(sub_category, '/')[0] AS category_l2",
        # ── Business derivations ──────────────────────────────────────────────
        "ROUND((unit_price - unit_cost) / NULLIF(unit_price,0) * 100, 2) AS margin_pct",
        "ROUND((unit_price - unit_cost) / NULLIF(unit_cost, 0) * 100, 2) AS markup_pct",
        """CASE WHEN CAST(unit_price AS DOUBLE) < 20   THEN 'budget'
               WHEN CAST(unit_price AS DOUBLE) <= 100 THEN 'mid'
               ELSE 'premium' END AS price_tier""",
        "((unit_price - unit_cost) / NULLIF(unit_price,0) * 100 >= 40) AS is_high_margin",
        "ROUND(unit_price - unit_cost, 2) AS unit_margin_usd",
    )
    # ── md5 hash added as withColumn after selectExpr ─────────────────────────
    .withColumn(
        "_record_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in PRODUCT_TRACKED]))
    )
    .withColumn("_silver_ingest_time", current_timestamp())
    .withColumn("_bronze_source", lit("cdc_products_raw"))
)

df_products_cdc.createOrReplaceTempView("products_cdc_typed")

PROD_STAGE_COLS = ", ".join(
    [c for c in PRODUCTS_SCD_COLS if c not in ("_valid_from","_valid_to","_is_current")] +
    ["_ingest_time"]
)
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW products_cdc_stage AS
    SELECT {PROD_STAGE_COLS}
    FROM products_cdc_typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _ingest_time DESC) = 1
""")

# ── Step 1: Close current rows ─────────────────────────────────────────────────
spark.sql(f"""
    MERGE INTO {SILVER}.products_scd AS t
    USING products_cdc_stage AS s
    ON t.product_id = s.product_id
       AND t._is_current = true
    WHEN MATCHED
         AND (t._record_hash <> s._record_hash OR s.cdc_operation = 'DELETE')
    THEN UPDATE SET
        t._valid_to   = current_timestamp(),
        t._is_current = false
""")

# ── Step 2: INSERT new version — explicit 23-column INSERT ────────────────────
PINS_COLS = ", ".join(PRODUCTS_SCD_COLS)
PINS_VALS = ", ".join([
    "s.product_id", "s.product_name", "s.sku", "s.category", "s.sub_category",
    "s.unit_cost", "s.unit_price", "s.is_active", "s.supplier_id",
    "s.cdc_operation",
    "current_timestamp()",                    # _valid_from
    f"CAST('{OPEN_END}' AS TIMESTAMP)",       # _valid_to
    "true",                                   # _is_current
    "s._record_hash",
    "current_timestamp()",                    # _silver_ingest_time
    "'cdc_products_raw'",                     # _bronze_source
    "s.category_l1", "s.category_l2",
    "s.margin_pct", "s.markup_pct", "s.price_tier",
    "s.is_high_margin", "s.unit_margin_usd",
])

spark.sql(f"""
    INSERT INTO {SILVER}.products_scd ({PINS_COLS})
    SELECT {PINS_VALS}
    FROM products_cdc_stage s
    LEFT JOIN {SILVER}.products_scd t
        ON  s.product_id  = t.product_id
        AND t._is_current = true
    WHERE s.cdc_operation != 'DELETE'
      AND (t.product_id IS NULL OR s._record_hash <> t._record_hash)
""")

total   = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.products_scd").collect()[0][0]
current = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.products_scd WHERE _is_current = true").collect()[0][0]
print(f"✅ products_scd — {total:,} total versions  |  {current:,} current")
spark.sql(f"""
    SELECT price_tier, COUNT(*) versions, ROUND(AVG(margin_pct),1) avg_margin
    FROM {SILVER}.products_scd WHERE _is_current = true GROUP BY 1 ORDER BY 1
""").show()

# COMMAND ----------

# DBTITLE 1, Cell 4 — Verification
OPEN_END_TS = f"CAST('{OPEN_END}' AS TIMESTAMP)"
print("── CDC Silver — SCD Type 2 Verification ─────────────────────────────")

for tbl, pk, check_cols in [
    ("customers_scd", "customer_id", ["full_name", "customer_tier", "ltv_score", "_prior_hash"]),
    ("products_scd",  "product_id",  ["margin_pct", "price_tier", "category_l1"]),
]:
    total       = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl}").collect()[0][0]
    current_cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl} WHERE _is_current = true").collect()[0][0]
    dupes       = spark.sql(f"""
        SELECT COUNT(*) FROM (
            SELECT {pk} FROM {SILVER}.{tbl}
            WHERE _is_current = true GROUP BY {pk} HAVING COUNT(*) > 1
        )""").collect()[0][0]
    open_closed = spark.sql(f"""
        SELECT COUNT(*) FROM {SILVER}.{tbl}
        WHERE _is_current = false AND _valid_to IS NULL
    """).collect()[0][0]
    wrong_end   = spark.sql(f"""
        SELECT COUNT(*) FROM {SILVER}.{tbl}
        WHERE _is_current = true AND _valid_to <> {OPEN_END_TS}
    """).collect()[0][0]

    print(f"\n  {tbl}")
    print(f"    Total: {total:,}  Current: {current_cnt:,}  History: {total - current_cnt:,}")
    print(f"    Duplicate current rows        : {dupes}  {'✅' if dupes == 0 else '❌'}")
    print(f"    Closed rows missing _valid_to : {open_closed}  {'✅' if open_closed == 0 else '❌'}")
    print(f"    Current rows wrong _valid_to  : {wrong_end}  {'✅' if wrong_end == 0 else '❌'}")
    for c in check_cols:
        n = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl} WHERE {c} IS NULL AND _is_current = true").collect()[0][0]
        print(f"    {c} nulls (current)         : {n}  {'✅' if n == 0 else '⚠️'}")

print("\n  ✅ SCD Type 2 verification complete")
