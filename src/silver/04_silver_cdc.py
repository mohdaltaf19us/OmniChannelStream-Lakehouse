# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver — CDC SCD Type 2 Transforms
# MAGIC
# MAGIC **Transformation techniques demonstrated:**
# MAGIC
# MAGIC | Technique | Section | Applied To |
# MAGIC |-----------|---------|-----------|
# MAGIC | `intersectAll` | §2 Filter | Validate CDC operations before processing |
# MAGIC | `exceptAll` | §2 Filter | Isolate DELETE-only batch for fast close path |
# MAGIC | `lag() over window` | §5 Window | Detect prior hash for audit trail |
# MAGIC | `md5 + concat_ws` | §9 String | Hash tracked columns for change detection |
# MAGIC | `translate` | §9 String | customers — ASCII-safe name |
# MAGIC | `regexp_extract` | §9 String | customers — email domain |
# MAGIC | `months_between` | §9 Date | customers — account tenure |
# MAGIC | `dense_rank()` | §5 Window | customers — rank within segment |
# MAGIC | `selectExpr` | §1 Shaping | products — inline SQL expressions |
# MAGIC | `pandas_udf` | §12 UDF | customers — vectorised LTV score |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, current_timestamp, lit, md5, concat_ws, row_number,
    when, datediff, round as spark_round,
    lower, trim, upper, translate, regexp_extract,
    months_between, dense_rank, lag,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import pandas as pd
from pyspark.sql.functions import pandas_udf

BRONZE   = "omnioc_dev.bronze"
SILVER   = "omnioc_dev.silver"
OPEN_END = "9999-12-31 00:00:00"

VALID_CDC_OPS = ["INSERT", "UPDATE", "DELETE"]

print("✅ Config ready")
print(f"   Bronze   : {BRONZE}")
print(f"   Silver   : {SILVER}")
print(f"   Open-end : {OPEN_END}")

# COMMAND ----------
# DBTITLE 1, Cell 2 — customers_scd

CUSTOMER_TRACKED = [
    "first_name", "last_name", "email", "phone",
    "country", "city", "segment", "is_active", "lifetime_value"
]

@pandas_udf(DoubleType())
def ltv_score(lifetime_value: pd.Series, age_months: pd.Series) -> pd.Series:
    """Composite LTV score 0-100: 60% LTV weight + 40% tenure weight."""
    return ((lifetime_value.clip(0, 100_000) / 100_000 * 60)
          + (age_months.clip(0, 60) / 60 * 40)).round(2)

w_segment_rank  = Window.partitionBy("segment").orderBy(col("lifetime_value").desc())
w_customer_hist = Window.partitionBy("customer_id").orderBy(col("_ingest_time"))

df_customers_raw = spark.table(f"{BRONZE}.cdc_customers_raw")

# ── intersectAll — confirm only known operations arrive before any processing ──
from pyspark.sql import Row
df_valid_ops    = spark.createDataFrame([Row(cdc_operation=op) for op in VALID_CDC_OPS])
df_incoming_ops = df_customers_raw.select("cdc_operation").distinct()
confirmed_ops   = df_incoming_ops.intersectAll(df_valid_ops).count()
total_ops       = df_incoming_ops.count()
print(f"CDC operation validation: {confirmed_ops}/{total_ops} distinct ops are known values")

# ── exceptAll — isolate DELETE-only batch for fast close path ─────────────────
df_deletes_only = df_customers_raw.filter(col("cdc_operation") == "DELETE")
df_upserts      = df_customers_raw.exceptAll(df_deletes_only)   # INSERT + UPDATE rows
print(f"Deletes: {df_deletes_only.count()}  |  Upserts: {df_upserts.count()}")

# ── Full incoming with hash + all derived columns ─────────────────────────────
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
        # ── md5 hash — only tracked source columns ───────────────────────────
        md5(concat_ws("||", *[col(c).cast("string") for c in CUSTOMER_TRACKED]))
            .alias("_record_hash"),
        # ── lag() — prior hash to surface in audit log ────────────────────────
        lag(md5(concat_ws("||", *[col(c).cast("string") for c in CUSTOMER_TRACKED])), 1)
        .over(w_customer_hist).alias("_prior_hash"),
        # ── translate — ASCII-safe name for downstream search ─────────────────
        translate(
            concat_ws(" ", trim(col("first_name")), trim(col("last_name"))),
            "àáâãäåèéêëìíîïòóôõöùúûüý",
            "aaaaaaeeeeiiiioooouuuuy"
        ).alias("full_name_ascii"),
        concat_ws(" ", trim(col("first_name")), trim(col("last_name"))).alias("full_name"),
        # ── regexp_extract — email domain ────────────────────────────────────
        regexp_extract(lower(trim(col("email"))), r"@(.+)$", 1).alias("email_domain"),
        # ── months_between — account age in months ────────────────────────────
        spark_round(
            months_between(current_timestamp(), col("_ingest_time")), 1
        ).cast("int").alias("account_age_months"),
        # ── Business derivations ──────────────────────────────────────────────
        when(col("lifetime_value").cast("double") >= 50000, "VIP")
        .when(col("lifetime_value").cast("double") >= 10000, "Premium")
        .otherwise("Standard").alias("customer_tier"),
        (col("lifetime_value").cast("double") >= 10000).alias("is_high_value"),
        upper(trim(col("country"))).alias("country_code"),
        # ── dense_rank() — customer standing within segment ───────────────────
        dense_rank().over(w_segment_rank).alias("customer_rank_in_segment"),
    )
    # ── pandas_udf — applied as withColumn after base select ─────────────────
    .withColumn("ltv_score",
        ltv_score(col("lifetime_value"), col("account_age_months").cast("double"))
    )
)

df_customers_cdc.createOrReplaceTempView("customers_cdc_typed")

# ── Dedup — latest CDC op per customer ────────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TEMP VIEW customers_cdc_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingest_time DESC) AS _rn
        FROM customers_cdc_typed
    )
    WHERE _rn = 1
""")

# ── Step 1: Close current rows — hash mismatch OR DELETE ─────────────────────
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

# ── Step 2: Insert new version for changed or new keys (not DELETEs) ──────────
spark.sql(f"""
    INSERT INTO {SILVER}.customers_scd
    SELECT
        s.customer_id,
        s.first_name,
        s.last_name,
        s.email,
        s.phone,
        s.country,
        s.city,
        s.segment,
        s.is_active,
        s.lifetime_value,
        s.cdc_operation,
        s._record_hash,
        s._prior_hash,
        s.full_name,
        s.full_name_ascii,
        s.email_domain,
        s.account_age_months,
        s.customer_tier,
        s.is_high_value,
        s.country_code,
        s.customer_rank_in_segment,
        s.ltv_score,
        current_timestamp()                     AS _valid_from,
        CAST('{OPEN_END}' AS TIMESTAMP)         AS _valid_to,
        true                                    AS _is_current,
        current_timestamp()                     AS _silver_ingest_time,
        'cdc_customers_raw'                     AS _bronze_source
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

# COMMAND ----------
# DBTITLE 1, Cell 3 — products_scd

PRODUCT_TRACKED = [
    "product_name", "sku", "category", "sub_category",
    "unit_cost", "unit_price", "is_active", "supplier_id"
]

df_products_raw = spark.table(f"{BRONZE}.cdc_products_raw")

df_products_cdc = (
    # ── selectExpr — all casts + derivations in SQL string expressions ────────
    df_products_raw.selectExpr(
        "product_id",
        "product_name",
        "sku",
        "category",
        "sub_category",
        "CAST(unit_cost  AS DOUBLE) AS unit_cost",
        "CAST(unit_price AS DOUBLE) AS unit_price",
        "CAST(is_active  AS BOOLEAN) AS is_active",
        "supplier_id",
        "cdc_operation",
        "_ingest_time",
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
)

df_products_cdc.createOrReplaceTempView("products_cdc_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW products_cdc_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _ingest_time DESC) AS _rn
        FROM products_cdc_typed
    )
    WHERE _rn = 1
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

# ── Step 2: Insert new version ────────────────────────────────────────────────
spark.sql(f"""
    INSERT INTO {SILVER}.products_scd
    SELECT
        s.product_id,
        s.product_name,
        s.sku,
        s.category,
        s.sub_category,
        s.unit_cost,
        s.unit_price,
        s.is_active,
        s.supplier_id,
        s.cdc_operation,
        s.category_l1,
        s.category_l2,
        s.margin_pct,
        s.markup_pct,
        s.price_tier,
        s.is_high_margin,
        s.unit_margin_usd,
        s._record_hash,
        current_timestamp()                     AS _valid_from,
        CAST('{OPEN_END}' AS TIMESTAMP)         AS _valid_to,
        true                                    AS _is_current,
        current_timestamp()                     AS _silver_ingest_time,
        'cdc_products_raw'                      AS _bronze_source
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

# COMMAND ----------
# DBTITLE 1, Cell 4 — Verification
print("── CDC Silver — SCD Type 2 Verification ─────────────────────────────")

for tbl, pk, derived_cols in [
    ("customers_scd", "customer_id", ["full_name", "customer_tier", "ltv_score", "_prior_hash"]),
    ("products_scd",  "product_id",  ["margin_pct", "price_tier", "category_l1"]),
]:
    total       = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl}").collect()[0][0]
    current     = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl} WHERE _is_current = true").collect()[0][0]
    dupes       = spark.sql(f"SELECT COUNT(*) FROM (SELECT {pk} FROM {SILVER}.{tbl} WHERE _is_current=true GROUP BY {pk} HAVING COUNT(*) > 1)").collect()[0][0]
    open_closed = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl} WHERE _is_current=false AND _valid_to IS NULL").collect()[0][0]
    wrong_end   = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl} WHERE _is_current=true AND _valid_to <> CAST('{OPEN_END}' AS TIMESTAMP)").collect()[0][0]

    print(f"\n  {tbl}")
    print(f"    Total: {total:,}  |  Current: {current:,}  |  History: {total-current:,}")
    print(f"    Duplicate current rows        : {dupes}  {'✅' if dupes == 0 else '❌'}")
    print(f"    Closed rows missing _valid_to : {open_closed}  {'✅' if open_closed == 0 else '❌'}")
    print(f"    Current rows wrong _valid_to  : {wrong_end}  {'✅' if wrong_end == 0 else '❌'}")
    for c in derived_cols:
        n = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl} WHERE {c} IS NULL AND _is_current = true").collect()[0][0]
        print(f"    {c} nulls (current rows)    : {n}  {'✅' if n == 0 else '⚠️'}")

print("\n  ✅ SCD Type 2 verification complete")
