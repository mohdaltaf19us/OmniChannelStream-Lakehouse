# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver — CDC SCD Type 2 Transforms
# MAGIC
# MAGIC Reads bronze CDC tables and maintains full version history using SCD Type 2.
# MAGIC All merge logic written in Spark SQL for readability.
# MAGIC
# MAGIC **SCD Type 2 pattern — two SQL steps per table:**
# MAGIC
# MAGIC | Step | SQL | What it does |
# MAGIC |------|-----|--------------|
# MAGIC | 1 | MERGE with hash comparison | Closes current row when hash differs or DELETE received |
# MAGIC | 2 | INSERT SELECT with anti-join | Inserts new version for changed/new keys only |
# MAGIC
# MAGIC **Hash logic:**
# MAGIC - `_record_hash` = MD5 of all tracked business columns concatenated
# MAGIC - WHEN MATCHED AND `t._record_hash <> s._record_hash` → real change detected → close old row
# MAGIC - WHEN MATCHED AND hashes equal → no change → skip entirely
# MAGIC - WHEN MATCHED AND `cdc_operation = DELETE` → close regardless of hash
# MAGIC
# MAGIC | Cell | Bronze Source | Silver Target |
# MAGIC |------|--------------|---------------|
# MAGIC | 2 | cdc_customers_raw | customers_scd |
# MAGIC | 3 | cdc_products_raw | products_scd |
# MAGIC | 4 | Verification |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import col, current_timestamp, lit, md5, concat_ws, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

BRONZE   = "omnioc_dev.bronze"
SILVER   = "omnioc_dev.silver"
OPEN_END = "9999-12-31 00:00:00"  # sentinel _valid_to for current rows

def dedup_cdc(df, pk):
    """Keep latest CDC operation per key — source may send duplicates."""
    w = Window.partitionBy(pk).orderBy(col("_ingest_time").desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

print("✅ Config ready")
print(f"   Bronze   : {BRONZE}")
print(f"   Silver   : {SILVER}")
print(f"   Open-end : {OPEN_END}")

# COMMAND ----------
# DBTITLE 1, Cell 2 — customers_scd

# Columns whose change triggers a new SCD2 version
CUSTOMER_TRACKED = [
    "first_name", "last_name", "email", "phone",
    "country", "city", "segment", "is_active", "lifetime_value"
]

# ── Prepare incoming: dedup + compute hash ─────────────────────────────────────
(
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
    .createOrReplaceTempView("customers_cdc_stage")
)

# ── Step 1: Close current rows where hash differs or DELETE received ────────────
# Hash comparison in WHEN MATCHED condition — only rows with real changes are touched
# Rows where hash matches are skipped entirely (no update, no I/O cost)
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

# ── Step 2: Insert new version for changed or new keys (not DELETEs) ───────────
# Left join against current silver rows:
#   - No match (new key)   → existing_hash IS NULL        → insert
#   - Hash differs (change) → _record_hash <> existing_hash → insert
#   - Hash same (no change) → skip
#   - DELETE               → skip (no new version for deleted records)
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
        current_timestamp()                         AS _valid_from,
        CAST('{OPEN_END}' AS TIMESTAMP)             AS _valid_to,
        true                                        AS _is_current,
        current_timestamp()                         AS _silver_ingest_time,
        'cdc_customers_raw'                         AS _bronze_source
    FROM customers_cdc_stage s
    LEFT JOIN {SILVER}.customers_scd t
        ON  s.customer_id  = t.customer_id
        AND t._is_current  = true
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

(
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
    .createOrReplaceTempView("products_cdc_stage")
)

# ── Step 1: Close current rows where hash differs or DELETE received ────────────
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

# ── Step 2: Insert new version for changed or new keys (not DELETEs) ───────────
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
        s._record_hash,
        current_timestamp()                         AS _valid_from,
        CAST('{OPEN_END}' AS TIMESTAMP)             AS _valid_to,
        true                                        AS _is_current,
        current_timestamp()                         AS _silver_ingest_time,
        'cdc_products_raw'                          AS _bronze_source
    FROM products_cdc_stage s
    LEFT JOIN {SILVER}.products_scd t
        ON  s.product_id   = t.product_id
        AND t._is_current  = true
    WHERE s.cdc_operation != 'DELETE'
      AND (t.product_id IS NULL OR s._record_hash <> t._record_hash)
""")

total   = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.products_scd").collect()[0][0]
current = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.products_scd WHERE _is_current = true").collect()[0][0]
print(f"✅ products_scd — {total:,} total versions  |  {current:,} current")

# COMMAND ----------
# DBTITLE 1, Cell 4 — Verification
print("── CDC Silver — SCD Type 2 Verification ─────────────────────────────")

for tbl, pk in [("customers_scd", "customer_id"), ("products_scd", "product_id")]:

    total   = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl}").collect()[0][0]
    current = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl} WHERE _is_current = true").collect()[0][0]
    history = total - current

    # No PK should have more than one current row
    dupes = spark.sql(f"""
        SELECT COUNT(*) FROM (
            SELECT {pk}, COUNT(*) AS cnt
            FROM {SILVER}.{tbl}
            WHERE _is_current = true
            GROUP BY {pk}
            HAVING cnt > 1
        )
    """).collect()[0][0]

    # All closed rows must have _valid_to set
    open_closed = spark.sql(f"""
        SELECT COUNT(*) FROM {SILVER}.{tbl}
        WHERE _is_current = false AND _valid_to IS NULL
    """).collect()[0][0]

    # All current rows must have _valid_to = 9999-12-31
    wrong_open_end = spark.sql(f"""
        SELECT COUNT(*) FROM {SILVER}.{tbl}
        WHERE _is_current = true
          AND _valid_to <> CAST('{OPEN_END}' AS TIMESTAMP)
    """).collect()[0][0]

    print(f"\n  {tbl}")
    print(f"    Total versions                : {total:,}")
    print(f"    Current (_is_current = true)  : {current:,}")
    print(f"    Historical                    : {history:,}")
    print(f"    Duplicate current rows        : {dupes:<5} {'✅' if dupes == 0 else '❌'}")
    print(f"    Closed rows missing _valid_to : {open_closed:<5} {'✅' if open_closed == 0 else '❌'}")
    print(f"    Current rows wrong _valid_to  : {wrong_open_end:<5} {'✅' if wrong_open_end == 0 else '❌'}")

print("\n  ✅ SCD Type 2 verification complete")
