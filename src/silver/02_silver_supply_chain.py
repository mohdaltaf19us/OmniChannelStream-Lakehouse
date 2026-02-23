# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver — Supply Chain Transforms
# MAGIC
# MAGIC **Transformation techniques demonstrated:**
# MAGIC
# MAGIC | Technique | Section | Applied To |
# MAGIC |-----------|---------|-----------|
# MAGIC | `regexp_replace` | §9 String | suppliers — strip phone to digits |
# MAGIC | `lpad` | §9 String | suppliers — enforce ID width |
# MAGIC | `window rank()` | §5 Window | suppliers — rank by rating within country |
# MAGIC | `avg() over window` | §5 Window | suppliers — country average rating |
# MAGIC | `split()` + array index | §9 String | products — decompose category path |
# MAGIC | `selectExpr` | §1 Shaping | products — inline SQL expressions |
# MAGIC | `na.fill` | §1 Shaping | shipments — fill null status/carrier |
# MAGIC | `coalesce` | §9 Null | shipments — null-safe actual_arrival |
# MAGIC | `sortWithinPartitions` | §6 Sort | shipments — order within partition |
# MAGIC | `months_between` | §9 Date | supplier_contracts — duration in months |
# MAGIC | `date_trunc` | §9 Date | supplier_contracts — quarter bucket |
# MAGIC | `cube` | §4 Agg | shipments spot check — multi-dim rollup |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, lit,
    when, datediff, round as spark_round,
    lower, trim, upper,
    regexp_replace, split, lpad, concat_ws,
    months_between, date_trunc,
    coalesce,
    rank, avg as spark_avg,
)
from pyspark.sql.window import Window

BRONZE = "omnioc_dev.bronze"
SILVER = "omnioc_dev.silver"
print(f"✅ Config — Bronze: {BRONZE}  Silver: {SILVER}")

# COMMAND ----------
# DBTITLE 1, Cell 2 — suppliers
# Techniques: regexp_replace, lpad, window rank(), avg() over window

w_country_rank = Window.partitionBy("country").orderBy(col("rating").cast("double").desc())
w_country_avg  = Window.partitionBy("country").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_suppliers = (
    spark.table(f"{BRONZE}.suppliers_raw")
    .select(
        col("supplier_id"),
        col("supplier_name"),
        col("country"),
        col("city"),
        lower(trim(col("contact_email"))).alias("contact_email"),
        col("contact_phone"),
        col("rating").cast("double"),
        col("lead_time_days").cast("int"),
        col("payment_terms_days").cast("int"),
        col("is_active").cast("boolean"),
        to_date(col("contract_start_date")).alias("contract_start_date"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("updated_at")).alias("updated_at"),
        # ── regexp_replace — strip all non-digit chars from phone ─────────────
        regexp_replace(col("contact_phone"), r"[^\d]", "").alias("phone_normalized"),
        # ── Business derivations ──────────────────────────────────────────────
        when(col("payment_terms_days") <= 30, "Gold")
        .when(col("payment_terms_days") <= 60, "Silver")
        .otherwise("Bronze").alias("payment_tier"),
        when(col("lead_time_days") < 15,  "fast")
        .when(col("lead_time_days") <= 45, "standard")
        .otherwise("slow").alias("lead_time_category"),
        (col("rating").cast("double") >= 4.0).alias("is_high_rating"),
        # ── window rank() — supplier rank by rating within their country ───────
        rank().over(w_country_rank).alias("supplier_rank_in_country"),
        # ── avg() over window — country-level running average rating ──────────
        spark_round(
            spark_avg(col("rating").cast("double")).over(w_country_avg), 2
        ).alias("country_avg_rating"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("suppliers_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
)

df_suppliers.createOrReplaceTempView("suppliers_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW suppliers_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY _ingest_time DESC) AS _rn
        FROM suppliers_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.suppliers AS t
    USING suppliers_stage AS s
    ON t.supplier_id = s.supplier_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.suppliers").collect()[0][0]
print(f"✅ suppliers — {cnt:,} rows")
spark.sql(f"""
    SELECT country, COUNT(*) cnt, ROUND(AVG(rating),2) avg_rtg,
           MIN(supplier_rank_in_country) top_rank
    FROM {SILVER}.suppliers GROUP BY country ORDER BY avg_rtg DESC LIMIT 5
""").show()

# COMMAND ----------
# DBTITLE 1, Cell 3 — products
# Techniques: selectExpr (inline SQL exprs), split() array indexing

df_products = (
    # ── selectExpr — mix Python column refs and SQL string expressions ─────────
    spark.table(f"{BRONZE}.products_raw").selectExpr(
        "product_id",
        "product_name",
        "sku",
        "category",
        "sub_category",
        "CAST(unit_cost  AS DOUBLE) AS unit_cost",
        "CAST(unit_price AS DOUBLE) AS unit_price",
        "CAST(weight_kg  AS DOUBLE) AS weight_kg",
        "CAST(is_active  AS BOOLEAN) AS is_active",
        "supplier_id",
        "CAST(reorder_point    AS INT) AS reorder_point",
        "CAST(reorder_quantity AS INT) AS reorder_quantity",
        "TO_TIMESTAMP(created_at) AS created_at",
        "TO_TIMESTAMP(updated_at) AS updated_at",
        # ── split() — decompose 'Electronics/Consumer' path into levels ────────
        "split(category, '/')[0]     AS category_l1",
        "split(sub_category, '/')[0] AS category_l2",
        # ── Business derivations expressed as SQL strings ─────────────────────
        "ROUND((unit_price - unit_cost) / NULLIF(unit_price, 0) * 100, 2) AS margin_pct",
        "ROUND((unit_price - unit_cost) / NULLIF(unit_cost,  0) * 100, 2) AS markup_pct",
        """CASE WHEN CAST(unit_price AS DOUBLE) < 20   THEN 'budget'
               WHEN CAST(unit_price AS DOUBLE) <= 100 THEN 'mid'
               ELSE 'premium' END AS price_tier""",
        "((unit_price - unit_cost) / NULLIF(unit_price,0) * 100 >= 40) AS is_high_margin",
        "ROUND(unit_price - unit_cost, 2) AS unit_margin_usd",
        "current_timestamp() AS _silver_ingest_time",
        "'products_raw' AS _bronze_source",
        "_ingest_time",
    )
)

df_products.createOrReplaceTempView("products_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW products_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _ingest_time DESC) AS _rn
        FROM products_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.products AS t
    USING products_stage AS s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.products").collect()[0][0]
print(f"✅ products — {cnt:,} rows")
spark.sql(f"""
    SELECT category_l1, price_tier, COUNT(*) cnt,
           ROUND(AVG(margin_pct),1) avg_margin
    FROM {SILVER}.products GROUP BY 1,2 ORDER BY 1,2
""").show()

# COMMAND ----------
# DBTITLE 1, Cell 4 — warehouses
df_warehouses = (
    spark.table(f"{BRONZE}.warehouses_raw")
    .select(
        col("warehouse_id"),
        col("warehouse_name"),
        col("country"),
        col("city"),
        col("region"),
        col("capacity_sqft").cast("int"),
        col("is_active").cast("boolean"),
        col("manager_name"),
        lower(trim(col("contact_email"))).alias("contact_email"),
        to_timestamp(col("created_at")).alias("created_at"),
        when(col("capacity_sqft").cast("int") < 50000,    "small")
        .when(col("capacity_sqft").cast("int") <= 200000, "medium")
        .otherwise("large").alias("capacity_tier"),
        upper(trim(col("country"))).alias("country_code"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("warehouses_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
)

df_warehouses.createOrReplaceTempView("warehouses_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW warehouses_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY _ingest_time DESC) AS _rn
        FROM warehouses_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.warehouses AS t
    USING warehouses_stage AS s
    ON t.warehouse_id = s.warehouse_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.warehouses").collect()[0][0]
print(f"✅ warehouses — {cnt:,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 5 — shipments
# Techniques: na.fill, coalesce, sortWithinPartitions, cube aggregation

df_shipments_raw = spark.table(f"{BRONZE}.shipments_raw")

# ── na.fill — replace NULL strings before transforms so CASE WHEN is clean ────
df_shipments_filled = df_shipments_raw.na.fill({
    "status":  "unknown",
    "carrier": "unassigned",
})

df_shipments = (
    df_shipments_filled
    .select(
        col("shipment_id"),
        col("order_id"),
        col("supplier_id"),
        col("warehouse_id"),
        col("product_id"),
        col("quantity").cast("int"),
        to_date(col("shipment_date")).alias("shipment_date"),
        to_date(col("expected_arrival")).alias("expected_arrival"),
        # ── coalesce — if actual_arrival is NULL use expected_arrival as fallback
        coalesce(
            to_date(col("actual_arrival")),
            to_date(col("expected_arrival"))
        ).alias("actual_arrival"),
        col("carrier"),
        col("tracking_number"),
        col("status"),
        col("freight_cost").cast("double"),
        # ── Business derivations ──────────────────────────────────────────────
        (col("actual_arrival").isNotNull() &
         (to_date(col("actual_arrival")) > to_date(col("expected_arrival")))
        ).alias("is_delayed"),
        when(col("actual_arrival").isNotNull(),
             datediff(to_date(col("actual_arrival")), to_date(col("expected_arrival")))
        ).alias("delay_days"),
        when(col("actual_arrival").isNotNull(),
             datediff(to_date(col("actual_arrival")), to_date(col("shipment_date")))
        ).alias("transit_days"),
        datediff(to_date(col("expected_arrival")), to_date(col("shipment_date")))
            .alias("expected_transit_days"),
        when(col("actual_arrival").isNull(), "in_transit")
        .when(to_date(col("actual_arrival")) <= to_date(col("expected_arrival")), "on_time_or_early")
        .otherwise("delayed").alias("delivery_status"),
        spark_round(
            col("freight_cost").cast("double") / col("quantity").cast("double"), 4
        ).alias("freight_cost_per_unit"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("shipments_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
    # ── sortWithinPartitions — stable ordering within each partition pre-write ─
    .sortWithinPartitions("shipment_date", "shipment_id")
)

df_shipments.createOrReplaceTempView("shipments_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW shipments_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY _ingest_time DESC) AS _rn
        FROM shipments_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.shipments AS t
    USING shipments_stage AS s
    ON t.shipment_id = s.shipment_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.shipments").collect()[0][0]
print(f"✅ shipments — {cnt:,} rows")

# ── cube — multi-dimensional aggregation: all combos of status × carrier ───────
print("\n── Cube: delivery_status × carrier (NULLs = subtotals) ─────────────")
spark.table(f"{SILVER}.shipments") \
    .cube("delivery_status", "carrier") \
    .count() \
    .orderBy("delivery_status", "carrier") \
    .show(10)

# COMMAND ----------
# DBTITLE 1, Cell 6 — supplier_contracts
# Techniques: months_between, date_trunc

df_supplier_contracts = (
    spark.table(f"{BRONZE}.supplier_contracts_raw")
    .select(
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
        # ── months_between — precise fractional month count ───────────────────
        spark_round(
            months_between(to_date(col("end_date")), to_date(col("start_date"))), 1
        ).alias("contract_duration_months"),
        # ── date_trunc — round start to nearest quarter for period reporting ──
        date_trunc("quarter", to_date(col("start_date"))).alias("start_quarter"),
        # ── Business derivations ──────────────────────────────────────────────
        datediff(to_date(col("end_date")), current_timestamp().cast("date"))
            .alias("days_until_expiry"),
        (datediff(to_date(col("end_date")), current_timestamp().cast("date")).between(0, 90))
            .alias("is_expiring_soon"),
        (to_date(col("end_date")) < current_timestamp().cast("date"))
            .alias("is_expired"),
        datediff(to_date(col("end_date")), to_date(col("start_date")))
            .alias("contract_duration_days"),
        when(col("annual_value_usd").cast("double") < 100000,    "low")
        .when(col("annual_value_usd").cast("double") <= 1000000, "mid")
        .otherwise("high").alias("contract_value_tier"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("supplier_contracts_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
)

df_supplier_contracts.createOrReplaceTempView("supplier_contracts_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW supplier_contracts_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY _ingest_time DESC) AS _rn
        FROM supplier_contracts_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.supplier_contracts AS t
    USING supplier_contracts_stage AS s
    ON t.contract_id = s.contract_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.supplier_contracts").collect()[0][0]
print(f"✅ supplier_contracts — {cnt:,} rows")
spark.sql(f"""
    SELECT contract_value_tier, is_expiring_soon,
           ROUND(AVG(contract_duration_months),1) avg_months
    FROM {SILVER}.supplier_contracts GROUP BY 1,2 ORDER BY 1
""").show()

# COMMAND ----------
# DBTITLE 1, Cell 7 — Verification
print("── Supply Chain Silver — Verification ──────────────────────────────")
checks = [
    ("suppliers",          "supplier_id",  ["payment_tier", "phone_normalized", "supplier_rank_in_country"]),
    ("products",           "product_id",   ["margin_pct", "category_l1", "category_l2"]),
    ("warehouses",         "warehouse_id", ["capacity_tier"]),
    ("shipments",          "shipment_id",  ["delivery_status", "freight_cost_per_unit"]),
    ("supplier_contracts", "contract_id",  ["contract_duration_months", "days_until_expiry"]),
]
for tbl, pk, derived_cols in checks:
    cnt   = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl}").collect()[0][0]
    nulls = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl} WHERE {pk} IS NULL").collect()[0][0]
    dupes = spark.sql(f"SELECT COUNT(*) FROM (SELECT {pk} FROM {SILVER}.{tbl} GROUP BY {pk} HAVING COUNT(*) > 1)").collect()[0][0]
    print(f"\n  {'✅' if cnt > 0 and nulls == 0 and dupes == 0 else '❌'}  {tbl}")
    print(f"     Rows: {cnt:,}  |  PK nulls: {nulls}  |  Duplicates: {dupes}")
    for c in derived_cols:
        n = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.{tbl} WHERE {c} IS NULL").collect()[0][0]
        print(f"     {c} nulls: {n}")
