# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver — Finance Transforms
# MAGIC
# MAGIC **Transformation techniques demonstrated:**
# MAGIC
# MAGIC | Technique | Section | Applied To |
# MAGIC |-----------|---------|-----------|
# MAGIC | `regexp_extract` | §9 String | customers — extract email domain |
# MAGIC | `translate` | §9 String | customers — strip accents from name |
# MAGIC | `months_between` | §9 Date | customers — account age in months |
# MAGIC | `dense_rank() over window` | §5 Window | customers — rank within segment |
# MAGIC | `pandas_udf` | §12 UDF | customers — vectorized LTV scoring |
# MAGIC | `na.replace` | §1 Shaping | orders — normalise status values |
# MAGIC | `date_trunc` | §9 Date | orders — month_key for reporting |
# MAGIC | `rollup` | §4 Agg | orders spot check |
# MAGIC | `pivot` | §4 Agg | payments — status × method matrix |
# MAGIC | `withColumnRenamed` | §1 Shaping | order_lines — rename before merge |
# MAGIC | `lead() over window` | §5 Window | refunds — flag repeated refunder |
# MAGIC | `intersectAll` | §2 Filter | exchange_rates — validate currency set |

# COMMAND ----------
# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, lit,
    when, datediff, round as spark_round,
    lower, trim, upper, concat_ws, translate,
    regexp_replace, regexp_extract,
    months_between, date_trunc,
    coalesce, dense_rank, lead,
    sum as spark_sum,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import pandas as pd

BRONZE = "omnioc_dev.bronze"
SILVER = "omnioc_dev.silver"
MAJOR_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]
print(f"✅ Config — Bronze: {BRONZE}  Silver: {SILVER}")

# COMMAND ----------
# DBTITLE 1, Cell 2 — customers
# Techniques: regexp_extract (email domain), translate (accent strip),
#             months_between, dense_rank() over window, pandas_udf (LTV scoring)

# ── pandas_udf — vectorised LTV scoring (avoids Python UDF row-by-row overhead)
from pyspark.sql.functions import pandas_udf

@pandas_udf(DoubleType())
def ltv_score(lifetime_value: pd.Series, account_age_months: pd.Series) -> pd.Series:
    """
    Composite LTV score 0-100:
      60% weight on normalised lifetime_value (capped at $100k)
      40% weight on normalised tenure (capped at 60 months)
    """
    ltv_norm   = (lifetime_value.clip(0, 100_000) / 100_000 * 60).fillna(0)
    tenure_norm= (account_age_months.clip(0, 60)  / 60 * 40).fillna(0)
    return (ltv_norm + tenure_norm).round(2)

w_segment_rank = Window.partitionBy("segment").orderBy(col("lifetime_value").cast("double").desc())

df_customers = (
    spark.table(f"{BRONZE}.customers_raw")
    .select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        lower(trim(col("email"))).alias("email"),
        col("phone"),
        col("country"),
        col("city"),
        col("segment"),
        to_date(col("registration_date")).alias("registration_date"),
        col("is_active").cast("boolean"),
        col("lifetime_value").cast("double"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("updated_at")).alias("updated_at"),
        # ── concat_ws — full display name ─────────────────────────────────────
        concat_ws(" ", trim(col("first_name")), trim(col("last_name")))
            .alias("full_name"),
        # ── translate — strip common accented chars for ASCII-safe search ─────
        translate(
            concat_ws(" ", trim(col("first_name")), trim(col("last_name"))),
            "àáâãäåèéêëìíîïòóôõöùúûüýÀÁÂÃÄÅÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÝ",
            "aaaaaaeeeeiiiiooooouu uuyAAAAAA EEEEIIIIOOOOO UUUUY"
        ).alias("full_name_ascii"),
        # ── regexp_extract — pull domain from email (after @) ─────────────────
        regexp_extract(col("email"), r"@(.+)$", 1).alias("email_domain"),
        # ── months_between — account tenure in fractional months ─────────────
        spark_round(
            months_between(current_timestamp(), to_timestamp(col("created_at"))), 1
        ).cast("int").alias("account_age_months"),
        # ── Business derivations ──────────────────────────────────────────────
        when(col("lifetime_value").cast("double") >= 50000, "VIP")
        .when(col("lifetime_value").cast("double") >= 10000, "Premium")
        .otherwise("Standard").alias("customer_tier"),
        (col("lifetime_value").cast("double") >= 10000).alias("is_high_value"),
        datediff(current_timestamp().cast("date"), to_date(col("registration_date")))
            .alias("account_age_days"),
        upper(trim(col("country"))).alias("country_code"),
        # ── dense_rank() — rank within segment by lifetime_value ──────────────
        dense_rank().over(w_segment_rank).alias("customer_rank_in_segment"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("customers_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
    # ── pandas_udf applied after base select (needs account_age_months) ───────
    .withColumn("ltv_score", ltv_score(col("lifetime_value"), col("account_age_months").cast("double")))
)

df_customers.createOrReplaceTempView("customers_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW customers_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingest_time DESC) AS _rn
        FROM customers_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.customers AS t
    USING customers_stage AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.customers").collect()[0][0]
print(f"✅ customers — {cnt:,} rows")
spark.sql(f"""
    SELECT customer_tier, COUNT(*) cnt,
           ROUND(AVG(lifetime_value),0) avg_ltv,
           ROUND(AVG(ltv_score),1)      avg_score
    FROM {SILVER}.customers GROUP BY 1 ORDER BY avg_ltv DESC
""").show()

# COMMAND ----------
# DBTITLE 1, Cell 3 — orders
# Techniques: na.replace (normalise status values), date_trunc (month_key), rollup

df_orders_raw = spark.table(f"{BRONZE}.orders_raw")

# ── na.replace — map legacy status codes to canonical values ─────────────────
df_orders_normalised = df_orders_raw.na.replace({
    "pending":   "placed",
    "complete":  "delivered",
    "canceled":  "cancelled",
}, subset=["status"])

df_orders = (
    df_orders_normalised
    .select(
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
        # ── date_trunc — month reporting key e.g. '2024-01-01' ───────────────
        date_trunc("month", to_date(col("order_date"))).cast("string")
            .alias("month_key"),
        # ── Business derivations ──────────────────────────────────────────────
        spark_round(
            col("total_amount").cast("double") - col("discount_amount").cast("double"), 2
        ).alias("net_amount"),
        spark_round(
            col("total_amount").cast("double")
            + col("tax_amount").cast("double")
            + col("shipping_amount").cast("double"), 2
        ).alias("gross_revenue"),
        spark_round(
            when(col("total_amount").cast("double") > 0,
                 col("discount_amount").cast("double")
                 / col("total_amount").cast("double") * 100
            ).otherwise(0), 2
        ).alias("discount_rate_pct"),
        (col("discount_amount").cast("double") > 0).alias("is_discounted"),
        when(col("total_amount").cast("double") < 50,    "micro")
        .when(col("total_amount").cast("double") <= 200, "small")
        .when(col("total_amount").cast("double") <= 1000,"mid")
        .otherwise("large").alias("order_value_tier"),
        (col("channel") == "marketplace").alias("is_marketplace_order"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("orders_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
)

df_orders.createOrReplaceTempView("orders_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW orders_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingest_time DESC) AS _rn
        FROM orders_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.orders AS t
    USING orders_stage AS s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.orders").collect()[0][0]
print(f"✅ orders — {cnt:,} rows")

# ── rollup — hierarchical aggregation: channel → status → total ───────────────
print("\n── Rollup: channel → order_value_tier (NULLs = subtotals) ──────────")
spark.table(f"{SILVER}.orders") \
    .rollup("channel", "order_value_tier") \
    .agg(spark_round(spark_sum("net_amount"), 0).alias("net_revenue")) \
    .orderBy("channel", "order_value_tier") \
    .show(12)

# COMMAND ----------
# DBTITLE 1, Cell 4 — order_lines
# Techniques: withColumnRenamed before merge, all business logic in SQL

df_order_lines = (
    spark.table(f"{BRONZE}.order_lines_raw")
    .select(
        col("line_id"),
        col("order_id"),
        col("product_id"),
        col("quantity").cast("int"),
        col("unit_price").cast("double"),
        col("discount_pct").cast("double"),
        col("_ingest_time"),
    )
    # ── withColumnRenamed — rename before registering as temp view ─────────────
    .withColumnRenamed("discount_pct", "discount_pct_raw")
)

df_order_lines.createOrReplaceTempView("order_lines_raw_typed")

# All derived logic expressed in SQL for max readability
spark.sql("""
    CREATE OR REPLACE TEMP VIEW order_lines_stage AS
    SELECT
        line_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_pct_raw                                                 AS discount_pct,
        ROUND(unit_price * (1 - discount_pct_raw / 100), 2)             AS effective_unit_price,
        ROUND(quantity * unit_price * (1 - discount_pct_raw / 100), 2)  AS line_total,
        ROUND(quantity * unit_price * (discount_pct_raw / 100), 2)      AS discount_amount,
        (discount_pct_raw > 0)                                           AS is_discounted_line,
        current_timestamp()                                              AS _silver_ingest_time,
        'order_lines_raw'                                                AS _bronze_source
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY line_id ORDER BY _ingest_time DESC) AS _rn
        FROM order_lines_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.order_lines AS t
    USING order_lines_stage AS s
    ON t.line_id = s.line_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.order_lines").collect()[0][0]
print(f"✅ order_lines — {cnt:,} rows")

# COMMAND ----------
# DBTITLE 1, Cell 5 — payments
# Techniques: pivot — status × payment_method revenue matrix

df_payments = (
    spark.table(f"{BRONZE}.payments_raw")
    .select(
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
        # ── Business derivations ──────────────────────────────────────────────
        (col("status") == "completed").alias("is_successful"),
        (col("status") == "failed").alias("is_failed"),
        (col("status") == "refunded").alias("is_refunded"),
        col("payment_method").isin("credit_card", "debit_card")
            .alias("is_card_payment"),
        col("payment_method").isin("credit_card", "debit_card", "paypal", "crypto")
            .alias("is_digital_payment"),
        when(col("payment_method").isin("credit_card", "debit_card"), "card")
        .when(col("payment_method") == "paypal",        "digital_wallet")
        .when(col("payment_method") == "bank_transfer", "bank")
        .when(col("payment_method") == "crypto",        "crypto")
        .otherwise("other").alias("payment_category"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("payments_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
)

df_payments.createOrReplaceTempView("payments_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW payments_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _ingest_time DESC) AS _rn
        FROM payments_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.payments AS t
    USING payments_stage AS s
    ON t.payment_id = s.payment_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.payments").collect()[0][0]
print(f"✅ payments — {cnt:,} rows")

# ── pivot — count of payments by status across payment methods ─────────────────
print("\n── Pivot: payment_method × status (count) ───────────────────────────")
spark.table(f"{SILVER}.payments") \
    .groupBy("payment_category") \
    .pivot("status", ["pending", "completed", "failed", "refunded"]) \
    .count() \
    .orderBy("payment_category") \
    .show()

# COMMAND ----------
# DBTITLE 1, Cell 6 — refunds
# Techniques: lead() over window — detect if customer refunds repeatedly

w_customer_refund = Window.partitionBy("customer_id").orderBy("refund_date")

df_refunds = (
    spark.table(f"{BRONZE}.refunds_raw")
    .select(
        col("refund_id"),
        col("order_id"),
        col("payment_id"),
        col("customer_id"),
        to_date(col("refund_date")).alias("refund_date"),
        col("refund_amount").cast("double"),
        col("currency"),
        col("reason"),
        col("status"),
        # ── Business derivations ──────────────────────────────────────────────
        (col("refund_amount").cast("double") >= 500).alias("is_high_value_refund"),
        (col("status") == "completed").alias("is_processed"),
        (col("status") == "rejected").alias("is_rejected"),
        when(col("reason").isin("defective", "wrong_item"), "product_issue")
        .when(col("reason") == "not_delivered",              "fulfillment_issue")
        .when(col("reason") == "customer_request",           "customer_initiated")
        .otherwise("other").alias("refund_reason_category"),
        # ── lead() over window — next refund date for this customer ────────────
        lead(to_date(col("refund_date")), 1)
        .over(w_customer_refund).alias("next_refund_date"),
        # ── derived: days until next refund (repeat refunder signal) ─────────
        datediff(
            lead(to_date(col("refund_date")), 1).over(w_customer_refund),
            to_date(col("refund_date"))
        ).alias("days_to_next_refund"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("refunds_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
)

df_refunds.createOrReplaceTempView("refunds_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW refunds_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY refund_id ORDER BY _ingest_time DESC) AS _rn
        FROM refunds_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.refunds AS t
    USING refunds_stage AS s
    ON t.refund_id = s.refund_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.refunds").collect()[0][0]
print(f"✅ refunds — {cnt:,} rows")
spark.sql(f"""
    SELECT refund_reason_category, COUNT(*) cnt,
           ROUND(SUM(refund_amount),0) total,
           COUNT(next_refund_date) repeat_refunders
    FROM {SILVER}.refunds GROUP BY 1 ORDER BY total DESC
""").show()

# COMMAND ----------
# DBTITLE 1, Cell 7 — exchange_rates
# Techniques: intersectAll — validate that loaded currencies match expected set

df_exchange_rates = (
    spark.table(f"{BRONZE}.exchange_rates_raw")
    .select(
        col("rate_id"),
        upper(trim(col("from_currency"))).alias("from_currency"),
        upper(trim(col("to_currency"))).alias("to_currency"),
        to_date(col("rate_date")).alias("rate_date"),
        col("exchange_rate").cast("double"),
        col("source"),
        spark_round(lit(1.0) / col("exchange_rate").cast("double"), 8)
            .alias("inverse_rate"),
        concat_ws("_",
            upper(trim(col("from_currency"))),
            upper(trim(col("to_currency")))
        ).alias("pair_key"),
        (
            upper(trim(col("from_currency"))).isin(MAJOR_CURRENCIES) &
            upper(trim(col("to_currency"))).isin(MAJOR_CURRENCIES)
        ).alias("is_major_pair"),
        datediff(current_timestamp().cast("date"), to_date(col("rate_date")))
            .alias("rate_age_days"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("exchange_rates_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
)

df_exchange_rates.createOrReplaceTempView("exchange_rates_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW exchange_rates_stage AS
    SELECT * EXCEPT (_ingest_time, _rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY rate_id ORDER BY _ingest_time DESC) AS _rn
        FROM exchange_rates_raw_typed
    )
    WHERE _rn = 1
""")

spark.sql(f"""
    MERGE INTO {SILVER}.exchange_rates AS t
    USING exchange_rates_stage AS s
    ON t.rate_id = s.rate_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.exchange_rates").collect()[0][0]
print(f"✅ exchange_rates — {cnt:,} rows")

# ── intersectAll — confirm that loaded from_currencies overlap expected majors ─
from pyspark.sql import Row
df_expected  = spark.createDataFrame([Row(currency=c) for c in MAJOR_CURRENCIES])
df_loaded    = spark.table(f"{SILVER}.exchange_rates").select(col("from_currency").alias("currency")).distinct()
df_confirmed = df_expected.intersectAll(df_loaded)
print(f"\n── intersectAll: major currencies confirmed in loaded data ──────────")
print(f"   Expected: {len(MAJOR_CURRENCIES)}  |  Confirmed: {df_confirmed.count()}")
df_confirmed.show()

# COMMAND ----------
# DBTITLE 1, Cell 8 — Verification
print("── Finance Silver — Verification ───────────────────────────────────")
checks = [
    ("customers",     "customer_id",  ["email_domain", "customer_rank_in_segment", "ltv_score"]),
    ("orders",        "order_id",     ["net_amount", "month_key", "order_value_tier"]),
    ("order_lines",   "line_id",      ["line_total", "effective_unit_price"]),
    ("payments",      "payment_id",   ["payment_category", "is_successful"]),
    ("refunds",       "refund_id",    ["refund_reason_category", "days_to_next_refund"]),
    ("exchange_rates","rate_id",      ["inverse_rate", "pair_key"]),
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
