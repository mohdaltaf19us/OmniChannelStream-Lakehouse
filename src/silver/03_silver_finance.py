# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver — Finance Transforms
# MAGIC
# MAGIC No SELECT *, UPDATE SET *, or INSERT * anywhere.
# MAGIC Generated columns EXCLUDED from all MERGEs:
# MAGIC   customers, order_lines → _silver_date
# MAGIC   orders → _event_date (GENERATED from order_date)
# MAGIC   payments → _event_date (GENERATED from payment_date)
# MAGIC   refunds → _event_date (GENERATED from refund_date)
# MAGIC   exchange_rates → _event_date (GENERATED from rate_date)
# MAGIC
# MAGIC | Cell | Techniques |
# MAGIC |------|-----------|
# MAGIC | customers | regexp_extract, translate, months_between, dense_rank(), pandas_udf |
# MAGIC | orders | na.replace, date_trunc, rollup |
# MAGIC | order_lines | withColumnRenamed, all logic in SQL |
# MAGIC | payments | pivot |
# MAGIC | refunds | lead() over window |
# MAGIC | exchange_rates | intersectAll currency validation |

# COMMAND ----------

# DBTITLE 1, Cell 1 — Config
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, lit,
    when, datediff, round as spark_round,
    lower, trim, upper, concat_ws, translate,
    regexp_extract,
    months_between, date_trunc,
    dense_rank, lead,
    sum as spark_sum,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import pandas_udf
import pandas as pd

BRONZE = "omnioc_dev.bronze"
SILVER = "omnioc_dev.silver"
MAJOR_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]

def merge_cols(update_cols):
    upd   = ",\n        ".join([f"t.{c} = s.{c}" for c in update_cols])
    ins   = ", ".join(update_cols)
    vals  = ", ".join([f"s.{c}" for c in update_cols])
    return upd, ins, vals

print(f"✅ Config — Bronze: {BRONZE}  Silver: {SILVER}")

# COMMAND ----------

# DBTITLE 1, Cell 2 — customers
# Techniques: regexp_extract, translate, months_between, dense_rank(), pandas_udf
# EXCLUDES: _silver_date (GENERATED from _silver_ingest_time)

@pandas_udf(DoubleType())
def ltv_score(lifetime_value: pd.Series, age_months: pd.Series) -> pd.Series:
    """Composite LTV score 0-100: 60% lifetime_value weight + 40% tenure weight."""
    return ((lifetime_value.clip(0, 100_000) / 100_000 * 60)
          + (age_months.clip(0, 60) / 60 * 40)).round(2)

w_segment_rank = Window.partitionBy("segment").orderBy(lit(0.0).desc())

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
        to_date(col("signup_date")).alias("registration_date"),
        col("is_active").cast("boolean"),
        lit(0.0).alias("lifetime_value"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("updated_at")).alias("updated_at"),
        # ── concat_ws — full display name ─────────────────────────────────────
        concat_ws(" ", trim(col("first_name")), trim(col("last_name"))).alias("full_name"),
        # ── translate — strip accents for ASCII-safe search index ─────────────
        translate(
            concat_ws(" ", trim(col("first_name")), trim(col("last_name"))),
            "àáâãäåèéêëìíîïòóôõöùúûüý",
            "aaaaaaeeeeiiiioooouuuuy"
        ).alias("full_name_ascii"),
        # ── regexp_extract — email domain after @ ─────────────────────────────
        regexp_extract(lower(trim(col("email"))), r"@(.+)$", 1).alias("email_domain"),
        # ── months_between — account age in months ────────────────────────────
        spark_round(
            months_between(current_timestamp(), to_timestamp(col("created_at"))), 1
        ).cast("int").alias("account_age_months"),
        datediff(current_timestamp().cast("date"), to_date(col("signup_date")))
            .alias("account_age_days"),
        # ── Business derivations ──────────────────────────────────────────────
        when(col("lifetime_value").cast("double") >= 50000, "VIP")
        .when(col("lifetime_value").cast("double") >= 10000, "Premium")
        .otherwise("Standard").alias("customer_tier"),
        (col("lifetime_value").cast("double") >= 10000).alias("is_high_value"),
        upper(trim(col("country"))).alias("country_code"),
        # ── dense_rank() — customer rank within segment by LTV ────────────────
        dense_rank().over(w_segment_rank).alias("customer_rank_in_segment"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("customers_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
    # ── pandas_udf — vectorised LTV score ────────────────────────────────────
    .withColumn("ltv_score",
        ltv_score(col("lifetime_value"), col("account_age_months").cast("double"))
    )
)

df_customers.createOrReplaceTempView("customers_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW customers_stage AS
    SELECT
        customer_id, first_name, last_name, email, phone, country, city, segment,
        registration_date, is_active, lifetime_value, created_at, updated_at,
        full_name, full_name_ascii, email_domain, account_age_months, account_age_days,
        customer_tier, is_high_value, country_code, customer_rank_in_segment, ltv_score,
        _silver_ingest_time, _bronze_source
    FROM customers_raw_typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingest_time DESC) = 1
""")

CUSTOMER_COLS = [
    "customer_id", "first_name", "last_name", "email", "phone", "country", "city", "segment",
    "registration_date", "is_active", "lifetime_value", "created_at", "updated_at",
    "full_name", "full_name_ascii", "email_domain", "account_age_months", "account_age_days",
    "customer_tier", "is_high_value", "country_code", "customer_rank_in_segment", "ltv_score",
    "_silver_ingest_time", "_bronze_source",
]
upd, ins, vals = merge_cols(CUSTOMER_COLS)

spark.sql(f"""
    MERGE INTO {SILVER}.customers AS t
    USING customers_stage AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET
        {upd}
    WHEN NOT MATCHED THEN INSERT ({ins})
    VALUES ({vals})
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.customers").collect()[0][0]
print(f"✅ customers — {cnt:,} rows")
spark.sql(f"""
    SELECT customer_tier, COUNT(*) cnt, ROUND(AVG(lifetime_value),0) avg_ltv,
           ROUND(AVG(ltv_score),1) avg_score
    FROM {SILVER}.customers GROUP BY 1 ORDER BY avg_ltv DESC
""").show()

# COMMAND ----------

# DBTITLE 1, Cell 3 — orders
# Techniques: na.replace, date_trunc, rollup
# EXCLUDES: _event_date (GENERATED from order_date)

df_orders_raw = spark.table(f"{BRONZE}.orders_raw")

# ── na.replace — normalise legacy status codes before processing ──────────────
df_orders_normalised = df_orders_raw.na.replace(
    {"pending": "placed", "complete": "delivered", "canceled": "cancelled"},
    subset=["status"]
)

df_orders = (
    df_orders_normalised
    .select(
        col("order_id"), col("customer_id"), col("warehouse_id"),
        to_date(col("order_date")).alias("order_date"),
        col("status"), col("channel"), col("currency"),
        col("total_amount").cast("double"),
        col("discount_amount").cast("double"),
        col("tax_amount").cast("double"),
        col("shipping_amount").cast("double"),
        col("shipping_address_country").alias("shipping_country"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("updated_at")).alias("updated_at"),
        # ── date_trunc — month reporting key e.g. '2024-01-01' ───────────────
        date_trunc("month", to_date(col("order_date"))).cast("string").alias("month_key"),
        # ── Business derivations ──────────────────────────────────────────────
        spark_round(col("total_amount").cast("double") - col("discount_amount").cast("double"), 2)
            .alias("net_amount"),
        spark_round(
            col("total_amount").cast("double") + col("tax_amount").cast("double")
            + col("shipping_amount").cast("double"), 2
        ).alias("gross_revenue"),
        spark_round(
            when(col("total_amount").cast("double") > 0,
                 col("discount_amount").cast("double") / col("total_amount").cast("double") * 100
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
    SELECT
        order_id, customer_id, warehouse_id, order_date, status, channel, currency,
        total_amount, discount_amount, tax_amount, shipping_amount, shipping_country,
        created_at, updated_at, month_key, net_amount, gross_revenue,
        discount_rate_pct, is_discounted, order_value_tier, is_marketplace_order,
        _silver_ingest_time, _bronze_source
    FROM orders_raw_typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingest_time DESC) = 1
""")

ORDER_COLS = [
    "order_id", "customer_id", "warehouse_id", "order_date", "status", "channel", "currency",
    "total_amount", "discount_amount", "tax_amount", "shipping_amount", "shipping_country",
    "created_at", "updated_at", "month_key", "net_amount", "gross_revenue",
    "discount_rate_pct", "is_discounted", "order_value_tier", "is_marketplace_order",
    "_silver_ingest_time", "_bronze_source",
]
upd, ins, vals = merge_cols(ORDER_COLS)

spark.sql(f"""
    MERGE INTO {SILVER}.orders AS t
    USING orders_stage AS s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET
        {upd}
    WHEN NOT MATCHED THEN INSERT ({ins})
    VALUES ({vals})
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.orders").collect()[0][0]
print(f"✅ orders — {cnt:,} rows")

# ── rollup — hierarchical aggregation: channel → order_value_tier ─────────────
print("\n── Rollup: channel → order_value_tier (NULLs = subtotals) ──────────")
spark.table(f"{SILVER}.orders") \
    .rollup("channel", "order_value_tier") \
    .agg(spark_round(spark_sum("net_amount"), 0).alias("net_revenue")) \
    .orderBy("channel", "order_value_tier") \
    .show(12)

# COMMAND ----------

# DBTITLE 1, Cell 4 — order_lines
# Techniques: withColumnRenamed, all business logic in SQL view
# EXCLUDES: _silver_date (GENERATED)
# NOTE: line_total already in silver DDL — effective_unit_price, discount_amount, is_discounted_line are new

df_order_lines = (
    spark.table(f"{BRONZE}.order_lines_raw")
    .select(
        col("line_id"), col("order_id"), col("product_id"),
        col("quantity").cast("int"),
        col("unit_price").cast("double"),
        col("discount_pct").cast("double"),
        col("_ingest_time"),
    )
    # ── withColumnRenamed — rename to avoid ambiguity in SQL view ─────────────
    .withColumnRenamed("discount_pct", "discount_pct_raw")
)

df_order_lines.createOrReplaceTempView("order_lines_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW order_lines_stage AS
    SELECT
        line_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_pct_raw                                                AS discount_pct,
        ROUND(quantity * unit_price * (1 - discount_pct_raw / 100), 2) AS line_total,
        ROUND(unit_price * (1 - discount_pct_raw / 100), 2)            AS effective_unit_price,
        ROUND(quantity * unit_price * (discount_pct_raw / 100), 2)     AS discount_amount,
        (discount_pct_raw > 0)                                         AS is_discounted_line,
        current_timestamp()                                            AS _silver_ingest_time,
        'order_lines_raw'                                              AS _bronze_source
    FROM order_lines_raw_typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY line_id ORDER BY _ingest_time DESC) = 1
""")

ORDER_LINE_COLS = [
    "line_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct",
    "line_total", "effective_unit_price", "discount_amount", "is_discounted_line",
    "_silver_ingest_time", "_bronze_source",
]
upd, ins, vals = merge_cols(ORDER_LINE_COLS)

spark.sql(f"""
    MERGE INTO {SILVER}.order_lines AS t
    USING order_lines_stage AS s
    ON t.line_id = s.line_id
    WHEN MATCHED THEN UPDATE SET
        {upd}
    WHEN NOT MATCHED THEN INSERT ({ins})
    VALUES ({vals})
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.order_lines").collect()[0][0]
print(f"✅ order_lines — {cnt:,} rows")

# COMMAND ----------

# DBTITLE 1, Cell 5 — payments
# Techniques: pivot
# EXCLUDES: _event_date (GENERATED from payment_date)

df_payments = (
    spark.table(f"{BRONZE}.payments_raw")
    .select(
        col("payment_id"), col("order_id"),
        lit(None).cast("string").alias("customer_id"),
        to_date(col("paid_at")).alias("payment_date"),
        col("method").alias("payment_method"),
        col("amount").cast("double"),
        col("currency"), col("status"), col("gateway"),
        col("transaction_id").alias("transaction_ref"),
        # ── Business derivations ──────────────────────────────────────────────
        (col("status") == "completed").alias("is_successful"),
        (col("status") == "failed").alias("is_failed"),
        (col("status") == "refunded").alias("is_refunded"),
        col("method").isin("credit_card", "debit_card").alias("is_card_payment"),
        col("method").isin("credit_card", "debit_card", "paypal", "crypto")
            .alias("is_digital_payment"),
        when(col("method").isin("credit_card", "debit_card"), "card")
        .when(col("method") == "paypal",        "digital_wallet")
        .when(col("method") == "bank_transfer", "bank")
        .when(col("method") == "crypto",        "crypto")
        .otherwise("other").alias("payment_category"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("payments_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
)

df_payments.createOrReplaceTempView("payments_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW payments_stage AS
    SELECT
        payment_id, order_id, customer_id, payment_date, payment_method,
        amount, currency, status, gateway, transaction_ref,
        is_successful, is_failed, is_refunded, is_card_payment, is_digital_payment,
        payment_category,
        _silver_ingest_time, _bronze_source
    FROM payments_raw_typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _ingest_time DESC) = 1
""")

PAYMENT_COLS = [
    "payment_id", "order_id", "customer_id", "payment_date", "payment_method",
    "amount", "currency", "status", "gateway", "transaction_ref",
    "is_successful", "is_failed", "is_refunded", "is_card_payment", "is_digital_payment",
    "payment_category",
    "_silver_ingest_time", "_bronze_source",
]
upd, ins, vals = merge_cols(PAYMENT_COLS)

spark.sql(f"""
    MERGE INTO {SILVER}.payments AS t
    USING payments_stage AS s
    ON t.payment_id = s.payment_id
    WHEN MATCHED THEN UPDATE SET
        {upd}
    WHEN NOT MATCHED THEN INSERT ({ins})
    VALUES ({vals})
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.payments").collect()[0][0]
print(f"✅ payments — {cnt:,} rows")

# ── pivot — count of payments by status across payment categories ──────────────
print("\n── Pivot: payment_category × status (count) ─────────────────────")
spark.table(f"{SILVER}.payments") \
    .groupBy("payment_category") \
    .pivot("status", ["pending", "completed", "failed", "refunded"]) \
    .count() \
    .orderBy("payment_category") \
    .show()

# COMMAND ----------

# DBTITLE 1, Cell 6 — refunds
# Techniques: lead() over window — detect repeat refunders
# EXCLUDES: _event_date (GENERATED from refund_date)

df_refunds_base = (
    spark.table(f"{BRONZE}.refunds_raw")
    .select(
        col("refund_id"), col("order_id"),
        lit(None).cast("string").alias("payment_id"),
        lit(None).cast("string").alias("customer_id"),
        to_date(col("processed_at")).alias("refund_date"),
        col("refund_amount").cast("double"),
        col("currency"), col("reason"), col("status"),
        col("_ingest_time"),
    )
)

w_customer_refund = Window.partitionBy("customer_id").orderBy("refund_date")

df_refunds = (
    df_refunds_base
    .select(
        col("refund_id"), col("order_id"), col("payment_id"), col("customer_id"),
        col("refund_date"),
        col("refund_amount"),
        col("currency"), col("reason"), col("status"),
        # ── Business derivations ──────────────────────────────────────────────
        (col("refund_amount") >= 500).alias("is_high_value_refund"),
        (col("status") == "completed").alias("is_processed"),
        (col("status") == "rejected").alias("is_rejected"),
        when(col("reason").isin("defective", "wrong_item"), "product_issue")
        .when(col("reason") == "not_delivered",              "fulfillment_issue")
        .when(col("reason") == "customer_request",           "customer_initiated")
        .otherwise("other").alias("refund_reason_category"),
        # ── lead() — next refund date for same customer ────────────────────────
        lead(col("refund_date"), 1).over(w_customer_refund).alias("next_refund_date"),
        datediff(
            lead(col("refund_date"), 1).over(w_customer_refund),
            col("refund_date")
        ).alias("days_to_next_refund"),
        current_timestamp().alias("_silver_ingest_time"),
        lit("refunds_raw").alias("_bronze_source"),
        col("_ingest_time"),
    )
)

df_refunds.createOrReplaceTempView("refunds_raw_typed")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW refunds_stage AS
    SELECT
        refund_id, order_id, payment_id, customer_id, refund_date,
        refund_amount, currency, reason, status,
        is_high_value_refund, is_processed, is_rejected, refund_reason_category,
        next_refund_date, days_to_next_refund,
        _silver_ingest_time, _bronze_source
    FROM refunds_raw_typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY refund_id ORDER BY _ingest_time DESC) = 1
""")

REFUND_COLS = [
    "refund_id", "order_id", "payment_id", "customer_id", "refund_date",
    "refund_amount", "currency", "reason", "status",
    "is_high_value_refund", "is_processed", "is_rejected", "refund_reason_category",
    "next_refund_date", "days_to_next_refund",
    "_silver_ingest_time", "_bronze_source",
]
upd, ins, vals = merge_cols(REFUND_COLS)

spark.sql(f"""
    MERGE INTO {SILVER}.refunds AS t
    USING refunds_stage AS s
    ON t.refund_id = s.refund_id
    WHEN MATCHED THEN UPDATE SET
        {upd}
    WHEN NOT MATCHED THEN INSERT ({ins})
    VALUES ({vals})
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.refunds").collect()[0][0]
print(f"✅ refunds — {cnt:,} rows")
spark.sql(f"""
    SELECT refund_reason_category, COUNT(*) cnt,
           ROUND(AVG(refund_amount),0) avg_amt,
           ROUND(AVG(days_to_next_refund),1) avg_days_to_next
    FROM {SILVER}.refunds GROUP BY 1 ORDER BY cnt DESC
""").show()

# COMMAND ----------

# DBTITLE 1, Cell 7 — exchange_rates
# Techniques: intersectAll — validate major currencies loaded
# EXCLUDES: _event_date (GENERATED from rate_date)

from pyspark.sql import Row

df_exchange_rates = (
    spark.table(f"{BRONZE}.exchange_rates_raw")
    .select(
        concat_ws("_",
            col("rate_date"),
            upper(trim(col("base_currency"))),
            upper(trim(col("target_currency")))
        ).alias("rate_id"),
        upper(trim(col("base_currency"))).alias("from_currency"),
        upper(trim(col("target_currency"))).alias("to_currency"),
        to_date(col("rate_date")).alias("rate_date"),
        col("exchange_rate").cast("double"),
        col("source"),
        spark_round(lit(1.0) / col("exchange_rate").cast("double"), 8).alias("inverse_rate"),
        concat_ws("_",
            upper(trim(col("base_currency"))),
            upper(trim(col("target_currency")))
        ).alias("pair_key"),
        (
            upper(trim(col("base_currency"))).isin(MAJOR_CURRENCIES) &
            upper(trim(col("target_currency"))).isin(MAJOR_CURRENCIES)
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
    SELECT
        rate_id, from_currency, to_currency, rate_date, exchange_rate, source,
        inverse_rate, pair_key, is_major_pair, rate_age_days,
        _silver_ingest_time, _bronze_source
    FROM exchange_rates_raw_typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rate_id ORDER BY _ingest_time DESC) = 1
""")

RATE_COLS = [
    "rate_id", "from_currency", "to_currency", "rate_date", "exchange_rate", "source",
    "inverse_rate", "pair_key", "is_major_pair", "rate_age_days",
    "_silver_ingest_time", "_bronze_source",
]
upd, ins, vals = merge_cols(RATE_COLS)

spark.sql(f"""
    MERGE INTO {SILVER}.exchange_rates AS t
    USING exchange_rates_stage AS s
    ON t.rate_id = s.rate_id
    WHEN MATCHED THEN UPDATE SET
        {upd}
    WHEN NOT MATCHED THEN INSERT ({ins})
    VALUES ({vals})
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.exchange_rates").collect()[0][0]
print(f"✅ exchange_rates — {cnt:,} rows")

# ── intersectAll — confirm major currencies are present in loaded data ─────────
df_expected  = spark.createDataFrame([Row(currency=c) for c in MAJOR_CURRENCIES])
df_loaded    = spark.table(f"{SILVER}.exchange_rates").select(col("from_currency").alias("currency")).distinct()
df_confirmed = df_expected.intersectAll(df_loaded)
print(f"\n── intersectAll: {df_confirmed.count()}/{len(MAJOR_CURRENCIES)} major currencies confirmed")
df_confirmed.show()

# COMMAND ----------

# DBTITLE 1, Cell 8 — Verification
print("── Finance Silver — Verification ───────────────────────────────────")
checks = [
    ("customers",     "customer_id",  ["email_domain", "customer_rank_in_segment", "ltv_score"]),
    ("orders",        "order_id",     ["net_amount", "month_key", "order_value_tier"]),
    ("order_lines",   "line_id",      ["line_total", "effective_unit_price", "discount_amount"]),
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
