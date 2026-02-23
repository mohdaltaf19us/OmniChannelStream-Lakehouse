# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality: Great Expectations Validation
# MAGIC Runs post-load validations on Silver tables.
# MAGIC Results written to `omnioc_dev.bronze.dq_validation_results` for dashboarding.

# COMMAND ----------
# pip install great-expectations
# %pip install great-expectations databricks-sdk

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "omnioc_dev", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")

def silver(t): return f"{CATALOG}.silver.{t}"
def bronze(t): return f"{CATALOG}.bronze.{t}"

DQ_RESULTS_TABLE = f"{CATALOG}.bronze.dq_validation_results"
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

# COMMAND ----------
# MAGIC %md ## Pure PySpark DQ Validation (no GE dependency for basic checks)
# MAGIC
# MAGIC This approach uses PySpark directly to compute DQ metrics — same semantics as GE expectations
# MAGIC but runs natively on Databricks without external dependencies.

class DQSuite:
    """Lightweight DQ framework — defines and runs expectations on Spark DataFrames."""

    def __init__(self, table_name, df):
        self.table  = table_name
        self.df     = df
        self.total  = df.count()
        self.results = []

    def expect_column_not_null(self, column):
        nulls  = self.df.filter(F.col(column).isNull()).count()
        passed = nulls == 0
        self._record("not_null", column, passed,
                     f"{nulls} nulls out of {self.total:,}", nulls)
        return self

    def expect_column_unique(self, column):
        dupes  = self.total - self.df.select(column).dropDuplicates().count()
        passed = dupes == 0
        self._record("unique", column, passed, f"{dupes} duplicates", dupes)
        return self

    def expect_column_values_in_set(self, column, value_set):
        invalid = self.df.filter(~F.col(column).isin(value_set) & F.col(column).isNotNull()).count()
        passed  = invalid == 0
        self._record("values_in_set", column, passed,
                     f"{invalid} values not in {value_set}", invalid)
        return self

    def expect_column_min(self, column, min_val):
        viol   = self.df.filter(F.col(column) < min_val).count()
        passed = viol == 0
        self._record("min_value", column, passed,
                     f"{viol} values below {min_val}", viol)
        return self

    def expect_column_max(self, column, max_val):
        viol   = self.df.filter(F.col(column) > max_val).count()
        passed = viol == 0
        self._record("max_value", column, passed,
                     f"{viol} values above {max_val}", viol)
        return self

    def expect_column_between(self, column, lo, hi):
        viol   = self.df.filter(
            (F.col(column) < lo) | (F.col(column) > hi)
        ).count()
        passed = viol == 0
        self._record("between", column, passed,
                     f"{viol} values outside [{lo}, {hi}]", viol)
        return self

    def expect_row_count_between(self, lo, hi):
        passed = lo <= self.total <= hi
        self._record("row_count", "*", passed,
                     f"row count = {self.total:,} (expected {lo:,}-{hi:,})", 0)
        return self

    def expect_referential_integrity(self, fk_col, parent_table, pk_col):
        parent = spark.table(parent_table).select(F.col(pk_col).alias(fk_col))
        orphans = self.df.join(parent, fk_col, "left_anti").count()
        passed  = orphans == 0
        self._record("referential_integrity", fk_col, passed,
                     f"{orphans} orphan records (no match in {parent_table}.{pk_col})", orphans)
        return self

    def expect_freshness(self, ts_col, max_hours_old=24):
        oldest = self.df.select(F.max(F.col(ts_col))).collect()[0][0]
        if oldest:
            hours_old = (datetime.now(timezone.utc) - oldest.replace(tzinfo=timezone.utc)).seconds / 3600
            passed = hours_old <= max_hours_old
            self._record("freshness", ts_col, passed,
                         f"data is {hours_old:.1f}h old (max: {max_hours_old}h)", 0)
        else:
            self._record("freshness", ts_col, False, "No timestamp data found", 0)
        return self

    def _record(self, expectation_type, column, passed, message, failed_count):
        self.results.append({
            "run_id":            RUN_ID,
            "table_name":        self.table,
            "expectation_type":  expectation_type,
            "column":            column,
            "passed":            passed,
            "total_rows":        self.total,
            "failed_rows":       failed_count,
            "failure_rate_pct":  round(failed_count / self.total * 100, 4) if self.total > 0 else 0.0,
            "message":           message,
            "severity":          "CRITICAL" if not passed and failed_count / max(self.total,1) > 0.05
                                 else ("HIGH" if not passed else "PASS"),
            "run_timestamp":     datetime.now(timezone.utc).isoformat(),
        })
        status = "✅ PASS" if passed else f"❌ FAIL"
        print(f"  {status:8} | {expectation_type:<25} | {column:<25} | {message}")

    def summary(self):
        total  = len(self.results)
        passed = sum(1 for r in self.results if r["passed"])
        print(f"\n  {'─'*60}")
        print(f"  {self.table}: {passed}/{total} checks passed")
        return self.results

# COMMAND ----------
# MAGIC %md ## Run DQ Suites

all_results = []

# ── ORDERS (Silver) ───────────────────────────────────────────────────────────
if spark.catalog.tableExists(silver("orders")):
    print(f"\n{'═'*60}")
    print(f"  orders")
    print(f"{'═'*60}")
    suite = DQSuite("silver.orders", spark.table(silver("orders")))
    results = (
        suite
        .expect_row_count_between(1000, 10_000_000)
        .expect_column_not_null("order_id")
        .expect_column_not_null("customer_id")
        .expect_column_not_null("total_amount")
        .expect_column_unique("order_id")
        .expect_column_min("total_amount", 0.01)
        .expect_column_max("total_amount", 999_999)
        .expect_column_values_in_set("status", [
            "placed","confirmed","processing","shipped","delivered","cancelled","returned"
        ])
        .expect_column_values_in_set("currency", ["USD","EUR","GBP","AUD","CAD","JPY","SGD"])
        .expect_column_values_in_set("channel",  ["web","mobile_app","marketplace","in_store","api"])
        .summary()
    )
    all_results.extend(results)

# ── CUSTOMERS (Silver) ────────────────────────────────────────────────────────
if spark.catalog.tableExists(silver("customers")):
    print(f"\n{'═'*60}")
    print(f"  customers")
    print(f"{'═'*60}")
    suite = DQSuite("silver.customers", spark.table(silver("customers")))
    results = (
        suite
        .expect_row_count_between(100, 10_000_000)
        .expect_column_not_null("customer_id")
        .expect_column_not_null("email")
        .expect_column_unique("customer_id")
        .expect_column_values_in_set("segment", [
            "B2C_Regular","B2C_Premium","B2B_SMB","B2B_Enterprise"
        ])
        .summary()
    )
    all_results.extend(results)

# ── PRODUCTS (Bronze — always available) ──────────────────────────────────────
if spark.catalog.tableExists(bronze("products_raw")):
    print(f"\n{'═'*60}")
    print(f"  products_raw")
    print(f"{'═'*60}")
    suite = DQSuite("bronze.products_raw", spark.table(bronze("products_raw")))
    results = (
        suite
        .expect_row_count_between(100, 1_000_000)
        .expect_column_not_null("product_id")
        .expect_column_not_null("sku")
        .expect_column_unique("product_id")
        .expect_column_min("unit_cost",  0.01)
        .expect_column_min("unit_price", 0.01)
        .expect_column_values_in_set("category", [
            "Electronics","Clothing","Home & Garden","Sports","Books",
            "Food & Beverage","Automotive","Beauty","Toys","Office"
        ])
        .summary()
    )
    all_results.extend(results)

# ── PAYMENTS (Bronze) ─────────────────────────────────────────────────────────
if spark.catalog.tableExists(bronze("payments_raw")):
    print(f"\n{'═'*60}")
    print(f"  payments_raw")
    print(f"{'═'*60}")
    suite = DQSuite("bronze.payments_raw", spark.table(bronze("payments_raw")))
    results = (
        suite
        .expect_row_count_between(100, 10_000_000)
        .expect_column_not_null("payment_id")
        .expect_column_not_null("order_id")
        .expect_column_min("amount", 0.01)
        .expect_column_values_in_set("status", ["completed","pending","failed","refunded"])
        .expect_column_values_in_set("method", [
            "credit_card","debit_card","paypal","bank_transfer","crypto"
        ])
        .summary()
    )
    all_results.extend(results)

# COMMAND ----------
# MAGIC %md ## Persist DQ results to Delta table

if all_results:
    results_schema = StructType([
        StructField("run_id",           StringType(),  False),
        StructField("table_name",       StringType(),  False),
        StructField("expectation_type", StringType(),  False),
        StructField("column",           StringType(),  False),
        StructField("passed",           BooleanType(), False),
        StructField("total_rows",       LongType(),    True),
        StructField("failed_rows",      LongType(),    True),
        StructField("failure_rate_pct", DoubleType(),  True),
        StructField("message",          StringType(),  True),
        StructField("severity",         StringType(),  True),
        StructField("run_timestamp",    StringType(),  True),
    ])
    results_df = spark.createDataFrame(all_results, results_schema)
    results_df.write.format("delta").mode("append").saveAsTable(DQ_RESULTS_TABLE)
    print(f"\n✅ DQ results saved: {DQ_RESULTS_TABLE}")

# COMMAND ----------
# MAGIC %md ## Final Summary + CI/CD gate

total   = len(all_results)
passed  = sum(1 for r in all_results if r["passed"])
failed  = total - passed
critical= sum(1 for r in all_results if r["severity"] == "CRITICAL")

print(f"\n{'═'*60}")
print(f"  DQ VALIDATION SUMMARY  [{RUN_ID}]")
print(f"{'═'*60}")
print(f"  Total checks  : {total}")
print(f"  Passed        : {passed} ({passed/total*100:.1f}%)")
print(f"  Failed        : {failed}")
print(f"  Critical fails: {critical}")
print(f"{'═'*60}")

# CI/CD gate: fail the job if any CRITICAL checks fail
if critical > 0:
    failed_list = [r for r in all_results if r["severity"] == "CRITICAL"]
    for f in failed_list:
        print(f"  CRITICAL: {f['table_name']}.{f['column']} — {f['message']}")
    raise ValueError(f"❌ DQ GATE FAILED — {critical} critical checks failed. See {DQ_RESULTS_TABLE}")

print("\n✅ All DQ checks passed — pipeline can proceed")
