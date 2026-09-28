# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_CustomerTransaction
# MAGIC Port of `ssis/08_facts/FACT_Load_CustomerTransaction.dtsx` (`build_fact_packages.py::build_fact_load_customer_transaction`).
# MAGIC
# MAGIC * Source `silver.stg_customer_transaction` (`LastModifiedAt` watermark); natural key `CustomerTransactionBusinessKey`.
# MAGIC * Lookups: customer (SCD2 on transaction date, miss -> inferred member + late-arriving queue), transaction type (type-1, miss -> -1).
# MAGIC * Restatements: a re-delivered business key updates the existing row in place (MERGE on the stable `customer_transaction_key`) and bumps `correction_type_code` to `RES`.
# MAGIC * Derived: signed amount (payments/credits/refunds negative), amount excluding tax, aging bucket per region, fiscal year/period from `AccountingPeriodCode`.
# MAGIC * Target `gold.fact_customer_transaction` (liquid-clustered `transaction_date_key, region_code`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import Window
from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import fact_common as fc
import fact_load
import fact_rules as rules

# COMMAND ----------

PACKAGE_NAME = "FACT_Load_CustomerTransaction"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Customer Transaction"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_customer_transaction")))

# COMMAND ----------

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_customer_transaction",
    sourceTable="stg_customer_transaction", sourceDateCol="TransactionDate", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="CustomerTransactionBusinessKey", naturalKeyCols=["CustomerTransactionBusinessKey"],
    surrogateKeyCol="customer_transaction_key", dateKeyCol="transaction_date_key",
    validation=lambda df: F.col("TransactionAmount").isNull() | F.col("TransactionDate").isNull() | F.col("CustomerBusinessKey").isNull(),
    lookups=[
        fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", "TransactionDate", onMiss=fact_load.ON_MISS_INFER),
        fact_load.LookupSpec("Transaction Type", "TransactionTypeCode", "transaction_type_key"),
    ],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    amount = F.col("TransactionAmount")
    signed = rules.signedAmount(amount, F.col("TransactionTypeCode"), rules.NEGATIVE_CUSTOMER_TRANSACTION_TYPES)
    period = F.col("AccountingPeriodCode")
    return df.select(
        F.col("TransactionDate").cast("date").alias("transaction_date_key"),
        F.col("DueDate").cast("date").alias("due_date_key"),
        "customer_key", F.col("customer_key").alias("bill_to_customer_key"), "transaction_type_key",
        F.col("RegionCode").alias("region_code"),
        F.col("CustomerTransactionBusinessKey").alias("wwi_customer_transaction_id"),
        F.col("InvoiceNumber").alias("invoice_number"), F.col("TransactionTypeCode").alias("transaction_type_code"),
        F.col("SourceSystemCode").alias("source_system_code"),
        F.col("TransactionCurrency").alias("transaction_currency_code"),
        rules.money(amount - F.coalesce(F.col("TaxAmount"), F.lit(0))).alias("amount_excluding_tax"),
        rules.money(F.coalesce(F.col("TaxAmount"), F.lit(0))).alias("tax_amount"),
        rules.money(signed).alias("transaction_amount"),
        rules.money(F.col("OutstandingBalance")).alias("outstanding_balance"),
        rules.safeDivide(F.col("TransactionAmountUsd"), amount).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(rules.signedAmount(F.col("TransactionAmountUsd"), F.col("TransactionTypeCode"), rules.NEGATIVE_CUSTOMER_TRANSACTION_TYPES)).alias("transaction_amount_reporting"),
        rules.customerAgingBucket(F.col("DueDate"), F.lit(p["businessDate"]), F.col("RegionCode")).alias("aging_bucket_code"),
        rules.daysOverdue(F.col("DueDate"), F.lit(p["businessDate"])).alias("days_overdue"),
        (F.coalesce(F.col("OutstandingBalance"), F.lit(0)) == 0).alias("is_finalized"),
        F.substring(period, 1, 4).cast("smallint").alias("fiscal_year"),
        F.substring(period, 6, 2).cast("tinyint").alias("fiscal_period"),
        F.lit(fc.CORRECTION_ORIGINAL).alias("correction_type_code"),
        F.col("customer_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )


def flagRestatements(spark, catalog, factRows, merged):
    """Rows that already existed (updated by the MERGE) are restatements of a previously loaded transaction."""
    if merged.get("updated", 0) == 0:
        return
    fullName = naming.table(catalog, "gold", "fact_customer_transaction")
    restated = factRows.alias("s").join(spark.table(fullName).alias("t"), "natural_key_hash").where(F.col("t.lineage_key") != F.col("s.lineage_key")).select("t.customer_transaction_key")
    keys = [r[0] for r in restated.collect()]
    if keys:
        spark.sql("UPDATE %s SET correction_type_code = '%s' WHERE customer_transaction_key IN (%s)" % (fullName, fc.CORRECTION_RESTATEMENT, ",".join(map(str, keys))))

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform, afterMerge=flagRestatements)
    print(result)
