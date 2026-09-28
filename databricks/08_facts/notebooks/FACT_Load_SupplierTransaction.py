# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_SupplierTransaction
# MAGIC Port of `ssis/08_facts/FACT_Load_SupplierTransaction.dtsx` (`build_fact_packages.py::build_fact_load_supplier_transaction`).
# MAGIC
# MAGIC * `Reverse Prior Period Accruals` runs BEFORE the load: accrual rows of a closed accounting period without a reversal get an `ACCREV` row (amount * -1, `is_reversal`, `reverses_transaction_key`).
# MAGIC * Source `silver.stg_supplier_transaction` (`LastModifiedAt` watermark); natural key `SupplierTransactionBusinessKey`.
# MAGIC * Lookups: supplier (SCD2 on transaction date, miss -> inferred member), transaction type (type-1, -1).
# MAGIC * Derived: signed amount (payments / credits negative), supplier aging bucket, fiscal year/period from `AccountingPeriodCode`.
# MAGIC * Target `gold.fact_supplier_transaction` (liquid-clustered `transaction_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_SupplierTransaction"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Supplier Transaction"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_supplier_transaction")))

# COMMAND ----------

ACCRUAL_REVERSAL_TYPE = "ACCREV"

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_supplier_transaction",
    sourceTable="stg_supplier_transaction", sourceDateCol="TransactionDate", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="SupplierTransactionBusinessKey", naturalKeyCols=["SupplierTransactionBusinessKey"],
    surrogateKeyCol="supplier_transaction_key", dateKeyCol="transaction_date_key",
    validation=lambda df: F.col("TransactionAmount").isNull() | F.col("TransactionDate").isNull() | F.col("SupplierBusinessKey").isNull(),
    lookups=[
        fact_load.LookupSpec("Supplier", "SupplierBusinessKey", "supplier_key", "TransactionDate", onMiss=fact_load.ON_MISS_INFER),
        fact_load.LookupSpec("Transaction Type", "TransactionTypeCode", "transaction_type_key"),
    ],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    amount = F.col("TransactionAmount")
    period = F.col("AccountingPeriodCode")
    return df.select(
        F.col("TransactionDate").cast("date").alias("transaction_date_key"),
        F.col("DueDate").cast("date").alias("due_date_key"),
        "supplier_key", "transaction_type_key",
        F.col("RegionCode").alias("region_code"), F.col("LedgerCode").alias("ledger_code"),
        F.col("SupplierTransactionBusinessKey").alias("wwi_supplier_transaction_id"),
        F.col("SupplierInvoiceNumber").alias("supplier_invoice_number"), F.col("TransactionTypeCode").alias("transaction_type_code"),
        F.col("TransactionCurrency").alias("transaction_currency_code"),
        rules.money(rules.signedAmount(amount, F.col("TransactionTypeCode"), rules.NEGATIVE_SUPPLIER_TRANSACTION_TYPES)).alias("transaction_amount"),
        rules.money(F.col("OutstandingBalance")).alias("outstanding_balance"),
        rules.safeDivide(F.col("TransactionAmountUsd"), amount).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(rules.signedAmount(F.col("TransactionAmountUsd"), F.col("TransactionTypeCode"), rules.NEGATIVE_SUPPLIER_TRANSACTION_TYPES)).alias("transaction_amount_reporting"),
        F.coalesce(F.col("IsAccrual"), F.lit(False)).alias("accrual_flag"),
        F.lit(False).alias("is_reversal"), F.lit(None).cast("bigint").alias("reverses_transaction_key"),
        period.alias("accounting_period_code"),
        F.substring(period, 1, 4).cast("smallint").alias("fiscal_year"), F.substring(period, 6, 2).cast("tinyint").alias("fiscal_period"),
        rules.supplierAgingBucket(F.col("DueDate"), F.lit(p["businessDate"])).alias("aging_bucket_code"),
        F.col("supplier_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )


def reversePriorPeriodAccruals(spark, catalog, batchId, packageExecutionId):
    fullName = naming.table(catalog, "gold", "fact_supplier_transaction")
    if not fc.tableExists(spark, fullName):
        return 0
    fact = spark.table(fullName)
    currentPeriod = p["businessDate"].strftime("%Y-%m")
    reversed_ = fact.alias("r").where(F.col("r.reverses_transaction_key").isNotNull()).select(F.col("r.reverses_transaction_key").alias("k"))
    due = (
        fact.alias("f").join(reversed_, F.col("f.supplier_transaction_key") == F.col("k"), "left_anti")
        .where(F.col("f.accrual_flag") & ~F.col("f.is_reversal") & (F.col("f.accounting_period_code") < F.lit(currentPeriod)))
    )
    if due.limit(1).count() == 0:
        return 0
    rows = (
        due.withColumn("transaction_type_code", F.lit(ACCRUAL_REVERSAL_TYPE))
        .withColumn("transaction_amount", rules.negated(F.col("transaction_amount")))
        .withColumn("transaction_amount_reporting", rules.negated(F.col("transaction_amount_reporting")))
        .withColumn("outstanding_balance", F.lit(0).cast(rules.MONEY))
        .withColumn("is_reversal", F.lit(True))
        .withColumn("reverses_transaction_key", F.col("supplier_transaction_key"))
        .withColumn("natural_key_hash", fc.naturalKeyHash(F.lit(ACCRUAL_REVERSAL_TYPE), F.col("supplier_transaction_key")))
    )
    rows = fc.loadAuditColumns(rows, batchId, packageExecutionId)
    rows = fc.assignSurrogateKeys(spark, fullName, rows, "supplier_transaction_key", ["reverses_transaction_key"])
    inserted = fc.appendRows(spark, fullName, rows.select(*fact.columns))
    control.logRowCount(spark, catalog, packageExecutionId, "Fact.Supplier Transaction accrual reversals", insertRowCount=inserted)
    return inserted

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    reversals = reversePriorPeriodAccruals(spark, catalog, batchId, run.packageExecutionId)
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform)
    print(result)
