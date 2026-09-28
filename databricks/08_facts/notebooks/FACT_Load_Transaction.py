# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_Transaction
# MAGIC Port of `ssis/08_facts/FACT_Load_Transaction.dtsx` (`build_fact_packages.py::build_fact_load_transaction`).
# MAGIC
# MAGIC * Source `silver.stg_transaction` (`LastModifiedAt` watermark); natural key `TransactionBusinessKey`.
# MAGIC * `Route Ledger Rows`: CUSTOMER party -> customer lookup, SUPPLIER party -> supplier lookup (SCD2 on transaction date, miss -> -1), other party types rejected (`PARTY_TYPE_UNSUPPORTED`); unbalanced rows (excl. tax + tax != total beyond 0.01) rejected (`TRANSACTION_UNBALANCED`).
# MAGIC * Transaction type lookup (type-1, -1).
# MAGIC * `Recalculate Running Balances`: after the MERGE, `running_balance` is recomputed per party as a window sum ordered by date / key.
# MAGIC * Target `gold.fact_transaction` (liquid-clustered `transaction_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_Transaction"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Transaction"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_transaction")))

# COMMAND ----------

REJECT_PARTY = "PARTY_TYPE_UNSUPPORTED"
REJECT_UNBALANCED = "TRANSACTION_UNBALANCED"


def transactionSource(df):
    cls = rules.partyTypeClass(F.col("PartyTypeCode"))
    return (
        df.withColumn("PartyClass", cls)
        .withColumn("CustomerBusinessKey", F.when(cls == "CUSTOMER", F.col("PartyBusinessKey")))
        .withColumn("SupplierBusinessKey", F.when(cls == "SUPPLIER", F.col("PartyBusinessKey")))
    )


SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_transaction",
    sourceTable="stg_transaction", sourceDateCol="TransactionDate", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="TransactionBusinessKey", naturalKeyCols=["TransactionBusinessKey"],
    surrogateKeyCol="transaction_key", dateKeyCol="transaction_date_key",
    sourceFilter=transactionSource,
    validation=lambda df: F.col("TransactionAmount").isNull() | F.col("TransactionDate").isNull(),
    lookups=[
        fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", "TransactionDate", notApplicableWhenNull=True),
        fact_load.LookupSpec("Supplier", "SupplierBusinessKey", "supplier_key", "TransactionDate", notApplicableWhenNull=True),
        fact_load.LookupSpec("Transaction Type", "TransactionTypeCode", "transaction_type_key"),
    ],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    otherParty = ~F.col("PartyClass").isin("CUSTOMER", "SUPPLIER")
    fc.rejectRows(spark, catalog, df.where(otherParty), OBJECT_NAME, REJECT_PARTY, batchId, run.packageExecutionId, businessKeyColumn="TransactionBusinessKey", sourceSystemCode="DW")
    df = df.where(~otherParty)
    unbalanced = F.abs(F.coalesce(F.col("AmountExcludingTax"), F.lit(0)) + F.coalesce(F.col("TaxAmount"), F.lit(0)) - F.col("TransactionAmount")) > 0.01
    fc.rejectRows(spark, catalog, df.where(unbalanced), OBJECT_NAME, REJECT_UNBALANCED, batchId, run.packageExecutionId, businessKeyColumn="TransactionBusinessKey", sourceSystemCode="DW")
    df = df.where(~unbalanced)
    amount = F.col("TransactionAmount")
    return df.select(
        F.col("TransactionDate").cast("date").alias("transaction_date_key"),
        "customer_key", "supplier_key", "transaction_type_key",
        F.col("RegionCode").alias("region_code"),
        F.col("TransactionBusinessKey").alias("wwi_transaction_id"), F.col("PartyBusinessKey").alias("party_business_key"),
        F.col("PartyClass").alias("party_type_code"), F.col("TransactionTypeCode").alias("transaction_type_code"),
        F.col("SourceDocumentNumber").alias("source_document_number"),
        F.col("TransactionCurrency").alias("transaction_currency_code"),
        rules.money(F.col("AmountExcludingTax")).alias("amount_excluding_tax"), rules.money(F.coalesce(F.col("TaxAmount"), F.lit(0))).alias("tax_amount"),
        rules.money(amount).alias("transaction_amount"),
        rules.safeDivide(F.col("TransactionAmountUsd"), amount).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(F.col("TransactionAmountUsd")).alias("transaction_amount_reporting"),
        F.lit(None).cast(rules.MONEY).alias("running_balance"),
        F.col("AccountingPeriodCode").alias("accounting_period_code"),
        F.lit(False).alias("inferred_member_flag"),
        "natural_key_hash",
    )


def recalculateRunningBalances(spark, catalog, factRows, merged):
    from delta.tables import DeltaTable

    fullName = naming.table(catalog, "gold", "fact_transaction")
    parties = factRows.select("party_business_key").distinct()
    fact = spark.table(fullName).join(parties, "party_business_key")
    w = Window.partitionBy("party_business_key").orderBy("transaction_date_key", "transaction_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    balances = fact.select("transaction_key", F.sum("transaction_amount").over(w).cast(rules.MONEY).alias("new_balance"))
    versionBefore = fc.tableVersion(spark, fullName)
    DeltaTable.forName(spark, fullName).alias("t").merge(balances.alias("b"), "t.transaction_key = b.transaction_key").whenMatchedUpdate(
        set={"running_balance": "b.new_balance"}
    ).execute()
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact.Transaction running balances", updateRowCount=fc.lastOperationMetrics(spark, fullName, versionBefore).get("numTargetRowsUpdated", 0))

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform, afterMerge=recalculateRunningBalances)
    print(result)
