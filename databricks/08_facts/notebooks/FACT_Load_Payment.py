# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_Payment
# MAGIC Port of `ssis/08_facts/FACT_Load_Payment.dtsx` (`build_fact_packages.py::build_fact_load_payment`).
# MAGIC
# MAGIC * Source `silver.stg_payment` in the `Fact.Payment` watermark window; natural key `PaymentBusinessKey`.
# MAGIC * Party lookup on `CustomerBusinessKey` when the staging row carries one (legacy generator SQL), otherwise the AP `SupplierBusinessKey` (repo staging DDL) - see mapping doc. Miss -> held (`DIM_NOT_KEYED`) and re-matched next run (`Match Held Payments By Remittance`).
# MAGIC * `Remove Reprocessed Bank Batches`: after the MERGE, rows sharing `bank_reference` + `payment_date_key` keep only the newest `lineage_key` (window == legacy ROW_NUMBER ... DELETE).
# MAGIC * Derived: allocated = payment - unapplied, unallocated, settlement discount, allocation status, realised FX gain/loss, reporting amount.
# MAGIC * Target `gold.fact_payment` (liquid-clustered `payment_date_key, region_code`), MERGE on `payment_key`.

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

PACKAGE_NAME = "FACT_Load_Payment"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Payment"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_payment")))

# COMMAND ----------

def paymentSource(df):
    party = F.col("CustomerBusinessKey") if "CustomerBusinessKey" in df.columns else F.col("SupplierBusinessKey")
    return df.withColumn("PartyBusinessKey", party).withColumn("PaymentDate", F.col("PaymentDate").cast("date"))


SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_payment",
    sourceTable="stg_payment", sourceDateCol="PaymentDate", sourceTimestampCol="LoadedAtUtc",
    businessKeyCol="PaymentBusinessKey", naturalKeyCols=["PaymentBusinessKey"],
    surrogateKeyCol="payment_key", dateKeyCol="payment_date_key",
    sourceFilter=paymentSource,
    validation=lambda df: F.col("PaymentAmount").isNull() | F.col("PaymentDate").isNull() | F.col("VoidDate").isNotNull(),
    rejectReasonCode="PAYMENT_INVALID_OR_VOID",
    lookups=[fact_load.LookupSpec("Customer", "PartyBusinessKey", "customer_key", "PaymentDate", onMiss=fact_load.ON_MISS_HOLD)],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    amount = F.col("PaymentAmount")
    allocated = amount - F.coalesce(F.col("UnappliedAmount"), F.lit(0))
    return df.select(
        F.col("PaymentDate").alias("payment_date_key"),
        F.col("ClearedDate").cast("date").alias("value_date_key"),
        "customer_key", F.col("customer_key").alias("bill_to_customer_key"),
        F.col("RegionCode").alias("region_code"), F.col("LedgerCode").alias("ledger_code"),
        F.col("PaymentNumber").alias("receipt_number"), F.lit(1).alias("receipt_line_number"),
        F.col("RemittanceReference").alias("remittance_reference"), F.col("BankAccountReference").alias("bank_reference"),
        F.col("PaymentMethodCode").alias("payment_method_code"),
        F.col("TransactionCurrencyCode").alias("transaction_currency_code"),
        rules.money(amount).alias("payment_amount"), rules.money(allocated).alias("allocated_amount"),
        rules.paymentUnallocatedAmount(amount, allocated).alias("unallocated_amount"),
        rules.money(F.coalesce(F.col("DiscountTakenAmount"), F.lit(0))).alias("settlement_discount_amount"),
        F.col("TransactionFxRate").cast(rules.RATE).alias("fx_rate_to_reporting"), F.lit("SOURCE").alias("fx_rate_source_code"),
        rules.money(F.coalesce(F.col("PaymentAmountUsd"), rules.reportingAmount(amount, F.col("TransactionFxRate")))).alias("payment_amount_reporting"),
        rules.reportingAmount(allocated, F.col("TransactionFxRate")).alias("allocated_amount_reporting"),
        rules.money(F.coalesce(F.col("RealizedFxGainLossUsd"), F.lit(0))).alias("realised_fx_gain_loss"),
        F.col("AppliedInvoiceCount").alias("applied_invoice_count"),
        rules.paymentAllocationStatus(amount, allocated).alias("allocation_status_code"),
        F.col("PaymentStatusCode").alias("payment_status_code"), F.col("MatchStatusCode").alias("match_status_code"),
        F.lit(1).alias("restatement_version"), F.lit(None).cast("timestamp").alias("restated_datetime"),
        F.col("customer_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )


def removeReprocessedBankBatches(spark, catalog, factRows, merged):
    fullName = naming.table(catalog, "gold", "fact_payment")
    fact = spark.table(fullName).where(F.col("bank_reference").isNotNull())
    w = Window.partitionBy("bank_reference", "payment_date_key").orderBy(F.col("lineage_key").desc(), F.col("payment_key").desc())
    losers = fact.withColumn("rn", F.row_number().over(w)).where(F.col("rn") > 1).select("payment_key")
    if losers.limit(1).count() == 0:
        return
    keys = ",".join(str(r["payment_key"]) for r in losers.collect())
    deleted = fc.deleteWhere(spark, fullName, "payment_key IN (%s)" % keys)
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact.Payment reprocessed bank batches", deleteRowCount=deleted)

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform, afterMerge=removeReprocessedBankBatches)
    print(result)
