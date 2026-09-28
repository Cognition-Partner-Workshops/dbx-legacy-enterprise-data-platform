# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_SupplierPayment
# MAGIC Port of `ssis/08_facts/FACT_Load_SupplierPayment.dtsx` (`build_fact_packages.py::build_fact_load_supplier_payment`).
# MAGIC
# MAGIC * Source `silver.stg_supplier_payment` (`LastModifiedAt` watermark); natural key `SupplierPaymentBusinessKey`.
# MAGIC * Supplier lookup (SCD2 on settlement date): the legacy split routes an unknown supplier to `err.RejectedLookupFailure`, so misses are rejected (`SUPPLIER_NOT_FOUND`) instead of becoming -1.
# MAGIC * `Post FX Revaluation Entries`: settlement-date CLOSE rate from `silver.stg_fx_rate` vs the invoice-date rate gives `realised_fx_gain_loss`; source figure used when the rate is missing.
# MAGIC * Derived: early settlement discount, discount captured flag, days paid early/late (settlement - invoice date), reporting amount.
# MAGIC * Target `gold.fact_supplier_payment` (liquid-clustered `payment_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_SupplierPayment"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Supplier Payment"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_supplier_payment")))

# COMMAND ----------

REJECT_SUPPLIER_NOT_FOUND = "SUPPLIER_NOT_FOUND"

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_supplier_payment",
    sourceTable="stg_supplier_payment", sourceDateCol="SettlementDate", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="SupplierPaymentBusinessKey", naturalKeyCols=["SupplierPaymentBusinessKey"],
    surrogateKeyCol="supplier_payment_key", dateKeyCol="payment_date_key",
    validation=lambda df: F.col("SettledAmount").isNull() | F.col("SettlementDate").isNull(),
    lookups=[fact_load.LookupSpec("Supplier", "SupplierBusinessKey", "supplier_key", "SettlementDate")],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    unknown = df.where(F.col("supplier_key_miss"))
    fc.rejectRows(spark, catalog, unknown, OBJECT_NAME, REJECT_SUPPLIER_NOT_FOUND, batchId, run.packageExecutionId, businessKeyColumn="SupplierPaymentBusinessKey", sourceSystemCode="DW")
    df = df.where(~F.col("supplier_key_miss"))

    rates = fc.fxRates(spark, catalog, "CLOSE")
    df = fc.lookupEffectiveFxRate(df, rates, "TransactionCurrency", "SettlementDate", rateCol="SettlementFxRate", sourceCol="SettlementFxSource", rateDateCol="SettlementFxDate")
    df = fc.lookupEffectiveFxRate(df, rates, "TransactionCurrency", "InvoiceDate", rateCol="InvoiceFxRate", sourceCol="InvoiceFxSource", rateDateCol="InvoiceFxDate")

    invoice, settled, discount = F.col("InvoiceAmount"), F.col("SettledAmount"), F.col("SettlementDiscountAmount")
    earlyDiscount = rules.earlySettlementDiscount(invoice, settled, discount)
    return df.select(
        F.col("SettlementDate").cast("date").alias("payment_date_key"),
        F.col("InvoiceDate").cast("date").alias("supplier_invoice_date_key"),
        "supplier_key",
        F.col("RegionCode").alias("region_code"),
        F.col("PaymentRunCode").alias("payment_run_reference"), F.col("PaymentReference").alias("payment_reference"),
        F.col("SupplierInvoiceNumber").alias("supplier_invoice_number"), F.col("PaymentMethodCode").alias("payment_method_code"),
        F.col("TransactionCurrency").alias("transaction_currency_code"),
        rules.money(invoice).alias("invoice_amount"), rules.money(settled).alias("gross_payment_amount"),
        earlyDiscount.alias("early_settlement_discount"),
        rules.money(settled - earlyDiscount).alias("net_payment_amount"),
        F.col("SettlementFxRate").cast(rules.RATE).alias("fx_rate_to_reporting"), F.col("SettlementFxSource").alias("fx_rate_source_code"),
        rules.money(F.coalesce(F.col("SettledAmountUsd"), rules.reportingAmount(settled, F.col("SettlementFxRate")))).alias("net_payment_amount_reporting"),
        F.coalesce(rules.realizedFxGainLoss(settled, F.col("InvoiceFxRate"), F.col("SettlementFxRate")), rules.money(F.col("RealizedFxGainLossUsd"))).alias("realised_fx_gain_loss"),
        F.datediff(F.col("SettlementDate"), F.col("InvoiceDate")).cast("int").alias("days_paid_early_or_late"),
        (F.coalesce(discount, F.lit(0)) > 0).alias("discount_captured_flag"),
        F.col("MatchStatusCode").alias("match_status_code"),
        F.lit(False).alias("inferred_member_flag"),
        "natural_key_hash",
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform)
    print(result)
