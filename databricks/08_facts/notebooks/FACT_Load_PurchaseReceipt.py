# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_PurchaseReceipt
# MAGIC Port of `ssis/08_facts/FACT_Load_PurchaseReceipt.dtsx` (`build_fact_packages.py::build_fact_load_purchase_receipt`).
# MAGIC
# MAGIC * Source `silver.stg_purchase_receipt` (`LastModifiedAt` watermark); natural key `ReceiptBusinessKey`.
# MAGIC * Lookups: supplier (SCD2 on order date, miss -> inferred member), stock item (SCD2, miss -> -1).
# MAGIC * `Update Milestones In Place`: an existing receipt is matched on `wwi_receipt_id` (stable surrogate key) and only the milestone / quantity / amount columns are updated by the MERGE.
# MAGIC * `Recalculate GRNI Accrual`: grni_accrual_amount = received cost - invoiced amount (>= 0), price variance, quantity variance, milestone status.
# MAGIC * Target `gold.fact_purchase_receipt` (liquid-clustered `receipt_date_key`).

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

PACKAGE_NAME = "FACT_Load_PurchaseReceipt"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Purchase Receipt"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_purchase_receipt")))

# COMMAND ----------

MILESTONE_UPDATE_COLS = [
    "goods_received_date_key", "invoice_received_date_key", "payment_settled_date_key", "milestone_status_code",
    "order_to_receipt_days", "receipt_to_invoice_days", "invoice_to_payment_days", "quantity_received_base",
    "invoiced_amount", "grni_accrual_amount", "price_variance_amount", "quantity_variance", "in_full_flag",
    "lineage_key", "batch_id", "package_execution_id", "load_datetime",
]

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_purchase_receipt",
    sourceTable="stg_purchase_receipt", sourceDateCol="OrderRaisedDate", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="ReceiptBusinessKey", naturalKeyCols=["ReceiptBusinessKey"],
    surrogateKeyCol="purchase_receipt_key", dateKeyCol="receipt_date_key", regionCol="RegionCode",
    sourceFilter=lambda df: df.withColumn("RegionCode", F.lit(None).cast("string")) if "RegionCode" not in df.columns else df,
    validation=lambda df: F.col("QuantityOrdered").isNull() | F.col("OrderRaisedDate").isNull(),
    lookups=[
        fact_load.LookupSpec("Supplier", "SupplierBusinessKey", "supplier_key", "OrderRaisedDate", onMiss=fact_load.ON_MISS_INFER),
        fact_load.LookupSpec("Stock Item", "StockItemBusinessKey", "stock_item_key", "OrderRaisedDate"),
    ],
    updateCols=MILESTONE_UPDATE_COLS,
)


def transform(spark, catalog, df):
    received, invoiced = F.coalesce(F.col("ReceivedCostAmount"), F.lit(0)), F.coalesce(F.col("InvoicedAmount"), F.lit(0))
    return df.select(
        F.coalesce(F.col("GoodsReceivedDate"), F.col("OrderRaisedDate")).cast("date").alias("receipt_date_key"),
        F.col("OrderRaisedDate").cast("date").alias("purchase_order_date_key"),
        F.col("GoodsReceivedDate").cast("date").alias("goods_received_date_key"),
        F.col("InvoiceReceivedDate").cast("date").alias("invoice_received_date_key"),
        F.col("PaymentSettledDate").cast("date").alias("payment_settled_date_key"),
        "supplier_key", "stock_item_key",
        F.col("RegionCode").alias("region_code"),
        F.col("ReceiptBusinessKey").alias("wwi_receipt_id"), F.col("ReceiptNumber").alias("receipt_number"),
        F.col("PurchaseOrderNumber").alias("purchase_order_number"), F.col("PurchaseOrderLineNumber").alias("purchase_order_line_number"),
        F.col("TransactionCurrency").alias("transaction_currency_code"),
        F.col("QuantityOrdered").cast("decimal(18,4)").alias("quantity_ordered_base"),
        F.coalesce(F.col("QuantityReceived"), F.lit(0)).cast("decimal(18,4)").alias("quantity_received_base"),
        rules.quantityVariance(F.col("QuantityReceived"), F.col("QuantityOrdered")).alias("quantity_variance"),
        rules.money(received).alias("receipt_value"), rules.money(invoiced).alias("invoiced_amount"),
        rules.safeDivide(F.col("ReceivedCostAmountUsd"), received).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(F.col("ReceivedCostAmountUsd")).alias("receipt_value_reporting"),
        rules.priceVarianceAmount(invoiced, received).alias("price_variance_amount"),
        rules.grniAccrualAmount(received, invoiced).alias("grni_accrual_amount"),
        F.coalesce(F.col("DaysOrderToReceipt"), F.datediff(F.col("GoodsReceivedDate"), F.col("OrderRaisedDate"))).cast("int").alias("order_to_receipt_days"),
        F.coalesce(F.col("DaysReceiptToInvoice"), F.datediff(F.col("InvoiceReceivedDate"), F.col("GoodsReceivedDate"))).cast("int").alias("receipt_to_invoice_days"),
        F.datediff(F.col("PaymentSettledDate"), F.col("InvoiceReceivedDate")).cast("int").alias("invoice_to_payment_days"),
        rules.receiptMilestoneStatus(F.col("GoodsReceivedDate"), F.col("InvoiceReceivedDate"), F.col("PaymentSettledDate")).alias("milestone_status_code"),
        (F.coalesce(F.col("QuantityReceived"), F.lit(0)) >= F.col("QuantityOrdered")).alias("in_full_flag"),
        F.col("supplier_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform)
    print(result)
