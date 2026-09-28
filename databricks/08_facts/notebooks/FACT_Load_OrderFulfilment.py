# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_OrderFulfilment
# MAGIC Port of `ssis/08_facts/FACT_Load_OrderFulfilment.dtsx` (`build_fact_packages.py::build_fact_load_order_fulfilment`).
# MAGIC
# MAGIC * Source `silver.stg_order_fulfilment` (`LastModifiedAt` watermark); natural key `OrderNumber` (one row per order pipeline).
# MAGIC * Lookups: customer (SCD2 on order date, miss -> inferred member).
# MAGIC * `Update Fulfilment Milestones In Place`: existing orders only get milestone timestamps, lags, uninvoiced / uncollected amounts and the stalled flag refreshed.
# MAGIC * `Escalate Stalled Orders`: `is_stalled` when no cash has been received and the order is older than `StalledOrderDays` (package default 14, regional NA 7 / EU 10 / APAC 14, override via configuration `Fact.OrderFulfilment.StalledOrderDays`).
# MAGIC * Target `gold.fact_order_fulfilment` (liquid-clustered `order_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_OrderFulfilment"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Order Fulfilment"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_order_fulfilment")))

# COMMAND ----------

MILESTONE_UPDATE_COLS = [
    "allocation_date_key", "pick_date_key", "invoice_date_key", "cash_applied_date_key", "allocated_at", "picked_at", "invoiced_at", "cash_received_at",
    "milestone_status_code", "order_to_allocate_hours", "order_to_invoice_days", "invoice_to_cash_days", "order_to_cash_cycle_days",
    "invoiced_value_reporting", "cash_applied_reporting", "uninvoiced_amount", "uncollected_amount", "stalled_flag", "cycle_complete_flag",
    "pipeline_status_code", "lineage_key", "batch_id", "package_execution_id", "load_datetime",
]

stalledOrderDays = int(fc.configurationValue(spark, catalog, "Fact.OrderFulfilment.StalledOrderDays", p["environmentCode"], "14"))

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_order_fulfilment",
    sourceTable="stg_order_fulfilment", sourceDateCol="OrderDateOnly", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="OrderNumber", naturalKeyCols=["OrderNumber"],
    surrogateKeyCol="order_fulfilment_key", dateKeyCol="order_date_key",
    sourceFilter=lambda df: df.withColumn("OrderDateOnly", F.col("OrderedAt").cast("date")),
    validation=lambda df: F.col("OrderedAt").isNull() | F.col("OrderNumber").isNull(),
    lookups=[fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", "OrderDateOnly", onMiss=fact_load.ON_MISS_INFER)],
    updateCols=MILESTONE_UPDATE_COLS,
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    ordered, allocated, picked, invoiced, cash = (F.col(c) for c in ("OrderedAt", "AllocatedAt", "PickedAt", "InvoicedAt", "CashReceivedAt"))
    orderNet, invoicedAmt, cashAmt = F.col("OrderNetAmount"), F.coalesce(F.col("InvoicedAmount"), F.lit(0)), F.coalesce(F.col("CashReceivedAmount"), F.lit(0))
    asOf = F.lit(p["businessDate"])
    genericStalled = cash.isNull() & (F.datediff(asOf, ordered.cast("date")) > F.lit(stalledOrderDays))
    return df.select(
        F.col("OrderDateOnly").alias("order_date_key"),
        allocated.cast("date").alias("allocation_date_key"), picked.cast("date").alias("pick_date_key"),
        invoiced.cast("date").alias("invoice_date_key"), cash.cast("date").alias("cash_applied_date_key"),
        "customer_key",
        F.col("RegionCode").alias("region_code"),
        F.col("OrderNumber").alias("order_number"), F.col("OrderBusinessKey").alias("order_business_key"),
        ordered.alias("ordered_at"), allocated.alias("allocated_at"), picked.alias("picked_at"), invoiced.alias("invoiced_at"), cash.alias("cash_received_at"),
        F.col("TransactionCurrency").alias("transaction_currency_code"),
        rules.money(orderNet).alias("order_value"), rules.money(invoicedAmt).alias("invoiced_value"), rules.money(cashAmt).alias("cash_applied"),
        rules.safeDivide(F.col("OrderNetAmountUsd"), orderNet).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(F.col("OrderNetAmountUsd")).alias("order_value_reporting"),
        rules.reportingAmount(invoicedAmt, rules.safeDivide(F.col("OrderNetAmountUsd"), orderNet)).alias("invoiced_value_reporting"),
        rules.reportingAmount(cashAmt, rules.safeDivide(F.col("OrderNetAmountUsd"), orderNet)).alias("cash_applied_reporting"),
        rules.money(orderNet - invoicedAmt).alias("uninvoiced_amount"),
        rules.money(invoicedAmt - cashAmt).alias("uncollected_amount"),
        rules.latencyHours(ordered, allocated).alias("order_to_allocate_hours"),
        F.coalesce(F.col("DaysOrderToInvoice"), F.datediff(invoiced, ordered)).cast("int").alias("order_to_invoice_days"),
        F.coalesce(F.col("DaysInvoiceToCash"), F.datediff(cash, invoiced)).cast("int").alias("invoice_to_cash_days"),
        F.datediff(cash, ordered).cast("int").alias("order_to_cash_cycle_days"),
        rules.fulfilmentMilestoneStatus(allocated, picked, invoiced, cash).alias("milestone_status_code"),
        F.col("FulfilmentStatusCode").alias("pipeline_status_code"),
        (rules.isStalledOrder(F.col("RegionCode"), ordered, invoiced, asOf) | genericStalled).alias("stalled_flag"),
        cash.isNotNull().alias("cycle_complete_flag"),
        F.col("customer_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform)
    print(result)
