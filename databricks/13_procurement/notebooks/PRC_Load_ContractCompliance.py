# Databricks notebook source
# MAGIC %md
# MAGIC # PRC_Load_ContractCompliance
# MAGIC Migrated from `ssis/13_procurement/PRC_Load_ContractCompliance.dtsx` (WWI_Procurement).
# MAGIC
# MAGIC Classifies purchase order lines in the `ComplianceWindowDays` window as COMPLIANT /
# MAGIC PRICE_LEAKAGE / EXPIRED_CONTRACT / NON_PREFERRED / NO_CONTRACT, measures leakage, publishes the
# MAGIC per-supplier compliance figures onto `gold.agg_supplier_performance` (rows produced by
# MAGIC PRC_Load_SupplierScorecard for the same month) and raises non-compliant lines as
# MAGIC `etl.rejected_record` rows.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402

from procurement_lib import PROJECT_NAME, SOURCE_SYSTEM_CODE  # noqa: E402
from procurement_lib import contract_compliance as cc  # noqa: E402
from procurement_lib import delta_io  # noqa: E402
from procurement_lib import purchase_spend as ps  # noqa: E402
from procurement_lib.common import parseInt  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "PRC_Load_ContractCompliance"
STEP_NAME = "Procurement Mart"

for name, default in (("BatchId", "0"), ("BusinessDate", ""), ("ReloadFullHistory", "False"), ("EnvironmentCode", "DEV"),
                      ("RestartFromStep", ""), ("catalog", ""), ("ComplianceWindowDays", "30"), ("PriceLeakageTolerance", "2")):
    dbutils.widgets.text(name, default)

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
complianceWindowDays = parseInt(dbutils.widgets.get("ComplianceWindowDays"), 30)
priceLeakageTolerance = parseInt(dbutils.widgets.get("PriceLeakageTolerance"), 2)

stgPurchaseOrderLine = naming.table(catalog, "silver", "stg_purchase_order_line")
stgPurchaseOrder = naming.table(catalog, "silver", "stg_purchase_order")
stgVendorContract = naming.table(catalog, "silver", "stg_vendor_contract")
dimSupplier = naming.table(catalog, "gold", "dim_supplier")
workContractCompliance = naming.table(catalog, "silver", "work_contract_compliance")
aggSupplierPerformance = naming.table(catalog, "gold", "agg_supplier_performance")

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName=STEP_NAME) as run:
    # "Evaluate Contract Compliance" (Execute SQL into work.ContractCompliance)
    work = cc.evaluateContractCompliance(
        spark.table(stgPurchaseOrderLine), spark.table(stgPurchaseOrder), spark.table(stgVendorContract),
        businessDate, complianceWindowDays, priceLeakageTolerance,
    ).cache()
    delta_io.overwriteTable(work, workContractCompliance)
    rowsRead = delta_io.countWhere(spark, workContractCompliance)

    # "Measure Leakage"
    leakageAmount, expiredContractCount = cc.measureLeakage(work)

    # "Publish Compliance To Scorecard" (precedence: LeakageAmount > 0 || ExpiredContractCount > 0)
    rowsUpdated = 0
    if (leakageAmount or 0) > 0 or expiredContractCount > 0:
        supplierKeys = ps.currentSupplierKeys(spark.table(dimSupplier))
        summary = (
            cc.summariseComplianceBySupplier(work).join(supplierKeys, "SupplierId", "inner")
            .select(
                F.lit(businessDate.replace(day=1)).alias("calendar_month"),
                F.col("SupplierKey").alias("supplier_key"),
                F.col("OffContractAmount").alias("off_contract_spend_amount"),
                F.col("LeakageAmount").alias("leakage_amount"),
                F.col("CompliancePercent").alias("compliance_percent"),
                F.lit(int(batchId)).cast("bigint").alias("refresh_batch_id"),
                F.current_timestamp().alias("refreshed_datetime"),
            )
        )
        rowsUpdated = summary.count()
        delta_io.updateMatched(spark, summary, aggSupplierPerformance, keyColumns=["calendar_month", "supplier_key"],
                               updateColumns=["off_contract_spend_amount", "leakage_amount", "compliance_percent",
                                              "refresh_batch_id", "refreshed_datetime"])

    # "Raise Non Compliant Lines" -> etl.rejected_record
    rejects = cc.nonCompliantLines(work).withColumn("RejectReasonCode", F.col("ComplianceStatusCode"))
    rowsRejected = control.logRejectedRecordSet(
        spark, catalog, cc.REJECT_OBJECT_NAME, rejects, batchId=batchId,
        packageExecutionId=run.packageExecutionId, sourceSystemCode=SOURCE_SYSTEM_CODE,
        rejectStage="Mart", rejectReasonCode="NON_COMPLIANT", businessKeyColumn="BusinessKey",
    )
    print("LeakageAmount=%s ExpiredContractCount=%s RowsRejected=%s" % (leakageAmount, expiredContractCount, rowsRejected))

    # "Log Row Counts"
    control.logRowCount(spark, catalog, run.packageExecutionId, "Aggregate.Supplier Performance",
                        sourceRowCount=rowsRead, targetRowCount=rowsUpdated, insertRowCount=0,
                        updateRowCount=rowsUpdated, rejectRowCount=rowsRejected)
    run.rowsRead = rowsRead
    run.rowsInserted = 0
    run.rowsUpdated = rowsUpdated
    run.rowsRejected = rowsRejected
    work.unpersist()

dbutils.notebook.exit("%s: rowsRead=%s rowsUpdated=%s rowsRejected=%s leakage=%s"
                      % (PACKAGE_NAME, rowsRead, rowsUpdated, rowsRejected, leakageAmount))
