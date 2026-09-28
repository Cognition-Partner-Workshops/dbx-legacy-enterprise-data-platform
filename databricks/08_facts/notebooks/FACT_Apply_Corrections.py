# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Apply_Corrections
# MAGIC Port of `ssis/08_facts/FACT_Apply_Corrections.dtsx` (`build_fact_apply_corrections`) and `Integration.usp_ApplyFactCorrections`.
# MAGIC
# MAGIC Approved, unapplied requests from `silver.stg_fact_correction_request` are applied in `RequestId` order:
# MAGIC * `Fact.Sale` -> insert a negated `REV` row for each `ORIG` row matching `InvoiceNumber|InvoiceLineNumber`, then a `RES` row at the
# MAGIC   corrected quantity / gross amount (net = gross - discount, margin = net - cost, reporting = round(net * fx, 2)).
# MAGIC * `Fact.Payment` -> in-place update: allocated amount, unallocated = payment - allocated, restatement version + 1, restated datetime, batch/load metadata.
# MAGIC * `Fact.Order` -> in-place update: quantity ordered, net order amount, net order amount reporting = round(net * COALESCE(fx, 1), 2), batch/load metadata.
# MAGIC * Any other target -> `CORR_FACT_UNSUPPORTED` rejected record; every request is stamped `AppliedDatetime` / `AppliedBatchId`.
# MAGIC The legacy cursor is replaced by set-based Delta operations per target, executed in request order per target so that
# MAGIC repeated requests for the same key compose the same way (last request wins on the in-place updates).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import Window
from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import fact_common as fc
import fact_rules as rules
import fact_sale

# COMMAND ----------

PACKAGE_NAME = "FACT_Apply_Corrections"
STEP_NAME = "ApplyFactCorrections"
REQUEST_TABLE = "stg_fact_correction_request"
REJECT_UNSUPPORTED = "CORR_FACT_UNSUPPORTED"
SUPPORTED_TARGETS = ("Fact.Sale", "Fact.Payment", "Fact.Order")

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
factNameFilter = fc.configurationValue(spark, catalog, "Fact.Corrections.FactName", p["environmentCode"], "") or None

# COMMAND ----------


def pendingRequests(spark, catalog, factNameFilter):
    df = fc.readTable(spark, catalog, "silver", REQUEST_TABLE).where(
        (F.col("RequestStatusCode") == "APPROVED") & F.col("AppliedDatetime").isNull()
    )
    if factNameFilter:
        df = df.where(F.col("TargetFactName") == factNameFilter)
    return df.select("RequestId", "TargetFactName", "NaturalKeyValue", "CorrectionTypeCode", "NewAmount", "NewQuantity", "ReasonCode").orderBy("RequestId")


def latestPerKey(requests):
    """In-place targets: the cursor applied requests in RequestId order, so the last request for a key is what remains."""
    w = Window.partitionBy("NaturalKeyValue").orderBy(F.col("RequestId").desc())
    return requests.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def applySaleCorrections(spark, catalog, requests, batchId, packageExecutionId):
    factName = naming.table(catalog, "gold", fact_sale.FACT_TABLE)
    if not fc.tableExists(spark, factName) or requests.limit(1).count() == 0:
        return 0
    sale = spark.table(factName).where(F.coalesce(F.col("correction_type_code"), F.lit(fc.CORRECTION_ORIGINAL)) == fc.CORRECTION_ORIGINAL)
    sale = sale.withColumn("_nk", F.concat(F.col("invoice_number").cast("string"), F.lit("|"), F.col("invoice_line_number").cast("string")))
    matched = sale.join(requests.withColumnRenamed("NaturalKeyValue", "_nk"), "_nk", "inner")
    if matched.limit(1).count() == 0:
        return 0
    passthrough = [c for c in fact_sale.FACT_COLUMNS if c not in fact_sale.MEASURE_COLUMNS and c not in ("sale_key", "correction_type_code", "corrected_sale_key", "batch_id", "package_execution_id", "load_datetime")]
    audit = [F.lit(batchId).cast("bigint").alias("batch_id"), F.lit(packageExecutionId).cast("bigint").alias("package_execution_id"), F.current_timestamp().alias("load_datetime")]

    reversal = matched.select(
        *[F.col(c) for c in passthrough],
        *[(F.col(c) * -1).cast("decimal(18,4)").alias(c) if c in ("quantity", "quantity_base_uom") else rules.negated(F.col(c)).alias(c) for c in fact_sale.MEASURE_COLUMNS],
        F.lit(fc.CORRECTION_REVERSAL).alias("correction_type_code"),
        F.col("sale_key").alias("corrected_sale_key"),
        *audit,
        F.col("RequestId"),
    )
    newGross = F.coalesce(F.col("NewAmount"), F.col("gross_amount"))
    newQty = F.coalesce(F.col("NewQuantity"), F.col("quantity"))
    newNet = rules.money(newGross - F.coalesce(F.col("line_discount_amount"), F.lit(0)))
    newMargin = rules.money(newNet - F.coalesce(F.col("cost_of_sale_amount"), F.lit(0)))
    restated = matched.select(
        *[F.col(c) for c in passthrough],
        newQty.cast("decimal(18,4)").alias("quantity"),
        F.coalesce(F.col("NewQuantity"), F.col("quantity_base_uom")).cast("decimal(18,4)").alias("quantity_base_uom"),
        rules.money(newGross).alias("gross_amount"),
        F.col("line_discount_amount"),
        newNet.alias("net_amount"),
        F.col("tax_amount"),
        newNet.alias("total_excluding_tax"),
        rules.money(newNet + F.coalesce(F.col("tax_amount"), F.lit(0))).alias("total_including_tax"),
        newMargin.alias("profit"),
        F.col("freight_amount"),
        F.col("cost_of_sale_amount"),
        newMargin.alias("gross_margin_amount"),
        rules.money(newNet * F.coalesce(F.col("fx_rate_to_reporting"), F.lit(1))).alias("net_amount_reporting"),
        rules.money(newNet * F.coalesce(F.col("fx_rate_to_reporting"), F.lit(1))).alias("total_excluding_tax_reporting"),
        F.col("tax_amount_reporting"),
        F.lit(fc.CORRECTION_RESTATEMENT).alias("correction_type_code"),
        F.col("sale_key").alias("corrected_sale_key"),
        *audit,
        F.col("RequestId"),
    )
    rows = reversal.unionByName(restated)
    rows = fc.assignSurrogateKeys(spark, factName, rows, "sale_key", ["RequestId", "corrected_sale_key", "correction_type_code"]).drop("RequestId")
    return fc.appendRows(spark, factName, rows.select(*fact_sale.FACT_COLUMNS))


def applyPaymentCorrections(spark, catalog, requests, batchId, packageExecutionId):
    factName = naming.table(catalog, "gold", "fact_payment")
    if not fc.tableExists(spark, factName) or requests.limit(1).count() == 0:
        return 0
    from delta.tables import DeltaTable

    src = latestPerKey(requests).select("NaturalKeyValue", "NewAmount")
    target = DeltaTable.forName(spark, factName)
    versionBefore = fc.tableVersion(spark, factName)
    target.alias("t").merge(
        src.alias("s"), "concat(cast(t.receipt_number as string), '|', cast(t.receipt_line_number as string)) = s.NaturalKeyValue"
    ).whenMatchedUpdate(set={
        "allocated_amount": "coalesce(s.NewAmount, t.allocated_amount)",
        "unallocated_amount": "t.payment_amount - coalesce(s.NewAmount, t.allocated_amount)",
        "restatement_version": "coalesce(t.restatement_version, 1) + 1",
        "restated_datetime": "current_timestamp()",
        "batch_id": "cast(%d as bigint)" % batchId,
        "package_execution_id": "cast(%d as bigint)" % packageExecutionId,
        "load_datetime": "current_timestamp()",
    }).execute()
    return fc.lastOperationMetrics(spark, factName, versionBefore).get("numTargetRowsUpdated", 0)


def applyOrderCorrections(spark, catalog, requests, batchId, packageExecutionId):
    factName = naming.table(catalog, "gold", "fact_order")
    if not fc.tableExists(spark, factName) or requests.limit(1).count() == 0:
        return 0
    from delta.tables import DeltaTable

    src = latestPerKey(requests).select("NaturalKeyValue", "NewAmount", "NewQuantity")
    target = DeltaTable.forName(spark, factName)
    versionBefore = fc.tableVersion(spark, factName)
    target.alias("t").merge(
        src.alias("s"), "concat(cast(t.order_number as string), '|', cast(t.order_line_number as string)) = s.NaturalKeyValue"
    ).whenMatchedUpdate(set={
        "quantity_ordered": "coalesce(s.NewQuantity, t.quantity_ordered)",
        "net_order_amount": "coalesce(s.NewAmount, t.net_order_amount)",
        "net_order_amount_reporting": "round(coalesce(s.NewAmount, t.net_order_amount) * coalesce(t.fx_rate_to_reporting, 1), 2)",
        "batch_id": "cast(%d as bigint)" % batchId,
        "package_execution_id": "cast(%d as bigint)" % packageExecutionId,
        "load_datetime": "current_timestamp()",
    }).execute()
    return fc.lastOperationMetrics(spark, factName, versionBefore).get("numTargetRowsUpdated", 0)


def markApplied(spark, catalog, requests, batchId):
    from delta.tables import DeltaTable

    fullName = naming.table(catalog, "silver", REQUEST_TABLE)
    versionBefore = fc.tableVersion(spark, fullName)
    DeltaTable.forName(spark, fullName).alias("t").merge(requests.select("RequestId").alias("s"), "t.RequestId = s.RequestId").whenMatchedUpdate(set={
        "AppliedDatetime": "current_timestamp()", "AppliedBatchId": "cast(%d as bigint)" % batchId
    }).execute()
    return fc.lastOperationMetrics(spark, fullName, versionBefore).get("numTargetRowsUpdated", 0)


# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    requests = pendingRequests(spark, catalog, factNameFilter).cache()
    sourceRowCount = requests.count()

    inserted = applySaleCorrections(spark, catalog, requests.where(F.col("TargetFactName") == "Fact.Sale"), batchId, run.packageExecutionId)
    updated = applyPaymentCorrections(spark, catalog, requests.where(F.col("TargetFactName") == "Fact.Payment"), batchId, run.packageExecutionId)
    updated += applyOrderCorrections(spark, catalog, requests.where(F.col("TargetFactName") == "Fact.Order"), batchId, run.packageExecutionId)

    unsupported = requests.where(~F.col("TargetFactName").isin(*SUPPORTED_TARGETS))
    rejected = 0
    for row in unsupported.select("TargetFactName", "NaturalKeyValue").collect():
        control.logRejectedRecord(spark, catalog, row["TargetFactName"], REJECT_UNSUPPORTED, packageExecutionId=run.packageExecutionId, batchId=batchId,
                                  sourceSystemCode="DW", businessKey=row["NaturalKeyValue"], rejectReason="No correction pattern is defined for this fact", rejectStage="Correction")
        rejected += 1

    applied = markApplied(spark, catalog, requests, batchId) if sourceRowCount else 0
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact corrections", sourceRowCount=sourceRowCount, insertRowCount=inserted, updateRowCount=updated, rejectRowCount=rejected)
    run.rowsRead, run.rowsInserted, run.rowsUpdated, run.rowsRejected = sourceRowCount, inserted, updated, rejected
    requests.unpersist()
    print({"requests": sourceRowCount, "saleRowsInserted": inserted, "inPlaceUpdates": updated, "unsupported": rejected, "markedApplied": applied})
