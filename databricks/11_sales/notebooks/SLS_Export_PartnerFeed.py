# Databricks notebook source
# MAGIC %md
# MAGIC # SLS_Export_PartnerFeed
# MAGIC Legacy package `ssis/11_sales/SLS_Export_PartnerFeed.dtsx`: outbound partner sales feed.
# MAGIC Builds `work.PartnerFeedRow` from `Fact.Sale` + customer / stock item / partner dimensions and
# MAGIC the settlement FX rate, redacts (or suppresses) EU rows without sharing consent, writes
# MAGIC `partner_feed_YYYYMMDD.csv` (header, comma delimited, CRLF, code page 1252) to the UC Volume
# MAGIC `<VolumeRoot>/outbound/partner_feed/` with an archive copy under
# MAGIC `<VolumeRoot>/archive/partner_feed/yyyy/MM/`, appends the rows to `silver.work_partner_feed_archive`
# MAGIC and logs the export against `file:partner_feed.csv` in the control framework.
# MAGIC
# MAGIC Control flow: Log Package Start -> Truncate work_PartnerFeedRow -> Build Partner Feed Rows ->
# MAGIC Suppress Unconsented EU Rows (conditional) -> Count Feed Rows -> Derive Feed File Name ->
# MAGIC Export Partner Feed (RowsRead > 0) -> Log Row Counts -> Log Package Success.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402

import sales_common as sc  # noqa: E402
import sales_schemas as schemas  # noqa: E402
import sales_partner_feed as feed  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "SLS_Export_PartnerFeed"
ctx = sc.resolveContext(spark, dbutils, params, control, PACKAGE_NAME,
                        (("PartnerScope", "ALL"), ("SuppressUnconsentedEuRows", "True"), ("VolumeRoot", "")))
if sc.shouldSkipForRestart(ctx.restartFromStep, PACKAGE_NAME):
    dbutils.notebook.exit("Skipped: RestartFromStep=%s" % ctx.restartFromStep)

partnerScope = sc.parseOptional(sc.getWidget(dbutils, "PartnerScope", "ALL")) or "ALL"
suppressUnconsented = sc.parseBool(sc.getWidget(dbutils, "SuppressUnconsentedEuRows", "True"), True)
# Legacy $Project::ArchiveFileRoot (C:\WWI\<ENV>\archive) -> UC Volume root.
volumeRoot = sc.parseOptional(sc.getWidget(dbutils, "VolumeRoot")) or "/Volumes/%s/etl/files" % ctx.catalog


def t(schema, table):
    return naming.table(ctx.catalog, schema, table)


# COMMAND ----------

summary = {"package": PACKAGE_NAME, "batchId": ctx.batchId, "partnerScope": partnerScope}
with sc.legacyPackageRun(spark, control, ctx, PACKAGE_NAME) as run:
    pid = run.packageExecutionId
    rowName = t("silver", "work_partner_feed_row")
    archiveName = t("silver", "work_partner_feed_archive")
    sc.ensureTable(spark, rowName, schemas.WORK_PARTNER_FEED_ROW)
    sc.ensureTable(spark, archiveName, schemas.WORK_PARTNER_FEED_ARCHIVE, partitionBy=("BatchId",))

    run.currentTask = "Build Partner Feed Rows"
    factSale = feed.legacyFactSale(spark.table(t("gold", "fact_sale")))
    dimCustomer = feed.legacyDimCustomer(spark.table(t("gold", "dim_customer")))
    dimStockItem = feed.legacyDimStockItem(spark.table(t("gold", "dim_stock_item")))
    dimPartner = feed.legacyDimPartner(spark.table(t("gold", "dim_partner")))
    fxName = t("silver", "work_fx_revaluation_rate")
    fxRates = feed.legacyFxRevaluationRates(spark.table(fxName)) if spark.catalog.tableExists(fxName) else None
    if fxRates is None:
        control.logError(spark, ctx.catalog, packageExecutionId=pid, batchId=ctx.batchId,
                         errorSeverity="Warning", errorCode="FX_REVALUATION_RATE_MISSING",
                         sourceName=PACKAGE_NAME, sourceComponent="Build Partner Feed Rows",
                         errorDescription="%s does not exist; NetAmount left in the transaction currency." % fxName)
    rows = feed.buildPartnerFeedRows(factSale, dimCustomer, dimStockItem, dimPartner, fxRates,
                                     partnerScope, sc.yesterday(ctx.businessDate))
    run.currentTask = "Truncate work_PartnerFeedRow"
    builtRows = sc.overwriteTable(rows.withColumn("BatchId", F.lit(ctx.batchId).cast("long")),
                                  rowName, schemas.WORK_PARTNER_FEED_ROW)

    workRows = spark.table(rowName)
    if suppressUnconsented:
        run.currentTask = "Suppress Unconsented EU Rows"
        kept = feed.suppressUnconsentedEuRows(workRows)
        keptRows = sc.overwriteTable(kept, rowName, schemas.WORK_PARTNER_FEED_ROW)
        run.rowsDeleted = builtRows - keptRows
        run.rowsRejected = run.rowsDeleted
        workRows = spark.table(rowName)

    run.currentTask = "Count Feed Rows"
    run.rowsRead = workRows.count()
    run.currentTask = "Derive Feed File Name"
    fileName = feed.outboundFileName(ctx.businessDate)
    summary["fileName"] = fileName

    if run.rowsRead > 0:
        run.currentTask = "Export Partner Feed"
        ordered = feed.orderedFeedRows(workRows)
        content = feed.renderFeed(ordered.toLocalIterator())
        outboundPath, archivePath = feed.writeFeedFiles(content, volumeRoot, fileName, ctx.businessDate)
        summary.update({"outboundPath": outboundPath, "archivePath": archivePath})
        archiveRows = feed.toArchiveRows(workRows, fileName, ctx.batchId, pid)
        run.rowsInserted = sc.replaceWhere(archiveRows, archiveName, schemas.WORK_PARTNER_FEED_ARCHIVE,
                                           "BatchId = %d" % ctx.batchId)
    else:
        summary["outboundPath"] = None

    run.currentTask = "Log Row Counts"
    control.logRowCount(spark, ctx.catalog, pid, feed.FEED_OBJECT_NAME, sourceRowCount=builtRows,
                        targetRowCount=run.rowsInserted, deleteRowCount=run.rowsDeleted,
                        rejectRowCount=run.rowsRejected)
    control.logRowCount(spark, ctx.catalog, pid, "work.PartnerFeedArchive", sourceRowCount=run.rowsRead,
                        targetRowCount=run.rowsInserted)
    run.currentTask = "Log Package Success"
    summary.update({"status": "Succeeded", "rowsRead": run.rowsRead, "rowsInserted": run.rowsInserted,
                    "rowsRejected": run.rowsRejected})

print(summary)

# COMMAND ----------

dbutils.notebook.exit(str(summary))
