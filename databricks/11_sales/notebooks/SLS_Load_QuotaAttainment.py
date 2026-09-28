# Databricks notebook source
# MAGIC %md
# MAGIC # SLS_Load_QuotaAttainment
# MAGIC Legacy package `ssis/11_sales/SLS_Load_QuotaAttainment.dtsx`: quota attainment by territory
# MAGIC for one period. NA measures invoiced revenue (incl. tax), EU net revenue after credit notes,
# MAGIC APAC order intake. Target: `Aggregate.Regional Sales Performance` -> `gold.agg_regional_sales_performance`.
# MAGIC
# MAGIC Control flow: Log Package Start -> Truncate work_QuotaAttainment -> Check Territories Without Quota
# MAGIC -> Build Attainment By Region -> Load Regional Performance (Derive Attainment Metrics) ->
# MAGIC Log Row Counts -> Log Package Success.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402

import sales_common as sc  # noqa: E402
import sales_schemas as schemas  # noqa: E402
import sales_quota as quota  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "SLS_Load_QuotaAttainment"
ctx = sc.resolveContext(spark, dbutils, params, control, PACKAGE_NAME,
                        (("AttainmentPeriod", ""), ("IncludePartialPeriod", "True")))
if sc.shouldSkipForRestart(ctx.restartFromStep, PACKAGE_NAME):
    dbutils.notebook.exit("Skipped: RestartFromStep=%s" % ctx.restartFromStep)

attainmentPeriod = sc.parseOptional(sc.getWidget(dbutils, "AttainmentPeriod")) or sc.monthPeriod(ctx.businessDate)
# IncludePartialPeriod is declared by the legacy package but never referenced by any task
# or expression; it is accepted for parity and has no effect (see mapping doc).
includePartialPeriod = sc.parseBool(sc.getWidget(dbutils, "IncludePartialPeriod", "True"), True)


def t(schema, table):
    return naming.table(ctx.catalog, schema, table)


# COMMAND ----------

summary = {"package": PACKAGE_NAME, "batchId": ctx.batchId, "attainmentPeriod": attainmentPeriod}
with sc.legacyPackageRun(spark, control, ctx, PACKAGE_NAME) as run:
    pid = run.packageExecutionId
    workName = t("silver", "work_quota_attainment")
    targetName = t("gold", "agg_regional_sales_performance")
    sc.ensureTable(spark, workName, schemas.WORK_QUOTA_ATTAINMENT)
    sc.ensureTable(spark, targetName, schemas.AGG_REGIONAL_SALES_PERFORMANCE, partitionBy=("RegionCode",))

    run.currentTask = "Check Territories Without Quota"
    territories = quota.legacyTerritories(spark.table(t("silver", "stg_sales_territory")))
    quotas = quota.legacyQuotas(spark.table(t("silver", "stg_sales_quota")))
    summary["missingQuotaCount"] = quota.countTerritoriesWithoutQuota(territories, quotas, attainmentPeriod)

    run.currentTask = "Build Attainment By Region"
    saleLines = sc.batchFilter(
        sc.resolveColumns(spark.table(t("silver", "stg_sale_line")),
                          {**quota.SALE_LINE_COLUMNS, "LoadBatchId": ["BatchId", "LoadBatchId"]}),
        "LoadBatchId", ctx.batchId, ctx.reloadFullHistory)
    creditNotes = quota.legacyCreditNotes(spark.table(t("silver", "stg_credit_note")))
    orderLines = quota.legacyOrderLines(spark.table(t("silver", "stg_order_line")))
    fiscalCalendar = quota.legacyFiscalCalendar(spark.table(t("silver", "stg_fiscal_calendar445")))
    work = quota.buildAttainmentByRegion(territories, quotas, saleLines, creditNotes, orderLines,
                                         fiscalCalendar, attainmentPeriod)
    work = work.withColumn("BatchId", F.lit(ctx.batchId).cast("long"))
    run.currentTask = "Truncate work_QuotaAttainment"
    run.rowsRead = sc.overwriteTable(work, workName, schemas.WORK_QUOTA_ATTAINMENT)

    run.currentTask = "Load Regional Performance"
    metrics = quota.deriveAttainmentMetrics(spark.table(workName))
    target = quota.toRegionalSalesPerformance(metrics, ctx.batchId)
    mergeMetrics = sc.mergeInto(spark, target, targetName, schemas.AGG_REGIONAL_SALES_PERFORMANCE,
                                ("RegionCode", "TerritoryCode", "QuotaPeriod"))
    run.rowsInserted = int(mergeMetrics["num_affected_rows"] if mergeMetrics["num_affected_rows"] is not None
                           else run.rowsRead)

    run.currentTask = "Log Row Counts"
    control.logRowCount(spark, ctx.catalog, pid, "Aggregate.Regional Sales Performance",
                        sourceRowCount=run.rowsRead, targetRowCount=run.rowsInserted,
                        insertRowCount=mergeMetrics["num_inserted_rows"],
                        updateRowCount=mergeMetrics["num_updated_rows"], rejectRowCount=run.rowsRejected)
    run.currentTask = "Log Package Success"
    summary.update({"status": "Succeeded", "rowsRead": run.rowsRead, "rowsInserted": run.rowsInserted})

print(summary)

# COMMAND ----------

dbutils.notebook.exit(str(summary))
