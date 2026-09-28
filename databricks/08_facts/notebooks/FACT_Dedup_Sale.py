# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Dedup_Sale
# MAGIC Port of `ssis/08_facts/FACT_Dedup_Sale.dtsx` (`build_fact_dedup_sale`) and `Integration.usp_DeduplicateFactSale`.
# MAGIC
# MAGIC * Window: `[BusinessDate - DedupWindowDays, BusinessDate]` on `invoice_date_key` (default 90 days, `Fact.Sale.DedupWindowDays`).
# MAGIC * Only original rows compete: `COALESCE(correction_type_code, 'ORIG') = 'ORIG'`.
# MAGIC * Ranking (Spark window == legacy `ROW_NUMBER`): `PARTITION BY natural_key_hash ORDER BY COALESCE(source_row_version, 0) DESC, sale_key DESC`.
# MAGIC * Losers are archived to `gold.fact_sale_duplicate_archive` **before** they are deleted from `gold.fact_sale`, and logged as one
# MAGIC   `DUPLICATE_NATURAL_KEY` rejected-record set grouped by natural key.
# MAGIC * `ReportOnly` (package parameter -> configuration `Fact.Sale.DedupReportOnly`) archives + logs but does not delete.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from datetime import timedelta

from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import fact_common as fc

# COMMAND ----------

PACKAGE_NAME = "FACT_Dedup_Sale"
STEP_NAME = "Deduplicate Facts"
OBJECT_NAME = "Fact.Sale"
ARCHIVE_TABLE = "fact_sale_duplicate_archive"
REJECT_DUPLICATE = "DUPLICATE_NATURAL_KEY"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
factName = naming.table(catalog, "gold", "fact_sale")
archiveName = naming.table(catalog, "gold", ARCHIVE_TABLE)

# COMMAND ----------


def dedupWindow(spark, catalog, p):
    days = int(fc.configurationValue(spark, catalog, "Fact.Sale.DedupWindowDays", p["environmentCode"], "90"))
    reportOnly = fc.configurationValue(spark, catalog, "Fact.Sale.DedupReportOnly", p["environmentCode"], "False").strip().lower() in ("1", "true", "yes")
    return p["businessDate"] - timedelta(days=days), p["businessDate"], reportOnly


def duplicateLosers(spark, factName, loadStart, loadEnd):
    inWindow = spark.table(factName).where(F.col("invoice_date_key").between(F.lit(loadStart), F.lit(loadEnd)))
    return fc.saleDuplicateRank(inWindow).where(F.col("duplicate_rank") > 1)


# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    loadStart, loadEnd, reportOnly = dedupWindow(spark, catalog, p)
    if not fc.tableExists(spark, factName):
        raise RuntimeError("%s does not exist; run the regional sale loads first" % factName)

    losers = duplicateLosers(spark, factName, loadStart, loadEnd).cache()
    duplicateCount = losers.count()
    groups = losers.select("natural_key_hash").distinct().count()

    archive = (
        losers.drop("duplicate_rank")
        .withColumn("archived_batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("archived_package_execution_id", F.lit(run.packageExecutionId).cast("bigint"))
        .withColumn("archived_datetime", F.current_timestamp())
        .withColumn("archive_reason_code", F.lit(REJECT_DUPLICATE))
        .withColumn("report_only_flag", F.lit(reportOnly))
    )
    archived = fc.appendRows(spark, archiveName, archive, clusterCols=["invoice_date_key"]) if duplicateCount else 0

    rejectedGroups = (
        losers.groupBy("natural_key_hash", "invoice_number", "invoice_line_number", "region_code")
        .agg(F.count("*").alias("duplicate_rows"), F.collect_list("sale_key").alias("sale_keys"))
        .withColumn("business_key", F.concat_ws("|", F.col("invoice_number"), F.col("invoice_line_number").cast("string"), F.col("region_code")))
    )
    rejected = fc.rejectRows(spark, catalog, rejectedGroups, OBJECT_NAME, REJECT_DUPLICATE, batchId, run.packageExecutionId,
                             businessKeyColumn="business_key", sourceSystemCode="DW", rejectStage="Deduplicate") if duplicateCount else 0

    deleted = 0
    if duplicateCount and not reportOnly:
        deleted = fc.deleteWhere(spark, factName, "sale_key IN (SELECT sale_key FROM {archive} WHERE archived_batch_id = {batchId} AND report_only_flag = false)".format(archive=archiveName, batchId=batchId))

    control.logRowCount(spark, catalog, run.packageExecutionId, OBJECT_NAME, sourceRowCount=duplicateCount,
                        targetRowCount=spark.table(factName).count(), deleteRowCount=deleted, rejectRowCount=rejected)
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact.Sale Duplicate Archive", sourceRowCount=duplicateCount, insertRowCount=archived)
    run.rowsRead = duplicateCount
    run.rowsDeleted = deleted
    run.rowsRejected = rejected
    losers.unpersist()
    print({"window": (str(loadStart), str(loadEnd)), "reportOnly": reportOnly, "duplicates": duplicateCount, "groups": groups, "archived": archived, "deleted": deleted})
