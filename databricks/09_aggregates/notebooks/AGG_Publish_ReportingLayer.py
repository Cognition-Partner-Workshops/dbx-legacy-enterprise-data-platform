# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Publish_ReportingLayer
# MAGIC Migrated from `ssis/09_aggregates/AGG_Publish_ReportingLayer.dtsx` + `Integration.usp_PublishReportingLayer`.
# MAGIC
# MAGIC Publishes the `${catalog}.gold.rpt_*` views (legacy `Report.*`) from the `gold.agg_*` tables.
# MAGIC
# MAGIC **Strategy: `CREATE OR REPLACE VIEW` per report object (atomic repoint), gated by the legacy publish rules.**
# MAGIC
# MAGIC * Legacy `TRUNCATE Report.X; INSERT ... SELECT * FROM Aggregate.Y` per row of
# MAGIC   `Integration.ReportingPublication` becomes an atomic `CREATE OR REPLACE VIEW` over the
# MAGIC   Delta aggregate, in `PublishSequence` order. Readers never see a half-built object.
# MAGIC * `Rebuild Reporting Indexes` has no Delta equivalent (views are not stored); the aggregate
# MAGIC   tables are `OPTIMIZE`d instead of index-rebuilt (see `Optimize Published Aggregates` cell).
# MAGIC * Staleness gate (`MaxStalenessHours`, `FailOnStaleSource`), the three publish rules
# MAGIC   (daily sales present, daily inventory present, EU retention anonymised) and the
# MAGIC   `Report.PublishState` flip (`gold.rpt_publish_state`) are preserved; `ForcePublish`
# MAGIC   mirrors the procedure's `@ForcePublish`.
# MAGIC * The procedure's internal re-run of every refresh (`@SkipRefresh = 0`) is replaced by the
# MAGIC   job's `depends_on` edges: this task runs after all twelve `AGG_Refresh_*` tasks.

# COMMAND ----------

import datetime as dt
import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, params, naming  # noqa: E402

import agg_common  # noqa: E402
import agg_publish  # noqa: E402
from rpt_views import viewDdl  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("PublicationGroupCode", "DAILY")
dbutils.widgets.text("MaxStalenessHours", "26")
dbutils.widgets.text("FailOnStaleSource", "True")
dbutils.widgets.text("ForcePublish", "False")
dbutils.widgets.text("OptimizeAggregates", "True")

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
environmentCode = p["environmentCode"]
t = agg_common.resolveTables(catalog, naming.table)

publicationGroupCode = agg_common.getOptionalWidget(dbutils, "PublicationGroupCode", "DAILY") or "DAILY"
maxStalenessHours = agg_common.parseDecimal(agg_common.getOptionalWidget(dbutils, "MaxStalenessHours", "26"), 26.0)
failOnStaleSource = agg_common.parseBool(agg_common.getOptionalWidget(dbutils, "FailOnStaleSource", "True"), True)
forcePublish = agg_common.parseBool(agg_common.getOptionalWidget(dbutils, "ForcePublish", "False"), False)
optimizeAggregates = agg_common.parseBool(agg_common.getOptionalWidget(dbutils, "OptimizeAggregates", "True"), True)

# COMMAND ----------

# Init Publication Window
publicationStart = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
publishStateTable = t["rpt_publish_state"]
PACKAGE_NAME = "AGG_Publish_ReportingLayer"

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME,
                        projectName=agg_common.PROJECT_NAME, stepName="Publish Reporting Layer") as run:
    packageExecutionId = run.packageExecutionId

    # Read Publication List (etl.ufn_GetConfigurationValue('ReportingPublicationList'))
    try:
        configuredList = control.getConfiguration(spark, catalog, f"ReportingPublicationList.{publicationGroupCode}",
                                                  environmentCode=environmentCode)
    except Exception:
        try:
            configuredList = control.getConfiguration(spark, catalog, "ReportingPublicationList",
                                                      environmentCode=environmentCode)
        except Exception:
            configuredList = ""
    plan = agg_publish.readPublicationPlan(spark, t, publicationGroupCode, configuredList)
    print(f"Publication plan ({plan.source}): {plan.views}")

    # Check Aggregate Staleness + Quarantine Stale Publications
    watermarks = {}
    for objectName in agg_publish.requiredAggregatesFor(plan.views):
        _, watermarkTo = control.getWatermark(spark, catalog, agg_common.SOURCE_SYSTEM_CODE, objectName)
        watermarks[objectName] = watermarkTo
    staleness = agg_publish.evaluateStaleness(watermarks, publicationStart, maxStalenessHours)
    staleObjects = [s for s in staleness if s.isStale]
    for s in staleObjects:
        control.logRejectedRecord(
            spark, catalog, s.objectName, "AGG_STALE_AT_PUBLICATION",
            packageExecutionId=packageExecutionId, batchId=batchId,
            sourceSystemCode=agg_common.SOURCE_SYSTEM_CODE, businessKey=s.objectName,
            rejectReason=f"Aggregate is stale at publication time ({s.stalenessHours:.1f}h > {maxStalenessHours}h)",
            rejectStage="Publish", recordPayload=json.dumps({"watermarkTo": str(s.watermarkTo)}))
    if staleObjects and failOnStaleSource and not forcePublish:
        raise RuntimeError("Stale aggregates block publication: " + ", ".join(s.objectName for s in staleObjects))

    # Publish rules (usp_PublishReportingLayer). The legacy procedure checks SYSDATETIME()-1;
    # the nightly batch runs with BusinessDate = that calendar day, so BusinessDate is used to keep reruns publishable.
    rules = agg_publish.evaluatePublishRules(spark, t, expectedDate=businessDate, asOfDate=publicationStart.date())
    failedRules = sum(1 for r in rules if not r.passed) + len(staleObjects)
    for r in rules:
        print(f"rule {r.ruleName}: {'passed' if r.passed else 'FAILED'} - {r.detail}")
    canPublish, statusCode = agg_publish.publishDecision(failedRules, forcePublish)

    publishedViews = []
    viewRowCounts = {}
    if canPublish:
        # Publish Reporting Objects: one atomic CREATE OR REPLACE VIEW per Report object, in publish sequence.
        for viewName in plan.views:
            ddl = viewDdl(catalog, viewName, agg_publish.viewSelectSql(viewName, t), naming.table)
            spark.sql(ddl)
            publishedViews.append(viewName)
            fq = naming.table(catalog, "gold", viewName)
            viewRowCounts[viewName] = spark.sql(f"SELECT COUNT(*) AS c FROM {fq}").collect()[0]["c"]
            control.logRowCount(spark, catalog, packageExecutionId, agg_publish.legacyReportName(viewName),
                                targetRowCount=viewRowCounts[viewName])

        # Rebuild Reporting Indexes -> OPTIMIZE the aggregate tables the views read
        if optimizeAggregates:
            for objectName in agg_publish.requiredAggregatesFor(plan.views):
                key = next(k for k, v in agg_common.LEGACY_OBJECT_NAMES.items() if v == objectName)
                if agg_common.tableExists(spark, t[key]):
                    spark.sql(f"OPTIMIZE {t[key]}")

        # Stamp Publication Metadata + Report.PublishState flip
        stamped = agg_publish.stampPublicationMetadata(spark, t["int_reporting_publication"],
                                                       publicationGroupCode, packageExecutionId)
        updateRowCount = agg_publish.updatePublishState(spark, publishStateTable, batchId, statusCode, failedRules,
                                                        len(publishedViews), publicationGroupCode, packageExecutionId)
    else:
        stamped = 0
        updateRowCount = 0
        for r in rules:
            if not r.passed:
                control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                                 errorSeverity="Warning", errorCode="AGG_PUBLISH_RULE",
                                 sourceName=agg_publish.PUBLISH_STATE_OBJECT, sourceComponent="Publish rules",
                                 procedureName=PACKAGE_NAME, errorDescription=f"{r.ruleName} failed: {r.detail}")

    control.logRowCount(spark, catalog, packageExecutionId, agg_publish.PUBLISH_STATE_OBJECT,
                        updateRowCount=updateRowCount, rejectRowCount=failedRules)
    control.assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=not forcePublish)

    if failedRules > 0 and canPublish:
        control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                         errorSeverity="Warning", errorCode="AGG_FORCED_PUBLISH",
                         sourceName=agg_publish.PUBLISH_STATE_OBJECT, sourceComponent="Publish",
                         procedureName=PACKAGE_NAME,
                         errorDescription=f"Published with {failedRules} failed rule(s) because ForcePublish=True")
    if not canPublish:
        raise RuntimeError(f"Publication blocked: {failedRules} publish rule(s) failed; previous publication stays live")

    run.rowsRead = len(plan.views)
    run.rowsInserted = len(publishedViews)
    run.rowsUpdated = updateRowCount + stamped
    run.rowsRejected = failedRules

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "package": PACKAGE_NAME,
    "publicationGroupCode": publicationGroupCode,
    "publishStatusCode": statusCode,
    "failedRules": failedRules,
    "staleObjects": [s.objectName for s in staleObjects],
    "publishedViews": publishedViews,
    "viewRowCounts": viewRowCounts,
    "publishStateTable": publishStateTable,
}))
