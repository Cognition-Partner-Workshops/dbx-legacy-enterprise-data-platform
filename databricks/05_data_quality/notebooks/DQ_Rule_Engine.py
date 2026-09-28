# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_Rule_Engine
# MAGIC Migrated from `ssis/05_data_quality/DQ_Rule_Engine.dtsx` (WWI_DataQuality).
# MAGIC Evaluates every active row of `etl.data_quality_rule` against its target object and records one
# MAGIC `etl.data_quality_result` row per rule:
# MAGIC
# MAGIC 1. `control.evaluateDataQualityRules` (the shared re-implementation of `etl.usp_EvaluateDataQualityRules`)
# MAGIC    runs the whole active rule set for the batch.
# MAGIC 2. Rules the shared evaluator could not execute (no result row, or `MeasuredValue = -1` / `NotEvaluated`)
# MAGIC    are re-run here through `dq_quality.rules`, which translates the stored T-SQL predicate fragment to
# MAGIC    Spark SQL over the Unity Catalog object (the rule text itself is never modified). A rule that still
# MAGIC    cannot be evaluated keeps the legacy `-1` measure instead of aborting the sweep.
# MAGIC 3. The rule inventory data flow (severity classification, per-group aggregate) is persisted as
# MAGIC    `DQ_RULE_INVENTORY_*` result rows; `Count Failing Rules` is recomputed exactly as the legacy SQL
# MAGIC    (`MeasuredValue > ThresholdValue` over the batch's results).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, params, naming  # noqa: F401

from dq_quality import delta_io, rules
from dq_quality.gates import Gate, applyGates
from dq_quality.naming_map import controlTable, deltaTable
from dq_quality.notebook_support import PACKAGE_STEP, PROJECT_NAME, ensureWidgets, setTaskValue, shouldSkipForRestart

PACKAGE_NAME = "DQ_Rule_Engine"
SOURCE_SYSTEM_CODE = "WWI_OLTP"
OBJECT_NAME = "etl.DataQualityResult"
ERR_TABLE = "err.RejectedConstraintViolation"

ensureWidgets(dbutils)
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]

if shouldSkipForRestart(p["restartFromStep"], PACKAGE_STEP[PACKAGE_NAME]):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

ruleTable = controlTable(catalog, "data_quality_rule")
resultTable = controlTable(catalog, "data_quality_result")

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=PACKAGE_STEP[PACKAGE_NAME]) as run:
    packageExecutionId = run.packageExecutionId

    # ETL DataQualityRule Definitions (OLE DB Source) + Count Rules Loaded
    activeRules = (spark.table(ruleTable).filter(F.col("IsActive").cast("int") == 1)
                   .select("DataQualityRuleId", "RuleCode", "RuleGroupCode", "ObjectName", "RuleExpression",
                           "SeverityCode", "ThresholdValue"))
    ruleRows = activeRules.collect()
    rowsRead = len(ruleRows)
    definitions = [rules.RuleDefinition(r["DataQualityRuleId"], r["RuleCode"], r["RuleGroupCode"], r["ObjectName"],
                                        r["RuleExpression"], r["SeverityCode"],
                                        float(r["ThresholdValue"]) if r["ThresholdValue"] is not None else None)
                   for r in ruleRows]

    # Classify Rule Severity -> Summarize Rules By Group -> Route Rule Definitions -> ETL DataQualityResult
    inventory = (activeRules
                 .withColumn("SeverityCode", F.upper(F.trim(F.coalesce(F.col("SeverityCode"), F.lit("WARN")))))
                 .withColumn("BlockingFlag", F.when(F.col("SeverityCode") == "FAIL", "Y").otherwise("N"))
                 .groupBy("RuleGroupCode", "SeverityCode")
                 .agg(F.count("DataQualityRuleId").alias("RuleCount"), F.avg("ThresholdValue").alias("AverageThreshold"))
                 .collect())
    inventoryRows = [
        delta_io.measureRow(batchId, packageExecutionId, "etl.DataQualityRule",
                            "DQ_RULE_INVENTORY_%s_%s" % (r["RuleGroupCode"], r["SeverityCode"]), r["RuleCount"],
                            thresholdValue=float(r["AverageThreshold"]) if r["AverageThreshold"] is not None else None,
                            rowsEvaluated=r["RuleCount"], resultStatus="Passed",
                            detailText="Usable Rule output of Summarize Rules By Group")
        for r in inventory if r["RuleCount"] > 0]
    rowsInserted = len(inventoryRows)

    # 1. shared evaluator over the whole active set
    control.evaluateDataQualityRules(spark, catalog, batchId=batchId, packageExecutionId=packageExecutionId)

    # 2. Evaluate Each Configured Rule for whatever the shared evaluator did not cover
    covered = {r["RuleCode"] for r in
               spark.table(resultTable).filter((F.col("BatchId") == batchId) & (F.col("MeasuredValue") >= 0)
                                               & (F.col("ResultStatus") != "NotEvaluated"))
               .select("RuleCode").distinct().collect()}
    pending = [d for d in definitions if d.ruleCode not in covered]
    outcomes = rules.evaluateRules(pending, catalog, rules.sparkScalarRunner(spark))
    delta_io.writeResults(spark, catalog, rules.outcomeRows(outcomes, batchId, packageExecutionId) + inventoryRows, batchId)

    notEvaluated = [o for o in outcomes if o.error]
    if notEvaluated:
        for o in notEvaluated:
            control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                             errorSeverity="Warning", errorCode=50000, sourceName=PACKAGE_NAME,
                             sourceComponent="Evaluate Each Configured Rule",
                             errorDescription="%s not evaluated: %s" % (o.rule.ruleCode, o.error))
        violations = spark.createDataFrame(
            [(o.rule.objectName, o.rule.ruleCode, "RULE", o.rule.ruleCode, "RuleExpression", o.rule.ruleExpression,
              o.error, "DQ_RULE_NOT_EVALUATED", "Rule expression could not be evaluated", "RuleEngine", o.sql)
             for o in notEvaluated],
            ["TargetObjectName", "ConstraintName", "ConstraintTypeCode", "ViolatingBusinessKey", "ViolatingColumnName",
             "ViolatingValue", "SqlErrorMessage", "RejectReasonCode", "RejectReason", "RejectStage", "RecordPayload"]
        ).withColumn("BatchId", F.lit(batchId).cast("long")) \
         .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long")) \
         .withColumn("ReprocessStatusCode", F.lit("NEW")) \
         .withColumn("RejectedAtUtc", F.current_timestamp())
        delta_io.writeRejects(spark, violations, deltaTable(catalog, ERR_TABLE), batchId, ["DQ_RULE_NOT_EVALUATED"],
                              rejectStage="RuleEngine")
        delta_io.registerRejects(spark, catalog, violations, OBJECT_NAME, batchId, packageExecutionId,
                                 SOURCE_SYSTEM_CODE, "ViolatingBusinessKey", control.logRejectedRecordSet,
                                 rejectStage="RuleEngine")

    # Count Failing Rules (legacy: MeasuredValue > ThresholdValue over this batch's results)
    failedRuleCount = (spark.table(resultTable).alias("r")
                       .join(spark.table(ruleTable).alias("d"), F.col("r.RuleCode") == F.col("d.RuleCode"))
                       .filter((F.col("r.BatchId") == batchId) & (F.col("r.MeasuredValue") > F.col("d.ThresholdValue")))
                       .count())

    control.logRowCount(spark, catalog, packageExecutionId, OBJECT_NAME, sourceRowCount=rowsRead,
                        targetRowCount=rowsRead, insertRowCount=rowsInserted, rejectRowCount=len(notEvaluated))
    run.rowsRead, run.rowsInserted, run.rowsRejected = rowsRead, rowsInserted, len(notEvaluated)
    setTaskValue(dbutils, "failedRuleCount", failedRuleCount)
    setTaskValue(dbutils, "rowsRejected", len(notEvaluated))

    applyGates(spark, catalog, [
        Gate("Warn On Failing Rules", "One or more configured data quality rules exceeded their threshold.",
             "Warning", lambda m: m["FailedRuleCount"] > 0, "@[User::FailedRuleCount] > 0"),
        Gate("Fail On Blocking Rule Set", "Blocking data quality rules failed for this batch.",
             "Failure", lambda m: m["FailedRuleCount"] > 10, "@[User::FailedRuleCount] > 10"),
    ], {"FailedRuleCount": failedRuleCount}, packageExecutionId, batchId, PACKAGE_NAME, control.logError)
