# Databricks notebook source
# MAGIC %md
# MAGIC # FIN_Load_CostAllocation
# MAGIC Applies the cost-centre allocation rules in `RuleSequence` order (later rules allocate amounts written by earlier ones, so the pass is iterative like the legacy cursor), measures the unallocated residual and folds the per-target result into `gold.agg_finance_close_summary`.
# MAGIC
# MAGIC Legacy: `ssis/10_finance/FIN_Load_CostAllocation.dtsx` (WWI_Finance). Control framework calls go through
# MAGIC `dbx_etl_common` (session 00); finance logic lives in `../src/finance_rules.py`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import delta_io
import finance_common as fc
import finance_rules as rules
import finance_sources as sources
import finance_tables as tables
import period_lock

PACKAGE_NAME = "FIN_Load_CostAllocation"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
fin = fc.getFinanceParams(dbutils, p["businessDate"])
if fc.shouldSkipForRestart(PACKAGE_NAME, p.get("restartFromStep")):
    dbutils.notebook.exit(f"{PACKAGE_NAME} skipped: RestartFromStep={p.get('restartFromStep')}")

batchId, ownsBatch = fc.resolveBatchId(spark, catalog, p)
stepName, stepSequence, stepGroup = fc.stepFor(PACKAGE_NAME)
reloadFullHistory = bool(p.get("reloadFullHistory"))

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
tables.ensureFinanceTables(spark, catalog)
tables.ensureGoldTargets(spark, catalog)
print(f"{PACKAGE_NAME} batch={batchId} period={fin.accountingPeriod} businessDate={p['businessDate']} catalog={catalog}")

# COMMAND ----------


workAlloc = naming.table(catalog, "silver", "work_cost_allocation_result")
aggClose = naming.table(catalog, "gold", "agg_finance_close_summary")

with fc.PackageExecution(spark, catalog, batchId, PACKAGE_NAME, stepName) as run:
    period_lock.assertPeriodOpen(spark, catalog, fin.accountingPeriod, PACKAGE_NAME)

    ruleTable = spark.table(naming.table(catalog, "silver", "stg_cost_allocation_rule"))
    targets = spark.table(naming.table(catalog, "silver", "stg_cost_allocation_target"))
    balances = spark.table(naming.table(catalog, "silver", "stg_cost_centre_balance"))
    costCentres = spark.sql(sources.costCentreSql(catalog))

    # "Count Allocation Rules"
    activeRules = rules.activeRules(ruleTable, costCentres, fin.allocationRuleSet)
    ruleRows = [r.asDict() for r in activeRules.select("AllocationRuleId", "RuleSequence", "SourceCostCentreCode", "DriverCode", "RuleSetCode").collect()]
    ruleCount = len(ruleRows)
    run.rowsRead = ruleCount

    if ruleCount == 0:
        run.logRowCount("Aggregate.Finance Close Summary", sourceRowCount=0, targetRowCount=0)
    else:
        # "Apply Allocation Rules" (cursor in RuleSequence order)
        allocations = rules.allocateCosts(ruleRows, targets, balances, fin.accountingPeriod, batchId).cache()
        delta_io.replaceWhere(allocations, workAlloc, f"AccountingPeriod = '{fin.accountingPeriod}' AND BatchId = {batchId}")

        # "Measure Unallocated Residual"
        residual = rules.unallocatedResidual(balances, ruleTable, allocations, fin.accountingPeriod, fin.allocationRuleSet)

        # "Publish Allocation Summary" -> gold.agg_finance_close_summary
        summary = rules.summariseAllocationsByTarget(allocations)
        ccAttrs = costCentres.select(F.col("CostCentreCode").alias("TargetCostCentreCode"), "LedgerCode", "RegionCode").dropDuplicates(["TargetCostCentreCode"])
        aggRows = summary.join(ccAttrs, "TargetCostCentreCode", "left").select(
            F.col("TargetCostCentreCode").alias("CostCentreCode"), "AccountingPeriod", "LedgerCode", "RegionCode",
            "AllocatedCostAmount", F.col("AllocationRuleCount").cast("int").alias("AllocationRuleCount"),
            F.lit(fin.allocationRuleSet).alias("AllocationRuleSetCode"),
            F.lit(residual).cast("decimal(19,4)").alias("UnallocatedResidualAmount"),
            F.lit(batchId).cast("bigint").alias("RefreshBatchId"), F.current_timestamp().alias("RefreshedDatetime"),
        )
        run.rowsInserted = delta_io.mergeInto(spark, aggClose, aggRows, ["CostCentreCode", "AccountingPeriod"])
        run.logRowCount("Aggregate.Finance Close Summary", sourceRowCount=allocations.count(), targetRowCount=run.rowsInserted, insertRowCount=run.rowsInserted)
        print(f"rules={ruleCount} allocations={allocations.count()} residual={residual}")
        allocations.unpersist()

dbutils.notebook.exit(f"{PACKAGE_NAME} ok: rules={run.rowsRead} summaryRows={run.rowsInserted}")
