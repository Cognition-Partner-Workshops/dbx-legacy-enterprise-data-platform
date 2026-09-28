# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_CostCenter
# MAGIC Legacy: `ssis/04_staging/STG_Load_CostCenter.dtsx` (WWI_Staging). Conform raw.OracleCostCenter into stg.CostCenter: flatten the parent hierarchy to three levels (self-lookup, ignore miss -> ORPHAN), route active/closed rows to stg and orphans to err.RejectedLookupFailure. `stg.usp_TranslateSourceCodes CC_FUNCTION` = `refs.translateSourceCodes`.
# MAGIC
# MAGIC Control framework: `dbx_etl_common` (session 00) via `stg_common.loader.StagingRun`
# MAGIC (`control.packageRun` / `getWatermark` / `setWatermark` / `logRejectedRecordSet` / `logRowCount`).

# COMMAND ----------

import os
import sys

from pyspark.sql import Window  # noqa: F401
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402,F401
from stg_common import expressions as X  # noqa: E402,F401
from stg_common import refs  # noqa: E402,F401
from stg_common import transforms as T  # noqa: E402,F401
from stg_common import work as W  # noqa: E402,F401
from stg_common.loader import PHASE_STAGE_WORK, StagingRun, lookupIgnore, lookupLeft, splitByCondition  # noqa: E402,F401

p = params.getJobParams(dbutils)  # noqa: F821  (batchId, businessDate, reloadFullHistory, environmentCode, restartFromStep, catalog)

# COMMAND ----------

run = StagingRun(spark, dbutils, "STG_Load_CostCenter", sourceSystemCode="ORA_ERP", objectName="stg.CostCenter", jobParams=p)

COST_CENTER_COLUMNS = [
    "CostCenterCode", "CostCenterName", "ParentCostCenterCode", "ParentCostCenterName", "ParentFunctionCode", "CompanyCode",
    "RegionCode", "OwningRegionCode", "FunctionCode", "ConformedFunctionCode", "IsActiveFlag", "HierarchyPath", "HierarchyLevel", "ValidFromDate", "ChangeHash",
]


def load(run):
    src = run.bronze("raw_oracle_cost_center")
    run.countRead(src)
    cleansed = T.cleanseCostCenter(src).withColumn("ValidFromDate", F.col("VALID_FROM_DT").cast("date"))
    parents = cleansed.select(
        F.col("CostCenterCode").alias("ParentCostCenterCode"), F.col("CostCenterName").alias("ParentCostCenterName"), F.col("FunctionCode").alias("ParentFunctionCode")
    ).unionByName(spark.createDataFrame([("ROOT", "Corporate", "GEN")], ["ParentCostCenterCode", "ParentCostCenterName", "ParentFunctionCode"]))
    withParent = lookupIgnore(cleansed, parents, ["ParentCostCenterCode"], ["ParentCostCenterName", "ParentFunctionCode"])
    hierarchy = T.deriveCostCenterHierarchy(withParent).withColumn("OwningRegionCode", F.col("RegionCode"))
    active, closed, orphan = T.splitCostCenter(hierarchy)
    run.rejectLookupFailures(orphan, "Parent Cost Center", "ParentCostCenterCode", "CostCenterCode", "Parent cost centre not present in the extract")
    translated = refs.translateSourceCodes(run, active.unionByName(closed), "CC_FUNCTION", "FunctionCode", "ConformedFunctionCode", "CostCenterCode", unmatchedAction="LEAVE")
    run.truncateReload(translated.select(*COST_CENTER_COLUMNS), "stg_cost_center")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
