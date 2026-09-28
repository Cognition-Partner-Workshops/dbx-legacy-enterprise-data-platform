# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_LoyaltyLedger
# MAGIC Legacy: `ssis/04_staging/STG_Load_LoyaltyLedger.dtsx` (WWI_Staging). Watermarked (EntryDate) loyalty ledger: points typing (earn/redeem), expiry defaulting, per customer/program/region aggregation, tier attribution and ref.LoyaltyTier lookup; `stg.usp_TranslateSourceCodes stg.LoyaltyLedger` conforms the program code.
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

run = StagingRun(spark, dbutils, "STG_Load_LoyaltyLedger", sourceSystemCode="SQL_WWI", objectName="stg.LoyaltyLedger", watermark=True, jobParams=p)

LOYALTY_COLUMNS = [
    "CustomerId", "ProgramCode", "ConformedProgramCode", "RegionCode", "TotalPointsEarned", "TotalPointsRedeemed", "NetPointsBalance", "EntryCount",
    "LastEntryDate", "EarliestExpiryDate", "LoyaltyTierCode", "TierName", "TierDiscountPercent",
]


def load(run):
    src = run.bronzeWatermarked("raw_sql_loyalty_ledger", "EntryDate")
    run.countRead(src)
    derived = T.deriveLoyaltyLedger(src)
    aggregated = T.aggregateLoyalty(derived).withColumn("SourceSystemCode", F.lit(run.sourceSystemCode))
    tiers = run.silver("ref_loyalty_tier").where(F.col("IsActive") == F.lit(True)).select("LoyaltyTierCode", "TierName", F.col("DiscountPercent").alias("TierDiscountPercent"))
    matched, unknownTier = lookupLeft(aggregated, tiers, ["LoyaltyTierCode"], ["TierName", "TierDiscountPercent"])
    run.rejectLookupFailures(unknownTier, "Loyalty Tier", "LoyaltyTierCode", "CustomerId", "Derived tier not in ref.LoyaltyTier")
    translated = refs.translateSourceCodes(run, matched, "LOYALTY_PROGRAM", "ProgramCode", "ConformedProgramCode", "CustomerId", unmatchedAction="LEAVE")
    run.mergeByKey(translated.select(*LOYALTY_COLUMNS), "stg_loyalty_ledger", ["CustomerId", "ProgramCode", "RegionCode"])


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
