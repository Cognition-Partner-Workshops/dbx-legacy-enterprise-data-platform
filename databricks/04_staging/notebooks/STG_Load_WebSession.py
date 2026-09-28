# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_WebSession
# MAGIC Legacy: `ssis/04_staging/STG_Load_WebSession.dtsx` (WWI_Staging). Watermarked (SessionStartWhen) web session conformance: EU consent suppression of CustomerId, country->region (ignore miss), user-agent family, landing-path normalisation, bounce flag and duration screening; `stg.usp_CleanStringBatch stg.WebSession` trimming is folded into the derived columns.
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

run = StagingRun(spark, dbutils, "STG_Load_WebSession", sourceSystemCode="SQL_WWI", objectName="stg.WebSession", watermark=True, jobParams=p)

SESSION_COLUMNS = ["SessionGuid", "CustomerId", "RegionCode", "CountryCode", "ConsentGivenFlag", "ChannelCode", "DeviceTypeCode", "UserAgentFamily", "LandingPagePath", "PageViewCount", "DurationSeconds", "BounceFlag", "SessionStartWhen"]


def load(run):
    src = run.bronzeWatermarked("raw_sql_web_session", "SessionStartWhen")
    run.countRead(src)
    withRegion = lookupIgnore(src.withColumn("CountryCode", F.upper(F.trim(F.col("CountryCode")))), refs.country(run, activeOnly=False).select("CountryCode", "RegionCode"), ["CountryCode"], ["RegionCode"])
    derived = T.deriveWebSession(withRegion)
    valid, tooLong, malformed = T.splitWebSession(derived)
    run.rejectConstraint(tooLong, "stg.WebSession", "CK_stgWebSession_Duration", "SessionGuid", "DurationSeconds", "DURATION_OUTLIER", "Session longer than 24 hours")
    run.rejectConstraint(malformed, "stg.WebSession", "PK_stgWebSession", "SessionGuid", "SessionGuid", "MALFORMED_KEY", "Session GUID is not 36 characters", constraintType="PK")
    run.mergeByKey(valid.select(*SESSION_COLUMNS), "stg_web_session", ["SessionGuid"])


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
