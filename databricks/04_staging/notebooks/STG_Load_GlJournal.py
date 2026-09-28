# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_GlJournal
# MAGIC Legacy: `ssis/04_staging/STG_Load_GlJournal.dtsx` (WWI_Staging). Watermarked (ACCOUNTING_DT) GL journal-line conformance with regional fiscal-period derivation, GL account lookup and debit/credit screening; `stg.usp_ConvertCurrencyAmounts stg.GlJournalLine` re-values SignedAmount to USD.
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

run = StagingRun(spark, dbutils, "STG_Load_GlJournal", sourceSystemCode="ORA_ERP", objectName="stg.GlJournalLine", watermark=True, jobParams=p)

JOURNAL_COLUMNS = [
    "JournalId", "JournalLineNumber", "LedgerCode", "GlAccountCode", "AccountTypeCode", "IsIntercompany", "CostCenterCode", "RegionCode",
    "AccountingDate", "PeriodName", "FiscalYear", "FiscalPeriod", "DebitAmount", "CreditAmount", "SignedAmount", "JournalCurrencyCode", "SourceCode",
]


def load(run):
    src = run.bronzeWatermarked("raw_oracle_gl_journal_line", "ACCOUNTING_DT")
    run.countRead(src)
    derived = T.deriveGlJournal(src)
    accounts = run.silver("ref_gl_account").select(F.col("AccountCode").alias("GlAccountCode"), "AccountTypeCode", "IsIntercompany")
    matched, unknownAccount = lookupLeft(derived, accounts, ["GlAccountCode"], ["AccountTypeCode", "IsIntercompany"])
    run.rejectLookupFailures(unknownAccount, "GL Account", "GlAccountCode", "JournalId", "GL account not in ref.GlAccount")
    valid, bothSides, zeroValue = T.splitGlJournal(matched)
    keyed = lambda df: df.withColumn("JournalLineKey", F.concat_ws("|", "JournalId", "JournalLineNumber"))  # noqa: E731
    run.rejectConstraint(keyed(bothSides), "stg.GlJournalLine", "CK_stgGlJournalLine_OneSide", "JournalLineKey", "SignedAmount", "BOTH_SIDES", "Debit and credit both populated")
    run.rejectConstraint(keyed(zeroValue), "stg.GlJournalLine", "CK_stgGlJournalLine_NonZero", "JournalLineKey", "SignedAmount", "ZERO_VALUE", "Journal line has no value")
    converted = refs.convertCurrencyAmounts(run, valid, "SignedAmount", "JournalCurrencyCode", "AccountingDate", "RegionCode", "SignedAmountUsd", "JournalId", scratchObjectName="stg.GlJournalLine")
    run.mergeByKey(converted.select(*JOURNAL_COLUMNS, "SignedAmountUsd", "SignedAmountUsdRate", "SignedAmountUsdRateResolutionCode"), "stg_gl_journal_line", ["JournalId", "JournalLineNumber"])


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
