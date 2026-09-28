# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_ReturnAndCredit
# MAGIC Legacy: `ssis/04_staging/STG_Load_ReturnAndCredit.dtsx` (WWI_Staging). Watermarked returns (ReturnedWhen; regional return windows, RETURN_REASON crosswalk) into stg.Return and credit notes (IssuedWhen; regional approval bands) into stg.CreditNote; `stg.usp_TranslateSourceCodes stg.Return` conforms the reason code.
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

run = StagingRun(spark, dbutils, "STG_Load_ReturnAndCredit", sourceSystemCode="SQL_WWI", objectName="stg.Return", watermark=True, jobParams=p)

RETURN_COLUMNS = ["ReturnLineId", "InvoiceId", "StockItemId", "SourceReturnReasonCode", "ReturnReasonCode", "ReturnReasonDescription", "RegionCode", "ReturnWindowDays", "QuantityReturned", "ReturnedWhen"]
CREDIT_COLUMNS = ["CreditNoteId", "InvoiceId", "CreditReasonCode", "CreditAmount", "CurrencyCode", "IssuedWhen", "ApprovalBandCode", "ApprovedFlag"]


def load(run):
    returns = run.bronzeWatermarked("raw_sql_return_line", "ReturnedWhen")
    run.countRead(returns)
    derived = T.deriveReturn(returns)
    reasons = refs.codeCrosswalk(run, "RETURN_REASON").select(F.col("SourceCodeValue").alias("SourceReturnReasonCode"), F.col("ConformedCodeValue").alias("ReturnReasonCode"), F.col("SourceCodeDescription").alias("ReturnReasonDescription")).dropDuplicates(["SourceReturnReasonCode"])
    matched, unknownReason = lookupLeft(derived, reasons, ["SourceReturnReasonCode"], ["ReturnReasonCode", "ReturnReasonDescription"])
    run.rejectLookupFailures(unknownReason, "Return Reason", "SourceReturnReasonCode", "ReturnLineId", "Return reason has no RETURN_REASON crosswalk entry")
    valid, nonPositive = splitByCondition(matched, F.col("QuantityReturned") > 0)
    run.rejectConstraint(nonPositive, "stg.Return", "CK_stgReturn_Quantity", "ReturnLineId", "QuantityReturned", "NON_POSITIVE_QTY", "Returned quantity is not positive")
    run.mergeByKey(valid.select(*RETURN_COLUMNS), "stg_return", ["ReturnLineId"])

    credits = run.bronze("raw_sql_credit_note", currentBatchOnly=False)
    credits = run.applyWatermark(credits, "IssuedWhen")
    run.countRead(credits)
    creditDerived = T.deriveCreditNote(credits)
    approved, unapproved, zero = T.splitCreditNote(creditDerived)
    run.rejectConstraint(unapproved, "stg.CreditNote", "CK_stgCreditNote_Approved", "CreditNoteId", "ApprovedFlag", "UNAPPROVED", "Credit note not approved")
    run.rejectConstraint(zero, "stg.CreditNote", "CK_stgCreditNote_Amount", "CreditNoteId", "CreditAmount", "ZERO_CREDIT", "Credit amount is not positive")
    creditCount = run.mergeByKey(approved.select(*CREDIT_COLUMNS), "stg_credit_note", ["CreditNoteId"])
    run.logRowCount(objectName="stg.CreditNote", insertRowCount=creditCount)


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
