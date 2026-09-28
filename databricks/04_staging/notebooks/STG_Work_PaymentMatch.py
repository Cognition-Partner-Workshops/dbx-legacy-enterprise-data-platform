# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Work_PaymentMatch
# MAGIC Legacy: `ssis/04_staging/STG_Work_PaymentMatch.dtsx` (WWI_Staging). Rebuild work.PaymentMatched for the batch: payments joined to open AP invoices on supplier + currency and banded EXACT / TOLERANCE (2%) / VARIANCE (data flow), then allocated per `work.usp_MatchPaymentsToInvoices` (pass 2 EXACT_AMT incl. early-payment discount, pass 3 RESIDUAL oldest-first with regional tolerance EU 0.01 / APAC 0.5% / else 0.02, UNAPPLIED remainder); unapplied cash goes to err.RejectedPayment.
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

run = StagingRun(spark, dbutils, "STG_Work_PaymentMatch", sourceSystemCode="ORA_ERP", objectName="work.PaymentMatched", phase=PHASE_STAGE_WORK, jobParams=p)

MATCH_COLUMNS = [
    "PaymentBusinessKey", "PaymentNumber", "ApInvoiceBusinessKey", "InvoiceNumber", "SupplierBusinessKey", "SupplierCode", "MatchPassNumber", "MatchRuleCode", "MatchTypeCode",
    "AmountVariance", "DaysLate", "LatePaymentFlag", "AppliedAmount", "AppliedAmountUsd", "DiscountTakenAmount", "FxDifferenceUsd", "ResidualAmount", "WithinToleranceFlag",
    "MatchConfidence", "IsFinalAllocation", "UnmatchedReasonCode",
]


def load(run):
    payments = run.silver("stg_payment")
    invoices = run.silver("stg_ap_invoice")
    run.countRead(payments)
    banded = W.bandPaymentCandidates(payments, invoices)
    matched, variance, unmatched = W.splitPaymentBand(banded)
    allocations = W.allocatePayments(payments, invoices, sourceSystemCode=run.sourceSystemCode)
    outcome = W.paymentMatchOutcome(allocations)
    unapplied = payments.join(outcome.where(F.col("MatchStatusCode").isin("UNMATCHED", "PARTIAL")), "PaymentNumber")
    run.reject(
        unapplied, "err_rejected_payment", "UNAPPLIED_CASH", "payment could not be fully applied to open supplier invoices", businessKeyColumn="PaymentNumber", rejectStage="Match", objectName="stg.Payment",
        keyColumns={"PaymentBusinessKey": X.sourceSystemKey(F.lit(run.sourceSystemCode), "PaymentNumber"), "PaymentNumber": "PaymentNumber", "SupplierReference": "SupplierCode", "PaymentAmountText": F.col("PaymentAmount").cast("string"), "CurrencyCode": "PaymentCurrencyCode", "UnappliedAmount": "UnappliedAmount"},
    )
    inserted = run.rebuildForBatch(allocations.select(*MATCH_COLUMNS), "work_payment_matched")
    run.counters.rowsUpdated += variance.count()
    run.logRowCount(objectName="work.PaymentMatched", sourceRowCount=run.counters.rowsRead, insertRowCount=inserted, rejectRowCount=run.counters.rowsRejected)


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
