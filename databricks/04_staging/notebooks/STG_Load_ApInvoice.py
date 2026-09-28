# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_ApInvoice
# MAGIC Legacy: `ssis/04_staging/STG_Load_ApInvoice.dtsx` (WWI_Staging). Watermarked (INVOICE_DT) AP invoice header and distribution-line conformance with region-specific tax semantics (SUT / VAT / GST), payment-terms and FX lookups; `stg.usp_ConvertCurrencyAmounts stg.ApInvoice` re-values GrossAmount to USD.
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

run = StagingRun(spark, dbutils, "STG_Load_ApInvoice", sourceSystemCode="ORA_ERP", objectName="stg.ApInvoice", watermark=True, jobParams=p)

HEADER_COLUMNS = [
    "InvoiceNumber", "SupplierCode", "RegionCode", "TaxRegimeCode", "TaxRecoverableFlag", "GrossAmount", "TaxAmount", "NetAmount",
    "InvoiceCurrencyCode", "PaymentTermsCode", "NetDays", "DiscountPercent", "DiscountDays", "OnHoldFlag", "InvoiceDate", "DueDate",
    "DiscountDueDate", "ConversionRate", "GrossAmountUsd", "ChangeHash",
]
LINE_COLUMNS = ["InvoiceNumber", "LineNumber", "CostCenterCode", "CostCenterName", "OwningRegionCode", "GlAccountCode", "TaxCode", "TaxRatePercent", "LineTaxRegimeCode", "LineAmount", "LineTaxAmount", "PurchaseOrderNumber"]


def fxToUsd():
    return run.silver("stg_fx_rate").where((F.col("ToCurrencyCode") == "USD") & (F.col("RateTypeCode") == "SPOT")).select(
        F.col("FromCurrencyCode").alias("InvoiceCurrencyCode"), F.col("EffectiveFromDate").alias("FxEffectiveDate"), F.col("ExchangeRate").alias("ConversionRate")
    )


def rejectInvoice(df, code, reason):
    return run.reject(
        df, "err_rejected_invoice_line", code, reason, businessKeyColumn="InvoiceNumber",
        keyColumns={"InvoiceBusinessKey": X.sourceSystemKey(F.lit(run.sourceSystemCode), "InvoiceNumber"), "InvoiceNumber": "InvoiceNumber", "LineAmountText": F.col("GrossAmount").cast("string"), "ActualTaxAmount": F.col("TaxAmount").cast("decimal(19,4)")},
    )


def load(run):
    headers = run.bronzeWatermarked("raw_oracle_ap_invoice_hdr", "INVOICE_DT")
    run.countRead(headers)
    derived = T.deriveApInvoiceHeader(headers)
    terms = run.silver("ref_payment_terms").where(F.col("IsActive") == F.lit(True)).select("PaymentTermsCode", "NetDays", "DiscountPercent", "DiscountDays")
    withTerms, unknownTerms = lookupLeft(derived, terms, ["PaymentTermsCode"], ["NetDays", "DiscountPercent", "DiscountDays"])
    run.rejectLookupFailures(unknownTerms, "Payment Terms", "PaymentTermsCode", "InvoiceNumber", "Payment terms code not in ref.PaymentTerms")
    withFx, unknownFx = lookupLeft(withTerms, fxToUsd(), ["InvoiceCurrencyCode", "FxEffectiveDate"], ["ConversionRate"])
    run.rejectLookupFailures(unknownFx.withColumn("FxKey", F.concat_ws("|", "InvoiceCurrencyCode", "FxEffectiveDate")), "Invoice FX Rate", "FxKey", "InvoiceNumber", "No SPOT rate to USD on the invoice date")
    valid, taxExceeds, nonPositive = T.splitApInvoice(T.apInvoiceHeaderFx(withFx))
    rejectInvoice(taxExceeds, "TAX_EXCEEDS_GROSS", "Tax amount exceeds gross amount")
    rejectInvoice(nonPositive, "NON_POSITIVE_GROSS", "Gross amount is not positive")
    header = refs.convertCurrencyAmounts(run, valid, "GrossAmount", "InvoiceCurrencyCode", "InvoiceDate", "RegionCode", "GrossAmountUsdSpot", "InvoiceNumber", scratchObjectName="stg.ApInvoice")
    run.mergeByKey(header.select(*HEADER_COLUMNS, "GrossAmountUsdSpot", "GrossAmountUsdSpotRate", "GrossAmountUsdSpotRateResolutionCode"), "stg_ap_invoice", ["InvoiceNumber"])

    lines = run.bronze("raw_oracle_ap_invoice_line", currentBatchOnly=False).join(headers.select("INVOICE_NBR").distinct(), "INVOICE_NBR")
    lineDerived = T.deriveApInvoiceLine(lines)
    costCenters = run.silver("stg_cost_center").select("CostCenterCode", "CostCenterName", "OwningRegionCode")
    withCc, unknownCc = lookupLeft(lineDerived, costCenters, ["CostCenterCode"], ["CostCenterName", "OwningRegionCode"])
    run.rejectLookupFailures(unknownCc, "Cost Center", "CostCenterCode", "InvoiceNumber", "Cost centre not in stg.CostCenter", objectName="stg.ApInvoiceLine")
    taxRates = run.silver("stg_tax_rate").where(F.col("ValidToDate").isNull()).select("TaxCode", "TaxRatePercent", F.col("TaxRegimeCode").alias("LineTaxRegimeCode")).dropDuplicates(["TaxCode"])
    withTax = T.apInvoiceLineTax(lookupIgnore(withCc, taxRates, ["TaxCode"], ["TaxRatePercent", "LineTaxRegimeCode"]))
    lineCount = run.mergeByKey(withTax.select(*LINE_COLUMNS), "stg_ap_invoice_line", ["InvoiceNumber", "LineNumber"])
    run.counters.rowsUpdated += lineCount
    run.logRowCount(objectName="stg.ApInvoiceLine", insertRowCount=lineCount)


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
