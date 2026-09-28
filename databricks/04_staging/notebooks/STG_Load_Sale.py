# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Sale
# MAGIC Legacy: `ssis/04_staging/STG_Load_Sale.dtsx` (WWI_Staging). Watermarked (LastEditedWhen) invoice header conformance (regional tax regime, SPOT FX to USD with ignore-miss defaulting to 1) and line tax recomputation with a 0.02 variance tolerance; `stg.usp_AppendIncremental_SaleLine` delete-then-insert per batch is the keyed merge.
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

run = StagingRun(spark, dbutils, "STG_Load_Sale", sourceSystemCode="SQL_WWI", objectName="stg.Sale", watermark=True, jobParams=p)

SALE_COLUMNS = ["InvoiceId", "CustomerId", "OrderId", "RegionCode", "TaxRegimeCode", "DeliveryMethodCode", "SaleCurrencyCode", "ConversionRate", "EffectiveConversionRate", "FxImputedFlag", "DeliveryConfirmedFlag", "InvoiceDate", "ChangeHash"]
LINE_COLUMNS = ["InvoiceLineId", "InvoiceId", "StockItemId", "Quantity", "UnitPrice", "TaxRate", "TaxAmount", "NetAmount", "RecomputedTaxAmount", "TaxVarianceAmount", "LineProfitAmount", "ExtendedPrice"]


def fxToUsd():
    return run.silver("stg_fx_rate").where((F.col("ToCurrencyCode") == "USD") & (F.col("RateTypeCode") == "SPOT")).select(
        F.col("FromCurrencyCode").alias("SaleCurrencyCode"), F.col("EffectiveFromDate").alias("FxEffectiveDate"), F.col("ExchangeRate").alias("ConversionRate")
    )


def rejectLine(df, code, reason):
    return run.reject(
        df, "err_rejected_invoice_line", code, reason, businessKeyColumn="InvoiceLineId", objectName="stg.SaleLine",
        keyColumns={
            "InvoiceBusinessKey": X.sourceSystemKey(F.lit(run.sourceSystemCode), "InvoiceId"),
            "InvoiceLineBusinessKey": F.concat_ws("|", X.sourceSystemKey(F.lit(run.sourceSystemCode), "InvoiceId"), F.col("InvoiceLineId").cast("string")),
            "InvoiceNumber": F.col("InvoiceId").cast("string"), "LineNumber": F.col("InvoiceLineId").cast("string"), "LineAmountText": F.col("ExtendedPrice").cast("string"),
            "ExpectedTaxAmount": F.col("RecomputedTaxAmount").cast("decimal(19,4)"), "ActualTaxAmount": F.col("TaxAmount").cast("decimal(19,4)"), "VarianceAmount": F.col("TaxVarianceAmount").cast("decimal(19,4)"),
        },
    )


def load(run):
    invoices = run.bronzeWatermarked("raw_sql_invoice", "LastEditedWhen")
    run.countRead(invoices)
    tagged = T.tagSaleRegion(invoices)
    withFx = T.defaultMissingFx(lookupIgnore(tagged, fxToUsd(), ["SaleCurrencyCode", "FxEffectiveDate"], ["ConversionRate"]))
    run.mergeByKey(withFx.select(*SALE_COLUMNS), "stg_sale", ["InvoiceId"])

    lines = run.bronze("raw_sql_invoice_line", currentBatchOnly=False).join(invoices.select("InvoiceID").distinct(), "InvoiceID")
    recomputed = T.recomputeSaleLineTax(lines)
    valid, mismatch, zeroQty = T.splitSaleLine(recomputed)
    rejectLine(mismatch, "TAX_MISMATCH", "Recomputed tax differs from source tax by more than 0.02")
    rejectLine(zeroQty, "ZERO_QUANTITY", "Invoice line quantity is zero")
    lineCount = run.mergeByKey(valid.select(*LINE_COLUMNS), "stg_sale_line", ["InvoiceLineId"])
    run.counters.rowsUpdated += lineCount
    run.logRowCount(objectName="stg.SaleLine", insertRowCount=lineCount)


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
