"""PRC_Load_ReceiptMatching: three-way match of receipts against PO line and AP invoice line."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .common import MONEY, money, nullSafeDiv, withBatchColumns, zeroMoney

MATCH_MATCHED = "MATCHED"
MATCH_GRNI = "GRNI"
MATCH_QTY_EXCEPTION = "QTYEXCEPT"
MATCH_PRICE_EXCEPTION = "PRICEEXCEPT"
MATCH_ACCRUED = "ACCRUED"

DEFAULT_QUANTITY_TOLERANCE_PERCENT = Decimal("2")
DEFAULT_PRICE_TOLERANCE_PERCENT = Decimal("3")
DEFAULT_PRICE_TOLERANCE_ABSOLUTE = Decimal("1.00")

REJECT_OBJECT_NAME = "stg.Receipt"
REJECT_REASON = "Three-way match exception outside the regional tolerance"


def buildReceiptMatchInput(receiptDf: DataFrame, poLineDf: DataFrame, poHeaderDf: DataFrame,
                           apInvoiceLineDf: DataFrame, toleranceDf: DataFrame, batchId: int) -> DataFrame:
    """OLE DB source `stg Receipt` (RECEIPT_SQL) as a DataFrame."""
    r = receiptDf.alias("r")
    pol = poLineDf.alias("pol")
    poh = poHeaderDf.alias("poh")
    ail = apInvoiceLineDf.alias("ail")
    tol = toleranceDf.alias("tol")

    joined = (
        r.join(pol, (F.col("pol.PurchaseOrderNumber") == F.col("r.PurchaseOrderNumber"))
               & (F.col("pol.LineNumber") == F.col("r.LineNumber")), "inner")
        .join(poh, F.col("poh.PurchaseOrderNumber") == F.col("r.PurchaseOrderNumber"), "inner")
        .join(ail, (F.col("ail.PurchaseOrderNumber") == F.col("r.PurchaseOrderNumber"))
              & (F.col("ail.PurchaseOrderLineNumber") == F.col("r.LineNumber")), "left")
        .join(tol, (F.col("tol.RegionCode") == F.col("poh.RegionCode"))
              & (F.col("tol.CategoryCode") == F.col("pol.CategoryCode")), "left")
        .where(F.col("r.LoadBatchId") == F.lit(int(batchId)))
    )
    return joined.select(
        F.col("r.ReceiptId").alias("ReceiptId"),
        F.col("r.ReceiptNumber").alias("ReceiptNumber"),
        F.col("r.PurchaseOrderNumber").alias("PurchaseOrderNumber"),
        F.col("r.LineNumber").alias("LineNumber"),
        F.col("r.ReceivedAtUtc").alias("ReceivedAtUtc"),
        F.col("r.ReceivedOuters").alias("ReceivedOuters"),
        F.col("r.WarehouseSiteCode").alias("WarehouseSiteCode"),
        F.col("pol.OrderedOuters").alias("OrderedOuters"),
        money(F.col("pol.ExpectedUnitPricePerOuter")).alias("ExpectedUnitPricePerOuter"),
        F.col("ail.InvoicedOuters").alias("InvoicedOuters"),
        money(F.col("ail.InvoicedUnitPrice")).alias("InvoicedUnitPrice"),
        F.col("ail.ApInvoiceNumber").alias("ApInvoiceNumber"),
        F.col("poh.SupplierId").alias("SupplierId"),
        F.col("poh.RegionCode").alias("RegionCode"),
        money(F.coalesce(F.col("tol.QuantityTolerancePercent"), F.lit(DEFAULT_QUANTITY_TOLERANCE_PERCENT))).alias("QuantityTolerancePercent"),
        money(F.coalesce(F.col("tol.PriceTolerancePercent"), F.lit(DEFAULT_PRICE_TOLERANCE_PERCENT))).alias("PriceTolerancePercent"),
        money(F.coalesce(F.col("tol.PriceToleranceAbsolute"), F.lit(DEFAULT_PRICE_TOLERANCE_ABSOLUTE))).alias("PriceToleranceAbsolute"),
    )


def deriveMatchVariances(df: DataFrame) -> DataFrame:
    """Derived column `Derive Match Variances`.

    Note the legacy expression `ReceivedOuters - ISNULL(InvoicedOuters) ? 0 : InvoicedOuters`
    binds as `(ReceivedOuters - ISNULL(InvoicedOuters)) ? 0 : InvoicedOuters` in SSIS; the
    intent (and what this migration implements) is `ReceivedOuters - COALESCE(InvoicedOuters, 0)`.
    """
    return (
        df.withColumn("QuantityVariance", F.col("ReceivedOuters") - F.coalesce(F.col("InvoicedOuters"), F.lit(0)))
        .withColumn(
            "PriceVariance",
            F.when(F.col("InvoicedUnitPrice").isNull(), zeroMoney()).otherwise(
                money(F.col("InvoicedUnitPrice") - F.col("ExpectedUnitPricePerOuter"))
            ),
        )
        .withColumn("IsInvoiced", F.col("ApInvoiceNumber").isNotNull())
    )


def evaluateMatchResult(df: DataFrame, businessDate: date) -> DataFrame:
    """Derived column `Evaluate Match Result`.

    Price is in tolerance when the absolute variance is within the absolute floor OR within the
    percentage; only then is quantity tested. Receipt age is measured against the business date
    (the package used GETDATE(), which is not re-runnable).
    """
    priceVariancePct = nullSafeDiv(F.abs(F.col("PriceVariance")) * 100, F.col("ExpectedUnitPricePerOuter"))
    qtyVariancePct = nullSafeDiv(F.abs(F.col("QuantityVariance")) * 100, F.col("OrderedOuters"))
    priceOk = (F.abs(F.col("PriceVariance")) <= F.col("PriceToleranceAbsolute")) | (priceVariancePct <= F.col("PriceTolerancePercent"))
    qtyOk = qtyVariancePct <= F.col("QuantityTolerancePercent")
    result = (
        F.when(~F.col("IsInvoiced"), F.lit(MATCH_GRNI))
        .when(priceOk & qtyOk, F.lit(MATCH_MATCHED))
        .when(priceOk & ~qtyOk, F.lit(MATCH_QTY_EXCEPTION))
        .otherwise(F.lit(MATCH_PRICE_EXCEPTION))
    )
    return df.withColumn("MatchResultCode", result).withColumn(
        "ReceiptAgeDays", F.datediff(F.lit(businessDate), F.to_date(F.col("ReceivedAtUtc")))
    )


def splitMatchOutcome(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Conditional split `Split Match Outcome` -> (Matched, Grni, Exception)."""
    matched = df.where(F.col("MatchResultCode") == MATCH_MATCHED)
    grni = df.where(F.col("MatchResultCode") == MATCH_GRNI)
    exceptions = df.where(F.col("MatchResultCode").isin(MATCH_QTY_EXCEPTION, MATCH_PRICE_EXCEPTION))
    return matched, grni, exceptions


def buildAccruals(grniDf: DataFrame, supplierKeysDf: DataFrame, accrualCutoffDays: int) -> DataFrame:
    """Execute SQL `Accrue GRNI`: receipts younger than the cutoff are accrued at PO price."""
    return (
        grniDf.where(F.col("ReceiptAgeDays") <= F.lit(int(accrualCutoffDays)))
        .join(supplierKeysDf, "SupplierId", "inner")
        .withColumn("AccrualAmount", money(F.col("ReceivedOuters") * F.col("ExpectedUnitPricePerOuter")))
        .withColumn("MatchResultCode", F.lit(MATCH_ACCRUED))
    )


def toFactPurchaseReceipt(df: DataFrame, batchId: int, loadedAtUtc: datetime | None = None) -> DataFrame:
    """Shape matched / accrued rows for the MERGE into gold.fact_purchase_receipt."""
    cols = df.columns
    accrual = F.col("AccrualAmount") if "AccrualAmount" in cols else F.lit(None).cast(MONEY)
    invoicedQty = F.col("InvoicedOuters") if "InvoicedOuters" in cols else F.lit(None).cast("int")
    invoicedPrice = F.col("InvoicedUnitPrice") if "InvoicedUnitPrice" in cols else F.lit(None).cast(MONEY)
    apInvoice = F.col("ApInvoiceNumber") if "ApInvoiceNumber" in cols else F.lit(None).cast("string")
    out = withBatchColumns(df, batchId, loadedAtUtc)
    return out.select(
        F.to_date(F.col("ReceivedAtUtc")).alias("receipt_date_key"),
        F.col("SupplierKey").alias("supplier_key"),
        F.col("RegionCode").alias("region_code"),
        F.col("ReceiptNumber").alias("receipt_number"),
        F.col("LineNumber").alias("receipt_line_number"),
        F.col("PurchaseOrderNumber").alias("purchase_order_number"),
        F.col("LineNumber").alias("purchase_order_line_number"),
        F.col("WarehouseSiteCode").alias("warehouse_site_code"),
        F.col("OrderedOuters").cast("decimal(18,4)").alias("quantity_ordered_base_uom"),
        F.col("ReceivedOuters").cast("decimal(18,4)").alias("quantity_received_base_uom"),
        invoicedQty.alias("quantity_invoiced"),
        F.col("ExpectedUnitPricePerOuter").alias("unit_cost"),
        money(F.col("ReceivedOuters") * F.col("ExpectedUnitPricePerOuter")).alias("receipt_value"),
        invoicedPrice.alias("invoiced_unit_price"),
        apInvoice.alias("ap_invoice_number"),
        F.col("PriceVariance").alias("price_variance_amount"),
        F.col("QuantityVariance").alias("quantity_variance"),
        F.col("MatchResultCode").alias("match_result_code"),
        accrual.alias("accrual_amount"),
        F.col("ReceiptAgeDays").alias("receipt_age_days"),
        F.col("BatchId").alias("batch_id"),
        F.col("LoadedAtUtc").alias("load_datetime"),
    )


def measureMatchOutcomes(grniDf: DataFrame, exceptionDf: DataFrame) -> tuple[int, int]:
    """Execute SQL `Measure Match Outcomes` -> (GrniCount, MatchExceptionCount)."""
    return grniDf.count(), exceptionDf.count()
