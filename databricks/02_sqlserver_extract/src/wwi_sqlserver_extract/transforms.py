"""Spark equivalents of the SSIS Derived Column / Conditional Split expressions.

Every function takes the conformed source DataFrame and the run's UTC timestamp
(the SSIS GETDATE()/GETUTCDATE() reference point) and returns the DataFrame with
the package's business-derived columns appended.  Audit columns are added by
``extract.addAuditColumns`` so they are not repeated here.
"""
from __future__ import annotations

from datetime import datetime
from typing import Callable, Dict

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def ssisDateDiffHours(startCol: Column, endCol: Column) -> Column:
    """SSIS DATEDIFF("hh", start, end): hour boundaries crossed, like T-SQL."""
    startHour = F.unix_timestamp(F.date_trunc("hour", startCol))
    endHour = F.unix_timestamp(F.date_trunc("hour", endCol))
    return ((endHour - startHour) / F.lit(3600)).cast("int")


def ssisDateDiffDays(startCol: Column, endCol: Column) -> Column:
    """SSIS DATEDIFF("dd", start, end): day boundaries crossed."""
    return F.datediff(F.to_date(endCol), F.to_date(startCol)).cast("int")


def yesNo(condition: Column) -> Column:
    return F.when(condition, F.lit("Y")).otherwise(F.lit("N"))


def safeRatio(numerator: Column, denominator: Column, precision: int = 9, scale: int = 4) -> Column:
    """`den == 0 ? 0 : num / den` cast to the SSIS DT_NUMERIC target."""
    target = "decimal(%d,%d)" % (precision, scale)
    return F.when(denominator == 0, F.lit(0)).otherwise(numerator / denominator).cast(target)


def deriveOrders(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return (
        df.withColumn("BackorderFlag", F.when(F.col("BackorderOrderID").isNull(), F.lit("N")).otherwise(F.lit("Y")))
        .withColumn(
            "PickCycleHours",
            F.when(F.col("PickingCompletedWhen").isNull(), F.lit(-1)).otherwise(
                ssisDateDiffHours(F.col("OrderDate"), F.col("PickingCompletedWhen"))
            ),
        )
        .withColumn("DeleteFlag", F.lit("N"))
    )


def deriveOrderLines(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("NetLineAmount", F.col("ExtendedPrice") - F.col("LineDiscountAmount")).withColumn(
        "ShortPickFlag", yesNo(F.col("PickedQuantity") < F.col("Quantity"))
    )


def deriveInvoices(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn(
        "EffectiveTaxRate", safeRatio(F.col("TotalTaxAmount"), F.col("TotalExcludingTax"))
    ).withColumn(
        "SignedTotalIncludingTax",
        F.when(F.col("IsCreditNote"), -F.col("TotalIncludingTax")).otherwise(F.col("TotalIncludingTax")),
    )


def deriveInvoiceLines(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("GrossMarginPct", safeRatio(F.col("LineProfit"), F.col("ExtendedPrice"))).withColumn(
        "NegativeMarginFlag", yesNo(F.col("LineProfit") < 0)
    )


def derivePromotions(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("RecordKind", F.lit("PROMOTION")).withColumn(
        "RedemptionRatePct",
        safeRatio(F.col("RedemptionCount").cast("decimal(18,4)"), F.col("PromotionLineCount")),
    )


def deriveSalesTerritories(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("RecordKind", F.lit("TERRITORY")).withColumn(
        "FiscalCalendarCode",
        F.when(F.col("RegionCode") == "NA", F.lit("445"))
        .when(F.col("RegionCode") == "EU", F.lit("CAL"))
        .otherwise(F.lit("APR_MAR")),
    )


def deriveCustomerSegments(df: DataFrame, nowUtc: datetime) -> DataFrame:
    euMarketable = F.when(F.col("ConsentStatusCode") == "OPTIN", F.lit("Y")).otherwise(F.lit("N"))
    restMarketable = F.when(F.col("ConsentStatusCode") == "OPTOUT", F.lit("N")).otherwise(F.lit("Y"))
    return df.withColumn("RecordKind", F.lit("SEGMENT")).withColumn(
        "MarketableFlag", F.when(F.col("RegionCode") == "EU", euMarketable).otherwise(restMarketable)
    )


def deriveStockItems(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return (
        df.withColumn("BelowReorderFlag", yesNo(F.col("QuantityOnHand") < F.col("ReorderLevel")))
        .withColumn("HandlingClass", F.when(F.col("IsChillerStock"), F.lit("CHILL")).otherwise(F.lit("AMB")))
        .withColumn("DeleteFlag", F.lit("N"))
    )


def deriveStockMovements(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("AbsoluteQuantity", F.abs(F.col("Quantity"))).withColumn(
        "MovementClass",
        F.when(F.col("InvoiceID").isNull() & F.col("PurchaseOrderID").isNull(), F.lit("ADJ"))
        .when(F.col("InvoiceID").isNull(), F.lit("RCPT"))
        .otherwise(F.lit("ISSUE")),
    )


def deriveStockTransfers(df: DataFrame, nowUtc: datetime, inTransitToleranceDays: int = 14) -> DataFrame:
    stale = F.col("ReceivedWhen").isNull() & (
        ssisDateDiffDays(F.col("DispatchedWhen"), F.lit(nowUtc)) > inTransitToleranceDays
    )
    return (
        df.withColumn("InTransitQuantity", F.col("TransferQuantity") - F.col("ReceivedQuantity"))
        .withColumn("StaleTransitFlag", yesNo(stale))
        .withColumn("MovementClass", F.lit("XFER"))
    )


def deriveShipments(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn(
        "TransitHours",
        F.when(F.col("DeliveredWhen").isNull(), F.lit(-1)).otherwise(
            ssisDateDiffHours(F.col("DispatchedWhen"), F.col("DeliveredWhen"))
        ),
    ).withColumn(
        "CrossBorderFlag",
        F.when(F.col("OriginCountryCode") == F.col("DestinationCountryCode"), F.lit("N")).otherwise(F.lit("Y")),
    )


def deriveShipmentLines(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("AwaitingScanFlag", F.when(F.col("LastScanWhen").isNull(), F.lit("Y")).otherwise(F.lit("N")))


def deriveReturns(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn(
        "DaysToInspect",
        F.when(F.col("InspectedWhen").isNull(), F.lit(-1)).otherwise(
            ssisDateDiffDays(F.col("ReturnedWhen"), F.col("InspectedWhen"))
        ),
    ).withColumn("RestockableFlag", yesNo(F.col("DispositionCode") == "RESTOCK"))


def deriveCreditNotes(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("SignedCreditAmount", -F.col("CreditedIncludingTax")).withColumn(
        "VatReturnRequiredFlag", yesNo(F.col("TaxTreatmentCode") == "VAT")
    )


def deriveWebSessions(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("BounceFlag", yesNo(F.col("PageViewCount") <= 1)).withColumn(
        "ConversionFlag", F.when(F.col("HasCheckout"), F.lit("Y")).otherwise(F.lit("N"))
    )


def deriveLoyaltyLedger(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df


def deriveCustomerTransactions(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return (
        df.withColumn("RecordKind", F.lit("ARTRAN"))
        .withColumn("SettledFlag", F.when(F.col("FinalizationDate").isNull(), F.lit("N")).otherwise(F.lit("Y")))
        .withColumn(
            "DaysOutstanding",
            F.when(
                F.col("FinalizationDate").isNull(), ssisDateDiffDays(F.col("TransactionDate"), F.lit(nowUtc))
            ).otherwise(ssisDateDiffDays(F.col("TransactionDate"), F.col("FinalizationDate"))),
        )
    )


def deriveSupplierTransactions(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("RecordKind", F.lit("APTRAN")).withColumn(
        "DuplicateCheckKey",
        F.concat(
            F.upper(F.trim(F.col("SupplierReference"))), F.lit("|"), F.upper(F.trim(F.col("SupplierInvoiceNumber")))
        ),
    )


def derivePeople(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("RecordKind", F.lit("PERSON")).withColumn(
        "RoleCode", F.when(F.col("IsSalesperson"), F.lit("SALES")).otherwise(F.lit("EMP"))
    )


def deriveCities(df: DataFrame, nowUtc: datetime) -> DataFrame:
    continent = F.col("Continent")
    return (
        df.withColumn("RecordKind", F.lit("OLTPCITY"))
        .withColumn(
            "RegionCode",
            F.when(continent == "North America", F.lit("NA"))
            .when(continent == "Europe", F.lit("EU"))
            .when((continent == "Asia") | (continent == "Oceania"), F.lit("APAC"))
            .otherwise(F.lit("ROW")),
        )
        .withColumn(
            "PostalFormatCode",
            F.when(continent == "North America", F.lit("ZIP5_PLUS4"))
            .when(continent == "Europe", F.lit("ALPHANUM"))
            .otherwise(F.lit("NUMERIC6")),
        )
    )


def derivePaymentMethods(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("RecordKind", F.lit("PAYMETHOD")).withColumn(
        "ImmediateSettlementFlag", yesNo(F.col("SettlementDays") == 0)
    )


def deriveTransactionTypes(df: DataFrame, nowUtc: datetime) -> DataFrame:
    return df.withColumn("RecordKind", F.lit("TRANTYPE"))


def deriveDeleteMarkers(df: DataFrame, nowUtc: datetime) -> DataFrame:
    """Change-tracking delete rows: key + ChangeOperation/ChangeVersion + DeleteFlag = 'Y'."""
    return df.withColumn("DeleteFlag", F.lit("Y"))


SPLIT_CONDITIONS: Dict[str, Callable[[], Column]] = {
    "EXT_SQL_Invoices": lambda: F.col("IsCreditNote") == F.lit(True),
    "EXT_SQL_StockMovements": lambda: F.col("MovementClass") == "ADJ",
    "EXT_SQL_Returns": lambda: F.col("InspectionOutcomeCode") == "PENDING",
}
"""Conditional Split *default* (non-first) output per package.

Both split outputs land in the same raw table in the legacy package, so the split
only feeds a counter (CreditNoteCount, AdjustmentRowCount, PendingInspectionCount).
"""

TRANSFORMS: Dict[str, Callable[[DataFrame, datetime], DataFrame]] = {
    name: fn for name, fn in list(globals().items()) if name.startswith("derive") and callable(fn)
}


def applyTransform(transformName: str, df: DataFrame, nowUtc: datetime) -> DataFrame:
    try:
        fn = TRANSFORMS[transformName]
    except KeyError:
        raise KeyError("No Spark transform registered for %s" % transformName)
    return fn(df, nowUtc)
