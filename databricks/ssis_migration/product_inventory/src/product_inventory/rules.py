"""Pure business rules lifted from the SSIS derived-column / conditional-split
components and the T-SQL procedures. Every function is a Column expression so it
can be unit-tested on local Spark and reused by several packages."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


# ---------------------------------------------------------------- EXT_ORA_*
def productHandlingClass(chillerFlag: Column, hazmatClass: Column) -> Column:
    return (
        F.when(F.upper(F.trim(chillerFlag)) == "Y", F.lit("CHILL"))
        .when(F.trim(hazmatClass).isNotNull() & (F.trim(hazmatClass) != ""), F.lit("HAZ"))
        .otherwise(F.lit("AMB"))
    )


def productActiveFlag(statusCode: Column, discontinuedDate: Column, asOfDate: Column, graceDays: int = 180) -> Column:
    """Simplified `WWI_MDM.FN_PRODUCT_ACTIVE_FLAG`: N = inactive/obsolete, R = run-out
    (discontinued inside the regional grace period), Y = active."""
    status = F.upper(F.trim(statusCode))
    graceEnd = F.date_add(F.to_date(discontinuedDate), graceDays)
    return (
        F.when(status.isin("OBSL", "INAC", "DEL"), F.lit("N"))
        .when(discontinuedDate.isNotNull() & (F.to_date(asOfDate) > graceEnd), F.lit("N"))
        .when(discontinuedDate.isNotNull(), F.lit("R"))
        .otherwise(F.lit("Y"))
    )


def hierarchyLevelCode(level1: Column, level2: Column, level3: Column, level4: Column, level: Column) -> Column:
    return (
        F.when(level == 1, level1).when(level == 2, level2).when(level == 3, level3).otherwise(level4)
    )


# ---------------------------------------------------------------- EXT_SQL_*
def stockItemHandlingClass(isChillerStock: Column) -> Column:
    return F.when(isChillerStock.cast("boolean"), F.lit("CHILL")).otherwise(F.lit("AMB"))


def belowReorderFlag(quantityOnHand: Column, reorderLevel: Column) -> Column:
    return F.when(F.coalesce(quantityOnHand, F.lit(0)) < F.coalesce(reorderLevel, F.lit(0)), F.lit("Y")).otherwise(
        F.lit("N")
    )


def movementClass(invoiceId: Column, purchaseOrderId: Column) -> Column:
    return (
        F.when(invoiceId.isNull() & purchaseOrderId.isNull(), F.lit("ADJ"))
        .when(invoiceId.isNull(), F.lit("RCPT"))
        .otherwise(F.lit("ISSUE"))
    )


def movementDirection(quantity: Column) -> Column:
    return F.when(quantity >= 0, F.lit("IN")).otherwise(F.lit("OUT"))


def transferInTransitQuantity(despatched: Column, received: Column) -> Column:
    return despatched - F.coalesce(received, F.lit(0))


def staleTransitFlag(receivedWhen: Column, despatchedWhen: Column, asOf: Column, staleDays: int = 14) -> Column:
    return F.when(
        receivedWhen.isNull() & (F.datediff(F.to_date(asOf), F.to_date(despatchedWhen)) > staleDays), F.lit("Y")
    ).otherwise(F.lit("N"))


# ---------------------------------------------------------------- STG_Load_*
def cleanseCode(col: Column, default: str) -> Column:
    return F.upper(F.trim(F.coalesce(col, F.lit(default))))


def cleanseDescription(col: Column) -> Column:
    return F.trim(F.regexp_replace(F.regexp_replace(col, "\t", " "), "  ", " "))


def yesNoFlag(col: Column) -> Column:
    return F.when(F.upper(F.trim(F.coalesce(col, F.lit("N")))) == "Y", F.lit("Y")).otherwise(F.lit("N"))


def positiveOrDefault(col: Column, default: float) -> Column:
    return F.when(col.isNull() | (col <= 0), F.lit(default)).otherwise(col)


def priceBandCode(unitPrice: Column) -> Column:
    price = F.coalesce(unitPrice, F.lit(0))
    return F.when(price < 10, F.lit("LOW")).when(price < 100, F.lit("MID")).otherwise(F.lit("HGH"))


def counterpartyTypeCode(customerId: Column, supplierId: Column) -> Column:
    return (
        F.when(customerId.isNotNull(), F.lit("CUSTOMER"))
        .when(supplierId.isNotNull(), F.lit("SUPPLIER"))
        .otherwise(F.lit("INTERNAL"))
    )


def transactionMovementSign(transactionTypeName: Column) -> Column:
    """`ref.TransactionType.MovementSign` is not populated on the legacy host; the sign
    is derived from the WWI transaction type name (receipts +1, issues -1)."""
    return (
        F.when(transactionTypeName == "Stock Receipt", F.lit(1))
        .when(transactionTypeName == "Stock Issue", F.lit(-1))
        .otherwise(F.lit(1))
    )


def signedMovementQuantity(transactionTypeName: Column, quantity: Column, conversionFactor: Column) -> Column:
    """WWI stores issue quantities already negative; receipts/issues are normalised
    through the sign table, everything else (adjustments, transfers) keeps its sign."""
    factor = F.coalesce(conversionFactor, F.lit(1))
    signed = F.when(
        transactionTypeName.isin("Stock Receipt", "Stock Issue"),
        F.abs(quantity) * transactionMovementSign(transactionTypeName),
    ).otherwise(quantity)
    return signed * factor


def transactionTypeToMovementTypeCode(transactionTypeName: Column) -> Column:
    return (
        F.when(transactionTypeName == "Stock Issue", F.lit("ISSUE"))
        .when(transactionTypeName == "Stock Receipt", F.lit("RECEIPT"))
        .when(transactionTypeName == "Stock Transfer", F.lit("TRANSFER"))
        .when(transactionTypeName == "Stock Adjustment at Stocktake", F.lit("ADJUST"))
        .when(transactionTypeName == "Customer Invoice", F.lit("SALE"))
        .otherwise(F.lit("OTHER"))
    )


# ------------------------------------------------------ STG_Work_ProductCrosswalk
def normalisedMatchKey(identifier: Column, name: Column) -> Column:
    blank = identifier.isNull() | (F.trim(identifier) == "")
    return F.when(blank, F.upper(F.regexp_replace(F.trim(name), " ", ""))).otherwise(F.trim(identifier))


def matchRuleCode(identifier: Column) -> Column:
    blank = identifier.isNull() | (F.trim(identifier) == "")
    return F.when(blank, F.lit("NAME")).otherwise(F.lit("GTIN"))


def crosswalkSurvivorship(matchRule: Column, matchKey: Column, minimumNameLength: int = 8) -> Column:
    """Conditional split: GTIN matches are preferred, NAME matches only survive when the
    normalised key is long enough to be meaningful; everything else is unmatchable."""
    return (
        F.when(matchRule == "GTIN", F.lit("PREFERRED"))
        .when((matchRule == "NAME") & (F.length(matchKey) >= minimumNameLength), F.lit("NAME"))
        .otherwise(F.lit("UNMATCHABLE"))
    )


# ------------------------------------------------------ STG_Work_InventoryPosition
def stockPositionCode(netQuantity: Column) -> Column:
    return F.when(netQuantity < 0, F.lit("NEGATIVE")).when(netQuantity == 0, F.lit("ZERO")).otherwise(F.lit("POSITIVE"))


def highChurnFlag(movementCount: Column, threshold: int = 50) -> Column:
    return F.when(movementCount > threshold, F.lit("Y")).otherwise(F.lit("N"))


def plausiblePosition(netQuantity: Column, bound: int = 1_000_000) -> Column:
    return (netQuantity > -bound) & (netQuantity < bound)


# ---------------------------------------------------------------- DIM_Load_*
def grossMarginPercent(unitPrice: Column, standardUnitCost: Column) -> Column:
    return F.when(
        unitPrice > 0, ((unitPrice - F.coalesce(standardUnitCost, F.lit(0))) / unitPrice) * 100
    ).otherwise(F.lit(0.0)).cast("decimal(18,2)")


def dimensionPriceBandCode(unitPrice: Column) -> Column:
    price = F.coalesce(unitPrice, F.lit(0))
    return (
        F.when(price >= 100, F.lit("P5"))
        .when(price >= 50, F.lit("P4"))
        .when(price >= 20, F.lit("P3"))
        .when(price >= 5, F.lit("P2"))
        .otherwise(F.lit("P1"))
    )


def handlingCode(isChillerStock: Column, quantityPerOuter: Column) -> Column:
    return (
        F.when(isChillerStock.cast("boolean"), F.lit("CHILL"))
        .when(F.coalesce(quantityPerOuter, F.lit(0)) > 48, F.lit("PALLET"))
        .otherwise(F.lit("AMBIENT"))
    )


def categoryPath(parentCode: Column, categoryCode: Column) -> Column:
    noParent = parentCode.isNull() | (F.length(F.trim(parentCode)) == 0)
    return F.when(noParent, F.upper(F.trim(categoryCode))).otherwise(
        F.concat(F.upper(F.trim(parentCode)), F.lit(">"), F.upper(F.trim(categoryCode)))
    )


def reportingRollupCode(merchandiseGroupCode: Column) -> Column:
    return (
        F.when(merchandiseGroupCode == "CHILL", F.lit("PERISHABLE"))
        .when(merchandiseGroupCode.isin("TOY", "NOV"), F.lit("SEASONAL"))
        .otherwise(F.lit("CORE"))
    )


# ---------------------------------------------------------------- FACT_Load_*
def isNegativeMovementType(movementTypeCode: Column) -> Column:
    return movementTypeCode.isin("ISSUE", "SCRAP", "SALE")


def signedFactQuantity(movementTypeCode: Column, quantityMoved: Column) -> Column:
    """`SignedQuantity = type in (ISSUE,SCRAP,SALE) ? -QuantityMoved : QuantityMoved` where
    QuantityMoved is the movement magnitude."""
    return F.when(isNegativeMovementType(movementTypeCode), -F.abs(quantityMoved)).otherwise(quantityMoved)


def movementReasonGroup(reasonCode: Column) -> Column:
    return (
        F.when(reasonCode.isin("DMG", "EXP"), F.lit("WRITEOFF"))
        .when(reasonCode == "CYC", F.lit("CYCLECOUNT"))
        .when(reasonCode == "RET", F.lit("RETURN"))
        .otherwise(F.lit("OPERATIONAL"))
    )


def isInterWarehouse(fromLocation: Column, toLocation: Column) -> Column:
    return fromLocation.isNotNull() & toLocation.isNotNull() & (fromLocation != toLocation)


def quantityAvailable(quantityOnHand: Column, quantityAllocated: Column) -> Column:
    return F.coalesce(quantityOnHand, F.lit(0)) - F.coalesce(quantityAllocated, F.lit(0))


def coverRatio(quantityAvailable: Column, targetStockLevel: Column) -> Column:
    return F.when(F.coalesce(targetStockLevel, F.lit(0)) > 0, quantityAvailable / targetStockLevel).otherwise(
        F.lit(None).cast("decimal(18,4)")
    )


def stockStatusCode(quantityOnHand: Column, available: Column, reorderLevel: Column) -> Column:
    return (
        F.when(F.coalesce(quantityOnHand, F.lit(0)) <= 0, F.lit("OUTOFSTOCK"))
        .when(available < 0, F.lit("OVERSOLD"))
        .when(available <= F.coalesce(reorderLevel, F.lit(0)), F.lit("REORDER"))
        .otherwise(F.lit("HEALTHY"))
    )


def coverBandCode(daysOfCover: Column) -> Column:
    days = F.coalesce(daysOfCover, F.lit(0))
    return (
        F.when(days >= 90, F.lit("OVERSTOCKED"))
        .when(days >= 30, F.lit("COMFORTABLE"))
        .when(days >= 7, F.lit("TIGHT"))
        .otherwise(F.lit("CRITICAL"))
    )


def agedStockPercent(quantityAged: Column, quantityOnHand: Column) -> Column:
    return F.when(F.coalesce(quantityOnHand, F.lit(0)) > 0, (quantityAged / quantityOnHand) * 100).otherwise(
        F.lit(0.0)
    ).cast("decimal(9,2)")


def ageBucketCode(daysSinceLastMovement: Column) -> Column:
    days = F.coalesce(daysSinceLastMovement, F.lit(0))
    return (
        F.when(days <= 30, F.lit("0-30"))
        .when(days <= 90, F.lit("31-90"))
        .when(days <= 180, F.lit("91-180"))
        .when(days <= 365, F.lit("181-365"))
        .otherwise(F.lit("365+"))
    )


def obsolescenceProvisionAmount(daysSinceLastMovement: Column, stockValue: Column) -> Column:
    """Slow-moving stock provision: >365 days fully provided, >180 days 50 %."""
    days = F.coalesce(daysSinceLastMovement, F.lit(0))
    value = F.coalesce(stockValue, F.lit(0))
    return F.when(days > 365, value).when(days > 180, value * 0.5).otherwise(F.lit(0)).cast("decimal(18,2)")


# ---------------------------------------------------------------- INV_*
def cycleCountStatusCode(varianceQuantity: Column, varianceValue: Column, toleranceUnits: int, toleranceValue: float) -> Column:
    return F.when(
        (F.abs(varianceQuantity) <= toleranceUnits) & (F.abs(varianceValue) <= toleranceValue), F.lit("AUTO")
    ).otherwise(F.lit("HOLD"))


def regionalSafetyFactor(regionCode: Column) -> Column:
    return F.when(regionCode == "APAC", F.lit(1.5)).when(regionCode == "EU", F.lit(1.1)).otherwise(F.lit(1.25))


def reorderPoint(averageDailyDemand: Column, leadTimeDays: Column, safetyFactor: Column) -> Column:
    return (F.coalesce(averageDailyDemand, F.lit(0)) * F.coalesce(leadTimeDays, F.lit(0)) * safetyFactor).cast("int")


def projectedAvailable(quantityOnHand: Column, quantityOnOrder: Column, quantityAllocated: Column) -> Column:
    return (
        F.coalesce(quantityOnHand, F.lit(0)) + F.coalesce(quantityOnOrder, F.lit(0)) - F.coalesce(quantityAllocated, F.lit(0))
    ).cast("int")


def daysOfCover(quantityOnHand: Column, quantityAllocated: Column, averageDailyDemand: Column) -> Column:
    demand = F.coalesce(averageDailyDemand, F.lit(0))
    return F.when(demand == 0, F.lit(999)).otherwise(
        (F.coalesce(quantityOnHand, F.lit(0)) - F.coalesce(quantityAllocated, F.lit(0))) / demand
    ).cast("decimal(18,2)")


def rawSuggestedQuantity(averageDailyDemand: Column, coverDays: int, projected: Column) -> Column:
    return (F.coalesce(averageDailyDemand, F.lit(0)) * coverDays).cast("int") - projected


def suggestedQuantity(rawSuggested: Column, quantityPerOuter: Column) -> Column:
    """Suggestions are rounded UP to the supplier outer pack size."""
    outer = F.coalesce(quantityPerOuter, F.lit(1))
    rounded = F.ceil(rawSuggested / outer) * outer
    return F.when(rawSuggested <= 0, F.lit(0)).when(outer <= 1, rawSuggested).otherwise(rounded).cast("int")


def isStockoutRisk(projected: Column, averageDailyDemand: Column, leadTimeDays: Column) -> Column:
    return projected <= (F.coalesce(averageDailyDemand, F.lit(0)) * F.coalesce(leadTimeDays, F.lit(0))).cast("int")


def transferMovementUnitValue(fromRegion: Column, toRegion: Column, transferPrice: Column, unitCost: Column) -> Column:
    """Cross-region transfers carry the intercompany transfer price (default cost + 8 %)."""
    return F.when(fromRegion != toRegion, F.coalesce(transferPrice, unitCost * 1.08)).otherwise(unitCost).cast(
        "decimal(18,2)"
    )


def onHandVarianceClass(
    dwQuantity: Column,
    operationalQuantity: Column,
    lastMovementAt: Column,
    asOf: Column,
    timingWindowMinutes: int,
) -> Column:
    diff = F.coalesce(dwQuantity, F.lit(0)) - F.coalesce(operationalQuantity, F.lit(0))
    minutesSinceMovement = (F.unix_timestamp(asOf) - F.unix_timestamp(lastMovementAt)) / 60
    return (
        F.when(diff == 0, F.lit("MATCHED"))
        .when(F.coalesce(operationalQuantity, F.lit(0)) < 0, F.lit("NEGATIVE"))
        .when(lastMovementAt.isNotNull() & (minutesSinceMovement <= timingWindowMinutes), F.lit("TIMING"))
        .otherwise(F.lit("VARIANCE"))
    )
