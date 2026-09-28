"""Pure column-expression ports of the SSIS Derived Column / Conditional Split
expressions used by the 08_facts packages. Nothing in here touches Spark
sessions or tables, so every rule is unit-testable on a local DataFrame."""

from pyspark.sql import Column
from pyspark.sql import functions as F

REPORTING_CURRENCY = "USD"
REGION_NA = "NA"
REGION_EU = "EU"
REGION_APAC = "APAC"
FISCAL_YEAR_START_MONTH_APAC = 4
NEGATIVE_MOVEMENT_TYPES = ("ISSUE", "SCRAP", "SALE")
NEGATIVE_CUSTOMER_TRANSACTION_TYPES = ("PAYMENT", "CREDIT", "REFUND")
NEGATIVE_SUPPLIER_TRANSACTION_TYPES = ("PAYMENT", "CREDIT")
BOT_USER_AGENT_TERMS = ("bot", "crawler", "spider", "slurp", "headless")
BOT_MAX_PAGE_VIEWS = 500

MONEY = "decimal(18,2)"
RATE = "decimal(18,8)"
PCT = "decimal(9,4)"


def money(col: Column) -> Column:
    return F.round(col, 2).cast(MONEY)


def pct(col: Column) -> Column:
    return F.round(col, 4).cast(PCT)


def safeDivide(numerator: Column, denominator: Column) -> Column:
    return F.when(denominator.isNull() | (denominator == 0), F.lit(None)).otherwise(numerator / denominator)


def zeroIfNull(col: Column) -> Column:
    return F.coalesce(col, F.lit(0))


# ---------------------------------------------------------------- sale lines


def saleGrossAmount(quantity: Column, unitPrice: Column, uomFactor: Column = None) -> Column:
    factor = F.lit(1) if uomFactor is None else F.coalesce(uomFactor, F.lit(1))
    return money(quantity * factor * unitPrice)


def saleDiscountAmount(grossAmount: Column, discountPercent: Column) -> Column:
    return money(grossAmount * zeroIfNull(discountPercent) / 100)


def saleNetAmount(grossAmount: Column, discountAmount: Column) -> Column:
    return money(grossAmount - discountAmount)


def saleCostAmount(quantity: Column, unitCost: Column, uomFactor: Column = None) -> Column:
    factor = F.lit(1) if uomFactor is None else F.coalesce(uomFactor, F.lit(1))
    return money(quantity * factor * zeroIfNull(unitCost))


def saleMarginAmount(netAmount: Column, costAmount: Column) -> Column:
    return money(netAmount - costAmount)


def marginPercent(marginAmount: Column, netAmount: Column) -> Column:
    return pct(F.coalesce(safeDivide(marginAmount * 100, netAmount), F.lit(0)))


def naSalesTaxAmount(sourceTaxAmount: Column) -> Column:
    """NA keeps the source sales tax (the jurisdiction engine already applied it)."""
    return money(zeroIfNull(sourceTaxAmount))


def euIsReverseCharge(customerVatNumber: Column, customerCountryIsoCode: Column, shipToCountryIsoCode: Column) -> Column:
    hasVat = customerVatNumber.isNotNull() & (F.length(F.trim(customerVatNumber)) > 0)
    return hasVat & (customerCountryIsoCode != shipToCountryIsoCode)


def euVatRateApplied(isReverseCharge: Column, standardVatRatePercent: Column) -> Column:
    return F.when(isReverseCharge, F.lit(0)).otherwise(zeroIfNull(standardVatRatePercent)).cast("decimal(5,2)")


def euVatAmount(netAmount: Column, vatRateApplied: Column) -> Column:
    return money(netAmount * vatRateApplied / 100)


def apacGstAmount(quantity: Column, unitPrice: Column, gstRatePercent: Column, isPriceInclusive: Column) -> Column:
    base = quantity * unitPrice
    rate = zeroIfNull(gstRatePercent)
    exclusive = base * rate / 100
    inclusive = base - (base / (1 + rate / 100))
    return money(F.when(isPriceInclusive.isNull() | ~isPriceInclusive, exclusive).otherwise(inclusive))


def apacNetOfGst(grossAmount: Column, gstAmount: Column, isPriceInclusive: Column) -> Column:
    return money(F.when(F.coalesce(isPriceInclusive, F.lit(False)), grossAmount - gstAmount).otherwise(grossAmount))


def fiscalYearLabel(dateCol: Column, fiscalYearStartMonth: int) -> Column:
    year = F.year(dateCol)
    fiscalYear = F.when(F.month(dateCol) >= fiscalYearStartMonth, year + 1).otherwise(year)
    return F.concat(F.lit("FY"), fiscalYear.cast("string"))


def fiscalYear(dateCol: Column, startMonth: int = 1) -> Column:
    """Fiscal year the date falls in; APAC starts in April so Apr-Dec roll forward."""
    return F.when(F.month(dateCol) >= startMonth, F.year(dateCol) + (1 if startMonth > 1 else 0)).otherwise(F.year(dateCol)).cast("smallint")


def fiscalPeriod(dateCol: Column, startMonth: int = 1) -> Column:
    return (((F.month(dateCol) - startMonth + 12) % 12) + 1).cast("tinyint")


def apacDistributorRebateAccrual(netAmount: Column, isDistributor: Column, rebatePercent: Column) -> Column:
    return money(F.when(F.coalesce(isDistributor, F.lit(False)), netAmount * zeroIfNull(rebatePercent) / 100).otherwise(F.lit(0)))


def reportingAmount(amount: Column, fxRateToUsd: Column) -> Column:
    return money(zeroIfNull(amount) * F.coalesce(fxRateToUsd, F.lit(1)))


# ---------------------------------------------------------------- orders


def orderQuantityOutstanding(quantityOrdered: Column, quantityPicked: Column) -> Column:
    return quantityOrdered - zeroIfNull(quantityPicked)


def orderIsBackordered(quantityOrdered: Column, quantityPicked: Column, backorderNumber: Column) -> Column:
    return (zeroIfNull(quantityPicked) < quantityOrdered) & backorderNumber.isNotNull() & (F.length(F.trim(backorderNumber)) > 0)


def orderFillRatePercent(quantityOrdered: Column, quantityPicked: Column) -> Column:
    return pct(F.coalesce(safeDivide(zeroIfNull(quantityPicked) * 100, quantityOrdered), F.lit(0)))


# ---------------------------------------------------------------- payments


def paymentUnallocatedAmount(paymentAmount: Column, allocatedAmount: Column) -> Column:
    return money(paymentAmount - zeroIfNull(allocatedAmount))


def paymentAllocationStatus(paymentAmount: Column, allocatedAmount: Column) -> Column:
    allocated = zeroIfNull(allocatedAmount)
    return (
        F.when(allocated <= 0, F.lit("UNAPPLIED"))
        .when(allocated >= paymentAmount, F.lit("FULL"))
        .otherwise(F.lit("PARTIAL"))
    )


# ---------------------------------------------------------------- receivables / payables


def daysOverdue(dueDate: Column, asOfDate: Column) -> Column:
    return F.datediff(asOfDate, dueDate)


def customerAgingBucket(dueDate: Column, asOfDate: Column, regionCode: Column) -> Column:
    days = daysOverdue(dueDate, asOfDate)
    eu = F.when(days <= 30, "1-30").when(days <= 60, "31-60").otherwise("60+")
    apac = F.when(days <= 60, "1-60").when(days <= 120, "61-120").otherwise("120+")
    default = F.when(days <= 30, "1-30").when(days <= 60, "31-60").when(days <= 90, "61-90").otherwise("90+")
    return (
        F.when(days <= 0, F.lit("CURRENT"))
        .when(regionCode == REGION_EU, eu)
        .when(regionCode == REGION_APAC, apac)
        .otherwise(default)
    )


def supplierAgingBucket(dueDate: Column, asOfDate: Column) -> Column:
    days = daysOverdue(dueDate, asOfDate)
    return (
        F.when(days <= 0, F.lit("CURRENT"))
        .when(days <= 30, "1-30")
        .when(days <= 60, "31-60")
        .when(days <= 90, "61-90")
        .otherwise("90+")
    )


def signedAmount(amount: Column, transactionTypeCode: Column, negativeTypes) -> Column:
    return money(F.when(transactionTypeCode.isin(*negativeTypes), amount * -1).otherwise(amount))


def partyTypeClass(partyTypeCode: Column) -> Column:
    return (
        F.when(F.upper(partyTypeCode) == "CUSTOMER", "CUSTOMER")
        .when(F.upper(partyTypeCode) == "SUPPLIER", "SUPPLIER")
        .otherwise("OTHER")
    )


# ---------------------------------------------------------------- loyalty


LOYALTY_POINT_VALUE = {REGION_NA: 0.01, REGION_EU: 0.008, REGION_APAC: 0.012}
LOYALTY_EARN_RATE = {REGION_NA: 1.0, REGION_EU: 1.0, REGION_APAC: 2.0}
LOYALTY_EXPIRY_MONTHS = {REGION_NA: 24, REGION_EU: 36, REGION_APAC: 18}
DEFAULT_LOYALTY_POINT_VALUE = 0.01


def loyaltyPointCashValue(pointsQuantity: Column, regionCode: Column) -> Column:
    value = F.lit(DEFAULT_LOYALTY_POINT_VALUE)
    for region, unitValue in LOYALTY_POINT_VALUE.items():
        value = F.when(regionCode == region, F.lit(unitValue)).otherwise(value)
    return money(pointsQuantity * value)


def loyaltyEarnMultiplier(regionCode: Column, programCode: Column) -> Column:
    return F.when((regionCode == REGION_APAC) & (F.upper(programCode) == "DOUBLE"), F.lit(2.0)).otherwise(F.lit(1.0))


def loyaltyExpiryDate(eventDate: Column, regionCode: Column, sourceExpiryDate: Column) -> Column:
    computed = F.lit(None).cast("date")
    for region, months in LOYALTY_EXPIRY_MONTHS.items():
        computed = F.when(regionCode == region, F.add_months(eventDate, months)).otherwise(computed)
    return F.coalesce(sourceExpiryDate, computed, F.add_months(eventDate, 24))


# ---------------------------------------------------------------- web sessions


def isBotSession(userAgent: Column, pageViewCount: Column) -> Column:
    ua = F.lower(F.coalesce(userAgent, F.lit("")))
    termHit = F.lit(False)
    for term in BOT_USER_AGENT_TERMS:
        termHit = termHit | ua.contains(term)
    return termHit | (zeroIfNull(pageViewCount) > BOT_MAX_PAGE_VIEWS)


def isBounce(pageViewCount: Column, sessionDurationSeconds: Column) -> Column:
    return (zeroIfNull(pageViewCount) <= 1) | (zeroIfNull(sessionDurationSeconds) < 10)


def pagesPerMinute(pageViewCount: Column, sessionDurationSeconds: Column) -> Column:
    return pct(F.coalesce(safeDivide(zeroIfNull(pageViewCount) * 60.0, sessionDurationSeconds), F.lit(0)))


# ---------------------------------------------------------------- returns / credit notes


EU_WITHDRAWAL_DAYS = 14
NA_FREE_RETURN_DAYS = 30
NA_RESTOCKING_FEE_PERCENT = 0.15
APAC_RESTOCKING_FEE_FLAT = 5.00


def restockingFeeAmount(regionCode: Column, originalInvoiceDate: Column, returnDate: Column, quantityReturned: Column, unitPrice: Column, euWithdrawalDays: int = EU_WITHDRAWAL_DAYS) -> Column:
    days = F.datediff(returnDate, originalInvoiceDate)
    return money(
        F.when((regionCode == REGION_EU) & (days <= euWithdrawalDays), F.lit(0))
        .when((regionCode == REGION_NA) & (days > NA_FREE_RETURN_DAYS), quantityReturned * unitPrice * NA_RESTOCKING_FEE_PERCENT)
        .when(regionCode == REGION_APAC, F.lit(APAC_RESTOCKING_FEE_FLAT))
        .otherwise(F.lit(0))
    )


def negated(amount: Column) -> Column:
    return money(zeroIfNull(amount) * -1)


CREDIT_APPROVAL_THRESHOLD = 1000.00


def creditRequiresApproval(grossAmount: Column, approvedByName: Column, threshold: float = CREDIT_APPROVAL_THRESHOLD) -> Column:
    return (F.abs(grossAmount) > threshold) & (approvedByName.isNull() | (F.length(F.trim(approvedByName)) == 0))


# ---------------------------------------------------------------- inventory


def movementSignedQuantity(quantityMoved: Column, movementTypeCode: Column) -> Column:
    return F.when(movementTypeCode.isin(*NEGATIVE_MOVEMENT_TYPES), quantityMoved * -1).otherwise(quantityMoved)


def movementSignedValue(movementValueAmount: Column, movementTypeCode: Column) -> Column:
    return money(F.when(movementTypeCode.isin(*NEGATIVE_MOVEMENT_TYPES), movementValueAmount * -1).otherwise(movementValueAmount))


def movementReasonGroup(reasonCode: Column, movementTypeCode: Column) -> Column:
    code = F.upper(F.coalesce(reasonCode, movementTypeCode, F.lit("")))
    return (
        F.when(code.isin("RECEIPT", "PO", "GRN"), "RECEIVED")
        .when(code.isin("ISSUE", "SALE", "PICK"), "ISSUED")
        .when(code.isin("ADJ", "ADJUST", "COUNT", "STOCKTAKE"), "ADJUSTED")
        .when(code.isin("SCRAP", "DAMAGE", "WRITEOFF"), "SCRAPPED")
        .when(code.isin("XFER", "TRANSFER"), "TRANSFER")
        .otherwise("OTHER")
    )


def isInterWarehouse(fromLocationCode: Column, toLocationCode: Column) -> Column:
    return fromLocationCode.isNotNull() & toLocationCode.isNotNull() & (fromLocationCode != toLocationCode)


def quantityAvailable(quantityOnHand: Column, quantityAllocated: Column) -> Column:
    return zeroIfNull(quantityOnHand) - zeroIfNull(quantityAllocated)


def stockValueAmount(quantityOnHand: Column, unitCostAmount: Column) -> Column:
    return money(zeroIfNull(quantityOnHand) * zeroIfNull(unitCostAmount))


def reorderStatus(quantityAvailable: Column, reorderLevel: Column, targetStockLevel: Column) -> Column:
    return (
        F.when(quantityAvailable <= 0, "OUT_OF_STOCK")
        .when(quantityAvailable <= zeroIfNull(reorderLevel), "REORDER")
        .when(quantityAvailable > zeroIfNull(targetStockLevel), "OVERSTOCK")
        .otherwise("OK")
    )


def coverBand(daysOfCover: Column) -> Column:
    return (
        F.when(daysOfCover.isNull(), "UNKNOWN")
        .when(daysOfCover < 7, "CRITICAL")
        .when(daysOfCover < 30, "LOW")
        .when(daysOfCover < 90, "NORMAL")
        .otherwise("EXCESS")
    )


COSTING_METHOD = {REGION_NA: "WAVG", REGION_EU: "FIFO", REGION_APAC: "STD"}


def costingMethodCode(regionCode: Column) -> Column:
    expr = F.lit("WAVG")
    for region, method in COSTING_METHOD.items():
        expr = F.when(regionCode == region, F.lit(method)).otherwise(expr)
    return expr


# ---------------------------------------------------------------- daily sales snapshot


def commissionAccrual(regionCode: Column, netAmount: Column, marginAmount: Column) -> Column:
    net = zeroIfNull(netAmount)
    marginRatio = F.coalesce(safeDivide(zeroIfNull(marginAmount), net), F.lit(0))
    return money(
        F.when(regionCode == REGION_NA, F.when(net > 50000, net * 0.03).otherwise(net * 0.02))
        .when(regionCode == REGION_EU, F.when(marginRatio >= 0.30, net * 0.025).otherwise(net * 0.015))
        .when(regionCode == REGION_APAC, net * 0.018)
        .otherwise(net * 0.01)
    )


def averageOrderValue(netAmount: Column, orderCount: Column) -> Column:
    return money(F.coalesce(safeDivide(netAmount, orderCount), F.lit(0)))


# ---------------------------------------------------------------- procure to pay


def landedCostAmount(quantityOrdered: Column, unitCostAmount: Column, freightAmount: Column, regionCode: Column) -> Column:
    """APAC freight is capitalised into landed cost; other regions expense it."""
    base = zeroIfNull(quantityOrdered) * zeroIfNull(unitCostAmount)
    return money(F.when(regionCode == REGION_APAC, base + zeroIfNull(freightAmount)).otherwise(base))


def receiptMilestoneStatus(goodsReceivedDate: Column, invoiceReceivedDate: Column, paymentSettledDate: Column) -> Column:
    return (
        F.when(paymentSettledDate.isNotNull(), "SETTLED")
        .when(invoiceReceivedDate.isNotNull(), "INVOICED")
        .when(goodsReceivedDate.isNotNull(), "RECEIVED")
        .otherwise("ORDERED")
    )


def quantityVariance(quantityReceived: Column, quantityOrdered: Column) -> Column:
    return zeroIfNull(quantityReceived) - zeroIfNull(quantityOrdered)


def priceVarianceAmount(invoicedAmount: Column, receivedCostAmount: Column) -> Column:
    return money(zeroIfNull(invoicedAmount) - zeroIfNull(receivedCostAmount))


def grniAccrualAmount(receivedCostAmount: Column, invoicedAmount: Column) -> Column:
    return money(F.greatest(zeroIfNull(receivedCostAmount) - zeroIfNull(invoicedAmount), F.lit(0)))


def earlySettlementDiscount(invoiceAmount: Column, settledAmount: Column, settlementDiscountAmount: Column) -> Column:
    return money(F.coalesce(settlementDiscountAmount, zeroIfNull(invoiceAmount) - zeroIfNull(settledAmount)))


def realizedFxGainLoss(settledAmount: Column, invoiceFxRate: Column, settlementFxRate: Column) -> Column:
    return money(zeroIfNull(settledAmount) * (F.coalesce(settlementFxRate, F.lit(1)) - F.coalesce(invoiceFxRate, F.lit(1))))


# ---------------------------------------------------------------- shipments / fulfilment


def shipmentMilestoneStatus(lastScanEventCode: Column, deliveredDateTime: Column) -> Column:
    code = F.upper(F.coalesce(lastScanEventCode, F.lit("")))
    return (
        F.when(code == "LOST", "LOST")
        .when(deliveredDateTime.isNotNull() | (code == "DELIVERED"), "DELIVERED")
        .when(code == "ATTEMPT", "ATTEMPTED")
        .when(code.isin("COLLECT", "PICK", "PACK"), "IN_TRANSIT")
        .otherwise("CREATED")
    )


def latencyHours(startTs: Column, endTs: Column) -> Column:
    return pct((F.unix_timestamp(endTs) - F.unix_timestamp(startTs)) / 3600.0)


def onTimeDeliveryFlag(promisedUtc: Column, deliveredUtc: Column) -> Column:
    return F.when(deliveredUtc.isNull() | promisedUtc.isNull(), F.lit(None).cast("boolean")).otherwise(deliveredUtc <= promisedUtc)


def fulfilmentMilestoneStatus(allocatedAt: Column, pickedAt: Column, invoicedAt: Column, cashReceivedAt: Column) -> Column:
    return (
        F.when(cashReceivedAt.isNotNull(), "CASH_RECEIVED")
        .when(invoicedAt.isNotNull(), "INVOICED")
        .when(pickedAt.isNotNull(), "PICKED")
        .when(allocatedAt.isNotNull(), "ALLOCATED")
        .otherwise("ORDERED")
    )


STALLED_ORDER_DAYS = {REGION_NA: 7, REGION_EU: 10, REGION_APAC: 14}


def isStalledOrder(regionCode: Column, orderedAt: Column, invoicedAt: Column, asOfDate: Column) -> Column:
    threshold = F.lit(10)
    for region, days in STALLED_ORDER_DAYS.items():
        threshold = F.when(regionCode == region, F.lit(days)).otherwise(threshold)
    return invoicedAt.isNull() & (F.datediff(asOfDate, F.to_date(orderedAt)) > threshold)


# ---------------------------------------------------------------- GL


def glSignedAmount(debitAmount: Column, creditAmount: Column) -> Column:
    return money(zeroIfNull(debitAmount) - zeroIfNull(creditAmount))


def isManualJournal(journalSourceCode: Column) -> Column:
    return F.upper(F.coalesce(journalSourceCode, F.lit(""))).isin("MAN", "MANUAL", "MJ")


# ---------------------------------------------------------------- stock holding / inventory snapshot extensions

SLOW_MOVING_AGED_SHARE = 0.5
OBSOLESCENCE_PROVISION_RATE = {"NA": 0.25, "EU": 0.30, "APAC": 0.20}
DEFAULT_OBSOLESCENCE_PROVISION_RATE = 0.25


def isBelowReorderLevel(quantityOnHand: Column, quantityAllocated: Column, reorderLevel: Column) -> Column:
    """SSIS: (QuantityOnHand - QuantityAllocated) < ReorderLevel."""
    return (zeroIfNull(quantityOnHand) - zeroIfNull(quantityAllocated)) < zeroIfNull(reorderLevel)


def agedShare(quantityAged: Column, quantityOnHand: Column) -> Column:
    return F.coalesce(safeDivide(zeroIfNull(quantityAged), quantityOnHand), F.lit(0))


def isSlowMoving(quantityAged: Column, quantityOnHand: Column) -> Column:
    return (zeroIfNull(quantityOnHand) > 0) & (agedShare(quantityAged, quantityOnHand) >= SLOW_MOVING_AGED_SHARE)


def stockAgeBucket(quantityAged: Column, quantityOnHand: Column) -> Column:
    share = agedShare(quantityAged, quantityOnHand)
    return (
        F.when(zeroIfNull(quantityOnHand) <= 0, F.lit("NONE"))
        .when(share == 0, F.lit("FRESH"))
        .when(share < 0.25, F.lit("AGING"))
        .when(share < SLOW_MOVING_AGED_SHARE, F.lit("SLOW"))
        .otherwise(F.lit("OBSOLETE"))
    )


def obsolescenceProvision(stockValue: Column, quantityAged: Column, quantityOnHand: Column, regionCode: Column) -> Column:
    """Provision the aged share of the stock value at the regional rate."""
    rate = F.lit(DEFAULT_OBSOLESCENCE_PROVISION_RATE)
    for region, r in OBSOLESCENCE_PROVISION_RATE.items():
        rate = F.when(regionCode == region, F.lit(r)).otherwise(rate)
    return money(zeroIfNull(stockValue) * agedShare(quantityAged, quantityOnHand) * rate)
