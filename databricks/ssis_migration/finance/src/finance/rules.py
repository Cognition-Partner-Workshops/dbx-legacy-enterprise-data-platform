"""Pure, column-level finance rules ported from the Oracle functions and SSIS expressions.

Every function returns a pyspark Column so the rules can be applied to any
DataFrame and unit-tested on a local SparkSession.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

DEFAULT_TOLERANCE = 0.005  # whole-journal balance tolerance (Fact.GL Posting gate)


def fiscalPeriodArithmetic(dateCol: Column, regionCol: Column) -> Column:
    """WWI_REF.FN_FISCAL_PERIOD fallback: APAC April year-start, NA ISO-week 4-4-5, else calendar."""
    month = F.month(dateCol)
    year = F.year(dateCol)
    apacYear = year + F.when(month >= 4, 1).otherwise(0)
    apacPeriod = ((month + 8) % 12) + 1
    naPeriod = F.least(F.ceil(F.weekofyear(dateCol) / F.lit(4.333)), F.lit(12))
    fy = F.when(F.upper(regionCol) == "APAC", apacYear).otherwise(year)
    period = (
        F.when(F.upper(regionCol) == "APAC", apacPeriod)
        .when(F.upper(regionCol) == "NA", naPeriod)
        .otherwise(month)
    )
    return F.when(dateCol.isNull(), F.lit(None).cast("string")).otherwise(
        F.concat(fy.cast("string"), F.lit("-"), F.lpad(period.cast("string"), 2, "0"))
    )


def fiscalPeriod(dateCol: Column, regionCol: Column, calendarPeriodCol: Column) -> Column:
    """Calendar lookup first (only well-formed YYYY-NN codes), arithmetic fallback second."""
    wellFormed = calendarPeriodCol.rlike(r"^\d{4}-\d{2}$")
    return F.when(wellFormed, calendarPeriodCol).otherwise(fiscalPeriodArithmetic(dateCol, regionCol))


def oracleAgingBucket(daysCol: Column, regionCol: Column) -> Column:
    """WWI_FIN.FN_AGING_BUCKET (regional bucket codes)."""
    days = F.coalesce(daysCol, F.lit(0))
    region = F.upper(regionCol)
    notDue = F.when(region == "EU", "NOT_DUE").otherwise("CURRENT")
    eu = (
        F.when(days <= 30, "D01_30")
        .when(days <= 60, "D31_60")
        .when(days <= 90, "D61_90")
        .otherwise("D90_PLUS")
    )
    apac = (
        F.when(days <= 15, "D01_15")
        .when(days <= 30, "D16_30")
        .when(days <= 45, "D31_45")
        .when(days <= 60, "D46_60")
        .otherwise("D60_PLUS")
    )
    na = (
        F.when(days <= 30, "B1_1_30")
        .when(days <= 60, "B2_31_60")
        .when(days <= 90, "B3_61_90")
        .when(days <= 120, "B4_91_120")
        .otherwise("B5_120_PLUS")
    )
    return F.when(days <= 0, notDue).when(region == "EU", eu).when(region == "APAC", apac).otherwise(na)


def ssisAgingBucket(daysPastDue: Column) -> Column:
    """FIN_Load_ApAging bucket codes (CURRENT / B030 / B060 / B090 / B090P)."""
    return (
        F.when(daysPastDue <= 0, "CURRENT")
        .when(daysPastDue <= 30, "B030")
        .when(daysPastDue <= 60, "B060")
        .when(daysPastDue <= 90, "B090")
        .otherwise("B090P")
    )


def agingBucketSort(bucket: Column) -> Column:
    return (
        F.when(bucket == "CURRENT", 0)
        .when(bucket == "B030", 1)
        .when(bucket == "B060", 2)
        .when(bucket == "B090", 3)
        .otherwise(4)
    )


def taxRegime(regionCol: Column) -> Column:
    """SSIS STG_Load_ApInvoice: EU=VAT, APAC=GST, everything else SUT (sales & use tax)."""
    r = F.upper(regionCol)
    return F.when(r == "EU", "VAT").when(r == "APAC", "GST").otherwise("SUT")


def ledgerTaxRegime(ledgerCol: Column) -> Column:
    """FIN_Load_GlPostings: EU% ledgers VAT, APAC% GST, else SALESTAX."""
    return F.when(ledgerCol.like("EU%"), "VAT").when(ledgerCol.like("APAC%"), "GST").otherwise("SALESTAX")


def residualTolerance(regionCol: Column, remainingCol: Column) -> Column:
    """work.usp_MatchPaymentsToInvoices pass-3 tolerance: NA 0.02, EU 0.01, APAC 0.5% of remaining."""
    r = F.upper(regionCol)
    return F.when(r == "NA", F.lit(0.02)).when(r == "EU", F.lit(0.01)).otherwise(F.abs(remainingCol) * 0.005)


def closeTolerance(regionCol: Column) -> Column:
    """Integration.usp_RefreshAggregateFinanceClose materiality: NA 500, EU 250, else 1000."""
    r = F.upper(regionCol)
    return F.when(r == "NA", F.lit(500.0)).when(r == "EU", F.lit(250.0)).otherwise(F.lit(1000.0))


def paymentStatus(sourceStatus: Column, voidDate: Column) -> Column:
    """STG_Load_Payment: paid / void / pending.  Oracle codes: CLRD paid, VOID (or void date) void, rest pending."""
    s = F.upper(F.trim(sourceStatus))
    return (
        F.when(voidDate.isNotNull() | (s == "VOID") | (s == "V"), "VOID")
        .when((s == "CLRD") | (s == "P") | (s == "PAID"), "PAID")
        .otherwise("PEND")
    )


def valueDate(valueDt: Column, paymentDt: Column, regionCol: Column) -> Column:
    """EU settles through SEPA at T+2, everywhere else at T+1, unless the source supplies a value date."""
    default = F.when(F.upper(regionCol) == "EU", F.date_add(paymentDt, 2)).otherwise(F.date_add(paymentDt, 1))
    return F.coalesce(valueDt, default)


def settlementStatus(
    voidDt: Column, regionCol: Column, paymentDt: Column, clearedDt: Column, asOf: Column
) -> Column:
    """WWI_FIN.V_AP_PAYMENT_EXTRACT.SETTLEMENT_STATUS_CD."""
    return (
        F.when(voidDt.isNotNull(), "VOID")
        .when((F.upper(regionCol) == "EU") & (paymentDt <= F.date_sub(asOf, 2)), "SETTLED")
        .when(clearedDt.isNotNull(), "SETTLED")
        .otherwise("IN_FLIGHT")
    )


def dueDate(
    baseDt: Column,
    termBasis: Column,
    netDays: Column,
    dayOfMonth: Column,
    monthsFwd: Column,
    regionCol: Column,
) -> Column:
    """WWI_FIN.FN_DUE_DATE: PREPAY / DOM / EOM / EU day-of-month / net days, then regional snapping."""
    region = F.upper(regionCol)
    months = F.coalesce(monthsFwd, F.lit(1)).cast("int")
    dom = F.coalesce(dayOfMonth, F.lit(1)).cast("int")
    net = F.coalesce(netDays, F.lit(30)).cast("int")
    domDue = F.least(
        F.date_add(F.add_months(F.trunc(baseDt, "MM"), months), dom - 1),
        F.last_day(F.add_months(baseDt, months)),
    )
    eomDue = F.last_day(F.add_months(baseDt, months))
    euBase = F.date_add(F.last_day(baseDt), F.coalesce(netDays, F.lit(0)).cast("int"))
    euDue = F.least(F.date_add(F.trunc(euBase, "MM"), dayOfMonth.cast("int") - 1), F.last_day(euBase))
    raw = (
        F.when(termBasis == "PREPAY", baseDt)
        .when(termBasis == "DOM", domDue)
        .when(termBasis == "EOM", eomDue)
        .when((region == "EU") & dayOfMonth.isNotNull(), euDue)
        .otherwise(F.date_add(baseDt, net))
    )
    dow = F.dayofweek(raw)  # 1 = Sunday, 7 = Saturday
    na = F.when(dow == 7, F.date_add(raw, 2)).when(dow == 1, F.date_add(raw, 1)).otherwise(raw)
    eu = F.when(dow == 7, F.date_sub(raw, 1)).when(dow == 1, F.date_sub(raw, 2)).otherwise(raw)
    apac = F.when(F.dayofmonth(raw) <= 15, F.date_add(F.trunc(raw, "MM"), 14)).otherwise(F.last_day(raw))
    return F.when(baseDt.isNull(), F.lit(None).cast("date")).otherwise(
        F.when(region == "NA", na).when(region == "EU", eu).when(region == "APAC", apac).otherwise(raw)
    )


def withholdingAmount(
    regionCol: Column,
    lineAmount: Column,
    serviceCategory: Column,
    supplierTaxRegistration: Column,
    withholdingRate: Column,
    treatyRate: Column,
    thresholdAmount: Column,
) -> Column:
    """FIN_Load_WithholdingTax regional withholding rule."""
    r = F.upper(regionCol)
    rate = F.coalesce(withholdingRate, F.lit(0.0))
    treaty = F.coalesce(treatyRate, withholdingRate)
    hasRegistration = supplierTaxRegistration.isNotNull() & (F.trim(supplierTaxRegistration) != "")
    return (
        F.when((r == "NA") & serviceCategory.isin("CONS", "LEGL", "MEDI", "RENT"), lineAmount * rate / 100)
        .when((r == "EU") & hasRegistration, lineAmount * treaty / 100)
        .when(r == "EU", lineAmount * rate / 100)
        .when(
            (r == "APAC") & (lineAmount >= F.coalesce(thresholdAmount, F.lit(0.0))), lineAmount * rate / 100
        )
        .otherwise(F.lit(0.0))
    )


def reportableAgingAmount(
    regionCol: Column, invoiceAmt: Column, recoverableVat: Column, gstCredit: Column
) -> Column:
    """FIN_Load_ApAging: EU nets recoverable VAT, APAC nets GST input credit, NA gross."""
    r = F.upper(regionCol)
    return (
        F.when(r == "EU", invoiceAmt - F.coalesce(recoverableVat, F.lit(0.0)))
        .when(r == "APAC", invoiceAmt - F.coalesce(gstCredit, F.lit(0.0)))
        .otherwise(invoiceAmt)
    )


def signedAmount(debit: Column, credit: Column) -> Column:
    return F.coalesce(debit, F.lit(0.0)) - F.coalesce(credit, F.lit(0.0))


def postingSide(debit: Column, credit: Column) -> Column:
    return F.when(F.coalesce(debit, F.lit(0.0)) > 0, "DR").otherwise("CR")


def rowHash(*cols: Column) -> Column:
    """Order-independent-safe per-row hash over business columns (all cast to string, nulls marked)."""
    return F.sha2(F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit("<null>")) for c in cols]), 256)


def revaluationQuoteCurrency(regionCol: Column, entityCurrency: Column) -> Column:
    """FIN_Currency_Revaluation: EU revalues to EUR, APAC to the entity currency, NA to USD."""
    r = F.upper(regionCol)
    return F.when(r == "EU", F.lit("EUR")).when(r == "APAC", entityCurrency).otherwise(F.lit("USD"))


def revaluationRateType(accountType: Column) -> Column:
    """P&L accounts revalue at the AVERAGE rate, balance-sheet accounts at CLOSING."""
    return F.when(accountType.isin("REV", "EXP"), "AVERAGE").otherwise("CLOSING")
