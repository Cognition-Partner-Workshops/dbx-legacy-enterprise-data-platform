"""SLS_Load_QuotaAttainment: attainment by territory against the region's own
definition of bookings - NA invoiced revenue (incl. tax), EU net revenue after
credit notes, APAC order intake - unioned into Aggregate.Regional Sales Performance.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_common import MONEY, resolveColumns

TERRITORY_COLUMNS = {
    "TerritoryCode": ["TerritoryCode", "SalesTerritoryCode"],
    "RegionCode": ["RegionCode"],
    "IsActive": ["IsActive"],
}
QUOTA_COLUMNS = {
    "TerritoryCode": ["TerritoryCode", "SalesTerritoryCode"],
    "QuotaPeriod": ["QuotaPeriod"],
    "QuotaAmount": ["QuotaAmount"],
}
SALE_LINE_COLUMNS = {
    "InvoiceNumber": ["SaleBusinessKey", "InvoiceNumber"],
    "InvoiceDate": ["InvoiceDate"],
    "TerritoryCode": ["TerritoryCode", "SalesTerritoryCode"],
    "RegionCode": ["RegionCode"],
    "ExtendedPrice": ["ExtendedPrice", "NetLineAmount"],
    "TaxAmount": ["TaxAmount"],
    "NetAmount": ["NetAmount", "NetLineAmount"],
}
CREDIT_NOTE_COLUMNS = {
    "InvoiceNumber": ["InvoiceNumber", "AppliedToSaleBusinessKey", "OriginalSaleBusinessKey"],
    "CreditAmount": ["CreditAmount", "NetAmount"],
}
ORDER_LINE_COLUMNS = {
    "TerritoryCode": ["TerritoryCode", "SalesTerritoryCode"],
    "OrderDate": ["OrderDate"],
    "OrderLineAmount": ["OrderLineAmount", "NetLineAmount"],
}
FISCAL_CALENDAR_COLUMNS = {
    "CalendarDate": ["CalendarDate", "Date"],
    "FiscalPeriod445": ["FiscalPeriod445", "FiscalPeriod"],
}


def legacyTerritories(df: DataFrame) -> DataFrame:
    return resolveColumns(df, TERRITORY_COLUMNS)


def legacyQuotas(df: DataFrame) -> DataFrame:
    return resolveColumns(df, QUOTA_COLUMNS)


def legacySaleLines(df: DataFrame) -> DataFrame:
    return resolveColumns(df, SALE_LINE_COLUMNS)


def legacyCreditNotes(df: DataFrame) -> DataFrame:
    return resolveColumns(df, CREDIT_NOTE_COLUMNS)


def legacyOrderLines(df: DataFrame) -> DataFrame:
    return resolveColumns(df, ORDER_LINE_COLUMNS)


def legacyFiscalCalendar(df: DataFrame) -> DataFrame:
    return resolveColumns(df, FISCAL_CALENDAR_COLUMNS)


def countTerritoriesWithoutQuota(territories: DataFrame, quotas: DataFrame, attainmentPeriod: str) -> int:
    active = territories.where(F.col("IsActive").cast("boolean")).select("TerritoryCode")
    quoted = quotas.where(F.col("QuotaPeriod") == attainmentPeriod).select("TerritoryCode").distinct()
    return active.join(quoted, "TerritoryCode", "left_anti").count()


def _territoryQuotas(territories: DataFrame, quotas: DataFrame, regionCode: str, attainmentPeriod: str) -> DataFrame:
    t = territories.where(F.col("RegionCode") == regionCode).select("TerritoryCode")
    q = quotas.where(F.col("QuotaPeriod") == attainmentPeriod).select("TerritoryCode", "QuotaPeriod", "QuotaAmount")
    return t.join(q, "TerritoryCode", "inner")


def buildAttainmentByRegion(territories: DataFrame, quotas: DataFrame, saleLines: DataFrame,
                            creditNotes: DataFrame, orderLines: DataFrame, fiscalCalendar: DataFrame,
                            attainmentPeriod: str) -> DataFrame:
    """'Build Attainment By Region' - the three UNION ALL branches of the legacy INSERT."""
    monthOf = F.date_format(F.col("InvoiceDate"), "yyyy-MM")

    # NA: invoiced revenue including sales tax, calendar month of the invoice.
    tq = _territoryQuotas(territories, quotas, "NA", attainmentPeriod)
    na = (saleLines.where(F.col("RegionCode") == "NA")
                   .select("TerritoryCode", monthOf.alias("QuotaPeriod"),
                           (F.col("ExtendedPrice") + F.col("TaxAmount")).alias("amt")))
    naAgg = na.groupBy("TerritoryCode", "QuotaPeriod").agg(F.sum("amt").alias("ActualAmount"))
    naRows = (tq.join(naAgg, ["TerritoryCode", "QuotaPeriod"], "left")
                .select("TerritoryCode", F.lit("NA").alias("RegionCode"), "QuotaPeriod", "QuotaAmount",
                        F.coalesce(F.col("ActualAmount"), F.lit(0)).cast(MONEY).alias("ActualAmount"),
                        F.lit("INVOICED").alias("MeasureBasisCode")))

    # EU: net revenue less credit notes matched on the invoice number
    # (SUM over the sale-line x credit-note join, exactly as the legacy query).
    tq = _territoryQuotas(territories, quotas, "EU", attainmentPeriod)
    eu = (saleLines.where(F.col("RegionCode") == "EU")
                   .select("TerritoryCode", monthOf.alias("QuotaPeriod"), "InvoiceNumber", "NetAmount"))
    cn = creditNotes.select(F.col("InvoiceNumber").alias("cn_InvoiceNumber"), "CreditAmount")
    eu = eu.join(cn, eu["InvoiceNumber"] == cn["cn_InvoiceNumber"], "left")
    euAgg = eu.groupBy("TerritoryCode", "QuotaPeriod").agg(F.sum("NetAmount").alias("net"),
                                                            F.sum("CreditAmount").alias("credits"))
    euRows = (tq.join(euAgg, ["TerritoryCode", "QuotaPeriod"], "left")
                .select("TerritoryCode", F.lit("EU").alias("RegionCode"), "QuotaPeriod", "QuotaAmount",
                        (F.coalesce(F.col("net"), F.lit(0)) - F.coalesce(F.col("credits"), F.lit(0)))
                        .cast(MONEY).alias("ActualAmount"),
                        F.lit("NET_OF_CREDITS").alias("MeasureBasisCode")))

    # APAC: order intake. The legacy LEFT JOIN to stg.FiscalCalendar445 restricts nothing
    # (the calendar is joined but not filtered on), so the sum covers every order line of
    # the territory. Reproduced as-is; see the mapping doc "needs decision".
    tq = _territoryQuotas(territories, quotas, "APAC", attainmentPeriod)
    ol = orderLines.select("TerritoryCode", F.col("OrderDate").cast("date").alias("OrderDate"), "OrderLineAmount")
    cal = fiscalCalendar.select(F.col("CalendarDate").cast("date").alias("cal_Date"), "FiscalPeriod445")
    ol = ol.join(cal, (ol["OrderDate"] == cal["cal_Date"]) & (cal["FiscalPeriod445"] == F.lit(attainmentPeriod)), "left")
    olAgg = ol.groupBy("TerritoryCode").agg(F.sum("OrderLineAmount").alias("ActualAmount"))
    apacRows = (tq.join(olAgg, ["TerritoryCode"], "left")
                  .select("TerritoryCode", F.lit("APAC").alias("RegionCode"), "QuotaPeriod", "QuotaAmount",
                          F.coalesce(F.col("ActualAmount"), F.lit(0)).cast(MONEY).alias("ActualAmount"),
                          F.lit("ORDER_INTAKE").alias("MeasureBasisCode")))

    return naRows.unionByName(euRows).unionByName(apacRows)


def deriveAttainmentMetrics(work: DataFrame) -> DataFrame:
    """Derived column 'Derive Attainment Metrics'."""
    pct = F.col("ActualAmount") * 100 / F.col("QuotaAmount")
    noQuota = F.col("QuotaAmount") == 0
    return (work.withColumn("AttainmentPercent", F.when(noQuota, F.lit(0)).otherwise(pct).cast(MONEY))
                .withColumn("AttainmentBandCode",
                            F.when(noQuota, "NOQUOTA")
                             .when(pct >= 120, "OVER120")
                             .when(pct >= 100, "AT")
                             .when(pct >= 80, "NEAR")
                             .otherwise("UNDER")))


def toRegionalSalesPerformance(metrics: DataFrame, batchId: int) -> DataFrame:
    """Add the Aggregate.Regional Sales Performance period columns derivable from QuotaPeriod
    ('YYYY-MM' calendar month, or 'YYYY-Pnn' 4-4-5 period for APAC)."""
    isMonth = F.col("QuotaPeriod").rlike(r"^\d{4}-\d{2}$")
    isP445 = F.col("QuotaPeriod").rlike(r"^\d{4}-P\d{2}$")
    return (metrics
            .withColumn("FiscalYear", F.substring(F.col("QuotaPeriod"), 1, 4).cast("int"))
            .withColumn("FiscalPeriod",
                        F.when(isMonth, F.substring(F.col("QuotaPeriod"), 6, 2).cast("int"))
                         .when(isP445, F.substring(F.col("QuotaPeriod"), 7, 2).cast("int")))
            .withColumn("CalendarMonth",
                        F.when(isMonth, F.to_date(F.concat(F.col("QuotaPeriod"), F.lit("-01")))))
            .withColumn("RefreshBatchId", F.lit(int(batchId)).cast("long"))
            .withColumn("RefreshedDatetime", F.current_timestamp()))
