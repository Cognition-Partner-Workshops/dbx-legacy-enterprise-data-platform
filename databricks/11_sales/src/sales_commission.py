"""Regional commission rules (SLS_NA / SLS_EU / SLS_APAC_Load_Commission).

The three legacy packages were never merged because payroll could not agree on
a common accrual month; here they share one parameterised implementation but
each region keeps its own rules, selected by RegionCode:

  NA   - USD only; commission on ExtendedPrice + TaxAmount (gross, tax included);
         calendar-month period; accelerator above the plan threshold; house
         accounts paid at HouseAccountRatePercent of the plan rate.
  EU   - commission on the VAT-exclusive net amount (NetAmount, else back the
         VAT out of the gross, else gross - VatAmount); converted to EUR at the
         month-end AVERAGE rate; statutory per-country cap; cash-basis
         countries (CashBasisCountries) are held until a CLEARED payment.
  APAC - GST-exclusive amount; 4-4-5 period from stg.FiscalCalendar445;
         converted to the plan currency at the AVERAGE rate (missing rate ->
         reject); team-selling split when TeamSplitEnabled.

All functions take DataFrames already carrying the legacy column names (see
saleLineColumnMap / commissionPlanColumnMap) and return DataFrames.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from sales_common import MONEY, RATE, SAMPLE_LINE_TYPES, legacyColumnCandidates, resolveColumns

REGION_CODES = ("NA", "EU", "APAC")
FAR_FUTURE = "9999-12-31"

# Legacy generator column -> candidates in silver.stg_sale_line
# (sqlserver/staging/tables/22_stg_tables_sales.sql names first, generator names second).
_SALE_LINE_COMMON = {
    "SaleLineId": ["SaleLineBusinessKey", "SaleLineId", "StagingSaleLineId"],
    "InvoiceNumber": ["SaleBusinessKey", "InvoiceNumber"],
    "InvoiceDate": ["InvoiceDate"],
    "SalespersonPersonId": ["SalespersonPersonId", "SalespersonBusinessKey", "SalespersonPersonID"],
    "CustomerId": ["CustomerId", "CustomerBusinessKey", "CustomerID"],
    "StockItemId": ["StockItemId", "StockItemBusinessKey", "StockItemID"],
    "TerritoryCode": ["TerritoryCode", "SalesTerritoryCode"],
    "CountryCode": ["CountryCode"],
    "CurrencyCode": ["TransactionCurrencyCode", "CurrencyCode"],
    "QuantitySold": ["Quantity", "QuantitySold"],
    "TaxAmount": ["TaxAmount"],
    "LineProfit": ["LineProfitAmount", "LineProfit"],
    "RegionCode": ["RegionCode"],
    "LoadBatchId": ["BatchId", "LoadBatchId"],
    "LineTypeCode": ["LineTypeCode"],
}


def saleLineColumnMap(regionCode: str) -> dict:
    """ExtendedPrice is tax-exclusive in NA (NA adds TaxAmount to reach the gross) but
    tax-inclusive in EU/APAC (they back the VAT/GST out of it)."""
    columns = dict(_SALE_LINE_COMMON)
    if regionCode == "NA":
        columns["ExtendedPrice"] = ["ExtendedPrice", "NetLineAmount"]
    else:
        columns["ExtendedPrice"] = ["ExtendedPrice", "GrossLineAmount"]
    if regionCode == "EU":
        columns["VatAmount"] = ["VatAmount", "TaxAmount"]
        columns["VatRatePercent"] = ["VatRatePercent", "TaxRatePercent"]
        columns["NetAmount"] = ["NetAmount", "NetLineAmount"]
    if regionCode == "APAC":
        columns["GstAmount"] = ["GstAmount", "TaxAmount"]
    return columns


SALE_LINE_OPTIONAL = ("LineTypeCode", "CountryCode", "StockItemId", "LineProfit", "QuantitySold")

COMMISSION_PLAN_COLUMNS = {
    "SalespersonPersonId": ["SalespersonPersonId", "SalespersonBusinessKey"],
    "RegionCode": ["RegionCode"],
    "PlanCode": ["PlanCode"],
    "BaseRatePercent": ["BaseRatePercent"],
    "AcceleratorRatePercent": ["AcceleratorRatePercent"],
    "AcceleratorThresholdAmount": ["AcceleratorThresholdAmount"],
    "StatutoryCapAmount": ["StatutoryCapAmount"],
    "PlanCurrencyCode": ["PlanCurrencyCode"],
    "TeamSplitPercent": ["TeamSplitPercent"],
    "EffectiveFrom": ["EffectiveFrom", "EffectiveFromDate"],
    "EffectiveTo": ["EffectiveTo", "EffectiveToDate"],
}
COMMISSION_PLAN_OPTIONAL = ("AcceleratorRatePercent", "AcceleratorThresholdAmount",
                            "StatutoryCapAmount", "PlanCurrencyCode", "TeamSplitPercent")

# stg.FxRate: generator names -> 21_stg_tables_finance.sql names
FX_RATE_COLUMNS = {
    "CurrencyCode": ["FromCurrencyCode", "CurrencyCode", "BaseCurrencyCode"],
    "QuoteCurrencyCode": ["ToCurrencyCode", "QuoteCurrencyCode"],
    "RateTypeCode": ["RateTypeCode"],
    "RateDate": ["RateDate"],
    "ConversionRate": ["ConversionRate", "Rate"],
}

FISCAL_CALENDAR_COLUMNS = {
    "CalendarDate": ["CalendarDate", "Date"],
    "FiscalPeriod445": ["FiscalPeriod445", "FiscalPeriod"],
    "FiscalYear445": ["FiscalYear445", "FiscalYear"],
    "FiscalWeek445": ["FiscalWeek445", "FiscalWeek"],
}

CUSTOMER_PAYMENT_COLUMNS = {
    "InvoiceNumber": ["InvoiceNumber", "SaleBusinessKey"],
    "PaymentDate": ["PaymentDate"],
    "PaymentStatusCode": ["PaymentStatusCode"],
}

DIM_CUSTOMER_HOUSE_ACCOUNT_COLUMNS = {
    "CustomerId": legacyColumnCandidates("WWI Customer ID") + ["CustomerId", "SourceCustomerReference"],
    "IsHouseAccount": legacyColumnCandidates("Is House Account"),
    "ValidTo": legacyColumnCandidates("Valid To"),
}


# ---------------------------------------------------------------------------
# source shaping
# ---------------------------------------------------------------------------
def legacySaleLines(stgSaleLine: DataFrame, regionCode: str) -> DataFrame:
    return resolveColumns(stgSaleLine, saleLineColumnMap(regionCode), optional=SALE_LINE_OPTIONAL)


def legacyCommissionPlans(stgCommissionPlan: DataFrame) -> DataFrame:
    return resolveColumns(stgCommissionPlan, COMMISSION_PLAN_COLUMNS, optional=COMMISSION_PLAN_OPTIONAL)


def legacyFxRates(stgFxRate: DataFrame) -> DataFrame:
    return resolveColumns(stgFxRate, FX_RATE_COLUMNS)


def legacyFiscalCalendar(stgFiscalCalendar445: DataFrame) -> DataFrame:
    return resolveColumns(stgFiscalCalendar445, FISCAL_CALENDAR_COLUMNS)


def legacyCustomerPayments(stgCustomerPayment: DataFrame) -> DataFrame:
    return resolveColumns(stgCustomerPayment, CUSTOMER_PAYMENT_COLUMNS)


def regionSaleLines(saleLines: DataFrame, regionCode: str) -> DataFrame:
    """WHERE sl.RegionCode = <region> AND sl.LineTypeCode NOT IN ('SAMPLE','INTERNAL') (+ USD for NA).
    The batch filter is applied by the caller (sales_common.batchFilter)."""
    df = saleLines.where(F.col("RegionCode") == F.lit(regionCode))
    # NOT IN with a NULL LineTypeCode is FALSE in T-SQL, so NULL line types are excluded too.
    df = df.where(~F.coalesce(F.col("LineTypeCode"), F.lit("")).isin(*SAMPLE_LINE_TYPES)
                  & F.col("LineTypeCode").isNotNull()) if "LineTypeCode" in df.columns else df
    if regionCode == "NA":
        df = df.where(F.col("CurrencyCode") == F.lit("USD"))
    return df


def joinCommissionPlans(saleLines: DataFrame, plans: DataFrame, regionCode: str) -> DataFrame:
    """INNER JOIN stg.CommissionPlan ON rep AND region AND InvoiceDate BETWEEN EffectiveFrom/To."""
    cp = plans.where(F.col("RegionCode") == F.lit(regionCode)).drop("RegionCode")
    cp = cp.withColumnRenamed("SalespersonPersonId", "cp_SalespersonPersonId")
    cond = (
        (cp["cp_SalespersonPersonId"] == saleLines["SalespersonPersonId"])
        & (saleLines["InvoiceDate"] >= cp["EffectiveFrom"])
        & (saleLines["InvoiceDate"] <= F.coalesce(cp["EffectiveTo"], F.lit(FAR_FUTURE).cast("date")))
    )
    return saleLines.join(cp, cond, "inner").drop("cp_SalespersonPersonId", "EffectiveFrom", "EffectiveTo")


def countUnplannedReps(saleLines: DataFrame, plans: DataFrame, regionCode: str) -> int:
    """'Find Reps Without A Plan': reps with region rows but no plan row in that region at all."""
    reps = saleLines.where(F.col("RegionCode") == F.lit(regionCode)).select("SalespersonPersonId").distinct()
    planned = plans.where(F.col("RegionCode") == F.lit(regionCode)).select("SalespersonPersonId").distinct()
    return reps.join(planned, "SalespersonPersonId", "left_anti").count()


def commissionPeriodMonth(col):
    """CONVERT(char(7), InvoiceDate, 126) -> 'YYYY-MM'."""
    return F.date_format(col, "yyyy-MM")


# ---------------------------------------------------------------------------
# NA
# ---------------------------------------------------------------------------
def currentHouseAccountFlags(dimCustomer: DataFrame) -> DataFrame | None:
    """Lookup 'House Account Flag': current Dimension.Customer rows ([Valid To] > now)."""
    lower = {c.lower() for c in dimCustomer.columns}
    if not any(c.lower() in lower for c in DIM_CUSTOMER_HOUSE_ACCOUNT_COLUMNS["IsHouseAccount"]):
        return None  # Dimension.Customer DDL in the repo has no [Is House Account]; caller logs a warning
    df = resolveColumns(dimCustomer, DIM_CUSTOMER_HOUSE_ACCOUNT_COLUMNS, optional=("ValidTo",))
    if not isinstance(df.schema["ValidTo"].dataType, T.NullType):
        df = df.where(F.col("ValidTo").isNull() | (F.col("ValidTo") > F.current_timestamp()))
    return (df.select(F.col("CustomerId").cast("string").alias("CustomerId"),
                      F.col("IsHouseAccount").cast("boolean").alias("IsHouseAccount"))
              .dropDuplicates(["CustomerId"]))


def computeNaCommission(planned: DataFrame, houseAccounts: DataFrame | None,
                        houseAccountRatePercent: int) -> DataFrame:
    df = planned.withColumn("CommissionableAmount",
                            (F.col("ExtendedPrice") + F.col("TaxAmount")).cast(MONEY))
    df = df.withColumn("CommissionPeriod", commissionPeriodMonth(F.col("InvoiceDate")))
    df = df.withColumn("PlanCurrencyCode", F.lit("USD"))
    if houseAccounts is not None:
        df = df.join(houseAccounts.withColumnRenamed("CustomerId", "ha_CustomerId"),
                     df["CustomerId"].cast("string") == F.col("ha_CustomerId"), "left").drop("ha_CustomerId")
    else:
        df = df.withColumn("IsHouseAccount", F.lit(None).cast("boolean"))
    factor = F.lit(houseAccountRatePercent).cast(MONEY) / F.lit(100)
    df = (df.withColumn("BaseCommissionAmount",
                        (F.col("CommissionableAmount") * F.col("BaseRatePercent") / 100).cast(MONEY))
            .withColumn("AcceleratorCommissionAmount",
                        F.when(F.col("CommissionableAmount") > F.col("AcceleratorThresholdAmount"),
                               (F.col("CommissionableAmount") - F.col("AcceleratorThresholdAmount"))
                               * F.col("AcceleratorRatePercent") / 100)
                         .otherwise(F.lit(0)).cast(MONEY))
            .withColumn("HouseAccountFactor",
                        F.when(F.col("IsHouseAccount").isNull(), F.lit(1))
                         .when(F.col("IsHouseAccount"), factor)
                         .otherwise(F.lit(1)).cast(MONEY)))
    df = (df.withColumn("CommissionAmount",
                        ((F.col("BaseCommissionAmount") + F.col("AcceleratorCommissionAmount"))
                         * F.col("HouseAccountFactor")).cast(MONEY))
            .withColumn("RegionCode", F.lit("NA")))
    return df


# ---------------------------------------------------------------------------
# EU
# ---------------------------------------------------------------------------
def netCommissionableAmount():
    return (F.when(F.col("NetAmount").isNotNull(), F.col("NetAmount"))
             .when(F.coalesce(F.col("VatRatePercent"), F.lit(0)) > 0,
                   F.col("ExtendedPrice") / (1 + F.col("VatRatePercent") / 100))
             .otherwise(F.col("ExtendedPrice") - F.coalesce(F.col("VatAmount"), F.lit(0)))
             .cast(MONEY))


def eurMonthAverageRates(fxRates: DataFrame) -> DataFrame:
    """stg.FxRate rows quoting a currency into EUR at the AVERAGE rate."""
    return (fxRates.where((F.col("QuoteCurrencyCode") == "EUR") & (F.col("RateTypeCode") == "AVERAGE"))
                   .select(F.col("CurrencyCode").alias("fx_CurrencyCode"),
                           F.col("RateDate").alias("fx_RateDate"),
                           F.col("ConversionRate").cast(RATE).alias("EurConversionRate"))
                   .dropDuplicates(["fx_CurrencyCode", "fx_RateDate"]))


def computeEuCommission(planned: DataFrame, fxRates: DataFrame, cashBasisCountries: list[str]) -> DataFrame:
    """Returns every EU line with IsCashBasisCountry; the caller splits Accrual / HeldOnCash."""
    fx = eurMonthAverageRates(fxRates)
    df = planned.withColumn("NetCommissionableAmount", netCommissionableAmount())
    df = df.withColumn("CommissionPeriod", commissionPeriodMonth(F.col("InvoiceDate")))
    # LEFT JOIN stg.FxRate ... AND fx.RateDate = EOMONTH(sl.InvoiceDate)
    df = df.join(fx, (fx["fx_CurrencyCode"] == df["CurrencyCode"])
                 & (fx["fx_RateDate"] == F.last_day(df["InvoiceDate"])), "left").drop("fx_CurrencyCode", "fx_RateDate")
    countries = [c.upper() for c in cashBasisCountries]
    df = (df.withColumn("NetAmountEur",
                        F.when(F.col("EurConversionRate").isNull() | (F.col("EurConversionRate") == 0),
                               F.col("NetCommissionableAmount"))
                         .otherwise(F.col("NetCommissionableAmount") * F.col("EurConversionRate").cast(MONEY))
                         .cast(MONEY))
            .withColumn("IsCashBasisCountry",
                        F.upper(F.col("CountryCode")).isin(*countries) if countries else F.lit(False)))
    raw = F.col("NetAmountEur") * F.col("BaseRatePercent") / 100
    df = (df.withColumn("RawCommissionAmount", raw.cast(MONEY))
            .withColumn("CommissionAmount",
                        F.when((F.col("StatutoryCapAmount") > 0) & (raw > F.col("StatutoryCapAmount")),
                               F.col("StatutoryCapAmount"))
                         .otherwise(raw).cast(MONEY))
            .withColumn("RegionCode", F.lit("EU")))
    return df


def splitCashBasis(euLines: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Conditional split 'Split Cash Basis Lines' -> (Accrual, HeldOnCash)."""
    accrual = euLines.where(~F.coalesce(F.col("IsCashBasisCountry"), F.lit(False)))
    held = euLines.where(F.coalesce(F.col("IsCashBasisCountry"), F.lit(False)))
    return accrual, held


def releaseHeldEuLines(held: DataFrame, customerPayments: DataFrame) -> DataFrame:
    """'Release Cash Basis Lines With Payment': held lines whose invoice has a CLEARED payment
    accrue in the payment month. Only the legacy INSERT's column list is populated."""
    cleared = (customerPayments.where(F.col("PaymentStatusCode") == "CLEARED")
                               .select(F.col("InvoiceNumber").alias("p_InvoiceNumber"), "PaymentDate"))
    released = held.join(cleared, held["InvoiceNumber"] == cleared["p_InvoiceNumber"], "inner")
    return released.select(
        "SaleLineId", "SalespersonPersonId",
        commissionPeriodMonth(F.col("PaymentDate")).alias("CommissionPeriod"),
        "NetAmountEur", "CommissionAmount", "RegionCode", "CountryCode",
    )


def countCappedReps(commissionEu: DataFrame) -> int:
    return (commissionEu.where(F.col("RawCommissionAmount") > F.col("CommissionAmount"))
                        .select("SalespersonPersonId").distinct().count())


# ---------------------------------------------------------------------------
# APAC
# ---------------------------------------------------------------------------
def countMissingCalendarDays(apacLines: DataFrame, fiscalCalendar: DataFrame) -> int:
    """'Check 445 Calendar Coverage': distinct invoice dates with no 4-4-5 calendar row."""
    dates = apacLines.select(F.col("InvoiceDate").cast("date").alias("CalendarDate")).distinct()
    return dates.join(fiscalCalendar.select("CalendarDate").distinct(), "CalendarDate", "left_anti").count()


def periodAverageRates(fxRates: DataFrame) -> DataFrame:
    """Lookup 'Period Average Rate' (full-cache lookup keyed on currency pair; the legacy
    query has no date filter so the newest rate per pair is used)."""
    ranked = (fxRates.where(F.col("RateTypeCode") == "AVERAGE")
                     .withColumn("_rn", F.row_number().over(
                         Window.partitionBy("CurrencyCode", "QuoteCurrencyCode")
                               .orderBy(F.col("RateDate").desc_nulls_last()))))
    return (ranked.where(F.col("_rn") == 1)
                  .select(F.col("CurrencyCode").alias("fx_CurrencyCode"),
                          F.col("QuoteCurrencyCode").alias("fx_PlanCurrencyCode"),
                          F.col("ConversionRate").cast(RATE).alias("ConversionRate")))


def computeApacCommission(planned: DataFrame, fiscalCalendar: DataFrame, fxRates: DataFrame,
                          teamSplitEnabled: bool) -> tuple[DataFrame, DataFrame]:
    """Returns (commission rows, rejected rows without an FX rate)."""
    cal = fiscalCalendar.select(F.col("CalendarDate").alias("cal_Date"),
                                F.col("FiscalPeriod445").alias("CommissionPeriod"),
                                F.col("FiscalYear445").cast("int").alias("CommissionFiscalYear"),
                                F.col("FiscalWeek445").cast("int").alias("CommissionFiscalWeek"))
    df = planned.withColumn("GstExclusiveAmount",
                            (F.col("ExtendedPrice") - F.coalesce(F.col("GstAmount"), F.lit(0))).cast(MONEY))
    df = df.join(cal, df["InvoiceDate"].cast("date") == cal["cal_Date"], "inner").drop("cal_Date")
    fx = periodAverageRates(fxRates)
    df = df.join(fx, (df["CurrencyCode"] == fx["fx_CurrencyCode"])
                 & (df["PlanCurrencyCode"] == fx["fx_PlanCurrencyCode"]), "left").drop("fx_CurrencyCode", "fx_PlanCurrencyCode")
    rejected = (df.where(F.col("ConversionRate").isNull()).drop("ConversionRate")
                  .withColumn("RejectReasonCode", F.lit("FX_RATE_MISSING")))
    matched = df.where(F.col("ConversionRate").isNotNull())
    splitFactor = (F.col("TeamSplitPercent") / 100) if teamSplitEnabled else F.lit(1)
    matched = (matched.withColumn("PlanCurrencyAmount",
                                  (F.col("GstExclusiveAmount") * F.col("ConversionRate").cast(MONEY)).cast(MONEY))
                      .withColumn("SplitFactor", splitFactor.cast(MONEY)))
    matched = (matched.withColumn("CommissionAmount",
                                  (F.col("PlanCurrencyAmount") * F.col("BaseRatePercent") / 100
                                   * F.col("SplitFactor")).cast(MONEY))
                      .withColumn("RegionCode", F.lit("APAC"))
                      .withColumn("IsPeriodBoundaryLine",
                                  F.month(F.col("InvoiceDate"))
                                  != F.substring(F.col("CommissionPeriod"), 7, 2).cast("int")))
    return matched, rejected


def countPeriodBoundaryLines(commissionApac: DataFrame) -> int:
    return commissionApac.where(F.col("IsPeriodBoundaryLine")).count()


# ---------------------------------------------------------------------------
# posting (Integration.usp_PostCommission)
# ---------------------------------------------------------------------------
def postingRows(workRows: DataFrame, regionCode: str, postedCommissionPeriod: str,
                batchId: int, packageExecutionId) -> DataFrame:
    """Rows handed to Integration.usp_PostCommission(@BatchId, @RegionCode, @CommissionPeriod).
    The procedure is not in the repo; the migration posts every work row of the region into
    gold.fact_sale_commission keyed on (RegionCode, SaleLineId, CommissionPeriod)."""
    commissionable = {
        "NA": "CommissionableAmount",
        "EU": "NetAmountEur",
        "APAC": "PlanCurrencyAmount",
    }[regionCode]
    df = workRows
    for optionalColumn in ("CountryCode", "PlanCurrencyCode", "InvoiceNumber", "InvoiceDate",
                           "CustomerId", "TerritoryCode", "PlanCode", commissionable):
        if optionalColumn not in df.columns:
            df = df.withColumn(optionalColumn, F.lit(None))
    return df.select(
        F.lit(regionCode).alias("RegionCode"),
        F.col("SaleLineId").cast("string").alias("SaleLineId"),
        "CommissionPeriod", "InvoiceNumber", "InvoiceDate", "SalespersonPersonId", "CustomerId",
        "TerritoryCode", "CountryCode", "PlanCode", "PlanCurrencyCode",
        F.col(commissionable).cast(MONEY).alias("CommissionableAmount"),
        F.col("CommissionAmount").cast(MONEY).alias("CommissionAmount"),
        F.lit(postedCommissionPeriod).alias("PostedCommissionPeriod"),
        F.lit(int(batchId)).cast("long").alias("BatchId"),
        F.lit(packageExecutionId).cast("long").alias("PackageExecutionId"),
        F.current_timestamp().alias("PostedAtUtc"),
    )
