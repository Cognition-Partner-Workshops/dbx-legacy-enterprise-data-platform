"""Dimension.Date / Dimension.Fiscal Calendar generation.

Replaces the recursive T-SQL DateSpine CTE (OPTION (MAXRECURSION 0)) in REF_Load_DateDimension with
``sequence()`` + ``explode()``. Fiscal year start months come from silver.ref_region exactly as the
legacy package read them from ref.Region, so the region policy remains the only place a year end is
defined. Date bounds are job parameters (DateRangeStart / DateRangeEnd).
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from wwi_ref.schemas import SENTINEL_NOT_APPLICABLE_DATE, SENTINEL_UNKNOWN_DATE

SENTINELS = [
    (-1, SENTINEL_UNKNOWN_DATE, "Unknown", "UNK"),
    (-2, SENTINEL_NOT_APPLICABLE_DATE, "Not Applicable", "N/A"),
]


def fiscalYear(month, year, startMonth, yearEndConvention):
    """SSIS 'Derive Fiscal Calendars':
    NA  : CalendarMonth >= start ? CalendarYear + 1 : CalendarYear   (named by the year it ends in)
    EU  : start == 1 ? CalendarYear : (CalendarMonth >= start ? CalendarYear + 1 : CalendarYear)
    APAC: CalendarMonth >= start ? CalendarYear : CalendarYear - 1   (named by the year it starts in)
    """
    if yearEndConvention == "APAC":
        return F.when(month >= startMonth, year).otherwise(year - 1)
    if yearEndConvention == "EU":
        return F.when(startMonth == 1, year).otherwise(F.when(month >= startMonth, year + 1).otherwise(year))
    return F.when(month >= startMonth, year + 1).otherwise(year)


def fiscalPeriod(month, startMonth):
    return F.when(month >= startMonth, month - startMonth + 1).otherwise(month + 12 - startMonth + 1)


def dateSpine(spark, startDate, endDate):
    return (spark.range(1)
            .select(F.explode(F.sequence(F.lit(str(startDate)).cast("date"), F.lit(str(endDate)).cast("date"),
                                         F.expr("INTERVAL 1 DAY"))).alias("CalendarDate")))


def buildDateDimension(spark, startDate, endDate, fiscalStartMonths, lineageKey, batchId):
    """fiscalStartMonths: {"NA": 7, "EU": 1, "APAC": 4} read from silver.ref_region."""
    na = F.lit(int(fiscalStartMonths["NA"]))
    eu = F.lit(int(fiscalStartMonths["EU"]))
    apac = F.lit(int(fiscalStartMonths["APAC"]))
    d = F.col("CalendarDate")
    month = F.month(d)
    year = F.year(d)
    df = (dateSpine(spark, startDate, endDate)
          .withColumn("CalendarYear", year).withColumn("CalendarMonth", month).withColumn("CalendarDay", F.dayofmonth(d))
          .withColumn("DayOfWeekNumber", F.dayofweek(d))  # Sunday = 1 like DATEPART(weekday) with DATEFIRST 7
          .withColumn("MonthName", F.date_format(d, "MMMM")).withColumn("DayName", F.date_format(d, "EEEE"))
          .withColumn("FiscalYearNa", fiscalYear(month, year, na, "NA"))
          .withColumn("FiscalPeriodNa", fiscalPeriod(month, na))
          .withColumn("FiscalYearEu", fiscalYear(month, year, eu, "EU"))
          .withColumn("FiscalPeriodEu", fiscalPeriod(month, eu))
          .withColumn("FiscalYearApac", fiscalYear(month, year, apac, "APAC"))
          .withColumn("FiscalPeriodApac", fiscalPeriod(month, apac))
          .withColumn("CalendarQuarter", ((month - 1) / 3).cast("int") + 1)
          .withColumn("WeekendFlag", F.when(F.col("DayOfWeekNumber").isin(1, 7), "Y").otherwise("N"))
          .withColumn("WorkingDayFlag", F.when(F.col("DayOfWeekNumber").isin(1, 7), "N").otherwise("Y")))
    # Dimension.Date columns (sqlserver/warehouse/dimensions/Dimension.Date.sql); the corporate fiscal
    # columns (FiscalYear / FiscalMonthNumber) follow the NA calendar, the reporting currency region.
    fyStart = F.make_date(F.when(month >= na, year).otherwise(year - 1), na, F.lit(1))
    out = (df.select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("DateKey"),
        d.alias("Date"),
        F.col("CalendarDay").alias("DayNumber"),
        F.date_format(d, "dd").alias("Day"),
        F.col("DayName").alias("DayOfWeek"),
        F.col("DayOfWeekNumber"),
        F.col("MonthName").alias("Month"),
        F.date_format(d, "MMM").alias("ShortMonth"),
        month.alias("CalendarMonthNumber"),
        F.concat(F.lit("CY"), year.cast("string"), F.lit("-"), F.date_format(d, "MMM")).alias("CalendarMonthLabel"),
        F.col("CalendarQuarter").alias("CalendarQuarterNumber"),
        year.alias("CalendarYear"),
        F.concat(F.lit("CY"), year.cast("string")).alias("CalendarYearLabel"),
        F.col("FiscalPeriodNa").alias("FiscalMonthNumber"),
        F.concat(F.lit("FY"), F.col("FiscalYearNa").cast("string"), F.lit("-"), F.date_format(d, "MMM")).alias("FiscalMonthLabel"),
        F.col("FiscalYearNa").alias("FiscalYear"),
        F.concat(F.lit("FY"), F.col("FiscalYearNa").cast("string")).alias("FiscalYearLabel"),
        F.weekofyear(d).alias("ISOWeekNumber"),
        F.year(F.date_sub(F.next_day(d, "Mon"), 4)).alias("ISOYear"),
        F.dayofyear(d).alias("DayOfYear"),
        F.col("WeekendFlag"), F.col("WorkingDayFlag"),
        F.col("FiscalYearNa"), F.col("FiscalPeriodNa"),
        (((F.col("FiscalPeriodNa") - 1) / 3).cast("int") + 1).alias("FiscalQuarterNa"),
        (F.floor(F.datediff(d, fyStart) / 7) + 1).cast("int").alias("FiscalWeekNa"),
        F.col("FiscalYearEu"), F.col("FiscalPeriodEu"),
        (((F.col("FiscalPeriodEu") - 1) / 3).cast("int") + 1).alias("FiscalQuarterEu"),
        F.weekofyear(d).alias("FiscalWeekEu"),
        F.col("FiscalYearApac"), F.col("FiscalPeriodApac"),
        (((F.col("FiscalPeriodApac") - 1) / 3).cast("int") + 1).alias("FiscalQuarterApac"),
        F.when(month >= 7, year).otherwise(year - 1).alias("FiscalYearApacAu"),
        fiscalPeriod(month, F.lit(7)).alias("FiscalPeriodApacAu"),
        (((fiscalPeriod(month, F.lit(7)) - 1) / 3).cast("int") + 1).alias("FiscalQuarterApacAu"),
        F.lit(False).alias("IsReservedMember"),
        F.lit(lineageKey).alias("LineageKey"),
        F.lit(batchId).cast("bigint").alias("LastLoadBatchId"),
    ))
    return out


def sentinelDateRows(spark, lineageKey, batchId):
    """90_unknown_members.sql: the Date dimension uses sentinel dates rather than negative keys
    (1900-01-01 unknown, 1900-01-02 not applicable); DateKey stays negative so fact lookups can route to it."""
    rows = []
    for key, date, label, short in SENTINELS:
        rows.append((key, date, 0, "00", label, 0, label, short, 0, "CY1900", 0, 1900, "CY1900", 0, label, 1900, "FY1900",
                     0, 1900, 0, "N", "N", 1900, 0, 0, 0, 1900, 0, 0, 0, 1900, 0, 0, 1900, 0, 0, True, lineageKey, batchId))
    cols = ["DateKey", "Date", "DayNumber", "Day", "DayOfWeek", "DayOfWeekNumber", "Month", "ShortMonth",
            "CalendarMonthNumber", "CalendarMonthLabel", "CalendarQuarterNumber", "CalendarYear", "CalendarYearLabel",
            "FiscalMonthNumber", "FiscalMonthLabel", "FiscalYear", "FiscalYearLabel", "ISOWeekNumber", "ISOYear",
            "DayOfYear", "WeekendFlag", "WorkingDayFlag", "FiscalYearNa", "FiscalPeriodNa", "FiscalQuarterNa", "FiscalWeekNa",
            "FiscalYearEu", "FiscalPeriodEu", "FiscalQuarterEu", "FiscalWeekEu", "FiscalYearApac", "FiscalPeriodApac",
            "FiscalQuarterApac", "FiscalYearApacAu", "FiscalPeriodApacAu", "FiscalQuarterApacAu", "IsReservedMember",
            "LineageKey", "LastLoadBatchId"]
    df = spark.createDataFrame(rows, cols)
    return df.withColumn("Date", F.col("Date").cast("date")).withColumn("LastLoadBatchId", F.col("LastLoadBatchId").cast("bigint"))


def buildFiscalCalendar(dateDf, countryDf, regionDf, batchId):
    """Dimension.Fiscal Calendar at country x date grain: the regional fiscal columns of Dimension.Date
    projected through ref.Country -> ref.Region (FiscalCalendarCode / FiscalYearStartMonth). Public-holiday
    columns stay NULL: the legacy estate has no holiday feed (see mapping doc, 'not migrated')."""
    country = (countryDf.where(F.col("IsActive") == True)  # noqa: E712
               .select("CountryCode", "RegionCode")
               .join(regionDf.select("RegionCode", "FiscalCalendarCode", "FiscalYearStartMonth"), "RegionCode", "inner"))
    dates = dateDf.where(~F.col("IsReservedMember"))
    joined = dates.crossJoin(country)
    region = F.col("RegionCode")
    fy = (F.when(region == "EU", F.col("FiscalYearEu")).when(region == "APAC", F.col("FiscalYearApac")).otherwise(F.col("FiscalYearNa")))
    fp = (F.when(region == "EU", F.col("FiscalPeriodEu")).when(region == "APAC", F.col("FiscalPeriodApac")).otherwise(F.col("FiscalPeriodNa")))
    fq = (F.when(region == "EU", F.col("FiscalQuarterEu")).when(region == "APAC", F.col("FiscalQuarterApac")).otherwise(F.col("FiscalQuarterNa")))
    fw = (F.when(region == "EU", F.col("FiscalWeekEu")).otherwise(F.col("FiscalWeekNa")))
    w = Window.partitionBy("CountryCode", "CalendarYear", "CalendarMonthNumber")
    wYear = Window.partitionBy("CountryCode", fy)
    wOrder = Window.partitionBy("CountryCode").orderBy("Date")
    isWorking = F.col("WorkingDayFlag") == "Y"
    out = (joined
           .withColumn("FiscalYear", fy).withColumn("FiscalPeriod", fp).withColumn("FiscalQuarter", fq).withColumn("FiscalWeek", fw)
           .withColumn("PeriodStartDate", F.min("Date").over(w)).withColumn("PeriodEndDate", F.max("Date").over(w))
           .withColumn("IsPeriodEnd", F.col("Date") == F.col("PeriodEndDate"))
           .withColumn("IsYearEnd", F.col("Date") == F.max("Date").over(wYear))
           .withColumn("IsWorkingDay", isWorking)
           .withColumn("WorkingDayOfMonth", F.sum(F.when(isWorking, 1).otherwise(0)).over(w.orderBy("Date")).cast("int"))
           .withColumn("WorkingDayOfYear", F.sum(F.when(isWorking, 1).otherwise(0)).over(wYear.orderBy("Date")).cast("int"))
           .withColumn("WorkingDaysRemainingMonth", (F.sum(F.when(isWorking, 1).otherwise(0)).over(w) - F.col("WorkingDayOfMonth")).cast("int"))
           .withColumn("NextWorkingDate", F.first(F.when(isWorking, F.col("Date")), ignorenulls=True)
                       .over(wOrder.rowsBetween(1, 7)))
           .withColumn("PreviousWorkingDate", F.last(F.when(isWorking, F.col("Date")), ignorenulls=True)
                       .over(wOrder.rowsBetween(-7, -1))))
    return out.select(
        (F.col("DateKey").cast("bigint") * 1000 + F.dense_rank().over(Window.orderBy("CountryCode"))).alias("FiscalCalendarKey"),
        "CountryCode", "Date", "DateKey", "RegionCode", "FiscalCalendarCode", "FiscalYear",
        F.concat(F.lit("FY"), F.col("FiscalYear").cast("string")).alias("FiscalYearLabel"),
        "FiscalQuarter", "FiscalPeriod",
        F.concat(F.lit("FY"), F.col("FiscalYear").cast("string"), F.lit("-P"), F.lpad(F.col("FiscalPeriod").cast("string"), 2, "0")).alias("FiscalPeriodLabel"),
        "FiscalWeek", "PeriodStartDate", "PeriodEndDate", "IsPeriodEnd", "IsYearEnd",
        F.lit(False).alias("IsPublicHoliday"), F.lit(None).cast("string").alias("HolidayName"),
        F.lit(None).cast("string").alias("HolidayScopeCode"), F.lit(None).cast("string").alias("HolidaySubdivisionCode"),
        "IsWorkingDay", F.col("IsWorkingDay").alias("IsWarehouseOperatingDay"), F.col("IsWorkingDay").alias("IsCarrierCollectionDay"),
        "WorkingDayOfMonth", "WorkingDayOfYear", "WorkingDaysRemainingMonth", "NextWorkingDate", "PreviousWorkingDate",
        F.lit(None).cast("string").alias("TaxReturnPeriodLabel"), F.lit(None).cast("date").alias("TaxReturnDueDate"),
        F.lit(None).cast("date").alias("StatutoryCloseDueDate"),
        F.lit("REF").alias("SourceSystemCode"), F.lit(batchId).cast("bigint").alias("LastLoadBatchId"))
