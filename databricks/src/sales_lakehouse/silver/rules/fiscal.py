"""Fiscal-calendar rules: which region's calendar a transaction falls in.

Spec (docs/domain-model/business-domains.md, "Fiscal calendar"):

    The fiscal year does not start in the same month in all three regions,
    and the period boundaries are not the same kind of thing:
    `oracle/reference/11_fiscal_calendars_and_periods.sql` holds all three,
    and `Dimension.Fiscal Calendar` carries them into the warehouse.
    `Fact.Sale` and the finance facts carry `[Fiscal Year]` and
    `[Fiscal Period]` per row, resolved by the region of the transaction
    rather than by the region of the reader. A single invoice date therefore
    lands in three different periods depending on which region's books it is
    in, and any cross-region period comparison has to choose a convention.
    The aggregates choose the NA convention, silently.

Calendar codes are the OLTP ``Sales.SalesTerritories.FiscalCalendarCode``
values (``NA445`` / ``EUCAL`` / ``APACJUN``, sqlserver/oltp/08_seed/
8000_seed_sales_reference.sql). The period arithmetic reproduces
``Integration.usp_PopulateDateDimension`` (sqlserver/procedures/dimensions)
and the arithmetic fallback of ``WWI_REF.FN_FISCAL_PERIOD`` (oracle/functions):

- ``NA445``  4-4-5 retail calendar, 52/53 weeks. The year is named after the
  calendar year it mostly falls in, starts on the Sunday nearest 1 January
  and ends on the Saturday nearest 31 December; week 53 belongs to period 12.
- ``EUCAL``  calendar months, January to December, ISO weeks.
- ``APACJUN`` April to March, twelve calendar-month periods. The fiscal year
  is named after the year it *closes* in (SSIS ``FACT_APAC_Load_Sale``
  ``FiscalYearLabel`` and ``FN_FISCAL_PERIOD``: ``YEAR + 1`` when month >= 4).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.silver.rules.dq import tagDq

CALENDAR_NA = "NA445"
CALENDAR_EU = "EUCAL"
CALENDAR_APAC = "APACJUN"
CALENDAR_CODES: tuple[str, ...] = (CALENDAR_NA, CALENDAR_EU, CALENDAR_APAC)
REGION_CALENDARS: dict[str, str] = {"NA": CALENDAR_NA, "EU": CALENDAR_EU, "APAC": CALENDAR_APAC}

DIM_FISCAL_CALENDAR = "dim_fiscal_calendar"

OUTPUT_COLUMNS: tuple[str, ...] = ("fiscal_calendar_code", "fiscal_year", "fiscal_period", "fiscal_period_key")


def _asColumn(col: str | Column) -> Column:
    return F.col(col) if isinstance(col, str) else col


def regionCalendarCode(regionCol: str | Column) -> Column:
    """NA -> NA445, EU -> EUCAL, APAC -> APACJUN; anything else -> NULL."""
    region = F.upper(F.trim(_asColumn(regionCol)))
    expr = F.when(region == "NA", F.lit(CALENDAR_NA))
    expr = expr.when(region == "EU", F.lit(CALENDAR_EU))
    expr = expr.when(region == "APAC", F.lit(CALENDAR_APAC))
    return expr.otherwise(F.lit(None).cast("string"))


def fiscalPeriodKey(fiscalYear: Column, fiscalPeriod: Column) -> Column:
    """``yyyypp`` integer; unique only together with the calendar code."""
    return (fiscalYear * 100 + fiscalPeriod).cast("int")


def fiscalPeriodName(fiscalYear: Column, fiscalPeriod: Column) -> Column:
    return F.concat(F.lit("FY"), fiscalYear.cast("string"), F.lit("-P"), F.lpad(fiscalPeriod.cast("string"), 2, "0"))


# --------------------------------------------------------------------------- NA445
def _na445YearStart(yearCol: Column) -> Column:
    """Sunday nearest 1 January of ``yearCol`` (usp_PopulateDateDimension)."""
    jan1 = F.make_date(yearCol, F.lit(1), F.lit(1))
    daysAfterSunday = F.dayofweek(jan1) - 1
    return F.when(daysAfterSunday <= 3, F.date_sub(jan1, daysAfterSunday)).otherwise(
        F.date_add(jan1, F.lit(7) - daysAfterSunday)
    )


def na445Parts(dateCol: Column) -> dict[str, Column]:
    calYear = F.year(dateCol)
    fiscalYear = (
        F.when(dateCol >= _na445YearStart(calYear + 1), calYear + 1)
        .when(dateCol >= _na445YearStart(calYear), calYear)
        .otherwise(calYear - 1)
    )
    yearStart = _na445YearStart(fiscalYear)
    yearEnd = F.date_sub(_na445YearStart(fiscalYear + 1), 1)
    week0 = F.floor(F.datediff(dateCol, yearStart) / 7).cast("int")
    quarter0 = F.floor(week0 / 13).cast("int")
    weekInQuarter = week0 % 13
    slot = F.when(weekInQuarter < 4, F.lit(0)).when(weekInQuarter < 8, F.lit(1)).otherwise(F.lit(2))
    # week 53 of a 53-week year rolls into period 12
    period0 = F.when(week0 >= 52, F.lit(11)).otherwise(quarter0 * 3 + slot)
    startWeek = F.when(week0 >= 52, F.lit(47)).otherwise(quarter0 * 13 + slot * 4)
    lengthWeeks = F.when(slot == 2, F.lit(5)).otherwise(F.lit(4))
    fiscalPeriod = (period0 + 1).cast("int")
    periodStart = F.date_add(yearStart, startWeek * 7)
    periodEnd = F.when(fiscalPeriod == 12, yearEnd).otherwise(
        F.date_sub(F.date_add(yearStart, (startWeek + lengthWeeks) * 7), 1)
    )
    return {
        "fiscal_year": fiscalYear.cast("int"),
        "fiscal_quarter": (F.floor(period0 / 3) + 1).cast("int"),
        "fiscal_period": fiscalPeriod,
        "period_start": periodStart,
        "period_end": periodEnd,
    }


# --------------------------------------------------------------------------- EUCAL
def eucalParts(dateCol: Column) -> dict[str, Column]:
    return {
        "fiscal_year": F.year(dateCol).cast("int"),
        "fiscal_quarter": F.quarter(dateCol).cast("int"),
        "fiscal_period": F.month(dateCol).cast("int"),
        "period_start": F.trunc(dateCol, "MM"),
        "period_end": F.last_day(dateCol),
    }


# --------------------------------------------------------------------------- APACJUN
def apacjunParts(dateCol: Column) -> dict[str, Column]:
    month = F.month(dateCol)
    fiscalPeriod = F.when(month >= 4, month - 3).otherwise(month + 9).cast("int")
    return {
        "fiscal_year": F.when(month >= 4, F.year(dateCol) + 1).otherwise(F.year(dateCol)).cast("int"),
        "fiscal_quarter": (F.floor((fiscalPeriod - 1) / 3) + 1).cast("int"),
        "fiscal_period": fiscalPeriod,
        "period_start": F.trunc(dateCol, "MM"),
        "period_end": F.last_day(dateCol),
    }


_PART_BUILDERS = {CALENDAR_NA: na445Parts, CALENDAR_EU: eucalParts, CALENDAR_APAC: apacjunParts}
PART_NAMES: tuple[str, ...] = ("fiscal_year", "fiscal_quarter", "fiscal_period", "period_start", "period_end")


def fiscalParts(dateCol: Column, calendarCode: str) -> dict[str, Column]:
    """Arithmetic fiscal attributes of ``dateCol`` under one named calendar."""
    return _PART_BUILDERS[calendarCode](dateCol)


def fiscalPartsFor(dateCol: Column, calendarCodeCol: Column) -> dict[str, Column]:
    """Arithmetic fiscal attributes where the calendar is chosen per row."""
    perCalendar = {code: fiscalParts(dateCol, code) for code in CALENDAR_CODES}
    out: dict[str, Column] = {}
    for name in PART_NAMES:
        expr = F.when(calendarCodeCol == CALENDAR_NA, perCalendar[CALENDAR_NA][name])
        expr = expr.when(calendarCodeCol == CALENDAR_EU, perCalendar[CALENDAR_EU][name])
        expr = expr.when(calendarCodeCol == CALENDAR_APAC, perCalendar[CALENDAR_APAC][name])
        out[name] = expr.otherwise(F.lit(None))
    return out


# --------------------------------------------------------------------------- public API
def resolveFiscalPeriod(
    df: DataFrame,
    spark: SparkSession,
    cfg: PipelineConfig,
    dateCol: str,
    regionCol: str = "region_code",
) -> DataFrame:
    """Add ``fiscal_calendar_code``, ``fiscal_year``, ``fiscal_period``, ``fiscal_period_key``.

    The calendar is chosen by the *transaction* region (spec above). Lookup is
    against ``silver.dim_fiscal_calendar``; a date outside the loaded calendar
    falls back to the same arithmetic that built the dimension, mirroring
    ``FN_FISCAL_PERIOD`` ("table lookup first ... arithmetic fallback").
    Rows whose region maps to no calendar are kept with NULL fiscal columns
    and ``dq_status_code = 'WARN'``.
    """
    base = df.drop(*[c for c in OUTPUT_COLUMNS if c in df.columns])
    calendar = (
        spark.table(cfg.fqn("silver", DIM_FISCAL_CALENDAR))
        .select(
            F.col("calendar_code").alias("_cal_code"),
            F.col("fiscal_year").alias("_cal_fiscal_year"),
            F.col("fiscal_period").alias("_cal_fiscal_period"),
            F.col("period_start").alias("_cal_period_start"),
            F.col("period_end").alias("_cal_period_end"),
        )
    )
    withCode = base.withColumn("fiscal_calendar_code", regionCalendarCode(regionCol))
    txnDate = F.col(dateCol).cast("date")
    joined = withCode.join(
        F.broadcast(calendar),
        (F.col("fiscal_calendar_code") == F.col("_cal_code"))
        & (txnDate >= F.col("_cal_period_start"))
        & (txnDate <= F.col("_cal_period_end")),
        "left",
    )
    fallback = fiscalPartsFor(txnDate, F.col("fiscal_calendar_code"))
    fiscalYear = F.coalesce(F.col("_cal_fiscal_year"), fallback["fiscal_year"]).cast("int")
    fiscalPeriod = F.coalesce(F.col("_cal_fiscal_period"), fallback["fiscal_period"]).cast("int")
    out = (
        joined.withColumn("fiscal_year", fiscalYear)
        .withColumn("fiscal_period", fiscalPeriod)
        .withColumn("fiscal_period_key", fiscalPeriodKey(F.col("fiscal_year"), F.col("fiscal_period")))
        .drop("_cal_code", "_cal_fiscal_year", "_cal_fiscal_period", "_cal_period_start", "_cal_period_end")
    )
    return tagDq(out, F.col("fiscal_calendar_code").isNull() | F.col("fiscal_period_key").isNull(), "WARN", "FISCAL_PERIOD_UNRESOLVED")


def naFiscalPeriodFor(df: DataFrame, dateCol: str) -> DataFrame:
    """Fiscal columns under the NA calendar regardless of the row's region.

    Used by the cross-region aggregates.
    """
    # LEGACY QUIRK: the legacy DW aggregates compare periods across regions using
    # the NA 4-4-5 convention silently ("The aggregates choose the NA
    # convention, silently." - business-domains.md). Reproduced here on purpose;
    # an EU or APAC row aggregated this way is reported in a period that is not
    # the period on its own books.
    parts = na445Parts(F.col(dateCol).cast("date"))
    base = df.drop(*[c for c in OUTPUT_COLUMNS if c in df.columns])
    return (
        base.withColumn("fiscal_calendar_code", F.lit(CALENDAR_NA))
        .withColumn("fiscal_year", parts["fiscal_year"])
        .withColumn("fiscal_period", parts["fiscal_period"])
        .withColumn("fiscal_period_key", fiscalPeriodKey(F.col("fiscal_year"), F.col("fiscal_period")))
    )
