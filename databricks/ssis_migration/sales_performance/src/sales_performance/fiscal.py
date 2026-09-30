"""Regional fiscal calendars used by commissions, quota attainment and the aggregates.

Sales.SalesTerritories is the system of record for which calendar a region uses:
NA = NA445 (4-4-5 on the calendar year), EU = EUCAL (calendar months),
APAC = APACJUN (fiscal year ending 30 June, 4-4-5 periods from 1 July).
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

CALENDAR_NA = "NA445"
CALENDAR_EU = "EUCAL"
CALENDAR_APAC = "APACJUN"


def _period445(dayOfPeriodYear: Column) -> Column:
    """Map a 0-based day offset within a fiscal year onto a 4-4-5 period (1..12)."""
    week = F.least(F.floor(dayOfPeriodYear / 7), F.lit(51))
    quarter = F.floor(week / 13)
    weekInQuarter = week % 13
    periodInQuarter = F.when(weekInQuarter < 4, 0).when(weekInQuarter < 8, 1).otherwise(2)
    return (quarter * 3 + periodInQuarter + 1).cast("int")


def fiscalYear(dateCol: Column, calendarCode: Column) -> Column:
    return (
        F.when(calendarCode == CALENDAR_APAC, F.when(F.month(dateCol) >= 7, F.year(dateCol) + 1).otherwise(F.year(dateCol)))
        .otherwise(F.year(dateCol))
        .cast("int")
    )


def fiscalYearStart(dateCol: Column, calendarCode: Column) -> Column:
    fy = fiscalYear(dateCol, calendarCode)
    return F.when(calendarCode == CALENDAR_APAC, F.make_date(fy - 1, F.lit(7), F.lit(1))).otherwise(F.make_date(fy, F.lit(1), F.lit(1)))


def fiscalPeriod(dateCol: Column, calendarCode: Column) -> Column:
    offset = F.datediff(dateCol, fiscalYearStart(dateCol, calendarCode))
    return F.when(calendarCode == CALENDAR_EU, F.month(dateCol)).otherwise(_period445(offset)).cast("int")


def fiscalPeriodLabel(dateCol: Column, calendarCode: Column) -> Column:
    return F.concat(
        F.lit("FY"),
        fiscalYear(dateCol, calendarCode).cast("string"),
        F.lit("-P"),
        F.lpad(fiscalPeriod(dateCol, calendarCode).cast("string"), 2, "0"),
    )


def calendarMonth(dateCol: Column) -> Column:
    return F.date_format(dateCol, "yyyy-MM")


def commissionPeriod(dateCol: Column, regionCode: Column) -> Column:
    """NA and EU accrue on calendar months; APAC accrues on its 4-4-5 fiscal period."""
    return F.when(regionCode == "APAC", fiscalPeriodLabel(dateCol, F.lit(CALENDAR_APAC))).otherwise(calendarMonth(dateCol))


def calendarForRegion(regionCode: Column) -> Column:
    return F.when(regionCode == "EU", F.lit(CALENDAR_EU)).when(regionCode == "APAC", F.lit(CALENDAR_APAC)).otherwise(F.lit(CALENDAR_NA))


def fiscalPeriodStart(dateCol: Column, calendarCode: Column) -> Column:
    """First day of the fiscal period that contains ``dateCol``."""
    period = fiscalPeriod(dateCol, calendarCode)
    quarter = F.floor((period - 1) / 3)
    pos = (period - 1) % 3
    weeksBefore = quarter * 13 + F.when(pos == 0, 0).when(pos == 1, 4).otherwise(8)
    start445 = F.date_add(fiscalYearStart(dateCol, calendarCode), (weeksBefore * 7).cast("int"))
    return F.when(calendarCode == CALENDAR_EU, F.trunc(dateCol, "month")).otherwise(start445)
