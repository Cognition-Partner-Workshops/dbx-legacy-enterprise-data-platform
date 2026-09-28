"""Fiscal calendar arithmetic for NA445 (4-4-5, FY starts 1 Nov), EUCAL (calendar
year) and APACJUN (monthly, FY starts 1 Jul). Shared by the Oracle CALENDAR_FISCAL /
GL_PERIOD_STATUS generators and the SQL Server SalesQuotas generator."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

NA445 = "NA445"
EUCAL = "EUCAL"
APACJUN = "APACJUN"
CALENDAR_REGION = {NA445: "NA", EUCAL: "EU", APACJUN: "APAC"}
FISCAL_YEAR_START_MONTH = {NA445: 11, EUCAL: 1, APACJUN: 7}

# 4-4-5 week pattern: 12 periods of 28/28/35 days = 364 days.
NA445_PERIOD_DAYS = [28, 28, 35] * 4


@dataclass(frozen=True)
class FiscalPeriod:
    calendarCode: str
    fiscalYear: int
    quarter: int
    period: int
    week: int
    dayOfPeriod: int
    periodStart: date
    periodEnd: date
    quarterStart: date
    quarterEnd: date
    yearStart: date
    yearEnd: date

    @property
    def periodCode(self) -> str:
        return f"{self.fiscalYear}-{self.period:02d}"

    @property
    def label(self) -> str:
        return f"FY{self.fiscalYear}P{self.period:02d}"


def fiscalYearStart(calendarCode: str, day: date) -> date:
    startMonth = FISCAL_YEAR_START_MONTH[calendarCode]
    year = day.year if day.month >= startMonth else day.year - 1
    return date(year, startMonth, 1)


def fiscalYearNumber(calendarCode: str, yearStart: date) -> int:
    # FY is named after the calendar year in which it ends.
    return yearStart.year if calendarCode == EUCAL else yearStart.year + 1


def _addMonths(day: date, months: int) -> date:
    monthIndex = day.month - 1 + months
    return date(day.year + monthIndex // 12, monthIndex % 12 + 1, 1)


def fiscalPeriodFor(calendarCode: str, day: date) -> FiscalPeriod:
    yearStart = fiscalYearStart(calendarCode, day)
    yearEnd = _addMonths(yearStart, 12) - timedelta(days=1)
    fiscalYear = fiscalYearNumber(calendarCode, yearStart)
    if calendarCode == NA445:
        offset = (day - yearStart).days
        cursor = yearStart
        for index, length in enumerate(NA445_PERIOD_DAYS):
            periodEnd = cursor + timedelta(days=length - 1)
            if index == 11:
                periodEnd = yearEnd  # the 53rd-week remainder is absorbed by period 12
            if day <= periodEnd:
                period = index + 1
                periodStart = cursor
                break
            cursor = periodEnd + timedelta(days=1)
        week = min(offset // 7 + 1, 53)
        quarter = (period - 1) // 3 + 1
        quarterStart = yearStart + timedelta(days=sum(NA445_PERIOD_DAYS[: (quarter - 1) * 3]))
        quarterEnd = yearEnd if quarter == 4 else yearStart + timedelta(days=sum(NA445_PERIOD_DAYS[: quarter * 3]) - 1)
    else:
        period = (day.month - FISCAL_YEAR_START_MONTH[calendarCode]) % 12 + 1
        periodStart = date(day.year, day.month, 1)
        periodEnd = _addMonths(periodStart, 1) - timedelta(days=1)
        quarter = (period - 1) // 3 + 1
        quarterStart = _addMonths(yearStart, (quarter - 1) * 3)
        quarterEnd = _addMonths(quarterStart, 3) - timedelta(days=1)
        week = (day - yearStart).days // 7 + 1
    return FiscalPeriod(calendarCode, fiscalYear, quarter, period, week, (day - periodStart).days + 1,
                        periodStart, periodEnd, quarterStart, quarterEnd, yearStart, yearEnd)


def periodsBetween(calendarCode: str, start: date, end: date) -> list[FiscalPeriod]:
    periods: list[FiscalPeriod] = []
    cursor = start
    while cursor <= end:
        fp = fiscalPeriodFor(calendarCode, cursor)
        periods.append(fp)
        cursor = fp.periodEnd + timedelta(days=1)
    return periods
