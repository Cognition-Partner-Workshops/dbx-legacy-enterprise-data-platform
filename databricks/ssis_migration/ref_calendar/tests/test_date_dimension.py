from datetime import date

from ref_calendar import dimensions


def test_date_dimension_range_and_reserved_rows(spark):
    dim = dimensions.buildDimDate(spark).cache()
    assert dim.count() == 1461 + 2
    reserved = sorted(r.date for r in dim.where("date < '1901-01-01'").collect())
    assert reserved == [date(1900, 1, 1), date(1900, 1, 2)]
    assert dim.where("date = '2013-01-01'").first().calendar_month_label == "CY2013-Jan"


def test_wwi_fiscal_year_starts_in_november(spark):
    dim = dimensions.buildDimDate(spark)
    rows = {r.date: r for r in dim.where("date IN ('2013-10-31', '2013-11-01', '2014-01-15')").collect()}
    assert rows[date(2013, 10, 31)].fiscal_year == 2013 and rows[date(2013, 10, 31)].fiscal_month_number == 12
    assert rows[date(2013, 11, 1)].fiscal_year == 2014 and rows[date(2013, 11, 1)].fiscal_month_number == 1
    assert rows[date(2014, 1, 15)].fiscal_year == 2014 and rows[date(2014, 1, 15)].fiscal_month_number == 3
    assert rows[date(2013, 11, 1)].fiscal_year_label == "FY2014"


def test_regional_fiscal_columns(spark):
    row = dimensions.buildDimDate(spark).where("date = '2014-04-01'").first()
    assert row.na_fiscal_year == 2014 and row.na_fiscal_period == 10 and row.na_fiscal_quarter == 4
    assert row.eu_fiscal_year == 2014 and row.eu_fiscal_period == 4
    assert row.apac_fiscal_year == 2014 and row.apac_fiscal_period == 1 and row.apac_fiscal_period_label == "FY2014-P01"


def test_iso_week_and_trading_day(spark):
    dim = dimensions.buildDimDate(spark)
    row = dim.where("date = '2016-01-01'").first()
    assert row.iso_week_number == 53
    assert row.is_weekend_na == "N" and row.na_is_trading_day == "Y"
    assert dim.where("date = '2016-01-02'").first().is_weekend_na == "Y"
