"""In-test bronze frames shaped like the Oracle DDL (oracle/tables/*.sql).

Shared by the ``test_silver_rules_*`` modules; nothing here depends on another
workstream's loader.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import overwriteTable
from sales_lakehouse.silver import reference

D = dt.date


def _str(name: str) -> StructField:
    return StructField(name, StringType(), True)


def _date(name: str) -> StructField:
    return StructField(name, DateType(), True)


def _dec(name: str, p: int, s: int) -> StructField:
    return StructField(name, DecimalType(p, s), True)


FX_RATE_DAILY_SCHEMA = StructType(
    [
        _str("FROM_CURR_CD"), _str("TO_CURR_CD"), _date("RATE_DT"), _str("RATE_TYPE_CD"),
        _dec("RATE", 18, 8), _dec("INVERSE_RATE", 18, 8), _str("RATE_SOURCE_CD"), _str("FEED_REGION_CD"),
        _str("INTERPOLATED_FLG"), _str("SUPERSEDED_FLG"),
    ]
)

TAX_JURISDICTION_SCHEMA = StructType(
    [
        _str("JURISDICTION_CD"), _str("JURISDICTION_NAME"), _str("REGION_CD"), _str("COUNTRY_CD"),
        _str("STATE_PROV_CD"), _str("COUNTY_TXT"), _str("CITY_TXT"), _str("POSTAL_FROM_CD"), _str("POSTAL_TO_CD"),
        _str("JURISDICTION_LEVEL_CD"), _str("TAX_REGIME_CD"), _str("STACKS_WITH_PARENT_FLG"),
        _str("PARENT_JURISDICTION_CD"), _date("EFFECTIVE_FROM_DT"), _date("EFFECTIVE_TO_DT"), _str("ACTIVE_FLG"),
    ]
)

TAX_RATE_SCHEMA = StructType(
    [
        _str("TAX_CODE_CD"), _str("JURISDICTION_CD"), _str("REGION_CD"), _str("TAX_REGIME_CD"),
        _str("RATE_CATEGORY_CD"), _dec("RATE_PCT", 7, 5), _dec("RECOVERABLE_PCT", 5, 2),
        _date("EFFECTIVE_FROM_DT"), _date("EFFECTIVE_TO_DT"), _str("REVERSE_CHARGE_FLG"), _str("SELF_ASSESS_FLG"),
        _str("PRODUCT_CATEGORY_CD"), _str("RATE_DESC"), _str("ACTIVE_FLG"),
    ]
)

CALENDAR_FISCAL_SCHEMA = StructType(
    [
        _str("CALENDAR_CD"), _date("CALENDAR_DT"), StructField("FISCAL_YEAR_NBR", DecimalType(4, 0)),
        StructField("FISCAL_QUARTER_NBR", DecimalType(1, 0)), StructField("FISCAL_PERIOD_NBR", DecimalType(2, 0)),
        _str("PERIOD_CD"), _date("PERIOD_START_DT"), _date("PERIOD_END_DT"), _str("WORKING_DAY_FLG"),
        _str("HOLIDAY_FLG"), _str("HOLIDAY_NAME"), _str("ADJUSTMENT_PERIOD_FLG"), _str("REGION_CD"),
    ]
)


def _fx(fromCcy, toCcy, day, rateType, rate, source, region, interpolated="N", inverse=None):
    return (
        fromCcy, toCcy, day, rateType, Decimal(rate), Decimal(inverse) if inverse else None,
        source, region, interpolated, "N",
    )


def fxRateDailyRows() -> list[tuple]:
    rows = []
    # NA feed: CAD SPOT, weekday rows plus interpolated weekend copies (02_currency_and_fx_rates.sql)
    for day in (D(2024, 3, 4), D(2024, 3, 5), D(2024, 3, 6), D(2024, 3, 7)):
        rows.append(_fx("CAD", "USD", day, "SPOT", "0.73500000", "BOC", "NA"))
    rows.append(_fx("CAD", "USD", D(2024, 3, 8), "SPOT", "0.74000000", "BOC", "NA"))
    rows.append(_fx("CAD", "USD", D(2024, 3, 9), "SPOT", "0.74000000", "BOC", "NA", interpolated="Y"))
    rows.append(_fx("CAD", "USD", D(2024, 3, 10), "SPOT", "0.74000000", "BOC", "NA", interpolated="Y"))
    rows.append(_fx("CAD", "USD", D(2024, 3, 15), "SPOT", "0.73000000", "BOC", "NA"))
    rows.append(_fx("CAD", "USD", D(2024, 4, 1), "SPOT", "0.72000000", "BOC", "NA"))
    # EU feed: EUR ECB weekdays + interpolated weekend; GBP with a long gap
    rows.append(_fx("EUR", "USD", D(2024, 3, 7), "ECB", "1.08000000", "ECB", "EU"))
    rows.append(_fx("EUR", "USD", D(2024, 3, 8), "ECB", "1.09000000", "ECB", "EU"))
    rows.append(_fx("EUR", "USD", D(2024, 3, 9), "ECB", "1.09000000", "ECB", "EU", interpolated="Y"))
    rows.append(_fx("EUR", "USD", D(2024, 3, 10), "ECB", "1.09000000", "ECB", "EU", interpolated="Y"))
    rows.append(_fx("GBP", "USD", D(2024, 1, 15), "ECB", "1.27000000", "ECB", "EU"))
    # APAC feed: AUD CORP month rate, a mid-month CORP row that must be ignored,
    # and a June rate published on the last business day before a weekend 1st
    rows.append(_fx("AUD", "USD", D(2024, 3, 1), "CORP", "0.65000000", "APAC_TREASURY", "APAC"))
    rows.append(_fx("AUD", "USD", D(2024, 3, 15), "CORP", "0.66000000", "APAC_TREASURY", "APAC"))
    rows.append(_fx("AUD", "USD", D(2024, 5, 31), "CORP", "0.67000000", "APAC_TREASURY", "APAC"))
    # quoted the other way round (USD -> JPY) and a cross quote (NZD -> AUD)
    rows.append(_fx("USD", "JPY", D(2024, 3, 1), "CORP", "150.00000000", "APAC_TREASURY", "APAC", inverse="0.00666667"))
    rows.append(_fx("NZD", "AUD", D(2024, 3, 1), "CORP", "0.92000000", "APAC_TREASURY", "APAC"))
    # superseded row must be ignored
    rows.append(("EUR", "USD", D(2024, 3, 8), "ECB", Decimal("9.99000000"), None, "ECB", "EU", "N", "Y"))
    return rows


def taxJurisdictionRows() -> list[tuple]:
    def jur(code, name, region, country, state, county, city, level, regime, stacks, parent, frm):
        return (code, name, region, country, state, county, city, None, None, level, regime, stacks, parent, frm, None, "Y")

    return [
        jur("US-IL", "Illinois", "NA", "US", "IL", None, None, "STATE", "SALES", "N", None, D(1998, 1, 1)),
        jur("US-IL-COOK", "Cook County", "NA", "US", "IL", "Cook", None, "COUNTY", "SALES", "Y", "US-IL", D(1998, 1, 1)),
        jur("US-IL-COOK-CHI", "Chicago", "NA", "US", "IL", "Cook", "Chicago", "CITY", "SALES", "Y", "US-IL-COOK", D(1998, 1, 1)),
        jur("US-IL-RTA", "Regional Transportation Authority", "NA", "US", "IL", "Cook", None, "SPEC", "SALES", "Y", "US-IL-COOK", D(2008, 4, 1)),
        jur("US-TX-DALLAS-DAL", "Dallas", "NA", "US", "TX", "Dallas", "Dallas", "CITY", "SALES", "N", None, D(1998, 1, 1)),
        jur("DE-VAT", "Germany VAT", "EU", "DE", None, None, None, "CNTRY", "VAT", "N", None, D(1998, 1, 1)),
        jur("FR-VAT", "France TVA", "EU", "FR", None, None, None, "CNTRY", "VAT", "N", None, D(1998, 1, 1)),
        jur("NL-VAT", "Netherlands BTW", "EU", "NL", None, None, None, "CNTRY", "VAT", "N", None, D(1998, 1, 1)),
        jur("AU-GST", "Australia GST", "APAC", "AU", None, None, None, "CNTRY", "GST", "N", None, D(2000, 7, 1)),
        jur("SG-GST", "Singapore GST", "APAC", "SG", None, None, None, "CNTRY", "GST", "N", None, D(2007, 7, 1)),
        jur("JP-CONS", "Japan Consumption Tax", "APAC", "JP", None, None, None, "CNTRY", "CONS", "N", None, D(2019, 10, 1)),
    ]


def taxRateRows() -> list[tuple]:
    def rate(code, jur, region, regime, cat, pct, frm, to=None, rc="N", selfAssess="N", product=None, active="Y"):
        return (code, jur, region, regime, cat, Decimal(pct), Decimal("100.00") if region != "NA" else Decimal("0.00"),
                frm, to, rc, selfAssess, product, f"{code} rate", active)

    return [
        rate("IL_STD", "US-IL", "NA", "SALES", "STD", "6.25000", D(2008, 7, 1)),
        rate("COOK_STD", "US-IL-COOK", "NA", "SALES", "STD", "1.75000", D(2016, 7, 1)),
        rate("CHI_STD", "US-IL-COOK-CHI", "NA", "SALES", "STD", "1.25000", D(2016, 1, 1)),
        rate("RTA_STD", "US-IL-RTA", "NA", "SALES", "STD", "1.00000", D(2008, 4, 1)),
        rate("IL_RESALE", "US-IL", "NA", "SALES", "RESALE", "0.00000", D(1998, 1, 1), product="RESALE"),
        rate("IL_USE", "US-IL", "NA", "USE", "STD", "6.25000", D(2008, 7, 1), selfAssess="Y"),
        rate("TX_STD", "US-TX-DALLAS-DAL", "NA", "SALES", "STD", "8.25000", D(1998, 1, 1)),
        rate("DE_STD", "DE-VAT", "EU", "VAT", "STD", "19.00000", D(2007, 1, 1)),
        rate("DE_RED", "DE-VAT", "EU", "VAT", "RED1", "7.00000", D(2007, 1, 1), product="FOOD"),
        rate("DE_STD_COVID", "DE-VAT", "EU", "VAT", "STD", "16.00000", D(2020, 7, 1), D(2020, 12, 31), active="N"),
        rate("FR_STD", "FR-VAT", "EU", "VAT", "STD", "20.00000", D(2014, 1, 1)),
        rate("NL_ICA", "NL-VAT", "EU", "VAT", "ZERO", "0.00000", D(2002, 1, 1), rc="Y", selfAssess="Y"),
        rate("AU_GST", "AU-GST", "APAC", "GST", "STD", "10.00000", D(2000, 7, 1)),
        rate("AU_FREE", "AU-GST", "APAC", "GST", "ZERO", "0.00000", D(2000, 7, 1), product="FOOD"),
        rate("SG_GST7", "SG-GST", "APAC", "GST", "STD", "7.00000", D(2007, 7, 1), D(2022, 12, 31), active="N"),
        rate("SG_GST9", "SG-GST", "APAC", "GST", "STD", "9.00000", D(2024, 1, 1)),
        rate("JP_CONS10", "JP-CONS", "APAC", "CONS", "STD", "10.00000", D(2019, 10, 1)),
        rate("JP_INPUT", "JP-CONS", "APAC", "CONS", "INPUT", "10.00000", D(2023, 10, 1)),
    ]


def calendarFiscalRows() -> list[tuple]:
    def cal(code, day, fy, fq, fp, start, end, region, holiday=None, adj="N"):
        return (code, day, Decimal(fy), Decimal(fq), Decimal(fp), f"{fy}-{fp:02d}", start, end,
                "N" if holiday else "Y", "Y" if holiday else "N", holiday, adj, region)

    return [
        cal("NA_CAL", D(2023, 1, 1), 2023, 1, 1, D(2023, 1, 1), D(2023, 1, 28), "NA", holiday="New Year's Day"),
        cal("EU_CAL445", D(2023, 1, 1), 2023, 1, 1, D(2023, 1, 1), D(2023, 1, 31), "EU", holiday="New Year's Day"),
        cal("APAC_APR", D(2023, 1, 1), 2023, 4, 10, D(2023, 1, 1), D(2023, 1, 31), "APAC"),
        cal("NA_CAL", D(2025, 12, 31), 2025, 4, 12, D(2025, 11, 30), D(2026, 1, 3), "NA"),
        cal("EU_CAL445", D(2025, 12, 31), 2025, 4, 13, D(2025, 12, 29), D(2025, 12, 31), "EU", adj="Y"),
        cal("APAC_APR", D(2025, 12, 31), 2026, 3, 9, D(2025, 12, 1), D(2025, 12, 31), "APAC"),
    ]


def fxRateDaily(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(fxRateDailyRows(), FX_RATE_DAILY_SCHEMA)


def taxJurisdiction(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(taxJurisdictionRows(), TAX_JURISDICTION_SCHEMA)


def taxRate(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(taxRateRows(), TAX_RATE_SCHEMA)


def calendarFiscal(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(calendarFiscalRows(), CALENDAR_FISCAL_SCHEMA)


def loadBronzeReference(spark: SparkSession, cfg: PipelineConfig) -> None:
    overwriteTable(fxRateDaily(spark), cfg.fqn("bronze", reference.BRONZE_FX_RATE_DAILY))
    overwriteTable(taxJurisdiction(spark), cfg.fqn("bronze", reference.BRONZE_TAX_JURISDICTION))
    overwriteTable(taxRate(spark), cfg.fqn("bronze", reference.BRONZE_TAX_RATE))
    overwriteTable(calendarFiscal(spark), cfg.fqn("bronze", reference.BRONZE_CALENDAR_FISCAL))


def buildSilverReference(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Load bronze and run the reference builder once per test session."""
    if not spark.catalog.tableExists(cfg.fqn("silver", reference.DIM_DATE)):
        loadBronzeReference(spark, cfg)
        reference.run(spark, cfg)
