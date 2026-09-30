from datetime import date

from pyspark.sql import functions as F

from conftest import SALE_LINE_SCHEMA, d, saleLine
from sales_performance import fiscal
from sales_performance.quota import calculateQuotaAttainment, regionalRevenue


def test_quota_bands_zero_quota_and_missing_quota(spark):
    lines = spark.createDataFrame(
        [
            saleLine(1, "NA", date(2016, 5, 2), 1000, 100, salespersonId=1, territory="NA-US-EAST"),
            saleLine(2, "NA", date(2016, 5, 2), 100, 0, salespersonId=1, territory="NA-US-EAST", reversal=True),
            saleLine(3, "EU", date(2016, 5, 2), 900, 171, salespersonId=2, territory="EU-DE"),
            saleLine(4, "EU", date(2016, 5, 2), 500, 95, salespersonId=3, territory="EU-NL"),
            saleLine(5, "NA", date(2016, 5, 2), 50, 0, salespersonId=4, territory="NA-CA"),
        ],
        SALE_LINE_SCHEMA,
    )
    credits = spark.createDataFrame(
        [("EU", "EU-DE", 2, "EUCAL", "FY2016-P11", d(100))],
        "region_code string, territory_code string, salesperson_id bigint, fiscal_calendar_code string, fiscal_period_label string, credit_amount decimal(19,4)",
    )
    quotas = spark.createDataFrame(
        [
            (1, "NA-US-EAST", "FY2016-P11", d(1000), "USD", None),
            (2, "EU-DE", "FY2016-P11", d(1000), "EUR", None),
            (3, "EU-NL", "FY2016-P11", d(0), "EUR", None),
            (9, "AP-AU", "FY2016-P11", d(10), "AUD", None),
        ],
        "salesperson_id bigint, territory_code string, fiscal_period_label string, quota_amount decimal(19,4), quota_currency_code string, stretch_quota_amount decimal(19,4)",
    )
    out = calculateQuotaAttainment(regionalRevenue(lines, creditNotes=credits), quotas)
    rows = {(r["territory_code"], r["salesperson_id"]): r for r in out.collect()}
    assert str(rows[("NA-US-EAST", 1)]["attainment_amount"]) == "1100.0000" and rows[("NA-US-EAST", 1)]["attainment_band"] == "AT"
    assert str(rows[("EU-DE", 2)]["attainment_amount"]) == "800.0000" and rows[("EU-DE", 2)]["attainment_band"] == "UNDER"
    assert rows[("EU-NL", 3)]["attainment_band"] == "NOQUOTA" and str(rows[("EU-NL", 3)]["attainment_percent"]) == "0.00"
    assert rows[("NA-CA", 4)]["is_missing_quota"] and rows[("NA-CA", 4)]["attainment_band"] == "NOQUOTA"
    assert rows[("AP-AU", 9)]["region_code"] == "AP" and str(rows[("AP-AU", 9)]["attainment_amount"]) == "0.0000"
    over = calculateQuotaAttainment(regionalRevenue(lines), quotas.withColumn("quota_amount", F.lit(d(800)).cast("decimal(19,4)")))
    assert {r["salesperson_id"]: r["attainment_band"] for r in over.collect()}[1] == "OVER120"


def test_445_fiscal_calendar(spark):
    df = spark.createDataFrame(
        [(date(2015, 7, 1),), (date(2015, 7, 28),), (date(2015, 7, 29),), (date(2016, 6, 30),), (date(2016, 1, 1),), (date(2016, 2, 15),)],
        "d date",
    )
    cal = F.lit(fiscal.CALENDAR_APAC)
    rows = df.select(
        "d",
        fiscal.fiscalYear(F.col("d"), cal).alias("fy"),
        fiscal.fiscalPeriod(F.col("d"), cal).alias("p"),
        fiscal.fiscalPeriodStart(F.col("d"), cal).alias("ps"),
    ).collect()
    got = {r["d"].isoformat(): (r["fy"], r["p"], r["ps"].isoformat()) for r in rows}
    assert got["2015-07-01"] == (2016, 1, "2015-07-01")
    assert got["2015-07-28"] == (2016, 1, "2015-07-01")
    assert got["2015-07-29"] == (2016, 2, "2015-07-29")
    assert got["2016-06-30"][0] == 2016 and got["2016-06-30"][1] == 12
    na = df.select(
        "d",
        fiscal.fiscalYear(F.col("d"), F.lit(fiscal.CALENDAR_NA)).alias("fy"),
        fiscal.fiscalPeriod(F.col("d"), F.lit(fiscal.CALENDAR_NA)).alias("p"),
    ).collect()
    naGot = {r["d"].isoformat(): (r["fy"], r["p"]) for r in na}
    assert naGot["2016-01-01"] == (2016, 1) and naGot["2016-02-15"] == (2016, 2)
    eu = df.select(fiscal.fiscalPeriodLabel(F.col("d"), F.lit(fiscal.CALENDAR_EU)).alias("l")).collect()
    assert eu[0]["l"] == "FY2015-P07"
