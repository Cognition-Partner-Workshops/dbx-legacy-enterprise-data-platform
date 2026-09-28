import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

from wwi_ref import datecalendar, transforms

XW_SCHEMA = T.StructType([
    T.StructField("CodeDomainCode", T.StringType()), T.StructField("SourceSystemCode", T.StringType()),
    T.StructField("SourceCodeValue", T.StringType()), T.StructField("SourceCodeDescription", T.StringType()),
    T.StructField("ConformedCodeValue", T.StringType()), T.StructField("RegionCode", T.StringType()),
    T.StructField("IsDefaultForConformed", T.BooleanType()), T.StructField("EffectiveFromDate", T.StringType()),
    T.StructField("EffectiveToDate", T.StringType()),
])


def crosswalk(spark, rows):
    df = spark.createDataFrame(rows, XW_SCHEMA)
    return (df.withColumn("EffectiveFromDate", F.col("EffectiveFromDate").cast("date"))
              .withColumn("EffectiveToDate", F.col("EffectiveToDate").cast("date")))


def test_conformed_code_query_one_row_per_code_and_region(spark):
    df = crosswalk(spark, [
        ("PAYMENT_METHOD", "ORA_ERP", "1", "Wire transfer", "WIRE", "EU", True, "2024-01-01", None),
        ("PAYMENT_METHOD", "WWI_OLTP", "W", "Wire", "WIRE", "EU", False, "2024-01-01", None),
        ("PAYMENT_METHOD", "WWI_OLTP", "OLD", "Old wire", "WIRE", "EU", False, "2023-01-01", "2023-12-31"),
        ("PAYMENT_METHOD", "WWI_OLTP", "CHQ", "Cheque", "CHQ", None, True, "2024-01-01", None),
        ("OTHER", "WWI_OLTP", "X", "Other", "X", None, True, "2024-01-01", None),
    ])
    out = transforms.paymentMethodDimension(df).orderBy("PaymentMethodCode").collect()
    assert [(r["PaymentMethodCode"], r["RegionCode"], r["PaymentMethodName"], r["SourceCodeCount"]) for r in out] == [
        ("CHQ", "ALL", "Cheque", 1), ("WIRE", "EU", "Wire transfer", 2)]
    byCode = {r["PaymentMethodCode"]: r for r in out}
    assert byCode["WIRE"]["SettlementTypeCode"] == "SEPA" and byCode["WIRE"]["ElectronicFlag"] == "Y"
    assert byCode["CHQ"]["SettlementTypeCode"] == "ACH" and byCode["CHQ"]["ElectronicFlag"] == "N"


def test_payment_terms_parsing_and_regional_caps(spark):
    df = crosswalk(spark, [
        ("PAYMENT_TERMS", "ORA_ERP", "N90", "Net 90", "NET90", "EU", True, "2024-01-01", None),
        ("PAYMENT_TERMS", "ORA_ERP", "N120", "Net 120", "NET120", "APAC", True, "2024-01-01", None),
        ("PAYMENT_TERMS", "ORA_ERP", "N30", "Net 30", "NET30", None, True, "2024-01-01", None),
        ("PAYMENT_TERMS", "ORA_ERP", "E", "End of month", "EOM", None, True, "2024-01-01", None),
        ("PAYMENT_TERMS", "ORA_ERP", "D", "2/10 net 30", "DISC210", None, True, "2024-01-01", None),
        ("PAYMENT_TERMS", "ORA_ERP", "?", "Unparseable", "WHENEVER", None, True, "2024-01-01", None),
    ])
    ok, bad = transforms.screenPaymentTerms(transforms.paymentTermsDimension(df))
    rows = {r["PaymentTermsCode"]: r for r in ok.collect()}
    assert rows["NET90"]["NetDays"] == 90 and rows["NET90"]["NetDaysCapped"] == 60
    assert rows["NET120"]["NetDaysCapped"] == 90
    assert rows["NET30"]["NetDaysCapped"] == 30 and rows["EOM"]["NetDays"] == 30
    assert rows["DISC210"]["DiscountDays"] == 10 and float(rows["DISC210"]["DiscountPercent"]) == 2.0
    assert rows["DISC210"]["EarlySettlementFlag"] == "Y" and rows["NET30"]["EarlySettlementFlag"] == "N"
    assert [r["PaymentTermsCode"] for r in bad.collect()] == ["WHENEVER"]


def test_transaction_type_direction_and_flags(spark):
    df = crosswalk(spark, [
        ("TRANSACTION_TYPE", "ORA_ERP", "PO", "Purchase", "PURCH", None, True, "2024-01-01", None),
        ("TRANSACTION_TYPE", "ORA_ERP", "SO", "Sale", "SALE", None, True, "2024-01-01", None),
        ("TRANSACTION_TYPE", "ORA_ERP", "RMA", "Return", "RET", None, True, "2024-01-01", None),
        ("TRANSACTION_TYPE", "ORA_ERP", "ADJ", "Adjust", "ADJ", None, True, "2024-01-01", None),
    ])
    rows = {r["TransactionTypeCode"]: r for r in transforms.transactionTypeDimension(df).collect()}
    assert (rows["PURCH"]["MovementDirectionCode"], rows["PURCH"]["MovementSign"], rows["PURCH"]["AffectsLedgerFlag"]) == ("IN", 1, "Y")
    assert (rows["SALE"]["MovementDirectionCode"], rows["SALE"]["MovementSign"], rows["SALE"]["ReversalAllowedFlag"]) == ("OUT", -1, "N")
    assert rows["RET"]["ReversalAllowedFlag"] == "Y" and rows["ADJ"]["AffectsLedgerFlag"] == "N"


def test_cost_center_hierarchy_hash_and_screen(spark):
    df = crosswalk(spark, [
        ("COST_CENTER", "ORA_ERP", "1000", "Corporate", "CORP", None, True, "2024-01-01", None),
        ("COST_CENTER", "ORA_ERP", "2100", "Sales NA", "SLNA", "NA", True, "2024-01-01", None),
        ("COST_CENTER", "ORA_ERP", "9999", "Suspense", "SUSP", None, True, "2024-01-01", None),
    ])
    ok, bad = transforms.screenCostCenter(transforms.costCenterDimension(df))
    rows = {r["CostCenterCode"]: r for r in ok.collect()}
    assert rows["CORP"]["ParentCostCenterCode"] == "ROOT" and rows["SLNA"]["ParentCostCenterCode"] == "CORP"
    assert rows["SLNA"]["FunctionCode"] == "SL" and rows["SUSP"]["SuspenseFlag"] == "Y"
    assert bad.count() == 0
    renamed = df.withColumn("SourceCodeDescription",
                            F.when(F.col("ConformedCodeValue") == "SLNA", "Sales North America").otherwise(F.col("SourceCodeDescription")))
    hashes = {r["CostCenterCode"]: r["RowHashType2"] for r in transforms.costCenterDimension(renamed).collect()}
    assert hashes["SLNA"] != rows["SLNA"]["RowHashType2"] and hashes["CORP"] == rows["CORP"]["RowHashType2"]


def test_standardize_postal_code(spark):
    df = spark.createDataFrame([("90210-1234", 5), (" sw1a 1aa ", 0), (None, 0), ("k1a 0b1", None)],
                               T.StructType([T.StructField("PostalCode", T.StringType()), T.StructField("TruncateToLength", T.IntegerType())]))
    out = [r["Std"] for r in df.withColumn("Std", transforms.standardizePostalCode("PostalCode", "TruncateToLength")).collect()]
    assert out == ["90210", "SW1A1AA", "", "K1A0B1"]


def test_date_dimension_fiscal_calendars(spark):
    df = datecalendar.buildDateDimension(spark, datetime.date(2024, 6, 29), datetime.date(2024, 7, 2),
                                         {"NA": 7, "EU": 1, "APAC": 4}, "test", 1)
    rows = {r["DateKey"]: r for r in df.collect()}
    assert sorted(rows) == [20240629, 20240630, 20240701, 20240702]
    june30, july1 = rows[20240630], rows[20240701]
    assert (june30["FiscalYearNa"], june30["FiscalPeriodNa"], june30["FiscalQuarterNa"]) == (2024, 12, 4)
    assert (july1["FiscalYearNa"], july1["FiscalPeriodNa"], july1["FiscalQuarterNa"], july1["FiscalWeekNa"]) == (2025, 1, 1, 1)
    assert (july1["FiscalYear"], july1["FiscalMonthNumber"], july1["FiscalYearLabel"]) == (2025, 1, "FY2025")
    assert (july1["FiscalYearEu"], july1["FiscalPeriodEu"], july1["FiscalQuarterEu"]) == (2024, 7, 3)
    assert (july1["FiscalYearApac"], july1["FiscalPeriodApac"]) == (2024, 4)   # APAC year named by its start year
    assert june30["FiscalYearApacAu"] == 2023 and july1["FiscalYearApacAu"] == 2024
    assert june30["DayOfWeek"] == "Sunday" and june30["WeekendFlag"] == "Y" and june30["WorkingDayFlag"] == "N"
    assert july1["ISOWeekNumber"] == 27 and july1["Month"] == "July" and july1["ShortMonth"] == "Jul"
    assert july1["CalendarMonthLabel"] == "CY2024-Jul" and july1["CalendarQuarterNumber"] == 3
    assert all(r["IsReservedMember"] is False for r in rows.values())
    sentinels = {r["DateKey"]: r for r in datecalendar.sentinelDateRows(spark, "test", 1).collect()}
    assert sentinels[-1]["Date"] == datetime.date(1900, 1, 1) and sentinels[-2]["Date"] == datetime.date(1900, 1, 2)
    assert set(sentinels[-1].asDict()) == set(df.columns)


def test_fiscal_year_scalar_rules(spark):
    df = spark.createDataFrame([(6, 2024), (7, 2024), (3, 2024)], ["m", "y"])
    out = df.select(
        datecalendar.fiscalYear(F.col("m"), F.col("y"), F.lit(7), "NA").alias("na"),
        datecalendar.fiscalYear(F.col("m"), F.col("y"), F.lit(7), "APAC").alias("apac"),
        datecalendar.fiscalYear(F.col("m"), F.col("y"), F.lit(1), "EU").alias("eu"),
        datecalendar.fiscalPeriod(F.col("m"), F.lit(7)).alias("p7"),
    ).collect()
    assert [(r["na"], r["apac"], r["eu"], r["p7"]) for r in out] == [(2024, 2023, 2024, 12), (2025, 2024, 2024, 1), (2024, 2023, 2024, 9)]


def test_currency_dimension_rate_status(spark):
    currency = spark.createDataFrame([
        ("USD", "US Dollar", "$", 2, "HALF_EVEN", True, False, None, True),
        ("EUR", "Euro", "E", 2, "HALF_EVEN", False, False, None, True),
        ("AUD", "Aus Dollar", "$", 2, "HALF_EVEN", False, False, None, True),
        ("NOK", "Krone", "kr", 2, "HALF_EVEN", False, False, None, True),
        ("DEM", "Deutsche Mark", "DM", 2, "HALF_EVEN", False, True, "2001-12-31", True),
        ("XXX", "Dead", "x", 2, "HALF_EVEN", False, False, None, False),
    ], T.StructType([
        T.StructField("CurrencyCode", T.StringType()), T.StructField("CurrencyName", T.StringType()),
        T.StructField("CurrencySymbol", T.StringType()), T.StructField("MinorUnitDigits", T.IntegerType()),
        T.StructField("RoundingRuleCode", T.StringType()), T.StructField("IsReportingCurrency", T.BooleanType()),
        T.StructField("IsEuroLegacy", T.BooleanType()), T.StructField("RetiredDate", T.StringType()),
        T.StructField("IsActive", T.BooleanType())])).withColumn("RetiredDate", F.col("RetiredDate").cast("date"))
    fx = spark.createDataFrame([
        ("EUR", "USD", "2024-03-01", "CORPORATE", 1.08), ("EUR", "USD", "2024-03-03", "CORPORATE", 1.09),
        ("EUR", "USD", "2024-03-04", "SPOT", 9.99), ("AUD", "USD", "2023-01-01", "CORPORATE", 0.68),
    ], ["FromCurrencyCode", "ToCurrencyCode", "RateDate", "RateTypeCode", "ConversionRate"]).withColumn("RateDate", F.col("RateDate").cast("date"))
    rows = {r["CurrencyCode"]: r for r in transforms.currencyDimension(currency, fx, asOfDate=datetime.date(2024, 3, 4)).collect()}
    assert "XXX" not in rows
    assert rows["EUR"]["RateStatusCode"] == "RATED" and float(rows["EUR"]["LatestRateToUsd"]) == 1.09 and rows["EUR"]["RateStalenessDays"] == 1
    assert rows["AUD"]["RateStatusCode"] == "STALE" and rows["NOK"]["RateStatusCode"] == "UNRATED" and rows["NOK"]["RateStalenessDays"] == 9999
    assert rows["DEM"]["CurrencyStatusCode"] == "LEGACY" and rows["EUR"]["CurrencyStatusCode"] == "ACTIVE"
    assert rows["USD"]["IsReportingCurrency"] is True
