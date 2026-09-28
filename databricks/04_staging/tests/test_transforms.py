"""Unit tests for the shared cleansing expressions and package transformations."""

import datetime
from decimal import Decimal

from pyspark.sql import functions as F

from stg_common import expressions as X
from stg_common import transforms as T


def rows(df, *cols):
    return [tuple(r) for r in df.select(*cols).collect()]


def test_safe_decimal_handles_legacy_sign_and_separator_conventions(spark):
    df = spark.createDataFrame([("1,234.50",), ("(12.5)",), ("99CR",), ("7-",), ("abc",), ("$ 3",)], "v string")
    got = [r[0] for r in df.select(X.safeDecimal("v", precision=18, scale=2).alias("d")).collect()]
    assert got == [Decimal("1234.50"), Decimal("-12.50"), Decimal("-99.00"), Decimal("-7.00"), None, Decimal("3.00")]
    eu = spark.createDataFrame([("1.234,50",)], "v string")
    assert eu.select(X.safeDecimal("v", ",", 18, 2)).first()[0] == Decimal("1234.50")


def test_source_system_key_collapses_regional_instances_and_pads_numeric_erp_keys(spark):
    df = spark.createDataFrame([("ORA_ERP_EU", "42"), ("WWI_WEB", "a|b"), ("ORA_ERP", ""), ("WWI_OLTP", "X1")], "s string, k string")
    got = [r[0] for r in df.select(X.sourceSystemKey("s", "k")).collect()]
    assert got == ["ORA_ERP|0000000042", "WWI_OLTP|A/B", None, "WWI_OLTP|X1"]


def test_safe_date_is_region_aware_and_nulls_sentinels(spark):
    df = spark.createDataFrame([("03/04/2024", "NA"), ("03/04/2024", "EU"), ("20240304", "APAC"), ("4712-12-31", "NA"), ("N/A", "EU")], "d string, r string")
    got = [r[0] for r in df.select(X.safeDate("d", "r")).collect()]
    assert got[0] == datetime.datetime(2024, 3, 4)
    assert got[1] == datetime.datetime(2024, 4, 3)
    assert got[2] == datetime.datetime(2024, 3, 4)
    assert got[3] is None and got[4] is None


def test_partner_sale_parses_regional_text_and_routes_rejects(spark):
    df = spark.createDataFrame(
        [
            ("p1", "ORD-1", "C1", "IT 1", "2", "1,250.00", "usd", "France", "04/03/2024"),
            ("p2", "ORD-2", "C2", "IT2", "1", "oops", "EUR", "Germany", "2024-03-04"),
            ("p3", "", "C3", "IT3", "1", "10", "EUR", "Germany", "2024-03-04"),
        ],
        "PartnerCode string, PartnerOrderRef string, CustomerRef string, ItemRef string, QuantityText string, AmountText string, CurrencyText string, CountryText string, SaleDateText string",
    )
    derived = T.derivePartnerSale(df)
    valid, badAmount, missingRef = T.splitPartnerSale(derived)
    assert rows(valid, "PartnerCode", "ItemRef", "GrossAmount", "PartnerCurrencyCode", "SaleDate", "CountryName") == [
        ("P1", "IT1", Decimal("1250.00"), "USD", datetime.date(2024, 3, 4), "FRANCE")
    ]
    assert rows(badAmount, "PartnerOrderRef") == [("ORD-2",)]
    assert rows(missingRef, "CustomerRef") == [("C3",)]


def test_product_cleansing_defaults_units_and_splits_priceless(spark):
    df = spark.createDataFrame(
        [
            ("p1 ", "Widget\t large", None, None, Decimal("0"), Decimal("10.00"), None, Decimal("2.5"), "g", "y", None),
            ("p2", "Old", "FAM", "CS", Decimal("6"), Decimal("5.00"), "eur", None, None, "N", "Y"),
            ("p3", "Free", "FAM", "EA", Decimal("1"), None, "USD", None, None, "N", "N"),
        ],
        "PROD_CODE string, PROD_DESC string, PROD_FAMILY_CD string, BASE_UOM_CD string, PACK_QTY decimal(18,4), LIST_PRICE_AMT decimal(18,2), LIST_PRICE_CCY string, NET_WEIGHT decimal(18,4), WEIGHT_UOM_CD string, HAZMAT_FLG string, DISCONTINUED_FLG string",
    )
    cleansed = T.cleanseProduct(df).withColumn("ConversionFactor", F.when(F.col("BaseUomCode") == "CS", F.lit(12))).withColumn("WeightFactorKg", F.when(F.col("WeightUomCode") == "G", F.lit(0.001)))
    converted = T.convertProductUnits(cleansed)
    sellable, discontinued, priceless = T.splitProduct(converted)
    assert rows(sellable, "ProductCode", "ProductDescription", "ProductFamilyCode", "BaseUomCode", "PackQuantity", "HazardousFlag", "EachesPerPack", "NetWeightKg", "ListPriceCurrencyCode") == [
        ("P1", "Widget large", "UNCLASS", "EA", Decimal("1.0000"), "Y", Decimal("1.0000"), Decimal("0.0025"), "USD")
    ]
    assert rows(discontinued, "ProductCode", "EachesPerPack", "ListPriceCurrencyCode") == [("P2", Decimal("72.0000"), "EUR")]
    assert rows(priceless, "ProductCode", "ListPriceAmount") == [("P3", Decimal("0.00"))]


def test_customer_rules_reject_missing_name_and_short_code(spark):
    df = spark.createDataFrame(
        [("C001", "Acme  Ltd", "Acme  Ltd", "RTL", "GOOD", "us", "12-34", "Y", Decimal("100"), "usd", None, "NA", datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)),
         ("C002", "  ", None, None, None, None, None, None, None, None, None, "EU", None, None),
         ("C3", "Short", None, None, None, None, None, None, None, None, None, "EU", None, None)],
        "CUST_CODE string, CUST_NAME string, TRADING_NAME string, CUST_CLASS_CD string, CREDIT_STATUS_CD string, COUNTRY_CD string, TAX_REG_NBR string, CONSENT_FLAG string, CREDIT_LIMIT decimal(18,2), CREDIT_CCY string, LAST_UPD_DT timestamp, REGION_CD string, CREATED_DT timestamp, LAST_UPDATE_DT timestamp",
    )
    valid, missing, malformed = T.splitCustomer(T.cleanseCustomer(df))
    assert rows(valid, "CustomerCode", "CustomerName", "TradingName", "CustomerClassCode", "CountryCode") == [("C001", "Acme Ltd", None, "RTL", "US")]
    assert rows(missing, "CustomerCode") == [("C002",)]
    assert rows(malformed, "CustomerCode") == [("C3",)]


def test_survivorship_dedupe_keeps_the_ordered_winner(spark):
    df = spark.createDataFrame([("k", 1, "old"), ("k", 3, "new"), ("j", 2, "only")], "key string, v int, tag string")
    got = rows(T.survivorshipDedupe(df, ["key"], [F.col("v").desc()]).orderBy("key"), "key", "tag")
    assert got == [("j", "only"), ("k", "new")]
