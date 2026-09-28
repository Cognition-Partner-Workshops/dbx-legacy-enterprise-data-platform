"""Conformed customer + hybrid SCD2 dim_customer: insert / change / no-change /
re-run idempotency, CODE_XLAT pass-through, region derivation, survivorship."""

from datetime import date, datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

from sales_lakehouse.common.tables import overwriteTable
from sales_lakehouse.silver import customers as cu
from sales_lakehouse.silver import party_resolution as pr

HIGH = datetime(9999, 12, 31, 23, 59, 59)

CUSTOMER_SCHEMA = T.StructType(
    [
        T.StructField("CustomerID", T.IntegerType()),
        T.StructField("CustomerName", T.StringType()),
        T.StructField("BillToCustomerID", T.IntegerType()),
        T.StructField("CustomerCategoryID", T.IntegerType()),
        T.StructField("BuyingGroupID", T.IntegerType()),
        T.StructField("PrimaryContactPersonID", T.IntegerType()),
        T.StructField("DeliveryCityID", T.IntegerType()),
        T.StructField("PostalCityID", T.IntegerType()),
        T.StructField("CreditLimit", T.DecimalType(18, 2)),
        T.StructField("AccountOpenedDate", T.DateType()),
        T.StructField("StandardDiscountPercentage", T.DecimalType(18, 3)),
        T.StructField("IsOnCreditHold", T.BooleanType()),
        T.StructField("PaymentDays", T.IntegerType()),
        T.StructField("PhoneNumber", T.StringType()),
        T.StructField("WebsiteURL", T.StringType()),
        T.StructField("DeliveryPostalCode", T.StringType()),
        T.StructField("PostalPostalCode", T.StringType()),
        T.StructField("SalesTerritoryID", T.IntegerType()),
        T.StructField("RegionCode", T.StringType()),
        T.StructField("TaxRegistrationNumber", T.StringType()),
        T.StructField("MarketingConsentFlag", T.BooleanType()),
        T.StructField("ConsentCapturedWhen", T.TimestampType()),
        T.StructField("DataRetentionExpiresOn", T.DateType()),
        T.StructField("ValidFrom", T.TimestampType()),
        T.StructField("ValidTo", T.TimestampType()),
    ]
)


def customer(
    cid,
    name,
    validFrom=datetime(2020, 1, 1),
    creditLimit="1000.00",
    territory=1,
    region=None,
    tax=None,
    consent=None,
    consentWhen=None,
    retention=None,
    phone="(555) 1234",
    postal="90210",
    category=1,
    group=None,
):
    from decimal import Decimal

    return (
        cid,
        name,
        cid,
        category,
        group,
        10,
        1,
        1,
        Decimal(creditLimit),
        date(2013, 1, 1),
        Decimal("0.000"),
        False,
        7,
        phone,
        "https://example.test",
        postal,
        postal,
        territory,
        region,
        tax,
        consent,
        consentWhen,
        retention,
        validFrom,
        HIGH,
    )


def refTables(spark):
    categories = spark.createDataFrame(
        [(1, "Novelty Shop", datetime(2013, 1, 1), HIGH), (2, "Supermarket", datetime(2013, 1, 1), HIGH)],
        ["CustomerCategoryID", "CustomerCategoryName", "ValidFrom", "ValidTo"],
    )
    groups = spark.createDataFrame(
        [(1, "Tailspin Toys", datetime(2013, 1, 1), HIGH)], ["BuyingGroupID", "BuyingGroupName", "ValidFrom", "ValidTo"]
    )
    territories = spark.createDataFrame(
        [
            (1, "na-west", "NA West", "NA", "USA", "USD"),
            (2, "eu-de", "EU Germany", "EU", "DEU", "EUR"),
            (3, "ap-jp", "APAC Japan", "APAC", "JPN", "JPY"),
        ],
        ["SalesTerritoryID", "TerritoryCode", "TerritoryName", "RegionCode", "CountryISO3", "ReportingCurrencyCode"],
    )
    people = spark.createDataFrame(
        [(10, "Kayla Woodcock", "Kayla", "kayla@example.test", datetime(2013, 1, 1), HIGH)],
        ["PersonID", "FullName", "PreferredName", "EmailAddress", "ValidFrom", "ValidTo"],
    )
    custMaster = spark.createDataFrame(
        [
            (100, "C-NA-0000100", "Tailspin Toys (Head Office)", "NA", "US", "AC", "N30", "USD", None, None, None),
            (200, "C-EU-0000200", "Wingtip Toys (Berlin)", "EU", "DE", "AC", "N60", "EUR", None, "DE123456789", None),
        ],
        "CUST_ID long, CUST_NBR string, CUST_NAME string, REGION_CD string, COUNTRY_CD string, CUST_STATUS_CD string, "
        "PAYMENT_TERMS_CD string, PRIMARY_CURR_CD string, TAX_REG_NBR string, VAT_REG_NBR string, GST_REG_NBR string",
    )
    partyResolution = spark.createDataFrame(
        [(1, 100, 100, pr.STATUS_DIRECT), (2, 200, 200, pr.STATUS_DIRECT)],
        ["wwi_customer_id", "raw_party_id", "resolved_party_id", "resolution_status_code"],
    )
    codeTranslation = spark.createDataFrame(
        [
            # generic mapping, then a region-specific EU override that must win
            ("PAYMENT_TERMS", "ORA_ERP", "N30", "NET30", None, "Y", datetime(2010, 1, 1), None),
            ("PAYMENT_TERMS", "ORA_ERP", "N60", "NET60", None, "Y", datetime(2010, 1, 1), None),
            ("PAYMENT_TERMS", "ORA_ERP", "N60", "NET60_EU", "EU", "Y", datetime(2012, 1, 1), None),
            ("PAYMENT_TERMS", "ORA_ERP", "N60", "NET60_OLD", "EU", "N", datetime(2011, 1, 1), None),
        ],
        "CODE_SET_CD string, SOURCE_SYS_CD string, SOURCE_VALUE_TXT string, TARGET_VALUE_TXT string, REGION_CD string, "
        "ACTIVE_FLG string, EFFECTIVE_FROM_DT timestamp, EFFECTIVE_TO_DT timestamp",
    )
    return categories, groups, territories, people, custMaster, partyResolution, codeTranslation


def conform(spark, rows, batchId=1):
    customers = spark.createDataFrame(rows, CUSTOMER_SCHEMA)
    return cu.conformCustomers(customers, *refTables(spark), batchId=batchId)


def byId(df):
    return {r.wwi_customer_id: r for r in df.collect()}


def test_conformation_region_currency_and_code_translation(spark):
    rows = byId(
        conform(
            spark,
            [
                customer(1, "Tailspin Toys (Head Office)"),
                customer(2, "Wingtip Toys (Berlin)", territory=2, consent=True, consentWhen=datetime(2019, 5, 1)),
            ],
        )
    )
    na, eu = rows[1], rows[2]
    assert na.region_code == "NA" and eu.region_code == "EU"
    assert na.credit_limit_currency_code == "USD" and eu.credit_limit_currency_code == "EUR"
    assert na.customer_category_name == "Novelty Shop"
    assert na.erp_party_id == 100 and na.erp_customer_number == "C-NA-0000100"
    # translated code, region-specific active mapping wins over the generic one
    assert na.payment_terms_code == "NET30" and na.payment_terms_code_translated_flag is True
    assert eu.payment_terms_code == "NET60_EU" and eu.payment_terms_code_translated_flag is True
    # LEGACY QUIRK: no CUST_STATUS mapping -> source code passes through untranslated
    assert na.customer_status_code == "AC" and na.customer_status_code_translated_flag is False
    assert na.dq_status_code == "PASS" and na.is_survivor_row is True


def test_untranslated_code_passes_through_when_no_mapping(spark):
    categories, groups, territories, people, custMaster, party, xlat = refTables(spark)
    customers = spark.createDataFrame([customer(1, "Tailspin Toys")], CUSTOMER_SCHEMA)
    out = byId(
        cu.conformCustomers(customers, categories, groups, territories, people, custMaster, party, xlat.limit(0), 1)
    )
    assert out[1].payment_terms_code == "N30" and out[1].payment_terms_code_translated_flag is False


def test_screens_and_regional_consent(spark):
    rows = byId(
        conform(
            spark,
            [
                customer(1, "   ", region="NA"),  # MISSING_NAME
                customer(7, "No Region", territory=None),  # BAD_REGION (no territory, no ERP party)
                customer(3, "EU no consent", territory=2),  # NO_CONSENT (opt-in region)
                customer(4, "NA null consent is fine", region="NA"),  # opt-out region -> PASS
                customer(5, "Stale", region="NA", retention=date(2000, 1, 1)),  # WARN
                customer(6, "Negative credit", region="NA", creditLimit="-1.00"),  # BAD_CREDIT
            ],
        )
    )
    assert (rows[1].dq_status_code, rows[1].dq_reason_code) == ("FAIL", "MISSING_NAME")
    assert (rows[7].dq_status_code, rows[7].dq_reason_code) == ("FAIL", "BAD_REGION")
    assert (rows[3].dq_status_code, rows[3].dq_reason_code) == ("FAIL", "NO_CONSENT")
    assert rows[4].dq_status_code == "PASS"
    assert (rows[5].dq_status_code, rows[5].dq_reason_code) == ("WARN", "STALE_ACCOUNT")
    assert rows[5].suppress_marketing_attributes_flag is True
    assert (rows[6].dq_status_code, rows[6].dq_reason_code) == ("FAIL", "BAD_CREDIT")


def test_latest_source_version_and_survivorship(spark):
    rows = byId(
        conform(
            spark,
            [
                # two extracts of the same customer: latest ValidFrom wins
                customer(1, "Old Name", validFrom=datetime(2020, 1, 1), region="NA"),
                customer(1, "New Name", validFrom=datetime(2021, 1, 1), region="NA"),
                # two OLTP customers sharing a tax number: higher score survives
                customer(2, "Dup A", region="NA", tax="US 12-345", phone=None, validFrom=datetime(2020, 1, 1)),
                customer(3, "Dup B", region="NA", tax="US12345", validFrom=datetime(2021, 6, 1)),
            ],
        )
    )
    assert len(rows) == 3 and rows[1].customer_name == "New Name"
    assert rows[3].is_survivor_row is True and rows[3].dq_status_code == "PASS"
    assert rows[2].is_survivor_row is False and rows[2].dq_status_code == "WARN"
    assert rows[2].dq_reason_code == "DUPLICATE_LOSER"
    assert rows[2].duplicate_group_id == rows[3].duplicate_group_id
    assert rows[2].survivorship_rule_applied == "TAX_EXACT"


def writeBronze(spark, cfg, rows):
    categories, groups, territories, people, custMaster, party, xlat = refTables(spark)
    overwriteTable(spark.createDataFrame(rows, CUSTOMER_SCHEMA), cfg.fqn("bronze", cu.BRONZE_CUSTOMERS))
    overwriteTable(categories, cfg.fqn("bronze", cu.BRONZE_CUSTOMER_CATEGORIES))
    overwriteTable(groups, cfg.fqn("bronze", cu.BRONZE_BUYING_GROUPS))
    overwriteTable(territories, cfg.fqn("bronze", cu.BRONZE_SALES_TERRITORIES))
    overwriteTable(people, cfg.fqn("bronze", cu.BRONZE_PEOPLE))
    overwriteTable(custMaster, cfg.fqn("bronze", cu.BRONZE_CUST_MASTER))
    overwriteTable(xlat, cfg.fqn("bronze", cu.BRONZE_CODE_TRANSLATION))
    overwriteTable(party, cfg.fqn("silver", pr.PARTY_RESOLUTION_TABLE))


def dim(spark, cfg):
    return spark.table(cfg.fqn("silver", cu.DIM_CUSTOMER_TABLE)).filter(F.col("customer_key") != cu.UNKNOWN_MEMBER_KEY)


def test_dim_customer_scd2_insert_change_nochange_rerun(spark, cfg):
    spark.sql(f"DROP TABLE IF EXISTS {cfg.fqn('silver', cu.DIM_CUSTOMER_TABLE)}")
    t1, t2 = datetime(2020, 1, 1), datetime(2021, 3, 1)
    base = [
        customer(1, "Tailspin Toys", validFrom=t1, region="NA"),
        customer(2, "Wingtip Toys", validFrom=t1, region="NA"),
    ]

    # insert
    writeBronze(spark, cfg, base)
    cu.run(spark, cfg)
    d = dim(spark, cfg)
    assert d.count() == 2 and d.filter("is_current").count() == 2
    unknown = spark.table(cfg.fqn("silver", cu.DIM_CUSTOMER_TABLE)).filter(f"customer_key = {cu.UNKNOWN_MEMBER_KEY}")
    assert unknown.count() == 1 and unknown.first().customer_name == "Unknown"

    # rerun of the same batch: no new versions
    cu.run(spark, cfg)
    assert dim(spark, cfg).count() == 2

    # Type 2 change on customer 1 (name), Type 1 change on customer 2 (phone), rerun twice
    changed = [
        customer(1, "Tailspin Toys Renamed", validFrom=t2, region="NA"),
        customer(2, "Wingtip Toys", validFrom=t2, region="NA", phone="(555) 9999"),
    ]
    writeBronze(spark, cfg, changed)
    cu.run(spark, cfg)
    cu.run(spark, cfg)
    d = dim(spark, cfg)
    assert d.count() == 3
    c1 = {r.version_number: r for r in d.filter("wwi_customer_id = 1").collect()}
    assert c1[1].is_current is False and c1[1].valid_to == t2
    assert c1[2].is_current is True and c1[2].valid_from == t2 and c1[2].customer_name == "Tailspin Toys Renamed"
    assert c1[2].valid_to == HIGH
    c2 = d.filter("wwi_customer_id = 2").collect()
    assert len(c2) == 1 and c2[0].is_current is True and c2[0].phone_number == "(555) 9999"
    assert d.select("customer_key").distinct().count() == 3

    # customer table carries the silver metadata
    cust = spark.table(cfg.fqn("silver", cu.CUSTOMER_TABLE))
    assert {"row_hash", "change_hash", "batch_id", "loaded_at_utc", "dq_status_code", "erp_party_id"} <= set(
        cust.columns
    )
    assert cust.count() == 2
