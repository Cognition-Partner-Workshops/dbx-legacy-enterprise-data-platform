from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import functions as F

from customer_party.dim_customer import buildRegionalCandidates
from customer_party.extract import deriveCustomerStatus, standardizePostalCode
from customer_party.quality import applyDqOutcome, evaluateRules
from customer_party.staging import retentionMonthsFor, standardizeCustomerName

NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)


def _eval(spark, rows, schema, exprs):
    return spark.createDataFrame(rows, schema).select(*exprs).collect()


def test_customer_name_standardization_strips_punctuation_and_legal_suffix(spark):
    rows = _eval(
        spark,
        [("Tailspin  Toys, Inc.",), ("Smith & Sons Pty Ltd",), ("O'Neil-Baker GmbH",), ("LLC",)],
        "n string",
        [F.col("n"), standardizeCustomerName(F.col("n")).alias("s")],
    )
    got = {r.n: r.s for r in rows}
    assert got["Tailspin  Toys, Inc."] == "TAILSPIN TOYS"
    assert got["Smith & Sons Pty Ltd"] == "SMITH AND SONS"
    assert got["O'Neil-Baker GmbH"] == "ONEIL BAKER"
    assert got["LLC"] == "LLC"


def test_postal_code_standardization_by_region(spark):
    rows = _eval(
        spark,
        [("NA", "606011234", None), ("NA", "60601", "5678"), ("NA", "60601", None), ("EU", "sw1a 1aa", None), ("APAC", " 2000 ", None)],
        "region string, postal string, zip4 string",
        [F.col("postal"), standardizePostalCode(F.col("region"), F.col("postal"), F.col("zip4")).alias("s")],
    )
    assert [r.s for r in rows] == ["60601-1234", "60601-5678", "60601", "SW1A1AA", "2000"]


def test_customer_status_and_retention_rules(spark):
    rows = _eval(
        spark,
        [("AC", "N", "N", "EU"), ("AC", "Y", "N", "APAC"), ("AC", "N", "Y", "NA"), (None, "N", "N", "XX")],
        "status string, hold string, deleted string, region string",
        [
            deriveCustomerStatus(F.col("status"), F.col("hold"), F.col("deleted")).alias("status"),
            retentionMonthsFor(F.col("region")).alias("retention"),
        ],
    )
    assert [(r.status, r.retention) for r in rows] == [("AC", 24), ("HD", 60), ("CL", 84), (None, 84)]


STG_SCHEMA = (
    "customer_business_key string, customer_name string, tax_registration_number string, region_code string, "
    "country_code string, marketing_consent_flag string, consent_captured_date timestamp, retention_months int, "
    "credit_limit_amount decimal(18,2), dq_status_code string"
)


def test_dq_screen_marks_fail_and_warn(spark):
    staged = spark.createDataFrame(
        [
            ("ORA:1", "Good Co", "US12345", "NA", "US", "N", None, 84, Decimal("1000.00"), "PASS"),
            ("ORA:2", "", "US12345", "NA", "US", "N", None, 84, Decimal("1000.00"), "PASS"),
            ("ORA:3", "Short Tax", "US1", "NA", "US", "N", None, 84, Decimal("1000.00"), "PASS"),
            ("ORA:4", "Stale Consent", "DE12345", "EU", "DE", "Y", datetime(2020, 1, 1, tzinfo=timezone.utc), 24, Decimal("1000.00"), "PASS"),
            ("ORA:5", "Wrong Region", "DE12345", "EU", "US", "N", None, 24, Decimal("1000.00"), "PASS"),
            ("ORA:6", "Big Credit", "AU12345", "APAC", "AU", "N", None, 60, Decimal("99000000.00"), "PASS"),
        ],
        STG_SCHEMA,
    )
    results = evaluateRules(staged, F.lit(NOW).cast("timestamp"))
    outcome = {r.customer_business_key: r for r in applyDqOutcome(staged, results).collect()}
    assert outcome["ORA:1"].dq_status_code == "PASS"
    assert outcome["ORA:2"].dq_status_code == "FAIL"
    assert outcome["ORA:3"].dq_status_code == "WARN"
    assert outcome["ORA:4"].dq_status_code == "FAIL" and outcome["ORA:4"].dq_rule_codes == "EU_CONSENT_RETENTION_BREACH"
    assert outcome["ORA:5"].dq_status_code == "FAIL"
    assert outcome["ORA:6"].dq_status_code == "WARN"


DIM_STG_SCHEMA = (
    "customer_business_key string, source_customer_id bigint, customer_code string, customer_name string, trading_name string, "
    "customer_name_standardized string, customer_class_code string, credit_status_code string, customer_status_code string, "
    "country_code string, region_code string, tax_registration_number string, vat_registration_number string, "
    "gst_registration_number string, tax_exempt_flag string, marketing_consent_flag string, consent_captured_date timestamp, "
    "consent_source_code string, retention_until_date timestamp, retention_months int, credit_limit_amount decimal(18,2), "
    "credit_currency_code string, is_on_credit_hold boolean, buying_group_code string, payment_terms_code string, "
    "account_manager_code string, first_order_date timestamp, last_order_date timestamp, source_created_date timestamp, "
    "source_modified_date timestamp, source_system_code string, change_hash string, duplicate_group_id bigint, "
    "is_survivor_row boolean, survivorship_rule_applied string, dq_status_code string"
)
BILLING_SCHEMA = (
    "customer_business_key string, source_address_id bigint, address_type_code string, address_line_1_std string, "
    "city_name_std string, state_province_code string, postal_code_std string, country_code string, region_code string, "
    "postal_standard string, address_quality_code string"
)


def _stg(bk, region, country, **kw):
    base = {
        "customer_business_key": bk, "source_customer_id": 1, "customer_code": "C0001", "customer_name": "Cust", "trading_name": None,
        "customer_name_standardized": "CUST", "customer_class_code": "RET", "credit_status_code": "OK", "customer_status_code": "AC",
        "country_code": country, "region_code": region, "tax_registration_number": "12345678", "vat_registration_number": "DE123456789",
        "gst_registration_number": "51824753556", "tax_exempt_flag": "N", "marketing_consent_flag": "Y", "consent_captured_date": NOW,
        "consent_source_code": "WEB", "retention_until_date": None, "retention_months": 84, "credit_limit_amount": Decimal("5000.00"),
        "credit_currency_code": "USD", "is_on_credit_hold": False, "buying_group_code": None, "payment_terms_code": "NET30",
        "account_manager_code": None, "first_order_date": None, "last_order_date": None, "source_created_date": NOW,
        "source_modified_date": NOW, "source_system_code": "ORA_ERP", "change_hash": "h", "duplicate_group_id": None,
        "is_survivor_row": True, "survivorship_rule_applied": "SINGLETON", "dq_status_code": "PASS",
    }
    base.update(kw)
    return base


def test_regional_candidates_apply_region_specific_rules(spark):
    staged = spark.createDataFrame(
        [
            _stg("ORA:1", "NA", "US", is_on_credit_hold=True),
            _stg("ORA:2", "EU", "DE", source_created_date=datetime(2015, 1, 1, tzinfo=timezone.utc), retention_months=24),
            _stg("ORA:3", "APAC", "AU", gst_registration_number="51 824 753 556"),
            _stg("ORA:4", "NA", "US", is_survivor_row=False),
            _stg("ORA:5", "NA", "US", dq_status_code="FAIL"),
        ],
        DIM_STG_SCHEMA,
    )
    billing = spark.createDataFrame(
        [("ORA:1", 1, "BILL", "1 MAIN", "CHICAGO", "IL", "606011234", "US", "NA", "USPS", "OK"),
         ("ORA:2", 2, "BILL", "1 STR", "BERLIN", "BE", "10115", "DE", "EU", "UPU", "OK"),
         ("ORA:3", 3, "BILL", "1 RD", "SYDNEY", "NSW", "2000", "AU", "APAC", "APAC-LOCAL", "OK")],
        BILLING_SCHEMA,
    )
    na, _ = buildRegionalCandidates(staged, billing, "NA", NOW)
    eu, _ = buildRegionalCandidates(staged, billing, "EU", NOW)
    apac, _ = buildRegionalCandidates(staged, billing, "APAC", NOW)
    naRows = {r.customer_business_key: r for r in na.collect()}
    assert set(naRows) == {"ORA:1"}  # losers and DQ failures never reach the dimension
    assert naRows["ORA:1"].credit_limit_usd == 0 and naRows["ORA:1"].credit_limit == 5000  # hold zeroes the reported limit
    assert naRows["ORA:1"].region == "NA" and naRows["ORA:1"].postal_code_plus_four == "60601-1234"
    euRow = eu.collect()[0]
    assert euRow.region == "EU" and euRow.retention_expired and euRow.is_pseudonymised and euRow.vat_number_is_well_formed
    apacRow = apac.collect()[0]
    assert apacRow.region == "APAC" and apacRow.gst_registration_clean == "51824753556" and apacRow.gst_registration_is_valid
