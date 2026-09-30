from datetime import datetime, timezone
from decimal import Decimal

from customer_party.dedup import deduplicateCustomers

CUSTOMER_SCHEMA = (
    "customer_business_key string, source_customer_id bigint, customer_code string, customer_name string, "
    "customer_name_standardized string, tax_registration_number string, country_code string, region_code string, "
    "source_system_code string, source_modified_date timestamp, credit_limit_amount decimal(18,2), "
    "customer_class_code string, credit_status_code string, marketing_consent_flag string"
)
ADDRESS_SCHEMA = (
    "customer_business_key string, source_address_id bigint, address_type_code string, address_line_1 string, "
    "city_name string, state_province_code string, postal_code string, country_code string, region_code string, "
    "postal_standard string, is_primary boolean, effective_from_date timestamp, postal_is_valid boolean"
)
T_OLD = datetime(2025, 1, 1, tzinfo=timezone.utc)
T_NEW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _cust(bk, code, name, tax, country, region, source, modified, consent="N"):
    return (bk, int(bk.split(":")[1]), code, name, name.upper(), tax, country, region, source, modified, Decimal("1000.00"), "RET", "OK", consent)


def _addr(bk, postal, region="NA", country="US"):
    return (bk, int(bk.split(":")[1]), "BILL", "1 Main St", "Springfield", "IL", postal, country, region, "USPS", True, T_OLD, True)


def test_exact_tax_match_wins_over_name_and_source_rank_picks_survivor(spark):
    customers = spark.createDataFrame(
        [
            _cust("ORA:1", "C0001", "Acme Ltd", "US-123-456", "US", "NA", "WWI_OLTP", T_OLD),
            _cust("ORA:2", "C0002", "ACME Limited", "US123456", "US", "NA", "ORA_ERP", T_NEW),
            _cust("ORA:3", "C0003", "Other Co", "US999", "US", "NA", "ORA_ERP", T_OLD),
        ],
        CUSTOMER_SCHEMA,
    )
    addresses = spark.createDataFrame([_addr("ORA:1", "60601"), _addr("ORA:2", "60602"), _addr("ORA:3", "60603")], ADDRESS_SCHEMA)
    dedup, _ = deduplicateCustomers(customers, addresses)
    rows = {r.customer_business_key: r for r in dedup.collect()}
    assert rows["ORA:1"].duplicate_group_id == rows["ORA:2"].duplicate_group_id
    assert rows["ORA:1"].match_rule_code == "EXACT_TAXNUM"
    # ORA_ERP (rank 30, most recent +10) beats WWI_OLTP (rank 20) even though ORA:1 has the lower business key
    assert rows["ORA:2"].is_survivor_row is True and rows["ORA:1"].is_survivor_row is False
    assert rows["ORA:1"].survivor_business_key == "ORA:2"
    assert rows["ORA:1"].survivorship_rule_applied == "RETIRED:EXACT_TAXNUM"
    assert rows["ORA:3"].match_rule_code == "SINGLETON" and rows["ORA:3"].is_survivor_row is True


def test_name_postal_match_when_tax_numbers_differ(spark):
    customers = spark.createDataFrame(
        [
            _cust("ORA:1", "C0001", "Globex", "GB111", "GB", "EU", "ORA_ERP", T_OLD),
            _cust("ORA:2", "C0002", "Globex", "GB222", "GB", "EU", "ORA_ERP", T_NEW, consent="Y"),
        ],
        CUSTOMER_SCHEMA,
    )
    addresses = spark.createDataFrame([_addr("ORA:1", "SW1A 1AA", "EU", "GB"), _addr("ORA:2", "sw1a1aa", "EU", "GB")], ADDRESS_SCHEMA)
    dedup, standardized = deduplicateCustomers(customers, addresses)
    rows = {r.customer_business_key: r for r in dedup.collect()}
    assert rows["ORA:1"].match_rule_code == "NAME_POSTAL"
    assert rows["ORA:1"].duplicate_group_id == rows["ORA:2"].duplicate_group_id
    # explicit EU opt-in (+50) and recency (+10) make ORA:2 the survivor despite the lower business key of ORA:1
    assert rows["ORA:2"].is_survivor_row and rows["ORA:2"].eu_consent_bonus == 50 and rows["ORA:2"].recency_bonus == 10
    assert {r.postal_code_std for r in standardized.collect()} == {"SW1A1AA"}


def test_fuzzy_name_country_match_only_without_tax_numbers(spark):
    customers = spark.createDataFrame(
        [
            _cust("ORA:1", "C0001", "Initech Corporation Pty", None, "AU", "APAC", "WWI_WEB", T_OLD),
            _cust("ORA:2", "C0002", "Initech Corporate Services", None, "AU", "APAC", "WWI_WEB", T_OLD),
            _cust("ORA:3", "C0003", "Initech Corporation", "AU555", "AU", "APAC", "WWI_WEB", T_OLD),
        ],
        CUSTOMER_SCHEMA,
    )
    addresses = spark.createDataFrame([_addr("ORA:1", "2000", "APAC", "AU"), _addr("ORA:2", "3000", "APAC", "AU")], ADDRESS_SCHEMA)
    dedup, _ = deduplicateCustomers(customers, addresses)
    rows = {r.customer_business_key: r for r in dedup.collect()}
    assert rows["ORA:1"].match_rule_code == "NAME_FUZZY" and rows["ORA:2"].match_rule_code == "NAME_FUZZY"
    assert rows["ORA:1"].is_survivor_row and not rows["ORA:2"].is_survivor_row  # tie -> lowest business key
    assert rows["ORA:3"].match_rule_code == "SINGLETON"
