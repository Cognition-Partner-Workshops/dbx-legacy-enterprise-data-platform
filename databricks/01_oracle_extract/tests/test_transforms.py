from pyspark.sql import Row

from oracle_extract.model import ConditionalSplit, DerivedColumn, Lookup
from oracle_extract.specs import PACKAGES
from oracle_extract.transforms import (
    addIngestionMetadata, applyConditionalSplit, applyDerivedColumns, applyLookup, assignRowNumberKey, rejectPayload,
)


def test_customer_address_postal_standardisation(spark):
    spec = PACKAGES["EXT_ORA_CustomerAddress"]
    derived = spec.sources[0].derived
    df = spark.createDataFrame([
        Row(ADDRESS_LINE_1=" 12 high st ", CITY_NAME=" london ", POSTAL_CD="sw1a 1aa", REGION_CD="EU"),
        Row(ADDRESS_LINE_1="1 Main", CITY_NAME="Denver", POSTAL_CD=" 80202 ", REGION_CD="NA"),
    ])
    out = {r["REGION_CD"]: r for r in applyDerivedColumns(df, derived).collect()}
    assert out["EU"]["AddressLine1Std"] == "12 HIGH ST"
    assert out["EU"]["CityNameStd"] == "LONDON"
    assert out["EU"]["PostalCdStd"] == "SW1A1AA"
    assert out["NA"]["PostalCdStd"] == "80202"


def test_purchase_order_line_receipt_pct_guards_zero_quantity(spark):
    derived = [d for d in PACKAGES["EXT_ORA_PurchaseOrderLine"].sources[0].derived if d.name == "ReceiptCompletePct"]
    assert derived
    df = spark.createDataFrame([Row(ORDER_QTY=0.0, RECEIVED_QTY=5.0), Row(ORDER_QTY=8.0, RECEIVED_QTY=2.0)])
    out = sorted(float(r["ReceiptCompletePct"]) for r in applyDerivedColumns(df, derived).collect())
    assert out == [0.0, 0.25]


def test_conditional_split_default_receives_null_and_false(spark):
    split = ConditionalSplit(name="Route", caseName="Active", matchExpr="STATUS_CD = 'A'", defaultName="Other", defaultIsReject=True)
    df = spark.createDataFrame([Row(ID=1, STATUS_CD="A"), Row(ID=2, STATUS_CD="X"), Row(ID=3, STATUS_CD=None)])
    result = applyConditionalSplit(df, split)
    assert sorted(r["ID"] for r in result.matched.collect()) == [1]
    assert sorted(r["ID"] for r in result.default.collect()) == [2, 3]


def test_lookup_redirects_unmatched_and_dedups_reference(spark):
    lookup = Lookup(name="Lookup Geography Key", legacyTable="raw.OracleGeography",
                    joinColumns=(("COUNTRY_CD", "CountryCode"), ("POSTAL_CD", "PostalCode")),
                    outputColumns=(("GeographyId", "GeographyKey", "int"),), rejectReasonCode="GEO_NOMATCH")
    inp = spark.createDataFrame([Row(ID=1, COUNTRY_CD="US", POSTAL_CD="80202"), Row(ID=2, COUNTRY_CD="US", POSTAL_CD="99999")])
    ref = spark.createDataFrame([Row(CountryCode="US", PostalCode="80202", GeographyId=7),
                                 Row(CountryCode="US", PostalCode="80202", GeographyId=8)])
    result = applyLookup(inp, lookup, ref)
    matched = result.matched.collect()
    assert len(matched) == 1 and matched[0]["GeographyKey"] in (7, 8)
    unmatched = result.unmatched.collect()
    assert [r["ID"] for r in unmatched] == [2] and set(unmatched[0].__fields__) == {"ID", "COUNTRY_CD", "POSTAL_CD"}


def test_metadata_and_reject_payload(spark):
    df = spark.createDataFrame([Row(CUST_ID=1, CUST_NBR="C1")])
    out = addIngestionMetadata(df, 42, 99, "ORA_ERP", "2024-01-01 00:00:00", "2024-01-02 00:00:00").first()
    assert (out["BatchId"], out["PackageExecutionId"], out["SourceSystemCode"]) == (42, 99, "ORA_ERP")
    assert out["WatermarkFrom"] == "2024-01-01 00:00:00" and out["ExtractedAtUtc"] is not None
    payload = rejectPayload(df, "CUST_NBR", "GEO_NOMATCH", "no geography").first()
    assert payload["BusinessKey"] == "C1" and '"CUST_ID":1' in payload["RecordPayload"]


def test_cost_center_surrogate_keys_follow_code_order(spark):
    df = spark.createDataFrame([Row(COST_CENTER_CD="CC-200", CostCenterKey=0), Row(COST_CENTER_CD="CC-100", CostCenterKey=0)])
    out = {r["COST_CENTER_CD"]: r["CostCenterKey"] for r in assignRowNumberKey(df, "CostCenterKey", "COST_CENTER_CD").collect()}
    assert out == {"CC-100": 1, "CC-200": 2}


def test_all_derived_expressions_parse(spark):
    """Every SSIS expression translation must be valid Spark SQL against the source columns."""
    for spec in PACKAGES.values():
        for source in spec.sources:
            cols = {c.name: c.sparkType for c in source.columns}
            df = spark.createDataFrame([], ", ".join(f"`{n}` {t}" for n, t in cols.items()))
            applyDerivedColumns(df, source.derived).schema
            if source.split:
                applyConditionalSplit(applyDerivedColumns(df, source.derived), source.split).matched.schema
