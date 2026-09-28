from datetime import date

from pyspark.sql import Row
from pyspark.sql import functions as F

from dq_quality import screens


def _codes(df):
    return sorted(r["RejectReasonCode"] for r in df.collect())


def test_screen_customer_routes_like_ordered_conditional_split(spark):
    customers = spark.createDataFrame([
        Row(CustomerCode="C1", CustomerName="Ok Ltd", CountryCode="US", RegionCode="NA", CustomerClassCode="A",
            TaxRegistrationNumber="123456789", MarketingConsentFlag="Y", RetentionMonths=12.0, CreditLimitAmount=1000.0),
        Row(CustomerCode="C2", CustomerName="   ", CountryCode="US", RegionCode="NA", CustomerClassCode="A",
            TaxRegistrationNumber="123456789", MarketingConsentFlag="Y", RetentionMonths=12.0, CreditLimitAmount=1000.0),
        Row(CustomerCode="C3", CustomerName="EU GmbH", CountryCode="DE", RegionCode="EU", CustomerClassCode="A",
            TaxRegistrationNumber="DE123456", MarketingConsentFlag="U", RetentionMonths=12.0, CreditLimitAmount=1000.0),
        Row(CustomerCode="C4", CustomerName="Neg", CountryCode="US", RegionCode="NA", CustomerClassCode="A",
            TaxRegistrationNumber="123456789", MarketingConsentFlag="N", RetentionMonths=12.0, CreditLimitAmount=-5.0),
        Row(CustomerCode="C5", CustomerName="Nowhere", CountryCode="ZZ", RegionCode="NA", CustomerClassCode="A",
            TaxRegistrationNumber="123456789", MarketingConsentFlag="N", RetentionMonths=12.0, CreditLimitAmount=5.0),
    ])
    countries = spark.createDataFrame([Row(CountryCode="US", ReferenceRegionCode="NA"),
                                       Row(CountryCode="DE", ReferenceRegionCode="EU")])
    out = screens.screenCustomer(customers, countries)
    assert [r["CustomerCode"] for r in out.passed.collect()] == ["C1"]
    assert _codes(out.rejected) == ["DQ_CUST_CONSENT", "DQ_CUST_COUNTRY", "DQ_CUST_CREDIT_NEG", "DQ_CUST_NAME_NULL"]


def test_screen_supplier_duplicates_and_missing(spark):
    suppliers = spark.createDataFrame([
        Row(SupplierCode="S1", SupplierName="A", TaxIdentifier="DE 123-456", PaymentTermsCode="N30", CountryCode="DE", RegionCode="EU", IsActive=True),
        Row(SupplierCode="S2", SupplierName="B", TaxIdentifier="de123456", PaymentTermsCode="N30", CountryCode="DE", RegionCode="EU", IsActive=True),
        Row(SupplierCode="S3", SupplierName="C", TaxIdentifier=None, PaymentTermsCode="N30", CountryCode="US", RegionCode="NA", IsActive=True),
        Row(SupplierCode="S4", SupplierName="D", TaxIdentifier="US999", PaymentTermsCode=None, CountryCode="US", RegionCode="NA", IsActive=True),
        Row(SupplierCode="S5", SupplierName="E", TaxIdentifier="US111", PaymentTermsCode="N60", CountryCode="US", RegionCode="NA", IsActive=True),
    ])
    out = screens.screenSupplier(suppliers)
    # Legacy "Route Supplier Failures" tests SupplierCount == 1 first: a lone NULL identifier ("NONE")
    # and a missing-terms row are still "Unique Supplier"; only real duplicate groups are quarantined.
    assert sorted(r["SupplierCode"] for r in out.passed.collect()) == ["S3", "S4", "S5"]
    rejected = {r["SupplierCode"]: r["RejectReasonCode"] for r in out.rejected.collect()}
    assert rejected == {"S1": "DQ_SUPP_TAXID_DUP", "S2": "DQ_SUPP_TAXID_DUP"}
    assert set(rejected.values()) <= set(screens.SUPPLIER_REASON_CODES)


def test_screen_order_line_lookup_miss_redirects(spark):
    lines = spark.createDataFrame([
        Row(OrderLineId="L1", OrderId="O1", CustomerId="C1", StockItemId="I1", Quantity=5, UnitPriceAmount=10.0, ExtendedAmount=50.0, RegionCode="NA"),
        Row(OrderLineId="L2", OrderId="O1", CustomerId="C1", StockItemId="I1", Quantity=0, UnitPriceAmount=10.0, ExtendedAmount=0.0, RegionCode="NA"),
        Row(OrderLineId="L3", OrderId="O2", CustomerId="C9", StockItemId="I1", Quantity=5, UnitPriceAmount=10.0, ExtendedAmount=50.0, RegionCode="NA"),
    ])
    customers = spark.createDataFrame([Row(CustomerId="C1", CustomerCode="C1")])
    out = screens.screenOrderLine(lines, customers)
    assert [r["OrderLineId"] for r in out.passed.collect()] == ["L1"]
    assert [r["OrderLineId"] for r in out.branches["Customer Lookup Failure"].collect()] == ["L3"]
    # lookup misses are a separate error output (ERR Customer Lookup Failure), not the rule rejects
    assert _codes(out.rejected) == ["DQ_OL_QTY_RANGE"]


def test_screen_file_rows_structural_checks(spark):
    rows = spark.createDataFrame([
        Row(FileRowId="f:1", SourceFileName="f", FileLineNumber=1, RawLine="a|b|c|d|e|f|g|h|i", DelimiterCount=8,
            SaleDateText="2024-01-02", AmountText="10.00"),
        Row(FileRowId="f:2", SourceFileName="f", FileLineNumber=2, RawLine="a|b|c", DelimiterCount=2,
            SaleDateText="2024-01-02", AmountText="10.00"),
        Row(FileRowId="f:3", SourceFileName="f", FileLineNumber=3, RawLine="a|b|c|d|e|f|g|h|i", DelimiterCount=8,
            SaleDateText=None, AmountText="10.00"),
        Row(FileRowId="f:4", SourceFileName="f", FileLineNumber=4, RawLine="a|b|c|d|e|f|g|h|\ufffd", DelimiterCount=8,
            SaleDateText="2024-01-02", AmountText="10.00"),
    ])
    out = screens.screenFileRows(rows)
    assert [r["FileRowId"] for r in out.passed.collect()] == ["f:1"]
    assert out.rejected.count() == 3


def test_reconstruct_file_row_counts_delimiters(spark):
    df = spark.createDataFrame([("1", "2", None)], "A string, B string, C string")
    out = screens.reconstructFileRow(df, ["A", "B", "C"]).collect()[0]
    assert out["RawLine"] == "1|2"  # a NULL field drops its delimiter
    assert out["DelimiterCount"] == 1


def test_referential_screens(spark):
    orders = spark.createDataFrame([Row(OrderLineId="L1", StockItemId="I1", PackageTypeCode="P1", SourceObjectName="stg.OrderLine"),
                                    Row(OrderLineId="L2", StockItemId="I9", PackageTypeCode="P1", SourceObjectName="stg.OrderLine")])
    stock = spark.createDataFrame([Row(StockItemId="I1", StockItemName="Widget")])
    pkg = spark.createDataFrame([Row(PackageTypeCode="P1", PackageTypeName="Each")])
    out = screens.screenReferentialOrder(orders, stock, pkg)
    assert [r["OrderLineId"] for r in out.passed.collect()] == ["L1"]
    miss = out.rejected.collect()
    assert len(miss) == 1 and miss[0]["LookupName"].startswith("Lookup Stock Item")


def test_prepare_reprocess_routes_outcomes(spark):
    rejects = spark.createDataFrame([
        Row(RejectedRowId=1, ObjectName="stg.OrderLine", BusinessKey="L1|I1", RejectReasonCode="DQ_REF_STOCK", RetryCount=1,
            FirstRejectedAtUtc=date(2024, 1, 1), PayloadJson="{}"),
        Row(RejectedRowId=2, ObjectName="stg.OrderLine", BusinessKey="L2|I1", RejectReasonCode="DQ_REF_STOCK", RetryCount=1,
            FirstRejectedAtUtc=date(2023, 1, 1), PayloadJson="{}"),
        Row(RejectedRowId=3, ObjectName="stg.OrderLine", BusinessKey="L3|I9", RejectReasonCode="DQ_REF_STOCK", RetryCount=None,
            FirstRejectedAtUtc=date(2024, 1, 1), PayloadJson="{}"),
    ])
    stock = spark.createDataFrame([Row(StockItemId="I1", StockItemName="Widget")])
    out = screens.prepareReprocess(rejects, stock, nowUtc=F.lit("2024-01-10").cast("timestamp"))
    assert [r["RejectedRowId"] for r in out.passed.collect()] == [1]
    assert [r["RejectedRowId"] for r in out.branches["Aged Out"].collect()] == [2]
    unresolved = out.branches["Still Unresolved"].collect()
    assert [r["RejectedRowId"] for r in unresolved] == [3] and unresolved[0]["RetryCount"] == 1
