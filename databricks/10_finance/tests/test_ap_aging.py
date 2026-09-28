import datetime as dt
from decimal import Decimal

from pyspark.sql import Row

import finance_rules as rules

AS_OF = dt.date(2024, 3, 31)


def invoices(spark):
    cols = ["ApInvoiceKey", "SupplierId", "SupplierSiteCode", "InvoiceNumber", "InvoiceDate", "DueDate", "CurrencyCode",
            "LedgerCode", "RegionCode", "InvoiceAmount", "PaidAmount", "RecoverableVatAmount", "GstInputCreditAmount",
            "PaymentTermsCode", "InvoiceStatusCode", "IsOnHold", "HoldReasonCode", "LoadBatchId"]
    d = lambda x: Decimal(str(x))  # noqa: E731
    rows = [
        # current, NA, 2% discount terms
        (1, "S1", None, "INV1", dt.date(2024, 3, 1), dt.date(2024, 4, 10), "USD", "NA01", "NA", d(1000), d(0), None, None, "N30", "OPEN", False, None, 5),
        # 15 days past due, EU with recoverable VAT
        (2, "S2", None, "INV2", dt.date(2024, 2, 1), dt.date(2024, 3, 16), "EUR", "EU01", "EU", d(1200), d(200), d(200), None, "N30", "OPEN", False, None, 5),
        # 45 days past due, APAC with GST credit
        (3, "S3", None, "INV3", dt.date(2024, 1, 1), dt.date(2024, 2, 15), "AUD", "APAC01", "APAC", d(500), d(0), None, d(50), None, "OPEN", False, None, 5),
        # 75 days past due, other region
        (4, "S1", None, "INV4", dt.date(2024, 1, 1), dt.date(2024, 1, 16), "USD", "LATAM", "LATAM", d(300), d(0), None, None, None, "OPEN", False, None, 5),
        # 120 days past due
        (5, "S1", None, "INV5", dt.date(2023, 11, 1), dt.date(2023, 12, 2), "USD", "NA01", "NA", d(100), d(0), None, None, None, "OPEN", False, None, 5),
        # excluded: fully paid / cancelled / other batch / disputed
        (6, "S1", None, "INV6", dt.date(2024, 3, 1), dt.date(2024, 4, 1), "USD", "NA01", "NA", d(100), d(100), None, None, None, "OPEN", False, None, 5),
        (7, "S1", None, "INV7", dt.date(2024, 3, 1), dt.date(2024, 4, 1), "USD", "NA01", "NA", d(100), d(0), None, None, None, "CANC", False, None, 5),
        (8, "S1", None, "INV8", dt.date(2024, 3, 1), dt.date(2024, 4, 1), "USD", "NA01", "NA", d(100), d(0), None, None, None, "OPEN", False, None, 4),
        (9, "S9", None, "INV9", dt.date(2024, 3, 1), dt.date(2024, 4, 1), "USD", "NA01", "NA", d(100), d(0), None, None, None, "OPEN", True, "DISP", 5),
    ]
    schema = ("ApInvoiceKey BIGINT, SupplierId STRING, SupplierSiteCode STRING, InvoiceNumber STRING, InvoiceDate DATE, DueDate DATE, "
              "CurrencyCode STRING, LedgerCode STRING, RegionCode STRING, InvoiceAmount DECIMAL(19,4), PaidAmount DECIMAL(19,4), "
              "RecoverableVatAmount DECIMAL(19,4), GstInputCreditAmount DECIMAL(19,4), PaymentTermsCode STRING, InvoiceStatusCode STRING, "
              "IsOnHold BOOLEAN, HoldReasonCode STRING, LoadBatchId BIGINT")
    return spark.createDataFrame(rows, schema)


def terms(spark):
    return spark.createDataFrame([Row(PaymentTermsCode="N30", DiscountPercent=Decimal("2.0000"), RegionCode="ALL", IsActive=True)])


def test_ap_aging_buckets_and_regional_reportable(spark):
    out = rules.apAgingOpenItems(invoices(spark), terms(spark), AS_OF, 5)
    rows = {r["InvoiceNumber"]: r for r in out.collect()}
    assert set(rows) == {"INV1", "INV2", "INV3", "INV4", "INV5"}
    assert rows["INV1"]["AgingBucketCode"] == "CURRENT" and rows["INV1"]["AgingBucketSort"] == 0 and not rows["INV1"]["IsPastDue"]
    assert rows["INV2"]["AgingBucketCode"] == "B030" and rows["INV2"]["AgingBucketSort"] == 1 and rows["INV2"]["IsPastDue"]
    assert rows["INV3"]["AgingBucketCode"] == "B060" and rows["INV3"]["AgingBucketSort"] == 2
    assert rows["INV4"]["AgingBucketCode"] == "B090" and rows["INV4"]["AgingBucketSort"] == 3
    assert rows["INV5"]["AgingBucketCode"] == "B090P" and rows["INV5"]["AgingBucketSort"] == 4
    # regional reportable amount
    assert rows["INV1"]["ReportableAmount"] == Decimal("1000")
    assert rows["INV2"]["ReportableAmount"] == Decimal("1000")   # 1200 - 200 VAT
    assert rows["INV3"]["ReportableAmount"] == Decimal("450")    # 500 - 50 GST
    assert rows["INV4"]["ReportableAmount"] == Decimal("300")
    # open amount and discount at risk only when not past due
    assert rows["INV2"]["OpenAmount"] == Decimal("1000")
    assert rows["INV1"]["DiscountAtRisk"] == Decimal("20.0000")
    assert rows["INV2"]["DiscountAtRisk"] == Decimal("0.0000")


def test_ap_aging_includes_disputed_when_enabled(spark):
    out = rules.apAgingOpenItems(invoices(spark), terms(spark), AS_OF, 5, includeDisputed=True)
    assert "INV9" in {r["InvoiceNumber"] for r in out.collect()}


def test_supplier_lookup_quarantines_unmatched(spark):
    aging = rules.apAgingOpenItems(invoices(spark), terms(spark), AS_OF, 5)
    dim = spark.createDataFrame([Row(SupplierKey=11, SupplierId="S1"), Row(SupplierKey=12, SupplierId="S2")])
    matched, rejected = rules.lookupSupplierKey(aging, dim)
    assert {r["InvoiceNumber"] for r in matched.collect()} == {"INV1", "INV2", "INV4", "INV5"}
    rej = rejected.collect()
    assert [r["InvoiceNumber"] for r in rej] == ["INV3"] and rej[0]["RejectReasonCode"] == "SUPPLIER_NOT_FOUND"


def test_ap_aging_summary(spark):
    aging = rules.apAgingOpenItems(invoices(spark), terms(spark), AS_OF, 5)
    summary = {(r["LedgerCode"], r["AgingBucketCode"]): r for r in rules.apAgingSummary(aging).collect()}
    assert summary[("NA01", "CURRENT")]["OpenItemCount"] == 1
    assert summary[("NA01", "B090P")]["OpenAmount"] == Decimal("100")
    assert summary[("EU01", "B030")]["ReportableAmount"] == Decimal("1000")
