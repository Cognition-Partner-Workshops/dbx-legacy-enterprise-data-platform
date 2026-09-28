import datetime as dt
from decimal import Decimal

from pyspark.sql import Row

import finance_rules as rules

LINE_COLS = ["ApInvoiceLineId", "ApInvoiceBusinessKey", "ApInvoiceKey", "SupplierId", "SupplierTaxRegistrationNumber", "JurisdictionCode",
             "RegionCode", "LedgerCode", "InvoiceDate", "LineAmount", "TaxCode", "ServiceCategoryCode", "LineTypeCode", "LoadBatchId"]


def lines(spark):
    d = Decimal
    day = dt.date(2024, 3, 10)
    rows = [
        (1, "I1", 1, "S1", None, "US-CA", "NA", "NA01", day, d("1000"), "T", "CONS", "SERVICE", 5),     # NA consulting -> 24%
        (2, "I1", 1, "S1", None, "US-CA", "NA", "NA01", day, d("1000"), "T", "GOODS", "GOODS", 5),     # NA goods -> no withholding
        (3, "I2", 2, "S2", "DE123", "DE", "EU", "EU01", day, d("1000"), "T", "CONS", "SERVICE", 5),    # EU registered -> treaty 5%
        (4, "I3", 3, "S3", "", "DE", "EU", "EU01", day, d("1000"), "T", "CONS", "SERVICE", 5),         # EU unregistered -> standard 15%
        (5, "I4", 4, "S4", None, "AU", "APAC", "APAC01", day, d("500"), "T", "CONS", "SERVICE", 5),    # APAC below threshold
        (6, "I4", 4, "S4", None, "AU", "APAC", "APAC01", day, d("5000"), "T", "CONS", "SERVICE", 5),   # APAC above threshold -> 10%
        (7, "I5", 5, "S5", None, "ZZ", "EU", "EU01", day, d("1000"), "T", "CONS", "SERVICE", 5),       # unmapped jurisdiction
        (8, "I1", 1, "S1", None, "US-CA", "NA", "NA01", day, d("1000"), "T", "CONS", "FREIGHT", 5),    # freight excluded
        (9, "I1", 1, "S1", None, "US-CA", "NA", "NA01", day, d("1000"), "T", "CONS", "SERVICE", 4),    # other batch
    ]
    schema = ("ApInvoiceLineId BIGINT, ApInvoiceBusinessKey STRING, ApInvoiceKey BIGINT, SupplierId STRING, SupplierTaxRegistrationNumber STRING, "
              "JurisdictionCode STRING, RegionCode STRING, LedgerCode STRING, InvoiceDate DATE, LineAmount DECIMAL(19,4), TaxCode STRING, "
              "ServiceCategoryCode STRING, LineTypeCode STRING, LoadBatchId BIGINT")
    return spark.createDataFrame(rows, schema)


def rates(spark):
    d = Decimal
    rows = [
        ("US-CA", "CONS", d("24"), None, d("0"), dt.date(2020, 1, 1), None),
        ("US-CA", "GOODS", d("24"), None, d("0"), dt.date(2020, 1, 1), None),
        ("DE", "CONS", d("15"), d("5"), d("0"), dt.date(2020, 1, 1), None),
        ("AU", "CONS", d("10"), None, d("1000"), dt.date(2020, 1, 1), None),
        ("AU", "CONS", d("99"), None, d("0"), dt.date(2010, 1, 1), dt.date(2019, 12, 31)),  # expired
    ]
    schema = ("JurisdictionCode STRING, ServiceCategoryCode STRING, WithholdingRatePercent DECIMAL(9,4), TreatyRatePercent DECIMAL(9,4), "
              "WithholdingThresholdAmount DECIMAL(19,4), EffectiveFrom DATE, EffectiveTo DATE")
    return spark.createDataFrame(rows, schema)


def test_withholding_regional_rules(spark):
    out = rules.withholdingLines(lines(spark), rates(spark), 5)
    rows = {r["ApInvoiceLineId"]: r for r in out.collect()}
    assert set(rows) == {1, 2, 3, 4, 5, 6, 7}
    assert rows[1]["WithholdingAmount"] == Decimal("240.0000") and rows[1]["NetPayableAmount"] == Decimal("760.0000") and rows[1]["IsWithheld"]
    assert rows[2]["WithholdingAmount"] == Decimal("0.0000") and not rows[2]["IsWithheld"]
    assert rows[3]["WithholdingAmount"] == Decimal("50.0000") and rows[3]["WithholdingCertificateRequired"]
    assert rows[4]["WithholdingAmount"] == Decimal("150.0000") and rows[4]["WithholdingCertificateRequired"]
    assert rows[5]["WithholdingAmount"] == Decimal("0.0000")
    assert rows[6]["WithholdingAmount"] == Decimal("500.0000") and not rows[6]["WithholdingCertificateRequired"]
    assert rows[7]["WithholdingAmount"] == Decimal("0.0000") and rows[7]["WithholdingRatePercent"] == Decimal("0.0000")


def test_split_unmapped_and_certificate_queue(spark):
    out = rules.withholdingLines(lines(spark), rates(spark), 5)
    mapped, unmapped = rules.splitUnmappedJurisdictions(out)
    # an unmapped jurisdiction only quarantines when it would withhold; the legacy
    # expression yields 0 for both so ZZ stays on the mapped path with 0 withholding
    assert unmapped.count() == 0 and mapped.count() == 7
    forced = out.withColumn("WithholdingAmount", out["WithholdingAmount"] + 1)
    _, unmapped2 = rules.splitUnmappedJurisdictions(forced)
    assert [r["ApInvoiceLineId"] for r in unmapped2.collect()] == [7] and unmapped2.first()["RejectReasonCode"] == "JURISDICTION_UNMAPPED"

    queue = {(r["SupplierId"], r["JurisdictionCode"]): r for r in rules.certificateQueue(mapped, 5).collect()}
    assert set(queue) == {("S2", "DE"), ("S3", "DE")}
    assert queue[("S3", "DE")]["WithholdingAmount"] == Decimal("150.0000") and queue[("S3", "DE")]["BatchId"] == 5


def test_jurisdiction_scope_filter(spark):
    out = rules.withholdingLines(lines(spark), rates(spark), 5, jurisdictionScope="DE")
    assert {r["ApInvoiceLineId"] for r in out.collect()} == {3, 4}
