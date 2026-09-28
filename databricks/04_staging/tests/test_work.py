"""Unit tests for the STG_Work_* algorithms (dedup ranking, crosswalk precedence,
inventory roll-forward, payment allocation)."""

import datetime
from decimal import Decimal

from pyspark.sql import functions as F

from stg_common import work as W


def rows(df, *cols):
    return [tuple(r) for r in df.select(*cols).collect()]


CUSTOMER_SCHEMA = (
    "CustomerCode string, CustomerNameStandardized string, NormalizedName string, TaxKey string, PostalCode string, CountryCode string, "
    "SourceModifiedDate timestamp, TradingName string, TaxRegistrationNumber string, CreditLimitAmount decimal(18,2), CreditCurrencyCode string, "
    "CustomerClassCode string, SourceCreatedDate timestamp, RegionCode string, MarketingConsentFlag string, SourceSystemCode string"
)


def test_customer_survivorship_prefers_consented_eu_row_then_completeness(spark):
    d1 = datetime.datetime(2024, 1, 1)
    d2 = datetime.datetime(2024, 2, 1)
    df = spark.createDataFrame(
        [
            ("C1", "ACME", "ACME", "TAX1", "1000", "DE", d2, "Acme", "T1", Decimal("5"), "EUR", "RTL", d1, "EU", "N", "ORA_ERP"),
            ("C2", "ACME", "ACME", "TAX1", "1000", "DE", d1, None, None, None, None, None, None, "EU", "Y", "ORA_ERP"),
            ("C3", "OTHER", "OTHER", "NONE", "2000", "US", d1, None, None, None, None, None, None, "NA", "N", "WWI_OLTP"),
        ],
        CUSTOMER_SCHEMA,
    )
    scored = W.scoreCustomerSurvivorship(df).orderBy("CustomerCode")
    got = rows(scored, "CustomerCode", "MatchRuleCode", "IsSelectedSurvivor", "GroupSize", "LosesToBusinessKey", "SourceRank")
    assert got == [
        ("C1", "EXACT_TAXNUM", False, 2, "ORA_ERP|C2", 30),
        ("C2", "EXACT_TAXNUM", True, 2, None, 30),
        ("C3", "NAME_POSTAL", True, 1, None, 20),
    ]


def test_product_crosswalk_precedence_manual_then_barcode_then_name(spark):
    products = spark.createDataFrame(
        [("E1", "Blue Widget Large", "ACME", "111"), ("E2", "Red Widget", "ACME", "222"), ("E3", "Green Widget Small", "ACME", None), ("E4", "Unknown Thing", "ZZZ", None)],
        "ProductCode string, ProductDescription string, ProductFamilyCode string, Gtin string",
    )
    stockItems = spark.createDataFrame(
        [(10, "Blue Widget Large", "ACME", "999"), (20, "Red Widget A", "ACME", "222"), (21, "Red Widget B", "ACME", "222"), (30, "Green Widget Small", "ACME", None)],
        "StockItemId int, StockItemName string, BrandName string, Barcode string",
    )
    xref = spark.createDataFrame([("Product", "ORA_ERP", "E1", "WWI_OLTP|10", "MANUAL", True, "steward")], "EntityName string, SourceSystemCode string, SourceKeyValue string, ConformedBusinessKey string, MatchMethodCode string, IsActive boolean, MaintainedByName string")
    resolved = W.resolveProductCrosswalk(W.shapeErpCrosswalkSide(products), W.shapeOltpCrosswalkSide(stockItems), manualXref=xref).orderBy("ErpProductCode")
    got = rows(resolved, "ErpProductCode", "MatchMethodCode", "OltpStockItemId", "ResolvedFlag", "IsAmbiguous", "CandidateCount")
    assert got == [
        ("E1", "MANUAL_XREF", 10, True, False, 1),
        ("E2", "BARCODE", None, False, True, 2),
        ("E3", "NAME", 30, True, False, 1),
        ("E4", "UNMATCHED", None, False, False, 0),
    ]
    assert rows(resolved.where(F.col("ErpProductCode") == "E3"), "NameTokenOverlapPercent") == [(Decimal("100.00"),)]


def test_inventory_roll_forward_carries_balance_and_flags_gaps(spark):
    d = datetime.date
    movements = spark.createDataFrame(
        [
            (1, "W1", d(2024, 3, 1), "RECEIPT", Decimal("10"), Decimal("2.0")),
            (1, "W1", d(2024, 3, 2), "SALE", Decimal("-4"), None),
            (1, "W1", d(2024, 3, 4), "SALE", Decimal("-8"), None),
        ],
        "StockItemId int, WarehouseCode string, MovementDate date, TransactionTypeCode string, SignedQuantity decimal(18,3), UnitCostAmount decimal(18,4)",
    )
    daily = W.classifyPosition(W.aggregateDailyPosition(movements))
    got = rows(W.rollForwardInventory(daily).orderBy("PositionDate"), "PositionDate", "OpeningQuantity", "ClosingQuantity", "AverageUnitCostUsd", "NegativeBalanceFlag", "RollForwardBrokenFlag", "StockPositionCode")
    assert got == [
        (d(2024, 3, 1), Decimal("0.000"), Decimal("10.000"), Decimal("2.000000"), False, False, "POSITIVE"),
        (d(2024, 3, 2), Decimal("10.000"), Decimal("6.000"), Decimal("2.000000"), False, False, "NEGATIVE"),
        (d(2024, 3, 4), Decimal("6.000"), Decimal("-2.000"), Decimal("2.000000"), True, True, "NEGATIVE"),
    ]


PAYMENT_SCHEMA = "PaymentNumber string, SupplierCode string, PaymentAmount decimal(19,4), PaymentCurrencyCode string, PaymentDate date, RegionCode string"
INVOICE_SCHEMA = "InvoiceNumber string, SupplierCode string, GrossAmount decimal(19,4), InvoiceCurrencyCode string, InvoiceDate date, DueDate date, DiscountDueDate date, OnHoldFlag string"


def test_payment_allocation_exact_discount_and_oldest_first_residual(spark):
    d = datetime.date
    payments = spark.createDataFrame(
        [("P1", "S1", Decimal("100"), "USD", d(2024, 3, 1), "NA"), ("P2", "S1", Decimal("98"), "USD", d(2024, 3, 1), "EU"), ("P3", "S2", Decimal("150"), "USD", d(2024, 3, 10), "APAC"), ("P4", "S3", Decimal("5"), "USD", d(2024, 3, 10), "NA")],
        PAYMENT_SCHEMA,
    )
    invoices = spark.createDataFrame(
        [
            ("I1", "S1", Decimal("100"), "USD", d(2024, 2, 1), d(2024, 3, 3), None, "N"),
            ("I2", "S1", Decimal("100"), "USD", d(2024, 2, 5), d(2024, 3, 7), d(2024, 3, 5), "N"),
            ("I3", "S2", Decimal("100"), "USD", d(2024, 1, 1), d(2024, 2, 1), None, "N"),
            ("I4", "S2", Decimal("100"), "USD", d(2024, 2, 1), d(2024, 3, 1), None, "N"),
            ("I5", "S2", Decimal("100"), "USD", d(2024, 1, 15), d(2024, 2, 15), None, "Y"),
        ],
        INVOICE_SCHEMA,
    )
    alloc = W.allocatePayments(payments, invoices).orderBy("PaymentNumber", "InvoiceNumber")
    got = rows(alloc, "PaymentNumber", "InvoiceNumber", "MatchRuleCode", "AppliedAmount", "DiscountTakenAmount", "ResidualAmount")
    assert ("P1", "I1", "EXACT_AMT", Decimal("100.0000"), Decimal("0.0000"), Decimal("0.0000")) in got
    assert ("P2", "I2", "EXACT_AMT", Decimal("98.0000"), Decimal("2.0000"), Decimal("0.0000")) in got
    residual = [g for g in got if g[0] == "P3"]
    assert residual == [
        ("P3", "I3", "RESIDUAL", Decimal("100.0000"), None, Decimal("0.0000")),
        ("P3", "I4", "RESIDUAL", Decimal("50.0000"), None, Decimal("50.0000")),
    ]
    assert [g for g in got if g[0] == "P4"] == [("P4", None, "UNAPPLIED", Decimal("0.0000"), None, Decimal("5.0000"))]
    outcome = W.paymentMatchOutcome(alloc)
    assert outcome is not None
