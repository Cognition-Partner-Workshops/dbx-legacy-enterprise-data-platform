from datetime import date
from decimal import Decimal

from pyspark.sql import Row

from procurement_lib import purchase_spend as ps


def _sources(spark):
    poLines = spark.createDataFrame([
        # ONCONTRACT: contract found via join
        Row(PurchaseOrderLineId=1, PurchaseOrderNumber="PO-1", LineNumber=1, StockItemId=10, OrderedOuters=10,
            ExpectedUnitPricePerOuter=Decimal("10.50"), ReceivedOuters=10, IsOrderLineFinalized=True,
            CategoryCode="PACK", LegacyContractRef=None, LoadBatchId=7),
        # OFFCONTRACT: no contract row, legacy five-digit ref resolves to C-12345
        Row(PurchaseOrderLineId=2, PurchaseOrderNumber="PO-2", LineNumber=1, StockItemId=11, OrderedOuters=4,
            ExpectedUnitPricePerOuter=Decimal("3.00"), ReceivedOuters=0, IsOrderLineFinalized=False,
            CategoryCode="CHEM", LegacyContractRef="12345", LoadBatchId=7),
        # MAVERICK: no contract, unusable legacy ref
        Row(PurchaseOrderLineId=3, PurchaseOrderNumber="PO-3", LineNumber=1, StockItemId=12, OrderedOuters=2,
            ExpectedUnitPricePerOuter=Decimal("100.00"), ReceivedOuters=0, IsOrderLineFinalized=False,
            CategoryCode="MISC", LegacyContractRef="ABC", LoadBatchId=7),
        # cancelled order -> excluded
        Row(PurchaseOrderLineId=4, PurchaseOrderNumber="PO-4", LineNumber=1, StockItemId=12, OrderedOuters=2,
            ExpectedUnitPricePerOuter=Decimal("1.00"), ReceivedOuters=0, IsOrderLineFinalized=False,
            CategoryCode="MISC", LegacyContractRef=None, LoadBatchId=7),
        # other batch -> excluded
        Row(PurchaseOrderLineId=5, PurchaseOrderNumber="PO-1", LineNumber=2, StockItemId=12, OrderedOuters=2,
            ExpectedUnitPricePerOuter=Decimal("1.00"), ReceivedOuters=0, IsOrderLineFinalized=False,
            CategoryCode="MISC", LegacyContractRef=None, LoadBatchId=6),
        # unknown supplier -> rejected by the lookup
        Row(PurchaseOrderLineId=6, PurchaseOrderNumber="PO-9", LineNumber=1, StockItemId=12, OrderedOuters=1,
            ExpectedUnitPricePerOuter=Decimal("1.00"), ReceivedOuters=0, IsOrderLineFinalized=False,
            CategoryCode="MISC", LegacyContractRef="C-777", LoadBatchId=7),
    ])
    headers = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", SupplierId=100, OrderDate=date(2024, 3, 10), BuyerPersonId=5, RegionCode="EU", CurrencyCode="EUR", OrderStatusCode="OPEN"),
        Row(PurchaseOrderNumber="PO-2", SupplierId=100, OrderDate=date(2024, 3, 11), BuyerPersonId=5, RegionCode="EU", CurrencyCode="EUR", OrderStatusCode="OPEN"),
        Row(PurchaseOrderNumber="PO-3", SupplierId=101, OrderDate=date(2024, 3, 12), BuyerPersonId=6, RegionCode="NA", CurrencyCode="USD", OrderStatusCode="OPEN"),
        Row(PurchaseOrderNumber="PO-4", SupplierId=101, OrderDate=date(2024, 3, 12), BuyerPersonId=6, RegionCode="NA", CurrencyCode="USD", OrderStatusCode="CANC"),
        Row(PurchaseOrderNumber="PO-9", SupplierId=999, OrderDate=date(2024, 3, 12), BuyerPersonId=6, RegionCode="NA", CurrencyCode="USD", OrderStatusCode="OPEN"),
    ])
    contracts = spark.createDataFrame([
        Row(SupplierId=100, CategoryCode="PACK", ContractNumber="C-1000", ContractPricePerOuter=Decimal("10.00"),
            ContractStartDate=date(2024, 1, 1), ContractEndDate=None, IsPreferred=True),
        # expired before the order date -> not joined
        Row(SupplierId=100, CategoryCode="CHEM", ContractNumber="C-0999", ContractPricePerOuter=Decimal("2.00"),
            ContractStartDate=date(2023, 1, 1), ContractEndDate=date(2023, 12, 31), IsPreferred=False),
    ])
    dimSupplier = spark.createDataFrame([
        Row(supplier_key=1, wwi_supplier_id=100, valid_to=date(9999, 12, 31)),
        Row(supplier_key=2, wwi_supplier_id=101, valid_to=date(9999, 12, 31)),
        Row(supplier_key=3, wwi_supplier_id=101, valid_to=date(2020, 1, 1)),
    ])
    return poLines, headers, contracts, dimSupplier


def test_contract_resolution_and_spend_classification(spark):
    poLines, headers, contracts, _ = _sources(spark)
    df = ps.deriveSavings(ps.classifySpend(ps.buildPurchaseSpendLines(poLines, headers, contracts, batchId=7)))
    rows = {r.PurchaseOrderLineId: r for r in df.collect()}

    assert set(rows) == {1, 2, 3, 6}, "cancelled order and other batch must be excluded"
    assert rows[1].ResolvedContractNumber == "C-1000" and rows[1].SpendClassCode == "ONCONTRACT"
    assert rows[1].OrderedAmount == Decimal("105.00")
    assert rows[1].ContractedAmount == Decimal("100.00") and rows[1].SavingsAmount == Decimal("-5.00")
    assert rows[1].PriceVariancePercent == Decimal("5.00")

    assert rows[2].ResolvedContractNumber == "C-12345" and rows[2].SpendClassCode == "OFFCONTRACT"
    assert rows[2].ContractedAmount == rows[2].OrderedAmount == Decimal("12.00")
    assert rows[2].SavingsAmount == Decimal("0.00") and rows[2].PriceVariancePercent == Decimal("0.00")

    assert rows[3].ResolvedContractNumber is None and rows[3].SpendClassCode == "MAVERICK"
    assert rows[6].ResolvedContractNumber == "C-777" and rows[6].SpendClassCode == "OFFCONTRACT"


def test_category_scope_filters_lines(spark):
    poLines, headers, contracts, _ = _sources(spark)
    df = ps.buildPurchaseSpendLines(poLines, headers, contracts, batchId=7, categoryScope="PACK")
    assert [r.PurchaseOrderLineId for r in df.collect()] == [1]


def test_supplier_lookup_redirects_unknown_suppliers(spark):
    poLines, headers, contracts, dimSupplier = _sources(spark)
    work = ps.deriveSavings(ps.classifySpend(ps.buildPurchaseSpendLines(poLines, headers, contracts, batchId=7)))
    keys = ps.currentSupplierKeys(dimSupplier, asOf=__import__("pyspark.sql.functions").sql.functions.lit(date(2024, 6, 1)))
    matched, rejected = ps.lookupSupplierKey(work, keys)
    assert sorted(r.PurchaseOrderLineId for r in matched.collect()) == [1, 2, 3]
    assert {r.SupplierKey for r in matched.collect()} == {1, 2}
    rej = rejected.collect()
    assert [r.PurchaseOrderLineId for r in rej] == [6] and rej[0].RejectReasonCode == "UNKNOWN_SUPPLIER"

    offContract, maverickAmount = ps.measureMaverickSpend(matched)
    assert offContract == 2 and maverickAmount == Decimal("200.00")

    fact = ps.toFactPurchase(matched, batchId=7)
    assert {"date_key", "supplier_key", "purchase_order_number", "purchase_order_line_number", "spend_class_code",
            "contracted_amount", "savings_amount", "batch_id", "load_datetime"} <= set(fact.columns)
    assert fact.where("batch_id = 7").count() == 3
