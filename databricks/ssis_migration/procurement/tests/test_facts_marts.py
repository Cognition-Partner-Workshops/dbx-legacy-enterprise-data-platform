from pyspark.sql import Row
from pyspark.sql import functions as F

from conftest import d, dec, ts
from procurement.facts import accrualReversals, buildLegacyPurchaseFact, landedCostAmount, onTimeFlag
from procurement.marts import matchReceipts


def test_legacy_purchase_fact_keys_watermark_and_unknown_members(spark):
    hdr = spark.createDataFrame([
        Row(PurchaseOrderID=100, SupplierID=4, OrderDate=d("2016-01-10"), LastEditedWhen=ts("2016-01-10 08:00:00")),
        Row(PurchaseOrderID=101, SupplierID=99, OrderDate=d("2016-02-10"), LastEditedWhen=ts("2016-02-10 08:00:00")),  # late-arriving supplier
        Row(PurchaseOrderID=102, SupplierID=4, OrderDate=d("2015-01-10"), LastEditedWhen=ts("2015-01-10 08:00:00")),  # before watermark
    ])
    lines = spark.createDataFrame([
        Row(PurchaseOrderLineID=1, PurchaseOrderID=100, StockItemID=7, OrderedOuters=3, ReceivedOuters=3, PackageTypeID=1, IsOrderLineFinalized=True, LastEditedWhen=ts("2016-01-10 08:00:00")),
        Row(PurchaseOrderLineID=2, PurchaseOrderID=101, StockItemID=7, OrderedOuters=2, ReceivedOuters=0, PackageTypeID=1, IsOrderLineFinalized=False, LastEditedWhen=ts("2016-02-10 08:00:00")),
        Row(PurchaseOrderLineID=3, PurchaseOrderID=102, StockItemID=7, OrderedOuters=1, ReceivedOuters=1, PackageTypeID=1, IsOrderLineFinalized=True, LastEditedWhen=ts("2015-01-10 08:00:00")),
    ])
    items = spark.createDataFrame([Row(StockItemID=7, QuantityPerOuter=12)])
    packages = spark.createDataFrame([Row(PackageTypeID=1, PackageTypeName="Carton")])
    dimSupplier = spark.createDataFrame([
        Row(supplier_key=5, wwi_supplier_id=4, valid_from=ts("2013-01-01 00:00:00"), valid_to=ts("2016-01-20 00:00:00")),
        Row(supplier_key=6, wwi_supplier_id=4, valid_from=ts("2016-01-20 00:00:00"), valid_to=ts("9999-12-31 23:59:59")),
    ])
    dimStockItem = spark.createDataFrame([
        Row(stock_item_key=70, wwi_stock_item_id=7, valid_from=ts("2013-01-01 00:00:00"), valid_to=ts("2016-02-01 00:00:00")),
        Row(stock_item_key=71, wwi_stock_item_id=7, valid_from=ts("2016-02-01 00:00:00"), valid_to=ts("9999-12-31 23:59:59")),
    ])
    out = {r.wwi_purchase_order_id: r for r in buildLegacyPurchaseFact(hdr, lines, items, packages, dimSupplier, dimStockItem, "2016-01-01 00:00:00", "2016-12-31 00:00:00").collect()}
    assert set(out) == {100, 101}, "LastEditedWhen watermark excludes 2015 rows"
    assert out[100].supplier_key == 6, "supplier resolves to the CURRENT dimension row (legacy behaviour)"
    assert out[100].stock_item_key == 70 and out[101].stock_item_key == 71, "stock item resolves to the version valid on the order date"
    assert out[101].supplier_key == 0, "late-arriving supplier -> unknown member 0"
    assert out[100].ordered_quantity == 36 and out[100].package == "Carton"


def test_regional_landed_cost_and_on_time_grace(spark):
    df = spark.createDataFrame([Row(region="NA", days=0), Row(region="EU", days=2), Row(region="EU", days=3), Row(region="APAC", days=3), Row(region="NA", days=1)])
    out = df.select("region", "days", onTimeFlag(F.col("region"), F.col("days")).alias("ot"),
                    landedCostAmount(F.col("region"), F.lit(100.0), F.lit(10.0), F.lit(5.0), F.lit(2.0)).alias("lc")).collect()
    assert [(r.ot, r.lc) for r in out] == [(True, 100.0), (True, 115.0), (False, 115.0), (True, 117.0), (False, 100.0)]


def test_accrual_reversals_only_for_open_accruals(spark):
    cols = ["supplier_transaction_business_key", "source_system_code", "transaction_type_code", "transaction_amount", "transaction_amount_reporting",
            "amount_excluding_tax", "accrual_flag", "is_reversal", "reverses_transaction_key", "wwi_supplier_transaction_id", "natural_key_hash", "lineage_key", "batch_id"]
    fact = spark.createDataFrame([
        (1, "WWI_OLTP", "INV", dec(100), dec(100), dec(90), True, False, None, 1, "h1", 1, 1),
        (2, "WWI_OLTP", "INV", dec(50), dec(50), dec(45), True, False, None, 2, "h2", 1, 1),
        (-2, "WWI_OLTP", "ACCREV", dec(-50), dec(-50), dec(-45), False, True, 2, 2, "h3", 1, 1),
        (3, "WWI_OLTP", "PAY", dec(-10), dec(-10), dec(-10), False, False, None, 3, "h4", 1, 1),
    ], cols)
    rev = accrualReversals(fact, batchId=2).collect()
    assert len(rev) == 1
    assert rev[0].supplier_transaction_business_key == -1 and rev[0].reverses_transaction_key == 1
    assert float(rev[0].transaction_amount) == -100.0 and rev[0].transaction_type_code == "ACCREV" and rev[0].is_reversal


def test_three_way_match_statuses(spark):
    receipts = spark.createDataFrame([
        Row(receipt_line_id=1, receipt_number="R1", receipt_line_number=1, purchase_order_number="P1", purchase_order_line_business_key=11, supplier_key=1, source_supplier_id=1,
            region_code="NA", receipt_date_key=d("2026-01-01"), quantity_received_base_uom=dec(100), unit_cost=dec(10), receipt_value=dec(1000), receipt_value_reporting=dec(1000)),
        Row(receipt_line_id=2, receipt_number="R2", receipt_line_number=1, purchase_order_number="P2", purchase_order_line_business_key=12, supplier_key=1, source_supplier_id=1,
            region_code="NA", receipt_date_key=d("2026-01-01"), quantity_received_base_uom=dec(100), unit_cost=dec(10), receipt_value=dec(1000), receipt_value_reporting=dec(1000)),
        Row(receipt_line_id=3, receipt_number="R3", receipt_line_number=1, purchase_order_number="P3", purchase_order_line_business_key=13, supplier_key=1, source_supplier_id=1,
            region_code="NA", receipt_date_key=d("2026-01-01"), quantity_received_base_uom=dec(100), unit_cost=dec(10), receipt_value=dec(1000), receipt_value_reporting=dec(1000)),
        Row(receipt_line_id=4, receipt_number="R4", receipt_line_number=1, purchase_order_number="P4", purchase_order_line_business_key=14, supplier_key=1, source_supplier_id=1,
            region_code="NA", receipt_date_key=d("2025-01-01"), quantity_received_base_uom=dec(100), unit_cost=dec(10), receipt_value=dec(1000), receipt_value_reporting=dec(1000)),
    ])
    invLines = spark.createDataFrame([
        Row(INVOICE_LINE_ID=1, INVOICE_ID=1, RECEIPT_LINE_ID=1, PO_LINE_ID=None, QUANTITY=dec(101), UNIT_PRICE=dec(10), LINE_AMOUNT=dec(1010)),      # within 2% qty -> MATCHED
        Row(INVOICE_LINE_ID=2, INVOICE_ID=1, RECEIPT_LINE_ID=None, PO_LINE_ID=12, QUANTITY=dec(110), UNIT_PRICE=dec(10), LINE_AMOUNT=dec(1100)),     # PO-line fallback, qty +10% -> QTY_EXCEPTION
        Row(INVOICE_LINE_ID=3, INVOICE_ID=1, RECEIPT_LINE_ID=3, PO_LINE_ID=None, QUANTITY=dec(100), UNIT_PRICE=dec("10.5"), LINE_AMOUNT=dec(1050)),  # price +5% -> PRICE_EXCEPTION
    ])
    invHdr = spark.createDataFrame([Row(INVOICE_ID=1, INVOICE_NBR="INV-1", INVOICE_DT=ts("2026-01-05 00:00:00"))])
    out = {r.receipt_line_id: r for r in matchReceipts(receipts, invLines, invHdr, asOf="2026-03-01", batchId=1).collect()}
    assert out[1].match_status_code == "MATCHED" and out[1].supplier_invoice_number == "INV-1"
    assert out[2].match_status_code == "QTY_EXCEPTION"
    assert out[3].match_status_code == "PRICE_EXCEPTION"
    assert out[4].match_status_code == "GRNI" and out[4].grni_accrual_flag and float(out[4].grni_accrual_amount_reporting) == 1000.0
