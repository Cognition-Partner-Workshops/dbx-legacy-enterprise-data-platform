from pyspark.sql import Row

from conftest import dec, ts
from procurement.extracts import transformPurchaseOrderLine, transformSupplierMaster, transformSupplierTransactions


def test_supplier_master_timestamp_watermark_and_dormant_filter(spark):
    supp = spark.createDataFrame(
        [
            Row(SUPP_ID=1, SUPP_STATUS_CD="AC", UPDATED_DT=ts("2026-01-05 10:00:00"), LAST_PO_DT=ts("2025-12-01 00:00:00"), DELETED_FLG="N", REGION_CD="NA", WITHHOLDING_FLG="Y"),
            Row(SUPP_ID=2, SUPP_STATUS_CD="AC", UPDATED_DT=ts("2025-12-31 23:59:59"), LAST_PO_DT=ts("2025-12-01 00:00:00"), DELETED_FLG="N", REGION_CD="NA", WITHHOLDING_FLG="N"),  # before watermark
            Row(SUPP_ID=3, SUPP_STATUS_CD="IN", UPDATED_DT=ts("2026-01-06 00:00:00"), LAST_PO_DT=ts("2010-01-01 00:00:00"), DELETED_FLG="N", REGION_CD="EU", WITHHOLDING_FLG="Y"),  # dormant + inactive
            Row(SUPP_ID=4, SUPP_STATUS_CD="AC", UPDATED_DT=ts("2026-01-06 00:00:00"), LAST_PO_DT=ts("2025-12-01 00:00:00"), DELETED_FLG="Y", REGION_CD="NA", WITHHOLDING_FLG="Y"),  # deleted
            Row(SUPP_ID=5, SUPP_STATUS_CD="IN", UPDATED_DT=ts("2026-01-06 00:00:00"), LAST_PO_DT=ts("2025-11-01 00:00:00"), DELETED_FLG="N", REGION_CD="EU", WITHHOLDING_FLG="Y"),  # inactive but recent PO
        ]
    )
    cert = spark.createDataFrame(
        [Row(SUPP_ID=1, CERT_TYPE_CD="ISO9001", EXPIRY_DT=ts("2020-01-01 00:00:00")), Row(SUPP_ID=1, CERT_TYPE_CD="ISO14001", EXPIRY_DT=ts("2030-01-01 00:00:00"))]
    )
    out = transformSupplierMaster(supp, cert, "2026-01-01 00:00:00", "2026-01-07 00:00:00", asOf="2026-01-07 00:00:00").orderBy("supp_id").collect()
    assert [r.supp_id for r in out] == [1, 5]
    assert out[0].quality_cert_cd == "ISO14001" and out[0].certification_expired_flag == "N"
    assert out[0].withholding_applies == "Y"
    assert out[1].certification_expired_flag == "U" and out[1].withholding_applies == "N"  # EU never withholds


def test_po_line_numeric_watermark_and_derivations(spark):
    lines = spark.createDataFrame(
        [
            Row(PO_LINE_ID=10, ORDER_QTY=dec(10), RECEIVED_QTY=dec(4), CANCELLED_QTY=None, UNIT_PRICE=dec("2.5")),
            Row(PO_LINE_ID=11, ORDER_QTY=dec(0), RECEIVED_QTY=None, CANCELLED_QTY=None, UNIT_PRICE=dec("1")),
            Row(PO_LINE_ID=12, ORDER_QTY=dec(5), RECEIVED_QTY=dec(5), CANCELLED_QTY=None, UNIT_PRICE=dec("1")),
        ],
        schema="PO_LINE_ID long, ORDER_QTY decimal(15,5), RECEIVED_QTY decimal(15,5), CANCELLED_QTY decimal(15,5), UNIT_PRICE decimal(15,5)",
    )
    out = {r.po_line_id: r for r in transformPurchaseOrderLine(lines, keyFrom=10, keyTo=11).collect()}
    assert set(out) == {11}, "rows at/below the previous watermark and above the new one are excluded"
    assert float(out[11].receipt_complete_pct) == 0.0
    full = {r.po_line_id: r for r in transformPurchaseOrderLine(lines, keyFrom=0, keyTo=99).collect()}
    assert full[10].open_qty == 6 and float(full[10].extended_amt) == 25.0 and float(full[10].receipt_complete_pct) == 0.4


def test_supplier_transactions_key_watermark_and_overlap_key(spark):
    tx = spark.createDataFrame(
        [
            Row(SupplierTransactionID=1, SupplierID=7, TransactionTypeID=5, PurchaseOrderID=None, SupplierInvoiceNumber=" inv-1 ", TransactionDate=ts("2026-01-01 00:00:00"),
                AmountExcludingTax=dec(10), TaxAmount=dec(1), TransactionAmount=dec(11), OutstandingBalance=dec(11), FinalizationDate=None, LastEditedWhen=ts("2026-01-01 00:00:00")),
            Row(SupplierTransactionID=2, SupplierID=7, TransactionTypeID=5, PurchaseOrderID=None, SupplierInvoiceNumber="INV-2", TransactionDate=ts("2026-01-02 00:00:00"),
                AmountExcludingTax=dec(10), TaxAmount=dec(1), TransactionAmount=dec(11), OutstandingBalance=dec(11), FinalizationDate=None, LastEditedWhen=ts("2026-01-02 00:00:00")),
        ],
        schema=("SupplierTransactionID int, SupplierID int, TransactionTypeID int, PurchaseOrderID int, SupplierInvoiceNumber string, TransactionDate timestamp, "
                "AmountExcludingTax decimal(18,2), TaxAmount decimal(18,2), TransactionAmount decimal(18,2), OutstandingBalance decimal(18,2), "
                "FinalizationDate timestamp, LastEditedWhen timestamp"),
    )
    suppliers = spark.createDataFrame([Row(SupplierID=7, SupplierReference="ACME")])
    types = spark.createDataFrame([Row(TransactionTypeID=5, TransactionTypeName="Supplier Invoice")])
    out = transformSupplierTransactions(tx, suppliers, types, keyFrom=1).collect()
    assert [r.supplier_transaction_id for r in out] == [2]
    assert out[0].duplicate_check_key == "ACME|INV-2" and out[0].record_kind == "APTRAN"
