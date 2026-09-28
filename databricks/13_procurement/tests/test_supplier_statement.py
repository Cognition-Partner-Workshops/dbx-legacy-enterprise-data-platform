from datetime import date
from decimal import Decimal

from pyspark.sql import Row

from procurement_lib import supplier_statement as st


def _sources(spark):
    fact = spark.createDataFrame([
        Row(supplier_transaction_key=1, supplier_key=1, transaction_date=date(2024, 3, 3), transaction_type_code="INVOICE", transaction_reference="INV-0001", transaction_amount=Decimal("100.00"), currency_code="EUR", tax_amount=Decimal("20.00")),
        Row(supplier_transaction_key=2, supplier_key=1, transaction_date=date(2024, 3, 1), transaction_type_code="PAYMENT", transaction_reference="PAY-0001", transaction_amount=Decimal("-40.00"), currency_code="EUR", tax_amount=Decimal("0.00")),
        Row(supplier_transaction_key=3, supplier_key=1, transaction_date=date(2024, 3, 3), transaction_type_code="CREDITNOTE", transaction_reference="CN-0001-WITH-A-VERY-LONG-REFERENCE-VALUE", transaction_amount=Decimal("-10.00"), currency_code="EUR", tax_amount=Decimal("-2.00")),
        Row(supplier_transaction_key=4, supplier_key=2, transaction_date=date(2024, 3, 9), transaction_type_code="INVOICE", transaction_reference="INV-0002", transaction_amount=Decimal("55.00"), currency_code="USD", tax_amount=Decimal("5.00")),
        Row(supplier_transaction_key=5, supplier_key=3, transaction_date=date(2024, 3, 9), transaction_type_code="INVOICE", transaction_reference="INV-0003", transaction_amount=Decimal("1.00"), currency_code="USD", tax_amount=Decimal("0.00")),
        Row(supplier_transaction_key=6, supplier_key=1, transaction_date=date(2024, 4, 1), transaction_type_code="INVOICE", transaction_reference="INV-0009", transaction_amount=Decimal("999.00"), currency_code="EUR", tax_amount=Decimal("0.00")),
    ])
    dim = spark.createDataFrame([
        Row(supplier_key=1, wwi_supplier_id=42, supplier="Acme EU", region_code="EU", is_self_billing=False),
        Row(supplier_key=2, wwi_supplier_id=1234567, supplier="Widgets NA", region_code="NA", is_self_billing=False),
        Row(supplier_key=3, wwi_supplier_id=7, supplier="SelfBill", region_code="NA", is_self_billing=True),
    ])
    return fact, dim


def test_statement_lines_filter_period_vat_and_self_billing(spark):
    fact, dim = _sources(spark)
    lines = st.buildStatementLines(fact, dim, "2024-03", excludeSelfBilling=True)
    rows = {r.SupplierStatementLineId: r for r in lines.collect()}
    assert set(rows) == {1, 2, 3, 4}, "April row and self-billing supplier excluded"
    assert rows[1].VatAmount == Decimal("20.00") and rows[4].VatAmount is None, "VAT only for EU"

    withSelfBilling = st.buildStatementLines(fact, dim, "2024-03", excludeSelfBilling=False)
    assert withSelfBilling.count() == 5
    assert st.countStatements(lines) == 2


def test_running_balance_and_fixed_layout(spark):
    fact, dim = _sources(spark)
    lines = st.formatStatementRows(st.computeRunningBalances(st.buildStatementLines(fact, dim, "2024-03", True)))
    out = st.orderedStatementRows(lines).collect()
    assert out[0].SupplierId == 42
    # ordered by date then line id: PAYMENT (-40), INVOICE (+100 -> 60), CREDITNOTE (-10 -> 50)
    assert [r.TransactionTypeCode for r in out[:3]] == ["PAYMENT", "INVOICE", "CREDITNOTE"]
    assert [r.RunningBalance for r in out[:3]] == [Decimal("-40.00"), Decimal("60.00"), Decimal("50.00")]
    assert out[3].SupplierId == 1234567 and out[3].RunningBalance == Decimal("55.00")

    assert out[0].StatementLineText == "0000000042" + "PAYMENT" + "PAY-0001"
    assert out[2].StatementLineText == "0000000042" + "CREDITNOTE" + "CN-0001-WITH-A-VERY-LONG-REFER"
    assert out[3].StatementLineText.startswith("0001234567INVOICE")
    assert out[0].IncludesVatBlock is True and out[3].IncludesVatBlock is False
    assert list(out[0].asDict().keys()) == st.STATEMENT_FILE_COLUMNS


def test_statement_file_name():
    assert st.statementFileName("2024-03") == "supplier_statement_2024-03.csv"
