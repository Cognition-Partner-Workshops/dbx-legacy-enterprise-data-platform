import datetime as dt
from decimal import Decimal

from pyspark.sql import Row

import finance_rules as rules


def fx(spark):
    d = Decimal
    rows = [
        Row(CurrencyCode="EUR", QuoteCurrencyCode="USD", RateDate=dt.date(2024, 3, 29), RateTypeCode="CLOSING", ConversionRate=d("1.08000000"), RateSourceCode="ECB", LoadBatchId=5),
        Row(CurrencyCode="EUR", QuoteCurrencyCode="USD", RateDate=dt.date(2024, 3, 29), RateTypeCode="AVERAGE", ConversionRate=d("1.09000000"), RateSourceCode="ECB", LoadBatchId=5),
        Row(CurrencyCode="GBP", QuoteCurrencyCode="EUR", RateDate=dt.date(2024, 3, 29), RateTypeCode="CLOSING", ConversionRate=d("1.17000000"), RateSourceCode="ECB", LoadBatchId=5),
        Row(CurrencyCode="JPY", QuoteCurrencyCode="AUD", RateDate=dt.date(2024, 3, 29), RateTypeCode="CLOSING", ConversionRate=d("0.01000000"), RateSourceCode="RBA", LoadBatchId=5),
        Row(CurrencyCode="XXX", QuoteCurrencyCode="USD", RateDate=dt.date(2024, 3, 29), RateTypeCode="CLOSING", ConversionRate=d("0"), RateSourceCode="ECB", LoadBatchId=5),
        Row(CurrencyCode="EUR", QuoteCurrencyCode="USD", RateDate=dt.date(2024, 3, 29), RateTypeCode="SPOT", ConversionRate=d("1.07000000"), RateSourceCode="ECB", LoadBatchId=5),
        Row(CurrencyCode="EUR", QuoteCurrencyCode="USD", RateDate=dt.date(2024, 4, 1), RateTypeCode="CLOSING", ConversionRate=d("2.00000000"), RateSourceCode="ECB", LoadBatchId=6),
        Row(CurrencyCode="EUR", QuoteCurrencyCode="USD", RateDate=dt.date(2024, 3, 1), RateTypeCode="CLOSING", ConversionRate=d("3.00000000"), RateSourceCode="ECB", LoadBatchId=4),
    ]
    return spark.createDataFrame(rows)


def test_closing_rates_latest_on_or_before_and_inverse(spark):
    rates = rules.closingRates(fx(spark), dt.date(2024, 3, 31))
    rows = {(r["CurrencyCode"], r["RateTypeCode"]): r for r in rates.collect()}
    assert all(r["RateDate"] == dt.date(2024, 3, 29) for r in rows.values())
    assert set(rows) == {("EUR", "CLOSING"), ("EUR", "AVERAGE"), ("GBP", "CLOSING"), ("JPY", "CLOSING"), ("XXX", "CLOSING")}
    assert rows[("XXX", "CLOSING")]["InverseRate"] == Decimal("0E-8")
    assert rows[("JPY", "CLOSING")]["InverseRate"] == Decimal("100.00000000")
    assert rows[("GBP", "CLOSING")]["IsTriangulated"] is True and rows[("EUR", "CLOSING")]["IsTriangulated"] is False


def test_missing_closing_rate_currencies(spark):
    rates = rules.closingRates(fx(spark), dt.date(2024, 3, 31))
    inv = spark.createDataFrame([
        Row(CurrencyCode="EUR", InvoiceAmount=Decimal("10"), PaidAmount=Decimal("0")),
        Row(CurrencyCode="CHF", InvoiceAmount=Decimal("10"), PaidAmount=Decimal("0")),
        Row(CurrencyCode="SEK", InvoiceAmount=Decimal("10"), PaidAmount=Decimal("10")),  # closed item, ignored
    ])
    assert [r["CurrencyCode"] for r in rules.missingClosingRateCurrencies(inv, rates).collect()] == ["CHF"]


def test_revalue_open_items_uses_average_for_pl_and_ledger_quote_currency(spark):
    rates = rules.closingRates(fx(spark), dt.date(2024, 3, 31))
    payments = spark.createDataFrame([
        Row(PaymentBusinessKey="P1", LedgerCode="NA01", AccountClass="BS", TransactionCurrencyCode="EUR", EntityCurrencyCode="USD", TransactionAmount=Decimal("100"), FunctionalAmount=Decimal("105"), OpenAmount=Decimal("100")),
        Row(PaymentBusinessKey="P2", LedgerCode="NA01", AccountClass="PL", TransactionCurrencyCode="EUR", EntityCurrencyCode="USD", TransactionAmount=Decimal("100"), FunctionalAmount=Decimal("105"), OpenAmount=Decimal("100")),
        Row(PaymentBusinessKey="P3", LedgerCode="EU01", AccountClass="BS", TransactionCurrencyCode="GBP", EntityCurrencyCode="EUR", TransactionAmount=Decimal("100"), FunctionalAmount=Decimal("110"), OpenAmount=Decimal("100")),
        Row(PaymentBusinessKey="P4", LedgerCode="APAC01", AccountClass="BS", TransactionCurrencyCode="JPY", EntityCurrencyCode="AUD", TransactionAmount=Decimal("10000"), FunctionalAmount=Decimal("90"), OpenAmount=Decimal("10000")),
        Row(PaymentBusinessKey="P5", LedgerCode="NA01", AccountClass="BS", TransactionCurrencyCode="EUR", EntityCurrencyCode="USD", TransactionAmount=Decimal("100"), FunctionalAmount=Decimal("105"), OpenAmount=Decimal("0")),  # closed
        Row(PaymentBusinessKey="P6", LedgerCode="NA01", AccountClass="BS", TransactionCurrencyCode="GBP", EntityCurrencyCode="USD", TransactionAmount=Decimal("100"), FunctionalAmount=Decimal("105"), OpenAmount=Decimal("100")),  # GBP->USD missing
    ])
    out = {r["PaymentBusinessKey"]: r for r in rules.revalueOpenItems(payments, rates).collect()}
    assert set(out) == {"P1", "P2", "P3", "P4"}
    assert out["P1"]["RevaluedFunctionalAmount"] == Decimal("108.0000") and out["P1"]["UnrealisedGainLossAmount"] == Decimal("3.0000") and out["P1"]["RevaluationRateType"] == "CLOSING"
    assert out["P2"]["RevaluedFunctionalAmount"] == Decimal("109.0000") and out["P2"]["RevaluationRateType"] == "AVERAGE"
    assert out["P3"]["RevaluedFunctionalAmount"] == Decimal("117.0000")
    assert out["P4"]["RevaluedFunctionalAmount"] == Decimal("100.0000") and out["P4"]["UnrealisedGainLossAmount"] == Decimal("10.0000")
