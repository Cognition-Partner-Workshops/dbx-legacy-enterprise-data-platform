from decimal import Decimal

from pyspark.sql import Row

import finance_rules as rules


def test_reconciliation_set_full_outer_and_tolerance(spark):
    payments = spark.createDataFrame([
        Row(LedgerCode="EU01", AccountingPeriod="2024-03", ControlAccount="2100", FunctionalAmount=Decimal("100.0000")),
        Row(LedgerCode="EU01", AccountingPeriod="2024-03", ControlAccount="2100", FunctionalAmount=Decimal("50.5000")),
        Row(LedgerCode="NA01", AccountingPeriod="2024-03", ControlAccount="2100", FunctionalAmount=Decimal("70.0000")),
        Row(LedgerCode="NA01", AccountingPeriod="2024-02", ControlAccount="2100", FunctionalAmount=Decimal("999.0000")),  # other period
        Row(LedgerCode="APAC01", AccountingPeriod="2024-03", ControlAccount="2150", FunctionalAmount=Decimal("10.0000")),  # no GL side
    ])
    gl = spark.createDataFrame([
        Row(LedgerCode="EU01", AccountingPeriod="2024-03", AccountCode="2100", FunctionalDebitAmount=Decimal("150.0000"), FunctionalCreditAmount=Decimal("0.0000")),
        Row(LedgerCode="NA01", AccountingPeriod="2024-03", AccountCode="2100", FunctionalDebitAmount=Decimal("100.0000"), FunctionalCreditAmount=Decimal("0.0000")),
        Row(LedgerCode="NA01", AccountingPeriod="2024-03", AccountCode="4000", FunctionalDebitAmount=Decimal("0.0000"), FunctionalCreditAmount=Decimal("25.0000")),  # no subledger side
    ])
    out = rules.buildReconciliationSet(payments, gl, "2024-03", 1, 7)
    rows = {(r["LedgerCode"], r["AccountCode"]): r for r in out.collect()}
    assert set(rows) == {("EU01", "2100"), ("NA01", "2100"), ("APAC01", "2150"), ("NA01", "4000")}
    assert rows[("EU01", "2100")]["VarianceAmount"] == Decimal("0.5000") and rows[("EU01", "2100")]["VarianceStatus"] == "Within tolerance"
    assert rows[("NA01", "2100")]["VarianceAmount"] == Decimal("-30.0000") and rows[("NA01", "2100")]["VarianceStatus"] == "Variance"
    assert rows[("APAC01", "2150")]["TargetAmount"] == Decimal("0.0000") and rows[("APAC01", "2150")]["VarianceStatus"] == "Variance"
    assert rows[("NA01", "4000")]["SourceAmount"] == Decimal("0.0000") and rows[("NA01", "4000")]["VarianceAmount"] == Decimal("25.0000")
    assert all(r["BatchId"] == 7 and r["ReconciliationName"] == "Subledger to GL" for r in rows.values())

    known = spark.createDataFrame([Row(AccountCode="4000", ExplanationCode="TIMING")])
    explained = rules.applyKnownExplanations(out, known)
    byAcct = {(r["LedgerCode"], r["AccountCode"]): r for r in explained.collect()}
    assert byAcct[("NA01", "4000")]["VarianceStatus"] == "Explained" and byAcct[("NA01", "4000")]["ExplanationCode"] == "TIMING"
    assert byAcct[("NA01", "2100")]["VarianceStatus"] == "Variance"
    unexplained = rules.unexplainedVariances(explained)
    assert {(r["LedgerCode"], r["AccountCode"]) for r in unexplained.collect()} == {("NA01", "2100"), ("APAC01", "2150")}
