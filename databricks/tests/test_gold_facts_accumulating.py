from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from sales_lakehouse.gold import (
    fact_credit_note,
    fact_order,
    fact_order_fulfilment,
    fact_payment,
    fact_return,
    fact_sale,
    facts,
)
from tests.gold_facts_fixtures import (
    allocation,
    creditNote,
    defaultAllocations,
    isolatedConfig,
    payment,
    rejected,
    seedAll,
    seedCreditNotes,
    seedPayments,
    seedReturns,
)


def test_return_milestones_fill_in_and_never_erase(spark, cfg):
    iso = isolatedConfig(spark, cfg, "ret")
    seedAll(spark, iso)
    fact_sale.run(spark, iso)
    fact_return.run(spark, iso)
    fqn = iso.fqn("gold", "fact_return")
    r = spark.table(fqn).collect()[0]
    assert r["quantity_returned"] == Decimal("-2.0000") and r["net_credit_amount"] == Decimal("-21.6000")
    assert r["cost_of_returned_goods"] == Decimal("-12.0000")  # original sale cost 6/unit (net 100 - profit 40)
    assert r["received_date_key"] is None and r["credit_issued_date_key"] is None
    assert r["return_to_credit_lag_days"] is None and r["days_since_invoice"] == 10
    assert r["within_statutory_window_flag"] is True and r["customer_key"] == 10

    seedReturns(spark, iso, processedDate=date(2024, 3, 28))
    seedCreditNotes(
        spark,
        iso,
        extra=[
            creditNote("CN1", "C1", "INV1", "RMA1", "20", "1.6", date(2024, 4, 1), "Approver", "NA", "USD"),
        ],
    )
    fact_return.run(spark, iso)
    r = spark.table(fqn).collect()[0]
    assert r["received_date_key"] == date(2024, 3, 28) and r["credit_issued_date_key"] == date(2024, 4, 1)
    assert r["return_to_receipt_lag_days"] == 3 and r["return_to_credit_lag_days"] == 7
    assert r["receipt_to_credit_lag_days"] == 4 and r["credit_note_number"] == "CN1"

    # Source regresses (processed date lost) -> the milestone already filled is kept.
    seedReturns(spark, iso, processedDate=None)
    fact_return.run(spark, iso)
    r = spark.table(fqn).collect()[0]
    assert r["received_date_key"] == date(2024, 3, 28) and r["return_to_receipt_lag_days"] == 3
    assert spark.table(fqn).count() == 1


def test_credit_note_excludes_return_backed_rejects_unapproved_and_accumulates(spark, cfg):
    iso = isolatedConfig(spark, cfg, "cn")
    seedAll(spark, iso)
    seedCreditNotes(
        spark,
        iso,
        extra=[
            creditNote("CN1", "C1", "INV1", "RMA1", "20", "1.6", date(2024, 4, 1), "Approver", "NA", "USD"),
        ],
    )
    fact_credit_note.run(spark, iso)
    fqn = iso.fqn("gold", "fact_credit_note")
    fact = spark.table(fqn)
    assert [r[0] for r in fact.select("credit_note_business_key").collect()] == ["CN2"]
    assert rejected(spark, iso, "FACT_CREDIT_NOTE_UNAPPROVED").count() == 1
    cn2 = fact.collect()[0]
    assert cn2["credit_including_tax"] == Decimal("-24.0000") and cn2["credit_including_tax_reporting"] == Decimal(
        "-26.4000"
    )
    assert cn2["original_invoice_date_key"] == date(2024, 7, 10) and cn2["invoice_to_credit_lag_days"] == 10
    assert cn2["applied_date_key"] is None and cn2["customer_key"] == 11 and cn2["fiscal_period_key"] == 202407

    seedPayments(
        spark, iso, defaultAllocations() + [allocation("A9", "PAY3", None, "24", date(2024, 7, 25), creditNote="CN2")]
    )
    fact_credit_note.run(spark, iso)
    cn2 = spark.table(fqn).collect()[0]
    assert cn2["applied_date_key"] == date(2024, 7, 25) and cn2["credit_to_applied_lag_days"] == 5
    assert spark.table(fqn).count() == 1


def test_order_fulfilment_pipeline_status_and_lag_recompute(spark, cfg):
    iso = isolatedConfig(spark, cfg, "ful")
    seedAll(spark, iso)
    fact_sale.run(spark, iso)
    fact_order.run(spark, iso)
    fact_payment.run(spark, iso)
    fact_order_fulfilment.run(spark, iso)
    fqn = iso.fqn("gold", "fact_order_fulfilment")

    def row(key):
        return spark.table(fqn).filter(F.col("order_business_key") == key).collect()[0]

    ord1 = row("ORD1")
    assert ord1["pick_date_key"] is None  # ORD1-2 still open
    # cash applied = receipt (payment) date of the latest allocation, PAY1 on 2024-03-20
    assert ord1["invoice_date_key"] == date(2024, 3, 15) and ord1["cash_applied_date_key"] == date(2024, 3, 20)
    assert ord1["pipeline_status_code"] == "CASH" and ord1["cycle_complete_flag"] is True
    assert ord1["invoice_to_cash_days"] == 5 and ord1["order_to_cash_cycle_days"] == 10
    ord2 = row("ORD2")
    assert ord2["pick_date_key"] == date(2024, 7, 3) and ord2["pipeline_status_code"] == "INVOICED"
    assert ord2["cash_applied_date_key"] is None and ord2["invoice_to_cash_days"] is None
    assert ord2["order_to_pick_days"] == 2 and ord2["open_milestone_count"] == 3

    seedPayments(
        spark,
        iso,
        defaultAllocations() + [allocation("A8", "PAY4", "INV2", "110", date(2024, 7, 30))],
        extraPayments=[payment("PAY4", "C1", "110", date(2024, 7, 30), "EU", "APPLIED")],
    )
    fact_payment.run(spark, iso)
    fact_order_fulfilment.run(spark, iso)
    ord2 = row("ORD2")
    assert ord2["pipeline_status_code"] == "CASH" and ord2["cash_applied_date_key"] == date(2024, 7, 30)
    assert ord2["invoice_to_cash_days"] == 20 and ord2["order_to_cash_cycle_days"] == 29
    assert ord2["pick_date_key"] == date(2024, 7, 3)
    assert spark.table(fqn).count() == 2


def test_facts_run_end_to_end_twice(spark, cfg):
    iso = isolatedConfig(spark, cfg, "all")
    seedAll(spark, iso)
    facts.run(spark, iso)
    counts1 = {t: spark.table(iso.fqn("gold", t)).count() for t in facts.GOLD_TABLES}
    facts.run(spark, iso)
    counts2 = {t: spark.table(iso.fqn("gold", t)).count() for t in facts.GOLD_TABLES}
    assert counts1 == counts2
    assert all(c > 0 for c in counts1.values()), counts1
    for t in facts.GOLD_TABLES:
        cols = set(spark.table(iso.fqn("gold", t)).columns)
        assert {"batch_id", "loaded_at_utc"} <= cols, t
