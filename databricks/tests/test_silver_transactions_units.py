"""Unit tests for the focused silver modules: business keys, fulfilment flags,
order-line dedup ranking and the payment matcher passes."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from sales_lakehouse.silver.business_keys import lineBusinessKey, sourceSystemKey
from sales_lakehouse.silver.dedup import EXACT_COPY_RANK_COL, flagRekeyDuplicates, rankExactCopies
from sales_lakehouse.silver.fulfilment_flags import parseFulfilmentFlags
from sales_lakehouse.silver.payment_matching import matchPaymentsToInvoices, summarisePayments


# --------------------------------------------------------------------------- #
# business keys (stg.ufn_SourceSystemKey)
# --------------------------------------------------------------------------- #
def test_source_system_key_matches_legacy_function(spark):
    df = spark.createDataFrame(
        [
            ("wwi_oltp", " 42 ", "7"),
            ("WWI_WEB", "42", "7"),
            ("ORA_ERP_EU", "42", "1"),
            ("ORA_ERP_NA", "AB|42", "1"),
            ("WWI_OLTP", "", "1"),
            (None, "42", "1"),
            ("SQLSERVER_WWI_OLTP", "42", "7"),
            ("ORACLE_WWIGERP", "42", "1"),
        ],
        "sys string, key string, line string",
    )
    out = df.select(
        sourceSystemKey(F.col("sys"), F.col("key")).alias("k"),
        lineBusinessKey(sourceSystemKey(F.col("sys"), F.col("key")), F.col("line")).alias("lk"),
    ).collect()
    assert [r.k for r in out] == ["WWI_OLTP|42", "WWI_OLTP|42", "ORA_ERP|0000000042", "ORA_ERP|AB/42", None, None, "WWI_OLTP|42", "ORA_ERP|0000000042"]
    assert out[0].lk == "WWI_OLTP|42|7"
    assert out[4].lk is None


# --------------------------------------------------------------------------- #
# fulfilment flags
# --------------------------------------------------------------------------- #
def test_fulfilment_flags_good_empty_and_malformed(spark):
    df = spark.createDataFrame(
        [(1, "H|B"), (2, ""), (3, None), (4, "P||B"), (5, "S|X|"), (6, " h | zz ")],
        "id int, FulfilmentFlags string",
    )
    rows = {r.id: r for r in parseFulfilmentFlags(df).orderBy("id").collect()}

    good = rows[1]
    assert (good.is_hold_flagged, good.is_backorder_flagged, good.is_split_flagged, good.is_export_flagged) == (True, True, False, False)
    assert good.fulfilment_flags_unknown == [] and good.fulfilment_flags_malformed is False
    assert good.fulfilment_flags_raw == "H|B"

    for emptyId in (2, 3):
        empty = rows[emptyId]
        assert not any([empty.is_hold_flagged, empty.is_backorder_flagged, empty.is_split_flagged, empty.is_export_flagged])
        assert empty.fulfilment_flags_unknown == [] and empty.fulfilment_flags_malformed is False

    malformed = rows[4]  # P||B parses leniently: B set, P unknown, WARN-worthy
    assert malformed.is_backorder_flagged is True and malformed.fulfilment_flags_unknown == ["P"]
    assert malformed.fulfilment_flags_malformed is True

    trailing = rows[5]
    assert trailing.is_split_flagged and trailing.is_export_flagged and trailing.fulfilment_flags_malformed is True

    wide = rows[6]  # whitespace and case are tolerated, multi-letter token is unknown + malformed
    assert wide.is_hold_flagged is True and wide.fulfilment_flags_unknown == ["ZZ"] and wide.fulfilment_flags_malformed is True


# --------------------------------------------------------------------------- #
# dedup ranking (stg.usp_DeduplicateOrderLine)
# --------------------------------------------------------------------------- #
def test_exact_copy_rank_keeps_latest_copy(spark):
    df = spark.createDataFrame(
        [
            ("K1", datetime(2026, 1, 1), datetime(2026, 1, 1, 1)),
            ("K1", datetime(2026, 1, 2), datetime(2026, 1, 1, 1)),
            ("K1", datetime(2026, 1, 2), datetime(2026, 1, 1, 2)),
            ("K2", None, datetime(2026, 1, 1, 1)),
        ],
        "order_line_business_key string, source_modified_at_utc timestamp, _load_ts timestamp",
    )
    ranked = rankExactCopies(df).filter(F.col(EXACT_COPY_RANK_COL) == 1).collect()
    winners = {r.order_line_business_key: r for r in ranked}
    assert len(winners) == 2
    assert winners["K1"]._load_ts == datetime(2026, 1, 1, 2)
    assert winners["K2"].source_modified_at_utc is None


def test_rekey_duplicates_highest_line_number_wins(spark):
    df = spark.createDataFrame(
        [
            ("O1", "S1", Decimal("2"), Decimal("10"), 1, datetime(2026, 1, 1), datetime(2026, 1, 1)),
            ("O1", "S1", Decimal("2"), Decimal("10"), 5, datetime(2026, 1, 1), datetime(2026, 1, 1)),
            ("O1", "S1", Decimal("2"), Decimal("10"), 3, datetime(2026, 1, 9), datetime(2026, 1, 1)),
            ("O1", "S1", Decimal("3"), Decimal("10"), 2, datetime(2026, 1, 1), datetime(2026, 1, 1)),
            ("O2", "S1", Decimal("2"), Decimal("10"), 1, datetime(2026, 1, 1), datetime(2026, 1, 1)),
        ],
        "order_business_key string, stock_item_business_key string, ordered_quantity decimal(18,4), "
        "unit_price_amount_local decimal(19,4), line_number int, source_modified_at_utc timestamp, _load_ts timestamp",
    )
    out = flagRekeyDuplicates(df).collect()
    byLine = {(r.order_business_key, r.line_number): r for r in out}
    assert byLine[("O1", 5)].is_duplicate_loser is False  # highest LineNumber wins, not latest edit
    assert byLine[("O1", 1)].is_duplicate_loser is True
    assert byLine[("O1", 3)].is_duplicate_loser is True
    assert byLine[("O1", 1)].duplicate_group_id == byLine[("O1", 5)].duplicate_group_id is not None
    assert byLine[("O1", 2)].duplicate_group_id is None and byLine[("O1", 2)].is_duplicate_loser is False
    assert byLine[("O2", 1)].duplicate_group_id is None


# --------------------------------------------------------------------------- #
# payment matching (work.usp_MatchPaymentsToInvoices)
# --------------------------------------------------------------------------- #
def _payments(spark, rows):
    return spark.createDataFrame(
        [(k, c, date(2026, 1, 20), Decimal(str(a)), r, v) for k, c, a, r, v in rows],
        "payment_business_key string, customer_business_key string, payment_date date, "
        "payment_amount_local decimal(19,4), region_code string, is_void boolean",
    )


def _invoices(spark, rows):
    return spark.createDataFrame(
        [(k, c, d, Decimal(str(a)), h) for k, c, d, a, h in rows],
        "sale_business_key string, customer_business_key string, invoice_date date, open_amount_local decimal(19,4), is_on_hold boolean",
    )


def _explicit(spark, rows):
    return spark.createDataFrame(
        [(p, i, Decimal(str(a))) for p, i, a in rows],
        "payment_business_key string, sale_business_key string, allocated_amount_local decimal(19,4)",
    )


def test_pass1_explicit_reference_wins_and_is_final(spark):
    payments = _payments(spark, [("P1", "C1", 80, "NA", False)])
    invoices = _invoices(spark, [("I1", "C1", date(2026, 1, 1), 100, False), ("I2", "C1", date(2026, 1, 2), 80, False)])
    explicit = _explicit(spark, [("P1", "I1", 80)])
    out = matchPaymentsToInvoices(payments, invoices, explicit).collect()
    assert len(out) == 1
    row = out[0]
    assert row.allocation_method_code == "REMIT_REF" and row.match_pass_number == 1
    assert row.sale_business_key == "I1" and row.allocated_amount_local == Decimal("80.0000")
    assert row.residual_amount_local == Decimal("20.0000") and row.within_tolerance_flag is False
    assert row.match_confidence == Decimal("100.00") and row.is_final_allocation is True
    summary = summarisePayments(payments, matchPaymentsToInvoices(payments, invoices, explicit)).collect()[0]
    assert summary.match_status_code == "MATCHED_REF"
    assert summary.is_underpayment is True and summary.is_overpayment is False


def test_pass1_reference_to_another_customers_invoice_is_ignored(spark):
    payments = _payments(spark, [("P1", "C1", 100, "NA", False)])
    invoices = _invoices(spark, [("I9", "C2", date(2026, 1, 1), 100, False), ("I1", "C1", date(2026, 1, 1), 100, False)])
    out = matchPaymentsToInvoices(payments, invoices, _explicit(spark, [("P1", "I9", 100)])).collect()
    assert [(r.allocation_method_code, r.sale_business_key) for r in out] == [("EXACT_AMT", "I1")]


def test_pass2_exact_amount_single_candidate(spark):
    payments = _payments(spark, [("P1", "C1", 115, "NA", False)])
    invoices = _invoices(spark, [("I1", "C1", date(2026, 1, 1), 200, False), ("I2", "C1", date(2026, 1, 2), 115, False)])
    out = matchPaymentsToInvoices(payments, invoices, _explicit(spark, [])).collect()
    assert len(out) == 1
    assert out[0].allocation_method_code == "EXACT_AMT" and out[0].sale_business_key == "I2"
    assert out[0].match_confidence == Decimal("90.00") and out[0].residual_amount_local == Decimal("0.0000")
    assert summarisePayments(payments, matchPaymentsToInvoices(payments, invoices, _explicit(spark, []))).collect()[0].match_status_code == "MATCHED_AMT"


def test_pass2_tie_falls_through_to_fifo(spark):
    payments = _payments(spark, [("P1", "C1", 100, "NA", False)])
    invoices = _invoices(spark, [("I1", "C1", date(2026, 1, 5), 100, False), ("I2", "C1", date(2026, 1, 1), 100, False)])
    out = matchPaymentsToInvoices(payments, invoices, _explicit(spark, [])).collect()
    assert [(r.allocation_method_code, r.sale_business_key) for r in out] == [("RESIDUAL", "I2")]  # oldest first


def test_pass3_fifo_residual_across_invoices_and_payments(spark):
    payments = _payments(spark, [("P1", "C1", 150, "NA", False), ("P2", "C1", 70, "NA", False)])
    invoices = _invoices(
        spark,
        [
            ("I-OLD", "C1", date(2026, 1, 1), 100, False),
            ("I-HELD", "C1", date(2026, 1, 2), 50, True),
            ("I-NEW", "C1", date(2026, 1, 3), 100, False),
        ],
    )
    out = matchPaymentsToInvoices(payments, invoices, _explicit(spark, [])).collect()
    alloc = {(r.payment_business_key, r.sale_business_key): r for r in out}
    assert set(alloc) == {("P1", "I-OLD"), ("P1", "I-NEW"), ("P2", "I-NEW"), ("P2", None)}
    assert alloc[("P2", None)].allocation_method_code == "UNAPPLIED" and alloc[("P2", None)].residual_amount_local == Decimal("20.0000")
    assert alloc[("P1", "I-OLD")].allocated_amount_local == Decimal("100.0000")
    assert alloc[("P1", "I-OLD")].within_tolerance_flag is True and alloc[("P1", "I-OLD")].is_final_allocation is False
    assert alloc[("P1", "I-NEW")].allocated_amount_local == Decimal("50.0000")
    assert alloc[("P1", "I-NEW")].residual_amount_local == Decimal("50.0000") and alloc[("P1", "I-NEW")].is_final_allocation is True
    assert alloc[("P2", "I-NEW")].allocated_amount_local == Decimal("50.0000")  # invoice depleted by P1
    assert all(r.allocation_method_code == "RESIDUAL" and r.match_confidence == Decimal("65.00") for r in out if r.sale_business_key)
    summary = {r.payment_business_key: r for r in summarisePayments(payments, matchPaymentsToInvoices(payments, invoices, _explicit(spark, []))).collect()}
    assert summary["P1"].match_status_code == "MATCHED_RESIDUAL" and summary["P1"].applied_invoice_count == 2
    assert summary["P2"].match_status_code == "PARTIAL" and summary["P2"].unallocated_amount_local == Decimal("20.0000")
    assert summary["P2"].is_overpayment is True


def test_unapplied_remainder_and_regional_tolerance(spark):
    payments = _payments(
        spark,
        [
            ("P-NONE", "C9", 40, "NA", False),  # no invoices at all
            ("P-EU", "C1", 100.01, "EU", False),  # 0.01 over -> within EU tolerance, no UNAPPLIED row
            ("P-NA", "C2", 100.05, "NA", False),  # 0.05 over -> above NA 0.02 tolerance
            ("P-VOID", "C1", 100, "NA", True),
        ],
    )
    invoices = _invoices(spark, [("I1", "C1", date(2026, 1, 1), 100, False), ("I2", "C2", date(2026, 1, 1), 100, False)])
    out = matchPaymentsToInvoices(payments, invoices, _explicit(spark, [])).collect()
    byPayment: dict[str, list] = {}
    for r in out:
        byPayment.setdefault(r.payment_business_key, []).append(r)

    none = byPayment["P-NONE"]
    assert len(none) == 1 and none[0].allocation_method_code == "UNAPPLIED"
    assert none[0].sale_business_key is None and none[0].residual_amount_local == Decimal("40.0000")
    assert none[0].unmatched_reason_code == "NO_OPEN_INVOICE" and none[0].allocated_amount_local == Decimal("0.0000")

    assert [r.allocation_method_code for r in byPayment["P-EU"]] == ["RESIDUAL"]
    assert byPayment["P-EU"][0].is_final_allocation is True

    na = sorted(byPayment["P-NA"], key=lambda r: r.allocation_method_code)
    assert [r.allocation_method_code for r in na] == ["RESIDUAL", "UNAPPLIED"]
    assert na[1].residual_amount_local == Decimal("0.0500")

    assert "P-VOID" not in byPayment
    summary = {r.payment_business_key: r for r in summarisePayments(payments, matchPaymentsToInvoices(payments, invoices, _explicit(spark, []))).collect()}
    assert summary["P-VOID"].match_status_code == "UNMATCHED"
    assert summary["P-NONE"].match_status_code == "PARTIAL" and summary["P-NONE"].unallocated_amount_local == Decimal("40.0000")
    assert summary["P-NA"].match_status_code == "PARTIAL" and summary["P-NA"].is_overpayment is True
