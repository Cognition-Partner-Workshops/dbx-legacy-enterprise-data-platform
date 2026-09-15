"""Fixture suite for the Sales Facts slice.

Each test pins one rule of Integration.usp_LoadFactSale / usp_DeduplicateFactSale /
usp_ApplyFactCorrections / stg.usp_AppendIncremental_SaleLine against the fixture
extract in ../fixtures (batches 1..3 of a three-day parallel run)."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from pyspark.sql import functions as F

from sales_facts import transforms as t

from .conftest import one, rows, run_slice

D = Decimal


def _fact(outputs, invoice, line=1, correction="ORIG", **extra):
    return one(
        outputs["fact_sale"],
        invoice_number=invoice,
        invoice_line_number=line,
        correction_type_code=correction,
        **extra,
    )


# --- regional tax + measures -------------------------------------------------


def test_na_line_keeps_source_tax_and_excludes_freight_from_net(outputs):
    r = _fact(outputs, "INV-NA-1001")
    assert r["gross_amount"] == D("250.00")
    assert r["net_amount"] == D("245.00")  # gross - discount, freight excluded
    assert r["tax_amount"] == D("21.25")  # NA: source amount, not recomputed
    assert r["total_including_tax"] == D("266.25")
    assert r["cost_of_sale_amount"] == D("120.00")
    assert r["gross_margin_amount"] == D("125.00")
    assert r["freight_amount"] == D("3.5000")
    assert r["margin_percent"] == D("51.0204")


def test_eu_line_recomputes_vat_from_net(outputs):
    full = _fact(outputs, "INV-EU-2001", line=1)
    discounted = _fact(outputs, "INV-EU-2001", line=2)
    assert full["tax_amount"] == D("80.00")
    assert discounted["net_amount"] == D("360.00")
    assert discounted["tax_amount"] == D("72.00")  # 20% of net, source said 80


def test_eu_reverse_charge_line_has_zero_vat(outputs):
    r = _fact(outputs, "INV-EU-2002")
    assert r["tax_amount"] == D("0.00")
    assert r["customer_vat_number"] == "DE123456789"
    assert r["total_including_tax"] == r["net_amount"] == D("100.00")


def test_apac_gst_recomputed_and_gst_free_line_zeroed(outputs):
    taxed = _fact(outputs, "INV-APAC-3001", line=1)
    gst_free = _fact(outputs, "INV-APAC-3001", line=2)
    assert taxed["tax_amount"] == D("60.00")
    assert taxed["promotion_key"] == 901
    assert gst_free["tax_amount"] == D("0.00")
    assert gst_free["gst_free_flag"] == 1
    assert gst_free["promotion_key"] == t.NOT_APPLICABLE_KEY


def test_uom_conversion_feeds_base_quantity_and_measures(outputs):
    # Legacy quirk kept on purpose: the staging variance check uses source-UOM
    # quantity (1 x 10.00 x 10% = 1.00 -> the fixture's TaxAmount), while the
    # fact recomputes GST from the base-UOM net (120.00 x 10% = 12.00).
    r = _fact(outputs, "INV-APAC-3002")
    assert r["quantity_source_uom"] == D("1.0000")
    assert r["quantity_base_uom"] == D("12.0000")  # 1 case x 12
    assert r["gross_amount"] == D("120.00")
    assert r["tax_amount"] == D("12.00")


# --- FX -------------------------------------------------------------------------


def test_fx_uses_latest_group_rate_on_or_before_invoice_date(outputs):
    r = _fact(outputs, "INV-EU-2001", line=1)
    assert r["fx_rate_source_code"] == "GROUP"
    assert r["fx_rate_date"] == dt.date(2026, 3, 10)  # 03-11 rate ignored
    assert r["fx_rate"] == D("1.080000")
    assert r["net_amount_reporting"] == D("432.00")


def test_apac_uses_treasury_feed_which_lags_group_feed(outputs):
    r = _fact(outputs, "INV-APAC-3002")
    assert r["fx_rate_source_code"] == "APAC_TREASURY"
    assert r["fx_rate_date"] == dt.date(2026, 3, 8)  # no treasury rate on 03-09
    assert r["fx_rate"] == D("0.660000")
    assert r["net_amount_reporting"] == D("79.20")


def test_missing_fx_rate_falls_back_to_one_on_invoice_date(outputs):
    r = _fact(outputs, "INV-NA-1002")
    assert r["transaction_currency"] == "CAD"
    assert r["fx_rate"] == D("1.000000")
    assert r["fx_rate_date"] == r["invoice_date"] == dt.date(2026, 3, 10)
    assert r["net_amount_reporting"] == r["net_amount"]


# --- dimension keys ------------------------------------------------------------


def test_dimension_keys_and_defaults(outputs):
    r = _fact(outputs, "INV-NA-1002")
    assert r["customer_key"] == 1001
    assert r["bill_to_customer_key"] == 1002
    assert r["salesperson_key"] == t.NOT_APPLICABLE_KEY  # blank in source
    assert r["sales_channel_key"] == 1  # blank channel defaults to DIRECT
    assert r["city_key"] == 7001
    assert r["stock_item_key"] == 10


def test_early_arriving_customer_is_inferred_not_rejected(outputs):
    r = _fact(outputs, "INV-NA-1007")
    assert r["customer_key"] == t.UNKNOWN_MEMBER_KEY
    assert r["inferred_member_flag"] == 1
    inferred = one(outputs["silver_inferred_customer"], customer_business_key="777")
    assert inferred["customer_name"] == "*** INFERRED 777"
    assert inferred["first_seen_invoice_date"] == dt.date(2026, 3, 10)


# --- rejects and holds -------------------------------------------------------------


def test_structural_and_tax_rejects_use_legacy_reason_text(outputs):
    rejects = {r["invoice_number"]: r["reject_reason"] for r in rows(outputs["silver_rejected_invoice_line"])}
    assert rejects == {
        "INV-NA-1003": t.REJECT_QUANTITY_NULL,
        "INV-NA-1004": t.REJECT_UNIT_PRICE_NULL,  # 'abc' parsed as NULL
        "INV-NA-1005": t.REJECT_STOCK_ITEM_NULL,
        "INV-XX-9001": t.REJECT_REGION_UNMAPPED,
        "INV-NA-1009": t.REJECT_TAX_VARIANCE,  # 9.00 vs 8.50 expected, NA tolerance 0.02
        "INV-EU-2003": t.REJECT_REVERSE_CHARGE_TAX,
    }
    fact_invoices = {r["invoice_number"] for r in rows(outputs["fact_sale"])}
    assert fact_invoices.isdisjoint(rejects)


def test_draft_invoices_never_reach_any_output(outputs):
    for name, df in outputs.items():
        if "invoice_number" in df.columns:
            assert rows(df, invoice_number="INV-NA-1010") == [], name


def test_missing_stock_item_is_held_not_loaded_against_unknown_member(outputs):
    assert rows(outputs["fact_sale"], invoice_number="INV-NA-1006") == []
    hold = one(outputs["silver_fact_load_hold"], natural_key_text="INV-NA-1006|1")
    assert hold["missing_dimension_name"] == "dim_stock_item"
    assert hold["missing_business_key"] == "S999"
    assert hold["hold_reason_code"] == t.HOLD_REASON_DIM_NOT_KEYED
    assert hold["hold_status_code"] == "HELD"
    assert '"stock_item_code":"S999"' in hold["source_payload"]


def test_held_line_releases_once_dimension_catches_up(spark, bronze, dims):
    late_item = spark.createDataFrame(
        [("S999", 99, dt.date(2013, 1, 1), dt.date(9999, 12, 31))],
        "stock_item_code string, stock_item_key bigint, valid_from date, valid_to date",
    )
    caught_up = {**dims, "dim_stock_item": dims["dim_stock_item"].unionByName(late_item)}
    out = run_slice(bronze, caught_up)
    assert rows(out["silver_fact_load_hold"]) == []
    released = _fact(out, "INV-NA-1006")
    assert released["stock_item_key"] == 99


# --- duplicates ------------------------------------------------------------------------


def test_highest_source_row_version_wins_within_a_batch(outputs):
    r = _fact(outputs, "INV-NA-1008")
    assert r["source_row_version"] == 7
    assert r["quantity_source_uom"] == D("2.0000")
    dropped = one(outputs["fact_sale_duplicate_archive"], invoice_number="INV-NA-1008")
    assert dropped["source_row_version"] == 5
    assert dropped["archive_reason"] == "REPLAY_WITHIN_BATCH"


def test_exact_replay_in_later_batch_is_archived_not_restated(outputs):
    fact_rows = rows(outputs["fact_sale"], invoice_number="INV-EU-2001", invoice_line_number=1)
    assert [r["correction_type_code"] for r in fact_rows] == ["ORIG"]
    assert fact_rows[0]["batch_id"] == 1
    archived = one(outputs["fact_sale_duplicate_archive"], invoice_number="INV-EU-2001")
    assert archived["batch_id"] == 2
    assert archived["source_row_version"] == 3
    assert archived["archive_reason"] == "UNCHANGED_ACROSS_BATCHES"


# --- corrections (REVERSAL pattern) -----------------------------------------------------


def test_restated_line_keeps_original_and_adds_rev_and_res(outputs):
    by_type = {
        (r["batch_id"], r["correction_type_code"]): r
        for r in rows(outputs["fact_sale"], invoice_number="INV-NA-1001", invoice_line_number=1)
    }
    assert sorted(by_type) == [(1, "ORIG"), (2, "RES"), (2, "REV"), (3, "RES"), (3, "REV")]

    orig, rev2, res2, rev3, res3 = (
        by_type[(1, "ORIG")],
        by_type[(2, "REV")],
        by_type[(2, "RES")],
        by_type[(3, "REV")],
        by_type[(3, "RES")],
    )
    # original untouched
    assert orig["net_amount"] == D("245.00") and orig["corrected_sale_key"] is None
    # batch 2: 12 units -> net 295; REV negates the ORIG, RES carries the new values
    assert rev2["net_amount"] == D("-245.00") and rev2["tax_amount"] == D("-21.25")
    assert rev2["quantity_source_uom"] == D("-10.0000") and rev2["freight_amount"] == D("-3.5000")
    assert res2["net_amount"] == D("295.00") and res2["source_row_version"] == 9
    assert rev2["corrected_sale_key"] == res2["corrected_sale_key"] == orig["sale_key"]
    # batch 3: back to 10 units; the chain points at the previous RES, not the ORIG
    assert rev3["net_amount"] == D("-295.00")
    assert res3["net_amount"] == D("245.00")
    assert rev3["corrected_sale_key"] == res3["corrected_sale_key"] == res2["sale_key"]


def test_summing_all_rows_gives_current_position(outputs):
    total = (
        outputs["fact_sale"]
        .where((F.col("invoice_number") == "INV-NA-1001") & (F.col("invoice_line_number") == 1))
        .agg(F.sum("net_amount").alias("net"), F.sum("tax_amount").alias("tax"))
        .first()
    )
    assert total["net"] == D("245.00")
    assert total["tax"] == D("21.25")
    as_at_batch_2 = (
        outputs["fact_sale"]
        .where((F.col("invoice_number") == "INV-NA-1001") & (F.col("batch_id") <= 2))
        .agg(F.sum("net_amount").alias("net"))
        .first()
    )
    assert as_at_batch_2["net"] == D("295.00")


def test_sale_keys_are_unique(outputs):
    fact = outputs["fact_sale"]
    assert fact.count() == fact.select("sale_key").distinct().count()
    assert fact.where(F.col("sale_key").isNull()).count() == 0


# --- five-day reload window / idempotency ------------------------------------------------


def test_backdated_invoice_in_later_batch_is_loaded_once_as_orig(outputs):
    r = _fact(outputs, "INV-NA-1011")
    assert r["invoice_date"] == dt.date(2026, 3, 8)
    assert r["batch_id"] == 2
    assert rows(outputs["fact_sale"], invoice_number="INV-NA-1011", correction_type_code="REV") == []


def test_replaying_an_extract_file_does_not_change_the_fact(bronze, dims, outputs):
    replayed = {
        **bronze,
        "invoice": bronze["invoice"].unionAll(bronze["invoice"].where(F.col("BatchId") == "1")),
        "invoice_line": bronze["invoice_line"].unionAll(bronze["invoice_line"].where(F.col("BatchId") == "1")),
    }
    again = run_slice(replayed, dims)
    baseline = outputs["fact_sale"]
    assert again["fact_sale"].count() == baseline.count()
    assert again["fact_sale"].exceptAll(baseline).count() == 0
    assert baseline.exceptAll(again["fact_sale"]).count() == 0


def test_natural_key_hash_matches_legacy_hashbytes_input(outputs):
    import hashlib

    r = _fact(outputs, "INV-NA-1001")
    assert r["natural_key_hash"] == hashlib.sha256(b"INV-NA-1001|1|NA").hexdigest()
