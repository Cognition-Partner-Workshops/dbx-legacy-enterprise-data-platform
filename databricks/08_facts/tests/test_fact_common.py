from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql import types as T

import fact_common as fc
from conftest import writeDelta
from dbx_etl_common import control


def test_scd2_lookup_matches_effective_date_and_falls_back_to_unknown(spark, catalog, scd2Dimension):
    spec = fc.DIMENSIONS["Customer"]
    dim = scd2Dimension(fc.tableName(catalog, "gold", spec.table), spec, [
        (10, "C1", "v1", datetime(2020, 1, 1), datetime(2024, 2, 1), False, False, "NA", 1),
        (11, "C1", "v2", datetime(2024, 2, 1), datetime(9999, 12, 31, 23, 59, 59), True, False, "NA", 1),
    ])
    src = spark.createDataFrame([("C1", date(2024, 1, 15)), ("C1", date(2024, 3, 1)), ("C9", date(2024, 3, 1)), (None, date(2024, 3, 1))], "bk string, d date")
    out = fc.lookupDimension(src, spark.table(dim), spec, "bk", "customer_key", "d").orderBy("d", "bk").collect()
    keys = {(r["bk"], r["d"]): (r["customer_key"], r["customer_key_miss"]) for r in out}
    assert keys[("C1", date(2024, 1, 15))] == (10, False)
    assert keys[("C1", date(2024, 3, 1))] == (11, False)
    assert keys[("C9", date(2024, 3, 1))] == (fc.UNKNOWN_KEY, True)
    assert keys[(None, date(2024, 3, 1))] == (fc.UNKNOWN_KEY, False)


def test_infer_members_creates_open_ended_inferred_rows_once(spark, catalog, scd2Dimension):
    spec = fc.DIMENSIONS["Supplier"]
    dim = scd2Dimension(fc.tableName(catalog, "gold", spec.table), spec, [(5, "S1", "Acme", datetime(2020, 1, 1), datetime(9999, 12, 31, 23, 59, 59), True, False, "EU", 1)])
    missing = spark.createDataFrame([("S2", "EU"), ("S2", "EU"), ("S1", "EU")], "SupplierBusinessKey string, RegionCode string")
    assert fc.inferMembers(spark, catalog, spec, missing, "SupplierBusinessKey", 1, 2, "WWI", regionCol="RegionCode") == 1
    assert fc.inferMembers(spark, catalog, spec, missing, "SupplierBusinessKey", 1, 3, "WWI", regionCol="RegionCode") == 0
    row = spark.table(dim).where(F.col("wwi_supplier_id") == "S2").first()
    assert row["supplier_key"] == 6 and row["is_inferred_member"] is True and row["is_current_row"] is True
    assert row["valid_from"].date() == fc.INFERRED_VALID_FROM and row["valid_to"].year == 9999 and row["region_code"] == "EU"


def test_dedup_by_row_version_keeps_latest(spark):
    df = spark.createDataFrame([("k1", 1, "old"), ("k1", 3, "new"), ("k2", 1, "only")], "natural_key_hash string, ver int, v string")
    out = {r["natural_key_hash"]: r["v"] for r in fc.dedupByRowVersion(df, ["natural_key_hash"], "ver").collect()}
    assert out == {"k1": "new", "k2": "only"}


def test_merge_fact_is_idempotent_and_updates_changed_rows(spark, catalog):
    fullName = fc.tableName(catalog, "gold", "fact_merge_test")
    spark.sql("DROP TABLE IF EXISTS %s" % fullName)
    rows = spark.createDataFrame([(1, date(2024, 1, 1), Decimal("10.00")), (2, date(2024, 1, 2), Decimal("20.00"))], "k int, dk date, amt decimal(18,2)")
    first = fc.mergeFact(spark, fullName, rows, ["k"], clusterCols=["dk"])
    assert first["inserted"] == 2
    again = fc.mergeFact(spark, fullName, rows, ["k"], clusterCols=["dk"])
    assert again["inserted"] == 0 and spark.table(fullName).count() == 2
    changed = spark.createDataFrame([(2, date(2024, 1, 2), Decimal("25.00")), (3, date(2024, 1, 3), Decimal("30.00"))], "k int, dk date, amt decimal(18,2)")
    third = fc.mergeFact(spark, fullName, changed, ["k"], clusterCols=["dk"])
    assert third["inserted"] == 1 and third["updated"] == 1
    assert spark.table(fullName).where("k = 2").first()["amt"] == Decimal("25.00")


def test_assign_surrogate_keys_is_stable_for_existing_natural_keys(spark, catalog):
    fullName = fc.tableName(catalog, "gold", "fact_key_test")
    spark.sql("DROP TABLE IF EXISTS %s" % fullName)
    seed = spark.createDataFrame([(1, "h1"), (2, "h2")], "fk bigint, natural_key_hash string")
    writeDelta(spark, fullName, seed)
    incoming = spark.createDataFrame([("h2",), ("h3",), ("h4",)], "natural_key_hash string")
    out = {r["natural_key_hash"]: r["fk"] for r in fc.assignSurrogateKeys(spark, fullName, incoming, "fk", ["natural_key_hash"], matchCols=["natural_key_hash"]).collect()}
    assert out["h2"] == 2
    assert sorted([out["h3"], out["h4"]]) == [3, 4]


def test_replace_date_range_swaps_only_the_snapshot_partition(spark, catalog):
    fullName = fc.tableName(catalog, "gold", "fact_snapshot_test")
    spark.sql("DROP TABLE IF EXISTS %s" % fullName)
    day1, day2 = date(2024, 3, 1), date(2024, 3, 2)
    initial = spark.createDataFrame([(day1, "a", 1), (day1, "b", 2), (day2, "a", 5)], "snapshot_date_key date, item string, qty int")
    fc.replaceDateRange(spark, fullName, initial, "snapshot_date_key", day1, day2)
    rerun = spark.createDataFrame([(day2, "a", 7), (day2, "c", 1)], "snapshot_date_key date, item string, qty int")
    fc.replaceDateRange(spark, fullName, rerun, "snapshot_date_key", day2, day2)
    result = {(r["snapshot_date_key"], r["item"]): r["qty"] for r in spark.table(fullName).collect()}
    assert result == {(day1, "a"): 1, (day1, "b"): 2, (day2, "a"): 7, (day2, "c"): 1}


def test_effective_fx_rate_uses_latest_on_or_before_and_flags_misses(spark):
    rates = spark.createDataFrame(
        [("EUR", date(2024, 1, 1), Decimal("1.10000000"), "ECB"), ("EUR", date(2024, 1, 3), Decimal("1.12000000"), "ECB")],
        "CurrencyCode string, RateDate date, RateToUsd decimal(18,8), RateSourceCode string",
    )
    df = spark.createDataFrame([("EUR", date(2024, 1, 2)), ("EUR", date(2024, 1, 5)), ("USD", date(2024, 1, 5)), ("GBP", date(2024, 1, 5))], "ccy string, d date")
    out = {(r["ccy"], r["d"]): (r["FxRateToUsd"], r["FxRateToUsd_miss"]) for r in fc.lookupEffectiveFxRate(df, rates, "ccy", "d").collect()}
    assert out[("EUR", date(2024, 1, 2))][0] == Decimal("1.10000000")
    assert out[("EUR", date(2024, 1, 5))][0] == Decimal("1.12000000")
    assert out[("USD", date(2024, 1, 5))] == (Decimal("1.00000000"), False)
    assert out[("GBP", date(2024, 1, 5))] == (None, True)


def test_hold_retry_and_abandon_follow_regional_budget(spark, catalog):
    holdName = fc.tableName(catalog, "gold", fc.FACT_LOAD_HOLD_TABLE)
    spark.sql("DROP TABLE IF EXISTS %s" % holdName)
    src = spark.createDataFrame([("NA", "INV1", 1, "SI-9", date(2024, 3, 1)), ("APAC", "INV2", 1, "SI-9", date(2024, 3, 1))], "RegionCode string, InvoiceNumber string, InvoiceLineNumber int, StockItemBusinessKey string, InvoiceDate date")
    held = fc.holdRows(spark, catalog, src, "Fact.Sale", "Stock Item", "StockItemBusinessKey", fc.HOLD_REASON_DIM_NOT_KEYED, 1, 1,
                       businessDateCol="InvoiceDate", naturalKeyCols=("InvoiceNumber", "InvoiceLineNumber", "RegionCode"), sourceSystemCode="WWI")
    assert held == 2
    assert fc.holdRows(spark, catalog, src, "Fact.Sale", "Stock Item", "StockItemBusinessKey", fc.HOLD_REASON_DIM_NOT_KEYED, 2, 2, businessDateCol="InvoiceDate", naturalKeyCols=("InvoiceNumber", "InvoiceLineNumber", "RegionCode")) == 0
    limits = {r["region_code"]: r["max_retry_count"] for r in spark.table(holdName).collect()}
    assert limits == {"NA": fc.HOLD_RETRY_LIMITS["NA"], "APAC": fc.HOLD_RETRY_LIMITS["APAC"]}

    rehydrated = fc.readHeldRows(spark, catalog, "Fact.Sale", src.schema)
    assert rehydrated.count() == 2 and set(rehydrated.columns) >= set(src.columns) | {"fact_load_hold_key", "retry_count", "max_retry_count"}

    naKey = spark.table(holdName).where("region_code = 'NA'").first()["fact_load_hold_key"]
    apacKey = spark.table(holdName).where("region_code = 'APAC'").first()["fact_load_hold_key"]
    retried = spark.createDataFrame([(naKey,), (apacKey,)], "fact_load_hold_key bigint")
    for attempt in range(fc.HOLD_RETRY_LIMITS["NA"]):
        result = fc.settleHolds(spark, catalog, None, retried, 10 + attempt, 10 + attempt)
    statuses = {r["region_code"]: r["hold_status_code"] for r in spark.table(holdName).collect()}
    assert statuses["NA"] == fc.HOLD_STATUS_ABANDONED and statuses["APAC"] == fc.HOLD_STATUS_HELD
    assert result["abandoned"] == 1
    assert any(r["rejectReasonCode"] == fc.REJECT_HOLD_EXPIRED for r in control.rejects)

    released = spark.createDataFrame([(apacKey, 999)], "fact_load_hold_key bigint, released_fact_key bigint")
    fc.settleHolds(spark, catalog, released, None, 20, 20)
    apac = spark.table(holdName).where("region_code = 'APAC'").first()
    assert apac["hold_status_code"] == fc.HOLD_STATUS_RELEASED and apac["released_fact_key"] == 999


def test_sale_duplicate_rank_prefers_newest_row_version_then_highest_key(spark):
    df = spark.createDataFrame(
        [(1, "h1", 5, "ORIG"), (2, "h1", 7, "ORIG"), (3, "h1", 7, "ORIG"), (4, "h2", 1, "ORIG"), (5, "h1", 9, "REV")],
        "sale_key bigint, natural_key_hash string, source_row_version bigint, correction_type_code string",
    )
    ranked = {r["sale_key"]: r["duplicate_rank"] for r in fc.saleDuplicateRank(df).collect()}
    assert ranked[3] == 1 and ranked[2] == 2 and ranked[1] == 3 and ranked[4] == 1
    assert 5 not in ranked
