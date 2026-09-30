from datetime import date, datetime

from pyspark.sql import functions as F

from finance import facts
from finance.facts import DIM_SCHEMA, applyScd2, buildGlPostingFact, unknownCostCenterRow, withCostCenterKey
from finance.staging import latestPerKey, withSupplierKey


def _incoming(spark, rows):
    cols = ["cost_center_code", *facts.SCD2_ATTRS]
    data = []
    for code, name, hsh in rows:
        d = {c: None for c in cols}
        d.update(
            cost_center_code=code,
            cost_center_name=name,
            region_code="NA",
            hierarchy_level=1,
            is_active=True,
            scd_hash=hsh,
        )
        data.append(d)
    schema = DIM_SCHEMA.replace("cost_center_key int, ", "").replace(
        ", valid_from timestamp, valid_to timestamp, is_current boolean, batch_id bigint", ""
    )
    return spark.createDataFrame(data, schema).select(*cols)


def test_scd2_expire_insert_keep(spark):
    t0, t1 = datetime(2024, 1, 1), datetime(2024, 12, 31)
    existing = unknownCostCenterRow(spark)
    dim1 = applyScd2(existing, _incoming(spark, [("A", "Alpha", "h1"), ("B", "Beta", "h2")]), t0, 1)
    assert dim1.count() == 3  # unknown + 2
    dim2 = applyScd2(
        dim1,
        _incoming(spark, [("A", "Alpha renamed", "h1x"), ("B", "Beta", "h2"), ("C", "Gamma", "h3")]),
        t1,
        2,
    )
    rows = {(r["cost_center_code"], r["is_current"]): r for r in dim2.collect()}
    assert dim2.count() == 5  # unknown, A old, A new, B, C
    assert rows[("A", False)]["valid_to"] == t1 and rows[("A", True)]["cost_center_name"] == "Alpha renamed"
    assert rows[("B", True)]["batch_id"] == 1  # unchanged row kept, not re-versioned
    assert rows[("C", True)]["batch_id"] == 2
    keys = [r["cost_center_key"] for r in dim2.collect()]
    assert len(keys) == len(set(keys)) and 0 in keys  # surrogate keys unique, unknown member preserved


def test_dedup_latest_per_key(spark):
    df = spark.createDataFrame(
        [(1, "old", datetime(2024, 1, 1)), (1, "new", datetime(2024, 6, 1)), (2, "only", None)],
        "k int, v string, last_upd_dt timestamp",
    )
    got = {r["k"]: r["v"] for r in latestPerKey(df, "k").collect()}
    assert got == {1: "new", 2: "only"}


def test_unknown_member_keys(spark):
    dim = spark.createDataFrame(
        [(7, 100, True), (8, 200, False)], "supplier_key int, wwi_supplier_id int, is_current_row boolean"
    )
    df = spark.createDataFrame([(100,), (200,), (300,), (None,)], "supp_id int")
    got = {
        r["supp_id"]: (r["supplier_key"], r["supplier_key_is_unknown"])
        for r in withSupplierKey(df, dim).collect()
    }
    assert got[100] == (7, False)
    assert got[200] == (0, True)  # only expired row -> late-arriving / unknown member
    assert got[300] == (0, True) and got[None] == (0, True)

    ccDim = spark.createDataFrame([("NA-CORP", 5)], "_dim_cc_code string, _dim_cc_key int")
    cc = withCostCenterKey(spark.createDataFrame([("NA-CORP",), ("ZZ",)], "cost_center_code string"), ccDim)
    assert {r["cost_center_code"]: r["cost_center_key"] for r in cc.collect()} == {"NA-CORP": 5, "ZZ": 0}


GL = (
    "journal_line_id decimal(12,0), journal_id decimal(12,0), journal_num string, ledger_code string, region_code string, org_cd string, "
    "accounting_period string, fiscal_year_nbr int, fiscal_period_nbr int, gl_date date, posted_dt date, journal_source_cd string, "
    "journal_category_cd string, line_num int, gl_account_id decimal(12,0), account_code string, account_name string, account_type_cd string, "
    "account_class_cd string, normal_balance_cd string, cost_center_code string, project_cd string, intercompany_cd string, currency_cd string, "
    "debit_amount decimal(18,5), credit_amount decimal(18,5), net_amount decimal(18,5), posting_side string, base_debit_amt_usd decimal(18,5), "
    "base_credit_amt_usd decimal(18,5), tax_regime_code string, src_doc_type_cd string, src_doc_id string, subledger_source_key string, "
    "reversal_flag string, accrual_flag string, period_status_cd string, posted_flag string, posting_status_cd string, journal_line_cnt int"
)


def _line(lid, jid, period, acct, dr, cr, status="OPEN", region="NA", cnt=2, posted="Y"):
    return (
        lid,
        jid,
        f"J{jid}",
        "NA_USD",
        region,
        "ORG",
        period,
        2024,
        12,
        date(2024, 12, 15),
        date(2024, 12, 15),
        "AP",
        "PURCHASE",
        1,
        1,
        acct,
        "acct",
        "EXP",
        "OPEX",
        "DR",
        "NA-CORP",
        None,
        None,
        "USD",
        dr,
        cr,
        dr - cr,
        "DR" if dr else "CR",
        dr,
        cr,
        "SALES",
        None,
        None,
        None,
        "N",
        "N",
        status,
        posted,
        "POST",
        cnt,
    )


def test_gl_gate_balance_completeness_and_period_lock(spark, typed):
    lines = typed(
        [
            _line(1, 10, "2024-12", "6000", 100.0, 0.0),
            _line(2, 10, "2024-12", "2100", 0.0, 100.0),  # balanced, complete
            _line(3, 11, "2024-12", "6000", 100.0, 0.0),
            _line(4, 11, "2024-12", "2100", 0.0, 90.0),  # unbalanced by 10
            _line(5, 12, "2024-12", "6000", 50.0, 0.0, cnt=2),  # late-arriving: 2nd line not yet extracted
            _line(6, 13, "2024-11", "6000", 10.0, 0.0, status="CLSD", cnt=1),  # Oracle closed
            _line(7, 14, "2024-10", "6000", 10.0, 0.0, cnt=1),  # locked by FIN_Close_PeriodLock
            _line(8, 15, "2024-12", "6000", 100.0, 0.0, cnt=2, posted="N"),  # not posted -> ignored
            _line(9, 15, "2024-12", "2100", 0.0, 100.0, cnt=2, posted="N"),
        ],
        GL,
    )
    ccDim = spark.createDataFrame([("NA-CORP", 5)], "_dim_cc_code string, _dim_cc_key int")
    locks = typed([("NA_USD", "NA", "2024-10", "LOCKED", datetime(2024, 11, 5), 1, 0, "")], facts.LOCK_SCHEMA)
    postable, rejected = buildGlPostingFact(lines, ccDim, locks)
    assert sorted(int(r["journal_line_id"]) for r in postable.collect()) == [1, 2]
    reasons = {int(r["journal_line_id"]): r["_reject_reason"] for r in rejected.collect()}
    assert reasons == {
        3: "JOURNAL_UNBALANCED",
        4: "JOURNAL_UNBALANCED",
        5: "JOURNAL_INCOMPLETE",
        6: "PERIOD_NOT_OPEN",
        7: "PERIOD_NOT_OPEN",
    }
    assert postable.select("cost_center_key").distinct().collect()[0][0] == 5

    # AllowUnbalancedJournals lets the imbalance through but never the closed period
    postable2, _ = buildGlPostingFact(lines, ccDim, locks, allowUnbalanced=True)
    assert sorted(int(r["journal_line_id"]) for r in postable2.collect()) == [1, 2, 3, 4]
    assert postable2.where(F.col("period_is_closed")).count() == 0
