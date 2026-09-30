from datetime import date, datetime

from finance.close import apacPeriodEnd, decidePeriodLock, maskToRegex, runAllocationRules


def _rule(rid, rset, seq, src, tgt, method, pct=None, fixed=None, driver=None, curr=None):
    return {
        "alloc_rule_id": rid,
        "rule_set_cd": rset,
        "rule_seq_nbr": seq,
        "region_cd": "NA",
        "source_cost_center_cd": src,
        "target_cost_center_cd": tgt,
        "allocation_method_cd": method,
        "allocation_pct": pct,
        "fixed_amt": fixed,
        "driver_cd": driver,
        "fixed_curr_cd": curr,
        "reverse_next_period_flg": "N",
    }


def test_allocation_rules_are_sequential_and_method_specific():
    rules = [
        _rule(1, "NA", 10, "CHI", "PROC", "PCT", pct=15),
        _rule(2, "NA", 20, "CHI", "DAL", "DRIVER", driver="HEADCT"),
        _rule(3, "NA", 30, "PROC", "TOR", "STEP"),
        _rule(4, "NA", 40, "CORP", "EU", "FIXED", fixed=42000, curr="USD"),
        _rule(5, "NA", 50, "HQ", "A", "EVEN"),
        _rule(6, "NA", 50, "HQ", "B", "EVEN"),
    ]
    rows = runAllocationRules(rules, {"CHI": 1000.0, "CORP": 50000.0, "HQ": 300.0})
    byId = {r["alloc_rule_id"]: r for r in rows if r["alloc_rule_id"]}
    assert byId[1]["allocated_amount"] == 150.0 and byId[1]["pool_amount_after"] == 850.0
    assert byId[2]["allocation_status"] == "DRIVER_UNAVAILABLE" and byId[2]["allocated_amount"] == 0.0
    assert (
        byId[3]["pool_amount_before"] == 150.0 and byId[3]["allocated_amount"] == 150.0
    )  # STEP sees what rule 10 moved in
    assert byId[4]["allocated_amount"] == 42000.0
    assert byId[5]["allocated_amount"] == 150.0 and byId[6]["allocated_amount"] == 150.0
    residual = {
        r["source_cost_center_cd"]: r["pool_amount_after"]
        for r in rows
        if r["allocation_status"] == "UNALLOCATED"
    }
    assert residual == {"CHI": 850.0, "CORP": 8000.0}

    # driver supplied -> proportional share among the DRIVER rules of the same source
    rows2 = runAllocationRules(rules[:2], {"CHI": 1000.0}, {"HEADCT": 40.0})
    assert {r["alloc_rule_id"]: r["allocated_amount"] for r in rows2 if r["alloc_rule_id"]} == {
        1: 150.0,
        2: 850.0,
    }


def test_mask_to_regex():
    import re

    assert re.match(maskToRegex("0000-63%"), "0000-6300-000-000")
    assert not re.match(maskToRegex("0000-63%"), "0000-6900-000-000")


def test_apac_period_end():
    assert apacPeriodEnd("2024-12") == date(2024, 3, 31)  # FY2024 P12 = March 2024
    assert apacPeriodEnd("2025-01") == date(2024, 4, 30)  # FY2025 P1 = April 2024
    assert apacPeriodEnd("2024-09") == date(2023, 12, 31)


def test_period_lock_decisions():
    now = datetime(2025, 1, 5)
    rows = decidePeriodLock("2024-12", "ALL", {"EU": 2}, date(2024, 12, 31), date(2024, 3, 31), 9, now)
    got = {r["ledger_code"]: r["lock_status"] for r in rows}
    assert got == {"NA_USD": "LOCKED", "EU_EUR": "REFUSED", "AP_AUD": "LOCKED"}
    assert [r for r in rows if r["ledger_code"] == "EU_EUR"][0]["variance_count"] == 2

    # APAC period still running -> deferred; ledger scope narrows the decision set
    rows = decidePeriodLock("2025-01", "AP_AUD", {}, date(2024, 4, 15), date(2024, 4, 30), 9, now)
    assert [(r["ledger_code"], r["lock_status"]) for r in rows] == [("AP_AUD", "DEFERRED")]
