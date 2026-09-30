"""Verdict rules of the evidence task (pure Python, no Spark)."""

from logistics_returns.recon import extractVerdict, verdictFor


def matching(**extra) -> list[dict]:
    return [
        {"check": "row_count", "source": 2200, "target": 2200, "pass": True},
        {"check": "checksum", "source": "1", "target": "1", "pass": True},
        {
            "check": "source_vs_legacy_baseline",
            "live_oltp_rows": extra.get("live", 2200),
            "legacy_raw_rows": 2200,
            "pass": extra.get("live", 2200) >= 2200,
        },
    ]


def test_extract_seeded_from_legacy_raw_with_empty_live_source_is_fail_not_pass():
    assert extractVerdict(matching(live=0)) == "FAIL"


def test_extract_with_consistent_live_source_can_pass():
    assert extractVerdict(matching(live=2200)) == "PASS"


def test_extract_without_baseline_check_is_fail():
    assert extractVerdict(matching()[:2]) == "FAIL"


def test_source_derived_is_capped_at_partial():
    checks = matching()[:2]
    assert verdictFor(checks, sourceDerived=True) == "PARTIAL"
    checks[0]["pass"] = False
    assert verdictFor(checks, sourceDerived=True) == "FAIL"
