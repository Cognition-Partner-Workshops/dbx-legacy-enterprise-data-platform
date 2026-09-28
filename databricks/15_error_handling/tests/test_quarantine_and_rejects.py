from datetime import date

from err_handling import quarantine, rejects


def test_quarantine_reason_order_matches_legacy_case():
    assert quarantine.quarantineReasonCode(0, "Failed", None) == "ZERO_LENGTH"
    assert quarantine.quarantineReasonCode(10, "Failed", None) == "STRUCTURE"
    assert quarantine.quarantineReasonCode(10, "Passed", None) == "UNKNOWN_FEED"
    assert quarantine.quarantineReasonCode(10, "Passed", "CUST") == "OTHER"


def test_quarantine_path_uses_yyyymm_subfolder():
    d = date(2026, 3, 9)
    assert quarantine.quarantineFolder("/Volumes/wwi_dev/bronze/quarantine/", "quarantine", d) == "/Volumes/wwi_dev/bronze/quarantine/quarantine/202603"
    assert quarantine.quarantineDestination("/Volumes/x/y/z", "q", d, "a.csv") == "/Volumes/x/y/z/q/202603/a.csv"


def test_reject_file_name():
    assert rejects.rejectFileName(42, date(2026, 3, 9)) == "rejects_42_20260309.csv"


def test_routing_destination():
    assert rejects.routingDestination(True) == "QUARANTINE"
    assert rejects.routingDestination(False) == "REPROCESS"


def test_err_table_contract_covers_all_ten_legacy_err_tables():
    assert len(rejects.ERR_TABLES) == 10
    assert all(name.startswith("silver.err_rejected_") for name in rejects.ERR_TABLES)
