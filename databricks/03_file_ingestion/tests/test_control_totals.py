import datetime
from decimal import Decimal

import pytest

from wwi_file_ingestion import control_totals as ct
from wwi_file_ingestion import feeds


def totals(**kwargs):
    base = ct.FileTotals(fileName="f.csv")
    for key, value in kwargs.items():
        setattr(base, key, value)
    return base


def test_na_footer_count_strict_vs_legacy():
    mismatch = totals(detailRowCount=3, footerRowCount=2)
    assert ct.fileStatus(feeds.PARTNER_SALES_NA, mismatch, ct.MODE_STRICT) == ct.STATUS_QUARANTINED
    assert ct.fileStatus(feeds.PARTNER_SALES_NA, mismatch, ct.MODE_LEGACY) == ct.STATUS_PROCESSED
    assert any("do not reconcile" in w for w in mismatch.warnings)
    # missing footer passes in both modes ("? = 0" branch of the legacy expression)
    assert ct.fileStatus(feeds.PARTNER_SALES_NA, totals(detailRowCount=3), ct.MODE_STRICT) == ct.STATUS_PROCESSED


def test_eu_apac_amount_totals_compare_to_cents():
    ok = totals(footerAmountTotal=Decimal("100.004"), detailAmountTotal=Decimal("100.00"))
    bad = totals(footerAmountTotal=Decimal("100.02"), detailAmountTotal=Decimal("100.00"))
    assert ct.intendedTotalsMatch(feeds.PARTNER_SALES_EU, ok)
    assert not ct.intendedTotalsMatch(feeds.PARTNER_SALES_APAC, bad)


def test_supplier_checksum_quarantines_even_in_legacy_mode():
    bad = totals(detailRowCount=2, footerRowCount=2, footerChecksum=123, priceChecksum=456)
    assert ct.fileStatus(feeds.SUPPLIER_CATALOG, bad, ct.MODE_LEGACY) == ct.STATUS_QUARANTINED
    good = totals(detailRowCount=2, footerRowCount=2, footerChecksum=456, priceChecksum=456)
    assert ct.fileStatus(feeds.SUPPLIER_CATALOG, good, ct.MODE_LEGACY) == ct.STATUS_PROCESSED
    rows = totals(detailRowCount=1, footerRowCount=2, footerChecksum=456, priceChecksum=456)
    assert ct.fileStatus(feeds.SUPPLIER_CATALOG, rows, ct.MODE_STRICT) == ct.STATUS_QUARANTINED
    assert ct.fileStatus(feeds.SUPPLIER_CATALOG, rows, ct.MODE_LEGACY) == ct.STATUS_PROCESSED


def test_carrier_sidecar_total_only_binds_in_strict_mode():
    t = totals(detailRowCount=5, sidecarRowCount=6)
    assert ct.fileStatus(feeds.CARRIER_SCAN, t, ct.MODE_STRICT) == ct.STATUS_QUARANTINED
    assert ct.fileStatus(feeds.CARRIER_SCAN, t, ct.MODE_LEGACY) == ct.STATUS_PROCESSED
    assert ct.fileStatus(feeds.CARRIER_SCAN, totals(detailRowCount=5), ct.MODE_STRICT) == ct.STATUS_PROCESSED


def test_quarantine_sweep_status():
    assert ct.fileStatus(feeds.QUARANTINE_MALFORMED, totals(replayEligibleCount=1)) == ct.STATUS_SWEPT
    assert ct.fileStatus(feeds.QUARANTINE_MALFORMED, totals(replayEligibleCount=0)) == ct.STATUS_UNREADABLE


def test_unknown_mode_rejected():
    with pytest.raises(ValueError):
        ct.controlTotalsMatch(feeds.PARTNER_SALES_NA, totals(), "lenient")


def test_duplicate_detection_same_name_and_size_only():
    prior = [
        {"FileName": "a.csv", "Status": ct.STATUS_PROCESSED, "FileSizeBytes": 100},
        {"FileName": "b.csv", "Status": ct.STATUS_QUARANTINED, "FileSizeBytes": 50},
    ]
    assert ct.isDuplicateFile("a.csv", 100, prior)
    assert not ct.isDuplicateFile("a.csv", 101, prior)  # re-send with different content: load again
    assert not ct.isDuplicateFile("b.csv", 50, prior)  # quarantined files may be re-dropped
    assert not ct.isDuplicateFile("c.csv", 100, prior)


def test_volume_layouts():
    when = datetime.datetime(2024, 5, 17, 6, 30)
    assert ct.archivePath("wwi_dev", feeds.PARTNER_SALES_NA, "x.csv", when) == "/Volumes/wwi_dev/bronze/archive/partner/na/2024/05/x.csv"
    assert ct.poisonPath("wwi_dev", "x.csv") == "/Volumes/wwi_dev/bronze/quarantine/poison/x.csv"
    assert ct.rejectFilePath("wwi_dev", feeds.SUPPLIER_CATALOG, "x.psv") == "/Volumes/wwi_dev/bronze/quarantine/supplier/x.psv.rej"
    assert ct.duplicatePath("wwi_dev", feeds.CARRIER_SCAN, "x.csv") == "/Volumes/wwi_dev/bronze/quarantine/carrier/duplicate/x.csv"
