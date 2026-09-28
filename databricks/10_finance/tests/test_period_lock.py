from pyspark.sql import Row

import period_lock


def test_locked_ledgers_from_lock_table(spark):
    locks = spark.createDataFrame([
        Row(LedgerCode="EU01", AccountingPeriod="2024-02", LockStatusCode="Locked"),
        Row(LedgerCode="NA01", AccountingPeriod="2024-02", LockStatusCode="Unlocked"),
        Row(LedgerCode="APAC01", AccountingPeriod="2024-03", LockStatusCode="Locked"),
    ])
    assert period_lock.lockedLedgersFrom(locks, "2024-02") == ["EU01"]
    assert period_lock.lockedLedgersFrom(locks, "2024-02", ["NA01"]) == []
    assert period_lock.lockedLedgersFrom(locks, "2024-03") == ["APAC01"]
    assert period_lock.lockedLedgersFrom(locks, "2024-04") == []


def test_assert_period_open_raises(monkeypatch):
    monkeypatch.setattr(period_lock, "lockedLedgers", lambda *a, **k: ["EU01", "NA01"])
    try:
        period_lock.assertPeriodOpen(None, "wwi_dev", "2024-02", "FIN_Load_GlPostings")
    except period_lock.PeriodLockedError as e:
        assert "EU01, NA01" in str(e) and "2024-02" in str(e)
    else:
        raise AssertionError("expected PeriodLockedError")
    monkeypatch.setattr(period_lock, "lockedLedgers", lambda *a, **k: [])
    period_lock.assertPeriodOpen(None, "wwi_dev", "2024-03", "FIN_Load_GlPostings")
