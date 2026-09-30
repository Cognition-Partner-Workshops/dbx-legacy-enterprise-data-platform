from datetime import datetime, timedelta, timezone

from customer_party.config import LOW_DATE
from customer_party.watermark import computeWatermarkWindow

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def test_first_run_starts_at_low_date():
    assert computeWatermarkWindow(None, NOW, 120, False) == (LOW_DATE, NOW)


def test_reload_full_history_ignores_previous_watermark():
    last = NOW - timedelta(days=1)
    assert computeWatermarkWindow(last, NOW, 120, True) == (LOW_DATE, NOW)


def test_incremental_window_applies_lookback():
    last = NOW - timedelta(days=1)
    windowFrom, windowTo = computeWatermarkWindow(last, NOW, 120, False)
    assert windowFrom == last - timedelta(minutes=120)
    assert windowTo == NOW
