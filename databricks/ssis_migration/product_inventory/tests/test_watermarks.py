from datetime import datetime, timedelta

from product_inventory.control import resolveKeyWindow, resolveTimestampWindow


def test_timestamp_window_applies_four_hour_lookback():
    now = datetime(2026, 9, 30, 12, 0, 0)
    last = datetime(2026, 9, 30, 6, 0, 0)
    windowFrom, windowTo = resolveTimestampWindow(last, False, 240, now)
    assert windowFrom == last - timedelta(hours=4)
    assert windowTo == now


def test_timestamp_window_full_history_uses_floor():
    now = datetime(2026, 9, 30, 12, 0, 0)
    assert resolveTimestampWindow(datetime(2026, 1, 1), True, 240, now) == (datetime(1900, 1, 1), now)
    assert resolveTimestampWindow(None, False, 240, now) == (datetime(1900, 1, 1), now)


def test_key_window_is_half_open_and_monotonic():
    assert resolveKeyWindow(None, 500, False) == (0, 500)
    assert resolveKeyWindow(120, 500, False) == (120, 500)
    assert resolveKeyWindow(120, 500, True) == (0, 500)
    # source has no new keys: window collapses to [last, last] so nothing is re-extracted
    assert resolveKeyWindow(120, None, False) == (120, 120)
    assert resolveKeyWindow(120, 100, False) == (120, 120)
