from sales_o2c.watermark import EPOCH, NUMERIC_KEY, TIMESTAMP, resolveWatermarkFrom


def test_numeric_key_defaults_to_zero_when_no_history():
    assert resolveWatermarkFrom(None, NUMERIC_KEY, False) == "0"
    assert resolveWatermarkFrom("", NUMERIC_KEY, False) == "0"


def test_timestamp_defaults_to_epoch_when_no_history():
    assert resolveWatermarkFrom(None, TIMESTAMP, False) == EPOCH


def test_reload_full_history_ignores_stored_value():
    assert resolveWatermarkFrom("73595", NUMERIC_KEY, True) == "0"
    assert resolveWatermarkFrom("2016-05-31T00:00:00", TIMESTAMP, True) == EPOCH


def test_stored_value_is_used_for_incremental_run():
    assert resolveWatermarkFrom("73595", NUMERIC_KEY, False) == "73595"
