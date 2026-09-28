from datetime import date, datetime

import pytest

from oracle_extract.model import WATERMARK_DATE_WINDOW, WATERMARK_NUMERIC_KEY, WATERMARK_TIMESTAMP
from oracle_extract.source_reader import boundSourceSql, stripOrderBy
from oracle_extract.specs import PACKAGES
from oracle_extract.watermark import bindOracleSql, buildWindow, windowPredicate


def test_timestamp_window_binds_to_date_with_legacy_format():
    w = buildWindow(WATERMARK_TIMESTAMP, "2024-01-01T00:00:00", datetime(2024, 1, 2, 3, 4, 5))
    sql = bindOracleSql("WHERE c.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') AND c.LAST_UPDATE_DT < TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')",
                        ("from", "to"), w)
    assert sql == ("WHERE c.LAST_UPDATE_DT >= TO_DATE('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') "
                   "AND c.LAST_UPDATE_DT < TO_DATE('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS')")


def test_numeric_window_is_exclusive_lower_inclusive_upper():
    w = buildWindow(WATERMARK_NUMERIC_KEY, "1500", 2000)
    assert windowPredicate(w, "l.PO_LINE_ID", lowerInclusive=False, upperOpen=False) == "l.PO_LINE_ID > 1500 AND l.PO_LINE_ID <= 2000"


def test_numeric_unbounded_upper_only_emits_lower_bound():
    w = buildWindow(WATERMARK_NUMERIC_KEY, 0, None)
    assert windowPredicate(w, "RECEIPT_LINE_ID", False, False, upperUnbounded=True) == "RECEIPT_LINE_ID > 0"


def test_date_window_uses_dates_and_spark_literals():
    w = buildWindow(WATERMARK_DATE_WINDOW, datetime(2024, 3, 1, 10, 0), date(2024, 3, 5))
    assert w.fromText == "2024-03-01" and w.toText == "2024-03-05"
    assert windowPredicate(w, "RATE_DT", True, True, oracle=False) == "RATE_DT >= DATE'2024-03-01' AND RATE_DT < DATE'2024-03-05'"
    assert windowPredicate(w, "RATE_DT", True, True, oracle=True) == "RATE_DT >= TO_DATE('2024-03-01', 'YYYY-MM-DD') AND RATE_DT < TO_DATE('2024-03-05', 'YYYY-MM-DD')"


def test_bind_marker_count_mismatch_raises():
    w = buildWindow(WATERMARK_TIMESTAMP, "2024-01-01 00:00:00", "2024-01-02 00:00:00")
    with pytest.raises(ValueError):
        bindOracleSql("WHERE x >= ? AND y < ? AND z = ?", ("from", "to"), w)


@pytest.mark.parametrize("packageName", sorted(PACKAGES))
def test_every_source_query_binds_its_markers(packageName):
    spec = PACKAGES[packageName]
    if spec.isIncremental:
        w = buildWindow(spec.watermarkType, "0" if spec.watermarkType == WATERMARK_NUMERIC_KEY else "2024-01-01 00:00:00", "10" if spec.watermarkType == WATERMARK_NUMERIC_KEY else "2024-01-02 00:00:00")
    else:
        w = None
    for source in spec.sources:
        sql = boundSourceSql(source, w)
        assert "?" not in sql, f"{packageName}/{source.name} left an unbound marker"
        assert len(source.columns) > 0


def test_strip_trailing_order_by_only():
    assert stripOrderBy("SELECT a FROM t WHERE x IN (SELECT y FROM u ORDER BY y)").endswith("ORDER BY y)")
    assert stripOrderBy("SELECT a FROM t ORDER BY a") == "SELECT a FROM t"
