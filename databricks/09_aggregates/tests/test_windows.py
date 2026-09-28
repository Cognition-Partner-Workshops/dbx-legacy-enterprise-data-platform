import datetime as dt

import agg_common as ac


def test_daily_window_defaults_to_trailing_days_ending_business_date():
    w = ac.dailyWindow("", "", dt.date(2024, 5, 10), trailingDays=3, reloadFullHistory=False)
    assert (w.fromDate, w.toDate) == (dt.date(2024, 5, 7), dt.date(2024, 5, 10))
    assert w.sqlLiteral("sales_date") == "sales_date BETWEEN DATE'2024-05-07' AND DATE'2024-05-10'"


def test_daily_window_honours_explicit_parameters_and_full_reload():
    w = ac.dailyWindow("2024-01-01", "2024-01-31", dt.date(2024, 5, 10), trailingDays=3, reloadFullHistory=False)
    assert (w.fromDate, w.toDate) == (dt.date(2024, 1, 1), dt.date(2024, 1, 31))
    full = ac.dailyWindow("2024-01-01", "2024-01-31", dt.date(2024, 5, 10), trailingDays=3, reloadFullHistory=True)
    assert full.fromDate == dt.date(2013, 1, 1) and full.toDate == dt.date(2024, 1, 31)


def test_period_window_rebuilds_prior_closed_periods():
    w = ac.periodWindow("2024-03", 2, dt.date(2024, 4, 2), reloadFullHistory=False)
    assert (w.fromDate, w.toDate) == (dt.date(2024, 1, 1), dt.date(2024, 3, 31))
    assert ac.monthsInWindow(w) == [dt.date(2024, 1, 1), dt.date(2024, 2, 1), dt.date(2024, 3, 1)]


def test_period_window_defaults_to_business_date_month():
    w = ac.periodWindow("", 0, dt.date(2024, 4, 15), reloadFullHistory=False)
    assert (w.fromDate, w.toDate) == (dt.date(2024, 4, 1), dt.date(2024, 4, 30))
    assert ac.accountingPeriodStart("1900-01", dt.date(2024, 4, 15)) == dt.date(2024, 4, 1)


def test_iso_week_and_month_helpers():
    assert ac.isoWeekStart(dt.date(2024, 5, 12)) == dt.date(2024, 5, 6)   # Sunday -> Monday
    assert ac.isoWeekStart(dt.date(2024, 5, 6)) == dt.date(2024, 5, 6)
    assert ac.monthEnd(dt.date(2024, 2, 10)) == dt.date(2024, 2, 29)
    assert ac.addMonths(dt.date(2024, 1, 31), 1) == dt.date(2024, 2, 29)
    assert ac.addMonths(dt.date(2024, 3, 15), -12) == dt.date(2023, 3, 15)


def test_parsers():
    assert ac.parseBool("true") and ac.parseBool("1") and not ac.parseBool("False") and ac.parseBool(None, True)
    assert ac.parseInt("7", 0) == 7 and ac.parseInt("x", 3) == 3
    assert ac.parseDecimal("1.5", 0) == 1.5 and ac.parseDecimal("", 2.0) == 2.0
    assert ac.parseDate("2024-05-01") == dt.date(2024, 5, 1) and ac.parseDate("", dt.date(2024, 1, 1)) == dt.date(2024, 1, 1)


def test_reconciliation_variance_matches_row_count_audit_formula():
    assert ac.reconciliationVariance(10, 8, 2) == 0
    ac.assertAggregateReconciliation(10, 8, 2, "Aggregate.X")
    try:
        ac.assertAggregateReconciliation(10, 7, 2, "Aggregate.X")
    except RuntimeError as exc:
        assert "variance 1" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected reconciliation failure")
