"""Transaction timing: seasonality, weekday shape, period-end spikes, lateness.

Transaction dates are not uniform. This module produces the shape a wholesale
distributor actually has, deterministically and without materialising a date
histogram for the whole history window:

* a strong Q4 peak and a January trough (``MONTH_WEIGHT``);
* a weekday-heavy week with a near-dead Sunday (``WEEKDAY_WEIGHT``);
* month-end and quarter-end spikes as orders are pulled into the period;
* a holiday-shutdown dip in the last week of December;
* late-arriving records, where the business event happened days before the
  row reaches the feed, and out-of-order timestamps within a batch.

Dates are drawn by inverse-transform over a per-day weight, using a bucketed
cumulative table built once per run. That keeps the draw O(log n) with no
per-row rejection loop, so ``large`` stays bounded in both time and memory.
"""

from __future__ import annotations

import bisect
import datetime

from . import rng

MONTH_WEIGHT = (0.62, 0.71, 0.88, 0.92, 0.97, 1.02,
                0.95, 0.98, 1.08, 1.21, 1.38, 1.28)

WEEKDAY_WEIGHT = (1.18, 1.12, 1.08, 1.06, 0.98, 0.42, 0.11)

# Trend: the business grew, then flattened after the 2021 supply squeeze.
YEAR_TREND = {2018: 0.78, 2019: 0.86, 2020: 0.72, 2021: 0.94,
              2022: 1.05, 2023: 1.12, 2024: 1.16}


def day_weight(day: datetime.date) -> float:
    weight = MONTH_WEIGHT[day.month - 1] * WEEKDAY_WEIGHT[day.weekday()]
    weight *= YEAR_TREND.get(day.year, 1.0)
    next_day = day + datetime.timedelta(days=1)
    is_month_end = next_day.month != day.month
    days_to_month_end = 0
    probe = day
    while probe.month == day.month and days_to_month_end < 6:
        probe += datetime.timedelta(days=1)
        if probe.month != day.month:
            break
        days_to_month_end += 1
    if days_to_month_end <= 2 or is_month_end:
        weight *= 1.45
        if day.month in (3, 6, 9, 12):
            weight *= 1.35
    if day.month == 12 and day.day >= 24:
        weight *= 0.28
    if day.month == 1 and day.day <= 2:
        weight *= 0.2
    return weight


class DateSampler:
    """Inverse-transform sampler over the weighted history window."""

    def __init__(self, start: datetime.date, end: datetime.date):
        self.start = start
        self.end = end
        self.days = (end - start).days + 1
        cumulative = []
        running = 0.0
        for offset in range(self.days):
            running += day_weight(start + datetime.timedelta(days=offset))
            cumulative.append(running)
        self._cumulative = cumulative
        self._total = running

    def date_for(self, seed: int, *parts) -> datetime.date:
        target = rng.unit(seed, *parts) * self._total
        offset = bisect.bisect_left(self._cumulative, target)
        if offset >= self.days:
            offset = self.days - 1
        return self.start + datetime.timedelta(days=offset)

    def datetime_for(self, seed: int, *parts) -> datetime.datetime:
        day = self.date_for(seed, *parts)
        # Order entry clusters in the working day with a post-lunch second peak.
        minute_of_day = rng.weighted_pick(
            seed,
            (8 * 60, 9 * 60, 10 * 60, 11 * 60, 13 * 60, 14 * 60, 15 * 60, 16 * 60, 19 * 60, 2 * 60),
            (0.6, 1.3, 1.5, 1.2, 1.4, 1.35, 1.1, 0.8, 0.3, 0.05),
            "tod", *parts)
        minute_of_day += rng.stable_hash(seed, "tod-min", *parts) % 60
        second = rng.stable_hash(seed, "tod-sec", *parts) % 60
        return datetime.datetime(day.year, day.month, day.day,
                                 minute_of_day // 60 % 24, minute_of_day % 60, second)


def lateness_days(seed: int, late_rate: float, *parts) -> int:
    """How many days after the business event the row actually arrives."""
    if not rng.chance(seed, late_rate, "late", *parts):
        return 0
    bucket = rng.weighted_pick(seed, (1, 2, 3, 7, 14, 45, 190), (34, 22, 14, 12, 9, 6, 3),
                               "late-bucket", *parts)
    return bucket + rng.stable_hash(seed, "late-jitter", *parts) % 3


def out_of_order_shift(seed: int, rate: float, *parts) -> int:
    """Seconds to shift a timestamp backwards so a batch is not monotonic."""
    if not rng.chance(seed, rate, "ooo", *parts):
        return 0
    return -(30 + rng.stable_hash(seed, "ooo-shift", *parts) % 5400)


def add_months(day: datetime.date, months: int) -> datetime.date:
    month_index = day.month - 1 + months
    year = day.year + month_index // 12
    month = month_index % 12 + 1
    last_day = [31, 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28,
                31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]
    return datetime.date(year, month, min(day.day, last_day))


def business_days_after(day: datetime.date, days: int) -> datetime.date:
    result = day
    remaining = days
    while remaining > 0:
        result += datetime.timedelta(days=1)
        if result.weekday() < 5:
            remaining -= 1
    return result


def attribute_change_dates(seed: int, entity_kind: str, ordinal: int,
                           start: datetime.date, end: datetime.date, max_changes: int):
    """Dates on which a dimension attribute changed, for SCD Type 2 history.

    Most entities never change. A minority change once or twice; a handful of
    heavily-managed accounts change often. Returned ascending.
    """
    change_count = rng.weighted_pick(seed, tuple(range(max_changes + 1)),
                                     tuple(max(1, 40 // (index + 1) ** 2) for index in range(max_changes + 1)),
                                     "chg-count", entity_kind, ordinal)
    span = max((end - start).days, 1)
    offsets = sorted({30 + rng.stable_hash(seed, "chg-at", entity_kind, ordinal, index) % max(span - 60, 1)
                      for index in range(change_count)})
    return [start + datetime.timedelta(days=offset) for offset in offsets]
