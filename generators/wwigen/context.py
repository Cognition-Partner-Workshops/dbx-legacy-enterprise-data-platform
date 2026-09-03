"""Per-run shared objects that are expensive to build but cheap to share.

The context holds only things that are derived from the run configuration and
are therefore identical for every table: the weighted date sampler, the FX
rate surface, and the fiscal calendar. It never holds a population.
"""

from __future__ import annotations

import datetime

from . import regions, rng, timeline

# Base rates against USD at the start of the history window. The daily series
# is a deterministic random walk from these, so every table that converts an
# amount gets the same rate for the same day without storing the series.
BASE_RATE_USD = {
    "USD": 1.0, "CAD": 1.31, "MXN": 19.6, "GBP": 0.76, "EUR": 0.88,
    "AUD": 1.42, "NZD": 1.52, "SGD": 1.36, "JPY": 110.5,
}

CURRENCY_DECIMALS = {"JPY": 0, "MXN": 2}


class RunContext:
    """Shared, read-only derivations for one generator run."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.dates = timeline.DateSampler(cfg.history_start, cfg.history_end)
        self.currencies = tuple(sorted(BASE_RATE_USD))
        self.regions = regions.REGIONS

    # -- foreign exchange ------------------------------------------------

    def rate_to_usd(self, currency: str, when: datetime.date) -> float:
        """Deterministic daily rate: a bounded walk seeded by currency and day.

        There is no interpolation and no weekend handling - the ERP publishes
        a rate for every calendar day, including the ones the market was shut,
        which is exactly the behaviour the FX reconciliation has to cope with.
        """
        base = BASE_RATE_USD.get(currency, 1.0)
        if currency == "USD":
            return 1.0
        day_index = (when - self.cfg.history_start).days
        drift = 1.0 + (day_index / 3650.0) * 0.04
        wobble = 1.0 + (rng.unit(self.cfg.seed, "fx", currency, day_index) - 0.5) * 0.035
        return round(base * drift * wobble, 6)

    def cross_rate(self, from_currency: str, to_currency: str, when: datetime.date) -> float:
        if from_currency == to_currency:
            return 1.0
        return round(self.rate_to_usd(to_currency, when) / self.rate_to_usd(from_currency, when), 6)

    def reporting_amount(self, region: str, currency: str, amount: float,
                         when: datetime.date) -> float:
        """Restate a local amount under the region's own FX convention."""
        target = regions.REPORTING_CURRENCY[region]
        convention = regions.FX_CONVENTION[region]
        if convention == "MONTH_AVG":
            first = when.replace(day=1)
            rate = sum(self.cross_rate(currency, target,
                                       first + datetime.timedelta(days=offset))
                       for offset in (0, 9, 18, 27)) / 4.0
        elif convention == "EUR_TRIANGULATE":
            rate = (self.cross_rate(currency, "EUR", when)
                    * self.cross_rate("EUR", target, when))
        else:
            rate = self.cross_rate(currency, target, when)
        return round(amount * rate, 2)

    # -- calendar --------------------------------------------------------

    def fiscal_period(self, region: str, when: datetime.date) -> str:
        return regions.fiscal_period(region, when)
