"""Activity skew: a few accounts and products carry most of the volume.

Uniform picks over the customer base would produce data no real warehouse has
ever seen. These helpers give the estate its Pareto shape:

* ``pareto_ordinal`` maps a uniform draw onto an entity ordinal such that
  roughly the top fifth of the population carries about four fifths of the
  transactions, with the exponent controlling how brutal the concentration is;
* ``dominant_ordinal`` reserves a handful of house accounts that alone account
  for a configurable slice of everything, the way a distributor's two biggest
  grocery chains do;
* ``long_tail_quantity`` produces order quantities with the usual
  small-numbers-mostly, occasional-pallet distribution.
"""

from __future__ import annotations

from . import rng

# How many entities are treated as dominant accounts, and their combined share.
DOMINANT_COUNT_DIVISOR = 2000
DOMINANT_MIN = 3
DOMINANT_MAX = 40
DOMINANT_SHARE = 0.17

PARETO_EXPONENT = 3.2


def dominant_count(population: int) -> int:
    return max(DOMINANT_MIN, min(DOMINANT_MAX, population // DOMINANT_COUNT_DIVISOR or DOMINANT_MIN))


def pareto_ordinal(seed: int, population: int, *parts) -> int:
    """Pick an entity ordinal in [0, population) with heavy head concentration."""
    if population <= 1:
        return 0
    if rng.chance(seed, DOMINANT_SHARE, "dom", *parts):
        return dominant_ordinal(seed, population, *parts)
    draw = rng.unit(seed, "pareto", *parts)
    ordinal = int(population * (draw ** PARETO_EXPONENT))
    return min(ordinal, population - 1)


def dominant_ordinal(seed: int, population: int, *parts) -> int:
    """Pick one of the reserved house accounts."""
    count = dominant_count(population)
    if population <= count:
        return rng.stable_hash(seed, "dom-small", *parts) % max(population, 1)
    # House accounts are the lowest ordinals: they were keyed first, in 1998.
    return rng.stable_hash(seed, "dom-pick", *parts) % count


def is_dominant(population: int, ordinal: int) -> bool:
    return ordinal < dominant_count(population)


def activity_weight(population: int, ordinal: int) -> float:
    """Relative transaction weight of an entity, for reporting and sanity checks."""
    if is_dominant(population, ordinal):
        return 40.0
    if population <= 1:
        return 1.0
    position = (ordinal + 1) / float(population)
    return max(0.02, position ** -(1.0 / PARETO_EXPONENT) / 8.0)


def long_tail_quantity(seed: int, *parts) -> int:
    """Line quantities: mostly single digits, occasionally a pallet."""
    bucket = rng.weighted_pick(seed, (1, 2, 3, 6, 12, 24, 48, 144, 480),
                               (30, 22, 15, 12, 9, 6, 3, 2, 1), "qty", *parts)
    if bucket >= 48:
        return bucket + (rng.stable_hash(seed, "qty-jit", *parts) % 12) * 6
    return bucket


def line_count(seed: int, average: float, *parts) -> int:
    """Lines per document, skewed low with a heavy tail on big accounts."""
    base = max(1, int(average))
    draw = rng.unit(seed, "lines", *parts)
    if draw > 0.97:
        return base + 6 + rng.stable_hash(seed, "lines-tail", *parts) % 30
    if draw > 0.80:
        return base + 2 + rng.stable_hash(seed, "lines-mid", *parts) % 3
    if draw < 0.35:
        return max(1, base - 1)
    return base
