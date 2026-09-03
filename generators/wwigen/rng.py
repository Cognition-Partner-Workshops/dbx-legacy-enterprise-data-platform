"""Deterministic randomness primitives.

Everything in the generation suite draws from this module. Nothing here ever
consults the clock, the process id, the environment, or Python's global RNG,
so a given seed reproduces byte-identical output on any machine and any
CPython 3 build.

Two mechanisms are provided:

``substream(seed, *parts)``
    A ``random.Random`` seeded from a hash of the run seed plus a label. Used
    for sequential row generation where the row order inside a table is fixed.

``unit(seed, *parts)`` / ``pick(...)`` / ``integer(...)``
    Stateless, position-addressable draws. These let any table recompute an
    entity's attributes from its ordinal alone, which is what makes tables
    independently runnable and resumable without materialising the whole
    estate in memory.

Note the deliberate avoidance of ``hash()``: it is salted per process for
strings, so it would break reproducibility across runs.
"""

from __future__ import annotations

import hashlib
import random
import struct

_DIGEST_BYTES = 16


def _material(seed: int, parts) -> bytes:
    chunks = [b"wwigen", str(int(seed)).encode("ascii")]
    for part in parts:
        if isinstance(part, bytes):
            chunks.append(part)
        else:
            chunks.append(str(part).encode("utf-8"))
    return b"\x1f".join(chunks)


def stable_hash(seed: int, *parts) -> int:
    """A stable, process-independent 128-bit integer for a labelled position."""
    digest = hashlib.blake2b(_material(seed, parts), digest_size=_DIGEST_BYTES).digest()
    return int.from_bytes(digest, "big")


def substream(seed: int, *parts) -> random.Random:
    """A private ``random.Random`` for one named sequence."""
    return random.Random(stable_hash(seed, *parts))


def unit(seed: int, *parts) -> float:
    """A stateless uniform draw in [0, 1) for a labelled position."""
    digest = hashlib.blake2b(_material(seed, parts), digest_size=8).digest()
    (raw,) = struct.unpack(">Q", digest)
    return (raw >> 11) * (1.0 / (1 << 53))


def integer(seed: int, low: int, high: int, *parts) -> int:
    """A stateless integer draw in [low, high] for a labelled position."""
    if high < low:
        raise ValueError("high must not be below low")
    span = high - low + 1
    return low + stable_hash(seed, *parts) % span


def pick(seed: int, choices, *parts):
    """A stateless choice from an indexable sequence."""
    if not choices:
        raise ValueError("cannot pick from an empty sequence")
    return choices[stable_hash(seed, *parts) % len(choices)]


def weighted_pick(seed: int, choices, weights, *parts):
    """A stateless weighted choice. ``weights`` must be positive numbers."""
    total = float(sum(weights))
    if total <= 0:
        raise ValueError("weights must sum above zero")
    threshold = unit(seed, *parts) * total
    running = 0.0
    for choice, weight in zip(choices, weights):
        running += weight
        if threshold < running:
            return choice
    return choices[-1]


def chance(seed: int, probability: float, *parts) -> bool:
    """A stateless Bernoulli trial."""
    return unit(seed, *parts) < probability


def jitter(seed: int, centre: float, spread: float, *parts) -> float:
    """A stateless symmetric perturbation around ``centre``."""
    return centre + (unit(seed, *parts) * 2.0 - 1.0) * spread
