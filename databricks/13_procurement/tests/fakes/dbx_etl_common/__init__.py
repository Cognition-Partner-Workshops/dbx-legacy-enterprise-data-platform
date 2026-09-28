"""Minimal test-only fake of the shared dbx_etl_common contract (session 00 owns the real one).

Only the functions the 13_procurement notebooks call are implemented, with the same signatures,
recording calls in memory so tests can assert on control-framework usage. Never ship this.
"""

from . import control, naming, params  # noqa: F401
