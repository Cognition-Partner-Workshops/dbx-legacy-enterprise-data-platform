"""Test-only stand-in for session 00's dbx_etl_common (interface contract only).

Never shipped: it exists so the 04_staging notebooks' helpers can be exercised
with local PySpark. Every call is recorded in `control.STATE` for assertions.
"""

from dbx_etl_common import control, naming, params

__all__ = ["control", "naming", "params"]
