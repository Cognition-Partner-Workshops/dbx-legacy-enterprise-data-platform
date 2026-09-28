"""Test-only stand-in for the shared ``dbx_etl_common`` package (owned by session 00).

Only the surface the dq_quality helpers touch is faked: ``naming.table`` plus no-op
``control`` / ``params`` entry points. Never deploy this; the bundle installs the real wheel.
"""
from . import control, naming, params  # noqa: F401
