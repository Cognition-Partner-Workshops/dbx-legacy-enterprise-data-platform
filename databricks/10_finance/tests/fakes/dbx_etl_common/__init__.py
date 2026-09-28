"""Test-only stand-in for session 00's dbx_etl_common (never used at runtime).

Implements just the surface the finance src modules import so they can be unit
tested locally. It records calls instead of writing to etl.* tables.
"""
from . import control, naming, params  # noqa: F401
