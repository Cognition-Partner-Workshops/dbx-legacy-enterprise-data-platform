"""Test-only fake of the session-00 dbx_etl_common contract (never shipped to a workspace).

Implements exactly the functions the REF_Load_* notebooks call, records every call in-memory and
persists row counts / rejects to local etl.* Delta tables so tests can assert on them.
"""
