"""Test-only stand-in for the shared dbx_etl_common package (owned by session 00).

Only the functions the 02_sqlserver_extract notebooks call are implemented, with the
contract signatures; state is kept in memory so the tests can assert on the calls.
"""
