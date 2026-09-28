"""Test-only fake of the session-00 ``dbx_etl_common`` interface contract.

Only the functions the Oracle extract package calls are implemented, with the
exact signatures of the contract; state is kept in memory so the runner can be
exercised with local PySpark. Never shipped - the real package comes from
databricks/common (session 00).
"""
from dbx_etl_common import control, naming, params  # noqa: F401
