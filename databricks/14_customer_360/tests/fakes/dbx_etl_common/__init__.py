"""TEST-ONLY fake of the shared ``dbx_etl_common`` control layer (owned by session 00 under
``databricks/common/``). Only the functions called by ``c360_lib`` are stubbed; they record
calls so tests can assert lifecycle behaviour. Never ship this fake."""
from . import control, naming, params  # noqa: F401
