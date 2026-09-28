"""TEST-ONLY fake of the session-00 ``dbx_etl_common`` package.

Records the calls the 03_file_ingestion notebooks make so the tests can assert
on them. It implements only the names this bundle imports and is never
deployed; the bundle installs the real wheel published by session 00.
"""

from . import control, naming, params  # noqa: F401
