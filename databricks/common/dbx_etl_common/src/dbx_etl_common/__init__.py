"""dbx_etl_common - shared Databricks control layer for the WWI SSIS migration.

Public modules (interface contract shared by all migration sessions):

* :mod:`dbx_etl_common.control`  - batch / step / package lifecycle, logging, watermarks,
  configuration, reconciliation, data-quality, purge (mirrors ``sqlserver/control/procedures``).
* :mod:`dbx_etl_common.params`   - job parameter / widget parsing.
* :mod:`dbx_etl_common.naming`   - Unity Catalog naming contract (legacy object -> Delta table).

Supporting modules: :mod:`dbx_etl_common.schema` (control table specification),
:mod:`dbx_etl_common.bootstrap` (create schema / tables / seeds / views),
:mod:`dbx_etl_common.seeds`, :mod:`dbx_etl_common.views`,
:mod:`dbx_etl_common.expressions` (SSIS precedence-expression evaluator used by the master jobs).
"""
from . import control, naming, params  # noqa: F401

__version__ = "0.1.0"
__all__ = ["control", "naming", "params", "__version__"]
