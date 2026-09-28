"""Shared helpers for the WWI_Staging (04_staging) notebooks.

Only cross-package plumbing lives here: the Spark equivalents of the SSIS
derived-column idioms and the stg.ufn_* scalar functions, reference-table
lookups shared by several packages, and the per-package run wrapper that
binds a notebook to dbx_etl_common. Package-specific business rules stay in
the notebook that owns them.
"""

from stg_common import expressions, loader, refs

__all__ = ["expressions", "loader", "refs"]
