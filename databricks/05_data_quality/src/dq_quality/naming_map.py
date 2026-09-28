"""Legacy SQL Server object names -> Unity Catalog Delta names.

Implements the naming contract from the migration brief:
raw.X -> bronze.raw_x, stg.X -> silver.stg_x, work.X -> silver.work_x,
err.X -> silver.err_x, ref.X -> silver.ref_x, Dimension.X -> gold.dim_x,
Fact.X -> gold.fact_x, Aggregate.X -> gold.agg_x, Report.X -> gold.rpt_x,
Integration.X -> silver.int_x, etl.X -> etl.x. The fully qualified name is
always produced by ``dbx_etl_common.naming.table`` so the catalog is never
hard-coded here.
"""
from __future__ import annotations

import re

from dbx_etl_common import naming

SCHEMA_MAP = {
    "raw": ("bronze", "raw_"),
    "stg": ("silver", "stg_"),
    "work": ("silver", "work_"),
    "err": ("silver", "err_"),
    "ref": ("silver", "ref_"),
    "dimension": ("gold", "dim_"),
    "fact": ("gold", "fact_"),
    "aggregate": ("gold", "agg_"),
    "report": ("gold", "rpt_"),
    "integration": ("silver", "int_"),
    "etl": ("etl", ""),
}

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def snakeCase(name: str) -> str:
    cleaned = name.strip().strip("[]").replace(" ", "_")
    return _CAMEL_BOUNDARY.sub("_", cleaned).lower()


def legacyToDelta(objectName: str) -> tuple[str, str]:
    """``'stg.OrderLine'`` -> ``('silver', 'stg_order_line')``."""
    schema, _, table = objectName.strip().partition(".")
    if not table:
        raise ValueError("expected <schema>.<object>, got %r" % objectName)
    key = schema.strip("[]").lower()
    if key not in SCHEMA_MAP:
        raise ValueError("no Delta mapping for legacy schema %r (%s)" % (schema, objectName))
    targetSchema, prefix = SCHEMA_MAP[key]
    return targetSchema, prefix + snakeCase(table)


def deltaTable(catalog: str, objectName: str) -> str:
    """``deltaTable('wwi_dev', 'stg.OrderLine')`` -> ``'wwi_dev.silver.stg_order_line'``."""
    schema, table = legacyToDelta(objectName)
    return naming.table(catalog, schema, table)


def controlTable(catalog: str, tableName: str) -> str:
    return naming.table(catalog, "etl", tableName)
