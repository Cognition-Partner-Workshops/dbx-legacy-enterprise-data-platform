"""Create the ``etl`` control schema: tables (from :mod:`schema`), seeds and views.

Uses the Delta ``DeltaTableBuilder`` API so identity columns, generated columns, comments,
CHECK constraints and column defaults are created identically on Databricks and on local
delta-spark (whose SQL parser does not accept ``GENERATED ALWAYS AS IDENTITY``).
``renderDdl`` produces the equivalent Databricks SQL for review / manual deployment; the two
are kept in sync by ``databricks/common/tests/test_schema_render.py``.
"""
from __future__ import annotations

from typing import Any, Iterable, List, Optional

from . import seeds as _seeds
from . import views as _views
from .naming import ETL, controlTable, table
from .schema import TABLES, Column, Table

DEFAULTS_PROPERTY = ("delta.feature.allowColumnDefaults", "supported")


def _sparkType(dataType: str) -> Any:
    from pyspark.sql.types import _parse_datatype_string
    return _parse_datatype_string(dataType.lower() if dataType.upper() != "STRING" else "string")


def createSchema(spark: Any, catalog: str, schema: str = ETL, comment: Optional[str] = None) -> None:
    c = f" COMMENT '{comment}'" if comment else ""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}{c}")


def tableExists(spark: Any, fullName: str) -> bool:
    return bool(spark.catalog.tableExists(fullName))


def createTable(spark: Any, catalog: str, spec: Table) -> bool:
    """Create one control table if it does not exist. Returns True when it was created."""
    from delta.tables import DeltaTable, IdentityGenerator

    fullName = controlTable(catalog, spec.name)
    if tableExists(spark, fullName):
        return False
    # DeltaTableBuilder.tableName only parses ``schema.table`` on OSS Delta, so pin the catalog for the call.
    previousCatalog = spark.catalog.currentCatalog()
    spark.catalog.setCurrentCatalog(catalog)
    builder = DeltaTable.createIfNotExists(spark).tableName(f"{ETL}.{spec.name}")
    for col in spec.columns:
        kwargs = {}
        if col.identity:
            kwargs["generatedAlwaysAs"] = IdentityGenerator()
        elif col.generatedAs:
            kwargs["generatedAlwaysAs"] = col.generatedAs
        if col.comment:
            kwargs["comment"] = col.comment
        builder = builder.addColumn(col.name, _sparkType(col.dataType), nullable=col.nullable, **kwargs)
    builder = builder.comment(spec.comment).property(*DEFAULTS_PROPERTY)
    try:
        builder.execute()
    finally:
        spark.catalog.setCurrentCatalog(previousCatalog)
    for col in spec.columns:
        if col.default is not None:
            spark.sql(f"ALTER TABLE {fullName} ALTER COLUMN {col.name} SET DEFAULT {col.default}")
    for name, expr in spec.checks.items():
        spark.sql(f"ALTER TABLE {fullName} ADD CONSTRAINT {name} CHECK ({expr})")
    return True


def createControlTables(spark: Any, catalog: str, tables: Iterable[Table] = TABLES) -> List[str]:
    """Create every control table (idempotent). Returns the names that were newly created."""
    created = []
    for spec in tables:
        if createTable(spark, catalog, spec):
            created.append(spec.name)
    return created


def bootstrap(spark: Any, catalog: str, withSeeds: bool = True, withViews: bool = True) -> List[str]:
    """Schema + tables + seeds + views. Safe to re-run."""
    createSchema(spark, catalog, ETL, "WWI ETL control framework (migrated from sqlserver/control)")
    created = createControlTables(spark, catalog)
    if withSeeds:
        _seeds.applySeeds(spark, catalog)
    if withViews:
        _views.createViews(spark, catalog)
    return created


# ----------------------------------------------------------------------------- SQL rendering

def _renderColumn(col: Column) -> str:
    parts = [col.name, col.dataType]
    if col.identity:
        parts.append("GENERATED ALWAYS AS IDENTITY")
    elif col.generatedAs:
        parts.append(f"GENERATED ALWAYS AS ({col.generatedAs})")
    if not col.nullable and not col.identity:
        parts.append("NOT NULL")
    if col.default is not None:
        parts.append(f"DEFAULT {col.default}")
    if col.comment:
        parts.append("COMMENT '" + col.comment.replace("'", "''") + "'")
    return " ".join(parts)


def renderTableDdl(spec: Table, catalog: str = "${catalog}") -> str:
    fullName = controlTable(catalog, spec.name)
    cols = ",\n".join("    " + _renderColumn(c) for c in spec.columns)
    checks = "".join(f",\n    CONSTRAINT {n} CHECK ({e})" for n, e in spec.checks.items())
    comment = spec.comment.replace("'", "''")
    return (
        f"CREATE TABLE IF NOT EXISTS {fullName}\n(\n{cols}{checks}\n)\nUSING DELTA\n"
        f"COMMENT '{comment}'\nTBLPROPERTIES ('{DEFAULTS_PROPERTY[0]}' = '{DEFAULTS_PROPERTY[1]}');"
    )


def renderDdl(catalog: str = "${catalog}", tables: Iterable[Table] = TABLES) -> str:
    header = (
        "-- Delta DDL for the WWI ETL control framework (schema etl).\n"
        "-- GENERATED from dbx_etl_common.schema by databricks/common/tools/render_sql.py - do not edit by hand.\n"
        "-- Legacy sources: sqlserver/control/02_tables_control_framework.sql, 04_tables_data_quality.sql,\n"
        "--                 06_tables_reconciliation.sql, 07_tables_operations.sql (+ tables created inline by procedures).\n"
        "-- ${catalog} is the bundle variable (default wwi_${bundle.target}); PK/UQ/FK/index intent is in the table comments.\n\n"
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{ETL};\n\n"
    )
    return header + "\n\n".join(renderTableDdl(t, catalog) for t in tables) + "\n"


def renderSeedSql(catalog: str = "${catalog}") -> str:
    header = (
        "-- Idempotent seed data for the WWI ETL control framework.\n"
        "-- GENERATED from dbx_etl_common.seeds by databricks/common/tools/render_sql.py - do not edit by hand.\n"
        "-- Legacy sources: sqlserver/control/03_seed_control_data.sql, 05_seed_data_quality_rules.sql.\n\n"
    )
    return header + "\n\n".join(_seeds.renderMerge(s, catalog) for s in _seeds.SEEDS) + "\n"


def renderViewSql(catalog: str = "${catalog}") -> str:
    header = (
        "-- Operational views for the WWI ETL control framework (etl.v_*).\n"
        "-- GENERATED from dbx_etl_common.views by databricks/common/tools/render_sql.py - do not edit by hand.\n"
        "-- Legacy source: sqlserver/control/views/etl.OperationalViews.sql.\n\n"
    )
    return header + _views.renderAll(catalog) + "\n"


__all__ = [
    "bootstrap", "createSchema", "createControlTables", "createTable", "renderDdl", "renderSeedSql",
    "renderViewSql", "renderTableDdl", "table",
]
