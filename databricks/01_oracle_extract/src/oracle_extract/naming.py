"""Legacy object name -> Unity Catalog name helpers (naming contract of the migration)."""
import re

from dbx_etl_common import naming

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

_SCHEMA_MAP = {
    "raw": ("bronze", "raw_"),
    "stg": ("silver", "stg_"),
    "work": ("silver", "work_"),
    "err": ("silver", "err_"),
    "ref": ("silver", "ref_"),
}


def snakeCase(name: str) -> str:
    return _CAMEL_BOUNDARY.sub("_", name.replace(" ", "_")).lower()


def deltaName(legacyTable: str):
    """'raw.OracleCustomerMaster' -> ('bronze', 'raw_oracle_customer_master')."""
    schema, table = legacyTable.split(".", 1)
    ucSchema, prefix = _SCHEMA_MAP[schema.lower()]
    return ucSchema, prefix + snakeCase(table)


def deltaTable(catalog: str, legacyTable: str) -> str:
    """'raw.OracleCustomerMaster' -> '<catalog>.bronze.raw_oracle_customer_master'."""
    ucSchema, table = deltaName(legacyTable)
    return naming.table(catalog, ucSchema, table)
