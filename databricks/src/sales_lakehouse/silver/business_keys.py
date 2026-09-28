"""Business-key construction - lakehouse port of ``stg.ufn_SourceSystemKey``.

Legacy: sqlserver/staging/functions/stg.ufn_SourceSystemKey.sql (lines 37-59)
and the CONCAT pattern in stg.usp_AppendIncremental_OrderLine.sql (lines 62-80):
``<SYSTEM>|<KEY>`` for headers and ``<SYSTEM>|<KEY>|<LINE>`` for lines.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

DEFAULT_SOURCE_SYSTEM = "WWI_OLTP"

_REGIONAL_ORACLE_INSTANCES = ("ORA_ERP_NA", "ORA_ERP_EU", "ORA_ERP_AP")

# Bronze ``_source_system`` labels (bronze/source_table.py SOURCE_SYSTEM_LABELS)
# -> the legacy SourceSystemCode the staging keys were built with.
_BRONZE_LABEL_TO_LEGACY_CODE = {"SQLSERVER_WWI_OLTP": "WWI_OLTP", "ORACLE_WWIGERP": "ORA_ERP"}


def sourceSystemKey(sourceSystem: Column, naturalKey: Column, collapseRegionalInstances: bool = True) -> Column:
    """``stg.ufn_SourceSystemKey``: NULL when either part is blank."""
    system = F.upper(F.trim(F.coalesce(sourceSystem.cast("string"), F.lit(""))))
    key = F.upper(F.trim(F.coalesce(naturalKey.cast("string"), F.lit(""))))

    for bronzeLabel, legacyCode in _BRONZE_LABEL_TO_LEGACY_CODE.items():
        system = F.when(system == bronzeLabel, F.lit(legacyCode)).otherwise(system)

    if collapseRegionalInstances:
        # LEGACY QUIRK: regional Oracle instances and the web front end collapse
        # onto one system code (ufn_SourceSystemKey lines 44-50).
        system = F.when(system.isin(*_REGIONAL_ORACLE_INSTANCES), F.lit("ORA_ERP")).otherwise(system)
        system = F.when(system == "WWI_WEB", F.lit("WWI_OLTP")).otherwise(system)

    # LEGACY QUIRK: Oracle pads numeric identifiers to 10 digits, the OLTP does
    # not (ufn_SourceSystemKey lines 52-54).
    key = F.when((system == "ORA_ERP") & key.rlike("^[0-9]+$"), F.lpad(key, 10, "0")).otherwise(key)
    # LEGACY QUIRK: embedded pipes are escaped as '/' (lines 56-58).
    key = F.regexp_replace(key, r"\|", "/")

    return F.when((key == "") | (system == ""), F.lit(None).cast("string")).otherwise(
        F.concat(system, F.lit("|"), key)
    )


def lineBusinessKey(headerBusinessKey: Column, lineId: Column) -> Column:
    """``CONCAT(headerKey, '|', LTRIM(RTRIM(lineId)))`` (usp_AppendIncremental_OrderLine line 63)."""
    return F.when(
        headerBusinessKey.isNull() | lineId.isNull(), F.lit(None).cast("string")
    ).otherwise(F.concat(headerBusinessKey, F.lit("|"), F.trim(lineId.cast("string"))))
