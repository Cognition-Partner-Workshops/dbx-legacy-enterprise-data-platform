"""Reserved (unknown / not applicable / invalid / inferred pending / error) members.

Port of sqlserver/warehouse/dimensions/90_unknown_members.sql and
Integration.usp_EnsureUnknownMembers: every dimension gets the reserved band
before any fact load; the inserts are guarded so the call is re-runnable.
"""

from typing import Dict, List

from pyspark.sql import SparkSession

from wwi_dimensions import specs


def reservedMemberRows(dimensionSpec: specs.DimensionSpec) -> List[Dict[str, object]]:
    rows = []
    for keyValue, description in specs.RESERVED_MEMBERS:
        if keyValue == -4 and not dimensionSpec.supportsInferred:
            continue
        row: Dict[str, object] = {dimensionSpec.keyColumn: keyValue}
        for column, _ in dimensionSpec.attributeColumns:
            row[column] = None
        row[dimensionSpec.businessKeyColumn] = str(keyValue) if _isString(dimensionSpec, dimensionSpec.businessKeyColumn) else keyValue
        for column in dimensionSpec.labelColumns:
            row[column] = description
        for column, value in dimensionSpec.reservedDefaults.items():
            row[column] = keyValue if value == "KEY" else value
        for column, _ in dimensionSpec.metadataColumns:
            row[column] = None
        row["IsCurrentRow"] = True
        row["ValidFrom"] = specs.EPOCH_TIMESTAMP
        row["ValidTo"] = specs.OPEN_ENDED_TIMESTAMP
        row["LineageKey"] = 0
        if dimensionSpec.isType2:
            row["VersionNumber"] = 1
            row["EffectiveFrom"] = specs.EPOCH_TIMESTAMP
            row["EffectiveTo"] = specs.OPEN_ENDED_TIMESTAMP
            row["EffectiveFromDate"] = "1900-01-01"
            row["EffectiveSequence"] = 1
            row["IsInferredMember"] = False
        rows.append(row)
    return rows


def _isString(dimensionSpec: specs.DimensionSpec, column: str) -> bool:
    return dict(dimensionSpec.allColumns)[column].upper() == "STRING"


def ensureUnknownMembers(spark: SparkSession, catalog: str, dimensionSpec: specs.DimensionSpec) -> int:
    """Insert any missing reserved member; returns the number inserted."""
    table = dimensionSpec.fullTableName(catalog)
    columns = [name for name, _ in dimensionSpec.allColumns]
    ddl = ", ".join(f"`{name}` STRING" for name in columns)
    rows = [tuple(_asString(r.get(c)) for c in columns) for r in reservedMemberRows(dimensionSpec)]
    src = spark.createDataFrame(rows, ddl)
    for name, dtype in dimensionSpec.allColumns:
        src = src.withColumn(name, src[name].cast(dtype))
    src.createOrReplaceTempView("_reserved_members")
    before = spark.sql(f"SELECT COUNT(*) AS c FROM {table} WHERE {dimensionSpec.keyColumn} <= 0").collect()[0]["c"]
    insertCols = ", ".join(columns)
    insertVals = ", ".join(f"s.{c}" for c in columns)
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING _reserved_members AS s
          ON t.{dimensionSpec.keyColumn} = s.{dimensionSpec.keyColumn}
        WHEN NOT MATCHED THEN INSERT ({insertCols}) VALUES ({insertVals})
        """
    )
    after = spark.sql(f"SELECT COUNT(*) AS c FROM {table} WHERE {dimensionSpec.keyColumn} <= 0").collect()[0]["c"]
    return int(after - before)


def _asString(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
