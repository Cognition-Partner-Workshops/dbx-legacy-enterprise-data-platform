"""Turn the legacy CREATE TABLE scripts under sqlserver/staging/tables into empty
Delta tables so the notebooks can be executed end to end against local Spark
with the exact raw / ref column names the estate defines."""

import os
import re

from pyspark.sql import types as T

HERE = os.path.dirname(os.path.abspath(__file__))
TABLES_DIR = os.path.join(HERE, "..", "..", "..", "sqlserver", "staging", "tables")

_TYPE_RULES = (
    (r"^N?VARCHAR|^N?CHAR|^UNIQUEIDENTIFIER|^N?TEXT|^VARBINARY", lambda m: T.StringType()),
    (r"^BIGINT", lambda m: T.LongType()),
    (r"^SMALLINT|^TINYINT", lambda m: T.ShortType()),
    (r"^INT", lambda m: T.IntegerType()),
    (r"^BIT", lambda m: T.BooleanType()),
    (r"^DECIMAL\((\d+),\s*(\d+)\)|^NUMERIC\((\d+),\s*(\d+)\)", lambda m: T.DecimalType(int(m.group(1) or m.group(3)), int(m.group(2) or m.group(4)))),
    (r"^DECIMAL|^NUMERIC|^MONEY", lambda m: T.DecimalType(18, 4)),
    (r"^FLOAT|^REAL", lambda m: T.DoubleType()),
    (r"^DATETIME|^SMALLDATETIME", lambda m: T.TimestampType()),
    (r"^DATE", lambda m: T.DateType()),
    (r"^TIME", lambda m: T.StringType()),
)


def sparkType(sqlType):
    for pattern, build in _TYPE_RULES:
        m = re.match(pattern, sqlType.upper())
        if m:
            return build(m)
    return T.StringType()


def snake(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def parseCreateTables(text):
    """{ 'raw.OracleCostCenter': StructType, ... } for every CREATE TABLE in the script."""
    out = {}
    for m in re.finditer(r"CREATE TABLE\s+(\w+)\.(\w+)\s*\((.*?)\n\s*\);", text, re.S):
        schema, table, body = m.group(1), m.group(2), m.group(3)
        fields = []
        for line in body.splitlines():
            line = line.split("--")[0].strip().rstrip(",")
            cm = re.match(r"^\[?([A-Za-z_][A-Za-z0-9_]*)\]?\s+([A-Z]+(?:\(\s*\d+(?:,\s*\d+)?\s*\)|\(MAX\))?)", line)
            if not cm or cm.group(1).upper() in ("CONSTRAINT", "PRIMARY", "UNIQUE", "INDEX", "CHECK", "FOREIGN"):
                continue
            fields.append(T.StructField(cm.group(1), sparkType(cm.group(2)), True))
        out["%s.%s" % (schema, table)] = T.StructType(fields)
    return out


def legacyTables():
    tables = {}
    for name in sorted(os.listdir(TABLES_DIR)):
        if name.endswith(".sql"):
            with open(os.path.join(TABLES_DIR, name), encoding="utf-8") as fh:
                tables.update(parseCreateTables(fh.read()))
    return tables


def deltaName(catalog, legacyName):
    schema, table = legacyName.split(".")
    layer = "bronze" if schema == "raw" else "silver"
    return "%s.%s.%s_%s" % (catalog, layer, schema, snake(table))


def createEmptyTables(spark, catalog, legacyNames=None, extra=None):
    """Create the bronze raw_* and silver ref_* tables (empty) plus any `extra`
    {'silver.ref_x': 'ColA string, ColB int'} the notebooks look up but the
    estate never scripted."""
    tables = legacyTables()
    wanted = [n for n in tables if n.startswith(("raw.", "ref."))] if legacyNames is None else legacyNames
    for legacy in wanted:
        spark.createDataFrame([], tables[legacy]).write.format("delta").mode("overwrite").saveAsTable(deltaName(catalog, legacy))
    for fqn, ddl in (extra or {}).items():
        spark.createDataFrame([], ddl).write.format("delta").mode("overwrite").saveAsTable("%s.%s" % (catalog, fqn))
