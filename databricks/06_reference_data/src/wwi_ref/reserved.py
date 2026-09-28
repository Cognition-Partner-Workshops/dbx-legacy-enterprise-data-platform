"""Reserved (unknown) members: port of sqlserver/warehouse/dimensions/90_unknown_members.sql.

Every dimension gets the four reserved rows -1 Unknown, -2 Not Applicable, -3 Invalid and -9 Error
(Dimension.Date uses the 1900-01-01 / 1900-01-02 sentinel dates instead). The insert is a
WHEN NOT MATCHED MERGE on the surrogate key so re-running is a no-op, and existing reserved rows are
never rewritten (stewards may have relabelled them). Dimensions owned by other bundles are seeded
from their Delta schema (string NOT NULL -> label, numbers -> 0, booleans -> false, dates -> 1900-01-01)
and a failure there is a warning, not an error, exactly like the dynamic seeding loop in the legacy script.
"""
import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

from wwi_ref import schemas
from wwi_ref.schemas import HIGH_DATE, SENTINEL_UNKNOWN_DATE

RESERVED_MEMBERS = [
    (-1, "Unknown", "UNK"),
    (-2, "Not Applicable", "N/A"),
    (-3, "Invalid", "INV"),
    (-9, "Error", "ERR"),
]

EPOCH = datetime.datetime(1900, 1, 1)
HIGH = datetime.datetime(9999, 12, 31, 23, 59, 59)


def _defaultFor(field, key, label, code, lineageKey, keyCol):
    name = field.name
    dt = field.dataType
    if name == keyCol:
        return int(key)
    if name in ("IsCurrentRow", "IsReservedMember"):
        return True
    if name == "VersionNumber":
        return 1
    if name == "LineageKey":
        return lineageKey
    if name in ("ValidFrom", "EffectiveFrom"):
        return EPOCH
    if name in ("ValidTo", "EffectiveTo"):
        return HIGH
    if name.endswith("Code") and name not in ("RegionCode",) and isinstance(dt, T.StringType):
        return code if len(code) <= 20 else label
    if name == "RegionCode":
        return "ALL"
    if not field.nullable:
        if isinstance(dt, T.StringType):
            return label
        if isinstance(dt, T.BooleanType):
            return False
        if isinstance(dt, (T.IntegerType, T.LongType, T.ShortType, T.ByteType)):
            return int(key) if name.endswith("Id") else 0
        if isinstance(dt, T.DecimalType):
            from decimal import Decimal
            return Decimal(0)
        if isinstance(dt, (T.DoubleType, T.FloatType)):
            return 0.0
        if isinstance(dt, T.DateType):
            return EPOCH.date()
        if isinstance(dt, T.TimestampType):
            return EPOCH
        return None
    if isinstance(dt, T.StringType) and name.endswith("Name"):
        return label
    return None


def reservedRows(spark, tableFqn, keyCol, lineageKey):
    schema = spark.table(tableFqn).schema
    rows = []
    for key, label, code in RESERVED_MEMBERS:
        rows.append(tuple(_defaultFor(f, key, label, code, lineageKey, keyCol) for f in schema.fields))
    return spark.createDataFrame(rows, schema)


def seedReserved(spark, tableFqn, keyCol, lineageKey):
    """Insert the missing reserved rows; returns how many were inserted."""
    df = reservedRows(spark, tableFqn, keyCol, lineageKey)
    view = "wwi_ref_reserved_%s" % abs(hash(tableFqn))
    df.createOrReplaceTempView(view)
    cols = ", ".join(df.columns)
    result = spark.sql("""
        MERGE INTO %s AS t USING %s AS s ON t.%s = s.%s
        WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
    """ % (tableFqn, view, keyCol, keyCol, cols, ", ".join("s.%s" % c for c in df.columns)))
    spark.catalog.dropTempView(view)
    if "num_inserted_rows" in result.columns:
        return result.first()["num_inserted_rows"]
    return None


def seedDateSentinels(spark, tableFqn, lineageKey, batchId):
    from wwi_ref import datecalendar
    df = datecalendar.sentinelDateRows(spark, lineageKey, batchId)
    view = "wwi_ref_reserved_date"
    df.createOrReplaceTempView(view)
    cols = spark.table(tableFqn).columns
    result = spark.sql("""
        MERGE INTO %s AS t USING %s AS s ON t.DateKey = s.DateKey
        WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
    """ % (tableFqn, view, ", ".join(cols), ", ".join("s.%s" % c for c in cols)))
    spark.catalog.dropTempView(view)
    return result.first()["num_inserted_rows"] if "num_inserted_rows" in result.columns else None


def otherDimensions(spark, catalog):
    """gold.dim_* tables that exist in the catalog but are not maintained by this bundle."""
    owned = {tableName for _, tableName in (schemas.deltaName(n) for n in schemas.ownedDimensions())}
    try:
        tables = spark.sql("SHOW TABLES IN %s.gold" % catalog).collect()
    except Exception:
        return []
    found = []
    for row in tables:
        name = row["tableName"]
        if name.startswith("dim_") and name not in owned and not row["isTemporary"]:
            found.append(name)
    return sorted(found)


def guessKeyColumn(spark, tableFqn):
    fields = spark.table(tableFqn).schema.fields
    keys = [f.name for f in fields if f.name.endswith("Key") and isinstance(f.dataType, (T.IntegerType, T.LongType))]
    return keys[0] if keys else None


def seedAllReservedMembers(ctx):
    """90_unknown_members.sql for every dimension. Returns {legacy or delta name: inserted rows}."""
    spark = ctx.spark
    lineageKey = ctx.packageName
    results = {}
    for legacyName in schemas.ownedDimensions():
        tableFqn = ctx.table(legacyName)
        if legacyName == schemas.DATE_DIMENSION[0]:
            results[legacyName] = seedDateSentinels(spark, tableFqn, lineageKey, ctx.batchId)
            continue
        if legacyName == schemas.FISCAL_CALENDAR[0]:
            continue  # outrigger keyed by (CountryCode, Date); no reserved rows in the legacy script either
        results[legacyName] = seedReserved(spark, tableFqn, schemas.dimensionKeyColumn(legacyName), lineageKey)
    for name in otherDimensions(spark, ctx.catalog):
        tableFqn = "%s.gold.%s" % (ctx.catalog, name)
        keyCol = guessKeyColumn(spark, tableFqn)
        if keyCol is None:
            ctx.logWarning("no surrogate key column found on %s; reserved members not seeded" % tableFqn, errorCode="RESERVED_NO_KEY")
            continue
        try:
            results[tableFqn] = seedReserved(spark, tableFqn, keyCol, lineageKey)
        except Exception as exc:  # legacy: dynamic seeding failures are warnings
            ctx.logWarning("reserved members for %s could not be seeded: %s" % (tableFqn, str(exc).splitlines()[0]),
                           errorCode="RESERVED_SEED_FAILED")
    return results


def reservedMemberCount(spark, tableFqn, keyCol):
    return spark.table(tableFqn).where(F.col(keyCol) < 0).count()
