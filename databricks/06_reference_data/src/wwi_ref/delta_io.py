"""Idempotent Delta writers for the reference loads.

The legacy packages TRUNCATE each dimension and reload it; here the same end state is reached with a
MERGE so that surrogate keys stay stable across runs, reserved members (key < 0) survive, and a re-run
for the same BatchId is a no-op. Surrogate keys are assigned as max(existing) + row_number() instead
of identity columns so the logic runs identically on Databricks and on local OSS Delta in pytest.
"""
import uuid
from collections import namedtuple

from pyspark.sql import Window
from pyspark.sql import functions as F

from wwi_ref.schemas import HIGH_DATE

MergeCounts = namedtuple("MergeCounts", ["inserted", "updated", "deleted"])


def _tempView(df):
    name = "wwi_ref_%s" % uuid.uuid4().hex
    df.createOrReplaceTempView(name)
    return name


def _quoted(value):
    return "'%s'" % str(value).replace("'", "''")


def changeHash(columns):
    """Port of build_reference_packages.hash_expression: pipe-delimited, upper-cased, trimmed strings,
    hashed with SHA-256 (legacy stores HASHBYTES over the same concatenation in Row Hash Type 1/2)."""
    parts = [F.upper(F.trim(F.coalesce(F.col(c).cast("string"), F.lit("")))) for c in columns]
    return F.sha2(F.concat_ws("|", *parts), 256)


def mergeMetrics(result):
    """Delta returns num_inserted_rows / num_updated_rows / num_deleted_rows from MERGE; older engines
    return an empty frame, in which case counts are unknown (None)."""
    cols = set(result.columns)
    if not {"num_inserted_rows", "num_updated_rows", "num_deleted_rows"} <= cols:
        return MergeCounts(None, None, None)
    row = result.first()
    if row is None:
        return MergeCounts(None, None, None)
    return MergeCounts(row["num_inserted_rows"], row["num_updated_rows"], row["num_deleted_rows"])


def conformToTable(spark, targetFqn, df):
    """Project ``df`` onto the target's columns (missing -> NULL) and cast every column to the target
    type so MERGE / append never trip over INT vs BIGINT or STRING vs DATE differences."""
    schema = spark.table(targetFqn).schema
    out = df
    for field in schema.fields:
        if field.name not in out.columns:
            out = out.withColumn(field.name, F.lit(None).cast(field.dataType))
    return out.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def maxPositiveKey(target, keyCol):
    """Highest surrogate key in use, ignoring the reserved members (-1/-2/-3/-9) so the first real row gets key 1."""
    return target.where(F.col(keyCol) > 0).agg(F.max(F.col(keyCol))).first()[0] or 0


def assignSurrogateKeys(spark, targetFqn, df, keyCol, businessKeyCols, filterSql=None):
    """Reuse the existing surrogate key for known business keys; new business keys get
    max(existing key) + row_number() ordered by the business key (deterministic)."""
    target = spark.table(targetFqn)
    if filterSql:
        target = target.where(filterSql)
    existing = target.select(keyCol, *businessKeyCols)
    maxKey = maxPositiveKey(target, keyCol)
    joined = df.join(existing, on=businessKeyCols, how="left")
    isNew = F.col(keyCol).isNull()
    ranked = joined.withColumn("_newRank", F.when(isNew, F.row_number().over(
        Window.partitionBy(isNew).orderBy(*[F.col(c) for c in businessKeyCols]))))
    return (ranked
            .withColumn(keyCol, F.when(isNew, F.lit(maxKey) + F.col("_newRank")).otherwise(F.col(keyCol)).cast("bigint"))
            .drop("_newRank"))


def publishScd1(spark, targetFqn, df, keyCol, businessKeyCols, lineageKey, batchId, hashCol="ChangeHash",
                deleteMissing=True):
    """Dimension.X full-refresh (legacy TRUNCATE + insert) expressed as an SCD1 MERGE.

    * matched & hash changed  -> UPDATE every non-key column, LastLoadBatchId
    * not matched             -> INSERT with a new surrogate key, ValidFrom = now, ValidTo = 9999-12-31
    * in target, not in source, owned by this lineage and not a reserved member -> DELETE (truncate semantics)
    """
    targetCols = spark.table(targetFqn).columns
    keyed = assignSurrogateKeys(spark, targetFqn, df, keyCol, businessKeyCols)
    staged = (keyed
              .withColumn("LineageKey", F.lit(lineageKey))
              .withColumn("LastLoadBatchId", F.lit(batchId).cast("bigint"))
              .withColumn("ValidFrom", F.current_timestamp())
              .withColumn("ValidTo", F.to_timestamp(F.lit(HIGH_DATE))))
    staged = conformToTable(spark, targetFqn, staged)
    view = _tempView(staged)
    on = " AND ".join("t.%s = s.%s" % (c, c) for c in businessKeyCols)
    updateCols = [c for c in targetCols if c not in businessKeyCols + [keyCol, "ValidFrom"]]
    updateSet = ", ".join("t.%s = s.%s" % (c, c) for c in updateCols)
    insertCols = ", ".join(targetCols)
    insertVals = ", ".join("s.%s" % c for c in targetCols)
    sql = [
        "MERGE INTO %s AS t USING %s AS s ON %s" % (targetFqn, view, on),
        "WHEN MATCHED AND (t.%s IS NULL OR t.%s <> s.%s) THEN UPDATE SET %s" % (hashCol, hashCol, hashCol, updateSet),
        "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (insertCols, insertVals),
    ]
    if deleteMissing:
        sql.append("WHEN NOT MATCHED BY SOURCE AND t.%s > 0 AND t.LineageKey = %s THEN DELETE"
                   % (keyCol, _quoted(lineageKey)))
    result = spark.sql("\n".join(sql))
    spark.catalog.dropTempView(view)
    return mergeMetrics(result)


def publishScd2(spark, targetFqn, df, keyCol, businessKeyCols, lineageKey, batchId, effectiveFrom,
                hashCol="RowHashType2"):
    """Type-2 versioning (Dimension.Cost Center): changed or new business keys open a new version,
    the previous current version is closed at effectiveFrom; business keys that disappeared from the
    source are closed as well. Re-running with the same source is a no-op."""
    targetCols = spark.table(targetFqn).columns
    current = spark.table(targetFqn).where("IsCurrentRow = true AND %s > 0" % keyCol)
    currentKeyed = current.select(*businessKeyCols, F.col(keyCol).alias("_existingKey"),
                                  F.col(hashCol).alias("_existingHash"), F.col("VersionNumber").alias("_existingVersion"))
    joined = df.join(currentKeyed, on=businessKeyCols, how="full")
    isNew = F.col("_existingKey").isNull()
    isGone = F.col(hashCol).isNull()
    isChanged = (~isNew) & (~isGone) & (F.col("_existingHash") != F.col(hashCol))

    toInsert = joined.where(isNew | isChanged).drop("_existingKey", "_existingHash")
    toInsert = toInsert.withColumn("VersionNumber", F.coalesce(F.col("_existingVersion") + 1, F.lit(1))).drop("_existingVersion")
    maxKey = maxPositiveKey(spark.table(targetFqn), keyCol)
    window = Window.orderBy(*[F.col(c) for c in businessKeyCols])
    toInsert = (toInsert
                .withColumn(keyCol, (F.lit(maxKey) + F.row_number().over(window)).cast("bigint"))
                .withColumn("EffectiveFrom", F.lit(effectiveFrom).cast("timestamp"))
                .withColumn("EffectiveTo", F.to_timestamp(F.lit(HIGH_DATE)))
                .withColumn("IsCurrentRow", F.lit(True))
                .withColumn("LineageKey", F.lit(lineageKey))
                .withColumn("LastLoadBatchId", F.lit(batchId).cast("bigint"))
                .withColumn("ValidFrom", F.current_timestamp())
                .withColumn("ValidTo", F.to_timestamp(F.lit(HIGH_DATE)))
                .withColumn("_mergeKey", F.lit(None).cast("bigint")))
    mergeKey = toInsert.select("_mergeKey", *businessKeyCols)
    toInsert = conformToTable(spark, targetFqn, toInsert).join(
        mergeKey, businessKeyCols, "inner").select("_mergeKey", *targetCols)

    toExpire = joined.where(isChanged | isGone).select(F.col("_existingKey").cast("bigint").alias("_mergeKey"))
    for f in spark.table(targetFqn).schema.fields:
        toExpire = toExpire.withColumn(f.name, F.lit(None).cast(f.dataType))
    staged = toInsert.unionByName(toExpire.select("_mergeKey", *targetCols))
    view = _tempView(staged)
    insertCols = ", ".join(targetCols)
    insertVals = ", ".join("s.%s" % c for c in targetCols)
    result = spark.sql("""
        MERGE INTO %s AS t USING %s AS s ON t.%s = s._mergeKey
        WHEN MATCHED AND t.IsCurrentRow = true THEN UPDATE SET
            t.IsCurrentRow = false,
            t.EffectiveTo = CAST(%s AS TIMESTAMP) - INTERVAL 1 SECOND,
            t.ValidTo = current_timestamp(),
            t.LastLoadBatchId = %d
        WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
    """ % (targetFqn, view, keyCol, _quoted(effectiveFrom), batchId, insertCols, insertVals))
    spark.catalog.dropTempView(view)
    return mergeMetrics(result)


def mergeReference(spark, targetFqn, df, keyCols, updateCols=None, insertOnly=False, notMatchedBySourceSql=None):
    """Generic upsert used by the ref.usp_Load* ports.

    ``notMatchedBySourceSql`` (e.g. ``"t.MaintainedByName = 'REF_Load'"``) restricts which target rows
    that vanished from the source are deleted, so steward-maintained rows are preserved.
    """
    targetCols = spark.table(targetFqn).columns
    staged = conformToTable(spark, targetFqn, df)
    view = _tempView(staged)
    on = " AND ".join("t.%s <=> s.%s" % (c, c) for c in keyCols)
    updateCols = updateCols or [c for c in targetCols if c not in keyCols]
    sql = ["MERGE INTO %s AS t USING %s AS s ON %s" % (targetFqn, view, on)]
    if not insertOnly and updateCols:
        changed = " OR ".join("NOT (t.%s <=> s.%s)" % (c, c) for c in updateCols)
        sql.append("WHEN MATCHED AND (%s) THEN UPDATE SET %s" % (
            changed, ", ".join("t.%s = s.%s" % (c, c) for c in updateCols)))
    sql.append("WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (
        ", ".join(targetCols), ", ".join("s.%s" % c for c in targetCols)))
    if notMatchedBySourceSql:
        sql.append("WHEN NOT MATCHED BY SOURCE AND (%s) THEN DELETE" % notMatchedBySourceSql)
    result = spark.sql("\n".join(sql))
    spark.catalog.dropTempView(view)
    return mergeMetrics(result)


def nextSequence(spark, targetFqn, column):
    maxValue = spark.table(targetFqn).agg(F.max(F.col(column))).first()[0]
    return (maxValue or 0) + 1


def withSequence(spark, targetFqn, df, column, orderCols):
    """Identity-column replacement: rows without a value in ``column`` get max + row_number()."""
    start = nextSequence(spark, targetFqn, column)
    window = Window.orderBy(*[F.col(c) for c in orderCols])
    return df.withColumn(column, F.coalesce(F.col(column).cast("bigint"), F.lit(start - 1) + F.row_number().over(window)))


def replaceWhere(spark, targetFqn, df, whereSql):
    """Delete + append for partition-like idempotent reloads (e.g. FX rates for a rate date range)."""
    spark.sql("DELETE FROM %s WHERE %s" % (targetFqn, whereSql))
    conformToTable(spark, targetFqn, df).write.format("delta").mode("append").saveAsTable(targetFqn)
    return df.count()
