"""Spark equivalents of the SSIS data-flow components used by the extract packages."""
from dataclasses import dataclass
from typing import Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from oracle_extract.model import ConditionalSplit, DerivedColumn, Lookup

METADATA_COLUMNS = ("BatchId", "PackageExecutionId", "ExtractedAtUtc", "SourceSystemCode", "WatermarkFrom", "WatermarkTo")


def applyDerivedColumns(df: DataFrame, derived: Sequence[DerivedColumn]) -> DataFrame:
    """Derived Column transformation: each expression is evaluated against the
    columns available at that point (SSIS evaluates them in declaration order)."""
    for d in derived:
        expr = F.expr(d.sparkExpr)
        if d.sparkType:
            expr = expr.cast(d.sparkType)
        df = df.withColumn(d.name, expr)
    return df


def applyConstants(df: DataFrame, constants: Sequence[Sequence[str]]) -> DataFrame:
    for name, value in constants:
        df = df.withColumn(name, F.lit(value))
    return df


@dataclass
class SplitResult:
    matched: DataFrame
    default: DataFrame


def applyConditionalSplit(df: DataFrame, split: Optional[ConditionalSplit]) -> SplitResult:
    """Conditional Split: SSIS sends a row to the first case whose expression is
    TRUE; NULL / FALSE fall through to the default output. Spark's ``where`` has
    exactly that TRUE-only semantics, so the default branch is ``NOT coalesce(expr, false)``."""
    if split is None:
        return SplitResult(df, df.limit(0))
    matchExpr = F.expr(split.matchExpr)
    return SplitResult(df.where(matchExpr), df.where(~F.coalesce(matchExpr, F.lit(False))))


@dataclass
class LookupResult:
    matched: DataFrame
    unmatched: DataFrame


def applyLookup(df: DataFrame, lookup: Optional[Lookup], referenceDf: Optional[DataFrame]) -> LookupResult:
    """Lookup with no-match rows redirected (no_match='RD'): left join on the
    join columns, matched rows gain the output columns, unmatched rows keep the
    input shape and go to the reject output. Lookup reference rows are
    de-duplicated on the join key as the SSIS full-cache lookup does (first hit wins)."""
    if lookup is None:
        return LookupResult(df, df.limit(0))
    if referenceDf is None:
        raise ValueError(f"{lookup.name}: reference DataFrame required")
    ref = referenceDf
    if lookup.filterExpr:
        ref = ref.where(F.expr(lookup.filterExpr))
    refKeys = [F.col(refCol).alias(f"__lk_{i}") for i, (_, refCol) in enumerate(lookup.joinColumns)]
    refOut = [F.col(refCol).alias(outName) for refCol, outName, _ in lookup.outputColumns]
    ref = ref.select(*refKeys, *refOut).dropDuplicates([f"__lk_{i}" for i in range(len(lookup.joinColumns))])
    cond = None
    for i, (inCol, _) in enumerate(lookup.joinColumns):
        term = F.col(inCol) == F.col(f"__lk_{i}")
        cond = term if cond is None else (cond & term)
    matchFlag = F.col("__lk_0").isNotNull()
    joined = df.join(ref, cond, "left")
    matched = joined.where(matchFlag).drop(*[f"__lk_{i}" for i in range(len(lookup.joinColumns))])
    for refCol, outName, sparkType in lookup.outputColumns:
        matched = matched.withColumn(outName, F.col(outName).cast(sparkType))
    unmatched = joined.where(~matchFlag).select(*df.columns)
    return LookupResult(matched, unmatched)


def addIngestionMetadata(df: DataFrame, batchId: int, packageExecutionId: Optional[int], sourceSystemCode: str,
                         watermarkFrom: Optional[str], watermarkTo: Optional[str], extractedAtUtc=None) -> DataFrame:
    """Audit trailer of every raw table (legacy SourceSystemCode / ExtractedAtUtc /
    PackageExecutionId derived columns + the BatchId the OLE DB destination bound)
    plus the watermark window the rows were extracted with."""
    extracted = F.lit(extractedAtUtc).cast("timestamp") if extractedAtUtc is not None else F.current_timestamp()
    return (
        df.withColumn("BatchId", F.lit(batchId).cast("bigint"))
        .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("bigint"))
        .withColumn("ExtractedAtUtc", extracted)
        .withColumn("SourceSystemCode", F.lit(sourceSystemCode).cast("string"))
        .withColumn("WatermarkFrom", F.lit(watermarkFrom).cast("string"))
        .withColumn("WatermarkTo", F.lit(watermarkTo).cast("string"))
    )


def assignRowNumberKey(df: DataFrame, keyColumn: str, orderColumn: str) -> DataFrame:
    """'Assign Surrogate Keys' (UPDATE ... ROW_NUMBER() OVER (ORDER BY CostCenterCode))."""
    from pyspark.sql.window import Window

    return df.withColumn(keyColumn, F.row_number().over(Window.orderBy(F.col(orderColumn))).cast("int"))


def rejectPayload(df: DataFrame, businessKeyColumn: Optional[str], rejectReasonCode: str, rejectReason: str) -> DataFrame:
    """Shape a rejected branch for control.logRejectedRecordSet: keep the business
    key, serialise the whole row as JSON payload."""
    out = df.withColumn("RecordPayload", F.to_json(F.struct(*[F.col(c) for c in df.columns])))
    out = out.withColumn("RejectReasonCode", F.lit(rejectReasonCode)).withColumn("RejectReason", F.lit(rejectReason))
    if businessKeyColumn and businessKeyColumn in df.columns:
        out = out.withColumn("BusinessKey", F.col(businessKeyColumn).cast("string"))
    else:
        out = out.withColumn("BusinessKey", F.lit(None).cast("string"))
    return out
