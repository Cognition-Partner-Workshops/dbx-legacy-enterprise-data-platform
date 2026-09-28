"""Flat File Source replacement: whole files -> decoded lines -> delimited columns.

Auto Loader lands every feed file as one ``binaryFile`` row (path,
modificationTime, length, content). Decoding the bytes with the connection
manager's code page and exploding the lines keeps three things the CSV reader
cannot give at once: the exact raw line (``err.RejectedFileRow.RawRowText``),
the 1-based line number (``SourceRowNumber``) and a per-file view for the
footer / control-total logic of the Foreach loop.
"""

from __future__ import annotations

import csv
from typing import List, Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .feeds import FeedSpec

# Arrival metadata added to every landed and rejected row.
FILE_COLUMNS = ("FileName", "FilePath", "FileModifiedAtUtc", "FileSizeBytes", "ArrivedAtUtc")
LINE_COLUMNS = FILE_COLUMNS + ("SourceRowNumber", "RawLine")


def decodeLines(content: Optional[bytes], codec: str) -> List[str]:
    """Decode a file's bytes with the feed code page and split into lines.

    Undecodable bytes become U+FFFD, exactly what a code-page mismatch produced
    in the legacy loader (DQ_File_Screen looks for that replacement character).
    A UTF-8 BOM is dropped so the first header cell keeps its name.
    """
    if content is None:
        return []
    text = content.decode(codec, errors="replace")
    if text.startswith("\ufeff"):
        text = text[1:]
    if text == "":
        return []
    return text.splitlines()


def explodeLines(filesDf: DataFrame, spec: FeedSpec) -> DataFrame:
    """binaryFile rows -> one row per line with the arrival metadata columns.

    Expects the Auto Loader binaryFile columns ``path``, ``modificationTime``,
    ``length`` and ``content``.
    """
    codec = spec.pythonCodec
    decodeUdf = F.udf(lambda content: decodeLines(content, codec), T.ArrayType(T.StringType()))
    exploded = filesDf.select(
        F.element_at(F.split(F.col("path"), "/"), -1).alias("FileName"),
        F.col("path").alias("FilePath"),
        F.col("modificationTime").alias("FileModifiedAtUtc"),
        F.col("length").alias("FileSizeBytes"),
        F.current_timestamp().alias("ArrivedAtUtc"),
        F.posexplode_outer(decodeUdf(F.col("content"))).alias("linePos", "RawLine"),
    )
    return exploded.withColumn("SourceRowNumber", (F.col("linePos") + F.lit(1)).cast("long")).drop("linePos")


def dataLines(linesDf: DataFrame, spec: FeedSpec) -> DataFrame:
    """Drop the column-header line when the connection manager had one."""
    if spec.header:
        return linesDf.where(F.col("SourceRowNumber") > 1)
    return linesDf


def splitDelimited(line: Optional[str], delimiter: str, qualifier: Optional[str]) -> List[str]:
    """Split one line the way the Flat File connection manager did (text qualifier ``"`` honoured, doubled inside)."""
    if line is None:
        return []
    if qualifier and qualifier in line:
        return next(csv.reader([line], delimiter=delimiter, quotechar=qualifier, doublequote=True, strict=False), [])
    return line.split(delimiter)


def splitColumns(linesDf: DataFrame, spec: FeedSpec) -> DataFrame:
    """Delimited line -> one string column per connection-manager column.

    ``ActualColumnCount`` / ``ExpectedColumnCount`` are kept so a short or long
    line can be rejected as COLUMN_COUNT (the Flat File Source error output).
    Fields are trimmed of the surrounding whitespace the mainframe feeds pad
    with; values are otherwise landed as text exactly as received.
    """
    delimiter, qualifier = spec.delimiter, spec.textQualifier
    splitUdf = F.udf(lambda line: splitDelimited(line, delimiter, qualifier), T.ArrayType(T.StringType()))
    df = linesDf.withColumn("_parts", splitUdf(F.col("RawLine")))
    df = df.withColumn("ActualColumnCount", F.when(F.col("RawLine") == "", F.lit(0)).otherwise(F.size(F.col("_parts"))))
    df = df.withColumn("ExpectedColumnCount", F.lit(spec.expectedColumnCount))
    for index, name in enumerate(spec.columns):
        df = df.withColumn(name, F.trim(F.try_element_at(F.col("_parts"), F.lit(index + 1))))
    return df.drop("_parts")


def emptyText(col: Column) -> Column:
    """LEN(TRIM(x)) == 0 with SSIS semantics (NULL counts as empty)."""
    return F.coalesce(F.length(F.trim(col)), F.lit(0)) == 0


def nonEmptyText(col: Column) -> Column:
    return ~emptyText(col)


def selectOrdered(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    return df.select(*[F.col(c) for c in columns])
