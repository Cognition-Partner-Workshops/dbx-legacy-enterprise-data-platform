"""Row-count / hash fingerprints for the procurement reconciliation notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Columns that legitimately differ between the SQL Server baseline and the Delta load.
DEFAULT_EXCLUDED_COLUMNS = ("batch_id", "load_datetime", "refresh_batch_id", "refreshed_datetime",
                            "BatchId", "LoadedAtUtc", "PackageExecutionId")


def fingerprint(df: DataFrame, excludeColumns=DEFAULT_EXCLUDED_COLUMNS) -> tuple[int, int]:
    """(row_count, order-independent xxhash64 digest) over every column except the excluded ones.

    Each row is hashed as the concatenation of its columns cast to string (NULL -> '<null>'),
    and the row hashes are summed modulo 2^63 so the digest does not depend on row order.
    """
    cols = [c for c in df.columns if c not in excludeColumns]
    rowHash = F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in sorted(cols)])
    row = df.agg(F.count(F.lit(1)).alias("row_count"),
                 F.coalesce(F.sum(rowHash % F.lit(2 ** 62)), F.lit(0)).alias("digest")).collect()[0]
    return int(row["row_count"]), int(row["digest"])


def compareWithBaseline(actualRowCount: int, actualDigest: int, baseline: dict | None) -> dict:
    """Compare against {"row_count": n, "digest": d}; missing baseline -> status 'NoBaseline'."""
    if not baseline:
        return {"status": "NoBaseline", "baselineRowCount": None, "baselineDigest": None, "variance": None}
    baselineRowCount = baseline.get("row_count")
    baselineDigest = baseline.get("digest")
    variance = None if baselineRowCount is None else int(baselineRowCount) - int(actualRowCount)
    countOk = baselineRowCount is None or int(baselineRowCount) == int(actualRowCount)
    digestOk = baselineDigest is None or int(baselineDigest) == int(actualDigest)
    status = "Matched" if countOk and digestOk else ("CountMismatch" if not countOk else "HashMismatch")
    return {"status": status, "baselineRowCount": baselineRowCount, "baselineDigest": baselineDigest, "variance": variance}
