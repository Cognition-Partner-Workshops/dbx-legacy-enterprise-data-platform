"""Record and print the row count of every wave-1 bronze table after a load.

Writes one row per table into ``<catalog>.<bronze_prefix>_wwi_fin._row_counts``
(run_id, bronze table, rows, contract-expected rows) and prints the same as a
markdown table so the job run page carries the evidence.
"""

from __future__ import annotations

import argparse
import os
import sys


def _src_root() -> str:
    if "--src-root" in sys.argv:
        return sys.argv[sys.argv.index("--src-root") + 1]
    return os.path.dirname(os.path.abspath(__file__))


sys.path.insert(0, _src_root())

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from plan_tables import parse_tables  # noqa: E402
from sources import TableRef  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-prefix", required=True)
    parser.add_argument("--source-tables", required=True)
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--src-root", default=None)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    log = "%s.%s_wwi_fin._ingest_log" % (args.catalog, args.bronze_prefix)
    counts_table = "%s.%s_wwi_fin._row_counts" % (args.catalog, args.bronze_prefix)

    expected = {
        r.legacy_table: r.rows
        for r in spark.sql(
            f"SELECT legacy_table, SUM(rows_loaded) AS rows FROM {log} GROUP BY legacy_table"
        ).collect()
    }

    rows = []
    for qualified in parse_tables(args.source_tables):
        table = TableRef.parse(qualified)
        target = table.bronze_table(args.catalog, args.bronze_prefix)
        n = spark.table(target).count() if spark.catalog.tableExists(target) else 0
        rows.append((args.run_id, table.qualified, target, int(n), int(expected.get(table.qualified, 0))))

    (
        spark.createDataFrame(
            rows, "run_id STRING, legacy_table STRING, bronze_table STRING, rows BIGINT, rows_logged BIGINT"
        )
        .withColumn("counted_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(counts_table)
    )

    print("| legacy table | bronze table | rows | rows per ingest log |")
    print("|---|---|---:|---:|")
    for _, legacy, target, n, logged in rows:
        print("| %s | %s | %s | %s |" % (legacy, target, format(n, ","), format(logged, ",")))
    print("total rows: %s" % format(sum(r[3] for r in rows), ","))


if __name__ == "__main__":
    main()
