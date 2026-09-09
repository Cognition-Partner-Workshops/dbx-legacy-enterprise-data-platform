"""Load one legacy table into its bronze Delta table.

Bronze keeps the extract's columns and physical types untouched and adds ingest
metadata. Loads are file-idempotent: every landed file is recorded in
``<catalog>.<bronze_prefix>_wwi_fin._ingest_log`` and a rerun skips files already
loaded, so a partial job can simply be run again.
"""

from __future__ import annotations

import argparse
import os
import sys


def _src_root() -> str:
    # Serverless spark_python_task exec()s the file without __file__, so the job
    # passes the workspace path explicitly; local runs fall back to the file location.
    if "--src-root" in sys.argv:
        return sys.argv[sys.argv.index("--src-root") + 1]
    return os.path.dirname(os.path.abspath(__file__))


sys.path.insert(0, _src_root())

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from sources import TableRef, make_reader  # noqa: E402

META_COLUMNS = ("_source_kind", "_source_ref", "_source_file", "_ingest_ts", "_run_id")


def ingest_log_table(catalog: str, bronze_prefix: str) -> str:
    return "%s.%s_wwi_fin._ingest_log" % (catalog, bronze_prefix)


def ensure_ingest_log(spark: SparkSession, name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {name} (
            legacy_table   STRING,
            bronze_table   STRING,
            source_kind    STRING,
            source_file    STRING,
            rows_loaded    BIGINT,
            run_id         STRING,
            loaded_at      TIMESTAMP
        ) USING DELTA
        COMMENT 'Wave-1 bronze file-level ingest log; drives idempotent reruns.'
        """
    )


def already_loaded(spark: SparkSession, log: str, legacy_table: str) -> set:
    rows = spark.sql(
        f"SELECT source_file FROM {log} WHERE legacy_table = '{legacy_table}'"
    ).collect()
    return {r.source_file for r in rows}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="legacy SCHEMA.TABLE")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--landing-path", required=True)
    parser.add_argument("--bronze-prefix", required=True)
    parser.add_argument("--source-kind", default="volume_delimited")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--src-root", default=None, help="directory holding sources.py")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    table = TableRef.parse(args.table)
    target = table.bronze_table(args.catalog, args.bronze_prefix)
    log = ingest_log_table(args.catalog, args.bronze_prefix)
    ensure_ingest_log(spark, log)

    reader = make_reader(args.source_kind, spark, args.landing_path)
    batch = reader.read(table)

    done = already_loaded(spark, log, table.qualified)
    pending = [f for f in batch.files if f not in done]

    df = (
        batch.df.where(F.col("_source_file").isin(pending))
        .withColumn("_source_kind", F.lit(batch.source_kind))
        .withColumn("_source_ref", F.lit(batch.source_ref))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(args.run_id))
    )

    if not spark.catalog.tableExists(target):
        # Contract-shaped empty table so downstream layers can bind even before
        # the first non-empty extract lands.
        df.limit(0).write.format("delta").saveAsTable(target)
        spark.sql(
            f"COMMENT ON TABLE {target} IS 'Bronze copy of Oracle {table.qualified}; "
            f"raw extract columns plus ingest metadata. Source kind: {batch.source_kind}.'"
        )

    if not pending:
        print("%s: nothing new under %s (%d file(s) already loaded)" % (target, batch.source_ref, len(done)))
        return

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .saveAsTable(target)
    )

    per_file = (
        spark.table(target)
        .where((F.col("_run_id") == args.run_id) & F.col("_source_file").isin(pending))
        .groupBy("_source_file").count().collect()
    )
    log_rows = [
        (table.qualified, target, batch.source_kind, r["_source_file"], int(r["count"]), args.run_id)
        for r in per_file
    ]
    (
        spark.createDataFrame(
            log_rows,
            "legacy_table STRING, bronze_table STRING, source_kind STRING, source_file STRING, "
            "rows_loaded BIGINT, run_id STRING",
        )
        .withColumn("loaded_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(log)
    )
    loaded = sum(r["count"] for r in per_file)
    total = spark.table(target).count()
    print(
        "%s: +%d rows from %d file(s) (contract expected %d); table now %d rows"
        % (target, loaded, len(pending), batch.contract.expected_rows, total)
    )


if __name__ == "__main__":
    main()
