"""Package execution wrapper: mirrors etl.usp_LogPackageStart/End on etl_package_execution."""
import traceback
from collections.abc import Callable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import RunContext
from sales_o2c.tables import appendRows


def runPackage(spark: SparkSession, ctx: RunContext, packageName: str, body: Callable[[], dict]) -> dict:
    """Run a package body, log start/end + row counters, re-raise failures (SSIS FailPackageOnFailure)."""
    status, message, counters = "Succeeded", None, {}
    try:
        counters = body() or {}
    except Exception as exc:  # noqa: BLE001 - logged then re-raised
        status, message = "Failed", "".join(traceback.format_exception_only(type(exc), exc))[:4000]
        raise
    finally:
        row = (
            int(ctx.batchId), int(ctx.packageExecutionId), packageName, status,
            int(counters.get("rowsRead", 0)), int(counters.get("rowsInserted", 0)), int(counters.get("rowsUpdated", 0)),
            int(counters.get("rowsRejected", 0)), int(counters.get("rowsDeleted", 0)), message, bool(ctx.reloadFullHistory),
        )
        schema = (
            "batch_id bigint, package_execution_id bigint, package_name string, status string, rows_read bigint, rows_inserted bigint, "
            "rows_updated bigint, rows_rejected bigint, rows_deleted bigint, error_message string, reload_full_history boolean"
        )
        appendRows(spark, ctx.table("etl_package_execution"), spark.createDataFrame([row], schema).withColumn("logged_at_utc", F.current_timestamp()))
    return counters
