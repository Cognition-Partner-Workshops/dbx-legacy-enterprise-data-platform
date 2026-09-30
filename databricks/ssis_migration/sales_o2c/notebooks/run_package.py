# Databricks notebook source
# MAGIC %md
# MAGIC # sales_o2c: run one migrated SSIS package
# MAGIC Thin wrapper: parameters -> `RunContext` -> `sales_o2c.packages.executePackage`. All logic lives in `../src`.

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from sales_o2c.config import RunContext, parseBool  # noqa: E402
from sales_o2c.packages import executePackage  # noqa: E402

# COMMAND ----------
for name, default in [
    ("package", ""), ("catalog", "otterorders_migration"), ("schema", "ssis_sales_o2c"), ("evidence_schema", "evidence"),
    ("git_sha", "local"), ("reload_full_history", "false"), ("batch_id", "0"), ("task_run_id", "0"),
    ("snapshot_date", ""), ("agg_from_date", ""), ("agg_to_date", ""),
]:
    dbutils.widgets.text(name, default)  # noqa: F821 - Databricks runtime global

w = {k: dbutils.widgets.get(k) for k in ["package", "catalog", "schema", "evidence_schema", "git_sha", "reload_full_history", "batch_id", "task_run_id", "snapshot_date", "agg_from_date", "agg_to_date"]}  # noqa: F821


def asInt(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


ctx = RunContext(
    catalog=w["catalog"], schema=w["schema"], evidenceSchema=w["evidence_schema"],
    batchId=asInt(w["batch_id"]), packageExecutionId=asInt(w["task_run_id"]) or asInt(w["batch_id"]),
    reloadFullHistory=parseBool(w["reload_full_history"]), gitSha=w["git_sha"],
    snapshotDate=w["snapshot_date"], aggFromDate=w["agg_from_date"], aggToDate=w["agg_to_date"],
)

# COMMAND ----------
counters = executePackage(spark, ctx, w["package"])  # noqa: F821 - Databricks runtime global
print(w["package"], counters)
dbutils.notebook.exit(str(counters))  # noqa: F821
