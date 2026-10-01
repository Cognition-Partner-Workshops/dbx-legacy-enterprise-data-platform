# Databricks notebook source
# MAGIC %md
# MAGIC # Sales lakehouse - end-to-end orchestration
# MAGIC Thin wrapper around `sales_lakehouse.orchestration.pipeline.runAll`: runs the selected stages in dependency
# MAGIC order (bronze -> silver reference -> party resolution -> customers -> dimensions -> transactions -> gold facts ->
# MAGIC aggregates -> reporting views -> sales ops -> quality checks -> mock validation) inside one notebook task.
# MAGIC Replaces the SSIS `Master_Daily_ETL` / `Master_Hourly_Incremental` / `Master_Month_End` packages; the
# MAGIC `sales_lakehouse_pipeline` job in `resources/` fans the same stages out over one serverless task each and uses this
# MAGIC notebook for the `quality` / `month_end` stages.

# COMMAND ----------

import os
import sys
import time

dbutils.widgets.text("catalog", "wwi_sales", "Unity Catalog catalog (blank = two-level names)")  # noqa: F821
dbutils.widgets.text("mock_data_root", "/Volumes/wwi_sales/sales_bronze/mock_source", "Mock source CSV root")  # noqa: F821
dbutils.widgets.text("batch_id", "", "Batch id shared by every task of the run (blank = epoch seconds)")  # noqa: F821
dbutils.widgets.text("stages", "all", "'all', a comma list of stage names, or '+mock' / '+month_end'")  # noqa: F821
dbutils.widgets.text("partner_feed_dir", "", "Partner feed output dir (blank = <mock_data_root>/outbound/partner_feed)")  # noqa: F821
dbutils.widgets.text("close_month", "", "month_end stage: YYYY-MM-01 to freeze (blank = previous month)")  # noqa: F821
dbutils.widgets.dropdown("keep_going", "false", ["false", "true"], "Run independent stages after a failure")  # noqa: F821

catalog = dbutils.widgets.get("catalog").strip() or None  # noqa: F821
mockDataRoot = dbutils.widgets.get("mock_data_root").strip()  # noqa: F821
batchIdText = dbutils.widgets.get("batch_id").strip()  # noqa: F821
batchId = int(batchIdText) if batchIdText else int(time.time())
stages = dbutils.widgets.get("stages").strip() or "all"  # noqa: F821
partnerFeedDir = dbutils.widgets.get("partner_feed_dir").strip() or None  # noqa: F821
closeMonthText = dbutils.widgets.get("close_month").strip()  # noqa: F821
keepGoing = dbutils.widgets.get("keep_going") == "true"  # noqa: F821

srcRoot = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src"))
if srcRoot not in sys.path:
    sys.path.insert(0, srcRoot)

# COMMAND ----------

import datetime as dt  # noqa: E402

from sales_lakehouse.common.config import PipelineConfig  # noqa: E402
from sales_lakehouse.common.spark import ensureSchemas  # noqa: E402
from sales_lakehouse.orchestration import pipeline  # noqa: E402

cfg = PipelineConfig(catalog=catalog, mockDataRoot=mockDataRoot, batchId=batchId)
ensureSchemas(spark, cfg)  # noqa: F821
options = pipeline.RunOptions(
    partnerFeedDir=partnerFeedDir,
    closeMonth=dt.date.fromisoformat(closeMonthText) if closeMonthText else None,
    failFast=not keepGoing,
)

# COMMAND ----------

run = pipeline.runAll(spark, cfg, stages=stages, options=options)  # noqa: F821
print(run.table())
display(  # noqa: F821
    spark.createDataFrame(  # noqa: F821
        [(r.stage, r.status, round(r.seconds, 1), r.message) for r in run.results],
        "stage string, status string, seconds double, message string",
    )
)

# COMMAND ----------

if not run.ok:
    failed = ", ".join(r.stage for r in run.results if r.status == "FAILED")
    raise RuntimeError(f"pipeline batch {batchId} failed in: {failed}")
dbutils.notebook.exit(f"batch_id={batchId} stages={stages} {run.summary()}")  # noqa: F821
