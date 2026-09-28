# Databricks notebook source
# MAGIC %md
# MAGIC # Validate the lakehouse against the mock manifest
# MAGIC Re-derives the expected footprint of every planted edge case from `manifest.json` + the raw CSVs under
# MAGIC `mock_data_root` and compares it with silver / gold (`sales_lakehouse.quality.validate_mock`). Read-only; the
# MAGIC task fails when any check fails so the job run shows red.

# COMMAND ----------

import os
import sys
import time

dbutils.widgets.text("catalog", "wwi_sales", "Unity Catalog catalog (blank = two-level names)")  # noqa: F821
dbutils.widgets.text("mock_data_root", "/Volumes/wwi_sales/sales_bronze/mock_source", "Mock source CSV root")  # noqa: F821
dbutils.widgets.text("batch_id", "", "Batch id shared by every task of the run (blank = epoch seconds)")  # noqa: F821
dbutils.widgets.text("partner_feed_dir", "", "Partner feed dir written by gold_sales_ops (blank = <mock_data_root>/outbound/partner_feed)")  # noqa: F821

catalog = dbutils.widgets.get("catalog").strip() or None  # noqa: F821
mockDataRoot = dbutils.widgets.get("mock_data_root").strip()  # noqa: F821
batchIdText = dbutils.widgets.get("batch_id").strip()  # noqa: F821
batchId = int(batchIdText) if batchIdText else int(time.time())
partnerFeedDir = dbutils.widgets.get("partner_feed_dir").strip() or None  # noqa: F821

srcRoot = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src"))
if srcRoot not in sys.path:
    sys.path.insert(0, srcRoot)

# COMMAND ----------

from sales_lakehouse.common.config import PipelineConfig  # noqa: E402
from sales_lakehouse.quality import validate_mock  # noqa: E402

cfg = PipelineConfig(catalog=catalog, mockDataRoot=mockDataRoot, batchId=batchId)
report = validate_mock.validate(spark, cfg, partnerFeedDir)  # noqa: F821
print(report.table())
display(  # noqa: F821
    spark.createDataFrame(  # noqa: F821
        [(r.code, r.status, r.observed, r.expected, r.detail) for r in report.results],
        "check string, status string, observed string, expected string, detail string",
    )
)

# COMMAND ----------

if not report.ok:
    raise RuntimeError(f"{len(report.failed)} validation check(s) failed: {', '.join(r.code for r in report.failed)}")
dbutils.notebook.exit(f"batch_id={batchId} {report.describe()}")  # noqa: F821
