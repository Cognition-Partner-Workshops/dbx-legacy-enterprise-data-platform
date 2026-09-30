# Databricks notebook source
# MAGIC %md
# MAGIC Thin wrapper: runs one migrated SSIS package from the `customer_engagement` library.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from customer_engagement.config import CeConfig  # noqa: E402
from customer_engagement.runner import runPackage  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("package", "")
dbutils.widgets.text("catalog", "otterorders_migration")
dbutils.widgets.text("schema", "ssis_customer_engagement")
dbutils.widgets.text("as_of_date", "auto")
dbutils.widgets.text("staging_source_mode", "legacy_raw")
dbutils.widgets.text("default_region", "NA")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("batch_id", "0")
dbutils.widgets.text("git_sha", "")

# COMMAND ----------

batchIdText = dbutils.widgets.get("batch_id") or "0"
cfg = CeConfig(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    stagingSourceMode=dbutils.widgets.get("staging_source_mode"),
    asOfDate=dbutils.widgets.get("as_of_date") or "auto",
    defaultRegion=dbutils.widgets.get("default_region") or "NA",
    runId=dbutils.widgets.get("run_id"),
    batchId=int(batchIdText) if batchIdText.isdigit() else 0,
    gitSha=dbutils.widgets.get("git_sha"),
)
result = runPackage(spark, cfg, dbutils.widgets.get("package"))
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
