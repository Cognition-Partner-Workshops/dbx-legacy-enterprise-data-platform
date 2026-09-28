# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - sales transactions (workstream 4)
# MAGIC Thin wrapper around `sales_lakehouse.silver.transactions.run`. Parameters
# MAGIC `catalog` and `mock_data_root` come from the job definition
# MAGIC (`resources/silver_transactions_job.yml`).

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "wwi_sales")
dbutils.widgets.text("mock_data_root", "/Volumes/wwi_sales/sales_bronze/mock_source")
dbutils.widgets.text("batch_id", "")

srcRoot = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src"))
if srcRoot not in sys.path:
    sys.path.insert(0, srcRoot)

# COMMAND ----------

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.spark import ensureSchemas
from sales_lakehouse.silver import transactions

batchIdText = dbutils.widgets.get("batch_id").strip()
cfg = PipelineConfig(
    catalog=dbutils.widgets.get("catalog") or None,
    mockDataRoot=dbutils.widgets.get("mock_data_root"),
    **({"batchId": int(batchIdText)} if batchIdText else {}),
)
ensureSchemas(spark, cfg)
transactions.run(spark, cfg)
