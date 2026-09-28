# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Return accumulating snapshot
# MAGIC Thin wrapper: all logic lives in `sales_lakehouse.gold.fact_return` so the same code runs
# MAGIC locally under pytest and on Databricks. Parameters: `catalog`, `mock_data_root`, `batch_id`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from sales_lakehouse.common.config import PipelineConfig  # noqa: E402
from sales_lakehouse.gold import fact_return  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "wwi_sales")
dbutils.widgets.text("mock_data_root", "/Volumes/wwi_sales/sales_bronze/mock_source")
dbutils.widgets.text("batch_id", "")

batchIdText = dbutils.widgets.get("batch_id")
cfg = PipelineConfig(
    catalog=dbutils.widgets.get("catalog") or None,
    mockDataRoot=dbutils.widgets.get("mock_data_root"),
    **({"batchId": int(batchIdText)} if batchIdText else {}),
)

# COMMAND ----------

fact_return.run(spark, cfg)
