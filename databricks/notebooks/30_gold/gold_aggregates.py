# Databricks notebook source
# MAGIC %md
# MAGIC # 30_gold / gold_aggregates
# MAGIC
# MAGIC Refresh `sales_gold.agg_*` (daily, monthly, regional, customer 360, rolling 12m, product, margin) from the fact tables.
# MAGIC Replaces `Integration.usp_RefreshAggregate*`.
# MAGIC Thin wrapper: all logic lives in `sales_lakehouse.gold.aggregates`. Full rebuild, safe to re-run.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "", "Unity Catalog catalog (blank = two-level names)")  # noqa: F821
dbutils.widgets.text("mock_data_root", "mock_data/output", "Mock source CSV root")  # noqa: F821
dbutils.widgets.text("batch_id", "", "Batch id shared by every task of the run (blank = epoch seconds)")  # noqa: F821

catalog = dbutils.widgets.get("catalog").strip() or None  # noqa: F821
mockDataRoot = dbutils.widgets.get("mock_data_root").strip()  # noqa: F821
batchIdText = dbutils.widgets.get("batch_id").strip()  # noqa: F821

srcPath = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src"))
if srcPath not in sys.path:
    sys.path.insert(0, srcPath)

# COMMAND ----------

from sales_lakehouse.common.config import PipelineConfig  # noqa: E402
from sales_lakehouse.common.spark import ensureSchemas  # noqa: E402
from sales_lakehouse.gold import aggregates  # noqa: E402

cfg = PipelineConfig(catalog=catalog, mockDataRoot=mockDataRoot, **({"batchId": int(batchIdText)} if batchIdText else {}))
ensureSchemas(spark, cfg)  # noqa: F821
aggregates.run(spark, cfg)  # noqa: F821
