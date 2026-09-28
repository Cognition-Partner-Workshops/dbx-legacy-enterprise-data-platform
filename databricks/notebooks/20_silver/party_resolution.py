# Databricks notebook source
import os
import sys

# Thin wrapper: silver.party_resolution.run(spark, cfg). Logic lives in src/sales_lakehouse.
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("mock_data_root", "mock_data/output")
dbutils.widgets.text("batch_id", "")

srcRoot = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src"))
if srcRoot not in sys.path:
    sys.path.insert(0, srcRoot)

# COMMAND ----------
from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.silver import party_resolution

batchIdText = dbutils.widgets.get("batch_id").strip()
cfg = PipelineConfig(
    catalog=dbutils.widgets.get("catalog") or None,
    mockDataRoot=dbutils.widgets.get("mock_data_root"),
    **({"batchId": int(batchIdText)} if batchIdText else {}),
)
party_resolution.run(spark, cfg)
