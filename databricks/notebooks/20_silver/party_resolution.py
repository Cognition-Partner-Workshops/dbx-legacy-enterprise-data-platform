# Databricks notebook source
# Thin wrapper: silver.party_resolution.run(spark, cfg). Logic lives in src/sales_lakehouse.
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("mock_data_root", "mock_data/output")

# COMMAND ----------
from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.silver import party_resolution

cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog") or None, mockDataRoot=dbutils.widgets.get("mock_data_root"))
party_resolution.run(spark, cfg)
