# Databricks notebook source
# Thin wrapper: silver.customers.run(spark, cfg). Logic lives in src/sales_lakehouse.
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("mock_data_root", "mock_data/output")

# COMMAND ----------
from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.silver import customers

cfg = PipelineConfig(catalog=dbutils.widgets.get("catalog") or None, mockDataRoot=dbutils.widgets.get("mock_data_root"))
customers.run(spark, cfg)
