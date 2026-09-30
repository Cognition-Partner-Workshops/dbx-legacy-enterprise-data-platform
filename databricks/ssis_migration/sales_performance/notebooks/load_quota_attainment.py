# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""SLS_Load_QuotaAttainment."""

import json

from sales_performance.quota import runQuotaAttainment

batchId = batchIdParam()
print(json.dumps({"batch_id": batchId, **runQuotaAttainment(spark, batchId)}, default=str))
