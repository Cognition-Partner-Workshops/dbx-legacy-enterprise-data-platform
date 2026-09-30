# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""AGG_Refresh_* (parameter ``aggregate_table``); also used standalone outside the publish."""

import json

from sales_performance.aggregates import runAggregate

batchId = batchIdParam()
table = taskParam("aggregate_table", "gold_agg_monthly_sales_summary")
monthsBack = int(taskParam("months_back", 0))
print(json.dumps({"batch_id": batchId, "table": table, **runAggregate(spark, table, batchId, monthsBack)}, default=str))
