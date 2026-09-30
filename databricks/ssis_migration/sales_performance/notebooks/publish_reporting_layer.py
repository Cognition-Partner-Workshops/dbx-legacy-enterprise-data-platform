# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""AGG_Publish_ReportingLayer: refresh the five in-scope aggregates in dependency order, gate, publish."""

import json

from sales_performance.publish import runPublish

batchId = batchIdParam()
result = runPublish(
    spark,
    batchId,
    refreshAggregates=taskParam("refresh_aggregates", "true").lower() == "true",
    forcePublish=taskParam("force_publish", "false").lower() == "true",
    staleAfterHours=int(taskParam("stale_after_hours", 36)),
    monthsBack=int(taskParam("months_back", 0)),
)
print(json.dumps({"batch_id": batchId, **result}, indent=2, default=str))
