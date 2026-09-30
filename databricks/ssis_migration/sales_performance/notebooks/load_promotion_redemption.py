# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""SLS_Load_PromotionRedemption."""

import json

from sales_performance.promotions import runPromotionRedemption

batchId = batchIdParam()
strictMode = taskParam("strict_attribution_mode", "false").lower() == "true"
print(json.dumps({"batch_id": batchId, **runPromotionRedemption(spark, batchId, strictMode)}, default=str))
