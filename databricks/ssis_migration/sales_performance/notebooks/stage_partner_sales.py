# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""STG_Load_PartnerSale: bronze partner rows -> silver_partner_sale (watermarked append)."""

import json
import os

from sales_performance import config
from sales_performance.partner_staging import runStaging

batchId = batchIdParam()
seedPath = taskParam("crosswalk_seed", os.path.join(config.volumeRoot(), "seeds", "partner_customer_crosswalk.csv"))
seed = None
if os.path.exists(seedPath):
    seed = spark.read.option("header", True).csv(seedPath)
result = runStaging(spark, batchId, seed)
print(json.dumps({"batch_id": batchId, **result}, default=str))
