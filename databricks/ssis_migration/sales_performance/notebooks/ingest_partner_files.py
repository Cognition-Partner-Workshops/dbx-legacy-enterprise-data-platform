# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""ING_FILE_PartnerSales_{NA,EU,APAC}: one task per region (parameter ``region``)."""

import json

from sales_performance import config
from sales_performance.partner_files import ensureTables, ingestFeed

region = taskParam("region", "NA")
rootPath = taskParam("landing_root", config.volumeRoot())
batchId = batchIdParam()

ensureTables(spark)
results = ingestFeed(spark, region, rootPath, batchId, moveFiles=True)
print(json.dumps({"region": region, "batch_id": batchId, "files": results}, indent=2, default=str))
