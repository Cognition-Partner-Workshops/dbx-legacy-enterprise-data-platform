# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
dbutils.widgets.text("packages", "all", "Packages (layer name, 'all' or comma list)")
dbutils.widgets.text("batch_id", "", "Batch id (defaults to job run id)")

# COMMAND ----------
from procurement import io
from procurement.pipeline import runPackages

packages = dbutils.widgets.get("packages")
batchId = dbutils.widgets.get("batch_id").strip() or io.newBatchId()
results = runPackages(spark, int(batchId), packages)
print({"batch_id": int(batchId), "results": results})
