# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""FACT_Apply_Corrections. Seeds work_fact_rekey_queue from the committed sample queue on first run
(the legacy work.FactRekeyQueue is empty), then applies every pending row."""

import json
import os

from pyspark.sql import functions as F

from sales_performance import config
from sales_performance.common import saveTable
from sales_performance.corrections import QUEUE_SCHEMA, QUEUE_TABLE, runCorrections

batchId = batchIdParam()
seedPath = taskParam("queue_seed", os.path.join(config.volumeRoot(), "seeds", "fact_rekey_queue.csv"))
if not spark.catalog.tableExists(config.tableName(QUEUE_TABLE)) and os.path.exists(seedPath):
    seed = spark.read.option("header", True).schema(QUEUE_SCHEMA).csv(seedPath)
    seed = seed.withColumn("applied_flag", F.coalesce(F.col("applied_flag"), F.lit(False))).withColumn(
        "created_at_utc", F.coalesce(F.col("created_at_utc"), F.current_timestamp())
    )
    saveTable(seed, QUEUE_TABLE)
result = runCorrections(spark, batchId, taskParam("correction_period_code", "") or None, int(taskParam("max_corrections", 50000)))
print(json.dumps({"batch_id": batchId, **result}, default=str))
