# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""Silver: legacy Fact.Sale + dimensions -> silver_sale_line and the reference tables; then the gold fact copies."""

from sales_performance.sale_line import loadGoldFacts, loadSilverLayer

batchId = batchIdParam()
resetCorrections = taskParam("reset_corrections", "false").lower() == "true"
print("silver_sale_line rows:", loadSilverLayer(spark, batchId))
print("gold_fact_sale rows:", loadGoldFacts(spark, batchId, resetCorrections))
