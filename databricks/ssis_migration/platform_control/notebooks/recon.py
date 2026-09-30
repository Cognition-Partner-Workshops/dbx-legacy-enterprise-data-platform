# Databricks notebook source
# Re-runnable reconciliation evidence: one row per owned package into <catalog>.evidence.recon_results.
# MAGIC %run ./_bootstrap

# COMMAND ----------
import json

from platform_control.control import ControlFramework
from platform_control.recon import runRecon
from platform_control.spark import getSpark

cfg = platformConfig()
spark = getSpark()
cf = ControlFramework(spark, cfg)
result = runRecon(cf)
print(json.dumps(result, default=str, indent=1))
dbutils.notebook.exit(json.dumps({k: v for k, v in result.items() if k != "rows"}, default=str))
