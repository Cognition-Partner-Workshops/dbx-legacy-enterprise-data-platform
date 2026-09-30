# Databricks notebook source
# MAGIC %md
# MAGIC Reconciliation evidence: one row per finance package into `otterorders_migration.evidence.recon_results`.

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from finance.config import configFromParams  # noqa: E402
from finance.recon import runRecon  # noqa: E402

# COMMAND ----------
PARAM_KEYS = ["catalog", "schema", "evidence_schema", "accounting_period", "business_date", "git_sha"]
for key in PARAM_KEYS:
    dbutils.widgets.text(key, "")  # noqa: F821
params = {key: dbutils.widgets.get(key) for key in PARAM_KEYS}  # noqa: F821

# COMMAND ----------
summary = runRecon(spark, configFromParams(params))  # noqa: F821
display(summary)  # noqa: F821
