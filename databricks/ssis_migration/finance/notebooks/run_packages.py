# Databricks notebook source
# MAGIC %md
# MAGIC Thin entrypoint: runs one or more finance SSIS packages (library code lives in `../src/finance`).

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from finance.packages import parsePackageList, runPackages  # noqa: E402

# COMMAND ----------
PARAM_KEYS = [
    "packages",
    "catalog",
    "schema",
    "evidence_schema",
    "accounting_period",
    "business_date",
    "git_sha",
    "allow_unbalanced_journals",
    "fail_on_missing_rate",
    "include_disputed",
    "ledger_scope",
]
for key in PARAM_KEYS:
    dbutils.widgets.text(key, "")  # noqa: F821
params = {key: dbutils.widgets.get(key) for key in PARAM_KEYS}  # noqa: F821

# COMMAND ----------
results = runPackages(spark, params, parsePackageList(params["packages"]))  # noqa: F821
display(spark.createDataFrame([r.asRow() for r in results]))  # noqa: F821
