# Databricks notebook source
# MAGIC %md
# MAGIC # sales_o2c: reconciliation evidence
# MAGIC Re-runnable: appends one row per package (new `run_id`) to `otterorders_migration.evidence.recon_results`.

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from sales_o2c.config import RunContext  # noqa: E402
from sales_o2c.recon import runRecon  # noqa: E402

# COMMAND ----------
for name, default in [("catalog", "otterorders_migration"), ("schema", "ssis_sales_o2c"), ("evidence_schema", "evidence"), ("git_sha", "local"), ("batch_id", "0")]:
    dbutils.widgets.text(name, default)  # noqa: F821

ctx = RunContext(
    catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"), evidenceSchema=dbutils.widgets.get("evidence_schema"),  # noqa: F821
    gitSha=dbutils.widgets.get("git_sha"),  # noqa: F821
)

# COMMAND ----------
evidence = runRecon(spark, ctx)  # noqa: F821
display(evidence.select("run_id", "unit", "verdict", "summary"))  # noqa: F821
