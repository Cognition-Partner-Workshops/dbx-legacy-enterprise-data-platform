# Databricks notebook source
# MAGIC %md
# MAGIC # customer_party package runner
# MAGIC Thin wrapper: all logic lives in `src/customer_party`. The `packages` widget takes a
# MAGIC comma-separated list of SSIS package names (or `RECON`) in Master_Daily_ETL order.

# COMMAND ----------

import os
import sys

notebookDir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())  # noqa: F821
for candidate in (os.path.join("/Workspace" + notebookDir, "..", "src"), os.path.join(os.getcwd(), "..", "src")):
    srcPath = os.path.abspath(candidate)
    if os.path.isdir(srcPath):
        if srcPath not in sys.path:
            sys.path.insert(0, srcPath)
        break

# COMMAND ----------

dbutils.widgets.text("packages", "")  # noqa: F821
dbutils.widgets.text("catalog", "otterorders_migration")  # noqa: F821
dbutils.widgets.text("schema", "ssis_customer_party")  # noqa: F821
dbutils.widgets.text("evidence_schema", "evidence")  # noqa: F821
dbutils.widgets.text("batch_id", "1")  # noqa: F821
dbutils.widgets.text("reload_full_history", "false")  # noqa: F821
dbutils.widgets.text("git_sha", "unknown")  # noqa: F821

# COMMAND ----------

from customer_party.run import configFromEnv, runPackages  # noqa: E402

packages = [p.strip() for p in dbutils.widgets.get("packages").split(",") if p.strip()]  # noqa: F821
cfg = configFromEnv(
    {
        "CP_CATALOG": dbutils.widgets.get("catalog"),  # noqa: F821
        "CP_SCHEMA": dbutils.widgets.get("schema"),  # noqa: F821
        "CP_EVIDENCE_SCHEMA": dbutils.widgets.get("evidence_schema"),  # noqa: F821
        "CP_BATCH_ID": dbutils.widgets.get("batch_id"),  # noqa: F821
        "CP_RELOAD_FULL_HISTORY": dbutils.widgets.get("reload_full_history"),  # noqa: F821
        "CP_GIT_SHA": dbutils.widgets.get("git_sha"),  # noqa: F821
    }
)
counts = runPackages(packages, cfg, spark)  # noqa: F821
dbutils.notebook.exit(str(counts))  # noqa: F821
