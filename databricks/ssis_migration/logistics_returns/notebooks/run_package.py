# Databricks notebook source
# MAGIC %md
# MAGIC # Run one migrated SSIS package
# MAGIC Thin wrapper: `package` selects the implementation in `logistics_returns.runner.PACKAGES`; all logic lives in `src/`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import json

from logistics_returns.runner import runPackage

dbutils.widgets.text("package", "", "SSIS package name (as in docs/inventories/ssis-packages.csv)")
packageName = dbutils.widgets.get("package").strip()
ctx = buildContext()
print(f"package={packageName} catalog={ctx.catalog} schema={ctx.schema} batch_id={ctx.batchId} git_sha={ctx.gitSha}")

# COMMAND ----------

result = runPackage(ctx, packageName)
print(json.dumps(result, default=str, indent=2))
dbutils.notebook.exit(json.dumps({"package": packageName, "result": result}, default=str))
