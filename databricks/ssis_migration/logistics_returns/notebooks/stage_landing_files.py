# Databricks notebook source
# MAGIC %md
# MAGIC # Stage the sample carrier files into the landing volume
# MAGIC Stand-in for the file share `C:\WWI\Landing\inbound\carrier`: copies `landing_samples/carrier/*` into
# MAGIC `/Volumes/<catalog>/<schema>/landing/inbound/carrier/` (only files that are not there yet).

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import os

from logistics_returns.runner import stageLandingFiles

ctx = buildContext()
copied = stageLandingFiles(ctx, os.path.join(bundleRoot, "landing_samples", "carrier"))
print(f"copied {len(copied)} file(s) into {ctx.volumePath('inbound', 'carrier')}: {copied}")
