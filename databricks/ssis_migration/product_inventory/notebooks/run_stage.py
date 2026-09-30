# Databricks notebook source
# MAGIC %md
# MAGIC # ssis_product_inventory – stage runner
# MAGIC Thin notebook: resolves widgets into a `PipelineConfig` and runs one stage
# MAGIC (bronze / silver / dimensions / facts / inventory / aggregates) of the
# MAGIC `product_inventory` package group from the `src/` library.

# COMMAND ----------
import os
import sys

from databricks.sdk.runtime import dbutils

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from product_inventory.config import configFromParams  # noqa: E402
from product_inventory.packages import runStage  # noqa: E402
from product_inventory.spark import getSpark  # noqa: E402

# COMMAND ----------
WIDGETS = {
    "stage": "bronze",
    "catalog": "otterorders_migration",
    "schema": "ssis_product_inventory",
    "batch_id": "",
    "business_date": "",
    "reload_full_history": "true",
    "git_sha": "unknown",
    "position_window_days": "90",
    "cover_days": "21",
    "suppress_chiller_suggestions": "false",
    "in_transit_age_alert_days": "10",
    "timing_window_minutes": "30",
    "site_scope": "ALL",
    "retention_days": "400",
    "agg_days_back": "7",
}
for name, default in WIDGETS.items():
    dbutils.widgets.text(name, default)
params = {name: dbutils.widgets.get(name) for name in WIDGETS}

# COMMAND ----------
cfg = configFromParams(params)
results = runStage(getSpark(), cfg, params["stage"])
for package, counters in results.items():
    print(package, counters)
dbutils.notebook.exit(str(results))
