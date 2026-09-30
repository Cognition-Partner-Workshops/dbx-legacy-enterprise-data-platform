# Databricks notebook source
# MAGIC %md
# MAGIC # ssis_product_inventory – reconciliation evidence
# MAGIC Re-runnable: appends one `recon_results` row per package for this run id.

# COMMAND ----------
import os
import sys

from databricks.sdk.runtime import dbutils, display

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from product_inventory.config import configFromParams  # noqa: E402
from product_inventory.recon import runRecon  # noqa: E402
from product_inventory.spark import getSpark  # noqa: E402

# COMMAND ----------
WIDGETS = {
    "catalog": "otterorders_migration",
    "schema": "ssis_product_inventory",
    "batch_id": "",
    "business_date": "",
    "git_sha": "unknown",
    "position_window_days": "90",
    "cover_days": "21",
    "suppress_chiller_suggestions": "false",
    "in_transit_age_alert_days": "10",
    "timing_window_minutes": "30",
    "site_scope": "ALL",
    "agg_days_back": "7",
}
for name, default in WIDGETS.items():
    dbutils.widgets.text(name, default)
params = {name: dbutils.widgets.get(name) for name in WIDGETS}

# COMMAND ----------
evidence = runRecon(getSpark(), configFromParams(params))
display(evidence.select("unit", "verdict", "summary"))
