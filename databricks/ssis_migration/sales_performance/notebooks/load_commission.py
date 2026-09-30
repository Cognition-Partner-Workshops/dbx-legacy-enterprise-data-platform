# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""SLS_{NA,EU,APAC}_Load_Commission (parameter ``region``)."""

import json

from sales_performance.commissions import runRegion

region = taskParam("region", "NA")
batchId = batchIdParam()
params = {
    "houseAccountRatePercent": taskParam("house_account_rate_percent", 50),
    "cashBasisCountries": taskParam("cash_basis_countries", "DEU,AUT"),
    "statutoryCapAmount": taskParam("statutory_cap_amount", None),
    "teamSplitEnabled": taskParam("team_split_enabled", "false").lower() == "true",
    "teamSplitPercent": taskParam("team_split_percent", 100),
}
if params["statutoryCapAmount"] in ("", "None", None):
    params["statutoryCapAmount"] = None
print(json.dumps({"region": region, "batch_id": batchId, **runRegion(spark, region, batchId, params)}, default=str))
