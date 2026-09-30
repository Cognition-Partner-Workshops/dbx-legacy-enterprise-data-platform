# Databricks notebook source
# MAGIC %md
# MAGIC Reconciliation task: writes one evidence row per package to `otterorders_migration.evidence.recon_results`.

# COMMAND ----------

import json
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from customer_engagement.config import CeConfig, resolveAsOfDate  # noqa: E402
from customer_engagement.recon import runRecon  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "otterorders_migration")
dbutils.widgets.text("schema", "ssis_customer_engagement")
dbutils.widgets.text("as_of_date", "auto")
dbutils.widgets.text("default_region", "NA")
dbutils.widgets.text("git_sha", "")
dbutils.widgets.text("evidence_run_id", "")

# COMMAND ----------

cfg = CeConfig(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    asOfDate=dbutils.widgets.get("as_of_date") or "auto",
    defaultRegion=dbutils.widgets.get("default_region") or "NA",
    runId=dbutils.widgets.get("evidence_run_id") or str(uuid.uuid4()),
    gitSha=dbutils.widgets.get("git_sha"),
)
runId, evidence = runRecon(spark, cfg, resolveAsOfDate(spark, cfg))
display(evidence.select("unit", "verdict", "summary"))
counts = {r["verdict"]: r["count"] for r in evidence.groupBy("verdict").count().collect()}
print(json.dumps({"run_id": runId, "verdicts": counts}))
dbutils.notebook.exit(json.dumps({"run_id": runId, "verdicts": counts}))
