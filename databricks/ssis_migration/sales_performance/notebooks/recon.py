# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
"""Reconciliation evidence -> otterorders_migration.evidence.recon_results (one row per package)."""

import json
import os

from sales_performance import config
from sales_performance.common import newRunId
from sales_performance.recon import safeBuildEvidence, summarize, writeEvidence

rootPath = taskParam("landing_root", config.volumeRoot())
outboundDir = taskParam("outbound_dir", os.path.join(config.volumeRoot(), "outbound", "partner_feed"))
gitSha = taskParam("git_sha", "unknown")
runId = newRunId()
rows = safeBuildEvidence(spark, rootPath, outboundDir)
writeEvidence(spark, rows, runId, gitSha)
print(json.dumps({"run_id": runId, "git_sha": gitSha, **summarize(rows)}, indent=2, default=str))
display(spark.sql(f"SELECT unit, verdict, summary FROM {config.evidenceTable('recon_results')} WHERE run_id = '{runId}' ORDER BY unit"))  # noqa: F821
