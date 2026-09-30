# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
dbutils.widgets.text("git_sha", "", "Git SHA of the deployed code")
dbutils.widgets.text("run_id", "", "Evidence run id (UUID; generated when empty)")

# COMMAND ----------
from procurement.recon import runRecon, summarize

runId, rows = runRecon(spark, dbutils.widgets.get("git_sha") or "unknown", dbutils.widgets.get("run_id").strip() or None)
print({"run_id": runId, "verdicts": summarize(rows)})
for r in rows:
    print(f"{r[4]:<15} {r[2]:<32} {r[9][:160]}")
