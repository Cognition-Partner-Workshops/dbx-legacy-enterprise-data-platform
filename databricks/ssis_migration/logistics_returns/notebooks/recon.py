# Databricks notebook source
# MAGIC %md
# MAGIC # Reconciliation evidence
# MAGIC One row per package (single `run_id`) appended to `otterorders_migration.evidence.recon_results`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from logistics_returns.recon import runRecon

ctx = buildContext()
runId, evidence = runRecon(ctx)
print(f"run_id={runId} git_sha={ctx.gitSha}")
display(evidence.select("unit", "verdict", "target_object", "summary"))

# COMMAND ----------

failures = [r["unit"] for r in evidence.where("verdict = 'FAIL'").select("unit").collect()]
print(
    {
        "run_id": runId,
        "verdicts": {r["verdict"]: r["count"] for r in evidence.groupBy("verdict").count().collect()},
        "failed": failures,
    }
)
