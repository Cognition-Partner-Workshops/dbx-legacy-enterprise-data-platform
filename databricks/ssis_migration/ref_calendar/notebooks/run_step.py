# Databricks notebook source
# MAGIC %md
# MAGIC # ssis_ref_calendar – package group runner
# MAGIC Thin entrypoint: every job task calls this notebook with a `step` parameter and the work happens in `src/ref_calendar`.

# COMMAND ----------
import os
import shutil
import sys

dbutils.widgets.text("step", "extracts")
dbutils.widgets.text("git_sha", "unknown")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("fx_window_from", "")
dbutils.widgets.text("fx_window_to", "")

step = dbutils.widgets.get("step")
gitSha = dbutils.widgets.get("git_sha")
fxWindowFrom = dbutils.widgets.get("fx_window_from") or None
fxWindowTo = dbutils.widgets.get("fx_window_to") or None

notebookDir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
bundleRoot = os.path.dirname("/Workspace" + notebookDir)
sys.path.insert(0, os.path.join(bundleRoot, "src"))

from ref_calendar import city_scd2, common, config, dimensions, extracts, fx_override, recon, reference, staging  # noqa: E402

batchId = int(dbutils.widgets.get("batch_id") or common.newBatchId())
print(f"step={step} batch_id={batchId} git_sha={gitSha} bundle_root={bundleRoot}")

# COMMAND ----------
if step == "seed_landing":
    inbound = os.path.join(config.LANDING_VOLUME_PATH, fx_override.INBOUND)
    os.makedirs(inbound, exist_ok=True)
    samples = os.path.join(bundleRoot, "samples", "fx_override")
    copied = []
    already = set(os.listdir(inbound)) | set(os.listdir(os.path.join(config.LANDING_VOLUME_PATH, fx_override.ARCHIVE)) if os.path.isdir(os.path.join(config.LANDING_VOLUME_PATH, fx_override.ARCHIVE)) else set()) | set(os.listdir(os.path.join(config.LANDING_VOLUME_PATH, fx_override.QUARANTINE)) if os.path.isdir(os.path.join(config.LANDING_VOLUME_PATH, fx_override.QUARANTINE)) else set())
    for name in sorted(os.listdir(samples)):
        if name.endswith(".csv") and name not in already:
            shutil.copy(os.path.join(samples, name), os.path.join(inbound, name))
            copied.append(name)
    result = {"copied": copied}
elif step == "extracts":
    result = extracts.runAllExtracts(spark, batchId, fxWindowFrom, fxWindowTo)
elif step == "fx_override":
    result = fx_override.runIngFileFxOverride(spark, batchId)
elif step == "reference":
    result = reference.runReferenceLayer(spark, batchId)
elif step == "staging":
    result = {}
    result.update(staging.runStgLoadCurrency(spark, batchId))
    result.update(staging.runStgLoadGeography(spark, batchId))
    result.update(staging.runStgLoadTaxAndTerms(spark, batchId))
elif step == "dimensions":
    result = dimensions.runRefLoadDimensions(spark, batchId)
elif step == "city":
    result = city_scd2.runDimLoadCity(spark, batchId)
elif step == "recon":
    evidence = recon.runRecon(spark, gitSha)
    display(evidence.select("unit", "verdict", "summary"))
    result = {r["verdict"]: r["count"] for r in evidence.groupBy("verdict").count().collect()}
else:
    raise ValueError(f"unknown step {step}")

print(result)
dbutils.notebook.exit(str(result))
