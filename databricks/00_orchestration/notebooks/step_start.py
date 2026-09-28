# Databricks notebook source
# MAGIC %md
# MAGIC "Start Step <Phase>" -> `control.startBatchStep` (legacy `etl.usp_StartBatchStep`); publishes task
# MAGIC value `BatchStepId`. With `restartRecovery=true` (the plan's "Restart Recovery" phase) it also
# MAGIC marks every step that precedes `RestartFromStep` as `Skipped` in `etl.batch_step` and logs an
# MAGIC Information entry, mirroring the runner's resume bookkeeping.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
batchId = widgetInt("BatchId", 0)
if batchId <= 0:
    raise ValueError("BatchId could not be resolved from the Start Batch task")
stepName = widget("stepName")
stepSequence = widgetInt("stepSequence", 0)
stepGroup = widget("stepGroup", "") or None

batchStepId = control.startBatchStep(spark, ctx["catalog"], batchId, stepName, stepSequence, stepGroup=stepGroup)
setTaskValue("BatchStepId", batchStepId)

if widget("restartRecovery", "false") == "true":
    restartFromStep = widget("RestartFromStep", "")
    planSteps = json.loads(widget("planSteps", "[]") or "[]")
    if not isUnresolved(restartFromStep):
        if restartFromStep not in planSteps:
            raise ValueError(f"RestartFromStep '{restartFromStep}' is not a phase of {ctx['masterName']}: {planSteps}")
        skipped = []
        for seq, name in enumerate(planSteps, start=1):
            if name == restartFromStep:
                break
            if name == stepName:
                continue
            skipped.append(name)
            sid = control.startBatchStep(spark, ctx["catalog"], batchId, name, seq * 10, stepGroup="Skipped")
            control.endBatchStep(spark, ctx["catalog"], sid, status="Skipped")
        control.logError(
            spark, ctx["catalog"], batchId=batchId, errorSeverity="Information", errorCode=0,
            sourceName=ctx["masterName"], sourceComponent="Restart Recovery", procedureName="step_start",
            errorDescription=f"Resumed from step '{restartFromStep}'; skipped: {', '.join(skipped) or '(none)'}",
        )
        print({"restartFromStep": restartFromStep, "skipped": skipped})
print({"batchId": batchId, "batchStepId": batchStepId, "stepName": stepName})
