# Databricks notebook source
# MAGIC %md
# MAGIC "End Step <Phase>" -> `control.endBatchStep` (legacy `etl.usp_EndBatchStep`). The task runs with
# MAGIC `run_if: ALL_DONE`, inspects the phase's child tasks through the Jobs API (`{{job.run_id}}`) and
# MAGIC closes the step `Succeeded` or `Failed`; a failed step raises so the phase's `Failure` edges fire
# MAGIC and its `Success` edges do not - the same outcome as the SSIS sequence container.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
batchStepId = widgetInt("BatchStepId", 0)
stepName = widget("stepName")
childTaskKeys = json.loads(widget("childTaskKeys", "[]") or "[]")

failed: List[str] = []
if childTaskKeys:
    from databricks.sdk import WorkspaceClient

    run = WorkspaceClient().jobs.get_run(int(ctx["jobRunId"]))
    states = {t.task_key: t for t in (run.tasks or [])}
    for k in childTaskKeys:
        task = states.get(k)
        result = None
        if task is not None:
            if task.status is not None and task.status.termination_details is not None:
                result = task.status.termination_details.code.value if task.status.termination_details.code else None
            elif task.state is not None and task.state.result_state is not None:
                result = task.state.result_state.value
        if result != "SUCCESS":
            failed.append(f"{k}={result}")

status = "Failed" if failed else "Succeeded"
if batchStepId > 0:
    control.endBatchStep(spark, ctx["catalog"], batchStepId, status=status)
setTaskValue("StepStatus", status)
variables = readVariables()
print({"stepName": stepName, "status": status, "failed": failed, "edges": evaluateEdges(variables)})
if failed:
    raise RuntimeError(f"step '{stepName}' failed: {', '.join(failed)}")
