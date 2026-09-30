# Databricks notebook source
# One task of a ssis_platform_control_Master_* job. Widgets: action, master, node, package, job_run_id,
# plus the SSIS $Package:: parameters as job parameters. Semantics live in platform_control.orchestration.
# MAGIC %run ./_bootstrap

# COMMAND ----------
import json

from platform_control.control import ControlFramework
from platform_control.orchestration import (
    RUNNING,
    MasterPlan,
    Orchestrator,
    databricksSiblingRunner,
    fiscalCalendarGate,
)
from platform_control.runners import runPackage
from platform_control.spark import getSpark

cfg = platformConfig()
spark = getSpark()
cf = ControlFramework(spark, cfg)

action = widget("action", "node")
master = widget("master")
nodeName = widget("node")
package = widget("package")
jobRunId = widget("job_run_id") or "local"

plan = MasterPlan.load(master)
parameters = {name: widget(name, str(default)) for name, default in plan.parameters.items()}
gate = None
if master == "Master_Month_End":
    gate = fiscalCalendarGate(cf, override=widget("AgentGateOverride", "false").lower() == "true")

orchestrator = Orchestrator(
    cf,
    plan,
    jobRunId,
    parameters=parameters,
    siblingRunner=databricksSiblingRunner(cfg),
    packageRunner=lambda pkg, context: runPackage(cf, pkg, context),
    batchGate=gate,
)

# COMMAND ----------
if action == "node":
    outcome = orchestrator.runNode(nodeName)
elif action == "start_phase":
    outcome = orchestrator.runNode(nodeName)
    if outcome != RUNNING:
        print(f"phase '{nodeName}' not started: {outcome}")
elif action == "child":
    outcome = orchestrator.runChild(nodeName, package)
elif action == "end_phase":
    outcome = orchestrator.endPhase(nodeName)
elif action == "finalize":
    outcome = orchestrator.finalizeBatch()
elif action == "agent_package":
    batchId = orchestrator.batchId()
    outcome = runPackage(cf, package, {"batchId": batchId, "batchStepId": None, "packageExecutionId": None, "parameters": parameters, "variables": {}}) if batchId else "no batch"
else:
    raise ValueError(f"Unknown action {action!r}")

print(json.dumps({"action": action, "master": master, "node": nodeName, "package": package, "outcome": outcome}, default=str))
dbutils.notebook.exit(json.dumps({"outcome": outcome}, default=str))
