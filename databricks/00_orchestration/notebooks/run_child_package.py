# Databricks notebook source
# MAGIC %md
# MAGIC One Execute Package Task -> run the owning project's job `wwi_<NN_project>` for just that package
# MAGIC (`run_now(only=[<Package>])`, Jobs API 2.2) with the plan's parameter assignments as
# MAGIC `job_parameters`, and wait for it. With `skipIfSucceeded=true` (retry phases) the package is
# MAGIC skipped when `etl.package_execution` already holds a `Succeeded` row for this batch, which is
# MAGIC what the legacy retry pass did through `Invoke-EstatePackage`. The child's own notebook logs its
# MAGIC `etl.package_execution` row through `control.packageRun`, so nothing is double-logged here.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
import time

from databricks.sdk import WorkspaceClient

project = widget("project")
projectJob = widget("projectJob")
package = widget("package")
jobParameters = json.loads(widget("jobParameters", "{}") or "{}")
extraJobParameters = json.loads(widget("extraJobParameters", "{}") or "{}")
skipIfSucceeded = widget("skipIfSucceeded", "false") == "true"

for k, v in list(jobParameters.items()):
    if isinstance(v, str) and v.startswith("{{"):
        raise ValueError(f"job parameter {k} for {package} is unresolved: {v}")
batchId = int(float(jobParameters.get("BatchId", "0") or 0))
if batchId <= 0:
    raise ValueError("BatchId could not be resolved from the Start Batch task")

if skipIfSucceeded:
    done = spark.sql(
        f"SELECT count(*) FROM {naming.controlTable(ctx['catalog'], 'package_execution')} "
        "WHERE BatchId = :b AND PackageName = :p AND Status = 'Succeeded'",
        args={"b": batchId, "p": package},
    ).first()[0]
    if done:
        setTaskValue("ChildResult", "SKIPPED")
        print({"package": package, "skipped": True, "reason": "already Succeeded in this batch"})
        dbutils.notebook.exit("SKIPPED")  # noqa: F821

w = WorkspaceClient()
jobs = [j for j in w.jobs.list(name=projectJob)]
if len(jobs) != 1:
    raise LookupError(f"expected exactly one job named {projectJob} (project {project}); found {len(jobs)}")
params = {k: str(v) for k, v in jobParameters.items()}
if extraJobParameters:
    print({"note": "plan parameters without a matching job parameter on the child job are logged, not passed",
           "extraJobParameters": extraJobParameters})

waiter = w.jobs.run_now(job_id=jobs[0].job_id, job_parameters=params, only=[package])
print({"package": package, "childJob": projectJob, "runId": waiter.run_id, "jobParameters": params})
run = waiter.result()
state = run.status.termination_details.code.value if run.status and run.status.termination_details and run.status.termination_details.code else (
    run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN")
setTaskValue("ChildResult", state)
setTaskValue("ChildRunId", waiter.run_id)
if state != "SUCCESS":
    raise RuntimeError(f"child package {package} (job {projectJob}, run {waiter.run_id}) ended {state}: {run.run_page_url}")
