"""Generate resources/*.job.yml from config/orchestration-plan.json (+ the SQL Server Agent schedules).

    python tools/generate_jobs.py            # rewrite resources/
    python tools/generate_jobs.py --check    # exit 1 when resources/ is stale

Every plan node becomes one notebook task (phases: ``<key>__start``, one task per child package,
``<key>__end``) calling notebooks/orchestration_task.py; the precedence-constraint semantics are evaluated at
run time by platform_control.orchestration (see README "Orchestration translation").
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from platform_control.config import ALL_OWNED_PACKAGES, GROUP_SLUG, OWNED_PACKAGES  # noqa: E402
from platform_control.orchestration import (  # noqa: E402
    MasterPlan,
    loadSiblingGroups,
    siblingJobName,
    taskKey,
)

RESOURCES = ROOT / "resources"
JOB_PREFIX = f"ssis_{GROUP_SLUG}_"
NOTEBOOK = "../notebooks/orchestration_task.py"
PACKAGE_NOTEBOOK = "../notebooks/run_package.py"

# SQL Server Agent jobs (sqlserver/agent/1x_job_*.sql): schedule, master-step retry, retry interval (min).
AGENT_JOBS: dict[str, dict] = {
    "Master_Daily_ETL": {"agent": "WWI_Daily_ETL", "cron": "0 30 1 * * ?", "retries": 2, "retryMinutes": 15},
    "Master_Hourly_Incremental": {"agent": "WWI_Hourly_Incremental", "cron": "0 0 5-23 * * ?", "retries": 3, "retryMinutes": 2},
    "Master_Intraday_Inventory": {"agent": "WWI_Intraday_Inventory", "cron": "0 0/20 5-21 * * ?", "retries": 2, "retryMinutes": 1},
    "Master_Customer_Sync": {"agent": "WWI_Customer_Sync", "cron": "0 15 0 * * ?", "retries": 2, "retryMinutes": 10},
    "Master_File_Ingestion": {"agent": "WWI_File_Ingestion", "cron": "0 0/15 * * * ?", "retries": 1, "retryMinutes": 3},
    "Master_Weekly_Reference_Load": {"agent": "WWI_Weekly_Reference_Load", "cron": "0 0 3 ? * SUN", "retries": 2, "retryMinutes": 20},
    "Master_Weekly_Maintenance": {"agent": "WWI_Weekly_Maintenance", "cron": "0 0 22 ? * SAT", "retries": 0, "retryMinutes": 0},
    "Master_Month_End": {"agent": "WWI_Month_End", "cron": "0 0 4 * * ?", "retries": 1, "retryMinutes": 30},
    "Master_Finance_Close": {"agent": "WWI_Finance_Close", "cron": "0 0 5 1 * ?", "retries": 1, "retryMinutes": 20},
}
PACKAGE_SCHEDULES = {
    "DQ_Reject_Reprocess": ("WWI_Reject_Reprocess", "0 10 0/4 * * ?"),
    "MNT_Purge_ControlHistory": ("WWI_Control_History_Purge", "0 0 5 ? * SUN"),
}
# Extra SQL Agent steps around the master (not part of the .dtsx) that we keep as job tasks.
AGENT_WRAPPER_TASKS = {
    "Master_Daily_ETL": [("agent_post_load_statistics", "MNT_Update_Statistics", "ALL_SUCCESS")],
    "Master_Weekly_Maintenance": [("agent_maintenance_summary", "ERR_Notify_Operations", "ALL_DONE")],
}

COMMON_PARAMETERS = {
    "job_run_id": "{{job.run_id}}",
    "catalog": "${var.catalog}",
    "schema": "${var.schema}",
    "environment_code": "${var.environment_code}",
    "git_sha": "${var.git_sha}",
    "sibling_dispatch_on_missing": "${var.sibling_dispatch_on_missing}",
}


def _notebookTask(action: str, master: str, node: str = "", package: str = "", notebook: str = NOTEBOOK) -> dict:
    params = {"action": action, "master": master, "node": node, "package": package, **COMMON_PARAMETERS}
    return {"notebook_path": notebook, "source": "WORKSPACE", "base_parameters": params}


def _finalKey(plan: MasterPlan, name: str) -> str:
    return f"{taskKey(name)}__end" if plan.nodes[name]["kind"] == "phase" else taskKey(name)


def buildMasterJob(plan: MasterPlan, siblingGroups: dict[str, str]) -> dict:
    agent = AGENT_JOBS[plan.root]
    tasks: list[dict] = []
    for name in plan.topologicalOrder():
        node = plan.nodes[name]
        key = taskKey(name)
        dependsOn = [{"task_key": _finalKey(plan, e["from"])} for e in plan.predecessors(name)]
        dependsOn = list({d["task_key"]: d for d in dependsOn}.values())
        if node["kind"] != "phase":
            task = {"task_key": key, "notebook_task": _notebookTask("node", plan.root, name)}
            if dependsOn:
                task.update(depends_on=dependsOn, run_if="ALL_DONE")
            tasks.append(task)
            continue
        start = {"task_key": f"{key}__start", "notebook_task": _notebookTask("start_phase", plan.root, name)}
        if dependsOn:
            start.update(depends_on=dependsOn, run_if="ALL_DONE")
        tasks.append(start)
        childKeys = []
        for child in node["children"]:
            package = child["package"]
            childKey = f"{key}__{taskKey(package)}"
            childKeys.append(childKey)
            task = {
                "task_key": childKey,
                "description": f"{'owned package' if package in ALL_OWNED_PACKAGES else 'sibling job ' + siblingJobName(package, siblingGroups)}"
                f" (streams={node.get('streams', 1)})",
                "depends_on": [{"task_key": f"{key}__start"}],
                "run_if": "ALL_DONE",
                "notebook_task": _notebookTask("child", plan.root, name, package),
            }
            if package not in ALL_OWNED_PACKAGES and agent["retries"]:
                task.update(max_retries=agent["retries"], min_retry_interval_millis=agent["retryMinutes"] * 60_000, retry_on_timeout=True)
            tasks.append(task)
        tasks.append(
            {
                "task_key": f"{key}__end",
                "depends_on": [{"task_key": k} for k in childKeys],
                "run_if": "ALL_DONE",
                "notebook_task": _notebookTask("end_phase", plan.root, name),
            }
        )
    sinks = [n for n in plan.nodes if not plan.successors(n)]
    tasks.append(
        {
            "task_key": "finalize_batch",
            "depends_on": [{"task_key": _finalKey(plan, n)} for n in sinks],
            "run_if": "ALL_DONE",
            "notebook_task": _notebookTask("finalize", plan.root),
        }
    )
    for key, package, runIf in AGENT_WRAPPER_TASKS.get(plan.root, []):
        tasks.append(
            {
                "task_key": key,
                "depends_on": [{"task_key": "finalize_batch"}],
                "run_if": runIf,
                "notebook_task": _notebookTask("agent_package", plan.root, "", package),
            }
        )
    tasks.append(
        {
            "task_key": "agent_failure_notice",
            "description": f"SQL Agent {agent['agent']} failure branch: ERR_Notify_Operations",
            "depends_on": [{"task_key": "finalize_batch"}],
            "run_if": "AT_LEAST_ONE_FAILED",
            "notebook_task": _notebookTask("agent_package", plan.root, "", "ERR_Notify_Operations"),
        }
    )
    job = {
        "name": f"{JOB_PREFIX}{plan.root}",
        "description": f"{plan.description} Generated from config/orchestration-plan.json by tools/generate_jobs.py; "
        f"legacy SQL Agent job {agent['agent']}.",
        "tags": {"ssis_group": GROUP_SLUG, "ssis_project": plan.project, "legacy_agent_job": agent["agent"], "ssis_package": plan.root},
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "schedule": {"quartz_cron_expression": agent["cron"], "timezone_id": "UTC", "pause_status": "${var.schedule_pause_status}"},
        "email_notifications": {"on_failure": ["${var.operator_email}"], "no_alert_for_skipped_runs": True},
        "parameters": [{"name": k, "default": str(v)} for k, v in plan.parameters.items()],
        "tasks": tasks,
    }
    if plan.root == "Master_Month_End":
        job["parameters"].append({"name": "AgentGateOverride", "default": "false"})
    if "MaintenanceWindowMinutes" in plan.parameters:
        job["timeout_seconds"] = int(plan.parameters["MaintenanceWindowMinutes"]) * 60
    return job


def buildPackageJobs() -> dict[str, dict]:
    jobs: dict[str, dict] = {}
    for group, packages in OWNED_PACKAGES.items():
        if group == "orchestration":
            continue
        for package in packages:
            job = {
                "name": f"{JOB_PREFIX}{package}",
                "description": f"Standalone run of {package} ({group}) against the Delta control tables.",
                "tags": {"ssis_group": GROUP_SLUG, "ssis_package": package, "package_group": group},
                "max_concurrent_runs": 1,
                "email_notifications": {"on_failure": ["${var.operator_email}"]},
                "parameters": [{"name": "batch_id", "default": ""}, {"name": "parameters_json", "default": "{}"}],
                "tasks": [
                    {
                        "task_key": taskKey(package),
                        "max_retries": 1,
                        "min_retry_interval_millis": 300_000,
                        "notebook_task": _notebookTask("standalone", "", "", package, notebook=PACKAGE_NOTEBOOK),
                    }
                ],
            }
            if package in PACKAGE_SCHEDULES:
                agentName, cron = PACKAGE_SCHEDULES[package]
                job["tags"]["legacy_agent_job"] = agentName
                job["schedule"] = {"quartz_cron_expression": cron, "timezone_id": "UTC", "pause_status": "${var.schedule_pause_status}"}
            jobs[f"{JOB_PREFIX}{package}"] = job
    return jobs


def buildPlatformJobs() -> dict[str, dict]:
    setupTask = {"task_key": "setup", "notebook_task": _notebookTask("setup", "", notebook="../notebooks/setup.py")}
    reconTask = {"task_key": "recon", "notebook_task": _notebookTask("recon", "", notebook="../notebooks/recon.py")}
    packageTasks = [
        {
            "task_key": taskKey(package),
            "depends_on": [{"task_key": "setup"}],
            "notebook_task": _notebookTask("standalone", "", "", package, notebook=PACKAGE_NOTEBOOK),
        }
        for group, packages in OWNED_PACKAGES.items()
        if group != "orchestration"
        for package in packages
    ]
    e2eRecon = dict(reconTask, depends_on=[{"task_key": t["task_key"]} for t in packageTasks], run_if="ALL_DONE")
    common = {"max_concurrent_runs": 1, "tags": {"ssis_group": GROUP_SLUG}, "email_notifications": {"on_failure": ["${var.operator_email}"]}}
    return {
        f"{JOB_PREFIX}setup": {
            "name": f"{JOB_PREFIX}setup",
            "description": "Create the Delta control tables + landing volume, seed them from wwi_legacy_staging, upload sample files.",
            "tasks": [setupTask],
            **common,
        },
        f"{JOB_PREFIX}recon": {
            "name": f"{JOB_PREFIX}recon",
            "description": "Re-runnable reconciliation: writes one evidence row per owned package to evidence.recon_results.",
            "parameters": [{"name": "git_sha", "default": "${var.git_sha}"}],
            "tasks": [reconTask],
            **common,
        },
        f"{JOB_PREFIX}e2e_smoke": {
            "name": f"{JOB_PREFIX}e2e_smoke",
            "description": "End-to-end: setup -> every owned DQ/ERR/MNT package standalone -> recon evidence.",
            "parameters": [{"name": "git_sha", "default": "${var.git_sha}"}],
            "tasks": [setupTask, *packageTasks, e2eRecon],
            **common,
        },
    }


def render(jobs: dict[str, dict]) -> str:
    header = "# GENERATED by tools/generate_jobs.py from config/orchestration-plan.json - do not edit by hand.\n"
    return header + yaml.safe_dump({"resources": {"jobs": jobs}}, sort_keys=False, width=1000, allow_unicode=True)


def generate() -> dict[str, str]:
    siblingGroups = loadSiblingGroups()
    files: dict[str, str] = {}
    for plan in MasterPlan.loadAll():
        job = buildMasterJob(plan, siblingGroups)
        files[f"{plan.root.lower()}.job.yml"] = render({job["name"]: job})
    files["packages.job.yml"] = render(buildPackageJobs())
    files["platform.job.yml"] = render(buildPlatformJobs())
    return files


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    files = generate()
    RESOURCES.mkdir(exist_ok=True)
    stale = []
    for name, content in files.items():
        path = RESOURCES / name
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                stale.append(name)
        else:
            path.write_text(content, encoding="utf-8")
            print(f"wrote {path.relative_to(ROOT)} ({content.count('task_key:')} tasks)")
    if stale:
        print("stale: " + ", ".join(stale))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
