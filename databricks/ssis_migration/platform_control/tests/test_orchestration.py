import json

import pytest

from platform_control import orchestration as orch
from platform_control.config import ALL_OWNED_PACKAGES, OWNED_PACKAGES
from platform_control.control import ControlFramework
from platform_control.orchestration import (
    FAILED,
    RUNNING,
    SKIPPED,
    SUCCEEDED,
    MasterPlan,
    Orchestrator,
    shouldRun,
)


@pytest.fixture(scope="module")
def plans():
    return {p.root: p for p in MasterPlan.loadAll()}


def testPlanCoversAllMastersAndIsAcyclic(plans):
    assert sorted(plans) == sorted(OWNED_PACKAGES["orchestration"])
    groups = orch.loadSiblingGroups()
    for plan in plans.values():
        order = plan.topologicalOrder()
        assert len(order) == len(plan.nodes)
        for package in plan.childPackages():
            assert package in ALL_OWNED_PACKAGES or package in groups, f"{package} has no owner"
            assert orch.siblingJobName(package, groups).startswith("ssis_")


def testShouldRunSemantics(plans):
    daily = plans["Master_Daily_ETL"]
    params = dict(daily.parameters)
    # Stage Load has two incoming edges: File Screen (Success + restart expression) and File Quarantine (Completion).
    ok, _ = shouldRun(daily, "Stage Load", {"File Screen": SUCCEEDED}, {}, params)
    assert ok
    # File Screen failed but branched to File Quarantine -> Stage Load runs once File Quarantine completes.
    ok, _ = shouldRun(daily, "Stage Load", {"File Screen": FAILED, "File Quarantine": SUCCEEDED}, {}, params)
    assert ok
    # A failed predecessor that did not branch elsewhere blocks the successor.
    ok, reason = shouldRun(daily, "Stage Work Tables", {"Stage Load": FAILED}, {}, params)
    assert not ok and reason
    # Nothing satisfied -> skipped
    ok, _ = shouldRun(daily, "Stage Load", {"File Screen": SKIPPED}, {}, params)
    assert not ok
    weekly = plans["Master_Weekly_Reference_Load"]
    calendarPreds = [e["from"] for e in weekly.predecessors("Calendar")]
    if len(calendarPreds) >= 2:
        statuses = {calendarPreds[0]: SUCCEEDED, calendarPreds[1]: FAILED}
        ok, _ = shouldRun(weekly, "Calendar", statuses, {}, weekly.parameters)
        assert not ok


def testConditionalEdgeUsesVariablesAndParameters(plans):
    hourly = plans["Master_Hourly_Incremental"]
    params = dict(hourly.parameters)
    ok, _ = shouldRun(hourly, "Log Stand Down", {"Check Nightly Batch": SUCCEEDED}, {"NightlyBatchRunning": 1}, params)
    assert ok
    ok, _ = shouldRun(hourly, "Start Batch", {"Check Nightly Batch": SUCCEEDED}, {"NightlyBatchRunning": 1}, params)
    assert not ok
    ok, _ = shouldRun(hourly, "Start Batch", {"Check Nightly Batch": SUCCEEDED}, {"NightlyBatchRunning": 0}, params)
    assert ok


def _orchestrator(spark, cfg, root, runId, siblingRunner, packageRunner=None, **kw):
    cf = ControlFramework(spark, cfg)
    return Orchestrator(cf, MasterPlan.load(root), runId, siblingRunner=siblingRunner, packageRunner=packageRunner or (lambda p, c: {"rowsRead": 1}), **kw)


def testHourlyHappyPath(spark, cfg):
    o = _orchestrator(spark, cfg, "Master_Hourly_Incremental", "run-hourly-ok", lambda job, params: SUCCEEDED)
    statuses = o.runAll()
    assert statuses["Check Nightly Batch"] == SUCCEEDED
    assert statuses["Log Stand Down"] == SKIPPED
    assert statuses["Incremental Extract"] == SUCCEEDED
    assert statuses["Incremental Extract Retry"] == SKIPPED
    assert statuses["Intraday Failure Notice"] == SKIPPED
    assert statuses["End Batch"] == SUCCEEDED
    batch = spark.sql(f"SELECT status FROM {cfg.table('etl_batch')} WHERE job_run_id = 'run-hourly-ok'").first()
    assert batch["status"] in ("Succeeded", "SucceededWithWarnings")
    steps = spark.sql(f"SELECT s.status FROM {cfg.table('etl_batch_step')} s JOIN {cfg.table('etl_batch')} b USING (batch_id) WHERE b.job_run_id = 'run-hourly-ok'").collect()
    assert steps and all(s["status"] == "Succeeded" for s in steps)


def testHourlyExtractFailureTakesRetryAndFailurePaths(spark, cfg):
    def sibling(jobName, params):
        return FAILED if jobName.endswith("_EXT_SQL_Orders") else SUCCEEDED

    o = _orchestrator(spark, cfg, "Master_Hourly_Incremental", "run-hourly-fail", sibling)
    with pytest.raises(RuntimeError):
        o.runAll(raiseOnFailure=True)
    statuses = o.statuses()
    assert statuses["Incremental Extract"] == FAILED
    assert statuses["Increment Extract Attempt"] == SUCCEEDED
    assert statuses["Incremental Extract Retry"] == FAILED
    assert statuses["Intraday Failure Notice"] in (SUCCEEDED, FAILED)
    assert statuses["Incremental Stage"] == SKIPPED
    batch = spark.sql(f"SELECT status FROM {cfg.table('etl_batch')} WHERE job_run_id = 'run-hourly-fail'").first()
    assert batch["status"] == "Failed"
    assert o.variables()["ExtractAttempt"] == 2


def testHourlyStandsDownWhenDailyRunning(spark, cfg):
    cf = ControlFramework(spark, cfg)
    dailyId = cf.startBatch("Master_Daily_ETL", "Daily", notes="blocking", jobRunId="daily-blocking")
    try:
        o = _orchestrator(spark, cfg, "Master_Hourly_Incremental", "run-hourly-standdown", lambda j, p: SUCCEEDED)
        statuses = o.runAll()
        assert statuses["Log Stand Down"] == SUCCEEDED
        assert statuses["Start Batch"] == SKIPPED
        assert statuses.get("End Batch", SKIPPED) == SKIPPED
        assert spark.sql(f"SELECT COUNT(*) AS n FROM {cfg.table('etl_batch')} WHERE job_run_id = 'run-hourly-standdown'").first()["n"] == 0
    finally:
        cf.endBatch(dailyId, forceStatus="Cancelled")


def testUnresolvedSiblingIsSkippedNotFailed(spark, cfg):
    # Weekly Reference Load is all sibling (REF_*) packages: none deployed yet -> warnings, batch still succeeds
    o = _orchestrator(spark, cfg, "Master_Weekly_Reference_Load", "run-ref-unresolved", lambda j, p: orch.UNRESOLVED)
    statuses = o.runAll()
    assert statuses["End Batch"] == SUCCEEDED
    warnings = spark.sql(
        f"SELECT COUNT(*) AS n FROM {cfg.table('etl_error_log')} e JOIN {cfg.table('etl_batch')} b USING (batch_id) "
        "WHERE b.job_run_id = 'run-ref-unresolved' AND e.error_severity = 'Warning'"
    ).first()["n"]
    assert warnings > 0
    assert spark.sql(f"SELECT status FROM {cfg.table('etl_batch')} WHERE job_run_id = 'run-ref-unresolved'").first()["status"] == "SucceededWithWarnings"
    # Weekly Maintenance is entirely our own packages: they run through the package runner and are recorded as package executions
    o = _orchestrator(spark, cfg, "Master_Weekly_Maintenance", "run-weekly-unresolved", lambda j, p: orch.UNRESOLVED)
    statuses = o.runAll()
    assert statuses["End Batch"] == SUCCEEDED
    own = spark.sql(
        f"SELECT package_name FROM {cfg.table('etl_package_execution')} pe JOIN {cfg.table('etl_batch')} b USING (batch_id) WHERE b.job_run_id = 'run-weekly-unresolved'"
    ).collect()
    assert {r["package_name"] for r in own} >= {"MNT_Purge_ControlHistory", "MNT_Update_Statistics"}


def testMonthEndGateSkipsWholeRun(spark, cfg):
    o = _orchestrator(spark, cfg, "Master_Month_End", "run-monthend-gated", lambda j, p: SUCCEEDED, batchGate=lambda: (False, "not a close date"))
    statuses = o.runAll()
    assert statuses["Start Batch"] == SKIPPED and all(s == SKIPPED for s in statuses.values())


def testGeneratedResourcesAreCurrentAndMatchPlan():
    import generate_jobs

    files = generate_jobs.generate()
    for name, content in files.items():
        assert (generate_jobs.RESOURCES / name).read_text(encoding="utf-8") == content, f"{name} is stale: run tools/generate_jobs.py"
    import yaml

    from platform_control.recon import _jobEdges

    for plan in MasterPlan.loadAll():
        job = yaml.safe_load(files[f"{plan.root.lower()}.job.yml"])["resources"]["jobs"][f"ssis_platform_control_{plan.root}"]
        edges = {(orch.taskKey(e["from"]), orch.taskKey(e["to"])) for e in plan.edges}
        assert _jobEdges(job) == edges
        keys = [t["task_key"] for t in job["tasks"]]
        assert len(keys) == len(set(keys))
        assert job["schedule"]["quartz_cron_expression"]
        assert job["email_notifications"]["on_failure"]
        assert {p["name"] for p in job["parameters"]} >= set(plan.parameters)


def testStatusesRoundTripThroughVariables(spark, cfg):
    cf = ControlFramework(spark, cfg)
    cf.setVariable("rt", "node_status:X", RUNNING)
    cf.setVariable("rt", "payload", {"a": 1})
    assert cf.getVariables("rt")["payload"] == json.loads('{"a": 1}')
