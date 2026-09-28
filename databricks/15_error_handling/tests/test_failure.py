from types import SimpleNamespace

from err_handling import failure


def test_transient_codes_match_legacy_list():
    assert failure.TRANSIENT_ERROR_CODES == {1205, 1222, 10054, 10060, 12154, 12541, 64, 121}


def test_classify_transient_and_permanent():
    assert failure.classifyFailure(1205) == ("TRANSIENT", 1)
    assert failure.classifyFailure("12541") == ("TRANSIENT", 1)
    assert failure.classifyFailure(2627) == ("PERMANENT", 0)
    assert failure.classifyFailure(None) == ("PERMANENT", 0)
    assert failure.classifyFailure("not-a-number") == ("PERMANENT", 0)


def test_extract_error_code_from_messages():
    assert failure.extractErrorCode("ORA-12541: TNS:no listener") == 12541
    assert failure.extractErrorCode("Msg 1205, Level 13, State 45: deadlock victim") == 1205
    assert failure.extractErrorCode("Error code 10054 connection reset") == 10054
    assert failure.extractErrorCode("something else entirely") == 0
    assert failure.extractErrorCode(None) == 0


def _task(key, state, runId=1):
    return SimpleNamespace(task_key=key, run_id=runId, state=SimpleNamespace(result_state=SimpleNamespace(value=state)))


def test_failed_tasks_from_run_excludes_handler_and_successes():
    run = SimpleNamespace(tasks=[
        _task("EXT_ORA_CustomerMaster", "FAILED", 11),
        _task("EXT_SQL_Orders", "SUCCESS", 12),
        _task("ERR_Handle_PackageFailure", "FAILED", 13),
        _task("STG_Load", "TIMED_OUT", 14),
    ])
    failed = failure.failedTasksFromRun(run)
    assert [f["taskKey"] for f in failed] == ["EXT_ORA_CustomerMaster", "STG_Load"]
    assert failed[0]["runId"] == 11 and failed[1]["resultState"] == "TIMED_OUT"
