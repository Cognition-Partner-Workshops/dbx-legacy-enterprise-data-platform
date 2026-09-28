"""ERR_Handle_PackageFailure: failure classification and failed-task discovery."""

import re

# The transient list operations built up over the years: SQL Server deadlock
# (1205) and lock timeout (1222), connection resets (10054, 10060), Oracle
# listener refusals (ORA-12154, ORA-12541) and the file share dropping out
# (Win32 64, 121). Everything else is permanent.
TRANSIENT_ERROR_CODES = frozenset({1205, 1222, 10054, 10060, 12154, 12541, 64, 121})

TRANSIENT = "TRANSIENT"
PERMANENT = "PERMANENT"

_ERROR_CODE_PATTERNS = (
    re.compile(r"ORA-(\d{5})"),
    re.compile(r"\b(?:Msg|Error(?: code)?|SQLSTATE[^0-9]*|errno)\s*[:=]?\s*(\d{2,6})\b", re.I),
    re.compile(r"\berror\s*code\s*[:=]?\s*(\d+)", re.I),
)


def classifyFailure(errorCode):
    """Return (FailureClass, RetryableFlag) exactly like the legacy Classify Failure task."""
    try:
        code = int(errorCode) if errorCode is not None else 0
    except (TypeError, ValueError):
        code = 0
    if code in TRANSIENT_ERROR_CODES:
        return TRANSIENT, 1
    return PERMANENT, 0


def extractErrorCode(message):
    """Best-effort native error code from a task error message; 0 when none is found."""
    if not message:
        return 0
    for pattern in _ERROR_CODE_PATTERNS:
        match = pattern.search(message)
        if match:
            return int(match.group(1))
    return 0


def failedTasksFromRun(run):
    """Failed task summaries from a ``databricks.sdk`` Run object.

    Returns a list of dicts ``{taskKey, runId, resultState}`` for every task
    whose result state is FAILED / TIMED_OUT / INTERNAL_ERROR, excluding the
    failure-handler task itself.
    """
    failed = []
    for task in getattr(run, "tasks", None) or []:
        state = getattr(task, "state", None)
        resultState = getattr(state, "result_state", None)
        name = getattr(resultState, "value", resultState)
        if name in ("FAILED", "TIMED_OUT", "INTERNAL_ERROR", "MAXIMUM_CONCURRENT_RUNS_REACHED"):
            if task.task_key == "ERR_Handle_PackageFailure":
                continue
            failed.append({"taskKey": task.task_key, "runId": task.run_id, "resultState": name})
    return failed
