"""Emit the WWI_ErrorHandling utility packages (ssis/15_error_handling).

These are the packages the estate reaches for when something has gone wrong:
the central failure handler, the reject router, the operator notification writer,
the bounded retry driver, the bad-file quarantine and the row-count
reconciliation. They are utilities - they take a batch id and work on the
control tables rather than loading business data.

ERR_Retry_FailedSteps used to be reachable only from the four-hourly recovery
Agent job. Master_Daily_ETL now drives it twice: once from the Restart Recovery
container, which reopens the retryable steps of an adopted batch before the
first extract phase, and once from the Extract Retry Driver container on the
failure path of the extract phases, where MaxRetryAttempts is bound to the
master's MaxExtractAttempts and the attempt number is written back through
etl.BatchStep. ERR_Quarantine_BadFiles is likewise on the failure path of the
nightly File Screen container as well as the file ingestion master.

Run:  python3 ssis/15_error_handling/build_error_handling_packages.py
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import (CONN_DW, CONN_FILES, CONN_STAGING, log_package_start,  # noqa: E402
                      log_package_success, log_row_count, new_package, truncate)
from ssisgen import (Column, Container, DataFlow, DataFlowTask, ExecuteSql,  # noqa: E402
                     Expression, FileSystemTask, date_col, int_col, money_col,
                     str_col)

PROJECT_NAME = "WWI_ErrorHandling"
CONNECTIONS = ["WWI_Staging_DB", "WWI_DW_Destination_DB", "WWI_Inbound_Files",
               "WWI_Archive_Files", "WWI_Reject_Files"]


def bool_col(name):
    return Column(name, "bool")


# ---------------------------------------------------------------------------
# ERR_Handle_PackageFailure
# ---------------------------------------------------------------------------


def build_err_handle_packagefailure():
    pkg = new_package(
        "ERR_Handle_PackageFailure",
        "Central failure handler. Child packages call this after an unhandled error so that the "
        "error is written once to etl.ErrorLog, the open batch step and the package execution row "
        "are closed as failed, and the failure is classified as transient or permanent from the "
        "error code. Transient failures are marked retryable so ERR_Retry_FailedSteps can pick "
        "them up; permanent ones fail the batch.",
        connections=(CONN_DW,),
        extra_variables=[("FailedPackageName", "", "string"), ("FailureClass", "PERMANENT", "string"),
                         ("FailedStepId", 0, "int"), ("RetryableFlag", 0, "int")],
    )
    pkg.add_parameter("FailedPackage", "", dtype="string",
                      description="Name of the package that failed.")
    pkg.add_parameter("FailureErrorCode", 0, dtype="int",
                      description="Native error code captured by the failing package.")
    pkg.add_parameter("FailureMessage", "", dtype="string",
                      description="Error description captured by the failing package.")
    pkg.add_parameter("FailBatchOnPermanent", "True", dtype="bool",
                      description="Permanent failures end the batch as failed.")

    start = pkg.add(log_package_start(pkg))
    capture = pkg.add(Expression(
        "Capture Failed Package",
        "@[User::FailedPackageName] = @[$Package::FailedPackage]",
    ))
    log_error = pkg.add(ExecuteSql(
        "Log Error",
        CONN_STAGING,
        "EXEC etl.usp_LogError @BatchId = ?, @PackageName = ?, @ErrorCode = ?, "
        "@ErrorDescription = ?, @SourceComponent = N'ERR_Handle_PackageFailure';",
        is_stored_procedure=False,
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::FailedPackage", 1, "NVARCHAR"),
            ("$Package::FailureErrorCode", 2, "LONG"),
            ("$Package::FailureMessage", 3, "NVARCHAR"),
        ],
    ))
    # The transient list is the one operations built up over the years: dead
    # locks, connection resets, Oracle listener refusals and the file share
    # dropping out. Everything else is treated as permanent.
    classify = pkg.add(ExecuteSql(
        "Classify Failure",
        CONN_STAGING,
        "SELECT CASE WHEN ? IN (1205, 1222, 10054, 10060, 12154, 12541, 64, 121) "
        "            THEN N'TRANSIENT' ELSE N'PERMANENT' END AS FailureClass, "
        "       CASE WHEN ? IN (1205, 1222, 10054, 10060, 12154, 12541, 64, 121) "
        "            THEN 1 ELSE 0 END AS RetryableFlag;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[
            ("$Package::FailureErrorCode", 0, "LONG"),
            ("$Package::FailureErrorCode", 1, "LONG"),
        ],
        result_bindings=[("0", "User::FailureClass"), ("1", "User::RetryableFlag")],
    ))
    close_step = pkg.add(ExecuteSql(
        "Close Failed Batch Step",
        CONN_STAGING,
        "UPDATE etl.BatchStep "
        "SET    StepStatus = N'Failed', EndedAtUtc = SYSUTCDATETIME(), "
        "       IsRetryable = ?, ErrorMessage = ? "
        "WHERE  BatchId = ? AND StepStatus = N'Running' AND PackageName = ?;",
        parameter_bindings=[
            ("User::RetryableFlag", 0, "LONG"),
            ("$Package::FailureMessage", 1, "NVARCHAR"),
            ("$Package::BatchId", 2, "LONG"),
            ("$Package::FailedPackage", 3, "NVARCHAR"),
        ],
    ))
    close_exec = pkg.add(ExecuteSql(
        "Close Package Execution",
        CONN_STAGING,
        "UPDATE etl.PackageExecution "
        "SET    ExecutionStatus = N'Failed', EndedAtUtc = SYSUTCDATETIME() "
        "WHERE  BatchId = ? AND PackageName = ? AND ExecutionStatus = N'Running';",
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::FailedPackage", 1, "NVARCHAR"),
        ],
    ))
    fail_batch = pkg.add(ExecuteSql(
        "Fail Batch",
        CONN_STAGING,
        "EXEC etl.usp_EndBatch @BatchId = ?, @ForceStatus = N'Failed';",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    mark_retryable = pkg.add(ExecuteSql(
        "Mark Batch Retryable",
        CONN_STAGING,
        "UPDATE etl.Batch SET BatchStatus = N'RetryPending' "
        "WHERE BatchId = ? AND BatchStatus = N'Running';",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("etl.ErrorLog"))
    done = pkg.add(log_package_success())

    pkg.link(start, capture)
    pkg.link(capture, log_error)
    pkg.link(log_error, classify, value="Completion")
    pkg.link(classify, close_step)
    pkg.link(close_step, close_exec)
    pkg.link(close_exec, fail_batch,
             expression='@[User::FailureClass] == "PERMANENT" '
                        '&& @[$Package::FailBatchOnPermanent]')
    pkg.link(close_exec, mark_retryable, expression='@[User::FailureClass] == "TRANSIENT"')
    pkg.link(fail_batch, counts, value="Completion")
    pkg.link(mark_retryable, counts, value="Completion")
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# ERR_Route_RejectedRows
# ---------------------------------------------------------------------------


def build_err_route_rejectedrows():
    pkg = new_package(
        "ERR_Route_RejectedRows",
        "Routes the per-object reject tables in the err schema onto the error file share and "
        "registers each rejected row in etl.RejectedRecord so the data stewards work from one "
        "queue. Rows that a steward has already resolved are not re-routed, and rejects older "
        "than the escalation age are raised to the domain owner.",
        connections=(CONN_FILES,),
        extra_variables=[("RejectFileName", "rejects.csv", "string"), ("RoutedRowCount", 0, "int"),
                         ("EscalatedCount", 0, "int")],
    )
    pkg.add_parameter("RejectEscalationDays", 5, dtype="int",
                      description="Age at which an unresolved reject is escalated.")
    pkg.add_parameter("ObjectScope", "ALL", dtype="string",
                      description="ALL, or a single object name to re-route.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.RejectRoutingSet"))
    gather = pkg.add(ExecuteSql(
        "Gather Unrouted Rejects",
        CONN_STAGING,
        "INSERT INTO work.RejectRoutingSet\n"
        "    (RejectedRecordId, BatchId, ObjectName, RejectStage, RejectReasonCode,\n"
        "     RejectReasonDescription, SourceKey, RejectedAtUtc, AgeDays)\n"
        "SELECT  r.RejectedRecordId\n"
        ",       r.BatchId\n"
        ",       r.ObjectName\n"
        ",       r.RejectStage\n"
        ",       r.RejectReasonCode\n"
        ",       r.RejectReason\n"
        ",       r.BusinessKey\n"
        ",       r.LoggedAtUtc\n"
        ",       DATEDIFF(DAY, r.LoggedAtUtc, SYSUTCDATETIME())\n"
        "FROM    etl.RejectedRecord AS r\n"
        "WHERE   r.IsReprocessed = 0\n"
        "AND     (? = N'ALL' OR r.ObjectName = ?)\n"
        "AND     NOT EXISTS (SELECT 1 FROM work.RejectRoutingHistory AS h\n"
        "                    WHERE h.RejectedRecordId = r.RejectedRecordId);",
        parameter_bindings=[
            ("$Package::ObjectScope", 0, "NVARCHAR"),
            ("$Package::ObjectScope", 1, "NVARCHAR"),
        ],
    ))
    name_file = pkg.add(Expression(
        "Build Reject File Name",
        '@[User::RejectFileName] = "rejects_" + (DT_WSTR,20)@[$Package::BatchId] + "_" + '
        '(DT_WSTR,4)YEAR(GETDATE()) + RIGHT("0" + (DT_WSTR,2)MONTH(GETDATE()), 2) + '
        'RIGHT("0" + (DT_WSTR,2)DAY(GETDATE()), 2) + ".csv"',
    ))

    columns = [
        int_col("RejectedRecordId"), int_col("BatchId"), str_col("ObjectName", 128),
        str_col("RejectStage", 20), str_col("RejectReasonCode", 40),
        str_col("RejectReasonDescription", 400), str_col("SourceKey", 200),
        date_col("RejectedAtUtc"), int_col("AgeDays"),
    ]
    flow = DataFlow("Route Rejects To Share")
    flow.oledb_source(
        "work RejectRoutingSet", CONN_STAGING,
        "SELECT RejectedRecordId, BatchId, ObjectName, RejectStage, RejectReasonCode, "
        "RejectReasonDescription, SourceKey, RejectedAtUtc, AgeDays "
        "FROM work.RejectRoutingSet ORDER BY ObjectName, RejectedAtUtc;", columns, timeout=900)
    flow.derived_column("Classify Reject Age", [
        ("IsEscalated",
         "AgeDays > @[$Package::RejectEscalationDays] ? (DT_BOOL)1 : (DT_BOOL)0",
         bool_col("IsEscalated")),
        ("RejectFileLine",
         '(DT_WSTR,128)ObjectName + "|" + (DT_WSTR,40)RejectReasonCode + "|" + '
         '(DT_WSTR,200)SourceKey',
         str_col("RejectFileLine", 400)),
    ])
    flow.conditional_split("Split Escalations", [
        ("Escalated", "IsEscalated == (DT_BOOL)1"),
        ("Standard", "IsEscalated == (DT_BOOL)0"),
    ])
    flow.row_count("Count Routed Rejects", "User::RoutedRowCount")
    flow.oledb_destination("work RejectRoutingHistory", CONN_STAGING,
                           "[work].[RejectRoutingHistory]", batch_size=20000)
    flow.branch_destination("work RejectEscalation", CONN_STAGING, "[work].[RejectEscalation]",
                            "Split Escalations", "Escalated")
    route = pkg.add(DataFlowTask(flow))

    escalate = pkg.add(ExecuteSql(
        "Escalate Aged Rejects",
        CONN_STAGING,
        "INSERT INTO etl.OperatorNotification "
        "    (BatchId, NotificationTypeCode, Severity, Subject, Body, RaisedAtUtc, IsAcknowledged) "
        "SELECT ?, N'REJECT_ESCALATION', N'WARNING', "
        "       CONCAT(N'Aged rejects for ', ObjectName), "
        "       CONCAT(N'Rejects unresolved beyond the escalation window: ', COUNT(*)), "
        "       SYSUTCDATETIME(), 0 "
        "FROM   work.RejectEscalation GROUP BY ObjectName;",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    count_escalated = pkg.add(ExecuteSql(
        "Count Escalations",
        CONN_STAGING,
        "SELECT COUNT(*) AS Escalations FROM work.RejectEscalation;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::EscalatedCount")],
    ))
    counts = pkg.add(log_row_count("etl.RejectedRecord"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, gather)
    pkg.link(gather, name_file)
    pkg.link(name_file, route)
    pkg.link(route, escalate)
    pkg.link(escalate, count_escalated, value="Completion")
    pkg.link(count_escalated, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# ERR_Notify_Operations
# ---------------------------------------------------------------------------


def build_err_notify_operations():
    pkg = new_package(
        "ERR_Notify_Operations",
        "Writes operator notification rows for failed batches. The estate has no mail server "
        "available to the SSIS runtime any more, so notifications are written to "
        "etl.OperatorNotification and the monitoring tool polls that table. Severity is derived "
        "from the criticality of the failing packages, and a notification is only raised once per "
        "batch and failure class so the operators are not flooded.",
        connections=(CONN_DW,),
        extra_variables=[("NotificationCount", 0, "int"), ("Severity", "INFO", "string"),
                         ("FailedStepCount", 0, "int")],
    )
    pkg.add_parameter("NotifyOnWarnings", "False", dtype="bool",
                      description="Also raise a notification for warning-level conditions.")
    pkg.add_parameter("EnvironmentCode", "DEV", dtype="string",
                      description="Environment stamped into the notification subject.")

    start = pkg.add(log_package_start(pkg))
    assess = pkg.add(ExecuteSql(
        "Assess Batch Outcome",
        CONN_STAGING,
        "SELECT COUNT(*) AS FailedSteps, "
        "       CASE WHEN SUM(CASE WHEN s.Criticality = N'high' THEN 1 ELSE 0 END) > 0 "
        "            THEN N'CRITICAL' "
        "            WHEN COUNT(*) > 0 THEN N'WARNING' ELSE N'INFO' END AS Severity "
        "FROM   etl.BatchStep AS s "
        "WHERE  s.BatchId = ? AND s.StepStatus = N'Failed';",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::FailedStepCount"), ("1", "User::Severity")],
    ))
    raise_notification = pkg.add(ExecuteSql(
        "Raise Operator Notification",
        CONN_STAGING,
        "INSERT INTO etl.OperatorNotification\n"
        "    (BatchId, NotificationTypeCode, Severity, Subject, Body, RaisedAtUtc, IsAcknowledged)\n"
        "SELECT  b.BatchId\n"
        ",       N'BATCH_FAILURE'\n"
        ",       ?\n"
        ",       CONCAT(N'[', ?, N'] Batch ', CAST(b.BatchId AS nvarchar(20)),\n"
        "                N' (', b.BatchType, N') failed')\n"
        ",       CONCAT(N'Failed steps: ',\n"
        "                STUFF((SELECT N', ' + s.StepName\n"
        "                       FROM   etl.BatchStep AS s\n"
        "                       WHERE  s.BatchId = b.BatchId AND s.StepStatus = N'Failed'\n"
        "                       FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 2, N''),\n"
        "                N'. Last error: ',\n"
        "                ISNULL((SELECT TOP (1) e.ErrorDescription FROM etl.ErrorLog AS e\n"
        "                        WHERE e.BatchId = b.BatchId ORDER BY e.LoggedAtUtc DESC),\n"
        "                       N'none recorded'))\n"
        ",       SYSUTCDATETIME()\n"
        ",       0\n"
        "FROM    etl.Batch AS b\n"
        "WHERE   b.BatchId = ?\n"
        "AND     NOT EXISTS (SELECT 1 FROM etl.OperatorNotification AS n\n"
        "                    WHERE n.BatchId = b.BatchId\n"
        "                      AND n.NotificationTypeCode = N'BATCH_FAILURE');",
        parameter_bindings=[
            ("User::Severity", 0, "NVARCHAR"),
            ("$Package::EnvironmentCode", 1, "NVARCHAR"),
            ("$Package::BatchId", 2, "LONG"),
        ],
    ))
    warn_notification = pkg.add(ExecuteSql(
        "Raise Warning Notification",
        CONN_STAGING,
        "INSERT INTO etl.OperatorNotification "
        "    (BatchId, NotificationTypeCode, Severity, Subject, Body, RaisedAtUtc, IsAcknowledged) "
        "SELECT ?, N'BATCH_WARNING', N'WARNING', "
        "       N'Batch completed with rejected rows', "
        "       CONCAT(N'Rejected rows in this batch: ', CAST(COUNT(*) AS nvarchar(20))), "
        "       SYSUTCDATETIME(), 0 "
        "FROM   etl.RejectedRecord WHERE BatchId = ? AND IsReprocessed = 0 "
        "HAVING COUNT(*) > 0;",
        parameter_bindings=[("$Package::BatchId", 0, "LONG"), ("$Package::BatchId", 1, "LONG")],
    ))
    summarise = pkg.add(ExecuteSql(
        "Count Notifications",
        CONN_STAGING,
        "SELECT COUNT(*) AS Notifications FROM etl.OperatorNotification "
        "WHERE BatchId = ? AND IsAcknowledged = 0;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::NotificationCount")],
    ))
    counts = pkg.add(log_row_count("etl.OperatorNotification"))
    done = pkg.add(log_package_success())

    pkg.link(start, assess)
    pkg.link(assess, raise_notification, expression="@[User::FailedStepCount] > 0")
    pkg.link(assess, warn_notification,
             expression="@[User::FailedStepCount] == 0 && @[$Package::NotifyOnWarnings]")
    pkg.link(assess, summarise,
             expression="@[User::FailedStepCount] == 0 && !@[$Package::NotifyOnWarnings]")
    pkg.link(raise_notification, summarise, value="Completion")
    pkg.link(warn_notification, summarise, value="Completion")
    pkg.link(summarise, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# ERR_Retry_FailedSteps
# ---------------------------------------------------------------------------


def build_err_retry_failedsteps():
    pkg = new_package(
        "ERR_Retry_FailedSteps",
        "Bounded retry driver for transient extract failures. It walks the retryable failed steps "
        "of a batch, re-runs each one through the control framework up to the configured attempt "
        "limit with an increasing backoff, and gives up cleanly when the limit is reached so the "
        "operators see a single permanent failure rather than an endless loop.",
        connections=(CONN_DW,),
        extra_variables=[("RetryableStepCount", 0, "int"), ("AttemptNumber", 0, "int"),
                         ("BackoffSeconds", 30, "int"), ("StillFailingCount", 0, "int")],
    )
    pkg.add_parameter("MaxRetryAttempts", 3, dtype="int",
                      description="Maximum number of retry sweeps over the failed steps.")
    pkg.add_parameter("BackoffBaseSeconds", 30, dtype="int",
                      description="Base backoff; the wait grows with the attempt number.")

    start = pkg.add(log_package_start(pkg))
    find = pkg.add(ExecuteSql(
        "Find Retryable Steps",
        CONN_STAGING,
        "SELECT COUNT(*) AS RetryableSteps FROM etl.BatchStep "
        "WHERE BatchId = ? AND StepStatus = N'Failed' AND ISNULL(IsRetryable, 0) = 1 "
        "AND ISNULL(AttemptNumber, 1) < ?;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::MaxRetryAttempts", 1, "LONG"),
        ],
        result_bindings=[("0", "User::RetryableStepCount")],
    ))

    init = pkg.add(Expression(
        "Initialise Attempt Counter",
        "@[User::AttemptNumber] = 1",
    ))

    def sweep(title):
        """One retry sweep: back off, reset the failed steps and request a rerun."""
        container = Container(
            title,
            description="Retry sweep: backs off, resets the retryable failed steps and "
                        "requests their rerun through the control framework.",
        )
        backoff = container.add(Expression(
            "Compute Backoff (%s)" % title,
            "@[User::BackoffSeconds] = @[$Package::BackoffBaseSeconds] "
            "* (@[User::AttemptNumber] + 1)",
        ))
        wait = container.add(ExecuteSql(
            "Wait Backoff (%s)" % title,
            CONN_STAGING,
            "DECLARE @Delay char(8) = CONVERT(char(8), "
            "DATEADD(SECOND, ?, CAST('00:00:00' AS time)), 108); WAITFOR DELAY @Delay;",
            parameter_bindings=[("User::BackoffSeconds", 0, "LONG")],
        ))
        reset = container.add(ExecuteSql(
            "Reset Steps For Retry (%s)" % title,
            CONN_STAGING,
            "UPDATE etl.BatchStep "
            "SET    StepStatus = N'Pending', AttemptNumber = ISNULL(AttemptNumber, 1) + 1, "
            "       EndedAtUtc = NULL "
            "WHERE  BatchId = ? AND StepStatus = N'Failed' AND ISNULL(IsRetryable, 0) = 1 "
            "AND    ISNULL(AttemptNumber, 1) < ?;",
            parameter_bindings=[
                ("$Package::BatchId", 0, "LONG"),
                ("$Package::MaxRetryAttempts", 1, "LONG"),
            ],
        ))
        rerun = container.add(ExecuteSql(
            "Request Step Rerun (%s)" % title,
            CONN_STAGING,
            "INSERT INTO etl.BatchStepRerunRequest "
            "    (BatchId, BatchStepId, PackageName, AttemptNumber, RequestedAtUtc, RequestStatus) "
            "SELECT BatchId, BatchStepId, PackageName, AttemptNumber, SYSUTCDATETIME(), "
            "       N'Requested' "
            "FROM   etl.BatchStep "
            "WHERE  BatchId = ? AND StepStatus = N'Pending' AND ISNULL(IsRetryable, 0) = 1;",
            parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        ))
        recheck = container.add(ExecuteSql(
            "Recheck Retryable Steps (%s)" % title,
            CONN_STAGING,
            "SELECT COUNT(*) AS RetryableSteps FROM etl.BatchStep "
            "WHERE BatchId = ? AND StepStatus IN (N'Failed', N'Pending') "
            "AND ISNULL(IsRetryable, 0) = 1 AND ISNULL(AttemptNumber, 1) < ?;",
            result_type="ResultSetType_SingleRow",
            parameter_bindings=[
                ("$Package::BatchId", 0, "LONG"),
                ("$Package::MaxRetryAttempts", 1, "LONG"),
            ],
            result_bindings=[("0", "User::RetryableStepCount")],
        ))
        container.link(backoff, wait)
        container.link(wait, reset)
        container.link(reset, rerun)
        container.link(rerun, recheck, value="Completion")
        return pkg.add(container)

    loop = sweep("Retry Sweep")
    second_sweep = sweep("Retry Sweep Final")

    bump = pkg.add(Expression(
        "Increment Attempt Number",
        "@[User::AttemptNumber] = @[User::AttemptNumber] + 1",
    ))
    record_attempt = pkg.add(ExecuteSql(
        "Record Retry Attempt",
        CONN_STAGING,
        "UPDATE etl.BatchStep SET AttemptNumber = ? "
        "WHERE BatchId = ? AND StepStatus IN (N'Failed', N'Pending') "
        "AND ISNULL(IsRetryable, 0) = 1;",
        parameter_bindings=[
            ("User::AttemptNumber", 0, "LONG"),
            ("$Package::BatchId", 1, "LONG"),
        ],
    ))

    exhausted = pkg.add(ExecuteSql(
        "Mark Retries Exhausted",
        CONN_STAGING,
        "UPDATE etl.BatchStep "
        "SET    StepStatus = N'Failed', IsRetryable = 0, "
        "       ErrorMessage = CONCAT(ISNULL(ErrorMessage, N''), N' | retry limit reached') "
        "WHERE  BatchId = ? AND ISNULL(AttemptNumber, 1) >= ? "
        "AND    StepStatus IN (N'Failed', N'Pending');",
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::MaxRetryAttempts", 1, "LONG"),
        ],
    ))
    still = pkg.add(ExecuteSql(
        "Count Still Failing Steps",
        CONN_STAGING,
        "SELECT COUNT(*) AS StillFailing FROM etl.BatchStep "
        "WHERE BatchId = ? AND StepStatus = N'Failed';",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::StillFailingCount")],
    ))
    counts = pkg.add(log_row_count("etl.BatchStep"))
    done = pkg.add(log_package_success())

    pkg.link(start, init)
    pkg.link(init, find)
    pkg.link(find, loop, expression="@[User::RetryableStepCount] > 0")
    pkg.link(find, still, expression="@[User::RetryableStepCount] == 0")
    pkg.link(loop, bump, value="Completion")
    pkg.link(bump, record_attempt)
    # Expression-and-constraint retry: the sweep is repeated while attempts remain
    # and steps are still retryable; otherwise the retries are declared exhausted.
    pkg.link(record_attempt, second_sweep,
             expression="@[User::AttemptNumber] <= @[$Package::MaxRetryAttempts] "
                        "&& @[User::RetryableStepCount] > 0")
    pkg.link(record_attempt, exhausted,
             expression="@[User::AttemptNumber] > @[$Package::MaxRetryAttempts] "
                        "|| @[User::RetryableStepCount] == 0")
    pkg.link(second_sweep, exhausted, value="Completion")
    pkg.link(exhausted, still)
    pkg.link(still, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# ERR_Quarantine_BadFiles
# ---------------------------------------------------------------------------


def build_err_quarantine_badfiles():
    pkg = new_package(
        "ERR_Quarantine_BadFiles",
        "Moves unparsable inbound files out of the landing share into quarantine so the next "
        "ingestion run does not trip over them again. A file is quarantined when it failed a "
        "structural check, when it is zero length, or when its name does not match any known "
        "feed pattern. Every move is registered so the file can be replayed once the supplier "
        "sends a corrected copy.",
        connections=(CONN_FILES,),
        extra_variables=[("BadFileCount", 0, "int"), ("CurrentFilePath", "", "string"),
                         ("QuarantinePath", "quarantine", "string")],
    )
    pkg.add_parameter("QuarantineFolder", "quarantine", dtype="string",
                      description="Sub-folder of the reject share files are moved into.")
    pkg.add_parameter("DeleteZeroLengthFiles", "False", dtype="bool",
                      description="Zero-length files may be deleted rather than quarantined.")

    start = pkg.add(log_package_start(pkg))
    gather = pkg.add(ExecuteSql(
        "Gather Bad Files",
        CONN_STAGING,
        "INSERT INTO work.BadFileQueue (FileName, FilePath, QuarantineReasonCode, DetectedAtUtc)\n"
        "SELECT  f.FileName\n"
        ",       f.FilePath\n"
        ",       CASE\n"
        "            WHEN f.FileSizeBytes = 0                THEN N'ZERO_LENGTH'\n"
        "            WHEN f.StructuralCheckStatus = N'Failed' THEN N'STRUCTURE'\n"
        "            WHEN f.FeedCode IS NULL                  THEN N'UNKNOWN_FEED'\n"
        "            ELSE N'OTHER'\n"
        "        END\n"
        ",       SYSUTCDATETIME()\n"
        "FROM    etl.InboundFileRegister AS f\n"
        "WHERE   f.ProcessingStatus = N'Failed'\n"
        "AND     f.IsQuarantined = 0;",
    ))
    count_files = pkg.add(ExecuteSql(
        "Count Bad Files",
        CONN_STAGING,
        "SELECT COUNT(*) AS BadFiles FROM work.BadFileQueue WHERE IsMoved = 0;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::BadFileCount")],
    ))

    move_loop = Container(
        "Quarantine Each File",
        kind="foreach",
        enumerator={"folder": "%INBOUND_FILE_ROOT%\\failed", "file_spec": "*.*"},
        variable_mappings=["User::CurrentFilePath"],
        description="Moves each unparsable file in the failed landing folder into quarantine.",
    )
    move = move_loop.add(FileSystemTask(
        "Move File To Quarantine",
        operation="MoveFile",
        source_variable="User::CurrentFilePath",
        destination_variable="User::QuarantinePath",
    ))
    register = move_loop.add(ExecuteSql(
        "Register Quarantined File",
        CONN_STAGING,
        "UPDATE work.BadFileQueue SET IsMoved = 1, MovedAtUtc = SYSUTCDATETIME() "
        "WHERE FilePath = ?;",
        parameter_bindings=[("User::CurrentFilePath", 0, "NVARCHAR")],
    ))
    move_loop.link(move, register, value="Completion")
    loop = pkg.add(move_loop)

    build_path = pkg.add(Expression(
        "Build Quarantine Path",
        '@[User::QuarantinePath] = @[$Package::QuarantineFolder] + "\\\\" + '
        '(DT_WSTR,4)YEAR(GETDATE()) + RIGHT("0" + (DT_WSTR,2)MONTH(GETDATE()), 2)',
    ))
    mark = pkg.add(ExecuteSql(
        "Mark Register Quarantined",
        CONN_STAGING,
        "UPDATE r SET r.IsQuarantined = 1, r.QuarantinedAtUtc = SYSUTCDATETIME() "
        "FROM etl.InboundFileRegister AS r "
        "INNER JOIN work.BadFileQueue AS q ON q.FilePath = r.FilePath AND q.IsMoved = 1;",
    ))
    notify = pkg.add(ExecuteSql(
        "Notify Quarantine",
        CONN_STAGING,
        "INSERT INTO etl.OperatorNotification "
        "    (BatchId, NotificationTypeCode, Severity, Subject, Body, RaisedAtUtc, IsAcknowledged) "
        "SELECT ?, N'FILE_QUARANTINE', N'WARNING', N'Inbound files quarantined', "
        "       CONCAT(N'Files moved to quarantine: ', CAST(COUNT(*) AS nvarchar(20))), "
        "       SYSUTCDATETIME(), 0 "
        "FROM   work.BadFileQueue WHERE IsMoved = 1 HAVING COUNT(*) > 0;",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("etl.InboundFileRegister"))
    done = pkg.add(log_package_success())

    pkg.link(start, gather)
    pkg.link(gather, count_files)
    pkg.link(count_files, build_path, expression="@[User::BadFileCount] > 0")
    pkg.link(count_files, counts, expression="@[User::BadFileCount] == 0")
    pkg.link(build_path, loop)
    pkg.link(loop, mark, value="Completion")
    pkg.link(mark, notify)
    pkg.link(notify, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# ERR_Reconcile_RowCounts
# ---------------------------------------------------------------------------


def build_err_reconcile_rowcounts():
    pkg = new_package(
        "ERR_Reconcile_RowCounts",
        "Compares the source, staging and warehouse row counts recorded for each object in a "
        "batch. Differences are allowed within the object's configured tolerance and where a "
        "documented reason exists - deliberate filtering, deduplication or known rejects - and "
        "anything left over is raised as a reconciliation failure against the batch.",
        connections=(CONN_DW,),
        extra_variables=[("FailedObjectCount", 0, "int"), ("ToleratedCount", 0, "int")],
    )
    pkg.add_parameter("DefaultTolerancePercent", 0, dtype="int",
                      description="Default allowed percentage difference when no object rule exists.")
    pkg.add_parameter("RaiseOnFailure", "True", dtype="bool",
                      description="Fail the batch when an object cannot be reconciled.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.RowCountReconciliation"))
    build = pkg.add(ExecuteSql(
        "Build Reconciliation Set",
        CONN_STAGING,
        "INSERT INTO work.RowCountReconciliation\n"
        "    (BatchId, ObjectName, SourceRowCount, StagingRowCount, TargetRowCount,\n"
        "     RejectedRowCount, TolerancePercent, ExplanationCode)\n"
        "SELECT  a.BatchId\n"
        ",       a.ObjectName\n"
        ",       MAX(CASE WHEN a.CountStage = N'Source'  THEN a.RowCount_ ELSE 0 END)\n"
        ",       MAX(CASE WHEN a.CountStage = N'Staging' THEN a.RowCount_ ELSE 0 END)\n"
        ",       MAX(CASE WHEN a.CountStage = N'Target'  THEN a.RowCount_ ELSE 0 END)\n"
        ",       ISNULL((SELECT COUNT(*) FROM etl.RejectedRecord AS r\n"
        "                WHERE r.BatchId = a.BatchId AND r.ObjectName = a.ObjectName), 0)\n"
        ",       ISNULL(t.TolerancePercent, ?)\n"
        ",       t.ExplanationCode\n"
        "FROM    etl.RowCountAudit AS a\n"
        "LEFT OUTER JOIN etl.RowCountTolerance AS t ON t.ObjectName = a.ObjectName\n"
        "WHERE   a.BatchId = ?\n"
        "GROUP BY a.BatchId, a.ObjectName, t.TolerancePercent, t.ExplanationCode;",
        parameter_bindings=[
            ("$Package::DefaultTolerancePercent", 0, "LONG"),
            ("$Package::BatchId", 1, "LONG"),
        ],
    ))

    columns = [
        int_col("BatchId"), str_col("ObjectName", 128), int_col("SourceRowCount"),
        int_col("StagingRowCount"), int_col("TargetRowCount"), int_col("RejectedRowCount"),
        money_col("TolerancePercent"), str_col("ExplanationCode", 40),
    ]
    flow = DataFlow("Evaluate Reconciliation")
    flow.oledb_source(
        "work RowCountReconciliation", CONN_STAGING,
        "SELECT BatchId, ObjectName, SourceRowCount, StagingRowCount, TargetRowCount, "
        "RejectedRowCount, TolerancePercent, ExplanationCode FROM work.RowCountReconciliation;",
        columns, timeout=600)
    flow.derived_column("Derive Differences", [
        ("ExpectedTargetRowCount",
         "StagingRowCount - RejectedRowCount", int_col("ExpectedTargetRowCount")),
        ("DifferenceRowCount",
         "TargetRowCount - (StagingRowCount - RejectedRowCount)", int_col("DifferenceRowCount")),
        ("DifferencePercent",
         "StagingRowCount == 0 ? (DT_NUMERIC,18,2)0 : "
         "ABS(TargetRowCount - (StagingRowCount - RejectedRowCount)) * 100 / StagingRowCount",
         money_col("DifferencePercent")),
    ])
    flow.derived_column("Evaluate Tolerance", [
        ("ReconciliationStatus",
         'ABS(TargetRowCount - (StagingRowCount - RejectedRowCount)) == 0 ? (DT_WSTR,12)"MATCHED" : '
         '(!ISNULL(ExplanationCode) ? (DT_WSTR,12)"EXPLAINED" : '
         '((StagingRowCount == 0 ? (DT_NUMERIC,18,2)0 : '
         'ABS(TargetRowCount - (StagingRowCount - RejectedRowCount)) * 100 / StagingRowCount) '
         '<= TolerancePercent ? (DT_WSTR,12)"TOLERATED" : (DT_WSTR,12)"FAILED"))',
         str_col("ReconciliationStatus", 12)),
    ])
    flow.conditional_split("Split Reconciliation Outcome", [
        ("Failed", 'ReconciliationStatus == "FAILED"'),
        ("Passed", 'ReconciliationStatus != "FAILED"'),
    ])
    flow.row_count("Count Reconciled Objects", "User::RowsRead")
    flow.oledb_destination("etl ReconciliationResult", CONN_STAGING,
                           "[etl].[ReconciliationResult]", batch_size=10000)
    flow.branch_destination("work RowCountFailure", CONN_STAGING, "[work].[RowCountFailure]",
                            "Split Reconciliation Outcome", "Failed")
    evaluate = pkg.add(DataFlowTask(flow))

    measure = pkg.add(ExecuteSql(
        "Measure Reconciliation Outcome",
        CONN_STAGING,
        "SELECT (SELECT COUNT(*) FROM work.RowCountFailure) AS FailedObjects, "
        "       (SELECT COUNT(*) FROM etl.ReconciliationResult "
        "        WHERE BatchId = ? AND VarianceStatus = N'TOLERATED') AS ToleratedObjects;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::FailedObjectCount"), ("1", "User::ToleratedCount")],
    ))
    assert_counts = pkg.add(ExecuteSql(
        "Assert Row Count Reconciliation",
        CONN_STAGING,
        "EXEC etl.usp_AssertRowCountReconciliation @BatchId = ?, @RaiseOnFailure = 1;",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("etl.RowCountAudit"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, build)
    pkg.link(build, evaluate)
    pkg.link(evaluate, measure)
    pkg.link(measure, assert_counts,
             expression="@[User::FailedObjectCount] > 0 && @[$Package::RaiseOnFailure]")
    pkg.link(measure, counts,
             expression="@[User::FailedObjectCount] == 0 || !@[$Package::RaiseOnFailure]")
    pkg.link(assert_counts, counts)
    pkg.link(counts, done)
    return pkg


BUILDERS = [
    build_err_handle_packagefailure,
    build_err_route_rejectedrows,
    build_err_notify_operations,
    build_err_retry_failedsteps,
    build_err_quarantine_badfiles,
    build_err_reconcile_rowcounts,
]


def main():
    written = []
    names = []
    for builder in BUILDERS:
        pkg = builder()
        written.append(pkg.write(os.path.join(HERE, pkg.name + ".dtsx")))
        names.append(pkg.name)
    written.extend(project.write_project(HERE, PROJECT_NAME, names, CONNECTIONS))
    for path in written:
        print(os.path.relpath(path, REPO_ROOT))


if __name__ == "__main__":
    main()
