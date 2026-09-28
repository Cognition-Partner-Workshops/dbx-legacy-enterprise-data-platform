# 15_error_handling (WWI_ErrorHandling) -> Databricks package mapping

Legacy source: `ssis/15_error_handling/` (6 `ERR_*` packages emitted by `build_error_handling_packages.py`).
Target: bundle `databricks/15_error_handling/` (`wwi_15_error_handling`), one notebook per package, one reusable job.

All notebooks import the shared control layer exactly as contracted (`from dbx_etl_common import control, naming, params`),
read the six standard job parameters through `params.getJobParams(dbutils)` and resolve every table as
`naming.table(catalog, schema, table)`. Nothing under `databricks/common/` is copied or re-implemented; the
`tests/dbx_etl_common/` fake is test-only.

## 1. Package -> notebook -> task

| Legacy package | Notebook | Task key (`wwi_15_error_handling`) | Orchestration phase (`ssis/orchestration-plan.json`) | Lifecycle logging |
|---|---|---|---|---|
| `ERR_Handle_PackageFailure` | `notebooks/ERR_Handle_PackageFailure.py` | `ERR_Handle_PackageFailure` (`run_if: AT_LEAST_ONE_FAILED`) | Failure Handling (seq 98) + every child package's `OnError` | `logPackageStart/End`, `logError`, `endBatchStep`, `endBatch(forceStatus="Failed")` |
| `ERR_Retry_FailedSteps` | `notebooks/ERR_Retry_FailedSteps.py` | `ERR_Retry_FailedSteps` | Restart Recovery (seq 5), Extract Retry Driver (seq 12) | `logPackageStart/End`, `logRowCount("etl.BatchStep")` |
| `ERR_Route_RejectedRows` | `notebooks/ERR_Route_RejectedRows.py` | `ERR_Route_RejectedRows` | Reject Routing (seq 45) | `logPackageStart/End`, `logRejectedRecordSet`, `logRowCount("etl.RejectedRecord")` |
| `ERR_Quarantine_BadFiles` | `notebooks/ERR_Quarantine_BadFiles.py` | `ERR_Quarantine_BadFiles` | File Quarantine (seq 26) | `logPackageStart/End`, `logError` (per-file, Warning), `logRowCount("etl.InboundFileRegister")` |
| `ERR_Reconcile_RowCounts` | `notebooks/ERR_Reconcile_RowCounts.py` | `ERR_Reconcile_RowCounts` | Failure Handling (seq 98) | `logPackageStart/End`, `assertRowCountReconciliation`, `logRowCount("etl.RowCountAudit")` |
| `ERR_Notify_Operations` | `notebooks/ERR_Notify_Operations.py` | `ERR_Notify_Operations` (`run_if: ALL_DONE`) | Failure Handling (seq 98) | `logPackageStart/End`, `logError` (webhook HTTP errors, Warning), `logRowCount("etl.OperatorNotification")` |

Every notebook mirrors the legacy `Log Package Start` / `Log Row Counts` / `Log Package Success` tasks with explicit
`control.logPackageStart` -> `control.logRowCount` -> `control.logPackageEnd(status="Succeeded")`, and the legacy
`OnError` handler (`Log Error` + `Mark Execution Failed`) with `except: control.logError(...); control.logPackageEnd(status="Failed"); raise`.
`control.packageRun` was not used because the notebooks need the `packageExecutionId` for `logRowCount` /
`logRejectedRecordSet` and must publish it as a task value.

## 2. Source objects -> Delta tables

| Legacy object | Delta table | Owner | Notes |
|---|---|---|---|
| `etl.Batch` | `etl.batch` | session 00 | `Status` set to `RetryPending` by the failure handler on transient failures (legacy `Mark Batch Retryable`) |
| `etl.BatchStep` | `etl.batch_step` | session 00 | `Status`, `AttemptNumber`, `CompletedAtUtc` updated by the retry driver |
| `etl.PackageExecution` | `etl.package_execution` | session 00 | read by retry driver / failure handler; closed via `logPackageEnd` |
| `etl.ErrorLog` | `etl.error_log` | session 00 | written only via `control.logError` |
| `etl.RowCountAudit` | `etl.row_count_log` | session 00 | reconciliation input; written only via `control.logRowCount` |
| `etl.RejectedRecord` | `etl.rejected_record` | session 00 | routing input; err.* rows registered via `control.logRejectedRecordSet` |
| `etl.OperatorNotification` | `etl.operator_notification` | session 00 | `REJECT_ESCALATION`, `FILE_QUARANTINE`, `BATCH_FAILURE`, `BATCH_WARNING` rows inserted with Spark SQL (no contract function exists) |
| `etl.BatchStepRerunRequest` | `etl.batch_step_rerun_request` | session 00 | inserted by the retry driver |
| `etl.InboundFileRegister` | `etl.inbound_file_register` | session 00 | `IsQuarantined`, `QuarantinedAtUtc`, `QuarantineReason`, `ProcessingStatus='Quarantined'` via `MERGE` |
| `etl.ReconciliationResult` | `etl.reconciliation_result` | session 00 | `ReconciliationName='ROW_COUNT'` rows, delete+append per batch |
| `etl.RowCountTolerance` | `etl.row_count_tolerance` | session 00 | optional (`tableExists` guard) |
| `err.Rejected*` (10 tables) | `silver.err_rejected_customer` ... `silver.err_rejected_constraint_violation` | sessions 02-06 | read-only here; registration tracked in `silver.work_err_reject_registration` |
| `work.BadFileQueue` | `silver.work_bad_file_queue` | **this session** | created by `src/err_handling/tables.py` (no legacy DDL exists) |
| `work.RejectRoutingSet` | `silver.work_reject_routing_set` | this session | legacy `TRUNCATE` -> `DELETE WHERE RoutedInBatchId = BatchId` |
| `work.RejectRoutingHistory` | `silver.work_reject_routing_history` | this session | append; `RoutingDestination` = `REPROCESS` / `QUARANTINE` |
| `work.RejectEscalation` | `silver.work_reject_escalation` | this session | delete+append per batch |
| `work.RowCountReconciliation` | `silver.work_row_count_reconciliation` | this session | delete+append per batch |
| `work.RowCountFailure` | `silver.work_row_count_failure` | this session | delete+append per batch (legacy never truncated it) |
| `etl.BatchStep.IsRetryable / ErrorMessage` (columns that do not exist in `sqlserver/control`) | `silver.work_step_failure` | this session | failure class, retryable flag, error code/message per failed step |
| `WWI_Inbound_Files` share (`failed\`) | `${var.inbound_volume_path}/failed/` | volume | default `/Volumes/${catalog}/bronze/inbound` |
| `WWI_Quarantine_Files` share | `${var.quarantine_volume_path}/<QuarantineFolder>/<yyyyMM>/` | volume | default `/Volumes/${catalog}/bronze/quarantine` |
| error file share (`rejects_<BatchId>_<yyyyMMdd>.csv`) | `${var.reject_volume_path}` | volume | default `/Volumes/${catalog}/silver/rejects` |

## 3. SSIS component -> Spark construct (per package)

### ERR_Handle_PackageFailure
| SSIS | Databricks |
|---|---|
| Parameters `FailedPackage`, `FailureErrorCode`, `FailureMessage` (set by the calling child package) | job parameters of the same name **or** discovered: `dbutils.jobs.taskValues.get(taskKey, "errorCode"/"errorMessage"/"packageExecutionId")` for every failed task returned by `WorkspaceClient().jobs.get_run(run_id=jobRunId)` (+ `get_run_output` for the error text). `jobRunId` is bound to `{{job.run_id}}` |
| Execute SQL `Classify Failure` (`CASE WHEN ? IN (1205,1222,10054,10060,12154,12541,64,121)`) | `err_handling.failure.classifyFailure` (same code list, `TRANSIENT_ERROR_CODES`) |
| Execute SQL `Log Error` (`etl.usp_LogError`) | `control.logError(spark, catalog, packageExecutionId, batchId, "Error", errorCode, sourceName=failedPackage, sourceComponent="ERR_Handle_PackageFailure", errorDescription)` |
| Execute SQL `Close Failed Batch Step` (`UPDATE etl.BatchStep ... Failed, IsRetryable, ErrorMessage`) | `control.endBatchStep(batchStepId, "Failed")` for the open step + one `silver.work_step_failure` row (`FailureClass`, `IsRetryable`, `ErrorCode`, `ErrorMessage`) |
| Execute SQL `Close Package Execution` | `control.logPackageEnd(packageExecutionId, status="Failed")` for every `Running` execution of the failed package in the batch |
| Precedence `FailureClass == "PERMANENT" && FailBatchOnPermanent` -> `Fail Batch` (`etl.usp_EndBatch @ForceStatus='Failed'`) | `control.endBatch(spark, catalog, batchId, forceStatus="Failed")` |
| Precedence `FailureClass == "TRANSIENT"` -> `Mark Batch Retryable` | `UPDATE etl.batch SET Status='RetryPending' WHERE BatchId=... AND Status='Running'` |
| Output variables `FailureClass`, `RetryableFlag` | task values `failureClass`, `retryableFlag`, `failedPackages`, `batchOutcome`; `dbutils.notebook.exit(json)` |

### ERR_Retry_FailedSteps
| SSIS | Databricks |
|---|---|
| Parameters `MaxRetryAttempts` (3), `BackoffBaseSeconds` (30) | same job parameters; plus `StepGroupScope` ("" = all) and `RerunMode` (`request` / `plan`) |
| Execute SQL `Find Retryable Steps` | `RETRYABLE_STEPS_SQL`: failed steps whose `silver.work_step_failure.IsRetryable = 1` **or** whose failed package execution has an `etl.error_log.ErrorCode` in the transient list, with `AttemptNumber < MaxRetryAttempts` |
| Derived `BackoffSeconds = Base * (Attempt + 1)` | `err_handling.retry.backoffSeconds` + `time.sleep` |
| Sequence container `Retry Sweep` (Reset -> Request -> Recheck) and `Retry Sweep Final` | `sweep(attemptNumber)` called at most `MAX_SWEEPS_PER_INVOCATION = 2` times; the edge expressions are `retry.shouldSweepAgain` / `retry.retriesExhausted` |
| `Reset Retryable Steps` (`Status='Pending', AttemptNumber+1`) | `UPDATE etl.batch_step SET Status='Pending', AttemptNumber=COALESCE(AttemptNumber,1)+1, CompletedAtUtc=NULL` |
| `Request Step Reruns` (`INSERT etl.BatchStepRerunRequest`) | `INSERT INTO etl.batch_step_rerun_request (... RequestStatus='Requested')` joined to the failed package names |
| `Mark Retries Exhausted` | `UPDATE etl.batch_step SET Status='Failed'` for steps at the limit + `silver.work_step_failure.IsRetryable=0, ErrorMessage += ' | retry limit reached'` |
| `Count Still Failing` | `stillFailingCount` |
| Row count against `etl.BatchStep` | `control.logRowCount(..., "etl.BatchStep", sourceRowCount=candidates, targetRowCount=pending, updateRowCount=pending)` |

### ERR_Route_RejectedRows
| SSIS | Databricks |
|---|---|
| Parameters `RejectEscalationDays` (5), `ObjectScope` (`ALL`) | same job parameters + `rejectVolumePath` (bundle variable) |
| `Clear Routing Set` (`TRUNCATE work.RejectRoutingSet`) | `DELETE FROM silver.work_reject_routing_set WHERE RoutedInBatchId = BatchId` |
| `Gather Unresolved Rejects` (`etl.RejectedRecord` not in `work.RejectRoutingHistory`) | `INSERT ... SELECT ... WHERE IsReprocessed = false AND NOT EXISTS (history)`; `AgeDays = DATEDIFF(now, LoggedAtUtc)` |
| (implicit) `err.*` feeding `etl.RejectedRecord` | `registerErrTables()`: for each `silver.err_rejected_*` table, rows (with `ReprocessStatusCode='NEW'` where the column exists) not yet in `silver.work_err_reject_registration` are passed to `control.logRejectedRecordSet(objectName="silver.err_...", rejectedDf, batchId, packageExecutionId, businessKeyColumn=<per table>)` |
| Data Flow `Classify Reject Age` (Derived Column: `IsEscalated = AgeDays > RejectEscalationDays`, `RejectFileLine = ObjectName + "|" + RejectReasonCode + "|" + SourceKey`) | `err_handling.rejects.classifyRejects` (`withColumn` chain) |
| Conditional Split `Split Escalations` -> `Escalated` / `Standard` | `RoutingDestination = QUARANTINE / REPROCESS`; `escalated = classified.where("IsEscalated")` |
| OLE DB Destinations `work.RejectRoutingHistory`, `work.RejectEscalation` | `write.format("delta").mode("append").saveAsTable(...)` |
| Flat file `rejects_<BatchId>_<yyyyMMdd>.csv` | `dbutils.fs.put(<rejectVolumePath>/rejects_<BatchId>_<yyyyMMdd>.csv, header + RejectFileLine rows, overwrite=True)` |
| `Escalate Aged Rejects` (`INSERT etl.OperatorNotification ... 'REJECT_ESCALATION','WARNING'` per object) | same `INSERT ... GROUP BY ObjectName` in Spark SQL |
| Row count against `etl.RejectedRecord` | `control.logRowCount(..., "etl.RejectedRecord", sourceRowCount=routed, targetRowCount=routed, insertRowCount=routed, rejectRowCount=escalated)` |

### ERR_Quarantine_BadFiles
| SSIS | Databricks |
|---|---|
| Parameters `QuarantineFolder` (`quarantine`), `DeleteZeroLengthFiles` (`False`) | same job parameters + `quarantineVolumePath`, `inboundVolumePath` (bundle variables) |
| `Gather Bad Files` (`INSERT work.BadFileQueue ... CASE ... ZERO_LENGTH / STRUCTURE / UNKNOWN_FEED / OTHER`) | same `INSERT ... SELECT` into `silver.work_bad_file_queue` (idempotent: `NOT EXISTS` on unmoved rows), plus files found in `<inboundVolumePath>/failed/` via `dbutils.fs.ls` |
| Foreach File `Quarantine Each File` | `for row in queued:` |
| Expression `Build Quarantine Path` (`QuarantineFolder + "\\" + yyyyMM`) | `err_handling.quarantine.quarantineFolder` -> `<root>/<QuarantineFolder>/<yyyyMM>` |
| File System Task `Move File To Quarantine` (MoveFile) | `dbutils.fs.mkdirs` + `dbutils.fs.mv`; `ZERO_LENGTH` + `DeleteZeroLengthFiles` -> `dbutils.fs.rm` |
| `Register Quarantined File` (`UPDATE work.BadFileQueue SET IsMoved=1`) | `UPDATE silver.work_bad_file_queue SET IsMoved=1, MovedAtUtc, QuarantinePath` (or `MoveError` + `control.logError(Warning)` on failure) |
| `Mark Register Quarantined` | `MERGE INTO etl.inbound_file_register ... SET IsQuarantined=true, QuarantinedAtUtc, QuarantineReason, ProcessingStatus='Quarantined'` |
| `Notify Quarantine` (`FILE_QUARANTINE` warning) | same `INSERT INTO etl.operator_notification` |
| Row count against `etl.InboundFileRegister` | `control.logRowCount(..., "etl.InboundFileRegister", sourceRowCount=badFiles, targetRowCount=moved, updateRowCount=moved, rejectRowCount=failedMoves)` |

### ERR_Reconcile_RowCounts
| SSIS | Databricks |
|---|---|
| Parameters `DefaultTolerancePercent` (0), `RaiseOnFailure` (`True`) | same job parameters |
| `Clear Reconciliation Set` + `Build Reconciliation Set` (aggregate `etl.RowCountAudit` per object, join `etl.RowCountTolerance`, count `etl.RejectedRecord`) | `DELETE ... WHERE BatchId` + `INSERT ... SELECT` from `etl.row_count_log` joined to `etl.package_execution` (batch scoping), `LEFT JOIN etl.row_count_tolerance` when present |
| Derived Column `Derive Differences` (`Expected = Staging - Rejected`, `Difference = Target - Expected`, `Percent`) and `Evaluate Tolerance` (`MATCHED` > `EXPLAINED` > `TOLERATED` > `FAILED`) | `err_handling.reconcile.evaluateDataFrame` (Python reference `reconcile.reconciliationStatus` used by the tests) |
| Conditional Split `Split Failures` | `evaluated.where(ReconciliationStatus == 'FAILED')` |
| OLE DB Destination `etl.ReconciliationResult` (`ReconciliationName='ROW_COUNT'`, `SourceAmount=Expected`, `TargetAmount=Target`, `VarianceAmount=Difference`) | delete+append to `etl.reconciliation_result` with the same column semantics |
| OLE DB Destination `work.RowCountFailure` | delete+append to `silver.work_row_count_failure` |
| `Assert Row Count Reconciliation` (`etl.usp_AssertRowCountReconciliation @RaiseOnFailure`) on `FailedObjectCount > 0` | `control.assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True)` |
| Row count against `etl.RowCountAudit` | `control.logRowCount(..., "etl.RowCountAudit", sourceRowCount=objects, targetRowCount=objects, insertRowCount=objects, rejectRowCount=failed)` |

### ERR_Notify_Operations
| SSIS | Databricks |
|---|---|
| Parameters `NotifyOnWarnings` (`False`), `EnvironmentCode` | `NotifyOnWarnings` job parameter; `EnvironmentCode` from `params.getJobParams` |
| `Assess Batch Outcome` (`CRITICAL` if a `high`-criticality step failed, `WARNING` if any failed, else `INFO`) | `err_handling.notify.deriveSeverity`; criticality from `src/err_handling/package_criticality.csv` (copy of the `criticality` column of `docs/inventories/ssis-packages.csv`) because `etl.batch_step` has no criticality column |
| `Raise Failure Notification` (dedup on `BATCH_FAILURE`; subject `[ENV] Batch N (Type) failed`; body `Failed steps: ... Last error: ...`) | `notify.batchFailureSubject` / `notify.batchFailureBody`, dedup via `COUNT(*) ... NotificationTypeCode='BATCH_FAILURE'` |
| `Raise Warning Notification` (`BATCH_WARNING`, `Rejected rows in this batch: N`) | `notify.batchWarningBody` |
| Send Mail / notification delivery | (a) job-level `email_notifications.on_failure` / `on_duration_warning_threshold_exceeded` + `webhook_notifications.on_failure` in `resources/wwi_15_error_handling.job.yml`; (b) `postWebhook()` posts every notification raised in the run as JSON (`notify.webhookPayload`: severity, subject, body, batch, environment) to `dbutils.secrets.get(scope=webhookSecretScope, key=webhookSecretKey)`; HTTP >= 300 is logged as a `Warning` via `control.logError` |
| `Count Notifications` | `unacknowledgedNotifications`; row count against `etl.OperatorNotification` |

## 4. Parameters

Standard job parameters (strings): `BatchId` (`"0"`), `BusinessDate` (`${var.businessDate}`), `ReloadFullHistory` (`"False"`),
`EnvironmentCode` (`${var.environmentCode}`), `RestartFromStep` (`""`), `catalog` (`${var.catalog}`, default `wwi_${bundle.target}`).

Package parameters (strings, defaults are the legacy defaults): `FailedPackage` `""`, `FailureErrorCode` `"0"`, `FailureMessage` `""`,
`FailBatchOnPermanent` `"True"`, `MaxRetryAttempts` `"3"`, `BackoffBaseSeconds` `"30"`, `StepGroupScope` `""`, `RerunMode` `request`,
`RejectEscalationDays` `"5"`, `ObjectScope` `ALL`, `QuarantineFolder` `quarantine`, `DeleteZeroLengthFiles` `"False"`,
`DefaultTolerancePercent` `"0"`, `RaiseOnFailure` `"True"`, `NotifyOnWarnings` `"False"`, `PostToWebhook` `"True"`.

Bundle variables: `catalog`, `warehouse_id`, `businessDate`, `environmentCode`, `dbx_etl_common_wheel`, `quarantine_volume_path`,
`inbound_volume_path`, `reject_volume_path`, `webhook_secret_scope` (`wwi`), `webhook_secret_key` (`ops-webhook-url`), `ops_email`,
`ops_webhook_destination_ids` (list of notification-destination IDs). The webhook URL itself is only ever read from the secret scope.

### Shared wheel
Tasks run on serverless with a job `environments` entry (`environment_key: dbx_etl_common`) whose dependency is
`${var.dbx_etl_common_wheel}` (default `/Workspace/Shared/wwi/dbx_etl_common/dbx_etl_common-0.1.0-py3-none-any.whl`).
This was chosen over `libraries: - whl: ../common/dist/...` because that path is outside this bundle's root (bundles cannot
upload files above `databricks.yml`) and session 00's PR had not shipped a wheel path when this bundle was written. Once the
session 00 bundle publishes the wheel, point the variable at its workspace/Volumes path (or at a PyPI-style index). No `%pip install` cells are used.

## 5. How session 00 references each task

The job is deployable on its own for operator use, but the master jobs should not wait on the whole job. Reference the pieces as follows:

1. **`ERR_Handle_PackageFailure` (failure handler)** — add to each master job a `notebook_task` pointing at
   `/Workspace/Users/<deployer>/.bundle/wwi_15_error_handling/<target>/files/notebooks/ERR_Handle_PackageFailure.py`
   (i.e. `${workspace.file_path}/notebooks/ERR_Handle_PackageFailure.py` of this bundle), `depends_on` every protected task,
   `run_if: AT_LEAST_ONE_FAILED` (use `ALL_FAILED` for the stream-level "all extracts failed" variant), `base_parameters:
   jobRunId: "{{job.run_id}}"`, `FailBatchOnPermanent: "True"`, and the six standard parameters. Protected notebooks should
   publish `dbutils.jobs.taskValues.set("packageExecutionId" | "errorCode" | "errorMessage", ...)` in their `except`
   block; when they do not, the handler falls back to `get_run_output().error` and parses `ORA-nnnnn` / `Msg nnnn` codes.
   Outputs: task values `failureClass`, `retryableFlag`, `failedPackages`, `batchOutcome`.
2. **`ERR_Retry_FailedSteps` (ExtractAttempt loop)** — in `Master_Daily_ETL`, after `Record Extract Attempt`, add a
   `notebook_task` on `ERR_Retry_FailedSteps.py` with `base_parameters: MaxRetryAttempts: "{{job.parameters.MaxExtractAttempts}}"`,
   `StepGroupScope: "Extract"`, `BackoffBaseSeconds: "30"`, and a `condition_task` on
   `{{tasks.ERR_Retry_FailedSteps.values.retriesExhausted}} == "false"` guarding the re-run of the extract tasks (a
   `for_each_task` over `{{tasks.ERR_Retry_FailedSteps.values.retryPackages}}`, split on `,`, is the cleanest way to re-run only
   the failed packages; each iteration runs the corresponding extract notebook). The `true` branch goes to Failure Handling —
   this is the legacy `ExtractAttempt > MaxExtractAttempts` edge. Restart Recovery (`RestartFromStep != ""`) is the same task
   with `StepGroupScope: ""` and `RerunMode: request`. `RerunMode: plan` only reports what would be retried (no sleeps, no writes).
   Callable interface: inputs `BatchId`, `catalog`, `MaxRetryAttempts`, `BackoffBaseSeconds`, `StepGroupScope`, `RerunMode`;
   outputs (task values and `dbutils.notebook.exit` JSON) `retryableStepCount`, `attemptNumber`, `stillFailingCount`,
   `pendingStepCount`, `retriesExhausted` (`"true"`/`"false"`), `retryPackages` (comma-separated legacy package names).
3. **`ERR_Quarantine_BadFiles`** — `notebook_task` in the File Quarantine phase (`depends_on` the File Screen tasks,
   `run_if: ALL_DONE`), `base_parameters: quarantineVolumePath`, `inboundVolumePath` bound to the master's volume variables.
4. **`ERR_Route_RejectedRows`** — `notebook_task` in the Reject Routing phase after `DQ_Reject_Reprocess`, `base_parameters:
   rejectVolumePath`, `RejectEscalationDays: "5"`.
5. **`ERR_Reconcile_RowCounts`** — first task of Failure Handling (`run_if: ALL_DONE`); `RaiseOnFailure: "True"` makes the
   task fail when `control.assertRowCountReconciliation` raises, which then triggers the failure handler.
6. **`ERR_Notify_Operations`** — last task of every master job, `run_if: ALL_DONE`, `base_parameters: webhookSecretScope`,
   `webhookSecretKey`, `NotifyOnWarnings`. Alternatively run the whole job with a `run_job_task` (`job_id: ${resources.jobs.wwi_15_error_handling.id}`
   when the bundles are merged, or the deployed job id) and `job_parameters: {BatchId, catalog, ...}` — the whole job then runs
   the Failure Handling sequence (`ERR_Reconcile_RowCounts` -> `ERR_Handle_PackageFailure` (only if something failed) -> `ERR_Notify_Operations`).

## 6. Validation and deployment

Offline (done in this PR): `python -m py_compile databricks/15_error_handling/notebooks/*.py databricks/15_error_handling/validation/*.py`,
`cd databricks/15_error_handling/tests && python -m pytest`, `cd databricks/15_error_handling && databricks bundle validate -t dev` (and `-t prod`).
The bundle validation was run with the demo workspace credentials (schema + workspace resolution); nothing was deployed or executed.

Deploy: `cd databricks/15_error_handling && databricks bundle deploy -t dev` after the `dbx_etl_common` wheel exists at
`${var.dbx_etl_common_wheel}`, the `wwi` secret scope contains `ops-webhook-url`, and the volumes referenced by the three
`*_volume_path` variables exist. Run: `databricks bundle run wwi_15_error_handling -t dev --params BatchId=<id>`.

Reconciliation notebook `validation/ERR_ReconcileTargets.py`: for every table written by this project it computes the per-`BatchId`
row count and `sum(xxhash64(concat_ws('|', <sorted non-volatile columns>)))`, compares with a baseline Delta table / JSON parameter
(`ObjectName, BatchId, RowCount, RowHash`) captured from SQL Server (queries in the notebook, ported from
`validation/runtime/01_control_framework_health.sql` and `02_row_count_reconciliation.sql`), and writes `control.logRowCount`
rows (`SourceRowCount` = baseline, `TargetRowCount` = Delta, `RejectRowCount` = 1 on hash mismatch).

Tests (`tests/`): failure classification and error-code extraction, failed-task discovery from a Jobs `Run`, retry backoff /
sweep expressions, notification severity and message text, quarantine reason order and `yyyyMM` path, reject file name and
routing destination, Spark tests for `evaluateDataFrame` and `classifyRejects`, and a contract guard that the notebooks only
call the contracted `dbx_etl_common` names and never hard-code a catalog.

## 7. Not migrated / needs decision

| Item | Why | Proposed handling |
|---|---|---|
| `etl.BatchStep.IsRetryable`, `ErrorMessage`, `StepStatus`, `Criticality` columns used by the legacy SQL | not present in `sqlserver/control/tables/*.sql` (the generator's SQL references columns the schema does not define) | stored in `silver.work_step_failure`; criticality from the package inventory CSV. Session 00 could add the columns to `etl.batch_step` instead — say so and the notebooks change in one place each. |
| `work.*` scratch tables (6) | no legacy DDL | created as `silver.work_*` by `tables.ensureWorkTable`; column lists inferred from the package SQL |
| `etl.RowCountAudit.CountStage` pivot (`Source` / `Staging` / `Target`) in `Build Reconciliation Set` | `etl.RowCountAudit` has `SourceRowCount` / `TargetRowCount` columns, no `CountStage` | `StagingRowCount = SourceRowCount = SUM(SourceRowCount)`, `TargetRowCount = SUM(TargetRowCount)` from `etl.row_count_log`. If session 00 models hops differently, adjust `buildReconciliationSet()` |
| Legacy `TRUNCATE` of `work.RejectRoutingSet` / `work.RowCountReconciliation` | whole-table truncation is not safe with concurrent batches | `DELETE ... WHERE <batch column> = BatchId` (idempotent per batch) |
| Retry execution of the failed packages | the legacy package only re-queues steps; the master orchestrator re-runs them | unchanged: the driver publishes `retryPackages`; session 00's `for_each_task` performs the re-run |
| Send Mail task | legacy notification is a table row (no SMTP task in the generator); e-mail delivery is provided by job `email_notifications` | webhook post added as requested; the recipient list is a bundle variable (`ops_email`) with a placeholder default that must be overridden per target |
| `webhook_notifications` destination IDs | notification destinations are workspace objects created by admins | `ops_webhook_destination_ids` variable (empty by default) |
| `ProcessingStatus = 'Quarantined'` on `etl.inbound_file_register` | legacy `Mark Register Quarantined` only sets `IsQuarantined`; the CHECK constraint in `sqlserver/control` allows `Quarantined` | set both; drop the status update if session 00's table lacks the value |
| Repo offline check `validation/static/run_all_checks.py` `forbidden-content` | it scans every non-legacy directory for the word "Databricks"/`dbutils`, so any `databricks/**` deliverable fails it (22 hits here, all in this project's files). `validation/` is read-only for this session | PR #18 adds a `MIGRATION_TARGET_DIRS = ("databricks",)` skip to that check; the parent session should land that (or an equivalent) with session 00. `run_all_checks.py --path ssis --path sqlserver --path oracle --path deployment --path config --path tools` passes; `run_deep_checks.py` passes |

## 8. Open questions
1. Should `IsRetryable` / `FailureClass` live on `etl.batch_step` (session 00 schema change) rather than `silver.work_step_failure`?
2. Confirm the workspace path (or Volumes path) where session 00 publishes the `dbx_etl_common` wheel, so `dbx_etl_common_wheel` can be defaulted correctly.
3. Confirm the volume names for inbound / quarantine / rejects (`/Volumes/<catalog>/bronze/inbound`, `/Volumes/<catalog>/bronze/quarantine`, `/Volumes/<catalog>/silver/rejects` are placeholders).
4. Confirm the operations e-mail list and the webhook notification destination IDs per target.
