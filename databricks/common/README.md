# `databricks/common` — shared control layer (`dbx_etl_common`)

Session 00 deliverable. Everything under `databricks/<NN_project>/` consumes this package; nobody re-implements
`sqlserver/control/procedures/*.sql` anywhere else.

| Path | What it is |
|------|------------|
| `dbx_etl_common/` | Python package (`pyproject.toml`, version **0.1.0**, `src/dbx_etl_common/`). Build -> wheel. |
| `sql/00_etl_control_tables.sql` | Delta DDL for **every** table in `sqlserver/control/02,04,06,07_*.sql` plus the tables procedures create inline (`rejected_record_staging`, `control_purge_audit`). Rendered from `schema.py`. |
| `sql/01_etl_seed_control_data.sql` | Idempotent `MERGE` seeds from `03_seed_control_data.sql` + `05_seed_data_quality_rules.sql`. Rendered from `seeds.py`. |
| `views/00_etl_operational_views.sql` | `etl.v_*` re-implementations of `sqlserver/control/views/etl.OperationalViews.sql`. Rendered from `views.py`. |
| `notebooks/00_control_bootstrap.py`, `01_control_verify.py` | Bootstrap + verification notebooks run by job `wwi_00_control_bootstrap`. |
| `databricks.yml`, `resources/wwi_00_control_bootstrap.job.yml` | Bundle `wwi_00_common`: builds/uploads the wheel as an artifact, runs the bootstrap job. |
| `tests/` | pytest suite on local PySpark 3.5 + delta-spark 3.3 (temp schema per session). |
| `tools/render_sql.py` | Regenerates the three SQL files from the Python definitions (`--check` in CI/tests). |

## Install / build

```bash
python -m venv .venv && source .venv/bin/activate
pip install build pytest "pyspark>=3.5,<4" "delta-spark>=3.3,<4"
cd databricks/common/dbx_etl_common && python -m build --wheel      # -> dist/dbx_etl_common-0.1.0-py3-none-any.whl
pip install dist/dbx_etl_common-0.1.0-py3-none-any.whl
```

The package has **no runtime dependencies** of its own (it uses the `spark` session that Databricks provides);
`pyspark`/`delta-spark` are only needed locally (`pip install "dbx_etl_common[test]"`).

Run the tests (about a minute; the Spark session is shared per pytest session):

```bash
cd databricks/common/tests && python -m pytest -q
```

`tests/conftest.py` looks for Delta jars in `$DELTA_SPARK_JARS` (comma separated) or `~/.venvs/dbx/jars/*.jar`
before falling back to `configure_spark_with_delta_pip` (Maven download).

## Bootstrapping a workspace

```bash
cd databricks/common
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run wwi_00_control_bootstrap -t dev            # creates <catalog>.etl.* + seeds + views
```

`catalog` is the bundle variable (default `wwi_${bundle.target}`; `dev` -> `wwi_dev`, `prod` -> `wwi_prod`).
The bootstrap notebook also creates the `bronze`, `silver`, `gold` schemas (`createSchemas=True`). Alternatively
run `sql/00_etl_control_tables.sql`, `sql/01_etl_seed_control_data.sql` and `views/00_etl_operational_views.sql`
on a SQL warehouse after replacing `${catalog}`.

## Consuming the wheel from a project bundle

Either attach the wheel built by this bundle:

```yaml
# databricks/<NN_project>/resources/wwi_<NN_project>.job.yml
tasks:
  - task_key: EXT_ORA_CustomerMaster
    notebook_task:
      notebook_path: ../notebooks/EXT_ORA_CustomerMaster.py
    libraries:
      - whl: ../../common/dbx_etl_common/dist/dbx_etl_common-*.whl     # classic compute
# serverless: environments[].spec.dependencies: ["../../common/dbx_etl_common/dist/*.whl"]
```

(and build it first: `cd databricks/common/dbx_etl_common && python -m build --wheel`, or declare the same
`artifacts:` block as `databricks/common/databricks.yml` in your bundle), **or** `%pip install` it from the workspace
path the common bundle uploaded it to:

```python
# MAGIC %pip install /Workspace/Users/<deployer>/.bundle/wwi_00_common/<target>/artifacts/.internal/dbx_etl_common-0.1.0-py3-none-any.whl
```

Notebook-importable form: `sys.path.append("<repo>/databricks/common/dbx_etl_common/src")` works too (the package
is pure Python).

## Public API (contract — signatures are frozen; functions may be added, never renamed/reordered)

```python
from dbx_etl_common import control, params, naming

p = params.getJobParams(dbutils)   # batchId:int, businessDate:date, reloadFullHistory:bool, environmentCode:str,
                                   # restartFromStep:str, catalog:str (+ maxParallelStreams, maxExtractAttempts, raw)
naming.table(catalog, "gold", "dim_customer")          # "wwi_dev.gold.dim_customer"
naming.controlTable(catalog, "batch")                  # "wwi_dev.etl.batch"
naming.legacyToDelta(catalog, "Dimension.Stock Item")  # "wwi_dev.gold.dim_stock_item"

batchId = control.startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None, allowAdoptRunning=False, notes=None)
control.endBatch(spark, catalog, batchId, forceStatus=None)          # -> {"batchStatus", "failedPackageCount", "warningCount", "runningPackageCount"}
batchStepId = control.startBatchStep(spark, catalog, batchId, stepName, stepSequence, stepGroup=None)
control.endBatchStep(spark, catalog, batchStepId, status="Succeeded")
packageExecutionId = control.logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None)
control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None, rowsUpdated=None, rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None)
with control.packageRun(spark, catalog, batchId, "EXT_ORA_CustomerMaster", projectName="WWI_Extract_Oracle", stepName="Extract Oracle") as run:
    run.rowsRead = ...; run.rowsInserted = ...        # Succeeded on exit, Failed + logError on exception (re-raised)

control.logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None, sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None)
control.logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None, insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None)
control.logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId=None, batchId=None, sourceSystemCode=None, businessKey=None, rejectReason=None, rejectStage="Stage", recordPayload=None)
control.logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None, sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None) -> int

watermarkFrom, watermarkTo = control.getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=False)
control.setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId=None, allowRewind=False)
control.getConfiguration(spark, catalog, configurationKey, environmentCode=None) -> str

control.assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True) -> failedObjectCount
control.assertRowCountTolerance(spark, catalog, batchId, scope="ALL", objectName=None, absoluteTolerance=None, percentTolerance=None, raiseOnFailure=True) -> failedObjectCount
control.evaluateDataQualityRules(spark, catalog, batchId=None, packageExecutionId=None, ruleGroupCode=None, objectName=None, regionCode=None, businessDate=None) -> failedRuleCount
control.purgeControlHistory(spark, catalog, retentionDays=None, executionHistoryMonths=13, errorHistoryMonths=24, rejectHistoryMonths=12, qualityHistoryMonths=13, whatIf=False)
```

Additions beyond the contract (safe to use, documented in the docstrings): `control.logReject` (usp_LogReject),
`control.stageRejectedRecords` + `logRejectedRecordSet(loadTag=, purgeStaging=)` (the `etl.RejectedRecordStaging` /
`@PurgeStaging` path), `control.markBatchStepSkipped` (RestartFromStep), `control.getFailedPackages`,
`control.getWatermarkRow`, `control.getConfigurationValue` (no-raise), `control.rowCountBalance`,
`control.ControlError` (carries the legacy `THROW` number in `.number`), `naming.legacyToDelta` /
`translateLegacyReferences`, `params.parseBool/parseInt/parseDate`, `bootstrap.bootstrap`, `seeds.applySeeds`,
`views.createViews`.

### Procedure -> function map

| Legacy procedure | `dbx_etl_common.control` |
|---|---|
| `usp_StartBatch` / `usp_EndBatch` | `startBatch` / `endBatch` (status precedence: forced > failed package > still-running package > warning -> `SucceededWithWarnings` > `Succeeded`; steps left `Running` are closed with the batch) |
| `usp_StartBatchStep` / `usp_EndBatchStep` | `startBatchStep` (AttemptNumber = prior attempts + 1) / `endBatchStep` (Succeeded downgraded to Failed when a package of the step failed or is still running) |
| `usp_LogPackageStart` / `usp_LogPackageEnd` | `logPackageStart` (links the running step by `StepName`) / `logPackageEnd` (reject tolerance from `MaxRejectPercent` -> `Failed` + Critical error) |
| `usp_LogError`, `usp_LogRowCount`, `usp_LogRejectedRecord`, `usp_LogReject`, `usp_LogRejectedRecordSet` | `logError` (never raises), `logRowCount` (also increments `package_execution` counters), `logRejectedRecord`, `logReject`, `logRejectedRecordSet` (+ `stageRejectedRecords`) |
| `usp_GetWatermark` / `usp_SetWatermark` | `getWatermark` (Timestamp / NumericKey / DateWindow, `LookbackMinutes`, `IsLocked` -> error 51010, epoch on full reload) / `setWatermark` (rewind refused with a Warning unless `allowRewind=True`; new objects are created as `Timestamp`) |
| `usp_GetConfiguration` | `getConfiguration` (environment override then `ALL`; sensitive keys are never returned) |
| `usp_AssertRowCountReconciliation` / `usp_AssertRowCountTolerance` | `assertRowCountReconciliation` / `assertRowCountTolerance` (`Variance = Source - Target - Rejected`, fails only when **both** absolute and percent tolerance are exceeded, `etl.reconciliation_exemption` honoured) |
| `usp_EvaluateDataQualityRules` | `evaluateDataQualityRules` (rule expressions are Spark SQL predicates over the Delta object; legacy `stg.X` references inside expressions are rewritten via `naming.translateLegacyReferences`; evaluation errors -> `NotEvaluated`, never fail the batch) |
| `usp_PurgeControlHistory` | `purgeControlHistory` (children before parents, chunked deletes, `whatIf`, audit rows in `etl.control_purge_audit`) |

## Identity columns and concurrency (read this)

* Delta `GENERATED ALWAYS AS IDENTITY` columns are **BIGINT only**, so the legacy `INT IDENTITY` keys
  (`SourceSystemId`, `WatermarkId`, `DataQualityRuleId`, ...) are widened to `BIGINT`.
* Delta does not return generated keys (`SCOPE_IDENTITY()`); every `start*/log*` function inserts a row with a
  **unique natural predicate** (`BatchName + BusinessDate + EnvironmentCode + StartedAtUtc`, `PackageName +
  AttemptNumber + BatchId`, ...) and reads the new id back with `MAX(id)` over that predicate. Assumption: two
  writers never insert the *same* natural key within the same microsecond. Two parallel streams starting different
  packages are fine; two concurrent starts of the same package in the same batch produce distinct `AttemptNumber`s
  only if they are serialized by the orchestrator (Databricks task dependencies do this).
* Identity values are unique and increasing but **not gapless**: a write that fails a CHECK constraint still consumes
  values. Nothing may rely on `id + 1`.
* Writes are wrapped in a small retry on Delta `ConcurrentAppendException` / `ConcurrentDeleteReadException`
  (`control._withRetry`), which is how the concurrent appends from parallel streams are absorbed.
* PK / UNIQUE / FK / index intent from the T-SQL is recorded in each table's `COMMENT`; Delta does not enforce them.
  Uniqueness of `SourceSystemCode`, `ConfigurationKey+EnvironmentCode`, `RuleCode`, etc. is guaranteed by the MERGE
  seeds and by the control functions, not by the storage layer.
* The legacy `ALTER TABLE ... ADD CONSTRAINT ... CHECK` rules are Delta CHECK constraints (enforced on write).
  `etl.package_execution.DurationSeconds` is a Delta generated column (`unix_timestamp(CompletedAtUtc) - unix_timestamp(StartedAtUtc)`).
