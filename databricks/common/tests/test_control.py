import datetime as dt
import decimal

import pytest
from pyspark.sql import functions as F

from dbx_etl_common import control, naming
from dbx_etl_common.control import ControlError


def _t(catalog, name):
    return naming.controlTable(catalog, name)


def _row(spark, catalog, table, where):
    return spark.sql(f"SELECT * FROM {_t(catalog, table)} WHERE {where}").first().asDict()


# ----------------------------------------------------------------------------- batch lifecycle

def test_start_batch_returns_identity_and_defaults(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "Master_Daily_ETL", businessDate=dt.date(2024, 1, 15))
    b = _row(spark, catalog, "batch", f"BatchId = {bid}")
    assert b["Status"] == "Running" and b["EnvironmentCode"] == "DEV" and b["BatchType"] == "Daily"
    assert b["StartedAtUtc"] is not None and b["InitiatedBy"]
    bid2 = control.startBatch(spark, catalog, "Master_Daily_ETL", businessDate=dt.date(2024, 1, 16))
    assert bid2 > bid


def test_start_batch_refuses_duplicate_running_then_adopts(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "Master_Daily_ETL", businessDate=dt.date(2024, 1, 15), notes="first")
    with pytest.raises(ControlError) as exc:
        control.startBatch(spark, catalog, "Master_Daily_ETL", businessDate=dt.date(2024, 1, 15))
    assert exc.value.number == 51001 and str(bid) in str(exc.value)
    adopted = control.startBatch(spark, catalog, "Master_Daily_ETL", businessDate=dt.date(2024, 1, 15), allowAdoptRunning=True)
    assert adopted == bid
    assert _row(spark, catalog, "batch", f"BatchId = {bid}")["Notes"].startswith("first | Adopted by recovery rerun at ")


def test_end_batch_status_derivation(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 1, 1))
    assert control.endBatch(spark, catalog, bid)["batchStatus"] == "Succeeded"

    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 1, 2))
    control.logError(spark, catalog, batchId=bid, errorSeverity="Warning", errorDescription="w")
    assert control.endBatch(spark, catalog, bid)["batchStatus"] == "SucceededWithWarnings"

    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 1, 3))
    pe = control.logPackageStart(spark, catalog, bid, "PKG_A")
    control.logPackageEnd(spark, catalog, pe, status="Failed")
    r = control.endBatch(spark, catalog, bid)
    assert r["batchStatus"] == "Failed" and r["failedPackageCount"] == 1

    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 1, 4))
    stepId = control.startBatchStep(spark, catalog, bid, "Extract", 1, "Extract")
    control.logPackageStart(spark, catalog, bid, "PKG_STILL_RUNNING", stepName="Extract")
    r = control.endBatch(spark, catalog, bid)
    assert r["batchStatus"] == "Failed"
    b = _row(spark, catalog, "batch", f"BatchId = {bid}")
    assert "1 package execution(s) were still marked Running" in b["Notes"] and b["CompletedAtUtc"] is not None
    assert _row(spark, catalog, "batch_step", f"BatchStepId = {stepId}")["Status"] == "Failed"

    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 1, 5))
    pe = control.logPackageStart(spark, catalog, bid, "PKG_A")
    control.logPackageEnd(spark, catalog, pe, status="Failed")
    assert control.endBatch(spark, catalog, bid, forceStatus="Cancelled")["batchStatus"] == "Cancelled"

    with pytest.raises(ControlError) as exc:
        control.endBatch(spark, catalog, 999999)
    assert exc.value.number == 51002


def test_batch_step_attempts_and_downgrade(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 2, 1))
    s1 = control.startBatchStep(spark, catalog, bid, "Load Facts", 5, "Fact")
    s2 = control.startBatchStep(spark, catalog, bid, "Load Facts", 5, "Fact")
    assert _row(spark, catalog, "batch_step", f"BatchStepId = {s1}")["AttemptNumber"] == 1
    assert _row(spark, catalog, "batch_step", f"BatchStepId = {s2}")["AttemptNumber"] == 2
    pe = control.logPackageStart(spark, catalog, bid, "LOAD_FactSale", projectName="P", stepName="Load Facts")
    assert _row(spark, catalog, "package_execution", f"PackageExecutionId = {pe}")["BatchStepId"] == s2
    assert control.endBatchStep(spark, catalog, s2) == "Failed"          # package still running
    control.logPackageEnd(spark, catalog, pe)
    assert control.endBatchStep(spark, catalog, s2) == "Succeeded"
    skipped = control.markBatchStepSkipped(spark, catalog, bid, "Aggregates", 6, "Aggregate")
    assert _row(spark, catalog, "batch_step", f"BatchStepId = {skipped}")["Status"] == "Skipped"


# ----------------------------------------------------------------------------- packages

def test_package_lifecycle_counters_and_reject_tolerance(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 3, 1))
    pe = control.logPackageStart(spark, catalog, bid, "EXT_ORA_CustomerMaster", projectName="WWI_Extract_Oracle")
    pe2 = control.logPackageStart(spark, catalog, bid, "EXT_ORA_CustomerMaster", projectName="WWI_Extract_Oracle")
    assert _row(spark, catalog, "package_execution", f"PackageExecutionId = {pe2}")["AttemptNumber"] == 2
    control.logRowCount(spark, catalog, pe, "stg.Customer", sourceRowCount=100, targetRowCount=98, insertRowCount=90,
                        updateRowCount=8, rejectRowCount=2)
    r = _row(spark, catalog, "package_execution", f"PackageExecutionId = {pe}")
    assert (r["RowsRead"], r["RowsInserted"], r["RowsUpdated"], r["RowsRejected"]) == (100, 90, 8, 2)
    status = control.logPackageEnd(spark, catalog, pe, rowsRead=100, rowsRejected=2, watermarkFrom="a", watermarkTo="b")
    assert status == "Succeeded"
    r = _row(spark, catalog, "package_execution", f"PackageExecutionId = {pe}")
    assert r["Status"] == "Succeeded" and r["WatermarkTo"] == "b" and r["DurationSeconds"] is not None
    # reject tolerance (MaxRejectPercent = 5 in ALL) breached -> Failed + Critical error
    status = control.logPackageEnd(spark, catalog, pe2, rowsRead=100, rowsRejected=6)
    assert status == "Failed"
    e = _row(spark, catalog, "error_log", f"PackageExecutionId = {pe2}")
    assert e["ErrorSeverity"] == "Critical" and "Reject tolerance breached" in e["ErrorDescription"] and e["BatchId"] == bid


def test_package_start_without_batch(spark, catalog, cleanControl):
    pe = control.logPackageStart(spark, catalog, 0, "ADHOC_Package")
    r = _row(spark, catalog, "package_execution", f"PackageExecutionId = {pe}")
    assert r["BatchId"] is None and r["AttemptNumber"] == 1


def test_package_run_context_manager(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 3, 2))
    with control.packageRun(spark, catalog, bid, "PKG_OK", projectName="P", stepName="S") as run:
        run.rowsRead, run.rowsInserted = 10, 9
    assert _row(spark, catalog, "package_execution", f"PackageExecutionId = {run.packageExecutionId}")["Status"] == "Succeeded"
    with pytest.raises(ValueError):
        with control.packageRun(spark, catalog, bid, "PKG_BAD") as run2:
            run2.rowsRead = 3
            raise ValueError("boom")
    r = _row(spark, catalog, "package_execution", f"PackageExecutionId = {run2.packageExecutionId}")
    assert r["Status"] == "Failed" and r["RowsRead"] == 3
    e = _row(spark, catalog, "error_log", f"PackageExecutionId = {run2.packageExecutionId}")
    assert e["ErrorDescription"] == "boom" and e["SourceComponent"] == "ValueError" and e["BatchId"] == bid
    assert control.getFailedPackages(spark, catalog, bid) == ["PKG_BAD"]


# ----------------------------------------------------------------------------- logging

def test_log_error_infers_batch_and_never_raises(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 4, 1))
    pe = control.logPackageStart(spark, catalog, bid, "P")
    control.logError(spark, catalog, packageExecutionId=pe, errorCode=7, sourceName="src", errorDescription="d")
    e = _row(spark, catalog, "error_log", f"PackageExecutionId = {pe}")
    assert e["BatchId"] == bid and e["ErrorSeverity"] == "Error" and e["ErrorCode"] == 7
    control.logError(spark, catalog, batchId=0, packageExecutionId=0, errorDescription="orphan")
    assert _row(spark, catalog, "error_log", "ErrorDescription = 'orphan'")["BatchId"] is None
    control.logError("not-a-spark-session", catalog, errorDescription="swallowed")  # must not raise


def test_log_rejected_record_and_set(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 4, 2))
    pe = control.logPackageStart(spark, catalog, bid, "P")
    control.logRejectedRecord(spark, catalog, "stg.Customer", "NULL_NAME", packageExecutionId=pe, businessKey="C1",
                              rejectReason="name missing", recordPayload='{"k":1}')
    control.logReject(spark, catalog, "stg.Customer", businessKey="C2", packageExecutionId=pe)
    rows = spark.table(_t(catalog, "rejected_record")).orderBy("RejectedRecordId").collect()
    assert [r.RejectReasonCode for r in rows] == ["NULL_NAME", "UNSPECIFIED"]
    assert rows[0].BatchId is None and rows[0].RejectStage == "Stage" and rows[0].IsReprocessed is False  # legacy proc does not infer BatchId
    assert _row(spark, catalog, "package_execution", f"PackageExecutionId = {pe}")["RowsRejected"] == 2

    df = spark.createDataFrame([("K1", "BAD_CODE", "why", 5), ("K2", None, None, 6)],
                               "CustomerKey string, RejectReasonCode string, RejectReason string, Amount int")
    n = control.logRejectedRecordSet(spark, catalog, "stg.Customer", df, packageExecutionId=pe, sourceSystemCode="ORA_ERP",
                                     rejectReasonCode="DEFAULT_CODE", businessKeyColumn="CustomerKey")
    assert n == 2
    got = {r.BusinessKey: r for r in spark.table(_t(catalog, "rejected_record")).where("SourceSystemCode = 'ORA_ERP'").collect()}
    assert got["K1"].RejectReasonCode == "BAD_CODE" and got["K1"].RejectReason == "why" and got["K1"].BatchId == bid
    assert got["K2"].RejectReasonCode == "DEFAULT_CODE" and '"Amount":6' in got["K2"].RecordPayload
    with pytest.raises(ControlError):
        control.logRejectedRecordSet(spark, catalog, "stg.Customer")


def test_rejected_record_staging_and_purge_staging(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 4, 3))
    df = spark.createDataFrame([("K1", "R1"), ("K2", "R2")], "BusinessKey string, RejectReasonCode string")
    assert control.stageRejectedRecords(spark, catalog, "load-1", df, "stg.Order", batchId=bid) == 2
    assert control.stageRejectedRecords(spark, catalog, "load-2", df.limit(1), "stg.Order", batchId=bid) == 1
    n = control.logRejectedRecordSet(spark, catalog, "stg.Order", batchId=bid, loadTag="load-1", purgeStaging=False)
    assert n == 2
    assert spark.table(_t(catalog, "rejected_record_staging")).count() == 3
    n = control.logRejectedRecordSet(spark, catalog, "stg.Order", batchId=bid, loadTag="load-2", rejectStage="Load")
    assert n == 1
    assert spark.table(_t(catalog, "rejected_record_staging")).where("LoadTag = 'load-2'").count() == 0
    assert spark.table(_t(catalog, "rejected_record_staging")).count() == 2
    assert spark.table(_t(catalog, "rejected_record")).count() == 3


# ----------------------------------------------------------------------------- configuration & watermarks

def test_get_configuration(spark, catalog, cleanControl):
    assert control.getConfiguration(spark, catalog, "MaxRejectPercent") == "5"
    assert control.getConfiguration(spark, catalog, "MaxRejectPercent", "PROD") == "1"
    assert control.getConfiguration(spark, catalog, "EnvironmentCode", "PROD") == "PROD"
    with pytest.raises(ControlError) as exc:
        control.getConfiguration(spark, catalog, "OraclePasswordSecretName")   # sensitive -> hidden
    assert exc.value.number == 51020
    with pytest.raises(ControlError):
        control.getConfiguration(spark, catalog, "NoSuchKey", "DEV")


def test_watermark_timestamp_lookback_epoch_and_rewind(spark, catalog, cleanControl):
    frm, to = control.getWatermark(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER")
    assert frm == "1899-12-31T23:00:00.000" and to is not None       # epoch minus 60 min lookback
    w = control.getWatermarkRow(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER")
    assert w["WatermarkType"] == "Timestamp" and w["LookbackMinutes"] == 60
    assert control.setWatermark(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER", "2024-05-01T10:00:00.000", packageExecutionId=0)
    frm, to = control.getWatermark(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER")
    assert frm == "2024-05-01T09:00:00.000"
    frm, _ = control.getWatermark(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER", reloadFullHistory=True)
    assert frm == "1899-12-31T23:00:00.000"
    # rewind refused -> warning, unchanged
    assert control.setWatermark(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER", "2024-04-01T00:00:00") is False
    assert control.getWatermarkRow(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER")["LastValue"] == "2024-05-01T10:00:00.000"
    e = _row(spark, catalog, "error_log", "ProcedureName = 'etl.usp_SetWatermark'")
    assert e["ErrorSeverity"] == "Warning" and "Refused to rewind" in e["ErrorDescription"]
    assert control.setWatermark(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER", "2024-04-01T00:00:00", allowRewind=True)
    w = control.getWatermarkRow(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER")
    assert w["LastValue"] == "2024-04-01T00:00:00" and w["PreviousValue"] == "2024-05-01T10:00:00.000"
    assert control.setWatermark(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER", None) is False
    assert control.setWatermark(spark, catalog, "ORA_ERP", "CUSTOMER_MASTER", "garbage") is False   # TRY_CONVERT NULL


def test_watermark_numeric_datewindow_and_lock(spark, catalog, cleanControl):
    spark.sql(f"INSERT INTO {_t(catalog, 'watermark')} (SourceSystemCode, ObjectName, WatermarkType, LastValue, LookbackMinutes, IsLocked) "
              "VALUES ('WWI_OLTP', 'ORDERS', 'NumericKey', '500', 0, false), "
              "('WWI_OLTP', 'SNAP', 'DateWindow', '2024-01-10T15:00:00', 0, false), "
              "('WWI_OLTP', 'LOCKED', 'Timestamp', '2024-01-01T00:00:00', 0, true)")
    assert control.getWatermark(spark, catalog, "WWI_OLTP", "ORDERS") == ("500", None)
    assert control.getWatermark(spark, catalog, "WWI_OLTP", "ORDERS", reloadFullHistory=True) == ("0", None)
    frm, to = control.getWatermark(spark, catalog, "WWI_OLTP", "SNAP")
    assert frm == "2024-01-10" and to == dt.datetime.now(dt.timezone.utc).date().isoformat()
    assert control.getWatermark(spark, catalog, "WWI_OLTP", "SNAP", reloadFullHistory=True)[0] == "1900-01-01"
    with pytest.raises(ControlError) as exc:
        control.getWatermark(spark, catalog, "WWI_OLTP", "LOCKED")
    assert exc.value.number == 51010
    assert control.setWatermark(spark, catalog, "WWI_OLTP", "ORDERS", "499") is False
    assert control.setWatermark(spark, catalog, "WWI_OLTP", "ORDERS", "600") is True
    # unknown object on set -> created as Timestamp
    assert control.setWatermark(spark, catalog, "WWI_OLTP", "NEW", "2024-06-01T00:00:00", packageExecutionId=None)
    assert control.getWatermarkRow(spark, catalog, "WWI_OLTP", "NEW")["WatermarkType"] == "Timestamp"


# ----------------------------------------------------------------------------- reconciliation

def _auditBatch(spark, catalog, bd):
    bid = control.startBatch(spark, catalog, "B", businessDate=bd)
    pe = control.logPackageStart(spark, catalog, bid, "STG_LoadCustomer", projectName="WWI_Stage")
    control.logRowCount(spark, catalog, pe, "stg.Customer", sourceRowCount=1000, targetRowCount=990, rejectRowCount=10)
    control.logRowCount(spark, catalog, pe, "stg.Supplier", sourceRowCount=1000, targetRowCount=900, rejectRowCount=0)
    control.logRowCount(spark, catalog, pe, "Dimension.Customer", sourceRowCount=100, targetRowCount=150)   # exempt
    return bid, pe


def test_assert_row_count_reconciliation(spark, catalog, cleanControl):
    bid, _ = _auditBatch(spark, catalog, dt.date(2024, 6, 1))
    with pytest.raises(ControlError) as exc:
        control.assertRowCountReconciliation(spark, catalog, bid)
    assert exc.value.number == 51030 and "1 object(s)" in str(exc.value)
    assert control.assertRowCountReconciliation(spark, catalog, bid, raiseOnFailure=False) == 1
    errs = spark.table(_t(catalog, "error_log")).where(f"BatchId = {bid} AND ProcedureName = 'etl.usp_AssertRowCountReconciliation'").collect()
    assert {e.SourceName for e in errs} == {"stg.Supplier"}
    assert "variance of 100 row(s) (10.0000%)" in errs[0].ErrorDescription


def test_assert_row_count_tolerance(spark, catalog, cleanControl):
    bid, _ = _auditBatch(spark, catalog, dt.date(2024, 6, 2))
    # ALL delegates to the reconciliation gate
    assert control.assertRowCountTolerance(spark, catalog, bid, raiseOnFailure=False) == 1
    # both thresholds must be exceeded to fail
    assert control.assertRowCountTolerance(spark, catalog, bid, scope="Supplier", absoluteTolerance=200, percentTolerance=0, raiseOnFailure=False) == 0
    assert control.assertRowCountTolerance(spark, catalog, bid, scope="Supplier", absoluteTolerance=0, percentTolerance=20, raiseOnFailure=False) == 0
    with pytest.raises(ControlError) as exc:
        control.assertRowCountTolerance(spark, catalog, bid, objectName="stg.Supplier", absoluteTolerance=10, percentTolerance=1)
    assert "stg.Supplier src=1000 tgt=900 rej=0 var=100" in str(exc.value)
    assert spark.table(_t(catalog, "error_log")).where("ErrorSeverity = 'Critical' AND SourceName = 'etl.usp_AssertRowCountTolerance'").count() == 1
    # exempt object never fails; scope match on project name
    assert control.assertRowCountTolerance(spark, catalog, bid, objectName="Dimension.Customer") == 0
    assert control.assertRowCountTolerance(spark, catalog, bid, scope="wwi_stage", raiseOnFailure=False) == 1
    # empty scope -> warning (+ raise)
    with pytest.raises(ControlError):
        control.assertRowCountTolerance(spark, catalog, bid, scope="NoSuchScope")
    assert control.assertRowCountTolerance(spark, catalog, bid, scope="NoSuchScope", raiseOnFailure=False) == 0
    assert spark.table(_t(catalog, "error_log")).where("ErrorSeverity = 'Warning' AND ErrorDescription LIKE 'No row count audit rows%'").count() == 2
    statuses = {r["ObjectName"]: r["ToleranceStatus"] for r in control.rowCountBalance(spark, catalog, bid)}
    assert statuses == {"stg.Customer": "WithinTolerance", "stg.Supplier": "Breached", "Dimension.Customer": "Exempt"}
    with pytest.raises(ControlError):
        control.assertRowCountTolerance(spark, catalog, None)


# ----------------------------------------------------------------------------- data quality

def test_evaluate_data_quality_rules(spark, catalog, cleanControl):
    schema = naming.ETL  # the temp schema doubles as "silver" for the test objects
    spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{schema}.dq_customer (CustomerBusinessKey STRING, CustomerName STRING, "
              "CustomerCategoryCode STRING, CountryCode STRING, RegionCode STRING, PostalCodeIsValid BOOLEAN, AccountOpenedDate DATE) USING DELTA")
    spark.sql(f"INSERT INTO {catalog}.{schema}.dq_customer VALUES "
              "('C1', 'Alpha', 'RET', 'US', 'NA', true, DATE'2020-01-01'), ('C1', '  ', NULL, 'US', 'NA', false, DATE'2020-01-01'), "
              "('C2', NULL, NULL, 'DE', 'EU', false, DATE'2999-01-01')")
    spark.sql(f"DELETE FROM {_t(catalog, 'data_quality_rule')} WHERE RuleGroupCode = 'TESTDQ'")
    rules = [
        ("T_NULL_NAME", "CustomerName IS NULL OR trim(CustomerName) = ''", "FAIL", 0, None),
        ("T_DUP_BKEY", f"CustomerBusinessKey IN (SELECT CustomerBusinessKey FROM etl.dq_customer GROUP BY CustomerBusinessKey HAVING count(*) > 1)", "FAIL", 0, None),
        ("T_NO_CATEGORY", "CustomerCategoryCode IS NULL", "WARN", 250, None),
        ("T_BAD_POSTCODE_NA", "CountryCode IN ('US', 'CA') AND CAST(PostalCodeIsValid AS INT) = 0", "WARN", 0, "NA"),
        ("T_BAD_POSTCODE_EU", "RegionCode = 'EU' AND CAST(PostalCodeIsValid AS INT) = 0", "FAIL", 0, "EU"),
        ("T_FUTURE_OPENED", "AccountOpenedDate > current_date()", "FAIL", 0, None),
        ("T_BROKEN", "NoSuchColumn = 1", "FAIL", 0, None),
    ]
    spark.sql(f"INSERT INTO {_t(catalog, 'data_quality_rule')} (RuleCode, RuleGroupCode, ObjectName, RuleName, RuleExpression, SeverityCode, ThresholdValue, RegionCode) VALUES "
              + ", ".join(f"('{c}', 'TESTDQ', 'etl.dq_customer', '{c}', '{e.replace(chr(39), chr(92) + chr(39))}', '{s}', {t}, {'NULL' if r is None else repr(r)})" for c, e, s, t, r in rules))
    spark.sql(f"INSERT INTO {_t(catalog, 'data_quality_rule_exception')} (RuleCode, ObjectName, RegionCode, EffectiveFrom, EffectiveTo, Reason, ApprovedBy) "
              "VALUES ('T_FUTURE_OPENED', NULL, NULL, DATE'2024-01-01', DATE'2024-12-31', 'known bad test data', 'qa')")
    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 7, 1))

    failed = control.evaluateDataQualityRules(spark, catalog, batchId=bid, ruleGroupCode="TESTDQ", businessDate=dt.date(2024, 7, 1))
    res = {r.RuleCode: r for r in spark.table(_t(catalog, "data_quality_result")).where(f"BatchId = {bid}").collect()}
    assert res["T_NULL_NAME"].ResultStatus == "Failed" and res["T_NULL_NAME"].MeasuredValue == 2 and res["T_NULL_NAME"].RowsEvaluated == 3
    assert res["T_DUP_BKEY"].ResultStatus == "Failed" and res["T_DUP_BKEY"].MeasuredValue == 2
    assert res["T_NO_CATEGORY"].ResultStatus == "Passed"                  # 2 <= 250
    assert res["T_BAD_POSTCODE_NA"].ResultStatus == "Warned"              # WARN severity
    assert res["T_BAD_POSTCODE_EU"].ResultStatus == "Failed"
    assert res["T_FUTURE_OPENED"].ResultStatus == "Warned"                # active exception
    assert res["T_BROKEN"].ResultStatus == "NotEvaluated" and res["T_BROKEN"].MeasuredValue == -1
    assert res["T_BROKEN"].DetailText.startswith("Rule could not be evaluated: ")
    assert failed == 3
    assert spark.table(_t(catalog, "error_log")).where(f"BatchId = {bid} AND SourceComponent = 'etl.dq_customer' AND ErrorSeverity = 'Warning'").count() == 1
    # region filter keeps region-less rules plus matching region
    spark.sql(f"DELETE FROM {_t(catalog, 'data_quality_result')}")
    control.evaluateDataQualityRules(spark, catalog, batchId=bid, ruleGroupCode="TESTDQ", regionCode="EU", businessDate=dt.date(2025, 7, 1))
    codes = {r.RuleCode for r in spark.table(_t(catalog, "data_quality_result")).collect()}
    assert "T_BAD_POSTCODE_NA" not in codes and "T_BAD_POSTCODE_EU" in codes and "T_NULL_NAME" in codes
    assert spark.table(_t(catalog, "data_quality_result")).where("RuleCode = 'T_FUTURE_OPENED'").first().ResultStatus == "Failed"  # exception expired
    spark.sql(f"DELETE FROM {_t(catalog, 'data_quality_rule')} WHERE RuleGroupCode = 'TESTDQ'")


def test_seeded_rules_translate_and_parse(spark, catalog):
    """Every seeded RuleExpression must be valid Spark SQL once legacy references are rewritten."""
    from dbx_etl_common import seeds
    for row in seeds.DATA_QUALITY_RULES.rows:
        code, obj, expr = row[0], row[2], row[4]
        sql = f"SELECT count(*) FROM {naming.legacyToDelta('c', obj)} AS {naming.aliasFor(obj)} WHERE {naming.translateLegacyReferences('c', expr)}"
        try:
            spark.sql(sql)
        except Exception as exc:  # noqa: BLE001
            # missing tables are expected locally; syntax errors are not
            assert "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "REQUIRES_SINGLE_PART_NAMESPACE" in str(exc), f"{code}: {exc}"


# ----------------------------------------------------------------------------- purge

def test_purge_control_history(spark, catalog, cleanControl):
    old = "TIMESTAMP'2020-01-01 00:00:00'"
    spark.sql(f"INSERT INTO {_t(catalog, 'batch')} (BatchName, BatchType, BusinessDate, EnvironmentCode, StartedAtUtc, Status, InitiatedBy) "
              f"VALUES ('OLD_EMPTY', 'Daily', DATE'2020-01-01', 'DEV', {old}, 'Succeeded', 'x'), "
              f"('OLD_WITH_STEP', 'Daily', DATE'2020-01-02', 'DEV', {old}, 'Succeeded', 'x'), "
              f"('OLD_WITH_PKG', 'Daily', DATE'2020-01-03', 'DEV', {old}, 'Succeeded', 'x')")
    ids = {r.BatchName: r.BatchId for r in spark.table(_t(catalog, "batch")).collect()}
    spark.sql(f"INSERT INTO {_t(catalog, 'batch_step')} (BatchId, StepName, StepSequence, StartedAtUtc, Status, AttemptNumber) "
              f"VALUES ({ids['OLD_WITH_STEP']}, 'S', 1, {old}, 'Succeeded', 1), ({ids['OLD_WITH_PKG']}, 'S', 1, {old}, 'Succeeded', 1)")
    spark.sql(f"INSERT INTO {_t(catalog, 'package_execution')} (BatchId, PackageName, MachineName, ExecutedBy, StartedAtUtc, Status, AttemptNumber) "
              f"VALUES ({ids['OLD_WITH_PKG']}, 'P', 'm', 'u', {old}, 'Succeeded', 1)")
    pe = spark.table(_t(catalog, "package_execution")).first().PackageExecutionId
    spark.sql(f"INSERT INTO {_t(catalog, 'row_count_audit')} (PackageExecutionId, ObjectName) VALUES ({pe}, 'o')")
    spark.sql(f"INSERT INTO {_t(catalog, 'error_log')} (ErrorSeverity, LoggedAtUtc, ErrorDescription) "
              + ", ".join(f"VALUES ('Error', {old}, 'old {i}')" if i == 0 else f"('Error', {old}, 'old {i}')" for i in range(2500)))
    control.logError(spark, catalog, errorDescription="recent")
    spark.sql(f"INSERT INTO {_t(catalog, 'rejected_record')} (ObjectName, RejectReasonCode, RejectStage, LoggedAtUtc, IsReprocessed) "
              f"VALUES ('o', 'r', 'Stage', {old}, true), ('o', 'r', 'Stage', {old}, false)")
    spark.sql(f"INSERT INTO {_t(catalog, 'data_quality_result')} (ObjectName, RuleCode, MeasuredValue, ThresholdValue, ResultStatus, EvaluatedAtUtc) "
              f"VALUES ('o', 'R', 1, 0, 'Failed', {old})")

    preview = {r["TableName"]: r for r in control.purgeControlHistory(spark, catalog, whatIf=True)}
    assert preview["etl.ErrorLog"]["RowsThatWouldBeDeleted"] == 2500 and preview["etl.RejectedRecord"]["RowsThatWouldBeDeleted"] == 2
    assert preview["etl.PackageExecution"]["RowsThatWouldBeDeleted"] == 1 and preview["etl.RowCountAudit"]["RowsThatWouldBeDeleted"] == 1
    assert spark.table(_t(catalog, "error_log")).count() == 2501     # whatIf writes nothing

    audit = {r["TableName"]: r["RowsDeleted"] for r in control.purgeControlHistory(spark, catalog, retentionDays=30, chunkSize=1000)}
    assert audit == {"etl.RowCountAudit": 1, "etl.DataQualityResult": 1, "etl.RejectedRecord": 1, "etl.RejectedRecordStaging": 0,
                     "etl.ErrorLog": 2500, "etl.PackageExecution": 1, "etl.BatchStep": 2, "etl.Batch": 3}
    assert spark.table(_t(catalog, "error_log")).count() == 1
    assert spark.table(_t(catalog, "batch")).count() == 0                # children gone first, then parents
    assert spark.table(_t(catalog, "package_execution")).count() == 0
    assert spark.table(_t(catalog, "rejected_record")).where("IsReprocessed = false").count() == 1
    assert spark.table(_t(catalog, "control_purge_audit")).count() == 8


def test_operational_views_query(spark, catalog, cleanControl):
    bid = control.startBatch(spark, catalog, "B", businessDate=dt.date(2024, 8, 1))
    pe = control.logPackageStart(spark, catalog, bid, "P")
    control.logRowCount(spark, catalog, pe, "stg.Customer", sourceRowCount=5, targetRowCount=5)
    control.logRejectedRecord(spark, catalog, "stg.Customer", "X", packageExecutionId=pe, batchId=bid)
    control.logError(spark, catalog, packageExecutionId=pe, errorDescription="e")
    control.getWatermark(spark, catalog, "ORA_ERP", "V")
    control.logPackageEnd(spark, catalog, pe)
    control.endBatch(spark, catalog, bid)
    bs = spark.table(_t(catalog, "v_batch_status")).where(f"BatchId = {bid}").first()
    assert bs.PackageCount == 1 and bs.RowsRejected == 1 and bs.ElapsedMinutes is not None
    assert spark.table(_t(catalog, "v_package_execution_history")).where(f"BatchId = {bid}").first().RecencyRank == 1
    spark.table(_t(catalog, "v_slow_packages")).collect()
    rc = spark.table(_t(catalog, "v_row_count_reconciliation")).where(f"BatchId = {bid}").first()
    assert rc.VarianceRowCount == 0 and rc.IsExempt == 0
    assert spark.table(_t(catalog, "v_reject_summary")).where(f"BatchId = {bid}").first().RejectCount == 1
    assert spark.table(_t(catalog, "v_watermark_status")).where("ObjectName = 'V'").first().SourceSystemName.startswith("Oracle ERP")
    assert spark.table(_t(catalog, "v_recent_errors")).where(f"BatchId = {bid}").first().PackageName == "P"
    assert spark.table(_t(catalog, "row_count_log")).count() == 1
