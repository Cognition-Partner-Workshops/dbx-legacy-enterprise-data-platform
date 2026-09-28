import datetime

from pyspark.sql import functions as F

from dbx_etl_common import control
from stg_common.loader import StagingRun, lookupLeft, splitByCondition


def _run(spark, jobParams, name="STG_Load_Test", watermark=False, **kw):
    return StagingRun(spark, None, name, "ORA_ERP", "stg.Test", watermark=watermark, jobParams=jobParams, **kw)


def test_truncate_reload_replaces_whole_table_and_logs_package(spark, jobParams):
    run = _run(spark, jobParams)
    with run.execute():
        run.truncateReload(spark.createDataFrame([(1, "A"), (2, "B")], "Id int, Name string"), "stg_test_tr")
    run2 = _run(spark, dict(jobParams, batchId=8))
    with run2.execute():
        run2.truncateReload(spark.createDataFrame([(3, "C")], "Id int, Name string"), "stg_test_tr")
    rows = spark.table("spark_catalog.silver.stg_test_tr").collect()
    assert [r["Id"] for r in rows] == [3]
    assert rows[0]["BatchId"] == 8 and rows[0]["SourceSystemCode"] == "ORA_ERP"
    pkg = control.STATE["packages"][-1]
    assert pkg["status"] == "Succeeded" and pkg["rowsInserted"] == 1 and pkg["stepName"] == "Stage Load"


def test_rebuild_for_batch_only_replaces_own_batch(spark, jobParams):
    run = _run(spark, jobParams)
    with run.execute():
        run.rebuildForBatch(spark.createDataFrame([(1,)], "Id int"), "work_test_rb")
    other = _run(spark, dict(jobParams, batchId=99))
    with other.execute():
        other.rebuildForBatch(spark.createDataFrame([(2,)], "Id int"), "work_test_rb")
    rerun = _run(spark, jobParams)
    with rerun.execute():
        rerun.rebuildForBatch(spark.createDataFrame([(3,), (4,)], "Id int"), "work_test_rb")
    rows = spark.table("spark_catalog.silver.work_test_rb").select("Id", "BatchId").collect()
    assert sorted((r["Id"], r["BatchId"]) for r in rows) == [(2, 99), (3, 7), (4, 7)]
    assert rerun.counters.rowsDeleted == 1 and rerun.counters.rowsInserted == 2


def test_merge_by_key_is_idempotent_and_counts_updates(spark, jobParams):
    run = _run(spark, jobParams)
    with run.execute():
        run.mergeByKey(spark.createDataFrame([(1, "A"), (2, "B")], "Id int, Name string"), "stg_test_mg", ["Id"])
    rerun = _run(spark, jobParams)
    with rerun.execute():
        rerun.mergeByKey(spark.createDataFrame([(2, "B2"), (3, "C")], "Id int, Name string"), "stg_test_mg", ["Id"])
    rows = {r["Id"]: r["Name"] for r in spark.table("spark_catalog.silver.stg_test_mg").collect()}
    assert rows == {1: "A", 2: "B2", 3: "C"}
    assert rerun.counters.rowsInserted == 1 and rerun.counters.rowsUpdated == 1


def test_watermark_read_and_set(spark, jobParams):
    src = spark.createDataFrame(
        [(1, datetime.datetime(2024, 1, 1), 7), (2, datetime.datetime(2024, 3, 1), 7), (3, None, 7)],
        "Id int, LAST_UPD_DT timestamp, BatchId long",
    )
    src.write.format("delta").mode("overwrite").saveAsTable("spark_catalog.bronze.raw_wm_test")
    control.STATE["watermarks"][("ORA_ERP", "stg.Test")] = "2024-01-15T00:00:00.000"
    run = _run(spark, jobParams, watermark=True)
    with run.execute():
        df = run.bronzeWatermarked("raw_wm_test", "LAST_UPD_DT")
        assert [r["Id"] for r in df.collect()] == [2]
    assert control.STATE["watermarks"][("ORA_ERP", "stg.Test")] == "2024-03-01T00:00:00.000"
    full = _run(spark, dict(jobParams, reloadFullHistory=True), watermark=True)
    with full.execute():
        assert full.bronzeWatermarked("raw_wm_test", "LAST_UPD_DT").count() == 2


def test_reject_writes_err_table_and_registers_each_key(spark, jobParams):
    run = _run(spark, jobParams)
    with run.execute():
        bad = spark.createDataFrame([("C1", "x"), ("C2", "y")], "CustomerCode string, Reason string")
        n = run.reject(
            bad,
            "err_rejected_customer",
            F.col("Reason"),
            "bad row",
            businessKeyColumn="CustomerBusinessKey",
            keyColumns={"CustomerBusinessKey": F.col("CustomerCode"), "SourceCustomerId": "CustomerCode"},
        )
    assert n == 2
    err = spark.table("spark_catalog.silver.err_rejected_customer")
    assert set(err.columns) >= {"CustomerBusinessKey", "RejectReasonCode", "RecordPayload", "RejectStage", "BatchId"}
    assert sorted(r["RejectReasonCode"] for r in err.collect()) == ["x", "y"]
    assert sorted(r["businessKey"] for r in control.STATE["rejects"]) == ["C1", "C2"]
    assert control.STATE["packages"][-1]["rowsRejected"] == 2


def test_reject_lookup_failures_groups_by_value(spark, jobParams):
    run = _run(spark, jobParams)
    with run.execute():
        miss = spark.createDataFrame([("K1", "ZZ"), ("K2", "ZZ"), ("K3", "QQ")], "Key string, CountryCode string")
        run.rejectLookupFailures(miss, "ref.Country", "CountryCode", "Key", "unknown country")
    err = spark.table("spark_catalog.silver.err_rejected_lookup_failure").where(F.col("PackageExecutionId") == run.packageExecutionId)
    rows = {r["LookupValue"]: r["OccurrenceCount"] for r in err.collect()}
    assert rows == {"ZZ": 2, "QQ": 1}


def test_failure_logs_error_and_reraises(spark, jobParams):
    run = _run(spark, jobParams)
    try:
        with run.execute():
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert control.STATE["packages"][-1]["status"] == "Failed"
    assert control.STATE["errors"][-1]["errorDescription"] == "boom"


def test_restart_from_step_skips_earlier_phase(spark, jobParams):
    run = _run(spark, dict(jobParams, restartFromStep="Stage Work Tables"))
    with run.execute():
        pass
    assert run.skipped and not control.STATE["packages"]
    work = StagingRun(spark, None, "STG_Work_X", "ORA_ERP", "work.X", phase="Stage Work Tables", jobParams=dict(jobParams, restartFromStep="Stage Work Tables"))
    with work.execute():
        pass
    assert not work.skipped


def test_split_and_lookup_helpers(spark):
    df = spark.createDataFrame([(1, "A"), (2, "B"), (3, None)], "Id int, Code string")
    lk = spark.createDataFrame([("A", "Alpha")], "Code string, Name string")
    matched, missed = lookupLeft(df, lk, ["Code"], ["Name"])
    assert [r["Id"] for r in matched.collect()] == [1]
    assert sorted(r["Id"] for r in missed.collect()) == [2, 3]
    yes, no = splitByCondition(df, F.col("Code") == "A")
    assert yes.count() == 1 and no.count() == 2
