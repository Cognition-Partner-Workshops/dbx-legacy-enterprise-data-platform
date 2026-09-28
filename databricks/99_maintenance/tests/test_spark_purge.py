"""Local Delta tests for the Spark-facing purge / statistics helpers."""
from datetime import date, datetime

import pytest

import maintenance_lib as mnt


@pytest.fixture(scope="module")
def stagingTable(spark, tmp_path_factory):
    path = str(tmp_path_factory.mktemp("delta") / "stg_customer")
    df = spark.createDataFrame(
        [(1, 10, datetime(2023, 1, 1)), (2, 11, datetime(2023, 6, 1)), (3, 12, datetime(2024, 5, 1))],
        "CustomerId INT, BatchId BIGINT, LoadedAtUtc TIMESTAMP")
    df.write.format("delta").mode("overwrite").save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS stg_customer USING DELTA LOCATION '{path}'")
    return "stg_customer"


def test_retention_delete_uses_plan_predicate(spark, stagingTable):
    predicate = mnt.purgePredicate("x", "LoadedAtUtc", True, mnt.cutoffDate(date(2024, 3, 1), 90))
    spark.sql(f"DELETE FROM {stagingTable} WHERE {predicate}")
    remaining = [r[0] for r in spark.table(stagingTable).select("CustomerId").collect()]
    assert remaining == [3]


def test_delete_is_idempotent(spark, stagingTable):
    predicate = mnt.purgePredicate("x", "LoadedAtUtc", True, mnt.cutoffDate(date(2024, 3, 1), 90))
    spark.sql(f"DELETE FROM {stagingTable} WHERE {predicate}")
    assert spark.table(stagingTable).count() == 1


def test_history_modification_counter(spark, stagingTable):
    history = mnt.tableHistory(spark, stagingTable)
    assert history[-1]["operation"] == "WRITE"
    assert mnt.modifiedRowsSince(history, None) >= 3 + 2  # 3 written + 2 deleted
    assert mnt.modifiedRowsSince(history, history[0]["timestamp"]) == 0


def test_describe_detail_and_retention(spark, stagingTable):
    detail = mnt.deltaTableDetail(spark, stagingTable)
    assert detail["numFiles"] >= 1 and detail["sizeInBytes"] > 0
    assert mnt.deletedFileRetentionHours(detail["properties"]) == 168
    spark.sql(f"ALTER TABLE {stagingTable} SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')")
    assert mnt.deletedFileRetentionHours(mnt.deltaTableDetail(spark, stagingTable)["properties"]) == 720


def test_insert_rows_appends_typed_plan(spark, tmp_path):
    spark.sql("CREATE TABLE IF NOT EXISTS work_plan (SchemaName STRING, TableName STRING, RetentionDays INT, "
              "CutoffDate DATE, PlannedAtUtc TIMESTAMP) USING DELTA")
    rows = [{"SchemaName": "silver", "TableName": "stg_customer", "RetentionDays": 90,
             "CutoffDate": date(2024, 1, 1), "PlannedAtUtc": datetime(2024, 3, 1, 22, 0)}]
    assert mnt.insertRows(spark, "work_plan", rows,
                          "SchemaName STRING, TableName STRING, RetentionDays INT, CutoffDate DATE, PlannedAtUtc TIMESTAMP") == 1
    assert mnt.insertRows(spark, "work_plan", [], "SchemaName STRING") == 0
    assert spark.table("work_plan").count() == 1


def test_reconciliation_hash_is_deterministic(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], "Id INT, Name STRING")
    df.createOrReplaceTempView("recon_a")
    df.orderBy("Name", ascending=False).createOrReplaceTempView("recon_b")
    expr = "COALESCE(SUM(xxhash64(concat_ws('|', coalesce(cast(Id as string), ''), coalesce(cast(Name as string), '')))), 0)"
    a = spark.sql(f"SELECT {expr} FROM recon_a").collect()[0][0]
    b = spark.sql(f"SELECT {expr} FROM recon_b").collect()[0][0]
    assert a == b


def test_fake_control_contract_matches_notebook_usage():
    from dbx_etl_common import control, naming, params
    control.reset()
    pid = control.logPackageStart(None, "c", None, "MNT_Purge_StagingHistory", projectName="WWI_Maintenance", stepName="Purge")
    control.logRowCount(None, "c", pid, "silver.work_staging_purge_plan", targetRowCount=3, deleteRowCount=2)
    control.purgeControlHistory(None, "c", executionHistoryMonths=mnt.monthsFromDays(400),
                                errorHistoryMonths=mnt.monthsFromDays(180), rejectHistoryMonths=mnt.monthsFromDays(90))
    control.logPackageEnd(None, "c", pid, status="Succeeded", rowsDeleted=2)
    assert [c[0] for c in control.calls] == ["logPackageStart", "logRowCount", "purgeControlHistory", "logPackageEnd"]
    assert control.calls[2][1]["executionHistoryMonths"] == 13
    assert naming.table("wwi_dev", "gold", "dim_customer") == "wwi_dev.gold.dim_customer"

    class W:
        def get(self, name):
            return {"BatchId": "7", "BusinessDate": "2024-03-01", "catalog": "wwi_dev"}[name]

    class D:
        widgets = W()

    p = params.getJobParams(D())
    assert p["batchId"] == 7 and p["businessDate"] == date(2024, 3, 1) and p["environmentCode"] == "DEV"
