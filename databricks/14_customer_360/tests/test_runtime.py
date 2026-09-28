from datetime import date

import pytest

from dbx_etl_common import control
from c360_lib import runtime as R


def ctx():
    return R.JobContext(catalog="wwi_test", batchId=7, businessDate=date(2024, 6, 30), reloadFullHistory=False,
                        environmentCode="DEV", restartFromStep="")


def test_package_lifecycle_success_and_failure(spark):
    control.calls.clear()
    with R.packageLifecycle(spark, ctx(), "C360_Build_ChurnFlags", "Customer 360 Build") as run:
        run.rowsRead, run.rowsInserted = 10, 9
        R.logRowCounts(spark, ctx(), run, "Customer360.CustomerChurnFlag")
    names = [c[0] for c in control.calls]
    assert names == ["logPackageStart", "logRowCount", "logPackageEnd"]
    assert control.calls[0][1]["projectName"] == "WWI_Customer360" and control.calls[0][1]["batchId"] == 7
    assert control.calls[1][1] == {"packageExecutionId": run.packageExecutionId, "objectName": "Customer360.CustomerChurnFlag",
                                   "sourceRowCount": 10, "targetRowCount": 9, "rejectRowCount": 0}
    assert control.calls[2][1]["status"] == "Succeeded" and control.calls[2][1]["rowsInserted"] == 9

    control.calls.clear()
    with pytest.raises(ValueError):
        with R.packageLifecycle(spark, ctx(), "C360_Publish_Segments", "Customer 360 Publish"):
            raise ValueError("boom")
    names = [c[0] for c in control.calls]
    assert names == ["logPackageStart", "logError", "logPackageEnd"]
    assert control.calls[1][1]["errorSeverity"] == "Error" and "boom" in control.calls[1][1]["errorDescription"]
    assert control.calls[2][1]["status"] == "Failed"


def test_restart_from_step():
    c = ctx()
    assert not R.shouldSkipForRestart(c, "C360_Build_CustomerProfile")
    c.restartFromStep = "C360_Build_ChurnFlags"
    assert R.shouldSkipForRestart(c, "C360_Build_CustomerProfile")
    assert R.shouldSkipForRestart(c, "C360_Build_LoyaltyOverlay")
    assert not R.shouldSkipForRestart(c, "C360_Build_ChurnFlags")
    assert not R.shouldSkipForRestart(c, "C360_Publish_Segments")
    c.restartFromStep = "SomethingElse"
    assert not R.shouldSkipForRestart(c, "C360_Build_CustomerProfile")


def test_normalize_columns_and_bool(spark):
    df = spark.createDataFrame([(1, 2)], ["Customer Key", "WWI Customer ID"])
    assert R.normalizeColumns(df).columns == ["CustomerKey", "WWICustomerID"]
    assert R.asBool("True") and R.asBool("1") and not R.asBool("False") and not R.asBool("")


def test_table_mapping():
    from c360_lib import tables as T
    assert T.table("wwi_dev", "Customer360.CustomerSegment") == "wwi_dev.gold.c360_customer_segment"
    assert T.table("wwi_dev", "Aggregate.Customer Rolling 12 Month") == "wwi_dev.gold.agg_customer_rolling_12_month"
    assert T.table("wwi_dev", "work.LoyaltyPointLedger") == "wwi_dev.silver.work_loyalty_point_ledger"
