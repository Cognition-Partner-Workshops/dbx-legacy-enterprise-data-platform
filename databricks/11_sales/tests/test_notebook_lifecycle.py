"""The legacy control flow (Log Package Start -> work -> Log Row Counts -> Log Package Success,
OnError -> Log Error -> Mark Execution Failed) is reproduced by sales_common.legacyPackageRun."""
from datetime import date

import pytest

import sales_common as sc
from dbx_etl_common import control, params


class FakeWidgets:
    def __init__(self, values):
        self.values = dict(values)

    def text(self, name, default):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


class FakeDbutils:
    def __init__(self, values):
        self.widgets = FakeWidgets(values)


def test_success_path_logs_start_and_end_with_counts(spark):
    control.reset()
    dbutils = FakeDbutils({"BatchId": "7", "BusinessDate": "2024-03-05", "catalog": "wwi_test"})
    ctx = sc.resolveContext(spark, dbutils, params, control, "SLS_NA_Load_Commission", (("RegionCode", "NA"),))
    assert (ctx.batchId, ctx.businessDate, ctx.catalog, ctx.ownsBatch) == (7, date(2024, 3, 5), "wwi_test", False)
    with sc.legacyPackageRun(spark, control, ctx, "SLS_NA_Load_Commission") as run:
        run.rowsRead = 5
        run.rowsInserted = 4
        run.rowsRejected = 1
    names = [c[0] for c in control.calls]
    assert names == ["logPackageStart", "logPackageEnd"]
    start = control.calls[0][1]
    assert start["batchId"] == 7 and start["projectName"] == "WWI_Sales" and start["stepName"] == "Sales Mart"
    end = control.calls[1][1]
    assert end["status"] == "Succeeded" and end["rowsRead"] == 5 and end["rowsInserted"] == 4 and end["rowsRejected"] == 1
    assert end["packageExecutionId"] == run.packageExecutionId


def test_failure_path_logs_error_marks_failed_and_reraises(spark):
    control.reset()
    dbutils = FakeDbutils({"BatchId": "0", "catalog": "wwi_test", "EnvironmentCode": "TEST"})
    ctx = sc.resolveContext(spark, dbutils, params, control, "SLS_Export_PartnerFeed")
    assert ctx.ownsBatch and ctx.batchId > 0  # standalone run opens its own batch
    with pytest.raises(RuntimeError):
        with sc.legacyPackageRun(spark, control, ctx, "SLS_Export_PartnerFeed") as run:
            run.currentTask = "Build Partner Feed Rows"
            raise RuntimeError("boom")
    names = [c[0] for c in control.calls]
    assert names == ["startBatch", "logPackageStart", "logError", "logPackageEnd", "endBatch"]
    err = control.calls[2][1]
    assert err["errorSeverity"] == "Error" and err["sourceComponent"] == "Build Partner Feed Rows" and "boom" in err["errorDescription"]
    assert control.calls[3][1]["status"] == "Failed"
    assert control.calls[4][1]["forceStatus"] == "Failed"


def test_missing_catalog_is_an_error(spark):
    with pytest.raises(ValueError):
        sc.resolveContext(spark, FakeDbutils({"BatchId": "1"}), params, control, "X")
