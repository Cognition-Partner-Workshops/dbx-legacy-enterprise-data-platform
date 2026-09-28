"""PackageContext against the test-only dbx_etl_common fake: lifecycle + audit calls."""

from datetime import date

import pytest

from dbx_etl_common import control
from inv_common import contracts, runtime


class _Widgets:
    def __init__(self, values):
        self.values = values

    def get(self, name):
        if name not in self.values:
            raise KeyError(name)
        return self.values[name]


class _DbUtils:
    def __init__(self, values):
        self.widgets = _Widgets(values)


BASE = {"BatchId": "0", "BusinessDate": "2024-03-15", "ReloadFullHistory": "False", "EnvironmentCode": "DEV",
        "RestartFromStep": "", "catalog": "wwi_test"}


def test_standalone_run_opens_and_closes_its_own_batch(spark):
    control.reset()
    ctx = runtime.PackageContext(spark, _DbUtils(BASE), "INV_Load_DailySnapshot")
    assert ctx.startedBatch and ctx.businessDate == date(2024, 3, 15)
    assert ctx.table("Fact.Daily Inventory Snapshot") == "wwi_test.gold.fact_daily_inventory_snapshot"

    def body(c):
        c.rowsRead, c.rowsInserted = 10, 9
        c.logPackageRowCounts("Fact.Daily Inventory Snapshot")

    runtime.runPackage(ctx, body)
    names = [c[0] for c in control.calls]
    assert names == ["startBatch", "logPackageStart", "logRowCount", "logPackageEnd", "endBatch"]
    assert control.calls[2] == ("logRowCount", "Fact.Daily Inventory Snapshot", 10, 9, 0)
    assert control.calls[3][2] == "Succeeded"


def test_master_supplied_batch_is_not_closed_and_failure_is_logged(spark):
    control.reset()
    ctx = runtime.PackageContext(spark, _DbUtils({**BASE, "BatchId": "77"}), "INV_Reconcile_OnHand")
    assert not ctx.startedBatch and ctx.batchId == 77

    def body(c):
        raise ValueError("boom")

    with pytest.raises(ValueError):
        runtime.runPackage(ctx, body)
    names = [c[0] for c in control.calls]
    assert names == ["logPackageStart", "logError", "logPackageEnd"]
    assert control.calls[2][2] == "Failed"


def test_rejected_set_accumulates_rows_rejected(spark):
    control.reset()
    ctx = runtime.PackageContext(spark, _DbUtils({**BASE, "BatchId": "5"}), "INV_Load_StockTransfer")
    df = spark.createDataFrame([("TR-1", "aged", "{}")], "BusinessKey string, RejectReason string, RecordPayload string")
    n = ctx.logRejectedSet("stg.StockMovement", df, contracts.REASON_TRANSFER_AGED_IN_TRANSIT)
    assert n == 1 and ctx.rowsRejected == 1
    assert control.calls[-1] == ("logRejectedRecordSet", "stg.StockMovement", "TRANSFER_AGED_IN_TRANSIT", "Fact", 1)


def test_widget_defaults():
    d = _DbUtils({"CoverDays": "", "SiteScope": "LDN"})
    assert runtime.widget(d, "CoverDays", "21") == "21"
    assert runtime.widget(d, "SiteScope", "ALL") == "LDN"
    assert runtime.widget(d, "Missing", "x") == "x"
    assert runtime.asBool("True") and runtime.asBool("1") and not runtime.asBool("False")


def test_every_binding_follows_naming_contract():
    for legacy, (schema, name) in contracts.TABLE_BINDINGS.items():
        assert schema in {"bronze", "silver", "gold", "etl"}
        assert name == name.lower() and " " not in name
        prefix = legacy.split(".")[0]
        expected = {"stg": "stg_", "work": "work_", "err": "err_", "Dimension": "dim_", "Fact": "fact_", "Aggregate": "agg_"}.get(prefix)
        if expected:
            assert name.startswith(expected), (legacy, name)
