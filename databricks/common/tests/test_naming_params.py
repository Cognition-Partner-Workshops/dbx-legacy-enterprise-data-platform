import datetime as dt

import pytest

from dbx_etl_common import naming, params


@pytest.mark.parametrize("legacy,expected", [
    ("raw.CustomerMaster", "wwi_dev.bronze.raw_customer_master"),
    ("stg.Customer", "wwi_dev.silver.stg_customer"),
    ("stg.[Order]", "wwi_dev.silver.stg_order"),
    ("work.CustomerDedup", "wwi_dev.silver.work_customer_dedup"),
    ("err.RejectedFileRow", "wwi_dev.silver.err_rejected_file_row"),
    ("ref.FxRateDaily", "wwi_dev.silver.ref_fx_rate_daily"),
    ("Dimension.Stock Item", "wwi_dev.gold.dim_stock_item"),
    ("Fact.Order Fulfilment", "wwi_dev.gold.fact_order_fulfilment"),
    ("Aggregate.Customer 360", "wwi_dev.gold.agg_customer_360"),
    ("Report.Sales Summary", "wwi_dev.gold.rpt_sales_summary"),
    ("Integration.OrderStaging", "wwi_dev.silver.int_order_staging"),
    ("etl.PackageExecution", "wwi_dev.etl.package_execution"),
    ("wwi_dev.gold.dim_customer", "wwi_dev.gold.dim_customer"),
])
def test_legacy_to_delta(legacy, expected):
    assert naming.legacyToDelta("wwi_dev", legacy) == expected


def test_table_and_control_table():
    assert naming.table("wwi_dev", "gold", "dim_customer") == "wwi_dev.gold.dim_customer"
    assert naming.controlTable("wwi_prod", "batch") == "wwi_prod.etl.batch"


def test_translate_references_and_alias():
    sql = "NOT EXISTS (SELECT 1 FROM stg.Order AS o WHERE o.OrderBusinessKey = OrderLine.OrderBusinessKey)"
    out = naming.translateLegacyReferences("c", sql)
    assert "FROM c.silver.stg_order AS o" in out
    assert "OrderLine.OrderBusinessKey" in out  # not a legacy schema prefix, untouched
    assert naming.aliasFor("stg.Stock Item") == "Stock_Item"
    assert naming.translateLegacyReferences("c", "x IN (SELECT k FROM stg.Customer GROUP BY k)").count("c.silver.stg_customer") == 1


def test_unknown_schema_raises():
    with pytest.raises(ValueError):
        naming.legacyToDelta("c", "dbo.Whatever")


def test_get_job_params(fakeDbutils):
    d = fakeDbutils({"BatchId": "42", "BusinessDate": "2024-03-31", "ReloadFullHistory": "True",
                     "EnvironmentCode": "TEST", "RestartFromStep": "Load Facts", "catalog": "wwi_dev",
                     "MaxParallelStreams": "6", "MaxExtractAttempts": "2"})
    p = params.getJobParams(d)
    assert p["batchId"] == 42 and p["businessDate"] == dt.date(2024, 3, 31) and p["reloadFullHistory"] is True
    assert p["environmentCode"] == "TEST" and p["restartFromStep"] == "Load Facts" and p["catalog"] == "wwi_dev"
    assert p["maxParallelStreams"] == 6 and p["maxExtractAttempts"] == 2


def test_get_job_params_defaults(fakeDbutils):
    p = params.getJobParams(fakeDbutils({"catalog": "wwi_dev", "BatchId": "0", "BusinessDate": ""}))
    assert p["batchId"] == 0 and p["businessDate"] == dt.datetime.now(dt.timezone.utc).date()
    assert p["reloadFullHistory"] is False and p["environmentCode"] == "DEV" and p["restartFromStep"] == ""
    assert p["maxParallelStreams"] == 4 and p["maxExtractAttempts"] == 3


def test_catalog_required(fakeDbutils):
    with pytest.raises(ValueError):
        params.getJobParams(fakeDbutils({"BatchId": "1"}))


def test_parse_bool_variants():
    assert params.parseBool("1") and params.parseBool("yes") and not params.parseBool("False")
    assert params.parseBool("", default=True) is True
    with pytest.raises(ValueError):
        params.parseBool("maybe")
