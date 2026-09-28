"""Execute every generated notebook against local Spark with empty bronze /
reference tables shaped from the legacy DDL, so every column reference,
lookup and write resolves and every package logs a Succeeded execution."""

import os
import re

import pytest
import yaml

from dbx_etl_common import control
from tests import ddl_fixtures

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
NOTEBOOKS = os.path.join(ROOT, "notebooks")

# lookups the packages read from ref.* objects the estate never scripted
UNSCRIPTED_REFS = {
    "silver.ref_payment_terms": "PaymentTermsCode string, NetDays int, DiscountPercent decimal(5,2), DiscountDays int, IsActive boolean",
    "silver.ref_gl_account": "AccountCode string, AccountName string, AccountTypeCode string, IsIntercompany boolean",
    "silver.ref_loyalty_tier": "LoyaltyTierCode string, TierName string, DiscountPercent decimal(5,2), IsActive boolean",
    "silver.ref_carrier": "CarrierCode string, CarrierName string, ServiceLevelCode string, IsActive boolean",
    "silver.ref_transaction_type": "TransactionTypeCode string, TransactionTypeName string, MovementSign int",
    "silver.ref_warehouse_site": "WarehouseSiteId int, WarehouseSiteCode string, RegionCode string",
    # OLTP extracts the packages join to raw.SqlOrder that the estate never scripted
    "bronze.raw_sql_person": "PersonID int, FullName string, PreferredName string, EmailAddress string, PhoneNumber string, RegionCode string, ValidFrom timestamp, ValidTo timestamp, BatchId bigint",
    "bronze.raw_sql_promotion": "PromotionCode string, PromotionName string, PromotionTypeCode string, DiscountPercent decimal(9,4), DiscountAmount decimal(18,2), RegionCode string, ValidFrom timestamp, ValidTo timestamp, BatchId bigint",
    "bronze.raw_sql_sales_territory": "SalesTerritoryCode string, SalesTerritoryName string, RegionCode string, CountryCode string, ParentTerritoryCode string, BatchId bigint",
    "bronze.raw_sql_salesperson_quota": "SalespersonPersonID int, SalesTerritoryCode string, QuotaAmount decimal(18,2), QuotaCurrencyCode string, QuotaYear int, BatchId bigint",
}


class _Widgets:
    def __init__(self, values):
        self.values = values

    def get(self, name):
        return self.values[name]


class FakeDbutils:
    def __init__(self, values):
        self.widgets = _Widgets(values)


def jobOrder():
    with open(os.path.join(ROOT, "resources", "wwi_04_staging.yml")) as fh:
        tasks = yaml.safe_load(fh)["resources"]["jobs"]["wwi_04_staging"]["tasks"]
    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in tasks}
    ordered = []
    while len(ordered) < len(deps):
        ready = sorted(k for k, d in deps.items() if k not in ordered and all(x in ordered for x in d))
        assert ready, "cycle in job dependencies"
        ordered.extend(ready)
    return ordered


def runNotebook(spark, name, params):
    with open(os.path.join(NOTEBOOKS, name + ".py"), encoding="utf-8") as fh:
        source = fh.read()
    source = re.sub(r"^# MAGIC.*$", "", source, flags=re.M)
    ns = {"spark": spark, "dbutils": FakeDbutils(params), "__name__": "__notebook__"}
    cwd = os.getcwd()
    os.chdir(NOTEBOOKS)
    try:
        exec(compile(source, name + ".py", "exec"), ns)
    finally:
        os.chdir(cwd)
    return ns


@pytest.fixture(scope="module")
def estate(spark):
    ddl_fixtures.createEmptyTables(spark, "spark_catalog", extra=UNSCRIPTED_REFS)
    return spark


ORDER = jobOrder()


def test_job_wires_every_package():
    dtsx = sorted(f[:-5] for f in os.listdir(os.path.join(ROOT, "..", "..", "ssis", "04_staging")) if f.endswith(".dtsx"))
    notebooks = sorted(f[:-3] for f in os.listdir(NOTEBOOKS) if f.endswith(".py"))
    assert dtsx == notebooks == sorted(ORDER)
    assert len(dtsx) == 28


@pytest.mark.parametrize("name", ORDER)
def test_notebook_runs_on_empty_estate(estate, name):
    params = {"BatchId": "0", "BusinessDate": "2024-03-04", "ReloadFullHistory": "False", "EnvironmentCode": "DEV", "RestartFromStep": "", "catalog": "spark_catalog"}
    ns = runNotebook(estate, name, params)
    run = ns["run"]
    assert not run.skipped
    pkg = [x for x in control.STATE["packages"] if x["packageName"] == name][-1]
    assert pkg["status"] == "Succeeded", pkg
