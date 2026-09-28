import os

import yaml

from oracle_extract.naming import deltaName, deltaTable, snakeCase
from oracle_extract.specs import PACKAGES, PACKAGE_ORDER

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def test_snake_case_and_delta_names():
    assert snakeCase("OracleCustomerMaster") == "oracle_customer_master"
    assert snakeCase("OracleApInvoiceHdr") == "oracle_ap_invoice_hdr"
    assert snakeCase("OracleFxRate") == "oracle_fx_rate"
    assert deltaName("raw.OracleCustomerMaster") == ("bronze", "raw_oracle_customer_master")
    assert deltaName("err.RejectedCustomer") == ("silver", "err_rejected_customer")
    assert deltaTable("wwi_dev", "raw.OracleGeography") == "wwi_dev.bronze.raw_oracle_geography"


def test_22_packages_match_legacy_dtsx_and_notebooks():
    dtsx = sorted(f[:-5] for f in os.listdir(os.path.join(ROOT, "..", "..", "ssis", "01_oracle_extract")) if f.endswith(".dtsx"))
    notebooks = sorted(f[:-3] for f in os.listdir(os.path.join(ROOT, "notebooks")) if f.endswith(".py"))
    assert len(PACKAGES) == 22
    assert sorted(PACKAGES) == dtsx == notebooks
    assert list(PACKAGE_ORDER) == list(PACKAGES)


def test_job_has_one_task_per_package_with_dependencies():
    with open(os.path.join(ROOT, "resources", "wwi_01_oracle_extract.job.yml")) as fh:
        job = yaml.safe_load(fh)["resources"]["jobs"]["wwi_01_oracle_extract"]
    assert job["name"] == "wwi_01_oracle_extract"
    tasks = {t["task_key"]: t for t in job["tasks"]}
    for name, spec in PACKAGES.items():
        assert name in tasks
        assert tasks[name]["notebook_task"]["notebook_path"] == f"../notebooks/{name}.py"
        deps = {d["task_key"] for d in tasks[name].get("depends_on", [])}
        assert deps == set(spec.dependsOn)
    assert {p["name"] for p in job["parameters"]} == {"BatchId", "BusinessDate", "ReloadFullHistory", "EnvironmentCode", "RestartFromStep", "catalog"}
    assert set(tasks["Reconcile_Raw_Layer"]["depends_on"][0].keys()) == {"task_key"}


def test_dependencies_reference_known_packages():
    for spec in PACKAGES.values():
        for dep in spec.dependsOn:
            assert dep in PACKAGES


def test_watermark_shapes():
    incremental = {n for n, s in PACKAGES.items() if s.isIncremental}
    assert incremental == {
        "EXT_ORA_CustomerMaster", "EXT_ORA_CustomerAddress", "EXT_ORA_SupplierMaster", "EXT_ORA_ProductMaster",
        "EXT_ORA_PurchaseOrderHdr", "EXT_ORA_PurchaseOrderLine", "EXT_ORA_ReceiptLine", "EXT_ORA_ApInvoiceHdr",
        "EXT_ORA_ApInvoiceLine", "EXT_ORA_ApPayment", "EXT_ORA_ApPaymentApply", "EXT_ORA_GlJournalLine", "EXT_ORA_FxRateDaily",
    }
    numeric = {n for n, s in PACKAGES.items() if s.watermarkType == "NumericKey"}
    assert numeric == {"EXT_ORA_PurchaseOrderLine", "EXT_ORA_ReceiptLine", "EXT_ORA_ApInvoiceLine", "EXT_ORA_ApPaymentApply"}
    assert {n for n, s in PACKAGES.items() if s.watermarkType == "DateWindow"} == {"EXT_ORA_GlJournalLine", "EXT_ORA_FxRateDaily"}


def test_shared_bronze_tables_declare_row_ownership():
    byTarget = {}
    for spec in PACKAGES.values():
        byTarget.setdefault(spec.legacyTargetTable, []).append(spec)
    shared = {t: s for t, s in byTarget.items() if len(s) > 1}
    assert set(shared) == {"raw.OracleCustomerMaster", "raw.OracleProductMaster", "raw.OracleApInvoiceHdr", "raw.OracleApPayment"}
    for table, writers in shared.items():
        assert all(w.ownership is not None for w in writers), table
        assert sum(1 for w in writers if w.ownership.defaultOwner) == 1, table
        for w in writers:
            if w.scopePredicate:
                assert w.ownership.predicate == w.scopePredicate
    for table, (spec,) in ((t, s) for t, s in byTarget.items() if len(s) == 1):
        assert spec.ownership is None, table
