"""Guards that the notebooks only import names from the dbx_etl_common contract."""

import os
import re

NOTEBOOKS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "notebooks")
ALLOWED = {
    "control": {"logPackageStart", "logPackageEnd", "endBatchStep", "endBatch", "logError", "logRowCount",
                "logRejectedRecordSet", "assertRowCountReconciliation"},
    "params": {"getJobParams"},
    "naming": {"table"},
}


def test_notebooks_only_use_contract_functions():
    for name in sorted(os.listdir(NOTEBOOKS)):
        if not name.endswith(".py"):
            continue
        source = open(os.path.join(NOTEBOOKS, name)).read()
        assert source.startswith("# Databricks notebook source"), name
        assert "from dbx_etl_common import control, naming, params" in source, name
        for module, allowed in ALLOWED.items():
            used = set(re.findall(r"\b%s\.(\w+)\(" % module, source))
            assert used <= allowed, (name, module, used - allowed)


def test_no_hard_coded_catalog():
    for name in sorted(os.listdir(NOTEBOOKS)):
        if name.endswith(".py"):
            source = open(os.path.join(NOTEBOOKS, name)).read()
            assert re.search(r"\bwwi_(dev|prod|test)\b", source) is None, name
