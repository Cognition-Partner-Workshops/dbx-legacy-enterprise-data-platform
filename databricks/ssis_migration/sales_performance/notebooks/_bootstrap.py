# Databricks notebook source
"""Shared notebook bootstrap: put ../src on sys.path and read the common task parameters."""

import os
import sys


def _srcPath():
    here = os.getcwd()
    try:
        nbPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
        here = os.path.dirname("/Workspace" + nbPath)
    except Exception:  # noqa: BLE001 - not running inside a Databricks notebook
        pass
    return os.path.abspath(os.path.join(here, "..", "src"))


srcPath = _srcPath()
if srcPath not in sys.path:
    sys.path.insert(0, srcPath)


def taskParam(name, default=""):
    try:
        dbutils.widgets.text(name, str(default))  # noqa: F821
        value = dbutils.widgets.get(name)  # noqa: F821
    except Exception:  # noqa: BLE001
        value = os.environ.get(name.upper(), str(default))
    return value if value not in ("", None) else default


def batchIdParam():
    from sales_performance.common import newBatchId

    raw = taskParam("batch_id", "")
    try:
        return int(str(raw).replace("-", "")[:18]) if raw else newBatchId()
    except ValueError:
        return newBatchId()
