# Databricks notebook source
# Shared bootstrap for the thin task notebooks: put the bundle's src/ on sys.path and build a RunContext from widgets.
import os
import sys

notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
bundleRoot = os.path.normpath(os.path.join("/Workspace", notebookPath.lstrip("/"), "..", ".."))
srcPath = os.path.join(bundleRoot, "src")
if srcPath not in sys.path:
    sys.path.insert(0, srcPath)

from logistics_returns.config import DEFAULT_CATALOG, DEFAULT_SCHEMA, RunContext, parseBool  # noqa: E402

dbutils.widgets.text("catalog", DEFAULT_CATALOG, "Unity Catalog catalog")  # noqa: F821
dbutils.widgets.text("schema", DEFAULT_SCHEMA, "Landing schema")  # noqa: F821
dbutils.widgets.text("batch_id", "", "Batch id (blank = yyyymmdd01)")  # noqa: F821
dbutils.widgets.text("package_execution_id", "", "Package execution id (blank = epoch seconds)")  # noqa: F821
dbutils.widgets.text("git_sha", "unknown", "Git commit that produced this run")  # noqa: F821
dbutils.widgets.text("reload_full_history", "false", "Ignore watermarks / processed-file log and reload")  # noqa: F821
dbutils.widgets.text("seed_from_legacy_raw", "true", "Seed bronze from the legacy raw.* baseline on first run")  # noqa: F821


def buildContext() -> RunContext:
    w = dbutils.widgets  # noqa: F821
    return RunContext(
        spark=spark,  # noqa: F821
        catalog=w.get("catalog").strip() or DEFAULT_CATALOG,
        schema=w.get("schema").strip() or DEFAULT_SCHEMA,
        batchId=int(w.get("batch_id").strip() or 0),
        packageExecutionId=int(w.get("package_execution_id").strip() or 0),
        gitSha=w.get("git_sha").strip() or "unknown",
        reloadFullHistory=parseBool(w.get("reload_full_history")),
        seedFromLegacyRaw=parseBool(w.get("seed_from_legacy_raw")),
    )
