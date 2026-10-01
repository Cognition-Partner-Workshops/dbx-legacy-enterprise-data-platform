# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC Lands every registered mock CSV under `mock_data_root` into `<catalog>.sales_bronze.<system>_<schema>_<table>`
# MAGIC (see `sales_lakehouse.bronze.ingest.run`). Replaces the SSIS `EXT_SQL_*` / `EXT_ORA_*` extract packages and the
# MAGIC legacy `raw.*` landing tables.

# COMMAND ----------

import os
import sys
import time

dbutils.widgets.text("catalog", "wwi_sales", "Unity Catalog catalog")
dbutils.widgets.text("mock_data_root", "/Volumes/wwi_sales/sales_bronze/mock_source", "Mock source CSV root")
dbutils.widgets.text("batch_id", "", "Batch id (blank = epoch seconds)")
dbutils.widgets.text("tables", "", "Comma-separated bronze tables / Schema.Table names (blank = all)")

catalog = dbutils.widgets.get("catalog").strip() or None
mockDataRoot = dbutils.widgets.get("mock_data_root").strip()
batchIdText = dbutils.widgets.get("batch_id").strip()
batchId = int(batchIdText) if batchIdText else int(time.time())
tablesText = dbutils.widgets.get("tables").strip()
tables = [t.strip() for t in tablesText.split(",") if t.strip()] or None

# COMMAND ----------

# The bundle syncs databricks/ to the workspace; put databricks/src on the path relative to this notebook.
notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
srcPath = os.path.normpath(os.path.join("/Workspace", notebookPath.lstrip("/"), "..", "..", "..", "src"))
if srcPath not in sys.path:
    sys.path.insert(0, srcPath)

from sales_lakehouse.bronze import ingest  # noqa: E402
from sales_lakehouse.common.config import PipelineConfig  # noqa: E402
from sales_lakehouse.common.spark import ensureSchemas  # noqa: E402

cfg = PipelineConfig(catalog=catalog, mockDataRoot=mockDataRoot, batchId=batchId)
ensureSchemas(spark, cfg)

# COMMAND ----------

results = ingest.run(spark, cfg, tables=tables)
display(
    spark.createDataFrame(
        [(r.sourceObject, r.bronzeTable, r.status, r.rowsRead, r.rowsLoaded, r.rowsRejected, r.message) for r in results],
        "source_object string, bronze_table string, status string, rows_read long, rows_loaded long, rows_rejected long, message string",
    )
)

# COMMAND ----------

dbutils.notebook.exit(f"batch_id={batchId} tables={len(results)} missing={sum(r.status == 'MISSING_SOURCE' for r in results)}")
