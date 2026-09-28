# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_ReceiptLine
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_ReceiptLine.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Numeric-key incremental receipt line extract joined to PO_RECEIPT_HDR and the PO line, carrying the price/quantity variance percentage used by the supplier scorecard.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleReceiptLine` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_receipt_line` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_PROC.PO_RECEIPT_LINE` (NumericKey) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract Receipt Lines -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA PO_RECEIPT_LINE` | `WWI_PROC/PO_RECEIPT_LINE.dat` | VarianceBand | Route Inspection Failures | - |
# MAGIC
# MAGIC Job parameters: `BatchId`, `BusinessDate`, `ReloadFullHistory`, `EnvironmentCode`, `RestartFromStep`, `catalog`
# MAGIC (read via `dbx_etl_common.params.getJobParams`). Task parameters: `source_mode` (`jdbc` | `files`),
# MAGIC `oracle_host`, `oracle_port`, `oracle_service`, `oracle_user`, `oracle_secret_scope`, `oracle_password_key`,
# MAGIC `extract_volume_path`, `jdbc_fetch_size`, `jdbc_num_partitions`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from oracle_extract import notebook, specs

# COMMAND ----------

spec = specs.getSpec("EXT_ORA_ReceiptLine")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
