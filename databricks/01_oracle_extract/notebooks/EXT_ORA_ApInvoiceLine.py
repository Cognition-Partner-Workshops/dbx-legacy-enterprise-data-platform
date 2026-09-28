# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_ApInvoiceLine
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_ApInvoiceLine.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Numeric-key incremental AP invoice distribution extract. Cost centres are looked up against raw.OracleCostCenter and unmatched distributions are sent to err.RejectedInvoiceLine rather than defaulted to a suspense account.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleApInvoiceLine` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_ap_invoice_line` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_FIN.AP_INVOICE_LINE` (NumericKey) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract AP Invoice Lines -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA AP_INVOICE_LINE` | `WWI_FIN/AP_INVOICE_LINE.dat` | AccrualReversalFlag | - | Lookup Cost Center |
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

spec = specs.getSpec("EXT_ORA_ApInvoiceLine")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
