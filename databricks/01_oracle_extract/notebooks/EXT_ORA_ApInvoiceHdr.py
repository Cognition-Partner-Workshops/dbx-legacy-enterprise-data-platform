# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_ApInvoiceHdr
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_ApInvoiceHdr.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Incremental AP invoice header extract. Regional tax treatment diverges in the source query: NA invoices carry state and local sales tax, EU invoices carry recoverable and non-recoverable VAT with a registration number, APAC invoices carry GST. Invoices sitting on an unresolved hold are excluded.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleApInvoiceHdr` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_ap_invoice_hdr` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_FIN.AP_INVOICE_HDR` (Timestamp) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract AP Invoice Headers -> Count Suppressed Holds -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA V_AP_INVOICE_EXTRACT` | `WWI_FIN/AP_INVOICE_HDR.dat` | TaxRatePct, VatRecoverableFlag | - | - |
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

spec = specs.getSpec("EXT_ORA_ApInvoiceHdr")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
