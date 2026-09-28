# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_VendorContract
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_VendorContract.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Full truncate-and-load of vendor contracts with contract line commitments aggregated in the source query. Contracts are few and change rarely, so the package reloads the whole set rather than tracking a watermark.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleVendorContract` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_vendor_contract` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | none (full load) |
# MAGIC | Load mode | `truncate` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Truncate raw_OracleVendorContract -> Load Vendor Contracts -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA VENDOR_CONTRACT` | `WWI_PROC/VENDOR_CONTRACT.dat` | UtilisationPct, RenewalDueFlag | - | - |
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

spec = specs.getSpec("EXT_ORA_VendorContract")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
