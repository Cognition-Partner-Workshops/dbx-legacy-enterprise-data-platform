# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_SupplierMaster
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_SupplierMaster.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Incremental supplier master extract joined to V_SUPPLIER_BANK_MASKED and SUPP_CERTIFICATION, filtered source-side to suppliers transacted in the last seven years (retention rule) and to non-merged parties.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleSupplierMaster` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_supplier_master` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_MDM.SUPP_MASTER` (Timestamp) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract Supplier Master -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA SUPP_MASTER` | `WWI_MDM/SUPP_MASTER.dat` | CertificationExpiredFlag, WithholdingApplies | - | - |
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

spec = specs.getSpec("EXT_ORA_SupplierMaster")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
