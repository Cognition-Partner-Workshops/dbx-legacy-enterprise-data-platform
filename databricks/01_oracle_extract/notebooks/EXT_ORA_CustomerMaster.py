# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_CustomerMaster
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_CustomerMaster.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Incremental customer master extract (LAST_UPDATE_DT watermark with lookback), denormalised over CUST_MASTER, CUST_CLASSIFICATION and CUST_CREDIT_PROFILE, plus a delete-detection pass over WWI_AUDIT.CHANGE_LOG.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleCustomerMaster` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_customer_master` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_MDM.CUST_MASTER` (Timestamp) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract Customer Master -> Detect Deleted Customers -> Capture Insert Count -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA CUST_MASTER` | `WWI_MDM/CUST_MASTER.dat` | DeleteFlag | Route Unusable Customers | - |
# MAGIC | `ORA CHANGE_LOG Customer Deletes` | `WWI_AUDIT/CHANGE_LOG__CUST_MASTER_DELETES.dat` | DeleteFlag | - | - |
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

spec = specs.getSpec("EXT_ORA_CustomerMaster")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
