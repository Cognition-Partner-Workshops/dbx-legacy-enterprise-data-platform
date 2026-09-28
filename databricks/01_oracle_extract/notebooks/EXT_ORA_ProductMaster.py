# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_ProductMaster
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_ProductMaster.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Incremental product master extract joined to PRODUCT_CATEGORY and PRODUCT_UOM_CONV, converting Oracle NUMBER to the staging decimal scale and detecting obsoletions recorded in WWI_AUDIT.CHANGE_LOG.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleProductMaster` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_product_master` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_MDM.PRODUCT_MASTER` (Timestamp) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract Product Master -> Detect Obsoleted Products -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA PRODUCT_MASTER` | `WWI_MDM/PRODUCT_MASTER.dat` | StandardCostAmount, ListPriceAmount, SellToBaseFactor, HandlingClass, DeleteFlag | - | - |
# MAGIC | `ORA CHANGE_LOG Product Deletes` | `WWI_AUDIT/CHANGE_LOG__PRODUCT_MASTER_DELETES.dat` | DeleteFlag | - | - |
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

spec = specs.getSpec("EXT_ORA_ProductMaster")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
