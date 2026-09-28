# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_ProductHierarchy
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_ProductHierarchy.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Full truncate-and-load of the flattened product hierarchy. The source query walks PRODUCT_HIERARCHY with CONNECT BY, so the extract is small but expensive and runs on the weekly reference cadence.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleProductMaster` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_product_master` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | none (full load) |
# MAGIC | Load mode | `delete_scope` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Delete Hierarchy Rows -> Load Product Hierarchy -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA PRODUCT_HIERARCHY` | `WWI_MDM/PRODUCT_HIERARCHY.dat` | - | - | - |
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

spec = specs.getSpec("EXT_ORA_ProductHierarchy")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
