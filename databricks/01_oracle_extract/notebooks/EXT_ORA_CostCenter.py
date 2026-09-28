# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_CostCenter
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_CostCenter.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Full truncate-and-load of the cost centre hierarchy from V_COST_CENTER_HIERARCHY, including the allocation rule that applies to each centre. Small dimension, reloaded whole every night.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleCostCenter` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_cost_center` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | none (full load) |
# MAGIC | Load mode | `truncate` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Truncate raw_OracleCostCenter -> Load Cost Centers -> Assign Surrogate Keys -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA V_COST_CENTER_HIERARCHY` | `WWI_FIN/COST_CENTER.dat` | IsActive, CostCenterKey | - | - |
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

spec = specs.getSpec("EXT_ORA_CostCenter")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
