# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_FxRateDaily
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_FxRateDaily.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Date-window FX rate extract. Spot, corporate and period-average rates are pulled for the requested rate-date window; EUR and SGD cross rates are triangulated through USD because the ERP only publishes USD pairs for the minor currencies.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleFxRate` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_fx_rate` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_REF.FX_RATE_DAILY` (DateWindow) |
# MAGIC | Load mode | `delete_window` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Clear Rate Window -> Extract FX Rates -> Count Missing Rate Days -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA FX_RATE_DAILY` | `WWI_REF/FX_RATE_DAILY.dat` | RatePairCd | - | - |
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

spec = specs.getSpec("EXT_ORA_FxRateDaily")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
