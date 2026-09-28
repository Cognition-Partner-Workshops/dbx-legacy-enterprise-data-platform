# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_Currency
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_Currency.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Reference refresh of the currency master from V_CURRENCY_EXTRACT. Carries minor-unit precision and the regional reporting currency each ledger rolls up to (USD for NA, EUR for EU, SGD for APAC).
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleCurrency` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_currency` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | none (full load) |
# MAGIC | Load mode | `truncate` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Truncate raw_OracleCurrency -> Load Currencies -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA V_CURRENCY_EXTRACT` | `WWI_REF/CURRENCY_CODE.dat` | - | - | - |
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

spec = specs.getSpec("EXT_ORA_Currency")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
