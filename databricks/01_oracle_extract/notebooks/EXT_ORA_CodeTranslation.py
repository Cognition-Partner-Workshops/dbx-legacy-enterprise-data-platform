# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_CodeTranslation
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_CodeTranslation.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Reference refresh of WWI_REF.CODE_TRANSLATION - the cryptic legacy code map (status, reason, incoterm, payment method) that every downstream package joins to. Region-specific code sets are kept distinct rather than merged.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleCustomerMaster` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_customer_master` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | none (full load) |
# MAGIC | Load mode | `delete_scope` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Delete Code Rows -> Load Code Translations -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA CODE_TRANSLATION` | `WWI_REF/CODE_TRANSLATION.dat` | - | - | - |
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

spec = specs.getSpec("EXT_ORA_CodeTranslation")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
