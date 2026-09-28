# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_ApPayment
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_ApPayment.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Incremental AP payment extract from V_AP_PAYMENT_EXTRACT. Realised FX gain and loss is computed in the source query against the payment-date rate, and void payments are landed with a reversal flag so downstream nets them off.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleApPayment` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_ap_payment` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_FIN.AP_PAYMENT` (Timestamp) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract AP Payments -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA V_AP_PAYMENT_EXTRACT` | `WWI_FIN/AP_PAYMENT.dat` | ReversalFlag, DaysToClear | Split Void Payments | - |
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

spec = specs.getSpec("EXT_ORA_ApPayment")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
