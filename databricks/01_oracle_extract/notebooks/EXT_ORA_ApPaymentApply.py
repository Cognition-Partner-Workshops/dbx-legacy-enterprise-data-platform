# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_ApPaymentApply
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_ApPaymentApply.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Numeric-key incremental extract of payment applications, denormalised over AP_PAYMENT_APPLY, AP_PAYMENT and AP_INVOICE_HDR so the settlement, the discount taken and the withheld amount arrive on one row.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleApPayment` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_ap_payment` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_FIN.AP_PAYMENT_APPLY` (NumericKey) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract Payment Applications -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA AP_PAYMENT_APPLY` | `WWI_FIN/AP_PAYMENT_APPLY.dat` | SettlementDays, DiscountTakenFlag | - | - |
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

spec = specs.getSpec("EXT_ORA_ApPaymentApply")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
