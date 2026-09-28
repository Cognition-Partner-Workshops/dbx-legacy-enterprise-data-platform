# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_PaymentTerms
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_PaymentTerms.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Reference refresh of payment terms from V_PAYMENT_TERMS_EXTRACT, including the discount ladder (2/10 net 30 style) that the AP matching rules read.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OraclePaymentTerms` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_payment_terms` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | none (full load) |
# MAGIC | Load mode | `truncate` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Truncate raw_OraclePaymentTerms -> Load Payment Terms -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA V_PAYMENT_TERMS_EXTRACT` | `WWI_FIN/PAYMENT_TERMS.dat` | EarlyPayIncentiveFlag, EffectiveAnnualisedPct | - | - |
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

spec = specs.getSpec("EXT_ORA_PaymentTerms")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
