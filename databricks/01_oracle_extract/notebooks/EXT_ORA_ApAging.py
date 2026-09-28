# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_ApAging
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_ApAging.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Full reload of the AP aging snapshot. The ERP recomputes the buckets nightly, so the package clears the current as-of date and reloads it; the aging bucket boundaries come from the ERP function rather than being reimplemented here.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleApInvoiceHdr` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_ap_invoice_hdr` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | none (full load) |
# MAGIC | Load mode | `delete_snapshot` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Clear Current Snapshot -> Load AP Aging Snapshot -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA V_AP_AGING_CURRENT` | `WWI_FIN/AP_AGING_SNAPSHOT.dat` | - | - | - |
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

spec = specs.getSpec("EXT_ORA_ApAging")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
