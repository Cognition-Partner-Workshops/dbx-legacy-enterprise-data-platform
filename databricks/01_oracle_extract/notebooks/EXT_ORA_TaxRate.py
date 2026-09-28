# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_TaxRate
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_TaxRate.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Reference refresh of tax rates on the weekly reference cadence. The source query unions the three regimes the estate actually runs - NA state/county sales tax, EU VAT with reduced and zero rates, APAC GST - because they live in TAX_RATE with different jurisdiction granularity.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleTaxRate` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_tax_rate` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | none (full load) |
# MAGIC | Load mode | `truncate` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Truncate raw_OracleTaxRate -> Load Tax Rates -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA TAX_RATE` | `WWI_FIN/TAX_RATE.dat` | RateBasisPoints, CurrentFlag | - | - |
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

spec = specs.getSpec("EXT_ORA_TaxRate")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
