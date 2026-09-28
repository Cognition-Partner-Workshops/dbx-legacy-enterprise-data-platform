# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_GlJournalLine
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_GlJournalLine.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Date-window GL journal line extract. The window comes from etl.usp_GetWatermark (DateWindow style) and is re-runnable for any bounded accounting-date range; only periods that the ledger reports as open or recently closed are pulled, and the fiscal period is resolved per region.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleGlJournalLine` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_gl_journal_line` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_FIN.GL_JOURNAL_LINE` (DateWindow) |
# MAGIC | Load mode | `delete_window` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Clear Target Window -> Extract GL Journal Lines -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA GL_JOURNAL_LINE` | `WWI_FIN/GL_JOURNAL_LINE.dat` | NetAmount, FiscalCalendarCd | - | - |
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

spec = specs.getSpec("EXT_ORA_GlJournalLine")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
