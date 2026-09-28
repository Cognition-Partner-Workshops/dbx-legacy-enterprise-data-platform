# Databricks notebook source
# MAGIC %md
# MAGIC # ING_FILE_FxOverride
# MAGIC
# MAGIC Migrated from `ssis/03_file_ingestion/ING_FILE_FxOverride.dtsx` (project `WWI_Ingest_Files`, generator `generate_file_ingestion.py`).
# MAGIC
# MAGIC Treasury FX override requests (`inbound/treasury`, Windows-1252, comma, header row). `FXO` rows are
# MAGIC looked up against the published SPOT rates (`bronze.raw_oracle_fx_rate`); precedence exactly as the data flow:
# MAGIC unknown pair -> `FX_UNKNOWN_PAIR`, then the four-eyes gate (approver != requester, approval ticket present,
# MAGIC deviation <= 500 bp) -> `FX_TOLERANCE` rejects. Runs before the partner drops so approved overrides are available.
# MAGIC
# MAGIC Target: `${catalog}.bronze.raw_file_fx_override` (legacy `raw.FileFxOverride`) with arrival metadata
# MAGIC (`FileName`, `FilePath`, `FileModifiedAtUtc`, `FileSizeBytes`, `ArrivedAtUtc`, `BatchId`, `PackageExecutionId`, `RegionCode`).
# MAGIC
# MAGIC Flow (see `docs/migration/03_file_ingestion-package-mapping.md`):
# MAGIC 1. Auto Loader (`cloudFiles`, `binaryFile`) discovers `fx_override_*.csv` on `/Volumes/${catalog}/bronze/inbound/inbound/treasury` into `bronze.raw_file_inbound_manifest`.
# MAGIC 2. Each pending file is decoded (windows-1252), split on `,` with the explicit column list, typed with `try_cast`, and screened; landed rows -> target, malformed rows -> `silver.err_rejected_file_row` + `.rej` file + `control.logRejectedRecordSet`.
# MAGIC 3. Control totals decide Processed (archive `archive/treasury/yyyy/MM/`) vs Quarantined (`quarantine/poison`); duplicates (same name + size already processed) go to `quarantine/treasury/duplicate/`.
# MAGIC 4. `control.logRowCount` / `control.logPackageEnd` close the package execution.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, params  # session 00 wheel (job library)

from wwi_file_ingestion import feeds, notebook_support

# COMMAND ----------

notebook_support.defineWidgets(dbutils)
p = params.getJobParams(dbutils)
extra = notebook_support.readExtraParams(dbutils)
catalog = p["catalog"]
spec = feeds.FX_OVERRIDE

# COMMAND ----------

summary = notebook_support.runPackageNotebook(
    spark, dbutils, control, p, spec, catalog,
    controlTotalMode=extra["controlTotalMode"], useAutoLoader=extra["useAutoLoader"],
)

# COMMAND ----------

for message in summary.warnings:
    print("WARNING", message)
print(
    "%s: files=%d processed=%d quarantined=%d duplicate=%d rowsRead=%d rowsInserted=%d rowsRejected=%d"
    % (spec.packageName, summary.filesDiscovered, summary.filesProcessed, summary.filesQuarantined,
       summary.filesDuplicate, summary.rowsRead, summary.rowsInserted, summary.rowsRejected)
)
dbutils.notebook.exit(notebook_support.summaryJson(summary))
