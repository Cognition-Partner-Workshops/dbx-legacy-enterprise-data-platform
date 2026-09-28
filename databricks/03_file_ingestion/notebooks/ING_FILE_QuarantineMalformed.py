# Databricks notebook source
# MAGIC %md
# MAGIC # ING_FILE_QuarantineMalformed
# MAGIC
# MAGIC Migrated from `ssis/03_file_ingestion/ING_FILE_QuarantineMalformed.dtsx` (project `WWI_Ingest_Files`, generator `generate_file_ingestion.py`).
# MAGIC
# MAGIC Sweeps `quarantine_rejects_*.dat` files from the quarantine volume: every raw line is recorded in
# MAGIC `silver.err_rejected_file_row` with its origin feed, replay-eligible lines mark the file `Swept` (archived), files with
# MAGIC no replayable line are moved to `quarantine/poison`. Runs last (`run_if: ALL_DONE`) like the master's Completion edges.
# MAGIC
# MAGIC Target: `${catalog}.silver.err_rejected_file_row` (legacy `err.RejectedFileRow`) with arrival metadata
# MAGIC (`FileName`, `FilePath`, `FileModifiedAtUtc`, `FileSizeBytes`, `ArrivedAtUtc`, `BatchId`, `PackageExecutionId`, `RegionCode`).
# MAGIC
# MAGIC Flow (see `docs/migration/03_file_ingestion-package-mapping.md`):
# MAGIC 1. Auto Loader (`cloudFiles`, `binaryFile`) discovers `quarantine_rejects_*.dat` on `/Volumes/${catalog}/bronze/inbound/quarantine` into `bronze.raw_file_inbound_manifest`.
# MAGIC 2. Each pending file is decoded (utf-8), split on `(raw line, no split)` with the explicit column list, typed with `try_cast`, and screened; landed rows -> target, malformed rows -> `silver.err_rejected_file_row` + `.rej` file + `control.logRejectedRecordSet`.
# MAGIC 3. Control totals decide Processed (archive `archive/quarantine/yyyy/MM/`) vs Quarantined (`quarantine/poison`); duplicates (same name + size already processed) go to `quarantine/quarantine/duplicate/`.
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
spec = feeds.QUARANTINE_MALFORMED

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
