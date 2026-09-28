# Databricks notebook source
# MAGIC %md
# MAGIC # ING_FILE_CarrierScan
# MAGIC
# MAGIC Migrated from `ssis/03_file_ingestion/ING_FILE_CarrierScan.dtsx` (project `WWI_Ingest_Files`, generator `generate_file_ingestion.py`).
# MAGIC
# MAGIC Carrier tracking scans (`inbound/carrier`, Windows-1252, comma, header row). No record types: every
# MAGIC well-formed line is a scan event. ISO timestamp split into wall-clock + offset minutes exactly as the derived column did,
# MAGIC `DeliveredFlag` for `DLV`/`POD`, duplicate scans are counted (never rejected), expected row count comes from
# MAGIC the `etl.file_control_total` sidecar. Rejects `SCAN_MALFORMED`.
# MAGIC
# MAGIC Target: `${catalog}.bronze.raw_file_carrier_scan` (legacy `raw.FileCarrierScan`) with arrival metadata
# MAGIC (`FileName`, `FilePath`, `FileModifiedAtUtc`, `FileSizeBytes`, `ArrivedAtUtc`, `BatchId`, `PackageExecutionId`, `RegionCode`).
# MAGIC
# MAGIC Flow (see `docs/migration/03_file_ingestion-package-mapping.md`):
# MAGIC 1. Auto Loader (`cloudFiles`, `binaryFile`) discovers `carrier_scan_*.csv` on `/Volumes/${catalog}/bronze/inbound/inbound/carrier` into `bronze.raw_file_inbound_manifest`.
# MAGIC 2. Each pending file is decoded (windows-1252), split on `,` with the explicit column list, typed with `try_cast`, and screened; landed rows -> target, malformed rows -> `silver.err_rejected_file_row` + `.rej` file + `control.logRejectedRecordSet`.
# MAGIC 3. Control totals decide Processed (archive `archive/carrier/yyyy/MM/`) vs Quarantined (`quarantine/poison`); duplicates (same name + size already processed) go to `quarantine/carrier/duplicate/`.
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
spec = feeds.CARRIER_SCAN

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
