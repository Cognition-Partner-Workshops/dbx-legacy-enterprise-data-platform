# Databricks notebook source
# MAGIC %md
# MAGIC # ING_FILE_SupplierCatalog
# MAGIC
# MAGIC Migrated from `ssis/03_file_ingestion/ING_FILE_SupplierCatalog.dtsx` (project `WWI_Ingest_Files`, generator `generate_file_ingestion.py`).
# MAGIC
# MAGIC Supplier price catalogue (`inbound/supplier`, ISO-8859-1, pipe, header row). `HDR`/`DTL`/`TRL`
# MAGIC records, yyyyMMdd effective dates (open-ended -> 9999-12-31), `HazardousFlag`, `ListPrice >= NetPrice` rule and the
# MAGIC `TRL` row-count + price checksum (`SUM(NetPrice*100) % 1000000`) control totals. Rejects `CATALOG_BAD`.
# MAGIC
# MAGIC Target: `${catalog}.bronze.raw_file_supplier_catalog` (legacy `raw.FileSupplierCatalog`) with arrival metadata
# MAGIC (`FileName`, `FilePath`, `FileModifiedAtUtc`, `FileSizeBytes`, `ArrivedAtUtc`, `BatchId`, `PackageExecutionId`, `RegionCode`).
# MAGIC
# MAGIC Flow (see `docs/migration/03_file_ingestion-package-mapping.md`):
# MAGIC 1. Auto Loader (`cloudFiles`, `binaryFile`) discovers `supplier_catalog_*.psv` on `/Volumes/${catalog}/bronze/inbound/inbound/supplier` into `bronze.raw_file_inbound_manifest`.
# MAGIC 2. Each pending file is decoded (iso-8859-1), split on `|` with the explicit column list, typed with `try_cast`, and screened; landed rows -> target, malformed rows -> `silver.err_rejected_file_row` + `.rej` file + `control.logRejectedRecordSet`.
# MAGIC 3. Control totals decide Processed (archive `archive/supplier/yyyy/MM/`) vs Quarantined (`quarantine/poison`); duplicates (same name + size already processed) go to `quarantine/supplier/duplicate/`.
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
spec = feeds.SUPPLIER_CATALOG

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
