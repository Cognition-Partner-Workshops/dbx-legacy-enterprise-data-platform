# Databricks notebook source
# MAGIC %md
# MAGIC # ING_FILE_PartnerSales_EU
# MAGIC
# MAGIC Migrated from `ssis/03_file_ingestion/ING_FILE_PartnerSales_EU.dtsx` (project `WWI_Ingest_Files`, generator `generate_file_ingestion.py`).
# MAGIC
# MAGIC Partner drops for Europe (`inbound/partner/eu`, UTF-8, comma, header row - the generator docstring
# MAGIC says semicolon, the emitted connection manager and `config/landing-zone.yaml` say comma; the DTSX wins).
# MAGIC Record types `1`/`2`/`9`; decimal-comma amounts, dd.MM.yyyy dates, VAT backed out of the gross amount, consent flag `J`/`Y`
# MAGIC -> `MarketableFlag`; `9` footer gross total is the control total. Rejects `VAT_MISSING`.
# MAGIC
# MAGIC Target: `${catalog}.bronze.raw_file_partner_sales` (legacy `raw.FilePartnerSales`) with arrival metadata
# MAGIC (`FileName`, `FilePath`, `FileModifiedAtUtc`, `FileSizeBytes`, `ArrivedAtUtc`, `BatchId`, `PackageExecutionId`, `RegionCode`).
# MAGIC
# MAGIC Flow (see `docs/migration/03_file_ingestion-package-mapping.md`):
# MAGIC 1. Auto Loader (`cloudFiles`, `binaryFile`) discovers `partner_sales_eu_*.csv` on `/Volumes/${catalog}/bronze/inbound/inbound/partner/eu` into `bronze.raw_file_inbound_manifest`.
# MAGIC 2. Each pending file is decoded (utf-8), split on `,` with the explicit column list, typed with `try_cast`, and screened; landed rows -> target, malformed rows -> `silver.err_rejected_file_row` + `.rej` file + `control.logRejectedRecordSet`.
# MAGIC 3. Control totals decide Processed (archive `archive/partner/eu/yyyy/MM/`) vs Quarantined (`quarantine/poison`); duplicates (same name + size already processed) go to `quarantine/partner/eu/duplicate/`.
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
spec = feeds.PARTNER_SALES_EU

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
