# 01_oracle_extract (WWI_Extract_Oracle) -> Databricks package mapping

Legacy source: `ssis/01_oracle_extract/` — **22** `EXT_ORA_*` packages emitted by `generate_oracle_extracts.py`
(connection `WWI_Oracle_ERP.conmgr` -> `WWI_Staging_DB.conmgr`, project parameters in `Project.params`).
Target: bundle `databricks/01_oracle_extract/` (`wwi_01_oracle_extract`), one notebook per package, one job.

> **Package count.** The assignment says 23 packages; the generator defines 22 builders and the directory contains
> 22 `.dtsx` files (`docs/inventories/ssis-packages.csv` also lists 22 for this project). All 22 are migrated; see
> §7 for the discrepancy.

Every notebook is a thin wrapper (`notebooks/<Package>.py`) over a declarative spec in
`src/oracle_extract/specs.py` (generated from the generator + emitted .dtsx: verbatim Oracle SQL, columns/types,
derived-column expressions, conditional splits, lookups, precedence, pre-load SQL) executed by
`src/oracle_extract/runner.py`. The shared control layer is consumed exactly as contracted
(`from dbx_etl_common import control, naming, params`); nothing under `databricks/common/` is copied.
`tests/dbx_etl_common/` is a test-only fake.

## 1. Package -> notebook -> task

Task keys equal the legacy package names. `depends_on` edges: `docs/inventories/package-dependencies.csv` has no
package-to-package edges for this project (the master ran the 22 packages as four parallel streams of phase
`Extract Oracle`), so the edges below are the data dependencies inside the phase (lookups on a sibling's raw table,
`RecordKind` slices appended to a sibling's raw table). `Reconcile_Raw_Layer` (`run_if: ALL_DONE`) runs last.

| Legacy package | Notebook / task | Legacy target | Bronze table (`${catalog}.`) | Load | Watermark (`etl.watermark` ObjectName / type) | JDBC partitionColumn | depends_on |
|---|---|---|---|---|---|---|---|
| `EXT_ORA_CustomerMaster` | `notebooks/EXT_ORA_CustomerMaster.py` | `raw.OracleCustomerMaster` | `bronze.raw_oracle_customer_master` | append | `WWI_MDM.CUST_MASTER` / Timestamp | `CUST_ID` | – |
| `EXT_ORA_CustomerAddress` | `notebooks/EXT_ORA_CustomerAddress.py` | `raw.OracleCustomerAddress` | `bronze.raw_oracle_customer_address` | append | `WWI_MDM.CUST_ADDRESS` / Timestamp | `CUST_ADDRESS_ID` | `EXT_ORA_Geography` |
| `EXT_ORA_SupplierMaster` | `notebooks/EXT_ORA_SupplierMaster.py` | `raw.OracleSupplierMaster` | `bronze.raw_oracle_supplier_master` | append | `WWI_MDM.SUPP_MASTER` / Timestamp | `SUPP_ID` | – |
| `EXT_ORA_ProductMaster` | `notebooks/EXT_ORA_ProductMaster.py` | `raw.OracleProductMaster` | `bronze.raw_oracle_product_master` | append | `WWI_MDM.PRODUCT_MASTER` / Timestamp | `PRODUCT_ID` | – |
| `EXT_ORA_ProductHierarchy` | `notebooks/EXT_ORA_ProductHierarchy.py` | `raw.OracleProductMaster` (`RecordKind='HIERARCHY'`) | `bronze.raw_oracle_product_master` | delete scope + append (full) | – | – | `EXT_ORA_ProductMaster` |
| `EXT_ORA_PurchaseOrderHdr` | `notebooks/EXT_ORA_PurchaseOrderHdr.py` | `raw.OraclePurchaseOrderHdr` | `bronze.raw_oracle_purchase_order_hdr` | append | `WWI_PROC.PURCHASE_ORDER_HDR` / Timestamp | `PO_HDR_ID` | – |
| `EXT_ORA_PurchaseOrderLine` | `notebooks/EXT_ORA_PurchaseOrderLine.py` | `raw.OraclePurchaseOrderLine` | `bronze.raw_oracle_purchase_order_line` | append | `WWI_PROC.PURCHASE_ORDER_LINE` / NumericKey (upper bound = `MAX(PO_LINE_ID)` probe) | `PO_LINE_ID` | – |
| `EXT_ORA_ReceiptLine` | `notebooks/EXT_ORA_ReceiptLine.py` | `raw.OracleReceiptLine` | `bronze.raw_oracle_receipt_line` | append | `WWI_PROC.PO_RECEIPT_LINE` / NumericKey (lower bound only) | `RECEIPT_LINE_ID` | – |
| `EXT_ORA_VendorContract` | `notebooks/EXT_ORA_VendorContract.py` | `raw.OracleVendorContract` | `bronze.raw_oracle_vendor_contract` | truncate (full) | – | – | – |
| `EXT_ORA_ApInvoiceHdr` | `notebooks/EXT_ORA_ApInvoiceHdr.py` | `raw.OracleApInvoiceHdr` | `bronze.raw_oracle_ap_invoice_hdr` | append | `WWI_FIN.AP_INVOICE_HDR` / Timestamp | `AP_INVOICE_ID` | – |
| `EXT_ORA_ApInvoiceLine` | `notebooks/EXT_ORA_ApInvoiceLine.py` | `raw.OracleApInvoiceLine` | `bronze.raw_oracle_ap_invoice_line` | append | `WWI_FIN.AP_INVOICE_LINE` / NumericKey | `AP_INVOICE_LINE_ID` | `EXT_ORA_CostCenter` |
| `EXT_ORA_ApPayment` | `notebooks/EXT_ORA_ApPayment.py` | `raw.OracleApPayment` | `bronze.raw_oracle_ap_payment` | append | `WWI_FIN.AP_PAYMENT` / Timestamp | `AP_PAYMENT_ID` | – |
| `EXT_ORA_ApPaymentApply` | `notebooks/EXT_ORA_ApPaymentApply.py` | `raw.OracleApPayment` | `bronze.raw_oracle_ap_payment` | append | `WWI_FIN.AP_PAYMENT_APPLY` / NumericKey | `PAYMENT_APPLY_ID` | `EXT_ORA_ApPayment` |
| `EXT_ORA_ApAging` | `notebooks/EXT_ORA_ApAging.py` | `raw.OracleApInvoiceHdr` (`RecordKind='AGING'`) | `bronze.raw_oracle_ap_invoice_hdr` | delete today's snapshot + append (full) | – | – | `EXT_ORA_ApInvoiceHdr` |
| `EXT_ORA_GlJournalLine` | `notebooks/EXT_ORA_GlJournalLine.py` | `raw.OracleGlJournalLine` | `bronze.raw_oracle_gl_journal_line` | delete window + append | `WWI_FIN.GL_JOURNAL_LINE` / DateWindow (`ACCOUNTING_DT`) | `GL_JOURNAL_LINE_ID` | – |
| `EXT_ORA_CostCenter` | `notebooks/EXT_ORA_CostCenter.py` | `raw.OracleCostCenter` | `bronze.raw_oracle_cost_center` | truncate (full) | – | – | – |
| `EXT_ORA_TaxRate` | `notebooks/EXT_ORA_TaxRate.py` | `raw.OracleTaxRate` | `bronze.raw_oracle_tax_rate` | truncate (full) | – | – | – |
| `EXT_ORA_PaymentTerms` | `notebooks/EXT_ORA_PaymentTerms.py` | `raw.OraclePaymentTerms` | `bronze.raw_oracle_payment_terms` | truncate (full) | – | – | – |
| `EXT_ORA_Currency` | `notebooks/EXT_ORA_Currency.py` | `raw.OracleCurrency` | `bronze.raw_oracle_currency` | truncate (full) | – | – | – |
| `EXT_ORA_FxRateDaily` | `notebooks/EXT_ORA_FxRateDaily.py` | `raw.OracleFxRate` | `bronze.raw_oracle_fx_rate` | delete window + append | `WWI_REF.FX_RATE_DAILY` / DateWindow (`RATE_DT`) | – | `EXT_ORA_Currency` |
| `EXT_ORA_Geography` | `notebooks/EXT_ORA_Geography.py` | `raw.OracleGeography` | `bronze.raw_oracle_geography` | truncate (full) | – | – | – |
| `EXT_ORA_CodeTranslation` | `notebooks/EXT_ORA_CodeTranslation.py` | `raw.OracleCustomerMaster` (`RecordKind='CODEXREF'`) | `bronze.raw_oracle_customer_master` | delete scope + append (full) | – | – | `EXT_ORA_CustomerMaster` |
| (validation) | `validation/raw_layer_reconciliation.py` / `Reconcile_Raw_Layer` | `validation/runtime/02_row_count_reconciliation.sql` | `etl.row_count_log` | – | – | – | all 22 |

Naming rule (`src/oracle_extract/naming.py`): `raw.X` -> `bronze.raw_<snake_case(X)>`, `err.X` -> `silver.err_x`
(only used for reject object names), qualified through `dbx_etl_common.naming.table(catalog, schema, table)`;
the catalog always comes from the `catalog` job parameter.

## 2. Source objects -> bronze columns

Every bronze table mirrors the legacy `raw.OracleX` DDL (`sqlserver/staging/tables/10_raw_tables_oracle.sql`): the
Oracle projection of the legacy OLE DB source query (same names, Spark types from the raw DDL types:
`NUMBER(12)` -> `decimal(12,0)`, `NUMBER(18,4)` -> `decimal(18,4)`, `VARCHAR2` -> `string`, `DATE`/`TIMESTAMP` -> `timestamp`),
the legacy derived columns, plus the ingestion metadata columns
`BatchId`, `PackageExecutionId`, `ExtractedAtUtc`, `SourceSystemCode`, `WatermarkFrom`, `WatermarkTo`
(replacing the legacy `LoadedAtUtc` / `SourceRowNumber` audit derivations; `SourceRowNumber` is not reproduced —
see §7). Shared tables carry the union of their writers' columns (Delta `mergeSchema`): `RecordKind` is `NULL` for the
owning package's rows and `'HIERARCHY'` / `'AGING'` / `'CODEXREF'` for the slices; `PAYMENT_APPLY_ID` is `NULL` for
`EXT_ORA_ApPayment` rows and populated for `EXT_ORA_ApPaymentApply` rows.

| Oracle source (first `FROM` object of the legacy query; DDL in `oracle/tables`, `oracle/views`) | Package | Generated extract file (`source_mode=files`, under `extract_volume_path`) |
|---|---|---|
| `WWI_MDM.CUST_MASTER`, `WWI_AUDIT.CHANGE_LOG` | CustomerMaster | `WWI_MDM/CUST_MASTER.dat`, `WWI_AUDIT/CHANGE_LOG__CUST_MASTER_DELETES.dat` |
| `WWI_MDM.V_CUSTOMER_ADDRESS_CURRENT` | CustomerAddress | `WWI_MDM/CUST_ADDRESS.dat` |
| `WWI_MDM.SUPP_MASTER` | SupplierMaster | `WWI_MDM/SUPP_MASTER.dat` |
| `WWI_MDM.PRODUCT_MASTER`, `WWI_AUDIT.CHANGE_LOG` | ProductMaster | `WWI_MDM/PRODUCT_MASTER.dat`, `WWI_AUDIT/CHANGE_LOG__PRODUCT_MASTER_DELETES.dat` |
| `WWI_MDM.PRODUCT_HIERARCHY` | ProductHierarchy | `WWI_MDM/PRODUCT_HIERARCHY.dat` |
| `WWI_PROC.V_PURCHASE_ORDER_EXTRACT` | PurchaseOrderHdr | `WWI_PROC/PURCHASE_ORDER_HDR.dat` |
| `WWI_PROC.V_PO_LINE_EXTRACT` | PurchaseOrderLine | `WWI_PROC/PURCHASE_ORDER_LINE.dat` |
| `WWI_PROC.PO_RECEIPT_LINE` | ReceiptLine | `WWI_PROC/PO_RECEIPT_LINE.dat` |
| `WWI_PROC.VENDOR_CONTRACT` | VendorContract | `WWI_PROC/VENDOR_CONTRACT.dat` |
| `WWI_FIN.V_AP_INVOICE_EXTRACT` | ApInvoiceHdr | `WWI_FIN/AP_INVOICE_HDR.dat` |
| `WWI_FIN.AP_INVOICE_LINE` | ApInvoiceLine | `WWI_FIN/AP_INVOICE_LINE.dat` |
| `WWI_FIN.V_AP_PAYMENT_EXTRACT` | ApPayment | `WWI_FIN/AP_PAYMENT.dat` |
| `WWI_FIN.AP_PAYMENT_APPLY` | ApPaymentApply | `WWI_FIN/AP_PAYMENT_APPLY.dat` |
| `WWI_FIN.V_AP_AGING_CURRENT` | ApAging | `WWI_FIN/AP_AGING_SNAPSHOT.dat` |
| `WWI_FIN.GL_JOURNAL_LINE` | GlJournalLine | `WWI_FIN/GL_JOURNAL_LINE.dat` |
| `WWI_FIN.V_COST_CENTER_HIERARCHY` | CostCenter | `WWI_FIN/COST_CENTER.dat` |
| `WWI_FIN.TAX_RATE` | TaxRate | `WWI_FIN/TAX_RATE.dat` |
| `WWI_FIN.V_PAYMENT_TERMS_EXTRACT` | PaymentTerms | `WWI_FIN/PAYMENT_TERMS.dat` |
| `WWI_REF.V_CURRENCY_EXTRACT` | Currency | `WWI_REF/CURRENCY_CODE.dat` |
| `WWI_REF.FX_RATE_DAILY` | FxRateDaily | `WWI_REF/FX_RATE_DAILY.dat` |
| `WWI_REF.V_GEOGRAPHY_EXTRACT` | Geography | `WWI_REF/GEOGRAPHY.dat` |
| `WWI_REF.CODE_TRANSLATION` | CodeTranslation | `WWI_REF/CODE_TRANSLATION.dat` |

The exact file name per data-flow source is `SourceQuery.fileName + ".dat"` in `specs.py` (pipe-delimited, no header,
column order = the Oracle projection; `generators/README.md`).

## 3. SSIS component -> Spark construct

### Control flow (identical in all 22 packages)
| SSIS task | Databricks |
|---|---|
| `Init Batch Variables` (Expression Task) | `notebook.readSettings` -> `params.getJobParams(dbutils)` (BatchId, BusinessDate, ReloadFullHistory, EnvironmentCode, RestartFromStep, catalog); `BatchId=0` -> `runner.ensureBatch` (`control.startBatch(..., batchName="WWI_Extract_Oracle", allowAdoptRunning=True)`) |
| `Log Package Start` (`etl.usp_LogPackageStart`) | `control.logPackageStart(spark, catalog, batchId, packageName, projectName="WWI_Extract_Oracle", stepName="Extract Oracle")` |
| `Get Watermark` (`etl.usp_GetWatermark @ReloadFullHistory`) | `control.getWatermark(spark, catalog, "ORA_ERP", watermarkObject, reloadFullHistory)` -> `watermark.buildWindow` (Timestamp `[from, to)`, NumericKey `(from, to]`, DateWindow `[from, to)`) |
| `Read Source Max Key` (PurchaseOrderLine) | `OracleJdbcReader.scalar(numericUpperBoundSql)` (files mode: `MAX()` over the extract file) |
| `TRUNCATE TABLE raw.X` / `DELETE ... WHERE RecordKind = ...` / `DELETE ... WHERE <date> BETWEEN` | `bronze_writer.prepareTarget`: overwrite / `DELETE FROM` Delta with the same predicate (`deleteScope`, window delete on the bronze column) |
| Data Flow Task (per source, see below) | `runner.runSource` -> `writeBronze(df, mode)` (`format("delta").saveAsTable`, `mergeSchema`) |
| `Capture Insert Count` / `Count Suppressed Holds` / `Count Missing Rate Days` (Execute SQL, result -> variable) | `runner.applyTargetPostSteps`: Spark SQL `COUNT(*)` over the written bronze table (`summary.counters`) |
| `Assign Cost Center Surrogate Keys` (`UPDATE ... ROW_NUMBER() OVER (ORDER BY COST_CENTER_CD)`) | `transforms.assignRowNumberKey` before the write |
| `Set Watermark` (`etl.usp_SetWatermark`) | `control.setWatermark(spark, catalog, "ORA_ERP", watermarkObject, windowTo, packageExecutionId, allowRewind=reloadFullHistory)` — only after the bronze write; skipped when the window upper bound is `NULL` (ReceiptLine, like the legacy procedure) |
| `Log Row Counts` (`etl.usp_LogRowCount`) | `control.logRowCount(spark, catalog, packageExecutionId, "raw.OracleX", sourceRowCount=rowsRead, targetRowCount=rowsInserted, insertRowCount, deleteRowCount, rejectRowCount)` |
| `Log Package Success` (`etl.usp_LogPackageEnd`) | `control.logPackageEnd(..., status="Succeeded", rowsRead, rowsInserted, rowsDeleted, rowsRejected, watermarkFrom, watermarkTo)` |
| `OnError` handler (`Log Error` + `Mark Execution Failed`) | `except: control.logError(...); control.logPackageEnd(status="Failed"); raise` |
| `ReloadFullHistory = True` | epoch window from `control.getWatermark`; the package's own bronze rows are replaced (`bronze_writer.deleteOwnRows`: sole writer -> overwrite, shared table -> `DELETE` the package's slice then append) and `setWatermark(allowRewind=True)` |

### Data flow components
| SSIS component | Databricks (`src/oracle_extract/`) |
|---|---|
| OLE DB Source (`WWI_Oracle_ERP`, parameterised SQL `?`) | `source_reader.OracleJdbcReader`: `spark.read.format("jdbc")` with `driver=oracle.jdbc.OracleDriver`, `url=jdbc:oracle:thin:@//host:port/service`, `fetchsize`, `queryTimeout`; watermark bounds bound as literals into the legacy SQL (`watermark.bindOracleSql`) so the window is pushed down to Oracle. Big tables (`partitionColumn` above): `MIN/MAX` probe of the key inside the window -> `partitionColumn`/`lowerBound`/`upperBound`/`numPartitions` (job parameter `jdbc_num_partitions`); trailing `ORDER BY` stripped for partitioned reads. `source_mode=files`: `source_reader.GeneratedFileReader` reads `extract_volume_path/<fileName>.dat` with the spec schema and applies the same window as a DataFrame filter. Both implement `read(source, window)` and are selected by `source_reader.buildReader(source_mode, ...)`. |
| Derived Column | `transforms.applyDerivedColumns` — every SSIS expression translated to Spark SQL (`DerivedColumn.legacyExpr` keeps the original): e.g. `UPPER(TRIM(...))`, `DATEDIFF("dd", ...)` -> `datediff`, `ISNULL(x) ? a : b` -> `CASE WHEN x IS NULL`, `(DT_NUMERIC,18,4)` -> `cast(... as decimal(18,4))`, `REPLACE`, `ABS`, `LEN`, `SUBSTRING` -> `substring` |
| Conditional Split (`Route Unusable Customers`, `Split Cancelled Orders`, `Route Inspection Failures`, `Split Void Payments`) | `transforms.applyConditionalSplit` -> matched / default DataFrames. Default outputs that fed an `err.*` destination (`CUST_UNUSABLE`, `PAY_VOID`) become `control.logRejectedRecordSet(..., rejectStage="Extract")`; default outputs that were still loaded (cancelled POs, failed inspections) stay in bronze |
| Lookup (`Lookup Geography Key` on `raw.OracleGeography`, `Lookup Cost Center` on `raw.OracleCostCenter`; no-match -> `err.RejectedLookupFailure`) | `transforms.applyLookup`: left join on the (deduplicated) sibling bronze table, matched rows carry the looked-up key, unmatched rows -> `control.logRejectedRecordSet(rejectReasonCode="GEO_NOMATCH" / "CC_NOMATCH")` (task `depends_on` the producing package) |
| Union All (delete-detection flows merged with the main flow) | both sources written to the same bronze table (`DeleteFlag='Y'` rows from `CHANGE_LOG`) |
| Row Count (`User::RowsRead`, `RowsDeleted`, ...) | `DataFrame.count()` -> `RunSummary.rowsRead / rowsInserted / rowsDeleted / rowsRejected` |
| Derived audit columns (`BatchId`, `PackageExecutionId`, `LoadedAtUtc`, `SourceSystemCode`, ...) | `transforms.addIngestionMetadata` (`BatchId`, `PackageExecutionId`, `ExtractedAtUtc`, `SourceSystemCode`, `WatermarkFrom`, `WatermarkTo`) + `applyConstants` for `RecordKind` |
| OLE DB Destination (`WWI_Staging_DB`, fast load into `raw.X`, error output -> `err.*`) | `bronze_writer.writeBronze` (Delta append/overwrite). Destination error output rows (constraint violations) cannot occur on Delta; `legacyErrTable` is recorded in the spec for traceability |

### Reconciliation (`validation/raw_layer_reconciliation.py`, task `Reconcile_Raw_Layer`)
`validation/runtime/02_row_count_reconciliation.sql` queries 1, 2, 3 and 5 are ported to Spark SQL over `etl.row_count_log` /
`etl.package_execution` / `etl.rejected_record` (`reconciliation.hopVarianceSql`). For each of the 18 bronze tables the
notebook computes `COUNT(*)` (per `BatchId` when given) and a deterministic hash
(`sum(xxhash64(concat_ws('|', coalesce(cast(col as string), ''))))` over the business columns, metadata excluded), compares
with the SQL Server baseline supplied as a Delta table (`baseline_table`) or JSON (`baseline_json`:
`{"raw.OracleCustomerMaster": {"rowCount": 123, "rowHash": "..."}}`) and writes one `control.logRowCount` per table
(`SourceRowCount` = baseline, `TargetRowCount` = bronze). Missing baselines are reported, mismatches fail the task.

## 4. Parameters

| Legacy (`Project.params` / package parameter) | Databricks | Default |
|---|---|---|
| `BatchId`, `BusinessDate`, `ReloadFullHistory`, `EnvironmentCode`, `RestartFromStep` | job parameters (string) read via `params.getJobParams` | `"0"`, `${var.businessDate}`, `"False"`, `${var.environment_code}` (`DEV`/`PROD`), `""` |
| (staging catalog) | job parameter `catalog` | `${var.catalog}` = `wwi_${bundle.target}` |
| `OracleHost`, `OraclePort`, `OracleService`, `OracleUser` | task parameters `oracle_host`, `oracle_port`, `oracle_service`, `oracle_user` (bundle variables) | `""`, `1521`, `WWIGERP`, `WWI_EXTRACT` |
| `OraclePassword` (sensitive project parameter) | `dbutils.secrets.get(oracle_secret_scope, oracle_password_key)` | scope `wwi`, key `oracle-erp-password` |
| `OracleFetchArraySize` / `CommandTimeout` | `jdbc_fetch_size`, `SourceQuery.timeoutSeconds` (3600, from the .dtsx) | `10000` |
| – | `source_mode` (`jdbc` \| `files`), `extract_volume_path`, `jdbc_num_partitions` | `jdbc`, `/Volumes/${catalog}/bronze/oracle_extracts`, `8` |
| `RejectThresholdPercent` | not enforced in bronze (see §7) | – |
| `RestartFromStep` | accepted and logged; a single-notebook package has no restartable inner steps | – |

Shared wheel: each task lists `libraries: - whl: ${var.common_wheel_path}` (default
`/Workspace/Shared/wwi/dbx_etl_common/dbx_etl_common-0.1.0-py3-none-any.whl`) so `dbx_etl_common` resolves at cluster start;
the notebooks add `../src` to `sys.path` for `oracle_extract`. The Oracle thin driver is a Maven library
(`${var.oracle_jdbc_coordinates}`, default `com.oracle.database.jdbc:ojdbc11:23.5.0.24.07`).

## 5. Control-framework calls used

`control.startBatch`, `control.logPackageStart`, `control.getWatermark`, `control.setWatermark`,
`control.logRejectedRecordSet`, `control.logRowCount`, `control.logPackageEnd`, `control.logError`,
`control.getConfiguration` (`FxMandatoryPairs` for the FX missing-pair count), `params.getJobParams`, `naming.table`.
`control.packageRun` is not used because `packageExecutionId` is needed for `logRowCount` / `logRejectedRecordSet`.

## 6. Validate / deploy

```bash
cd databricks/01_oracle_extract
python -m py_compile notebooks/*.py src/oracle_extract/*.py validation/*.py
python -m pytest tests -q                    # local PySpark + Delta (DELTA_JARS=<local jars> when Maven is unreachable)
databricks bundle validate -t dev            # and -t prod
databricks bundle deploy -t dev              # not run by the migration session
databricks bundle run -t dev wwi_01_oracle_extract --params BatchId=0,ReloadFullHistory=False,source_mode=files
```

Prerequisites at deploy time: session-00 wheel at `${var.common_wheel_path}`, secret `wwi/oracle-erp-password`,
`etl.*` seeded (`etl.watermark` rows for the 12 incremental objects, `etl.source_system` `ORA_ERP`), and for
`source_mode=files` the `generators/` output landed under `${var.extract_volume_path}`.

## 7. Not migrated / needs decision

| Item | Why / proposal |
|---|---|
| **23 vs 22 packages** | The assignment says 23 `EXT_ORA_*` packages; the generator (`generate_oracle_extracts.py`) has 22 builders, the directory holds 22 `.dtsx`, and `docs/inventories/ssis-packages.csv` lists 22. Nothing else in the repo names a 23rd package. Migrated the 22 that exist; please confirm no package is missing from the legacy checkout. |
| `SourceRowNumber` audit column | Legacy derived a per-buffer row number in the data flow; not deterministic in Spark and not used downstream (`stg.*` loads key on business columns). Dropped. |
| `LoadedAtUtc` | Renamed to `ExtractedAtUtc` per the target metadata contract; silver sessions reading `raw.*` must use the new name. |
| `RejectThresholdPercent` (Project.params) | Legacy failed the package when rejects exceeded the threshold via `etl.usp_LogReject`. Bronze logs rejects with `logRejectedRecordSet`; threshold enforcement belongs in the shared `control.assertRowCountTolerance` gate (session 00 / 15). |
| OLE DB destination error outputs (`err.RejectedConstraintViolation`) | Delta has no row-level constraint redirect; the spec keeps `legacyErrTable` for traceability. Constraint checking (NOT NULL on keys) is left to the silver `stg.*` loads. |
| `ReloadFullHistory` on incremental packages | Legacy re-extracted from the epoch **without** truncating, so `raw.*` accumulated duplicates that the staging `MERGE` de-duplicated. On Delta the package's own rows are replaced (overwrite for a sole writer; `DELETE` of the package's slice when the raw table is shared — `RecordKind IS NULL`, `RecordKind='CODEXREF'`, `PAYMENT_APPLY_ID IS NULL/NOT NULL`, ...). If duplicates must be preserved 1:1, switch `deleteOwnRows` to a no-op. |
| `RecordKind` column on shared raw tables | `10_raw_tables_oracle.sql` has no `RecordKind` column although the legacy pre-load SQL deletes on it; bronze adds it (nullable string) via schema merge. |
| Master-job orchestration (`Master_Daily_ETL` phase `Extract Oracle`, 4 parallel streams, `ExtractAttempt` retry loop, `RestartFromStep`) | Owned by session 00's master job; this bundle exposes one job (`wwi_01_oracle_extract`) whose tasks can be referenced by `run_job_task` or notebook path. Task parallelism is left to the job scheduler (no stream assignment). |
| `etl.Configuration` lookups (`OracleFetchArraySize`, `FxMandatoryPairs`) | Fetch size is a task parameter; `FxMandatoryPairs` is read through `control.getConfiguration` and defaults to none when the key is absent. |
| Static repo check `validation/static/run_all_checks.py` | Its `forbidden-content` rule flags the words *Databricks*, *Unity Catalog* and `dbutils` anywhere under the repo, so it fails on every `databricks/*` bundle by design (all other migration branches too). The legacy estate still passes (`--path ssis/sqlserver/oracle/...` = 0 failures). Decision for the parent session: scope the rule to the legacy paths or add `databricks/` + `docs/migration/` to its exclusions (the check lives under `validation/`, which migration sessions may not edit). |
| Oracle JDBC driver version | `ojdbc11:23.5.0.24.07` is a bundle variable; pin to the version approved for the workspace. |
| Databricks CLI auth | Bundle validated offline (`databricks bundle validate -t dev/-t prod` succeed without a workspace); no deploy/run performed. |
