# 02_sqlserver_extract -> `wwi_02_sqlserver_extract` package mapping

Source project: `ssis/02_sqlserver_extract/` (`WWI_Extract_SqlServer`, generator `generate_sqlserver_extracts.py`, 22 `EXT_SQL_*.dtsx`).
Target bundle: `databricks/02_sqlserver_extract/` (bundle `wwi_02_sqlserver_extract`, one job `wwi_02_sqlserver_extract` with 22 tasks keyed by package name).
Layer: **bronze** (`${catalog}.bronze.raw_sql_*`), mirroring `sqlserver/staging/tables/11_raw_tables_sqlserver.sql`.

All 22 packages are migrated. Package count: **22** (16 incremental, 6 full reload).

## How a package runs

Every `.dtsx` produced by the generator has the same control flow, so every notebook is a thin
binding over a shared runner (`src/wwi_sqlserver_extract/extract.py`); the package-specific part
(source SQL, column contract, derived columns, splits, watermark object, reject disposition,
delete-detection SQL) is data in `src/wwi_sqlserver_extract/specs.py`, transcribed from the
generator (the `?` placeholders keep their legacy position; `PackageSpec.sqlParams` says what binds to each).

| SSIS construct (generator / .dtsx) | Spark / `dbx_etl_common` equivalent |
|---|---|
| Project connection `WWI_Source_DB` (OLE DB, `SqlServerHost/Port/OltpDb`, `PasswordSecretName`) | `jdbc.SqlServerConnection.fromWidgets(dbutils)`: Spark JDBC `com.microsoft.sqlserver.jdbc.SQLServerDriver`, host/port/db/trust flag from task `base_parameters` (bundle variables), login + password from `dbutils.secrets.get(scope, key)` |
| Project connection `WWI_Staging_DB` (raw/etl destination) | Unity Catalog `${catalog}.bronze.*` Delta tables via `naming.table(catalog, "bronze", ...)`; control calls go to `etl.*` through `dbx_etl_common.control` |
| Project.params `SqlServerHost`, `SqlServerPort`, `SqlServerOltpDb`, `SqlServerTrustServerCertificate`, `SourceQueryTimeoutSeconds`, `DefaultBatchSize`, `SqlServerSecretScope`, `SqlServerUserSecretKey`, `SqlServerPasswordSecretKey` | notebook widgets with the same names (`jdbc.PARAMETER_DEFAULTS`), fed by `notebook_task.base_parameters` from bundle variables `sqlserver_*` |
| Package parameters `BatchId`, `BusinessDate`, `ReloadFullHistory`, `EnvironmentCode`, `RestartFromStep` | job parameters (all string) read through `params.getJobParams(dbutils)` |
| `EXEC etl.usp_LogPackageStart` | `control.logPackageStart(spark, catalog, batchId, packageName, projectName="WWI_Extract_SqlServer", stepName="Extract SQL Server")` |
| `EXEC etl.usp_GetWatermark @SourceSystemCode, @ObjectName, @ReloadFullHistory` | `control.getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory)` (LookbackMinutes, IsLocked and epoch-on-reload live in the shared package) |
| `SELECT MAX(<key>) FROM <source>` (bounded numeric-key packages) | `spec.maxKeySql` executed over JDBC; result becomes `watermarkTo` |
| Data Flow: OLE DB Source with parameterised SQL | `jdbc.renderSql(spec.sourceSql, spec.sqlParams, bindings)` + `spark.read.format("jdbc").option("query", ...)`; top-level `ORDER BY` of the legacy SQL is dropped (Delta has no row order), nested ORDER BY (e.g. `OUTER APPLY ... TOP 1`) is kept |
| Data Flow: Derived Column (`SSIS expression`) | `transforms.derive<Package>()` column expressions (see per-package derived columns below), e.g. `DATEDIFF("hh", ...)` -> `ssisDateDiffHours`, `x == 0 ? 0 : a / b` -> `safeRatio` |
| Data Flow: Conditional Split (counter only) | `transforms.SPLIT_CONDITIONS` -> `df.filter(...).count()` logged as a package metric (`result.metrics[splitName]`) |
| Data Flow: Row Count -> `User::RowsExtracted` | `df.count()` -> `rowsRead`; `control.logRowCount(...)` per target object |
| Data Flow: OLE DB Destination `raw.*`, `ErrorRowDisposition = RedirectRow` -> `err.Rejected*` | `extract.splitRejects()`: rows with a null business key go to `control.logRejectedRecordSet(spark, catalog, objectName, rejectedDf, ..., rejectStage="Extract", rejectReasonCode="NULL_KEY")`; `FailComponent` raises; `IgnoreFailure` keeps the rows |
| OLE DB Destination fast load, `DefaultBatchSize` | Delta MERGE / append / `replaceWhere` (`spec.batchSize` -> JDBC `fetchsize`) |
| Second Data Flow "Extract deleted keys" (`CHANGETABLE(CHANGES ...)` where `SYS_CHANGE_OPERATION='D'`) | `extract.extractDeleteMarkers()`: same SQL over JDBC, appended as `DeleteFlag='Y'` marker rows with audit columns (raw layer keeps history; downstream STG applies the delete) |
| `EXEC etl.usp_SetWatermark` (only on success path) | `control.setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId)` after the Delta write succeeds |
| `EXEC etl.usp_LogRowCount` | `control.logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount, targetRowCount, insertRowCount, updateRowCount, deleteRowCount, rejectRowCount)` |
| `EXEC etl.usp_LogPackageEnd` (success) | `control.logPackageEnd(..., status="Succeeded", rowsRead, rowsInserted, rowsUpdated, rowsDeleted, rowsRejected, watermarkFrom, watermarkTo)` |
| OnError event handler: `etl.usp_LogError` + `etl.usp_LogPackageEnd 'Failed'` | `except Exception: control.logError(...); control.logPackageEnd(..., status="Failed"); raise` |
| Precedence constraints (`Success` between the SQL tasks and the data flows) | sequential Python; failure anywhere short-circuits before `setWatermark` |
| Ingestion metadata (`BatchId`, `PackageExecutionId`, `LoadedAtUtc`, `SourceSystemCode`, `SourceRowNumber`) | `BatchId`, `PackageExecutionId`, `ExtractedAtUtc`, `SourceSystemCode`, `WatermarkFrom`, `WatermarkTo` (`extract.AUDIT_COLUMNS`). `SourceRowNumber` is not carried (no stable row order in Spark; see decisions) |

Idempotency per pattern: numeric-key and timestamp packages MERGE on the business key (plus
`RecordKind` for shared tables) so re-running the same window updates rather than duplicates;
date-window packages `replaceWhere` the window (the legacy package deleted the window first);
full reloads `replaceWhere RecordKind = '<kind>'` (the legacy package deleted that RecordKind
slice first). `ReloadFullHistory=True` makes `getWatermark` return the epoch / `0` lower bound and
the run overwrites the table for that package's `RecordKind`.

## Packages

| Package | Notebook / task | Source objects (OLTP) | Legacy target -> Delta target | Load pattern / idempotency | Watermark (`SourceSystemCode` / `ObjectName`) | Derived columns | Conditional splits (counters) | Delete detection | Null-key disposition |
|---|---|---|---|---|---|---|---|---|---|
| `EXT_SQL_Orders` | `notebooks/EXT_SQL_Orders.py` / task `EXT_SQL_Orders` | `Sales.Orders`, `Sales.SalesChannels`, `Sales.SalesTerritories` | `raw.SqlOrder` -> `${catalog}.bronze.raw_sql_order` | Incremental, numeric key: `OrderID > watermarkFrom AND OrderID <= watermarkTo` (watermarkTo = source MAX at run start); Delta MERGE on key | `WWI_OLTP` / `Sales.Orders` | `BackorderFlag`, `PickCycleHours`, `DeleteFlag` | - | change tracking on `Sales.Orders` -> `DeleteFlag='Y'` markers | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_OrderLines` | `notebooks/EXT_SQL_OrderLines.py` / task `EXT_SQL_OrderLines` | `Sales.OrderDiscounts`, `Sales.vw_OrderLineExtract` | `raw.SqlOrderLine` -> `${catalog}.bronze.raw_sql_order_line` | Incremental, numeric key: `OrderLineID > watermarkFrom AND OrderLineID <= watermarkTo` (watermarkTo = source MAX at run start); Delta MERGE on key | `WWI_OLTP` / `Sales.OrderLines` | `NetLineAmount`, `ShortPickFlag` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_Invoices` | `notebooks/EXT_SQL_Invoices.py` / task `EXT_SQL_Invoices` | `Sales.vw_InvoiceExtract` | `raw.SqlInvoice` -> `${catalog}.bronze.raw_sql_invoice` | Incremental, numeric key: `InvoiceID > watermarkFrom AND InvoiceID <= watermarkTo` (watermarkTo = source MAX at run start); Delta MERGE on key | `WWI_OLTP` / `Sales.Invoices` | `EffectiveTaxRate`, `SignedTotalIncludingTax` | `Split Credit Notes` | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `None`) |
| `EXT_SQL_InvoiceLines` | `notebooks/EXT_SQL_InvoiceLines.py` / task `EXT_SQL_InvoiceLines` | `Sales.InvoiceLines`, `Warehouse.StockItems` | `raw.SqlInvoiceLine` -> `${catalog}.bronze.raw_sql_invoice_line` | Incremental, numeric key: `InvoiceLineID > watermarkFrom AND InvoiceLineID <= watermarkTo` (watermarkTo = source MAX at run start); Delta MERGE on key | `WWI_OLTP` / `Sales.InvoiceLines` | `GrossMarginPct`, `NegativeMarginFlag` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_Promotions` | `notebooks/EXT_SQL_Promotions.py` / task `EXT_SQL_Promotions` | `Sales.PromotionLines`, `Sales.PromotionRedemptions`, `Sales.Promotions` | `raw.SqlOrder` (`RecordKind='PROMOTION'`) -> `${catalog}.bronze.raw_sql_promotion` | Full reload: `replaceWhere RecordKind = 'PROMOTION'` then insert (legacy: DELETE ... WHERE RecordKind, then insert); no watermark | - | `RecordKind`, `RedemptionRatePct` | - | - | FailComponent -> null key raises, package Failed |
| `EXT_SQL_SalesTerritories` | `notebooks/EXT_SQL_SalesTerritories.py` / task `EXT_SQL_SalesTerritories` | `Sales.CommissionPlans`, `Sales.SalesQuotas`, `Sales.SalesTerritories` | `raw.SqlOrder` (`RecordKind='TERRITORY'`) -> `${catalog}.bronze.raw_sql_sales_territory` | Full reload: `replaceWhere RecordKind = 'TERRITORY'` then insert (legacy: DELETE ... WHERE RecordKind, then insert); no watermark | - | `RecordKind`, `FiscalCalendarCode` | - | - | FailComponent -> null key raises, package Failed |
| `EXT_SQL_CustomerSegments` | `notebooks/EXT_SQL_CustomerSegments.py` / task `EXT_SQL_CustomerSegments` | `Sales.CustomerSegments`, `Sales.vw_CustomerSegmentCurrent` | `raw.SqlOrder` (`RecordKind='SEGMENT'`) -> `${catalog}.bronze.raw_sql_customer_segment` | Full reload: `replaceWhere RecordKind = 'SEGMENT'` then insert (legacy: DELETE ... WHERE RecordKind, then insert); no watermark | - | `RecordKind`, `MarketableFlag` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedCustomer`) |
| `EXT_SQL_StockItems` | `notebooks/EXT_SQL_StockItems.py` / task `EXT_SQL_StockItems` | `Warehouse.ReplenishmentRules`, `Warehouse.StockItemHoldings`, `Warehouse.StockItems` | `raw.SqlStockItem` -> `${catalog}.bronze.raw_sql_stock_item` | Incremental, timestamp watermark with LookbackMinutes on `ValidFrom` (temporal `ValidFrom`); Delta MERGE on key | `WWI_OLTP` / `Warehouse.StockItems` | `BelowReorderFlag`, `HandlingClass`, `DeleteFlag` | - | change tracking on `Warehouse.StockItems` -> `DeleteFlag='Y'` markers | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedProduct`) |
| `EXT_SQL_StockMovements` | `notebooks/EXT_SQL_StockMovements.py` / task `EXT_SQL_StockMovements` | `Application.TransactionTypes`, `Warehouse.Bins`, `Warehouse.WarehouseSites`, `Warehouse.vw_StockMovementExtract` | `raw.SqlStockMovement` -> `${catalog}.bronze.raw_sql_stock_movement` | Incremental, numeric key: `StockItemTransactionID > watermarkFrom AND StockItemTransactionID <= watermarkTo` (watermarkTo = source MAX at run start); Delta MERGE on key | `WWI_OLTP` / `Warehouse.StockItemTransactions` | `AbsoluteQuantity`, `MovementClass` | `Split Adjustments` | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_StockTransfers` | `notebooks/EXT_SQL_StockTransfers.py` / task `EXT_SQL_StockTransfers` | `Warehouse.StockTransferLines`, `Warehouse.StockTransfers`, `Warehouse.WarehouseSites` | `raw.SqlStockMovement` -> `${catalog}.bronze.raw_sql_stock_transfer` | Incremental, open numeric key: ` > watermarkFrom`; watermarkTo = max key extracted; Delta MERGE on key | `WWI_OLTP` / `Warehouse.StockTransferLines` | `InTransitQuantity`, `StaleTransitFlag`, `MovementClass` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_Shipments` | `notebooks/EXT_SQL_Shipments.py` / task `EXT_SQL_Shipments` | `Shipping.Carriers`, `Shipping.CustomsDeclarations`, `Shipping.DeliveryRoutes`, `Shipping.FreightRates`, `Shipping.vw_ShipmentExtract` | `raw.SqlShipment` -> `${catalog}.bronze.raw_sql_shipment` | Incremental, numeric key: `ShipmentHeaderID > watermarkFrom AND ShipmentHeaderID <= watermarkTo` (watermarkTo = source MAX at run start); Delta MERGE on key | `WWI_OLTP` / `Shipping.ShipmentHeaders` | `TransitHours`, `CrossBorderFlag` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_ShipmentLines` | `notebooks/EXT_SQL_ShipmentLines.py` / task `EXT_SQL_ShipmentLines` | `Shipping.PackagingTypes`, `Shipping.ShipmentEvents`, `Shipping.ShipmentLines` | `raw.SqlShipmentLine` -> `${catalog}.bronze.raw_sql_shipment_line` | Incremental, numeric key: `ShipmentLineID > watermarkFrom AND ShipmentLineID <= watermarkTo` (watermarkTo = source MAX at run start); Delta MERGE on key | `WWI_OLTP` / `Shipping.ShipmentLines` | `AwaitingScanFlag` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_Returns` | `notebooks/EXT_SQL_Returns.py` / task `EXT_SQL_Returns` | `Returns.ReturnAuthorizations`, `Returns.ReturnInspections`, `Returns.ReturnReasons`, `Returns.vw_ReturnExtract` | `raw.SqlReturnLine` -> `${catalog}.bronze.raw_sql_return_line` | Incremental, open numeric key: ` > watermarkFrom`; watermarkTo = max key extracted; Delta MERGE on key | `WWI_OLTP` / `Returns.ReturnLines` | `DaysToInspect`, `RestockableFlag` | `Route Pending Inspections` | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `None`) |
| `EXT_SQL_CreditNotes` | `notebooks/EXT_SQL_CreditNotes.py` / task `EXT_SQL_CreditNotes` | `Returns.CreditNotes`, `Returns.vw_CreditNoteExtract` | `raw.SqlCreditNote` -> `${catalog}.bronze.raw_sql_credit_note` | Incremental, open numeric key: ` > watermarkFrom`; watermarkTo = max key extracted; Delta MERGE on key | `WWI_OLTP` / `Returns.CreditNoteLines` | `SignedCreditAmount`, `VatReturnRequiredFlag` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_WebSessions` | `notebooks/EXT_SQL_WebSessions.py` / task `EXT_SQL_WebSessions` | `Ecommerce.CartHeaders`, `Ecommerce.WebSessions` | `raw.SqlWebSession` -> `${catalog}.bronze.raw_sql_web_session` | Bounded date window on `SessionStartedWhen` (`>= watermarkFrom AND < watermarkTo`); Delta `replaceWhere` on the window, empty window skipped | `WWI_WEB` / `Ecommerce.WebSessions` | `BounceFlag`, `ConversionFlag` | - | - | IgnoreFailure -> null keys kept |
| `EXT_SQL_LoyaltyLedger` | `notebooks/EXT_SQL_LoyaltyLedger.py` / task `EXT_SQL_LoyaltyLedger` | `Loyalty.LoyaltyMembers`, `Loyalty.LoyaltyPointsLedger`, `Loyalty.LoyaltyPrograms` | `raw.SqlLoyaltyLedger` -> `${catalog}.bronze.raw_sql_loyalty_ledger` | Incremental, open numeric key: ` > watermarkFrom`; watermarkTo = max key extracted; Delta MERGE on key | `WWI_OLTP` / `Loyalty.LoyaltyPointsLedger` | - | - | - | FailComponent -> null key raises, package Failed |
| `EXT_SQL_CustomerTransactions` | `notebooks/EXT_SQL_CustomerTransactions.py` / task `EXT_SQL_CustomerTransactions` | `Application.PaymentMethods`, `Application.TransactionTypes`, `Sales.CustomerTransactions` | `raw.SqlInvoice` (`RecordKind='ARTRAN'`) -> `${catalog}.bronze.raw_sql_customer_transaction` | Incremental, numeric key: `CustomerTransactionID > watermarkFrom AND CustomerTransactionID <= watermarkTo` (watermarkTo = source MAX at run start); Delta MERGE on key | `WWI_OLTP` / `Sales.CustomerTransactions` | `RecordKind`, `SettledFlag`, `DaysOutstanding` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_SupplierTransactions` | `notebooks/EXT_SQL_SupplierTransactions.py` / task `EXT_SQL_SupplierTransactions` | `Application.TransactionTypes`, `Purchasing.SupplierTransactions`, `Purchasing.Suppliers` | `raw.SqlInvoice` (`RecordKind='APTRAN'`) -> `${catalog}.bronze.raw_sql_supplier_transaction` | Incremental, open numeric key: ` > watermarkFrom`; watermarkTo = max key extracted; Delta MERGE on key | `WWI_OLTP` / `Purchasing.SupplierTransactions` | `RecordKind`, `DuplicateCheckKey` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_People` | `notebooks/EXT_SQL_People.py` / task `EXT_SQL_People` | `Application.People` | `raw.SqlOrder` (`RecordKind='PERSON'`) -> `${catalog}.bronze.raw_sql_person` | Full reload: `replaceWhere RecordKind = 'PERSON'` then insert (legacy: DELETE ... WHERE RecordKind, then insert); no watermark | - | `RecordKind`, `RoleCode` | - | - | FailComponent -> null key raises, package Failed |
| `EXT_SQL_Cities` | `notebooks/EXT_SQL_Cities.py` / task `EXT_SQL_Cities` | `Application.Cities`, `Application.Countries`, `Application.StateProvinces` | `raw.OracleGeography` (`RecordKind='OLTPCITY'`) -> `${catalog}.bronze.raw_sql_city` | Full reload: `replaceWhere RecordKind = 'OLTPCITY'` then insert (legacy: DELETE ... WHERE RecordKind, then insert); no watermark | - | `RecordKind`, `RegionCode`, `PostalFormatCode` | - | - | RedirectRow -> `control.logRejectedRecordSet` (legacy `err.RejectedConstraintViolation`) |
| `EXT_SQL_PaymentMethods` | `notebooks/EXT_SQL_PaymentMethods.py` / task `EXT_SQL_PaymentMethods` | `Application.PaymentMethods` | `raw.SqlInvoice` (`RecordKind='PAYMETHOD'`) -> `${catalog}.bronze.raw_sql_payment_method` | Full reload: `replaceWhere RecordKind = 'PAYMETHOD'` then insert (legacy: DELETE ... WHERE RecordKind, then insert); no watermark | - | `RecordKind`, `ImmediateSettlementFlag` | - | - | FailComponent -> null key raises, package Failed |
| `EXT_SQL_TransactionTypes` | `notebooks/EXT_SQL_TransactionTypes.py` / task `EXT_SQL_TransactionTypes` | `Application.TransactionTypes` | `raw.SqlInvoice` (`RecordKind='TRANTYPE'`) -> `${catalog}.bronze.raw_sql_transaction_type` | Full reload: `replaceWhere RecordKind = 'TRANTYPE'` then insert (legacy: DELETE ... WHERE RecordKind, then insert); no watermark | - | `RecordKind` | - | - | FailComponent -> null key raises, package Failed |

Every package additionally lands `SourceSystemCode`, `ExtractedAtUtc`, `PackageExecutionId`, `BatchId`, `WatermarkFrom`, `WatermarkTo`.

Notable package-specific semantics preserved from the generator:

* `EXT_SQL_Orders` / `EXT_SQL_StockItems` run the second "deleted keys" data flow (change tracking) and land `DeleteFlag='Y'` markers.
* `EXT_SQL_StockItems` is the only timestamp package; the source predicate is on the temporal `ValidFrom` column and `getWatermark` supplies the lookback for late edits.
* `EXT_SQL_LoyaltyLedger` binds `expiryLookbackDays` (package variable, default 30) as the second `?`.
* `EXT_SQL_WebSessions` (`SourceSystemCode = WWI_WEB`) is the only date-window extract; EU sessions without analytics consent are landed without fingerprint/referrer exactly as the source SQL does; a window where `watermarkFrom >= watermarkTo` is skipped (legacy behaviour: 0 rows, no error).
* `EXT_SQL_CustomerSegments` MarketableFlag: EU rows are marketable only on `OPTIN`, the rest of world only not on `OPTOUT`.
* `EXT_SQL_Invoices` (`EffectiveTaxRate`, `SignedTotalIncludingTax`) and `EXT_SQL_InvoiceLines` (`GrossMarginPct`, `NegativeMarginFlag`) keep the SSIS divide-by-zero guard (`0` when the denominator is 0).
* `EXT_SQL_TransactionTypes` logs an extra row count for `Application.TransactionTypes` like the legacy package.

## Job, parameters and dependencies

* `resources/wwi_02_sqlserver_extract.job.yml`: job `wwi_02_sqlserver_extract`, 22 notebook tasks, task_key = package name, no intra-job `depends_on`:
  `docs/inventories/package-dependencies.csv` has no `EXT_SQL_* -> EXT_SQL_*` edge (all consumers are `STG_*`
  packages in `03_staging_load`/`04_staging_transform`) and `ssis/orchestration-plan.json` runs the extract phase as
  independent `ExtractAttempt` loops. The retry loop is replaced by task `max_retries: 2` / 5-minute interval;
  `max_concurrent_runs: 1` keeps one batch at a time like the master.
* Job parameters (string): `BatchId="0"`, `BusinessDate={{job.start_time.[iso_date]}}` (variable `business_date`),
  `ReloadFullHistory="False"`, `EnvironmentCode` (`DEV`/`PROD` per target), `RestartFromStep=""`, `catalog=${var.catalog}` (`wwi_${bundle.target}`).
* `RestartFromStep` is accepted and passed through; skipping steps is a master-job (session 00) concern because all 22 extracts belong to one step (`Extract SQL Server`).
* Compute: serverless (`environments.default`, environment_version 2) with the shared wheel as its only dependency. JDBC to
  SQL Server from serverless needs egress (NCC / private link) configured by the platform team; on classic compute the
  Microsoft SQL Server JDBC driver is part of the runtime.

### Secrets and connection settings

| Setting | Where | Default |
|---|---|---|
| `SqlServerHost`, `SqlServerPort`, `SqlServerOltpDb`, `SqlServerTrustServerCertificate` | bundle variables `sqlserver_*` -> `base_parameters` -> widgets | `sqlserver.internal.example`, `1433`, `WideWorldImporters`, `false` |
| SQL login | secret scope `${var.sqlserver_secret_scope}` (`wwi`), key `${var.sqlserver_user_secret_key}` (`sqlserver-oltp-user`) | read with `dbutils.secrets.get` |
| SQL password | same scope, key `${var.sqlserver_password_secret_key}` (`sqlserver-oltp-password`) | read with `dbutils.secrets.get` |

Nothing secret is in the repo; override the variables per target (`databricks bundle validate -t prod --var="sqlserver_host=..."`)
or in the `targets.<name>.variables` block.

### `dbx_etl_common` consumption

The job environment installs the wheel from `${var.dbx_etl_common_wheel}` (default `/Workspace/Shared/wwi/dbx_etl_common/dbx_etl_common-0.1.0-py3-none-any.whl`,
the path where session 00's `databricks/common` bundle is expected to publish its artifact; override it if that bundle
publishes elsewhere, or point it at `../common/dist/dbx_etl_common-*.whl` once the wheel is built in this checkout).
Notebooks import exactly `from dbx_etl_common import control, naming, params` and add `../src` to `sys.path` for
`wwi_sqlserver_extract`. Nothing from `databricks/common` is copied; the fake under `tests/fakes/dbx_etl_common/` is
test-only and implements just the functions the runner calls.

## Reconciliation (`validation/RECON_SqlServerExtract.py`)

Spark port of `validation/runtime/02_row_count_reconciliation.sql`:

* per target: total row count, row count for `BatchId`, order-independent `bit_xor(xxhash64(<business columns>))` hash (`reconcile.computeTargetMetrics`);
* baseline from `BaselineTable` (Delta: `ObjectName, SourceRowCount, SourceRowHash`) or `BaselineJson`; legacy object names (`raw.SqlOrder`) or Delta names are both accepted;
* variance %, tolerance from `etl.configuration` `RowCountVariancePercentTolerance` (fallback 0.5 %), statuses `Matched / WithinTolerance / OutsideTolerance / HashMismatch / NoBaseline / MissingTarget`;
* one `control.logRowCount` row per object, lifecycle via `logPackageStart/End` (`SucceededWithWarnings` when anything is outside tolerance);
* the three legacy result sets ported to Spark SQL: logged hops per package (`etl.row_count_log` x `etl.package_execution`), packages without any row-count row ("no audit"), rejects outstanding past SLA;
* `control.assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=FailOnVariance)`.

It is not a task of the extract job (the job has exactly the 22 package tasks); session 00's master job or an operator runs it after the extract phase with the same `BatchId`.

## Tests (`tests/`, local PySpark 3.5 + delta-spark 3.2)

`test_specs.py` (22 specs, placeholders, targets, watermark metadata), `test_transforms.py` (every derived column /
split: tax rate, margins, consent suppression, flags, pick-cycle hours, regions/postal formats, ...), `test_jdbc.py`
(positional `?` binding, secret-scope wiring, driver), `test_extract.py` (end-to-end on local Delta: bounded/open
numeric keys, timestamp lookback, ReloadFullHistory, date-window replace + empty window, shared-table full reload,
rejects, failed-package lifecycle, split counters, delete markers). Run:

```bash
pip install pyspark==3.5.1 delta-spark==3.2.0 pytest
python3 -m pytest databricks/02_sqlserver_extract/tests -q
# offline (no Maven access): WWI_DELTA_JARS=<dir with delta-spark_2.12-3.2.0, delta-storage-3.2.0, antlr4-runtime-4.9.3 jars>
```

## Validate / deploy

```bash
python3 -m py_compile databricks/02_sqlserver_extract/notebooks/*.py databricks/02_sqlserver_extract/validation/*.py
cd databricks/02_sqlserver_extract && databricks bundle validate -t dev
databricks bundle deploy -t dev            # not run from the migration session
databricks bundle run wwi_02_sqlserver_extract -t dev --params BatchId=<id>,BusinessDate=YYYY-MM-DD
```

## Not migrated / needs decision

1. **Shared legacy raw tables were split per package.** Ten packages write a `RecordKind` slice of another
   object's raw table in the legacy estate (`EXT_SQL_Promotions`, `_SalesTerritories`, `_CustomerSegments`, `_People`
   -> `raw.SqlOrder`; `_CustomerTransactions`, `_SupplierTransactions`, `_PaymentMethods`, `_TransactionTypes` ->
   `raw.SqlInvoice`; `_StockTransfers` -> `raw.SqlStockMovement`; `_Cities` -> `raw.OracleGeography`). The DDL in
   `11_raw_tables_sqlserver.sql` has neither a `RecordKind` column nor those packages' columns, so the legacy inserts
   cannot have succeeded as written (the deep checks already flag `sqlserver_unlanded_extracts`). The migration lands each
   package in its own bronze table (`bronze.raw_sql_promotion`, `raw_sql_sales_territory`, `raw_sql_customer_segment`,
   `raw_sql_person`, `raw_sql_customer_transaction`, `raw_sql_supplier_transaction`, `raw_sql_payment_method`,
   `raw_sql_transaction_type`, `raw_sql_stock_transfer`, `raw_sql_city`) and keeps the `RecordKind` column, so the
   staging sessions can either read the dedicated tables or a `UNION ALL` view named after the legacy table. The write
   path already scopes full reloads / merges by `RecordKind`, so consolidating back into `bronze.raw_sql_order` etc. is a
   one-line `targetTable` change per spec if the parent prefers the legacy layout. **Decision needed by session 00 / 03.**
2. **Raw column types.** Legacy `raw.*` columns are all `NVARCHAR`; bronze keeps the JDBC-inferred source types
   (ints, decimals, datetime2) and casts only where the generator's derived column demands it. STG casts become no-ops
   rather than failures. Flag if a string-typed bronze is required for parity.
3. **`SourceRowNumber` / `LoadedAtUtc`** are not populated (no deterministic row order in Spark); `ExtractedAtUtc`
   replaces `LoadedAtUtc`. Reconciliation hashes are order independent so nothing depends on it.
4. **Delete markers instead of physical deletes.** Legacy packages inserted deleted keys into the same raw table with
   `DeleteFlag='Y'`; this is preserved 1:1. Applying the delete to silver stays with the STG packages.
5. **Repo static check (`validation/static/run_all_checks.py`).** Its `forbidden-content` rule fails any file mentioning
   "databricks"/"dbutils"/"Unity Catalog" outside the exempt `wwi-*` folders, so it cannot pass unscoped for any
   `databricks/*` deliverable (`# Databricks notebook source` is mandatory). It passes scoped to the legacy estate
   (`--path ssis --path sqlserver --path oracle --path docs/inventories --path config --path deployment --path tools --path validation`)
   and `run_deep_checks.py` passes (0 errors). Adding `databricks/` and `docs/migration/` to the checker's exemption list
   is a `validation/` change this session may not make. **Decision needed by the parent.**
6. **Lakeflow Connect (SQL Server connector) as the long-term replacement.** The 22 packages are plain
   change-tracking / watermark extracts of OLTP tables, which is exactly the managed connector's use case: it uses SQL
   Server change tracking / CDC, lands to bronze streaming tables, handles deletes natively and removes the JDBC
   egress, driver and watermark bookkeeping. It does not cover: the derived columns / consent suppression / split counters
   inside the extract (they would move to silver), the `etl.*` lifecycle and watermark rows that downstream reconciliation
   queries depend on, views as sources (`Sales.vw_*Extract`, `Sales.vw_CustomerSegmentCurrent`), or the bounded
   `WWI_WEB` date-window semantics. Recommendation: keep the JDBC notebooks as the cut-over migration; pilot the
   connector on `Sales.Orders`/`Sales.OrderLines` (largest volumes, change tracking already enabled) once the gateway
   can reach the SQL Server host, and retire per-table notebooks as parity is proven.
7. **Serverless egress to SQL Server** (NCC / firewall) and the **Databricks secret scope `wwi`** with the two keys must be
   provisioned by the platform team before the first run; nothing in this PR can be executed until then.
8. `RestartFromStep` semantics are delegated to the master job (see above).
