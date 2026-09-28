# 12_inventory (WWI_Inventory) -> `databricks/12_inventory` package mapping

Source: `ssis/12_inventory/` (generator `build_inventory_packages.py`, 5 `INV_*` packages).
Target: bundle `wwi_12_inventory` (`databricks/12_inventory/databricks.yml`), job `wwi_12_inventory`
(`resources/wwi_12_inventory.job.yml`), one Python notebook per package under `notebooks/`,
shared helpers in `src/inv_common/`, reconciliation under `validation/`, pytest suite under `tests/`.

All code consumes session 00's `dbx_etl_common` (`control`, `params`, `naming`) and never
re-implements it; `tests/fakes/dbx_etl_common` is a test-only stand-in that is not shipped.

## 1. Packages -> notebooks / tasks

| Legacy package | Notebook | Task key | Legacy master(s) | Intraday-safe |
|---|---|---|---|---|
| `INV_Load_DailySnapshot` | `notebooks/INV_Load_DailySnapshot.py` | `INV_Load_DailySnapshot` | `Master_Daily_ETL` (phase Daily Inventory Mart, seq 91, group Mart) | **No** - one snapshot per business date; a rerun overwrites the date partition, an intraday run would overwrite the closing position with an intraday one |
| `INV_Load_StockTransfer` | `notebooks/INV_Load_StockTransfer.py` | `INV_Load_StockTransfer` | `Master_Daily_ETL` seq 91; `Master_Intraday_Inventory` phase Inventory Marts (seq 40) | Yes - work table rebuilt per batch, `Fact.Movement` MERGE on natural key |
| `INV_Load_CycleCountVariance` | `notebooks/INV_Load_CycleCountVariance.py` | `INV_Load_CycleCountVariance` | `Master_Daily_ETL` seq 91; `Master_Intraday_Inventory` seq 40 | Yes - same as above; recount queue is keyed by `CycleCountId` |
| `INV_Reconcile_OnHand` | `notebooks/INV_Reconcile_OnHand.py` | `INV_Reconcile_OnHand` | `Master_Daily_ETL` seq 91; `Master_Intraday_Inventory` seq 40 | Yes - results replaced per `BatchId`/`ObjectName`; `TimingWindowMinutes` exists precisely for the intraday case |
| `INV_Load_Replenishment` | `notebooks/INV_Load_Replenishment.py` | `INV_Load_Replenishment` | `Master_Daily_ETL` seq 91; `Master_Intraday_Inventory` phase Replenishment (seq 50, gated) | Yes, gated - only when `PickingWindowOpen && OnHandVariance <= OnHandVarianceTolerance` |

Extra tasks in the job (not legacy packages): `Read_On_Hand_Variance` (`validation/Read_On_Hand_Variance.py`, the
orchestration-plan control node), `Replenishment_Gate` (condition task), `Validate_Inventory_Reconciliation`
(`validation/INV_Reconcile_Targets.py`).

### Job wiring

`docs/inventories/package-dependencies.csv` has no `INV_* -> INV_*` edge and the `.dtsx` files have only
in-package precedence constraints, so inside the job the four mart packages are siblings (the daily plan runs
them with 2 streams). External upstream data dependencies (owned by other projects / sessions):

| Upstream package (project) | Feeds | Consumed by |
|---|---|---|
| `STG_Work_InventoryPosition` (WWI_Staging) | `work.InventoryPositionDaily` | all five |
| `STG_Load_StockItem` (WWI_Staging) | `stg.StockItem` | Replenishment, DailySnapshot, CycleCount, StockTransfer |
| `STG_Load_StockMovement` (WWI_Staging) | `stg.StockMovement` | StockTransfer, CycleCountVariance, Reconcile_OnHand |
| `FACT_Load_StockHolding` (WWI_Warehouse) | `Fact.Stock Holding` | `INV_Reconcile_OnHand` |
| `DIM_Load_StockItem` / `DIM_Load_WarehouseSite` | `Dimension.Stock Item`, `Dimension.Warehouse Site` | key lookups |

Session 00's master jobs must run `wwi_12_inventory` after those; the job itself does not model them.

The one intra-project ordering that exists is the `Master_Intraday_Inventory` gate
(`Inventory Marts --Completion--> Read On Hand Variance --[PickingWindowOpen && OnHandVariance <= OnHandVarianceTolerance]--> Replenishment`).
It is reproduced with `Read_On_Hand_Variance` (`run_if: ALL_DONE` = Completion edge) -> `Replenishment_Gate`
(condition task on task value `replenishmentAllowed`) -> `INV_Load_Replenishment`. The other edge
(`OnHandVariance > tolerance -> On Hand Variance Escalation`, packages `ERR_Reconcile_RowCounts` /
`ERR_Notify_Operations`) belongs to project 15; `Read_On_Hand_Variance` publishes `escalateOnHandVariance`
as a task value and writes a `Warning` row through `control.logError` for it.

### `Master_Intraday_Inventory` cadence

* `docs/runbooks/execution.md` and `docs/dependency-maps/etl-dependency-map.md`: **every 20 minutes, 05:00-22:00**.
* `ssis/orchestration-plan.json` description of the same root: **"runs every two hours during warehouse operating hours"**.

The two sources disagree; the SQL Agent schedule (20 min) is the operational truth and the plan text is treated as
stale, but the decision is flagged in section 6. Mapping onto the job:

| Cadence | What runs | Notes |
|---|---|---|
| Nightly (`Master_Daily_ETL`) | all 5 tasks + validation | `BusinessDate` = batch date; `INV_Load_DailySnapshot` is the only place the snapshot is produced |
| Every 20 min 05:00-22:00 (`Master_Intraday_Inventory`) | `INV_Load_StockTransfer`, `INV_Load_CycleCountVariance`, `INV_Reconcile_OnHand`, then `Read_On_Hand_Variance` -> gate -> `INV_Load_Replenishment` | session 00 should invoke the job with a task subset (or the intraday master job should `run_job` only those task keys); `INV_Load_DailySnapshot` and `Validate_Inventory_Reconciliation` must be excluded. `TimingWindowMinutes` (30) is deliberately larger than the 20-minute cadence so movements from the previous tick classify as `Timing`, not `Variance`. |

Why the four are intraday-safe: every write is either a work-table rebuild scoped to the batch, a `MERGE` on a
deterministic natural key (`Fact.Movement`, `Aggregate.Daily Inventory Health`), or a `replaceWhere` on
`BatchId`/`ObjectName` (`etl.reconciliation_result`); reject rows are keyed by business key and reason code.
`INV_Load_DailySnapshot` is not: it `replaceWhere`s the whole `SnapshotDateKey` partition with the position
as of run time, which would destroy the closing snapshot if run intraday.

## 2. Source / target objects -> Delta tables

| Legacy object | Delta table | Layer | Read / written by |
|---|---|---|---|
| `work.InventoryPositionDaily` | `silver.work_inventory_position_daily` | silver | read by all five |
| `work.StockItemDemand` | `silver.work_stock_item_demand` | silver | read by Replenishment |
| `stg.StockItem` | `silver.stg_stock_item` | silver | read |
| `stg.StockMovement` | `silver.stg_stock_movement` | silver | read (TRANSFER rows; recent movements for timing) |
| `stg.CycleCount` | `silver.stg_cycle_count` | silver | read |
| `stg.WarehouseSite` | `silver.stg_warehouse_site` | silver | read |
| `stg.TransferPrice` | `silver.stg_transfer_price` | silver | read |
| `work.CycleCountVariance` | `silver.work_cycle_count_variance` | silver | rebuilt (overwrite) by CycleCountVariance |
| `work.ReplenishmentSuggestion` | `silver.work_replenishment_suggestion` | silver | rebuilt by Replenishment |
| `work.StockTransferMovement` | `silver.work_stock_transfer_movement` | silver | rebuilt by StockTransfer (Despatched split output) |
| `work.StockTransferReceiptOnly` | `silver.work_stock_transfer_receipt_only` | silver | rebuilt by StockTransfer (ReceiptOnly split output) |
| `err.InventorySnapshotReject` | `silver.err_inventory_snapshot_reject` | silver | appended by DailySnapshot (lookup no-match) |
| `Dimension.Stock Item` | `gold.dim_stock_item` | gold | lookup (current rows: `ValidTo > now`) |
| `Dimension.Warehouse Site` | `gold.dim_warehouse_site` | gold | lookup (site key / region) |
| `Fact.Daily Inventory Snapshot` | `gold.fact_daily_inventory_snapshot` | gold | `replaceWhere SnapshotDateKey = :date` |
| `Fact.Movement` | `gold.fact_movement` | gold | MERGE on `NaturalKeyHash` (CycleCount + StockTransfer) |
| `Fact.Stock Holding` | `gold.fact_stock_holding` | gold | read by Reconcile_OnHand |
| `Aggregate.Daily Inventory Health` | `gold.agg_daily_inventory_health` | gold | MERGE on (`SnapshotDate`,`WarehouseSiteCode`,`ProductCategoryKey`) |
| `etl.ReconciliationResult` | `etl.reconciliation_result` | etl | `replaceWhere BatchId = :b AND ObjectName = 'Fact.Stock Holding'` |
| `etl.RowCountAudit` | `etl.row_count_log` | etl | via `control.logRowCount`; read by `Read_On_Hand_Variance` |
| `etl.RejectedRecord` | `etl.rejected_record` | etl | via `control.logRejectedRecordSet` |
| `etl.PackageExecution`, `etl.Batch`, `etl.ErrorLog` | `etl.package_execution`, `etl.batch`, `etl.error_log` | etl | via `dbx_etl_common` only |

Warehouse column names keep the legacy names with spaces removed (`[Quantity On Hand]` -> `QuantityOnHand`).
`runtime.ensureProjectTables` creates the tables this project *writes* if absent (`CREATE TABLE IF NOT EXISTS`);
tables owned by other sessions are never created here.

## 3. SSIS components -> Spark constructs

### INV_Load_DailySnapshot
| SSIS component | Type | Spark construct |
|---|---|---|
| Log Package Start / Success, Log Error, Mark Execution Failed | Execute SQL (etl.usp_*) | `runtime.PackageContext` -> `control.logPackageStart` / `logPackageEnd` / `logError` |
| Resolve Snapshot Date | Execute SQL | `SnapshotBusinessDate` widget; `1900-01-01`/empty -> job `BusinessDate` |
| Delete Existing Snapshot (`@[$Package::DeleteExistingSnapshot]`) | Execute SQL DELETE | folded into `replaceWhere("SnapshotDateKey = DATE'...'")` (atomic delete+insert); flag off -> plain append, as legacy |
| `work InventoryPositionDaily` (SNAPSHOT_SQL) | OLE DB Source | `transforms.buildDailySnapshotSource` (date filter, join to stg.StockItem, `QuantityAvailable`, `OnHandValue`, `AgeBandCode`, `IsExpiredChillerStock`) |
| Lookup Stock Item Key (no-match -> error output) | Lookup | left join to `transforms.currentStockItemKeys(dim)`; null key -> `err.InventorySnapshotReject` (`UNKNOWN_STOCK_ITEM`) |
| Derive Snapshot Measures | Derived Column | `transforms.deriveSnapshotMeasures` (`DaysCoverAtCurrentRate`, `ObsolescenceProvisionAmount`) |
| Fact Daily Inventory Snapshot | OLE DB Destination | `transforms.toFactDailyInventorySnapshot` + `runtime.replaceWhere` |
| Count Expired Chiller Stock | Execute SQL -> `User::ExpiredChillerCount` | count on the written partition (printed) |
| Log Row Counts | Execute SQL (etl.usp_LogRowCount) | `ctx.logPackageRowCounts("Fact.Daily Inventory Snapshot")` |

### INV_Load_CycleCountVariance
| SSIS component | Type | Spark construct |
|---|---|---|
| Truncate work_CycleCountVariance + Build Variance Set | Execute SQL TRUNCATE + INSERT..SELECT | `transforms.buildCycleCountVariance` + `runtime.overwriteTable` |
| Measure Variances | Execute SQL -> `VarianceCount`, `HeldForRecountCount` | `transforms.measureVariances` |
| Post Adjustment Movements (cursor over AUTO rows calling `Integration.usp_PostInventoryAdjustment`) | Execute SQL (cursor) | set-based `transforms.buildAdjustmentMovements` -> `runtime.mergeByKey(Fact.Movement, NaturalKeyHash)`; natural key `CYCLECOUNT|CycleCountId`, `MovementReasonCode='CYCLECOUNT'`, `StockTakeReference=CycleCountId` |
| Queue Recounts (`HeldForRecountCount > 0`) | Execute SQL INSERT etl.RejectedRecord | `control.logRejectedRecordSet(objectName="stg.CycleCount", rejectStage="Fact", rejectReasonCode="COUNT_VARIANCE_HELD", businessKeyColumn="BusinessKey")` |
| Log Row Counts | Execute SQL | `ctx.logPackageRowCounts("Fact.Movement")` |
| Precedence expressions `@[User::VarianceCount] > 0`, `@[User::HeldForRecountCount] > 0` | constraints | Python `if` on the measured counts |

### INV_Load_Replenishment
| SSIS component | Type | Spark construct |
|---|---|---|
| Truncate work_ReplenishmentSuggestion | Execute SQL | `runtime.overwriteTable` |
| `stg StockItem With Position` (REPLENISH_SQL) | OLE DB Source | `transforms.buildReplenishmentSource` (non-discontinued, position inner join, demand left join `COALESCE(..,0)`, `SafetyFactor` APAC 1.5 / EU 1.1 / else 1.25) |
| Derive Reorder Point, Derive Suggested Quantity | Derived Column | `transforms.deriveReplenishment` - `(DT_I4)` casts -> `cast int` (truncation), `/` on DT_I4 -> `div` |
| Split Suggestions (Suggest / NoAction) | Conditional Split | `where SuggestedQuantity > 0` (NoAction branch dropped, as legacy) |
| work ReplenishmentSuggestion | OLE DB Destination | `runtime.overwriteTable` |
| Suppress Chiller Suggestions (`@[$Package::SuppressChillerSuggestions]`) | Execute SQL DELETE | filter `IsChillerStock != true` before the write |
| Publish Inventory Health | Execute SQL MERGE on `[Warehouse Site Code]` | `transforms.aggregateInventoryHealth` + `runtime.mergeByKey(agg_daily_inventory_health, [SnapshotDate, WarehouseSiteCode, ProductCategoryKey])` |
| Count Stockout Risks | Execute SQL -> `StockoutRiskCount` | count on the work table (printed) |
| Log Row Counts | Execute SQL | `ctx.logPackageRowCounts("Aggregate.Daily Inventory Health")` |

### INV_Load_StockTransfer
| SSIS component | Type | Spark construct |
|---|---|---|
| Truncate work_StockTransferMovement | Execute SQL | `runtime.overwriteTable` |
| `stg StockMovement Transfers` (TRANSFER_SQL) | OLE DB Source | `transforms.buildTransferSource` (TRANSFER rows for the batch, from/to site join, transfer price left join, `MovementUnitValue` = transfer price or cost*1.08 cross-region, cost same-region, `QuantityInTransit`) |
| Derive Movement Attributes | Derived Column | `transforms.deriveTransferAttributes` (`IsCrossRegion`, `IssueValue`, `ReceiptValue`, `TransitDays`; `GETDATE()` pinned to one `utcNow()` per run) |
| Split Transfer Legs (Despatched / ReceiptOnly) | Conditional Split | `transforms.splitTransferLegs` -> two work tables |
| Post Transfer Movements (`Integration.usp_PostTransferMovements @RowsInserted OUTPUT`) | Execute SQL | set-based `transforms.buildTransferMovements` (ISSUE leg at from-site, RECEIPT leg at to-site when anything received) -> `runtime.mergeByKey(Fact.Movement, NaturalKeyHash)`; `@RowsInserted` from Delta `operationMetrics` |
| Escalate Aged In Transit | Execute SQL INSERT etl.RejectedRecord | `control.logRejectedRecordSet(objectName="stg.StockMovement", rejectReasonCode="TRANSFER_AGED_IN_TRANSIT", rejectStage="Fact")` on `QuantityInTransit > 0 AND TransitDays > InTransitAgeAlertDays` |
| Log Row Counts | Execute SQL | `ctx.logPackageRowCounts("Fact.Movement")` |

### INV_Reconcile_OnHand
| SSIS component | Type | Spark construct |
|---|---|---|
| Build On Hand Comparison | Execute SQL INSERT etl.ReconciliationResult (FULL OUTER JOIN, EXISTS timing test, `SiteScope`) | `transforms.buildOnHandComparison` + `runtime.replaceWhere(reconciliation_result, "BatchId = :b AND ObjectName = 'Fact.Stock Holding'")` |
| Classify Differences | Execute SQL -> `GenuineDifferenceCount`, `TimingDifferenceCount` | `transforms.classifyDifferences` |
| Escalate Genuine Differences (`GenuineDifferenceCount > 0`) | Execute SQL INSERT etl.RejectedRecord | `control.logRejectedRecordSet(objectName="Fact.Stock Holding", rejectReasonCode="ONHAND_VARIANCE", rejectStage="Fact")` |
| Log Row Counts | Execute SQL | `ctx.logPackageRowCounts("etl.ReconciliationResult")` **plus** `control.logRowCount(objectName="Fact.Stock Holding", sourceRowCount=SUM(SourceAmount), targetRowCount=SUM(TargetAmount), rejectRowCount=#Variance)` so the master's `Read On Hand Variance` query (`MAX(ABS(SourceRowCount - TargetRowCount))` for `Fact.Stock Holding`) has a row to read, and `control.assertRowCountTolerance(scope="OBJECT", objectName="Fact.Stock Holding", absoluteTolerance=OnHandVarianceTolerance, raiseOnFailure=False)` when the tolerance is supplied |

## 4. Control-framework calls -> `dbx_etl_common`

| Legacy call | `dbx_etl_common` call | Where |
|---|---|---|
| `etl.usp_LogPackageStart @BatchId, @PackageName, @ProjectName='WWI_Inventory'` | `control.logPackageStart(spark, catalog, batchId, packageName, projectName="WWI_Inventory", stepName=packageName)` | `runtime.PackageContext.__init__` |
| `etl.usp_LogPackageEnd @Status='Succeeded', @RowsRead, @RowsInserted, @RowsRejected` | `control.logPackageEnd(..., status="Succeeded", rowsRead, rowsInserted, rowsUpdated, rowsDeleted, rowsRejected)` | `PackageContext.succeed` |
| OnError: `etl.usp_LogError` + `etl.usp_LogPackageEnd @Status='Failed'` | `control.logError(...)` + `control.logPackageEnd(status="Failed")` then re-raise | `PackageContext.fail` / `runtime.runPackage` |
| `etl.usp_LogRowCount @PackageExecutionId, @ObjectName, @SourceRowCount, @TargetRowCount, @RejectRowCount` | `control.logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount, targetRowCount, rejectRowCount=...)` | `PackageContext.logPackageRowCounts` |
| direct `INSERT etl.RejectedRecord (BatchId, ObjectName, RejectStage, RejectReasonCode, RejectReason, BusinessKey, ...)` | `control.logRejectedRecordSet(spark, catalog, objectName, df[BusinessKey, RejectReason, RecordPayload], batchId, packageExecutionId, sourceSystemCode="WWIOLTP", rejectStage="Fact", rejectReasonCode, businessKeyColumn="BusinessKey")` | `PackageContext.logRejectedSet` |
| `etl.usp_StartBatch` / `usp_EndBatch` (only when run standalone with `BatchId = 0`) | `control.startBatch(..., "wwi_12_inventory", batchType="Daily", allowAdoptRunning=True)` / `control.endBatch` | `PackageContext` |
| `etl.usp_AssertRowCountTolerance` (master job) | `control.assertRowCountTolerance(...)` | `INV_Reconcile_OnHand`, `INV_Reconcile_Targets` |
| `$Package::BatchId`, `$Project::*` parameters | `params.getJobParams(dbutils)` | every notebook |
| three-part legacy names | `naming.table(catalog, schema, table)` via `inv_common.contracts.table` | everywhere |

Wheel strategy: each task lists `libraries: - whl: ${var.common_wheel}` (default
`../../common/dist/dbx_etl_common-0.1.0-py3-none-any.whl`, i.e. session 00's build under `databricks/common/dist`).
Override `common_wheel` per target if session 00 publishes to a Volume/workspace path instead.

## 5. Parameters

Job parameters (all strings, from the shared contract): `BatchId` (`"0"`), `BusinessDate` (`yyyy-MM-dd`, default
`${var.businessDate}`), `ReloadFullHistory` (`"False"`), `EnvironmentCode` (`${var.environmentCode}`),
`RestartFromStep` (`""`), `catalog` (`${var.catalog}`), plus the intraday-master gate inputs
`PickingWindowOpen` (`"True"`) and `OnHandVarianceTolerance` (`"25"`).

| Package | Legacy `$Package::` parameter | Default | Task `base_parameters` / widget |
|---|---|---|---|
| DailySnapshot | `SnapshotBusinessDate` (DateTime, `1900-01-01`) | job `BusinessDate` | `SnapshotBusinessDate = {{job.parameters.BusinessDate}}` |
| DailySnapshot | `DeleteExistingSnapshot` (Boolean) | `True` | `DeleteExistingSnapshot` |
| CycleCountVariance | `CountToleranceUnits` (Int32) | `2` | `CountToleranceUnits` |
| CycleCountVariance | `CountToleranceValue` (Int32) | `50` | `CountToleranceValue` |
| Replenishment | `CoverDays` (Int32) | `21` | `CoverDays` |
| Replenishment | `SuppressChillerSuggestions` (Boolean) | `False` | `SuppressChillerSuggestions` |
| StockTransfer | `InTransitAgeAlertDays` (Int32) | `10` | `InTransitAgeAlertDays` |
| Reconcile_OnHand | `SiteScope` (String) | `ALL` | `SiteScope` |
| Reconcile_OnHand | `TimingWindowMinutes` (Int32) | `30` | `TimingWindowMinutes` |
| Reconcile_OnHand | (master) `OnHandVarianceTolerance` | `25` | `OnHandVarianceTolerance = {{job.parameters.OnHandVarianceTolerance}}` |

`ReloadFullHistory` and `RestartFromStep` are read but have no effect in these packages (the legacy packages
ignore them too; restart granularity is the task, handled by "repair run" on the job).

## 6. Not migrated / needs decision

1. **`work.InventoryPositionDaily` schema mismatch (data semantics - needs a decision).** The package SQL
   (generator) reads an *operational position* shape: `StockItemId, WarehouseSiteCode, BinLocationCode,
   SnapshotDate, QuantityOnHand, QuantityAllocated, QuantityOnOrder, QuantityInTransit, LastMovementDate,
   ReceiptDate`. The checked-in DDL (`sqlserver/staging/tables/30_work_tables.sql`) and
   `work.usp_BuildInventoryPositionDaily` define a *roll-forward* shape: `PositionDate, StockItemBusinessKey,
   WarehouseCode, OpeningQuantity .. ClosingQuantity, ClosingValueUsd, AverageUnitCostUsd, ...` with no
   allocated / on-order / in-transit / last-movement / receipt-date columns. The notebooks implement the
   package SQL faithfully (the columns the packages *ask for*) and therefore expect `silver.work_inventory_position_daily`
   to expose that shape. Options: (a) session 04/STG publishes a view/table with the operational columns
   (`ClosingQuantity -> QuantityOnHand`, `AverageUnitCostUsd` as fallback cost, allocated/on-order taken from
   the stock-holding conform step `stg.usp_ConformStockHoldingForFact`), or (b) this project remaps to the
   roll-forward columns and drops `QuantityAllocated/OnOrder/InTransit`, `LastMovementDate`, `ReceiptDate`
   (which would zero `QuantityAvailable` logic, age bands and the expired-chiller flag). Not guessed; (a) is
   recommended. The same applies to `stg.StockItem` (`StockItemId`, `UnitCost`, `RegionCode`, `IsDiscontinued`,
   `SupplierId`, `ReorderLevel`, `TargetStockLevel`, `ShelfLifeDays` are used by the packages but not all are in
   `20_stg_tables_master.sql`), `stg.StockMovement` (transfer columns `StockTransferId, TransferReference,
   From/ToWarehouseSiteCode, DespatchedAtUtc, ReceivedAtUtc, QuantityDespatched, QuantityReceived,
   TransferStatusCode, CarrierCode, LoadBatchId` are package-only), and `stg.CycleCount`, `stg.WarehouseSite`,
   `stg.TransferPrice`, `work.StockItemDemand`, which have no DDL in `sqlserver/staging/` at all. All of these are
   read through `inv_common.contracts.TABLE_BINDINGS`, so a rename is a one-line change once decided.
2. **`Integration.usp_PostInventoryAdjustment` / `Integration.usp_PostTransferMovements` bodies are not in the
   repo** (`sqlserver/procedures/` has no Integration inventory procs). They were replaced by set-based MERGEs
   into `gold.fact_movement` with an explicit natural key (`CYCLECOUNT|CycleCountId`, `TRANSFER|StockTransferId|leg`)
   so reruns are idempotent (the legacy cursor would re-post on rerun). Movement type/reason codes
   (`ADJUST`/`CYCLECOUNT`, `TRANSFER`/`XFER_ISSUE`/`XFER_RECEIPT`), `CostingMethodCode` (`XFERP`/`STD`) and the
   `Fact.Movement` column set used (`Fact.Movement.Extensions.sql` shape) need confirmation against whatever
   `FACT_Load_Movement` (session 08) writes. `InferredMemberFlag` is set when the dimension has no current
   member instead of failing.
3. **`Aggregate.Daily Inventory Health` grain.** The legacy MERGE matched on `[Warehouse Site Code]` only, while the
   table is keyed by snapshot date / site / product category. Here the grain is (`SnapshotDate` = BusinessDate,
   `WarehouseSiteCode`, `ProductCategoryKey = 0`) and `RegionCode` is the *site's* region (the legacy grouped by the
   stock item's region, which would have produced multi-row-per-site MERGE conflicts). The other health measures
   (`SkuCount`, `ExcessStockSkuCount`, `SlowMovingSkuCount`, `StockOnHandValue`, ...) are filled by
   `AGG_Refresh_DailyInventoryHealth` (project 10), not by this package.
4. **`Fact.Stock Holding` join columns.** Reconcile joins `gold.fact_stock_holding` on `WWIStockItemID` /
   `WarehouseSiteCode` and reads `QuantityOnHand` (legacy: `[WWI Stock Item ID]`, `[Warehouse Site Code]`,
   `[Quantity On Hand]` per `Fact.Stock Holding.Extensions.sql`). Confirm with session 08's column naming.
5. **Timing test.** The legacy `EXISTS` used `stg.StockMovement.MovementAtUtc`; the checked-in staging DDL calls it
   `MovementDateTimeUtc`. Bound as `MovementAtUtc` in `transforms.buildOnHandComparison` (per the package SQL);
   rename in one place if the staging session ships the DDL name.
6. **Intraday cadence conflict (20 min vs 2 h)** - see section 1. Recommendation: schedule the intraday job every
   20 minutes 05:00-22:00 as documented in the runbook and treat the plan text as stale; if 2 h is intended,
   `TimingWindowMinutes` should be raised accordingly.
7. **Forbidden-content static check.** `validation/static/run_all_checks.py` scans every `.py/.yml/.md` in the
   repo for the words "Databricks"/"dbutils"; the required `# Databricks notebook source` header therefore fails
   `forbidden-content` for every `databricks/**` file and this document. PR #18 adds a `MIGRATION_TARGET_DIRS`
   carve-out for `databricks/`; `docs/migration/` needs the same carve-out. `validation/` is read-only for this
   session, so the carve-out must land via session 00 / the parent. All other static checks and the deep checks pass.
8. **Not migrated on purpose:** `NoAction` split output (legacy discarded it), the `Integration.*` cursors
   (replaced, see 2), `ERR_Reconcile_RowCounts` / `ERR_Notify_Operations` escalation phase (project 15),
   `AGG_Refresh_DailyInventoryHealth` (project 10), and the SSIS `MaxParallelStreams` / `ExtractAttempt` retry loop
   (extract projects).

## 7. Validation

* `databricks/12_inventory/validation/INV_Reconcile_Targets.py` - for each target in section 2 that this job
  writes, row count (per `BatchId`, else per `BusinessDate`) plus an order-independent fingerprint
  (`SUM(xxhash64(concat_ws(<sorted columns>)))`, volatile load-timestamp columns excluded) compared with a
  baseline supplied as `BaselineTable` (`ObjectName, BusinessDate, BatchId, RowCount, RowHashSum`) or
  `BaselineJson`; results written with `control.logRowCount` (baseline = source, Delta = target, hash mismatch ->
  `rejectRowCount = 1`) and summarised through `control.assertRowCountTolerance(absoluteTolerance=0, raiseOnFailure=False)`.
  Baseline capture on SQL Server: section 1 of `validation/runtime/02_row_count_reconciliation.sql` for counts,
  and for the hash e.g. `SELECT SUM(CAST(HASHBYTES('SHA2_256', CONCAT_WS(CHAR(1), <sorted columns>)) AS ...))` -
  the hash *function* differs between engines, so the hash comparison is only meaningful once the baseline is
  recomputed with `xxhash64` on Delta after the initial full load (row counts are comparable immediately).
* `tests/` - 23 pytest cases over `transforms` (age bands, obsolescence provision, lookup rejects, tolerance
  classification, adjustment/transfer legs, valuation rules, replenishment integer arithmetic, aggregate grain,
  reconciliation classification/timing window/site scope, deterministic hash, package lifecycle against the fake
  control layer).
* Offline: `python -m py_compile` on every notebook, `pytest`, `databricks bundle validate -t dev`.
