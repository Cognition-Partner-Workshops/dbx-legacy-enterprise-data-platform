# SSIS → Databricks migration — group `product_inventory`

Databricks-native replacement for the 21 **Product master & inventory / stock** SSIS packages
of the WideWorldImporters estate (parent master `Master_Daily_ETL`).

| | |
|---|---|
| Landing schema | `otterorders_migration.ssis_product_inventory` |
| Evidence | `otterorders_migration.evidence.recon_results` (`branch = 'ssis_product_inventory'`) |
| Workspace path | `/Workspace/Shared/ssis_migration/product_inventory` |
| Jobs | `ssis_product_inventory_end_to_end` (bronze → silver → dimensions → facts → inventory → aggregates → recon), `ssis_product_inventory_recon` (evidence only, re-runnable) |
| Compute | serverless (notebook tasks, no cluster definitions) |
| Style | PySpark library in `src/product_inventory/`, thin notebooks in `notebooks/`, pytest on local Spark in `tests/`; functions/variables camelCase, tables/columns snake_case |

## Layout

```
databricks.yml               bundle (target dev → /Workspace/Shared/ssis_migration/product_inventory)
resources/job_end_to_end.yml the two jobs
notebooks/run_stage.py       thin notebook: widgets → PipelineConfig → runStage(stage)
notebooks/run_recon.py       thin notebook: evidence writer
src/product_inventory/
  config.py     PipelineConfig (catalog/schema/batch/business date/parameters), foreign-catalog helpers
  control.py    etl.Watermark / etl.PackageExecution / err.RejectedRecord / work.LateArrivingDimensionQueue equivalents
  tables.py     Delta IO helpers (overwrite, append, replaceWhere, truncate-reload)
  rules.py      pure Column expressions for every SSIS derived column / conditional split
  scd.py        hybrid SCD2/SCD1, SCD1, reserved members, as-of surrogate-key lookup
  legacy.py     remote_query() access to legacy DW objects whose names contain spaces
  bronze.py     EXT_* packages       silver.py  STG_* packages
  gold_dimensions.py DIM_*           gold_facts.py FACT_*      gold_inventory.py INV_*      gold_aggregates.py AGG_*
  recon.py      reconciliation evidence (one row per package per run)
  packages.py   stage → package registry used by the notebooks
tests/          watermarks, SCD2/SCD1, dedup, as-of/unknown-member lookup, crosswalk precedence, inventory rules
```

## Package → artifact mapping

Legend for *Verdict*: from the latest `recon_results` run (see "Evidence").
Load types are the SSIS ones from `docs/inventories/ssis-packages.csv`.

| # | Package | Load type | Source (read via) | Target Delta table | Runner | Verdict |
|---|---|---|---|---|---|---|
| 1 | `EXT_ORA_ProductMaster` | incremental_timestamp | `wwi_legacy_oracle.wwi_mdm.product_master` (+ `product_category`, `product_uom_conv`) | `bronze_ora_product_master`, `bronze_ora_product_category` | `bronze.runExtOraProductMaster` | PARTIAL |
| 2 | `EXT_ORA_ProductHierarchy` | full | `wwi_legacy_oracle.wwi_mdm.product_hierarchy` | `bronze_ora_product_hierarchy` | `bronze.runExtOraProductHierarchy` | PARTIAL |
| 3 | `EXT_SQL_StockItems` | incremental_timestamp (ValidFrom, 4 h lookback) | `wwi_legacy_oltp.Warehouse.StockItems` + `StockItems_Archive` + `StockItemHoldings` + `ReplenishmentRules` | `bronze_sql_stock_item` | `bronze.runExtSqlStockItems` | FAIL* |
| 4 | `EXT_SQL_StockMovements` | incremental_key (StockItemTransactionID) | `wwi_legacy_oltp.Warehouse.StockItemTransactions` + `Application.TransactionTypes` + `Warehouse.StockMovementDetails` | `bronze_sql_stock_movement` | `bronze.runExtSqlStockMovements` | FAIL* |
| 5 | `EXT_SQL_StockTransfers` | incremental_key (StockTransferLineID) | `wwi_legacy_oltp.Warehouse.StockTransfers/StockTransferLines/WarehouseSites` | `bronze_sql_stock_transfer` | `bronze.runExtSqlStockTransfers` | FAIL* |
| 6 | `STG_Load_Product` | truncate_reload | `bronze_ora_product_master` | `silver_product` (+ `err_rejected_record`) | `silver.runStgLoadProduct` | PARTIAL |
| 7 | `STG_Load_StockItem` | truncate_reload | `bronze_sql_stock_item` | `silver_stock_item` | `silver.runStgLoadStockItem` | PARTIAL |
| 8 | `STG_Load_StockMovement` | incremental_append | `bronze_sql_stock_movement`, `silver_stock_item` | `silver_stock_movement` | `silver.runStgLoadStockMovement` | PARTIAL |
| 9 | `STG_Work_ProductCrosswalk` | work_rebuild | `silver_stock_item`, `silver_product` | `work_product_crosswalk` | `silver.runStgWorkProductCrosswalk` | PARTIAL |
| 10 | `STG_Work_InventoryPosition` | work_rebuild (90 days) | `silver_stock_movement`, `silver_stock_item` | `work_inventory_position_daily` | `silver.runStgWorkInventoryPosition` | PARTIAL |
| 11 | `DIM_Load_StockItem` | SCD2 (hybrid) | `silver_stock_item`, `work_product_crosswalk`, `silver_product` | `gold_dim_stock_item` | `gold_dimensions.runDimLoadStockItem` | PARTIAL |
| 12 | `DIM_Load_ProductCategory` | SCD1 | `bronze_ora_product_category` | `gold_dim_product_category` | `gold_dimensions.runDimLoadProductCategory` | PARTIAL |
| 13 | `FACT_Load_Movement` | incremental_fact | `silver_stock_movement`, `gold_dim_stock_item`, legacy `Dimension.Customer/Supplier/Transaction Type/Date` | `gold_fact_movement` (+ `work_late_arriving_dimension_queue`) | `gold_facts.runFactLoadMovement` | FAIL* |
| 14 | `FACT_Load_StockHolding` | snapshot_fact | `silver_stock_item`, `gold_dim_stock_item` | `gold_fact_stock_holding` | `gold_facts.runFactLoadStockHolding` | see Evidence |
| 15 | `FACT_Load_DailyInventorySnapshot` | snapshot_fact | `silver_stock_item`, `silver_stock_movement`, `work_inventory_position_daily` | `gold_fact_daily_inventory_snapshot` | `gold_facts.runFactLoadDailyInventorySnapshot` | PARTIAL |
| 16 | `INV_Load_CycleCountVariance` | business_rule | `wwi_legacy_oltp.Warehouse.CycleCounts/CycleCountLines/Bins`, `work_inventory_position_daily` | `gold_inv_cycle_count_variance`, `gold_inv_adjustment_movement` | `gold_inventory.runInvLoadCycleCountVariance` | PARTIAL |
| 17 | `INV_Load_DailySnapshot` | business_rule | `silver_stock_item`, `bronze_sql_warehouse_site`, `work_inventory_position_daily` | `gold_inv_daily_snapshot` | `gold_inventory.runInvLoadDailySnapshot` | PARTIAL |
| 18 | `INV_Load_Replenishment` | business_rule | `silver_stock_item`, `silver_stock_movement` | `gold_inv_replenishment_suggestion` | `gold_inventory.runInvLoadReplenishment` | PARTIAL |
| 19 | `INV_Load_StockTransfer` | business_rule | `bronze_sql_stock_transfer`, `silver_stock_item` | `gold_inv_stock_transfer_movement` | `gold_inventory.runInvLoadStockTransfer` | PARTIAL |
| 20 | `INV_Reconcile_OnHand` | business_rule | `gold_fact_stock_holding`, `work_inventory_position_daily` | `gold_inv_onhand_reconciliation` | `gold_inventory.runInvReconcileOnHand` | PARTIAL |
| 21 | `AGG_Refresh_DailyInventoryHealth` | aggregate_rebuild | `gold_fact_daily_inventory_snapshot`, `gold_inv_replenishment_suggestion` | `gold_agg_daily_inventory_health` | `gold_aggregates.runAggRefreshDailyInventoryHealth` | PARTIAL |

Control tables (group-local equivalents of `etl.*` / `err.*` / `work.*`): `etl_watermark`,
`etl_package_execution`, `err_rejected_record`, `work_late_arriving_dimension_queue`, `recon_evidence`
(local copy of the evidence rows).

\* see "Evidence" for the one-line causes.

## How to run

```bash
export DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST="$DATABRICKS_DEMO_HOST" DATABRICKS_TOKEN="$DATABRICKS_DEMO_TOKEN"
cd databricks/ssis_migration/product_inventory
ruff check . && python -m pyflakes src notebooks && pytest -q tests
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev --var="git_sha=$(git rev-parse HEAD)"
databricks bundle run ssis_product_inventory_end_to_end -t dev
databricks bundle run ssis_product_inventory_recon -t dev          # evidence only
```

Job parameters (all optional): `business_date` (default = latest movement date), `batch_id`
(default = run timestamp), `reload_full_history` (default `true`: the SSIS
`ReloadFullHistory` flag — set to `false` for watermark-driven daily runs), `site_scope`
(`ALL` or one warehouse site code for `INV_Reconcile_OnHand`), `cover_days`, `position_window_days`,
`suppress_chiller_suggestions`, `in_transit_age_alert_days`, `timing_window_minutes`,
`retention_days`, `agg_days_back`, `git_sha`.

## Design decisions

* **One PySpark library, one stage notebook.** Every package is a `run<Package>(spark, cfg)`
  function returning row counters; `packages.py` maps stage → package list and the notebook is
  only widgets + `runStage`. Pure transformation logic (`rules.py`, `scd.py`, the `shape*`/`build*`
  functions) takes and returns DataFrames so it can be tested on local Spark without Unity Catalog.
* **Sources through Lakehouse Federation.** All OLTP / Oracle reads use the foreign catalogs with
  3-level names. Legacy DW objects whose names contain spaces (`Dimension.Stock Item`,
  `Fact.Stock Holding`, …) cannot be resolved by the federation parser, so `legacy.py` reads
  them with `remote_query(<connection>, database => 'WideWorldImportersDW', query => '<T-SQL>')`.
  No DDL/DML is ever issued against SQL Server, Oracle or SSISDB.
* **Cross-group dimensions are read from the legacy DW** (`Dimension.Customer`, `Dimension.Supplier`,
  `Dimension.Transaction Type`, `Dimension.Date`) for surrogate keys in `FACT_Load_Movement`, as the
  shared context requires; only `Stock Item` and `Product Category` are built here.
* **Control tables are group-local Delta tables**: watermarks (`etl_watermark`), package executions,
  rejects (`err_rejected_record` with the full row as JSON payload, one reason code per SSIS error
  output / conditional-split reject) and the late-arriving dimension queue.
* **Load semantics preserved per package**
  * `EXT_ORA_ProductMaster`: `[watermark-240min, now)` timestamp window on `updated_dt`,
    category + primary UOM conversion joins, ERP defaults (`UNCLASS`, `EA`, pack 1, hazard `N`,
    price 0 `USD`, weight `KG`), sellable/discontinued/rejected routing, `source_system_code='ORA_ERP'`.
  * `EXT_ORA_ProductHierarchy`: full reload; the flat level columns are unpivoted into
    root/parent/child nodes with `node_level`, `node_path`, `leaf_flg`, sibling ordering.
  * `EXT_SQL_StockItems`: `ValidFrom` watermark with 4 h lookback over `StockItems` ∪
    `StockItems_Archive` (temporal history), holdings + replenishment-rule joins, deleted-row
    detection (`delete_flag`), `source_system_code='WWI_OLTP'`.
  * `EXT_SQL_StockMovements`: `StockItemTransactionID > from AND <= to` key window, transaction
    type / site / bin enrichment, `movement_direction_code`, `movement_class`.
  * `EXT_SQL_StockTransfers`: `StockTransferLineID` key window, header/line/site joins,
    `in_transit_quantity`, cancelled transfers excluded, stale-transit flag.
  * `STG_Load_*`: truncate/reload (`silver_product`, `silver_stock_item`) or append
    (`silver_stock_movement`) with the SSIS derived columns and conditional splits
    (`rules.py`), rejects to `err_rejected_record`.
  * `STG_Work_ProductCrosswalk`: GTIN/barcode key first, normalised name second, name keys
    shorter than 8 characters are `UNMATCHABLE`, one preferred match per stock item
    (`survivorship_rank = 1`), unmatched rows are rejects.
  * `STG_Work_InventoryPosition`: 90-day rebuild with running balance seeded from all prior
    movements, item/site/day grain, `NEGATIVE|ZERO|POSITIVE`, high-churn flag (> 50 movements),
    plausibility band ±1,000,000, lookup failures rejected.
  * `DIM_Load_StockItem`: hybrid SCD — commercial attributes type 2 (new version, previous
    `valid_to = next valid_from − 1 s`), marketing/search/comment attributes type 1, far-future
    current rows, `type1_hash`/`type2_hash`/`row_version`, reserved members `0`/`-1`/`-2`
    (`Integration.EnsureUnknownMembers`) and inferred members created by the fact load.
  * `DIM_Load_ProductCategory`: SCD1 with `-1` parent fallback, `category_path`,
    `CHILL→PERISHABLE`, `TOY|NOV→SEASONAL`, else `CORE`.
  * `FACT_Load_Movement`: `LastModifiedAt > watermark_from AND <= watermark_to`, as-of surrogate
    lookup with fallback to the current row then unknown key `0`, late-arriving queue + inferred
    members, movement sign / reason group, reversal links, corrected transaction ids replace the
    earlier fact rows.
  * `FACT_Load_StockHolding`: full rebuild of the requested `as_at_date_key`, available quantity,
    stock value, cover, `OUTOFSTOCK|OVERSOLD|REORDER|HEALTHY`.
  * `FACT_Load_DailyInventorySnapshot`: dense item × warehouse-site grid for the business date,
    carry-forward of the prior day's closing quantity, zero-denominator guards, cover bands,
    aged stock + obsolescence provision, retention trim (`retention_days`).
  * `INV_*`: implemented as gold marts. Cycle-count auto-post rows are written to
    `gold_inv_adjustment_movement` (the SSIS package would have posted them to OLTP — we never
    write to the legacy host); the replenishment safety factors are `APAC 1.5 / EU 1.1 / else 1.25`;
    cross-region transfers are valued at transfer price or `standard cost × 1.08`; on-hand
    reconciliation classifies `MATCHED|TIMING|NEGATIVE|VARIANCE` and supports `site_scope`.
  * `AGG_Refresh_DailyInventoryHealth`: rebuilds the `[business_date − agg_days_back, business_date]`
    window per site/day (cover bands, stockout counts, aged-stock %, provision, replenishment counts).

## Deliberate deviations from the SSIS packages

* Row-by-row `Lookup` components (customer / supplier / transaction type / stock item) are
  set-based joins; the OLE DB batch sizes, `MaxInsertCommitSize`, `TABLOCK` hints and `FastLoad`
  options have no equivalent and were dropped.
* `Integration.EnsureUnknownMembers` and `Integration.GetLineageKey` stored procedures are
  replaced by `ensureStockItemUnknownMembers()` and the `batch_id`-based `lineage_key`.
* `etl.usp_GetWatermark/usp_SetWatermark` become append-only rows in `etl_watermark`.
* `INV_Load_CycleCountVariance` never posts adjustments to `Warehouse.*`; the auto-post output
  lives in `gold_inv_adjustment_movement` for a downstream integration to consume.
* Error outputs that SSIS redirected to `err.RejectedRecord` carry the *whole* row as JSON
  (`payload_json`) instead of the truncated `ErrorColumn`/`ErrorCode` pair.
* The Stock Item dimension only creates a new version when a **commercial (type-2)** attribute
  changes. The legacy DW contains a version for every temporal (`StockItems_Archive`) row even
  when nothing tracked changed (444 archive rows carry no type-2 change), so history row counts
  differ by design; current rows reconcile (see Evidence).
* Product category and hierarchy: the Oracle `product_uom_conv` table is empty on the host, so
  the UOM-conversion join is exercised only by its defaults.

## Reconciliation evidence

`recon.py` builds, for each package, an *expected* and a *target* DataFrame over the same
business columns (surrogate keys and load timestamps excluded), casts them to identical types and
compares row count, `SUM(xxhash64(cols))`, `BIT_XOR(xxhash64(cols))` and a key-column null rate.
One row per package is appended to `otterorders_migration.evidence.recon_results` (and copied to
`recon_evidence` in the landing schema) with `checks` as a JSON array.

* **legacy baseline** (PASS possible): `EXT_SQL_StockItems`, `EXT_SQL_StockMovements`,
  `EXT_SQL_StockTransfers` (legacy `raw.*`), `DIM_Load_StockItem` (current rows of
  `Dimension.Stock Item`), `FACT_Load_Movement` (`Fact.Movement` joined back to WWI ids),
  `FACT_Load_StockHolding` (`Fact.Stock Holding`).
* **source_derived baseline** (capped at PARTIAL, `{"baseline":"source_derived"}` in checks):
  every package whose legacy target is empty on the host (`raw.OracleProductMaster`, `stg.*`,
  `work.*`, `Dimension.Product Category`, `Fact.Daily Inventory Snapshot`,
  `Aggregate.Daily Inventory Health`, `etl.ReconciliationResult`, cycle counts / transfers with no
  source rows). The expected result is re-derived from the live source with the package's own
  logic.

### Latest run

EVIDENCE_PLACEHOLDER

## Open questions

* The legacy `raw.SqlStockItem` / `raw.SqlStockMovement` tables contain a synthetic seed
  (StockItemID 500–1299, 4,000 transactions) rather than an SSIS extract of the live OLTP tables;
  the extract packages therefore reconcile as FAIL against that baseline although they extract the
  live `Warehouse.*` data correctly (source-derived checks pass). Confirm whether the seed or the
  live OLTP is the intended baseline.
* `Fact.Movement` on the legacy host includes 4 zero-quantity transactions that
  `STG_Load_StockMovement` rejects by specification; confirm which behaviour is wanted.
* `Warehouse.StockTransfers`, `CycleCounts`, `ReplenishmentRules`, `Bins` and `product_uom_conv`
  are empty on the host, so the transfer / cycle-count / replenishment-rule paths are only
  exercised by unit tests and by their defaults.
* `Dimension.Product Category` / `Product Hierarchy` are empty in the legacy DW, so the category
  dimension can only be source-derived.
