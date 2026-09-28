# 11_sales (WWI_Sales) -> `databricks/11_sales` package mapping

Legacy project `ssis/11_sales` (generator `build_sales_packages.py`, orchestration phase **Sales Mart**,
sequence 90, group `Mart`, three parallel streams, step variable `BatchStepIdSalesMart`) migrated to the
Databricks Asset Bundle `wwi_11_sales` with one job `wwi_11_sales`.

* Bundle: `databricks/11_sales/databricks.yml` (targets `dev`, `prod`; `catalog` defaults to `wwi_${bundle.target}`)
* Job: `databricks/11_sales/resources/wwi_11_sales.job.yml` (task keys = legacy package names)
* Shared logic: `databricks/11_sales/src/*.py` (pure DataFrame functions, unit tested under `tests/`)
* Reconciliation: `databricks/11_sales/validation/SLS_Reconcile_SalesMart.py`

## 1. Packages -> notebooks / tasks

| Legacy package | Job task_key | Notebook | depends_on (in job) | Notes |
|---|---|---|---|---|
| `SLS_NA_Load_Commission` | `SLS_NA_Load_Commission` | `notebooks/SLS_Load_Commission.py` with `RegionCode=NA` (thin per-package wrapper `notebooks/SLS_NA_Load_Commission.py` kept for traceability) | - | stream 1 |
| `SLS_EU_Load_Commission` | `SLS_EU_Load_Commission` | `notebooks/SLS_Load_Commission.py` with `RegionCode=EU` (`notebooks/SLS_EU_Load_Commission.py`) | - | stream 2 |
| `SLS_APAC_Load_Commission` | `SLS_APAC_Load_Commission` | `notebooks/SLS_Load_Commission.py` with `RegionCode=APAC` (`notebooks/SLS_APAC_Load_Commission.py`) | - | stream 3 |
| `SLS_Load_QuotaAttainment` | `SLS_Load_QuotaAttainment` | `notebooks/SLS_Load_QuotaAttainment.py` | - | inputs come from STG_* phases |
| `SLS_Load_PromotionRedemption` | `SLS_Load_PromotionRedemption` | `notebooks/SLS_Load_PromotionRedemption.py` | - | inputs come from `STG_Load_PromotionAndTerritory` |
| `SLS_Export_PartnerFeed` | `SLS_Export_PartnerFeed` | `notebooks/SLS_Export_PartnerFeed.py` | the three commission tasks | `package-dependencies.csv`: `SLS_*_Load_Commission -> SLS_Export_PartnerFeed (data:Fact.Sale)` |
| (new) | `SLS_Reconcile_SalesMart` | `validation/SLS_Reconcile_SalesMart.py` | quota, promotion, partner feed; `run_if: ALL_DONE` | row counts + hashes vs SQL Server baseline |

Cross-project edges (`STG_Load_Sale`, `STG_Load_PromotionAndTerritory`, `FACT_*_Load_Sale`, `FACT_Dedup_Sale`,
`FACT_Apply_Corrections` -> `SLS_*`; `SLS_*_Load_Commission` -> `C360_*`, `FACT_Dedup_Sale`) are phase-level
edges owned by session 00's master job; inside this job they are implicit (the whole job runs in the Sales Mart phase).

## 2. Source / target objects -> Delta tables

| Legacy object | Role | Delta table | Written by |
|---|---|---|---|
| `stg.SaleLine` | source | `silver.stg_sale_line` | STG session |
| `stg.CommissionPlan` | source | `silver.stg_commission_plan` | STG session |
| `stg.FxRate` | source (EU / APAC AVERAGE rates) | `silver.stg_fx_rate` | STG session |
| `stg.FiscalCalendar445` | source (APAC, quota) | `silver.stg_fiscal_calendar445` | STG session |
| `stg.CustomerPayment` | source (EU cash-basis release) | `silver.stg_customer_payment` | STG session |
| `Dimension.Customer` | lookup (NA house accounts, partner feed) | `gold.dim_customer` | DIM session |
| `Dimension.Stock Item` | lookup (partner feed) | `gold.dim_stock_item` | DIM session |
| `Dimension.Partner` | lookup (partner feed) | `gold.dim_partner` | see §6 |
| `Fact.Sale` | source (partner feed) | `gold.fact_sale` | FACT session |
| `work.FxRevaluationRate` | lookup (partner feed settlement FX) | `silver.work_fx_revaluation_rate` | FIN session (optional here, see §6) |
| `stg.SalesTerritory`, `stg.SalesQuota`, `stg.CreditNote`, `stg.OrderLine` | source (quota) | `silver.stg_sales_territory`, `silver.stg_sales_quota`, `silver.stg_credit_note`, `silver.stg_order_line` | STG session |
| `stg.Promotion`, `stg.PromotionRedemption` | source (promotion) | `silver.stg_promotion`, `silver.stg_promotion_redemption` | STG session |
| `work.CommissionNa` / `work.CommissionEu` / `work.CommissionEuHeld` / `work.CommissionApac` | work | `silver.work_commission_na` / `_eu` / `_eu_held` / `_apac` | this bundle (overwrite per run = legacy TRUNCATE) |
| `err.CommissionApacReject` | reject | `silver.err_commission_apac_reject` + `control.logRejectedRecordSet` | this bundle |
| `Integration.usp_PostCommission` -> `Fact.Sale` | target | `gold.fact_sale_commission` (MERGE on `RegionCode, SaleLineId, CommissionPeriod`) | this bundle, see §6 |
| `work.QuotaAttainment` | work | `silver.work_quota_attainment` | this bundle |
| `Aggregate.Regional Sales Performance` | target | `gold.agg_regional_sales_performance` (MERGE on `RegionCode, TerritoryCode, QuotaPeriod`) | this bundle |
| `work.PromotionRedemption` / `work.PromotionSpill` | work | (in-memory) / `silver.work_promotion_spill` | this bundle |
| `Aggregate.Promotion Effectiveness` | target | `gold.agg_promotion_effectiveness` (MERGE on `RegionCode, PromotionId`) | this bundle |
| `work.PartnerFeedRow` | work | `silver.work_partner_feed_row` | this bundle |
| `work.PartnerFeedArchive` | archive of exported rows | `silver.work_partner_feed_archive` (`replaceWhere BatchId`) | this bundle |
| `file:partner_feed.csv` (`$Project::ArchiveFileRoot`, `WWI_Archive_Files` connection) | flat-file destination | UC Volume `<VolumeRoot>/outbound/partner_feed/partner_feed_YYYYMMDD.csv` + `<VolumeRoot>/archive/partner_feed/yyyy/MM/partner_feed_YYYYMMDD.csv` | this bundle |
| baseline for reconciliation | input | `etl.sales_reconciliation_baseline` (or `BaselineJson` parameter) | captured from SQL Server |

Column normalisation: the generator SQL uses logical names (`QuantitySold`, `ExtendedPrice`, `VatAmount`, `GstAmount`,
`CurrencyCode`, `LoadBatchId`, `LineTypeCode`, `SaleLineId`) that differ from the conformed `stg.SaleLine` DDL
(`Quantity`, `NetLineAmount`/`GrossLineAmount`, `TaxAmount`, `TransactionCurrencyCode`, `BatchId`, `SaleLineBusinessKey`).
`sales_common.resolveColumns` maps each logical name to an ordered list of candidate physical columns
(`sales_commission.saleLineColumnMap`, `sales_quota.*_COLUMNS`, `sales_partner_feed.legacy*`) and fails fast when
a mandatory column is absent, so the notebooks work against either naming.

## 3. SSIS components -> Spark constructs

| Package | SSIS component | Spark equivalent |
|---|---|---|
| all | `Log Package Start` / `Log Package Success` / OnError `Log Error` + `Mark Execution Failed` | `sales_common.legacyPackageRun` -> `control.logPackageStart` / `control.logPackageEnd(status="Succeeded"|"Failed", rows...)` / `control.logError`; exception re-raised |
| all | Execute SQL `TRUNCATE TABLE work.*` | `sales_common.overwriteTable` (Delta `overwrite` + `overwriteSchema`) |
| all | Execute SQL `Log Row Counts` (`etl.usp_LogRowCount`) | `control.logRowCount(...)` per legacy object name |
| all | `$Package::` parameters / project parameters | job parameters (widgets) `CommissionMonth`, `HouseAccountRatePercent`, `CashBasisCountries`, `FiscalPeriod445`, `TeamSplitEnabled`, `AttainmentPeriod`, `IncludePartialPeriod`, `AttributionMode`, `PartnerScope`, `SuppressUnconsentedEuRows`, `VolumeRoot` |
| NA | `Find Reps Without A Plan` (Execute SQL -> variable) | `sales_commission.countUnplannedReps` -> summary |
| NA | `Calculate NA Commission` (Execute SQL / derived columns) | `computeNaCommission`: `ExtendedPrice + TaxAmount`, base rate, accelerator above `AcceleratorThresholdAmount`, house-account factor `HouseAccountRatePercent/100` |
| NA/EU/APAC | Lookup `Dimension.Customer` (house account) | `currentHouseAccountFlags` (current rows only; warning + factor 1 when the column is missing) |
| EU | `Calculate EU Commission` | `computeEuCommission`: `COALESCE(NetAmount, Gross/(1+VatRate/100), Gross-VatAmount)`, month-end `AVERAGE` FX to EUR (left join, rate 1 when missing / EUR), statutory cap `LEAST(raw, StatutoryCapAmount)` |
| EU | Conditional split cash-basis countries -> `work.CommissionEuHeld` | `splitCashBasis` -> `silver.work_commission_eu_held` |
| EU | `Release Cash Basis Lines With Payment` | `releaseHeldEuLines`: inner join `CLEARED` payments, `CommissionPeriod` = payment month, union into work table |
| EU | `Count Capped Representatives` | `countCappedReps` |
| APAC | `Check 445 Calendar Coverage` (precedence constraint blocks when days missing) | `countMissingCalendarDays` > 0 -> `logError(Warning)` + zero row counts, package succeeds without posting (legacy "skip" branch) |
| APAC | Lookup `stg.FxRate` with error output -> `err.CommissionApacReject` | `computeApacCommission` returns `(matched, rejected)`; rejected -> `silver.err_commission_apac_reject` + `control.logRejectedRecordSet(reason FX_RATE_MISSING)` |
| APAC | Derived column team split | `CommissionAmount * TeamSplitPercent/100` when `TeamSplitEnabled` |
| APAC | `Count Period Boundary Lines` | `countPeriodBoundaryLines` |
| NA/EU/APAC | `Post <Region> Commission` (`EXEC Integration.usp_PostCommission`) | `postingRows` + `sales_common.mergeInto` into `gold.fact_sale_commission` |
| Quota | `Check Territories Without Quota` | `countTerritoriesWithoutQuota` |
| Quota | `Build Attainment By Region` (3 regional SELECTs UNION ALL) | `buildAttainmentByRegion`: NA `sum(ExtendedPrice+TaxAmount)` by invoice month, EU `sum(NetAmount) - sum(credit notes)`, APAC order intake left-joined to the 4-4-5 calendar |
| Quota | `Derive Attainment Metrics` (derived column) | `deriveAttainmentMetrics`: `AttainmentPercent`, bands `NOQUOTA / OVER120 / AT / NEAR / UNDER` |
| Quota | `Load Regional Performance` (OLE DB destination) | `toRegionalSalesPerformance` + MERGE |
| Promotion | `Attribute Redemptions` join + `Classify Attribution` (derived column) | `joinRedemptions`, `classifyAttribution`: `AttributionEndDate = EndDate + 30 (NA) / +14 (APAC) / +0 (EU)`, `STRICT` uses `EndDate`; `DiscountCostAmount` PCT vs FIXED |
| Promotion | `Route Spill` (conditional split) | `splitSpill` -> `silver.work_promotion_spill` |
| Promotion | `Summarise Promotion` (aggregate) | `summarisePromotion`: `sum(RedeemedAmount)`, `sum(DiscountCostAmount)`, `count`, `countDistinct(CustomerId)` |
| Promotion | `Flag Over Budget Promotions` (Execute SQL UPDATE) | `flagOverBudget`: `BudgetStatus = 'OVER'` when cost > budget |
| Partner feed | `Build Partner Feed Rows` (OLE DB source `work PartnerFeedRow`) | `buildPartnerFeedRows`: fact -> customer -> stock item -> partner joins, settlement FX `AVERAGE` rate, invoice date > BusinessDate-1 window, `PartnerScope` filter, EU no-consent `CustomerReference = 'REDACTED'` |
| Partner feed | `Suppress Unconsented EU Rows` (conditional) | `suppressUnconsentedEuRows` when `SuppressUnconsentedEuRows=True`; deleted rows counted as `rowsRejected` |
| Partner feed | `Count Exported Rows` (Row Count) | `workRows.count()` -> `run.rowsRead` |
| Partner feed | `Derive Feed File Name` (expression) | `outboundFileName` -> `partner_feed_YYYYMMDD.csv` |
| Partner feed | Sort `PartnerCode, InvoiceNumber` + Flat File destination | `orderedFeedRows` + `renderFeed` (header row, `,` delimiter, no text qualifier unless needed, `\r\n`, code page 1252) + `writeFeedFiles` to outbound and archive Volume paths |
| Partner feed | OLE DB destination `work PartnerFeedArchive` | `toArchiveRows` + `replaceWhere BatchId` into `silver.work_partner_feed_archive` |
| Partner feed | `RowsRead > 0` precedence constraint | `if run.rowsRead > 0:` around the export |

## 4. Control framework -> `dbx_etl_common`

| Legacy call | `dbx_etl_common` call |
|---|---|
| `etl.usp_StartBatch` (only when run standalone, `BatchId = 0`) | `control.startBatch(spark, catalog, "WWI_Sales:<package>", batchType="Adhoc", businessDate, environmentCode)` then `control.endBatch` |
| `etl.usp_LogPackageStart @ProjectName='WWI_Sales', @StepName='Sales Mart'` | `control.logPackageStart(spark, catalog, batchId, packageName, projectName="WWI_Sales", stepName="Sales Mart")` |
| `etl.usp_LogPackageEnd` | `control.logPackageEnd(..., status, rowsRead, rowsInserted, rowsUpdated, rowsDeleted, rowsRejected)` |
| `etl.usp_LogError` (OnError handler) | `control.logError(..., errorSeverity="Error", sourceName=package, sourceComponent=<task name>, errorDescription)` |
| `etl.usp_LogRowCount` | `control.logRowCount(..., objectName=<legacy object>, sourceRowCount, targetRowCount, insertRowCount, updateRowCount, deleteRowCount, rejectRowCount)` |
| `etl.usp_LogRejectedRecordSet` (APAC FX lookup error output) | `control.logRejectedRecordSet(..., "stg.SaleLine", rejectedDf, rejectReasonCode="FX_RATE_MISSING", businessKeyColumn="SaleLineId")` |
| `etl.usp_GetConfiguration('RowCountVarianceTolerancePercent')` (reconciliation) | `control.getConfiguration(...)` |
| `RestartFromStep` | packages skip themselves when `RestartFromStep` names a phase after Sales Mart (`sales_common.shouldSkipForRestart`) |

`legacyPackageRun` is a thin wrapper over `logPackageStart` / `logPackageEnd` / `logError` (not a reimplementation of
`control.packageRun`) so the OnError semantics of the legacy packages (log error with the failing task name, mark the
execution failed, fail the standalone batch) are preserved. The wheel is attached through a serverless job environment
(`environments[0].spec.dependencies = ${var.dbx_etl_common_whl}`, default `/Workspace/Shared/wwi/common/dist/dbx_etl_common-0.1.0-py3-none-any.whl`).
No `[dbx-migration 00]` PR existed when this bundle was written, so imports follow the interface contract verbatim;
`tests/fakes/dbx_etl_common` is a test-only fake.

## 5. Parameters

| Parameter | Default | Used by |
|---|---|---|
| `BatchId`, `BusinessDate`, `ReloadFullHistory`, `EnvironmentCode`, `RestartFromStep`, `catalog` | `"0"`, `${var.business_date}`, `"False"`, `${var.environment_code}`, `""`, `${var.catalog}` | all |
| `RegionCode` (task base parameter) | `NA` / `EU` / `APAC` | commission notebook |
| `CommissionMonth` | month of `BusinessDate` | NA, EU posting period |
| `HouseAccountRatePercent` | `50` | NA |
| `CashBasisCountries` | `DE,AT` | EU |
| `FiscalPeriod445` | period of `BusinessDate` from `stg_fiscal_calendar445` | APAC |
| `TeamSplitEnabled` | `True` | APAC |
| `AttainmentPeriod` | month of `BusinessDate` | quota |
| `IncludePartialPeriod` | `True` | quota (declared by the legacy package, never referenced - kept for parity) |
| `AttributionMode` | `REGIONAL` (`STRICT` allowed) | promotion |
| `PartnerScope` | `ALL` | partner feed |
| `SuppressUnconsentedEuRows` | `True` | partner feed |
| `VolumeRoot` | `/Volumes/<catalog>/etl/files` | partner feed (`$Project::ArchiveFileRoot`) |
| `BaselineTable`, `BaselineJson`, `VarianceTolerancePercent` | `<catalog>.etl.sales_reconciliation_baseline`, `""`, `etl.configuration` value | reconciliation |

## 6. Not migrated / needs decision

1. **`Integration.usp_PostCommission` is not in the repo.** The procedure body is not under `sqlserver/procedures/`
   or `sqlserver/warehouse/`, so its exact effect on `Fact.Sale` is unknown. The migration posts the commission
   lines into a dedicated `gold.fact_sale_commission` Delta table (MERGE on `RegionCode, SaleLineId, CommissionPeriod`)
   instead of updating columns on `gold.fact_sale`. **Decision:** keep the side table, or agree with the FACT session
   on commission columns in `gold.fact_sale` and turn the merge into an update.
2. **`Dimension.Customer` has no `IsHouseAccount` column** in `sqlserver/warehouse/dimensions`. NA house-account
   handling looks for `IsHouseAccount` / `HouseAccountFlag` / `Is House Account`; when none exists it logs a warning and
   applies a factor of 1 (no house-account discount). **Decision:** where does the house-account flag come from?
3. **`Dimension.Partner`, `c.[Share Consent Flag]`, `c.[Customer Reference]`, `work.FxRevaluationRate`** referenced by the
   partner-feed SQL have no DDL in the repo. The notebook expects `gold.dim_partner` (`PartnerKey`, `PartnerCode`,
   `SettlementCurrencyCode`), consent / reference columns on `gold.dim_customer` (candidates `ShareConsentFlag`,
   `Share Consent Flag`, `SourceCustomerReference`, `Customer Reference`, `CustomerReference`) and treats
   `silver.work_fx_revaluation_rate` as optional (warning + amounts left in transaction currency when absent).
4. **Partner feed flat-file format.** The `.dtsx` exposes the flat-file connection with code page 1252 but no explicit
   column delimiter / text qualifier / header flag; the notebook uses the SSIS defaults (header row, `,` delimiter,
   `\r\n`, quoting only when a value contains `,`/`"`/newline) and dates as `yyyy-MM-dd`, amounts with 2 decimals.
   Confirm against a legacy sample file before go-live.
5. **APAC quota actuals.** The legacy APAC branch LEFT JOINs `stg.FiscalCalendar445` and does not restrict rows to the
   requested period, so all order lines of the territory are summed; the migration preserves that (documented in
   `sales_quota.buildAttainmentByRegion`). Confirm whether the intent was `FiscalPeriod445 = @AttainmentPeriod`.
6. **Target schemas are reduced** to the columns the packages produce (see `src/sales_schemas.py`), not the full
   `Aggregate.*` DDL width; columns the DDL has but no package fills are omitted rather than defaulted.
7. **Static estate check `forbidden-content`.** `validation/static/run_all_checks.py` forbids the words
   Databricks / dbutils / Unity Catalog in every `.py/.md/.yml` outside the legacy sample dirs; `# Databricks notebook source`
   is the mandatory notebook header, so every migration session's output trips it. `validation/` is read-only for this
   session; the carve-out for `databricks/**` and `docs/migration/**` must land via session 00 / the parent session.
8. `IncludePartialPeriod` (quota) is declared but unused by the legacy package; kept as a no-op parameter.
9. No workspace deployment or run was performed (`databricks bundle validate -t dev/-t prod` only).
