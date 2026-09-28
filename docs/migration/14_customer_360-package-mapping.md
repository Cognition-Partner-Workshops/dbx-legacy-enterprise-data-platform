# 14_customer_360 — SSIS `WWI_Customer360` -> Databricks package mapping

Session 14 of the SSIS -> Databricks migration. Source: `ssis/14_customer_360/` (5 packages emitted by
`build_customer360_packages.py`). Target: Databricks Asset Bundle `databricks/14_customer_360/`
(bundle `wwi_14_customer_360`, one job `wwi_14_customer_360`, one notebook per package).

Nothing under `ssis/`, `sqlserver/`, `oracle/`, `validation/` or `databricks/common/` was modified.

## 1. Package -> notebook / job task

| SSIS package (.dtsx) | Notebook | Job task_key | depends_on | Legacy target | Delta target |
|---|---|---|---|---|---|
| `C360_Build_CustomerProfile` | `notebooks/C360_Build_CustomerProfile.py` | `C360_Build_CustomerProfile` | – (upstream dims/facts owned by other sessions) | `Customer360.CustomerProfile` | `gold.c360_customer_profile` |
| `C360_Build_RollingMetrics` | `notebooks/C360_Build_RollingMetrics.py` | `C360_Build_RollingMetrics` | – | `Customer360.CustomerRollingMetric` | `gold.c360_customer_rolling_metric` |
| `C360_Build_LoyaltyOverlay` | `notebooks/C360_Build_LoyaltyOverlay.py` | `C360_Build_LoyaltyOverlay` | – | `Customer360.LoyaltyOverlay` | `gold.c360_loyalty_overlay` |
| `C360_Build_ChurnFlags` | `notebooks/C360_Build_ChurnFlags.py` | `C360_Build_ChurnFlags` | RollingMetrics, LoyaltyOverlay, CustomerProfile | `Customer360.CustomerChurnFlag` | `gold.c360_customer_churn_flag` |
| `C360_Publish_Segments` | `notebooks/C360_Publish_Segments.py` | `C360_Publish_Segments` | CustomerProfile, LoyaltyOverlay, ChurnFlags | `Customer360.CustomerSegment` + `Report.vw_CustomerSegment` | `gold.c360_customer_segment` + view `gold.rpt_customer_segment` |

Dependency edges come from `docs/inventories/package-dependencies.csv`
(`C360_Build_RollingMetrics -> C360_Build_ChurnFlags`, `C360_Build_CustomerProfile -> C360_Publish_Segments`,
`C360_Build_LoyaltyOverlay -> C360_Publish_Segments`) plus the table reads inside the .dtsx
(ChurnFlags reads `work.LoyaltyOverlay` and `work.CustomerSalesSummary`; Publish reads
`Customer360.CustomerChurnFlag`). The three build packages are independent and run in parallel, which
matches the `Customer 360 Build` phase (sequence 93) -> `Customer 360 Publish` phase (sequence 94) in
`ssis/orchestration-plan.json`.

Cross-project edges (`AGG_Refresh_Customer360 -> C360_Publish_Segments`,
`AGG_Refresh_CustomerRolling12Month -> C360_Build_ChurnFlags`, `FACT_* / SLS_* -> C360_Build_*`) are
*not* expressed inside this job — they are job-to-job edges for session 00's master jobs (see §7).

Shared transformation code lives in `src/c360_lib/` (pure DataFrame functions, unit-tested) and the
notebooks only do parameter handling, I/O and control-framework calls:

| Module | Content |
|---|---|
| `src/c360_lib/tables.py` | `LEGACY_TO_DELTA` map + `table(catalog, legacyName)` (wraps `naming.table`) |
| `src/c360_lib/profile.py` | address standardisation, identity graph, sales/payment summaries, profile build, households |
| `src/c360_lib/rolling.py` | rolling window aggregate, APAC 4-4-5 realignment, ratios/activity status, RFM deciles |
| `src/c360_lib/loyalty.py` | point expiry, qualifying sales, accrual, tier ladders, tier movement |
| `src/c360_lib/churn.py` | feature set, churn source join, rule scoring 2019.3, high-risk split, outreach queue |
| `src/c360_lib/segments.py` | segment CASE precedence, suppression filter, migration/movement |
| `src/c360_lib/runtime.py` | `JobContext` (from `params.getJobParams`), widgets, Delta read/write helpers, `packageLifecycle`, row-count logging, `RestartFromStep` skip |

## 2. Source / target objects -> Delta tables

Naming follows the shared contract (snake_case, spaces -> `_`). `Customer360.*` is a mart schema with
no rule in the contract; it is mapped to `gold.c360_*` (see §6, decision needed).

| Legacy object | Delta table | Role |
|---|---|---|
| `Dimension.Customer` | `gold.dim_customer` | read (all 5 packages) |
| `Fact.Sale` | `gold.fact_sale` | read (Profile, Rolling, Loyalty) |
| `Fact.Payment` | `gold.fact_payment` | read (Profile) |
| `Fact.Return` | `gold.fact_return` | mapped; not read (returns come from `Fact.Sale` negative quantity, as in the T-SQL) |
| `Aggregate.Customer 360` | `gold.agg_customer_360` | upstream refresh by `AGG_Refresh_Customer360`; declared dependency only |
| `Aggregate.Customer Rolling 12 Month` | `gold.agg_customer_rolling_12_month` | read (ChurnFlags: `AverageOrderGapDays`, prior basket) |
| `stg.FiscalCalendar445Period` | `silver.stg_fiscal_calendar_445_period` | read (Rolling, APAC realignment) |
| `work.CustomerSalesSummary` | `silver.work_customer_sales_summary` | Profile writes, ChurnFlags reads |
| `work.CustomerPaymentSummary` | `silver.work_customer_payment_summary` | Profile |
| `work.CustomerAddressStandardised` | `silver.work_customer_address_standardised` | Profile |
| `work.CustomerIdentityGraph` | `silver.work_customer_identity_graph` | Profile |
| `work.CustomerProfile` | `silver.work_customer_profile` | Profile |
| `Customer360.CustomerProfile` | `gold.c360_customer_profile` | Profile target |
| `work.CustomerRollingMetric` | `silver.work_customer_rolling_metric` | Rolling |
| `Customer360.CustomerRollingMetric` | `gold.c360_customer_rolling_metric` | Rolling target |
| `work.LoyaltyQualifyingSale` | `silver.work_loyalty_qualifying_sale` | Loyalty |
| `work.LoyaltyPointLedger` | `silver.work_loyalty_point_ledger` | Loyalty (persisted ledger, MERGE/append) |
| `work.LoyaltyOverlayCurrent` | `silver.work_loyalty_overlay_current` | Loyalty (previous tier snapshot) |
| `work.LoyaltyOverlay` | `silver.work_loyalty_overlay` | Loyalty writes, ChurnFlags reads |
| `Customer360.LoyaltyOverlay` | `gold.c360_loyalty_overlay` | Loyalty target |
| `work.ChurnFeatureSet` | `silver.work_churn_feature_set` | ChurnFlags |
| `work.CustomerChurnHighRisk` | `silver.work_customer_churn_high_risk` | ChurnFlags |
| `work.CustomerOutreachQueue` | `silver.work_customer_outreach_queue` | ChurnFlags (append, de-duplicated) |
| `Customer360.CustomerChurnFlag` | `gold.c360_customer_churn_flag` | ChurnFlags target |
| `work.CustomerSegmentPrevious` | `silver.work_customer_segment_previous` | Publish |
| `work.CustomerSegment` | `silver.work_customer_segment` | Publish |
| `Customer360.CustomerSegment` | `gold.c360_customer_segment` | Publish target |
| `Report.vw_CustomerSegment` | `gold.rpt_customer_segment` (view) | Publish (view swap) |
| `etl.*` control tables | `etl.package_execution`, `etl.error_log`, `etl.row_count_log`, `etl.configuration`, … | via `dbx_etl_common` only |

All references are built as `naming.table(catalog, schema, name)` from the `catalog` job parameter —
no catalog, host or date is hard-coded. `SYSDATETIME()` / `GETDATE()` in the T-SQL is replaced by the
`BusinessDate` job parameter so a re-run for a past date reproduces the legacy output.

## 3. SSIS component -> Spark construct (per package)

### C360_Build_CustomerProfile
| SSIS component | Type | Spark implementation |
|---|---|---|
| Log Package Start / End, OnError handler | Execute SQL (`etl.usp_LogPackageStart/End`, `usp_LogError`) | `runtime.packageLifecycle` -> `control.logPackageStart`, `control.logPackageEnd`, `control.logError` |
| Truncate work.CustomerSalesSummary / PaymentSummary / AddressStandardised / IdentityGraph / CustomerProfile | Execute SQL `TRUNCATE` | Delta `mode("overwrite")` of the rebuilt frame (`runtime.overwriteTable`) — same "rebuild every run" semantics, idempotent |
| Build Customer Sales Summary (`INSERT … FROM Fact.Sale GROUP BY`) | Execute SQL | `profile.buildCustomerSalesSummary` (`groupBy` + `countDistinct`, `sum`, `min/max`) |
| Build Customer Payment Summary | Execute SQL | `profile.buildCustomerPaymentSummary` |
| DF Standardise Addresses: OLE DB source (active customers `[Valid To] > SYSDATETIME() AND [WWI Customer ID] > 0`) -> Derived Column (regional postal/email rules) -> OLE DB destination | Data Flow | `profile.activeCustomer(asOfDate)` filter, `profile.buildAddressStandardisationInput`, `profile.standardiseAddresses` (`when/otherwise`, `regexp_replace`, `upper`, `lower`, `trim`) |
| Resolve Identity Graph (self-join MERGE, `CASE` tax=100 / email=95 / name+postal=88, `MAX` score, `MIN` id survivor, `>= @MatchThresholdScore`) | Execute SQL | `profile.resolveIdentityGraph(addr, matchThreshold)` — self-join with `b.CustomerId <= a.CustomerId` (includes self-match exactly like the T-SQL), `max(score)`, `min(CustomerId)`, threshold filter |
| Count Duplicate Clusters (row count into variable, warning if > 0) | Execute SQL + expression | `profile.countDuplicateClusters` + `runtime.logWarning` (`control.logError(errorSeverity="Warning")`) |
| DF Build Profile: source join dim + summaries -> Lookup identity graph -> Derived Column (consent, `WITHHELD`, `MasterCustomerId`, `TenureDays`, `AverageOrderValue`) -> destination work.CustomerProfile | Data Flow | `profile.buildProfileSource`, `profile.buildCustomerProfile` (left join to graph = lookup with no-match rows passed through, `coalesce`, `datediff`, zero-safe division) |
| Assign Households (`DENSE_RANK() OVER (ORDER BY postal, household name)` UPDATE) | Execute SQL | `profile.assignHouseholds` (`Window.orderBy` + `dense_rank`, join back) |
| Publish Customer360.CustomerProfile (`DELETE` + `INSERT`) | Execute SQL | `runtime.overwriteTable` of stamped frame (`BatchId`, `BusinessDate`, `LoadedAtUtc`) |
| Log row counts (`etl.usp_LogRowCount`) | Execute SQL | `runtime.logRowCounts` -> `control.logRowCount` |
| `RebuildIdentityGraph` parameter | package param | widget; when `False` and the graph table already exists for this run it is still rebuilt from the standardised addresses (see §6) |

### C360_Build_RollingMetrics
| SSIS component | Spark implementation |
|---|---|
| Truncate work.CustomerRollingMetric | overwrite |
| Aggregate Rolling Window (`Fact.Sale` last `@WindowMonths` months, `COUNT(DISTINCT invoice)`, net revenue, returns = negative qty, distinct items, last order date) | `rolling.aggregateRollingWindow` |
| Realign APAC Window to latest completed 4-4-5 period (`stg.FiscalCalendar445Period`) | `rolling.latestCompleted445Period` + `rolling.realignApacWindow` (APAC rows only, join on `RegionCode = 'APAC'`) |
| Derive Ratios & Status (avg basket, return rate, `DaysSinceLastOrder` 9999 when null, `INACTIVE` / `FREQUENT` / `ACTIVE`, orders per month) | `rolling.deriveRollingMetrics` (`when` chain in the same order as the T-SQL `CASE`) |
| Assign RFM Deciles (`NTILE(10) OVER (PARTITION BY RegionCode ORDER BY …)`) | `rolling.assignRfmDeciles` (`ntile(10)` over region windows, recency DESC / frequency ASC / monetary ASC) |
| Publish Customer360.CustomerRollingMetric | overwrite + `logRowCount` |

### C360_Build_LoyaltyOverlay
| SSIS component | Spark implementation |
|---|---|
| Expire Aged Points (`UPDATE ledger SET Status='EXPIRED'` older than regional months) | `loyalty.expireAgedPoints` (row-level `when` on `add_months`) then overwrite ledger |
| DF Qualifying Sales: source `Fact.Sale` join dim -> Derived Column (regional accrual amount / points) -> Lookup existing ledger invoices (no-match output only) -> destination ledger | `loyalty.buildLoyaltyQualifyingSale` + `loyalty.accruePoints` (`left_anti` join on `SourceInvoiceId` = lookup no-match, `floor`) then append |
| Recalculate Tier Ladders (regional thresholds, previous tier from work.LoyaltyOverlayCurrent) | `loyalty.tierCode`, `loyalty.recalculateTierLadders` |
| Measure Tier Movement (`SAME` / `NEW` / `CHANGED`) | `loyalty.deriveTierMovement` |
| Refresh work.LoyaltyOverlayCurrent, publish Customer360.LoyaltyOverlay | overwrite both + `logRowCount` |

### C360_Build_ChurnFlags
| SSIS component | Spark implementation |
|---|---|
| Build Churn Feature Set (rolling metric + dim credit hold + sales summary + `Aggregate.Customer Rolling 12 Month`) | `churn.buildChurnFeatureSet` |
| DF Score Churn: source feature set join work.LoyaltyOverlay -> Derived Column (5 rule scores, `ChurnScore`, `RiskBand`, `RuleSetVersion='2019.3'`) -> Conditional Split (`RiskBand == 'HIGH'`) -> destinations Customer360.CustomerChurnFlag + work.CustomerChurnHighRisk | `churn.buildChurnSource`, `churn.scoreChurnRisk(highRiskThreshold, orderGapMultiplier)`, `churn.splitHighRisk` |
| Queue High-Risk Outreach (`INSERT … WHERE NOT EXISTS`) | `churn.newOutreachQueueRows` (`left_anti` join) then append |
| Row counts | `logRowCount` |

### C360_Publish_Segments
| SSIS component | Spark implementation |
|---|---|
| Snapshot Previous Segments (`INSERT work.CustomerSegmentPrevious SELECT FROM work.CustomerSegment`; then truncate work.CustomerSegment) | overwrite `work_customer_segment_previous` from current `work_customer_segment` (or empty frame on first run) |
| DF Assign Segments: 4-way join -> Derived Column (8-branch CASE precedence: SUPPRESSED, AT_RISK_HIGH_VALUE, AT_RISK, CHAMPION, NEW_PROMISING, LOYAL_PREMIUM, DORMANT, CORE) -> Conditional Split on `@PublishSuppressedRows` -> destination work.CustomerSegment | `segments.assignSegments(segmentModelVersion)`, `segments.dropSuppressedRows(publishSuppressedRows)` |
| Measure Segment Migration (`NEW` / `SAME` / `MOVED`) | `segments.deriveSegmentMovement` |
| Publish (`BEGIN TRAN; DELETE Customer360.CustomerSegment; INSERT …; ALTER VIEW Report.vw_CustomerSegment; COMMIT`) | Delta overwrite of `gold.c360_customer_segment` (single atomic Delta commit) + `CREATE OR REPLACE VIEW gold.rpt_customer_segment AS SELECT * FROM <table>` — readers of the view never see a half-published set, which is the legacy table/view-swap guarantee |
| Row counts | `logRowCount` |

## 4. Control framework calls (`dbx_etl_common`)

Every notebook does `from dbx_etl_common import control, params, naming` via `c360_lib.runtime` /
`c360_lib.tables` and uses only the contract API:

| Legacy | Databricks |
|---|---|
| `etl.usp_LogPackageStart` | `control.logPackageStart(spark, catalog, batchId, packageName, projectName="WWI_Customer360", stepName=...)` |
| `etl.usp_LogPackageEnd` | `control.logPackageEnd(spark, catalog, packageExecutionId, status, rowsRead=..., rowsInserted=..., ...)` |
| `OnError` -> `etl.usp_LogError` | `control.logError(spark, catalog, packageExecutionId=..., batchId=..., errorSeverity="Error", sourceName=packageName, errorDescription=...)` then re-raise |
| Duplicate-cluster warning | `control.logError(..., errorSeverity="Warning", sourceComponent="Count Duplicate Clusters")` |
| `etl.usp_LogRowCount` | `control.logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount, targetRowCount, insertRowCount, ...)` |
| `etl.usp_GetConfiguration` | `control.getConfiguration` is available through `runtime`; the C360 packages carry their thresholds as package parameters, so no configuration keys are read |
| job parameters | `params.getJobParams(dbutils)` -> `batchId`, `businessDate`, `reloadFullHistory`, `environmentCode`, `restartFromStep`, `catalog` |
| table names | `naming.table(catalog, schema, name)` |

Batch/step lifecycle (`startBatch`/`endBatch`/`startBatchStep`) is *not* called here: in the legacy
estate the master packages own the batch and pass `BatchId` down; session 00's master job does the same.

**Wheel**: each task lists `libraries: - whl: `../../common/dbx_etl_common/dist/*.whl`` (session 00's build
output, uploaded by `bundle deploy`). Notebooks add `../src` to `sys.path` for `c360_lib` only — the shared package is
never copied.

Session 00's PR (`[dbx-migration 00]`) did not exist when this PR was opened, so the boilerplate follows the
interface contract in the assignment; re-check imports when 00 lands.

## 5. Parameters

Job parameters (all strings, contract): `BatchId="0"`, `BusinessDate=${var.businessDate}`,
`ReloadFullHistory="False"`, `EnvironmentCode=${var.environmentCode}`, `RestartFromStep=""`,
`catalog=${var.catalog}`.

Package parameters carried over as task `base_parameters` (same defaults as the .dtsx):

| Package | Parameter | Default |
|---|---|---|
| CustomerProfile | `MatchThresholdScore` | `85` |
| CustomerProfile | `RebuildIdentityGraph` | `False` |
| RollingMetrics | `WindowMonths` | `12` |
| RollingMetrics | `InactiveThresholdDays` | `270` |
| LoyaltyOverlay | `PointsExpiryMonthsNa` / `Eu` / `Apac` | `24` / `36` / `18` |
| ChurnFlags | `HighRiskScoreThreshold` | `60` |
| ChurnFlags | `OrderGapMultiplier` | `2` |
| Publish_Segments | `SegmentModelVersion` | `SEG-2021A` |
| Publish_Segments | `PublishSuppressedRows` | `True` |

`SourceSystemCode=WWIOLTP` is present in every package but unused by their SQL; it is not surfaced.
`RestartFromStep`: when set to one of the five package names, notebooks earlier in the legacy order
(Profile, Rolling, Loyalty, Churn, Publish) exit immediately (`runtime.shouldSkipForRestart`), mirroring
the `Invoke-EstateOrchestration.ps1` restart semantics.

Bundle variables: `catalog` (default `wwi_${bundle.target}`; dev=`wwi_dev`, prod=`wwi_prod`),
`warehouse_id`, `businessDate`, `environmentCode`, `sparkVersion`, `nodeType`.
Targets: `dev` (mode development, default) and `prod` (mode production).

## 6. Not migrated / needs decision

| Item | Status / why |
|---|---|
| `Customer360.*` schema name | No contract rule for the mart schema. Mapped to `gold.c360_*`. **Decision:** keep `gold.c360_*` or create a dedicated `customer360` schema? Session 00's master job and `Report.*` consumers must agree. |
| `Report.vw_CustomerSegment` | Migrated as a `CREATE OR REPLACE VIEW gold.rpt_customer_segment` over the published table (view/table swap). If downstream expects the legacy column order/renames from the SQL Server view definition (not in the repo), the view SQL needs that projection. |
| `gold.agg_customer_360` | **Not produced here.** It is refreshed by `AGG_Refresh_Customer360` (`Integration.usp_RefreshAggregateCustomer360`, project 09/10 scope). The C360 packages only *depend* on it (inventory edge). The aggregate churn score (100/60/40/20 + 15/10/15, bands ≥70 HIGH / ≥40 MED) is a different rule set from the C360 `2019.3` rules (30/20/15/15/25, ≥60 HIGH / ≥30 MEDIUM) and is intentionally not merged. |
| Identity graph self-match | The T-SQL self-join (`b.CustomerId <= a.CustomerId`) matches every customer with itself (email 95 / name+postal 88), so every active customer with an email or name+postal appears in the graph with itself as survivor. Reproduced exactly; tests document it. |
| `RebuildIdentityGraph` | Legacy `MERGE work.CustomerIdentityGraph` upserts (no delete clause), so the graph is a persisted table and the flag is not referenced by any SQL. Databricks: `MERGE INTO silver.work_customer_identity_graph` (same UPDATE/INSERT, threshold applied) by default; `RebuildIdentityGraph=True` or `ReloadFullHistory=True` (or a missing table) replaces the whole table instead. |
| `work.LoyaltyPointLedger` persistence | Legacy ledger is a persisted work table that accrues across runs. Kept as a Delta table (`silver.work_loyalty_point_ledger`): each run reads it, applies expiry and unions the new accruals, then overwrites it in one commit; never truncated. Re-running the same `BusinessDate` is idempotent because accrual excludes invoices already in the ledger. |
| `work.CustomerOutreachQueue` | Same: persisted queue, append with `WHERE NOT EXISTS` semantics (`left_anti`). Never truncated. |
| `BusinessDate` vs `SYSDATETIME()` | Legacy uses the wall clock; Databricks uses the `BusinessDate` parameter (`1900-01-01` default must be overridden by the master job). Timestamps (`*AtUtc`) still use `current_timestamp()`. |
| Transactions | `BEGIN TRAN … COMMIT` around DELETE+INSERT+ALTER VIEW has no multi-statement equivalent; a single Delta overwrite commit + view replace gives the same reader-visible guarantee. |
| Work-table DDL | The repo has no `CREATE TABLE` for the C360 work/mart tables; schemas are inferred from the generator SQL. Delta tables are created on first write (`overwriteSchema`). |
| `Fact.Return` | Mapped but unused: the T-SQL derives returns from `Fact.Sale` rows with negative quantity. |
| Job cluster | `Standard_DS3_v2` / DBR 15.4 LTS placeholders; adjust per workspace (or switch to serverless once the wheel install path is agreed). |

## 7. `Master_Customer_Sync` cadence notes (for session 00)

From `ssis/orchestration-plan.json` / `docs/dependency-maps/etl-dependency-map.md`:

- `Master_Daily_ETL` runs phase **Customer 360 Build** (sequence 93: the three `C360_Build_*` + `C360_Build_ChurnFlags`)
  after the sales / inventory / procurement mart phases, then **Customer 360 Publish** (sequence 94:
  `C360_Publish_Segments`) before reporting publication / reconciliation. -> master job task
  `wwi_14_customer_360` depends on the fact / aggregate jobs (`AGG_Refresh_CustomerRolling12Month`,
  `AGG_Refresh_Customer360`, `FACT_*_Load_Sale`, `FACT_Load_Payment`, `FACT_Load_LoyaltyPoints`).
- `Master_Customer_Sync` runs **nightly ahead of the main nightly load**: customer support dimensions
  (`DIM_Load_Customer` etc.) -> Customer 360 build -> publish, so that `Dimension.Customer` and the C360
  mart see the same ERP snapshot. In Databricks this is the same job `wwi_14_customer_360` triggered by
  the `Master_Customer_Sync` master job after the dim jobs, with the same `BatchId` / `BusinessDate`.
  It is safe to run twice per night (Customer_Sync then Daily_ETL): every table is rebuilt for the
  `BusinessDate`, the ledger/outreach appends are de-duplicated.
- `Master_Month_End` runs `AGG_Refresh_Customer360` -> customer mart refresh -> `C360_Publish_Segments`
  (publish only, using the already-built C360 tables). Use `RestartFromStep=C360_Publish_Segments` on
  this job to run just the publish task.
- Run-frequency parameters to carry from the masters: `BatchId` (from `control.startBatch` in the
  master), `BusinessDate` (yyyy-MM-dd), `EnvironmentCode`, `catalog`.

## 8. Open questions

1. Schema for `Customer360.*` (see §6) — `gold.c360_*` assumed.
3. Does anything downstream read `Report.vw_CustomerSegment` with a specific column list? The legacy view DDL is not in the repo.
4. Should the SQL Server baseline for reconciliation be captured per `BatchId` or per `BusinessDate`? The notebook supports both.

## 9. Validation and deployment

```bash
cd databricks/14_customer_360
pip install pyspark==3.5.3 delta-spark==3.2.1 pytest
python3 -m pytest tests -q                         # 21 tests, local Spark, test-only fake dbx_etl_common under tests/fakes
python3 -m py_compile notebooks/*.py validation/*.py src/c360_lib/*.py
databricks bundle validate --strict -t dev         # also -t prod
cd ../.. && python3 validation/static/run_all_checks.py && python3 validation/checks/run_deep_checks.py
```

Deploy (not done from the migration session): `databricks bundle deploy -t dev` then
`databricks bundle run wwi_14_customer_360 -t dev --params BatchId=<id>,BusinessDate=<yyyy-MM-dd>`.

Reconciliation: `validation/C360_Reconciliation.py` computes, for the five `Customer360.*` targets, row
counts per `BatchId`/`BusinessDate` and `sum(xxhash64(concat_ws('|', <ordered columns>)))`, compares them
with a baseline supplied as a Delta table (`BaselineTable`) or JSON (`BaselineJson`), writes each result with
`control.logRowCount` and finishes with `control.assertRowCountReconciliation`. The notebook header contains
the SQL Server queries (ported from `validation/runtime/02_row_count_reconciliation.sql`) to capture the
baseline figures.
