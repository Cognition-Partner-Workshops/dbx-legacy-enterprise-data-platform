# Child-session backlog

The Databricks migration split into seven child sessions a parent
orchestration session dispatches. Each session is independently executable
against this repository plus the contracts published by the sessions it
depends on. The design every session implements is
`docs/migration/databricks-migration-plan.md` (referenced below as "the
plan", with section numbers); the package-level index is
`docs/migration/package-mapping-appendix.md` (the "appendix").

This estate has never run, so acceptance is against the checked-in
definitions and unit/contract tests, plus parity in the parallel run
(Session G). No session deploys to a workspace until the Wave 0 decisions
(plan section 2, D1-D9) are confirmed; until then every deliverable is code,
bundle configuration and tests that run locally (`pytest` with a local Spark
session / `delta-spark`) and in CI.

## 1. Parent orchestration contract

What every child session receives, what it must hand back, and how the parent
sequences them.

### 1.1 Dispatch order

| Wave | Sessions | Starts when |
| --- | --- | --- |
| 0 | parent only | D1-D9 confirmed by the user; `{catalog}` schemas and volumes created (or agreed placeholders kept) |
| 1 | A, C-part-1 (rules module) | immediately; they are libraries |
| 2 | B, C-part-2 | A's `wwi_control` API and `ctl` schema merged; B publishes the bronze schema contract first thing so C can code against it |
| 3 | D | C's `stg`/`work`/`ref` schema contract merged |
| 4 | E | D's `dim`/`fact`/`agg` contract merged |
| 5 | F | may start in wave 1 from the plan JSON; wires notebooks as B-E land them; complete when all nine jobs dry-run in TEST |
| 6 | G | static coverage from wave 1; runtime reconciliation notebooks from wave 3; parallel run after wave 5 |

### 1.2 Common inputs (every session)

- `docs/migration/databricks-migration-plan.md`, `docs/migration/package-mapping-appendix.md`
- `docs/inventories/ssis-packages.csv`, `docs/inventories/source-target-map.csv`, `docs/inventories/package-dependencies.csv`, `docs/inventories/sql-objects.csv`
- `ssis/orchestration-plan.json`, `docs/dependency-maps/etl-dependency-map.md`
- `docs/domain-model/business-domains.md`, `docs/known-unvalidated-items.md`, `docs/runbooks/execution.md`
- `config/estate-catalog.yaml` (object contract), `config/landing-zone.yaml`
- The decisions table (plan section 2) with the parent's confirmed values

### 1.3 Repository layout for deliverables

```
databricks/
  bundle/                    databricks.yml + resources/*.yml   (F owns; others add resources for their tasks)
  lib/wwi_control/           A
  lib/wwi_rules/             C
  notebooks/ingest/          B
  notebooks/stage/ quality/ reference/ ops/   C
  notebooks/dim/ fact/ agg/  D
  notebooks/mart/<domain>/   E
  notebooks/workflows/       F (start_batch, retry_driver, condition tasks, run_package)
  sql/ddl/<schema>/          owner of the schema (A: ctl, B: bronze, C: stg/work/err/ref, D: dim/fact/agg/report, E: mart_*)
  seeds/                     A (ctl seeds), C (ref seeds)
  tests/unit/<session>/      each session
  tests/contract/            each session publishes its schema contract tests here
validation/migration/        G
docs/migration/              the plan, this backlog, the appendix, per-session handoff notes
```

### 1.4 Handoff contract (what each session returns to the parent)

Each child session ends with a PR against `main` and a handoff note at
`docs/migration/handoff/session-<letter>.md` containing:

1. **Contract published**: table DDL paths and module API signatures the next wave depends on, with the contract test file that pins them.
2. **Coverage**: the appendix rows the session implemented (package names) and any it could not, with the reason.
3. **Decisions consumed / raised**: which D-items were used and any new decision for the user, written as "Decision needed: … / working assumption: …".
4. **Hazards touched**: which H-items were implemented and how (replicate / fix / defer).
5. **Tests**: the command(s) to run them and the last result.
6. **Open items**: for the parent to route to another session.

The parent merges the PR, updates the appendix if package coverage changed
(`python3 tools/migration/build_package_mapping.py`), and dispatches the next
wave with the handoff notes as additional inputs.

### 1.5 Definition of done (every session)

- All acceptance criteria below met and demonstrated by tests that run in CI without a workspace.
- `python3 validation/static/run_all_checks.py --path docs --path databricks` passes (the docs conventions apply to handoff notes).
- No region literal outside `wwi_rules` (lint rule from C).
- No invented cloud/catalog/path/credential values: `{catalog}`, `{volume_root}` placeholders or bundle variables only.
- Handoff note committed.

---

## 2. Session A - Control framework

**Scope.** Port `sqlserver/control/` (tables, seeds, `etl.usp_*` semantics,
watermarks, batches, steps, retries, package/task execution, reconciliation,
DQ result tables, file register, notifications, purge) to the `ctl` Delta
schema and the `wwi_control` Python module described in plan section 5. Parent
creates the batch identity; children update statuses; one control record per
run (fixes H2).

**Inputs.**
- `sqlserver/control/01_schemas.sql` … `07_tables_operations.sql`
- `sqlserver/control/procedures/etl.usp_*.sql` (all), `sqlserver/control/views/`
- `sqlserver/agent/19_job_WWI_Reject_Reprocess.sql`, `20_job_WWI_Control_History_Purge.sql`, `21_job_WWI_Health_Check.sql` (the control-plane Agent jobs)
- `ssis/15_error_handling/build_error_handling_packages.py` (the retry query and backoff)
- `validation/runtime/01_control_framework_health.sql`
- Plan sections 5, 6.1 (task-value contract), 9 (H2, H8, H10)

**Outputs.**
- `databricks/sql/ddl/ctl/*.sql` - one file per table in plan 5.1, CDF enabled
- `databricks/seeds/ctl/*.csv|sql` - `source_system`, `configuration`, `reconciliation_exemption`, `dq_rule` seeds ported from `03_seed_control_data.sql` and `05_seed_data_quality_rules.sql` (with `UnknownMemberKey` corrected per D14/H10)
- `databricks/lib/wwi_control/` - the API in plan 5.2, packaged as a wheel; SQL function wrappers for the pure-SQL tasks
- `databricks/notebooks/ops/health_check`, `ops/purge_control_history`
- `tests/unit/a/` - one test module per procedure semantic; `tests/contract/ctl_schema_test.py`
- `docs/migration/handoff/session-a.md`

**Dependencies.** None (wave 1). Publishes the contract B-G import.

**Suggested prompt.**

> Implement Session A of `docs/migration/child-session-backlog.md` in
> `Cognition-Partner-Workshops/dbx-legacy-enterprise-data-platform`. Read plan
> section 5 first. Port every table in `sqlserver/control/` to Delta DDL under
> `databricks/sql/ddl/ctl/` and every `etl.usp_*` procedure's semantics into
> the `wwi_control` Python module (API in plan 5.2), preserving:
> single-running-batch per (batch_name, business_date) with adoption; parent
> creates the batch and children only claim steps and record task executions;
> `RestartFromStep` skip semantics; attempt counters with `MaxExtractAttempts`;
> watermark `[from, to)` windows with lookback, lock, previous value and
> refused rewind; row-count audit and reconciliation with tolerances and
> exemptions; structured error/reject JSON payloads; retention purge that never
> touches watermarks. Write pytest unit tests with a local Spark + Delta session
> for each semantic (including concurrency: two start_batch calls for the same
> key). Do not connect to any workspace. Finish with
> `docs/migration/handoff/session-a.md` per backlog section 1.4 and a PR.

**Acceptance criteria.**
1. Every `etl` table and every `etl.usp_*` procedure in `docs/inventories/sql-objects.csv` maps to a `ctl` table or a `wwi_control` function (a coverage test reads the inventory).
2. `start_batch` twice for the same key: second call raises unless `allow_adopt_running`; adoption re-uses the id and appends the note.
3. `restart_from_step="Facts"` marks steps with lower sequence `Skipped` and runs the rest; a run without the parameter runs all.
4. Watermark: first call creates the row from `WatermarkEpoch`; lookback applied; lock held for the task; `set_watermark` with an earlier value is refused and logged `Warning`; `allow_rewind` succeeds and records history.
5. `retryable_steps` returns exactly the rows the `ERR_Retry_FailedSteps` query would (`Failed`, `is_retryable`, `attempt_number < max`).
6. `end_batch` derives `Succeeded` / `Failed` / `SucceededWithWarnings` as `etl.usp_EndBatch` does, and honours `force_status`.
7. Reconciliation honours `ReconAbsoluteTolerance`, `ReconPercentTolerance` and the ten seeded exemptions.
8. Health-check and purge notebooks port the three Agent jobs' SQL to `ctl` and are covered by tests on fixture data.

**Tests.** `pytest tests/unit/a tests/contract/ctl_schema_test.py`; fixture-based tests for each semantic above; a property test that any sequence of `claim_step`/`complete_step` calls is idempotent.

**Artifacts to commit/attach.** DDL, seeds, wheel source, notebooks, tests, handoff note, PR. Attach the coverage test output to the handoff.

---

## 3. Session B - Ingestion

**Scope.** All packages in `ssis/01_oracle_extract`, `ssis/02_sqlserver_extract`,
`ssis/03_file_ingestion` (22 + 22 + 7, appendix A1-A3) as table-driven ingest
tasks: JDBC (or CDC where D3/D4 choose it) into `bronze`, Auto Loader for the
six feeds plus quarantine, `ctl.extract_definition` and `ctl.file_register`,
ingest metadata, watermarks, bounded retry, delete detection, archive and
quarantine handling, missing-file gate. Plan section 4.

**Inputs.**
- `ssis/01_oracle_extract/generate_oracle_extracts.py`, `ssis/02_sqlserver_extract/generate_sqlserver_extracts.py`, `ssis/03_file_ingestion/generate_file_ingestion.py` (source SQL, watermark bindings, column metadata, encodings)
- `tools/ssisgen/patterns.py` (the eight-step package skeleton)
- `config/landing-zone.yaml`, `oracle/packages/WWI_AUDIT.PKG_EXTRACT_CONTROL*.sql`, `oracle/tables/WWI_AUDIT.CHANGE_LOG.sql`, `oracle/views/`, `sqlserver/oltp/**/Integration.vw_*` extract views
- `sqlserver/control/03_seed_control_data.sql` (`etl.Watermark` seeds, `etl.SourceSystem`)
- `ssis/15_error_handling/build_error_handling_packages.py` (`ERR_Quarantine_BadFiles`, `ERR_Retry_FailedSteps`), `ssis/99_maintenance/*` (`MNT_Archive_ProcessedFiles`)
- Session A handoff (`wwi_control`)
- Plan sections 4, 5.4, 9 (H5, H6)

**Outputs.**
- `databricks/sql/ddl/bronze/*.sql` - 34 bronze tables (plus `_deletes` tables) with the `_ingest` metadata columns; `ctl.extract_definition`, `ctl.file_register` DDL (agreed with A)
- `databricks/seeds/ctl/extract_definition.csv` - one row per extract package, generated from the spec modules by a script committed under `tools/migration/`
- `databricks/notebooks/ingest/oracle/run_extract`, `ingest/sqlserver/run_extract`, `ingest/files/run_feed`, `ingest/files/screen`, `ingest/files/quarantine`, `ingest/files/archive`, `ingest/files/missing_file_gate`
- `tests/unit/b/`, `tests/contract/bronze_schema_test.py`
- `docs/migration/handoff/session-b.md`, including the confirmation of the H5 lineage items

**Dependencies.** A (wave 2). Publishes the bronze contract C consumes.

**Suggested prompt.**

> Implement Session B of `docs/migration/child-session-backlog.md`. Read plan
> section 4 and appendix A1-A3. Generate `ctl.extract_definition` rows for all
> 51 extract and file packages from the three generator spec modules (do not
> hand-copy SQL); build one generic JDBC extract notebook and one Auto Loader
> feed notebook that implement the eight-step package skeleton with
> `wwi_control`. Preserve the four watermark strategies exactly (lookback,
> source-MAX-first for key incrementals, re-runnable date windows, full
> reload), delete detection from `WWI_AUDIT.CHANGE_LOG` and `CHANGETABLE`,
> and every file-format quirk in `config/landing-zone.yaml` (encodings,
> footers, checksum, no-header APAC). Bronze writes must be idempotent per
> (batch_id, window). Implement the file register, structural screen,
> quarantine after 2 attempts, archive layout and the missing-file gate.
> Confirm the three lineage items in appendix A9 against the `.dtsx` files
> and record the answer in the handoff. Use placeholders for hosts, paths and
> secrets. Tests run locally against fixture files and an in-memory JDBC
> source (SQLite/H2 is acceptable for the generic reader). PR + handoff note.

**Acceptance criteria.**
1. Every package in appendix A1-A3 has an `extract_definition` row and a bronze table; the coverage test asserts it against `ssis-packages.csv`.
2. For each load type, a fixture test shows: window computed as plan 4.2; re-running the same window leaves bronze unchanged; failure leaves the watermark unchanged.
3. The six feed fixtures (one good, one malformed each) land, screen, reject and quarantine as `config/landing-zone.yaml` describes; the APAC file is transcoded from ISO-8859-1; footers and checksum are validated.
4. `ctl.row_count_audit` gets a row per task with source vs landed counts.
5. Delete rows land in `bronze.<table>_deletes` for the objects that record them.
6. H5 lineage items confirmed or corrected in the handoff and in `tools/migration/build_package_mapping.py` `LINEAGE_FLAGS`.
7. No code path writes `stg` or beyond.

**Tests.** `pytest tests/unit/b tests/contract/bronze_schema_test.py`; fixture files under `tests/fixtures/landing/`.

**Artifacts.** DDL, seeds + generator script, notebooks, tests, fixtures, handoff note, PR.

---

## 4. Session C - Staging, DQ, reference and the regional rules module

**Scope.** Two parts. **Part 1 (wave 1):** the `wwi_rules` regional module
and `ref` seeds (plan section 8), because B, D and E call it. **Part 2 (wave
2):** `ssis/04_staging` (28 packages incl. the four work tables and the
generator column contract), `ssis/05_data_quality` (10 screens),
`ssis/06_reference_data` (14 loads), and the reject/quarantine/reprocess flows
in `ssis/15_error_handling` (`ERR_Route_RejectedRows`, `DQ_Reject_Reprocess`,
`ERR_Reconcile_RowCounts` wrapper). Plan sections 7.1-7.4, 8, 9 (H1 replay
side, H3).

**Inputs.**
- `ssis/04_staging/build_staging_packages.py`, `ssis/05_data_quality/*`, `ssis/06_reference_data/*`, `ssis/15_error_handling/build_error_handling_packages.py`
- `sqlserver/staging/**` (`stg.*`, `work.*`, `err.*` tables), `sqlserver/staging/procedures/stg.usp_*.sql` (esp. `stg.usp_DeduplicateCustomer`), `sqlserver/reference/*.sql`
- `sqlserver/control/04_tables_data_quality.sql`, `05_seed_data_quality_rules.sql`
- `oracle/packages/WWI_FIN.PKG_TAX*.sql`, `WWI_REF.PKG_FX*.sql`, `WWI_REF.PKG_CODE_TRANSLATION*.sql`, `oracle/tables/WWI_FIN.TAX_RATE.sql`, `WWI_REF.FX_RATE_DAILY.sql`, `WWI_REF.CODE_TRANSLATION.sql`
- Regional logic embedded elsewhere, to be extracted not re-implemented per call site: `sqlserver/procedures/facts/Integration.usp_RefreshAggregateCustomer360.sql` (privacy), `Integration.usp_LoadFactPayment.sql` (instruments), `Integration.usp_RefreshAggregateMarginAnalysis.sql` (cost basis), `ssis/10_finance` `FIN_Close_PeriodLock` (calendars), the regional `DIM_*`/`FACT_*`/`ING_FILE_*` packages' derived columns
- `validation/runtime/04_regional_divergence.sql`
- Session A and B handoffs
- Plan sections 7.1-7.4, 8, 9

**Outputs.**
- Part 1: `databricks/lib/wwi_rules/` (interfaces in plan 8.2), `databricks/sql/ddl/ref/*.sql`, `databricks/seeds/ref/*` (effective-dated, `rule_set_version` 1), the "no region literal outside wwi_rules" lint, `tests/unit/c/rules/`
- Part 2: `databricks/sql/ddl/stg|work|err/*.sql` generated from `build_staging_packages.py` column declarations by a committed script; `databricks/notebooks/stage/*` (24 loads), `stage/work/customer_dedup|product_crosswalk|payment_match|inventory_position`, `quality/*` (10 screens incl. `referential_screen` used twice), `reference/*` (14), `ops/route_rejected_rows`, `ops/reject_reprocess`
- `tests/unit/c/`, `tests/contract/stg_work_ref_schema_test.py`, `tests/contract/rules_api_test.py`
- `docs/migration/handoff/session-c.md`

**Dependencies.** Part 1 none; Part 2 needs A and B's bronze contract.

**Suggested prompt (part 1).**

> Implement Session C part 1 of `docs/migration/child-session-backlog.md`:
> the `wwi_rules` regional rules module and `ref` seeds per plan section 8.
> Extract every NA/EU/APAC rule from the Oracle packages (`PKG_TAX`, `PKG_FX`,
> `PKG_CODE_TRANSLATION`), the staging procedures, the regional SSIS derived
> columns and the warehouse procedures listed in the backlog inputs into
> effective-dated configuration tables and the interfaces `tax`, `fx`,
> `fiscal`, `address`, `consent`, `retention`, `codes`, `cost_basis`,
> `payments`, `dedup.threshold`. Precedence: override -> region -> GLOBAL ->
> configured on_unmapped action; no implicit NA default - unknown region is a
> `REGION_UNKNOWN` reject. Port `SuppressConsentWithdrawn` and
> `DedupeThresholdScore` (85) as parameters. Add a lint that fails on region
> literals outside the module. Unit tests per interface per region plus
> effective-date boundaries and a 4-4-5 period edge. PR + handoff.

**Suggested prompt (part 2).**

> Implement Session C part 2: `ssis/04_staging`, `05_data_quality`,
> `06_reference_data` and the reject flows of `15_error_handling` per plan
> sections 7.1-7.4 and appendix A4/A7. Generate the `stg`/`work`/`err` DDL and
> PySpark schemas from `build_staging_packages.py`; implement the 24 stage
> loads as MERGE or INSERT OVERWRITE per their legacy load type; the four work
> tables as batch-partitioned notebooks following the `stg.usp_*`
> specifications (customer dedup: three rules, survivorship scoring, EU
> consent never loses, threshold rejects); the ten DQ screens on
> `ctl.dq_rule`; `referential_screen` scoped to the batch and run twice with
> no back-edge; reject routing to `err.*` + `ctl.rejected_record`;
> `reject_reprocess` with five attempts over thirty days that refuses to run
> while a Daily batch is Running (H1); code translation with `on_unmapped`
> per H3. All transforms are pure functions with unit tests. PR + handoff.

**Acceptance criteria.**
1. Every package in appendix A4 and the four `ERR_*`/`DQ_Reject_*` rows of A7 has a notebook; coverage test.
2. `wwi_rules` unit tests: for each of tax, fx, fiscal, address, consent, retention, codes - one passing case per region, one effective-date boundary, one unknown-region reject. The 04_regional_divergence assertions, expressed as tests on fixture data, pass.
3. Customer dedup fixture reproduces the `stg.usp_DeduplicateCustomer` comment examples: tax-number match beats name match; source rank order; EU opt-in survives; below-threshold pair becomes a `DUP_CANDIDATE` reject; result identical on re-run.
4. Referential screen + route + rescreen on a fixture with three reject classes yields the same reject set as a single pass plus the routed rows, and running the rescreen a third time changes nothing.
5. `MaxRejectPercent` fails the task when exceeded.
6. `reject_reprocess` stands down when `ctl.batch` has a running Daily batch (test).
7. Lint rule passes over `databricks/`.

**Tests.** `pytest tests/unit/c tests/contract/rules_api_test.py tests/contract/stg_work_ref_schema_test.py`; `python3 tools/migration/lint_region_literals.py databricks/`.

**Artifacts.** Module, DDL generator + DDL, seeds, notebooks, lint, tests, handoff note, PR (two PRs, one per part).

---

## 5. Session D - Dimensions, facts and aggregates

**Scope.** `ssis/07_dimensions` (15 packages: 11 SCD2, 3 SCD1,
`DIM_Rekey_LateArriving`), `ssis/08_facts` (23: 17 incremental, 5 snapshot,
`FACT_Apply_Corrections`, `FACT_Dedup_Sale`), `ssis/09_aggregates` (12
refreshes + `AGG_Publish_ReportingLayer`), and the `Integration.usp_*`
procedures they wrap (37 dimension, 33 fact/aggregate). Surrogate keys,
unknown and inferred members, the regional customer dimension, late-arriving
rekey and fact hold, correction styles, accumulating snapshots, rebuild-in-full
`Fact.Sales Margin`, aggregate refresh, the 16 `report` views and
`report.publish_state`. Plan sections 7.5-7.7, 10.

**Inputs.**
- `ssis/07_dimensions/*`, `ssis/08_facts/*`, `ssis/09_aggregates/*` (package specs and the procedures each calls)
- `sqlserver/warehouse/dimensions/*.sql` (incl. `01_dimension_key_registry.sql`, `90_unknown_members.sql`, `91_date_role_playing_views.sql`), `sqlserver/warehouse/facts/*.sql`, `sqlserver/warehouse/aggregates/*.sql`
- `sqlserver/procedures/dimensions/Integration.usp_*.sql`, `sqlserver/procedures/facts/Integration.usp_*.sql` (all; the header comments are the specifications, especially `usp_MigrateStagedCustomerDataV2`, `usp_LoadFactSale`, `usp_LoadFactPayment`, `usp_LoadFactShipment`, `usp_LoadFactOrderFulfilment`, `usp_RefreshAggregateMarginAnalysis`, `usp_RefreshAggregateCustomer360`, `usp_RekeyLateArrivingDimensions`, `usp_ApplyFactCorrections`, `usp_DeduplicateFactSale`, `usp_PublishReportingLayer`)
- `sqlserver/views/Report.vw_*.sql`, `power-bi-dashboards/README.md`
- `validation/runtime/03_dimension_fact_integrity.sql`
- Sessions A and C handoffs
- Plan sections 7.2, 7.5-7.7, 9 (H4 bridge, H10, H11), 10

**Outputs.**
- `databricks/sql/ddl/dim|fact|agg|report/*.sql` with the snake_case names and a generated `docs/migration/column-alias-map.csv` (legacy bracketed name -> target column) used by the `report` views
- `databricks/notebooks/dim/*` (one per dimension package + `ensure_unknown_members`, `allocate_key_range`, `insert_inferred_member`, `rekey_late_arriving`), `fact/*` (one per fact package + `apply_corrections`, `dedup_sale`, `fact_load_hold`), `agg/*` (12 SQL notebooks + `publish_reporting_layer`)
- `databricks/sql/ddl/report/vw_*.sql` - 16 views aliasing to legacy names and filtering on `report.publish_state`
- `tests/unit/d/`, `tests/contract/dim_fact_agg_schema_test.py`
- `docs/migration/handoff/session-d.md`

**Dependencies.** A, C (wave 3). Publishes the contract E consumes and the views G validates.

**Suggested prompt.**

> Implement Session D of `docs/migration/child-session-backlog.md` per plan
> sections 7.5-7.7 and 10, appendix A5. Treat each `Integration.usp_*` header
> comment as the specification and re-implement it as a notebook with pure
> transform functions. Preserve: key-range allocation with stable BIGINT keys
> and reserved -1/-2 members; SCD2 expire-and-insert MERGE with the
> Type-2/Type-1 attribute split and same-day `effective_sequence` from
> `usp_MigrateStagedCustomerDataV2`; one regional customer notebook
> parameterised by region using `wwi_rules`; inferred members, fact load hold
> (3 days) and late-arriving rekey (7 days) via `ctl.fact_rekey_queue`;
> delete-by-window fact loads with back-dating allowance; `Fact.Sale`
> REV/RES reversal rows vs `Fact.Payment`/`Fact.Order` in-place restatement;
> accumulating-snapshot MERGE for Shipment, Order Fulfilment and Procure To
> Pay; periodic snapshots with 36-month retention; `Fact.Sales Margin` as
> INSERT OVERWRITE with the regional cost basis and the price/volume/mix/cost
> bridge; `FACT_Dedup_Sale`; the 12 aggregate refresh windows; and the
> publish-state flip only after all rules pass. Generate the 16 `report`
> views with the legacy column aliases. Add the persisted
> `work.customer_dedup_map` bridge for H4 (no fact rewrite). Unit tests on
> fixture data for each pattern, including idempotent re-run. PR + handoff.

**Acceptance criteria.**
1. Every package in appendix A5 has a notebook; every `Integration.usp_*` in `docs/inventories/sql-objects.csv` maps to a notebook or is listed as intentionally dropped (`usp_RebuildColumnstoreIndexes` -> maintenance `OPTIMIZE`); coverage test.
2. SCD2 fixture: Type-2 change creates a new version and expires the old; Type-1 change updates all versions; same-day double change keeps both with `effective_sequence`; re-run is a no-op.
3. Inferred member -> enrichment -> rekey fixture: fact row first points at the inferred key, then the real key after `rekey_late_arriving`; a member arriving after 7 days is abandoned with a warning.
4. `Fact.Sale` correction produces REV+RES rows and leaves the original; `Fact.Payment` correction updates in place and increments `restatement_version`.
5. Accumulating snapshot fixture: three milestone arrivals update the same row, lags recomputed, open-milestone count decreases; row count unchanged.
6. `Fact.Sales Margin` rebuild on a fixture reproduces the bridge identity (price + volume + mix + cost = margin movement with residual in mix) and stamps `cost_basis_code` per region.
7. Every `report.vw_*` view compiles against the target DDL and its column list equals the legacy view's column list (test reads `sqlserver/views/`).
8. Publish: with one failing rule the previous business date stays published and the new one is invisible through the views.

**Tests.** `pytest tests/unit/d tests/contract/dim_fact_agg_schema_test.py`; 03_dimension_fact_integrity assertions as tests.

**Artifacts.** DDL, alias map, notebooks, views, tests, handoff note, PR (may be split: dimensions / facts / aggregates+report).

---

## 6. Session E - Domain marts

**Scope.** `ssis/10_finance` (7, close-gated), `ssis/11_sales` (6),
`ssis/12_inventory` (5), `ssis/13_procurement` (5), `ssis/14_customer_360` (5,
produce/consume split incl. `C360_Publish_Segments`), into per-domain gold
schemas `mart_finance`, `mart_sales`, `mart_inventory`, `mart_procurement`,
`mart_customer360`, plus the two outbound feeds. Plan sections 7.7, 6.2
(Finance Close, Month End, Customer Sync, Intraday Inventory gates), 9 (H4
view, H9).

**Inputs.**
- `ssis/10_finance/*` … `ssis/14_customer_360/*` package specs (the description strings are the business rules)
- `oracle/tables/WWI_FIN.GL_PERIOD_STATUS.sql` and the finance procedures the `FIN_*` packages call; `sqlserver/procedures/facts/Integration.usp_RefreshAggregateCustomer360.sql`, `usp_RefreshAggregateFinanceClose.sql`
- `docs/domain-model/business-domains.md`
- Sessions C (rules) and D (dim/fact/agg) handoffs
- Plan sections 6.2, 7.7, 9

**Outputs.**
- `databricks/sql/ddl/mart_*/*.sql`
- `databricks/notebooks/mart/finance/*` (7: ap_aging, withholding_tax, cost_allocation, currency_revaluation, gl_postings, reconcile_subledger_to_gl, close_period_lock), `mart/sales/*` (6 incl. `export_partner_feed`), `mart/inventory/*` (5 incl. `reconcile_on_hand`, `load_replenishment`), `mart/procurement/*` (5 incl. `export_supplier_statement`), `mart/customer360/*` (5: `build_customer_profile`, `build_loyalty_overlay`, `build_rolling_metrics`, `build_churn_flags`, `publish_segments`)
- `report.vw_customer360_rolled_up` (H4 roll-up option over `work.customer_dedup_map`)
- `tests/unit/e/`, `tests/contract/mart_schema_test.py`
- `docs/migration/handoff/session-e.md`

**Dependencies.** C, D (wave 4).

**Suggested prompt.**

> Implement Session E of `docs/migration/child-session-backlog.md` per plan
> section 7.7 and appendix A6. One notebook per mart package, reading only
> `dim`/`fact`/`agg`/`ref` and writing `mart_<domain>`. Finance: every rule
> that depends on ledger or period goes through `wwi_rules.fiscal`;
> `close_period_lock` locks NA, EU and APAC ledgers separately and writes
> period status through the same interface that `Master_Finance_Close` reads;
> subledger-to-GL reconciliation writes `ctl.reconciliation_result`.
> Customer 360: `build_*` write mart tables only, `publish_segments` reads
> them and writes the published segment table with EU no-consent customers in
> the suppressed segment; churn and rolling metrics use the `Customer 360`
> aggregate from Session D. Inventory: `reconcile_on_hand` classifies
> variance (timing vs genuine) and exposes the max variance the
> `OnHandVarianceTolerance` gate reads; replenishment only runs when the
> parent passes `PickingWindowOpen`. Sales/procurement exports write to the
> outbound volume with EU personal-data stripping from `wwi_rules.consent`.
> Add the H4 roll-up view. Unit tests on fixtures per notebook. PR + handoff.

**Acceptance criteria.**
1. Every package in appendix A6 has a notebook; coverage test.
2. Finance close fixture: period `Closed` -> no mart writes; variance > 0 and `AllowCloseWithVariance=False` -> escalation result, no lock; variance 0 -> lock per ledger with APAC on the 4-4-5 boundary.
3. Customer 360 fixture: an EU customer with consent withdrawn appears in the suppressed segment and is absent from the partner feed export; NA equivalent is published normally.
4. `reconcile_on_hand` fixture reproduces the timing-vs-genuine classification from the package description and returns the variance the gate needs.
5. Re-running any mart notebook for the same batch is a no-op.
6. Roll-up view returns one row per survivor for a merged pair.

**Tests.** `pytest tests/unit/e tests/contract/mart_schema_test.py`.

**Artifacts.** DDL, notebooks, view, tests, handoff note, PR (one per domain is acceptable).

---

## 7. Session F - Orchestration and scheduling

**Scope.** Rebuild all nine `Master_*` packages as parent jobs generated from
`ssis/orchestration-plan.json` (plan section 6): phase ordering, stream
parallelism bounded by `MaxParallelStreams`, extract retry loop with
`MaxExtractAttempts`, `RestartFromStep`, calendar and `GL_PERIOD_STATUS`
gates, month-end enablement state, the Agent schedule table, the
`15_error_handling` operational jobs (`ERR_Notify_Operations`,
`ERR_Reconcile_RowCounts` wrapper, reject reprocess job) and `99_maintenance`
(Delta `OPTIMIZE`/`ANALYZE`/`VACUUM`, purge, archive, config validation),
daily/intraday concurrency rules. Plan sections 6, 9 (H1 gate, H7, H8, H9, H12).

**Inputs.**
- `ssis/orchestration-plan.json`, `ssis/00_orchestration/build_orchestration_packages.py`, `docs/dependency-maps/etl-dependency-map.md`
- `sqlserver/agent/*.sql` (all schedules, enabled flags, retry settings, failure steps, operators)
- `ssis/15_error_handling/*`, `ssis/99_maintenance/*`
- `validation/static/Test-PlanConditionalEdge.ps1` (the conditional-edge rule to preserve)
- `docs/runbooks/execution.md`
- Session A handoff; notebook paths from B-E handoffs (or the appendix as placeholders until they land)
- Plan sections 5.3, 6, 9

**Outputs.**
- `tools/migration/build_workflows.py` - generates `databricks/bundle/resources/job_<master>.yml` for the nine masters from the plan JSON using the mapping rules in plan 6.1, plus `ops_reject_reprocess`, `ops_health_check`, `ops_control_history_purge` if kept separate
- `databricks/bundle/databricks.yml` with environment targets and variables for every placeholder (`catalog`, `volume_root`, hosts, secret scope)
- `databricks/notebooks/workflows/start_batch`, `end_batch`, `claim_step`, `complete_step`, `retry_driver`, `condition`, `run_package`, `calendar_gate`, `stand_down_if_daily_running`, `maintenance/optimize|analyze|vacuum|validate_configuration|check_storage`
- `ctl.package_registry` seed generated from `ssis-packages.csv` + appendix notebook paths
- `tests/unit/f/` - graph equivalence tests: for each master, the generated job's task graph has the same nodes, edges, run_if kinds and expressions as the plan; concurrency ceilings; schedule cron equivalence to the Agent schedule; a simulator that walks the graph with stubbed outcomes and asserts the loop-breaker, retry bound and restart-from-step behaviour
- `docs/migration/handoff/session-f.md` with the schedule table (Agent -> cron, enabled/paused)

**Dependencies.** A for the control API; can start from the plan in wave 1; complete in wave 5.

**Suggested prompt.**

> Implement Session F of `docs/migration/child-session-backlog.md` per plan
> section 6 and appendix A8. Write `tools/migration/build_workflows.py` that
> reads `ssis/orchestration-plan.json` and emits one bundle job per master
> using the mapping rules in plan 6.1: phases become claim_step ->
> for_each(run_package, concurrency = min(streams, MaxParallelStreams)) ->
> complete_step; Success/Completion/Failure edges become run_if kinds;
> expression edges become condition tasks that fire only when both the
> outcome and the expression hold; control queries become SQL tasks on
> `ctl`; reconcile and batch_end nodes call `wwi_control`. Add the
> retry_driver after the extract phases bounded by `MaxExtractAttempts`,
> restart-from-step through `claim_step`, the Daily stand-down check for the
> hourly and intraday jobs (H7), a `ctl.configuration` pause flag for the
> hourly self-disable (H8), and the reject-reprocess stand-down (H1). Port
> every `sqlserver/agent/` schedule to cron with the same enabled/paused
> state (Month End and Finance Close paused, H9; intraday inventory every 20
> minutes per D10). Map `99_maintenance` to OPTIMIZE/ANALYZE/VACUUM bounded by
> `MaintenanceWindowMinutes`. Write graph-equivalence tests and a graph
> simulator; do not deploy. PR + handoff.

**Acceptance criteria.**
1. For all nine masters the generated job graph equals the plan graph (nodes, edges, kinds, expressions) - test.
2. Simulator: Daily with an extract failure retries at most `MaxExtractAttempts` times then takes the failure path; `RestartFromStep="Facts"` skips earlier steps; referential rescreen runs exactly twice; Customer 360 build precedes publish; publish precedes reconcile; Finance Close with `Closed` period escalates; Month End with zero corrections skips the correction phase; File Ingestion with missing files and `RequirePartnerFiles=False` ends `SucceededWithWarnings`.
3. Every schedule in `sqlserver/agent/` has a cron with the same cadence and pause state - table in the handoff, asserted by test.
4. Hourly and intraday jobs stand down when a Daily batch is running (simulator).
5. `databricks bundle validate` passes for every target with placeholder variables.
6. Maintenance tasks cover every `MNT_*` package in appendix A7.

**Tests.** `pytest tests/unit/f`; `databricks bundle validate -t dev` (no deploy).

**Artifacts.** Generator, bundle YAML, workflow notebooks, registry seed, tests, schedule table, handoff note, PR.

---

## 8. Session G - Validation and cutover

**Scope.** Port `validation/` (static coverage for the migration, the four
runtime SQL suites), the `docs/inventories/source-target-map.csv` lineage into
reconciliation queries (row counts, hashes, regional parity, the
`ERR_Reconcile_RowCounts` equivalent), reporting compatibility checks for the
16 views and the two `.pbix` files, and the parallel-run, acceptance-gate,
cutover, rollback and operational sign-off plan. Plan section 11.

**Inputs.**
- `validation/README.md`, `validation/static/*`, `validation/checks/*` (esp. `check_source_to_target_coverage.py`, `check_orchestration_edges.py`, `check_control_framework_integration.py`), `validation/runtime/01`-`04.sql`
- `docs/inventories/source-target-map.csv`, `docs/known-unvalidated-items.md`
- `sqlserver/control/06_tables_reconciliation.sql`, `etl.usp_AssertRowCountReconciliation`, the `ReconciliationExemption` seed
- `sqlserver/views/Report.vw_*.sql`, `power-bi-dashboards/`
- `docs/runbooks/execution.md`
- All handoffs A-F
- Plan sections 9 (approved differences), 11

**Outputs.**
- `validation/migration/check_migration_coverage.py` - every package in `ssis-packages.csv` has an appendix row with a notebook path and target table; every `Integration.usp_*` and `etl.usp_*` has a mapping
- `validation/migration/runtime/01_control_health.sql`, `02_row_count_reconciliation.sql`, `03_dimension_fact_integrity.sql`, `04_regional_divergence.sql` as Databricks SQL over `ctl`/`dim`/`fact`
- `databricks/notebooks/validation/source_to_target_reconciliation` (per bronze table/window from `ctl.row_count_audit`), `legacy_vs_target_diff` (row count + business-column hash per object pair from the source-target map, per business date, with the H-approved difference list), `regional_parity`, `report_view_parity` (column lists, row counts and totals per `report.vw_*` vs `Report.vw_*`), `watermark_reconciliation`
- `docs/migration/parallel-run-and-cutover.md` - schedule, roles, acceptance gates with the D12 thresholds, daily diff report format, go/no-go checklist, cutover steps, rollback steps, operational sign-off (runbooks per job, on-call routing, health dashboard on `ctl`)
- `docs/migration/handoff/session-g.md`

**Dependencies.** Static coverage in wave 1; runtime notebooks after D (wave 3); parallel run after F (wave 5+).

**Suggested prompt.**

> Implement Session G of `docs/migration/child-session-backlog.md` per plan
> section 11. First add `validation/migration/check_migration_coverage.py`
> and wire it into `validation/static/run_all_checks.py` so the appendix and
> the code tree are checked for full package and procedure coverage. Port the
> four `validation/runtime` suites to Databricks SQL over `ctl`, `dim`, `fact`
> and `wwi_rules` outputs. Build the reconciliation notebooks: source-to-target
> per bronze window, legacy-vs-target row count and hash diff per object pair
> from `source-target-map.csv` with the hazard-approved difference list (H1,
> H3, H7, H10), regional parity from 04, report view parity for all 16 views
> and the two `.pbix` data sources, watermark reconciliation. Write
> `docs/migration/parallel-run-and-cutover.md` with the D12 window,
> acceptance gates (zero unexplained variance within the configured
> tolerances, view parity for the last published date, seven clean
> health-check days), cutover and rollback steps and operational sign-off.
> Tests on fixture data for every notebook. PR + handoff.

**Acceptance criteria.**
1. Coverage check passes on the merged tree and fails on a fixture with one package removed.
2. Each runtime suite runs against fixture `ctl`/`dim`/`fact` tables and flags the defect classes the legacy SQL comments describe (e.g. a region falling through to NA treatment).
3. Diff notebook on a fixture pair with one known difference reports exactly that difference, and suppresses it when it is on the approved list.
4. Report-view parity test: column lists equal for all 16 views; totals equal on fixture data.
5. Cutover document reviewed by the parent with every acceptance gate traceable to a notebook output and every rollback step to a runbook.
6. Operational sign-off checklist covers restart-from-step, adopt-running-batch, force-full-reload, manual replay and pause/resume for all nine jobs.

**Tests.** `pytest tests/unit/g`; `python3 validation/static/run_all_checks.py`.

**Artifacts.** Coverage check, SQL suites, notebooks, cutover document, tests, handoff note, PR.

---

## 9. Parent session checklist

1. Confirm D1-D14 with the user; record answers in plan section 2.
2. Dispatch wave 1 (A, C part 1) with this file and the plan.
3. On each handoff: merge, re-run `python3 tools/migration/build_package_mapping.py`, run `python3 validation/static/run_all_checks.py`, route open items, dispatch the next wave.
4. After F: dry-run all nine jobs in TEST against fixture data; after G: start the parallel run.
5. Go/no-go against the acceptance gates; cutover; keep the legacy estate warm for the rollback window.
