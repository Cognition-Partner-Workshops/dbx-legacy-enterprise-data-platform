# Known unvalidated items

The estate's standing ledger of what has **not** been proven. Read it before
you trust anything else in `docs/`.

No Oracle instance, SQL Server instance, SSIS catalog, SQL Agent service, file
share or other engine has been contacted by this repository. Nothing has been
compiled, deployed, executed, or measured. Every artifact here is a definition,
and the only assurance behind any of it is static analysis of text:
`validation/static/run_all_checks.py` and `validation/checks/`.

Two kinds of entry follow. **Unvalidated behaviour** is everything the static
checks structurally cannot reach. **Static findings** are inconsistencies the
checks *did* find and that remain open in the merged tree.

---

## 1. Unvalidated behaviour

### Oracle - schema and DDL (WP01)

- No PL/SQL or SQL DDL in `oracle/` has been parsed by Oracle. Syntax is
  unverified.
- Storage clauses, tablespace assignments, partition definitions and
  subpartition templates are unverified; none has been created.
- Sequences, synonyms, roles, grants and profiles are unverified: no privilege
  has been granted, no synonym resolved, no sequence allocated.
- Referential integrity is unverified. Foreign keys, check constraints and
  unique constraints have never been enforced against data.
- Seed and reference data has never been inserted; volumes, key collisions and
  code-value coverage are unknown.
- Cross-schema references between `WWI_MDM`, `WWI_PROC`, `WWI_FIN`, `WWI_REF`
  and `WWI_AUDIT` are unresolved, as are any database-link references.

### Oracle - packages and logic (WP02)

- The 14 packages, 24 procedures, 21 views and 11 functions have never been
  compiled. Package specification/body conformance is unverified.
- Dependency order between packages is unverified; a recompile is the first
  thing that would test it.
- Dynamic SQL inside the packages is unverified in every respect - the
  statements it composes, the objects they name, and the privileges they need.
- Tax, FX and rounding behaviour in `PKG_TAX` and `PKG_FX` is unverified. No
  number produced by either has been checked against anything.
- Trigger behaviour behind `WWI_AUDIT` change capture is unverified.

### SQL Server - OLTP (WP03)

- No T-SQL has been parsed by SQL Server. Syntax is unverified.
- Computed columns, persisted or otherwise, are unverified.
- Triggers are unverified: none has fired.
- Index definitions, filtered indexes and columnstore indexes are unverified,
  and no query plan has been produced or inspected.
- The extensions assume a restored Microsoft `WideWorldImporters` sample of a
  particular shape; that assumption has not been tested against a real restore.
- Temporal-table and In-Memory OLTP interaction with the new tables is
  unverified.

### SQL Server - staging (WP04)

- Dynamic SQL in the staging procedures is unverified.
- Truncation behaviour, `NVARCHAR` widths and decimal precision are unverified;
  no value has been passed through them.
- Regional rule application in staging is unverified.
- Cursor and loop constructs are unverified, as is their performance at any
  volume.
- `raw` to `stg` column mappings are unverified: the static checks read
  object-level declarations only and never column lineage.

### SQL Server - warehouse (WP05, WP06)

- `MERGE` statements are unverified, including their `WHEN MATCHED` conditions
  and their behaviour on duplicate source keys.
- Recursive CTEs for hierarchy loads are unverified, including termination.
- Cursors and dynamic SQL in the load procedures are unverified.
- Surrogate-key allocation, including the sequence-backed defaults and
  `Integration.usp_AllocateDimensionKeyRange`, is unverified.
- Date and fiscal arithmetic is unverified, in all three regional calendars.
- Fact-to-dimension fit is unverified: no lookup has been attempted, so the
  real unknown-member and `Fact.Fact Load Hold` volumes are unknown.
- SCD2 chain maintenance on `Dimension.Customer` - two coexisting mechanisms -
  is unverified, as is their agreement with each other.
- Aggregate rebuilds have never been compared against the facts they summarise.

### SSIS (WP07, WP08, WP09, WP10)

- No package has been executed by `dtexec` or opened in SSDT. That the XML
  parses is not evidence that the designer will load it.
- Data-flow metadata and column lineage are unverified. The generator emits
  column metadata that no engine has validated.
- Expressions - derived columns, variable expressions, parameterised connection
  strings - are unverified.
- Precedence constraints and their expressions are unverified, so the actual
  execution order inside a master is unproven.
- Connection-manager resolution and project/package parameter binding are
  unverified.
- Flat-file parsing (delimiters, code pages, ragged rows, the APAC Latin-1
  cases) is unverified.
- Watermark handling and delete detection are unverified.
- Package-to-procedure wiring is unverified: the control-framework check
  confirms that a procedure *name appears as text* in a package, never that the
  call is on a reachable path, correctly parameterised, or ever executed.
- Orchestration package references are unverified beyond the file existing;
  whether a child would be found at runtime depends on catalog deployment.

### SQL Agent, deployment and configuration (WP11)

- No Agent script has been run; job installation and its claimed idempotency
  are unverified.
- Security grants, roles and proxy/credential wiring are unverified.
- Linked-server definitions and their Oracle providers are unverified.
- PowerShell deployment scripts are unverified; none has been executed on
  Windows.
- `OPENROWSET` paths and landing-zone directory layout are unverified.
- The `.ispac` build and its hand-assembled fallback are unverified.
- SSIS environment binding is unverified: the rendered SQL has never been
  applied to a catalog.
- Infrastructure sizing, retention periods and alert thresholds in
  `infrastructure/` are declarations, not measurements.

### Generators (WP12)

- Medium and large generator modes have not been run; only the small sample
  output is present.
- Design-target volumes are unverified.
- Bounded-memory behaviour has not been profiled.
- Generated loader script syntax is unverified against any client.
- The generated target shape has never been loaded into a real table, so its
  fit against the staging DDL is unproven.
- Cross-machine determinism of the seeded generation is unverified.
- Quarantine reconciliation between generated rejects and generated inputs is
  unverified.
- Scale-driver assertions are unverified.
- APAC Latin-1 round-trip through file write and read is unverified.

### Validation itself (WP13)

- The queries in `validation/runtime/` have never been run. Their syntax,
  the columns they name, and their thresholds are all unverified.
- The static checks in `validation/checks/` are text and graph analysis. They
  cannot see anything composed at runtime, and objects populated by dynamic SQL
  or consumed outside this repository will look like orphans when they are not.
  `validation/README.md` has the full statement of what they can and cannot
  establish.

---

## 2. Static findings in the merged estate

The twelve build packages were merged, the deep checks were run over the whole
tree, and a second pass closed what they found. All four deep checks
(`source-to-target coverage`, `staging and warehouse orphans`, `package
dependency graph`, `control-framework integration`) and
`validation/static/run_all_checks.py` now report zero errors. What follows is
what was closed, and what remains open as warnings.

### Closed during integration

- **Duplicate object definition (WP05 / WP06).** The dimension and fact packages
  each created `Integration.usp_EnsureUnknownMembers` with different reserved-key
  conventions. The fact package's copy was removed and the dimension package's
  kept, because it is driven by `Integration.DimensionKeyRegistry` and agrees
  with the reserved keys the dimension tables are seeded with (`-1` Unknown,
  `-2` Not Applicable). Neither body has been executed.
- **Seventeen missing staging tables (RP01).** The four dimension sources
  (`stg.City`, `stg.CustomerCategory`, `stg.CustomerSegment`,
  `stg.ProductCategory`) and thirteen fact sources (`stg.CustomerTransaction`
  through `stg.Transaction`) that the loads read now exist in
  `sqlserver/staging/`, each with a load procedure in the staging layer's style.
- **The conformed reference layer (RP02).** The eleven `ref.*` tables that dozens
  of files read had no writer; `sqlserver/reference/` now holds one
  `ref.usp_Load<Object>` procedure per table, the effective-dated FX, tax and
  postal rules, and the `ref.CodeCrosswalk` mapping with its unmapped-code
  report. The weekly reference packages land through those procedures instead of
  writing `Dimension.*` directly.
- **Orphan staging and work objects (RP01).** `stg.Receipt`,
  `work.SupplierDedup`, `err.RejectedConstraintViolation`, the three
  `work.*Enriched` tables and `raw.SqlLoyaltyLedger` are now wired into the loads
  that populate or consume them. `raw.SqlLoyaltyLedger` gained the extract that
  lands it, `EXT_SQL_LoyaltyLedger`.
- **Missing control procedures.** The five procedures packages referenced but
  nothing created - `etl.usp_AssertRowCountTolerance`,
  `etl.usp_EvaluateDataQualityRules`, `etl.usp_LogReject`,
  `etl.usp_LogRejectedRecordSet`, `etl.usp_PurgeControlHistory` - are defined in
  `sqlserver/control/procedures/`, alongside the data-quality rule engine
  (`04_tables_data_quality.sql`, `05_seed_data_quality_rules.sql`), the
  reconciliation results table (`06_tables_reconciliation.sql`) and the
  nineteen operational tables the error-handling, maintenance and file-ingestion
  packages write (`07_tables_operations.sql`).
- **Control naming and reject columns.** The catalog declared `etl.GetWatermark`
  where the deployed name is `etl.usp_GetWatermark`; the catalog now carries the
  deployed names. Four package spec modules wrote `etl.RejectedRecord` columns
  that the table does not have (`RejectReasonDescription`, `SourceKey`,
  `RejectedAtUtc`) and were corrected to `RejectReason`, `BusinessKey`,
  `LoggedAtUtc`. `validation/checks/check_control_object_columns.py` was added so
  this class of drift fails a check rather than surviving a review.
- **Orchestration reachability (RP03).** `DQ_File_Screen`,
  `ERR_Quarantine_BadFiles` and `ERR_Retry_FailedSteps` are now executed by the
  masters that declare them as children, on the phases and failure paths where
  they belong, and the three data-flow cycles have their intended ordering
  declared in the orchestration rather than left to the schedule.
- **`Aggregate.Inventory Health`.** `INV_Load_Replenishment` targeted a name the
  warehouse does not define; the aggregate is `Aggregate.Daily Inventory Health`
  and the package, catalog and reconciliation seed now agree.

### Open as warnings

- **554 control-framework warnings.** Most are catalog declarations that a
  package's `.dtsx` does not textually reference, and 35 load procedures - the
  `Integration.usp_MigrateStaged*Data` family among them - that make no call into
  the `etl` schema at all, so their work is invisible to `etl.PackageExecution`
  and the operational views. That is legacy-realistic and deliberately left.
- **29 source-to-target warnings.** Extract packages reading Microsoft
  WideWorldImporters base-sample objects, which are outside the estate inventory
  by design.
- **17 orphan warnings and 5 package-graph warnings.** Objects consumed outside
  this repository or through dynamic SQL the checks cannot see, and cross-window
  data edges. Each needs a human, not a checker.

None of the closures above is evidence that anything runs. They mean the tree no
longer contradicts itself.

---

## 3. What would move an item off this list

Nothing in this repository can. Each entry needs an engine:

| To validate | You need |
| --- | --- |
| Oracle DDL and PL/SQL | an Oracle instance, a deployment, and a clean `ALL_OBJECTS` invalid check |
| T-SQL | a SQL Server instance and a successful deployment of all four stages |
| SSIS structure | SSDT opening every package without a metadata error |
| SSIS behaviour | `dtexec` against a populated staging database |
| Row counts and reconciliation | a completed run plus `validation/runtime/02_row_count_reconciliation.sql` |
| Regional behaviour | a completed run plus `validation/runtime/04_regional_divergence.sql` |
| Agent scheduling | an Agent service and a night |

Until then the honest statement about this estate is that it is fully specified
and entirely unproven.
