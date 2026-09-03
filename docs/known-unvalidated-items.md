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

These were found by the checks and are open. They belong to the work packages
that own the files, and WP13 records them rather than editing sibling-owned
paths to make a check go green.

### Duplicate object definition (WP05 / WP06) - resolved during integration

The dimension and fact work packages each created
`Integration.usp_EnsureUnknownMembers`, with different reserved-key
conventions. The fact package's copy was removed at merge time and the
dimension package's copy kept, because it is the one driven by
`Integration.DimensionKeyRegistry` and it agrees with the reserved keys the
dimension tables are seeded with (`-1` Unknown, `-2` Not Applicable). Neither
body has been executed.

### Unresolved source and target objects (18, WP08 / WP09)

`check_source_to_target_coverage.py` reports 18 object references that resolve
to nothing - not in the catalog, not created by any SQL file in the tree, not
part of the Microsoft sample:

- `stg.City`, `stg.CustomerCategory`, `stg.CustomerSegment`,
  `stg.ProductCategory` - sources of four dimension loads.
- `stg.CustomerTransaction`, `stg.DailyInventorySnapshot`,
  `stg.DailySalesSnapshot`, `stg.GLPosting`, `stg.LoyaltyPoints`,
  `stg.Movement`, `stg.OrderFulfilment`, `stg.Purchase`, `stg.PurchaseReceipt`,
  `stg.StockHolding`, `stg.SupplierPayment`, `stg.SupplierTransaction`,
  `stg.Transaction` - sources of thirteen fact loads.
- `Aggregate.Inventory Health` - target of `INV_Load_Replenishment`, where the
  catalog aggregate is named `Daily Inventory Health`.

The fact and dimension packages name staging tables the staging work package
did not create. Either the staging layer is missing seventeen tables or the
packages are naming them wrongly; the two sides were built independently and
never reconciled.

### Staging objects with no writer or no reader (18 errors, 17 warnings, WP04 / WP08)

`check_orphan_objects.py` over 196 staging and warehouse objects:

- Read but never written: `ref.CodeCrosswalk`, `ref.Country`, `ref.Currency`,
  `ref.FxRateDaily`, `ref.PostalFormatRule`, `ref.ReasonCode`, `ref.Region`,
  `ref.TaxJurisdiction`, `ref.UnitOfMeasure`, `ref.UomConversion`,
  `stg.Receipt`, `work.OrderLineEnriched`, `work.PurchaseLineEnriched`,
  `work.SaleLineEnriched`, `raw.SqlLoyaltyLedger`,
  `err.RejectedConstraintViolation`.
- Never referenced at all: `ref.StatusCode`, `work.SupplierDedup`.

The `ref.*` cluster is the significant one: ten reference tables that fifteen,
twelve, nine and seven files respectively read from, and that no load in the
tree populates. The weekly reference packages write into `Dimension.*` and the
Oracle `raw` tables directly, so the `ref` snapshot layer was defined and then
bypassed.

Some of these may be populated by dynamic SQL that the checks cannot see. That
possibility does not clear them; it means they need a human to look.

### Control-framework gaps (5 errors, 92 warnings, WP04 / WP08 / WP10)

`check_control_framework_integration.py`:

- Five procedures are referenced but created nowhere under
  `sqlserver/control/procedures`: `etl.usp_AssertRowCountTolerance`,
  `etl.usp_EvaluateDataQualityRules`, `etl.usp_LogReject`,
  `etl.usp_LogRejectedRecordSet`, `etl.usp_PurgeControlHistory`. Some are
  near-misses for procedures that do exist (`etl.usp_LogRejectedRecord`,
  `etl.usp_AssertRowCountReconciliation`), which is how they got past review.
- Catalog declarations disagree with the packages on disk: 30 extract packages
  declare `etl.GetWatermark`/`etl.SetWatermark` that their `.dtsx` does not
  reference, six maintenance packages declare `etl.PurgeControlHistory` they do
  not reference, and `REF_Load_CodeTranslation` declares
  `etl.GetConfigurationValue` it does not reference. The catalog's procedure
  names also use a different convention (`etl.GetWatermark`) from the deployed
  ones (`etl.usp_GetWatermark`).
- 35 load procedures make no call into the `etl` control schema at all,
  including all of the `Integration.usp_MigrateStaged*Data` family and
  `Integration.usp_EnsureUnknownMembers`. Work they do is invisible to
  `etl.PackageExecution` and to every operational view.

### Orchestration disagreements (WP10)

`extract_package_dependency_graph.py`:

- `ERR_Retry_FailedSteps` is executed by no master package.
- `DQ_File_Screen`, `ERR_Quarantine_BadFiles` and `ERR_Retry_FailedSteps`
  declare `parent: Master_Daily_ETL` in the catalog, but `Master_Daily_ETL`
  contains no Execute Package Task that references them.
- Three data-flow cycles exist (Customer 360, the data-quality cluster of 16
  packages, and the referential screen/reject pair). None is an execute cycle;
  all three are broken only by running the two halves in different windows.

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
