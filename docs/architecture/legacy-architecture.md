# The legacy estate

Wide World Importers, as it exists after twenty years of accretion. This
document describes what is in the repository: four databases, an ETL control
schema, 204 SSIS packages, a SQL Agent schedule, a deployment driver and a data
generator. It is the "before" picture.

Nothing here has been deployed. No Oracle instance, SQL Server instance, SSIS
catalog, Agent service or file share has been contacted by this repository, and
every statement below describes the checked-in definitions rather than observed
behaviour. `docs/known-unvalidated-items.md` is the standing list of what that
leaves unproven.

## The shape of it

```
   Oracle WWIGERP                     SQL Server WideWorldImporters
   (ERP, bought 2003)                 (OLTP, the original application)
   WWI_MDM  master data               Sales / Warehouse / Shipping
   WWI_PROC procurement               Returns / Loyalty / Ecommerce
   WWI_FIN  AP and GL                 Integration
   WWI_REF  reference data                     |
   WWI_AUDIT change capture                    |
        |                                      |
        |  nightly + hourly extracts           |  hourly + intraday extracts
        |                                      |
        +---------------+----------------------+-------- flat files, EDI drops
                        |                                (03_file_ingestion)
                        v
        SQL Server WideWorldImporters_Staging
        raw   landed, source-shaped, no rules applied
        stg   conformed, typed, deduplicated
        work  scratch for multi-pass loads
        err   rejects, by domain
        ref   reference snapshots
        etl   the control framework
                        |
                        v
        SQL Server WideWorldImportersDW
        Dimension  24    Fact  20    Aggregate  12    Report  16
        Integration      load procedures
        etl              a second copy of the control framework
```

Four databases, two engines, one shared control schema deployed twice. The
duplication of `etl` between staging and the warehouse is not a design; it is
what happened when the warehouse was split onto its own instance in 2011 and
nobody wanted to introduce a cross-server dependency in the load path.

## Oracle: WWIGERP

The ERP arrived with an acquisition and never left. Five schemas, 69 tables,
14 PL/SQL packages, 24 standalone procedures, 21 views and 11 functions.

| Schema | Owns | Tables |
| --- | --- | --- |
| `WWI_MDM` | customer, supplier, product and party master | 19 |
| `WWI_PROC` | requisition to receipt | 15 |
| `WWI_FIN` | AP invoices, payments, GL postings | 16 |
| `WWI_REF` | enterprise reference and code tables | 15 |
| `WWI_AUDIT` | source-side change capture and extract bookkeeping | 4 |

The packages are where the business logic that never made it into the
application lives: `PKG_TAX` and `PKG_FX` are the two that matter most for the
warehouse, because between them they decide what a line is worth and in what
currency, and both of them branch on region. `PKG_CODE_TRANSLATION` maps the
ERP's code sets onto the ones the OLTP system uses, and it is the reason a
"customer category" means two different things depending on which side of the
extract you are standing.

`WWI_AUDIT` is the change-capture mechanism: trigger-maintained change tables
plus an extract-control table the extracts read and write. It predates any
vendor CDC product and is the reason the incremental extracts are timestamp-
and key-based rather than log-based.

Reference data - tax rates, fiscal calendars, currencies, geography - is
mastered here and fully refreshed into the warehouse weekly. The regional tax
tables (`oracle/reference/05_tax_na_sales_and_use.sql`,
`06_tax_eu_vat.sql`, `07_tax_apac_gst.sql`) are three separate structures with
three separate shapes, because the three regions were integrated in three
different projects.

## SQL Server: WideWorldImporters (OLTP)

The original Microsoft sample, extended. The base sample's `Application`,
`Sales`, `Purchasing`, `Warehouse` and `Website` schemas are untouched; the
estate adds tables in seven schemas around them:

| Schema | New tables | What it is |
| --- | --- | --- |
| `Sales` | 15 | quotes, contracts, price lists, commission |
| `Warehouse` | 10 | bins, cycle counts, replenishment |
| `Shipping` | 9 | carriers, consignments, tracking events |
| `Returns` | 6 | RMA lifecycle |
| `Loyalty` | 5 | tiers, points ledger, accruals |
| `Ecommerce` | 6 | web sessions, carts, channel orders |
| `Integration` | 3 | outbound queues and extract bookkeeping |

Plus 23 procedures, 14 views and 7 functions. The Returns, Loyalty and
Ecommerce schemas are visibly later work - they use `NVARCHAR` keys, carry
their own audit columns, and none of them participate in the temporal tables
the base sample uses. That inconsistency is deliberate and is exactly the kind
of thing a migration has to reconcile.

## SQL Server: WideWorldImporters_Staging

The landing and conforming layer, and the home of the control framework.

| Schema | Tables | Rule |
| --- | --- | --- |
| `raw` | 34 | source-shaped. Every column wide and nullable, no constraints, no rules. |
| `stg` | 35 | conformed and typed. Deduplicated, keys resolved, regional rules applied. |
| `work` | 12 | scratch for multi-pass loads; truncated and rebuilt in place. |
| `err` | 10 | rejects, one table per domain. |
| `ref` | - | reference snapshots taken at load time. |
| `etl` | 13 | the control framework. |

22 procedures and 10 views. The `raw` tables carry the source system's names
(`raw.OracleCustomerMaster`, `raw.SqlOrderLine`) and the `stg` tables carry the
estate's (`stg.Customer`, `stg.OrderLine`); the mapping between the two is in
the load procedures and in the packages, not in any registry.

## SQL Server: WideWorldImportersDW

24 dimensions, 20 facts, 12 aggregates, 16 report views, 50 load procedures and
8 functions, plus the second copy of `etl`.

The facts that the Microsoft sample already provides - `Fact.Sale`,
`Fact.Order`, `Fact.Purchase`, `Fact.Movement`, `Fact.Stock Holding`,
`Fact.Transaction` - are **extended in place** by the `*.Extensions.sql` files
rather than replaced: currency and FX columns, regional tax columns, fiscal
period columns, channel and promotion keys. The estate's own facts
(`Fact.GL Posting`, `Fact.Procure To Pay`, `Fact.Supplier Ledger`,
`Fact.Web Session`, the snapshot facts, `Fact.Fact Load Hold`) are new tables.

Two mechanisms are worth naming because they shape every load:

- **Unknown members.** There are no foreign keys from fact to dimension. Every
  dimension carries a `-1` unknown member, and a fact row whose lookup fails
  points at it rather than failing the load.
- **`Fact.Fact Load Hold`.** Fact rows whose dimension has not arrived yet are
  parked here with a retry count and released later by the rekey load. Rows in
  this table are invisible to every report until they are released.

`Dimension.Customer` carries both generations of Type 2 mechanics: the original
`[Valid From]`/`[Valid To]` pair, and the `[Is Current Row]`/`[Version Number]`/
`[Row Hash Type 2]` columns added later when the first pair turned out to be
serving two purposes. Both are still maintained, because reports depend on both.

## The ETL control framework

One schema, `etl`, deployed into both the staging database and the warehouse.
13 tables, 15 procedures, 6 views. It is the only thing in the estate that
knows a run happened.

| Table | Holds |
| --- | --- |
| `etl.Batch` | one row per run |
| `etl.BatchStep` | one row per master-package section |
| `etl.PackageExecution` | one row per package execution, with timings and row counts |
| `etl.Watermark` | incremental extraction high-water marks |
| `etl.RowCountAudit` | per-object source/target/reject counts with a computed variance |
| `etl.ErrorLog` | every failure a package chose to log |
| `etl.RejectedRecord` | rows routed out of the pipeline, with their payload |
| `etl.Configuration` | per-environment settings |
| `etl.PackageDependency` | declared hard/soft ordering between packages |
| `etl.SourceSystem` | the source registry |

The procedures are the API every package calls: `usp_StartBatch`,
`usp_EndBatch`, `usp_StartBatchStep`, `usp_EndBatchStep`, `usp_LogPackageStart`,
`usp_LogPackageEnd`, `usp_LogError`, `usp_LogRowCount`, `usp_LogRejectedRecord`,
`usp_GetWatermark`, `usp_SetWatermark`, `usp_AssertRowCountReconciliation`.

The views are what an operator actually looks at: `etl.vw_BatchStatus`,
`etl.vw_PackageExecutionHistory`, `etl.vw_SlowPackages`,
`etl.vw_RowCountReconciliation`, `etl.vw_RejectSummary`,
`etl.vw_WatermarkStatus`, `etl.vw_RecentErrors`.

Because the framework is deployed twice, a nightly run opens a batch in each
database, and the two have to be correlated by business date and batch name.
There is no mechanism that enforces the correlation.

## SSIS

204 packages, generated from declarations by `tools/ssisgen` and checked in as
`.dtsx`. They are not hand-edited; the generator is the source of truth for
their structure and the catalog is the source of truth for their content.

| Folder | Packages | |
| --- | --- | --- |
| `00_orchestration` | 9 | the `Master_*` packages |
| `01_oracle_extract` | 22 | WWIGERP to `raw` |
| `02_sqlserver_extract` | 21 | OLTP to `raw` |
| `03_file_ingestion` | 7 | flat files and EDI drops to `raw` |
| `04_staging` | 28 | `raw` to `stg` |
| `05_data_quality` | 10 | screening and quarantine |
| `06_reference_data` | 14 | weekly reference refresh |
| `07_dimensions` | 15 | `stg` to `Dimension` |
| `08_facts` | 24 | `stg` to `Fact` |
| `09_aggregates` | 13 | `Fact` to `Aggregate` |
| `10_finance` - `14_customer_360` | 28 | domain pipelines |
| `15_error_handling` | 6 | reject reprocessing |
| `99_maintenance` | 7 | index, statistics, purge |

Load types are declared per package and drive what the generator emits:
`full`, `full_refresh`, `truncate_reload`, `incremental_timestamp`,
`incremental_key`, `incremental_append`, `date_window`, `SCD1`, `SCD2`,
`snapshot_fact`, `incremental_fact`, `aggregate_rebuild`, `quality_screen`,
`file_ingest`, `work_rebuild`, `rekey`, `dedup`, `correction`, `publish`,
`business_rule`, `utility`, `orchestration`.

The nine masters are `Master_Daily_ETL`, `Master_Hourly_Incremental`,
`Master_Finance_Close`, `Master_Weekly_Reference_Load`, `Master_Month_End`,
`Master_Weekly_Maintenance`, `Master_Customer_Sync`, `Master_Intraday_Inventory`
and `Master_File_Ingestion`. `Master_Daily_ETL` alone executes 192 children.
The execute graph is flat: masters call children, children do not call
children. `docs/dependency-maps/etl-dependency-map.md` has the numbers.

## SQL Agent

12 jobs, one per master plus reject reprocessing, control-history purge and a
health check. Schedules run from 00:00 to 22:00 across the day; the two
period-close jobs ship disabled because the fiscal calendar has to be loaded
before a close can be allowed to fire. Retry policies are per-failure-mode
rather than a house default. Details and the full table are in
`sqlserver/agent/README.md` and `docs/runbooks/execution.md`.

## Deployment

`deployment/deploy-all.sh` and `deployment/deploy-all.ps1` run the same four
stages: preflight, Oracle, SQL Server, SSIS. Both support a dry run that prints
what would execute. Environment definitions live in `config/environments/`
(`dev`, `test`, `prod`) and every credential is a variable name, never a value.
`docs/runbooks/deployment.md` has the order and the gates.

## Generators

`generators/` produces synthetic data as files only. It never connects to
anything: it writes delimited files plus a manifest, and emits loader scripts
that a human would run. Scale modes control volume. This is how the estate gets
populated for a demonstration without shipping data in the repository.

## What makes this estate legacy

- **Logic in three places.** The same tax decision exists in Oracle PL/SQL, in
  T-SQL load procedures and in SSIS derived-column expressions, and the three
  do not agree in every edge case.
- **Regional divergence encoded structurally.** NA, EU and APAC differ in tax
  regime, fiscal calendar, currency, address shape, consent and retention
  rules, and code sets, and each difference is expressed as a branch rather
  than as configuration.
- **Two generations of everything.** Two SCD2 mechanisms on the same dimension,
  two control-framework copies, two naming conventions for staging tables, two
  eras of OLTP schema design.
- **No enforced referential integrity where it matters.** Fact-to-dimension
  integrity is a convention maintained by load procedures and unknown members.
- **Orchestration by convention.** The `etl.PackageDependency` table declares
  ordering, and the master packages implement it independently. Nothing checks
  that the two agree, and the static checks in `validation/checks/` find places
  where they do not.
- **Rows that leave quietly.** `err.*`, `etl.RejectedRecord` and
  `Fact.Fact Load Hold` all absorb data that never reaches a report, and only
  the health-check job looks at them.
