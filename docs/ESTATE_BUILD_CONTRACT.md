# Legacy estate build contract

This document is the contract every work package in the legacy-estate expansion
codes against. It exists so that thirteen independently-built packages compose
into one coherent estate instead of thirteen private conventions.

Read this together with:

- `config/estate-catalog.yaml` - the deterministic object catalog. It is the
  single source of truth for schema names, table names, package names, load
  types, source-to-target mappings and package parentage. **Never invent a name
  that contradicts the catalog, and never rename a catalog object.**
- `tools/catalog/build_catalog.py` - generates the catalog. Only the foundation
  owns this file.
- `tools/ssisgen/` - the SSIS XML emitter. All `.dtsx` files are generated from
  Python specs through this library.

## Hard constraints

1. **No live systems.** Nothing in this repository may connect to Oracle, SQL
   Server, or Databricks. No script, test, generator, or validation step may
   require a live connection to run.
2. **No Databricks artifacts.** No notebooks, Delta tables, medallion layers,
   Lakeflow pipelines, Unity Catalog config, or migration code. This repository
   is the *before* state of a future migration demonstration.
3. **No credentials.** Not in SQL, not in `.dtsx`, not in `.conmgr`, not in
   config. Connection strings are built from project parameters that resolve
   from the environment variables documented in `.env.example`.
4. **No claimed runtime results.** Do not write "verified", "tested against",
   "row counts confirmed", or any statement implying execution. Everything in
   this repository has been checked *statically* only. Anything that cannot be
   statically verified belongs in `docs/known-unvalidated-items.md`.
5. **Preserve the WideWorldImporters heritage.** The Microsoft sample's schemas,
   naming and licensing stay intact. New objects extend it; they do not replace
   or rename it. Keep `LICENSE` and the attribution in `README.md`.
6. **Legacy on purpose.** This estate is meant to look like twenty years of
   accreted enterprise ETL: stored-procedure business logic, SSIS row-by-row
   patterns, regional divergence, hand-rolled SCD handling, dynamic SQL where a
   real shop would have used it. Do not modernise it away.

## Ownership

Each work package owns the paths listed under its id in
`config/estate-catalog.yaml` (`work_packages`). **Write only inside your owned
paths.** Never edit:

- another package's paths,
- `config/estate-catalog.yaml` or `tools/`,
- `sqlserver/control/` (owned by the foundation),
- the pre-existing WideWorldImporters directories (`wwi-ssdt`, `wwi-dw-ssdt`,
  `wwi-ssis`, `wwi-app`, `wwi-azure-functions`, `wwi-ssasmd`,
  `power-bi-dashboards`, `sample-scripts`, `workload-drivers`).

If you need something that lives in another package's path, code against the
catalog's declaration of it and note the assumption in your summary.

## Naming conventions

| Object | Convention | Example |
| --- | --- | --- |
| Oracle schema | `WWI_<DOMAIN>` | `WWI_MDM`, `WWI_PROC`, `WWI_FIN`, `WWI_REF` |
| Oracle table | `UPPER_SNAKE_CASE`, singular | `CUST_MASTER`, `PO_RECEIPT_LINE` |
| Oracle view | `V_<NAME>` | `V_GEOGRAPHY_EXTRACT` |
| Oracle package | `PKG_<DOMAIN>` | `PKG_AP_INVOICE` |
| Oracle procedure | `PRC_<VERB>_<NOUN>` | `PRC_LOAD_AP_AGING` |
| Oracle function | `FN_<NOUN>` | `FN_CONVERT_CURRENCY` |
| Oracle sequence | `SEQ_<TABLE>` | `SEQ_CUST_MASTER` |
| SQL Server staging table | `raw.<System><Object>`, `stg.<Object>`, `work.<Object>`, `err.<Object>` | `raw.OracleApInvoiceHdr`, `stg.Customer` |
| SQL Server procedure | `<schema>.usp_<Verb><Noun>` | `stg.usp_DeduplicateCustomer` |
| SQL Server function | `<schema>.ufn_<Noun>` | `etl.ufn_GetConfigurationValue` |
| SQL Server view | `<schema>.vw_<Noun>` | `etl.vw_BatchStatus` |
| DW dimension | `Dimension.<Name>` (WWI DW convention, spaces allowed) | `Dimension.Stock Item` |
| DW fact | `Fact.<Name>` | `Fact.Sale` |
| SSIS package | `<PREFIX>_<Domain>_<Object>` | `EXT_ORA_CustomerMaster`, `DIM_Load_Customer` |
| SQL Agent job | `WWI - <Purpose>` | `WWI - Daily ETL` |

SSIS package prefixes: `Master_` (orchestration), `EXT_ORA_`, `EXT_SQL_`,
`ING_FILE_`, `STG_`, `DQ_`, `REF_`, `DIM_`, `FACT_`, `AGG_`, `FIN_`, `SLS_`,
`INV_`, `PRC_`, `C360_`, `ERR_`, `MNT_`. Regional variants append the region
before the object: `DIM_NA_Load_Customer`, `FACT_EU_Load_Sale`.

## File conventions

- Every SQL file starts with a header comment block stating: the object, the
  deploy target database, the deploy order or dependencies, and who calls it.
- One primary object per SQL file. The file name is the object name
  (`etl.usp_LogError.sql`, `WWI_MDM.CUST_MASTER.sql`), except for grouped
  reference/seed scripts which are numbered (`03_seed_control_data.sql`).
- T-SQL uses `GO` batch separators, four-space indentation, `NVARCHAR` for text,
  explicit schema prefixes, and `SET NOCOUNT ON; SET XACT_ABORT ON;` in
  procedures.
- PL/SQL uses `/` terminators, `%TYPE` anchoring where sensible, and explicit
  exception blocks.
- Files end with a newline. No tabs. No trailing whitespace.

## The ETL control framework (foundation-owned, use it everywhere)

Deployed by `sqlserver/control/`. Every package and every data-moving procedure
integrates with it:

| Object | Purpose |
| --- | --- |
| `etl.Batch` / `etl.usp_StartBatch` / `etl.usp_EndBatch` | one nightly run |
| `etl.BatchStep` / `etl.usp_StartBatchStep` / `etl.usp_EndBatchStep` | one master-package section |
| `etl.PackageExecution` / `etl.usp_LogPackageStart` / `etl.usp_LogPackageEnd` | one package run, with timings, status and row counts |
| `etl.Watermark` / `etl.usp_GetWatermark` / `etl.usp_SetWatermark` | incremental windows (`Timestamp`, `NumericKey`, `DateWindow`) |
| `etl.RowCountAudit` / `etl.usp_LogRowCount` | source/target/reject counts per object |
| `etl.ErrorLog` / `etl.usp_LogError` | errors and warnings, safe to call from a CATCH block |
| `etl.RejectedRecord` / `etl.usp_LogRejectedRecord` | routed rejects, never silently dropped |
| `etl.Configuration` / `etl.ufn_GetConfigurationValue` | environment-aware settings |
| `etl.usp_AssertRowCountReconciliation` | static balance gate over a batch |

The standard package control flow, produced by `tools/ssisgen/patterns.py`:

```
[Init Variables] -> [Log Package Start] -> [Get Watermark]?
    -> <work: data flow(s) and/or Execute SQL Tasks>
    -> [Set Watermark]? -> [Log Row Counts] -> [Log Package Success]

OnError: [Log Error] -> [Mark Execution Failed]
```

## Generating SSIS packages

Do not hand-write `.dtsx` XML. Write a Python spec module inside your owned
`ssis/<folder>/` directory that imports from `tools/ssisgen` and emits the
packages listed for that folder in the catalog:

```python
import sys, os
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))
from ssisgen import Column, DataFlow, DataFlowTask, Expression, str_col, money_col, date_col
from patterns import new_package, log_package_start, get_watermark, set_watermark, \
    log_row_count, log_package_success, truncate, CONN_ORACLE, CONN_STAGING
```

Rules for generated packages:

- Every package in your folder listed in the catalog must exist, with exactly
  the catalog's name, and no package may exist that is not in the catalog.
- Packages must differ meaningfully. Vary source queries, transforms, load
  patterns, business rules and error handling by domain and region. Identical
  packages with a different name are a build failure, not a shortcut.
- Reference only the connection managers declared in
  `tools/ssisgen/project.py`.
- Emit the project scaffolding for your folder with
  `project.write_project(...)`.
- Commit both the generated `.dtsx` files and the spec module that produced
  them.

## Region divergence

`NA`, `EU`, `APAC` are not copies. At minimum they must differ in: tax
treatment (sales tax vs VAT vs GST), fiscal calendar, currency and FX handling,
address/postal standardisation, customer-consent and retention rules, and the
reference codes they translate. Encode the difference in the SQL and in the
package logic, not just in a parameter value.

## Documentation and inventories

Machine-readable inventories under `docs/inventories/` are generated by
`tools/inventory/build_inventory.py` (foundation-owned) from the repository
contents plus the catalog. Do not hand-edit them.

Every work package must add, to its own summary, anything that could not be
statically verified so it can be folded into
`docs/known-unvalidated-items.md`.

## Definition of done for a work package

1. All catalog objects for your paths exist, correctly named.
2. `python3 validation/static/run_all_checks.py` passes for your paths (XML
   well-formedness, naming, duplicates, catalog coverage, reference integrity).
3. No credentials, no Databricks artifacts, no runtime claims.
4. Work is committed and pushed to your assigned branch.
