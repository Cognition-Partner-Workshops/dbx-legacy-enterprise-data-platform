# SSIS -> Databricks migration

The legacy estate (`ssis/`, `sqlserver/`, `oracle/`) is read-only input. Each SSIS project is
migrated by one session into `databricks/<NN_project>/` (a Databricks Asset Bundle) and documented
in `docs/migration/<NN_project>-package-mapping.md`. This page is the index, the naming contract
everybody codes against, and the merge order.

## Mapping documents (17)

| # | SSIS project (`ssis/`) | Bundle (`databricks/`) | Mapping doc |
|---|---|---|---|
| 00 | `00_orchestration` (9 masters) + shared control layer | `00_orchestration/`, `common/` | [00_orchestration-package-mapping.md](00_orchestration-package-mapping.md) |
| 01 | `01_oracle_extract` | `01_oracle_extract/` | `01_oracle_extract-package-mapping.md` |
| 02 | `02_sqlserver_extract` | `02_sqlserver_extract/` | `02_sqlserver_extract-package-mapping.md` |
| 03 | `03_file_ingestion` | `03_file_ingestion/` | `03_file_ingestion-package-mapping.md` |
| 04 | `04_staging` | `04_staging/` | `04_staging-package-mapping.md` |
| 05 | `05_data_quality` | `05_data_quality/` | `05_data_quality-package-mapping.md` |
| 06 | `06_reference_data` | `06_reference_data/` | `06_reference_data-package-mapping.md` |
| 07 | `07_dimensions` | `07_dimensions/` | `07_dimensions-package-mapping.md` |
| 08 | `08_facts` | `08_facts/` | `08_facts-package-mapping.md` |
| 09 | `09_aggregates` | `09_aggregates/` | `09_aggregates-package-mapping.md` |
| 10 | `10_finance` | `10_finance/` | `10_finance-package-mapping.md` |
| 11 | `11_sales` | `11_sales/` | `11_sales-package-mapping.md` |
| 12 | `12_inventory` | `12_inventory/` | `12_inventory-package-mapping.md` |
| 13 | `13_procurement` | `13_procurement/` | `13_procurement-package-mapping.md` |
| 14 | `14_customer_360` | `14_customer_360/` | `14_customer_360-package-mapping.md` |
| 15 | `15_error_handling` | `15_error_handling/` | `15_error_handling-package-mapping.md` |
| 99 | `99_maintenance` | `99_maintenance/` | `99_maintenance-package-mapping.md` |

Documents other than 00 are delivered by their own session's PR (`[dbx-migration <NN>] ...`) and
appear here as they merge.

## Naming contract

* **Catalog**: bundle variable `catalog`, default `wwi_${bundle.target}` (`wwi_dev`, `wwi_prod`).
  Every notebook takes `catalog` as a job parameter / widget and builds names with
  `naming.table(catalog, schema, table)` - never a hard-coded catalog.
* **Schemas**: `bronze` (raw extracts), `silver` (stg / work / err / ref / Integration), `gold`
  (dimensions, facts, aggregates, Report views, marts), `etl` (control framework).
* **Tables**: snake_case of the legacy object (spaces -> `_`), prefixed by the legacy schema:

  | legacy | Delta |
  |---|---|
  | `raw.X` | `bronze.raw_x` |
  | `stg.X` / `work.X` / `err.X` / `ref.X` | `silver.stg_x` / `silver.work_x` / `silver.err_x` / `silver.ref_x` |
  | `Integration.X` | `silver.int_x` |
  | `Dimension.X` / `Fact.X` / `Aggregate.X` | `gold.dim_x` / `gold.fact_x` / `gold.agg_x` |
  | `Report.X` (views) | `gold.rpt_x` |
  | `etl.X` | `etl.<snake_case x>` - e.g. `etl.PackageExecution` -> `etl.package_execution` |

  `naming.legacyToDelta(catalog, "Dimension.Customer")` implements the table above;
  `naming.translateLegacyReferences(catalog, sql)` rewrites a legacy SQL text.
* **Columns keep the legacy PascalCase names** (`BatchId`, `StepName`, `RowsInserted`, ...) so the
  baseline queries in `validation/runtime/*.sql` port with minimal edits.
* **Control tables** (`databricks/common/sql/00_etl_control_tables.sql`): one Delta table per legacy
  `etl.*` table; `IDENTITY` -> `GENERATED ALWAYS AS IDENTITY` (BIGINT), `CHECK` -> Delta CHECK
  constraint, defaults preserved, PK/FK/UNIQUE/index intent recorded in the table comment.
  `etl.row_count_log` (contract name) is a view over `etl.row_count_audit` (legacy name).
* **Operational views**: `etl.v_batch_status`, `etl.v_package_execution_history`, `etl.v_slow_packages`,
  `etl.v_row_count_reconciliation`, `etl.v_reject_summary`, `etl.v_watermark_status`, `etl.v_recent_errors`
  (legacy `etl.vw_*` views; see `databricks/common/views/`).
* **Jobs**: one bundle per project named `wwi_<NN_project>` with one job `wwi_<NN_project>` whose
  task keys are the legacy package names. String job parameters: `BatchId` (`"0"`), `BusinessDate`
  (`yyyy-MM-dd`), `ReloadFullHistory` (`"False"`), `EnvironmentCode` (`"DEV"`), `RestartFromStep`
  (`""`), `catalog`. The master jobs (`wwi_00_master_*`) call these jobs with `only=[<Package>]`,
  so the task-key contract is what wires the estate together.
* **Secrets**: `{{secrets/wwi/<key>}}` only.

## Shared control layer `dbx_etl_common`

`databricks/common/dbx_etl_common/` is the only implementation of `sqlserver/control/procedures/*.sql`
(`control.startBatch` ... `control.purgeControlHistory`, `params.getJobParams`, `naming.table`).
The public signatures are listed in `databricks/common/README.md`; consumers import them, never copy
them. Reference the wheel from a consumer bundle as a library
(`libraries: - whl: ../common/dbx_etl_common/dist/dbx_etl_common-0.1.0-py3-none-any.whl`, or an
`environments` dependency for serverless) or `%pip install` it from the common bundle's workspace
path; both are shown in `databricks/common/README.md`. Build with `cd databricks/common/dbx_etl_common
&& python -m build --wheel`.

## Validation that must stay green

```
python3 validation/static/run_all_checks.py     # forbidden-content skips databricks/ and docs/migration/ only
python3 validation/checks/run_deep_checks.py
cd databricks/common/tests && python -m pytest  # local PySpark + delta-spark
cd databricks/<NN_project> && databricks bundle validate -t dev
```

## Merge order

Coordinated by the parent session; each PR only has to be mergeable against `main` + 00.

1. **00** - `databricks/common` (control tables, wheel), `databricks/00_orchestration`, this index,
   the `validation/static/run_all_checks.py` forbidden-content exemption.
2. **01-06** - extracts, file ingestion, staging, data quality, reference data.
3. **07-09** - dimensions, facts, aggregates.
4. **10-14** - finance, sales, inventory, procurement, customer 360 marts.
5. **15, 99** - error handling, maintenance.

Deploy order mirrors it: `wwi_00_common` (job `wwi_00_control_bootstrap` creates `etl` and seeds it)
before any project bundle, and the project bundles before `wwi_00_orchestration` (the masters look
the project jobs up by name at run time).
