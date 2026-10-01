# Wave 1 Databricks design: Finance AP

Design for migrating the 20-package Finance AP slice in
`config/waves/wave-01-finance-ap.yaml` to Databricks. Inputs: the closure check
(`docs/waves/wave-01-finance-ap-closure.md`), the estate lineage
(`docs/inventories/ssis-sql-lineage.json`) and the shared-plumbing inventory
(`docs/dependency-maps/shared-plumbing.md`). Static analysis only; nothing here
was verified against a live SQL Server or Oracle.

## 1. Slice boundary and what leaks

| Finding | Count | Decision |
| --- | ---: | --- |
| Upstream leak (`STG.FXRATE` written by `STG_Load_Currency`) | 1 | Freeze as a **synced input**: `STG_Load_Currency` is estate-wide plumbing shared with Sales/GL. Wave 1 reads `ref.fx_rate_daily` from a read-only Lakehouse Federation sync of the DW until the reference wave lands. |
| Co-written input (`WORK.CURRENCYCONVERSIONSCRATCH`) | 1 | Not migrated. It is a scratch table; the AP staging load gets its own temp view. Nothing else needs to change. |
| Orphan inputs (read by slice, written by no package) | 9 | **Verify on the live server before build of silver/gold.** Most look like views or pre-2018 manual loads (`STG.APINVOICEHEADER`, `STG.APPAYMENTRUN`, `STG.SUPPLIERPAYMENT` ...). Bronze does not depend on them; silver does. Tracked as an open item. |
| Shared conformed inputs (`Dimension.Date`, `Dimension.Currency`, `REF.CODECROSSWALK` ...) | 11 | Consume from the shared `ref` / `dim` schemas as synced read-only copies (same mechanism as `STG.FXRATE`). |
| Outputs consumed outside the slice (`Dimension.Supplier`, `Fact.Supplier Payment` ...) | 9 | **Coexistence sync back to SQL Server** during parallel run: Delta -> SQL Server via JDBC job after each successful gold load, dropped at cutover. |
| Outputs co-written from outside (`RAW.SQLINVOICE`, `INTEGRATION.DIMENSIONLOADAUDIT`, scratch) | 3 | `RAW.SQLINVOICE`: split ownership - the AP writer (`EXT_SQL_SupplierTransactions`) moves to its own bronze table; the sales writers stay. `DIMENSIONLOADAUDIT` is plumbing and is replaced by job run metadata. |
| Unresolved procedures (`Integration.usp_PostApAging`, `usp_PostWithholdingTax`, `usp_RefreshApAgingSummary`) | 3 | Bodies are not in the repo. Pull DDL from the live server before designing the month-end notebook; treat as a blocker for gold, not for bronze. |

Anything in the "pull in for fully closed upstream" list (69 packages) is **not**
pulled in: they are the reference and customer-dimension waves. Closing wave 1
by consumption (synced inputs) rather than by ownership keeps it at 20 packages.

## 2. Unity Catalog layout

Catalog `de_demo_workspace` in the demo workspace; `wwi_finance` in a real
engagement. One schema per legacy schema so the like-for-like layer needs no
naming mapping; medallion refactoring is a later, separately named change.

| Schema | Holds | Legacy equivalent |
| --- | --- | --- |
| `wave1_landing` | Volume `oracle_extract` (`oracle/<SCHEMA>/<TABLE>/*.dat` + `_contracts`) | Oracle extract files / `dtexec` pull |
| `wave1_bronze_wwi_fin` | `ap_invoice_hdr`, `ap_invoice_line`, `ap_invoice_hold`, `ap_payment`, `ap_payment_apply`, `ap_aging_snapshot`, `payment_terms`, `tax_rate`, `cost_center`, `_ingest_log`, `_row_counts` | `raw.OracleAp*` |
| `wave1_bronze_wwi_mdm` | `supp_master`, `supp_address`, `supp_bank_account` | `raw.OracleSupplier*` |
| `wave1_bronze_wwi_ref` | `payment_method_ref`, `fx_rate_daily` | `raw.OracleFxRate`, `raw.OraclePaymentMethod` |
| `wave1_silver` (next) | `stg_supplier`, `stg_ap_invoice`, `stg_ap_invoice_line`, `stg_payment_terms`, `stg_tax_rate`, `stg_cost_center` + `err_*` quarantine | `stg.*`, `err.*` |
| `wave1_gold` (next) | `dim_supplier`, `fact_supplier_payment`, `fact_supplier_transaction`, `fin_ap_aging`, `fin_withholding_tax` | `Dimension.Supplier`, `Fact.Supplier *`, `Integration.*` |
| `ref` / `dim` (shared, read-only) | federated or synced copies of the 11 shared conformed inputs | `REF.*`, `Dimension.Date/Currency/...` |
| `etl_control` (shared) | `watermark`, `batch`, `rejected_record` Delta tables | `ETL.*` |

Grants: `wave1_*` owned by the migration service principal; finance analysts get
`SELECT` on gold only; `landing` volume `WRITE VOLUME` only to the extract
principal.

## 3. What becomes a pipeline, SQL, or a notebook

| Legacy packages | Target form | Why |
| --- | --- | --- |
| `EXT_ORA_*` (9 extracts), `EXT_SQL_SupplierTransactions` | **Lakeflow Job, PySpark, serverless** (`wave1_finance_ap_bronze`, shipped) | Append-only file ingest with a swappable `SourceReader`; no transformation, so declarative pipeline adds nothing and would hide the Oracle seam. |
| `STG_Load_Supplier`, `STG_Load_TaxAndTerms`, `STG_Load_CostCenter`, `STG_Load_ApInvoice` | **Lakeflow Declarative Pipeline (SQL)** | Truncate-reload staging with lookups and code-crosswalk joins maps 1:1 onto materialized views; expectations replace the `err.*` reject inserts. |
| `DQ_Supplier_Screen` | **Expectations inside the silver pipeline** (`EXPECT ... ON VIOLATION DROP ROW` + quarantine table) | The screen is 12 rule rows in `ETL.usp_EvaluateDataQualityRules`; each rule becomes one expectation, failing rows land in `err_supplier`. |
| `DIM_Load_Supplier` (SCD2) | **Pipeline `APPLY CHANGES INTO ... STORED AS SCD TYPE 2`** | Replaces the hand-written MERGE + unknown-member helper; `-1` unknown member seeded by a one-off SQL task. |
| `FACT_Load_SupplierPayment`, `FACT_Load_SupplierTransaction` | **SQL tasks in a Lakeflow Job** (`MERGE`) on a SQL warehouse | Pure T-SQL today; keep it SQL so recon can diff statement-for-statement. |
| `FIN_Load_ApAging`, `FIN_Load_WithholdingTax` | **Notebook task** (PySpark + SQL) gated on `gl_period_status` | Month-end semantics: period gate, region-specific withholding (APAC), calls the 3 unresolved `Integration.usp_*` procs whose bodies must be recovered first. |
| `ETL.usp_Log*`, `usp_*Watermark`, `usp_AssertRowCountReconciliation` | **Dropped / replaced** | Run metadata comes from `system.lakeflow`; watermarks live in `etl_control.watermark`; row-count assertions become a recon task after each layer. |

Orchestration: one job per layer (`bronze`, `silver` pipeline, `gold`,
`month_end`) with `run_job_task` chaining, matching the legacy `02_extract ->
04_staging -> 06_dimension -> 07_fact -> 09_finance_close` phases. The
month-end job is scheduled paused, as `Master_Finance_Close` is today.

## 4. Where the DQ screens go

1. **Bronze**: no screens. Types are asserted by the contract (`_rescued_data`
   is not used because the extract is fixed-width-delimited with a known
   contract); `_ingest_log` and `_row_counts` give per-file counts to reconcile
   against the extract manifest.
2. **Silver pipeline**: every `ETL.usp_LogRejectedRecord` call site becomes an
   expectation. Hard rules (`supp_id` not null, FK to supplier, valid currency)
   `DROP ROW` into `err_*`; soft rules (missing tax registration, stale
   address) `WARN` and are counted.
3. **Gold**: `usp_AssertRowCountReconciliation` becomes a recon task comparing
   fact row counts and sum(gross_amt) against silver, failing the job on drift.
4. **Estate recon harness** remains the merge authority for every unit.

## 5. Bronze, as shipped

`databricks/wave1_finance_ap/` is a Databricks Asset Bundle:

- `databricks.yml` - variables for catalog, landing schema/volume, bronze
  prefix, `source_kind`, and the ordered table list; single `demo` target.
- `resources/unity_catalog.yml` - 4 schemas + the landing volume.
- `resources/wave1_bronze.job.yml` - serverless job: `plan` -> `for_each`
  `load_bronze_table` (concurrency 5) -> `publish_row_counts`.
- `src/wave1_bronze/sources.py` - the swappable read path. `SourceReader.read(table)`
  is the only thing bronze calls. `VolumeDelimitedReader` is live;
  `OracleJdbcReader` and `LakehouseFederationReader` are the real-source seams
  and raise until a connection exists. Selecting one is the `source_kind` variable.
- `src/wave1_bronze/load_bronze.py` - appends the typed frame plus
  `_source_kind`, `_source_ref`, `_source_file`, `_ingest_ts`, `_run_id`; file-level
  idempotency through `_ingest_log`.
- `scripts/land_synthetic_source.py` - runs the repo's deterministic `wwigen`
  generator at `--scale medium`, writes one column contract per table from the
  same registry (Oracle `NUMBER(p,s)` -> `decimal(p,s)`, `NUMBER(n)` -> `bigint`,
  `DATE` -> `date`, `VARCHAR2` -> `string`), and copies files + contracts into the
  volume with the CLI.

### Volume assumptions

`medium` is the generator's integration scale: 1,200 suppliers, 74,000 AP
invoices (~2.4 lines each), 90,000 payments, 7 years of daily FX. The `large`
scale (1.15M invoices, 2.1M payments) is the production-volume design target
and has never been run end to end in this repo; bronze is scale-agnostic, so
re-landing at `--scale large` is a rerun of the landing script, not a code change.
`WWI_REF.CURRENCY_CODE` is excluded because the generator emits it empty at every
scale; wave 1 needs only `FX_RATE_DAILY` from the currency reference.
