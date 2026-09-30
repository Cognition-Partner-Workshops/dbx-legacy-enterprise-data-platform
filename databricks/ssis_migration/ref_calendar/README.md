# SSIS → Databricks migration — group `ref_calendar` (reference data & calendar)

Databricks-native reimplementation of the 27 SSIS packages that make up the WideWorldImporters conformed
reference layer: Oracle/SQL Server extracts, the treasury FX-override file feed, staging, the reference
dimensions, the generated `Date` and unknown-member rows and the SCD2 `City` dimension.

| | |
|---|---|
| Branch / PR | `devin/ssis-migration-ref_calendar` → `main` |
| Landing schema | `otterorders_migration.ssis_ref_calendar` (bronze_* / silver_* / gold_* / err_* / etl_*) |
| Evidence | `otterorders_migration.evidence.recon_results` (`branch = 'ssis_ref_calendar'`) |
| Workspace path | `/Workspace/Shared/ssis_migration/ref_calendar` |
| Job | `ssis_ref_calendar_reference_load` (serverless; 8 sequential tasks, last one is `recon`) |
| Volume | `otterorders_migration.ssis_ref_calendar.landing` (`inbound/treasury`, `archive/treasury`, `quarantine/treasury`) |

## Layout

```
databricks.yml                 bundle (target dev → /Workspace/Shared/ssis_migration/ref_calendar)
resources/ref_calendar_job.yml job ssis_ref_calendar_reference_load + landing volume
notebooks/run_step.py          thin runner: one notebook, `step` widget selects the library entrypoint
src/ref_calendar/
  config.py                    catalogs, schema, constants (reserved keys, FX rules, fiscal calendars)
  common.py                    Spark helpers (writeTable, rowHash, audit columns, legacy DW reader)
  extracts.py                  EXT_ORA_* and EXT_SQL_* packages → bronze_*
  fx_override.py               ING_FILE_FxOverride → bronze_file_fx_override / silver_fx_override_approved
  staging.py                   STG_Load_* packages → silver_stg_*
  reference.py                 REF_Load_{Currency,Geography,PaymentTerms,CodeTranslation,...} conformance
  dimensions.py                REF_Load_{Carrier,LoyaltyTier,...,DateDimension,UnknownMembers} → gold_*
  city_scd2.py                 DIM_Load_City (SCD2) → silver_stg_city / gold_dim_city
  seeds.py                     legacy seed grids (Carrier, Warehouse Site, channels, ...) copied from the SSIS estate
  recon.py                     evidence writer (one row per package per run_id)
samples/fx_override/           representative fx_override_*.csv files (uploaded to the volume by `seed_landing`)
tests/                         pytest on local Spark (SCD2, dedup, FX window/fill-forward, date dim, unknown members)
```

Implementation choice: **PySpark library + thin notebook, one job**. The 27 packages share lookups
(currency/country conformance, code translation, unknown members) and the legacy masters run them strictly
sequentially, so one job with one task per SSIS phase keeps the dependency order explicit and re-runnable.
DLT was not used because half of the group is generation/SCD2 logic that needs explicit key allocation and
MERGE-style control, which is awkward to express declaratively. Python identifiers are camelCase (org
convention), tables/columns snake_case.

## Run it

```bash
export DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST="$DATABRICKS_DEMO_HOST" DATABRICKS_TOKEN="$DATABRICKS_DEMO_TOKEN"
cd databricks/ssis_migration/ref_calendar
ruff check . && python -m pytest -q
databricks bundle validate -t dev
databricks bundle deploy   -t dev --var "git_sha=$(git rev-parse HEAD)"
databricks bundle run      -t dev ssis_ref_calendar_reference_load
```

Job parameters: `git_sha` (stamped on evidence), `fx_window_from` / `fx_window_to` (override the FX
watermark window, ISO dates, half-open). Task order: `seed_landing → extracts → fx_override → reference →
staging → dimensions → city → recon`.

## Package → Databricks artifact mapping

Verdicts are from the latest `recon` run (see "Evidence" below). Legacy source objects are the tables the SSIS
package writes on the host; `PARTIAL` means that legacy table is empty on the shared baseline, so the target was
reconciled against an expectation derived from the live source with the package's own logic
(`"baseline":"source_derived"` in `checks`).

| Package | Load type | Source (federated) | Legacy target | Databricks target | Entrypoint | Verdict |
|---|---|---|---|---|---|---|
| EXT_ORA_CodeTranslation | full | `wwi_legacy_oracle.wwi_ref.code_translation` | `Staging.raw.OracleCustomerMaster` | `bronze_oracle_code_translation` | `extracts.runExtOraCodeTranslation` | PARTIAL |
| EXT_ORA_Currency | full | `wwi_ref.currency_code` | `raw.OracleCurrency` | `bronze_oracle_currency` | `extracts.runExtOraCurrency` | PARTIAL |
| EXT_ORA_Geography | full | `wwi_ref.city_ref/country_ref/region_ref/postal_ref` (the `V_GEOGRAPHY_EXTRACT` view, inlined) | `raw.OracleGeography` | `bronze_oracle_geography` (`record_kind='ORAGEO'`) | `extracts.runExtOraGeography` | PARTIAL |
| EXT_ORA_PaymentTerms | full | `wwi_fin.payment_terms` | `raw.OraclePaymentTerms` | `bronze_oracle_payment_terms` | `extracts.runExtOraPaymentTerms` | PARTIAL |
| EXT_ORA_TaxRate | full | `wwi_fin.tax_rate` ⋈ `tax_jurisdiction` | `raw.OracleTaxRate` | `bronze_oracle_tax_rate` | `extracts.runExtOraTaxRate` | PARTIAL |
| EXT_ORA_FxRateDaily | date_window (watermark) | `wwi_ref.fx_rate_daily` | `raw.OracleFxRate` | `bronze_oracle_fx_rate`, `etl_watermark` | `extracts.runExtOraFxRateDaily` | PARTIAL |
| EXT_SQL_Cities | full | `wwi_legacy_oltp.Application.Cities/StateProvinces/Countries` | `raw.OracleGeography` | `bronze_oracle_geography` (`record_kind='OLTPCITY'`) | `extracts.runExtSqlCities` | PARTIAL |
| EXT_SQL_PaymentMethods | full | `Application.PaymentMethods` + `_Archive` | `raw.SqlInvoice` | `bronze_sql_payment_methods` | `extracts.runExtSqlPaymentMethods` | PARTIAL |
| EXT_SQL_TransactionTypes | full | `Application.TransactionTypes` + `_Archive` | `raw.SqlInvoice` | `bronze_sql_transaction_types` | `extracts.runExtSqlTransactionTypes` | PARTIAL |
| STG_Load_Currency | truncate_reload | `bronze_oracle_currency`, `bronze_oracle_fx_rate`, `silver_fx_override_approved` | `stg.Currency`, `stg.FxRate` | `silver_stg_currency`, `silver_stg_fx_rate` | `staging.runStgLoadCurrency` | PARTIAL |
| STG_Load_Geography | truncate_reload | `bronze_oracle_geography` | `stg.Geography` | `silver_stg_geography` | `staging.runStgLoadGeography` | PARTIAL |
| STG_Load_TaxAndTerms | truncate_reload | `bronze_oracle_payment_terms`, `bronze_oracle_tax_rate` | `stg.PaymentTerms`, `stg.TaxRate` | `silver_stg_payment_terms`, `silver_stg_tax_rate` | `staging.runStgLoadTaxAndTerms` | PARTIAL |
| ING_FILE_FxOverride | file_ingest | volume `landing/inbound/treasury/fx_override_*.csv` | `raw.FileFxOverride` | `bronze_file_fx_override`, `silver_fx_override_approved`, `err_rejected_file_row`, `etl_file_registry` | `fx_override.runIngFileFxOverride` | PARTIAL |
| REF_Load_Carrier | full_refresh | seed grid (`seeds.py`) | `Dimension.Carrier` | `gold_dim_carrier` | `dimensions.runRefLoadSeededDimensions` | PARTIAL |
| REF_Load_CodeTranslation | full_refresh | `bronze_oracle_code_translation` | `etl.Configuration` / `ref.*` | `gold_ref_code_translation`, `gold_ref_unmapped_source_code` | `reference.runRefLoadCodeTranslation` | PARTIAL |
| REF_Load_Currency | full_refresh | `silver_stg_currency`, `silver_stg_fx_rate` | `Dimension.Currency` | `gold_dim_currency`, `gold_ref_fx_rate_daily` | `reference.runRefLoadCurrency` | PARTIAL |
| REF_Load_DateDimension | full_refresh (generated) | none — generated 2013-01-01..2016-12-31 + 1900-01-01/02 | `Dimension.Date` | `gold_dim_date` | `dimensions.runRefLoadDateDimension` | PASS |
| REF_Load_Geography | full_refresh | `silver_stg_geography` | `Dimension.Geography` (+ Country/Region) | `gold_dim_geography`, `silver_ref_country`, `silver_ref_region` | `reference.runRefLoadGeography` | PARTIAL |
| REF_Load_LoyaltyTier | full_refresh | seed grid | `Dimension.Loyalty Tier` | `gold_dim_loyalty_tier` | `dimensions.runRefLoadSeededDimensions` | PARTIAL |
| REF_Load_PaymentMethod | full_refresh | `bronze_sql_payment_methods` | `Dimension.Payment Method` | `gold_dim_payment_method` | `dimensions.runRefLoadPaymentMethod` | PASS |
| REF_Load_PaymentTerms | full_refresh | `silver_stg_payment_terms`, `silver_stg_tax_rate` | `Dimension.Payment Terms` | `gold_dim_payment_terms`, `gold_ref_tax_rate` | `reference.runRefLoadPaymentTerms` | PARTIAL |
| REF_Load_ReturnReason | full_refresh | seed grid | `Dimension.Return Reason` | `gold_dim_return_reason` | `dimensions.runRefLoadSeededDimensions` | PARTIAL |
| REF_Load_SalesChannel | full_refresh | seed grid | `Dimension.Sales Channel` | `gold_dim_sales_channel` | `dimensions.runRefLoadSeededDimensions` | PARTIAL |
| REF_Load_TransactionType | full_refresh | `bronze_sql_transaction_types` | `Dimension.Transaction Type` | `gold_dim_transaction_type` | `dimensions.runRefLoadTransactionType` | PASS |
| REF_Load_UnknownMembers | full_refresh (generated) | `wwi_legacy_dw.Integration.DimensionKeyRegistry` (35 rows) | `Dimension.*` (-1/-2/0 rows) | `gold_dim_unknown_member` + the reserved rows in every `gold_dim_*` | `dimensions.runRefLoadUnknownMembers` | PARTIAL |
| REF_Load_WarehouseSite | full_refresh | `wwi_legacy_staging.stg.StockMovement` site columns → fallback `wwi_legacy_oltp.Warehouse.WarehouseSites` (see deviations) | `Dimension.Warehouse Site` | `gold_dim_warehouse_site` | `dimensions.runDimensions` (`buildDimWarehouseSite`) | PARTIAL |
| DIM_Load_City | SCD2 | `bronze_oracle_geography` (`OLTPCITY`), `silver_ref_country` | `Dimension.City` (116,297 rows) | `silver_stg_city`, `gold_dim_city` | `city_scd2.runDimLoadCity` | FAIL |

## Design decisions

- **Federation first.** Every source read is a `SELECT` from `wwi_legacy_oracle` / `wwi_legacy_oltp` /
  `wwi_legacy_dw`. The only `remote_query(...)` use is in `common.readLegacyDw` for the *legacy* DW dimensions
  whose names contain spaces (`Dimension.[Payment Method]` etc.) which the foreign catalog cannot address
  directly; it is read-only and only used for reconciliation. No JDBC.
- **Oracle views are inlined.** `WWI_REF.V_GEOGRAPHY_EXTRACT` (and the other `v_*_extract` views the packages
  select from) are not exposed through the foreign catalog, so `extracts.py` reproduces the view SQL from
  `oracle/` over the base tables.
- **Watermark (EXT_ORA_FxRateDaily).** `etl.usp_GetWatermark/usp_SetWatermark` are reproduced in
  `etl_watermark` in our schema: the window is `[last watermark, today+1)` — half open, `RATE_DT >= from AND
  RATE_DT < to`, rate types `SPOT/CORP/AVG` only — and the first run back-fills from the oldest rate on the source
  exactly as the seeded watermark row did. The USD-triangulated EUR/SGD cross rates the package unions in are
  derived with the same expression.
- **File ingest (ING_FILE_FxOverride).** `read_files`-style CSV load over the UC volume, Windows-1252, header,
  `FXO` detail rows and one `CTL` control record per file. Count + hash-total mismatch quarantines the whole
  file (moved to `quarantine/treasury`), otherwise the file is archived (`archive/treasury`). Four-eyes,
  ticket-present and ±500 bp tolerance against the latest published SPOT rate drive approve/refuse exactly
  as the conditional split in the package; refused rows go to `err_rejected_file_row`. Two sample files are
  committed under `samples/fx_override/` (one reconciles, one is deliberately quarantined) and copied into
  `inbound/treasury` by the `seed_landing` task on every run.
- **Truncate/reload staging** is `overwrite` of the `silver_stg_*` tables with `overwriteSchema`.
- **Full-refresh reference dims** rebuild `gold_dim_*` from scratch each run, then `dimensions.finaliseDimension`
  prepends the reserved rows (`-2` Not applicable, `-1` Unknown, and `0` where the legacy DDL uses it) so the
  key ranges match `Integration.DimensionKeyRegistry`.
- **Date dimension** is generated, not extracted: 1,461 calendar days 2013-01-01..2016-12-31 plus the two reserved
  rows `1900-01-01` (unknown) and `1900-01-02` (not applicable) = 1,463 rows, WWI fiscal year starting 1 November,
  ISO week columns and the regional (EU/NA/APAC) fiscal columns from `usp_GenerateDateDimension`.
- **Unknown members** are generated from the 35-row `Integration.DimensionKeyRegistry` (read from the legacy DW,
  which we do not own) with the same `-1` / `-2` / `0` conventions per dimension; `gold_dim_unknown_member`
  is the flat registry of what was seeded where.
- **City SCD2** (`city_scd2.applyCityScd2`): hash over the ten Type-2 attributes, close the current row at the
  new `valid_from` and insert a new version; unchanged rows are untouched; late-arriving cities that were
  inserted as inferred members are promoted in place (same key, `is_inferred_member` cleared); historical
  versions arriving in the same batch are back-filled as closed rows; keys are allocated as
  `max(city_key)+row_number()`; reserved keys `-2/-1/0` are always preserved. Dedup keeps the most populous
  version per (country, state, city, effective ts), then the greatest source row.
- **Evidence** (`recon.py`): one `run_id` per run, one row per package, `checks` = row_count + checksum
  (`sum(cast(xxhash64(concat_ws('|', <business cols>)) as decimal(38,0)))` over canonicalised strings — the
  cast avoids the bigint overflow ANSI mode raises when summing 64-bit hashes) + a null-rate check where a key
  column exists. Populated legacy tables → direct comparison; empty legacy tables → `PARTIAL` with
  `"baseline":"source_derived"`.

## Deliberate deviations from the SSIS packages

- Row-by-row Lookup components, OLE DB batch sizes, `FastLoad` options and the per-row `RowCount`
  transformations are replaced by set-based joins; the counts are still logged to `etl_row_count_log`.
- `raw.SqlInvoice` (the inventory target of `EXT_SQL_PaymentMethods` / `EXT_SQL_TransactionTypes`) has no
  `RecordKind` column on the host, so those two extracts land in their own bronze tables instead of a
  shared multiplexed raw table.
- The packages' `err.*` reject tables and `etl.FileRegistry` live in our schema (`err_*`, `etl_*`) rather than
  in `WideWorldImporters_Staging`, which is a read-only baseline.
- `REF_Load_CodeTranslation` writes `gold_ref_code_translation` (the `ref.*` code-translation content) instead
  of `etl.Configuration`; the inventory's `etl.Configuration` target is the package's *configuration read*,
  not a data output.
- File moves use the volume (`archive/`, `quarantine/`) instead of `inbound/processed` + `inbound/failed`
  holding folders drained by `MNT_Archive_ProcessedFiles` / `ERR_Quarantine_BadFiles` (owned by another group).
- Success/failure e-mail tasks (`Send Mail`) are not reproduced — Databricks job notifications are the
  equivalent.
- `REF_Load_WarehouseSite` aggregates `stg.StockMovement` by `WarehouseSiteId/Code/Name/CountryCode/PostalCode`,
  but the deployed `stg.StockMovement` DDL only carries `WarehouseCode` (and the table is empty). When the site
  columns are absent, `dimensions.warehouseSiteSource` lists the sites from OLTP `Warehouse.WarehouseSites`
  (country = first two characters of the site code, exactly as the package derives it) with zero movements.
- Serverless/Unity Catalog substitutions with no semantic effect: `_metadata.file_path` instead of
  `input_file_name()`, no `DataFrame.cache()`, federated `char(n)`/`varchar(n)` columns landed as plain `string`
  (`common.stripCharTypes`), and reserved/inferred City rows built through string timestamps so the
  `9999-12-31` high date survives the Arrow conversion on Spark Connect.

## Open questions

- `Dimension.City` on the host has 116,297 rows, i.e. multiple historical versions per WWI city that came from
  the original WWI DW ETL (`Integration.City_Staging` + `MigrateStagedCityData`), not from `Application.Cities`
  as it stands today. Our SCD2 load over the live OLTP snapshot cannot recreate history that is no longer in
  the source; the evidence records the row-count and checksum gap rather than claiming parity.
- `STG_Load_TaxAndTerms` translates `TERMS_CODE` through the `PAYMENT_TERMS` crosswalk with a redirect-on-no-match
  lookup. The steward grid only maps `NET30`, `NET60`, `2/10NET30` and `EOM`, so 28 of the 30 Oracle terms
  (`N30`, `2P10N30`, `N45`, …) are redirected to `err_rejected_lookup_failure` exactly as the package does and
  only two terms reach `gold_dim_payment_terms`. Faithful, but almost certainly a gap in the legacy crosswalk.
- Whether `EXT_ORA_Geography` and `EXT_SQL_Cities` were intended to share `raw.OracleGeography` (inventory) or
  the generator mislabelled the target; we keep them in one bronze table discriminated by `record_kind`.

## Blocked / not done

Nothing is blocked: all 27 packages are implemented, deployed and executed. Items that could not be brought to
`PASS` are data-baseline facts recorded in the evidence rather than defects in the migration:

- The legacy `Dimension.*` reference tables (`Currency`, `Geography`, `Payment Method`, `Payment Terms`, `Carrier`,
  `Loyalty Tier`, `Return Reason`, `Sales Channel`, `Transaction Type`, `Warehouse Site`) and the `raw.*`/`stg.*`
  staging tables are empty on the host, so those packages are `PARTIAL` against a source-derived expectation.
- `Dimension.City` carries 116,297 historical versions that are not recoverable from the live OLTP source
  (see Open questions); `DIM_Load_City` therefore cannot reach `PASS`.
- `Dimension.Date` is the one populated legacy target and is compared directly (1,463 rows).

## Evidence

```sql
SELECT unit, verdict, summary
FROM otterorders_migration.evidence.recon_results
WHERE branch = 'ssis_ref_calendar'
  AND run_id = (SELECT max_by(run_id, run_at) FROM otterorders_migration.evidence.recon_results WHERE branch = 'ssis_ref_calendar')
ORDER BY unit;
```

Latest run (job `ssis_ref_calendar_reference_load`, all eight tasks `SUCCESS`): 27 rows, one per package —
**3 PASS, 23 PARTIAL, 1 FAIL, 0 NOT_APPLICABLE**.

| Verdict | Packages |
|---|---|
| PASS | `REF_Load_DateDimension` (1,463 rows, checksum match), `REF_Load_PaymentMethod`, `REF_Load_TransactionType` (the only legacy reference dimensions populated on the host) |
| PARTIAL | the 23 packages whose legacy `raw.*` / `stg.*` / `Dimension.*` target is empty on the host; each is compared with `"baseline":"source_derived"` against an expectation built with the package's own logic (row count and checksum both match the expectation) |
| FAIL | `DIM_Load_City` — legacy `Dimension.City` has 116,297 rows of WWI temporal history; the live source (`Application.Cities` + 28 `Cities_Archive` rows) yields 37,971; the checksum and current-row overlap are recorded in `checks` |

Every row carries `unit_type='ssis_package'`, `branch='ssis_ref_calendar'`, `actor='devin:ssis_ref_calendar'`,
`harness_version='ssis-migration-v1'` and the `git_sha` of the PR head that produced it.
