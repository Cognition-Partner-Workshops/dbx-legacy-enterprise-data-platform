# SSIS → Databricks: `sales_performance` (group 5/10)

Databricks-native reimplementation of the 17 SSIS packages that make up the sales performance /
commissions / partner sales / sales aggregates group of the WideWorldImporters estate.

* Landing schema: `otterorders_migration.ssis_sales_performance` (all Delta tables, the `landing`
  volume and the `report_vw_*` views live here; nothing is written elsewhere except append-only
  evidence rows in `otterorders_migration.evidence.recon_results`).
* Bundle: `databricks.yml` + `resources/*.yml`, deployed with `databricks bundle deploy -t dev` to
  `/Workspace/Shared/ssis_migration/sales_performance`.
* Jobs: `ssis_sales_performance_pipeline` (all 17 packages in dependency order, ends with the
  `recon` task) and `ssis_sales_performance_agg_refresh` (standalone month-end refresh of one
  `AGG_Refresh_*` aggregate).
* Sources: Lakehouse Federation catalogs `wwi_legacy_oltp`, `wwi_legacy_staging`, `wwi_legacy_dw`
  (read-only). Identifiers containing spaces (`Fact.Sale`, `Dimension.Stock Item`, …) are read
  through `remote_query(...)` because the federated 3-level names fail to parse for them; see
  `src/sales_performance/common.py`.

## Package → artifact mapping

| # | SSIS package | Load type | Legacy source → target | Databricks artifact (task → library) | Delta target(s) | Verdict |
|---|---|---|---|---|---|---|
| 1 | `ING_FILE_PartnerSales_NA` | file_ingest | `partner_sales_na_*.csv` → `raw.FilePartnerSales` | `ing_file_partner_sales_na` → `partner_files.ingestFeed("NA")` | `bronze_raw_file_partner_sales`, `bronze_rejected_file_row`, `bronze_inbound_file_register` | PARTIAL (source_derived) |
| 2 | `ING_FILE_PartnerSales_EU` | file_ingest | `partner_sales_eu_*.csv` → `raw.FilePartnerSales` | `ing_file_partner_sales_eu` → `partner_files.ingestFeed("EU")` | same | PARTIAL (source_derived) |
| 3 | `ING_FILE_PartnerSales_APAC` | file_ingest | `partner_sales_apac_*.txt` → `raw.FilePartnerSales` | `ing_file_partner_sales_apac` → `partner_files.ingestFeed("APAC")` | same | PARTIAL (source_derived) |
| 4 | `STG_Load_PartnerSale` | incremental_append | `raw.FilePartnerSales` → `stg.PartnerSale` | `stg_load_partner_sale` → `partner_staging.runStaging` | `silver_partner_sale`, `silver_partner_sale_reject`, `etl_watermark` | PARTIAL (source_derived) |
| 5 | `SLS_NA_Load_Commission` | business_rule | `stg.SaleLine` → `Fact.Sale` | `sls_na_load_commission` → `commissions.runCommissions("NA")` | `gold_fact_sale` (commission columns), `gold_commission_accrual` | PARTIAL (Fact.Sale rows/checksum PASS vs legacy; accrual source_derived) |
| 6 | `SLS_EU_Load_Commission` | business_rule | `stg.SaleLine` → `Fact.Sale` | `sls_eu_load_commission` → `commissions.runCommissions("EU")` | same | PARTIAL |
| 7 | `SLS_APAC_Load_Commission` | business_rule | `stg.SaleLine` → `Fact.Sale` | `sls_apac_load_commission` → `commissions.runCommissions("APAC")` | same | PARTIAL |
| 8 | `SLS_Load_PromotionRedemption` | business_rule | `stg.Promotion` → `Aggregate.Promotion Effectiveness` | `sls_load_promotion_redemption` → `promotions.runPromotionRedemption` | `silver_promotion`, `silver_promotion_line`, `gold_promotion_redemption`, `gold_promotion_summary` | PARTIAL (source_derived, empty legacy source) |
| 9 | `SLS_Load_QuotaAttainment` | business_rule | `stg.SaleLine` → `Aggregate.Regional Sales Performance` | `sls_load_quota_attainment` → `quota.runQuotaAttainment` | `gold_quota_attainment` | PARTIAL (source_derived) |
| 10 | `SLS_Export_PartnerFeed` | business_rule | `Fact.Sale` → `partner_feed.csv` | `sls_export_partner_feed` → `partner_feed.runPartnerFeed` | `gold_partner_feed_row` + `/Volumes/…/landing/outbound/partner_feed_YYYYMMDD.csv` | PARTIAL (no legacy file to compare) |
| 11 | `FACT_Apply_Corrections` | correction | `work.FactRekeyQueue` → `Fact.Sale` / `Fact.Order` / `Fact.Payment` | `fact_apply_corrections` → `corrections.runCorrections` | `gold_fact_sale` (reversal + replacement rows), `gold_fact_order`, `gold_fact_payment`, `gold_fact_correction_audit`, `gold_fact_correction_reject`, `work_fact_rekey_queue` | PARTIAL (source_derived; legacy queue empty) |
| 12 | `AGG_Refresh_MonthlySalesSummary` | aggregate_rebuild | `Dimension.*`, `Fact.*` → `Aggregate.Monthly Sales Summary` | `agg_publish_reporting_layer` / `agg_refresh` → `aggregates.runAggregate` | `gold_agg_monthly_sales_summary` | PARTIAL (source_derived) |
| 13 | `AGG_Refresh_RegionalSalesPerformance` | aggregate_rebuild | → `Aggregate.Regional Sales Performance` | same | `gold_agg_regional_sales_performance`, `gold_agg_regional_sales_reject` | PARTIAL |
| 14 | `AGG_Refresh_ProductPerformance` | aggregate_rebuild | → `Aggregate.Product Performance` | same | `gold_agg_product_performance` | PARTIAL |
| 15 | `AGG_Refresh_PromotionEffectiveness` | aggregate_rebuild | → `Aggregate.Promotion Effectiveness` | same | `gold_agg_promotion_effectiveness` | PARTIAL |
| 16 | `AGG_Refresh_MonthlyMarginAnalysis` | aggregate_rebuild | → `Aggregate.Monthly Margin Analysis` | same | `gold_agg_monthly_margin_analysis` | PARTIAL |
| 17 | `AGG_Publish_ReportingLayer` | publish | `Aggregate.*` → `Report.*` | `agg_publish_reporting_layer` → `publish.runPublish` | `report_vw_*` views, `gold_publish_state`, `gold_publish_gate_result` | PARTIAL (source_derived) |

Verdicts are what the `recon` task writes for a clean end-to-end run; the authoritative values are
in `otterorders_migration.evidence.recon_results` (`branch = 'ssis_sales_performance'`, latest
`run_id`). No package in this group can be `PASS`: every legacy target the packages write is
either unpopulated on the host (`raw.FilePartnerSales`, `stg.PartnerSale`, `stg.SaleLine`,
`Sales.CommissionAccruals`, `work.FactRekeyQueue`, every `Aggregate.*` table, `Report.*`) or has
no legacy counterpart (`partner_feed.csv`). Per the evidence contract those are reconciled against
a source-derived expectation (`"baseline":"source_derived"`) and capped at `PARTIAL`. The one
legacy-populated object the group touches, `WideWorldImportersDW.Fact.Sale` (228,265 rows), is
reconciled row-count + business-column checksum against `gold_fact_sale` inside the three
`SLS_*_Load_Commission` rows and passes.

## Layout

```
databricks.yml, resources/        DAB: volume + 2 jobs (serverless), target dev
src/sales_performance/            library (all logic; camelCase functions, snake_case tables/columns)
  config.py                       catalog/schema/volume names, package list, region/country maps
  common.py                       federation readers (remote_query), Delta save/merge, audit columns
  fiscal.py                       4-4-5 fiscal calendar (APAC), calendar YYYY-MM periods
  partner_files.py                ING_FILE_PartnerSales_{NA,EU,APAC}
  partner_staging.py              STG_Load_PartnerSale
  sale_line.py                    stg.SaleLine reconstruction + silver dims + gold facts
  commissions.py                  SLS_{NA,EU,APAC}_Load_Commission
  quota.py / promotions.py        SLS_Load_QuotaAttainment / SLS_Load_PromotionRedemption
  partner_feed.py                 SLS_Export_PartnerFeed
  corrections.py                  FACT_Apply_Corrections
  aggregates.py                   AGG_Refresh_* (5)
  publish.py                      AGG_Publish_ReportingLayer
  recon.py                        evidence contract (recon_results rows, 1 per package)
notebooks/                        thin wrappers, one per job task
tests/                            pytest on local Spark + Delta (22 tests)
samples/inbound/partner/{na,eu,apac}/  representative landing files (good + quarantine cases)
seeds/                            partner_customer_crosswalk.csv, fact_rekey_queue.csv
```

Choice of implementation: PySpark library + notebook tasks for every package (not DLT). The
group is dominated by row-level business rules (commission plans, fiscal calendars, corrections
with reversal/replacement rows, publish gates) that need imperative control flow and unit tests on
local Spark; DLT would only have fitted the three file ingests.

## Running it

```bash
export DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST="$DATABRICKS_DEMO_HOST" DATABRICKS_TOKEN="$DATABRICKS_DEMO_TOKEN"
cd databricks/ssis_migration/sales_performance
python -m ruff check src tests notebooks && python -m pytest -q
databricks bundle validate -t dev
databricks bundle deploy -t dev --var="git_sha=$(git rev-parse HEAD)"
# one-off: upload the representative inputs into the group's volume
V=dbfs:/Volumes/otterorders_migration/ssis_sales_performance/landing
for r in na eu apac; do databricks fs cp -r samples/inbound/partner/$r $V/inbound/partner/$r; done
databricks fs cp -r seeds $V/seeds
databricks bundle run -t dev ssis_sales_performance_pipeline
```

The pipeline is rerunnable: ingested files are moved to `archive/` or `quarantine/` and skipped
next time, staging is watermark-driven (`etl_watermark`), facts/dims are rebuilt from the legacy
warehouse while correction rows (`reverses_sale_key IS NOT NULL`) and restated order/payment rows
survive the reload, the correction queue closes applied rows, and every recon run appends a fresh
`run_id`.

## Design notes per package

**ING_FILE_PartnerSales_{NA,EU,APAC}** (`partner_files.py`). Files are read from the
`landing` volume as raw lines (one-column CSV read with the feed's encoding), classified into
header/detail/footer/unknown records, typed per region and validated with the package's rules
(NA: `MM/DD/YYYY`, tax = state + county, `SALESTAX`, footer = detail count; EU: `DD/MM/YYYY`,
comma decimals, VAT-inclusive gross backed out into net + VAT, `J`/`Y` consent → `Y`, VAT number
≥ 8 chars, footer = monetary total; APAC: `#HEAD`/`#TOTAL`, `YYYY/MM/DD`, GST separate,
gross = net + GST, postal district left-padded to 6, footer = monetary total). A file whose
footer control or unknown-record count fails is quarantined whole (`quarantine/`, one
`bronze_rejected_file_row` per offending row), otherwise its valid rows are appended to
`bronze_raw_file_partner_sales`, malformed rows go to `bronze_rejected_file_row`, the file is
registered in `bronze_inbound_file_register` and moved to `archive/`.

**STG_Load_PartnerSale** (`partner_staging.py`). Incremental append over bronze rows newer than
the `etl_watermark` row for the package; trim/upper normalisation, item refs stripped of spaces,
dates to ISO, currency symbols/thousands separators removed, currency defaulted to USD, country
lookup against `silver_ref_country` (built from `Application.Countries`), customer crosswalk
(`silver_partner_customer_crosswalk`, `PARTNER_CUSTOMER` rows seeded from
`seeds/partner_customer_crosswalk.csv` because the legacy `ref.CustomerCrosswalk` is empty),
validation (quantity > 0, gross > 0, order ref present); failures go to
`silver_partner_sale_reject` with a reason code.

**stg.SaleLine** does not exist populated on the legacy host, so `sale_line.py` rebuilds the
sale-line grain the `SLS_*` packages consume from `wwi_legacy_dw.Fact.Sale` joined to
`Dimension.Customer/Employee/Stock Item/City` and `Sales.Invoices` (salesperson, region via the
customer's country, currency, house-account flag, line type, VAT/GST split). `gold_fact_sale`
starts as a copy of that and is then updated in place by the commission packages
(`commission_amount`, `commission_period`, `commission_plan_code`) and appended to by corrections.

**SLS_{NA,EU,APAC}_Load_Commission** (`commissions.py`). Plans come from
`Sales.CommissionPlans` (7 rows; default plan per region), joined by salesperson/region/effective
date; unplanned reps fall back to the region default and are counted (`unplanned_rep_count`).
NA: (extended price + tax) in USD, base rate + accelerator above the plan threshold, house
accounts at half rate, `SAMPLE`/`INTERNAL` lines excluded, calendar `YYYY-MM`. EU: net amount (VAT
backed out from the rate, else VAT amount subtracted), converted to EUR at the period-average FX,
statutory cap per country, cash-basis countries (DE, AT by default) held in `HELD_CASH_BASIS`
until a payment is seen. APAC: GST-exclusive amount, 4-4-5 fiscal calendar (`fiscal.py`),
period-average FX (missing FX rejected), team split percentage where enabled, period-boundary
lines counted. Accruals land in `gold_commission_accrual`; the summed commission is posted onto
`gold_fact_sale` with a Delta MERGE (the legacy `Fact.Sale.[Commission Amount]` is null
everywhere on the host).

**SLS_Load_QuotaAttainment** (`quota.py`). NA invoiced gross, EU net after credit notes, APAC
order intake on the 4-4-5 calendar; `Sales.SalesQuotas` is empty on the host so every
territory/period gets `NOQUOTA` and the missing-quota counter equals the active territory count.
Bands: `NOQUOTA`, `OVER120`, `AT`, `NEAR`, `UNDER`.

**SLS_Load_PromotionRedemption** (`promotions.py`). Promotion window per region (NA end + 30 d,
APAC end + 14 d, EU = end date; strict mode = window only), percentage vs fixed discount cost,
attributed vs spill redemptions, distinct customers, over-budget flag. All legacy promotion
tables are empty, so the mart is empty but the rules are exercised in `tests/test_promotions.py`.

**SLS_Export_PartnerFeed** (`partner_feed.py`). Fact sales joined to customer, stock item,
partner (`silver_dim_partner` is derived from the customer category because `Dimension.Partner`
does not exist in the legacy DW) and average FX; EU customers without consent are `REDACTED`;
all-partner or single-partner scope; writes `gold_partner_feed_row` and
`partner_feed_YYYYMMDD.csv` into `/Volumes/otterorders_migration/ssis_sales_performance/landing/outbound/`.

**FACT_Apply_Corrections** (`corrections.py`). Queue = legacy `work.FactRekeyQueue` (empty) ∪
`work_fact_rekey_queue` (seeded once from `seeds/fact_rekey_queue.csv`). `Fact.Sale` corrections
are applied as reversal + replacement rows (natural key `Invoice Number|Invoice Line Number`,
original row preserved and flagged `is_correction`), `Fact.Order`/`Fact.Payment` are restated in
place through `DeltaTable.merge` (`is_restated = true`), unsupported targets (`Fact.Movement`)
are rejected to `gold_fact_correction_reject`, every application is written to
`gold_fact_correction_audit` (before/after state), applied queue rows are closed, and
source/insert/update/reject counts go to `gold_package_run_metric`.

**AGG_Refresh_*** (`aggregates.py`). Delete/rebuild of the window (`months_back`, 0 = full
rebuild) per aggregate over `gold_fact_sale` with reversals and reversed originals excluded;
derived metrics (margin %, average order value, attainment %, discount %, loss-making rows kept),
regional exceptions (`UNASSIGNED` territory) to `gold_agg_regional_sales_reject`.

**AGG_Publish_ReportingLayer** (`publish.py`). Dependency-ordered refresh of the group's five
aggregates, then per-aggregate gates (exists, non-empty unless its source is legitimately
empty — `gold_agg_promotion_effectiveness` — no null grain keys, refreshed after the fact and
within the staleness threshold). All gates pass → `report_vw_*` views are (re)created and
`gold_publish_state` = `PUBLISHED`; any gate fails → the previous views are left untouched and the
state is `QUARANTINED`; `force_publish` yields `FORCED`. Gate detail is kept in
`gold_publish_gate_result`.

## Reconciliation (`recon.py`, task `recon`)

One row per package per run (17 rows, one `run_id`), `unit_type = ssis_package`,
`branch = ssis_sales_performance`, `actor = devin:ssis_sales_performance`,
`harness_version = ssis-migration-v1`, `git_sha` from the bundle variable. Every row has a
`row_count` and a `checksum` check; checksums are `sum(xxhash64(business columns cast to string))`,
order independent and free of surrogate keys / load timestamps. Source-derived expectations are
computed independently of the pipeline code (Python re-parse of the landing files, plain SQL
restatements of the staging/commission/aggregate rules) rather than by re-running the library.

```sql
SELECT unit, verdict, summary
FROM otterorders_migration.evidence.recon_results
WHERE branch = 'ssis_sales_performance'
  AND run_id = (SELECT run_id FROM otterorders_migration.evidence.recon_results
                WHERE branch = 'ssis_sales_performance' ORDER BY run_at DESC LIMIT 1)
ORDER BY unit;
```

## Deliberate deviations from the SSIS packages

* **EU delimiter.** The generator prose says semicolon; the shipped `.dtsx` connection manager and
  `config/landing-zone.yaml` say comma. The persisted artefacts win: comma-delimited with
  quote-aware parsing (values such as `"1.210,00"` are quoted).
* **APAC encoding.** The generator prose mentions code page 932 (Shift-JIS); the `.dtsx` and
  `landing-zone.yaml` specify ISO-8859-1. Persisted artefacts win again: ISO-8859-1. Switching is
  a one-line change in `partner_files.FEEDS["APAC"]`. Postal districts shorter than 6 chars are
  left-padded; longer ones are preserved (SSIS truncated to 6).
* **Row-by-row lookups / OLE DB batch sizes** are replaced by set-based joins and Delta MERGE.
* **`Fact.Sale` commission posting** is a MERGE on `sale_key` instead of SSIS OLE DB Command
  updates; the extra `gold_commission_accrual` table is the auditable line-level accrual the
  legacy package only kept in package variables.
* **Corrections** keep the original `Fact.Sale` row and add reversal + replacement rows (SSIS
  updated in place) so the aggregates can exclude reversed rows and the audit trail is complete.
* **Publish** uses views over the gold aggregates instead of copying rows into `Report.*`
  tables; the quarantine semantics (keep previous publish on gate failure) are preserved.
* **Sibling aggregates.** The legacy `AGG_Publish_ReportingLayer` also refreshed
  `Aggregate.Customer Lifetime Value`, `Inventory Turnover`, `Supplier Scorecard` and
  `Loyalty Tier Movement`. Those packages belong to other groups and are deliberately not
  refreshed here (recorded in the evidence row as `sibling_aggregates_not_refreshed`).
* **`cache()`/persist** is not used anywhere: serverless compute rejects `PERSIST TABLE`.

## Open questions

* Which EU delimiter and APAC code page the production feeds really use (see above).
* `stg.SaleLine`, `Sales.CommissionAccruals`, `Sales.SalesQuotas`, `Sales.Promotions*`,
  `work.FactRekeyQueue`, `ref.CustomerCrosswalk` and every `Aggregate.*` table are empty on the
  legacy host, so the commission/quota/promotion/correction/aggregate outputs can only be
  verified against source-derived expectations and the committed seeds. A populated legacy run
  would allow upgrading those rows to `PASS`.
* `Dimension.Partner` does not exist in the legacy DW; the partner dimension is derived from the
  customer category, which should be confirmed with the feed consumers.
* The NA commission accelerator never triggers on the current data because `Sales.SalesQuotas`
  is empty (threshold = quota × band upper %); the rule is covered by `tests/test_commissions.py`.
