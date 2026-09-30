# SSIS → Databricks migration — group `finance` (AP, GL, payments, cost centres, period close)

Databricks-native re-implementation of the 25 finance SSIS packages of the WideWorldImporters estate.
Everything lives in this directory; nothing else in the repo is touched.

| Item | Value |
|---|---|
| Landing schema | `otterorders_migration.ssis_finance` (all bronze / silver / gold / fin_* / control tables) |
| Evidence | `otterorders_migration.evidence.recon_results` (append-only, `branch = 'ssis_finance'`) |
| Workspace path | `/Workspace/Shared/ssis_migration/finance` |
| Jobs | `ssis_finance_run_all`, `ssis_finance_daily_etl`, `ssis_finance_weekly_reference_load`, `ssis_finance_close`, `ssis_finance_month_end`, `ssis_finance_recon` |
| Compute | Serverless jobs compute (environment version 3), Lakehouse Federation reads |
| Sources | `wwi_legacy_oracle.wwi_fin / wwi_ref`, `wwi_legacy_staging.*`, `wwi_legacy_dw.*` (read-only foreign catalogs, no JDBC needed) |

## Layout

```
databricks.yml              bundle + variables (catalog, schema, evidence_schema, accounting_period, business_date, git_sha)
resources/*.job.yml         one job per SSIS master + run_all + recon
notebooks/run_packages.py   thin entrypoint: runs a comma-separated list of packages via finance.packages.runPackages
notebooks/recon.py          thin entrypoint: finance.recon.runRecon -> evidence.recon_results
src/finance/
  config.py                 FinanceConfig (widgets/job params -> typed config)
  io.py                     Delta I/O + control tables (etl_watermark, etl_batch, etl_reject)
  sources.py                foreign-catalog readers (Oracle wwi_fin/wwi_ref, legacy staging/DW lookups)
  rules.py                  reusable finance expressions (aging buckets, due dates, tolerances, withholding, fiscal periods ...)
  fx.py                     Oracle FN_CONVERT_AMOUNT equivalent (direct -> inverse -> USD triangulation, 7-day fallback)
  matching.py               3-pass payment-to-invoice matcher (remittance -> exact amount -> oldest-first residual)
  extracts.py               EXT_ORA_* (7)      -> bronze_ora_*
  staging.py                STG_* (5) + DQ_Payment_Screen -> silver_*, work_*, err_*, dq_*
  facts.py                  REF_Load_CostCenter (SCD2) + FACT_Load_* (3) -> gold_dim_/gold_fact_
  close.py                  FIN_* (7)          -> fin_*
  aggregates.py             AGG_Refresh_FinanceCloseSummary -> gold_agg_finance_close_summary
  packages.py               the 25-package registry + batch orchestration
  recon.py                  evidence writer (row count, order-independent checksum, null-rate, baseline)
tests/                      pytest on local Spark + Delta (18 tests)
```

Implementation choice: **PySpark library code + thin notebooks** for every package (consistent within the group;
the SSIS packages are procedural control flows with lookups/conditional splits that map 1:1 onto DataFrame code
and are easier to unit-test than DLT). Job orchestration mirrors the SSIS masters; `ssis_finance_run_all` is
the bootstrap/regression job that runs the whole group in dependency order and finishes with the recon task.

## Package → artifact map

Verdicts are from the latest `ssis_finance_run_all` evidence run (see "Evidence" below). Legacy target
= the SQL Server object the SSIS package loads; when it is empty on the host the comparison is
`baseline = source_derived` and the verdict is capped at `PARTIAL` as the evidence contract requires.

| # | Package | Load type | Source (federated) | Delta target (`ssis_finance.`) | Legacy target | Runner | Verdict |
|---|---|---|---|---|---|---|---|
| 1 | EXT_ORA_ApInvoiceHdr | incremental_timestamp (watermark `last_upd_dt`) | `wwi_legacy_oracle.wwi_fin.ap_invoice_hdr` | `bronze_ora_ap_invoice_hdr` | `raw.OracleApInvoiceHdr` (empty) | `extracts.runInvoiceHeaders` | PARTIAL |
| 2 | EXT_ORA_ApInvoiceLine | incremental_key (`invoice_line_id`) | `wwi_fin.ap_invoice_line` (+hdr, cost_center lookups) | `bronze_ora_ap_invoice_line` | `raw.OracleApInvoiceLine` (empty) | `extracts.runInvoiceLines` | PARTIAL |
| 3 | EXT_ORA_ApPayment | incremental_timestamp | `wwi_fin.ap_payment` (+`wwi_ref.supp_master`) | `bronze_ora_ap_payment` | `raw.OracleApPayment` (empty) | `extracts.runPayments` | PARTIAL |
| 4 | EXT_ORA_ApPaymentApply | incremental_key (`apply_id`) | `wwi_fin.ap_payment_apply` | `bronze_ora_ap_payment_apply` | `raw.OracleApPayment` (empty) | `extracts.runPaymentApplies` | PARTIAL |
| 5 | EXT_ORA_ApAging | full snapshot | `wwi_fin.ap_aging_snapshot` | `bronze_ora_ap_aging` | `raw.OracleApInvoiceHdr` (empty) | `extracts.runApAging` | PARTIAL |
| 6 | EXT_ORA_GlJournalLine | date_window (gl_date window, posted, non-STAT) | `wwi_fin.gl_journal_line/hdr`, `gl_account` | `bronze_ora_gl_journal_line` | `raw.OracleGlJournalLine` (empty) | `extracts.runGlJournalLines` | PARTIAL |
| 7 | EXT_ORA_CostCenter | full (hierarchy flattened) | `wwi_fin.cost_center` | `bronze_ora_cost_center` | `raw.OracleCostCenter` (empty) | `extracts.runCostCenters` | PARTIAL |
| 8 | STG_Load_ApInvoice | incremental_append + dedup | `bronze_ora_ap_invoice_hdr/line` | `silver_ap_invoice`, `silver_ap_invoice_line` | `stg.ApInvoice`, `stg.ApInvoiceLine` (empty) | `staging.runStgApInvoice` | PARTIAL |
| 9 | STG_Load_GlJournal | incremental_append + dedup | `bronze_ora_gl_journal_line` | `silver_gl_journal_line` | `stg.GlJournalLine` (empty) | `staging.runStgGlJournal` | PARTIAL |
| 10 | STG_Load_Payment | incremental_append + dedup + method/supplier lookups | `bronze_ora_ap_payment` | `silver_payment` | `stg.Payment` (empty) | `staging.runStgPayment` | PARTIAL |
| 11 | STG_Load_CostCenter | truncate_reload | `bronze_ora_cost_center` | `silver_cost_center` | `stg.CostCenter` (empty) | `staging.runStgCostCenter` | PARTIAL |
| 12 | STG_Work_PaymentMatch | work_rebuild (3-pass matcher) | `silver_payment`, `silver_ap_invoice`, applies | `work_payment_matched`, `work_payment_unapplied` | `work.PaymentMatched` (empty) | `staging.runPaymentMatch` | PARTIAL |
| 13 | DQ_Payment_Screen | quality_screen | `silver_payment` | `dq_payment_screen_result`, `err_rejected_payment` | `err.RejectedPayment` (empty) | `staging.runDqPaymentScreen` | PARTIAL |
| 14 | REF_Load_CostCenter | full_refresh, SCD2 | `silver_cost_center` | `gold_dim_cost_center` | `Dimension.Cost Center` | `facts.runRefCostCenter` | PARTIAL |
| 15 | FACT_Load_GLPosting | incremental_fact | `silver_gl_journal_line` + dim + period locks | `gold_fact_gl_posting` | `Fact.GL Posting` (empty) | `facts.runFactGlPosting` | PARTIAL |
| 16 | FACT_Load_Payment | incremental_fact | `silver_payment`, `work_payment_matched` | `gold_fact_payment` | `Fact.Payment` (empty) | `facts.runFactPayment` | PARTIAL |
| 17 | FACT_Load_SupplierPayment | incremental_fact | `work_payment_matched` + payments + invoices | `gold_fact_supplier_payment` | `Fact.Supplier Payment` (empty) | `facts.runFactSupplierPayment` | PARTIAL |
| 18 | FIN_Currency_Revaluation | business_rule | `silver_ap_invoice`, `wwi_ref.fx_rate`, `wwi_ref.currency` | `fin_fx_rate_snapshot`, `fin_currency_revaluation` | `Fact.Payment` (empty) | `close.runCurrencyRevaluation` | PARTIAL |
| 19 | FIN_Load_GlPostings | business_rule (period gate + balance) | `silver_gl_journal_line` | `gold_fact_gl_posting`, `fin_gl_held_line`, `fin_gl_posting_control` | `Fact.GL Posting` (empty) | `close.runFinGlPostings` | PARTIAL |
| 20 | FIN_Load_ApAging | business_rule | `silver_ap_invoice` (+aging snapshot) | `fin_ap_aging_detail`, `fin_ap_aging_summary` | `Fact.Payment` (empty) | `close.runFinApAging` | PARTIAL |
| 21 | FIN_Load_CostAllocation | business_rule (sequential rule engine) | `wwi_fin.cost_allocation_rule`, GL postings | `fin_cost_allocation`, `fin_cost_allocation_summary` | `Aggregate.Finance Close Summary` (empty) | `close.runFinCostAllocation` | PARTIAL |
| 22 | FIN_Load_WithholdingTax | business_rule | `silver_ap_invoice_line` + supplier master | `ref_withholding_tax_rate`, `fin_withholding_tax`, `fin_withholding_certificate_queue` | `Fact.Payment` (empty) | `close.runFinWithholdingTax` | PARTIAL |
| 23 | FIN_Reconcile_SubledgerToGl | business_rule | AP subledger vs `gold_fact_gl_posting` | `fin_recon_result` | `etl.ReconciliationResult` (empty) | `close.runFinReconcile` | PARTIAL |
| 24 | FIN_Close_PeriodLock | business_rule | `fin_recon_result`, `fin_gl_posting_control` | `fin_period_lock` | `etl.Batch` (empty) | `close.runFinPeriodLock` | PARTIAL |
| 25 | AGG_Refresh_FinanceCloseSummary | aggregate_rebuild | all gold/fin tables | `gold_agg_finance_close_summary` | `Aggregate.Finance Close Summary` (empty) | `aggregates.runAggFinanceCloseSummary` | PARTIAL |

Control tables (all in the landing schema): `etl_watermark` (per-package high-water mark, epoch
`1900-01-01`), `etl_batch` (one row per package execution: rows read/written/rejected, status), `etl_reject`
(every row diverted to an SSIS error output, with reason code and payload).

## Business rules preserved (where they live)

* **Payment-to-invoice application** (`matching.py`): void payments and CANC/VOID/DRAFT/on-hold invoices are
  excluded; pass 1 remittance reference (`ap_payment_apply`, non-reversed), pass 2 exact open amount incl.
  eligible early-payment discount, pass 3 oldest-first residual allocation; the remaining balance is
  written off when inside the regional tolerance (NA 0.02 abs, EU 0.01 abs, APAC 0.50 % of the payment)
  otherwise parked as unapplied cash (`work_payment_unapplied`). No invoice is ever over-applied.
* **DQ payment screen** (`staging.runDqPaymentScreen`): payment-method / currency / supplier code validation,
  large-payment flag (5 000 000), orphan-rate circuit breaker (5 %, PROD 1 %), rejects → `err_rejected_payment`.
* **GL posting gate** (`facts.buildGlPostingFact`, `close.runFinGlPostings`): whole-journal balance within
  0.005, journal completeness (`journal_line_cnt`), **posting into a closed/permanently-closed period is
  blocked** (source `gl_period_status` *and* our own `fin_period_lock`) and the lines are held in
  `fin_gl_held_line`; `allow_unbalanced_journals` reproduces the SSIS override parameter.
* **Period close** (`close.runFinPeriodLock`): a ledger/region period is `LOCKED` only when the
  subledger-to-GL reconciliation has no variance beyond the regional close tolerance and no held GL lines
  remain, otherwise `OPEN_WITH_VARIANCE`; re-runs are idempotent (delete/insert per period).
* **Regional tax / currency / calendar** (`rules.py`, `fx.py`): tax regime by region (SALES / VAT / GST / MIXED),
  reporting currency and fiscal calendar per region (APAC fiscal year starts in April, EU 4-4-5), Oracle
  due-date terms (PREPAY / DOM / EOM / net days, NA weekends roll forward, EU roll backward, APAC snaps to the
  15th / month-end), `FN_CONVERT_AMOUNT` resolution order with target-currency minor-unit rounding.
* **AP aging** (`close.runFinApAging`): excludes CANC/VOID/DRAFT, disputed invoices excluded unless
  `include_disputed`, SSIS buckets (CURRENT/B030/B060/B090/B090P) plus the regional Oracle bucket families.
* **Cost-centre hierarchy & SCD2** (`extracts.runCostCenters`, `facts.applyScd2`): hierarchy flattened
  (level, rollup path, root, cycle/orphan anomaly code), type-2 dimension keyed on `cost_center_code` with
  unknown member key 0; missing incoming rows are *not* treated as deletes (matches the SSIS SCD component).
* **Cost allocation** (`close.runAllocationRules`): rules applied in `rule_seq_nbr` order against the
  shrinking source balance — PCT, DRIVER (falls back to EVEN when the driver is unavailable), STEP, FIXED,
  EVEN, RESIDUAL — with per-rule-set control totals.
* **Withholding tax** (`rules.withholdingAmount`): NA backup withholding (24 %) on the reportable service
  categories, EU 20 % with the 10 % treaty rate for registered suppliers, APAC 47 % no-ABN rate with the
  75 % reportable threshold; certificates queued in `fin_withholding_certificate_queue`.
* **Finance close summary** (`aggregates.buildFinanceCloseSummary`): control totals per region/period across
  GL, AP, payments, aging, revaluation, allocations, withholding and the reconciliation/lock status.

## Deliberate deviations from the SSIS packages

* Row-by-row OLE DB lookups, OLE DB batch sizes / `FastLoadMaxInsertCommitSize`, Oracle fetch array size and
  the `SqlCommand` retry loops are not reproduced — they are SSIS execution artefacts, not business logic.
* SSIS "truncate + reload" targets are Delta overwrites; "incremental_append" targets are keyed MERGEs so a
  re-run of the same window is idempotent (SSIS would duplicate rows).
* `EXT_ORA_ApPaymentApply` / `EXT_ORA_ApAging` land in their *own* bronze tables (`bronze_ora_ap_payment_apply`,
  `bronze_ora_ap_aging`); the inventory maps them onto `raw.OracleApPayment` / `raw.OracleApInvoiceHdr`
  because the legacy staging reused those tables with a type discriminator.
* `FIN_Load_WithholdingTax` reads its rates from `ref_withholding_tax_rate`, seeded inside the landing schema,
  because the legacy `stg.WithholdingTaxRate` table does not exist on the host (see open questions).
* `FIN_Close_PeriodLock` writes `fin_period_lock` instead of updating `etl.Configuration` / `etl.Batch`
  (we must not write to the legacy control schema); `FACT_Load_GLPosting` reads the lock table directly.
* The Oracle `FN_CONVERT_AMOUNT` raises an application error on a missing rate; we flag the row
  (`*_rate_missing`) and either fail the package (`fail_on_missing_rate=true`) or leave the converted amount
  null and report it in the control table.
* Watermarks are stored in `etl_watermark` (Delta) rather than SSISDB environment variables.

## Evidence / reconciliation

`notebooks/recon.py` → `finance.recon.runRecon` appends one row per package (one `run_id` per run,
`unit_type='ssis_package'`, `branch='ssis_finance'`, `actor='devin:ssis_finance'`,
`harness_version='ssis-migration-v1'`, `git_sha` = commit deployed with `--var git_sha=...`).
Every row carries `row_count`, `checksum` (`SUM(xxhash64(concat_ws('|', <business cols>)))` over the
business columns shared by target and baseline, numerics normalised to `decimal(38,6)`, dates to
`yyyy-MM-dd HH:mm:ss`, strings upper/trimmed — surrogate keys, load timestamps, batch ids and SCD
housekeeping columns are excluded), `column_null_rate` on the business key, and — when the package has
a legacy target — `legacy_target_row_count`. The checks JSON ends with `{"baseline": "legacy" | "source_derived"}`.

All 25 legacy targets of this group are unpopulated on the host (`raw.*`, `stg.*`, `work.*`, `err.*`,
`Fact.GL Posting`, `Fact.Payment`, `Fact.Supplier Payment`, `Aggregate.Finance Close Summary`,
`etl.Batch`, `etl.ReconciliationResult` all return 0 rows; `Dimension.Cost Center` only holds the seeded
members), so every package is compared to a source-derived baseline and reported as `PARTIAL`.
No package is `NOT_APPLICABLE`; every finance package produces data.

Query the latest run:

```sql
WITH latest AS (
  SELECT run_id FROM otterorders_migration.evidence.recon_results
  WHERE branch = 'ssis_finance' ORDER BY run_at DESC LIMIT 1)
SELECT unit, verdict, summary
FROM otterorders_migration.evidence.recon_results
WHERE branch = 'ssis_finance' AND run_id = (SELECT run_id FROM latest)
ORDER BY unit;
```

## Running it

```bash
export DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST="$DATABRICKS_DEMO_HOST" DATABRICKS_TOKEN="$DATABRICKS_DEMO_TOKEN"
cd databricks/ssis_migration/finance
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev --var="git_sha=$(git rev-parse HEAD)"
databricks bundle run ssis_finance_run_all -t dev --var="git_sha=$(git rev-parse HEAD)"
```

Job parameters (all overridable per run): `catalog`, `schema`, `evidence_schema`, `accounting_period`
(default `2024-12`), `business_date` (default `2024-12-31`), `git_sha`, `allow_unbalanced_journals`,
`fail_on_missing_rate`, `include_disputed`, `ledger_scope` — these are the SSIS project/package parameters.

Local tests (`pytest`, local Spark 3.5 + delta-spark 3.2; Maven Central is rate-limited from some networks, so
`tests/conftest.py` points Ivy at the GCS mirror):

```bash
pip install -r requirements-dev.txt
ruff check . && ruff format --check . && python -m pytest -q
```

## Open questions

1. **Withholding-tax rates** — `stg.WithholdingTaxRate` does not exist on the legacy host and the SSIS package
   is the only consumer; the rates in `close.WITHHOLDING_RATE_SEED` come from the package's own expressions
   and the tax docs and need finance sign-off before production use.
2. **Payment-method crosswalk** — `ref.CodeCrosswalk` is empty on the host; the bounded mapping in
   `staging.py` (ACH/CARD/CHECK/LOCAL/SEPA/WIRE) reproduces the SSIS derived column.
3. **Legacy fact baselines** — because the DW facts are empty, a `PASS` cannot be demonstrated for any
   finance package; once the legacy SSIS run populates them, `recon.ReconSpec.legacyColMap` is the only
   thing to fill in to switch a package from `source_derived` to a `legacy` baseline.
4. **`Dimension.Cost Center`** on the host only holds the unknown/seed members, so the SCD2 dimension is also
   compared to its own silver input.
