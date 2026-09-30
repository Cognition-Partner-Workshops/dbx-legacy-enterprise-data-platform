# SSIS → Databricks: `customer_engagement` package group

Databricks-native re-implementation of the 13 WideWorldImporters SSIS packages that own loyalty, web-session
and Customer 360 processing. Everything in this directory is self-contained: a PySpark library (`src/`), two
thin notebooks (`notebooks/`), a Declarative Automation Bundle (`databricks.yml` + `resources/`), local-Spark
pytest coverage (`tests/`) and the reconciliation/evidence task (`src/customer_engagement/recon.py`).

| Item | Value |
| --- | --- |
| Branch / PR | `devin/ssis-migration-customer_engagement` → `main` |
| Landing schema | `otterorders_migration.ssis_customer_engagement` |
| Evidence | `otterorders_migration.evidence.recon_results` (`branch = 'ssis_customer_engagement'`) |
| Workspace path | `/Workspace/Shared/ssis_migration/customer_engagement` |
| Job | `ssis_customer_engagement_etl` (13 package tasks + `recon`, serverless notebook tasks) |
| Legacy sources | `wwi_legacy_oltp`, `wwi_legacy_staging`, `wwi_legacy_dw` (Lakehouse Federation, read-only) |

## Package → Databricks artifact mapping

All packages are dispatched by `notebooks/run_package.py` → `customer_engagement.runner.runPackage` (one job
task per package, same DAG as `Master_Daily_ETL` / `Master_Month_End`). Verdicts are from the latest evidence
run; see "Reconciliation" for why they are `PARTIAL`.

| # | SSIS package | Load type | Library entry point | Source (federated) | Target Delta table(s) | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `EXT_SQL_LoyaltyLedger` | incremental_key (`LoyaltyLedgerID` watermark + 7-day expiry lookback) | `extracts.runExtLoyaltyLedger` | `wwi_legacy_oltp.loyalty.loyaltypointsledger` + members/programs/tiers | `bronze_oltp_loyalty_ledger`, `etl_watermark` | FAIL (baseline discrepancy, see below) |
| 2 | `EXT_SQL_WebSessions` | date_window (`StartedWhen` window from watermark to run time) | `extracts.runExtWebSessions` | `wwi_legacy_oltp.ecommerce.websessions` + `cartheaders` | `bronze_oltp_web_session`, `etl_watermark` | FAIL (baseline discrepancy, see below) |
| 3 | `STG_Load_LoyaltyLedger` | incremental_append (`LastEditedWhen` watermark) | `staging.runStgLoyaltyLedger` | `wwi_legacy_staging.raw.sqlloyaltyledger` (or bronze) | `silver_loyalty_ledger`, `silver_loyalty_customer_balance`, `silver_loyalty_ledger_rejects` | PARTIAL |
| 4 | `STG_Load_WebSession` | incremental_append (`LastEditedWhen` watermark) | `staging.runStgWebSession` | `wwi_legacy_staging.raw.sqlwebsession` (or bronze) | `silver_web_session`, `silver_web_session_rejects` | PARTIAL |
| 5 | `FACT_Load_LoyaltyPoints` | incremental_fact (`LoadedAtUtc` watermark, insert-only on natural key) | `facts.runFactLoyaltyPoints` | `silver_loyalty_ledger`, `wwi_legacy_dw.dimension.customer/date` | `gold_fact_loyalty_points`, `gold_fact_loyalty_points_rejects` | PARTIAL |
| 6 | `FACT_Load_WebSession` | incremental_fact (`LoadedAtUtc` watermark, dedup on session key) | `facts.runFactWebSession` | `silver_web_session`, `wwi_legacy_dw.dimension.customer` | `gold_fact_web_session`, `gold_fact_web_session_rejects` | PARTIAL |
| 7 | `C360_Build_CustomerProfile` | business_rule (full rebuild) | `customer360.runBuildCustomerProfile` | `wwi_legacy_dw.dimension.customer`, `fact.sale`, `fact.payment` | `gold_c360_customer_profile`, `work_customer_address_standardised`, `work_customer_identity_graph` | PARTIAL |
| 8 | `C360_Build_LoyaltyOverlay` | business_rule (accrue + expire, rebuild overlay) | `customer360.runBuildLoyaltyOverlay` | `fact.sale`, `gold_c360_customer_profile` | `work_loyalty_point_ledger`, `gold_c360_loyalty_overlay` | PARTIAL |
| 9 | `C360_Build_RollingMetrics` | business_rule (trailing 12-month window, deciles) | `customer360.runBuildRollingMetrics` | `fact.sale`, `gold_c360_customer_profile` | `gold_c360_customer_rolling_metric` | PARTIAL |
| 10 | `C360_Build_ChurnFlags` | business_rule (5 weighted rules) | `customer360.runBuildChurnFlags` | rolling metric, loyalty overlay, profile, `fact.sale` | `gold_c360_customer_churn_flag`, `work_customer_outreach_queue` | PARTIAL |
| 11 | `C360_Publish_Segments` | business_rule (segment precedence, movement vs previous) | `customer360.runPublishSegments` | churn flag, rolling metric, overlay, profile | `gold_c360_customer_segment`, `work_customer_segment_previous` | PARTIAL |
| 12 | `AGG_Refresh_Customer360` | aggregate_rebuild (full) | `aggregates.runAggRefreshCustomer360` | `dimension.customer`, `fact.sale/payment`, loyalty + web facts | `gold_agg_customer_360`, `gold_agg_customer_360_rejects` | PARTIAL |
| 13 | `AGG_Refresh_CustomerRolling12Month` | aggregate_rebuild (target period + 11 prior) | `aggregates.runAggRefreshCustomerRolling12Month` | `dimension.customer`, `fact.sale` | `gold_agg_customer_rolling_12_month` | PARTIAL |

Control tables: `etl_watermark` (one row per SSIS watermark, mirrors `etl.Watermark`), `etl_package_run`
(mirrors the `Log Package Start/End` Execute SQL tasks). Every run/insert carries `BatchId`/`LineageKey` from the
job run id and `run_id` = `{{job.id}}-{{job.run_id}}`.

## Layout

```
databricks.yml                  bundle: variables (catalog, schema, warehouse_id, as_of_date, staging_source_mode, default_region, git_sha)
resources/customer_engagement_job.yml   job ssis_customer_engagement_etl — 13 package tasks + recon, DAG = legacy precedence constraints
notebooks/run_package.py        thin notebook: widgets -> CeConfig -> runner.runPackage
notebooks/run_recon.py          thin notebook: writes evidence rows, returns verdict counts
src/customer_engagement/
  config.py      CeConfig (catalog/schema/source catalogs/as-of/run metadata), resolveAsOfDate
  regions.py     region normalisation, consent flags, tier thresholds, expiry defaults, current-row rule
  tables.py      table names + Delta helpers (overwrite, append, insert-only on natural key, watermarks, package-run log)
  extracts.py    EXT_SQL_* (OLTP -> bronze)
  staging.py     STG_Load_* (raw -> silver, conform/route/reject)
  facts.py       FACT_Load_* (silver -> gold facts, lookups, expiry generation, running balance)
  customer360.py C360_Build_* / C360_Publish_Segments chain
  aggregates.py  AGG_Refresh_Customer360 / AGG_Refresh_CustomerRolling12Month
  recon.py       13 ReconSpecs -> otterorders_migration.evidence.recon_results
  runner.py      package name -> entry point
tests/           pytest on local Spark (pyspark 3.5): dedup, watermark filters, consent/privacy, expiry + running
                 balance, late/unknown members, churn scoring, segment precedence, 4-4-5 periods, rolling trends, recon verdicts
```

## How to run

```bash
export DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST="$DATABRICKS_DEMO_HOST" DATABRICKS_TOKEN="$DATABRICKS_DEMO_TOKEN"
cd databricks/ssis_migration/customer_engagement
pip install pyspark==3.5.1 pytest ruff && ruff check src tests && python -m pytest -q
databricks bundle validate -t dev
databricks bundle deploy -t dev --var="git_sha=$(git rev-parse HEAD)"
databricks bundle run -t dev ssis_customer_engagement_etl
```

`as_of_date` defaults to `auto` = `MAX(Fact.Sale.[Invoice Date Key])` (2016-05-31 on the baseline); pass an ISO
date to re-anchor the C360/aggregate windows. `staging_source_mode=bronze` switches the STG packages to read the
EXT bronze tables instead of the legacy `raw.*` landing tables.

## Design decisions

* **One PySpark library, thin notebooks, one job.** All 13 packages are batch loads with lookups, derived columns,
  conditional splits and Execute SQL bookkeeping; PySpark on serverless job compute reproduces them 1:1 and keeps
  the transformation logic unit-testable on local Spark. DLT/SDP was not used because several packages are
  stateful in ways SDP does not express naturally (numeric watermark + lookback, running balances recomputed over
  the whole fact, "expire points as of today" rows, previous-segment comparison).
* **Federation for every legacy read.** OLTP, staging and DW objects are read with 3-level names from
  `wwi_legacy_*`; nothing is read from any other `ssis_*` schema. Cross-group objects (`Dimension.Customer`,
  `Dimension.Date`, `Fact.Sale`, `Fact.Payment`) are the *legacy* versions.
* **Staging source mode.** The legacy `raw.SqlLoyaltyLedger` / `raw.SqlWebSession` landing tables are populated
  (1 500 / 1 800 rows) while the OLTP `Loyalty.*` / `Ecommerce.*` tables are empty on the baseline. The EXT
  packages are still fully implemented against OLTP (bronze), and the STG packages default to
  `staging_source_mode=legacy_raw` so the downstream chain has data. Both paths share the same conform code.
* **Watermarks and idempotency.** Each package keeps the SSIS watermark type (`incremental_key`, `date_window`,
  `incremental_append`, `incremental_fact`) in `etl_watermark`; fact/staging inserts are insert-only on the
  natural key so a re-run of the job is a no-op rather than a duplicate load (SSIS relied on the watermark alone).
* **Rejects/holds are tables, not dropped rows.** Every SSIS error output / hold destination has a Delta
  counterpart (`*_rejects`, `work_customer_outreach_queue`, `gold_agg_customer_360_rejects`) with a reason/route code.
* **As-of anchoring.** The C360 chain and aggregates anchor "today" on the latest legacy invoice date (variable
  `as_of_date=auto`) because the baseline DW stops in May 2016; running with the wall clock would classify every
  customer as inactive/dormant and produce empty windows. Point-expiry generation in `FACT_Load_LoyaltyPoints`
  (`ExpiryDate <= GETDATE()`) keeps the wall clock, exactly like the package.
* **Business rules preserved verbatim** (see module docstrings): regional expiry months (EU 12 / APAC 18 / NA 24),
  tier thresholds (PLT ≥ 50 000, GLD ≥ 20 000, SLV ≥ 5 000), signed points (REDEEM/EXPIRE negative), point cash
  values and earn rates per region, bot/bounce/pages-per-minute rules, EU 14-month / APAC 6-month / other
  3-year web retention, regional qualifying-amount bases and point multipliers for the loyalty overlay, the five
  churn rules (30/20/15/15/25) with their thresholds, segment precedence, the Customer 360 RFM/churn/retention
  rules and the rolling-12-month trend bands (±10 %).

## Deliberate deviations from the SSIS packages

| Area | SSIS behaviour | Databricks behaviour | Why |
| --- | --- | --- | --- |
| Current customer rows | Lookups filter `[Is Current Row] = 1` | `coalesce([Is Current Row], [Valid To] > now())` | On the live baseline `Is Current Row` is NULL for all 403 real customers (only the -3..-1 unknown members carry `1`); the literal filter would route every fact row to the unknown/hold output and empty the C360 chain. |
| Row-by-row lookups / OLE DB batch sizes / fast-load hints | Full-cache lookups, batch commits | Set-based joins, single Delta commit per output | SSIS artefacts. |
| Running balance | Recomputed by a T-SQL post-step over the fact | Window `sum(SignedPoints)` ordered by `(EventDate, MovementReference)` | Same result, ordering made explicit and deterministic. |
| Re-runs | Watermark only | Watermark **and** insert-only on the natural key | Makes the job safely re-runnable after a partial failure. |
| `work.*` scratch tables | `TRUNCATE` + reload in `WideWorldImporters_Staging.work` | Same names as `work_*` Delta tables in the landing schema | Only one writable schema. |
| Rolling-12-month "prior" | `Aggregate.Customer Rolling 12 Month` row for the previous period | Previous period from the rebuilt set, falling back to the existing table | Identical when the table exists; avoids the first-run `NEW` artefact on all 12 rebuilt periods. |
| `raw.*` typing | `varchar` columns cast by the OLE DB source | Same casts in Spark with `try_cast`-style NULL fallbacks routed to `*_rejects` | Avoids a whole-batch failure on one bad row. |
| Landing page path | `TOKEN(REPLACE(url,"://"," ")," ",2)` | `regexp_extract(url, '://([^ ]*)')` | Same output for well-formed URLs. |

## Reconciliation (evidence)

`recon.py` writes one row per package per run to `otterorders_migration.evidence.recon_results`
(`unit_type='ssis_package'`, `branch='ssis_customer_engagement'`, `actor='devin:ssis_customer_engagement'`,
`harness_version='ssis-migration-v1'`, `git_sha` = deployed commit). Every row has a `row_count` and a `checksum`
(`sum(cast(xxhash64(business cols) as decimal(38,0)))`, order-independent) check plus a `baseline` marker.

* Latest run: **11 PARTIAL, 2 FAIL, 0 PASS, 0 NOT_APPLICABLE**. Row counts on the landing schema:
  `silver_loyalty_ledger` 1 500, `silver_web_session` 1 800, `gold_fact_web_session` 1 800,
  `gold_fact_loyalty_points` 0 (+1 500 held), `gold_c360_*` 402 each, `gold_agg_customer_360` 406,
  `gold_agg_customer_rolling_12_month` 4 836 (403 customers × 12 periods).
* **`EXT_SQL_LoyaltyLedger` / `EXT_SQL_WebSessions` = FAIL.** Their legacy targets (`raw.SqlLoyaltyLedger`,
  `raw.SqlWebSession`) hold 1 500 / 1 800 rows, but the live OLTP tables the packages extract from
  (`Loyalty.LoyaltyPointsLedger`, `Ecommerce.WebSessions`) hold 0 rows, so the Databricks extract correctly lands
  0 rows and the row-count check against the legacy target cannot pass. This is a baseline inconsistency on the
  shared host (the raw tables were seeded independently of OLTP), not a logic gap: the extract SQL, joins, EU
  consent suppression and audit columns are implemented and unit-tested, and `checks` carries the
  `source_derived_row_count` (0 = 0) alongside the failed legacy comparison. `PARTIAL` was not used because the
  legacy target is populated.
* The legacy targets of the other 11 packages are **empty or not exposed on the baseline** (`stg.LoyaltyLedger`,
  `stg.WebSession`, `Fact.Loyalty Points`, `Fact.Web Session`, `Aggregate.Customer 360`,
  `Aggregate.Customer Rolling 12 Month`, `Dimension.Customer Segment`, `Report.vw_CustomerChurnRisk`; the OLTP
  `Loyalty.*`/`Ecommerce.*` sources of the EXT packages hold 0 rows). Per the contract each package is therefore
  compared against a **source-derived expectation** (`"baseline":"source_derived"`) built with SQL that re-applies
  the package's own rule set to the federated source, and the verdict is capped at `PARTIAL`.
* Where the legacy target has rows the spec compares against it directly (`legacyTarget`), so verdicts flip to
  `PASS`/`FAIL` automatically once the host is populated.
* `FACT_Load_LoyaltyPoints`: the 1 500 legacy ledger rows reference `CustomerID` 1000–1399, none of which exist in
  `Dimension.Customer` (WWI ids 0–601); they land in `gold_fact_loyalty_points_rejects` with
  `LOOKUP_CUSTOMER_NO_MATCH`, exactly as the package's "Hold Unmatched Customer" output. The expectation therefore
  reconciles to 0 matched rows and the held rows are reported separately in `checks`.

Latest-run query used for the definition of done:

```sql
SELECT unit, verdict, summary
FROM otterorders_migration.evidence.recon_results
WHERE branch = 'ssis_customer_engagement'
  AND run_id = (SELECT run_id FROM otterorders_migration.evidence.recon_results
                WHERE branch = 'ssis_customer_engagement' ORDER BY run_at DESC LIMIT 1)
ORDER BY unit;
```

## Open questions

1. `Dimension.Customer.[Is Current Row]` is NULL on the baseline — should the owning dimension group backfill it
   (then the current-row fallback here becomes a no-op)?
2. The loyalty ledger's customer ids (1000–1399) do not exist in any customer source we can read; is there an
   MDM cross-reference in `wwi_legacy_oracle.wwi_mdm` that should be used for the lookup?
3. `Fact.Payment` and `Fact.Return` are empty, so `AveragePaymentDays`, `OpenArBalance`, returns-based churn rules
   and the payment-based Customer 360 measures evaluate to their zero/NULL branches.
4. `Report.vw_CustomerChurnRisk` and `Dimension.Customer Segment` do not exist in the federated DW; their
   contracts were taken from the package/procedure DDL under `sqlserver/warehouse`.
