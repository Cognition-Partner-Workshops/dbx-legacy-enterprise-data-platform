# SSIS → Databricks: `logistics_returns` (group 8/10)

Databricks-native reimplementation of the 12 WideWorldImporters SSIS packages that cover shipments, shipment
lines, returns, credit notes, the carrier scan file feed and the month-end delivery-performance mart.

| | |
|---|---|
| Landing schema | `otterorders_migration.ssis_logistics_returns` (bronze / silver / gold / ctl / err tables) |
| Landing volume | `otterorders_migration.ssis_logistics_returns.landing` (`inbound/carrier`, `processed`, `failed`) |
| Workspace path | `/Workspace/Shared/ssis_migration/logistics_returns` |
| Jobs | `ssis_logistics_returns_daily_etl` (end-to-end, run on demand), `ssis_logistics_returns_hourly_carrier_scan` (file-arrival trigger, paused), `ssis_logistics_returns_month_end_aggregate` (monthly schedule, paused) |
| Compute | serverless jobs compute; SQL warehouse `565cd2fd713738c4` for ad-hoc verification |
| Evidence | `otterorders_migration.evidence.recon_results`, `branch='ssis_logistics_returns'`, plus a local copy in `ctl_recon_results` |

## Package → Databricks artifact mapping

All packages are PySpark library code in `src/logistics_returns/` invoked through one thin notebook
(`notebooks/run_package.py`, widget `package=<SSIS package name>`) as one job task per package. Source
data is read through the Lakehouse Federation catalogs (`wwi_legacy_oltp`, `wwi_legacy_staging`,
`wwi_legacy_dw`); objects with spaces in their names that do not bind through the foreign catalog fall
back to `remote_query` on the same read-only connection (`common.readLegacy`).

| Package | Load type | Source (legacy) | Target (Delta) | Module / function | Verdict |
|---|---|---|---|---|---|
| `EXT_SQL_Shipments` | incremental_key (NumericKey watermark) | `Shipping.ShipmentHeaders` → `raw.SqlShipment` | `bronze_sql_shipment` | `extracts.runExtract` | FAIL (baseline) |
| `EXT_SQL_ShipmentLines` | incremental_key | `Shipping.ShipmentLines` → `raw.SqlShipmentLine` | `bronze_sql_shipment_line` | `extracts.runExtract` | FAIL (baseline) |
| `EXT_SQL_Returns` | incremental_key | `Returns.ReturnLines` → `raw.SqlReturnLine` | `bronze_sql_return_line` | `extracts.runExtract` | FAIL (baseline) |
| `EXT_SQL_CreditNotes` | incremental_key | `Returns.CreditNoteLines` → `raw.SqlCreditNote` | `bronze_sql_credit_note` | `extracts.runExtract` | FAIL (baseline) |
| `ING_FILE_CarrierScan` | file_ingest (`read_files` over UC volume, `.ctl` sidecar) | `carrier_scan_*.csv` → `raw.FileCarrierScan` | `bronze_file_carrier_scan`, `err_rejected_file_row`, `ctl_file_ingestion_log` | `carrier_scan.runCarrierScanIngestion` | PARTIAL |
| `STG_Load_Shipment` | incremental_append (LastEditedWhen watermark, dedup on business key) | `raw.SqlShipment`, `raw.SqlShipmentLine` → `stg.Shipment`, `stg.ShipmentLine` | `silver_shipment`, `silver_shipment_line` | `staging.runStgLoadShipment` | PARTIAL |
| `STG_Load_ReturnAndCredit` | incremental_append | `raw.SqlReturnLine`, `raw.SqlCreditNote` → `stg.Return`, `stg.CreditNote` | `silver_return`, `silver_credit_note` | `staging.runStgLoadReturnAndCredit` | PARTIAL |
| `FACT_Load_Shipment` | snapshot_fact (accumulating snapshot, MERGE, milestones never regress) | `stg.Shipment` → `Fact.Shipment` | `gold_fact_shipment` | `facts.runFactLoadShipment` | PARTIAL |
| `FACT_Load_OrderFulfilment` | snapshot_fact (order grain, stalled thresholds, closed rows frozen) | `stg.OrderFulfilment` → `Fact.Order Fulfilment` | `gold_fact_order_fulfilment` | `facts.runFactLoadOrderFulfilment` | PARTIAL |
| `FACT_Load_Return` | incremental_fact (delete/insert by return-date window, negative measures) | `stg.Return` → `Fact.Return` | `gold_fact_return`, `gold_fact_sale_reversal`, `err_rejected_fact` | `facts.runFactLoadReturn` | PARTIAL |
| `FACT_Load_CreditNote` | incremental_fact (regional tax regime, approval hold) | `stg.CreditNote` → `Fact.Credit Note` | `gold_fact_credit_note`, `gold_fact_sale_restatement`, `gold_credit_note_approval_hold` | `facts.runFactLoadCreditNote` | PARTIAL |
| `AGG_Refresh_DeliveryPerformanceSummary` | aggregate_rebuild (trailing-window delete + rebuild) | `Fact.Shipment`, `Dimension.*` → `Aggregate.Delivery Performance Summary` | `gold_agg_delivery_performance_summary`, `gold_agg_delivery_performance_lane`, `gold_agg_delivery_performance_thin_sample` | `aggregate.runAggRefreshDeliveryPerformance` | PARTIAL |

Shared modules: `config.py` (constants, `RunContext`, table names), `common.py` (federation reads,
watermarks, hashing, `stg.ufn_*` equivalents, reject rows), `reference.py` (`ref.*` code crosswalk, FX,
carrier reference), `dimensions.py` (legacy `Dimension.*` surrogate-key lookups with unknown-member
fallback, `Fact.Sale` original-sale view), `recon.py` (evidence), `runner.py` (package dispatch).

## Evidence summary (latest run)

`run_id = c7076e42-ee0b-471a-9ae8-8bd6d3fad352`, `git_sha = 4024a915…`, one row per package (12 rows):

| Verdict | Count | Packages |
|---|---|---|
| PASS | 0 | |
| PARTIAL | 8 | `ING_FILE_CarrierScan`, both `STG_Load_*`, all four `FACT_Load_*`, `AGG_Refresh_DeliveryPerformanceSummary` — the legacy target is **empty on the baseline host**, so the expected result is source-derived (`"baseline":"source_derived"` in `checks`) and the contract caps the verdict at PARTIAL |
| FAIL | 4 | the four `EXT_SQL_*` extracts — legacy `raw.*` holds the SSIS output (2200 / 6051 / 260 / 190 rows) but the live OLTP sources `Shipping.ShipmentHeaders`, `Shipping.ShipmentLines`, `Returns.ReturnLines`, `Returns.CreditNotes` hold **0 rows** on the shared host, so the migrated extract cannot reproduce the SSIS output from a live source. Bronze is a *baseline seed* of `raw.*` (matches on count + checksum, `seeded_from_legacy_raw=true`), not extract output; the `source_vs_legacy_baseline` check (`live_oltp_rows=0`, `legacy_raw_rows=N`, `pass=false`) isolates the cause: baseline inconsistency, not a migration defect. |
| NOT_APPLICABLE | 0 | |

Every row has `row_count` and `checksum` checks (`sum(xxhash64(<business columns cast to string>))`,
summed as `decimal(38,0)`, order independent) plus a `column_null_rate` and package-specific checks
(rejects, watermark, unknown members, files processed). The table `ctl_row_count_audit` holds the
per-task row counts (`etl.RowCountAudit` equivalent).

Landing table row counts after the successful run: bronze 2200 / 6051 / 260 / 190 / 24 (carrier scan);
silver 2200 shipments, 5994 lines, 176 returns, 34 credit notes; gold 2200 shipment facts, 73 595 order
fulfilment rows, 176 return facts (+176 sale reversals), 34 credit-note facts (+34 sale restatements),
42 weekly delivery-performance rows, 63 thin-sample lane rows; err 3 file rows, 2284 lookup failures,
213 constraint violations, 176 fact orphans.

## Design decisions

* **PySpark library + thin notebooks + DAB**, one job task per package with the same dependency graph as
  the `Master_Daily_ETL` control flow (extracts and file ingestion in parallel → staging → facts →
  aggregate → recon). Chosen over DLT because the packages are dominated by watermark control tables,
  delete/insert windows, MERGE-style accumulating snapshots and reject side outputs, which map directly
  onto imperative Delta operations and are unit-testable on local Spark.
* **Baseline seed of bronze (not extract output).** The legacy `raw.*` tables were produced by the SSIS extracts;
  the live OLTP `Shipping.*` / `Returns.*` tables are empty, so the first run seeds bronze from `raw.*`
  (`seeded_from_legacy_raw = true`, reported separately as `rows_seeded_from_legacy_raw`), sets the NumericKey
  watermark to the max key and then runs the package's own OLTP query for keys above the watermark (0 rows on
  this baseline, reported as `rows_inserted`). Seeded rows are never counted as extracted; the seed exists only so
  the downstream staging/fact/aggregate packages have input. Later runs are pure incremental extracts.
* **Watermarks** live in `ctl_watermark` (`etl.Watermark` equivalent), one row per source object and
  watermark type; the `stg.*` loads use `LastEditedWhen`, the extracts use the numeric key.
* **Unknown members**: dimension lookups (`Dimension.Customer`, `Stock Item`, `Carrier`, `Return Reason`,
  `Warehouse Site`, `Sales Territory`, …) resolve to the legacy `-1` unknown member and set
  `inferred_member_flag` rather than dropping rows, matching the packages' "late arriving" path.
* **Carrier scans** are validated row by row (`tracking_number`, `scan_event_code`, timestamp parse),
  compared with the `.ctl` sidecar `ROWCOUNT`, duplicates tracked, rejects written to `err_rejected_file_row`
  and files moved to `processed/` or `failed/` in the volume; `ctl_file_ingestion_log` prevents re-processing.
* **Evidence** is computed by `recon.runRecon` (bundled `recon` task) per package with failure isolation:
  an error in one package's checks produces a `FAIL` row for that package only.

## Deliberate deviations from the SSIS packages

| Package | SSIS behaviour | Databricks behaviour | Why |
|---|---|---|---|
| `STG_Load_Shipment` | `Lookup Carrier (Full Cache)` with *fail on no match* against `ref.Carrier` | Lenient: unmatched carriers keep the row with `dq_status_code='CARRIER_UNMATCHED'`, and the miss is logged in `err_rejected_lookup_failure`. `strictCarrierLookup=True` restores the SSIS behaviour. | `ref.Carrier` does not exist and `Dimension.Carrier` is empty on the host; the raw shipment carrier codes (`PACRIM`, `MERIDIAN`, …) are not in `Shipping.Carriers` (`UPSNA`, `DHLEU`, …). Strict mode would reject 100 % of shipments. |
| `FACT_Load_Return` | Row-by-row `Fact.Sale` lookup, orphans redirected to an error output | Set-based join; returns without an original sale are loaded with inferred cost/tax, `original_sale_missing_flag=true`, and logged in `err_rejected_fact` (`RETURN_NO_ORIGINAL_SALE`). | Every `raw.SqlReturnLine` row on the baseline has `InvoiceLineID = NULL` and OLTP `Returns.*` is empty, so no return can be tied to an invoice. The seed path resolves `OriginalInvoiceID` through `Sales.InvoiceLines` when an invoice line id is present. |
| `AGG_Refresh_DeliveryPerformanceSummary` | Refresh window anchored on `GETDATE()` | Anchored on the latest `despatch_date_key` in `gold_fact_shipment` (falls back to the run date). | The baseline data ends 2024-12-31; anchoring on the run date always refreshes an empty window. `reload_full_history=true` rebuilds everything. |
| `FACT_Load_OrderFulfilment` | Reads `stg.OrderFulfilment` (owned by another group) | Order grain is seeded from legacy `Fact.Order` (one row per order) and milestones come from `Fact.Sale`, `Fact.Payment` and `gold_fact_shipment`. | `stg.OrderFulfilment` and `Fact.Order Fulfilment` are both empty on the host; other groups' `ssis_*` schemas must not be read. |
| all | OLE DB batch sizes, `MaxInsertCommitSize`, row-by-row lookups, `PERSIST`/cache hints | Not reproduced; Delta writes are set-based and serverless-safe. | SSIS artefacts. |
| `ING_FILE_CarrierScan` | Windows-1252 file share `C:\WWI\Landing\carrier` | UC volume `landing/inbound/carrier`; two representative sample files (`landing_samples/`) generated from `config/landing-zone.yaml` and staged by the `stage_landing_files` task. | No sample files in the repo; `raw.FileCarrierScan` is empty on the host. |

## Open questions

1. Carrier code set: raw shipments use a code set (`PACRIM`, `MERIDIAN`, `EUROLINK`, `SWIFTFRT`, `LOCALDEL`,
   `NORTHWAY`) that appears in neither `Shipping.Carriers` nor `ref.CodeCrosswalk`; a crosswalk is needed
   before the carrier lookup can be made strict again.
2. Return ↔ invoice linkage: the baseline `raw.SqlReturnLine` has no invoice line ids, so restocking /
   cost reversal use inferred values for all 176 returns. Confirm the intended source of
   `OriginalInvoiceID` once the OLTP `Returns.*` tables are populated.
3. `Fact.Order Fulfilment` grain: seeded from `Fact.Order`; confirm with the owner of `STG_Load_OrderFulfilment`
   whether stalled-threshold parameters (`etl` config) should be read from their schema once it exists.
4. Credit-note approval: 156 of 190 credit notes are held as `CREDIT_UNAPPROVED` by the approval-band rule
   (`autoApproveBand=True` auto-approves only the `AUTO` band; the extract carries no `ApprovedBy`); confirm the band thresholds with finance.

## Deploy / run

```bash
export DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST="$DATABRICKS_DEMO_HOST" DATABRICKS_TOKEN="$DATABRICKS_DEMO_TOKEN"
cd databricks/ssis_migration/logistics_returns
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev --var="git_sha=$(git rev-parse HEAD)"
databricks bundle run ssis_logistics_returns_daily_etl -t dev      # end-to-end incl. recon
databricks bundle run ssis_logistics_returns_month_end_aggregate -t dev
```

Job parameters (`daily_etl`): `catalog`, `schema`, `batch_id`, `package_execution_id`, `git_sha`,
`reload_full_history` (`true` re-seeds bronze and rebuilds snapshots/aggregates), `seed_from_legacy_raw`.

Last successful end-to-end run (all 14 tasks, run 885109454560538); latest recon-only run (evidence above): <https://dbc-8bc9474f-40ae.cloud.databricks.com/jobs/215317092544738/runs/458789302138973?o=7474651138173478>

## Local development

```bash
pip install -r requirements-dev.txt
ruff check src notebooks tests && ruff format --check src notebooks tests
python -m pytest tests            # 18 local-Spark tests
```

Tests cover: standardisation defaults and postal rules, strict vs lenient carrier lookup, latest-scan status
promotion, line constraints / cold chain, FX latest-rate conversion, regional return rules and statutory
windows, credit-note approval bands, shipment fact measures, accumulating-snapshot non-regression, order
fulfilment milestones / lags / stalled thresholds, closed-row freezing, order-grain collapse, negative return
measures and orphans, regional tax on credit notes, Sunday-week alignment with the T-SQL proc, regional SLA
targets with the EU service-credit cap, lane tiers and thin-sample rejects, refresh window.

Evidence query:

```sql
SELECT unit, verdict
FROM otterorders_migration.evidence.recon_results
WHERE branch = 'ssis_logistics_returns'
  AND run_id = (SELECT run_id FROM otterorders_migration.evidence.recon_results
                WHERE branch = 'ssis_logistics_returns' ORDER BY run_at DESC LIMIT 1)
ORDER BY unit;
```
