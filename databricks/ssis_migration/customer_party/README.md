# SSIS → Databricks migration — group `customer_party` (22 packages)

Customer & party master (Oracle MDM), people, territories, promotions: the six extracts, five staging
loads, the party-resolution pair (`STG_Work_CustomerDedup` + `DQ_Customer_Screen`), ten dimension
loads and the late-arriving rekey routine of `Master_Daily_ETL`.

* Landing schema: `otterorders_migration.ssis_customer_party` (all Delta tables below live there)
* Evidence: `otterorders_migration.evidence.recon_results` (`branch = 'ssis_customer_party'`)
* Workspace path: `/Workspace/Shared/ssis_migration/customer_party`
* Job: `ssis_customer_party_daily` (serverless; 18 tasks in `Master_Daily_ETL` dependency order)

## Package → Databricks artifact

All packages are PySpark library code in `src/customer_party/` (one module per SSIS project folder), run by
the single thin notebook `notebooks/run_packages.py` with a `packages` task parameter. Source reads go
through Lakehouse Federation (`wwi_legacy_oracle`, `wwi_legacy_oltp`, `wwi_legacy_staging`, `wwi_legacy_dw`);
no JDBC fallback was needed. Verdicts are from the latest evidence run (see "Reconciliation").

| Package | Load type | Module / entry point | Legacy source → legacy target | Delta target | Verdict |
|---|---|---|---|---|---|
| `EXT_ORA_CustomerMaster` | incremental_timestamp | `extract.extractOracleCustomerMaster` | `wwi_mdm.cust_master` (+`cust_classification`, `cust_credit_profile`, `wwi_audit.change_log`) → `raw.OracleCustomerMaster` | `bronze_raw_oracle_customer_master`, `etl_watermark` | PARTIAL |
| `EXT_ORA_CustomerAddress` | incremental_timestamp | `extract.extractOracleCustomerAddress` | `wwi_mdm.cust_address` (+`ref.Geography` lookup) → `raw.OracleCustomerAddress` | `bronze_raw_oracle_customer_address`, `etl_watermark` | PARTIAL |
| `EXT_SQL_CustomerSegments` | full | `extract.extractSqlCustomerSegments` | `Sales.CustomerSegmentAssignments` → `raw.SqlOrder` | `bronze_raw_sql_order` (`record_kind='SEGMENT'`) | PARTIAL |
| `EXT_SQL_People` | full | `extract.extractSqlPeople` | `Application.People` → `raw.SqlOrder` | `bronze_raw_sql_order` (`record_kind='PERSON'`) | PARTIAL |
| `EXT_SQL_SalesTerritories` | full | `extract.extractSqlSalesTerritories` | `Sales.SalesTerritories` (+`SalesQuotas`) → `raw.SqlOrder` | `bronze_raw_sql_order` (`record_kind='TERRITORY'`) | PARTIAL |
| `EXT_SQL_Promotions` | full | `extract.extractSqlPromotions` | `Sales.Promotions` (+`PromotionLines`, `PromotionRedemptions`) → `raw.SqlOrder` | `bronze_raw_sql_order` (`record_kind='PROMOTION'`) | PARTIAL |
| `STG_Load_Customer` | truncate_reload | `staging.loadStagedCustomer` | `raw.OracleCustomerMaster` → `stg.Customer` | `stg_customer` | PARTIAL |
| `STG_Load_CustomerAddress` | truncate_reload | `staging.loadStagedCustomerAddress` | `raw.OracleCustomerAddress` → `stg.CustomerAddress` | `stg_customer_address` | PARTIAL |
| `STG_Load_Employee` | truncate_reload | `staging.loadStagedEmployee` | `raw.SqlOrder` → `stg.Employee`, `stg.Salesperson` | `stg_employee`, `stg_salesperson` | PARTIAL |
| `STG_Load_PromotionAndTerritory` | truncate_reload | `staging.loadStagedPromotionAndTerritory` | `raw.SqlOrder` → `stg.Promotion`, `stg.SalesTerritory` | `stg_promotion`, `stg_sales_territory` | PARTIAL |
| `STG_Work_CustomerDedup` | work_rebuild | `dedup.runCustomerDedup` | `stg.Customer`, `stg.CustomerAddress` → `work.CustomerAddressStandardized`, `work.CustomerDedup` | `work_customer_address_standardized`, `work_customer_dedup` | PARTIAL |
| `DQ_Customer_Screen` | quality_screen | `quality.runCustomerQualityScreen` | `stg.Customer` → `err.RejectedCustomer` | `dq_customer_rule_result`, `err_rejected_customer` | PARTIAL |
| `DIM_NA_Load_Customer` | SCD2 | `dim_customer.loadRegionalCustomerDimension(region='NA')` | `stg.Customer` → `Dimension.Customer` | `dim_customer` (`region='NA'`) | PARTIAL |
| `DIM_EU_Load_Customer` | SCD2 | `dim_customer.loadRegionalCustomerDimension(region='EU')` | `stg.Customer` → `Dimension.Customer` | `dim_customer` (`region='EU'`) | PARTIAL |
| `DIM_APAC_Load_Customer` | SCD2 | `dim_customer.loadRegionalCustomerDimension(region='APAC')` | `stg.Customer` → `Dimension.Customer` | `dim_customer` (`region='APAC'`) | PARTIAL |
| `DIM_Load_CustomerCategory` | SCD1 | `dim_supporting.loadCustomerCategoryDimension` | `Sales.CustomerCategories` → `stg.CustomerCategory` → `Dimension.Customer Category` | `stg_customer_category`, `dim_customer_category` | PARTIAL |
| `DIM_Load_CustomerSegment` | SCD2 | `dim_supporting.loadCustomerSegmentDimension` | `Sales.CustomerSegments` (+assignments) → `stg.CustomerSegment` → `Dimension.Customer Segment` | `stg_customer_segment`, `dim_customer_segment` | PARTIAL |
| `DIM_Load_Employee` | SCD2 | `dim_supporting.loadEmployeeDimension` | `stg.Employee` → `Dimension.Employee` | `dim_employee` | PARTIAL |
| `DIM_Load_Salesperson` | SCD2 | `dim_supporting.loadSalespersonDimension` | `stg.Salesperson` → `Dimension.Salesperson` | `dim_salesperson`, `dim_salesperson_territory_bridge` | PARTIAL |
| `DIM_Load_SalesTerritory` | SCD1 | `dim_supporting.loadSalesTerritoryDimension` | `stg.SalesTerritory` → `Dimension.Sales Territory` | `dim_sales_territory` | PARTIAL |
| `DIM_Load_Promotion` | SCD2 | `dim_supporting.loadPromotionDimension` | `stg.Promotion` → `Dimension.Promotion` | `dim_promotion` | PARTIAL |
| `DIM_Rekey_LateArriving` | rekey | `rekey.runLateArrivingRekey` | `work.LateArrivingDimensionQueue` → `Fact.*` | `work_late_arriving_dimension_queue`, `work_fact_rekey_queue`, `rekey_result` | PARTIAL |

Every Delta table carries `batch_id` and `load_ts` (the SSIS `BatchId` / `LoadDate` audit columns).

## Layout

```
databricks.yml                  bundle `ssis_customer_party`, target dev -> /Workspace/Shared/ssis_migration/customer_party
resources/ssis_customer_party_daily.job.yml   the job (18 serverless notebook tasks)
notebooks/run_packages.py       thin runner: widgets -> customer_party.run.runPackages()
src/customer_party/
  config.py        catalogs, evidence constants (branch/actor/harness), PipelineConfig
  spark_session.py active Databricks session or local Spark for pytest
  watermark.py     etl.usp_GetWatermark / usp_SetWatermark (etl_watermark table, 120-min lookback, ReloadFullHistory)
  extract.py       01_oracle_extract + 02_sqlserver_extract packages -> bronze_*
  staging.py       04_staging STG_Load_* (Oracle FN_* / stg.usp_* rules) -> stg_*
  dedup.py         STG_Work_CustomerDedup (stg.usp_DeduplicateCustomer) -> work_*
  quality.py       DQ_Customer_Screen (etl.usp_EvaluateDataQualityRules) -> dq_*, err_*
  scd.py           generic SCD1 / SCD2 engine + reserved members (Integration.usp_EnsureUnknownMembers)
  dim_customer.py  NA / EU / APAC regional candidates -> one dim_customer
  dim_supporting.py category, segment, employee, salesperson (+territory bridge), territory, promotion
  rekey.py         DIM_Rekey_LateArriving as a reusable routine (see below)
  recon.py         evidence writer -> otterorders_migration.evidence.recon_results
  run.py           package-name -> runner dispatch (all 22 + RECON)
tests/             pytest on local Spark (SCD2, SCD1, dedup, watermark, rekey, staging rules, schema chain)
```

## Design decisions

* **One implementation style (PySpark library + thin notebook)**, not DLT: the group is dominated by
  imperative semantics (watermark bookkeeping, truncate-reload, MERGE-style SCD, a queue state machine,
  evidence writing) that map naturally to batch PySpark and are unit-testable on local Spark. DLT would have
  needed one pipeline per load type for the same code.
* **Federation only.** Everything the packages read (Oracle MDM/audit, OLTP, `ref.*` / `etl.*` control rows,
  legacy DW dimensions) is a plain `SELECT`, so `wwi_legacy_*` foreign catalogs are sufficient.
* **Cross-group objects are read from the legacy side** (`wwi_legacy_staging.ref.Geography`,
  `wwi_legacy_dw.Dimension.*` for baseline counts). No other `ssis_*` schema is read or written.
* **Watermarks** live in `etl_watermark` (source_system_code, object_name, watermark_value) — the equivalent of
  `etl.Watermark` + `usp_Get/SetWatermark`. The window is `[last watermark - 120 min, now]`; the first run (or
  `reload_full_history=true`) starts at 1900-01-01, exactly like the package's `ReloadFullHistory` parameter.
  `setWatermark` is an idempotent `CREATE TABLE IF NOT EXISTS` + `MERGE` because the two Oracle extracts run
  concurrently.
* **`raw.SqlOrder` as a multi-entity landing table** is reproduced as `bronze_raw_sql_order` with
  `record_kind` ∈ {SEGMENT, PERSON, TERRITORY, PROMOTION}, `source_key` and a JSON `payload` column — the
  four EXT_SQL packages all wrote into that one table in the legacy estate.
* **Three regional customer packages → one `dim_customer`** with a `region` column (partition column of the
  legacy `Dimension.Customer`). Each region's SSIS package had its own derived columns (NA: ZIP+4, CCPA status,
  tax nexus; EU: VAT validation, GDPR consent/retention/pseudonymisation, EUR credit; APAC: GST/ABN
  validity, consent regime, fiscal-year label, distributor tier). Those attributes are populated for their
  region and NULL for the others; the SCD2 merge runs per region, so a region load only touches its own
  partition (`replaceWhere region = ...`), preserving the legacy "three packages write one table" contract
  without lock contention.
* **SCD2** (`scd.applyScd2`): row hash over the tracked columns; changed rows close the current version
  (`valid_to = load time`, `is_current = false`) and open a new version; inferred members
  (`is_inferred_member = true`, created by `DIM_Rekey_LateArriving` consumers) are replaced in place, not
  versioned, as in `Integration.usp_*` for late arrivals. Unknown members `-1..-4, -9` are ensured on every load.
* **SCD1** (`scd.applyScd1`): overwrite in place, new business keys get contiguous surrogate keys above the
  current max; reserved negative keys are kept.
* **Dedup / party resolution** (`dedup.py`): match tiers EXACT_TAXNUM → NAME_POSTAL → NAME_FUZZY;
  survivorship = source rank + completeness + recency + EU consent bonus, tie-break on lowest business key.
  The survivor's key is written back to `stg_customer.survivor_business_key`, exactly what
  `stg.usp_DeduplicateCustomer` did with `work.CustomerDedup`.
* **DQ screen** (`quality.py`): rules and severities from `etl.DataQualityRule` are encoded as `DqRule`s.
  FAIL rows go to `err_rejected_customer` and are excluded from the dimension loads; WARN rows load flagged.
  The package fails when the blocking-failure share exceeds 25 % (SSIS `MaxFailurePct`).
* **`DIM_Rekey_LateArriving` as a reusable routine.** `rekey.py` exports `CUSTOMER_PARTY_DIMENSIONS`
  (a `DimensionRekeySpec` per dimension: table, business key, surrogate key, region column) and two entry
  points. Fact-owning groups call

  ```python
  from customer_party.rekey import CUSTOMER_PARTY_DIMENSIONS, rekeyFactTable, repointFactKeys
  spec = next(s for s in CUSTOMER_PARTY_DIMENSIONS if s.dimensionName == "Customer")
  # pure (testable): returns the fact DataFrame with placeholder / inferred keys re-pointed
  fixed = repointFactKeys(factDf, spark.table(cfgFqn("dim_customer")), spec, "customer_key", "customer_business_key")
  # or MERGE into their own Delta fact table
  rekeyFactTable(spark, "otterorders_migration.ssis_<group>.gold_fact_sale", spec, "customer_key", "customer_business_key", dimDf)
  ```

  Placeholder keys are `0`, `-1` (Unknown) and `-4` (Inferred Pending). The queue state machine
  (`resolveQueue`) applies the regional retry limits of `Integration.usp_RekeyLateArrivingDimensions`
  (NA 3, EU 7, APAC 21, other 5): RELEASED when the member now exists, RETRY otherwise, ABANDONED
  (`HOLD_EXPIRED`) at the limit. Within this group the queue is empty (no fact load of this group feeds it),
  so the package produces the queue/result tables and a zero-row run.

## Deliberate deviations from SSIS behaviour

| SSIS behaviour | Databricks behaviour | Why |
|---|---|---|
| Row-by-row Lookup components (geography, category, segment, territory) | Set-based `join` | SSIS artefact; identical semantics, no per-row round trips |
| OLE DB fast load with `BatchSize`/`MaxInsertCommitSize`, `TRUNCATE TABLE` then insert | Single atomic Delta `overwrite` | Batch sizes are an OLE DB tuning knob; Delta overwrite is atomic so a failed load leaves the previous table intact instead of an empty one |
| `DELETE Dimension.Customer WHERE Region = ?` + insert | `replaceWhere region = ?` | Same result, atomic |
| Oracle extract in SSIS re-reads rows in the 120-min lookback window and relies on the destination's PK to reject duplicates | The overlap is kept, duplicates are removed by `(cust_id, updated_dt)` before landing | The legacy raw table had no PK, so re-read rows were duplicated in `raw.*`; the staging `MAX(...)`/`ROW_NUMBER` load hid it |
| Inferred-member creation inside the Lookup error output of fact packages | Not in this group; the rekey routine expects `is_inferred_member` rows created by fact loads | Fact packages belong to other groups |
| `Script Component` fuzzy name match (Levenshtein-based) | Prefix(12)+country key match | SSIS Fuzzy Lookup is not reproducible; the prefix rule is what `stg.usp_DeduplicateCustomer`'s T-SQL fallback did |
| Package failure on DQ threshold via `ForceExecutionResult` | Python exception → task failure | Same effect on the job |
| Watermark set by `Execute SQL Task` at the end of the package | `setWatermark` after the Delta write commits | Same ordering; MERGE instead of `usp_SetWatermark` |

## Reconciliation

`recon.writeEvidence` (task `recon`) appends one row per package (22 rows, one `run_id`) to
`otterorders_migration.evidence.recon_results` with `unit_type='ssis_package'`, `branch='ssis_customer_party'`,
`actor='devin:ssis_customer_party'`, `harness_version='ssis-migration-v1'`, `git_sha` = the deployed commit.
Each row has a `row_count` check and an order-independent `checksum` check
(`sum(xxhash64(concat_ws('|', business cols)))`, surrogate keys and load timestamps excluded).

**No package in this group can be `PASS`.** On the baseline host the SSIS run never populated the legacy
targets of these packages: `raw.*`, `stg.*`, `work.*`, `err.*` in `WideWorldImporters_Staging` are empty and
`Dimension.Customer` / `Dimension.Employee` / `Dimension.Customer Category` / etc. in `WideWorldImportersDW`
hold the vendor sample data (e.g. 406 customers keyed by the WWI sample surrogate keys), not the output of
these Oracle-MDM-driven packages (`Dimension.Promotion`, `Dimension.Salesperson` are empty / reserved-only).
As required by the contract, every check therefore compares the Delta table against an expected result
re-derived from the federated sources with the package's own transformation (`"baseline":"source_derived"`),
the legacy row count is recorded as `legacy_row_count`, and the verdict is capped at `PARTIAL`. A mismatch
against the source-derived expectation is `FAIL`.

```sql
SELECT unit, verdict, summary
FROM otterorders_migration.evidence.recon_results
WHERE branch = 'ssis_customer_party'
  AND run_id = (SELECT run_id FROM otterorders_migration.evidence.recon_results
                WHERE branch = 'ssis_customer_party' ORDER BY run_at DESC LIMIT 1)
ORDER BY unit;
```

## Running

```bash
export DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST="$DATABRICKS_DEMO_HOST" DATABRICKS_TOKEN="$DATABRICKS_DEMO_TOKEN"
cd databricks/ssis_migration/customer_party
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run ssis_customer_party_daily -t dev            # full daily run + recon
databricks bundle run ssis_customer_party_daily -t dev --params reload_full_history=true
```

Local checks:

```bash
pip install pyspark==3.5.3 pytest ruff
ruff check src tests
python -m pytest tests -q      # ~2 min on local Spark
```

## Open questions

* The legacy `raw.SqlOrder` landing table is shared by four extract packages and by groups that own the
  order extracts; this group only lands its four `record_kind`s. If the order-extract group lands theirs in
  their own schema, the union that `stg.*` loads depended on no longer exists — by design (schemas are per group).
* `Sales.Promotions` is empty on the baseline, so `EXT_SQL_Promotions`, `stg_promotion` and `dim_promotion`
  are exercised with zero business rows (reserved members only).
* `DIM_Rekey_LateArriving` needs the fact groups to write `work_late_arriving_dimension_queue` rows (or call
  `rekeyFactTable`) — until they do, the package runs against an empty queue.
* `Dimension.Customer` on the legacy host carries the vendor sample data; once the SSIS estate is actually run
  on the host, `recon.py` will pick up the populated legacy counts automatically (`legacy_row_count`), but
  the verdict logic would need the `PASS` path re-enabled by comparing against the legacy checksum instead of
  the source-derived one.
