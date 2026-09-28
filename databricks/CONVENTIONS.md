# Sales lakehouse - shared conventions

Every workstream (mock data, bronze, silver x3, gold x2, integration) builds
against this contract. If you need to change it, change this file in your PR
and say so in the PR description; do not diverge silently.

## Layout

```
databricks/
  databricks.yml                 DAB bundle root (integration session finishes it)
  resources/*.yml                Lakeflow job definitions (one per pipeline stage)
  requirements.txt               local dev/test deps (pyspark + delta-spark, pytest)
  src/sales_lakehouse/           importable library - ALL transformation logic lives here
    common/                      config, spark session, table helpers, quarantine helper
    mock_data/                   mock source generators (workstream 1)
    bronze/                      raw ingestion (workstream 2)
    silver/                      conformation: customers/dims (3), transactions (4), rules engine (5)
    gold/                        facts (6), aggregates + report views (7)
    quality/                     data-quality / quarantine rules (integration)
    orchestration/               pipeline runner that calls the layers in dependency order
  notebooks/<layer>/*.py         thin Databricks notebooks (`# Databricks notebook source`)
                                 that import from sales_lakehouse and call one entry point
  tests/                         pytest, runs on local Spark + Delta (no Databricks needed)
  mock_data/output/              generator output, git-ignored
```

Rules:
- Logic goes in `src/sales_lakehouse/`, notebooks are wrappers. Everything must
  be runnable locally with `pytest` on the local Delta session from
  `sales_lakehouse.common.spark.getSpark()` AND on Databricks unchanged.
- Python: camelCase for variables/functions (org convention), snake_case for
  module and table/column names. Type hints on public functions. No `Any`.
- PySpark DataFrame API or Spark SQL strings - either is fine; do not use
  pandas for transformations (pandas is fine inside the mock generators).
- Delta everywhere: `df.write.format("delta")`, `DeltaTable.forName(...).merge(...)`.

## Namespaces

`PipelineConfig` (see `common/config.py`) resolves names. On Databricks the
catalog is `wwi_sales` (configurable); locally the catalog is `spark_catalog`
and tables are two-level. Always build names with `cfg.fqn(layer, table)` -
never hard-code `sales_bronze.x`.

| layer key | schema           | content                                              |
| --------- | ---------------- | ---------------------------------------------------- |
| bronze    | sales_bronze     | raw, source-shaped copies of the mock source tables  |
| silver    | sales_silver     | conformed entities (the legacy `stg.*`/`work.*` role)|
| gold      | sales_gold       | facts, aggregates, report views                      |
| quality   | sales_quality    | quarantined rows + DQ results (legacy `err.*` role)  |

## Mock source data (workstream 1 output, bronze input)

Root: `cfg.mockDataRoot` (default `databricks/mock_data/output`). One CSV per
source table, UTF-8, header row, comma-delimited, RFC4180 quoting, ISO-8601
dates (`YYYY-MM-DD`) and timestamps (`YYYY-MM-DDTHH:MM:SS`), empty string = NULL,
booleans as `0`/`1` (SQL Server BIT) or `Y`/`N` (Oracle CHAR(1) flags, as in the DDL):

```
<root>/sqlserver/<Schema>/<Table>.csv        e.g. sqlserver/Sales/Orders.csv
<root>/oracle/<SCHEMA>/<TABLE>.csv           e.g. oracle/WWI_MDM/CUST_MASTER.csv
<root>/manifest.json                         {table: {path, rows, columns, sha256}}
```

Column names are the source DDL column names **verbatim** (`OrderID`,
`CustomerPurchaseOrderNumber`, `PARTY_ID`, `EFFECTIVE_DATE`). The DDL under
`sqlserver/oltp/01_tables`, `sqlserver/oltp/02_extensions` (extensions ADD
columns to the base WWI tables in `wwi-ssdt/`) and `oracle/tables`,
`oracle/reference` is the schema contract - every workstream derives shapes
from the DDL, so nobody has to wait for the generator.

## Bronze tables (workstream 2 output, silver input)

`sales_bronze.<system>_<schema>_<table>` all lower snake_case, e.g.
`sqlserver_sales_orders`, `sqlserver_sales_order_lines`,
`oracle_wwi_mdm_cust_master`, `oracle_wwi_ref_fx_rate_daily`.
Columns = source columns verbatim (same case as the DDL) typed per the DDL
(DECIMAL(p,s) -> decimal(p,s), DATETIME2 -> timestamp, BIT -> boolean,
NVARCHAR -> string), plus metadata columns:

| column            | type      | meaning                                       |
| ----------------- | --------- | --------------------------------------------- |
| `_source_system`  | string    | `SQLSERVER_WWI_OLTP` or `ORACLE_WWIGERP`      |
| `_source_object`  | string    | `Sales.Orders`, `WWI_MDM.CUST_MASTER`         |
| `_source_file`    | string    | path of the CSV that produced the row         |
| `_load_ts`        | timestamp | ingestion time (UTC)                          |
| `_batch_id`       | bigint    | run id (`cfg.batchId`)                        |

Bronze is append-only per batch; a re-run of the same batch id replaces that
batch (`replaceWhere _batch_id = ...`).

## Silver tables (workstreams 3/4/5 output, gold input)

Lower snake_case, no prefix for conformed entities, `dim_` for dimensions,
`bridge_` for bridges, `ref_` for reference snapshots:

- workstream 3: `customer` (conformed, one row per OLTP customer, carries
  `wwiCustomerId`, resolved `erpPartyId`, `mergedIntoPartyId`), `dim_customer`
  (SCD2: `customerKey`, `validFrom`, `validTo`, `isCurrent`), `dim_sales_channel`,
  `dim_sales_territory`, `dim_salesperson`, `dim_buying_group`,
  `bridge_customer_buying_group` (allocationFactor sums to 1.0 per customer)
- workstream 4: `order`, `order_line`, `sale` (invoice header), `sale_line`,
  `payment`, `payment_allocation`, `order_hold`, `order_amendment`, `backorder`,
  `late_arriving_dimension_queue`, and `quote`/`quote_line` if time allows.
  Column names follow `sqlserver/staging/tables/22_stg_tables_sales.sql`
  converted to snake_case (`OrderBusinessKey` -> `order_business_key`,
  `NetLineAmountUsd` -> `net_line_amount_usd`).
- workstream 5: `ref_fx_rate` (region-aware daily rate, `rate_source_code`,
  `effective_date`), `ref_tax_rate_na`, `ref_tax_rate_eu`, `ref_tax_rate_apac`,
  `dim_fiscal_calendar` (calendar_code NA445/EUCAL/APACJUN, fiscal_year,
  fiscal_period, period_start, period_end), `dim_date`, and the pure-function
  library `silver/rules/{tax,fx,fiscal}.py` that workstream 4 and 6 call:
  `applyTax(df, regionCol=...)`, `applyFx(df, cfg, amountCols=[...])`,
  `resolveFiscalPeriod(df, dateCol, regionCol)`.

Silver column conventions: `region_code` (NA/EU/APAC), `source_system_code`,
`*_business_key` strings, `dq_status_code` (PASS/WARN/FAIL), `row_hash`,
`batch_id`, `loaded_at_utc`. Money columns `decimal(19,4)`, rates `decimal(19,8)`.

## Gold tables (workstreams 6/7 output)

`fact_sale`, `fact_order`, `fact_payment`, `fact_sales_margin`,
`fact_daily_sales_snapshot`, `fact_daily_backlog`, `fact_return`,
`fact_credit_note`, `fact_order_fulfilment`;
`agg_daily_sales`, `agg_monthly_sales`, `agg_regional_sales_performance`,
`agg_customer_360`, `agg_quota_attainment`, `agg_commission`;
report views `rpt_daily_sales_trend`, `rpt_sales_by_customer_month`,
`rpt_sales_by_product_month`, `rpt_sales_by_territory_month`,
`rpt_order_to_cash_cycle`, `rpt_margin_by_product_category`, `rpt_customer_360`,
`rpt_customer_churn_risk`. Fact column names = the legacy `Fact.*` column names
in snake_case (`[Sale Key]` -> `sale_key`, `[FX Rate To Reporting]` ->
`fx_rate_to_reporting`). Surrogate keys are `bigint`; unknown member = `-1`.

## Quarantine (legacy err.* behaviour)

Never silently drop a bad row. Use `sales_lakehouse.common.quality.quarantine(
spark, cfg, df, ruleCode, sourceTable, reasonCol)` which appends to
`sales_quality.rejected_rows` and returns the passing rows. Documented legacy
quirks that must be reproduced (FX default 1.0 on a missing rate, APAC
truncation, untranslated codes passing through, 'PILOT' non-commissionable)
are NOT rejections - reproduce them, tag the row (`fx_rate_source_code =
'DEFAULT_1'` etc.) and leave a `# LEGACY QUIRK:` comment at the point of code.

## Entry points

Each layer module exposes `run(spark, cfg) -> None` in
`sales_lakehouse/<layer>/<stage>.py` (e.g. `bronze.ingest.run`,
`silver.customers.run`, `gold.facts.run`). The orchestrator calls them in
order; the matching notebook is three lines. Idempotent re-runs are required.

## Tests

`cd databricks && pip install -r requirements.txt && pytest tests -q`.
Use the shared `spark` and `cfg` fixtures from `tests/conftest.py` (local
Delta warehouse under a tmp dir). Build small in-test DataFrames for your own
inputs; do not depend on another workstream's code having landed.

## Git

Branch from `devin/1790606342-databricks-sales-lakehouse` and open your PR
**into that branch**, not into `main`. Keep to your own package directories
plus `tests/`; touch `common/` only additively.
