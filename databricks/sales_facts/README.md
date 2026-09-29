# Sales Facts slice on Databricks

The `Fact.Sale` load path of the legacy SQL Server / SSIS warehouse, converted to
one Lakeflow Spark Declarative Pipeline over Delta tables. Everything under this
directory is the "after" state; nothing under `sqlserver/`, `ssis/` or the SSDT
projects was changed.

## What was inventoried (source, read-only)

| Legacy component | Kind | Role in the slice |
|---|---|---|
| `EXT_SQL_Invoices`, `EXT_SQL_InvoiceLines` | SSIS | extract `Sales.Invoices` / `Sales.InvoiceLines` to `raw.SqlInvoice` / `raw.SqlInvoiceLine` |
| `STG_Load_Sale` -> `stg.usp_AppendIncremental_SaleLine` | SSIS + proc | typing, defaults, regional tax-variance rejects |
| `FACT_NA_Load_Sale`, `FACT_EU_Load_Sale`, `FACT_APAC_Load_Sale` -> `Integration.usp_LoadFactSale` | SSIS + proc | structural rejects, in-batch dedup, dimension keys, holds, FX, tax, measures, delete-by-window + insert |
| `FACT_Dedup_Sale` -> `Integration.usp_DeduplicateFactSale` | SSIS + proc | cross-batch duplicate removal with archive |
| `FACT_Apply_Corrections` -> `Integration.usp_ApplyFactCorrections` | SSIS + proc | REV / RES correction rows |
| `stg.ExchangeRateDaily`, `Dimension.*` | tables | FX and dimension lookups |
| `Fact.Sale`, `Fact.[Fact Load Hold]`, `err.RejectedInvoiceLine`, `Fact.[Sale Duplicate Archive]` | tables | outputs |

Out of this slice, unchanged: `STG_Load_PartnerSale` (partner CSV feed into the
same fact), `FACT_Load_DailySalesSnapshot` and the downstream aggregates and
exports. They stay on the legacy path until their own slice.

## Target layout

```
databricks.yml                         bundle (targets: dev, parallel_run)
resources/sales_facts.pipeline.yml     the pipeline resource
src/sales_facts/transforms.py          pure PySpark rules, DataFrame in / DataFrame out
src/transformations/*.py               one dataset per file, thin `dp` wrappers around transforms
fixtures/                              deterministic CSV extract (3 batches) + dimension snapshots
tests/                                 pytest suite that runs the transforms on a local SparkSession
```

| Layer | Dataset | Type | Replaces |
|---|---|---|---|
| bronze | `bronze_sql_invoice` | streaming table (Auto Loader, all-string) | `raw.SqlInvoice` |
| bronze | `bronze_sql_invoice_line` | streaming table | `raw.SqlInvoiceLine` |
| bronze | `bronze_exchange_rate_daily` | streaming table | `stg.ExchangeRateDaily` |
| silver | `silver_sale_line` | materialized view | `#SaleWork` / `stg.SaleLine` after keys, FX, tax |
| silver | `silver_rejected_invoice_line` | materialized view | `err.RejectedInvoiceLine` |
| silver | `silver_fact_load_hold` | materialized view | `Fact.[Fact Load Hold]` |
| silver | `silver_inferred_customer` | materialized view | inferred-member insert into `Dimension.Customer` |
| gold | `fact_sale` | materialized view | `Fact.Sale` |
| gold | `fact_sale_duplicate_archive` | materialized view | `Fact.[Sale Duplicate Archive]` |

Dimensions (`dim_customer`, `dim_stock_item`, `dim_salesperson`, `dim_city`,
`dim_sales_channel`, `dim_promotion`) are read from `${var.dimension_schema}`;
they belong to the dimension slice. Expected columns: `<x>_business_key` /
`<x>_code`, `<x>_key`, and `valid_from` / `valid_to` for the effective-dated ones.

## Design decisions

**Bronze is append-only history; the fact is recomputed from it.** The legacy
proc is stateful: it deletes the five-day window and re-inserts, then writes
REV/RES rows by comparing to what is already in `Fact.Sale`. Here every batch of
an invoice line stays in bronze and `build_fact_sale()` derives ORIG / REV / RES
from the ordered versions of each natural key. Consequences:

- A refresh is idempotent. Re-landing an extract file changes nothing
  (`test_replaying_an_extract_file_does_not_change_the_fact`).
- The five-day backdating allowance becomes unnecessary: a late or backdated
  invoice is loaded when it arrives, whatever its date
  (`test_backdated_invoice_in_later_batch_is_loaded_once_as_orig`).
- `sale_key` is a deterministic hash (natural key, batch, source row version,
  correction type) instead of an IDENTITY; `corrected_sale_key` links REV/RES to
  the row they restate.
- Holds release on their own: once `dim_stock_item` has the key, the next
  refresh loads the line and drops it from `silver_fact_load_hold`
  (`test_held_line_releases_once_dimension_catches_up`). The legacy
  `usp_RekeyLateArrivingDimensions` sweep has no target equivalent.

**Inferred customers are published, not inserted.** The legacy fact proc wrote
`*** INFERRED <key>` rows into `Dimension.Customer`. A fact pipeline must not
write another slice's table, so `silver_inferred_customer` lists the keys and
the dimension pipeline creates the members. Until it does, those sales carry
`customer_key = 0` and `inferred_member_flag = 1` (same as legacy).

**Legacy quirks kept on purpose** (they are what the parallel run reconciles against):

- The tax-variance check uses source-UOM quantity x unit price, the fact
  computes gross from base-UOM quantity. Both as in the two legacy procs.
- NA keeps the source tax amount; EU recomputes VAT from net (so a discounted
  line's tax differs from the source's), reverse-charge EU lines get zero;
  APAC recomputes GST unless `GstFreeFlag = 1`.
- Freight is excluded from net and margin but carried as its own measure.
- Missing FX rate falls back to `1.0` dated on the invoice date rather than
  holding the row. Worth a decision before cutover; see below.

## `Fact.Sale` column map

| Legacy column | Target column | Note |
|---|---|---|
| `[Sale Key]` | `sale_key` | deterministic hash, not IDENTITY |
| `[City Key]`, `[Customer Key]`, `[Bill To Customer Key]`, `[Stock Item Key]`, `[Salesperson Key]`, `[Sales Channel Key]`, `[Promotion Key]` | `city_key`, `customer_key`, `bill_to_customer_key`, `stock_item_key`, `salesperson_key`, `sales_channel_key`, `promotion_key` | 0 = unknown member, -1 = not applicable |
| `[Invoice Date Key]`, `[Delivery Date Key]` | `invoice_date`, `delivery_date` | DATE |
| `[WWI Invoice ID]` | `wwi_invoice_id` | `TRY_CONVERT(INT, RIGHT(n, 9))` |
| `[Quantity]`, `[Quantity Base UOM]` | `quantity_base_uom` | one column |
| `[Quantity Source UOM]`, `[Source UOM Code]`, `[Unit Price]`, `[Tax Rate]` | same, snake_case | |
| `[Total Excluding Tax]`, `[Net Amount]` | `net_amount` | one column |
| `[Tax Amount]`, `[Total Including Tax]` | `tax_amount`, `total_including_tax` | |
| `[Profit]`, `[Gross Margin Amount]` | `gross_margin_amount` | one column |
| `[Gross Amount]`, `[Line Discount Amount]`, `[Freight Amount]`, `[Cost Of Sale Amount]`, `[Margin Percent]` | same, snake_case | |
| `[Net Amount Reporting]`, `[Tax Amount Reporting]`, `[Gross Margin Reporting]` | same, snake_case | |
| `[Fx Rate]`, `[Fx Rate Date]`, `[Fx Rate Source Code]`, `[Transaction Currency Code]` | `fx_rate`, `fx_rate_date`, `fx_rate_source_code`, `transaction_currency` | |
| `[Invoice Number]`, `[Order Number]`, `[Region Code]`, `[Tax Regime Code]`, `[Customer Vat Number]`, `[Gst Free Flag]`, `[Natural Key Hash]`, `[Correction Type Code]`, `[Inferred Member Flag]`, `[Batch Id]` | same, snake_case | `natural_key_hash` is the same SHA-256 over `invoice|line|region` |
| - | `corrected_sale_key`, `source_row_version`, `source_system_code` | new |
| `[Description]`, `[Package]`, `[Total Dry Items]`, `[Total Chiller Items]`, `[Lineage Key]`, `[Load Datetime]` | dropped | denormalised item attributes / constants; join `dim_stock_item` instead |

## Fixtures and tests

No Sales-Facts-specific fixture existed in the legacy repo (`validation/` covers
package structure, not data), so `fixtures/` is a new deterministic extract:
three batches, 20 invoices, every branch of the legacy rules hit at least once.

```
cd databricks/sales_facts
pip install -e ".[dev]"
pytest            # 22 tests, ~35 s on a local SparkSession
ruff check . && ruff format --check .
```

Covered: NA / EU / APAC valid lines; EU reverse charge; APAC GST-free; UOM
conversion; GROUP and APAC_TREASURY effective-dated FX; missing-FX fallback;
dimension defaults (bill-to fallback, DIRECT channel, -1 for no
salesperson/promotion); inferred customer; the four structural rejects; NA tax
variance; reverse-charge-with-tax reject; DRAFT exclusion; stock-item hold and
release; in-batch highest-version winner + archive; cross-batch exact replay +
archive; a two-step restatement chain (ORIG -> REV/RES -> REV/RES) including
`corrected_sale_key` links and as-at totals; unique sale keys; backdated
invoice; extract replay idempotency; natural-key hash equality with
`HASHBYTES('SHA2_256', ...)`.

## Deploying

```
export DATABRICKS_CONFIG_PROFILE=<migration principal profile>
export BUNDLE_VAR_migration_catalog=<catalog from .migration/allowed_targets.json>
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run sales_facts_pipeline -t dev
```

The target catalog allowlist is `.migration/allowed_targets.json` (`wwi_sales`).
The dev target reads extracts from the `wwi_sales.landing.legacy_extracts`
volume and dimensions from `wwi_sales.dimensions_<user>`; for a fixture run,
copy `fixtures/sqlserver` and `fixtures/reference` into the volume and load
`fixtures/dimensions` into that schema (keys as `BIGINT`, `valid_from`/`valid_to`
as `DATE`).
Land the extracts as CSV with headers under
`${var.landing_root}/sqlserver/Sales.Invoices/`, `.../Sales.InvoiceLines/` and
`.../reference/ExchangeRateDaily/`, one file per extract batch, with the
`BatchId` and `SourceSystemCode` columns the legacy extract packages add.

## Open decisions before parallel run

1. Missing FX rate: keep the legacy `1.0` fallback (reconciles, silently wrong
   reporting amounts) or hold the line like a missing stock item.
2. Whether `fact_sale` should also carry the partner-sale feed
   (`stg.PartnerSale`) now or when that slice is converted.
3. Whether `silver_fact_load_hold` needs the legacy retry-limit abandonment
   (`usp_RekeyLateArrivingDimensions`: `Hold Status Code = 'ABANDONED'` plus a
   `HOLD_EXPIRED` reject after the regional retry limit) or whether an open
   hold is acceptable. Retry counts have no meaning when the hold is
   recomputed from bronze each refresh; the equivalent would be an age cutoff.
