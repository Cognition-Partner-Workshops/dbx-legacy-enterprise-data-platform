# `sales_lakehouse.silver.rules` - tax / FX / fiscal-calendar rules

One region-parameterised, pure-function library that replaces the tax, FX and
fiscal-period logic scattered across the legacy SQL Server, SSIS and Oracle
estate. Every function takes and returns a DataFrame; the only I/O is reading
a silver reference table through `cfg.fqn("silver", ...)`. The reference
tables themselves are built by `sales_lakehouse.silver.reference.run(spark, cfg)`.

Spec: `docs/domain-model/business-domains.md`, "Cross-cutting: tax, FX and
fiscal calendar" (quoted in each module docstring).

## Public API (stable - other workstreams code against these)

| Function | Adds | Reads |
|---|---|---|
| `tax.applyTax(df, regionCol="region_code", grossCol=..., netCol=..., taxRateCol=..., isReverseChargeCol=..., vatRegCol=...)` | `net_amount_local`, `tax_amount_local`, `gross_amount_local` (`decimal(19,4)`), `tax_treatment_code`, `tax_residual_local` | nothing |
| `fx.applyFx(df, spark, cfg, amountCols, currencyCol="currency_code", dateCol="transaction_date", regionCol="region_code")` | `fx_rate_to_reporting` (`decimal(19,8)`), `fx_rate_source_code`, `fx_rate_effective_date`, `<col>_usd` per amount column | `silver.ref_fx_rate` |
| `fiscal.resolveFiscalPeriod(df, spark, cfg, dateCol, regionCol="region_code")` | `fiscal_calendar_code`, `fiscal_year`, `fiscal_period`, `fiscal_period_key` | `silver.dim_fiscal_calendar` |
| `fiscal.regionCalendarCode(regionCol) -> Column` | `NA -> NA445`, `EU -> EUCAL`, `APAC -> APACJUN` | nothing |
| `fiscal.naFiscalPeriodFor(df, dateCol)` | same four fiscal columns, always under `NA445` | nothing |

Data-quality: none of the functions drops or rejects a row. Documented quirks
set `dq_status_code` (`PASS` < `WARN` < `FAIL`, never lowered) and append a
reason to `dq_reason_codes` via `rules.dq.tagDq`. Callers quarantine `FAIL`
rows with `common.quality.quarantine` if they choose to.

Rates are fractions (`0.19`, not `19`). `ref_tax_rate_*` carries both
`*_rate_pct` (as in Oracle) and `*_rate` (fraction) columns.

## Reference tables (`silver.reference`)

| Table | Grain | Built from (bronze) | Legacy source |
|---|---|---|---|
| `ref_fx_rate` | currency / rate_date / rate_type / feed region | `oracle_wwi_ref_fx_rate_daily` | `WWI_REF.FX_RATE_DAILY`, `oracle/reference/02_currency_and_fx_rates.sql`, `PKG_FX.get_rate` (direct -> inverse -> triangulated) |
| `ref_tax_rate_na` | jurisdiction / regime / rate category / effective segment, with state, county, city, district components and `combined_tax_rate` | `oracle_wwi_fin_tax_jurisdiction`, `oracle_wwi_fin_tax_rate` | `05_tax_na_sales_and_use.sql`, `PKG_TAX.determine_tax` (additive parent chain) |
| `ref_tax_rate_eu` | country / VAT rate row (STD, RED*, ZERO) | same | `06_tax_eu_vat.sql` |
| `ref_tax_rate_apac` | country / GST rate row, `is_price_inclusive` | same | `07_tax_apac_gst.sql` |
| `dim_fiscal_calendar` | calendar_code / fiscal_year / fiscal_period | date span of `oracle_wwi_ref_calendar_fiscal` | `11_fiscal_calendars_and_periods.sql`, `usp_PopulateDateDimension`, `Dimension.Fiscal Calendar` |
| `dim_date` | one row per date (`date_key` yyyymmdd) with all three calendars' period keys and per-region holiday flags | same + holiday rows of the Oracle calendar | `Dimension.Date`, `usp_PopulateDateDimension` |

## Rules and their legacy references

### Tax (`rules/tax.py`)

| Region | Rule | Legacy reference |
|---|---|---|
| NA | tax-exclusive; `tax = round_half_up(net * rate, 2)`; `gross = net + tax`; the rate is the sum of the jurisdiction components (`ref_tax_rate_na.combined_tax_rate`) | SSIS `FACT_NA_Load_Sale` "Calculate NA Measures" (tax on the discounted net); `Integration.usp_LoadFactSale` step 8; `PKG_TAX` "sales tax is additive across state, county and city rows" |
| EU | VAT on top. Net carried by the source -> `tax = round(net * rate, 2)`; only gross carried -> `net = round(gross / (1 + rate), 2)`, `tax = gross - net`. Reverse charge -> `tax = 0`, `REVERSE_CHARGE` | SSIS `FACT_EU_Load_Sale` "Calculate EU Measures" (Article 138 -> zero rate); `usp_LoadFactSale` "VAT is recomputed from the net so that rounding matches the statutory invoice" |
| APAC | GST-inclusive. `net = truncate(gross / (1 + rate), 2)`; `tax = gross - net`; dropped fraction kept in `tax_residual_local`. A net-only line is treated as GST-exclusive (`GST_EXCLUSIVE`) | SSIS `FACT_APAC_Load_Sale` "GST-inclusive pricing: the tax is extracted from the gross"; `PKG_TAX` "APAC prices are tax inclusive"; business-domains.md truncation paragraph |

`tax_treatment_code` values: `SALES_TAX`, `VAT`, `REVERSE_CHARGE`,
`REVERSE_CHARGE_NO_VATREG`, `GST_INCLUSIVE`, `GST_EXCLUSIVE`, `UNMAPPED_REGION`.

### FX (`rules/fx.py`)

| Region | Effective-date convention | Legacy reference |
|---|---|---|
| NA | rate dated on the transaction date, else the most recent prior rate up to 7 days back | `stg.usp_ConvertCurrencyAmounts` header ("SPOT on the transaction date, falling back up to @MaxFallbackDays", default 7) |
| EU | rate dated on the invoice date, else the most recent prior day with no window | SSIS `FACT_EU_Load_Sale` "Lookup Effective FX Rate" + "Retry Held Lines With Prior Day Rate" (`RateDate < FxRateDate ORDER BY RateDate DESC`) |
| APAC | the rate effective on the first of the transaction month, carried forward up to 7 days when the 1st has no row | `usp_ConvertCurrencyAmounts` ("CORPORATE monthly rate ... AppliedRateDate is often weeks before RequestedRateDate"); `PKG_FX.backoff_days('CORP') = 7` |

Within a date the rate from the region's own feed and the region's preferred
rate type (NA `SPOT`, EU `ECB`, APAC `CORP`) wins. `fx_rate_effective_date`
is the publication date of the rate used, so a weekend transaction matched to
an interpolated weekend row reports the Friday. Amounts are converted with
`round(amount * rate, 2)` as in `usp_LoadFactSale`.

`fx_rate_source_code` is the feed's `RATE_SOURCE_CD` (`BOC`, `ECB`,
`APAC_TREASURY`, ...), or `SAME_CCY` / `DEFAULT_1`.

### Fiscal (`rules/fiscal.py`)

| Calendar | Rule | Legacy reference |
|---|---|---|
| `NA445` | 4-4-5 retail calendar: year starts on the Sunday nearest 1 January, 52 or 53 weeks, week 53 in period 12, year named for the calendar year it mostly falls in | `usp_PopulateDateDimension` NA block; `FN_FISCAL_PERIOD` |
| `EUCAL` | calendar months, January - December | `usp_PopulateDateDimension` EU block |
| `APACJUN` | April - March, twelve monthly periods, year named for the year it closes in (`FY2025` = Apr 2024 - Mar 2025) | SSIS `FACT_APAC_Load_Sale` `FiscalYearLabel`; `FN_FISCAL_PERIOD` |

`resolveFiscalPeriod` looks the date up in `dim_fiscal_calendar` and falls
back to the same arithmetic when the date is outside the loaded span
(`FN_FISCAL_PERIOD`: "table lookup first ... arithmetic fallback").
`fiscal_period_key = fiscal_year * 100 + fiscal_period` and is only unique
together with `fiscal_calendar_code`.

## Deliberate legacy quirks (`# LEGACY QUIRK:` in code)

| Quirk | Where | Effect |
|---|---|---|
| APAC truncates `gross / (1 + rate)` to the cent; the residual is kept in `tax_residual_local` | `tax.applyTax` | tax is over-stated by up to 0.99 cent per line, exactly as the legacy load |
| Reverse charge with no VAT registration still loads | `tax.applyTax` | `REVERSE_CHARGE_NO_VATREG`, `dq_status_code = WARN`, reason `TAX_RC_NO_VATREG` |
| Missing tax rate is treated as 0 % | `tax.applyTax` | tax 0, `WARN`, reason `TAX_RATE_MISSING` |
| Missing FX rate -> 1.0, never NULL, never rejected | `fx.applyFx` | `DEFAULT_1`, `WARN`, reason `FX_RATE_DEFAULTED`; the USD amount equals the local amount |
| APAC converts the whole month at the month-start rate | `fx.applyFx` | `fx_rate_effective_date` is the 1st (or the last publication before it) |
| Unmapped region on a fiscal lookup keeps the row, `WARN`, reason `FISCAL_PERIOD_UNRESOLVED`; on tax `UNMAPPED_REGION` / `TAX_REGION_UNMAPPED` | `fiscal.resolveFiscalPeriod`, `tax.applyTax` | rows are never dropped |
| Cross-region aggregates use the NA calendar silently | `fiscal.naFiscalPeriodFor` | an EU/APAC row is reported in a period that is not the one on its own books |
| APAC list prices are GST-inclusive; the Oracle reference has no flag | `reference.buildRefTaxRateApac` | `is_price_inclusive = true` for output-tax rows, `false` for `INPUT` credits |

## Open questions

- `usp_ConvertCurrencyAmounts` uses the PERIOD_END rate for EU lines in a
  *closed* GL period. There is no period-status feed in the lakehouse, so the
  EU path always uses the invoice-date / prior-day rate (the SSIS behaviour).
- NA sales tax in `usp_LoadFactSale` copies the OLTP engine's tax amount
  ("the county breakdown is not exposed to the warehouse") while SSIS and
  `PKG_TAX` recompute it. The library recomputes; a caller that wants the
  source amount can pass it through untouched.
- Oracle `PKG_TAX` rounds the APAC net where the spec says truncate. The
  spec (and the SSIS package) win.
- `NL_ICA` is the only reverse-charge-flagged rate row in the seed. Whether
  every EU country should be reverse-charge eligible is a business decision;
  `applyTax` takes the decision from the caller's `isReverseChargeCol`.

## Running

```bash
cd databricks
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pytest tests -q                       # whole suite
pytest tests/test_silver_rules_*.py   # this workstream only
databricks bundle validate --target dev
```

Notebook: `notebooks/20_silver/build_reference.py`; job:
`resources/silver_reference_job.yml` (parameters `catalog`, `mock_data_root`).
