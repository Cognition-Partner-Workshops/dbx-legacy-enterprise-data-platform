# Conformed reference layer (`ref`)

The `ref` schema is the estate's single conformed view of the code sets and
reference data that both source systems - the Oracle ERP and the
WideWorldImporters SQL Server OLTP - describe in their own way. The tables live
in `sqlserver/staging/tables/50_ref_tables.sql`; the load logic that populates
them lives here.

Everything in this folder is a stored procedure or a view deployed into
**`WideWorldImporters_Staging`** (`SQLSERVER_STAGING_DB`). Nothing here has been
executed against a database.

## What loads what

| Procedure | Populates | Reads |
| --- | --- | --- |
| `ref.usp_LoadRegion` | `ref.Region` | maintained regional policy, `ref.CodeCrosswalk` |
| `ref.usp_LoadCountry` | `ref.Country` | `raw.OracleGeography`, `ref.Region`, `ref.CodeCrosswalk` |
| `ref.usp_LoadCurrency` | `ref.Currency` | `raw.OracleCurrency` |
| `ref.usp_LoadFxRateDaily` | `ref.FxRateDaily` | `raw.OracleFxRate`, `ref.Currency` |
| `ref.usp_LoadTaxJurisdiction` | `ref.TaxJurisdiction` | `raw.OracleTaxRate`, `ref.Country`, `ref.Region` |
| `ref.usp_LoadPostalFormatRule` | `ref.PostalFormatRule` | `ref.Country`, `ref.Region` |
| `ref.usp_LoadUnitOfMeasure` | `ref.UnitOfMeasure` | `raw.OracleProductMaster` |
| `ref.usp_LoadUomConversion` | `ref.UomConversion` | `raw.OracleProductMaster`, `ref.UnitOfMeasure` |
| `ref.usp_LoadStatusCode` | `ref.StatusCode` | maintained conformed status set |
| `ref.usp_LoadReasonCode` | `ref.ReasonCode` | maintained conformed reason set, `raw.SqlReturnLine` |
| `ref.usp_LoadCodeCrosswalk` | `ref.CodeCrosswalk` | steward grid, `ref.StatusCode`, `ref.ReasonCode` |
| `ref.usp_LoadSourceKeyCrosswalk` | `ref.SourceKeyCrosswalk` | `raw.OracleProductMaster`, `raw.OracleGeography`, `ref.Country` |
| `ref.usp_ReportUnmappedCodes` | `err.RejectedLookupFailure` | every code-bearing `raw.*` column, `ref.CodeCrosswalk` |
| `ref.vw_UnmappedSourceCode` | (view) | `err.RejectedLookupFailure`, `ref.CodeCrosswalk` |

Every procedure takes `@BatchId` and `@PackageExecutionId`, wraps its work in
`TRY`/`CATCH` with an explicit transaction, reports its counts through
`etl.usp_LogRowCount`, reports failures through `etl.usp_LogError`, and routes
rows it cannot conform into `err.RejectedLookupFailure` or
`err.RejectedConstraintViolation` rather than dropping them.

## Refresh cadence

The whole layer is refreshed by the weekly `Master_Weekly_Reference_Load`, which
runs the fourteen packages in `ssis/06_reference_data`. Each package calls the
procedures it needs before it publishes its dimension, so the conformed layer is
always rebuilt ahead of the warehouse read:

* weekly, with the reference load - regions, countries, currencies, tax
  jurisdictions, postal rules, units, status and reason codes, and every
  crosswalk domain;
* daily, with the finance extracts - `ref.usp_LoadFxRateDaily`, because the rate
  file arrives every business day;
* on demand - a steward who adds a mapping to the grid in
  `ref.usp_LoadCodeCrosswalk` re-runs `REF_Load_CodeTranslation` rather than
  waiting for the weekend.

`ref.FxRateDaily` is the only table that is genuinely incremental. Its
fill-forward rule is: a currency pair with no quote for a date carries the last
quoted rate forward for up to **five** consecutive days, which covers weekends
and the longest public holiday run the treasury file has ever skipped. Beyond
five days the rate is not carried and the pair is reported as a gap, so a
retired pair does not quietly keep its last rate for a year.

## How a code reaches the conformed set

1. The source system's code arrives in `raw.*` - `raw.Oracle*` for the ERP,
   `raw.Sql*` for the OLTP - exactly as the source spelled it.
2. `ref.usp_LoadCodeCrosswalk` holds the steward-maintained grid of mappings.
   Each row is `(CodeDomainCode, SourceSystemCode, SourceCodeValue) ->
   ConformedCodeValue`, optionally qualified by `RegionCode` and always
   effective-dated. Re-running the procedure closes the previous interval with
   an `EffectiveToDate` and opens a new one; nothing is deleted.
3. The conformed value must already exist in the conformed set for its domain -
   status domains are checked against `ref.StatusCode`, reason domains against
   `ref.ReasonCode` - and a mapping that points at a value which does not exist
   is rejected into `err.RejectedConstraintViolation` instead of being loaded.
4. Downstream loads translate by joining `ref.CodeCrosswalk` on the domain, the
   source system and the source code, taking the row whose effective interval
   covers the run date, and falling back to the `UNKNOWN` member.
5. Anything the grid does not cover is found by `ref.usp_ReportUnmappedCodes`,
   which scans the code-bearing columns of `raw.*` and writes one reject per
   unmapped value. The data-quality packages read
   `ref.vw_UnmappedSourceCode`, which collapses those rejects into one line per
   domain, source system and code and drops a code as soon as a mapping for it
   appears.

The same code can mean different things in different places, so the crosswalk
carries a region: the OLTP channel code `DIR` is the direct sales force in NA
and a distributor in APAC, and each maps to a different conformed channel.

## Regional divergence held here

* **Tax.** `ref.TaxJurisdiction` is effective-dated and the three regimes are
  genuinely different shapes. NA sales tax stacks state, county, city and
  special-district rates onto a postal-code range and carries no registration
  requirement. EU VAT is one country-level rate with a registration mask and a
  reverse-charge flag for cross-border B2B supply. APAC GST is a single national
  rate with no sub-national components at all.
* **Postal codes.** `ref.PostalFormatRule` is per country, not per region: what
  to strip, whether to upper-case, the mask, the length bounds and where the
  separator goes. NA truncates ZIP+4 to five; the EU keeps the country-specific
  masks; APAC strips separators.
* **Fiscal calendars, currencies, weight units and consent models** are
  attributes of `ref.Region` and are read from there by everything downstream,
  including `REF_Load_DateDimension`, so a region's fiscal year end is defined
  in exactly one place.
