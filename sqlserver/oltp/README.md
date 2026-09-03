# WP03 - WideWorldImporters OLTP extensions

Additive SQL Server scripts that extend the existing WideWorldImporters OLTP
database (`SQLSERVER_OLTP_DB`). Nothing here modifies `wwi-ssdt/`; every script
either creates a new object or `ALTER`s an existing table with new columns.

Nothing in this directory has been executed against a database. Only the static
checks in `validation/static/run_all_checks.py` have been run.

## Deploy order

Scripts are numbered so the order is unambiguous. Run each directory in name
order, then move to the next directory.

| Directory | Contents |
|---|---|
| `00_schemas` | `Shipping`, `Returns`, `Loyalty`, `Ecommerce`, `Integration` schemas and the shared sequences |
| `01_tables` | New tables for sales, warehouse, shipping, returns, loyalty, ecommerce and integration |
| `02_extensions` | `ALTER TABLE` extensions to `Sales.Orders`, `Sales.OrderLines`, `Sales.Invoices`, `Sales.Customers`, `Warehouse.StockItemHoldings`, plus the customer-transaction, employee and deletion-log tables that hang off them |
| `03_indexes` | Filtered and covering indexes used by the extract views |
| `04_functions` | Scalar and table-valued helpers (`ufn_` prefix per the build contract) |
| `05_views` | Extract and change-tracking views consumed by the ETL packages |
| `06_procedures` | Business logic (`SET NOCOUNT ON; SET XACT_ABORT ON;` throughout) |
| `07_triggers` | Audit, deletion-log and denormalisation-maintenance triggers |
| `08_seed` | Grouped, re-runnable reference data with regional divergence |

## Regional divergence

Region is carried as data (`RegionCode` of `NA`, `EU`, `APAC`) and as branching
inside the procedures, not as separate databases.

- **NA** - US sales tax / Canadian GST-HST, USD and CAD, 4-4-5 fiscal calendar,
  30 day change-of-mind window with a restocking fee, imperial units in the
  warehouse, marketing opt-out consent.
- **EU** - VAT and UK VAT, EUR and GBP, calendar fiscal year, 14 day statutory
  cooling-off with no fee chargeable, DAP incoterms, metric units, strict
  opt-in consent and shorter retention.
- **APAC** - AU/SG GST and JP consumption tax, AUD/SGD/JPY, July fiscal year,
  7 day return window with the highest restocking percentage, CIP incoterms,
  metric units.

## Deliberate legacy patterns

These are intentional and documented in the header comment of the file that
carries them.

- EAV extension attributes: `Application.ExtensionAttributeDefinitions` and
  `Application.ExtensionAttributeValues`.
- Delimited-list columns: `Shipping.Carriers.ServiceLevelList` and
  `Sales.Orders.FulfilmentFlags`.
- Overloaded status columns: `Sales.SalesChannels.ChannelStatus` means one thing
  to order entry and another to the commission run.
- Denormalised cached totals maintained by trigger rather than computed on read
  (`Sales.Orders` totals, `Warehouse.StockItemHoldings`, cart totals, loyalty
  balances).
- Row-by-row cursors and dynamic SQL inside the procedures.
- Hand-rolled amendment history (`Sales.OrderAmendments`) instead of temporal
  tables.
- Deletion logs (`Sales.OrderDeletionLog`, `Warehouse.StockItemDeletionLog`,
  `Integration.DeletedRowLog`) so incremental extracts can detect deletes.

## Change tracking for the extracts

`Integration.ChangeTrackingWatermark` holds one row per consumer and source
table; `Integration.usp_GetChangeWatermark` and
`Integration.usp_SetChangeWatermark` are the only supported way to read and
advance it. `Integration.vw_ChangedKeysSinceWatermark` and
`Integration.vw_DeletedKeysForExtract` expose changed and deleted keys to the
`EXT_SQL_*` packages.

## Configuration

No credentials are stored here. Connection details come from the environment
placeholders used across the estate: `SQLSERVER_HOST`, `SQLSERVER_PORT`,
`SQLSERVER_USER`, `SQLSERVER_PASSWORD`, `SQLSERVER_OLTP_DB`.
