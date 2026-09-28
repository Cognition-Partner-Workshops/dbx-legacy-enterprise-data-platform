# `sales_lakehouse.mock_data` — deterministic mock source data

The legacy WideWorldImporters estate is not reachable, so every layer of the sales lakehouse
is developed and tested against data produced by this package. It writes the layout defined in
`databricks/CONVENTIONS.md` § "Mock source data":

```
<root>/sqlserver/<Schema>/<Table>.csv      # SQL Server OLTP extracts (exact DDL casing)
<root>/oracle/<SCHEMA>/<TABLE>.csv         # Oracle WWI_MDM / WWI_REF / WWI_FIN extracts
<root>/manifest.json                       # tables, row counts, sha256, edge cases
```

CSV is RFC 4180, UTF-8, header row, `NULL` = empty field, SQL Server `BIT` = `0`/`1`, Oracle flags
= `Y`/`N`, dates ISO-8601, decimals with DDL scale. Column names and order come from
`schema.py`, which mirrors `sqlserver/oltp/01_tables` + `02_extensions` + `wwi-ssdt` and
`oracle/tables` + `oracle/reference`.

## Running

```bash
cd databricks && . .venv/bin/activate && export PYTHONPATH=src   # pytest.ini sets pythonpath=src for tests
python -m sales_lakehouse.mock_data.generate --scale small --out mock_data/output
python -m sales_lakehouse.mock_data.generate --scale medium --seed 7 --parquet --out /tmp/mock
```

| Option | Default | Meaning |
|---|---|---|
| `--seed` | `42` | Every RNG stream is derived from `(seed, stream name)`; same seed + scale + as-of ⇒ byte-identical files. |
| `--scale` | `small` | `small` ≈ 200 customers / 5k orders / 150 stock items / 24 reps; `medium` ≈ 10× (2k / 50k / 500 / 120). |
| `--out` | `mock_data/output` | Output root (git-ignored). |
| `--parquet` | off | Also writes `.parquet` twins (pyarrow) of the 5 largest tables next to their CSV; `manifest.tables[].parquetPath` is set. |
| `--as-of` | today | End of the 18-month span. Tests pin it so fixtures are stable; production runs leave it at today. |

On Databricks use `notebooks/00_mock_data/generate_mock_data.py` (widgets `catalog`,
`mock_data_root`, `scale`, `seed`, `parquet`); it writes to
`/Volumes/<catalog>/sales_bronze/mock_source` by default and calls the same `generate()`.

Programmatic use: `from sales_lakehouse.mock_data.generate import generate, buildContext`.

## Data rules

* Span: 18 months ending `--as-of`; daily `FX_RATE_DAILY` (with deliberate gaps); fiscal calendars
  `NA445` (4-4-5, FY from 1 Nov), `EUCAL` (calendar), `APACJUN` (FY from 1 Jul) cover the span
  plus a year either side.
* Regions NA / EU / APAC on customers, territories, channels, orders; target mix 40 / 35 / 25 %.
  Currencies NA = USD, CAD; EU = EUR, GBP; APAC = AUD, SGD, JPY. Tax regimes per territory
  (`USSALESTAX`, `CAGSTHST`, `UKVAT`, `EUVAT`, `AUGST`, `SGGST`, `JPCT`; `EU_RC` on reverse-charge
  orders).
* Referential integrity holds for every FK (asserted in `tests/test_mock_data_edge_cases.py`),
  except where an edge case deliberately breaks a *semantic* link (missing xref, retired party,
  untranslated code, deleted order). Nothing violates a DDL CHECK constraint.
* Sales identifiers are generated in the WWI ranges (`CustomerID` from 1, `OrderID` from 1, …);
  Oracle `PARTY_ID`s are 9xxxxx.

## Modules

| Module | Produces |
|---|---|
| `schema.py` | `TABLE_COLUMNS` — DDL column lists for all 70 tables; `columnsFor`, `tableKeys`. |
| `common.py` | `GenContext` (seeded RNG streams, span, scratch state, `edgeCase()` registry), CSV/Parquet writers, `manifest.json`. |
| `calendars.py` | NA445 / EUCAL / APACJUN fiscal-period arithmetic. |
| `domain.py` | Static legacy reference data (countries, territories, channels, plans, tax rates, names, products). |
| `oracle.py` | `WWI_REF.*`, `WWI_FIN.*`, `WWI_MDM.*` (MDM is built after SQL Server customers so xrefs line up). |
| `sqlserver.py` | Application / Warehouse / Sales master & reference tables (people, teams, stock, price lists, customers, quotas, promotions). |
| `sqlserver_transactions.py` | Quotes, orders, lines, holds/amendments/backorders, invoices, shipments, payments & allocations, disputes, write-offs, returns & credit notes, control tables. |
| `generate.py` | CLI + `generate()` / `buildContext()` orchestration. |

## Tables (small-scale row counts)

**SQL Server** — `Application`: Cities (200), DeliveryMethods, PaymentMethods, People (366), SalesTeamMembers,
SalesTeams, TransactionTypes · `Sales`: Customers (205), CustomerCategories, CustomerSegments (9),
CustomerSegmentAssignments (310), BuyingGroups,
SalesChannels (11), SalesTerritories (13), CommissionPlans (8), SalesQuotas (456), PriceLists (11),
PriceListLines (1 650), Promotions, SpecialDeals, QuoteHeaders (521), QuoteLines (1 473),
Orders (4 994), OrderLines (14 940), OrderAmendments, OrderHolds, Backorders, Invoices (4 511),
InvoiceLines (13 513), CustomerTransactions (8 012), CustomerPayments (3 410),
PaymentAllocations (3 425), CustomerDisputes, CustomerWriteOffs, OrderDeletionLog ·
`Returns`: ReturnReasons, ReturnAuthorizations, ReturnLines, CreditNotes, CreditNoteLines ·
`Shipping`: ShipmentHeaders (5 035), ShipmentLines (14 036) · `Warehouse`: StockItems (150),
StockItemHoldings, StockGroups, StockItemStockGroups, PackageTypes ·
`Integration`: ChangeTrackingWatermark, DeletedRowLog.

**Oracle** — `WWI_MDM`: CUST_MASTER (207), CUST_ADDRESS, CUST_CONTACT, PARTY_XREF (199),
MDM_MERGE_HISTORY, PRODUCT_CATEGORY, PRODUCT_MASTER (150, carries `UNIT_COST_STD` for margin),
PRODUCT_HIERARCHY (395) ·
`WWI_REF`: REGION_REF, COUNTRY_REF, CURRENCY_CODE, SOURCE_SYSTEM_REF, FX_RATE_DAILY (3 249),
CALENDAR_FISCAL (2 555), CODE_TRANSLATION, PAYMENT_METHOD_REF, REASON_CODE_REF, STATUS_CODE_REF ·
`WWI_FIN`: TAX_JURISDICTION, TAX_RATE, GL_PERIOD_STATUS.

Table names not in the DDL that the brief mentioned (`CustomerBuyingGroupAllocations`,
`SalesTerritoryAssignments`, `CommissionPlanAssignments`, `FX_RATE_SOURCE`, `CODE_XLAT`) map to
`Sales.Customers.BuyingGroupID`, `Application.SalesTeamMembers` (+ `SalesTerritories.SalesTeamID`),
`Sales.SalesQuotas.CommissionPlanID`, `FX_RATE_DAILY.RATE_TYPE_CD/SOURCE_SYS_CD` and
`WWI_REF.CODE_TRANSLATION` respectively.

## Edge cases (`manifest.json` → `edgeCases[]`)

Each entry is `{code, description, tables, keyCount, keys}`; `keys` holds up to 500 affected key
dicts so downstream validation can assert on exact rows.

| Code | Tables | What it is |
|---|---|---|
| `MISSING_FX_RATE` | WWI_REF.FX_RATE_DAILY | Isolated (currency, date) pairs with no FX row. |
| `MISSING_FX_WEEKEND` | WWI_REF.FX_RATE_DAILY | A complete weekend (Sat+Sun) with no FX rows for one currency (GBP). |
| `MISSING_FX_MONTH` | WWI_REF.FX_RATE_DAILY | One currency (SGD) has no FX rows for an entire calendar month. |
| `XREF_RETIRED_SINGLE_HOP` | WWI_MDM.PARTY_XREF | Xref points at a retired party merged into a survivor (one hop). |
| `XREF_RETIRED_TWO_HOP` | WWI_MDM.PARTY_XREF | Xref points at a retired party whose survivor was itself merged again. |
| `XREF_RETIRED_NO_MERGE` | WWI_MDM.PARTY_XREF | Xref points at a retired (MG) party with no MDM_MERGE_HISTORY record. |
| `DUPLICATE_XREF` | WWI_MDM.PARTY_XREF | Two active xref rows for the same WWI CustomerID. |
| `MISSING_XREF` | WWI_MDM.PARTY_XREF | WWI customer with no xref row at all. |
| `UNTRANSLATED_CODE` | WWI_REF.CODE_TRANSLATION | Channel / payment-method / return-reason codes with no translation row. |
| `STALE_CODE` | WWI_REF.CODE_TRANSLATION | Translation rows whose EFFECTIVE_TO_DT has passed / ACTIVE_FLG = N. |
| `UNTRANSLATED_PAYMENT_METHOD_USED` | Sales.CustomerPayments | Payments on `BILLOFEXCH`, which has no translation / PAYMENT_METHOD_REF row. |
| `UNTRANSLATED_RETURN_REASON_USED` | Returns.ReturnLines | Return lines using an untranslated ReasonCode. |
| `STALE_RETURN_REASON_USED` | Returns.ReturnLines | Return lines using a ReasonCode whose translation is stale. |
| `DUPLICATE_ORDER_LINE` | Sales.OrderLines | Same OrderID + StockItemID + Description, different LastEditedWhen. |
| `DUPLICATE_CUSTOMER_EXTRACT` | Sales.Customers | Same CustomerID twice with different ValidFrom (overlapping incremental extracts). |
| `EU_REVERSE_CHARGE` | Sales.Orders | EU cross-border B2B order, `TaxRegimeCode = 'EU_RC'`, zero VAT. |
| `EU_REVERSE_CHARGE_NULL_VAT` | Sales.Invoices | Reverse-charge invoice with NULL CustomerTaxNumber. |
| `GST_INCLUSIVE_RESIDUAL` | Sales.OrderLines | APAC GST-inclusive unit price that does not divide cleanly by (1 + rate). |
| `FULFILMENT_FLAGS_EMPTY` | Sales.Orders | `FulfilmentFlags = ''` (not NULL). |
| `FULFILMENT_FLAGS_MALFORMED` | Sales.Orders | `P||B`, trailing pipe, etc. (well-formed rows use `P|B|H`). |
| `PILOT_CHANNEL` | Sales.SalesChannels | Channel with `ChannelStatus = 'PILOT'`. |
| `PILOT_CHANNEL_ORDER` | Sales.Orders | Orders placed on that channel (orderable, non-commissionable). |
| `OVERPAYMENT` | Sales.CustomerPayments | Payment > invoice total; surplus stays unallocated. |
| `UNDERPAYMENT` | Sales.CustomerPayments | Payment < invoice total; invoice stays PARTPAID. |
| `MULTI_INVOICE_PAYMENT` | Sales.CustomerPayments | One payment allocated across several invoices. |
| `ON_ACCOUNT_PAYMENT` | Sales.CustomerPayments | `UNAPPLIED` payment with no allocations. |
| `LATE_ARRIVING_CUSTOMER` | Sales.Customers, Sales.Invoices | Customer row arrives (ValidFrom) after the invoices that reference it. |
| `COMMISSION_BASIS_COVERAGE` | Sales.CommissionPlans | Active plans for INVOICEDMARGIN, NETREVENUE and COLLECTEDCASH. |
| `COMMISSION_EU_CAP_HIT` | Sales.SalesQuotas | EU rep on the capped plan whose attainment exceeds Band3UpperPercent. |
| `EU_CONSENT_N` | Sales.Customers | EU customers with `MarketingConsentFlag = 'N'`. |
| `DELETED_ORDER` | Sales.OrderDeletionLog | OrderIDs in OrderDeletionLog / Integration.DeletedRowLog but absent from Orders. |

## Tests

`tests/test_mock_data_generate.py` — CLI, layout, manifest schema, DDL columns, CHECK-constraint
values, determinism, CSV/NULL/BIT encoding, Parquet, region mix, notebook.
`tests/test_mock_data_edge_cases.py` — every FK, primary keys, order/invoice/payment arithmetic,
MDM chains, translations, tax, commission behaviour, every edge case above, medium scale.
