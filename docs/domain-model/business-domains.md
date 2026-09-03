# Business domains

What the estate is actually about, and where each subject area lives. Names in
this document are the object names in the tree; the counts come from
`config/estate-catalog.yaml`.

Nothing here has been executed. This is a reading of the schema definitions,
not of any data.

## The eight domains

| Domain | Master lives in | Transactions live in | Warehouse |
| --- | --- | --- | --- |
| Party and customer | Oracle `WWI_MDM` | SQL Server `Sales` | `Dimension.Customer`, `Aggregate.Customer 360` |
| Product and inventory | Oracle `WWI_MDM`, SQL Server `Warehouse` | SQL Server `Warehouse` | `Dimension.Stock Item`, `Fact.Movement`, `Fact.Stock Holding` |
| Sales and order to cash | SQL Server `Sales` | SQL Server `Sales` | `Fact.Sale`, `Fact.Order`, `Fact.Sales Margin` |
| Procurement | Oracle `WWI_PROC` | Oracle `WWI_PROC` | `Fact.Purchase`, `Fact.Purchase Receipt`, `Fact.Procure To Pay` |
| Finance | Oracle `WWI_FIN` | Oracle `WWI_FIN` | `Fact.GL Posting`, `Fact.Payment`, `Fact.Supplier Ledger`, the monthly aging facts |
| Logistics and returns | SQL Server `Shipping`, `Returns` | same | `Fact.Shipment`, `Fact.Order Fulfilment`, `Fact.Return`, `Fact.Credit Note` |
| Customer engagement | SQL Server `Loyalty`, `Ecommerce` | same | `Fact.Loyalty Points`, `Fact.Web Session`, `Fact.Promotion Eligibility` |
| Reference and calendar | Oracle `WWI_REF` | - | `Dimension.Date`, `Dimension.Currency`, `Dimension.Fiscal Calendar`, `Dimension.Geography` |

Two things fall out of that table immediately. Customer master is in Oracle
while customer transactions are in SQL Server, so every customer-facing fact
depends on a cross-system key resolution. And procurement and finance are
entirely Oracle while sales is entirely SQL Server, which is why the finance
close and the daily sales load are separate pipelines with separate schedules.

## Party and customer

`WWI_MDM` holds the master: `CUST_MASTER`, `CUST_ADDRESS`, `CUST_CONTACT`,
`CUST_CLASSIFICATION`, `CUST_CREDIT_PROFILE`, `CUST_HIERARCHY`,
`CUST_SEGMENT_ASSIGN`, and the two that make this domain hard - `PARTY_XREF`
and `MDM_MERGE_HISTORY`.

`PARTY_XREF` maps an ERP party to its identifiers in every other system. It is
the only bridge between the Oracle customer and the SQL Server customer, and it
is maintained by `PKG_CUSTOMER_MASTER` rather than by a constraint.
`MDM_MERGE_HISTORY` records duplicate-party merges. A merge retires one party
in favour of another, and anything that resolved the retired identifier before
the merge is now pointing at a party that no longer exists. The warehouse deals
with this by keeping the durable `[WWI Customer ID]` on `Dimension.Customer` and
letting the Type 2 chain carry the change; nothing repairs facts loaded before
a merge.

The OLTP side adds contracts, quotes, price lists and commission in `Sales`,
plus segmentation in `Sales.CustomerSegments`. `Dimension.Customer` is SCD2,
`Dimension.Customer Category` and `Dimension.Customer Segment` come from two
different systems, and `Dimension.Customer Buying Group Bridge` handles the
many-to-many case with allocation factors that must sum to 1.0.

## Product and inventory

`PRODUCT_MASTER`, `PRODUCT_CATEGORY`, `PRODUCT_HIERARCHY`, `PRODUCT_UOM_CONV`
and `PRODUCT_SUBSTITUTE` in Oracle; the physical stock in SQL Server
`Warehouse` - bins, cycle counts, replenishment - on top of the base sample's
`Warehouse.StockItems`.

`PRODUCT_UOM_CONV` is the one to watch: the ERP sells in one unit and the
warehouse picks in another, and the conversion is applied during staging. The
`[Quantity Base UOM]` and `[Source UOM Code]` columns on `Fact.Sale` exist so
that the conversion is at least visible after the fact.

## Sales and order to cash

Entirely SQL Server. Quote, contract, order, invoice, payment, with
`Fact.Sale`, `Fact.Order` and `Fact.Payment` at transaction grain and
`Fact.Sales Margin` derived on top. `Fact.Daily Sales Snapshot` and
`Fact.Daily Backlog` are the periodic snapshots the sales reports actually read.

Margin depends on cost, cost comes from the ERP, and the two arrive on
different schedules. `Fact.Sales Margin` is therefore rebuilt rather than
loaded incrementally.

## Procurement

Requisition to receipt, entirely in `WWI_PROC`: `REQUISITION_HDR/LINE`,
`SOURCING_EVENT`, `SUPPLIER_QUOTE(_LINE)`, `PURCHASE_ORDER_HDR/LINE`,
`PO_CHANGE_ORDER`, `PO_RECEIPT_HDR/LINE`, `GOODS_RETURN_HDR/LINE`,
`VENDOR_CONTRACT(_LINE)`, `SUPPLIER_SCORECARD`.

`PO_CHANGE_ORDER` is why the purchase facts are not a simple incremental load:
a change order restates a line that may already be in the warehouse, so the
load has to detect and correct rather than append. `Fact.Procure To Pay` is the
accumulating view that joins requisition, order, receipt and invoice into one
row per line, and it is the fact most sensitive to any of the four arriving
late.

## Finance

`AP_INVOICE_HDR/LINE`, `AP_INVOICE_HOLD`, `AP_PAYMENT`, `AP_PAYMENT_APPLY`,
`AP_AGING_SNAPSHOT`, `GL_ACCOUNT`, `GL_JOURNAL_HDR/LINE`, `GL_PERIOD_STATUS`,
`COST_CENTER`, `COST_ALLOCATION_RULE`, `PAYMENT_TERMS`, `TAX_RATE`,
`TAX_JURISDICTION`, `WITHHOLDING_RULE`.

`GL_PERIOD_STATUS` gates the close: the finance pipeline is not supposed to run
for a period that is still open, and `Master_Finance_Close` is the only master
whose Agent job ships disabled for that reason. `COST_ALLOCATION_RULE` drives
allocations applied during the warehouse load rather than in the ERP, which
means the warehouse's cost-centre numbers are not the ERP's cost-centre numbers
and never were.

Withholding is region-specific and only APAC uses it.

## Logistics and returns

`Shipping` carries carriers, consignments and tracking events;`Returns` carries
the RMA lifecycle. `Fact.Shipment` and `Fact.Order Fulfilment` are accumulating
snapshots - a row is updated as milestones land, not inserted once - which is
why both are `incremental_fact` loads with an update path rather than appends.
`Fact.Return` and `Fact.Credit Note` are the financial consequence.

## Customer engagement

`Loyalty` (tiers, points ledger, accruals) and `Ecommerce` (sessions, carts,
channel orders) are the newest schemas in the OLTP database and the least
consistent with the rest of it. `Fact.Loyalty Points` and `Fact.Web Session`
are high-volume and shallow; `Aggregate.Customer 360` is where they are joined
to everything else, and it is the single widest object in the warehouse.

## Reference and calendar

`WWI_REF` masters currency, FX, country, region, city, postal, UOM, language,
incoterms, payment method, status codes, reason codes, the fiscal calendar and
`CODE_TRANSLATION`. Everything is refreshed weekly, in full, by
`Master_Weekly_Reference_Load`.

`CODE_TRANSLATION` deserves its own paragraph. It maps code values between
systems - the ERP's status codes to the OLTP's, the OLTP's to the warehouse's -
and it is incomplete. Where a translation is missing the loads fall back to
passing the source value through, so the warehouse contains both translated and
untranslated values in the same column.

---

# Regional divergence

Three regions - NA, EU, APAC - integrated in three different projects, and the
differences were encoded as branches in code rather than as configuration.
Every one of the following is a structural difference in this repository.

## Tax

| | NA | EU | APAC |
| --- | --- | --- | --- |
| Regime | sales and use tax | VAT | GST |
| Reference | `oracle/reference/05_tax_na_sales_and_use.sql` | `06_tax_eu_vat.sql` | `07_tax_apac_gst.sql` |
| Applied | on top of the line price | on top of the line price | **included in** the line price |
| Jurisdiction | state, county, city, district - up to four rates on one line | one rate per country, with cross-border rules | one national rate, with exemptions |
| Special path | use-tax accrual on untaxed purchases | reverse charge for registered cross-border customers | GST-free supplies |
| Fact columns | `[Tax Amount]`, `[Tax Regime Code]` | `[VAT Rate]`, `[VAT Reverse Charge Flag]`, `[Customer Tax Registration]` | `[GST Rate]`, `[GST Free Flag]`, `[Gross Amount]` |

The three tax reference tables have three different shapes, so the loads cannot
share a lookup. APAC's inclusive treatment means the APAC path derives the
net amount by division and **truncates** where the other two round; that is a
deliberate reproduction of the original behaviour, and it is why a systematic
sub-cent residual shows up on APAC rows.

Reverse charge is the EU path that has no equivalent elsewhere: the tax is not
charged and the customer accounts for it, which requires the customer's VAT
registration to have been captured. Where it was not, the row is still loaded.

## Fiscal calendar

The fiscal year does not start in the same month in all three regions, and the
period boundaries are not the same kind of thing:
`oracle/reference/11_fiscal_calendars_and_periods.sql` holds all three, and
`Dimension.Fiscal Calendar` carries them into the warehouse. `Fact.Sale` and
the finance facts carry `[Fiscal Year]` and `[Fiscal Period]` per row, resolved
by the region of the transaction rather than by the region of the reader. A
single invoice date therefore lands in three different periods depending on
which region's books it is in, and any cross-region period comparison has to
choose a convention. The aggregates choose the NA convention, silently.

## Currency and FX

Transactions are in local currency; the warehouse reports in a single currency.
`[Transaction Currency Code]`, `[Currency Key]`, `[FX Rate To Reporting]`,
`[FX Rate Effective Date]` and `[FX Rate Source Code]` on the facts carry the
conversion. Rates come from `WWI_REF.FX_RATE_DAILY` via `PKG_FX`.

The three regions do not use the same rate source or the same effective-date
convention - some rows convert at transaction date, some at period-end - and
`[FX Rate Source Code]` is the only record of which. Where no rate is found the
loads default the rate to 1 rather than failing, so a missing rate produces a
wrong number rather than a null.

## Address and postal

Three address shapes: NA state plus five-or-nine-digit ZIP, EU country-specific
formats with alphanumeric postcodes of varying length, APAC formats where the
postcode may be absent entirely. `WWI_MDM.CUST_ADDRESS` carries the union of
all three, so most columns are null for most rows, and
`work.CustomerAddressStandardized` is the pass that tries to normalise them.
`Dimension.Geography` and `Dimension.City` are the conformed result, and they
conform less than their names suggest.

## Consent and retention

EU rows carry consent flags and retention deadlines that NA and APAC rows do
not, because they were added for a European regulatory programme and never
generalised. Retention is enforced by a purge job (`WWI_AUDIT.PURGE_LOG`
records it on the ERP side) that applies the EU rules to EU rows only. A
customer who moves between regions keeps whichever rules applied when their row
was created.

## Reference codes

Status codes, reason codes and category codes differ per region, and
`CODE_TRANSLATION` is the mapping. It is regional too - the same source code
can translate differently depending on region - and where a region has no
mapping the source value passes through untranslated. The practical effect is
that a warehouse column can hold NA codes, EU codes, APAC codes and translated
canonical codes at the same time, distinguishable only by
`[Source System Code]` and `[Region Code]`.

## Where the branches live

The regional branch is taken in at least four places for the same transaction:
in `PKG_TAX` and `PKG_FX` on the Oracle side, in the staging load procedures, in
SSIS derived-column expressions inside the regional packages, and again in the
warehouse load procedures where reporting amounts are computed. Nothing keeps
the four in step. This is the single most important property of the estate for
anyone planning a migration.
