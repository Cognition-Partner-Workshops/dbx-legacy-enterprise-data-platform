"""Referential integrity and every documented edge case of ``sales_lakehouse.mock_data``
(each must be present in the data AND registered in manifest.json under ``edgeCases``)."""
from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from sales_lakehouse.mock_data.generate import generate
from sales_lakehouse.mock_data.schema import ORACLE, SQLSERVER

AS_OF = date(2026, 9, 28)

DOCUMENTED_EDGE_CASES = {
    "MISSING_FX_RATE", "MISSING_FX_WEEKEND", "MISSING_FX_MONTH",
    "XREF_RETIRED_SINGLE_HOP", "XREF_RETIRED_TWO_HOP", "XREF_RETIRED_NO_MERGE", "DUPLICATE_XREF", "MISSING_XREF",
    "UNTRANSLATED_CODE", "STALE_CODE", "UNTRANSLATED_PAYMENT_METHOD_USED", "UNTRANSLATED_RETURN_REASON_USED", "STALE_RETURN_REASON_USED",
    "DUPLICATE_ORDER_LINE", "DUPLICATE_CUSTOMER_EXTRACT",
    "EU_REVERSE_CHARGE", "EU_REVERSE_CHARGE_NULL_VAT", "GST_INCLUSIVE_RESIDUAL",
    "FULFILMENT_FLAGS_EMPTY", "FULFILMENT_FLAGS_MALFORMED",
    "PILOT_CHANNEL", "PILOT_CHANNEL_ORDER",
    "OVERPAYMENT", "UNDERPAYMENT", "MULTI_INVOICE_PAYMENT", "ON_ACCOUNT_PAYMENT",
    "LATE_ARRIVING_CUSTOMER", "COMMISSION_BASIS_COVERAGE", "COMMISSION_EU_CAP_HIT", "EU_CONSENT_N", "DELETED_ORDER",
}

# (child table, child column, parent table, parent column); child NULLs are ignored.
FOREIGN_KEYS: list[tuple[tuple[str, str, str], str, tuple[str, str, str], str]] = [
    ((SQLSERVER, "Sales", "Customers"), "BillToCustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "Customers"), "CustomerCategoryID", (SQLSERVER, "Sales", "CustomerCategories"), "CustomerCategoryID"),
    ((SQLSERVER, "Sales", "Customers"), "BuyingGroupID", (SQLSERVER, "Sales", "BuyingGroups"), "BuyingGroupID"),
    ((SQLSERVER, "Sales", "Customers"), "PrimaryContactPersonID", (SQLSERVER, "Application", "People"), "PersonID"),
    ((SQLSERVER, "Sales", "Customers"), "DeliveryMethodID", (SQLSERVER, "Application", "DeliveryMethods"), "DeliveryMethodID"),
    ((SQLSERVER, "Sales", "Customers"), "SalesTerritoryID", (SQLSERVER, "Sales", "SalesTerritories"), "SalesTerritoryID"),
    ((SQLSERVER, "Sales", "SalesTerritories"), "ParentTerritoryID", (SQLSERVER, "Sales", "SalesTerritories"), "SalesTerritoryID"),
    ((SQLSERVER, "Sales", "SalesTerritories"), "ManagerPersonID", (SQLSERVER, "Application", "People"), "PersonID"),
    ((SQLSERVER, "Application", "SalesTeams"), "SalesTerritoryID", (SQLSERVER, "Sales", "SalesTerritories"), "SalesTerritoryID"),
    ((SQLSERVER, "Application", "SalesTeams"), "ManagerPersonID", (SQLSERVER, "Application", "People"), "PersonID"),
    ((SQLSERVER, "Application", "SalesTeamMembers"), "SalesTeamID", (SQLSERVER, "Application", "SalesTeams"), "SalesTeamID"),
    ((SQLSERVER, "Application", "SalesTeamMembers"), "PersonID", (SQLSERVER, "Application", "People"), "PersonID"),
    ((SQLSERVER, "Application", "SalesTeamMembers"), "CommissionPlanID", (SQLSERVER, "Sales", "CommissionPlans"), "CommissionPlanID"),
    ((SQLSERVER, "Sales", "SalesQuotas"), "SalespersonPersonID", (SQLSERVER, "Application", "People"), "PersonID"),
    ((SQLSERVER, "Sales", "SalesQuotas"), "SalesTerritoryID", (SQLSERVER, "Sales", "SalesTerritories"), "SalesTerritoryID"),
    ((SQLSERVER, "Sales", "PriceLists"), "BuyingGroupID", (SQLSERVER, "Sales", "BuyingGroups"), "BuyingGroupID"),
    ((SQLSERVER, "Sales", "PriceListLines"), "PriceListID", (SQLSERVER, "Sales", "PriceLists"), "PriceListID"),
    ((SQLSERVER, "Sales", "PriceListLines"), "StockItemID", (SQLSERVER, "Warehouse", "StockItems"), "StockItemID"),
    ((SQLSERVER, "Sales", "SpecialDeals"), "StockItemID", (SQLSERVER, "Warehouse", "StockItems"), "StockItemID"),
    ((SQLSERVER, "Sales", "SpecialDeals"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "Customers"), "DeliveryCityID", (SQLSERVER, "Application", "Cities"), "CityID"),
    ((SQLSERVER, "Sales", "CustomerSegmentAssignments"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "CustomerSegmentAssignments"), "CustomerSegmentID", (SQLSERVER, "Sales", "CustomerSegments"), "CustomerSegmentID"),
    ((SQLSERVER, "Warehouse", "StockItemStockGroups"), "StockItemID", (SQLSERVER, "Warehouse", "StockItems"), "StockItemID"),
    ((SQLSERVER, "Warehouse", "StockItemStockGroups"), "StockGroupID", (SQLSERVER, "Warehouse", "StockGroups"), "StockGroupID"),
    ((SQLSERVER, "Warehouse", "StockItemHoldings"), "StockItemID", (SQLSERVER, "Warehouse", "StockItems"), "StockItemID"),
    ((SQLSERVER, "Warehouse", "StockItems"), "UnitPackageID", (SQLSERVER, "Warehouse", "PackageTypes"), "PackageTypeID"),
    ((SQLSERVER, "Sales", "QuoteHeaders"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "QuoteHeaders"), "ConvertedOrderID", (SQLSERVER, "Sales", "Orders"), "OrderID"),
    ((SQLSERVER, "Sales", "QuoteLines"), "QuoteID", (SQLSERVER, "Sales", "QuoteHeaders"), "QuoteID"),
    ((SQLSERVER, "Sales", "Orders"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "Orders"), "SalespersonPersonID", (SQLSERVER, "Application", "People"), "PersonID"),
    ((SQLSERVER, "Sales", "Orders"), "PickedByPersonID", (SQLSERVER, "Application", "People"), "PersonID"),
    ((SQLSERVER, "Sales", "Orders"), "SalesChannelID", (SQLSERVER, "Sales", "SalesChannels"), "SalesChannelID"),
    ((SQLSERVER, "Sales", "Orders"), "SalesTerritoryID", (SQLSERVER, "Sales", "SalesTerritories"), "SalesTerritoryID"),
    ((SQLSERVER, "Sales", "Orders"), "PriceListID", (SQLSERVER, "Sales", "PriceLists"), "PriceListID"),
    ((SQLSERVER, "Sales", "Orders"), "SourceQuoteID", (SQLSERVER, "Sales", "QuoteHeaders"), "QuoteID"),
    ((SQLSERVER, "Sales", "Orders"), "BackorderOrderID", (SQLSERVER, "Sales", "Orders"), "OrderID"),
    ((SQLSERVER, "Sales", "OrderLines"), "OrderID", (SQLSERVER, "Sales", "Orders"), "OrderID"),
    ((SQLSERVER, "Sales", "OrderLines"), "StockItemID", (SQLSERVER, "Warehouse", "StockItems"), "StockItemID"),
    ((SQLSERVER, "Sales", "OrderLines"), "PackageTypeID", (SQLSERVER, "Warehouse", "PackageTypes"), "PackageTypeID"),
    ((SQLSERVER, "Sales", "OrderLines"), "PriceListLineID", (SQLSERVER, "Sales", "PriceListLines"), "PriceListLineID"),
    ((SQLSERVER, "Sales", "OrderLines"), "PromotionID", (SQLSERVER, "Sales", "Promotions"), "PromotionID"),
    ((SQLSERVER, "Sales", "OrderAmendments"), "OrderID", (SQLSERVER, "Sales", "Orders"), "OrderID"),
    ((SQLSERVER, "Sales", "OrderHolds"), "OrderID", (SQLSERVER, "Sales", "Orders"), "OrderID"),
    ((SQLSERVER, "Sales", "Backorders"), "OrderID", (SQLSERVER, "Sales", "Orders"), "OrderID"),
    ((SQLSERVER, "Sales", "Backorders"), "OrderLineID", (SQLSERVER, "Sales", "OrderLines"), "OrderLineID"),
    ((SQLSERVER, "Sales", "Invoices"), "OrderID", (SQLSERVER, "Sales", "Orders"), "OrderID"),
    ((SQLSERVER, "Sales", "Invoices"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "Invoices"), "BillToCustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "Invoices"), "DeliveryMethodID", (SQLSERVER, "Application", "DeliveryMethods"), "DeliveryMethodID"),
    ((SQLSERVER, "Sales", "InvoiceLines"), "InvoiceID", (SQLSERVER, "Sales", "Invoices"), "InvoiceID"),
    ((SQLSERVER, "Sales", "InvoiceLines"), "StockItemID", (SQLSERVER, "Warehouse", "StockItems"), "StockItemID"),
    ((SQLSERVER, "Sales", "CustomerTransactions"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "CustomerTransactions"), "InvoiceID", (SQLSERVER, "Sales", "Invoices"), "InvoiceID"),
    ((SQLSERVER, "Sales", "CustomerTransactions"), "TransactionTypeID", (SQLSERVER, "Application", "TransactionTypes"), "TransactionTypeID"),
    ((SQLSERVER, "Sales", "CustomerTransactions"), "PaymentMethodID", (SQLSERVER, "Application", "PaymentMethods"), "PaymentMethodID"),
    ((SQLSERVER, "Sales", "CustomerPayments"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "PaymentAllocations"), "CustomerPaymentID", (SQLSERVER, "Sales", "CustomerPayments"), "CustomerPaymentID"),
    ((SQLSERVER, "Sales", "PaymentAllocations"), "InvoiceID", (SQLSERVER, "Sales", "Invoices"), "InvoiceID"),
    ((SQLSERVER, "Sales", "CustomerDisputes"), "InvoiceID", (SQLSERVER, "Sales", "Invoices"), "InvoiceID"),
    ((SQLSERVER, "Sales", "CustomerDisputes"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Sales", "CustomerWriteOffs"), "InvoiceID", (SQLSERVER, "Sales", "Invoices"), "InvoiceID"),
    ((SQLSERVER, "Sales", "CustomerWriteOffs"), "CustomerDisputeID", (SQLSERVER, "Sales", "CustomerDisputes"), "CustomerDisputeID"),
    ((SQLSERVER, "Sales", "OrderDeletionLog"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Returns", "ReturnAuthorizations"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Returns", "ReturnAuthorizations"), "OriginalInvoiceID", (SQLSERVER, "Sales", "Invoices"), "InvoiceID"),
    ((SQLSERVER, "Returns", "ReturnLines"), "ReturnAuthorizationID", (SQLSERVER, "Returns", "ReturnAuthorizations"), "ReturnAuthorizationID"),
    ((SQLSERVER, "Returns", "ReturnLines"), "OriginalInvoiceLineID", (SQLSERVER, "Sales", "InvoiceLines"), "InvoiceLineID"),
    ((SQLSERVER, "Returns", "ReturnLines"), "ReturnReasonID", (SQLSERVER, "Returns", "ReturnReasons"), "ReturnReasonID"),
    ((SQLSERVER, "Returns", "ReturnLines"), "StockItemID", (SQLSERVER, "Warehouse", "StockItems"), "StockItemID"),
    ((SQLSERVER, "Returns", "CreditNotes"), "ReturnAuthorizationID", (SQLSERVER, "Returns", "ReturnAuthorizations"), "ReturnAuthorizationID"),
    ((SQLSERVER, "Returns", "CreditNotes"), "OriginalInvoiceID", (SQLSERVER, "Sales", "Invoices"), "InvoiceID"),
    ((SQLSERVER, "Returns", "CreditNotes"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Returns", "CreditNoteLines"), "CreditNoteID", (SQLSERVER, "Returns", "CreditNotes"), "CreditNoteID"),
    ((SQLSERVER, "Returns", "CreditNoteLines"), "ReturnLineID", (SQLSERVER, "Returns", "ReturnLines"), "ReturnLineID"),
    ((SQLSERVER, "Shipping", "ShipmentHeaders"), "OrderID", (SQLSERVER, "Sales", "Orders"), "OrderID"),
    ((SQLSERVER, "Shipping", "ShipmentHeaders"), "InvoiceID", (SQLSERVER, "Sales", "Invoices"), "InvoiceID"),
    ((SQLSERVER, "Shipping", "ShipmentHeaders"), "CustomerID", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((SQLSERVER, "Shipping", "ShipmentLines"), "ShipmentID", (SQLSERVER, "Shipping", "ShipmentHeaders"), "ShipmentID"),
    ((SQLSERVER, "Shipping", "ShipmentLines"), "OrderLineID", (SQLSERVER, "Sales", "OrderLines"), "OrderLineID"),
    ((ORACLE, "WWI_MDM", "CUST_MASTER"), "COUNTRY_CD", (ORACLE, "WWI_REF", "COUNTRY_REF"), "COUNTRY_CD"),
    ((ORACLE, "WWI_MDM", "CUST_MASTER"), "REGION_CD", (ORACLE, "WWI_REF", "REGION_REF"), "REGION_CD"),
    ((ORACLE, "WWI_MDM", "CUST_MASTER"), "PRIMARY_CURR_CD", (ORACLE, "WWI_REF", "CURRENCY_CODE"), "CURR_CD"),
    ((ORACLE, "WWI_MDM", "CUST_ADDRESS"), "CUST_ID", (ORACLE, "WWI_MDM", "CUST_MASTER"), "CUST_ID"),
    ((ORACLE, "WWI_MDM", "CUST_CONTACT"), "CUST_ID", (ORACLE, "WWI_MDM", "CUST_MASTER"), "CUST_ID"),
    ((ORACLE, "WWI_MDM", "PARTY_XREF"), "CUST_ID", (ORACLE, "WWI_MDM", "CUST_MASTER"), "CUST_ID"),
    ((ORACLE, "WWI_MDM", "PARTY_XREF"), "SOURCE_KEY_TXT", (SQLSERVER, "Sales", "Customers"), "CustomerID"),
    ((ORACLE, "WWI_MDM", "MDM_MERGE_HISTORY"), "SURVIVOR_PARTY_ID", (ORACLE, "WWI_MDM", "CUST_MASTER"), "CUST_ID"),
    ((ORACLE, "WWI_MDM", "MDM_MERGE_HISTORY"), "MERGED_PARTY_ID", (ORACLE, "WWI_MDM", "CUST_MASTER"), "CUST_ID"),
    ((ORACLE, "WWI_MDM", "PRODUCT_MASTER"), "PRODUCT_CATEGORY_ID", (ORACLE, "WWI_MDM", "PRODUCT_CATEGORY"), "PRODUCT_CATEGORY_ID"),
    ((ORACLE, "WWI_MDM", "PRODUCT_MASTER"), "WWI_STOCK_ITEM_ID", (SQLSERVER, "Warehouse", "StockItems"), "StockItemID"),
    ((ORACLE, "WWI_MDM", "PRODUCT_HIERARCHY"), "PRODUCT_ID", (ORACLE, "WWI_MDM", "PRODUCT_MASTER"), "PRODUCT_ID"),
    ((ORACLE, "WWI_REF", "COUNTRY_REF"), "REGION_CD", (ORACLE, "WWI_REF", "REGION_REF"), "REGION_CD"),
    ((ORACLE, "WWI_REF", "FX_RATE_DAILY"), "FROM_CURR_CD", (ORACLE, "WWI_REF", "CURRENCY_CODE"), "CURR_CD"),
    ((ORACLE, "WWI_FIN", "TAX_RATE"), "JURISDICTION_CD", (ORACLE, "WWI_FIN", "TAX_JURISDICTION"), "JURISDICTION_CD"),
]


class Data:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        self.edge = {e["code"]: e for e in self.manifest["edgeCases"]}
        self._cache: dict[tuple[str, str, str], list[dict[str, str]]] = {}

    def rows(self, system: str, schema: str, table: str) -> list[dict[str, str]]:
        key = (system, schema, table)
        if key not in self._cache:
            with (self.root / system / schema / f"{table}.csv").open(encoding="utf-8", newline="") as handle:
                self._cache[key] = list(csv.DictReader(handle))
        return self._cache[key]

    def sql(self, schema: str, table: str) -> list[dict[str, str]]:
        return self.rows(SQLSERVER, schema, table)

    def ora(self, schema: str, table: str) -> list[dict[str, str]]:
        return self.rows(ORACLE, schema, table)

    def keys(self, code: str) -> list[dict]:
        assert code in self.edge, f"{code} not registered in manifest.edgeCases"
        keys = self.edge[code]["keys"]
        assert keys and self.edge[code]["keyCount"] >= len(keys)
        return keys


@pytest.fixture(scope="session")
def data(tmp_path_factory: pytest.TempPathFactory) -> Data:
    root = tmp_path_factory.mktemp("mock_edge")
    generate(root, seed=42, scale="small", asOf=AS_OF)
    return Data(root)


def byId(rows: list[dict[str, str]], column: str) -> dict[str, dict[str, str]]:
    return {r[column]: r for r in rows}


def test_every_documented_edge_case_is_in_manifest(data: Data) -> None:
    assert set(data.edge) == DOCUMENTED_EDGE_CASES
    for code, edge in data.edge.items():
        for table in edge["tables"]:
            schema, name = table.split(".")
            system = ORACLE if schema.startswith("WWI_") else SQLSERVER
            assert (data.root / system / schema / f"{name}.csv").is_file(), f"{code}: table {table} not generated"


@pytest.mark.parametrize("child,childCol,parent,parentCol", FOREIGN_KEYS, ids=lambda x: x if isinstance(x, str) else f"{x[1]}.{x[2]}")
def test_referential_integrity(data: Data, child: tuple[str, str, str], childCol: str, parent: tuple[str, str, str], parentCol: str) -> None:
    parentKeys = {r[parentCol] for r in data.rows(*parent)}
    orphans = {r[childCol] for r in data.rows(*child) if r[childCol] != "" and r[childCol] not in parentKeys}
    assert not orphans, f"{child[1]}.{child[2]}.{childCol} -> {parent[1]}.{parent[2]}.{parentCol}: {sorted(orphans)[:10]}"


def test_primary_keys_unique_except_documented_duplicates(data: Data) -> None:
    for (system, schema, table), pk in {
        (SQLSERVER, "Sales", "Orders"): "OrderID", (SQLSERVER, "Sales", "OrderLines"): "OrderLineID", (SQLSERVER, "Sales", "Invoices"): "InvoiceID",
        (SQLSERVER, "Sales", "InvoiceLines"): "InvoiceLineID", (SQLSERVER, "Sales", "CustomerPayments"): "CustomerPaymentID",
        (SQLSERVER, "Sales", "PaymentAllocations"): "PaymentAllocationID", (SQLSERVER, "Application", "People"): "PersonID",
        (SQLSERVER, "Warehouse", "StockItems"): "StockItemID", (ORACLE, "WWI_MDM", "CUST_MASTER"): "CUST_ID", (ORACLE, "WWI_MDM", "PARTY_XREF"): "PARTY_XREF_ID",
    }.items():
        values = [r[pk] for r in data.rows(system, schema, table)]
        assert len(values) == len(set(values)), f"{schema}.{table}.{pk} not unique"
    fx = Counter((r["FROM_CURR_CD"], r["RATE_DT"]) for r in data.ora("WWI_REF", "FX_RATE_DAILY"))
    assert max(fx.values()) == 1
    customers = Counter(r["CustomerID"] for r in data.sql("Sales", "Customers"))
    duplicated = {k for k, n in customers.items() if n > 1}
    assert duplicated == {str(k["CustomerID"]) for k in data.keys("DUPLICATE_CUSTOMER_EXTRACT")}


def test_every_order_has_lines_and_invoices_match_orders(data: Data) -> None:
    lines = defaultdict(list)
    for line in data.sql("Sales", "OrderLines"):
        lines[line["OrderID"]].append(line)
    orders = byId(data.sql("Sales", "Orders"), "OrderID")
    assert set(orders) <= set(lines)
    invoices = data.sql("Sales", "Invoices")
    for inv in invoices:
        order = orders[inv["OrderID"]]
        assert order["OrderStatusCode"] == "INVOICED" and inv["CustomerID"] == order["CustomerID"] and inv["CurrencyCode"] == order["CurrencyCode"]
        assert inv["InvoiceDate"] >= order["OrderDate"]
    assert len({i["OrderID"] for i in invoices}) == len(invoices)
    assert sum(1 for o in orders.values() if o["OrderStatusCode"] == "INVOICED") == len(invoices)


def test_invoice_totals_reconcile_to_lines_and_settlement(data: Data) -> None:
    lineTotals: dict[str, Decimal] = defaultdict(Decimal)
    for line in data.sql("Sales", "InvoiceLines"):
        lineTotals[line["InvoiceID"]] += Decimal(line["ExtendedPrice"])
    allocated: dict[str, Decimal] = defaultdict(Decimal)
    for alloc in data.sql("Sales", "PaymentAllocations"):
        if alloc["InvoiceID"]:
            allocated[alloc["InvoiceID"]] += Decimal(alloc["AllocatedAmount"])
    for inv in data.sql("Sales", "Invoices"):
        total = Decimal(inv["InvoiceTotalExTax"]) + Decimal(inv["InvoiceTaxAmount"])
        assert abs(total - lineTotals[inv["InvoiceID"]]) <= Decimal("0.05"), inv["InvoiceID"]
        if inv["SettlementStatus"] == "PAID":
            assert Decimal(inv["AmountOutstanding"]) == 0 and allocated[inv["InvoiceID"]] >= total
        elif inv["SettlementStatus"] == "OPEN":
            assert Decimal(inv["AmountOutstanding"]) == total and inv["InvoiceID"] not in allocated


def test_dates_within_18_month_span(data: Data) -> None:
    spanStart, spanEnd = data.manifest["spanStart"], data.manifest["spanEnd"]
    assert date.fromisoformat(spanStart) == date(2025, 3, 29)
    orderDates = [o["OrderDate"] for o in data.sql("Sales", "Orders")]
    assert min(orderDates) >= spanStart and max(orderDates) <= spanEnd
    assert min(orderDates) < "2025-05-01" and max(orderDates) > "2026-09-01"
    fxDates = {r["RATE_DT"] for r in data.ora("WWI_REF", "FX_RATE_DAILY")}
    assert min(fxDates) == spanStart and max(fxDates) == spanEnd
    for calendar in ("NA445", "EUCAL", "APACJUN"):
        days = sorted(r["CALENDAR_DT"] for r in data.ora("WWI_REF", "CALENDAR_FISCAL") if r["CALENDAR_CD"] == calendar)
        assert days[0] <= spanStart and days[-1] >= spanEnd and len(days) == len(set(days))
        assert (date.fromisoformat(days[-1]) - date.fromisoformat(days[0])).days + 1 == len(days)  # no holes


def test_fiscal_calendar_year_boundaries(data: Data) -> None:
    cal = data.ora("WWI_REF", "CALENDAR_FISCAL")
    starts = {(r["CALENDAR_CD"], r["YEAR_START_DT"][5:]) for r in cal}
    assert ("EUCAL", "01-01") in starts and ("APACJUN", "07-01") in starts
    assert all(r["CALENDAR_DT"] == r["YEAR_START_DT"] for r in cal if r["CALENDAR_CD"] == "EUCAL" and r["CALENDAR_DT"].endswith("-01-01"))
    na = {r["YEAR_START_DT"][5:] for r in cal if r["CALENDAR_CD"] == "NA445"}
    assert na == {"11-01"}  # FY starts 1 Nov
    naPeriodLengths = Counter((r["FISCAL_YEAR_NBR"], r["FISCAL_PERIOD_NBR"]) for r in cal if r["CALENDAR_CD"] == "NA445")
    assert set(naPeriodLengths.values()) <= {28, 35, 36, 37}  # 4-4-5 weeks; period 12 absorbs the 365/366-day remainder
    assert all(int(r["FISCAL_PERIOD_NBR"]) in range(1, 13) and int(r["FISCAL_QUARTER_NBR"]) in range(1, 5) for r in cal)


# ---------------------------------------------------------------------------------------------- FX


def test_missing_fx_edge_cases(data: Data) -> None:
    present = {(r["FROM_CURR_CD"], r["RATE_DT"]) for r in data.ora("WWI_REF", "FX_RATE_DAILY")}
    for code in ("MISSING_FX_RATE", "MISSING_FX_WEEKEND", "MISSING_FX_MONTH"):
        for key in data.keys(code):
            assert (key["FROM_CURR_CD"], key["RATE_DT"]) not in present, (code, key)
    weekend = sorted(date.fromisoformat(k["RATE_DT"]) for k in data.keys("MISSING_FX_WEEKEND"))
    assert len(weekend) == 2 and weekend[0].weekday() == 5 and weekend[1] == weekend[0] + timedelta(days=1)
    assert len({k["FROM_CURR_CD"] for k in data.keys("MISSING_FX_WEEKEND")}) == 1
    month = sorted(date.fromisoformat(k["RATE_DT"]) for k in data.keys("MISSING_FX_MONTH"))
    assert month[0].day == 1 and (month[-1] + timedelta(days=1)).day == 1 and len(month) == (month[-1] - month[0]).days + 1
    currency = {k["FROM_CURR_CD"] for k in data.keys("MISSING_FX_MONTH")}
    assert len(currency) == 1 and not any(c == next(iter(currency)) and month[0].isoformat()[:7] == d[:7] for c, d in present)
    assert len(data.keys("MISSING_FX_RATE")) >= 10
    assert {r["RATE_TYPE_CD"] for r in data.ora("WWI_REF", "FX_RATE_DAILY")} == {"CORP", "BANK", "ECB"}


# ---------------------------------------------------------------------------------------------- MDM


def test_mdm_xref_edge_cases(data: Data) -> None:
    master = byId(data.ora("WWI_MDM", "CUST_MASTER"), "CUST_ID")
    merges = data.ora("WWI_MDM", "MDM_MERGE_HISTORY")
    mergedInto = {m["MERGED_PARTY_ID"]: m["SURVIVOR_PARTY_ID"] for m in merges}
    xrefs = data.ora("WWI_MDM", "PARTY_XREF")
    xrefByCustomer = defaultdict(list)
    for x in xrefs:
        xrefByCustomer[x["SOURCE_KEY_TXT"]].append(x)
    customers = {c["CustomerID"] for c in data.sql("Sales", "Customers")}

    for key in data.keys("XREF_RETIRED_SINGLE_HOP"):
        retired, survivor = str(key["RETIRED_CUST_ID"]), str(key["SURVIVOR_CUST_ID"])
        assert master[retired]["CUST_STATUS_CD"] == "MG" and master[survivor]["CUST_STATUS_CD"] == "AC"
        assert mergedInto[retired] == survivor and survivor not in mergedInto
        assert {x["CUST_ID"] for x in xrefByCustomer[str(key["CustomerID"])]} == {retired}
    for key in data.keys("XREF_RETIRED_TWO_HOP"):
        retired, middle, survivor = str(key["RETIRED_CUST_ID"]), str(key["INTERMEDIATE_CUST_ID"]), str(key["SURVIVOR_CUST_ID"])
        assert mergedInto[retired] == middle and mergedInto[middle] == survivor and survivor not in mergedInto
        assert master[retired]["CUST_STATUS_CD"] == master[middle]["CUST_STATUS_CD"] == "MG"
        assert {x["CUST_ID"] for x in xrefByCustomer[str(key["CustomerID"])]} == {retired}
    for key in data.keys("XREF_RETIRED_NO_MERGE"):
        retired = str(key["RETIRED_CUST_ID"])
        assert master[retired]["CUST_STATUS_CD"] == "MG" and retired not in mergedInto
        assert {x["CUST_ID"] for x in xrefByCustomer[str(key["CustomerID"])]} == {retired}
    for key in data.keys("DUPLICATE_XREF"):
        rows = xrefByCustomer[str(key["CustomerID"])]
        assert len(rows) == 2 and all(x["ACTIVE_FLG"] == "Y" and x["CUST_ID"] == str(key["CUST_ID"]) for x in rows)
    for key in data.keys("MISSING_XREF"):
        assert str(key["CustomerID"]) in customers and str(key["CustomerID"]) not in xrefByCustomer
    flagged = {str(k["CustomerID"]) for code in ("DUPLICATE_XREF", "MISSING_XREF") for k in data.keys(code)}
    assert all(len(v) == 1 for c, v in xrefByCustomer.items() if c not in flagged)
    assert customers - set(xrefByCustomer) == {str(k["CustomerID"]) for k in data.keys("MISSING_XREF")}


def test_eu_consent_and_partner_feed(data: Data) -> None:
    customers = byId(data.sql("Sales", "Customers"), "CustomerID")
    keys = {str(k["CustomerID"]) for k in data.keys("EU_CONSENT_N")}
    assert keys == {c for c, r in customers.items() if r["RegionCode"] == "EU" and r["MarketingConsentFlag"] == "N"}
    assert len(keys) >= 10
    master = byId(data.ora("WWI_MDM", "CUST_MASTER"), "CUST_ID")
    for x in data.ora("WWI_MDM", "PARTY_XREF"):
        if x["SOURCE_KEY_TXT"] in keys and master[x["CUST_ID"]]["CUST_STATUS_CD"] == "AC":
            assert master[x["CUST_ID"]]["CONSENT_MARKETING_FLG"] == "N"


# ---------------------------------------------------------------------------------------------- codes


def test_code_translation_edge_cases(data: Data) -> None:
    xlat = data.ora("WWI_REF", "CODE_TRANSLATION")
    translated = {(r["CODE_SET_CD"], r["SOURCE_VALUE_TXT"], r["REGION_CD"]) for r in xlat}
    untranslated = data.keys("UNTRANSLATED_CODE")
    assert {k["CODE_SET_CD"] for k in untranslated} == {"SALES_CHANNEL", "PAYMENT_METHOD", "RETURN_REASON"}
    for key in untranslated:
        assert not any(r["CODE_SET_CD"] == key["CODE_SET_CD"] and r["SOURCE_VALUE_TXT"] == key["SOURCE_VALUE_TXT"] for r in xlat)
    assert (("SALES_CHANNEL", "FIELDNA", "NA") in translated)
    byId_ = byId(xlat, "TRANSLATION_ID")
    for key in data.keys("STALE_CODE"):
        row = byId_[str(key["TRANSLATION_ID"])]
        assert row["ACTIVE_FLG"] == "N" and row["EFFECTIVE_TO_DT"] and row["EFFECTIVE_TO_DT"] < data.manifest["spanStart"]
    channelCodes = {c["ChannelCode"] for c in data.sql("Sales", "SalesChannels")}
    assert all(k["SOURCE_VALUE_TXT"] in channelCodes for k in untranslated if k["CODE_SET_CD"] == "SALES_CHANNEL")

    payments = byId(data.sql("Sales", "CustomerPayments"), "CustomerPaymentID")
    for key in data.keys("UNTRANSLATED_PAYMENT_METHOD_USED"):
        assert payments[str(key["CustomerPaymentID"])]["PaymentMethodCode"] == key["PaymentMethodCode"]
        assert not any(r["CODE_SET_CD"] == "PAYMENT_METHOD" and r["SOURCE_VALUE_TXT"] == key["PaymentMethodCode"] for r in xlat)
    reasons = byId(data.sql("Returns", "ReturnReasons"), "ReturnReasonID")
    rmaLines = defaultdict(set)
    for line in data.sql("Returns", "ReturnLines"):
        rmaLines[line["ReturnAuthorizationID"]].add(reasons[line["ReturnReasonID"]]["ReasonCode"])
    for key in data.keys("UNTRANSLATED_RETURN_REASON_USED") + data.keys("STALE_RETURN_REASON_USED"):
        assert key["ReasonCode"] in rmaLines[str(key["ReturnAuthorizationID"])]


# ---------------------------------------------------------------------------------------------- incremental extracts


def test_duplicate_order_lines(data: Data) -> None:
    lines = byId(data.sql("Sales", "OrderLines"), "OrderLineID")
    keys = data.keys("DUPLICATE_ORDER_LINE")
    assert len(keys) >= 5
    for key in keys:
        a, b = (lines[i] for i in key["OrderLineIDs"].split(","))
        assert (a["OrderID"], a["StockItemID"], a["Description"]) == (b["OrderID"], b["StockItemID"], b["Description"]) == (str(key["OrderID"]), str(key["StockItemID"]), a["Description"])
        assert a["LastEditedWhen"] != b["LastEditedWhen"]
    grouped = Counter((r["OrderID"], r["StockItemID"], r["Description"]) for r in lines.values())
    assert sum(1 for n in grouped.values() if n > 1) == len(keys)


def test_duplicate_customer_rows_and_late_arriving_customers(data: Data) -> None:
    rows = defaultdict(list)
    for c in data.sql("Sales", "Customers"):
        rows[c["CustomerID"]].append(c)
    for key in data.keys("DUPLICATE_CUSTOMER_EXTRACT"):
        pair = rows[str(key["CustomerID"])]
        assert len(pair) == 2 and pair[0]["ValidFrom"] != pair[1]["ValidFrom"] and pair[0]["LastEditedBy"] != pair[1]["LastEditedBy"]
        assert {r["ValidTo"] for r in pair} == {"9999-12-31T23:59:59"}
    watermark = {(w["SourceSchemaName"], w["SourceTableName"]): w for w in data.sql("Integration", "ChangeTrackingWatermark")}
    customerWatermark = watermark[("Sales", "Customers")]["LastExtractedWhen"]
    invoices = defaultdict(list)
    for inv in data.sql("Sales", "Invoices"):
        invoices[inv["CustomerID"]].append(inv)
    late = data.keys("LATE_ARRIVING_CUSTOMER")
    assert len(late) >= 3
    for key in late:
        customer = rows[str(key["CustomerID"])]
        assert len(customer) == 1 and customer[0]["ValidFrom"] > customerWatermark  # only in the *next* customer extract
        assert invoices[str(key["CustomerID"])], "late-arriving customer must already have invoices"
        assert all(inv["LastEditedWhen"] <= watermark[("Sales", "Invoices")]["LastExtractedWhen"] for inv in invoices[str(key["CustomerID"])])
    assert int(watermark[("Sales", "Customers")]["OverlapMinutes"]) > 0


# ---------------------------------------------------------------------------------------------- tax


def test_eu_reverse_charge(data: Data) -> None:
    orders = byId(data.sql("Sales", "Orders"), "OrderID")
    customers = byId(data.sql("Sales", "Customers"), "CustomerID")
    territories = byId(data.sql("Sales", "SalesTerritories"), "SalesTerritoryID")
    rc = {str(k["OrderID"]) for k in data.keys("EU_REVERSE_CHARGE")}
    assert rc == {o for o, r in orders.items() if r["TaxRegimeCode"] == "EU_RC"} and len(rc) >= 50
    for orderId in rc:
        order = orders[orderId]
        assert territories[order["SalesTerritoryID"]]["RegionCode"] == "EU" and territories[order["SalesTerritoryID"]]["CountryISO3"] != "NLD"
    lines = defaultdict(list)
    for line in data.sql("Sales", "OrderLines"):
        lines[line["OrderID"]].append(line)
    assert all(Decimal(l["TaxRate"]) == 0 for o in rc for l in lines[o])
    invoices = byId(data.sql("Sales", "Invoices"), "InvoiceID")
    rcInvoices = [i for i in invoices.values() if i["TaxRegimeCode"] == "EU_RC"]
    assert rcInvoices and all(Decimal(i["InvoiceTaxAmount"]) == 0 for i in rcInvoices)
    nullVat = {str(k["InvoiceID"]) for k in data.keys("EU_REVERSE_CHARGE_NULL_VAT")}
    assert nullVat == {i["InvoiceID"] for i in rcInvoices if i["CustomerTaxNumber"] == ""}
    assert any(i["CustomerTaxNumber"] != "" for i in rcInvoices)
    for invoiceId in nullVat:  # the customer *does* have a VAT number, the invoice extract lost it
        assert customers[invoices[invoiceId]["CustomerID"]]["TaxRegistrationNumber"] != ""
    domestic = [o for o in orders.values() if o["TaxRegimeCode"] in ("EUVAT", "UKVAT")]
    assert domestic and any(Decimal(l["TaxRate"]) > 0 for o in domestic for l in lines[o["OrderID"]])


def test_apac_gst_inclusive_residual(data: Data) -> None:
    lines = byId(data.sql("Sales", "OrderLines"), "OrderLineID")
    orders = byId(data.sql("Sales", "Orders"), "OrderID")
    keys = data.keys("GST_INCLUSIVE_RESIDUAL")
    assert len(keys) >= 5
    for key in keys:
        line = lines[str(key["OrderLineID"])]
        order = orders[line["OrderID"]]
        assert order["IsTaxInclusivePricing"] == "1" and order["CurrencyCode"] in ("AUD", "SGD", "JPY")
        gross = Decimal(line["UnitPrice"]) * 100
        divisor = 100 + Decimal(line["TaxRate"])
        assert gross % divisor != 0, f"{line['UnitPrice']} divides cleanly at {line['TaxRate']}%"
    assert any(o["IsTaxInclusivePricing"] == "1" for o in orders.values()) and any(o["IsTaxInclusivePricing"] == "0" for o in orders.values())


# ---------------------------------------------------------------------------------------------- orders / channels


def test_fulfilment_flags(data: Data) -> None:
    orders = byId(data.sql("Sales", "Orders"), "OrderID")
    flags = Counter(o["FulfilmentFlags"] for o in orders.values())
    assert flags["P|B|H"] >= 1 and flags["P"] > 100
    empty = {str(k["OrderID"]) for k in data.keys("FULFILMENT_FLAGS_EMPTY")}
    assert empty and all(orders[o]["FulfilmentFlags"] == "" for o in empty)
    malformed = {orders[str(k["OrderID"])]["FulfilmentFlags"] for k in data.keys("FULFILMENT_FLAGS_MALFORMED")}
    assert "P||B" in malformed and any(f.endswith("|") for f in malformed)
    for key in data.keys("FULFILMENT_FLAGS_MALFORMED"):
        assert orders[str(key["OrderID"])]["FulfilmentFlags"] == key["FulfilmentFlags"]


def test_pilot_channel(data: Data) -> None:
    channels = byId(data.sql("Sales", "SalesChannels"), "SalesChannelID")
    pilot = {str(k["SalesChannelID"]) for k in data.keys("PILOT_CHANNEL")}
    assert pilot == {c for c, r in channels.items() if r["ChannelStatus"] == "PILOT"} and len(pilot) == 1
    orders = byId(data.sql("Sales", "Orders"), "OrderID")
    pilotOrders = {str(k["OrderID"]) for k in data.keys("PILOT_CHANNEL_ORDER")}
    assert pilotOrders == {o for o, r in orders.items() if r["SalesChannelID"] in pilot} and len(pilotOrders) >= 20
    assert {"ACTIVE", "PILOT", "CLOSED"} <= {c["ChannelStatus"] for c in channels.values()}


def test_deleted_orders_logged(data: Data) -> None:
    orders = {o["OrderID"] for o in data.sql("Sales", "Orders")}
    deleted = {str(k["OrderID"]) for k in data.keys("DELETED_ORDER")}
    assert deleted and not (deleted & orders)
    assert deleted == {r["OrderID"] for r in data.sql("Sales", "OrderDeletionLog")}
    assert deleted == {r["SourceKeyValue"] for r in data.sql("Integration", "DeletedRowLog") if r["SourceTableName"] == "Orders"}
    assert not any(line["OrderID"] in deleted for line in data.sql("Sales", "OrderLines"))


# ---------------------------------------------------------------------------------------------- payments


def test_payment_edge_cases(data: Data) -> None:
    payments = byId(data.sql("Sales", "CustomerPayments"), "CustomerPaymentID")
    invoices = byId(data.sql("Sales", "Invoices"), "InvoiceID")
    allocations = defaultdict(list)
    for a in data.sql("Sales", "PaymentAllocations"):
        allocations[a["CustomerPaymentID"]].append(a)

    def allocatedTotal(paymentId: str) -> Decimal:
        return sum((Decimal(a["AllocatedAmount"]) for a in allocations[paymentId]), Decimal(0))

    for key in data.keys("OVERPAYMENT"):
        pay = payments[str(key["CustomerPaymentID"])]
        assert Decimal(pay["ReceivedAmount"]) > allocatedTotal(pay["CustomerPaymentID"]) > 0
        invoiceIds = key["InvoiceIDs"].split(",")
        assert Decimal(pay["ReceivedAmount"]) > sum(Decimal(invoices[i]["InvoiceTotalExTax"]) + Decimal(invoices[i]["InvoiceTaxAmount"]) for i in invoiceIds)
    for key in data.keys("UNDERPAYMENT"):
        pay = payments[str(key["CustomerPaymentID"])]
        invoice = invoices[key["InvoiceIDs"]]
        assert Decimal(pay["ReceivedAmount"]) == allocatedTotal(pay["CustomerPaymentID"]) < Decimal(invoice["InvoiceTotalExTax"]) + Decimal(invoice["InvoiceTaxAmount"])
        assert invoice["SettlementStatus"] == "PARTPAID" and Decimal(invoice["AmountOutstanding"]) > 0
    for key in data.keys("MULTI_INVOICE_PAYMENT"):
        pay = payments[str(key["CustomerPaymentID"])]
        targets = {a["InvoiceID"] for a in allocations[pay["CustomerPaymentID"]]}
        assert len(targets) >= 2 and targets == set(key["InvoiceIDs"].split(","))
        assert all(invoices[i]["CustomerID"] == pay["CustomerID"] for i in targets)
    for key in data.keys("ON_ACCOUNT_PAYMENT"):
        pay = payments[str(key["CustomerPaymentID"])]
        assert not allocations[pay["CustomerPaymentID"]] and Decimal(pay["AllocatedAmount"]) == 0 and pay["PaymentStatus"] == "UNAPPLIED"
    for paymentId, pay in payments.items():
        assert Decimal(pay["AllocatedAmount"]) == allocatedTotal(paymentId) <= Decimal(pay["ReceivedAmount"])
    txnTypes = byId(data.sql("Application", "TransactionTypes"), "TransactionTypeID")
    kinds = Counter(txnTypes[t["TransactionTypeID"]]["TransactionTypeName"] for t in data.sql("Sales", "CustomerTransactions"))
    assert kinds["Customer Invoice"] == len(invoices) and kinds["Customer Payment Received"] == len(payments)


def test_disputes_and_write_offs(data: Data) -> None:
    invoices = byId(data.sql("Sales", "Invoices"), "InvoiceID")
    disputes = data.sql("Sales", "CustomerDisputes")
    assert disputes and all(invoices[d["InvoiceID"]]["DisputeFlag"] == "1" for d in disputes)
    writeOffs = data.sql("Sales", "CustomerWriteOffs")
    assert writeOffs and all(invoices[w["InvoiceID"]]["SettlementStatus"] == "WRITTENOFF" for w in writeOffs if w["CustomerDisputeID"] == "")


# ---------------------------------------------------------------------------------------------- commission


def test_commission_plans_and_eu_cap(data: Data) -> None:
    plans = byId(data.sql("Sales", "CommissionPlans"), "CommissionPlanID")
    covered = {plans[str(k["CommissionPlanID"])]["CommissionBasis"] for k in data.keys("COMMISSION_BASIS_COVERAGE")}
    assert covered == {"INVOICEDMARGIN", "NETREVENUE", "COLLECTEDCASH"}
    assert {p["CommissionBasis"] for p in plans.values()} == covered
    capPlans = [p for p in plans.values() if p["RegionCode"] == "EU" and p["Band3UpperPercent"] != ""]
    assert capPlans, "an EU plan with a statutory cap (Band3UpperPercent) is required"
    members = data.sql("Application", "SalesTeamMembers")
    quotas = byId(data.sql("Sales", "SalesQuotas"), "SalesQuotaID")
    for key in data.keys("COMMISSION_EU_CAP_HIT"):
        quota = quotas[str(key["SalesQuotaID"])]
        plan = next(p for p in capPlans if p["PlanCode"] == key["PlanCode"])
        assert quota["SalespersonPersonID"] == str(key["SalespersonPersonID"])
        assert any(m["PersonID"] == quota["SalespersonPersonID"] and m["CommissionPlanID"] == plan["CommissionPlanID"] for m in members)
        attainmentPct = Decimal(quota["AttainmentAmount"]) / Decimal(quota["QuotaAmount"]) * 100
        assert attainmentPct > Decimal(plan["Band3UpperPercent"]), "attainment must exceed the cap band"
    assert all(m["CommissionPlanID"] != "" for m in members if m["RoleCode"] == "REP")


def test_territory_allocation_quirks(data: Data) -> None:
    members = data.sql("Application", "SalesTeamMembers")
    teams = byId(data.sql("Application", "SalesTeams"), "SalesTeamID")
    shares = defaultdict(list)
    for m in members:
        if m["ValidTo"] == "" and m["QuotaSharePercent"] != "":
            shares[teams[m["SalesTeamID"]]["RegionCode"]].append(Decimal(m["QuotaSharePercent"]))
    assert {Decimal("80.00"), Decimal("20.00")} <= set(shares["NA"])  # primary + inside-sales overlay
    assert all(s >= 25 for s in shares["EU"])  # EU floor
    assert Decimal("100.00") in shares["APAC"]  # distributor-managed territory -> channel manager


def test_stock_items_have_cost_and_groups(data: Data) -> None:
    items = data.sql("Warehouse", "StockItems")
    holdings = byId(data.sql("Warehouse", "StockItemHoldings"), "StockItemID")
    grouped = {g["StockItemID"] for g in data.sql("Warehouse", "StockItemStockGroups")}
    assert {i["StockItemID"] for i in items} == set(holdings) == grouped
    assert all(Decimal(h["LastCostPrice"]) > 0 for h in holdings.values())
    products = data.ora("WWI_MDM", "PRODUCT_MASTER")
    assert {p["WWI_STOCK_ITEM_ID"] for p in products} == {i["StockItemID"] for i in items}
    assert all(Decimal(p["UNIT_COST_STD"]) > 0 for p in products)


def test_medium_scale_is_ten_times_small(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    manifest = generate(tmp_path, seed=42, scale="medium", asOf=AS_OF)
    counts = {t["path"]: t["rowCount"] for t in manifest["tables"]}
    assert 1_990 <= counts["sqlserver/Sales/Customers.csv"] <= 2_030
    assert 49_000 <= counts["sqlserver/Sales/Orders.csv"] <= 50_000
    assert {e["code"] for e in manifest["edgeCases"]} == DOCUMENTED_EDGE_CASES
