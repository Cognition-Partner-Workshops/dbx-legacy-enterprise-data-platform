"""SQL Server transactional generators: quotes, orders, order lines, holds, backorders,
amendments, deletions, invoices, shipments, customer transactions, payments and
allocations, disputes, write-offs, returns / credit notes and the ETL control tables.

Requires ``sqlserver.generateMasterData`` and ``oracle.generateReference`` to have run."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

from .common import GenContext, People, Row, money, qty, rate
from .domain import (
    ACTIVE_CHANNELS_BY_REGION,
    COUNTRIES,
    PAYMENT_METHOD_TO_WWI_ID,
    REGION_PAYMENT_METHODS,
    STALE_RETURN_REASON,
    STANDARD_TAX_RATE,
    UNTRANSLATED_RETURN_REASON,
    Channel,
    Country,
    Territory,
)
from .schema import SQLSERVER, TABLE_COLUMNS
from .sqlserver import RET, SALES, SYSTEM_USER, WH, _edited

INTEGRATION, SHIPPING = "Integration", "Shipping"
EU_SELLING_ENTITY_COUNTRY = "NL"
ZERO = Decimal("0.00")


def _ts(day: date, hour: int = 10, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, 0)


@dataclass
class OrderInfo:
    lines: list[Row]
    status: str
    customer: Row
    territory: Territory
    country: Country
    currency: str
    taxRate: Decimal
    taxRegime: str
    inclusive: bool
    reverseCharge: bool
    salesperson: int
    channel: Channel
    orderDate: date
    netTotal: Decimal


@dataclass
class InvoiceInfo:
    total: Decimal
    customer: Row
    currency: str
    dueDate: date
    invoiceDate: date
    order: Row


MALFORMED_FLAG_VARIANTS = ("P||B", "P|B|", "|P", "P|B|H|")
CREDIT_NOTE_TAX_REGIME = {"USSALESTAX": "SALESTAX", "CAGSTHST": "GST", "UKVAT": "VAT", "EUVAT": "VAT", "EU_RC": "VAT",
                          "AUGST": "GST", "SGGST": "GST", "JPCT": "CONSUMPTION"}


def creditNoteTaxRegime(orderTaxRegime: str, customerTaxNumber: str | None) -> str:
    # LEGACY QUIRK: Returns.CreditNotes carries the coarse regime family (SALESTAX/VAT/GST/CONSUMPTION/NONE) and
    # CK_Returns_CreditNotes_EuTaxNumber forbids an issued VAT credit note without a customer tax number,
    # so the returns desk books those as NONE.
    regime = CREDIT_NOTE_TAX_REGIME[orderTaxRegime]
    return "NONE" if regime == "VAT" and not customerTaxNumber else regime


class TransactionBuilder:
    def __init__(self, ctx: GenContext) -> None:
        self.ctx = ctx
        self.rng = ctx.rng("sqlserver.transactions")
        self.customers = ctx.scratch.customersById
        self.customerCountry = ctx.scratch.customerCountry
        self.customerTerritory = ctx.scratch.customerTerritory
        self.lateArriving = ctx.scratch.lateArrivingCustomers
        self.fx = ctx.scratch.fxRates
        self.priceLists = ctx.scratch.priceLists
        self.priceByListAndItem = ctx.scratch.priceByListAndItem
        self.costByItem = ctx.scratch.costByItem
        self.items: dict[int, Row] = {r["StockItemID"]: r for r in ctx.sql(WH, "StockItems")}
        self.itemIds = sorted(self.items)
        people = ctx.scratch.people
        assert people is not None
        self.people: People = people
        self.promotions = ctx.scratch.promotionsByRegion
        self.channels = {c.channelId: c for region in ACTIVE_CHANNELS_BY_REGION.values() for c in region}
        self.returnReasons = ctx.sql(RET, "ReturnReasons")
        watermark = ctx.scratch.extractWatermark
        assert watermark is not None
        self.watermark = watermark
        self.tables: dict[str, list[Row]] = defaultdict(list)
        # per-order working state
        self.orderInfo: dict[int, OrderInfo] = {}
        self.invoiceInfo: dict[int, InvoiceInfo] = {}

    # --- helpers ----------------------------------------------------------------
    def fxRate(self, currency: str, day: date) -> Decimal:
        return rate(self.fx[(currency, min(max(day, self.ctx.spanStart), self.ctx.spanEnd))])

    def salespersonFor(self, customerId: int) -> int:
        territory = self.customerTerritory[customerId]
        return self.rng.choice(self.people.salespeopleByTerritory[territory.code])

    def nextId(self, table: str) -> int:
        return len(self.tables[table]) + 1

    # --- orders -------------------------------------------------------------------
    def buildOrders(self) -> None:
        ctx, rng = self.ctx, self.rng
        customerIds = sorted(self.customers)
        regular = [c for c in customerIds if c not in self.lateArriving]
        activity = {c: rng.choice([1, 1, 2, 3, 5]) for c in regular}
        weights = [activity[c] for c in regular]
        total = ctx.params["orders"]
        watermarkDay = self.watermark.date()

        plan: list[tuple[date, int]] = []
        for _ in range(total - 3 * len(self.lateArriving)):
            plan.append((ctx.randomDate(rng), rng.choices(regular, weights=weights, k=1)[0]))
        for customerId in sorted(self.lateArriving):
            for _ in range(3):
                plan.append((ctx.randomDate(rng, watermarkDay - timedelta(days=30), watermarkDay - timedelta(days=12)), customerId))
        plan.sort()

        deletedSlots = set(rng.sample(range(50, len(plan)), 6))
        malformedFlagSlots = {slot: MALFORMED_FLAG_VARIANTS[i % len(MALFORMED_FLAG_VARIANTS)] for i, slot in enumerate(sorted(rng.sample(range(len(plan)), 6)))}
        emptyFlagSlots = set(rng.sample([i for i in range(len(plan)) if i not in malformedFlagSlots], 5))
        duplicateLineSlots = set(rng.sample(range(len(plan)), 8))
        gstResidualSlots = set()
        orderId = 0
        rowVersion = 0x1A000
        for slot, (orderDate, customerId) in enumerate(plan):
            orderId += 1
            rowVersion += rng.randint(1, 40)
            customer = self.customers[customerId]
            territory = self.customerTerritory[customerId]
            country = COUNTRIES[self.customerCountry[customerId]]
            region = territory.region
            if slot in deletedSlots:
                self.addDeletedOrder(orderId, customer, territory, orderDate)
                continue
            channelChoices = ACTIVE_CHANNELS_BY_REGION[region]
            channelWeights = [2 if c.status == "PILOT" else (10 if c.channelClass in ("FIELD", "WEB") else 5) for c in channelChoices]
            channel = rng.choices(channelChoices, weights=channelWeights, k=1)[0]
            priceListId = customer["DefaultPriceListID"]
            priceList = self.priceLists[priceListId]
            currency = priceList["CurrencyCode"]
            age = (ctx.asOf - orderDate).days
            isLate = customerId in self.lateArriving
            roll = rng.random()
            if isLate:
                status = "INVOICED"
            elif roll < 0.03:
                status = "CANCELLED"
            elif age > 10:
                status = "INVOICED" if roll < 0.95 else ("SHIPPED" if roll < 0.985 else "HOLD")
            else:
                status = rng.choice(["ENTERED", "CONFIRMED", "ALLOCATED", "PICKING", "SHIPPED", "HOLD"])

            # LEGACY QUIRK: Integration.usp_LoadFactSale zero-rates intra-community supplies only when the
            # order carries TaxRegimeCode 'EU_RC' AND a customer VAT number; GB is treated as domestic UKVAT.
            reverseCharge = (country.euVatArea and country.iso2 not in (EU_SELLING_ENTITY_COUNTRY, "GB")
                             and customer["TaxRegistrationNumber"] is not None and rng.random() < 0.6)
            taxRegime = "EU_RC" if reverseCharge else country.taxRegime
            taxExempt = customer["TaxExemptionCertificate"] is not None
            taxRate = ZERO if (reverseCharge or taxExempt) else money(STANDARD_TAX_RATE[country.iso2])
            inclusive = priceList["TaxTreatment"] == "INCLUSIVE"
            salesperson = self.salespersonFor(customerId)
            lineCount = rng.choice([1, 1, 2, 2, 3, 4, 5, 6])
            hasBackorder = status not in ("CANCELLED", "ENTERED") and rng.random() < 0.08
            promotion = rng.choice(self.promotions[region]) if rng.random() < 0.1 else None
            if promotion and not (promotion["StartDate"] <= orderDate <= promotion["EndDate"]):
                promotion = None

            lines: list[Row] = []
            netTotal = ZERO
            discountTotal = ZERO
            chosenItems = rng.sample(self.itemIds, lineCount)
            for lineIndex, stockItemId in enumerate(chosenItems):
                item = self.items[stockItemId]
                priceLine = self.priceByListAndItem[(priceListId, stockItemId)]
                unitPrice = priceLine["UnitPrice"]
                quantity = rng.choice([1, 2, 3, 5, 6, 10, 12, 24, 48])
                if region == "APAC" and inclusive and country.iso2 == "AU" and lineIndex == 0 and len(gstResidualSlots) < 10 and slot % 3 == 0:
                    unitPrice = money(Decimal(rng.choice(["10.99", "21.49", "7.95", "33.33", "12.34"])))
                    gstResidualSlots.add(slot)
                    residual = True
                else:
                    residual = False
                discountPct = qty(customer["StandardDiscountPercentage"])
                if promotion is not None:
                    discountPct = qty("10.000")
                gross = money(unitPrice * quantity)
                exTaxGross = money(gross / (1 + taxRate / 100)) if inclusive and taxRate else gross
                discountAmount = money(exTaxGross * discountPct / 100)
                lineNet = money(exTaxGross - discountAmount)
                picked = quantity if status in ("INVOICED", "SHIPPED", "PICKING") else 0
                backordered = 0
                if hasBackorder and lineIndex == lineCount - 1 and quantity > 1:
                    backordered = quantity // 2
                    picked = quantity - backordered if picked else 0
                lineStatus = {"CANCELLED": "CANCELLED", "INVOICED": "SHIPPED", "SHIPPED": "SHIPPED", "PICKING": "PICKED", "ALLOCATED": "ALLOCATED"}.get(status, "OPEN")
                if backordered and lineStatus == "SHIPPED":
                    lineStatus = "BACKORDER"
                line = {
                    "OrderLineID": self.nextId("OrderLines"), "OrderID": orderId, "StockItemID": stockItemId, "Description": item["StockItemName"],
                    "PackageTypeID": item["UnitPackageID"], "Quantity": quantity, "UnitPrice": unitPrice, "TaxRate": qty(taxRate), "PickedQuantity": picked,
                    "PickingCompletedWhen": _ts(orderDate + timedelta(days=1), 14) if picked else None, **_edited(salesperson, _ts(orderDate, 11, lineIndex)),
                    "PriceListLineID": priceLine["PriceListLineID"], "ListUnitPrice": priceLine["UnitPrice"], "DiscountPercent": discountPct, "DiscountAmount": discountAmount,
                    "PromotionID": promotion["PromotionID"] if promotion else None, "LineNetAmount": lineNet, "QuantityAllocated": qty(quantity - backordered if status not in ("CANCELLED", "ENTERED") else 0),
                    "QuantityShipped": qty(picked), "QuantityBackordered": qty(backordered), "LineStatusCode": lineStatus,
                    "RequestedDeliveryDate": orderDate + timedelta(days=rng.randint(3, 14)), "SourceLineReference": f"{channel.orderPrefix}-{orderId:07d}-{lineIndex + 1:02d}",
                }
                lines.append(line)
                self.tables["OrderLines"].append(line)
                if residual:
                    ctx.tag("GST_INCLUSIVE_RESIDUAL", "APAC GST-inclusive unit price that does not divide cleanly by (1 + rate); ex-tax amount leaves a truncation residual.",
                            f"{SALES}.OrderLines", OrderLineID=line["OrderLineID"], OrderID=orderId, UnitPrice=unitPrice, TaxRate=taxRate)
                if backordered:
                    self.addBackorder(orderId, line, orderDate, status)
                netTotal += lineNet
                discountTotal += discountAmount
            if slot in duplicateLineSlots and lines:
                # LEGACY QUIRK: the OrderLines extract keys on OrderID+StockItemID+Description, and a line
                # re-saved by the picking app shows up twice with a later LastEditedWhen.
                duplicate = dict(lines[0])
                duplicate["OrderLineID"] = self.nextId("OrderLines")
                duplicate["LastEditedWhen"] = lines[0]["LastEditedWhen"] + timedelta(days=1, hours=2)
                duplicate["LastEditedBy"] = self.people.pickers[0]
                self.tables["OrderLines"].append(duplicate)
                ctx.tag("DUPLICATE_ORDER_LINE", "Two OrderLines rows with the same OrderID + StockItemID + Description and different LastEditedWhen.", f"{SALES}.OrderLines",
                        OrderID=orderId, StockItemID=lines[0]["StockItemID"], OrderLineIDs=f"{lines[0]['OrderLineID']},{duplicate['OrderLineID']}")

            flagParts = []
            if any(line["PickedQuantity"] for line in lines):
                flagParts.append("P")
            if hasBackorder:
                flagParts.append("B")
            onHold = status == "HOLD" or rng.random() < 0.03
            if onHold:
                flagParts.append("H")
            flags = "|".join(flagParts)
            if slot in malformedFlagSlots:
                flags = malformedFlagSlots[slot]
                ctx.tag("FULFILMENT_FLAGS_MALFORMED", "FulfilmentFlags with empty segments or a trailing pipe.", f"{SALES}.Orders", OrderID=orderId, FulfilmentFlags=flags)
            elif slot in emptyFlagSlots:
                flags = ""
                ctx.tag("FULFILMENT_FLAGS_EMPTY", "FulfilmentFlags is an empty string (not NULL).", f"{SALES}.Orders", OrderID=orderId)
            amendmentCount = 1 if rng.random() < 0.05 and status != "CANCELLED" else 0
            sourceQuoteId = self.addConvertedQuote(orderId, customer, salesperson, channel, priceListId, currency, orderDate, lines, taxRate) if rng.random() < 0.08 else None
            order = {
                "OrderID": orderId, "CustomerID": customerId, "SalespersonPersonID": salesperson,
                "PickedByPersonID": rng.choice(self.people.pickers) if any(line["PickedQuantity"] for line in lines) else None,
                "ContactPersonID": customer["PrimaryContactPersonID"], "BackorderOrderID": None, "OrderDate": orderDate,
                "ExpectedDeliveryDate": orderDate + timedelta(days=rng.randint(2, 10)), "CustomerPurchaseOrderNumber": f"PO{rng.randint(10000, 99999)}" if rng.random() < 0.7 else None,
                "IsUndersupplyBackordered": hasBackorder, "Comments": None, "DeliveryInstructions": None, "InternalComments": None,
                "PickingCompletedWhen": _ts(orderDate + timedelta(days=1), 15) if any(line["PickedQuantity"] for line in lines) else None,
                **_edited(salesperson, _ts(orderDate, 10)), "SalesChannelID": channel.channelId, "SalesTerritoryID": territory.territoryId, "PriceListID": priceListId,
                "SourceQuoteID": sourceQuoteId, "OrderStatusCode": status, "FulfilmentFlags": flags, "CurrencyCode": currency, "ExchangeRateToUsd": self.fxRate(currency, orderDate),
                "TaxRegimeCode": taxRegime, "IsTaxInclusivePricing": inclusive, "OrderValueExTax": money(netTotal), "TotalDiscountAmount": money(discountTotal),
                "AmendmentCount": amendmentCount, "CreditHoldAppliedWhen": _ts(orderDate, 10, 5) if status == "HOLD" else None,
                "WebCartID": rng.randint(10**7, 10**8 - 1) if channel.channelClass in ("WEB", "MARKETPLACE") else None, "ExtractedRowVersion": f"0x{rowVersion:016X}",
            }
            self.tables["Orders"].append(order)
            self.orderInfo[orderId] = OrderInfo(lines, status, customer, territory, country, currency, taxRate, taxRegime, inclusive, reverseCharge,
                                                salesperson, channel, orderDate, money(netTotal))
            if channel.status == "PILOT":
                ctx.tag("PILOT_CHANNEL_ORDER", "Order placed on the PILOT channel (orderable, non-commissionable).", f"{SALES}.Orders", OrderID=orderId, SalesChannelID=channel.channelId)
            if reverseCharge:
                ctx.tag("EU_REVERSE_CHARGE", "EU cross-border B2B order: TaxRegimeCode = 'EU_RC', zero VAT on every line.", f"{SALES}.Orders", OrderID=orderId, CustomerID=customerId,
                        CustomerCountry=country.iso2)
            if onHold:
                self.addHold(orderId, orderDate, status, salesperson)
            if amendmentCount:
                self.addAmendment(orderId, orderDate, lines[0], salesperson, channel)
        self.addLostQuotes()

    def addDeletedOrder(self, orderId: int, customer: Row, territory: Territory, orderDate: date) -> None:
        deletedWhen = _ts(orderDate + timedelta(days=self.rng.randint(1, 5)), 16)
        self.tables["OrderDeletionLog"].append({
            "OrderDeletionLogID": self.nextId("OrderDeletionLog"), "OrderID": orderId, "CustomerID": customer["CustomerID"], "SalesTerritoryID": territory.territoryId,
            "OrderDate": orderDate, "OrderStatusAtDelete": "ENTERED", "OrderValueAtDelete": money(self.rng.randint(100, 5000)), "DeletedWhen": deletedWhen,
            "DeletedByLogin": "WWI\\salesadmin", "DeletedByApplication": "WWI Sales Desk", "LineCountAtDelete": self.rng.randint(1, 4), "IsCascadeFromCustomer": False,
            "ExtractedByConsumerList": "DW_SALES" if deletedWhen < self.watermark else None,
        })
        self.tables["DeletedRowLog"].append({
            "DeletedRowLogID": self.nextId("DeletedRowLog"), "SourceSchemaName": "Sales", "SourceTableName": "Orders", "SourceKeyValue": str(orderId), "SecondaryKeyValue": None,
            "DeletedWhen": deletedWhen, "DeletedByLogin": "WWI\\salesadmin", "DeletedByApplication": "WWI Sales Desk", "DeleteReasonCode": "DUPLICATE",
            "RowSnapshotText": f'{{"OrderID":{orderId},"CustomerID":{customer["CustomerID"]}}}', "IsPurgeNotDelete": False,
        })
        self.ctx.tag("DELETED_ORDER", "OrderID present in Sales.OrderDeletionLog / Integration.DeletedRowLog but absent from Sales.Orders.", f"{SALES}.OrderDeletionLog", OrderID=orderId)

    def addBackorder(self, orderId: int, line: Row, orderDate: date, status: str) -> None:
        raised = _ts(orderDate, 12)
        released = status in ("INVOICED", "SHIPPED")
        self.tables["Backorders"].append({
            "BackorderID": self.nextId("Backorders"), "OrderID": orderId, "OrderLineID": line["OrderLineID"], "StockItemID": line["StockItemID"], "QuantityShort": line["QuantityBackordered"],
            "QuantityReleased": line["QuantityBackordered"] if released else qty(0), "RaisedWhen": raised, "ShortageReasonCode": self.rng.choice(["NOSTOCK", "DAMAGED", "ALLOCFAIL"]),
            "PromisedDate": orderDate + timedelta(days=self.rng.randint(7, 28)), "PromiseSource": "PURCHASING", "RepromiseCount": self.rng.choice([0, 0, 1, 2]), "LinkedPurchaseOrderLineID": None,
            "CustomerNotifiedWhen": raised + timedelta(hours=2), "BackorderStatus": "RELEASED" if released else "OPEN", "ClosedWhen": raised + timedelta(days=9) if released else None,
            "LastEditedWhen": raised + timedelta(days=9) if released else raised,
        })

    def addHold(self, orderId: int, orderDate: date, status: str, salesperson: int) -> None:
        placed = _ts(orderDate, 10, 6)
        active = status == "HOLD"
        holdType = self.rng.choice(["CREDIT", "CREDIT", "STOCK", "FRAUD", "PRICE"])
        self.tables["OrderHolds"].append({
            "OrderHoldID": self.nextId("OrderHolds"), "OrderID": orderId, "HoldTypeCode": holdType, "HoldReasonCode": {"CREDIT": "LIMIT", "STOCK": "NOSTOCK", "FRAUD": "SCREEN", "PRICE": "APPROVAL"}[holdType],
            "HoldNarrative": None, "PlacedWhen": placed, "PlacedByPersonID": None if holdType == "CREDIT" else salesperson, "PlacedBySystem": "CreditEngine" if holdType == "CREDIT" else None,
            "ReleasedWhen": None if active else placed + timedelta(hours=self.rng.randint(1, 48)), "ReleasedByPersonID": None if active else self.people.accounts[0],
            "ReleaseNarrative": None if active else "Released after review", "AutoReleaseAfterWhen": placed + timedelta(days=5) if holdType == "CREDIT" else None,
            "EscalationLevel": 1 if active else 0, "IsBlockingDespatch": True,
        })

    def addAmendment(self, orderId: int, orderDate: date, line: Row, salesperson: int, channel: Channel) -> None:
        self.tables["OrderAmendments"].append({
            "OrderAmendmentID": self.nextId("OrderAmendments"), "OrderID": orderId, "AmendmentSequence": 1, "AmendedWhen": _ts(orderDate, 13), "AmendedByPersonID": salesperson,
            "AmendmentTypeCode": "QUANTITY", "TargetTableName": "Sales.OrderLines", "TargetKeyValue": str(line["OrderLineID"]), "ChangedColumnName": "Quantity",
            "OldValueText": str(line["Quantity"] + 1), "NewValueText": str(line["Quantity"]), "ReasonCode": "CUSTREQ", "ReasonNarrative": None, "RequiresCustomerApproval": False,
            "CustomerApprovedWhen": None, "SourceApplication": "WWI Sales Desk" if channel.channelClass != "WEB" else "WebStore",
        })

    def addConvertedQuote(self, orderId: int, customer: Row, salesperson: int, channel: Channel, priceListId: int, currency: str, orderDate: date, lines: list[Row], taxRate: Decimal) -> int:
        quoteDate = orderDate - timedelta(days=self.rng.randint(2, 20))
        quoteId = self.nextId("QuoteHeaders")
        self.tables["QuoteHeaders"].append({
            "QuoteID": quoteId, "QuoteReference": f"Q{quoteId:07d}", "CustomerID": customer["CustomerID"], "ContactPersonID": customer["PrimaryContactPersonID"], "SalespersonPersonID": salesperson,
            "SalesChannelID": channel.channelId, "PriceListID": priceListId, "QuoteDate": quoteDate, "ValidUntilDate": quoteDate + timedelta(days=30), "CurrencyCode": currency,
            "ExchangeRateToUSD": self.fxRate(currency, quoteDate), "TaxTreatment": self.priceLists[priceListId]["TaxTreatment"], "QuoteStatus": "CONVERTED", "RevisionNumber": 1,
            "SupersedesQuoteID": None, "ConvertedOrderID": orderId, "ConvertedWhen": _ts(orderDate, 9, 30), "LostReasonCode": None, "Comments": None, **_edited(salesperson, _ts(orderDate, 9, 30)),
        })
        for number, line in enumerate(lines, start=1):
            self.tables["QuoteLines"].append({
                "QuoteLineID": self.nextId("QuoteLines"), "QuoteID": quoteId, "LineNumber": number, "StockItemID": line["StockItemID"], "DescriptionSnapshot": line["Description"],
                "Quantity": qty(line["Quantity"]), "UnitPrice": line["UnitPrice"], "DiscountPercent": line["DiscountPercent"], "TaxRatePercent": money(taxRate), "PromisedLeadTimeDays": 7,
                "IsOptionalLine": False, "LineStatus": "CONVERTED", **_edited(salesperson, _ts(quoteDate, 9)),
            })
        return quoteId

    def addLostQuotes(self) -> None:
        rng = self.rng
        customerIds = [c for c in self.customers if c not in self.lateArriving]
        for _ in range(max(10, self.ctx.params["orders"] // 60)):
            customer = self.customers[rng.choice(customerIds)]
            quoteDate = self.ctx.randomDate(rng)
            priceListId = customer["DefaultPriceListID"]
            currency = self.priceLists[priceListId]["CurrencyCode"]
            expired = quoteDate + timedelta(days=30) < self.ctx.asOf
            status = rng.choice(["LOST", "EXPIRED"]) if expired else rng.choice(["DRAFT", "SENT"])
            salesperson = self.salespersonFor(customer["CustomerID"])
            quoteId = self.nextId("QuoteHeaders")
            self.tables["QuoteHeaders"].append({
                "QuoteID": quoteId, "QuoteReference": f"Q{quoteId:07d}", "CustomerID": customer["CustomerID"], "ContactPersonID": customer["PrimaryContactPersonID"], "SalespersonPersonID": salesperson,
                "SalesChannelID": ACTIVE_CHANNELS_BY_REGION[customer["RegionCode"]][0].channelId, "PriceListID": priceListId, "QuoteDate": quoteDate, "ValidUntilDate": quoteDate + timedelta(days=30),
                "CurrencyCode": currency, "ExchangeRateToUSD": self.fxRate(currency, quoteDate), "TaxTreatment": self.priceLists[priceListId]["TaxTreatment"], "QuoteStatus": status,
                "RevisionNumber": rng.choice([1, 1, 2]), "SupersedesQuoteID": None, "ConvertedOrderID": None, "ConvertedWhen": None,
                "LostReasonCode": rng.choice(["PRICE", "COMPETITOR", "NORESPONSE"]) if status == "LOST" else None, "Comments": None, **_edited(salesperson, _ts(quoteDate, 9)),
            })
            for number, stockItemId in enumerate(rng.sample(self.itemIds, rng.randint(1, 3)), start=1):
                priceLine = self.priceByListAndItem[(priceListId, stockItemId)]
                self.tables["QuoteLines"].append({
                    "QuoteLineID": self.nextId("QuoteLines"), "QuoteID": quoteId, "LineNumber": number, "StockItemID": stockItemId, "DescriptionSnapshot": self.items[stockItemId]["StockItemName"],
                    "Quantity": qty(rng.choice([1, 5, 10])), "UnitPrice": priceLine["UnitPrice"], "DiscountPercent": qty(0), "TaxRatePercent": money(STANDARD_TAX_RATE[self.customerCountry[customer["CustomerID"]]]),
                    "PromisedLeadTimeDays": 7, "IsOptionalLine": number == 3, "LineStatus": "DECLINED" if status == "LOST" else "OPEN", **_edited(salesperson, _ts(quoteDate, 9)),
                })

    # --- invoices & shipments -------------------------------------------------------
    def buildInvoicesAndShipments(self) -> None:
        ctx, rng = self.ctx, self.rng
        rcNullCounter = 0
        for order in self.tables["Orders"]:
            info = self.orderInfo[order["OrderID"]]
            status = info.status
            if status in ("INVOICED", "SHIPPED"):
                self.addShipment(order, info)
            if status != "INVOICED":
                continue
            customer = info.customer
            orderDate = info.orderDate
            invoiceDate = orderDate + timedelta(days=rng.randint(1, 4))
            invoiceId = self.nextId("Invoices")
            currency = info.currency
            taxRate = info.taxRate
            totalExTax = ZERO
            totalTax = ZERO
            dry = chiller = 0
            for line in info.lines:
                quantity = line["PickedQuantity"] or line["Quantity"]
                unitPrice = line["UnitPrice"]
                gross = money(unitPrice * quantity)
                exTax = money(gross / (1 + taxRate / 100)) if info.inclusive and taxRate else gross
                exTax = money(exTax - line["DiscountAmount"])
                taxAmount = money(exTax * taxRate / 100)
                cost = self.costByItem[line["StockItemID"]]
                # LEGACY QUIRK: WWI LineProfit is computed against the USD LastCostPrice without converting
                # the sale currency, exactly as the OLTP trigger did (Fact.Sales Margin recomputes it).
                lineProfit = money(exTax - cost * quantity)
                self.tables["InvoiceLines"].append({
                    "InvoiceLineID": self.nextId("InvoiceLines"), "InvoiceID": invoiceId, "StockItemID": line["StockItemID"], "Description": line["Description"], "PackageTypeID": line["PackageTypeID"],
                    "Quantity": quantity, "UnitPrice": unitPrice, "TaxRate": qty(taxRate), "TaxAmount": taxAmount, "LineProfit": lineProfit, "ExtendedPrice": money(exTax + taxAmount),
                    **_edited(self.people.accounts[0], _ts(invoiceDate, 17)),
                })
                totalExTax += exTax
                totalTax += taxAmount
                if self.items[line["StockItemID"]]["IsChillerStock"]:
                    chiller += 1
                else:
                    dry += 1
            customerTaxNumber = customer["TaxRegistrationNumber"]
            if info.reverseCharge:
                rcNullCounter += 1
                if rcNullCounter % 9 == 0:
                    customerTaxNumber = None
                    ctx.tag("EU_REVERSE_CHARGE_NULL_VAT", "Reverse-charge invoice (TaxRegimeCode = 'EU_RC') whose CustomerTaxNumber is NULL.", f"{SALES}.Invoices", InvoiceID=invoiceId, OrderID=order["OrderID"])
            dueDate = invoiceDate + timedelta(days=customer["PaymentDays"])
            invoice = {
                "InvoiceID": invoiceId, "CustomerID": customer["CustomerID"], "BillToCustomerID": customer["BillToCustomerID"], "OrderID": order["OrderID"], "DeliveryMethodID": customer["DeliveryMethodID"],
                "ContactPersonID": customer["PrimaryContactPersonID"], "AccountsPersonID": self.people.accounts[0], "SalespersonPersonID": info.salesperson, "PackedByPersonID": order["PickedByPersonID"],
                "InvoiceDate": invoiceDate, "CustomerPurchaseOrderNumber": order["CustomerPurchaseOrderNumber"], "IsCreditNote": False, "CreditNoteReason": None, "Comments": None,
                "DeliveryInstructions": None, "InternalComments": None, "TotalDryItems": dry, "TotalChillerItems": chiller, "DeliveryRun": None, "RunPosition": None,
                "ReturnedDeliveryData": None, **_edited(self.people.accounts[0], _ts(invoiceDate, 17)), "SalesTerritoryID": order["SalesTerritoryID"], "TaxRegimeCode": info.taxRegime,
                "CustomerTaxNumber": customerTaxNumber, "TaxPointDate": invoiceDate, "CurrencyCode": currency, "ExchangeRateToUsd": self.fxRate(currency, invoiceDate),
                "InvoiceTotalExTax": money(totalExTax), "InvoiceTaxAmount": money(totalTax), "AmountOutstanding": money(totalExTax + totalTax), "SettlementStatus": "OPEN",
                "PaymentDueDate": dueDate, "DisputeFlag": False, "LoyaltyMemberID": None, "LoyaltyPointsAccrued": None,
            }
            self.tables["Invoices"].append(invoice)
            self.invoiceInfo[invoiceId] = InvoiceInfo(money(totalExTax + totalTax), customer, currency, dueDate, invoiceDate, order)
            self.tables["CustomerTransactions"].append({
                "CustomerTransactionID": self.nextId("CustomerTransactions"), "CustomerID": customer["CustomerID"], "TransactionTypeID": 1, "InvoiceID": invoiceId, "PaymentMethodID": None,
                "TransactionDate": invoiceDate, "AmountExcludingTax": money(totalExTax), "TaxAmount": money(totalTax), "TransactionAmount": money(totalExTax + totalTax),
                "OutstandingBalance": money(totalExTax + totalTax), "FinalizationDate": None, **_edited(self.people.accounts[0], _ts(invoiceDate, 17)),
            })
            if customer["CustomerID"] in self.lateArriving:
                ctx.tag("LATE_ARRIVING_CUSTOMER", "Customer whose row arrives in a later extract than its invoices (ValidFrom after the extract watermark).", f"{SALES}.Invoices",
                        CustomerID=customer["CustomerID"], InvoiceID=invoiceId, InvoiceDate=invoiceDate)

    def addShipment(self, order: Row, info: OrderInfo) -> None:
        rng = self.rng
        orderDate = info.orderDate
        shippedLines = [line for line in info.lines if line["PickedQuantity"]]
        if not shippedLines:
            return
        splits = [shippedLines] if len(shippedLines) < 3 or rng.random() > 0.15 else [shippedLines[:1], shippedLines[1:]]
        region = info.territory.region
        for splitIndex, lines in enumerate(splits, start=1):
            shipmentId = self.nextId("ShipmentHeaders")
            despatched = _ts(orderDate + timedelta(days=1 + splitIndex), 16)
            delivered = despatched + timedelta(days=rng.randint(1, 6)) if info.status == "INVOICED" else None
            weight = sum((Decimal(self.items[line["StockItemID"]]["TypicalWeightPerUnit"]) * line["PickedQuantity"] for line in lines), Decimal(0))
            self.tables["ShipmentHeaders"].append({
                "ShipmentID": shipmentId, "ShipmentReference": f"SHP{shipmentId:08d}", "OrderID": order["OrderID"], "InvoiceID": None, "CustomerID": order["CustomerID"],
                "WarehouseSiteID": {"NA": 1, "EU": 2, "APAC": 3}[region], "CarrierID": rng.randint(1, 5), "ServiceLevelCode": rng.choice(["STD", "EXP", "ECO"]), "DeliveryRouteID": None,
                "WaveReference": f"WAVE-{orderDate:%Y%m%d}-{rng.randint(1, 9)}", "SplitSequence": splitIndex, "IsFinalShipment": splitIndex == len(splits), "PlannedDespatchDate": despatched.date(),
                "PickStartedWhen": despatched - timedelta(hours=6), "PackCompletedWhen": despatched - timedelta(hours=1), "DespatchedWhen": despatched, "PromisedDeliveryWhen": _ts(order["ExpectedDeliveryDate"], 17),
                "DeliveredWhen": delivered, "TrackingNumber": f"TRK{rng.randint(10**9, 10**10 - 1)}", "TotalPackages": len(lines), "TotalGrossWeightKg": qty(weight + Decimal("0.5") * len(lines)),
                "TotalVolumeM3": None, "ChargeableWeightKg": qty(weight + Decimal("0.5") * len(lines)), "FreightChargeAmount": money(Decimal(rng.randint(5, 80))), "FreightCurrencyCode": info.currency,
                "FreightRatedWhen": despatched, "IncotermCode": "DAP" if region == "EU" else None, "ShipmentStatus": "DELIVERED" if delivered else "INTRANSIT", "ExceptionCode": None,
                "DeliveryInstructions": None, "PackedByPersonID": order["PickedByPersonID"], **_edited(order["PickedByPersonID"] or SYSTEM_USER, delivered or despatched),
            })
            for number, line in enumerate(lines, start=1):
                self.tables["ShipmentLines"].append({
                    "ShipmentLineID": self.nextId("ShipmentLines"), "ShipmentID": shipmentId, "LineNumber": number, "OrderLineID": line["OrderLineID"], "StockItemID": line["StockItemID"],
                    "LotNumber": None, "PickedFromBinID": rng.randint(1, 400), "QuantityShipped": qty(line["PickedQuantity"]), "PackageNumber": number, "PackagingTypeID": 1,
                    "PackageGrossWeightKg": qty(Decimal(self.items[line["StockItemID"]]["TypicalWeightPerUnit"]) * line["PickedQuantity"] + Decimal("0.5")), "SerialNumberList": None,
                    "PickedByPersonID": order["PickedByPersonID"], "PickedWhen": despatched - timedelta(hours=5), "LineStatus": "PACKED", "LastEditedWhen": despatched,
                })

    # --- payments, disputes, write-offs --------------------------------------------
    def buildPayments(self) -> None:
        ctx, rng = self.ctx, self.rng
        settleBy = ctx.asOf - timedelta(days=3)
        byCustomer: dict[int, list[int]] = defaultdict(list)
        for invoiceId, info in self.invoiceInfo.items():
            if info.dueDate <= settleBy and info.customer["CustomerID"] not in self.lateArriving:
                byCustomer[info.customer["CustomerID"]].append(invoiceId)
        multiTarget = max(4, ctx.params["orders"] // 500)
        multiDone = 0
        for customerId in sorted(byCustomer):
            invoiceIds = byCustomer[customerId]
            handled: set[int] = set()
            if len(invoiceIds) >= 3 and multiDone < multiTarget:
                group = invoiceIds[:3]
                self.addPayment(customerId, group, sum((self.invoiceInfo[i].total for i in group), ZERO), "MULTI_INVOICE_PAYMENT")
                handled.update(group)
                multiDone += 1
            for invoiceId in invoiceIds:
                if invoiceId in handled:
                    continue
                total = self.invoiceInfo[invoiceId].total
                roll = rng.random()
                if roll < 0.70:
                    self.addPayment(customerId, [invoiceId], total, None)
                elif roll < 0.78:
                    self.addPayment(customerId, [invoiceId], money(total * Decimal(rng.choice(["0.5", "0.8", "0.95"]))), "UNDERPAYMENT")
                elif roll < 0.81:
                    self.addPayment(customerId, [invoiceId], money(total + Decimal(rng.choice(["0.01", "10.00", "100.00"]))), "OVERPAYMENT")
                elif roll < 0.84:
                    self.addDispute(invoiceId)
                elif roll < 0.86:
                    self.addWriteOff(invoiceId, None, "BADDEBT", total)
        onAccountCustomers = rng.sample(sorted(byCustomer), min(5, len(byCustomer)))
        for customerId in onAccountCustomers:
            self.addPayment(customerId, [], money(Decimal(rng.randint(100, 2000))), "ON_ACCOUNT_PAYMENT")

    def addPayment(self, customerId: int, invoiceIds: list[int], received: Decimal, edgeCode: str | None) -> None:
        ctx, rng = self.ctx, self.rng
        customer = self.customers[customerId]
        region = customer["RegionCode"]
        currency = self.priceLists[customer["DefaultPriceListID"]]["CurrencyCode"]
        if invoiceIds:
            receivedDay = max(self.invoiceInfo[i].dueDate for i in invoiceIds) + timedelta(days=rng.randint(-5, 20))
        else:
            receivedDay = ctx.randomDate(rng, ctx.spanStart + timedelta(days=30))
        receivedDay = min(receivedDay, ctx.asOf)
        paymentId = self.nextId("CustomerPayments")
        methodCode = rng.choice(REGION_PAYMENT_METHODS[region])
        allocated = ZERO
        allocations: list[Row] = []
        remaining = received
        for invoiceId in invoiceIds:
            info = self.invoiceInfo[invoiceId]
            amount = min(info.total, remaining)
            remaining = money(remaining - amount)
            allocated += amount
            allocations.append({
                "PaymentAllocationID": self.nextId("PaymentAllocations") + len(allocations), "CustomerPaymentID": paymentId, "AllocatedWhen": _ts(receivedDay, 15), "TargetTypeCode": "INVOICE",
                "InvoiceID": invoiceId, "CreditNoteID": None, "AllocatedAmount": money(amount), "SettlementDiscount": ZERO, "ExchangeDifference": ZERO,
                "MatchMethodCode": "AUTOREF" if len(invoiceIds) == 1 else "MANUAL", "MatchConfidence": 100 if len(invoiceIds) == 1 else 85, "ReversalOfAllocationID": None,
                "AllocatedByPersonID": self.people.accounts[1],
            })
            self.settleInvoice(invoiceId, money(amount), receivedDay)
        self.tables["PaymentAllocations"].extend(allocations)
        status = "UNAPPLIED" if not invoiceIds else ("ALLOCATED" if remaining == ZERO else "PARTALLOCATED")
        self.tables["CustomerPayments"].append({
            "CustomerPaymentID": paymentId, "PaymentReference": f"PAY{paymentId:08d}", "CustomerID": customerId, "ReceivedWhen": _ts(receivedDay, 9), "ValueDate": receivedDay,
            "PaymentMethodCode": methodCode, "CurrencyCode": currency, "ReceivedAmount": money(received), "ExchangeRateToUsd": self.fxRate(currency, receivedDay),
            "BankChargeAmount": money("12.50") if methodCode == "BANKXFER" else ZERO, "AllocatedAmount": money(allocated), "BankAccountCode": f"BANK-{region}-01",
            "BankStatementRef": f"STMT{receivedDay:%Y%m%d}", "CardLastFourDigits": f"{rng.randint(1000, 9999)}" if methodCode == "CARD" else None,
            "AcquirerReference": None, "PaymentStatus": status, "ReversalReasonCode": None, "ReversedWhen": None, "PostedByPersonID": self.people.accounts[1],
            "SourceInterfaceCode": {"BANKXFER": "BANKFILE", "DIRECTDEBIT": "BANKFILE", "CARD": "ACQUIRER", "CHEQUE": "MANUAL", "CASH": "MANUAL", "BILLOFEXCH": "MANUAL", "OFFSET": "MANUAL"}[methodCode],
            "LastEditedWhen": _ts(receivedDay, 15),
        })
        self.tables["CustomerTransactions"].append({
            "CustomerTransactionID": self.nextId("CustomerTransactions"), "CustomerID": customerId, "TransactionTypeID": 3, "InvoiceID": invoiceIds[0] if len(invoiceIds) == 1 else None,
            "PaymentMethodID": PAYMENT_METHOD_TO_WWI_ID[methodCode], "TransactionDate": receivedDay, "AmountExcludingTax": money(-received), "TaxAmount": ZERO, "TransactionAmount": money(-received),
            "OutstandingBalance": money(-remaining), "FinalizationDate": receivedDay if remaining == ZERO else None, **_edited(self.people.accounts[1], _ts(receivedDay, 15)),
        })
        if edgeCode:
            descriptions = {
                "MULTI_INVOICE_PAYMENT": "One CustomerPayments row allocated across several invoices.",
                "UNDERPAYMENT": "Payment smaller than the invoice total; invoice stays PARTPAID.",
                "OVERPAYMENT": "Payment larger than the invoice total; the surplus stays unallocated on the payment.",
                "ON_ACCOUNT_PAYMENT": "Payment with no PaymentAllocations rows (unapplied cash on account).",
            }
            ctx.tag(edgeCode, descriptions[edgeCode], f"{SALES}.CustomerPayments", CustomerPaymentID=paymentId, CustomerID=customerId,
                    InvoiceIDs=",".join(str(i) for i in invoiceIds), ReceivedAmount=received)
        if methodCode == "BILLOFEXCH":
            ctx.tag("UNTRANSLATED_PAYMENT_METHOD_USED", "Payment using PaymentMethodCode BILLOFEXCH, which has no CODE_TRANSLATION / PAYMENT_METHOD_REF row.", f"{SALES}.CustomerPayments",
                    CustomerPaymentID=paymentId, PaymentMethodCode=methodCode)

    def settleInvoice(self, invoiceId: int, amount: Decimal, when: date) -> None:
        invoice = self.tables["Invoices"][invoiceId - 1]
        invoice["AmountOutstanding"] = money(invoice["AmountOutstanding"] - amount)
        invoice["SettlementStatus"] = "PAID" if invoice["AmountOutstanding"] <= ZERO else "PARTPAID"
        invoice["LastEditedWhen"] = max(invoice["LastEditedWhen"], _ts(when, 15))
        for txn in self.tables["CustomerTransactions"]:
            if txn["InvoiceID"] == invoiceId and txn["TransactionTypeID"] == 1:
                txn["OutstandingBalance"] = invoice["AmountOutstanding"]
                if invoice["AmountOutstanding"] <= ZERO:
                    txn["FinalizationDate"] = when
                break

    def addDispute(self, invoiceId: int) -> None:
        rng = self.rng
        info = self.invoiceInfo[invoiceId]
        invoice = self.tables["Invoices"][invoiceId - 1]
        raised = _ts(info.dueDate - timedelta(days=rng.randint(1, 10)), 11)
        resolved = rng.random() < 0.5
        disputeId = self.nextId("CustomerDisputes")
        disputed = money(info.total * Decimal(rng.choice(["0.1", "0.25", "1.0"])))
        writeOff = money(disputed * Decimal("0.5")) if resolved else None
        self.tables["CustomerDisputes"].append({
            "CustomerDisputeID": disputeId, "DisputeReference": f"DSP{disputeId:06d}", "CustomerID": info.customer["CustomerID"], "InvoiceID": invoiceId, "RaisedWhen": raised,
            "RaisedByPersonID": self.people.accounts[2], "RaisedChannel": rng.choice(["EMAIL", "PHONE", "PORTAL"]), "DisputeCategoryCode": rng.choice(["PRICE", "QUANTITY", "QUALITY", "DELIVERY", "TAX"]),
            "DisputedAmount": disputed, "CurrencyCode": info.currency, "DisputeNarrative": "Customer disputes invoiced amount", "OwnerPersonID": self.people.accounts[2],
            "TargetResolutionDate": raised.date() + timedelta(days=30), "DisputeStatus": "RESOLVED" if resolved else rng.choice(["OPEN", "INVESTIGATING", "AWAITINGCUST"]),
            "ResolutionCode": "PARTIALCREDIT" if resolved else None, "ResolvedWhen": raised + timedelta(days=rng.randint(5, 25)) if resolved else None, "CreditNoteID": None,
            "WriteOffAmount": writeOff, "EscalationLevel": 0 if resolved else rng.choice([0, 1]), "LastEditedWhen": raised + timedelta(days=rng.randint(5, 25)) if resolved else raised,
        })
        invoice["DisputeFlag"] = True
        invoice["SettlementStatus"] = "DISPUTED"
        if resolved and writeOff:
            self.addWriteOff(invoiceId, disputeId, "DISPUTE", writeOff)

    def addWriteOff(self, invoiceId: int, disputeId: int | None, writeOffType: str, amount: Decimal) -> None:
        rng = self.rng
        info = self.invoiceInfo[invoiceId]
        invoice = self.tables["Invoices"][invoiceId - 1]
        writeOffDate = min(info.dueDate + timedelta(days=rng.randint(30, 90)), self.ctx.asOf)
        self.tables["CustomerWriteOffs"].append({
            "CustomerWriteOffID": self.nextId("CustomerWriteOffs"), "CustomerID": info.customer["CustomerID"], "InvoiceID": invoiceId, "CustomerDisputeID": disputeId, "WriteOffTypeCode": writeOffType,
            "WriteOffAmount": money(amount), "CurrencyCode": info.currency, "WriteOffDate": writeOffDate, "ReasonCode": writeOffType, "ReasonNarrative": None,
            "ApprovalThresholdUsed": money("500.00") if amount <= 500 else money("5000.00"), "ApprovedByPersonID": self.people.managers[info.customer["RegionCode"]],
            "ApprovedWhen": _ts(writeOffDate, 12), "GeneralLedgerCode": "6410-BADDEBT" if writeOffType == "BADDEBT" else "6420-DISPUTE", "PostedToLedgerWhen": _ts(writeOffDate, 18),
            "IsRecovered": False, "RecoveredAmount": None, "RecoveredWhen": None,
        })
        invoice["AmountOutstanding"] = money(invoice["AmountOutstanding"] - amount)
        invoice["SettlementStatus"] = "WRITTENOFF" if invoice["AmountOutstanding"] <= ZERO else invoice["SettlementStatus"]
        invoice["LastEditedWhen"] = max(invoice["LastEditedWhen"], _ts(writeOffDate, 18))

    # --- returns / credit notes ------------------------------------------------------
    def buildReturns(self) -> None:
        ctx, rng = self.ctx, self.rng
        invoiceLinesByInvoice: dict[int, list[Row]] = defaultdict(list)
        for line in self.tables["InvoiceLines"]:
            invoiceLinesByInvoice[line["InvoiceID"]].append(line)
        reasonsByRegion: dict[str, list[Row]] = defaultdict(list)
        for reason in self.returnReasons:
            reasonsByRegion[reason["RegionCode"]].append(reason)
        candidates = [i for i, info in self.invoiceInfo.items() if info.invoiceDate <= ctx.asOf - timedelta(days=10) and info.customer["CustomerID"] not in self.lateArriving]
        chosen = rng.sample(candidates, max(12, len(candidates) * 3 // 100))
        forced = {UNTRANSLATED_RETURN_REASON: 2, STALE_RETURN_REASON: 2}
        for _index, invoiceId in enumerate(chosen):
            info = self.invoiceInfo[invoiceId]
            customer = info.customer
            region = customer["RegionCode"]
            reason = rng.choice(reasonsByRegion[region])
            for (code, reasonRegion), needed in forced.items():
                if needed and reasonRegion == region:
                    reason = next(r for r in reasonsByRegion[region] if r["ReasonCode"] == code)
                    forced[(code, reasonRegion)] = needed - 1
                    break
            requested = _ts(info.invoiceDate + timedelta(days=rng.randint(2, 20)), 10)
            received = rng.random() < 0.7
            rmaId = self.nextId("ReturnAuthorizations")
            lines = invoiceLinesByInvoice[invoiceId][:1]
            expectedCredit = ZERO
            returnLines: list[Row] = []
            for number, invLine in enumerate(lines, start=1):
                quantity = max(1, invLine["Quantity"] // 2)
                unitCredit = invLine["UnitPrice"]
                expectedCredit += money(unitCredit * quantity)
                returnLines.append({
                    "ReturnLineID": self.nextId("ReturnLines") + len(returnLines), "ReturnAuthorizationID": rmaId, "LineNumber": number, "OriginalInvoiceLineID": invLine["InvoiceLineID"],
                    "StockItemID": invLine["StockItemID"], "ReturnReasonID": reason["ReturnReasonID"], "LotNumber": None, "QuantityAuthorized": qty(quantity),
                    "QuantityReceived": qty(quantity) if received else qty(0), "QuantityAccepted": qty(quantity) if received else qty(0), "QuantityScrapped": qty(0),
                    "UnitPriceAtSale": invLine["UnitPrice"], "TaxRatePercentAtSale": money(invLine["TaxRate"]), "RestockingPercent": reason["RestockingPercent"],
                    "DispositionCode": ("RESTOCK" if reason["AllowsResale"] else "SCRAP") if received else None, "LineStatus": "CREDITED" if received else "AUTHORIZED",
                    **_edited(self.people.accounts[2], requested + (timedelta(days=8) if received else timedelta())),
                })
            self.tables["ReturnLines"].extend(returnLines)
            self.tables["ReturnAuthorizations"].append({
                "ReturnAuthorizationID": rmaId, "RmaNumber": f"RMA-{region}-{rmaId:06d}", "CustomerID": customer["CustomerID"], "OriginalInvoiceID": invoiceId, "RegionCode": region,
                "RequestedWhen": requested, "RequestChannel": rng.choice(["WEB", "CALLCENTRE", "REP"]), "AuthorizationStatus": "CLOSED" if received else "APPROVED",
                "AuthorizedWhen": requested + timedelta(hours=4), "AuthorizedByPersonID": self.people.accounts[2], "ExpiresOnDate": requested.date() + timedelta(days=30),
                "ReturnCarrierID": rng.randint(1, 5), "ReturnTrackingNumber": f"RTN{rng.randint(10**8, 10**9 - 1)}" if received else None, "ReceivedAtSiteID": {"NA": 1, "EU": 2, "APAC": 3}[region] if received else None,
                "GoodsReceivedWhen": requested + timedelta(days=6) if received else None, "IsCoolingOffPeriod": reason["ReasonCode"] == "COOL", "CustomerNarrative": None, "InternalNote": None,
                "DeclineReason": None, "TotalExpectedCredit": money(expectedCredit), "CreditCurrencyCode": info.currency, **_edited(self.people.accounts[2], requested + timedelta(days=8 if received else 0)),
            })
            if (reason["ReasonCode"], region) == UNTRANSLATED_RETURN_REASON:
                ctx.tag("UNTRANSLATED_RETURN_REASON_USED", "Return line using a ReasonCode with no CODE_TRANSLATION row.", f"{RET}.ReturnLines", ReturnAuthorizationID=rmaId, ReasonCode=reason["ReasonCode"])
            if (reason["ReasonCode"], region) == STALE_RETURN_REASON:
                ctx.tag("STALE_RETURN_REASON_USED", "Return line using a ReasonCode whose CODE_TRANSLATION row is stale.", f"{RET}.ReturnLines", ReturnAuthorizationID=rmaId, ReasonCode=reason["ReasonCode"])
            if received:
                self.addCreditNote(rmaId, invoiceId, info, returnLines, reason, requested + timedelta(days=8))

    def addCreditNote(self, rmaId: int, invoiceId: int, info: InvoiceInfo, returnLines: list[Row], reason: Row, issued: datetime) -> None:
        customer = info.customer
        region = customer["RegionCode"]
        invoice = self.tables["Invoices"][invoiceId - 1]
        creditNoteId = self.nextId("CreditNotes")
        net = ZERO
        tax = ZERO
        restock = ZERO
        lines: list[Row] = []
        for number, rl in enumerate(returnLines, start=1):
            quantity = rl["QuantityAccepted"]
            unitCredit = rl["UnitPriceAtSale"]
            taxRate = rl["TaxRatePercentAtSale"]
            lineNet = money(unitCredit * quantity / (1 + taxRate / 100)) if invoice["TaxRegimeCode"] in ("AUGST", "SGGST", "JPCT") else money(unitCredit * quantity)
            net += lineNet
            tax += money(lineNet * taxRate / 100)
            lines.append({
                "CreditNoteLineID": self.nextId("CreditNoteLines") + len(lines), "CreditNoteID": creditNoteId, "LineNumber": number, "CreditLineType": "GOODS", "ReturnLineID": rl["ReturnLineID"],
                "StockItemID": rl["StockItemID"], "Description": self.items[rl["StockItemID"]]["StockItemName"], "Quantity": quantity, "UnitCreditAmount": unitCredit, "TaxRatePercent": taxRate,
                "GeneralLedgerCode": "4100-RETURNS", "LastEditedWhen": issued,
            })
            if reason["RestockingPercent"]:
                fee = money(lineNet * reason["RestockingPercent"] / 100)
                restock += fee
                lines.append({
                    "CreditNoteLineID": self.nextId("CreditNoteLines") + len(lines), "CreditNoteID": creditNoteId, "LineNumber": number + 50, "CreditLineType": "RESTOCKFEE", "ReturnLineID": rl["ReturnLineID"],
                    "StockItemID": None, "Description": f"Restocking fee {reason['RestockingPercent']}%", "Quantity": qty(1), "UnitCreditAmount": money(-fee), "TaxRatePercent": taxRate,
                    "GeneralLedgerCode": "4150-RESTOCK", "LastEditedWhen": issued,
                })
        self.tables["CreditNoteLines"].extend(lines)
        self.tables["CreditNotes"].append({
            "CreditNoteID": creditNoteId, "CreditNoteNumber": f"CN-{region}-{creditNoteId:06d}", "NumberSeriesCode": f"CN{region}", "NumberWithinSeries": creditNoteId, "CustomerID": customer["CustomerID"],
            "ReturnAuthorizationID": rmaId, "OriginalInvoiceID": invoiceId, "RegionCode": region, "CreditReasonCode": reason["ReasonCode"], "IssuedDate": issued.date(), "TaxPointDate": issued.date(),
            "TaxRegimeCode": creditNoteTaxRegime(invoice["TaxRegimeCode"], invoice["CustomerTaxNumber"]), "CustomerTaxNumber": invoice["CustomerTaxNumber"], "CurrencyCode": info.currency, "ExchangeRateToUsd": self.fxRate(info.currency, issued.date()),
            "NetAmount": money(net - restock), "TaxAmount": money(tax), "RestockingFeeAmount": money(restock), "AppliedAmount": ZERO, "CreditNoteStatus": "ISSUED", "IsRefundToCard": False,
            "RefundReference": None, "PostedToLedgerWhen": issued + timedelta(hours=6), **_edited(self.people.accounts[2], issued),
        })
        total = money(net - restock + tax)
        self.tables["CustomerTransactions"].append({
            "CustomerTransactionID": self.nextId("CustomerTransactions"), "CustomerID": customer["CustomerID"], "TransactionTypeID": 2, "InvoiceID": invoiceId, "PaymentMethodID": None,
            "TransactionDate": issued.date(), "AmountExcludingTax": money(-(net - restock)), "TaxAmount": money(-tax), "TransactionAmount": money(-total), "OutstandingBalance": ZERO,
            "FinalizationDate": issued.date(), **_edited(self.people.accounts[2], issued),
        })

    # --- ETL control ----------------------------------------------------------------
    def buildControl(self) -> None:
        rows: list[Row] = []
        for (system, schema, table), columns in TABLE_COLUMNS.items():
            if system != SQLSERVER or schema == INTEGRATION:
                continue
            tableRows = self.tables.get(table) if table in self.tables else self.ctx.tables.get((system, schema, table), [])
            watermarkColumn = "LastEditedWhen" if "LastEditedWhen" in columns else ("ValidFrom" if "ValidFrom" in columns else columns[0])
            rows.append({
                "ChangeTrackingWatermarkID": len(rows) + 1, "ConsumerCode": "DW_SALES", "SourceSchemaName": schema, "SourceTableName": table, "WatermarkColumnName": watermarkColumn,
                "LastExtractedWhen": self.watermark, "LastExtractedVersion": 900_000 + len(rows) * 17, "LastExtractedKeyValue": str(len(tableRows or [])), "OverlapMinutes": 30,
                "FullReloadRequested": False, "LastRunBatchID": 20_000 + self.ctx.seed % 1000, "LastRowCount": len(tableRows or []), "LastUpdatedWhen": self.watermark + timedelta(minutes=25),
                "UpdatedByProcess": "SSIS:WWI_Daily_Sales_Extract",
            })
        self.tables["ChangeTrackingWatermark"] = rows

    def store(self) -> None:
        schemaFor = {"OrderDeletionLog": SALES, "DeletedRowLog": INTEGRATION, "ChangeTrackingWatermark": INTEGRATION, "ShipmentHeaders": SHIPPING, "ShipmentLines": SHIPPING,
                     "ReturnAuthorizations": RET, "ReturnLines": RET, "CreditNotes": RET, "CreditNoteLines": RET}
        for table in ["QuoteHeaders", "QuoteLines", "Orders", "OrderLines", "OrderAmendments", "OrderHolds", "Backorders", "Invoices", "InvoiceLines", "CustomerTransactions",
                      "CustomerPayments", "PaymentAllocations", "CustomerDisputes", "CustomerWriteOffs", "OrderDeletionLog", "DeletedRowLog", "ReturnAuthorizations", "ReturnLines",
                      "CreditNotes", "CreditNoteLines", "ShipmentHeaders", "ShipmentLines", "ChangeTrackingWatermark"]:
            self.ctx.put(SQLSERVER, schemaFor.get(table, SALES), table, self.tables[table])


def generateTransactions(ctx: GenContext) -> None:
    builder = TransactionBuilder(ctx)
    builder.buildOrders()
    builder.buildInvoicesAndShipments()
    builder.buildPayments()
    builder.buildReturns()
    builder.buildControl()
    builder.store()
