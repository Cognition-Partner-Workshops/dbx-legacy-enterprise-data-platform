"""SQL Server master / reference generators (Application, Sales reference, Warehouse,
price lists, customers, commission plans, quotas). Transactional tables live in
``sqlserver_transactions.py``; both share the ``ctx.scratch`` lookups built here."""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from decimal import Decimal

from . import calendars
from .common import GenContext, People, Row, Value, money, qty
from .domain import (
    BUYING_GROUPS,
    CAPPED_EU_PLAN,
    CHANNELS,
    CITIES,
    COMMISSION_PLANS,
    COMPANY_STEMS,
    COMPANY_SUFFIXES,
    COUNTRIES,
    CUSTOMER_CATEGORIES,
    CUSTOMER_SEGMENTS,
    DELIVERY_METHODS,
    FIRST_NAMES,
    ITEM_ADJECTIVES,
    ITEM_NOUNS,
    LAST_NAMES,
    LEAF_TERRITORIES,
    PACKAGE_TYPES,
    RETURN_REASONS,
    STOCK_GROUPS,
    TERRITORIES,
    TERRITORY_WEIGHTS_IN_REGION,
    WWI_PAYMENT_METHODS,
    WWI_TRANSACTION_TYPES,
    Territory,
)
from .schema import SQLSERVER

APP, SALES, WH, RET = "Application", "Sales", "Warehouse", "Returns"
SYSTEM_USER = 1
END_OF_TIME = datetime(9999, 12, 31, 23, 59, 59)
SEED_TS = datetime(2013, 1, 1, 0, 0, 0)

TERRITORY_BY_CODE = {t.code: t for t in TERRITORIES}
# Currency conversion used for list prices (USD list -> local list). Deliberately static:
# price lists are re-costed yearly, not daily.
LIST_PRICE_FACTOR = {"USD": "1.00", "CAD": "1.35", "EUR": "0.92", "GBP": "0.79", "AUD": "1.50", "SGD": "1.35", "JPY": "150"}

PRICE_LISTS = [
    # (code, name, region, currency, basis, taxTreatment, taxRate, rounding, buyingGroupId, categoryId)
    ("NA-STD-USD", "North America standard list (USD)", "NA", "USD", "LIST", "EXCLUSIVE", None, "HALFUP2", None, None),
    ("NA-STD-CAD", "Canada standard list (CAD)", "NA", "CAD", "LIST", "EXCLUSIVE", None, "HALFUP2", None, None),
    ("NA-WEB-USD", "North America web list (USD)", "NA", "USD", "PROMOTIONAL", "EXCLUSIVE", None, "HALFUP2", None, None),
    ("NA-WHSL-USD", "Wholesaler contract list (USD)", "NA", "USD", "CONTRACT", "EXCLUSIVE", None, "HALFUP2", 1, 2),
    ("EU-STD-EUR", "Europe standard list (EUR)", "EU", "EUR", "LIST", "EXCLUSIVE", None, "HALFUP2", None, None),
    ("EU-STD-GBP", "UK standard list (GBP)", "EU", "GBP", "LIST", "EXCLUSIVE", None, "HALFUP2", None, None),
    ("EU-WEB-EUR", "Europe web list (EUR)", "EU", "EUR", "PROMOTIONAL", "EXCLUSIVE", None, "HALFUP2", None, None),
    ("AP-STD-AUD", "Australia standard list (AUD, GST inclusive)", "APAC", "AUD", "LIST", "INCLUSIVE", "10.00", "HALFUP2", None, None),
    ("AP-STD-SGD", "Singapore standard list (SGD, GST inclusive)", "APAC", "SGD", "LIST", "INCLUSIVE", "9.00", "HALFUP2", None, None),
    ("AP-STD-JPY", "Japan standard list (JPY, tax inclusive)", "APAC", "JPY", "LIST", "INCLUSIVE", "10.00", "UP0", None, None),
    ("AP-WEB-AUD", "APAC web list (AUD, GST inclusive)", "APAC", "AUD", "PROMOTIONAL", "INCLUSIVE", "10.00", "HALFUP2", None, None),
]
DEFAULT_LIST_BY_CURRENCY = {"USD": "NA-STD-USD", "CAD": "NA-STD-CAD", "EUR": "EU-STD-EUR", "GBP": "EU-STD-GBP",
                            "AUD": "AP-STD-AUD", "SGD": "AP-STD-SGD", "JPY": "AP-STD-JPY"}


def _temporal(lastEditedBy: int = SYSTEM_USER, validFrom: datetime = SEED_TS) -> dict[str, Value]:
    return {"LastEditedBy": lastEditedBy, "ValidFrom": validFrom, "ValidTo": END_OF_TIME}


def _edited(lastEditedBy: int = SYSTEM_USER, when: datetime = SEED_TS) -> dict[str, Value]:
    return {"LastEditedBy": lastEditedBy, "LastEditedWhen": when}


def _simpleReference(ctx: GenContext, schema: str, table: str, idColumn: str, nameColumn: str, values: dict[int, str]) -> None:
    ctx.put(SQLSERVER, schema, table, [{idColumn: k, nameColumn: v, **_temporal()} for k, v in values.items()])


def generateStaticReference(ctx: GenContext) -> None:
    _simpleReference(ctx, APP, "PaymentMethods", "PaymentMethodID", "PaymentMethodName", WWI_PAYMENT_METHODS)
    _simpleReference(ctx, APP, "TransactionTypes", "TransactionTypeID", "TransactionTypeName", WWI_TRANSACTION_TYPES)
    _simpleReference(ctx, APP, "DeliveryMethods", "DeliveryMethodID", "DeliveryMethodName", DELIVERY_METHODS)
    _simpleReference(ctx, SALES, "CustomerCategories", "CustomerCategoryID", "CustomerCategoryName", CUSTOMER_CATEGORIES)
    _simpleReference(ctx, SALES, "BuyingGroups", "BuyingGroupID", "BuyingGroupName", BUYING_GROUPS)
    _simpleReference(ctx, WH, "StockGroups", "StockGroupID", "StockGroupName", STOCK_GROUPS)
    _simpleReference(ctx, WH, "PackageTypes", "PackageTypeID", "PackageTypeName", PACKAGE_TYPES)

    channels = []
    for c in CHANNELS:
        channels.append({
            "SalesChannelID": c.channelId, "ChannelCode": c.code, "ChannelName": c.name,
            "ChannelClass": {"FIELD": "DIRECT", "CALLCENTRE": "CALLCENTRE", "WEB": "DIGITAL", "EDI": "EDI", "DISTRIBUTOR": "PARTNER", "MARKETPLACE": "PARTNER"}[c.channelClass],
            "RegionCode": c.region, "ChannelStatus": c.status, "PartnerIdentifier": c.partnerIdentifier, "DefaultPriceListCode": c.defaultPriceListCode,
            "CommissionModifierPercent": money(c.commissionModifierPercent), "RequiresManualApproval": c.requiresManualApproval, "OrderPrefix": c.orderPrefix,
            "ValidFromDate": date.fromisoformat(c.validFrom), "ValidToDate": date.fromisoformat(c.validTo) if c.validTo else None, **_edited(),
        })
        if c.status == "PILOT":
            ctx.tag("PILOT_CHANNEL", "Sales channel with ChannelStatus = 'PILOT' (orderable, but excluded from commission).", f"{SALES}.SalesChannels",
                    SalesChannelID=c.channelId, ChannelCode=c.code)
    ctx.put(SQLSERVER, SALES, "SalesChannels", channels)

    plans = []
    for planId, (code, name, region, basis, b1u, b1r, b2u, b2r, b3u, b3r, accel, clawback, minMargin, effFrom, effTo) in enumerate(COMMISSION_PLANS, start=1):
        plans.append({
            "CommissionPlanID": planId, "PlanCode": code, "PlanName": name, "RegionCode": region, "CommissionBasis": basis,
            "Band1UpperPercent": money(b1u), "Band1RatePercent": money(b1r), "Band2UpperPercent": money(b2u) if b2u else None,
            "Band2RatePercent": money(b2r) if b2r else None, "Band3UpperPercent": money(b3u) if b3u else None, "Band3RatePercent": money(b3r) if b3r else None,
            "AcceleratorPercent": money(accel) if accel else None, "ClawbackWindowDays": clawback, "MinimumMarginPercent": money(minMargin) if minMargin else None,
            "EffectiveFromDate": date.fromisoformat(effFrom), "EffectiveToDate": date.fromisoformat(effTo) if effTo else None, **_edited(),
        })
    ctx.put(SQLSERVER, SALES, "CommissionPlans", plans)
    ctx.scratch.planIdByCode = {p["PlanCode"]: p["CommissionPlanID"] for p in plans}
    for basis in ("INVOICEDMARGIN", "NETREVENUE", "COLLECTEDCASH"):
        for p in plans:
            if p["CommissionBasis"] == basis and p["EffectiveToDate"] is None:
                ctx.tag("COMMISSION_BASIS_COVERAGE", "Active commission plans exist for every CommissionBasis value.", f"{SALES}.CommissionPlans",
                        CommissionPlanID=p["CommissionPlanID"], PlanCode=p["PlanCode"], CommissionBasis=basis)

    reasons = []
    for reasonId, (code, region, desc, category, customerFault, restockApplies, restockPct, inspection, photo, resale, window, supplier) in enumerate(RETURN_REASONS, start=1):
        reasons.append({
            "ReturnReasonID": reasonId, "ReasonCode": code, "RegionCode": region, "ReasonDescription": desc, "ReasonCategory": category,
            "IsCustomerFault": bool(customerFault), "DefaultRestockingApplies": bool(restockApplies), "RestockingPercent": money(restockPct) if restockPct else None,
            "RequiresInspection": bool(inspection), "RequiresPhotoEvidence": bool(photo), "AllowsResale": bool(resale), "ReturnWindowDays": window,
            "SupplierRecoverable": bool(supplier), "IsActive": True, **_edited(),
        })
    ctx.put(SQLSERVER, RET, "ReturnReasons", reasons)


def generatePeopleAndTerritories(ctx: GenContext) -> None:
    rng = ctx.rng("sqlserver.people")
    people = People(rng)
    people.addPerson("Data Conversion Only", False, False, SEED_TS, isSystemUser=True)

    def addPerson(fullName: str, isEmployee: bool, isSalesperson: bool, validFrom: datetime = SEED_TS) -> int:
        return people.addPerson(fullName, isEmployee, isSalesperson, validFrom)

    def personName() -> str:
        return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"

    managers = {t.code: addPerson(personName(), True, False) for t in TERRITORIES}
    salespeopleByTerritory: dict[str, list[int]] = {t.code: [] for t in LEAF_TERRITORIES}
    regionOrder = [t for t in LEAF_TERRITORIES]
    weights = [TERRITORY_WEIGHTS_IN_REGION[t.code] * {"NA": 0.40, "EU": 0.35, "APAC": 0.25}[t.region] for t in regionOrder]
    for territory in regionOrder:  # at least one rep per territory
        salespeopleByTerritory[territory.code].append(addPerson(personName(), True, True))
    for _ in range(ctx.params["salespeople"] - len(regionOrder)):
        territory = rng.choices(regionOrder, weights=weights, k=1)[0]
        salespeopleByTerritory[territory.code].append(addPerson(personName(), True, True))
    insideSales = {t.code: addPerson(personName(), True, True) for t in LEAF_TERRITORIES if t.region == "NA"}
    pickers = [addPerson(personName(), True, False) for _ in range(6)]
    accounts = [addPerson(personName(), True, False) for _ in range(3)]
    people.managers, people.salespeopleByTerritory, people.insideSales = managers, salespeopleByTerritory, insideSales
    people.pickers, people.accounts = pickers, accounts
    ctx.scratch.people = people

    territoryRows = []
    for t in TERRITORIES:
        country = COUNTRIES[t.countryIso2] if t.countryIso2 else None
        territoryRows.append({
            "SalesTerritoryID": t.territoryId, "TerritoryCode": t.code, "TerritoryName": t.name,
            "ParentTerritoryID": TERRITORY_BY_CODE[t.parentCode].territoryId if t.parentCode else None, "TerritoryLevel": t.level, "RegionCode": t.region,
            "CountryISO3": country.iso3 if country else None,
            "TaxRegimeCode": country.taxRegime if country else {"NA": "USSALESTAX", "EU": "EUVAT", "APAC": "AUGST"}[t.region],
            "FiscalCalendarCode": t.fiscalCalendar, "ReportingCurrencyCode": country.currency if country else {"NA": "USD", "EU": "EUR", "APAC": "AUD"}[t.region],
            "PostalStandardCode": t.postalStandard, "ManagerPersonID": managers[t.code], "IsActive": True, **_edited(),
        })
    ctx.put(SQLSERVER, SALES, "SalesTerritories", territoryRows)

    planIdByCode = ctx.scratch.planIdByCode
    teams: list[Row] = []
    members: list[Row] = []
    validFrom = datetime(ctx.spanStart.year - 1, 1, 1)
    capPersonId: int | None = None
    distPlanAssigned = False
    marketPlanAssigned = False

    def addMember(teamId: int, personId: int, role: str, planCode: str, share: str, primary: bool) -> int:
        members.append({
            "SalesTeamMemberID": len(members) + 1, "SalesTeamID": teamId, "PersonID": personId, "RoleCode": role, "CommissionPlanID": planIdByCode[planCode],
            "QuotaSharePercent": money(share), "ValidFrom": validFrom, "ValidTo": None, "IsPrimaryAssignment": primary, "ReplacedBySalesTeamMemberID": None,
            "ChangeReasonCode": None, **_edited(when=validFrom),
        })
        return len(members)

    planByPerson: dict[int, str] = {}
    for t in LEAF_TERRITORIES:
        teamId = len(teams) + 1
        teamType = "DISTRIBUTOR" if t.distributorManaged else "FIELD"
        teams.append({
            "SalesTeamID": teamId, "TeamCode": f"{t.code}-{teamType[:3]}", "TeamName": f"{t.name} {teamType.lower()} team", "ParentSalesTeamID": None,
            "SalesTerritoryID": t.territoryId, "RegionCode": t.region, "TeamType": teamType, "ManagerPersonID": managers[t.code],
            "CostCentreCode": f"CC-{t.region}-{t.territoryId:03d}", "FormedOnDate": date(2016, 1, 1), "DisbandedOnDate": None, "IsActive": True, **_edited(),
        })
        reps = salespeopleByTerritory[t.code]
        if t.distributorManaged:
            # LEGACY QUIRK: usp_LoadBridgeEmployeeTerritory gives 100% of a distributor-managed APAC
            # territory to the channel manager; the reps in it carry no quota share of their own.
            addMember(teamId, managers[t.code], "MANAGER", "AP-FIELD-2020", "100.00", True)
            for rep in reps:
                addMember(teamId, rep, "SUPPORT", "AP-MARKET-2020", "1.00", False)
                planByPerson[rep] = "AP-MARKET-2020"
                marketPlanAssigned = True
            planByPerson[managers[t.code]] = "AP-FIELD-2020"
            continue
        if t.region == "NA":
            # LEGACY QUIRK: NA coverage is the primary rep at 0.80 plus an inside-sales overlay at 0.20.
            for rep in reps:
                addMember(teamId, rep, "REP", "NA-FIELD-2019", "80.00", True)
                planByPerson[rep] = "NA-FIELD-2019"
            overlayTeamId = len(teams) + 1
            teams.append({
                "SalesTeamID": overlayTeamId, "TeamCode": f"{t.code}-INS", "TeamName": f"{t.name} inside sales overlay", "ParentSalesTeamID": teamId,
                "SalesTerritoryID": t.territoryId, "RegionCode": t.region, "TeamType": "INSIDE", "ManagerPersonID": managers[t.code],
                "CostCentreCode": f"CC-NA-INS-{t.territoryId:03d}", "FormedOnDate": date(2019, 1, 1), "DisbandedOnDate": None, "IsActive": True, **_edited(),
            })
            addMember(overlayTeamId, insideSales[t.code], "SUPPORT", "NA-TELE-2019", "20.00", True)
            planByPerson[insideSales[t.code]] = "NA-TELE-2019"
        elif t.region == "EU":
            # LEGACY QUIRK: EU allocation below 0.25 is rejected by the bridge load, so shares are floored at 25%.
            share = max(Decimal(100) / len(reps), Decimal(25)).quantize(Decimal("0.01"))
            for index, rep in enumerate(reps):
                planCode = "EU-FIELD-2016"
                if capPersonId is None:
                    planCode, capPersonId = CAPPED_EU_PLAN, rep
                elif not distPlanAssigned:
                    planCode, distPlanAssigned = "EU-DIST-2016", True
                addMember(teamId, rep, "SENIORREP" if index == 0 else "REP", planCode, str(share), True)
                planByPerson[rep] = planCode
        else:
            share = (Decimal(100) / len(reps)).quantize(Decimal("0.01"))
            for rep in reps:
                planCode = "AP-FIELD-2020"
                if not marketPlanAssigned and t.code == "AP-AU":
                    planCode, marketPlanAssigned = "AP-MARKET-2020", True
                addMember(teamId, rep, "REP", planCode, str(share), True)
                planByPerson[rep] = planCode
    ctx.put(SQLSERVER, APP, "SalesTeams", teams)
    ctx.put(SQLSERVER, APP, "SalesTeamMembers", members)
    ctx.scratch.planByPerson = planByPerson
    ctx.scratch.capPersonId = capPersonId


def generateStockItems(ctx: GenContext) -> None:
    rng = ctx.rng("sqlserver.stock")
    items: list[Row] = []
    links: list[Row] = []
    holdings: list[Row] = []
    names: set[str] = set()
    for stockItemId in range(1, ctx.params["stockItems"] + 1):
        while True:
            name = f"{rng.choice(ITEM_ADJECTIVES)} {rng.choice(ITEM_NOUNS)} ({rng.choice(['Red', 'Blue', 'Black', 'White', 'Green', 'Yellow'])}) {rng.choice(['S', 'M', 'L', 'XL', '3XL', 'One size'])}"
            if name not in names:
                names.add(name)
                break
        # Ex-tax USD list price in 10-cent steps so APAC GST-inclusive prices divide cleanly unless we say otherwise.
        unitPrice = money(Decimal(rng.randint(20, 3000)) / 10)
        chiller = rng.random() < 0.05
        items.append({
            "StockItemID": stockItemId, "StockItemName": name, "SupplierID": rng.randint(1, 13), "ColorID": rng.choice([None, rng.randint(1, 36)]),
            "UnitPackageID": 7, "OuterPackageID": rng.choice([4, 6]), "Brand": rng.choice([None, "Northwind", "Tailspin", "WWI Own"]), "Size": name.rsplit(" ", 1)[-1],
            "LeadTimeDays": rng.randint(7, 30), "QuantityPerOuter": rng.choice([1, 6, 12, 24]), "IsChillerStock": chiller, "Barcode": f"{rng.randint(10**11, 10**12 - 1)}",
            "TaxRate": qty("15.000"), "UnitPrice": unitPrice, "RecommendedRetailPrice": money(unitPrice * Decimal("1.5")),
            "TypicalWeightPerUnit": qty(Decimal(rng.randint(50, 5000)) / 1000), "MarketingComments": None, "InternalComments": None,
            "CustomFields": None, **_temporal(),
        })
        groups = rng.sample(sorted(STOCK_GROUPS), k=rng.choice([1, 1, 2]))
        for groupId in groups:
            links.append({"StockItemStockGroupID": len(links) + 1, "StockItemID": stockItemId, "StockGroupID": groupId, **_edited()})
        onHand = rng.randint(0, 5000)
        holdings.append({
            "StockItemID": stockItemId, "QuantityOnHand": onHand, "BinLocation": f"{rng.choice('ABCDEFGH')}-{rng.randint(1, 40):02d}", "LastStocktakeQuantity": onHand + rng.randint(-20, 20),
            "LastCostPrice": money(unitPrice * Decimal(rng.choice(["0.55", "0.60", "0.65", "0.72"]))), "ReorderLevel": rng.randint(10, 200), "TargetStockLevel": rng.randint(200, 1000),
            **_edited(), "PrimaryWarehouseSiteID": rng.randint(1, 3), "QuantityOnHandAllSites": qty(onHand + rng.randint(0, 2000)), "QuantityReservedAllSites": qty(rng.randint(0, min(onHand, 200))),
            "QuantityInTransit": qty(rng.randint(0, 500)), "QuantityOnPurchaseOrder": qty(rng.randint(0, 1000)), "AbcClass": rng.choice(["A", "B", "B", "C", "C", None]),
            "LastCountedWhen": ctx.randomTimestamp(rng, ctx.randomDate(rng)), "LastMovementWhen": ctx.randomTimestamp(rng, ctx.spanEnd - timedelta(days=rng.randint(0, 10))),
        })
    ctx.put(SQLSERVER, WH, "StockItems", items)
    ctx.put(SQLSERVER, WH, "StockItemStockGroups", links)
    ctx.put(SQLSERVER, WH, "StockItemHoldings", holdings)
    ctx.scratch.costByItem = {h["StockItemID"]: h["LastCostPrice"] for h in holdings}


def generatePriceLists(ctx: GenContext) -> None:
    rng = ctx.rng("sqlserver.prices")
    people = ctx.scratch.people
    assert people is not None
    approver = people.managers["NA"]
    lists: list[Row] = []
    lines: list[Row] = []
    items = ctx.sql(WH, "StockItems")
    costByItem = ctx.scratch.costByItem
    priceByListAndItem: dict[tuple[int, int], Row] = {}
    listIdByCode: dict[str, int] = {}
    effectiveFrom = date(ctx.spanStart.year - 1, 1, 1)
    for listId, (code, name, region, currency, basis, treatment, taxRate, rounding, buyingGroupId, categoryId) in enumerate(PRICE_LISTS, start=1):
        listIdByCode[code] = listId
        lists.append({
            "PriceListID": listId, "PriceListCode": code, "PriceListName": name, "RegionCode": region, "CurrencyCode": currency, "PriceBasis": basis,
            "TaxTreatment": treatment, "TaxRatePercent": money(taxRate) if taxRate else None, "RoundingRuleCode": rounding, "BuyingGroupID": buyingGroupId,
            "CustomerCategoryID": categoryId, "EffectiveFromDate": effectiveFrom, "EffectiveToDate": None, "SupersedesPriceListID": None,
            "ApprovalStatus": "APPROVED", "ApprovedByPersonID": approver, "ApprovedWhen": datetime.combine(effectiveFrom, datetime.min.time()) - timedelta(days=10), **_edited(),
        })
        factor = Decimal(LIST_PRICE_FACTOR[currency]) * Decimal({"LIST": "1.00", "PROMOTIONAL": "0.95", "CONTRACT": "0.88"}[basis])
        for item in items:
            exTax = item["UnitPrice"] * factor
            if currency == "JPY":
                exTax = (exTax / 10).quantize(Decimal(1)) * 10  # whole tens of yen keep 10% inclusive prices integral
            else:
                exTax = exTax.quantize(Decimal("0.1")) if treatment == "INCLUSIVE" and taxRate == "10.00" else exTax.quantize(Decimal(1) if treatment == "INCLUSIVE" else Decimal("0.01"))
            price = exTax * (1 + Decimal(taxRate) / 100) if treatment == "INCLUSIVE" else exTax
            price = price.quantize(Decimal(1) if currency == "JPY" else Decimal("0.01"))
            line = {
                "PriceListLineID": len(lines) + 1, "PriceListID": listId, "StockItemID": item["StockItemID"], "MinimumQuantity": 1, "UnitPrice": money(price),
                "StandardCostAtLoad": money(costByItem[item["StockItemID"]] * Decimal(LIST_PRICE_FACTOR[currency])), "MarginFloorPercent": money(rng.choice(["15.00", "20.00", "25.00"])),
                "MaximumDiscountPercent": money(rng.choice(["10.00", "15.00", "25.00"])), "IsPromotionalPrice": False, "SourceSystemReference": f"PRC-{code}-{item['StockItemID']:05d}",
                **_edited(when=datetime.combine(effectiveFrom, datetime.min.time())),
            }
            lines.append(line)
            priceByListAndItem[(listId, item["StockItemID"])] = line
    ctx.put(SQLSERVER, SALES, "PriceLists", lists)
    ctx.put(SQLSERVER, SALES, "PriceListLines", lines)
    ctx.scratch.priceLists = {row["PriceListID"]: row for row in lists}
    ctx.scratch.listIdByCode = listIdByCode
    ctx.scratch.priceByListAndItem = priceByListAndItem

    promotions: list[Row] = []
    for promoId, (code, name, region, promoType, start, days, budget, currency) in enumerate([
        ("NA-SPRING", "Spring clearance", "NA", "PERCENT", 30, 45, "25000.00", "USD"),
        ("NA-BLACKFRI", "Black Friday web", "NA", "PERCENT", 380, 5, "60000.00", "USD"),
        ("EU-SUMMER", "Summer volume promotion", "EU", "BUNDLE", 120, 60, "30000.00", "EUR"),
        ("EU-UKLAUNCH", "UK relaunch bundle", "EU", "BUNDLE", 300, 30, "12000.00", "GBP"),
        ("AP-EOFY", "End of financial year sale", "APAC", "PERCENT", 250, 30, "20000.00", "AUD"),
        ("AP-MARKET", "Marketplace launch incentive", "APAC", "PERCENT", 60, 90, "8000.00", "AUD"),
    ], start=1):
        startDate = ctx.spanStart + timedelta(days=start)
        promotions.append({
            "PromotionID": promoId, "PromotionCode": code, "PromotionName": name, "RegionCode": region, "PromotionType": promoType, "CampaignReference": f"CMP-{promoId:04d}",
            "StartDate": startDate, "EndDate": startDate + timedelta(days=days), "BudgetAmount": money(budget), "BudgetCurrencyCode": currency,
            "SupplierFundedPercent": money(rng.choice(["0.00", "25.00", "50.00"])), "EligibleCustomerCategoryList": None, "MaximumRedemptionsPerCustomer": None,
            "RequiresCouponCode": promoType == "BUNDLE", "IsStackable": False, "PromotionStatus": "EXPIRED" if startDate + timedelta(days=days) < ctx.asOf else "LIVE",
            "ApprovedByPersonID": approver, "RedemptionCount": 0, "RedeemedValue": money("0.00"), **_edited(),
        })
    ctx.put(SQLSERVER, SALES, "Promotions", promotions)
    ctx.scratch.promotionsByRegion = {r: [p for p in promotions if p["RegionCode"] == r] for r in ("NA", "EU", "APAC")}


def generateCustomers(ctx: GenContext) -> None:
    rng = ctx.rng("sqlserver.customers")
    people = ctx.scratch.people
    assert people is not None
    listIdByCode = ctx.scratch.listIdByCode
    leaf = LEAF_TERRITORIES
    weights = [TERRITORY_WEIGHTS_IN_REGION[t.code] * {"NA": 0.40, "EU": 0.35, "APAC": 0.25}[t.region] for t in leaf]
    watermark = datetime.combine(ctx.asOf - timedelta(days=7), datetime.min.time()).replace(hour=2)
    ctx.scratch.extractWatermark = watermark
    total = ctx.params["customers"]
    lateArriving = set(range(total - 2, total + 1))  # last three customer ids arrive after the watermark
    rows: list[Row] = []
    customerCountry: dict[int, str] = {}
    customerTerritory: dict[int, Territory] = {}
    usedNames: set[str] = set()

    for customerId in range(1, total + 1):
        territory = rng.choices(leaf, weights=weights, k=1)[0]
        country = COUNTRIES[territory.countryIso2]
        region = territory.region
        while True:
            name = f"{rng.choice(COMPANY_STEMS)} {rng.choice(['Toys', 'Novelties', 'Trading', 'Retail', 'Gifts', 'Imports', 'Stores'])} {rng.choice(COMPANY_SUFFIXES[region])} ({rng.choice(CITIES[country.iso2])})"
            if name not in usedNames:
                usedNames.add(name)
                break
        if customerId in lateArriving:
            opened = (watermark + timedelta(days=1)).date()
        else:
            opened = ctx.randomDate(rng, date(2013, 1, 1), ctx.spanStart + timedelta(days=200))
        validFrom = datetime.combine(opened, datetime.min.time()).replace(hour=9, minute=rng.randint(0, 59))
        primary = people.addPerson(f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}", False, False, validFrom)
        alternate = people.addPerson(f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}", False, False, validFrom) if rng.random() < 0.6 else None
        categoryId = rng.choices(list(CUSTOMER_CATEGORIES), weights=[2, 6, 5, 3, 3, 5, 2, 8], k=1)[0]
        buyingGroupId = rng.choice(list(BUYING_GROUPS)) if rng.random() < 0.35 else None
        onHold = rng.random() < 0.05
        vatNumber = None
        if region == "EU":
            vatNumber = f"{country.iso2}{rng.randint(10**8, 10**9 - 1)}" if rng.random() < 0.85 else None
        elif region == "NA":
            vatNumber = f"{rng.randint(10, 99)}-{rng.randint(10**6, 10**7 - 1)}" if rng.random() < 0.7 else None
        else:
            vatNumber = f"{rng.randint(10**10, 10**11 - 1)}" if rng.random() < 0.8 else None
        consent = None
        consentWhen = None
        if region == "EU":
            consent = rng.random() >= 0.3
            consentWhen = validFrom + timedelta(days=rng.randint(0, 30))
        elif rng.random() < 0.5:
            consent = True
        city = rng.choice(CITIES[country.iso2])
        postal = f"{rng.randint(10000, 99999)}"
        row = {
            "CustomerID": customerId, "CustomerName": name, "BillToCustomerID": customerId, "CustomerCategoryID": categoryId, "BuyingGroupID": buyingGroupId,
            "PrimaryContactPersonID": primary, "AlternateContactPersonID": alternate, "DeliveryMethodID": rng.choice([1, 2, 3, 6]), "DeliveryCityID": 1000 + customerId,
            "PostalCityID": 1000 + customerId, "CreditLimit": money(rng.choice([None, 2500, 5000, 10000, 25000, 50000]) or 0) if rng.random() < 0.8 else None,
            "AccountOpenedDate": opened, "StandardDiscountPercentage": qty(rng.choice(["0.000", "0.000", "2.500", "5.000"])), "IsStatementSent": rng.random() < 0.7,
            "IsOnCreditHold": onHold, "PaymentDays": rng.choice([14, 30, 30, 45, 60]), "PhoneNumber": f"+{rng.randint(1, 81)} {rng.randint(100, 999)} {rng.randint(1000, 9999)}",
            "FaxNumber": None, "DeliveryRun": None, "RunPosition": None, "WebsiteURL": f"https://www.{name.split(' ')[0].lower()}{customerId}.example",
            "DeliveryAddressLine1": f"Unit {rng.randint(1, 40)}", "DeliveryAddressLine2": f"{rng.randint(1, 999)} {rng.choice(LAST_NAMES)} Road", "DeliveryPostalCode": postal,
            "PostalAddressLine1": f"PO Box {rng.randint(100, 9999)}", "PostalAddressLine2": city, "PostalPostalCode": postal, **_temporal(validFrom=validFrom),
            "SalesTerritoryID": territory.territoryId, "RegionCode": region, "TaxRegistrationNumber": vatNumber,
            "TaxExemptionCertificate": f"EX-{rng.randint(100000, 999999)}" if region == "NA" and rng.random() < 0.05 else None,
            "DefaultPriceListID": listIdByCode[DEFAULT_LIST_BY_CURRENCY[country.currency]],
            "CreditHoldReasonCode": rng.choice(["OVERDUE", "LIMIT", "DISPUTE"]) if onHold else None,
            "CreditHoldSetWhen": ctx.randomTimestamp(rng, ctx.randomDate(rng)) if onHold else None,
            "CreditScoreValue": rng.randint(300, 850) if rng.random() < 0.8 else None, "CreditScoreAgency": rng.choice(["DNB", "EXPERIAN", "EQUIFAX"]),
            "CreditScoreCheckedOn": ctx.randomDate(rng), "AverageDaysToPay": rng.randint(15, 75) if rng.random() < 0.9 else None,
            "MarketingConsentFlag": consent, "ConsentCapturedWhen": consentWhen,
            "DataRetentionExpiresOn": opened + timedelta(days=365 * 7) if region == "EU" else None,
        }
        if row["CreditLimit"] is not None and row["CreditLimit"] == 0:
            row["CreditLimit"] = None
        rows.append(row)
        customerCountry[customerId] = country.iso2
        customerTerritory[customerId] = territory
        if region == "EU" and consent is False:
            ctx.tag("EU_CONSENT_N", "EU customer with MarketingConsentFlag = 'N' (partner feed must strip consent fields).", f"{SALES}.Customers", CustomerID=customerId)
        if customerId in lateArriving:
            ctx.tag("LATE_ARRIVING_CUSTOMER", "Customer whose row arrives in a later extract than its invoices (ValidFrom after the extract watermark).",
                    f"{SALES}.Customers", CustomerID=customerId, ValidFrom=validFrom)

    # LEGACY QUIRK: the incremental customer extract overlaps by OverlapMinutes, so a customer edited
    # twice in the window appears twice with different ValidFrom / LastEditedBy (both open-ended).
    for customerId in rng.sample(range(1, total - 3), 5):
        original = next(r for r in rows if r["CustomerID"] == customerId)
        duplicate = dict(original)
        duplicate["ValidFrom"] = watermark - timedelta(hours=rng.randint(1, 20))
        duplicate["LastEditedBy"] = people.accounts[0]
        duplicate["CreditLimit"] = money((original["CreditLimit"] or Decimal(5000)) * Decimal("1.2"))
        duplicate["AverageDaysToPay"] = rng.randint(15, 75)
        rows.append(duplicate)
        ctx.tag("DUPLICATE_CUSTOMER_EXTRACT", "Same CustomerID appears twice in the Customers extract with different ValidFrom.", f"{SALES}.Customers",
                CustomerID=customerId, ValidFrom=duplicate["ValidFrom"])

    # A few customers bill through a parent account.
    for row in rng.sample(rows[: total - 3], max(3, total // 40)):
        parent = rng.choice([r for r in rows[: total - 3] if r["RegionCode"] == row["RegionCode"] and r["CustomerID"] != row["CustomerID"]])
        row["BillToCustomerID"] = parent["CustomerID"]

    ctx.put(SQLSERVER, SALES, "Customers", rows)
    ctx.put(SQLSERVER, APP, "People", people.rows)
    ctx.scratch.customerCountry = customerCountry
    ctx.scratch.customerTerritory = customerTerritory
    ctx.scratch.lateArrivingCustomers = lateArriving
    ctx.scratch.customersById = {r["CustomerID"]: r for r in rows}


def generateSalesQuotas(ctx: GenContext) -> None:
    rng = ctx.rng("sqlserver.quotas")
    people = ctx.scratch.people
    assert people is not None
    salespeopleByTerritory = people.salespeopleByTerritory
    capPersonId = ctx.scratch.capPersonId
    planByPerson = ctx.scratch.planByPerson
    rows: list[Row] = []
    capTagged = False
    for territory in LEAF_TERRITORIES:
        currency = COUNTRIES[territory.countryIso2].currency
        scale = Decimal(LIST_PRICE_FACTOR[currency])
        for personId in salespeopleByTerritory[territory.code]:
            for fp in calendars.periodsBetween(territory.fiscalCalendar, ctx.spanStart, ctx.spanEnd):
                quota = money(Decimal(rng.randint(40, 120)) * 1000 * scale)
                closed = fp.periodEnd < ctx.asOf
                attainment = money(quota * Decimal(str(round(rng.uniform(0.55, 1.25), 3)))) if closed else money(quota * Decimal(str(round(rng.uniform(0.0, 0.6), 3))))
                if personId == capPersonId and closed and not capTagged:
                    attainment = money(quota * Decimal("1.38"))  # above Band3UpperPercent (120) of EU-FIELD-CAP-2024
                    capTagged = True
                    ctx.tag("COMMISSION_EU_CAP_HIT", "EU salesperson on the capped plan whose attainment exceeds Band3UpperPercent (statutory cap).", f"{SALES}.SalesQuotas",
                            SalesQuotaID=len(rows) + 1, SalespersonPersonID=personId, PlanCode=planByPerson[personId], FiscalPeriodLabel=fp.label)
                rows.append({
                    "SalesQuotaID": len(rows) + 1, "SalesTerritoryID": territory.territoryId, "SalespersonPersonID": personId, "FiscalCalendarCode": territory.fiscalCalendar,
                    "FiscalPeriodLabel": fp.label, "PeriodStartDate": fp.periodStart, "PeriodEndDate": fp.periodEnd, "QuotaAmount": quota, "QuotaCurrencyCode": currency,
                    "StretchQuotaAmount": money(quota * Decimal("1.2")), "AttainmentAmount": attainment,
                    "AttainmentRefreshedWhen": datetime.combine(min(fp.periodEnd, ctx.asOf), datetime.min.time()) + timedelta(days=1, hours=3),
                    "QuotaStatus": rng.choice(["LOCKED", "LOCKED", "LOCKED", "RESTATED"]) if closed else "OPEN", **_edited(),
                })
    ctx.put(SQLSERVER, SALES, "SalesQuotas", rows)


def generateSpecialDeals(ctx: GenContext) -> None:
    rng = ctx.rng("sqlserver.deals")
    customers = ctx.sql(SALES, "Customers")
    items = ctx.sql(WH, "StockItems")
    rows: list[Row] = []
    for dealId in range(1, 9):
        kind = dealId % 4
        start = ctx.randomDate(rng, ctx.spanStart, ctx.spanEnd - timedelta(days=60))
        rows.append({
            "SpecialDealID": dealId, "StockItemID": rng.choice(items)["StockItemID"] if kind in (0, 1) else None,
            "CustomerID": rng.choice(customers)["CustomerID"] if kind == 0 else None, "BuyingGroupID": rng.choice(list(BUYING_GROUPS)) if kind == 2 else None,
            "CustomerCategoryID": rng.choice(list(CUSTOMER_CATEGORIES)) if kind == 3 else None, "StockGroupID": rng.choice(list(STOCK_GROUPS)) if kind in (2, 3) else None,
            "DealDescription": f"Special deal {dealId}", "StartDate": start, "EndDate": start + timedelta(days=rng.randint(30, 120)),
            "DiscountAmount": None, "DiscountPercentage": qty(rng.choice(["5.000", "7.500", "10.000"])), "UnitPrice": None, **_edited(),
        })
    ctx.put(SQLSERVER, SALES, "SpecialDeals", rows)


def generateCities(ctx: GenContext) -> None:
    rng = ctx.rng("sqlserver.cities")
    countryIso = sorted(COUNTRIES)
    rows: list[Row] = []
    seen: set[int] = set()
    for customer in ctx.sql(SALES, "Customers"):
        cityId = customer["DeliveryCityID"]
        customerId = customer["CustomerID"]
        if not isinstance(cityId, int) or not isinstance(customerId, int) or cityId in seen:
            continue
        seen.add(cityId)
        rows.append({
            "CityID": cityId, "CityName": customer["PostalAddressLine2"],
            "StateProvinceID": 100 * (countryIso.index(ctx.scratch.customerCountry[customerId]) + 1) + rng.randint(1, 5),
            "Location": None, "LatestRecordedPopulation": rng.choice([None, rng.randint(20_000, 8_000_000)]), **_temporal(),
        })
    ctx.put(SQLSERVER, APP, "Cities", rows)


def generateCustomerSegments(ctx: GenContext) -> None:
    rng = ctx.rng("sqlserver.segments")
    segments: list[Row] = []
    for segmentId, (code, name, family, region, consentRequired, retention) in enumerate(CUSTOMER_SEGMENTS, start=1):
        segments.append({
            "CustomerSegmentID": segmentId, "SegmentCode": code, "SegmentName": name, "SegmentFamily": family, "RegionCode": region,
            "SegmentRuleText": f"Customers in {region} matching the {name.lower()} rule (maintained by marketing ops).",
            "ConsentRequired": consentRequired, "ConsentBasisCode": ("CONSENT" if region == "EU" else "OPTIN") if consentRequired else None,
            "RetentionMonths": retention, "PriorityOrder": segmentId, "IsExclusive": family == "VALUE", "IsActive": True, **_edited(),
        })
    ctx.put(SQLSERVER, SALES, "CustomerSegments", segments)

    byRegion = {r: [s for s in segments if s["RegionCode"] == r] for r in ("NA", "EU", "APAC")}
    assignments: list[Row] = []
    seenCustomers: set[int] = set()
    for customer in ctx.sql(SALES, "Customers"):
        customerId = customer["CustomerID"]
        if not isinstance(customerId, int) or customerId in seenCustomers:
            continue
        seenCustomers.add(customerId)
        region = str(customer["RegionCode"])
        consent = customer["MarketingConsentFlag"]
        for segment in rng.sample(byRegion[region], rng.randint(1, 2)):
            if segment["ConsentRequired"] and consent is not True:
                continue
            validFrom = ctx.randomDate(rng, ctx.spanStart, ctx.spanEnd - timedelta(days=30))
            if rng.random() < 0.15:
                # LEGACY QUIRK: usp_AssignCustomerSegments closes the prior row by stamping ValidToDate / IsCurrentRow = 0,
                # but the pair is unenforced, so some closed rows still carry IsCurrentRow = 1.
                closedOn = validFrom + timedelta(days=rng.randint(30, 200))
                assignments.append(_segmentAssignment(len(assignments) + 1, customerId, segment, validFrom, closedOn, rng.random() < 0.3, customer, rng))
                validFrom = closedOn + timedelta(days=1)
            assignments.append(_segmentAssignment(len(assignments) + 1, customerId, segment, validFrom, None, True, customer, rng))
    ctx.put(SQLSERVER, SALES, "CustomerSegmentAssignments", assignments)


def _segmentAssignment(assignmentId: int, customerId: int, segment: Row, validFrom: date, validTo: date | None, current: bool, customer: Row, rng: random.Random) -> Row:
    retention = int(str(segment["RetentionMonths"]))
    return {
        "CustomerSegmentAssignmentID": assignmentId, "CustomerID": customerId, "CustomerSegmentID": segment["CustomerSegmentID"],
        "ValidFromDate": validFrom, "ValidToDate": validTo, "IsCurrentRow": current,
        "AssignmentReason": f"Rule {segment['SegmentCode']} matched", "ScoreValue": qty(rng.uniform(0, 100)) if segment["SegmentFamily"] in ("VALUE", "RISK") else None,
        "ConsentCapturedWhen": customer["ConsentCapturedWhen"] if segment["ConsentRequired"] else None,
        "ConsentSourceCode": ("WEBFORM" if rng.random() < 0.7 else "REP") if segment["ConsentRequired"] else None,
        "RetentionExpiryDate": validFrom + timedelta(days=30 * retention), "AssignedByProcess": "usp_AssignCustomerSegments", **_edited(),
    }


def generateMasterData(ctx: GenContext) -> None:
    generateStaticReference(ctx)
    generatePeopleAndTerritories(ctx)
    generateStockItems(ctx)
    generatePriceLists(ctx)
    generateCustomers(ctx)
    generateCities(ctx)
    generateCustomerSegments(ctx)
    generateSalesQuotas(ctx)
    generateSpecialDeals(ctx)
