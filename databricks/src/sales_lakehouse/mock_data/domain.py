"""Static domain vocabulary shared by the SQL Server and Oracle generators.

Values are lifted from the legacy seed scripts (``sqlserver/oltp/08_seed/*.sql``,
``oracle/reference/*.sql``) so that codes match what the legacy ETL expects."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Country:
    iso2: str
    iso3: str
    name: str
    region: str
    currency: str
    taxRegime: str  # SQL Server TaxRegimeCode used on territories/orders
    euVatArea: bool = False


COUNTRIES: dict[str, Country] = {c.iso2: c for c in [
    Country("US", "USA", "United States", "NA", "USD", "USSALESTAX"),
    Country("CA", "CAN", "Canada", "NA", "CAD", "CAGSTHST"),
    Country("GB", "GBR", "United Kingdom", "EU", "GBP", "UKVAT", True),
    Country("DE", "DEU", "Germany", "EU", "EUR", "EUVAT", True),
    Country("NL", "NLD", "Netherlands", "EU", "EUR", "EUVAT", True),
    Country("FR", "FRA", "France", "EU", "EUR", "EUVAT", True),
    Country("AU", "AUS", "Australia", "APAC", "AUD", "AUGST"),
    Country("SG", "SGP", "Singapore", "APAC", "SGD", "SGGST"),
    Country("JP", "JPN", "Japan", "APAC", "JPY", "JPCT"),
]}


@dataclass(frozen=True)
class Territory:
    territoryId: int
    code: str
    name: str
    region: str
    countryIso2: str | None
    parentCode: str | None
    level: int
    fiscalCalendar: str
    postalStandard: str
    # LEGACY QUIRK: Integration.usp_LoadBridgeEmployeeTerritory allocates 100% of a
    # distributor-managed APAC territory to the channel manager.
    distributorManaged: bool = False


TERRITORIES: list[Territory] = [
    Territory(1, "NA", "North America", "NA", None, None, 1, "NA445", "USPS"),
    Territory(2, "EU", "Europe", "EU", None, None, 1, "EUCAL", "EUDIN5008"),
    Territory(3, "APAC", "Asia Pacific", "APAC", None, None, 1, "APACJUN", "AUSPOST"),
    Territory(10, "NA-US-EAST", "United States East", "NA", "US", "NA", 2, "NA445", "USPS"),
    Territory(11, "NA-US-WEST", "United States West", "NA", "US", "NA", 2, "NA445", "USPS"),
    Territory(12, "NA-CA", "Canada", "NA", "CA", "NA", 2, "NA445", "CANADAPOST"),
    Territory(20, "EU-UK", "United Kingdom", "EU", "GB", "EU", 2, "EUCAL", "ROYALMAIL"),
    Territory(21, "EU-DE", "Germany", "EU", "DE", "EU", 2, "EUCAL", "EUDIN5008"),
    Territory(22, "EU-NL", "Netherlands", "EU", "NL", "EU", 2, "EUCAL", "EUDIN5008"),
    Territory(23, "EU-FR", "France", "EU", "FR", "EU", 2, "EUCAL", "EUDIN5008"),
    Territory(30, "AP-AU", "Australia", "APAC", "AU", "APAC", 2, "APACJUN", "AUSPOST"),
    Territory(31, "AP-SG", "Singapore", "APAC", "SG", "APAC", 2, "APACJUN", "AUSPOST", True),
    Territory(32, "AP-JP", "Japan", "APAC", "JP", "APAC", 2, "APACJUN", "JPPOST"),
]
LEAF_TERRITORIES = [t for t in TERRITORIES if t.level == 2]
# Region weights are applied to leaf territories so the customer mix lands at 40/35/25.
TERRITORY_WEIGHTS_IN_REGION = {
    "NA-US-EAST": 0.40, "NA-US-WEST": 0.35, "NA-CA": 0.25,
    "EU-UK": 0.30, "EU-DE": 0.30, "EU-NL": 0.20, "EU-FR": 0.20,
    "AP-AU": 0.45, "AP-SG": 0.25, "AP-JP": 0.30,
}


@dataclass(frozen=True)
class Channel:
    channelId: int
    code: str
    name: str
    channelClass: str
    region: str
    status: str
    partnerIdentifier: str | None
    defaultPriceListCode: str
    commissionModifierPercent: str
    requiresManualApproval: bool
    orderPrefix: str
    validFrom: str
    validTo: str | None = None
    conformedCode: str | None = "DIRECT"  # None => deliberately untranslated in CODE_TRANSLATION


CHANNELS: list[Channel] = [
    Channel(1, "FIELDNA", "Field sales - North America", "FIELD", "NA", "ACTIVE", None, "NA-STD-USD", "0.00", False, "NAF", "2005-01-01", conformedCode="FIELD"),
    Channel(2, "TELENA", "Inside sales desk - North America", "CALLCENTRE", "NA", "ACTIVE", None, "NA-STD-USD", "-0.25", False, "NAT", "2005-01-01", conformedCode="INSIDE"),
    Channel(3, "WEBNA", "Web store - North America", "WEB", "NA", "ACTIVE", None, "NA-WEB-USD", "-0.50", False, "NAW", "2012-03-01", conformedCode="WEB"),
    Channel(4, "EDINA", "EDI trading partners - North America", "EDI", "NA", "ACTIVE", "EDI-NA-001", "NA-STD-USD", "-0.75", False, "NAE", "2009-06-01", conformedCode="EDI"),
    Channel(5, "FIELDEU", "Field sales - Europe", "FIELD", "EU", "ACTIVE", None, "EU-STD-EUR", "0.00", False, "EUF", "2005-01-01", conformedCode="FIELD"),
    Channel(6, "WEBEU", "Web store - Europe", "WEB", "EU", "ACTIVE", None, "EU-WEB-EUR", "-0.50", False, "EUW", "2013-01-01", conformedCode="WEB"),
    Channel(7, "DISTEU", "Distributor network - Europe", "DISTRIBUTOR", "EU", "ACTIVE", "DIST-EU-007", "EU-STD-EUR", "-1.00", True, "EUD", "2016-01-01", conformedCode="DISTRIBUTOR"),
    Channel(8, "FIELDAP", "Field sales - APAC", "FIELD", "APAC", "ACTIVE", None, "AP-STD-AUD", "0.00", False, "APF", "2008-07-01", conformedCode="FIELD"),
    Channel(9, "WEBAP", "Web store - APAC", "WEB", "APAC", "ACTIVE", None, "AP-WEB-AUD", "-0.50", False, "APW", "2015-07-01", conformedCode="WEB"),
    # LEGACY QUIRK: PILOT channels are orderable but commission processing skips them.
    Channel(10, "MARKETAP", "Marketplace partner feed - APAC", "MARKETPLACE", "APAC", "PILOT", "MKT-AP-042", "AP-WEB-AUD", "-2.00", True, "APM", "2024-07-01", conformedCode=None),
    Channel(11, "FAXAP", "Fax and phone order desk - APAC", "CALLCENTRE", "APAC", "CLOSED", None, "AP-STD-AUD", "0.00", True, "APX", "2008-07-01", "2019-06-30", conformedCode="INSIDE"),
]
ACTIVE_CHANNELS_BY_REGION = {r: [c for c in CHANNELS if c.region == r and c.status != "CLOSED"] for r in ("NA", "EU", "APAC")}

PAYMENT_METHOD_CODES = ["BANKXFER", "DIRECTDEBIT", "CARD", "CHEQUE", "CASH", "BILLOFEXCH", "OFFSET"]
# BILLOFEXCH is deliberately absent from Oracle CODE_TRANSLATION / PAYMENT_METHOD_REF.
UNTRANSLATED_PAYMENT_METHOD = "BILLOFEXCH"
PAYMENT_METHOD_TRANSLATION = {"BANKXFER": "EFT", "DIRECTDEBIT": "DD", "CARD": "CARD", "CHEQUE": "CHQ", "CASH": "CASH", "OFFSET": "CONTRA"}
REGION_PAYMENT_METHODS = {
    "NA": ["BANKXFER", "CARD", "CHEQUE", "BANKXFER", "OFFSET"],
    "EU": ["BANKXFER", "DIRECTDEBIT", "CARD", "BANKXFER", "OFFSET"],
    "APAC": ["BANKXFER", "CARD", "BILLOFEXCH", "CASH", "BANKXFER"],
}

# WWI base Application.PaymentMethods / TransactionTypes ids.
WWI_PAYMENT_METHODS = {1: "Cash", 2: "Check", 3: "Credit-Card", 4: "EFT"}
WWI_TRANSACTION_TYPES = {1: "Customer Invoice", 2: "Customer Credit Note", 3: "Customer Payment Received", 4: "Customer Refund"}
PAYMENT_METHOD_TO_WWI_ID = {"BANKXFER": 4, "DIRECTDEBIT": 4, "CARD": 3, "CHEQUE": 2, "CASH": 1, "BILLOFEXCH": 2, "OFFSET": 4}

# (ReasonCode, Region, Description, Category, IsCustomerFault, DefaultRestockingApplies, RestockingPercent,
#  RequiresInspection, RequiresPhotoEvidence, AllowsResale, ReturnWindowDays, SupplierRecoverable)
RETURN_REASONS = [
    ("COM", "NA", "Change of mind - no fault found", "CHANGEOFMIND", 1, 1, "15.00", 1, 0, 1, 30, 0),
    ("DAMTR", "NA", "Damaged in transit", "DAMAGE", 0, 0, None, 1, 1, 0, 90, 1),
    ("PICKER", "NA", "Warehouse picked the wrong item", "PICKERROR", 0, 0, None, 1, 0, 1, 120, 0),
    ("QUAL", "NA", "Quality below specification", "QUALITY", 0, 0, None, 1, 1, 0, 365, 1),
    ("RECALL", "NA", "Supplier product recall", "RECALL", 0, 0, None, 0, 0, 0, None, 1),
    ("COOL", "EU", "Statutory cooling-off cancellation", "CHANGEOFMIND", 1, 0, None, 0, 0, 1, 14, 0),
    ("COM", "EU", "Change of mind after cooling-off period", "CHANGEOFMIND", 1, 1, "10.00", 1, 0, 1, 30, 0),
    ("DAMTR", "EU", "Damaged in transit", "DAMAGE", 0, 0, None, 1, 1, 0, 90, 1),
    ("CONFORM", "EU", "Goods not in conformity with contract", "QUALITY", 0, 0, None, 1, 1, 0, 730, 1),
    ("LATEEU", "EU", "Delivered outside agreed delivery window", "LATE", 0, 0, None, 0, 0, 1, 30, 1),
    ("COM", "APAC", "Change of mind - no fault found", "CHANGEOFMIND", 1, 1, "20.00", 1, 0, 1, 7, 0),
    ("DAMTR", "APAC", "Damaged in transit", "DAMAGE", 0, 0, None, 1, 1, 0, 60, 1),
    ("QUAL", "APAC", "Quality below specification", "QUALITY", 0, 0, None, 1, 1, 0, 180, 1),
    ("CUSTAP", "APAC", "Rejected at customs or quarantine", "OTHER", 0, 0, None, 1, 1, 0, 45, 0),
]
UNTRANSLATED_RETURN_REASON = ("CUSTAP", "APAC")
STALE_RETURN_REASON = ("LATEEU", "EU")
RETURN_REASON_TRANSLATION = {"COM": "CHANGE_OF_MIND", "DAMTR": "TRANSIT_DAMAGE", "PICKER": "PICK_ERROR", "QUAL": "QUALITY",
                             "RECALL": "RECALL", "COOL": "COOLING_OFF", "CONFORM": "QUALITY", "LATEEU": "LATE_DELIVERY"}

# (PlanCode, PlanName, Region, Basis, B1Upper, B1Rate, B2Upper, B2Rate, B3Upper, B3Rate, Accelerator, Clawback, MinMargin, From, To)
COMMISSION_PLANS = [
    ("NA-FIELD-2019", "NA field sales plan (2019 revision)", "NA", "INVOICEDMARGIN", "80.00", "1.50", "100.00", "2.50", "130.00", "3.50", "1.00", 90, "12.00", "2019-01-01", None),
    ("NA-TELE-2019", "NA call centre plan", "NA", "NETREVENUE", "100.00", "0.75", "140.00", "1.10", None, None, None, 60, "8.00", "2019-01-01", None),
    ("NA-FIELD-2012", "NA field sales plan (superseded)", "NA", "INVOICEDMARGIN", "90.00", "1.25", "120.00", "2.00", None, None, None, 30, "10.00", "2012-01-01", "2018-12-31"),
    ("EU-FIELD-2016", "EU field sales plan", "EU", "NETREVENUE", "85.00", "1.20", "105.00", "2.00", "125.00", "2.60", None, 180, None, "2016-01-01", None),
    ("EU-DIST-2016", "EU distributor management plan", "EU", "NETREVENUE", "100.00", "0.60", None, None, None, None, None, 180, None, "2016-01-01", None),
    # Band3UpperPercent is the statutory cap for this plan: attainment above 120% earns nothing extra.
    ("EU-FIELD-CAP-2024", "EU field sales plan (capped, works council agreement)", "EU", "NETREVENUE", "90.00", "1.00", "110.00", "1.80", "120.00", "2.20", None, 180, None, "2024-01-01", None),
    ("AP-FIELD-2020", "APAC field sales plan", "APAC", "COLLECTEDCASH", "90.00", "1.00", "115.00", "1.80", None, None, "0.50", 120, "9.00", "2020-07-01", None),
    ("AP-MARKET-2020", "APAC marketplace partner plan", "APAC", "COLLECTEDCASH", "100.00", "0.40", None, None, None, None, None, 120, None, "2020-07-01", None),
]
CAPPED_EU_PLAN = "EU-FIELD-CAP-2024"

CUSTOMER_CATEGORIES = {1: "Agent", 2: "Wholesaler", 3: "Novelty Shop", 4: "Supermarket", 5: "Computer Store",
                       6: "Gift Store", 7: "Corporate", 8: "General Retailer"}
BUYING_GROUPS = {1: "Tailspin Toys", 2: "Wingtip Toys", 3: "Northwind Traders", 4: "Contoso Retail Alliance"}
STOCK_GROUPS = {1: "Novelty Items", 2: "Clothing", 3: "Mugs", 4: "T-Shirts", 5: "Airline Novelties", 6: "Computing Novelties",
                7: "USB Novelties", 8: "Furry Footwear", 9: "Toys", 10: "Packaging Materials"}
PACKAGE_TYPES = {1: "Bag", 2: "Block", 3: "Bottle", 4: "Box", 5: "Can", 6: "Carton", 7: "Each", 8: "Kg", 9: "Packet", 10: "Pair", 11: "Pallet", 12: "Tray", 13: "Tub", 14: "Tube"}
DELIVERY_METHODS = {1: "Post", 2: "Courier", 3: "Delivery Van", 4: "Customer Collect", 5: "Air Freight", 6: "Road Freight", 7: "Refrigerated Van"}

ORDER_STATUSES = ["ENTERED", "CONFIRMED", "HOLD", "ALLOCATED", "PICKING", "PARTSHIP", "SHIPPED", "INVOICED", "CANCELLED"]
INVOICE_SETTLEMENT_STATUSES = ["OPEN", "PARTPAID", "PAID", "DISPUTED", "WRITTENOFF", "CREDITED"]

# Standard tax rate percent by customer country; Orders/Invoices carry EU_RC for reverse charge.
STANDARD_TAX_RATE = {"US": "8.25", "CA": "13.00", "GB": "20.00", "DE": "19.00", "NL": "21.00", "FR": "20.00",
                     "AU": "10.00", "SG": "9.00", "JP": "10.00"}

FIRST_NAMES = ["Amelia", "Benjamin", "Chloe", "Daniel", "Elena", "Felix", "Grace", "Hugo", "Isla", "Jonas", "Keiko", "Liam",
               "Mia", "Noah", "Olivia", "Priya", "Quentin", "Rosa", "Sven", "Tara", "Umar", "Vera", "Wei", "Xavier", "Yara", "Zoe"]
LAST_NAMES = ["Anderson", "Bakker", "Chen", "Dubois", "Evans", "Fischer", "Garcia", "Hansen", "Ito", "Jansen", "Kowalski",
              "Lambert", "Meyer", "Nakamura", "Okafor", "Patel", "Quinn", "Rossi", "Schmidt", "Tan", "Underwood", "Visser",
              "Wagner", "Xu", "Young", "Zimmermann"]
COMPANY_STEMS = ["Tailspin", "Wingtip", "Northwind", "Contoso", "Fabrikam", "Adventure", "Litware", "Proseware", "Woodgrove",
                 "Coho", "Alpine", "Bellows", "Consolidated", "Datum", "Graphic", "Humongous", "Lucerne", "Margie", "Trey",
                 "Wide World", "Blue Yonder", "City Power", "Fourth Coffee", "Southridge", "Tailwind"]
COMPANY_SUFFIXES = {"NA": ["Inc", "LLC", "Corp", "Co"], "EU": ["GmbH", "Ltd", "BV", "SARL"], "APAC": ["Pty Ltd", "Pte Ltd", "KK", "Ltd"]}
CITIES = {"US": ["New York", "Chicago", "Los Angeles", "Seattle", "Denver"], "CA": ["Toronto", "Vancouver", "Montreal"],
          "GB": ["London", "Manchester", "Leeds"], "DE": ["Berlin", "Hamburg", "Munich"], "NL": ["Amsterdam", "Rotterdam"],
          "FR": ["Paris", "Lyon", "Lille"], "AU": ["Sydney", "Melbourne", "Perth"], "SG": ["Singapore"], "JP": ["Tokyo", "Osaka", "Nagoya"]}
STATES = {"US": ["NY", "IL", "CA", "WA", "CO"], "CA": ["ON", "BC", "QC"]}
ITEM_ADJECTIVES = ["USB", "Novelty", "Animal", "Dinosaur", "Superhero", "Alien", "Ride-on", "Furry", "Chocolate", "Halloween",
                   "Shipping", "Office", "Air cushion", "Developer joke", "IT joke", "Black and orange", "Small", "Large"]
ITEM_NOUNS = ["mug", "t-shirt", "hoodie", "slippers", "toy", "cube", "keyring", "bubble wrap", "carton", "tape", "stickers",
              "lanyard", "hat", "gloves", "socks", "pen", "notebook", "backpack", "umbrella", "puzzle"]

# (SegmentCode, SegmentName, SegmentFamily, RegionCode, ConsentRequired, RetentionMonths) - Sales.CustomerSegments header:
# NA opt-out / 84 months, EU opt-in / 24 months on behavioural attributes, APAC per country.
CUSTOMER_SEGMENTS = [
    ("HIVAL", "High value", "VALUE", "NA", False, 84),
    ("FREQ", "Frequent buyer", "BEHAVIOUR", "NA", False, 84),
    ("CHURN", "Churn risk", "RISK", "NA", False, 84),
    ("HIVAL", "High value", "VALUE", "EU", False, 24),
    ("FREQ", "Frequent buyer", "BEHAVIOUR", "EU", True, 24),
    ("NEWCUST", "New customer", "LIFECYCLE", "EU", True, 24),
    ("HIVAL", "High value", "VALUE", "APAC", False, 60),
    ("WEBFIRST", "Web-first buyer", "CHANNEL", "APAC", True, 36),
    ("CHURN", "Churn risk", "RISK", "APAC", False, 60),
]
