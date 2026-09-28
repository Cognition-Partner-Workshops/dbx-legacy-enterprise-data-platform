"""Oracle source generators: WWI_REF reference/FX/calendars, WWI_FIN tax + GL periods,
WWI_MDM customer master / party cross-reference / merge history / product master.

``generateReference`` runs *before* the SQL Server generators (orders need FX rates);
``generateMdm`` runs *after* them (MDM mirrors the WWI customers and stock items)."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from . import calendars
from .common import CURRENCIES, GenContext, Row, addMonths, daysBetween, money, rate
from .domain import (
    CHANNELS,
    CITIES,
    COMMISSION_PLANS,
    COUNTRIES,
    FIRST_NAMES,
    INVOICE_SETTLEMENT_STATUSES,
    LAST_NAMES,
    ORDER_STATUSES,
    PAYMENT_METHOD_CODES,
    PAYMENT_METHOD_TRANSLATION,
    REGION_PAYMENT_METHODS,
    RETURN_REASON_TRANSLATION,
    RETURN_REASONS,
    STALE_RETURN_REASON,
    STATES,
    UNTRANSLATED_PAYMENT_METHOD,
    UNTRANSLATED_RETURN_REASON,
)
from .schema import ORACLE

MDM = "WWI_MDM"
REF = "WWI_REF"
FIN = "WWI_FIN"

# Base rates FROM currency TO USD; NA loads 'CORP', EU 'ECB', APAC 'BANK' (oracle/reference/02).
FX_BASE = {"CAD": "0.7300", "EUR": "1.0800", "GBP": "1.2700", "AUD": "0.6600", "SGD": "0.7400", "JPY": "0.0067"}
FX_FEED = {"CAD": ("CORP", "TREASURY", "NA"), "EUR": ("ECB", "ECB", "EU"), "GBP": ("ECB", "ECB", "EU"),
           "AUD": ("BANK", "WESTPAC", "APAC"), "SGD": ("BANK", "DBS", "APAC"), "JPY": ("BANK", "MUFG", "APAC")}
FX_CURRENCY_NAMES = {"USD": "US Dollar", "CAD": "Canadian Dollar", "EUR": "Euro", "GBP": "Pound Sterling",
                     "AUD": "Australian Dollar", "SGD": "Singapore Dollar", "JPY": "Yen"}
CURRENCY_NUM = {"USD": "840", "CAD": "124", "EUR": "978", "GBP": "826", "AUD": "036", "SGD": "702", "JPY": "392"}

SOURCE_SYSTEMS = [
    ("WWI_SQL", "WideWorldImporters OLTP", "ERP", "Microsoft", "Sales Systems", None, "PULL", "DAILY", "WWI_OLTP_CONN", "UTC"),
    ("ORA_ERP", "Corporate ERP (Oracle)", "ERP", "Oracle", "Finance Systems", None, "PULL", "DAILY", "ORA_ERP_CONN", "UTC"),
    ("MDM_HUB", "Customer MDM hub", "CRM", "Informatica", "Data Governance", None, "PUSH", "HOURLY", "MDM_CONN", "UTC"),
    ("ECB_FEED", "ECB reference rates", "FEED", "European Central Bank", "Treasury", "EU", "FILE", "DAILY", None, "Europe/Frankfurt"),
    ("APAC_BANK", "APAC bank rate feed", "FEED", "Bank consortium", "Treasury APAC", "APAC", "FILE", "DAILY", None, "Australia/Sydney"),
    ("TREASURY", "NA corporate treasury rate sheet", "MANUAL", None, "Treasury NA", "NA", "MANUAL", "MONTHLY", None, "America/New_York"),
    ("MF_LEGACY", "Mainframe customer file (decommissioned)", "MAINFRM", "IBM", "Legacy Ops", "NA", "FILE", "WEEKLY", None, "America/Chicago"),
]


def _audit(createdBy: str, createdDt: date, updatedBy: str | None = None, updatedDt: date | None = None) -> Row:
    return {"CREATED_BY": createdBy, "CREATED_DT": createdDt, "UPDATED_BY": updatedBy, "UPDATED_DT": updatedDt}


def _flag(value: bool) -> str:
    return "Y" if value else "N"


# --------------------------------------------------------------------------------
# WWI_REF static reference
# --------------------------------------------------------------------------------
def generateRegionCountryCurrency(ctx: GenContext) -> None:
    seeded = date(2004, 1, 15)
    regions = []
    for code, name, curr, fyMonth, cal, regime, addr, postal, mask, lang, consent, retention, tz in [
        ("NA", "North America", "USD", 11, "NA445", "SALES", "US", "ZIP", "MM/DD/YYYY", "en-US", "CANSPAM", 84, "America/New_York"),
        ("EU", "Europe", "EUR", 1, "EUCAL", "VAT", "DIN5008", "EUPOST", "DD.MM.YYYY", "en-GB", "GDPR", 24, "Europe/Amsterdam"),
        ("APAC", "Asia Pacific", "AUD", 7, "APACJUN", "GST", "AU", "AUPOST", "DD/MM/YYYY", "en-AU", "PRIVACYACT", 84, "Australia/Sydney"),
    ]:
        regions.append({"REGION_CD": code, "REGION_NAME": name, "REPORTING_CURR_CD": curr, "FISCAL_YEAR_START_MONTH": fyMonth,
                        "FISCAL_CALENDAR_CD": cal, "TAX_REGIME_CD": regime, "ADDRESS_FORMAT_CD": addr, "POSTAL_FORMAT_CD": postal,
                        "DATE_FORMAT_MASK": mask, "DEFAULT_LANGUAGE_CD": lang, "CONSENT_REGIME_CD": consent,
                        "RETENTION_MONTHS": retention, "TIMEZONE_TXT": tz, "ACTIVE_FLG": "Y", "RETIRED_DT": None,
                        "SOURCE_SYS": "ORA_ERP", **_audit("SEED", seeded)})
    ctx.put(ORACLE, REF, "REGION_REF", regions)

    countries = []
    for c in COUNTRIES.values():
        countries.append({"COUNTRY_CD": c.iso2, "COUNTRY_CD_3": c.iso3, "COUNTRY_NUM_CD": None, "LEGACY_MAINFRAME_CD": c.iso3[:3],
                          "COUNTRY_NAME": c.name, "OFFICIAL_NAME": c.name, "REGION_CD": c.region, "SUB_REGION_TXT": None,
                          "DEFAULT_CURR_CD": c.currency, "DEFAULT_LANGUAGE_CD": None, "EU_MEMBER_FLG": _flag(c.euVatArea and c.iso2 != "GB"),
                          "EU_VAT_AREA_FLG": _flag(c.euVatArea), "VAT_NBR_FORMAT_TXT": f"{c.iso2}999999999" if c.euVatArea else None,
                          "POSTAL_FORMAT_TXT": None, "POSTAL_REQUIRED_FLG": "Y", "STATE_PROV_REQUIRED_FLG": _flag(c.iso2 in STATES),
                          "PHONE_PREFIX_CD": None, "SANCTIONED_FLG": "N", "TRADE_BLOC_CD": "EU" if c.euVatArea and c.iso2 != "GB" else None,
                          "ACTIVE_FLG": "Y", "SOURCE_SYS": "ORA_ERP", **_audit("SEED", seeded)})
    ctx.put(ORACLE, REF, "COUNTRY_REF", countries)

    currencies = []
    for seq, curr in enumerate(CURRENCIES, start=1):
        primary = next(c.iso2 for c in COUNTRIES.values() if c.currency == curr)
        currencies.append({"CURR_CD": curr, "CURR_NUM_CD": CURRENCY_NUM[curr], "CURR_NAME": FX_CURRENCY_NAMES[curr], "CURR_SYMBOL": None,
                           # JPY has no minor unit; oracle/reference/02 notes the legacy rounding ignored this.
                           "MINOR_UNIT_DIGITS": 0 if curr == "JPY" else 2, "ROUNDING_RULE_CD": "HALFUP",
                           "PRIMARY_COUNTRY_CD": primary, "REGION_CD": COUNTRIES[primary].region, "EURO_LEGACY_FLG": "N",
                           "EURO_FIXED_RATE": None, "EURO_CONVERSION_DT": None, "ACTIVE_FLG": "Y", "TRADING_ALLOWED_FLG": "Y",
                           "DISPLAY_SEQ_NBR": seq, "RETIRED_DT": None, "SOURCE_SYS": "ORA_ERP", **_audit("SEED", seeded)})
    ctx.put(ORACLE, REF, "CURRENCY_CODE", currencies)

    systems = []
    for code, name, sysType, vendor, team, region, mode, freq, param, tz in SOURCE_SYSTEMS:
        decommissioned = date(2019, 12, 31) if code == "MF_LEGACY" else None
        systems.append({"SOURCE_SYS_CD": code, "SYSTEM_NAME": name, "SYSTEM_TYPE_CD": sysType, "VENDOR_TXT": vendor, "OWNING_TEAM_TXT": team,
                        "REGION_CD": region, "INTERFACE_MODE_CD": mode, "INTERFACE_FREQUENCY_CD": freq, "CONNECTION_PARAM_NAME": param,
                        "TIMEZONE_TXT": tz, "COMMISSIONED_DT": seeded, "DECOMMISSIONED_DT": decommissioned,
                        "ACTIVE_FLG": _flag(decommissioned is None), "TRUSTED_SOURCE_FLG": _flag(sysType != "MANUAL"), "NOTES_TXT": None,
                        **_audit("SEED", seeded)})
    ctx.put(ORACLE, REF, "SOURCE_SYSTEM_REF", systems)


def generateFxRates(ctx: GenContext) -> None:
    rng = ctx.rng("oracle.fx")
    spanStart, spanEnd = ctx.spanStart, ctx.spanEnd
    days = list(daysBetween(spanStart, spanEnd))

    # Deliberate gaps.
    weekendGapCurrency = "GBP"
    firstSaturday = next(d for d in days[30:] if d.weekday() == 5)
    weekendGap = {firstSaturday, firstSaturday + timedelta(days=1)}
    monthGapCurrency = "SGD"
    gapMonthStart = addMonths(date(spanStart.year, spanStart.month, 1), 7)
    gapMonthEnd = addMonths(gapMonthStart, 1) - timedelta(days=1)
    randomGaps: set[tuple[str, date]] = set()
    weekdays = [d for d in days if d.weekday() < 5]
    while len(randomGaps) < 12:
        curr = rng.choice(["CAD", "EUR", "AUD", "JPY"])
        day = rng.choice(weekdays[7:-7])
        randomGaps.add((curr, day))

    trueRates: dict[tuple[str, date], Decimal] = {}
    rows: list[Row] = []
    for curr, base in FX_BASE.items():
        rateType, sourceCode, feedRegion = FX_FEED[curr]
        level = Decimal(base)
        drift = Decimal(base) * Decimal("0.004")
        monthlyCorpRate: Decimal | None = None
        for day in days:
            level += Decimal(rng.uniform(-1, 1)) * drift
            level = max(level, Decimal(base) * Decimal("0.8"))
            if rateType == "CORP":
                # LEGACY QUIRK: NA treasury publishes one corporate rate per month and the loader
                # repeats it for every day of that month (oracle/reference/02_currency_and_fx_rates.sql).
                if day.day == 1 or monthlyCorpRate is None:
                    monthlyCorpRate = rate(level)
                dailyRate = monthlyCorpRate
            else:
                dailyRate = rate(level)
            trueRates[(curr, day)] = dailyRate
            trueRates[("USD", day)] = Decimal(1)

            gapReason = None
            if curr == weekendGapCurrency and day in weekendGap:
                gapReason = "MISSING_FX_WEEKEND"
            elif curr == monthGapCurrency and gapMonthStart <= day <= gapMonthEnd:
                gapReason = "MISSING_FX_MONTH"
            elif (curr, day) in randomGaps:
                gapReason = "MISSING_FX_RATE"
            if gapReason:
                descriptions = {
                    "MISSING_FX_WEEKEND": "A complete weekend (Sat+Sun) with no FX rows for one currency.",
                    "MISSING_FX_MONTH": "One currency has no FX rows for an entire calendar month.",
                    "MISSING_FX_RATE": "Isolated (currency, date) pairs with no FX row.",
                }
                ctx.tag(gapReason, descriptions[gapReason], f"{REF}.FX_RATE_DAILY", FROM_CURR_CD=curr, TO_CURR_CD="USD", RATE_DT=day)
                continue
            spread = dailyRate * Decimal("0.002") if rateType == "BANK" else None
            rows.append({
                "FROM_CURR_CD": curr, "TO_CURR_CD": "USD", "RATE_DT": day, "RATE_TYPE_CD": rateType, "RATE": dailyRate,
                "INVERSE_RATE": rate(Decimal(1) / dailyRate), "BID_RATE": rate(dailyRate - spread) if spread else None,
                "ASK_RATE": rate(dailyRate + spread) if spread else None, "RATE_SOURCE_CD": sourceCode, "FEED_REGION_CD": feedRegion,
                "INTERPOLATED_FLG": "N", "LOADED_TS": datetime(day.year, day.month, day.day, 6, 5, 0) + timedelta(days=1),
                "SUPERSEDED_FLG": "N", "SOURCE_SYS": "ORA_ERP", **_audit("FXLOAD", day + timedelta(days=1)),
            })
    ctx.put(ORACLE, REF, "FX_RATE_DAILY", rows)
    ctx.scratch.fxRates = trueRates
    ctx.scratch.fxGapMonth = (monthGapCurrency, gapMonthStart, gapMonthEnd)


def generateCalendars(ctx: GenContext) -> None:
    rows: list[Row] = []
    glRows: list[Row] = []
    holidays = {(1, 1): "New Year's Day", (12, 25): "Christmas Day", (12, 26): "Boxing Day"}
    for calendarCode in (calendars.NA445, calendars.EUCAL, calendars.APACJUN):
        region = calendars.CALENDAR_REGION[calendarCode]
        yearStart = calendars.fiscalYearStart(calendarCode, ctx.spanStart)
        yearEnd = calendars.fiscalPeriodFor(calendarCode, ctx.spanEnd).yearEnd
        for day in daysBetween(yearStart, yearEnd):
            fp = calendars.fiscalPeriodFor(calendarCode, day)
            holiday = holidays.get((day.month, day.day))
            rows.append({
                "CALENDAR_CD": calendarCode, "CALENDAR_DT": day, "FISCAL_YEAR_NBR": fp.fiscalYear, "FISCAL_QUARTER_NBR": fp.quarter,
                "FISCAL_PERIOD_NBR": fp.period, "PERIOD_CD": fp.periodCode, "FISCAL_WEEK_NBR": fp.week, "DAY_OF_PERIOD_NBR": fp.dayOfPeriod,
                "PERIOD_START_DT": fp.periodStart, "PERIOD_END_DT": fp.periodEnd, "QUARTER_START_DT": fp.quarterStart,
                "QUARTER_END_DT": fp.quarterEnd, "YEAR_START_DT": fp.yearStart, "YEAR_END_DT": fp.yearEnd,
                "WORKING_DAY_FLG": _flag(day.weekday() < 5 and holiday is None), "HOLIDAY_FLG": _flag(holiday is not None),
                "HOLIDAY_NAME": holiday, "HOLIDAY_COUNTRY_CD": None, "ADJUSTMENT_PERIOD_FLG": "N", "REGION_CD": region,
                "SOURCE_SYS": "ORA_ERP", **_audit("CALGEN", yearStart - timedelta(days=60)),
            })
        ledger = {"NA": "NA_GL", "EU": "EU_GL", "APAC": "AP_GL"}[region]
        for fp in calendars.periodsBetween(calendarCode, yearStart, yearEnd):
            if fp.periodEnd < ctx.asOf - timedelta(days=45):
                status, closedBy, closedDt = "CLSD", "GLCLOSE", fp.periodEnd + timedelta(days=12)
            elif fp.periodStart > ctx.asOf:
                status, closedBy, closedDt = "FUTR", None, None
            else:
                status, closedBy, closedDt = "OPEN", None, None
            glRows.append({
                "LEDGER_CD": ledger, "PERIOD_CD": fp.periodCode, "FISCAL_YEAR_NBR": fp.fiscalYear, "PERIOD_NBR": fp.period, "REGION_CD": region,
                "PERIOD_START_DT": fp.periodStart, "PERIOD_END_DT": fp.periodEnd, "ADJUSTMENT_PERIOD_FLG": "N", "AP_STATUS_CD": status,
                "GL_STATUS_CD": status, "PO_STATUS_CD": status, "CLOSED_BY_CD": closedBy, "CLOSED_DT": closedDt, "REOPENED_CNT": 0,
                "REOPEN_REASON_TXT": None, "SOFT_CLOSE_DT": fp.periodEnd + timedelta(days=5) if status == "CLSD" else None,
                "SOURCE_SYS": "ORA_ERP", **_audit("GLCLOSE", yearStart - timedelta(days=60)),
            })
    ctx.put(ORACLE, REF, "CALENDAR_FISCAL", rows)
    ctx.put(ORACLE, FIN, "GL_PERIOD_STATUS", glRows)


def generateTax(ctx: GenContext) -> None:
    seeded = date(2004, 1, 15)
    effectiveFrom = date(2019, 1, 1)
    jurisdictions: list[Row] = []
    rates: list[Row] = []
    jurisdictionId = 0
    rateId = 0

    def addJurisdiction(code: str, name: str, region: str, country: str, level: str, regime: str, parent: str | None,
                        state: str | None = None, county: str | None = None, city: str | None = None) -> None:
        nonlocal jurisdictionId
        jurisdictionId += 1
        jurisdictions.append({
            "TAX_JURISDICTION_ID": jurisdictionId, "JURISDICTION_CD": code, "JURISDICTION_NAME": name, "REGION_CD": region,
            "COUNTRY_CD": country, "STATE_PROV_CD": state, "COUNTY_TXT": county, "CITY_TXT": city, "POSTAL_FROM_CD": None,
            "POSTAL_TO_CD": None, "JURISDICTION_LEVEL_CD": level, "TAX_REGIME_CD": regime,
            "STACKS_WITH_PARENT_FLG": _flag(parent is not None and region == "NA"), "PARENT_JURISDICTION_CD": parent,
            "REGISTRATION_REQ_FLG": "Y", "OUR_REGISTRATION_NBR": f"REG-{code}", "FILING_FREQUENCY_CD": "MTH" if region == "EU" else "QTR",
            "FILING_DUE_DAY_NBR": 20, "AUTHORITY_NAME": f"{name} tax authority", "EFFECTIVE_FROM_DT": effectiveFrom,
            "EFFECTIVE_TO_DT": None, "ACTIVE_FLG": "Y", "SOURCE_SYS": "ORA_ERP", **_audit("TAXSEED", seeded),
        })

    def addRate(taxCode: str, jurisdiction: str, region: str, regime: str, category: str, pct: str, reverseCharge: bool = False,
                compoundSeq: int | None = None, desc: str | None = None, effectiveTo: date | None = None) -> None:
        nonlocal rateId
        rateId += 1
        rates.append({
            "TAX_RATE_ID": rateId, "TAX_CODE_CD": taxCode, "JURISDICTION_CD": jurisdiction, "REGION_CD": region, "TAX_REGIME_CD": regime,
            "RATE_CATEGORY_CD": category, "RATE_PCT": money(pct), "COMPOUND_FLG": _flag(compoundSeq is not None),
            "COMPOUND_SEQ_NBR": compoundSeq, "RECOVERABLE_PCT": money("100.00") if regime in ("VAT", "GST") else money("0.00"),
            "EFFECTIVE_FROM_DT": effectiveFrom, "EFFECTIVE_TO_DT": effectiveTo, "TAX_ACCOUNT_CD": f"2{region[:1]}{rateId:03d}",
            "REVERSE_CHARGE_FLG": _flag(reverseCharge), "SELF_ASSESS_FLG": _flag(reverseCharge), "REPORTING_BOX_CD": None,
            "PRODUCT_CATEGORY_CD": None, "RATE_DESC": desc or f"{taxCode} {category}", "LEGISLATION_REF_TXT": None,
            "ACTIVE_FLG": _flag(effectiveTo is None), "SOURCE_SYS": "ORA_ERP", **_audit("TAXSEED", seeded),
        })

    # NA: nested state / county / city / special district sales tax (oracle/reference/05).
    addJurisdiction("US", "United States", "NA", "US", "NATL", "SALES", None)
    for state, county, city, statePct, countyPct, cityPct, districtPct in [
        ("NY", "New York County", "New York", "4.00", "0.00", "4.50", "0.375"),
        ("IL", "Cook County", "Chicago", "6.25", "1.75", "1.25", "1.00"),
        ("CA", "Los Angeles County", "Los Angeles", "6.00", "0.25", "0.00", "3.25"),
        ("WA", "King County", "Seattle", "6.50", "0.00", "3.75", "0.00"),
        ("CO", "Denver County", "Denver", "2.90", "0.00", "4.81", "1.10"),
    ]:
        addJurisdiction(f"US-{state}", f"{state} state", "NA", "US", "STATE", "SALES", "US", state=state)
        addRate(f"US-{state}-STD", f"US-{state}", "NA", "SALES", "STD", statePct)
        addRate(f"US-{state}-RESALE", f"US-{state}", "NA", "SALES", "RESALE", "0.00")
        addJurisdiction(f"US-{state}-CTY", county, "NA", "US", "COUNTY", "SALES", f"US-{state}", state=state, county=county)
        addRate(f"US-{state}-CTY-STD", f"US-{state}-CTY", "NA", "SALES", "STD", countyPct)
        addJurisdiction(f"US-{state}-CITY", city, "NA", "US", "CITY", "SALES", f"US-{state}-CTY", state=state, county=county, city=city)
        addRate(f"US-{state}-CITY-STD", f"US-{state}-CITY", "NA", "SALES", "STD", cityPct)
        addJurisdiction(f"US-{state}-SPEC", f"{city} transit district", "NA", "US", "SPEC", "SALES", f"US-{state}-CITY", state=state, city=city)
        addRate(f"US-{state}-SPEC-STD", f"US-{state}-SPEC", "NA", "SALES", "STD", districtPct)
    addJurisdiction("CA", "Canada (federal GST)", "NA", "CA", "NATL", "GST", None)
    addRate("CA-GST", "CA", "NA", "GST", "STD", "5.00")
    for prov, pct, label in [("ON", "8.00", "Ontario HST provincial portion"), ("BC", "7.00", "British Columbia PST"), ("QC", "9.975", "Quebec QST")]:
        addJurisdiction(f"CA-{prov}", label, "NA", "CA", "STATE", "GST", "CA", state=prov)
        addRate(f"CA-{prov}-STD", f"CA-{prov}", "NA", "GST", "STD", pct, compoundSeq=2 if prov == "QC" else None)

    # EU: national VAT with standard / reduced / zero and the intra-community reverse charge (oracle/reference/06).
    for iso2, std, red1, red2 in [("GB", "20.00", "5.00", "0.00"), ("DE", "19.00", "7.00", None), ("NL", "21.00", "9.00", None), ("FR", "20.00", "10.00", "5.50")]:
        addJurisdiction(iso2, f"{COUNTRIES[iso2].name} VAT", "EU", iso2, "NATL", "VAT", None)
        addRate(f"{iso2}-VAT-STD", iso2, "EU", "VAT", "STD", std)
        addRate(f"{iso2}-VAT-RED1", iso2, "EU", "VAT", "RED1", red1)
        if red2 is not None:
            addRate(f"{iso2}-VAT-RED2", iso2, "EU", "VAT", "RED2", red2)
        addRate(f"{iso2}-VAT-ZERO", iso2, "EU", "VAT", "ZERO", "0.00")
        addRate(f"{iso2}-VAT-RC", iso2, "EU", "VAT", "ZERO", "0.00", reverseCharge=True,
                desc="Intra-community B2B supply: customer self-accounts VAT (reverse charge)")
        addRate(f"{iso2}-VAT-EXEMPT", iso2, "EU", "VAT", "EXEMPT", "0.00")
    # A superseded UK standard rate to give downstream an effective-dated join to get right.
    addRate("GB-VAT-STD-2010", "GB", "EU", "VAT", "STD", "17.50", effectiveTo=date(2011, 1, 3), desc="UK standard rate before 4 Jan 2011")

    # APAC: GST / consumption tax (oracle/reference/07).
    for iso2, regime, std, label in [("AU", "GST", "10.00", "Australia GST"), ("SG", "GST", "9.00", "Singapore GST"), ("JP", "CONS", "10.00", "Japan consumption tax")]:
        addJurisdiction(iso2, label, "APAC", iso2, "NATL", regime, None)
        addRate(f"{iso2}-{regime}-STD", iso2, "APAC", regime, "STD", std)
        addRate(f"{iso2}-{regime}-ZERO", iso2, "APAC", regime, "ZERO", "0.00", desc="GST-free / export")
        addRate(f"{iso2}-{regime}-INPUT", iso2, "APAC", regime, "INPUT", "0.00", desc="Input-taxed supply")
    addRate("SG-GST-STD-2023", "SG", "APAC", "GST", "STD", "8.00", effectiveTo=date(2024, 1, 1), desc="Singapore GST 8% during 2023")

    ctx.put(ORACLE, FIN, "TAX_JURISDICTION", jurisdictions)
    ctx.put(ORACLE, FIN, "TAX_RATE", rates)


def generateCodeReference(ctx: GenContext) -> None:
    seeded = date(2004, 1, 15)
    payRows: list[Row] = []
    for region, methods in REGION_PAYMENT_METHODS.items():
        for code in sorted(set(methods)):
            if code == UNTRANSLATED_PAYMENT_METHOD:
                continue  # deliberately absent from the Oracle reference data
            for country in [c for c in COUNTRIES.values() if c.region == region]:
                payRows.append({
                    "PAYMENT_METHOD_CD": code, "COUNTRY_CD": country.iso2, "METHOD_NAME": PAYMENT_METHOD_TRANSLATION[code], "REGION_CD": region,
                    "FILE_FORMAT_CD": "ISO20022" if code in ("BANKXFER", "DIRECTDEBIT") else None,
                    "SETTLEMENT_DAYS": {"BANKXFER": 1, "DIRECTDEBIT": 3, "CARD": 2, "CHEQUE": 5, "CASH": 0, "OFFSET": 0}[code],
                    "CUT_OFF_TIME_TXT": "16:00", "MIN_AMT": None, "MAX_AMT": money("5000.00") if code == "CASH" else None,
                    "METHOD_CURR_CD": country.currency, "BANK_CHARGE_AMT": money("12.50") if code == "BANKXFER" else money("0.00"),
                    "REQUIRES_IBAN_FLG": _flag(region == "EU" and code in ("BANKXFER", "DIRECTDEBIT")),
                    "REQUIRES_ROUTING_FLG": _flag(region == "NA" and code == "BANKXFER"), "REQUIRES_MANDATE_FLG": _flag(code == "DIRECTDEBIT"),
                    "REMITTANCE_ADVICE_FLG": _flag(code != "CASH"), "ACTIVE_FLG": "Y", "EFFECTIVE_FROM_DT": date(2010, 1, 1),
                    "EFFECTIVE_TO_DT": None, "SOURCE_SYS": "ORA_ERP", **_audit("PAYSEED", seeded),
                })
    ctx.put(ORACLE, REF, "PAYMENT_METHOD_REF", payRows)

    statusRows: list[Row] = []
    statusSets = {
        "ORDER": (ORDER_STATUSES, {"INVOICED", "CANCELLED"}),
        "INVOICE": (INVOICE_SETTLEMENT_STATUSES, {"PAID", "WRITTENOFF", "CREDITED"}),
        "PAYMENT": (["RECEIVED", "PARTALLOCATED", "ALLOCATED", "UNAPPLIED", "REVERSED", "BOUNCED"], {"ALLOCATED", "REVERSED", "BOUNCED"}),
        "SHIPMENT": (["PLANNED", "PICKING", "PACKED", "DESPATCHED", "INTRANSIT", "DELIVERED", "EXCEPTION", "CANCELLED"], {"DELIVERED", "CANCELLED"}),
        "RMA": (["REQUESTED", "APPROVED", "DECLINED", "AWAITINGGOODS", "RECEIVED", "CLOSED", "EXPIRED"], {"DECLINED", "CLOSED", "EXPIRED"}),
    }
    for entity, (codes, terminal) in statusSets.items():
        for seq, code in enumerate(codes, start=1):
            statusRows.append({
                "ENTITY_CD": entity, "STATUS_CD": code, "STATUS_NAME": code.title(), "STATUS_DESC": f"{entity.title()} status {code}",
                "STATUS_GROUP_CD": "CLOSED" if code in terminal else "OPEN", "DISPLAY_SEQ_NBR": seq, "IS_TERMINAL_FLG": _flag(code in terminal),
                "IS_ERROR_FLG": _flag(code in ("EXCEPTION", "BOUNCED")), "ALLOWS_UPDATE_FLG": _flag(code not in terminal),
                "NEXT_STATUS_LIST_TXT": None, "REGION_CD": None, "LEGACY_STATUS_CD": code[:2], "ACTIVE_FLG": "Y",
                "SOURCE_SYS": "ORA_ERP", **_audit("STATSEED", seeded),
            })
    ctx.put(ORACLE, REF, "STATUS_CODE_REF", statusRows)

    reasonRows: list[Row] = []
    for seq, (code, region, desc, category, customerFault, *_rest, supplierRecoverable) in enumerate(RETURN_REASONS, start=1):
        reasonRows.append({
            "ENTITY_CD": "RETURN", "REASON_CD": code, "REGION_CD": region, "REASON_NAME": desc[:40], "REASON_DESC": desc,
            "REASON_CATEGORY_CD": category[:10], "REQUIRES_COMMENT_FLG": _flag(category == "OTHER"), "REQUIRES_APPROVAL_FLG": _flag(category == "RECALL"),
            "APPROVAL_LEVEL_NBR": 2 if category == "RECALL" else None, "FINANCIAL_IMPACT_FLG": "Y", "SUPPLIER_FAULT_FLG": _flag(bool(supplierRecoverable)),
            "KPI_EXCLUDE_FLG": _flag(bool(customerFault)), "DISPLAY_SEQ_NBR": seq, "ACTIVE_FLG": "Y", "SOURCE_SYS": "ORA_ERP", **_audit("RSNSEED", seeded),
        })
    for seq, (entity, code, name) in enumerate([
        ("HOLD", "CREDIT", "Credit limit exceeded"), ("HOLD", "STOCK", "Insufficient stock"), ("HOLD", "FRAUD", "Fraud screening"),
        ("HOLD", "EXPORT", "Export control check"), ("HOLD", "PRICE", "Price approval required"),
        ("WRITEOFF", "BADDEBT", "Bad debt"), ("WRITEOFF", "SMALLBAL", "Small balance"), ("WRITEOFF", "GOODWILL", "Goodwill gesture"),
        ("WRITEOFF", "FXDIFF", "Exchange difference"), ("DISPUTE", "PRICE", "Price dispute"), ("DISPUTE", "QUANTITY", "Quantity dispute"),
        ("DISPUTE", "QUALITY", "Quality dispute"), ("DISPUTE", "DELIVERY", "Delivery dispute"), ("DISPUTE", "TAX", "Tax dispute"),
    ], start=100):
        reasonRows.append({
            "ENTITY_CD": entity, "REASON_CD": code, "REGION_CD": "ALL", "REASON_NAME": name, "REASON_DESC": name, "REASON_CATEGORY_CD": entity[:10],
            "REQUIRES_COMMENT_FLG": "N", "REQUIRES_APPROVAL_FLG": _flag(entity == "WRITEOFF"), "APPROVAL_LEVEL_NBR": 1 if entity == "WRITEOFF" else None,
            "FINANCIAL_IMPACT_FLG": _flag(entity != "HOLD"), "SUPPLIER_FAULT_FLG": "N", "KPI_EXCLUDE_FLG": "N", "DISPLAY_SEQ_NBR": seq,
            "ACTIVE_FLG": "Y", "SOURCE_SYS": "ORA_ERP", **_audit("RSNSEED", seeded),
        })
    ctx.put(ORACLE, REF, "REASON_CODE_REF", reasonRows)


def generateCodeTranslation(ctx: GenContext) -> None:
    seeded = date(2016, 1, 1)
    rows: list[Row] = []
    translationId = 0

    def add(codeSet: str, sourceValue: str, targetValue: str, region: str, entity: str, desc: str,
            effectiveTo: date | None = None, active: bool = True, priority: int = 10) -> None:
        nonlocal translationId
        translationId += 1
        rows.append({
            "TRANSLATION_ID": translationId, "CODE_SET_CD": codeSet, "SOURCE_SYS_CD": "WWI_SQL", "SOURCE_VALUE_TXT": sourceValue,
            "TARGET_VALUE_TXT": targetValue, "REGION_CD": region, "ENTITY_CD": entity, "VALUE_TYPE_CD": "CODE", "DESCRIPTION_TXT": desc,
            "EFFECTIVE_FROM_DT": seeded, "EFFECTIVE_TO_DT": effectiveTo, "PRIORITY_NBR": priority, "ACTIVE_FLG": _flag(active),
            "SOURCE_SYS": "MDM_HUB", **_audit("XLATSEED", seeded),
        })

    for channel in CHANNELS:
        if channel.conformedCode is None:
            ctx.tag("UNTRANSLATED_CODE", "Source code with no CODE_TRANSLATION row.", f"{REF}.CODE_TRANSLATION",
                    CODE_SET_CD="SALES_CHANNEL", SOURCE_VALUE_TXT=channel.code, REGION_CD=channel.region)
            continue
        if channel.status == "CLOSED":
            # Stale translation: effective-dated out and inactive but still present.
            add("SALES_CHANNEL", channel.code, channel.conformedCode, channel.region, "ORDER", channel.name, effectiveTo=date(2019, 6, 30), active=False)
            ctx.tag("STALE_CODE", "CODE_TRANSLATION row whose EFFECTIVE_TO_DT has passed / ACTIVE_FLG = N.", f"{REF}.CODE_TRANSLATION",
                    CODE_SET_CD="SALES_CHANNEL", SOURCE_VALUE_TXT=channel.code, TRANSLATION_ID=translationId)
            continue
        add("SALES_CHANNEL", channel.code, channel.conformedCode, channel.region, "ORDER", channel.name)

    for code in PAYMENT_METHOD_CODES:
        if code == UNTRANSLATED_PAYMENT_METHOD:
            ctx.tag("UNTRANSLATED_CODE", "Source code with no CODE_TRANSLATION row.", f"{REF}.CODE_TRANSLATION",
                    CODE_SET_CD="PAYMENT_METHOD", SOURCE_VALUE_TXT=code, REGION_CD="ALL")
            continue
        add("PAYMENT_METHOD", code, PAYMENT_METHOD_TRANSLATION[code], "ALL", "PAYMENT", f"Payment method {code}")

    for code, region, desc, *_rest in RETURN_REASONS:
        if (code, region) == UNTRANSLATED_RETURN_REASON:
            ctx.tag("UNTRANSLATED_CODE", "Source code with no CODE_TRANSLATION row.", f"{REF}.CODE_TRANSLATION",
                    CODE_SET_CD="RETURN_REASON", SOURCE_VALUE_TXT=code, REGION_CD=region)
            continue
        if (code, region) == STALE_RETURN_REASON:
            add("RETURN_REASON", code, RETURN_REASON_TRANSLATION[code], region, "RETURN", desc, effectiveTo=ctx.spanStart - timedelta(days=90), active=False)
            ctx.tag("STALE_CODE", "CODE_TRANSLATION row whose EFFECTIVE_TO_DT has passed / ACTIVE_FLG = N.", f"{REF}.CODE_TRANSLATION",
                    CODE_SET_CD="RETURN_REASON", SOURCE_VALUE_TXT=code, TRANSLATION_ID=translationId)
            continue
        add("RETURN_REASON", code, RETURN_REASON_TRANSLATION[code], region, "RETURN", desc)

    for status in ORDER_STATUSES:
        add("ORDER_STATUS", status, {"PARTSHIP": "PARTIALLY_SHIPPED"}.get(status, status), "ALL", "ORDER", f"Order status {status}")
    for status in INVOICE_SETTLEMENT_STATUSES:
        add("SETTLEMENT_STATUS", status, {"PARTPAID": "PARTIALLY_PAID", "WRITTENOFF": "WRITTEN_OFF"}.get(status, status), "ALL", "INVOICE", f"Settlement status {status}")
    for planCode, _name, region, basis, *_rest in COMMISSION_PLANS:
        add("COMMISSION_BASIS", basis, {"INVOICEDMARGIN": "MARGIN", "NETREVENUE": "REVENUE", "COLLECTEDCASH": "CASH"}[basis], region, "COMMISSION", planCode, priority=20)
    ctx.put(ORACLE, REF, "CODE_TRANSLATION", rows)


def generateReference(ctx: GenContext) -> None:
    generateRegionCountryCurrency(ctx)
    generateFxRates(ctx)
    generateCalendars(ctx)
    generateTax(ctx)
    generateCodeReference(ctx)
    generateCodeTranslation(ctx)


# --------------------------------------------------------------------------------
# WWI_MDM (after SQL Server customers / stock items exist)
# --------------------------------------------------------------------------------
PARTY_ID_BASE = 5_000_000


def generateMdm(ctx: GenContext) -> None:
    rng = ctx.rng("oracle.mdm")
    customers = ctx.sql("Sales", "Customers")
    categories = {r["CustomerCategoryID"]: r["CustomerCategoryName"] for r in ctx.sql("Sales", "CustomerCategories")}
    buyingGroups = {r["BuyingGroupID"]: r["BuyingGroupName"] for r in ctx.sql("Sales", "BuyingGroups")}
    priceLists = {r["PriceListID"]: r["PriceListCode"] for r in ctx.sql("Sales", "PriceLists")}
    territories = {r["SalesTerritoryID"]: r for r in ctx.sql("Sales", "SalesTerritories")}
    customerCountry: dict[int, str] = ctx.scratch.customerCountry

    # Latest version of each customer (duplicates exist deliberately in the extract).
    latest: dict[int, Row] = {}
    for row in customers:
        if row["CustomerID"] not in latest or row["ValidFrom"] > latest[row["CustomerID"]]["ValidFrom"]:
            latest[row["CustomerID"]] = row
    customerIds = sorted(latest)

    master: list[Row] = []
    addresses: list[Row] = []
    contacts: list[Row] = []
    xrefs: list[Row] = []
    merges: list[Row] = []
    partyByCustomer: dict[int, int] = {}
    nextParty = PARTY_ID_BASE
    typeByCategory = {"Agent": "DIST", "Wholesaler": "WHSL", "Novelty Shop": "RETL", "Supermarket": "RETL", "Computer Store": "RETL",
                      "Gift Store": "RETL", "Corporate": "INTC", "General Retailer": "RETL"}

    def newParty(customer: Row, status: str = "AC", deleted: bool = False, nameSuffix: str = "") -> int:
        nonlocal nextParty
        nextParty += 1
        partyId = nextParty
        country = customerCountry[customer["CustomerID"]]
        region = customer["RegionCode"]
        territory = territories[customer["SalesTerritoryID"]]
        created = customer["AccountOpenedDate"]
        consentFlag = customer["MarketingConsentFlag"] or ("N" if region == "EU" else "Y")
        master.append({
            "CUST_ID": partyId, "CUST_NBR": f"C{partyId:09d}", "LEGACY_CUST_CD": f"L{customer['CustomerID']:05d}"[:6],
            "CUST_NAME": (customer["CustomerName"] + nameSuffix)[:160], "CUST_NAME_ALT": None, "TRADING_NAME": customer["CustomerName"].split(" (")[0],
            "REGION_CD": region, "COUNTRY_CD": country, "CUST_TYPE_CD": typeByCategory.get(categories[customer["CustomerCategoryID"]], "RETL"),
            "CUST_STATUS_CD": status, "BUYING_GROUP_CD": (buyingGroups.get(customer["BuyingGroupID"]) or "")[:10].upper().replace(" ", "") or None,
            "PRICE_LIST_CD": priceLists[customer["DefaultPriceListID"]][:10] if customer["DefaultPriceListID"] else None,
            "ACCT_MANAGER_CD": f"AM{territory['ManagerPersonID']:04d}", "PRIMARY_CURR_CD": territory["ReportingCurrencyCode"],
            "PAYMENT_TERMS_CD": f"NET{customer['PaymentDays']}", "TAX_REG_NBR": customer["TaxRegistrationNumber"] if region == "NA" else None,
            "VAT_REG_NBR": customer["TaxRegistrationNumber"] if region == "EU" else None,
            "GST_REG_NBR": customer["TaxRegistrationNumber"] if region == "APAC" else None,
            "TAX_EXEMPT_FLG": _flag(customer["TaxExemptionCertificate"] is not None), "TAX_EXEMPT_CERT_NBR": customer["TaxExemptionCertificate"],
            "EDI_ENABLED_FLG": _flag(customer["CustomerCategoryID"] in (2, 4)), "EDI_PARTNER_ID": None,
            "CREDIT_HOLD_FLG": _flag(bool(customer["IsOnCreditHold"])), "ON_STOP_REASON_CD": (customer["CreditHoldReasonCode"] or "")[:4] or None,
            "FIRST_ORDER_DT": created + timedelta(days=rng.randint(3, 40)), "LAST_ORDER_DT": ctx.asOf - timedelta(days=rng.randint(0, 120)),
            "CONSENT_MARKETING_FLG": consentFlag, "CONSENT_CAPTURED_DT": customer["ConsentCapturedWhen"].date() if customer["ConsentCapturedWhen"] else None,
            "CONSENT_SOURCE_CD": "WEBFORM" if customer["ConsentCapturedWhen"] else None, "RETENTION_UNTIL_DT": customer["DataRetentionExpiresOn"],
            "SPECIAL_INSTR_TXT": None, "MISC_FLAG_1": "N", "MISC_FLAG_2": "N", "DELETED_FLG": _flag(deleted), "SOURCE_SYS": "MDM_HUB",
            **_audit("MDMLOAD", created, "MDMSTEW", customer["ValidFrom"].date()),
        })
        return partyId

    def addXref(partyId: int, customerId: int, method: str = "EXACT", score: str = "100.00", active: bool = True, review: bool = False) -> None:
        xrefs.append({
            "PARTY_XREF_ID": len(xrefs) + 1, "PARTY_TYPE_CD": "CUST", "CUST_ID": partyId, "SUPP_ID": None, "SOURCE_SYS_CD": "WWI_SQL",
            "SOURCE_KEY_TXT": str(customerId), "SOURCE_KEY_TYPE_CD": "CUSTID", "MATCH_METHOD_CD": method, "MATCH_SCORE": money(score),
            "MATCH_RUN_ID": 1000 + customerId % 7, "REVIEW_REQUIRED_FLG": _flag(review), "REVIEWED_BY_CD": None if review else "STEWARD1",
            "REVIEWED_DT": None if review else ctx.spanStart, "ACTIVE_FLG": _flag(active), "SOURCE_SYS": "MDM_HUB",
            **_audit("MDMLOAD", ctx.spanStart),
        })

    def addMerge(survivor: int, merged: int, when: date, reason: str = "DUP") -> int:
        merges.append({
            "MERGE_ID": len(merges) + 1, "PARTY_TYPE_CD": "CUST", "SURVIVOR_PARTY_ID": survivor, "MERGED_PARTY_ID": merged, "MERGE_REASON_CD": reason,
            "MERGE_RULE_TXT": "Name+postcode fuzzy match >= 0.92", "ATTRIBUTES_MOVED_TXT": "ADDRESS,CONTACT,TAXREG", "OPEN_TXN_COUNT": rng.randint(0, 3),
            "MERGED_BY_CD": "STEWARD2", "MERGE_DT": when, "APPROVED_BY_CD": "MDMLEAD", "UNMERGE_FLG": "N", "UNMERGE_DT": None,
            "UNMERGE_NOTES_TXT": None, "SOURCE_SYS": "MDM_HUB", **_audit("MDMLOAD", when),
        })
        return len(merges)

    for customerId in customerIds:
        partyByCustomer[customerId] = newParty(latest[customerId])

    # Edge-case customers (deterministic picks from the id list, disjoint sets).
    picks = rng.sample(customerIds[10:], 14)
    singleHop, twoHop, noMerge, duplicateXref, missingXref = picks[0:2], picks[2:4], picks[4:5], picks[5:8], picks[8:12]
    consumed = set(picks[0:12])
    mergeDay = ctx.spanStart + timedelta(days=200)

    for customerId in customerIds:
        if customerId in consumed:
            continue
        addXref(partyByCustomer[customerId], customerId)

    for customerId in singleHop:
        survivor = partyByCustomer[customerId]
        retired = newParty(latest[customerId], status="MG", deleted=True, nameSuffix=" (legacy record)")
        addXref(retired, customerId, method="MIGR")
        mergeId = addMerge(survivor, retired, mergeDay)
        ctx.tag("XREF_RETIRED_SINGLE_HOP", "PARTY_XREF points at a retired party that MDM_MERGE_HISTORY merged into a survivor (one hop).",
                f"{MDM}.PARTY_XREF", CustomerID=customerId, RETIRED_CUST_ID=retired, SURVIVOR_CUST_ID=survivor, MERGE_ID=mergeId)
    for customerId in twoHop:
        survivor = partyByCustomer[customerId]
        middle = newParty(latest[customerId], status="MG", deleted=True, nameSuffix=" (acquired entity)")
        oldest = newParty(latest[customerId], status="MG", deleted=True, nameSuffix=" (mainframe record)")
        addXref(oldest, customerId, method="MIGR")
        firstMerge = addMerge(middle, oldest, mergeDay - timedelta(days=120), reason="ACQ")
        secondMerge = addMerge(survivor, middle, mergeDay, reason="DUP")
        ctx.tag("XREF_RETIRED_TWO_HOP", "PARTY_XREF points at a retired party whose survivor was itself merged again (two hops).",
                f"{MDM}.PARTY_XREF", CustomerID=customerId, RETIRED_CUST_ID=oldest, INTERMEDIATE_CUST_ID=middle, SURVIVOR_CUST_ID=survivor,
                MERGE_IDS=f"{firstMerge},{secondMerge}")
    for customerId in noMerge:
        retired = newParty(latest[customerId], status="MG", deleted=True, nameSuffix=" (orphaned)")
        addXref(retired, customerId, method="MIGR", review=True)
        ctx.tag("XREF_RETIRED_NO_MERGE", "PARTY_XREF points at a retired (MG) party with no MDM_MERGE_HISTORY record.",
                f"{MDM}.PARTY_XREF", CustomerID=customerId, RETIRED_CUST_ID=retired, INTENDED_SURVIVOR_CUST_ID=partyByCustomer[customerId])
    for customerId in duplicateXref:
        party = partyByCustomer[customerId]
        addXref(party, customerId)
        addXref(party, customerId, method="FUZZY", score="93.50", review=True)
        ctx.tag("DUPLICATE_XREF", "Two active PARTY_XREF rows for the same WWI CustomerID.", f"{MDM}.PARTY_XREF",
                CustomerID=customerId, CUST_ID=party, PARTY_XREF_IDS=f"{len(xrefs) - 1},{len(xrefs)}")
    for customerId in missingXref:
        ctx.tag("MISSING_XREF", "WWI customer with no PARTY_XREF row at all.", f"{MDM}.PARTY_XREF", CustomerID=customerId, CUST_ID=partyByCustomer[customerId])

    for row in master:
        if row["CUST_STATUS_CD"] == "MG":
            continue
        partyId = row["CUST_ID"]
        country = row["COUNTRY_CD"]
        city = rng.choice(CITIES[country])
        for seq, addrType in enumerate(("BILL", "SHIP"), start=1):
            addresses.append({
                "CUST_ADDR_ID": len(addresses) + 1, "CUST_ID": partyId, "ADDR_TYPE_CD": addrType, "ADDR_SEQ_NBR": seq,
                "ADDR_LINE_1": f"{rng.randint(1, 999)} {rng.choice(LAST_NAMES)} Street", "ADDR_LINE_2": None, "ADDR_LINE_3": None, "ADDR_LINE_4": None,
                "CITY_TXT": city, "COUNTY_TXT": None, "STATE_PROV_CD": rng.choice(STATES[country]) if country in STATES else None,
                "PREFECTURE_TXT": f"{city}-to" if country == "JP" else None, "POSTAL_CD": f"{rng.randint(10000, 99999)}",
                "POSTAL_CD_NORM": None, "ZIP4_CD": f"{rng.randint(1000, 9999)}" if country == "US" else None, "COUNTRY_CD": country,
                "REGION_CD": row["REGION_CD"], "GEO_LAT": None, "GEO_LON": None, "ADDR_VERIFIED_FLG": _flag(rng.random() < 0.8),
                "ADDR_VERIFIED_DT": None, "ADDR_VERIFY_VENDOR_CD": None, "PRIMARY_FLG": _flag(seq == 1), "VALID_FROM_DT": row["CREATED_DT"],
                "VALID_TO_DT": None, "DELETED_FLG": "N", "SOURCE_SYS": "MDM_HUB", **_audit("MDMLOAD", row["CREATED_DT"]),
            })
        given, family = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        isEu = row["REGION_CD"] == "EU"
        consent = row["CONSENT_MARKETING_FLG"]
        contacts.append({
            "CUST_CONTACT_ID": len(contacts) + 1, "CUST_ID": partyId, "CONTACT_ROLE_CD": "BUYER", "SALUTATION_TXT": None, "GIVEN_NAME": given,
            "FAMILY_NAME": family, "FULL_NAME_NORM": f"{family.upper()}, {given.upper()}", "JOB_TITLE_TXT": "Purchasing Manager",
            "EMAIL_ADDR": f"{given}.{family}@{row['TRADING_NAME'].lower().replace(' ', '')}.example".lower(), "EMAIL_VALID_FLG": "Y",
            "PHONE_NBR": f"+{rng.randint(1, 99)} {rng.randint(100, 999)} {rng.randint(1000, 9999)}", "PHONE_EXT": None, "MOBILE_NBR": None,
            "FAX_NBR": None, "LANGUAGE_CD": {"NA": "en-US", "EU": "en-GB", "APAC": "en-AU"}[row["REGION_CD"]], "PREFERRED_CONTACT_TIME": None,
            "CONSENT_EMAIL_FLG": consent, "CONSENT_PHONE_FLG": consent if isEu else "Y", "CONSENT_UPDATED_DT": row["CONSENT_CAPTURED_DT"],
            "DO_NOT_CONTACT_FLG": _flag(isEu and consent == "N"), "PRIMARY_FLG": "Y", "ACTIVE_FLG": "Y", "LAST_CONTACT_DT": row["LAST_ORDER_DT"],
            "SOURCE_SYS": "MDM_HUB", **_audit("MDMLOAD", row["CREATED_DT"]),
        })

    ctx.put(ORACLE, MDM, "CUST_MASTER", master)
    ctx.put(ORACLE, MDM, "CUST_ADDRESS", addresses)
    ctx.put(ORACLE, MDM, "CUST_CONTACT", contacts)
    ctx.put(ORACLE, MDM, "PARTY_XREF", xrefs)
    ctx.put(ORACLE, MDM, "MDM_MERGE_HISTORY", merges)
    ctx.scratch.partyByCustomer = partyByCustomer
    generateProductMaster(ctx)


def generateProductMaster(ctx: GenContext) -> None:
    rng = ctx.rng("oracle.product")
    stockGroups = ctx.sql("Warehouse", "StockGroups")
    stockItems = ctx.sql("Warehouse", "StockItems")
    holdings = {r["StockItemID"]: r for r in ctx.sql("Warehouse", "StockItemHoldings")}
    itemGroups: dict[int, int] = {}
    for link in ctx.sql("Warehouse", "StockItemStockGroups"):
        itemGroups.setdefault(link["StockItemID"], link["StockGroupID"])
    seeded = date(2015, 6, 1)

    categories: list[Row] = [{
        "PRODUCT_CATEGORY_ID": 1, "CATEGORY_CD": "ALL", "CATEGORY_NAME": "All merchandise", "PARENT_CATEGORY_ID": None, "CATEGORY_LEVEL_NBR": 1,
        "MERCH_GROUP_CD": "MERCH", "DEFAULT_TAX_CLASS_CD": "STD", "MARGIN_TARGET_PCT": money("35.00"), "ACTIVE_FLG": "Y", "SORT_ORDER_NBR": 1,
        "SOURCE_SYS": "ORA_ERP", **_audit("PIMSEED", seeded),
    }]
    categoryByGroup: dict[int, int] = {}
    for group in stockGroups:
        categoryId = 100 + group["StockGroupID"]
        categoryByGroup[group["StockGroupID"]] = categoryId
        categories.append({
            "PRODUCT_CATEGORY_ID": categoryId, "CATEGORY_CD": group["StockGroupName"].upper().replace(" ", "")[:12], "CATEGORY_NAME": group["StockGroupName"],
            "PARENT_CATEGORY_ID": 1, "CATEGORY_LEVEL_NBR": 2, "MERCH_GROUP_CD": "MERCH", "DEFAULT_TAX_CLASS_CD": "STD",
            "MARGIN_TARGET_PCT": money(rng.choice(["30.00", "35.00", "40.00"])), "ACTIVE_FLG": "Y", "SORT_ORDER_NBR": group["StockGroupID"] + 1,
            "SOURCE_SYS": "ORA_ERP", **_audit("PIMSEED", seeded),
        })
    ctx.put(ORACLE, MDM, "PRODUCT_CATEGORY", categories)

    products: list[Row] = []
    for item in stockItems:
        stockItemId = item["StockItemID"]
        holding = holdings[stockItemId]
        products.append({
            "PRODUCT_ID": 700_000 + stockItemId, "ITEM_NBR": f"WWI-{stockItemId:06d}", "LEGACY_PART_CD": f"P{stockItemId:05d}", "WWI_STOCK_ITEM_ID": stockItemId,
            "ITEM_DESC": item["StockItemName"], "ITEM_DESC_SHORT": item["StockItemName"][:40], "PRODUCT_CATEGORY_ID": categoryByGroup[itemGroups[stockItemId]],
            "BRAND_CD": (item["Brand"] or "WWI")[:8].upper(), "ITEM_TYPE_CD": "STK", "LIFECYCLE_STATUS_CD": "ACT", "PRIMARY_UOM_CD": "EA",
            "PURCHASE_UOM_CD": "CS", "SELL_UOM_CD": "EA",
            # ERP standard cost is the margin cost basis (Fact.Sales Margin); it is deliberately a little
            # different from Warehouse.StockItemHoldings.LastCostPrice, which the legacy load also used.
            "UNIT_COST_STD": money(Decimal(holding["LastCostPrice"]) * Decimal(rng.choice(["0.97", "1.00", "1.03"]))), "COST_CURR_CD": "USD",
            "LIST_PRICE_AMT": item["UnitPrice"], "LIST_PRICE_CURR_CD": "USD", "TAX_CLASS_CD": "STD", "COMMODITY_CD": None,
            "HS_TARIFF_CD": f"{rng.randint(3900, 9600)}.{rng.randint(10, 99)}", "COUNTRY_OF_ORIGIN_CD": rng.choice(["CN", "VN", "US", "DE"]),
            "UNIT_WEIGHT_KG": item["TypicalWeightPerUnit"], "UNIT_VOLUME_M3": None, "SHELF_LIFE_DAYS": 365 if item["IsChillerStock"] else None,
            "CHILLER_FLG": _flag(bool(item["IsChillerStock"])), "HAZMAT_FLG": "N", "HAZMAT_CLASS_CD": None, "SERIALISED_FLG": "N",
            "LOT_CONTROLLED_FLG": _flag(bool(item["IsChillerStock"])), "REORDER_POINT_QTY": holding["ReorderLevel"], "SAFETY_STOCK_QTY": holding["ReorderLevel"] // 2,
            "DISCONTINUED_DT": None, "REPLACEMENT_PRODUCT_ID": None, "ENG_NOTES_TXT": None, "DELETED_FLG": "N", "SOURCE_SYS": "ORA_ERP",
            **_audit("PIMLOAD", seeded, "PIMLOAD", item["ValidFrom"].date()),
        })
    ctx.put(ORACLE, MDM, "PRODUCT_MASTER", products)

    categoryById = {c["PRODUCT_CATEGORY_ID"]: c for c in categories}
    hierarchy: list[Row] = []
    for product in products:
        category = categoryById[product["PRODUCT_CATEGORY_ID"]]
        brand = str(product["BRAND_CD"])
        for hierType in ("FIN", "PLAN", "WEB"):
            if hierType == "PLAN" and rng.random() < 0.4:
                continue
            hierarchy.append({
                "PRODUCT_HIER_ID": 20_000 + len(hierarchy) + 1, "HIER_TYPE_CD": hierType, "PRODUCT_ID": product["PRODUCT_ID"],
                "LEVEL_1_CD": {"FIN": "MERCH", "PLAN": "STK", "WEB": "SHOP"}[hierType], "LEVEL_1_NAME": {"FIN": "Merchandise", "PLAN": "Stocked goods", "WEB": "Online shop"}[hierType],
                "LEVEL_2_CD": category["CATEGORY_CD"], "LEVEL_2_NAME": category["CATEGORY_NAME"],
                "LEVEL_3_CD": brand if hierType != "PLAN" else None, "LEVEL_3_NAME": brand.title() if hierType != "PLAN" else None,
                "LEVEL_4_CD": None, "LEVEL_4_NAME": None,
                "PLANNER_CD": f"PL{rng.randint(1, 6):02d}" if hierType == "PLAN" else None, "BUYER_CD": f"BY{rng.randint(1, 9):02d}" if hierType == "PLAN" else None,
                "EFFECTIVE_DT": seeded, "END_DT": None, "SOURCE_SYS": "ORA_ERP", **_audit("PIMLOAD", seeded),
            })
    ctx.put(ORACLE, MDM, "PRODUCT_HIERARCHY", hierarchy)
