"""Regional rules shared across packages (region derivation, consent codes, tier ladders)."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# ref.Country is empty on the legacy baseline, so STG_Load_WebSession's country -> region lookup
# falls back to this table (ISO alpha-2 and alpha-3 as they appear in the OLTP/raw feeds).
COUNTRY_REGION = {
    "US": "NA",
    "USA": "NA",
    "CA": "NA",
    "CAN": "NA",
    "MX": "NA",
    "MEX": "NA",
    "GB": "EU",
    "GBR": "EU",
    "IE": "EU",
    "IRL": "EU",
    "NL": "EU",
    "NLD": "EU",
    "FR": "EU",
    "FRA": "EU",
    "DE": "EU",
    "DEU": "EU",
    "ES": "EU",
    "ESP": "EU",
    "IT": "EU",
    "ITA": "EU",
    "BE": "EU",
    "BEL": "EU",
    "AU": "APAC",
    "AUS": "APAC",
    "NZ": "APAC",
    "NZL": "APAC",
    "SG": "APAC",
    "SGP": "APAC",
    "JP": "APAC",
    "JPN": "APAC",
    "HK": "APAC",
    "HKG": "APAC",
    "IN": "APAC",
    "IND": "APAC",
}

# raw.SqlWebSession carries the OLTP ConsentStateCode rather than the Y/N ConsentFlag the STG
# package expected; these codes are the ones that constitute an affirmative consent.
CONSENT_GIVEN_CODES = ("OPTIN", "EXPR", "EINW", "GRANTED", "Y")

REGION_CODES = ("NA", "EU", "APAC")


def normaliseRegion(regionCol: Column, defaultRegion: str) -> Column:
    trimmed = F.upper(F.trim(regionCol))
    return F.when(trimmed.isNull() | (trimmed == ""), F.lit(defaultRegion)).otherwise(trimmed)


def regionFromCountry(countryCol: Column, refRegionCol: Column, defaultRegion: str) -> Column:
    """ref.Country lookup first, static fallback second, package default last."""
    mapping = F.create_map(*[F.lit(x) for kv in COUNTRY_REGION.items() for x in kv])
    fallback = mapping[F.upper(F.trim(countryCol))]
    return F.coalesce(F.upper(F.trim(refRegionCol)), fallback, F.lit(defaultRegion))


def regionFromProgram(programCol: Column) -> Column:
    """Legacy raw ledger rows carry the region only in the programme suffix (LOY-NA / LOY-EU / LOY-APAC)."""
    upper = F.upper(F.trim(programCol))
    return (
        F.when(upper.endswith("-APAC") | upper.endswith("APAC"), F.lit("APAC"))
        .when(upper.endswith("-EU") | upper.endswith("EU"), F.lit("EU"))
        .when(upper.endswith("-NA") | upper.endswith("NA"), F.lit("NA"))
        .otherwise(F.lit(None).cast("string"))
    )


def consentGivenFlag(consentCol: Column) -> Column:
    """'Y' when the code is an affirmative consent, otherwise 'N' (null -> 'N', as in the package)."""
    upper = F.upper(F.trim(F.coalesce(consentCol, F.lit("N"))))
    return F.when(upper.isin(*CONSENT_GIVEN_CODES), F.lit("Y")).otherwise(F.lit("N"))


def stagingTierCode(netPoints: Column) -> Column:
    """STG_Load_LoyaltyLedger 'Derive Loyalty Tier' banding."""
    return F.when(netPoints >= 50000, F.lit("PLT")).when(netPoints >= 20000, F.lit("GLD")).when(netPoints >= 5000, F.lit("SLV")).otherwise(F.lit("BRZ"))


# ref.LoyaltyTier does not exist on the baseline; the lookup would have routed every row to the
# reject output. The names/discounts below are the reference rows the package was written against.
STAGING_TIER_REFERENCE = [
    ("BRZ", "Bronze", 0.0),
    ("SLV", "Silver", 2.5),
    ("GLD", "Gold", 5.0),
    ("PLT", "Platinum", 7.5),
]


def overlayTierCode(regionCol: Column, activePoints: Column) -> Column:
    """C360_Build_LoyaltyOverlay 'Recalculate Tier Ladders' - three regional ladders."""
    na = F.when(activePoints >= 50000, "PLATINUM").when(activePoints >= 20000, "GOLD").when(activePoints >= 5000, "SILVER").otherwise("BASE")
    eu = F.when(activePoints >= 75000, "PREMIER").when(activePoints >= 30000, "PLUS").otherwise("STANDARD")
    apac = F.when(activePoints >= 40000, "DIAMOND").when(activePoints >= 15000, "JADE").otherwise("MEMBER")
    return F.when(regionCol == "NA", na).when(regionCol == "EU", eu).otherwise(apac)


def stagingExpiryDefault(regionCol: Column, entryDate: Column, expiryDate: Column) -> Column:
    """EU points expire after 12 months, APAC after 18, NA after 24 when the source gives no expiry."""
    return F.coalesce(
        expiryDate,
        F.when(regionCol == "EU", F.add_months(entryDate, 12)).when(regionCol == "APAC", F.add_months(entryDate, 18)).otherwise(F.add_months(entryDate, 24)),
    )


def nullIfBlank(col: Column) -> Column:
    """NULL when the trimmed value is empty (SSIS: TRIM(x) == "" ? NULL : x)."""
    trimmed = F.trim(col)
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)
