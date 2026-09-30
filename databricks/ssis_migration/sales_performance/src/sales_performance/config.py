"""Static configuration for the sales_performance migration group."""

CATALOG = "otterorders_migration"
SCHEMA = "ssis_sales_performance"
EVIDENCE_SCHEMA = "evidence"
LANDING_VOLUME = "landing"

LEGACY_OLTP = "wwi_legacy_oltp"
LEGACY_STAGING = "wwi_legacy_staging"
LEGACY_DW = "wwi_legacy_dw"
LEGACY_SQLSERVER_CONNECTION = "wwi_legacy_sqlserver"
LEGACY_DW_DATABASE = "WideWorldImportersDW"
LEGACY_STAGING_DATABASE = "WideWorldImporters_Staging"

GROUP_SLUG = "sales_performance"
EVIDENCE_BRANCH = "ssis_sales_performance"
EVIDENCE_ACTOR = "devin:ssis_sales_performance"
HARNESS_VERSION = "ssis-migration-v1"
UNIT_TYPE = "ssis_package"

UNKNOWN_KEY = -1
SOURCE_SYSTEM_PARTNER = "PARTNERFILE"

PACKAGES = [
    "ING_FILE_PartnerSales_NA",
    "ING_FILE_PartnerSales_EU",
    "ING_FILE_PartnerSales_APAC",
    "STG_Load_PartnerSale",
    "SLS_NA_Load_Commission",
    "SLS_EU_Load_Commission",
    "SLS_APAC_Load_Commission",
    "SLS_Load_PromotionRedemption",
    "SLS_Load_QuotaAttainment",
    "SLS_Export_PartnerFeed",
    "FACT_Apply_Corrections",
    "AGG_Refresh_MonthlySalesSummary",
    "AGG_Refresh_RegionalSalesPerformance",
    "AGG_Refresh_ProductPerformance",
    "AGG_Refresh_PromotionEffectiveness",
    "AGG_Refresh_MonthlyMarginAnalysis",
    "AGG_Publish_ReportingLayer",
]

# Region defaults that the OLTP Sales.SalesTerritories rows carry (see README).
REGION_CURRENCY = {"NA": "USD", "EU": "EUR", "APAC": "AUD"}
REGION_FISCAL_CALENDAR = {"NA": "NA445", "EU": "EUCAL", "APAC": "APACJUN"}
REGION_TAX_TREATMENT = {"NA": "SALESTAX", "EU": "VAT", "APAC": "GST"}

# ISO-3166 alpha-2 -> (country name as WWI spells it, region code, alpha-3).
COUNTRY_MAP = {
    "US": ("UNITED STATES", "NA", "USA"),
    "CA": ("CANADA", "NA", "CAN"),
    "MX": ("MEXICO", "NA", "MEX"),
    "GB": ("UNITED KINGDOM", "EU", "GBR"),
    "DE": ("GERMANY", "EU", "DEU"),
    "NL": ("NETHERLANDS", "EU", "NLD"),
    "FR": ("FRANCE", "EU", "FRA"),
    "AT": ("AUSTRIA", "EU", "AUT"),
    "BE": ("BELGIUM", "EU", "BEL"),
    "IE": ("IRELAND", "EU", "IRL"),
    "ES": ("SPAIN", "EU", "ESP"),
    "IT": ("ITALY", "EU", "ITA"),
    "AU": ("AUSTRALIA", "APAC", "AUS"),
    "SG": ("SINGAPORE", "APAC", "SGP"),
    "JP": ("JAPAN", "APAC", "JPN"),
    "NZ": ("NEW ZEALAND", "APAC", "NZL"),
}


def tableName(name):
    return ".".join(p for p in (CATALOG, SCHEMA, name) if p)


def evidenceTable(name):
    return ".".join(p for p in (CATALOG, EVIDENCE_SCHEMA, name) if p)


def volumeRoot():
    return f"/Volumes/{CATALOG}/{SCHEMA}/{LANDING_VOLUME}"
