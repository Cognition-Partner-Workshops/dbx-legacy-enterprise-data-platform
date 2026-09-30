"""Names and constants shared by every package in the ref_calendar group."""

GROUP_SLUG = "ref_calendar"
BRANCH = "ssis_ref_calendar"
ACTOR = "devin:ssis_ref_calendar"
HARNESS_VERSION = "ssis-migration-v1"

TARGET_CATALOG = "otterorders_migration"
TARGET_SCHEMA = f"{TARGET_CATALOG}.ssis_ref_calendar"
EVIDENCE_SCHEMA = f"{TARGET_CATALOG}.evidence"
RECON_TABLE = f"{EVIDENCE_SCHEMA}.recon_results"
LANDING_VOLUME = f"{TARGET_SCHEMA}.landing"
LANDING_VOLUME_PATH = "/Volumes/otterorders_migration/ssis_ref_calendar/landing"

LEGACY_OLTP = "wwi_legacy_oltp"
LEGACY_STAGING = "wwi_legacy_staging"
LEGACY_DW = "wwi_legacy_dw"
LEGACY_ORACLE = "wwi_legacy_oracle"
LEGACY_SQLSERVER_CONNECTION = "wwi_legacy_sqlserver"
LEGACY_DW_DATABASE = "WideWorldImportersDW"

SOURCE_SYSTEM_ORACLE = "ORA_ERP"
SOURCE_SYSTEM_OLTP = "WWI_OLTP"
SOURCE_SYSTEM_FILE = "FILE_FX"

HIGH_DATE = "9999-12-31"
HIGH_TIMESTAMP = "9999-12-31 23:59:59"
LOW_TIMESTAMP = "1900-01-01 00:00:00"
UNKNOWN_KEY = -1
NOT_APPLICABLE_KEY = -2
INVALID_KEY = -3
ERROR_KEY = -9

FX_RATE_TYPES = ("SPOT", "CORP", "AVG")
FX_TRIANGULATION_CURRENCIES = ("EUR", "SGD")
FX_OVERRIDE_MAX_DEVIATION_BPS = 500
FX_FILL_FORWARD_DAYS = 5

DATE_DIMENSION_START = "2013-01-01"
DATE_DIMENSION_END = "2016-12-31"
WWI_FISCAL_YEAR_START_MONTH = 11
CITY_TYPE2_POPULATION_THRESHOLD_PCT = 5.0

PACKAGES = [
    "EXT_ORA_CodeTranslation", "EXT_ORA_Currency", "EXT_ORA_Geography", "EXT_ORA_PaymentTerms",
    "EXT_ORA_TaxRate", "EXT_ORA_FxRateDaily", "EXT_SQL_Cities", "EXT_SQL_PaymentMethods",
    "EXT_SQL_TransactionTypes", "STG_Load_Currency", "STG_Load_Geography", "STG_Load_TaxAndTerms",
    "ING_FILE_FxOverride", "REF_Load_Carrier", "REF_Load_CodeTranslation", "REF_Load_Currency",
    "REF_Load_DateDimension", "REF_Load_Geography", "REF_Load_LoyaltyTier", "REF_Load_PaymentMethod",
    "REF_Load_PaymentTerms", "REF_Load_ReturnReason", "REF_Load_SalesChannel",
    "REF_Load_TransactionType", "REF_Load_UnknownMembers", "REF_Load_WarehouseSite", "DIM_Load_City",
]


def tbl(name: str) -> str:
    """Fully qualified name of a table in the group's landing schema."""
    return f"{TARGET_SCHEMA}.{name}"
