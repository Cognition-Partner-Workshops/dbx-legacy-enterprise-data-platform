"""Names, constants and the run context shared by every package."""
from dataclasses import dataclass

GROUP_SLUG = "sales_o2c"
BRANCH = "ssis_sales_o2c"
ACTOR = "devin:ssis_sales_o2c"
HARNESS_VERSION = "ssis-migration-v1"
SOURCE_SYSTEM_CODE = "WWI_OLTP"

OLTP_CATALOG = "wwi_legacy_oltp"
STAGING_CATALOG = "wwi_legacy_staging"
DW_CATALOG = "wwi_legacy_dw"
SQLSERVER_CONNECTION = "wwi_legacy_sqlserver"

PACKAGES = [
    "EXT_SQL_Orders",
    "EXT_SQL_OrderLines",
    "EXT_SQL_Invoices",
    "EXT_SQL_InvoiceLines",
    "EXT_SQL_CustomerTransactions",
    "STG_Load_Order",
    "STG_Load_Sale",
    "DQ_OrderLine_Screen",
    "DQ_InvoiceLine_Screen",
    "FACT_NA_Load_Sale",
    "FACT_EU_Load_Sale",
    "FACT_APAC_Load_Sale",
    "FACT_Dedup_Sale",
    "FACT_Load_Order",
    "FACT_Load_CustomerTransaction",
    "FACT_Load_Transaction",
    "FACT_Load_DailySalesSnapshot",
    "AGG_Refresh_DailySalesSummary",
]

UNKNOWN_KEY = 0  # legacy Integration.* procedures default unresolved surrogate keys to 0


@dataclass(frozen=True)
class RunContext:
    catalog: str = "otterorders_migration"
    schema: str = "ssis_sales_o2c"
    evidenceSchema: str = "evidence"
    batchId: int = 0
    packageExecutionId: int = 0
    reloadFullHistory: bool = False
    gitSha: str = "local"
    snapshotDate: str = ""
    aggFromDate: str = ""
    aggToDate: str = ""

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    @property
    def evidenceTable(self) -> str:
        return f"{self.catalog}.{self.evidenceSchema}.recon_results"


def parseBool(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "y")
