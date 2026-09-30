"""Static configuration for the procurement migration group."""

CATALOG = "otterorders_migration"
SCHEMA = "ssis_procurement"
EVIDENCE_SCHEMA = "evidence"
LANDING_VOLUME = "landing"
EXPORT_VOLUME = "exports"

LEGACY_OLTP = "wwi_legacy_oltp"
LEGACY_STAGING = "wwi_legacy_staging"
LEGACY_DW = "wwi_legacy_dw"
LEGACY_ORACLE = "wwi_legacy_oracle"
LEGACY_SQLSERVER_CONNECTION = "wwi_legacy_sqlserver"
LEGACY_DW_DATABASE = "WideWorldImportersDW"
LEGACY_STAGING_DATABASE = "WideWorldImporters_Staging"

GROUP_SLUG = "procurement"
EVIDENCE_BRANCH = "ssis_procurement"
EVIDENCE_ACTOR = "devin:ssis_procurement"
HARNESS_VERSION = "ssis-migration-v1"
UNIT_TYPE = "ssis_package"

SOURCE_SYSTEM_ORACLE = "ORA_ERP"
SOURCE_SYSTEM_OLTP = "WWI_OLTP"
SOURCE_SYSTEM_FILE = "FILE_CATALOG"

UNKNOWN_KEY = 0
NOT_APPLICABLE_KEY = -2
HIGH_DATE = "9999-12-31"
LOW_DATE = "1900-01-01"
HIGH_TS = "9999-12-31 23:59:59"
LOW_TS = "1900-01-01 00:00:00"

# Package name -> ordered position inside the daily job (documentation + recon ordering)
PACKAGES = [
    "EXT_ORA_SupplierMaster",
    "EXT_ORA_PurchaseOrderHdr",
    "EXT_ORA_PurchaseOrderLine",
    "EXT_ORA_ReceiptLine",
    "EXT_ORA_VendorContract",
    "EXT_SQL_SupplierTransactions",
    "ING_FILE_SupplierCatalog",
    "STG_Load_Supplier",
    "STG_Load_PurchaseOrder",
    "STG_Load_VendorContract",
    "DQ_Supplier_Screen",
    "DIM_Load_Supplier",
    "DIM_Load_VendorContract",
    "FACT_Load_Purchase",
    "FACT_Load_PurchaseReceipt",
    "FACT_Load_SupplierTransaction",
    "PRC_Load_PurchaseSpend",
    "PRC_Load_ReceiptMatching",
    "PRC_Load_ContractCompliance",
    "PRC_Load_SupplierScorecard",
    "PRC_Export_SupplierStatement",
    "AGG_Refresh_SupplierPerformance",
]

# Package parameters (SSIS Project.params / package variables), Databricks defaults
PARAMS = {
    "receiptMatchQtyTolerancePct": 2.0,
    "receiptMatchPriceTolerancePct": 1.0,
    "grniAccrualCutoffDays": 45,
    "scorecardWindowDays": 90,
    "scorecardMinimumOrders": 5,
    "complianceWindowDays": 365,
    "priceLeakageTolerancePct": 2.0,
    "excludeSelfBillingSuppliers": True,
    "dormantSupplierMonths": 84,
    "statementCurrency": "USD",
}


def qualified(tableName, schema=SCHEMA):
    return f"{CATALOG}.{schema}.{tableName}"


def volumePath(volumeName):
    return f"/Volumes/{CATALOG}/{SCHEMA}/{volumeName}"
