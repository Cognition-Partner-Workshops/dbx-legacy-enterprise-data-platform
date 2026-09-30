"""Package orchestration: maps every SSIS package name to its Databricks implementation and runs a
list of packages in inventory order inside one batch (the SSIS Master_Daily_ETL batch id)."""

from procurement import aggregates, catalog_ingest, dimensions, extracts, facts, marts, quality, staging
from procurement.config import PACKAGES

PACKAGE_RUNNERS = {
    "EXT_ORA_SupplierMaster": extracts.runSupplierMaster,
    "EXT_ORA_PurchaseOrderHdr": extracts.runPurchaseOrderHdr,
    "EXT_ORA_PurchaseOrderLine": extracts.runPurchaseOrderLine,
    "EXT_ORA_ReceiptLine": extracts.runReceiptLine,
    "EXT_ORA_VendorContract": extracts.runVendorContract,
    "EXT_SQL_SupplierTransactions": extracts.runSupplierTransactions,
    "ING_FILE_SupplierCatalog": catalog_ingest.runSupplierCatalog,
    "STG_Load_Supplier": staging.runSupplier,
    "STG_Load_PurchaseOrder": staging.runPurchaseOrder,
    "STG_Load_VendorContract": staging.runVendorContract,
    "DQ_Supplier_Screen": quality.runSupplierScreen,
    "DIM_Load_Supplier": dimensions.runDimSupplier,
    "DIM_Load_VendorContract": dimensions.runDimVendorContract,
    "FACT_Load_Purchase": facts.runFactPurchase,
    "FACT_Load_PurchaseReceipt": facts.runFactPurchaseReceipt,
    "FACT_Load_SupplierTransaction": facts.runFactSupplierTransaction,
    "PRC_Load_PurchaseSpend": marts.runPurchaseSpend,
    "PRC_Load_ReceiptMatching": marts.runReceiptMatching,
    "PRC_Load_ContractCompliance": marts.runContractCompliance,
    "PRC_Load_SupplierScorecard": marts.runSupplierScorecard,
    "PRC_Export_SupplierStatement": marts.runSupplierStatement,
    "AGG_Refresh_SupplierPerformance": aggregates.runAggregateSupplierPerformance,
}

LAYERS = {
    "extract": PACKAGES[0:7],
    "stage": PACKAGES[7:11],
    "dimension": PACKAGES[11:13],
    "fact": PACKAGES[13:16],
    "mart": PACKAGES[16:22],
}


def resolvePackages(spec):
    """'extract' | 'all' | 'PKG_A,PKG_B' -> ordered list of package names."""
    spec = (spec or "all").strip()
    if spec == "all":
        return list(PACKAGES)
    if spec in LAYERS:
        return list(LAYERS[spec])
    names = [p.strip() for p in spec.split(",") if p.strip()]
    unknown = [n for n in names if n not in PACKAGE_RUNNERS]
    if unknown:
        raise ValueError(f"unknown packages: {unknown}")
    return sorted(names, key=PACKAGES.index)


def runPackages(spark, batchId, packageSpec):
    results = {}
    for name in resolvePackages(packageSpec):
        print(f"[{batchId}] running {name}")
        results[name] = PACKAGE_RUNNERS[name](spark, batchId)
        print(f"[{batchId}] {name} -> {results[name]}")
    return results
