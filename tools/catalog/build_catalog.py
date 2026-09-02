"""Emit config/estate-catalog.yaml, the authoritative object contract for the estate.

The catalog is the single source of truth that every work package codes against:
schema and object names declared here must match the files produced under
oracle/, sqlserver/ and ssis/. Regenerate with:

    python3 tools/catalog/build_catalog.py

Deterministic: no clocks, no randomness. Ordering is the literal ordering below.
"""

from __future__ import annotations

import os
from collections import OrderedDict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_PATH = os.path.join(REPO_ROOT, "config", "estate-catalog.yaml")

# ---------------------------------------------------------------------------
# Oracle ERP source system (WWI Global ERP - "GERP")
# ---------------------------------------------------------------------------

ORACLE = OrderedDict(
    [
        (
            "WWI_MDM",
            {
                "domain": "Master data",
                "description": "Customer, supplier, product and party master owned by the ERP.",
                "tables": [
                    "CUST_MASTER",
                    "CUST_ADDRESS",
                    "CUST_CONTACT",
                    "CUST_CLASSIFICATION",
                    "CUST_CREDIT_PROFILE",
                    "CUST_HIERARCHY",
                    "CUST_SEGMENT_ASSIGN",
                    "SUPP_MASTER",
                    "SUPP_ADDRESS",
                    "SUPP_CONTACT",
                    "SUPP_BANK_ACCOUNT",
                    "SUPP_CERTIFICATION",
                    "PRODUCT_MASTER",
                    "PRODUCT_CATEGORY",
                    "PRODUCT_HIERARCHY",
                    "PRODUCT_UOM_CONV",
                    "PRODUCT_SUBSTITUTE",
                    "PARTY_XREF",
                    "MDM_MERGE_HISTORY",
                ],
            },
        ),
        (
            "WWI_PROC",
            {
                "domain": "Procurement",
                "description": "Requisition-to-receipt procurement transactions.",
                "tables": [
                    "REQUISITION_HDR",
                    "REQUISITION_LINE",
                    "SOURCING_EVENT",
                    "SUPPLIER_QUOTE",
                    "SUPPLIER_QUOTE_LINE",
                    "PURCHASE_ORDER_HDR",
                    "PURCHASE_ORDER_LINE",
                    "PO_CHANGE_ORDER",
                    "PO_RECEIPT_HDR",
                    "PO_RECEIPT_LINE",
                    "GOODS_RETURN_HDR",
                    "GOODS_RETURN_LINE",
                    "VENDOR_CONTRACT",
                    "VENDOR_CONTRACT_LINE",
                    "SUPPLIER_SCORECARD",
                ],
            },
        ),
        (
            "WWI_FIN",
            {
                "domain": "Finance / accounts payable",
                "description": "AP invoices, payments and general ledger postings.",
                "tables": [
                    "AP_INVOICE_HDR",
                    "AP_INVOICE_LINE",
                    "AP_INVOICE_HOLD",
                    "AP_PAYMENT",
                    "AP_PAYMENT_APPLY",
                    "AP_AGING_SNAPSHOT",
                    "GL_ACCOUNT",
                    "GL_JOURNAL_HDR",
                    "GL_JOURNAL_LINE",
                    "GL_PERIOD_STATUS",
                    "COST_CENTER",
                    "COST_ALLOCATION_RULE",
                    "PAYMENT_TERMS",
                    "TAX_RATE",
                    "TAX_JURISDICTION",
                    "WITHHOLDING_RULE",
                ],
            },
        ),
        (
            "WWI_REF",
            {
                "domain": "Reference data",
                "description": "Enterprise reference and code tables, fully refreshed weekly.",
                "tables": [
                    "CURRENCY_CODE",
                    "FX_RATE_DAILY",
                    "COUNTRY_REF",
                    "REGION_REF",
                    "CITY_REF",
                    "POSTAL_REF",
                    "UOM_REF",
                    "LANGUAGE_REF",
                    "INCOTERM_REF",
                    "PAYMENT_METHOD_REF",
                    "STATUS_CODE_REF",
                    "REASON_CODE_REF",
                    "CALENDAR_FISCAL",
                    "SOURCE_SYSTEM_REF",
                    "CODE_TRANSLATION",
                ],
            },
        ),
        (
            "WWI_AUDIT",
            {
                "domain": "Source-side change control",
                "description": "ERP-side change capture and extract bookkeeping.",
                "tables": [
                    "CHANGE_LOG",
                    "EXTRACT_CONTROL",
                    "INTERFACE_ERROR",
                    "PURGE_LOG",
                ],
            },
        ),
    ]
)

ORACLE_PACKAGES = [
    ("WWI_MDM", "PKG_CUSTOMER_MASTER", "Customer master maintenance and merge handling"),
    ("WWI_MDM", "PKG_SUPPLIER_MASTER", "Supplier master maintenance and certification checks"),
    ("WWI_MDM", "PKG_PRODUCT_MASTER", "Product master, hierarchy and UOM conversion"),
    ("WWI_PROC", "PKG_PURCHASE_ORDER", "Purchase order lifecycle and change orders"),
    ("WWI_PROC", "PKG_RECEIPTS", "Goods receipt matching and returns"),
    ("WWI_PROC", "PKG_SUPPLIER_PERF", "Supplier scorecard calculation"),
    ("WWI_FIN", "PKG_AP_INVOICE", "AP invoice validation, holds and three-way match"),
    ("WWI_FIN", "PKG_AP_PAYMENT", "Payment run, application and withholding"),
    ("WWI_FIN", "PKG_GL_POSTING", "GL journal creation and period control"),
    ("WWI_FIN", "PKG_TAX", "Tax rate resolution and jurisdiction lookup"),
    ("WWI_REF", "PKG_FX", "FX rate loading and currency conversion"),
    ("WWI_REF", "PKG_CODE_TRANSLATION", "Code translation between ERP and downstream systems"),
    ("WWI_AUDIT", "PKG_EXTRACT_CONTROL", "Extract watermark bookkeeping for downstream ETL"),
    ("WWI_AUDIT", "PKG_DATA_QUALITY", "Source-side data-quality screening"),
]

ORACLE_VIEWS = [
    ("WWI_MDM", "V_CUSTOMER_EXTRACT"),
    ("WWI_MDM", "V_CUSTOMER_ADDRESS_CURRENT"),
    ("WWI_MDM", "V_SUPPLIER_EXTRACT"),
    ("WWI_MDM", "V_SUPPLIER_BANK_MASKED"),
    ("WWI_MDM", "V_PRODUCT_EXTRACT"),
    ("WWI_MDM", "V_PRODUCT_HIERARCHY_FLAT"),
    ("WWI_PROC", "V_PURCHASE_ORDER_EXTRACT"),
    ("WWI_PROC", "V_PO_LINE_EXTRACT"),
    ("WWI_PROC", "V_OPEN_PO_BALANCE"),
    ("WWI_PROC", "V_RECEIPT_EXTRACT"),
    ("WWI_PROC", "V_SUPPLIER_SCORECARD_CURRENT"),
    ("WWI_FIN", "V_AP_INVOICE_EXTRACT"),
    ("WWI_FIN", "V_AP_PAYMENT_EXTRACT"),
    ("WWI_FIN", "V_AP_AGING_CURRENT"),
    ("WWI_FIN", "V_GL_JOURNAL_EXTRACT"),
    ("WWI_FIN", "V_COST_CENTER_HIERARCHY"),
    ("WWI_REF", "V_CURRENCY_EXTRACT"),
    ("WWI_REF", "V_FX_RATE_LATEST"),
    ("WWI_REF", "V_GEOGRAPHY_EXTRACT"),
    ("WWI_REF", "V_PAYMENT_TERMS_EXTRACT"),
    ("WWI_AUDIT", "V_EXTRACT_WATERMARK"),
]

ORACLE_FUNCTIONS = [
    ("WWI_MDM", "FN_NORMALIZE_NAME"),
    ("WWI_MDM", "FN_CUSTOMER_STATUS"),
    ("WWI_MDM", "FN_PRODUCT_ACTIVE_FLAG"),
    ("WWI_FIN", "FN_CONVERT_AMOUNT"),
    ("WWI_FIN", "FN_TAX_AMOUNT"),
    ("WWI_FIN", "FN_DUE_DATE"),
    ("WWI_FIN", "FN_AGING_BUCKET"),
    ("WWI_PROC", "FN_PO_OPEN_QTY"),
    ("WWI_PROC", "FN_RECEIPT_VARIANCE_PCT"),
    ("WWI_REF", "FN_TRANSLATE_CODE"),
    ("WWI_REF", "FN_FISCAL_PERIOD"),
]

# ---------------------------------------------------------------------------
# SQL Server OLTP (WideWorldImporters, expanded)
# ---------------------------------------------------------------------------

OLTP_NEW_TABLES = OrderedDict(
    [
        (
            "Sales",
            [
                "SalesChannels",
                "Promotions",
                "PromotionLines",
                "PromotionRedemptions",
                "PriceLists",
                "PriceListLines",
                "OrderDiscounts",
                "SalesTerritories",
                "SalesQuotas",
                "CommissionPlans",
                "CommissionAccruals",
                "CustomerSegments",
                "CustomerSegmentAssignments",
                "QuoteHeaders",
                "QuoteLines",
            ],
        ),
        (
            "Warehouse",
            [
                "Bins",
                "BinContents",
                "StockMovementDetails",
                "StockTransfers",
                "StockTransferLines",
                "CycleCounts",
                "CycleCountLines",
                "ReplenishmentRules",
                "ReplenishmentOrders",
                "WarehouseSites",
            ],
        ),
        (
            "Shipping",
            [
                "Carriers",
                "FreightRates",
                "ShipmentHeaders",
                "ShipmentLines",
                "ShipmentEvents",
                "DeliveryRoutes",
                "DeliveryStops",
                "PackagingTypes",
                "CustomsDeclarations",
            ],
        ),
        (
            "Returns",
            [
                "ReturnReasons",
                "ReturnAuthorizations",
                "ReturnLines",
                "ReturnInspections",
                "CreditNotes",
                "CreditNoteLines",
            ],
        ),
        (
            "Loyalty",
            [
                "LoyaltyPrograms",
                "LoyaltyTiers",
                "LoyaltyMembers",
                "LoyaltyPointsLedger",
                "LoyaltyRedemptions",
            ],
        ),
        (
            "Ecommerce",
            [
                "WebSessions",
                "CartHeaders",
                "CartLines",
                "ProductReviews",
                "WishLists",
                "WishListLines",
            ],
        ),
        (
            "Integration",
            [
                "OutboundInterfaceQueue",
                "InboundFileRegister",
                "ChangeTrackingWatermark",
            ],
        ),
    ]
)

OLTP_PROCS = [
    ("Sales", "ApplyPromotionToOrder"),
    ("Sales", "CalculateOrderDiscounts"),
    ("Sales", "RecalculateCommissionAccruals"),
    ("Sales", "AssignCustomerSegments"),
    ("Sales", "ConvertQuoteToOrder"),
    ("Sales", "RefreshSalesQuotaAttainment"),
    ("Warehouse", "PostStockMovement"),
    ("Warehouse", "ReconcileCycleCount"),
    ("Warehouse", "GenerateReplenishmentOrders"),
    ("Warehouse", "TransferStockBetweenSites"),
    ("Shipping", "CreateShipmentFromOrder"),
    ("Shipping", "RecordShipmentEvent"),
    ("Shipping", "RateShipment"),
    ("Returns", "AuthorizeReturn"),
    ("Returns", "PostReturnInspection"),
    ("Returns", "IssueCreditNote"),
    ("Loyalty", "AccruePointsForInvoice"),
    ("Loyalty", "RedeemLoyaltyPoints"),
    ("Loyalty", "RecalculateMemberTier"),
    ("Integration", "EnqueueOutboundChanges"),
    ("Integration", "RegisterInboundFile"),
    ("Integration", "GetChangeWatermark"),
    ("Integration", "SetChangeWatermark"),
]

OLTP_VIEWS = [
    ("Sales", "vw_OrderLineExtract"),
    ("Sales", "vw_InvoiceExtract"),
    ("Sales", "vw_PromotionEffectiveness"),
    ("Sales", "vw_CustomerSegmentCurrent"),
    ("Sales", "vw_SalespersonPerformance"),
    ("Warehouse", "vw_StockOnHandBySite"),
    ("Warehouse", "vw_StockMovementExtract"),
    ("Warehouse", "vw_CycleCountVariance"),
    ("Shipping", "vw_ShipmentExtract"),
    ("Shipping", "vw_DeliveryPerformance"),
    ("Returns", "vw_ReturnExtract"),
    ("Returns", "vw_CreditNoteExtract"),
    ("Loyalty", "vw_LoyaltyBalance"),
    ("Ecommerce", "vw_WebConversionFunnel"),
]

OLTP_FUNCTIONS = [
    ("Sales", "fn_LineNetAmount"),
    ("Sales", "fn_DiscountPercentForCustomer"),
    ("Sales", "fn_CommissionRate"),
    ("Warehouse", "fn_AvailableToPromise"),
    ("Shipping", "fn_FreightCost"),
    ("Returns", "fn_RestockingFee"),
    ("Loyalty", "fn_PointsForAmount"),
]

# ---------------------------------------------------------------------------
# SQL Server staging database (WWI_Staging)
# ---------------------------------------------------------------------------

STAGING_TABLES = OrderedDict(
    [
        (
            "raw",
            [
                "OracleCustomerMaster",
                "OracleCustomerAddress",
                "OracleSupplierMaster",
                "OracleProductMaster",
                "OraclePurchaseOrderHdr",
                "OraclePurchaseOrderLine",
                "OracleReceiptLine",
                "OracleApInvoiceHdr",
                "OracleApInvoiceLine",
                "OracleApPayment",
                "OracleGlJournalLine",
                "OracleCostCenter",
                "OracleCurrency",
                "OracleFxRate",
                "OracleTaxRate",
                "OraclePaymentTerms",
                "OracleGeography",
                "OracleVendorContract",
                "SqlOrder",
                "SqlOrderLine",
                "SqlInvoice",
                "SqlInvoiceLine",
                "SqlStockItem",
                "SqlStockMovement",
                "SqlShipment",
                "SqlShipmentLine",
                "SqlReturnLine",
                "SqlCreditNote",
                "SqlLoyaltyLedger",
                "SqlWebSession",
                "FilePartnerSales",
                "FileCarrierScan",
                "FileSupplierCatalog",
                "FileFxOverride",
            ],
        ),
        (
            "stg",
            [
                "Customer",
                "CustomerAddress",
                "Supplier",
                "Product",
                "PurchaseOrder",
                "PurchaseOrderLine",
                "Receipt",
                "ApInvoice",
                "ApInvoiceLine",
                "Payment",
                "GlJournalLine",
                "CostCenter",
                "Currency",
                "FxRate",
                "TaxRate",
                "PaymentTerms",
                "Geography",
                "VendorContract",
                "Order",
                "OrderLine",
                "Sale",
                "SaleLine",
                "StockItem",
                "StockMovement",
                "Shipment",
                "ShipmentLine",
                "Return",
                "CreditNote",
                "LoyaltyLedger",
                "WebSession",
                "PartnerSale",
                "Promotion",
                "SalesTerritory",
                "Employee",
                "Salesperson",
            ],
        ),
        (
            "work",
            [
                "CustomerDedup",
                "CustomerAddressStandardized",
                "SupplierDedup",
                "ProductCrosswalk",
                "OrderLineEnriched",
                "SaleLineEnriched",
                "PurchaseLineEnriched",
                "PaymentMatched",
                "InventoryPositionDaily",
                "CurrencyConversionScratch",
                "LateArrivingDimensionQueue",
                "FactRekeyQueue",
            ],
        ),
        (
            "err",
            [
                "RejectedCustomer",
                "RejectedSupplier",
                "RejectedProduct",
                "RejectedOrderLine",
                "RejectedInvoiceLine",
                "RejectedPayment",
                "RejectedShipment",
                "RejectedFileRow",
                "RejectedLookupFailure",
                "RejectedConstraintViolation",
            ],
        ),
    ]
)

STAGING_PROCS = [
    ("stg", "usp_TruncateAndReload_Customer"),
    ("stg", "usp_TruncateAndReload_Supplier"),
    ("stg", "usp_TruncateAndReload_Product"),
    ("stg", "usp_TruncateAndReload_Geography"),
    ("stg", "usp_AppendIncremental_OrderLine"),
    ("stg", "usp_AppendIncremental_SaleLine"),
    ("stg", "usp_AppendIncremental_Payment"),
    ("stg", "usp_AppendIncremental_StockMovement"),
    ("stg", "usp_AppendIncremental_Shipment"),
    ("stg", "usp_NormalizeCustomer"),
    ("stg", "usp_NormalizeSupplier"),
    ("stg", "usp_NormalizeAddress"),
    ("stg", "usp_TranslateSourceCodes"),
    ("stg", "usp_ConvertCurrencyAmounts"),
    ("stg", "usp_DeduplicateCustomer"),
    ("stg", "usp_DeduplicateOrderLine"),
    ("work", "usp_BuildProductCrosswalk"),
    ("work", "usp_BuildInventoryPositionDaily"),
    ("work", "usp_MatchPaymentsToInvoices"),
    ("work", "usp_QueueLateArrivingDimensions"),
    ("err", "usp_LogRejectedRows"),
    ("err", "usp_PurgeRejectedRows"),
]

STAGING_VIEWS = [
    ("stg", "vw_CustomerReadyForDimension"),
    ("stg", "vw_SupplierReadyForDimension"),
    ("stg", "vw_ProductReadyForDimension"),
    ("stg", "vw_OrderLineReadyForFact"),
    ("stg", "vw_SaleLineReadyForFact"),
    ("stg", "vw_PurchaseLineReadyForFact"),
    ("stg", "vw_PaymentReadyForFact"),
    ("stg", "vw_ShipmentReadyForFact"),
    ("err", "vw_RejectSummaryByBatch"),
    ("err", "vw_RejectTrend"),
]

STAGING_FUNCTIONS = [
    ("stg", "fn_CleanString"),
    ("stg", "fn_SafeDate"),
    ("stg", "fn_SafeDecimal"),
    ("stg", "fn_StandardizePostalCode"),
    ("stg", "fn_StandardizePhone"),
    ("stg", "fn_SourceSystemKey"),
]

# ---------------------------------------------------------------------------
# SQL Server data warehouse (WideWorldImportersDW, expanded)
# ---------------------------------------------------------------------------

DIMENSIONS = [
    ("Date", "role-playing", "N/A"),
    ("Customer", "SCD2", "Oracle WWI_MDM.CUST_MASTER"),
    ("Customer Category", "SCD1", "Oracle WWI_MDM.CUST_CLASSIFICATION"),
    ("Supplier", "SCD2", "Oracle WWI_MDM.SUPP_MASTER"),
    ("Vendor Contract", "SCD2", "Oracle WWI_PROC.VENDOR_CONTRACT"),
    ("Stock Item", "SCD2", "SQL Server Warehouse.StockItems"),
    ("Product Category", "SCD1", "Oracle WWI_MDM.PRODUCT_CATEGORY"),
    ("Employee", "SCD2", "SQL Server Application.People"),
    ("Salesperson", "SCD2", "SQL Server Application.People"),
    ("City", "SCD2", "SQL Server Application.Cities"),
    ("Geography", "SCD1", "Oracle WWI_REF.COUNTRY_REF"),
    ("Sales Territory", "SCD1", "SQL Server Sales.SalesTerritories"),
    ("Customer Segment", "SCD2", "SQL Server Sales.CustomerSegments"),
    ("Payment Method", "SCD1", "SQL Server Application.PaymentMethods"),
    ("Transaction Type", "SCD1", "SQL Server Application.TransactionTypes"),
    ("Currency", "SCD1", "Oracle WWI_REF.CURRENCY_CODE"),
    ("Cost Center", "SCD2", "Oracle WWI_FIN.COST_CENTER"),
    ("Warehouse Site", "SCD1", "SQL Server Warehouse.WarehouseSites"),
    ("Carrier", "SCD1", "SQL Server Shipping.Carriers"),
    ("Promotion", "SCD2", "SQL Server Sales.Promotions"),
    ("Sales Channel", "SCD1", "SQL Server Sales.SalesChannels"),
    ("Return Reason", "SCD1", "SQL Server Returns.ReturnReasons"),
    ("Loyalty Tier", "SCD1", "SQL Server Loyalty.LoyaltyTiers"),
    ("Payment Terms", "SCD1", "Oracle WWI_FIN.PAYMENT_TERMS"),
]

FACTS = [
    ("Sale", "transaction"),
    ("Order", "transaction"),
    ("Purchase", "transaction"),
    ("Purchase Receipt", "transaction"),
    ("Payment", "transaction"),
    ("Supplier Payment", "transaction"),
    ("Movement", "transaction"),
    ("Stock Holding", "periodic snapshot"),
    ("Transaction", "transaction"),
    ("Customer Transaction", "transaction"),
    ("Supplier Transaction", "transaction"),
    ("Shipment", "accumulating snapshot"),
    ("Return", "transaction"),
    ("Credit Note", "transaction"),
    ("Loyalty Points", "transaction"),
    ("Web Session", "transaction"),
    ("Daily Inventory Snapshot", "periodic snapshot"),
    ("Daily Sales Snapshot", "periodic snapshot"),
    ("Order Fulfilment", "accumulating snapshot"),
    ("GL Posting", "transaction"),
]

AGGREGATES = [
    "Daily Sales Summary",
    "Monthly Sales Summary",
    "Daily Inventory Health",
    "Monthly Margin Analysis",
    "Customer 360",
    "Customer Rolling 12 Month",
    "Product Performance",
    "Supplier Performance",
    "Regional Sales Performance",
    "Finance Close Summary",
    "Promotion Effectiveness",
    "Delivery Performance Summary",
]

REPORT_VIEWS = [
    "vw_SalesByCustomerMonth",
    "vw_SalesByProductMonth",
    "vw_SalesByTerritoryMonth",
    "vw_MarginByProductCategory",
    "vw_InventoryHealthCurrent",
    "vw_SupplierSpendYtd",
    "vw_SupplierOnTimeDelivery",
    "vw_ApAgingCurrent",
    "vw_Customer360",
    "vw_CustomerChurnRisk",
    "vw_PromotionRoi",
    "vw_OrderToCashCycle",
    "vw_ReturnsRateByCategory",
    "vw_LoyaltyProgramSummary",
    "vw_FinanceCloseStatus",
    "vw_DailySalesTrend",
]

DW_PROCS = [
    ("Integration", "MigrateStagedCustomerDataV2"),
    ("Integration", "MigrateStagedSupplierDataV2"),
    ("Integration", "MigrateStagedProductData"),
    ("Integration", "MigrateStagedGeographyData"),
    ("Integration", "MigrateStagedPromotionData"),
    ("Integration", "MigrateStagedSalesTerritoryData"),
    ("Integration", "MigrateStagedCostCenterData"),
    ("Integration", "MigrateStagedCurrencyData"),
    ("Integration", "MigrateStagedCarrierData"),
    ("Integration", "MigrateStagedReturnReasonData"),
    ("Integration", "MigrateStagedLoyaltyTierData"),
    ("Integration", "MigrateStagedVendorContractData"),
    ("Integration", "MigrateStagedPaymentTermsData"),
    ("Integration", "MigrateStagedCustomerSegmentData"),
    ("Integration", "MigrateStagedWarehouseSiteData"),
    ("Integration", "LoadFactSale"),
    ("Integration", "LoadFactOrder"),
    ("Integration", "LoadFactPurchase"),
    ("Integration", "LoadFactPurchaseReceipt"),
    ("Integration", "LoadFactPayment"),
    ("Integration", "LoadFactSupplierPayment"),
    ("Integration", "LoadFactMovement"),
    ("Integration", "LoadFactStockHolding"),
    ("Integration", "LoadFactShipment"),
    ("Integration", "LoadFactReturn"),
    ("Integration", "LoadFactCreditNote"),
    ("Integration", "LoadFactLoyaltyPoints"),
    ("Integration", "LoadFactWebSession"),
    ("Integration", "LoadFactDailyInventorySnapshot"),
    ("Integration", "LoadFactDailySalesSnapshot"),
    ("Integration", "LoadFactOrderFulfilment"),
    ("Integration", "LoadFactGlPosting"),
    ("Integration", "RekeyLateArrivingDimensions"),
    ("Integration", "ApplyFactCorrections"),
    ("Integration", "DeduplicateFactSale"),
    ("Integration", "EnsureUnknownMembers"),
    ("Integration", "PopulateDateDimensionRange"),
    ("Integration", "RefreshAggregateDailySales"),
    ("Integration", "RefreshAggregateMonthlySales"),
    ("Integration", "RefreshAggregateInventoryHealth"),
    ("Integration", "RefreshAggregateMarginAnalysis"),
    ("Integration", "RefreshAggregateCustomer360"),
    ("Integration", "RefreshAggregateProductPerformance"),
    ("Integration", "RefreshAggregateSupplierPerformance"),
    ("Integration", "RefreshAggregateRegionalSales"),
    ("Integration", "RefreshAggregateFinanceClose"),
    ("Integration", "RefreshAggregatePromotionEffectiveness"),
    ("Integration", "RefreshAggregateDeliveryPerformance"),
    ("Integration", "PublishReportingLayer"),
    ("Integration", "RebuildColumnstoreIndexes"),
]

DW_FUNCTIONS = [
    ("Integration", "GenerateDateDimensionColumnsV2"),
    ("Integration", "fn_SurrogateKeyForCustomer"),
    ("Integration", "fn_SurrogateKeyForStockItem"),
    ("Integration", "fn_ConvertToReportingCurrency"),
    ("Integration", "fn_FiscalPeriodForDate"),
    ("Integration", "fn_MarginPercent"),
    ("Integration", "fn_IsBusinessDay"),
    ("Integration", "fn_AgeBucket"),
]

# ---------------------------------------------------------------------------
# ETL control framework (deployed into every SQL Server database as schema etl)
# ---------------------------------------------------------------------------

CONTROL_TABLES = [
    "Batch",
    "BatchStep",
    "PackageExecution",
    "Watermark",
    "RowCountAudit",
    "ErrorLog",
    "RejectedRecord",
    "SourceSystem",
    "Configuration",
    "DependencyEdge",
    "ReconciliationResult",
    "DataQualityRule",
    "DataQualityResult",
]

CONTROL_PROCS = [
    "StartBatch",
    "EndBatch",
    "StartBatchStep",
    "EndBatchStep",
    "LogPackageStart",
    "LogPackageEnd",
    "LogError",
    "LogRowCount",
    "LogRejectedRecord",
    "GetWatermark",
    "SetWatermark",
    "GetConfigurationValue",
    "EvaluateDataQualityRules",
    "AssertRowCountTolerance",
    "PurgeControlHistory",
]

CONTROL_VIEWS = [
    "vw_BatchStatus",
    "vw_PackageDurations",
    "vw_RowCountVariance",
    "vw_ErrorsLast7Days",
    "vw_WatermarkCurrent",
    "vw_DataQualityFailures",
]

# ---------------------------------------------------------------------------
# SSIS estate
# ---------------------------------------------------------------------------
# Each entry: (package, folder, domain, source_system, source_objects,
#              target_system, target_objects, load_type, parent, procs, criticality)

REGIONS = ["NA", "EU", "APAC"]


def _oracle_extracts():
    """Oracle extract packages - one family per ERP domain."""
    specs = [
        ("EXT_ORA_CustomerMaster", "WWI_MDM.CUST_MASTER", "raw.OracleCustomerMaster", "incremental_timestamp", "high"),
        ("EXT_ORA_CustomerAddress", "WWI_MDM.CUST_ADDRESS", "raw.OracleCustomerAddress", "incremental_timestamp", "high"),
        ("EXT_ORA_SupplierMaster", "WWI_MDM.SUPP_MASTER", "raw.OracleSupplierMaster", "incremental_timestamp", "high"),
        ("EXT_ORA_ProductMaster", "WWI_MDM.PRODUCT_MASTER", "raw.OracleProductMaster", "incremental_timestamp", "high"),
        ("EXT_ORA_ProductHierarchy", "WWI_MDM.PRODUCT_HIERARCHY", "raw.OracleProductMaster", "full", "medium"),
        ("EXT_ORA_PurchaseOrderHdr", "WWI_PROC.PURCHASE_ORDER_HDR", "raw.OraclePurchaseOrderHdr", "incremental_timestamp", "high"),
        ("EXT_ORA_PurchaseOrderLine", "WWI_PROC.PURCHASE_ORDER_LINE", "raw.OraclePurchaseOrderLine", "incremental_key", "high"),
        ("EXT_ORA_ReceiptLine", "WWI_PROC.PO_RECEIPT_LINE", "raw.OracleReceiptLine", "incremental_key", "high"),
        ("EXT_ORA_VendorContract", "WWI_PROC.VENDOR_CONTRACT", "raw.OracleVendorContract", "full", "medium"),
        ("EXT_ORA_ApInvoiceHdr", "WWI_FIN.AP_INVOICE_HDR", "raw.OracleApInvoiceHdr", "incremental_timestamp", "high"),
        ("EXT_ORA_ApInvoiceLine", "WWI_FIN.AP_INVOICE_LINE", "raw.OracleApInvoiceLine", "incremental_key", "high"),
        ("EXT_ORA_ApPayment", "WWI_FIN.AP_PAYMENT", "raw.OracleApPayment", "incremental_timestamp", "high"),
        ("EXT_ORA_ApPaymentApply", "WWI_FIN.AP_PAYMENT_APPLY", "raw.OracleApPayment", "incremental_key", "medium"),
        ("EXT_ORA_ApAging", "WWI_FIN.AP_AGING_SNAPSHOT", "raw.OracleApInvoiceHdr", "full", "medium"),
        ("EXT_ORA_GlJournalLine", "WWI_FIN.GL_JOURNAL_LINE", "raw.OracleGlJournalLine", "date_window", "high"),
        ("EXT_ORA_CostCenter", "WWI_FIN.COST_CENTER", "raw.OracleCostCenter", "full", "medium"),
        ("EXT_ORA_TaxRate", "WWI_FIN.TAX_RATE", "raw.OracleTaxRate", "full", "medium"),
        ("EXT_ORA_PaymentTerms", "WWI_FIN.PAYMENT_TERMS", "raw.OraclePaymentTerms", "full", "medium"),
        ("EXT_ORA_Currency", "WWI_REF.CURRENCY_CODE", "raw.OracleCurrency", "full", "high"),
        ("EXT_ORA_FxRateDaily", "WWI_REF.FX_RATE_DAILY", "raw.OracleFxRate", "date_window", "high"),
        ("EXT_ORA_Geography", "WWI_REF.V_GEOGRAPHY_EXTRACT", "raw.OracleGeography", "full", "medium"),
        ("EXT_ORA_CodeTranslation", "WWI_REF.CODE_TRANSLATION", "raw.OracleCustomerMaster", "full", "medium"),
    ]
    out = []
    for name, src, tgt, load, crit in specs:
        out.append(
            dict(
                package=name,
                project="WWI_Extract_Oracle",
                folder="01_oracle_extract",
                domain="extract",
                source_system="Oracle",
                source_objects=[src],
                target_system="SQL Server Staging",
                target_objects=[tgt],
                load_type=load,
                parent="Master_Daily_ETL",
                procs=["etl.LogPackageStart", "etl.GetWatermark", "etl.SetWatermark", "etl.LogRowCount"],
                criticality=crit,
            )
        )
    return out


def _sqlserver_extracts():
    specs = [
        ("EXT_SQL_Orders", "Sales.Orders", "raw.SqlOrder", "incremental_key", "high"),
        ("EXT_SQL_OrderLines", "Sales.OrderLines", "raw.SqlOrderLine", "incremental_key", "high"),
        ("EXT_SQL_Invoices", "Sales.Invoices", "raw.SqlInvoice", "incremental_key", "high"),
        ("EXT_SQL_InvoiceLines", "Sales.InvoiceLines", "raw.SqlInvoiceLine", "incremental_key", "high"),
        ("EXT_SQL_Promotions", "Sales.Promotions", "raw.SqlOrder", "full", "medium"),
        ("EXT_SQL_SalesTerritories", "Sales.SalesTerritories", "raw.SqlOrder", "full", "medium"),
        ("EXT_SQL_CustomerSegments", "Sales.CustomerSegmentAssignments", "raw.SqlOrder", "full", "medium"),
        ("EXT_SQL_StockItems", "Warehouse.StockItems", "raw.SqlStockItem", "incremental_timestamp", "high"),
        ("EXT_SQL_StockMovements", "Warehouse.StockItemTransactions", "raw.SqlStockMovement", "incremental_key", "high"),
        ("EXT_SQL_StockTransfers", "Warehouse.StockTransferLines", "raw.SqlStockMovement", "incremental_key", "medium"),
        ("EXT_SQL_Shipments", "Shipping.ShipmentHeaders", "raw.SqlShipment", "incremental_key", "high"),
        ("EXT_SQL_ShipmentLines", "Shipping.ShipmentLines", "raw.SqlShipmentLine", "incremental_key", "high"),
        ("EXT_SQL_Returns", "Returns.ReturnLines", "raw.SqlReturnLine", "incremental_key", "medium"),
        ("EXT_SQL_CreditNotes", "Returns.CreditNoteLines", "raw.SqlCreditNote", "incremental_key", "medium"),
        ("EXT_SQL_WebSessions", "Ecommerce.WebSessions", "raw.SqlWebSession", "date_window", "low"),
        ("EXT_SQL_CustomerTransactions", "Sales.CustomerTransactions", "raw.SqlInvoice", "incremental_key", "high"),
        ("EXT_SQL_SupplierTransactions", "Purchasing.SupplierTransactions", "raw.SqlInvoice", "incremental_key", "medium"),
        ("EXT_SQL_People", "Application.People", "raw.SqlOrder", "full", "medium"),
        ("EXT_SQL_Cities", "Application.Cities", "raw.OracleGeography", "full", "medium"),
        ("EXT_SQL_PaymentMethods", "Application.PaymentMethods", "raw.SqlInvoice", "full", "low"),
        ("EXT_SQL_TransactionTypes", "Application.TransactionTypes", "raw.SqlInvoice", "full", "low"),
    ]
    out = []
    for name, src, tgt, load, crit in specs:
        out.append(
            dict(
                package=name,
                project="WWI_Extract_SqlServer",
                folder="02_sqlserver_extract",
                domain="extract",
                source_system="SQL Server OLTP",
                source_objects=[src],
                target_system="SQL Server Staging",
                target_objects=[tgt],
                load_type=load,
                parent="Master_Daily_ETL",
                procs=["etl.LogPackageStart", "etl.GetWatermark", "etl.SetWatermark", "etl.LogRowCount"],
                criticality=crit,
            )
        )
    return out


def _file_ingestion():
    specs = [
        ("ING_FILE_PartnerSales_NA", "partner_sales_na_*.csv", "raw.FilePartnerSales", "NA"),
        ("ING_FILE_PartnerSales_EU", "partner_sales_eu_*.csv", "raw.FilePartnerSales", "EU"),
        ("ING_FILE_PartnerSales_APAC", "partner_sales_apac_*.txt", "raw.FilePartnerSales", "APAC"),
        ("ING_FILE_CarrierScan", "carrier_scan_*.csv", "raw.FileCarrierScan", "GLOBAL"),
        ("ING_FILE_SupplierCatalog", "supplier_catalog_*.psv", "raw.FileSupplierCatalog", "GLOBAL"),
        ("ING_FILE_FxOverride", "fx_override_*.csv", "raw.FileFxOverride", "GLOBAL"),
        ("ING_FILE_QuarantineMalformed", "quarantine/*", "err.RejectedFileRow", "GLOBAL"),
    ]
    out = []
    for name, src, tgt, region in specs:
        out.append(
            dict(
                package=name,
                project="WWI_Ingest_Files",
                folder="03_file_ingestion",
                domain="ingestion",
                source_system="File share",
                source_objects=[src],
                target_system="SQL Server Staging",
                target_objects=[tgt],
                load_type="file_ingest",
                parent="Master_Hourly_Incremental",
                procs=["etl.LogPackageStart", "etl.LogRejectedRecord", "etl.LogRowCount"],
                criticality="medium" if region == "GLOBAL" else "low",
                region=region,
            )
        )
    return out


def _staging_packages():
    specs = [
        ("STG_Load_Customer", ["raw.OracleCustomerMaster"], ["stg.Customer"], "truncate_reload", "stg.usp_TruncateAndReload_Customer"),
        ("STG_Load_CustomerAddress", ["raw.OracleCustomerAddress"], ["stg.CustomerAddress"], "truncate_reload", "stg.usp_NormalizeAddress"),
        ("STG_Load_Supplier", ["raw.OracleSupplierMaster"], ["stg.Supplier"], "truncate_reload", "stg.usp_TruncateAndReload_Supplier"),
        ("STG_Load_Product", ["raw.OracleProductMaster"], ["stg.Product"], "truncate_reload", "stg.usp_TruncateAndReload_Product"),
        ("STG_Load_Geography", ["raw.OracleGeography"], ["stg.Geography"], "truncate_reload", "stg.usp_TruncateAndReload_Geography"),
        ("STG_Load_Currency", ["raw.OracleCurrency", "raw.OracleFxRate"], ["stg.Currency", "stg.FxRate"], "truncate_reload", "stg.usp_ConvertCurrencyAmounts"),
        ("STG_Load_TaxAndTerms", ["raw.OracleTaxRate", "raw.OraclePaymentTerms"], ["stg.TaxRate", "stg.PaymentTerms"], "truncate_reload", "stg.usp_TranslateSourceCodes"),
        ("STG_Load_CostCenter", ["raw.OracleCostCenter"], ["stg.CostCenter"], "truncate_reload", "stg.usp_TranslateSourceCodes"),
        ("STG_Load_VendorContract", ["raw.OracleVendorContract"], ["stg.VendorContract"], "truncate_reload", "stg.usp_TranslateSourceCodes"),
        ("STG_Load_PurchaseOrder", ["raw.OraclePurchaseOrderHdr", "raw.OraclePurchaseOrderLine"], ["stg.PurchaseOrder", "stg.PurchaseOrderLine"], "incremental_append", "work.usp_BuildProductCrosswalk"),
        ("STG_Load_ApInvoice", ["raw.OracleApInvoiceHdr", "raw.OracleApInvoiceLine"], ["stg.ApInvoice", "stg.ApInvoiceLine"], "incremental_append", "stg.usp_ConvertCurrencyAmounts"),
        ("STG_Load_Payment", ["raw.OracleApPayment"], ["stg.Payment"], "incremental_append", "stg.usp_AppendIncremental_Payment"),
        ("STG_Load_GlJournal", ["raw.OracleGlJournalLine"], ["stg.GlJournalLine"], "incremental_append", "stg.usp_ConvertCurrencyAmounts"),
        ("STG_Load_Order", ["raw.SqlOrder", "raw.SqlOrderLine"], ["stg.Order", "stg.OrderLine"], "incremental_append", "stg.usp_AppendIncremental_OrderLine"),
        ("STG_Load_Sale", ["raw.SqlInvoice", "raw.SqlInvoiceLine"], ["stg.Sale", "stg.SaleLine"], "incremental_append", "stg.usp_AppendIncremental_SaleLine"),
        ("STG_Load_StockItem", ["raw.SqlStockItem"], ["stg.StockItem"], "truncate_reload", "stg.usp_TruncateAndReload_Product"),
        ("STG_Load_StockMovement", ["raw.SqlStockMovement"], ["stg.StockMovement"], "incremental_append", "stg.usp_AppendIncremental_StockMovement"),
        ("STG_Load_Shipment", ["raw.SqlShipment", "raw.SqlShipmentLine"], ["stg.Shipment", "stg.ShipmentLine"], "incremental_append", "stg.usp_AppendIncremental_Shipment"),
        ("STG_Load_ReturnAndCredit", ["raw.SqlReturnLine", "raw.SqlCreditNote"], ["stg.Return", "stg.CreditNote"], "incremental_append", "stg.usp_TranslateSourceCodes"),
        ("STG_Load_LoyaltyLedger", ["raw.SqlLoyaltyLedger"], ["stg.LoyaltyLedger"], "incremental_append", "stg.usp_TranslateSourceCodes"),
        ("STG_Load_WebSession", ["raw.SqlWebSession"], ["stg.WebSession"], "incremental_append", "stg.usp_CleanStringBatch"),
        ("STG_Load_PartnerSale", ["raw.FilePartnerSales"], ["stg.PartnerSale"], "incremental_append", "stg.usp_NormalizeCustomer"),
        ("STG_Load_Employee", ["raw.SqlOrder"], ["stg.Employee", "stg.Salesperson"], "truncate_reload", "stg.usp_NormalizeCustomer"),
        ("STG_Load_PromotionAndTerritory", ["raw.SqlOrder"], ["stg.Promotion", "stg.SalesTerritory"], "truncate_reload", "stg.usp_TranslateSourceCodes"),
        ("STG_Work_ProductCrosswalk", ["stg.Product", "stg.StockItem"], ["work.ProductCrosswalk"], "work_rebuild", "work.usp_BuildProductCrosswalk"),
        ("STG_Work_InventoryPosition", ["stg.StockMovement"], ["work.InventoryPositionDaily"], "work_rebuild", "work.usp_BuildInventoryPositionDaily"),
        ("STG_Work_PaymentMatch", ["stg.Payment", "stg.ApInvoice"], ["work.PaymentMatched"], "work_rebuild", "work.usp_MatchPaymentsToInvoices"),
        ("STG_Work_CustomerDedup", ["stg.Customer", "stg.CustomerAddress"], ["work.CustomerDedup", "work.CustomerAddressStandardized"], "work_rebuild", "stg.usp_DeduplicateCustomer"),
    ]
    out = []
    for name, src, tgt, load, proc in specs:
        out.append(
            dict(
                package=name,
                project="WWI_Staging",
                folder="04_staging",
                domain="staging",
                source_system="SQL Server Staging (raw)",
                source_objects=src,
                target_system="SQL Server Staging",
                target_objects=tgt,
                load_type=load,
                parent="Master_Daily_ETL",
                procs=[proc, "etl.LogPackageStart", "etl.LogRowCount"],
                criticality="high" if load == "incremental_append" else "medium",
            )
        )
    return out


def _quality_packages():
    specs = [
        ("DQ_Customer_Screen", ["stg.Customer"], ["err.RejectedCustomer"], "Null required attributes, invalid country codes"),
        ("DQ_Supplier_Screen", ["stg.Supplier"], ["err.RejectedSupplier"], "Duplicate tax IDs, missing payment terms"),
        ("DQ_OrderLine_Screen", ["stg.OrderLine"], ["err.RejectedOrderLine"], "Failed customer lookup, invalid quantity"),
        ("DQ_InvoiceLine_Screen", ["stg.SaleLine"], ["err.RejectedInvoiceLine"], "Tax mismatch, invalid currency code"),
        ("DQ_Payment_Screen", ["stg.Payment"], ["err.RejectedPayment"], "Payment without invoice, future-dated payment"),
        ("DQ_File_Screen", ["raw.FilePartnerSales"], ["err.RejectedFileRow"], "Malformed delimiters, unparsable dates"),
        ("DQ_Referential_Screen", ["stg.OrderLine", "stg.SaleLine"], ["err.RejectedLookupFailure"], "Orphan foreign keys against dimensions"),
        ("DQ_Rule_Engine", ["etl.DataQualityRule"], ["etl.DataQualityResult"], "Configurable rule evaluation across staged tables"),
        ("DQ_Reject_Reprocess", ["err.RejectedLookupFailure"], ["stg.OrderLine"], "Reprocess rejects after late dimension arrival"),
        ("DQ_Threshold_Gate", ["etl.RowCountAudit"], ["etl.ReconciliationResult"], "Fail batch when reject rate exceeds tolerance"),
    ]
    out = []
    for name, src, tgt, note in specs:
        out.append(
            dict(
                package=name,
                project="WWI_DataQuality",
                folder="05_data_quality",
                domain="data quality",
                source_system="SQL Server Staging",
                source_objects=src,
                target_system="SQL Server Staging",
                target_objects=tgt,
                load_type="quality_screen",
                parent="Master_Daily_ETL",
                procs=["etl.EvaluateDataQualityRules", "etl.LogRejectedRecord", "etl.AssertRowCountTolerance"],
                criticality="high",
                notes=note,
            )
        )
    return out


def _reference_packages():
    specs = [
        ("REF_Load_Currency", "Dimension.Currency", "Integration.MigrateStagedCurrencyData"),
        ("REF_Load_PaymentMethod", "Dimension.Payment Method", "Integration.MigrateStagedPaymentMethodData"),
        ("REF_Load_TransactionType", "Dimension.Transaction Type", "Integration.MigrateStagedTransactionTypeData"),
        ("REF_Load_PaymentTerms", "Dimension.Payment Terms", "Integration.MigrateStagedPaymentTermsData"),
        ("REF_Load_ReturnReason", "Dimension.Return Reason", "Integration.MigrateStagedReturnReasonData"),
        ("REF_Load_LoyaltyTier", "Dimension.Loyalty Tier", "Integration.MigrateStagedLoyaltyTierData"),
        ("REF_Load_SalesChannel", "Dimension.Sales Channel", "Integration.MigrateStagedPromotionData"),
        ("REF_Load_Carrier", "Dimension.Carrier", "Integration.MigrateStagedCarrierData"),
        ("REF_Load_WarehouseSite", "Dimension.Warehouse Site", "Integration.MigrateStagedWarehouseSiteData"),
        ("REF_Load_CostCenter", "Dimension.Cost Center", "Integration.MigrateStagedCostCenterData"),
        ("REF_Load_Geography", "Dimension.Geography", "Integration.MigrateStagedGeographyData"),
        ("REF_Load_DateDimension", "Dimension.Date", "Integration.PopulateDateDimensionRange"),
        ("REF_Load_UnknownMembers", "Dimension.*", "Integration.EnsureUnknownMembers"),
        ("REF_Load_CodeTranslation", "etl.Configuration", "etl.GetConfigurationValue"),
    ]
    out = []
    for name, tgt, proc in specs:
        out.append(
            dict(
                package=name,
                project="WWI_ReferenceData",
                folder="06_reference_data",
                domain="reference",
                source_system="SQL Server Staging",
                source_objects=["stg.*"],
                target_system="SQL Server DW",
                target_objects=[tgt],
                load_type="full_refresh",
                parent="Master_Weekly_Reference_Load",
                procs=[proc, "etl.LogPackageStart", "etl.LogRowCount"],
                criticality="high" if "Unknown" in name or "Date" in name else "medium",
            )
        )
    return out


def _dimension_packages():
    regional = {"Customer"}
    out = []
    for dim, scd, source in DIMENSIONS:
        if dim in ("Date", "Currency", "Payment Method", "Transaction Type", "Cost Center", "Geography",
                   "Carrier", "Warehouse Site", "Return Reason", "Loyalty Tier", "Sales Channel", "Payment Terms"):
            continue  # loaded by the reference-data project
        base = dim.replace(" ", "")
        proc = "Integration.MigrateStaged{}Data".format(base)
        if dim in ("Customer", "Supplier"):
            proc += "V2"
        if dim in regional:
            for region in REGIONS:
                out.append(
                    dict(
                        package="DIM_{}_Load_{}".format(region, base),
                        project="WWI_Dimensions",
                        folder="07_dimensions",
                        domain="dimension",
                        source_system="SQL Server Staging",
                        source_objects=["stg.{}".format(base)],
                        target_system="SQL Server DW",
                        target_objects=["Dimension.{}".format(dim)],
                        load_type=scd,
                        parent="Master_Daily_ETL",
                        procs=[proc, "etl.LogPackageStart", "etl.LogRowCount"],
                        criticality="high",
                        region=region,
                    )
                )
        else:
            out.append(
                dict(
                    package="DIM_Load_{}".format(base),
                    project="WWI_Dimensions",
                    folder="07_dimensions",
                    domain="dimension",
                    source_system="SQL Server Staging",
                    source_objects=["stg.{}".format(base)],
                    target_system="SQL Server DW",
                    target_objects=["Dimension.{}".format(dim)],
                    load_type=scd,
                    parent="Master_Daily_ETL",
                    procs=[proc, "etl.LogPackageStart", "etl.LogRowCount"],
                    criticality="high" if scd == "SCD2" else "medium",
                )
            )
    out.append(
        dict(
            package="DIM_Rekey_LateArriving",
            project="WWI_Dimensions",
            folder="07_dimensions",
            domain="dimension",
            source_system="SQL Server Staging",
            source_objects=["work.LateArrivingDimensionQueue"],
            target_system="SQL Server DW",
            target_objects=["Fact.*"],
            load_type="rekey",
            parent="Master_Daily_ETL",
            procs=["Integration.RekeyLateArrivingDimensions"],
            criticality="high",
        )
    )
    return out


def _fact_packages():
    regional = {"Sale"}
    out = []
    for fact, grain in FACTS:
        base = fact.replace(" ", "")
        proc = "Integration.LoadFact{}".format(base)
        if fact in regional:
            for region in REGIONS:
                out.append(
                    dict(
                        package="FACT_{}_Load_{}".format(region, base),
                        project="WWI_Facts",
                        folder="08_facts",
                        domain="fact",
                        source_system="SQL Server Staging",
                        source_objects=["stg.{}Line".format(base) if fact in ("Sale", "Order") else "stg.{}".format(base)],
                        target_system="SQL Server DW",
                        target_objects=["Fact.{}".format(fact)],
                        load_type="incremental_fact",
                        parent="Master_Daily_ETL",
                        procs=[proc, "etl.LogPackageStart", "etl.LogRowCount"],
                        criticality="high",
                        region=region,
                        grain=grain,
                    )
                )
        else:
            out.append(
                dict(
                    package="FACT_Load_{}".format(base),
                    project="WWI_Facts",
                    folder="08_facts",
                    domain="fact",
                    source_system="SQL Server Staging",
                    source_objects=["stg.{}".format(base)],
                    target_system="SQL Server DW",
                    target_objects=["Fact.{}".format(fact)],
                    load_type="incremental_fact" if grain == "transaction" else "snapshot_fact",
                    parent="Master_Daily_ETL",
                    procs=[proc, "etl.LogPackageStart", "etl.LogRowCount"],
                    criticality="high" if grain == "transaction" else "medium",
                    grain=grain,
                )
            )
    out.extend(
        [
            dict(
                package="FACT_Dedup_Sale",
                project="WWI_Facts",
                folder="08_facts",
                domain="fact",
                source_system="SQL Server DW",
                source_objects=["Fact.Sale"],
                target_system="SQL Server DW",
                target_objects=["Fact.Sale"],
                load_type="dedup",
                parent="Master_Daily_ETL",
                procs=["Integration.DeduplicateFactSale"],
                criticality="medium",
            ),
            dict(
                package="FACT_Apply_Corrections",
                project="WWI_Facts",
                folder="08_facts",
                domain="fact",
                source_system="SQL Server Staging",
                source_objects=["work.FactRekeyQueue"],
                target_system="SQL Server DW",
                target_objects=["Fact.Sale", "Fact.Order", "Fact.Payment"],
                load_type="correction",
                parent="Master_Month_End",
                procs=["Integration.ApplyFactCorrections"],
                criticality="high",
            ),
        ]
    )
    return out


def _aggregate_packages():
    out = []
    for agg in AGGREGATES:
        base = agg.replace(" ", "").replace("12", "12")
        proc_map = {
            "Daily Sales Summary": "Integration.RefreshAggregateDailySales",
            "Monthly Sales Summary": "Integration.RefreshAggregateMonthlySales",
            "Daily Inventory Health": "Integration.RefreshAggregateInventoryHealth",
            "Monthly Margin Analysis": "Integration.RefreshAggregateMarginAnalysis",
            "Customer 360": "Integration.RefreshAggregateCustomer360",
            "Customer Rolling 12 Month": "Integration.RefreshAggregateCustomer360",
            "Product Performance": "Integration.RefreshAggregateProductPerformance",
            "Supplier Performance": "Integration.RefreshAggregateSupplierPerformance",
            "Regional Sales Performance": "Integration.RefreshAggregateRegionalSales",
            "Finance Close Summary": "Integration.RefreshAggregateFinanceClose",
            "Promotion Effectiveness": "Integration.RefreshAggregatePromotionEffectiveness",
            "Delivery Performance Summary": "Integration.RefreshAggregateDeliveryPerformance",
        }
        out.append(
            dict(
                package="AGG_Refresh_{}".format(base),
                project="WWI_Aggregates",
                folder="09_aggregates",
                domain="aggregate",
                source_system="SQL Server DW",
                source_objects=["Fact.*", "Dimension.*"],
                target_system="SQL Server DW",
                target_objects=["Aggregate.{}".format(agg)],
                load_type="aggregate_rebuild",
                parent="Master_Daily_ETL" if agg.startswith("Daily") else "Master_Month_End",
                procs=[proc_map[agg], "etl.LogPackageStart", "etl.LogRowCount"],
                criticality="medium",
            )
        )
    out.append(
        dict(
            package="AGG_Publish_ReportingLayer",
            project="WWI_Aggregates",
            folder="09_aggregates",
            domain="aggregate",
            source_system="SQL Server DW",
            source_objects=["Aggregate.*"],
            target_system="SQL Server DW",
            target_objects=["Report.*"],
            load_type="publish",
            parent="Master_Daily_ETL",
            procs=["Integration.PublishReportingLayer"],
            criticality="high",
        )
    )
    return out


def _domain_packages():
    """Business-unit projects that duplicate/extend core logic (organic divergence)."""
    specs = []
    finance = [
        ("FIN_Load_ApAging", ["stg.ApInvoice"], ["Fact.Payment"], "Month-end AP aging refresh with 30/60/90 buckets"),
        ("FIN_Load_GlPostings", ["stg.GlJournalLine"], ["Fact.GL Posting"], "GL posting load with period-status gating"),
        ("FIN_Reconcile_SubledgerToGl", ["Fact.Payment", "Fact.GL Posting"], ["etl.ReconciliationResult"], "Subledger vs GL tie-out"),
        ("FIN_Close_PeriodLock", ["etl.Configuration"], ["etl.Batch"], "Locks the period once close completes"),
        ("FIN_Load_CostAllocation", ["stg.CostCenter"], ["Aggregate.Finance Close Summary"], "Cost-centre allocation rules"),
        ("FIN_Currency_Revaluation", ["stg.FxRate"], ["Fact.Payment"], "Month-end FX revaluation of open items"),
        ("FIN_Load_WithholdingTax", ["stg.ApInvoiceLine"], ["Fact.Payment"], "Withholding tax split by jurisdiction"),
    ]
    for name, src, tgt, note in finance:
        specs.append(("WWI_Finance", "10_finance", "finance", name, src, tgt, "Master_Finance_Close", note))

    sales = [
        ("SLS_NA_Load_Commission", ["stg.SaleLine"], ["Fact.Sale"], "US commission plan, USD only"),
        ("SLS_EU_Load_Commission", ["stg.SaleLine"], ["Fact.Sale"], "EU commission plan with VAT-exclusive amounts"),
        ("SLS_APAC_Load_Commission", ["stg.SaleLine"], ["Fact.Sale"], "APAC plan on a 4-4-5 reporting calendar"),
        ("SLS_Load_QuotaAttainment", ["stg.SaleLine"], ["Aggregate.Regional Sales Performance"], "Quota attainment by territory"),
        ("SLS_Load_PromotionRedemption", ["stg.Promotion"], ["Aggregate.Promotion Effectiveness"], "Promotion redemption attribution"),
        ("SLS_Export_PartnerFeed", ["Fact.Sale"], ["file:partner_feed.csv"], "Outbound partner sales feed"),
    ]
    for name, src, tgt, note in sales:
        specs.append(("WWI_Sales", "11_sales", "sales", name, src, tgt, "Master_Daily_ETL", note))

    inventory = [
        ("INV_Load_DailySnapshot", ["work.InventoryPositionDaily"], ["Fact.Daily Inventory Snapshot"], "Daily stock position snapshot"),
        ("INV_Load_CycleCountVariance", ["stg.StockMovement"], ["Fact.Movement"], "Cycle-count variance posting"),
        ("INV_Load_Replenishment", ["stg.StockItem"], ["Aggregate.Inventory Health"], "Replenishment suggestion refresh"),
        ("INV_Load_StockTransfer", ["stg.StockMovement"], ["Fact.Movement"], "Inter-site transfer movements"),
        ("INV_Reconcile_OnHand", ["Fact.Stock Holding"], ["etl.ReconciliationResult"], "DW on-hand vs OLTP on-hand tie-out"),
    ]
    for name, src, tgt, note in inventory:
        specs.append(("WWI_Inventory", "12_inventory", "inventory", name, src, tgt, "Master_Daily_ETL", note))

    procurement = [
        ("PRC_Load_PurchaseSpend", ["stg.PurchaseOrderLine"], ["Fact.Purchase"], "Spend cube load with contract linkage"),
        ("PRC_Load_ReceiptMatching", ["stg.Receipt"], ["Fact.Purchase Receipt"], "Three-way match variance"),
        ("PRC_Load_SupplierScorecard", ["stg.Supplier"], ["Aggregate.Supplier Performance"], "Scorecard refresh"),
        ("PRC_Load_ContractCompliance", ["stg.VendorContract"], ["Aggregate.Supplier Performance"], "Off-contract spend detection"),
        ("PRC_Export_SupplierStatement", ["Fact.Supplier Transaction"], ["file:supplier_statement.csv"], "Supplier statement extract"),
    ]
    for name, src, tgt, note in procurement:
        specs.append(("WWI_Procurement", "13_procurement", "procurement", name, src, tgt, "Master_Daily_ETL", note))

    c360 = [
        ("C360_Build_CustomerProfile", ["Fact.Sale", "Fact.Payment"], ["Aggregate.Customer 360"], "Core profile build"),
        ("C360_Build_RollingMetrics", ["Fact.Sale"], ["Aggregate.Customer Rolling 12 Month"], "Rolling 12-month metrics"),
        ("C360_Build_LoyaltyOverlay", ["Fact.Loyalty Points"], ["Aggregate.Customer 360"], "Loyalty overlay merge"),
        ("C360_Build_ChurnFlags", ["Aggregate.Customer Rolling 12 Month"], ["Report.vw_CustomerChurnRisk"], "Churn-risk rule evaluation"),
        ("C360_Publish_Segments", ["Aggregate.Customer 360"], ["Dimension.Customer Segment"], "Writes derived segments back to the dimension"),
    ]
    for name, src, tgt, note in c360:
        specs.append(("WWI_Customer360", "14_customer_360", "customer 360", name, src, tgt, "Master_Daily_ETL", note))

    out = []
    for project, folder, domain, name, src, tgt, parent, note in specs:
        out.append(
            dict(
                package=name,
                project=project,
                folder=folder,
                domain=domain,
                source_system="SQL Server Staging" if src and src[0].startswith("stg") else "SQL Server DW",
                source_objects=src,
                target_system="SQL Server DW",
                target_objects=tgt,
                load_type="business_rule",
                parent=parent,
                procs=["etl.LogPackageStart", "etl.LogRowCount"],
                criticality="high" if project == "WWI_Finance" else "medium",
                notes=note,
            )
        )
    return out


def _error_and_maintenance():
    err = [
        ("ERR_Handle_PackageFailure", "Central OnError handler invoked by child packages"),
        ("ERR_Route_RejectedRows", "Routes reject tables to the error file share"),
        ("ERR_Notify_Operations", "Writes an operator notification row for failed batches"),
        ("ERR_Retry_FailedSteps", "Retries transient extract failures up to a configured count"),
        ("ERR_Quarantine_BadFiles", "Moves unparsable input files to quarantine"),
        ("ERR_Reconcile_RowCounts", "Compares source and target row counts per batch"),
    ]
    maint = [
        ("MNT_Purge_StagingHistory", "Purges raw/work tables beyond retention"),
        ("MNT_Purge_ControlHistory", "Purges etl control tables beyond retention"),
        ("MNT_Rebuild_Indexes", "Index and columnstore maintenance for the DW"),
        ("MNT_Update_Statistics", "Statistics refresh after the nightly load"),
        ("MNT_Archive_ProcessedFiles", "Archives processed input files"),
        ("MNT_Validate_Configuration", "Asserts required configuration keys exist"),
        ("MNT_Check_DiskSpace", "Pre-flight environment checks before the nightly batch"),
    ]
    out = []
    for name, note in err:
        out.append(
            dict(
                package=name,
                project="WWI_ErrorHandling",
                folder="15_error_handling",
                domain="error handling",
                source_system="SQL Server Staging",
                source_objects=["etl.ErrorLog", "err.*"],
                target_system="SQL Server Staging",
                target_objects=["etl.ErrorLog", "file:errors"],
                load_type="utility",
                parent="Master_Daily_ETL",
                procs=["etl.LogError", "etl.LogRejectedRecord"],
                criticality="high",
                notes=note,
            )
        )
    for name, note in maint:
        out.append(
            dict(
                package=name,
                project="WWI_Maintenance",
                folder="99_maintenance",
                domain="maintenance",
                source_system="SQL Server DW",
                source_objects=["etl.*"],
                target_system="SQL Server DW",
                target_objects=["etl.*"],
                load_type="utility",
                parent="Master_Weekly_Maintenance",
                procs=["etl.PurgeControlHistory", "Integration.RebuildColumnstoreIndexes"],
                criticality="low",
                notes=note,
            )
        )
    return out


MASTERS = [
    ("Master_Daily_ETL", "Nightly full warehouse load: reference -> dimensions -> facts -> aggregates -> publish"),
    ("Master_Hourly_Incremental", "Hourly incremental order, invoice and shipment load"),
    ("Master_Finance_Close", "Month-end finance close sequence"),
    ("Master_Weekly_Reference_Load", "Weekly full refresh of reference data"),
    ("Master_Month_End", "Month-end snapshot, corrections and aggregate rebuild"),
    ("Master_Weekly_Maintenance", "Weekend maintenance window"),
    ("Master_Customer_Sync", "Nightly customer master synchronisation from the ERP"),
    ("Master_Intraday_Inventory", "Intraday inventory movement refresh"),
    ("Master_File_Ingestion", "Partner and carrier file ingestion cycle"),
]


def _master_packages():
    out = []
    for name, note in MASTERS:
        out.append(
            dict(
                package=name,
                project="WWI_Orchestration",
                folder="00_orchestration",
                domain="orchestration",
                source_system="n/a",
                source_objects=[],
                target_system="n/a",
                target_objects=[],
                load_type="orchestration",
                parent=None,
                procs=["etl.StartBatch", "etl.EndBatch", "etl.StartBatchStep", "etl.EndBatchStep"],
                criticality="high",
                notes=note,
            )
        )
    return out


def all_packages():
    return (
        _master_packages()
        + _oracle_extracts()
        + _sqlserver_extracts()
        + _file_ingestion()
        + _staging_packages()
        + _quality_packages()
        + _reference_packages()
        + _dimension_packages()
        + _fact_packages()
        + _aggregate_packages()
        + _domain_packages()
        + _error_and_maintenance()
    )


# ---------------------------------------------------------------------------
# Work packages: who owns which paths during generation
# ---------------------------------------------------------------------------

WORK_PACKAGES = [
    ("WP01_oracle_schema", ["oracle/ddl", "oracle/tables", "oracle/reference", "oracle/seed"]),
    ("WP02_oracle_logic", ["oracle/procedures", "oracle/functions", "oracle/views", "oracle/packages"]),
    ("WP03_sqlserver_oltp", ["sqlserver/oltp"]),
    ("WP04_sqlserver_staging", ["sqlserver/staging"]),
    ("WP05_dw_dimensions", ["sqlserver/warehouse/dimensions", "sqlserver/procedures/dimensions"]),
    ("WP06_dw_facts_aggregates", ["sqlserver/warehouse/facts", "sqlserver/warehouse/aggregates", "sqlserver/procedures/facts", "sqlserver/views"]),
    ("WP07_ssis_extracts", ["ssis/01_oracle_extract", "ssis/02_sqlserver_extract", "ssis/03_file_ingestion"]),
    ("WP08_ssis_staging_quality", ["ssis/04_staging", "ssis/05_data_quality", "ssis/06_reference_data"]),
    ("WP09_ssis_dimensions_facts", ["ssis/07_dimensions", "ssis/08_facts", "ssis/09_aggregates"]),
    ("WP10_ssis_domain_orchestration", ["ssis/00_orchestration", "ssis/10_finance", "ssis/11_sales", "ssis/12_inventory", "ssis/13_procurement", "ssis/14_customer_360", "ssis/15_error_handling", "ssis/99_maintenance"]),
    ("WP11_agent_deployment_config", ["sqlserver/agent", "deployment", "config", "infrastructure", "sqlserver/security"]),
    ("WP12_generators", ["generators"]),
    ("WP13_validation_docs", ["validation/runtime", "validation/checks", "docs/architecture", "docs/domain-model", "docs/dependency-maps", "docs/runbooks"]),
]


# ---------------------------------------------------------------------------
# YAML emission (hand-rolled so the file has no third-party dependency)
# ---------------------------------------------------------------------------


def q(value):
    if value is None:
        return "null"
    text = str(value)
    if text == "":
        return '""'
    needs_quote = any(ch in text for ch in ":#{}[],&*?|-<>=!%@`'\"") or text[0].isdigit()
    if needs_quote:
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def emit():
    lines = []
    a = lines.append
    a("# Estate object catalog - GENERATED by tools/catalog/build_catalog.py. Do not hand-edit.")
    a("# This file is the contract every work package codes against: object names here")
    a("# must match the artefacts under oracle/, sqlserver/ and ssis/.")
    a("")
    a("estate:")
    a("  name: WideWorldImporters Enterprise Legacy Estate")
    a("  phase: legacy-generation")
    a("  databases:")
    a("    oracle_erp: WWIGERP")
    a("    sqlserver_oltp: WideWorldImporters")
    a("    sqlserver_staging: WideWorldImporters_Staging")
    a("    sqlserver_dw: WideWorldImportersDW")
    a("")
    a("oracle:")
    for schema, meta in ORACLE.items():
        a("  {}:".format(schema))
        a("    domain: {}".format(q(meta["domain"])))
        a("    description: {}".format(q(meta["description"])))
        a("    tables:")
        for t in meta["tables"]:
            a("      - {}".format(t))
    a("  packages:")
    for schema, name, desc in ORACLE_PACKAGES:
        a("    - {{schema: {}, name: {}, description: {}}}".format(schema, name, q(desc)))
    a("  views:")
    for schema, name in ORACLE_VIEWS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("  functions:")
    for schema, name in ORACLE_FUNCTIONS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("")
    a("sqlserver_oltp:")
    a("  new_tables:")
    for schema, tables in OLTP_NEW_TABLES.items():
        a("    {}:".format(schema))
        for t in tables:
            a("      - {}".format(t))
    a("  procedures:")
    for schema, name in OLTP_PROCS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("  views:")
    for schema, name in OLTP_VIEWS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("  functions:")
    for schema, name in OLTP_FUNCTIONS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("")
    a("sqlserver_staging:")
    a("  tables:")
    for schema, tables in STAGING_TABLES.items():
        a("    {}:".format(schema))
        for t in tables:
            a("      - {}".format(t))
    a("  procedures:")
    for schema, name in STAGING_PROCS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("  views:")
    for schema, name in STAGING_VIEWS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("  functions:")
    for schema, name in STAGING_FUNCTIONS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("")
    a("sqlserver_dw:")
    a("  dimensions:")
    for name, scd, source in DIMENSIONS:
        a("    - {{name: {}, scd: {}, source: {}}}".format(q(name), q(scd), q(source)))
    a("  facts:")
    for name, grain in FACTS:
        a("    - {{name: {}, grain: {}}}".format(q(name), q(grain)))
    a("  aggregates:")
    for name in AGGREGATES:
        a("    - {}".format(q(name)))
    a("  report_views:")
    for name in REPORT_VIEWS:
        a("    - {}".format(name))
    a("  procedures:")
    for schema, name in DW_PROCS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("  functions:")
    for schema, name in DW_FUNCTIONS:
        a("    - {{schema: {}, name: {}}}".format(schema, name))
    a("")
    a("etl_control:")
    a("  schema: etl")
    a("  deployed_to: [WideWorldImporters_Staging, WideWorldImportersDW]")
    a("  tables:")
    for t in CONTROL_TABLES:
        a("    - {}".format(t))
    a("  procedures:")
    for p in CONTROL_PROCS:
        a("    - {}".format(p))
    a("  views:")
    for v in CONTROL_VIEWS:
        a("    - {}".format(v))
    a("")
    a("ssis:")
    a("  packages:")
    for pkg in all_packages():
        a("    - package: {}".format(pkg["package"]))
        a("      project: {}".format(pkg["project"]))
        a("      folder: {}".format(pkg["folder"]))
        a("      domain: {}".format(q(pkg["domain"])))
        a("      source_system: {}".format(q(pkg["source_system"])))
        a("      source_objects: [{}]".format(", ".join(q(o) for o in pkg["source_objects"])))
        a("      target_system: {}".format(q(pkg["target_system"])))
        a("      target_objects: [{}]".format(", ".join(q(o) for o in pkg["target_objects"])))
        a("      load_type: {}".format(pkg["load_type"]))
        a("      parent: {}".format(q(pkg["parent"])))
        a("      procs: [{}]".format(", ".join(q(p) for p in pkg["procs"])))
        a("      criticality: {}".format(pkg["criticality"]))
        if "region" in pkg:
            a("      region: {}".format(pkg["region"]))
        if "grain" in pkg:
            a("      grain: {}".format(q(pkg["grain"])))
        if "notes" in pkg:
            a("      notes: {}".format(q(pkg["notes"])))
    a("")
    a("work_packages:")
    for name, paths in WORK_PACKAGES:
        a("  {}:".format(name))
        a("    owns:")
        for p in paths:
            a("      - {}".format(p))
    a("")
    return "\n".join(lines)


if __name__ == "__main__":
    text = emit()
    with open(OUT_PATH, "w") as handle:
        handle.write(text)
    pkgs = all_packages()
    print("wrote {} ({} ssis packages)".format(OUT_PATH, len(pkgs)))
    names = [p["package"] for p in pkgs]
    assert len(names) == len(set(names)), "duplicate package names"
