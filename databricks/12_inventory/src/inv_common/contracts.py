"""Legacy object -> Delta table bindings for the WWI_Inventory packages.

Every physical name the notebooks touch is resolved here so the naming contract
(`<catalog>.<schema>.<table>`) is applied in one place and never hard-coded.
Column names follow the estate rule: legacy PascalCase is kept; warehouse
columns that contain spaces (`[Quantity On Hand]`) have the spaces removed
(`QuantityOnHand`).
"""

from __future__ import annotations

from dbx_etl_common import naming

PROJECT_NAME = "WWI_Inventory"
BATCH_NAME = "wwi_12_inventory"

# legacy object name -> (schema, delta table)
TABLE_BINDINGS = {
    # silver (stg.* / work.* / err.*)
    "stg.StockItem": ("silver", "stg_stock_item"),
    "stg.StockMovement": ("silver", "stg_stock_movement"),
    "stg.CycleCount": ("silver", "stg_cycle_count"),
    "stg.WarehouseSite": ("silver", "stg_warehouse_site"),
    "stg.TransferPrice": ("silver", "stg_transfer_price"),
    "work.InventoryPositionDaily": ("silver", "work_inventory_position_daily"),
    "work.StockItemDemand": ("silver", "work_stock_item_demand"),
    "work.CycleCountVariance": ("silver", "work_cycle_count_variance"),
    "work.ReplenishmentSuggestion": ("silver", "work_replenishment_suggestion"),
    "work.StockTransferMovement": ("silver", "work_stock_transfer_movement"),
    "work.StockTransferReceiptOnly": ("silver", "work_stock_transfer_receipt_only"),
    "err.InventorySnapshotReject": ("silver", "err_inventory_snapshot_reject"),
    # gold
    "Dimension.Stock Item": ("gold", "dim_stock_item"),
    "Dimension.Warehouse Site": ("gold", "dim_warehouse_site"),
    "Fact.Daily Inventory Snapshot": ("gold", "fact_daily_inventory_snapshot"),
    "Fact.Movement": ("gold", "fact_movement"),
    "Fact.Stock Holding": ("gold", "fact_stock_holding"),
    "Aggregate.Daily Inventory Health": ("gold", "agg_daily_inventory_health"),
    # etl (control framework, owned by dbx_etl_common; read/written here only where the
    # legacy package wrote the table directly)
    "etl.ReconciliationResult": ("etl", "reconciliation_result"),
    "etl.RowCountAudit": ("etl", "row_count_log"),
    "etl.PackageExecution": ("etl", "package_execution"),
    "etl.RejectedRecord": ("etl", "rejected_record"),
}


def table(catalog: str, legacyName: str) -> str:
    schema, name = TABLE_BINDINGS[legacyName]
    return naming.table(catalog, schema, name)


def deltaName(legacyName: str) -> str:
    schema, name = TABLE_BINDINGS[legacyName]
    return f"{schema}.{name}"


# Reject reason codes carried over verbatim from the packages.
REASON_COUNT_VARIANCE_HELD = "COUNT_VARIANCE_HELD"
REASON_TRANSFER_AGED_IN_TRANSIT = "TRANSFER_AGED_IN_TRANSIT"
REASON_ONHAND_VARIANCE = "ONHAND_VARIANCE"
REASON_UNKNOWN_STOCK_ITEM = "UNKNOWN_STOCK_ITEM"

RECONCILIATION_NAME = "DW on-hand vs operational on-hand"

# Target DDL (Spark SQL column lists) for the tables this project owns / writes.
# Tables owned by other sessions (stg_*, dim_*, fact_stock_holding, fact_movement)
# are only created here when absent so a first dev run does not fail on a
# missing object; the shape must stay compatible with their producers.

FACT_DAILY_INVENTORY_SNAPSHOT_DDL = """
    SnapshotDateKey DATE NOT NULL,
    StockItemKey INT NOT NULL,
    WarehouseSiteKey INT,
    WarehouseSiteCode STRING,
    BinLocationCode STRING,
    WWIStockItemID INT,
    RegionCode STRING,
    QuantityOnHand DECIMAL(18,4) NOT NULL,
    QuantityAllocated DECIMAL(18,4),
    QuantityAvailable DECIMAL(18,4),
    QuantityOnOrder DECIMAL(18,4),
    QuantityInTransit DECIMAL(18,4),
    UnitCostAtSnapshot DECIMAL(18,4),
    StockValueAtCost DECIMAL(18,2),
    DaysOfCover DECIMAL(9,2),
    DaysSinceLastMovement INT,
    LastMovementDate DATE,
    StockAgeBucketCode STRING,
    IsChillerStock BOOLEAN,
    ShelfLifeDays INT,
    IsExpiredChillerStock INT,
    ObsolescenceProvisionAmount DECIMAL(18,2),
    LineageKey INT,
    BatchId BIGINT,
    LoadDatetime TIMESTAMP
"""

FACT_MOVEMENT_DDL = """
    DateKey DATE,
    StockItemKey INT,
    WWIStockItemID INT,
    WarehouseSiteKey INT,
    WarehouseSiteCode STRING,
    RegionCode STRING,
    BinLocation STRING,
    MovementTypeCode STRING,
    MovementReasonCode STRING,
    MovementDirection STRING,
    Quantity DECIMAL(18,4),
    QuantityBaseUOM DECIMAL(18,4),
    StandardCost DECIMAL(18,4),
    MovementValueReporting DECIMAL(18,2),
    CostingMethodCode STRING,
    StockTakeReference STRING,
    TransferReference STRING,
    NaturalKeyHash STRING,
    InferredMemberFlag BOOLEAN,
    BatchId BIGINT,
    LoadDatetime TIMESTAMP
"""

AGG_DAILY_INVENTORY_HEALTH_DDL = """
    SnapshotDate DATE,
    WarehouseSiteKey INT,
    WarehouseSiteCode STRING,
    ProductCategoryKey INT,
    RegionCode STRING,
    SuggestionCount INT,
    StockoutRiskCount INT,
    SuggestedUnits BIGINT,
    RefreshBatchId BIGINT,
    RefreshedDatetime TIMESTAMP
"""

WORK_CYCLE_COUNT_VARIANCE_DDL = """
    CycleCountId INT,
    StockItemId INT,
    WarehouseSiteCode STRING,
    BinLocationCode STRING,
    CountedQuantity INT,
    SystemQuantity INT,
    VarianceQuantity INT,
    VarianceValue DECIMAL(18,2),
    CountedAtUtc TIMESTAMP,
    CountStatusCode STRING,
    BatchId BIGINT
"""

WORK_REPLENISHMENT_SUGGESTION_DDL = """
    StockItemId INT,
    StockItemName STRING,
    SupplierId INT,
    LeadTimeDays INT,
    ReorderLevel INT,
    TargetStockLevel INT,
    QuantityPerOuter INT,
    IsChillerStock BOOLEAN,
    RegionCode STRING,
    WarehouseSiteCode STRING,
    QuantityOnHand INT,
    QuantityOnOrder INT,
    QuantityAllocated INT,
    AverageDailyDemand DECIMAL(18,4),
    SafetyFactor DECIMAL(18,4),
    ReorderPoint INT,
    ProjectedAvailable INT,
    DaysOfCover DECIMAL(18,2),
    RawSuggestedQuantity INT,
    SuggestedQuantity INT,
    IsStockoutRisk BOOLEAN,
    BatchId BIGINT
"""

WORK_STOCK_TRANSFER_MOVEMENT_DDL = """
    StockTransferId INT,
    TransferReference STRING,
    StockItemId INT,
    FromWarehouseSiteCode STRING,
    ToWarehouseSiteCode STRING,
    DespatchedAtUtc TIMESTAMP,
    ReceivedAtUtc TIMESTAMP,
    QuantityDespatched INT,
    QuantityReceived INT,
    QuantityInTransit INT,
    TransferStatusCode STRING,
    CarrierCode STRING,
    UnitCost DECIMAL(18,4),
    FromRegionCode STRING,
    ToRegionCode STRING,
    MovementUnitValue DECIMAL(18,4),
    IsCrossRegion BOOLEAN,
    IssueValue DECIMAL(18,2),
    ReceiptValue DECIMAL(18,2),
    TransitDays INT,
    BatchId BIGINT
"""

ERR_INVENTORY_SNAPSHOT_REJECT_DDL = """
    StockItemId INT,
    WarehouseSiteCode STRING,
    BinLocationCode STRING,
    SnapshotDate DATE,
    QuantityOnHand INT,
    RejectReasonCode STRING,
    BatchId BIGINT,
    PackageExecutionId BIGINT,
    LoggedAtUtc TIMESTAMP
"""

RECONCILIATION_RESULT_DDL = """
    BatchId BIGINT,
    ReconciliationName STRING,
    ObjectName STRING,
    SourceKey STRING,
    SourceAmount DECIMAL(18,4),
    TargetAmount DECIMAL(18,4),
    VarianceAmount DECIMAL(18,4),
    VarianceStatus STRING,
    EvaluatedAtUtc TIMESTAMP
"""
