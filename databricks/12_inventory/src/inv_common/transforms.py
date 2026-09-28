"""Pure DataFrame ports of the WWI_Inventory data flows and set-based T-SQL.

Every function takes DataFrames (already read from the bound Delta tables) and
returns DataFrames; nothing here touches Spark SQL catalogs or the control
framework, so the logic runs unchanged under local PySpark in the unit tests.
Column names are the legacy SSIS pipeline column names.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# INV_Load_DailySnapshot
# ---------------------------------------------------------------------------


def buildDailySnapshotSource(position: DataFrame, stockItem: DataFrame, snapshotDate) -> DataFrame:
    """`work InventoryPositionDaily` OLE DB source (SNAPSHOT_SQL)."""
    p = position.alias("p").where(F.col("p.SnapshotDate") == F.lit(snapshotDate).cast("date"))
    si = stockItem.alias("si")
    ageDays = F.datediff(F.col("p.SnapshotDate"), F.col("p.LastMovementDate"))
    return p.join(si, F.col("si.StockItemId") == F.col("p.StockItemId"), "inner").select(
        F.col("p.StockItemId"),
        F.col("p.WarehouseSiteCode"),
        F.col("p.BinLocationCode"),
        F.col("p.SnapshotDate"),
        F.col("p.QuantityOnHand"),
        F.col("p.QuantityAllocated"),
        F.col("p.QuantityOnOrder"),
        F.col("p.QuantityInTransit"),
        (F.col("p.QuantityOnHand") - F.col("p.QuantityAllocated")).alias("QuantityAvailable"),
        F.col("p.LastMovementDate"),
        F.col("si.UnitCost"),
        F.col("si.IsChillerStock"),
        F.col("si.ShelfLifeDays"),
        (F.col("p.QuantityOnHand") * F.col("si.UnitCost")).alias("OnHandValue"),
        F.when(F.col("p.LastMovementDate").isNull(), F.lit("NEVER"))
        .when(ageDays > 365, F.lit("D365P"))
        .when(ageDays > 180, F.lit("D180"))
        .when(ageDays > 90, F.lit("D090"))
        .otherwise(F.lit("FRESH"))
        .alias("AgeBandCode"),
        F.when(
            (F.col("si.IsChillerStock") == F.lit(True))
            & (F.datediff(F.col("p.SnapshotDate"), F.col("p.ReceiptDate")) > F.col("si.ShelfLifeDays")),
            F.lit(1),
        )
        .otherwise(F.lit(0))
        .alias("IsExpiredChillerStock"),
    )


def currentStockItemKeys(dimStockItem: DataFrame, asOf=None) -> DataFrame:
    """`Lookup Stock Item Key` reference set: current dimension rows only."""
    asOfCol = F.lit(asOf).cast("timestamp") if asOf is not None else F.current_timestamp()
    return dimStockItem.where(F.col("ValidTo") > asOfCol).select(
        F.col("StockItemKey"), F.col("WWIStockItemID").alias("StockItemId")
    )


def deriveSnapshotMeasures(df: DataFrame) -> DataFrame:
    """`Derive Snapshot Measures` derived-column transform."""
    return df.withColumn(
        "DaysCoverAtCurrentRate",
        F.when(F.col("QuantityAvailable") <= 0, F.lit(0)).otherwise(F.col("QuantityAvailable")).cast("int"),
    ).withColumn(
        "ObsolescenceProvisionAmount",
        F.when(F.col("AgeBandCode") == "D365P", F.col("OnHandValue"))
        .when(F.col("AgeBandCode") == "D180", F.col("OnHandValue") / 2)
        .otherwise(F.lit(0))
        .cast("decimal(18,2)"),
    )


def buildDailySnapshot(
    position: DataFrame, stockItem: DataFrame, dimStockItem: DataFrame, snapshotDate, asOf=None
) -> tuple[DataFrame, DataFrame]:
    """Full `Load Inventory Snapshot` data flow -> (matched rows, lookup no-match rejects)."""
    src = buildDailySnapshotSource(position, stockItem, snapshotDate)
    keys = currentStockItemKeys(dimStockItem, asOf)
    joined = src.join(keys, "StockItemId", "left")
    matched = deriveSnapshotMeasures(joined.where(F.col("StockItemKey").isNotNull()))
    rejected = joined.where(F.col("StockItemKey").isNull()).drop("StockItemKey")
    return matched, rejected


def toFactDailyInventorySnapshot(df: DataFrame, batchId: int, dimWarehouseSite: DataFrame | None = None) -> DataFrame:
    """Map the pipeline columns onto gold.fact_daily_inventory_snapshot."""
    out = df
    if dimWarehouseSite is not None:
        ws = dimWarehouseSite.select(
            F.col("WarehouseSiteCode"), F.col("WarehouseSiteKey"), F.col("RegionCode").alias("SiteRegionCode")
        )
        out = out.join(ws, "WarehouseSiteCode", "left")
    else:
        out = out.withColumn("WarehouseSiteKey", F.lit(None).cast("int")).withColumn(
            "SiteRegionCode", F.lit(None).cast("string")
        )
    return out.select(
        F.col("SnapshotDate").alias("SnapshotDateKey"),
        F.col("StockItemKey").cast("int"),
        F.col("WarehouseSiteKey").cast("int"),
        F.col("WarehouseSiteCode"),
        F.col("BinLocationCode"),
        F.col("StockItemId").cast("int").alias("WWIStockItemID"),
        F.coalesce(F.col("SiteRegionCode"), F.lit("UNK")).alias("RegionCode"),
        F.col("QuantityOnHand").cast("decimal(18,4)"),
        F.col("QuantityAllocated").cast("decimal(18,4)"),
        F.col("QuantityAvailable").cast("decimal(18,4)"),
        F.col("QuantityOnOrder").cast("decimal(18,4)"),
        F.col("QuantityInTransit").cast("decimal(18,4)"),
        F.col("UnitCost").cast("decimal(18,4)").alias("UnitCostAtSnapshot"),
        F.col("OnHandValue").cast("decimal(18,2)").alias("StockValueAtCost"),
        F.col("DaysCoverAtCurrentRate").cast("decimal(9,2)").alias("DaysOfCover"),
        F.datediff(F.col("SnapshotDate"), F.col("LastMovementDate")).cast("int").alias("DaysSinceLastMovement"),
        F.col("LastMovementDate"),
        F.col("AgeBandCode").alias("StockAgeBucketCode"),
        F.col("IsChillerStock").cast("boolean"),
        F.col("ShelfLifeDays").cast("int"),
        F.col("IsExpiredChillerStock").cast("int"),
        F.col("ObsolescenceProvisionAmount").cast("decimal(18,2)"),
        F.lit(0).alias("LineageKey"),
        F.lit(batchId).cast("bigint").alias("BatchId"),
        F.current_timestamp().alias("LoadDatetime"),
    )


# ---------------------------------------------------------------------------
# INV_Load_CycleCountVariance
# ---------------------------------------------------------------------------


def buildCycleCountVariance(
    cycleCount: DataFrame,
    position: DataFrame,
    stockItem: DataFrame,
    batchId: int,
    toleranceUnits: int,
    toleranceValue: int,
) -> DataFrame:
    """`Build Variance Set` INSERT ... SELECT into work.CycleCountVariance."""
    cc = cycleCount.alias("cc").where(F.col("cc.LoadBatchId") == F.lit(batchId))
    pos = position.alias("pos")
    si = stockItem.alias("si")
    systemQty = F.coalesce(F.col("pos.QuantityOnHand"), F.lit(0))
    varianceQty = F.col("cc.CountedQuantity") - systemQty
    varianceValue = varianceQty * F.coalesce(F.col("si.UnitCost"), F.lit(0))
    return (
        cc.join(
            pos,
            (F.col("pos.StockItemId") == F.col("cc.StockItemId"))
            & (F.col("pos.WarehouseSiteCode") == F.col("cc.WarehouseSiteCode"))
            & (F.col("pos.BinLocationCode") == F.col("cc.BinLocationCode")),
            "left",
        )
        .join(si, F.col("si.StockItemId") == F.col("cc.StockItemId"), "left")
        .where(F.col("cc.CountedQuantity") != systemQty)
        .select(
            F.col("cc.CycleCountId"),
            F.col("cc.StockItemId"),
            F.col("cc.WarehouseSiteCode"),
            F.col("cc.BinLocationCode"),
            F.col("cc.CountedQuantity"),
            systemQty.cast("int").alias("SystemQuantity"),
            varianceQty.cast("int").alias("VarianceQuantity"),
            varianceValue.cast("decimal(18,2)").alias("VarianceValue"),
            F.col("cc.CountedAtUtc"),
            F.when(
                (F.abs(varianceQty) <= F.lit(toleranceUnits)) & (F.abs(varianceValue) <= F.lit(toleranceValue)),
                F.lit("AUTO"),
            )
            .otherwise(F.lit("HOLD"))
            .alias("CountStatusCode"),
            F.lit(batchId).cast("bigint").alias("BatchId"),
        )
    )


def measureVariances(variance: DataFrame) -> tuple[int, int]:
    """`Measure Variances` -> (VarianceCount [AUTO], HeldForRecountCount [HOLD])."""
    row = variance.agg(
        F.sum(F.when(F.col("CountStatusCode") == "AUTO", 1).otherwise(0)).alias("autoPost"),
        F.sum(F.when(F.col("CountStatusCode") == "HOLD", 1).otherwise(0)).alias("held"),
    ).first()
    return int(row["autoPost"] or 0), int(row["held"] or 0)


def buildAdjustmentMovements(
    variance: DataFrame,
    dimStockItem: DataFrame,
    dimWarehouseSite: DataFrame,
    batchId: int,
    asOf=None,
) -> DataFrame:
    """Set-based replacement for the `Post Adjustment Movements` cursor.

    One movement per AUTO variance, keyed by `CYCLECOUNT|<CycleCountId>` so a rerun
    merges onto the same fact row instead of taking a second movement number.
    """
    auto = variance.where(F.col("CountStatusCode") == "AUTO").alias("v")
    keys = currentStockItemKeys(dimStockItem, asOf).alias("k")
    ws = dimWarehouseSite.select("WarehouseSiteCode", "WarehouseSiteKey", "RegionCode").alias("ws")
    return (
        auto.join(keys, F.col("k.StockItemId") == F.col("v.StockItemId"), "left")
        .join(ws, F.col("ws.WarehouseSiteCode") == F.col("v.WarehouseSiteCode"), "left")
        .select(
            F.to_date(F.col("v.CountedAtUtc")).alias("DateKey"),
            F.col("k.StockItemKey").cast("int").alias("StockItemKey"),
            F.col("v.StockItemId").cast("int").alias("WWIStockItemID"),
            F.col("ws.WarehouseSiteKey").cast("int").alias("WarehouseSiteKey"),
            F.col("v.WarehouseSiteCode").alias("WarehouseSiteCode"),
            F.col("ws.RegionCode").alias("RegionCode"),
            F.col("v.BinLocationCode").alias("BinLocation"),
            F.lit("ADJUST").alias("MovementTypeCode"),
            F.lit("CYCLECOUNT").alias("MovementReasonCode"),
            F.when(F.col("v.VarianceQuantity") < 0, F.lit("-")).otherwise(F.lit("+")).alias("MovementDirection"),
            F.col("v.VarianceQuantity").cast("decimal(18,4)").alias("Quantity"),
            F.col("v.VarianceQuantity").cast("decimal(18,4)").alias("QuantityBaseUOM"),
            F.lit(None).cast("decimal(18,4)").alias("StandardCost"),
            F.col("v.VarianceValue").cast("decimal(18,2)").alias("MovementValueReporting"),
            F.lit(None).cast("string").alias("CostingMethodCode"),
            F.col("v.CycleCountId").cast("string").alias("StockTakeReference"),
            F.lit(None).cast("string").alias("TransferReference"),
            F.sha2(F.concat_ws("|", F.lit("CYCLECOUNT"), F.col("v.CycleCountId").cast("string")), 256).alias(
                "NaturalKeyHash"
            ),
            F.col("k.StockItemKey").isNull().alias("InferredMemberFlag"),
            F.lit(batchId).cast("bigint").alias("BatchId"),
            F.current_timestamp().alias("LoadDatetime"),
        )
    )


def heldRecounts(variance: DataFrame) -> DataFrame:
    """`Queue Recounts` -> rows to hand to control.logRejectedRecordSet (COUNT_VARIANCE_HELD)."""
    return variance.where(F.col("CountStatusCode") == "HOLD").select(
        F.col("CycleCountId").cast("string").alias("BusinessKey"),
        F.lit("Cycle-count variance above tolerance; supervisor recount required").alias("RejectReason"),
        F.to_json(
            F.struct(
                "CycleCountId", "StockItemId", "WarehouseSiteCode", "BinLocationCode",
                "CountedQuantity", "SystemQuantity", "VarianceQuantity", "VarianceValue",
            )
        ).alias("RecordPayload"),
    )


# ---------------------------------------------------------------------------
# INV_Load_Replenishment
# ---------------------------------------------------------------------------


def buildReplenishmentSource(stockItem: DataFrame, position: DataFrame, demand: DataFrame) -> DataFrame:
    """`stg StockItem With Position` OLE DB source (REPLENISH_SQL)."""
    si = stockItem.alias("si").where(F.col("si.IsDiscontinued") == F.lit(False))
    pos = position.alias("pos")
    dem = demand.alias("dem")
    return (
        si.join(pos, F.col("pos.StockItemId") == F.col("si.StockItemId"), "inner")
        .join(
            dem,
            (F.col("dem.StockItemId") == F.col("si.StockItemId"))
            & (F.col("dem.WarehouseSiteCode") == F.col("pos.WarehouseSiteCode")),
            "left",
        )
        .select(
            F.col("si.StockItemId"),
            F.col("si.StockItemName"),
            F.col("si.SupplierId"),
            F.col("si.LeadTimeDays"),
            F.col("si.ReorderLevel"),
            F.col("si.TargetStockLevel"),
            F.col("si.QuantityPerOuter"),
            F.col("si.IsChillerStock"),
            F.col("si.RegionCode"),
            F.col("pos.WarehouseSiteCode"),
            F.col("pos.QuantityOnHand"),
            F.col("pos.QuantityOnOrder"),
            F.col("pos.QuantityAllocated"),
            F.coalesce(F.col("dem.AverageDailyDemand"), F.lit(0)).cast("decimal(18,4)").alias("AverageDailyDemand"),
            F.when(F.col("si.RegionCode") == "APAC", F.lit(1.5))
            .when(F.col("si.RegionCode") == "EU", F.lit(1.1))
            .otherwise(F.lit(1.25))
            .cast("decimal(18,4)")
            .alias("SafetyFactor"),
        )
    )


def deriveReplenishment(df: DataFrame, coverDays: int) -> DataFrame:
    """`Derive Reorder Point` + `Derive Suggested Quantity` (SSIS integer casts truncate)."""
    raw = (F.col("AverageDailyDemand") * F.lit(coverDays)).cast("int") - F.col("ProjectedAvailable")
    return (
        df.withColumn(
            "ReorderPoint",
            (F.col("AverageDailyDemand") * F.col("LeadTimeDays") * F.col("SafetyFactor")).cast("int"),
        )
        .withColumn(
            "ProjectedAvailable",
            (F.col("QuantityOnHand") + F.col("QuantityOnOrder") - F.col("QuantityAllocated")).cast("int"),
        )
        .withColumn(
            "DaysOfCover",
            F.when(F.col("AverageDailyDemand") == 0, F.lit(999))
            .otherwise((F.col("QuantityOnHand") - F.col("QuantityAllocated")) / F.col("AverageDailyDemand"))
            .cast("decimal(18,2)"),
        )
        .withColumn("RawSuggestedQuantity", raw.cast("int"))
        .withColumn(
            "SuggestedQuantity",
            F.when(F.col("RawSuggestedQuantity") <= 0, F.lit(0))
            .when(F.col("QuantityPerOuter") <= 1, F.col("RawSuggestedQuantity"))
            .otherwise(
                (F.expr("RawSuggestedQuantity div QuantityPerOuter") + F.lit(1)) * F.col("QuantityPerOuter")
            )
            .cast("int"),
        )
        .withColumn(
            "IsStockoutRisk",
            F.col("ProjectedAvailable") <= (F.col("AverageDailyDemand") * F.col("LeadTimeDays")).cast("int"),
        )
    )


def buildReplenishmentSuggestions(
    stockItem: DataFrame,
    position: DataFrame,
    demand: DataFrame,
    coverDays: int,
    batchId: int,
    suppressChiller: bool = False,
) -> DataFrame:
    """Data flow `Calculate Replenishment` (Suggest branch) + optional `Suppress Chiller Suggestions`."""
    out = deriveReplenishment(buildReplenishmentSource(stockItem, position, demand), coverDays)
    out = out.where(F.col("SuggestedQuantity") > 0)
    if suppressChiller:
        out = out.where(F.col("IsChillerStock") != F.lit(True))
    return out.withColumn("BatchId", F.lit(batchId).cast("bigint"))


def aggregateInventoryHealth(
    suggestions: DataFrame, snapshotDate, batchId: int, dimWarehouseSite: DataFrame | None = None
) -> DataFrame:
    """USING source of the `Publish Inventory Health` MERGE, mapped onto agg_daily_inventory_health.

    The legacy MERGE matched on Warehouse Site Code alone, so the grain here is one row per
    site (+ snapshot date); RegionCode is the site's region when the dimension is supplied,
    otherwise the (single) stock-item region seen in the suggestions.
    """
    agg = suggestions.groupBy("WarehouseSiteCode").agg(
        F.count(F.lit(1)).cast("int").alias("SuggestionCount"),
        F.sum(F.when(F.col("IsStockoutRisk") == F.lit(True), 1).otherwise(0)).cast("int").alias("StockoutRiskCount"),
        F.sum("SuggestedQuantity").cast("bigint").alias("SuggestedUnits"),
        F.max("RegionCode").alias("ItemRegionCode"),
    )
    if dimWarehouseSite is not None:
        ws = dimWarehouseSite.select("WarehouseSiteCode", "WarehouseSiteKey", F.col("RegionCode").alias("SiteRegionCode"))
        agg = agg.join(ws, "WarehouseSiteCode", "left")
    else:
        agg = agg.withColumn("WarehouseSiteKey", F.lit(None).cast("int")).withColumn(
            "SiteRegionCode", F.lit(None).cast("string")
        )
    return agg.select(
        F.lit(snapshotDate).cast("date").alias("SnapshotDate"),
        F.col("WarehouseSiteKey").cast("int"),
        F.col("WarehouseSiteCode"),
        F.lit(0).alias("ProductCategoryKey"),
        F.coalesce(F.col("SiteRegionCode"), F.col("ItemRegionCode")).alias("RegionCode"),
        F.col("SuggestionCount"),
        F.col("StockoutRiskCount"),
        F.col("SuggestedUnits"),
        F.lit(batchId).cast("bigint").alias("RefreshBatchId"),
        F.current_timestamp().alias("RefreshedDatetime"),
    )


# ---------------------------------------------------------------------------
# INV_Load_StockTransfer
# ---------------------------------------------------------------------------


def buildTransferSource(
    stockMovement: DataFrame,
    stockItem: DataFrame,
    warehouseSite: DataFrame,
    transferPrice: DataFrame,
    batchId: int,
) -> DataFrame:
    """`stg StockMovement Transfers` OLE DB source (TRANSFER_SQL)."""
    t = stockMovement.alias("t").where(
        (F.col("t.MovementTypeCode") == "TRANSFER") & (F.col("t.LoadBatchId") == F.lit(batchId))
    )
    si = stockItem.alias("si")
    fw = warehouseSite.alias("fw")
    tw = warehouseSite.alias("tw")
    tp = transferPrice.alias("tp")
    return (
        t.join(si, F.col("si.StockItemId") == F.col("t.StockItemId"), "inner")
        .join(fw, F.col("fw.WarehouseSiteCode") == F.col("t.FromWarehouseSiteCode"), "inner")
        .join(tw, F.col("tw.WarehouseSiteCode") == F.col("t.ToWarehouseSiteCode"), "inner")
        .join(
            tp,
            (F.col("tp.StockItemId") == F.col("t.StockItemId"))
            & (F.col("tp.FromRegionCode") == F.col("fw.RegionCode"))
            & (F.col("tp.ToRegionCode") == F.col("tw.RegionCode")),
            "left",
        )
        .select(
            F.col("t.StockTransferId"),
            F.col("t.TransferReference"),
            F.col("t.StockItemId"),
            F.col("t.FromWarehouseSiteCode"),
            F.col("t.ToWarehouseSiteCode"),
            F.col("t.DespatchedAtUtc"),
            F.col("t.ReceivedAtUtc"),
            F.col("t.QuantityDespatched"),
            F.col("t.QuantityReceived"),
            (F.col("t.QuantityDespatched") - F.coalesce(F.col("t.QuantityReceived"), F.lit(0))).alias("QuantityInTransit"),
            F.col("t.TransferStatusCode"),
            F.col("t.CarrierCode"),
            F.col("si.UnitCost"),
            F.col("fw.RegionCode").alias("FromRegionCode"),
            F.col("tw.RegionCode").alias("ToRegionCode"),
            F.when(
                F.col("fw.RegionCode") != F.col("tw.RegionCode"),
                F.coalesce(F.col("tp.TransferPrice"), F.col("si.UnitCost") * F.lit(1.08)),
            )
            .otherwise(F.col("si.UnitCost"))
            .cast("decimal(18,4)")
            .alias("MovementUnitValue"),
        )
    )


def deriveTransferAttributes(df: DataFrame, nowUtc: datetime) -> DataFrame:
    """`Derive Movement Attributes` derived-column transform (GETDATE() pinned to nowUtc)."""
    now = F.lit(nowUtc).cast("timestamp")
    return (
        df.withColumn("IsCrossRegion", F.col("FromRegionCode") != F.col("ToRegionCode"))
        .withColumn("IssueValue", (F.col("QuantityDespatched") * F.col("MovementUnitValue") * -1).cast("decimal(18,2)"))
        .withColumn("ReceiptValue", (F.col("QuantityReceived") * F.col("MovementUnitValue")).cast("decimal(18,2)"))
        .withColumn(
            "TransitDays",
            F.when(F.col("ReceivedAtUtc").isNull(), F.datediff(F.to_date(now), F.to_date("DespatchedAtUtc")))
            .otherwise(F.datediff(F.to_date("ReceivedAtUtc"), F.to_date("DespatchedAtUtc")))
            .cast("int"),
        )
    )


def splitTransferLegs(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """`Split Transfer Legs` conditional split -> (Despatched, ReceiptOnly)."""
    return df.where(F.col("QuantityDespatched") > 0), df.where(F.col("QuantityDespatched") == 0)


def buildTransferMovements(
    transfers: DataFrame, dimStockItem: DataFrame, dimWarehouseSite: DataFrame, batchId: int, asOf=None
) -> DataFrame:
    """Set-based replacement for Integration.usp_PostTransferMovements.

    Each transfer posts an ISSUE leg at the from-site for the despatched quantity and,
    once anything has been received, a RECEIPT leg at the to-site; natural key
    `TRANSFER|<StockTransferId>|<leg>` keeps the MERGE idempotent.
    """
    keys = currentStockItemKeys(dimStockItem, asOf).alias("k")
    ws = dimWarehouseSite.select("WarehouseSiteCode", "WarehouseSiteKey", "RegionCode").alias("ws")
    t = transfers.alias("t")

    def leg(df: DataFrame, legName: str, siteCol: str, qtyCol: str, valueCol: str, dateCol: str, direction: str):
        return (
            df.join(keys, F.col("k.StockItemId") == F.col("t.StockItemId"), "left")
            .join(ws, F.col("ws.WarehouseSiteCode") == F.col(f"t.{siteCol}"), "left")
            .select(
                F.to_date(F.col(f"t.{dateCol}")).alias("DateKey"),
                F.col("k.StockItemKey").cast("int").alias("StockItemKey"),
                F.col("t.StockItemId").cast("int").alias("WWIStockItemID"),
                F.col("ws.WarehouseSiteKey").cast("int").alias("WarehouseSiteKey"),
                F.col(f"t.{siteCol}").alias("WarehouseSiteCode"),
                F.col("ws.RegionCode").alias("RegionCode"),
                F.lit(None).cast("string").alias("BinLocation"),
                F.lit("TRANSFER").alias("MovementTypeCode"),
                F.lit(f"XFER_{legName}").alias("MovementReasonCode"),
                F.lit(direction).alias("MovementDirection"),
                (F.col(f"t.{qtyCol}") * F.lit(1 if direction == "+" else -1)).cast("decimal(18,4)").alias("Quantity"),
                (F.col(f"t.{qtyCol}") * F.lit(1 if direction == "+" else -1)).cast("decimal(18,4)").alias("QuantityBaseUOM"),
                F.col("t.MovementUnitValue").cast("decimal(18,4)").alias("StandardCost"),
                F.col(f"t.{valueCol}").cast("decimal(18,2)").alias("MovementValueReporting"),
                F.when(F.col("t.IsCrossRegion"), F.lit("XFERP")).otherwise(F.lit("STD")).alias("CostingMethodCode"),
                F.lit(None).cast("string").alias("StockTakeReference"),
                F.col("t.TransferReference").alias("TransferReference"),
                F.sha2(
                    F.concat_ws("|", F.lit("TRANSFER"), F.col("t.StockTransferId").cast("string"), F.lit(legName)), 256
                ).alias("NaturalKeyHash"),
                F.col("k.StockItemKey").isNull().alias("InferredMemberFlag"),
                F.lit(batchId).cast("bigint").alias("BatchId"),
                F.current_timestamp().alias("LoadDatetime"),
            )
        )

    issue = leg(
        t.where(F.col("t.QuantityDespatched") > 0),
        "ISSUE", "FromWarehouseSiteCode", "QuantityDespatched", "IssueValue", "DespatchedAtUtc", "-",
    )
    receipt = leg(
        t.where(F.coalesce(F.col("t.QuantityReceived"), F.lit(0)) > 0),
        "RECEIPT", "ToWarehouseSiteCode", "QuantityReceived", "ReceiptValue", "ReceivedAtUtc", "+",
    )
    return issue.unionByName(receipt)


def agedInTransit(transfers: DataFrame, alertDays: int) -> DataFrame:
    """`Escalate Aged In Transit` -> rows for control.logRejectedRecordSet (TRANSFER_AGED_IN_TRANSIT)."""
    return transfers.where((F.col("QuantityInTransit") > 0) & (F.col("TransitDays") > F.lit(alertDays))).select(
        F.col("TransferReference").alias("BusinessKey"),
        F.lit("Transfer despatched but not received within the alert window").alias("RejectReason"),
        F.to_json(
            F.struct(
                "StockTransferId", "TransferReference", "StockItemId", "FromWarehouseSiteCode",
                "ToWarehouseSiteCode", "QuantityInTransit", "TransitDays", "CarrierCode",
            )
        ).alias("RecordPayload"),
    )


# ---------------------------------------------------------------------------
# INV_Reconcile_OnHand
# ---------------------------------------------------------------------------


def buildOnHandComparison(
    position: DataFrame,
    stockHolding: DataFrame,
    stockMovement: DataFrame,
    batchId: int,
    timingWindowMinutes: int,
    siteScope: str,
    nowUtc: datetime,
    reconciliationName: str = "DW on-hand vs operational on-hand",
    objectName: str = "Fact.Stock Holding",
) -> DataFrame:
    """`Build On Hand Comparison` -> etl.ReconciliationResult rows.

    stockHolding is gold.fact_stock_holding with WWIStockItemID / WarehouseSiteCode /
    QuantityOnHand; stockMovement carries StockItemId / WarehouseSiteCode / MovementAtUtc.
    """
    pos = position.alias("pos")
    dw = stockHolding.alias("dw")
    threshold = F.lit(nowUtc).cast("timestamp") - F.expr(f"INTERVAL {int(timingWindowMinutes)} MINUTES")
    recent = (
        stockMovement.where(F.col("MovementAtUtc") >= threshold)
        .select(F.col("StockItemId").alias("mStockItemId"), F.col("WarehouseSiteCode").alias("mWarehouseSiteCode"))
        .distinct()
        .withColumn("HasRecentMovement", F.lit(True))
    )
    posQty = F.coalesce(F.col("pos.QuantityOnHand"), F.lit(0))
    dwQty = F.coalesce(F.col("dw.QuantityOnHand"), F.lit(0))
    joined = pos.join(
        dw,
        (F.col("dw.WWIStockItemID") == F.col("pos.StockItemId"))
        & (F.col("dw.WarehouseSiteCode") == F.col("pos.WarehouseSiteCode")),
        "full_outer",
    )
    if siteScope != "ALL":
        joined = joined.where(F.col("pos.WarehouseSiteCode") == F.lit(siteScope))
    joined = joined.join(
        recent,
        (F.col("mStockItemId") == F.col("pos.StockItemId")) & (F.col("mWarehouseSiteCode") == F.col("pos.WarehouseSiteCode")),
        "left",
    )
    return joined.select(
        F.lit(batchId).cast("bigint").alias("BatchId"),
        F.lit(reconciliationName).alias("ReconciliationName"),
        F.lit(objectName).alias("ObjectName"),
        F.concat(
            F.coalesce(F.col("pos.WarehouseSiteCode"), F.lit("")),
            F.lit("|"),
            F.coalesce(F.col("pos.StockItemId").cast("string"), F.lit("")),
        ).alias("SourceKey"),
        posQty.cast("decimal(18,4)").alias("SourceAmount"),
        dwQty.cast("decimal(18,4)").alias("TargetAmount"),
        (posQty - dwQty).cast("decimal(18,4)").alias("VarianceAmount"),
        F.when(posQty == dwQty, F.lit("Matched"))
        .when(F.col("HasRecentMovement") == F.lit(True), F.lit("Timing"))
        .when(posQty < 0, F.lit("Negative on hand"))
        .otherwise(F.lit("Variance"))
        .alias("VarianceStatus"),
        F.lit(nowUtc).cast("timestamp").alias("EvaluatedAtUtc"),
    )


def classifyDifferences(reconciliation: DataFrame) -> tuple[int, int]:
    """`Classify Differences` -> (GenuineDifferenceCount, TimingDifferenceCount)."""
    row = reconciliation.agg(
        F.sum(F.when(F.col("VarianceStatus") == "Variance", 1).otherwise(0)).alias("genuine"),
        F.sum(F.when(F.col("VarianceStatus") == "Timing", 1).otherwise(0)).alias("timing"),
    ).first()
    return int(row["genuine"] or 0), int(row["timing"] or 0)


def genuineDifferences(reconciliation: DataFrame) -> DataFrame:
    """`Escalate Genuine Differences` -> rows for control.logRejectedRecordSet (ONHAND_VARIANCE)."""
    return reconciliation.where(F.col("VarianceStatus") == "Variance").select(
        F.col("SourceKey").alias("BusinessKey"),
        F.lit("DW on-hand does not agree with the operational position").alias("RejectReason"),
        F.to_json(F.struct("SourceKey", "SourceAmount", "TargetAmount", "VarianceAmount")).alias("RecordPayload"),
    )


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def deterministicHash(df: DataFrame, excludeColumns: tuple[str, ...] = ("LoadDatetime", "RefreshedDatetime", "EvaluatedAtUtc")) -> DataFrame:
    """Row count + order-independent xxhash64 fingerprint (sum of per-row hashes over sorted columns)."""
    cols = sorted(c for c in df.columns if c not in excludeColumns)
    rowHash = F.xxhash64(F.concat_ws("\u0001", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]))
    return df.agg(F.count(F.lit(1)).alias("RowCount"), F.sum(rowHash).alias("RowHashSum"))


def dedupeLatest(df: DataFrame, keyColumns: list[str], orderColumn: str) -> DataFrame:
    """Keep one row per key (latest orderColumn) - used to guard MERGE sources."""
    w = Window.partitionBy(*keyColumns).orderBy(F.col(orderColumn).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
