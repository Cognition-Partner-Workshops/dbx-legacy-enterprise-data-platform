"""Emit the WWI_Inventory domain packages (ssis/12_inventory).

Inventory is the part of the estate that still thinks in warehouse sites and
chiller flags. The snapshot is a full daily rewrite of the position table, the
cycle-count variance posting is a row-by-row adjustment loop the auditors asked
for, and the on-hand reconciliation compares the warehouse against the DW every
run because the two have drifted before.

Run:  python3 ssis/12_inventory/build_inventory_packages.py
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import (CONN_DW, CONN_STAGING, exec_proc, log_package_start,  # noqa: E402
                      log_package_success, log_row_count, new_package, truncate)
from ssisgen import (Column, DataFlow, DataFlowTask, ExecuteSql, Expression,  # noqa: E402
                     date_col, int_col, money_col, str_col)

PROJECT_NAME = "WWI_Inventory"
CONNECTIONS = ["WWI_Staging_DB", "WWI_DW_Destination_DB", "WWI_Reject_Files"]


def bool_col(name):
    return Column(name, "bool")


# ---------------------------------------------------------------------------
# INV_Load_DailySnapshot
# ---------------------------------------------------------------------------

SNAPSHOT_SQL = """
SELECT  p.StockItemId
,       p.WarehouseSiteCode
,       p.BinLocationCode
,       p.SnapshotDate
,       p.QuantityOnHand
,       p.QuantityAllocated
,       p.QuantityOnOrder
,       p.QuantityInTransit
,       p.QuantityOnHand - p.QuantityAllocated          AS QuantityAvailable
,       p.LastMovementDate
,       si.UnitCost
,       si.IsChillerStock
,       si.ShelfLifeDays
,       (p.QuantityOnHand * si.UnitCost)                AS OnHandValue
        /* Ageing bands were agreed with the finance team in 2008 and are still
           what the obsolescence provision is calculated from. */
,       CASE
            WHEN p.LastMovementDate IS NULL                              THEN 'NEVER'
            WHEN DATEDIFF(DAY, p.LastMovementDate, p.SnapshotDate) > 365 THEN 'D365P'
            WHEN DATEDIFF(DAY, p.LastMovementDate, p.SnapshotDate) > 180 THEN 'D180'
            WHEN DATEDIFF(DAY, p.LastMovementDate, p.SnapshotDate) > 90  THEN 'D090'
            ELSE 'FRESH'
        END                                             AS AgeBandCode
,       CASE WHEN si.IsChillerStock = 1
                  AND DATEDIFF(DAY, p.ReceiptDate, p.SnapshotDate) > si.ShelfLifeDays
             THEN 1 ELSE 0 END                          AS IsExpiredChillerStock
FROM    work.InventoryPositionDaily AS p
INNER JOIN stg.StockItem AS si
        ON  si.StockItemId = p.StockItemId
WHERE   p.SnapshotDate = CAST(? AS date)
""".strip()


def build_inv_load_dailysnapshot():
    pkg = new_package(
        "INV_Load_DailySnapshot",
        "Full daily rewrite of the inventory position snapshot. Every stock item and site "
        "combination is snapshotted whether or not it moved, because the turns calculation needs a "
        "dense series; chiller stock past its shelf life is flagged so the write-off report can "
        "pick it up.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("SnapshotDate", "1900-01-01", "string"), ("ExpiredChillerCount", 0, "int")],
    )
    pkg.add_parameter("SnapshotBusinessDate", "1900-01-01", dtype="string",
                      description="Snapshot date; the master passes the batch business date.")
    pkg.add_parameter("DeleteExistingSnapshot", "True", dtype="bool",
                      description="Reruns delete the day's rows first rather than double-counting.")

    start = pkg.add(log_package_start(pkg))
    resolve = pkg.add(Expression("Resolve Snapshot Date",
                                 "@[User::SnapshotDate] = @[$Package::SnapshotBusinessDate]"))
    delete_existing = pkg.add(ExecuteSql(
        "Delete Existing Snapshot",
        CONN_DW,
        "DELETE FROM Fact.[Daily Inventory Snapshot] WHERE [Snapshot Date] = CAST(? AS date);",
        parameter_bindings=[("User::SnapshotDate", 0, "NVARCHAR")],
    ))

    columns = [
        int_col("StockItemId"), str_col("WarehouseSiteCode", 10), str_col("BinLocationCode", 12),
        date_col("SnapshotDate"), int_col("QuantityOnHand"), int_col("QuantityAllocated"),
        int_col("QuantityOnOrder"), int_col("QuantityInTransit"), int_col("QuantityAvailable"),
        date_col("LastMovementDate"), money_col("UnitCost"), bool_col("IsChillerStock"),
        int_col("ShelfLifeDays"), money_col("OnHandValue"), str_col("AgeBandCode", 6),
        int_col("IsExpiredChillerStock"),
    ]
    flow = DataFlow("Load Inventory Snapshot")
    flow.oledb_source("work InventoryPositionDaily", CONN_STAGING, SNAPSHOT_SQL, columns, timeout=3600)
    flow.lookup("Lookup Stock Item Key", CONN_DW,
                "SELECT [Stock Item Key] AS StockItemKey, [WWI Stock Item ID] AS StockItemId "
                "FROM Dimension.[Stock Item] WHERE [Valid To] > SYSDATETIME();",
                ["StockItemId"], [int_col("StockItemKey")], no_match="RD")
    flow.derived_column("Derive Snapshot Measures", [
        ("DaysCoverAtCurrentRate",
         "QuantityAvailable <= 0 ? (DT_I4)0 : QuantityAvailable",
         int_col("DaysCoverAtCurrentRate")),
        ("ObsolescenceProvisionAmount",
         'AgeBandCode == "D365P" ? OnHandValue : (AgeBandCode == "D180" ? OnHandValue / 2 : '
         '(DT_NUMERIC,18,2)0)',
         money_col("ObsolescenceProvisionAmount")),
    ])
    flow.row_count("Count Snapshot Rows", "User::RowsInserted")
    flow.oledb_destination("Fact Daily Inventory Snapshot", CONN_DW,
                           "[Fact].[Daily Inventory Snapshot]", batch_size=100000)
    flow.reject_destination("Reject Unknown Stock Items", CONN_STAGING,
                            "[err].[InventorySnapshotReject]", "Lookup Stock Item Key",
                            "Lookup No Match Output")
    load = pkg.add(DataFlowTask(flow))

    expired = pkg.add(ExecuteSql(
        "Count Expired Chiller Stock",
        CONN_DW,
        "SELECT COUNT(*) AS ExpiredChillerRows FROM Fact.[Daily Inventory Snapshot] "
        "WHERE [Snapshot Date] = CAST(? AS date) AND [Is Expired Chiller Stock] = 1;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("User::SnapshotDate", 0, "NVARCHAR")],
        result_bindings=[("0", "User::ExpiredChillerCount")],
    ))
    counts = pkg.add(log_row_count("Fact.Daily Inventory Snapshot"))
    done = pkg.add(log_package_success())

    pkg.link(start, resolve)
    pkg.link(resolve, delete_existing, expression="@[$Package::DeleteExistingSnapshot]")
    pkg.link(resolve, load, expression="!@[$Package::DeleteExistingSnapshot]")
    pkg.link(delete_existing, load)
    pkg.link(load, expired)
    pkg.link(expired, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# INV_Load_CycleCountVariance
# ---------------------------------------------------------------------------


def build_inv_load_cyclecountvariance():
    pkg = new_package(
        "INV_Load_CycleCountVariance",
        "Posts cycle-count variances as adjustment movements. Counts are matched to the position "
        "as at the count timestamp, variances inside the item's tolerance are auto-posted, and "
        "anything larger is held for a supervisor recount. The posting loop is row by row because "
        "each adjustment needs its own movement number from the sequence.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("VarianceCount", 0, "int"), ("HeldForRecountCount", 0, "int")],
    )
    pkg.add_parameter("CountToleranceUnits", 2, dtype="int",
                      description="Absolute unit tolerance auto-posted without supervisor approval.")
    pkg.add_parameter("CountToleranceValue", 50, dtype="int",
                      description="Absolute value tolerance in reporting currency.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.CycleCountVariance"))
    build = pkg.add(ExecuteSql(
        "Build Variance Set",
        CONN_STAGING,
        "INSERT INTO work.CycleCountVariance\n"
        "    (CycleCountId, StockItemId, WarehouseSiteCode, BinLocationCode, CountedQuantity,\n"
        "     SystemQuantity, VarianceQuantity, VarianceValue, CountedAtUtc, CountStatusCode)\n"
        "SELECT  cc.CycleCountId\n"
        ",       cc.StockItemId\n"
        ",       cc.WarehouseSiteCode\n"
        ",       cc.BinLocationCode\n"
        ",       cc.CountedQuantity\n"
        ",       ISNULL(pos.QuantityOnHand, 0)\n"
        ",       cc.CountedQuantity - ISNULL(pos.QuantityOnHand, 0)\n"
        ",       (cc.CountedQuantity - ISNULL(pos.QuantityOnHand, 0)) * ISNULL(si.UnitCost, 0)\n"
        ",       cc.CountedAtUtc\n"
        ",       CASE WHEN ABS(cc.CountedQuantity - ISNULL(pos.QuantityOnHand, 0)) <= ?\n"
        "              AND ABS((cc.CountedQuantity - ISNULL(pos.QuantityOnHand, 0))\n"
        "                      * ISNULL(si.UnitCost, 0)) <= ?\n"
        "             THEN N'AUTO' ELSE N'HOLD' END\n"
        "FROM    stg.CycleCount AS cc\n"
        "LEFT OUTER JOIN work.InventoryPositionDaily AS pos\n"
        "        ON  pos.StockItemId = cc.StockItemId\n"
        "        AND pos.WarehouseSiteCode = cc.WarehouseSiteCode\n"
        "        AND pos.BinLocationCode = cc.BinLocationCode\n"
        "LEFT OUTER JOIN stg.StockItem AS si ON si.StockItemId = cc.StockItemId\n"
        "WHERE   cc.LoadBatchId = ?\n"
        "AND     cc.CountedQuantity <> ISNULL(pos.QuantityOnHand, 0);",
        parameter_bindings=[
            ("$Package::CountToleranceUnits", 0, "LONG"),
            ("$Package::CountToleranceValue", 1, "LONG"),
            ("$Package::BatchId", 2, "LONG"),
        ],
    ))
    measure = pkg.add(ExecuteSql(
        "Measure Variances",
        CONN_STAGING,
        "SELECT SUM(CASE WHEN CountStatusCode = N'AUTO' THEN 1 ELSE 0 END) AS AutoPost, "
        "       SUM(CASE WHEN CountStatusCode = N'HOLD' THEN 1 ELSE 0 END) AS HeldForRecount "
        "FROM work.CycleCountVariance;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::VarianceCount"), ("1", "User::HeldForRecountCount")],
    ))
    # Adjustment movements are inserted one at a time so that each one takes the
    # next movement number and writes its own audit row, exactly as the warehouse
    # system does when an operator posts manually.
    post = pkg.add(ExecuteSql(
        "Post Adjustment Movements",
        CONN_DW,
        "DECLARE @CycleCountId int, @StockItemId int, @Site nvarchar(10), @Qty int, @Value decimal(18,2);\n"
        "DECLARE variance_cur CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT CycleCountId, StockItemId, WarehouseSiteCode, VarianceQuantity, VarianceValue\n"
        "    FROM   work.CycleCountVariance\n"
        "    WHERE  CountStatusCode = N'AUTO'\n"
        "    ORDER BY CountedAtUtc;\n"
        "OPEN variance_cur;\n"
        "FETCH NEXT FROM variance_cur INTO @CycleCountId, @StockItemId, @Site, @Qty, @Value;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    EXEC Integration.usp_PostInventoryAdjustment\n"
        "         @StockItemId = @StockItemId, @WarehouseSiteCode = @Site,\n"
        "         @AdjustmentQuantity = @Qty, @AdjustmentValue = @Value,\n"
        "         @MovementReasonCode = N'CYCLECOUNT', @SourceReference = @CycleCountId;\n"
        "    FETCH NEXT FROM variance_cur INTO @CycleCountId, @StockItemId, @Site, @Qty, @Value;\n"
        "END\n"
        "CLOSE variance_cur;\n"
        "DEALLOCATE variance_cur;",
    ))
    hold = pkg.add(ExecuteSql(
        "Queue Recounts",
        CONN_STAGING,
        "INSERT INTO etl.RejectedRecord "
        "    (BatchId, ObjectName, RejectStage, RejectReasonCode, RejectReason, "
        "     BusinessKey, LoggedAtUtc, IsReprocessed) "
        "SELECT ?, N'stg.CycleCount', N'Fact', N'COUNT_VARIANCE_HELD', "
        "       N'Cycle-count variance above tolerance; supervisor recount required', "
        "       CAST(CycleCountId AS nvarchar(20)), SYSUTCDATETIME(), 0 "
        "FROM   work.CycleCountVariance WHERE CountStatusCode = N'HOLD';",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("Fact.Movement"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, build)
    pkg.link(build, measure)
    pkg.link(measure, post, expression="@[User::VarianceCount] > 0")
    pkg.link(measure, hold, expression="@[User::HeldForRecountCount] > 0")
    pkg.link(post, counts, value="Completion")
    pkg.link(hold, counts, value="Completion")
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# INV_Load_Replenishment
# ---------------------------------------------------------------------------

REPLENISH_SQL = """
SELECT  si.StockItemId
,       si.StockItemName
,       si.SupplierId
,       si.LeadTimeDays
,       si.ReorderLevel
,       si.TargetStockLevel
,       si.QuantityPerOuter
,       si.IsChillerStock
,       si.RegionCode
,       pos.WarehouseSiteCode
,       pos.QuantityOnHand
,       pos.QuantityOnOrder
,       pos.QuantityAllocated
,       ISNULL(dem.AverageDailyDemand, 0)               AS AverageDailyDemand
        /* Reorder point uses lead-time demand plus a regional safety factor.
           APAC carries more cover because of shipping variability, EU less
           because of the consolidated distribution centre. */
,       CASE si.RegionCode
            WHEN 'APAC' THEN 1.5
            WHEN 'EU'   THEN 1.1
            ELSE 1.25
        END                                             AS SafetyFactor
FROM    stg.StockItem AS si
INNER JOIN work.InventoryPositionDaily AS pos
        ON  pos.StockItemId = si.StockItemId
LEFT OUTER JOIN work.StockItemDemand AS dem
        ON  dem.StockItemId = si.StockItemId
        AND dem.WarehouseSiteCode = pos.WarehouseSiteCode
WHERE   si.IsDiscontinued = 0
""".strip()


def build_inv_load_replenishment():
    pkg = new_package(
        "INV_Load_Replenishment",
        "Refreshes the replenishment suggestions. The reorder point is lead-time demand times a "
        "regional safety factor, suggested quantities are rounded up to the supplier's outer "
        "quantity, and chiller items are capped at the shelf-life-adjusted cover so the depots do "
        "not order stock that will expire.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("SuggestionCount", 0, "int"), ("StockoutRiskCount", 0, "int")],
    )
    pkg.add_parameter("CoverDays", 21, dtype="int",
                      description="Target days of cover the suggestion aims for.")
    pkg.add_parameter("SuppressChillerSuggestions", "False", dtype="bool",
                      description="Depots occasionally suspend chiller ordering during a heatwave.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.ReplenishmentSuggestion"))

    columns = [
        int_col("StockItemId"), str_col("StockItemName", 100), int_col("SupplierId"),
        int_col("LeadTimeDays"), int_col("ReorderLevel"), int_col("TargetStockLevel"),
        int_col("QuantityPerOuter"), bool_col("IsChillerStock"), str_col("RegionCode", 4),
        str_col("WarehouseSiteCode", 10), int_col("QuantityOnHand"), int_col("QuantityOnOrder"),
        int_col("QuantityAllocated"), money_col("AverageDailyDemand"), money_col("SafetyFactor"),
    ]
    flow = DataFlow("Calculate Replenishment")
    flow.oledb_source("stg StockItem With Position", CONN_STAGING, REPLENISH_SQL, columns, timeout=1800)
    flow.derived_column("Derive Reorder Point", [
        ("ReorderPoint",
         "(DT_I4)(AverageDailyDemand * LeadTimeDays * SafetyFactor)", int_col("ReorderPoint")),
        ("ProjectedAvailable",
         "QuantityOnHand + QuantityOnOrder - QuantityAllocated", int_col("ProjectedAvailable")),
        ("DaysOfCover",
         "AverageDailyDemand == 0 ? (DT_NUMERIC,18,2)999 : "
         "(QuantityOnHand - QuantityAllocated) / AverageDailyDemand", money_col("DaysOfCover")),
    ])
    flow.derived_column("Derive Suggested Quantity", [
        ("RawSuggestedQuantity",
         "(DT_I4)(AverageDailyDemand * @[$Package::CoverDays]) - ProjectedAvailable",
         int_col("RawSuggestedQuantity")),
        ("SuggestedQuantity",
         "((DT_I4)(AverageDailyDemand * @[$Package::CoverDays]) - ProjectedAvailable) <= 0 ? 0 : "
         "((QuantityPerOuter <= 1) ? "
         "((DT_I4)(AverageDailyDemand * @[$Package::CoverDays]) - ProjectedAvailable) : "
         "(((((DT_I4)(AverageDailyDemand * @[$Package::CoverDays]) - ProjectedAvailable) "
         "/ QuantityPerOuter) + 1) * QuantityPerOuter)",
         int_col("SuggestedQuantity")),
        ("IsStockoutRisk",
         "ProjectedAvailable <= (DT_I4)(AverageDailyDemand * LeadTimeDays) ? (DT_BOOL)1 : (DT_BOOL)0",
         bool_col("IsStockoutRisk")),
    ])
    flow.conditional_split("Split Suggestions", [
        ("Suggest", "SuggestedQuantity > 0"),
        ("NoAction", "SuggestedQuantity <= 0"),
    ])
    flow.row_count("Count Suggestions", "User::RowsInserted")
    flow.oledb_destination("work ReplenishmentSuggestion", CONN_STAGING,
                           "[work].[ReplenishmentSuggestion]", batch_size=50000)
    calculate = pkg.add(DataFlowTask(flow))

    suppress = pkg.add(ExecuteSql(
        "Suppress Chiller Suggestions",
        CONN_STAGING,
        "DELETE FROM work.ReplenishmentSuggestion WHERE IsChillerStock = 1;",
    ))
    publish = pkg.add(ExecuteSql(
        "Publish Inventory Health",
        CONN_DW,
        "MERGE Aggregate.[Daily Inventory Health] AS target\n"
        "USING (SELECT WarehouseSiteCode, RegionCode,\n"
        "              COUNT(*) AS SuggestionCount,\n"
        "              SUM(CASE WHEN IsStockoutRisk = 1 THEN 1 ELSE 0 END) AS StockoutRiskCount,\n"
        "              SUM(SuggestedQuantity) AS SuggestedUnits\n"
        "       FROM   work.ReplenishmentSuggestion\n"
        "       GROUP BY WarehouseSiteCode, RegionCode) AS source\n"
        "    ON  target.[Warehouse Site Code] = source.WarehouseSiteCode\n"
        "WHEN MATCHED THEN UPDATE SET target.[Suggestion Count] = source.SuggestionCount,\n"
        "                             target.[Stockout Risk Count] = source.StockoutRiskCount,\n"
        "                             target.[Suggested Units] = source.SuggestedUnits\n"
        "WHEN NOT MATCHED BY TARGET THEN\n"
        "    INSERT ([Warehouse Site Code], [Region Code], [Suggestion Count],\n"
        "            [Stockout Risk Count], [Suggested Units])\n"
        "    VALUES (source.WarehouseSiteCode, source.RegionCode, source.SuggestionCount,\n"
        "            source.StockoutRiskCount, source.SuggestedUnits);",
    ))
    risk = pkg.add(ExecuteSql(
        "Count Stockout Risks",
        CONN_STAGING,
        "SELECT COUNT(*) AS StockoutRisks FROM work.ReplenishmentSuggestion WHERE IsStockoutRisk = 1;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::StockoutRiskCount")],
    ))
    counts = pkg.add(log_row_count("Aggregate.Daily Inventory Health"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, calculate)
    pkg.link(calculate, suppress, expression="@[$Package::SuppressChillerSuggestions]")
    pkg.link(calculate, risk, expression="!@[$Package::SuppressChillerSuggestions]")
    pkg.link(suppress, risk)
    pkg.link(risk, publish)
    pkg.link(publish, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# INV_Load_StockTransfer
# ---------------------------------------------------------------------------

TRANSFER_SQL = """
SELECT  t.StockTransferId
,       t.TransferReference
,       t.StockItemId
,       t.FromWarehouseSiteCode
,       t.ToWarehouseSiteCode
,       t.DespatchedAtUtc
,       t.ReceivedAtUtc
,       t.QuantityDespatched
,       t.QuantityReceived
,       t.QuantityDespatched - ISNULL(t.QuantityReceived, 0) AS QuantityInTransit
,       t.TransferStatusCode
,       t.CarrierCode
,       si.UnitCost
,       fw.RegionCode                                   AS FromRegionCode
,       tw.RegionCode                                   AS ToRegionCode
        /* Cross-region transfers are treated as an intercompany sale in the
           finance mart, so the movement carries the transfer price rather than
           standard cost. */
,       CASE WHEN fw.RegionCode <> tw.RegionCode
             THEN ISNULL(tp.TransferPrice, si.UnitCost * 1.08)
             ELSE si.UnitCost
        END                                             AS MovementUnitValue
FROM    stg.StockMovement AS t
INNER JOIN stg.StockItem AS si ON si.StockItemId = t.StockItemId
INNER JOIN stg.WarehouseSite AS fw ON fw.WarehouseSiteCode = t.FromWarehouseSiteCode
INNER JOIN stg.WarehouseSite AS tw ON tw.WarehouseSiteCode = t.ToWarehouseSiteCode
LEFT OUTER JOIN stg.TransferPrice AS tp
        ON  tp.StockItemId = t.StockItemId
        AND tp.FromRegionCode = fw.RegionCode
        AND tp.ToRegionCode = tw.RegionCode
WHERE   t.MovementTypeCode = 'TRANSFER'
AND     t.LoadBatchId = ?
""".strip()


def build_inv_load_stocktransfer():
    pkg = new_package(
        "INV_Load_StockTransfer",
        "Loads inter-site stock transfer movements. A transfer produces two movement rows - an "
        "issue at the sending site and a receipt at the receiving site - and cross-region "
        "transfers are valued at the intercompany transfer price rather than standard cost. "
        "Transfers still in transit are carried forward so the in-transit balance ties out.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("InTransitCount", 0, "int"), ("CrossRegionCount", 0, "int")],
    )
    pkg.add_parameter("InTransitAgeAlertDays", 10, dtype="int",
                      description="Transfers older than this without a receipt are escalated.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.StockTransferMovement"))

    columns = [
        int_col("StockTransferId"), str_col("TransferReference", 30), int_col("StockItemId"),
        str_col("FromWarehouseSiteCode", 10), str_col("ToWarehouseSiteCode", 10),
        date_col("DespatchedAtUtc"), date_col("ReceivedAtUtc"), int_col("QuantityDespatched"),
        int_col("QuantityReceived"), int_col("QuantityInTransit"), str_col("TransferStatusCode", 10),
        str_col("CarrierCode", 10), money_col("UnitCost"), str_col("FromRegionCode", 4),
        str_col("ToRegionCode", 4), money_col("MovementUnitValue"),
    ]
    flow = DataFlow("Load Transfer Movements")
    flow.oledb_source("stg StockMovement Transfers", CONN_STAGING, TRANSFER_SQL, columns, timeout=1800)
    flow.derived_column("Derive Movement Attributes", [
        ("IsCrossRegion", "FromRegionCode != ToRegionCode ? (DT_BOOL)1 : (DT_BOOL)0",
         bool_col("IsCrossRegion")),
        ("IssueValue", "QuantityDespatched * MovementUnitValue * -1", money_col("IssueValue")),
        ("ReceiptValue", "QuantityReceived * MovementUnitValue", money_col("ReceiptValue")),
        ("TransitDays",
         "ISNULL(ReceivedAtUtc) ? DATEDIFF(\"Dy\", DespatchedAtUtc, GETDATE()) : "
         "DATEDIFF(\"Dy\", DespatchedAtUtc, ReceivedAtUtc)",
         int_col("TransitDays")),
    ])
    flow.conditional_split("Split Transfer Legs", [
        ("Despatched", "QuantityDespatched > 0"),
        ("ReceiptOnly", "QuantityDespatched == 0"),
    ])
    flow.row_count("Count Transfer Rows", "User::RowsRead")
    flow.oledb_destination("work StockTransferMovement", CONN_STAGING,
                           "[work].[StockTransferMovement]", batch_size=50000)
    flow.branch_destination("work StockTransferReceiptOnly", CONN_STAGING,
                            "[work].[StockTransferReceiptOnly]", "Split Transfer Legs", "ReceiptOnly")
    load = pkg.add(DataFlowTask(flow))

    post = pkg.add(exec_proc(
        "Post Transfer Movements",
        "EXEC Integration.usp_PostTransferMovements @BatchId = ?, @RowsInserted = ? OUTPUT;",
        connection=CONN_DW,
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    intransit = pkg.add(ExecuteSql(
        "Escalate Aged In Transit",
        CONN_STAGING,
        "INSERT INTO etl.RejectedRecord "
        "    (BatchId, ObjectName, RejectStage, RejectReasonCode, RejectReason, "
        "     BusinessKey, LoggedAtUtc, IsReprocessed) "
        "SELECT ?, N'stg.StockMovement', N'Fact', N'TRANSFER_AGED_IN_TRANSIT', "
        "       N'Transfer despatched but not received within the alert window', "
        "       TransferReference, SYSUTCDATETIME(), 0 "
        "FROM   work.StockTransferMovement "
        "WHERE  QuantityInTransit > 0 AND TransitDays > ?;",
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::InTransitAgeAlertDays", 1, "LONG"),
        ],
    ))
    counts = pkg.add(log_row_count("Fact.Movement"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, load)
    pkg.link(load, post)
    pkg.link(post, intransit, value="Completion")
    pkg.link(intransit, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# INV_Reconcile_OnHand
# ---------------------------------------------------------------------------


def build_inv_reconcile_onhand():
    pkg = new_package(
        "INV_Reconcile_OnHand",
        "Ties the warehouse on-hand quantity in the DW stock-holding fact back to the operational "
        "position by site and item. Differences are classified - timing (a movement landed between "
        "the two reads), sign (a negative on-hand the warehouse system allows and the DW does not) "
        "or genuine - and only genuine differences are escalated.",
        source_system="WWIOLTP",
        connections=(CONN_DW,),
        extra_variables=[("GenuineDifferenceCount", 0, "int"), ("TimingDifferenceCount", 0, "int")],
    )
    pkg.add_parameter("SiteScope", "ALL", dtype="string",
                      description="ALL, or a single warehouse site code.")
    pkg.add_parameter("TimingWindowMinutes", 30, dtype="int",
                      description="Movements inside this window are classified as timing differences.")

    start = pkg.add(log_package_start(pkg))
    build = pkg.add(ExecuteSql(
        "Build On Hand Comparison",
        CONN_DW,
        "INSERT INTO etl.ReconciliationResult\n"
        "    (BatchId, ReconciliationName, ObjectName, SourceKey, SourceAmount, TargetAmount,\n"
        "     VarianceAmount, VarianceStatus, EvaluatedAtUtc)\n"
        "SELECT  ?\n"
        ",       N'DW on-hand vs operational on-hand'\n"
        ",       N'Fact.Stock Holding'\n"
        ",       CONCAT(pos.WarehouseSiteCode, N'|', CAST(pos.StockItemId AS nvarchar(20)))\n"
        ",       ISNULL(pos.QuantityOnHand, 0)\n"
        ",       ISNULL(dw.[Quantity On Hand], 0)\n"
        ",       ISNULL(pos.QuantityOnHand, 0) - ISNULL(dw.[Quantity On Hand], 0)\n"
        ",       CASE\n"
        "            WHEN ISNULL(pos.QuantityOnHand, 0) = ISNULL(dw.[Quantity On Hand], 0)\n"
        "                THEN N'Matched'\n"
        "            WHEN EXISTS (SELECT 1 FROM stg.StockMovement AS m\n"
        "                         WHERE m.StockItemId = pos.StockItemId\n"
        "                           AND m.WarehouseSiteCode = pos.WarehouseSiteCode\n"
        "                           AND m.MovementAtUtc >= DATEADD(MINUTE, -?, SYSUTCDATETIME()))\n"
        "                THEN N'Timing'\n"
        "            WHEN ISNULL(pos.QuantityOnHand, 0) < 0 THEN N'Negative on hand'\n"
        "            ELSE N'Variance'\n"
        "        END\n"
        ",       SYSUTCDATETIME()\n"
        "FROM    work.InventoryPositionDaily AS pos\n"
        "FULL OUTER JOIN Fact.[Stock Holding] AS dw\n"
        "        ON  dw.[WWI Stock Item ID] = pos.StockItemId\n"
        "        AND dw.[Warehouse Site Code] = pos.WarehouseSiteCode\n"
        "WHERE   ? = N'ALL' OR pos.WarehouseSiteCode = ?;",
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::TimingWindowMinutes", 1, "LONG"),
            ("$Package::SiteScope", 2, "NVARCHAR"),
            ("$Package::SiteScope", 3, "NVARCHAR"),
        ],
    ))
    measure = pkg.add(ExecuteSql(
        "Classify Differences",
        CONN_DW,
        "SELECT SUM(CASE WHEN VarianceStatus = N'Variance' THEN 1 ELSE 0 END) AS GenuineDifferences, "
        "       SUM(CASE WHEN VarianceStatus = N'Timing' THEN 1 ELSE 0 END) AS TimingDifferences "
        "FROM etl.ReconciliationResult "
        "WHERE BatchId = ? AND ObjectName = N'Fact.Stock Holding';",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::GenuineDifferenceCount"), ("1", "User::TimingDifferenceCount")],
    ))
    escalate = pkg.add(ExecuteSql(
        "Escalate Genuine Differences",
        CONN_STAGING,
        "INSERT INTO etl.RejectedRecord "
        "    (BatchId, ObjectName, RejectStage, RejectReasonCode, RejectReason, "
        "     BusinessKey, LoggedAtUtc, IsReprocessed) "
        "SELECT ?, N'Fact.Stock Holding', N'Fact', N'ONHAND_VARIANCE', "
        "       N'DW on-hand does not agree with the operational position', "
        "       SourceKey, SYSUTCDATETIME(), 0 "
        "FROM   etl.ReconciliationResult "
        "WHERE  BatchId = ? AND VarianceStatus = N'Variance' "
        "AND    ObjectName = N'Fact.Stock Holding';",
        parameter_bindings=[("$Package::BatchId", 0, "LONG"), ("$Package::BatchId", 1, "LONG")],
    ))
    counts = pkg.add(log_row_count("etl.ReconciliationResult"))
    done = pkg.add(log_package_success())

    pkg.link(start, build)
    pkg.link(build, measure)
    pkg.link(measure, escalate, expression="@[User::GenuineDifferenceCount] > 0")
    pkg.link(measure, counts, expression="@[User::GenuineDifferenceCount] == 0")
    pkg.link(escalate, counts)
    pkg.link(counts, done)
    return pkg


BUILDERS = [
    build_inv_load_dailysnapshot,
    build_inv_load_cyclecountvariance,
    build_inv_load_replenishment,
    build_inv_load_stocktransfer,
    build_inv_reconcile_onhand,
]


def main():
    written = []
    names = []
    for builder in BUILDERS:
        pkg = builder()
        written.append(pkg.write(os.path.join(HERE, pkg.name + ".dtsx")))
        names.append(pkg.name)
    written.extend(project.write_project(HERE, PROJECT_NAME, names, CONNECTIONS))
    for path in written:
        print(os.path.relpath(path, REPO_ROOT))


if __name__ == "__main__":
    main()
