"""Emit the WWI_Procurement domain packages (ssis/13_procurement).

Procurement was the last domain onto the warehouse and it shows: the spend load
still carries the old ERP contract keys, three-way matching reimplements the
tolerance rules that also exist in the ERP, and the supplier statement export
writes a fixed-layout file because two of the larger suppliers never moved off
it.

Run:  python3 ssis/13_procurement/build_procurement_packages.py
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import (CONN_DW, CONN_FILES, CONN_STAGING, exec_proc,  # noqa: E402
                      log_package_start, log_package_success, log_row_count,
                      new_package, truncate)
from ssisgen import (Column, DataFlow, DataFlowTask, ExecuteSql, Expression,  # noqa: E402
                     date_col, int_col, money_col, str_col)

PROJECT_NAME = "WWI_Procurement"
CONNECTIONS = ["WWI_Staging_DB", "WWI_DW_Destination_DB", "WWI_Archive_Files", "WWI_Reject_Files"]


def bool_col(name):
    return Column(name, "bool")


# ---------------------------------------------------------------------------
# PRC_Load_PurchaseSpend
# ---------------------------------------------------------------------------

SPEND_SQL = """
SELECT  pol.PurchaseOrderLineId
,       pol.PurchaseOrderNumber
,       pol.LineNumber
,       poh.SupplierId
,       poh.OrderDate
,       poh.BuyerPersonId
,       poh.RegionCode
,       poh.CurrencyCode
,       pol.StockItemId
,       pol.OrderedOuters
,       pol.ExpectedUnitPricePerOuter
,       pol.OrderedOuters * pol.ExpectedUnitPricePerOuter       AS OrderedAmount
,       pol.ReceivedOuters
,       pol.IsOrderLineFinalized
,       pol.CategoryCode
,       vc.ContractNumber
,       vc.ContractPricePerOuter
,       vc.ContractStartDate
,       vc.ContractEndDate
        /* The ERP contract key changed format in 2013 but the old numbers were
           never migrated, so both shapes have to be matched. */
,       CASE
            WHEN vc.ContractNumber IS NOT NULL              THEN vc.ContractNumber
            WHEN pol.LegacyContractRef LIKE 'C-%'           THEN pol.LegacyContractRef
            WHEN pol.LegacyContractRef LIKE '[0-9][0-9][0-9][0-9][0-9]'
                THEN 'C-' + pol.LegacyContractRef
            ELSE NULL
        END                                                     AS ResolvedContractNumber
FROM    stg.PurchaseOrderLine AS pol
INNER JOIN stg.PurchaseOrderHeader AS poh
        ON  poh.PurchaseOrderNumber = pol.PurchaseOrderNumber
LEFT OUTER JOIN stg.VendorContract AS vc
        ON  vc.SupplierId = poh.SupplierId
        AND vc.CategoryCode = pol.CategoryCode
        AND poh.OrderDate BETWEEN vc.ContractStartDate AND ISNULL(vc.ContractEndDate, '9999-12-31')
WHERE   pol.LoadBatchId = ?
AND     poh.OrderStatusCode NOT IN ('CANC', 'DRAFT')
""".strip()


def build_prc_load_purchasespend():
    pkg = new_package(
        "PRC_Load_PurchaseSpend",
        "Loads purchase order spend into the purchase fact with contract linkage. Contract "
        "resolution has to cope with two generations of contract reference, and spend is "
        "classified as on-contract, off-contract or maverick (no contract exists for the category "
        "at all) because the savings reporting is built on that split.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("OffContractCount", 0, "int"), ("MaverickSpendAmount", 0, "decimal")],
    )
    pkg.add_parameter("SpendCategoryScope", "ALL", dtype="string",
                      description="ALL, or a single procurement category code.")
    pkg.add_parameter("PriceVarianceTolerancePercent", 5, dtype="int",
                      description="Percentage above contract price still treated as on-contract.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.PurchaseSpendLine"))

    columns = [
        int_col("PurchaseOrderLineId"), str_col("PurchaseOrderNumber", 20), int_col("LineNumber"),
        int_col("SupplierId"), date_col("OrderDate"), int_col("BuyerPersonId"),
        str_col("RegionCode", 4), str_col("CurrencyCode", 3), int_col("StockItemId"),
        int_col("OrderedOuters"), money_col("ExpectedUnitPricePerOuter"), money_col("OrderedAmount"),
        int_col("ReceivedOuters"), bool_col("IsOrderLineFinalized"), str_col("CategoryCode", 10),
        str_col("ContractNumber", 20), money_col("ContractPricePerOuter"),
        date_col("ContractStartDate"), date_col("ContractEndDate"),
        str_col("ResolvedContractNumber", 20),
    ]
    flow = DataFlow("Load Purchase Spend")
    flow.oledb_source("stg PurchaseOrderLine", CONN_STAGING, SPEND_SQL, columns, timeout=3600)
    flow.derived_column("Classify Spend", [
        ("PriceVariancePercent",
         "ISNULL(ContractPricePerOuter) || ContractPricePerOuter == 0 ? (DT_NUMERIC,18,2)0 : "
         "(ExpectedUnitPricePerOuter - ContractPricePerOuter) * 100 / ContractPricePerOuter",
         money_col("PriceVariancePercent")),
        ("SpendClassCode",
         'ISNULL(ResolvedContractNumber) ? (DT_WSTR,12)"MAVERICK" : '
         '(ISNULL(ContractPricePerOuter) ? (DT_WSTR,12)"OFFCONTRACT" : (DT_WSTR,12)"ONCONTRACT")',
         str_col("SpendClassCode", 12)),
    ])
    flow.derived_column("Derive Savings", [
        ("ContractedAmount",
         "ISNULL(ContractPricePerOuter) ? OrderedAmount : OrderedOuters * ContractPricePerOuter",
         money_col("ContractedAmount")),
        ("SavingsAmount",
         "ISNULL(ContractPricePerOuter) ? (DT_NUMERIC,18,2)0 : "
         "(OrderedOuters * ContractPricePerOuter) - OrderedAmount",
         money_col("SavingsAmount")),
    ])
    flow.lookup("Lookup Supplier Key", CONN_DW,
                "SELECT [Supplier Key] AS SupplierKey, [WWI Supplier ID] AS SupplierId "
                "FROM Dimension.Supplier WHERE [Valid To] > SYSDATETIME();",
                ["SupplierId"], [int_col("SupplierKey")], no_match="RD")
    flow.row_count("Count Spend Lines", "User::RowsRead")
    flow.oledb_destination("work PurchaseSpendLine", CONN_STAGING, "[work].[PurchaseSpendLine]",
                           batch_size=100000)
    flow.reject_destination("Reject Unknown Suppliers", CONN_STAGING, "[err].[PurchaseSpendReject]",
                            "Lookup Supplier Key", "Lookup No Match Output")
    load = pkg.add(DataFlowTask(flow))

    post = pkg.add(exec_proc(
        "Post Purchase Fact",
        "EXEC Integration.usp_PostPurchaseSpend @BatchId = ?, @CategoryScope = ?, "
        "@RowsInserted = ? OUTPUT;",
        connection=CONN_DW,
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("$Package::SpendCategoryScope", 1, "NVARCHAR"),
        ],
    ))
    maverick = pkg.add(ExecuteSql(
        "Measure Maverick Spend",
        CONN_STAGING,
        "SELECT SUM(CASE WHEN SpendClassCode <> N'ONCONTRACT' THEN 1 ELSE 0 END) AS OffContractLines, "
        "       ISNULL(SUM(CASE WHEN SpendClassCode = N'MAVERICK' THEN OrderedAmount ELSE 0 END), 0) "
        "           AS MaverickAmount "
        "FROM work.PurchaseSpendLine;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::OffContractCount"), ("1", "User::MaverickSpendAmount")],
    ))
    counts = pkg.add(log_row_count("Fact.Purchase"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, load)
    pkg.link(load, post)
    pkg.link(post, maverick, value="Completion")
    pkg.link(maverick, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# PRC_Load_ReceiptMatching
# ---------------------------------------------------------------------------

RECEIPT_SQL = """
SELECT  r.ReceiptId
,       r.ReceiptNumber
,       r.PurchaseOrderNumber
,       r.LineNumber
,       r.ReceivedAtUtc
,       r.ReceivedOuters
,       r.WarehouseSiteCode
,       pol.OrderedOuters
,       pol.ExpectedUnitPricePerOuter
,       ail.InvoicedOuters
,       ail.InvoicedUnitPrice
,       ail.ApInvoiceNumber
,       poh.SupplierId
,       poh.RegionCode
        /* Three-way match tolerances: quantity tolerance is a percentage,
           price tolerance is both a percentage and an absolute floor so that
           low-value lines do not fail on rounding. */
,       ISNULL(tol.QuantityTolerancePercent, 2)         AS QuantityTolerancePercent
,       ISNULL(tol.PriceTolerancePercent, 3)            AS PriceTolerancePercent
,       ISNULL(tol.PriceToleranceAbsolute, 1.00)        AS PriceToleranceAbsolute
FROM    stg.Receipt AS r
INNER JOIN stg.PurchaseOrderLine AS pol
        ON  pol.PurchaseOrderNumber = r.PurchaseOrderNumber
        AND pol.LineNumber = r.LineNumber
INNER JOIN stg.PurchaseOrderHeader AS poh
        ON  poh.PurchaseOrderNumber = r.PurchaseOrderNumber
LEFT OUTER JOIN stg.ApInvoiceLine AS ail
        ON  ail.PurchaseOrderNumber = r.PurchaseOrderNumber
        AND ail.PurchaseOrderLineNumber = r.LineNumber
LEFT OUTER JOIN stg.MatchTolerance AS tol
        ON  tol.RegionCode = poh.RegionCode
        AND tol.CategoryCode = pol.CategoryCode
WHERE   r.LoadBatchId = ?
""".strip()


def build_prc_load_receiptmatching():
    pkg = new_package(
        "PRC_Load_ReceiptMatching",
        "Three-way matches receipts against the purchase order and the AP invoice line and loads "
        "the receipt fact with the match result. Quantity and price variances are evaluated "
        "against the regional tolerance table; unmatched receipts are held as goods-received-not-"
        "invoiced so the accrual report can pick them up.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("GrniCount", 0, "int"), ("MatchExceptionCount", 0, "int")],
    )
    pkg.add_parameter("AccrualCutoffDays", 45, dtype="int",
                      description="Age at which an unmatched receipt is escalated rather than accrued.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.ReceiptMatch"))

    columns = [
        int_col("ReceiptId"), str_col("ReceiptNumber", 20), str_col("PurchaseOrderNumber", 20),
        int_col("LineNumber"), date_col("ReceivedAtUtc"), int_col("ReceivedOuters"),
        str_col("WarehouseSiteCode", 10), int_col("OrderedOuters"),
        money_col("ExpectedUnitPricePerOuter"), int_col("InvoicedOuters"),
        money_col("InvoicedUnitPrice"), str_col("ApInvoiceNumber", 30), int_col("SupplierId"),
        str_col("RegionCode", 4), money_col("QuantityTolerancePercent"),
        money_col("PriceTolerancePercent"), money_col("PriceToleranceAbsolute"),
    ]
    flow = DataFlow("Match Receipts")
    flow.oledb_source("stg Receipt", CONN_STAGING, RECEIPT_SQL, columns, timeout=3600)
    flow.derived_column("Derive Match Variances", [
        ("QuantityVariance",
         "ReceivedOuters - ISNULL(InvoicedOuters) ? 0 : InvoicedOuters", int_col("QuantityVariance")),
        ("PriceVariance",
         "ISNULL(InvoicedUnitPrice) ? (DT_NUMERIC,18,2)0 : "
         "InvoicedUnitPrice - ExpectedUnitPricePerOuter", money_col("PriceVariance")),
        ("IsInvoiced", "ISNULL(ApInvoiceNumber) ? (DT_BOOL)0 : (DT_BOOL)1", bool_col("IsInvoiced")),
    ])
    flow.derived_column("Evaluate Match Result", [
        ("MatchResultCode",
         'IsInvoiced == (DT_BOOL)0 ? (DT_WSTR,12)"GRNI" : '
         '(ABS(PriceVariance) <= PriceToleranceAbsolute || '
         'ABS(PriceVariance) * 100 / (ExpectedUnitPricePerOuter == 0 ? 1 : ExpectedUnitPricePerOuter) '
         '<= PriceTolerancePercent ? '
         '(ABS(QuantityVariance) * 100 / (OrderedOuters == 0 ? 1 : OrderedOuters) '
         '<= QuantityTolerancePercent ? (DT_WSTR,12)"MATCHED" : (DT_WSTR,12)"QTYEXCEPT") : '
         '(DT_WSTR,12)"PRICEEXCEPT")',
         str_col("MatchResultCode", 12)),
        ("ReceiptAgeDays", 'DATEDIFF("Dy", ReceivedAtUtc, GETDATE())', int_col("ReceiptAgeDays")),
    ])
    flow.conditional_split("Split Match Outcome", [
        ("Matched", 'MatchResultCode == "MATCHED"'),
        ("Grni", 'MatchResultCode == "GRNI"'),
        ("Exception", 'MatchResultCode == "QTYEXCEPT" || MatchResultCode == "PRICEEXCEPT"'),
    ])
    flow.row_count("Count Matched Receipts", "User::RowsInserted")
    flow.oledb_destination("Fact Purchase Receipt", CONN_DW, "[Fact].[Purchase Receipt]",
                           batch_size=100000)
    flow.branch_destination("work ReceiptGrni", CONN_STAGING, "[work].[ReceiptGrni]",
                            "Split Match Outcome", "Grni")
    flow.branch_destination("work ReceiptMatchException", CONN_STAGING,
                            "[work].[ReceiptMatchException]", "Split Match Outcome", "Exception")
    match = pkg.add(DataFlowTask(flow))

    accrue = pkg.add(ExecuteSql(
        "Accrue GRNI",
        CONN_DW,
        "INSERT INTO Fact.[Purchase Receipt] "
        "    ([Receipt Number], [Purchase Order Number], [Supplier Key], [Received Outers], "
        "     [Accrual Amount], [Match Result Code], [Receipt Date]) "
        "SELECT g.ReceiptNumber, g.PurchaseOrderNumber, s.[Supplier Key], g.ReceivedOuters, "
        "       g.ReceivedOuters * g.ExpectedUnitPricePerOuter, N'ACCRUED', g.ReceivedAtUtc "
        "FROM   work.ReceiptGrni AS g "
        "INNER JOIN Dimension.Supplier AS s "
        "        ON s.[WWI Supplier ID] = g.SupplierId AND s.[Valid To] > SYSDATETIME() "
        "WHERE  g.ReceiptAgeDays <= ?;",
        parameter_bindings=[("$Package::AccrualCutoffDays", 0, "LONG")],
    ))
    exceptions = pkg.add(ExecuteSql(
        "Raise Match Exceptions",
        CONN_STAGING,
        "INSERT INTO etl.RejectedRecord "
        "    (BatchId, ObjectName, RejectStage, RejectReasonCode, RejectReasonDescription, "
        "     SourceKey, RejectedAtUtc, IsReprocessed) "
        "SELECT ?, N'stg.Receipt', N'Fact', MatchResultCode, "
        "       N'Three-way match exception outside the regional tolerance', "
        "       ReceiptNumber, SYSUTCDATETIME(), 0 "
        "FROM   work.ReceiptMatchException;",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    measure = pkg.add(ExecuteSql(
        "Measure Match Outcomes",
        CONN_STAGING,
        "SELECT (SELECT COUNT(*) FROM work.ReceiptGrni) AS GrniRows, "
        "       (SELECT COUNT(*) FROM work.ReceiptMatchException) AS ExceptionRows;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::GrniCount"), ("1", "User::MatchExceptionCount")],
    ))
    counts = pkg.add(log_row_count("Fact.Purchase Receipt"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, match)
    pkg.link(match, accrue)
    pkg.link(accrue, exceptions, value="Completion")
    pkg.link(exceptions, measure)
    pkg.link(measure, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# PRC_Load_SupplierScorecard
# ---------------------------------------------------------------------------


def build_prc_load_supplierscorecard():
    pkg = new_package(
        "PRC_Load_SupplierScorecard",
        "Rebuilds the supplier scorecard: on-time delivery, quantity accuracy, price adherence, "
        "quality rejections and invoice accuracy, weighted into a single score band. The weights "
        "live in etl.Configuration because procurement changes them every couple of years, and "
        "suppliers with too few orders in the window are scored as 'insufficient data' rather than "
        "badly.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("ScoredSupplierCount", 0, "int"), ("InsufficientDataCount", 0, "int")],
    )
    pkg.add_parameter("ScoringWindowDays", 90, dtype="int",
                      description="Rolling window the scorecard is measured over.")
    pkg.add_parameter("MinimumOrdersForScore", 5, dtype="int",
                      description="Below this order count the supplier is not scored.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.SupplierScorecard"))
    build = pkg.add(ExecuteSql(
        "Build Scorecard Measures",
        CONN_STAGING,
        "INSERT INTO work.SupplierScorecard\n"
        "    (SupplierId, RegionCode, OrderCount, OnTimeCount, QuantityAccurateCount,\n"
        "     PriceAdherentCount, QualityRejectCount, InvoiceExceptionCount, WindowDays)\n"
        "SELECT  s.SupplierId\n"
        ",       s.RegionCode\n"
        ",       COUNT(DISTINCT r.PurchaseOrderNumber)\n"
        ",       SUM(CASE WHEN r.ReceivedAtUtc <= pol.PromisedDate THEN 1 ELSE 0 END)\n"
        ",       SUM(CASE WHEN r.ReceivedOuters = pol.OrderedOuters THEN 1 ELSE 0 END)\n"
        ",       SUM(CASE WHEN ABS(ISNULL(ail.InvoicedUnitPrice, pol.ExpectedUnitPricePerOuter)\n"
        "                          - pol.ExpectedUnitPricePerOuter) < 0.005 THEN 1 ELSE 0 END)\n"
        ",       SUM(CASE WHEN r.QualityStatusCode = N'REJECT' THEN 1 ELSE 0 END)\n"
        ",       SUM(CASE WHEN ail.ApInvoiceNumber IS NULL THEN 1 ELSE 0 END)\n"
        ",       ?\n"
        "FROM    stg.Supplier AS s\n"
        "INNER JOIN stg.PurchaseOrderHeader AS poh ON poh.SupplierId = s.SupplierId\n"
        "INNER JOIN stg.PurchaseOrderLine AS pol\n"
        "        ON  pol.PurchaseOrderNumber = poh.PurchaseOrderNumber\n"
        "LEFT OUTER JOIN stg.Receipt AS r\n"
        "        ON  r.PurchaseOrderNumber = pol.PurchaseOrderNumber\n"
        "        AND r.LineNumber = pol.LineNumber\n"
        "LEFT OUTER JOIN stg.ApInvoiceLine AS ail\n"
        "        ON  ail.PurchaseOrderNumber = pol.PurchaseOrderNumber\n"
        "        AND ail.PurchaseOrderLineNumber = pol.LineNumber\n"
        "WHERE   poh.OrderDate >= DATEADD(DAY, -?, CAST(SYSDATETIME() AS date))\n"
        "GROUP BY s.SupplierId, s.RegionCode;",
        parameter_bindings=[
            ("$Package::ScoringWindowDays", 0, "LONG"),
            ("$Package::ScoringWindowDays", 1, "LONG"),
        ],
    ))

    columns = [
        int_col("SupplierId"), str_col("RegionCode", 4), int_col("OrderCount"),
        int_col("OnTimeCount"), int_col("QuantityAccurateCount"), int_col("PriceAdherentCount"),
        int_col("QualityRejectCount"), int_col("InvoiceExceptionCount"), int_col("WindowDays"),
    ]
    flow = DataFlow("Score Suppliers")
    flow.oledb_source(
        "work SupplierScorecard", CONN_STAGING,
        "SELECT SupplierId, RegionCode, OrderCount, OnTimeCount, QuantityAccurateCount, "
        "PriceAdherentCount, QualityRejectCount, InvoiceExceptionCount, WindowDays "
        "FROM work.SupplierScorecard;", columns, timeout=900)
    flow.lookup("Lookup Scoring Weights", CONN_STAGING,
                "SELECT RegionCode, OnTimeWeight, AccuracyWeight, PriceWeight, QualityWeight "
                "FROM etl.SupplierScoringWeight;",
                ["RegionCode"],
                [money_col("OnTimeWeight"), money_col("AccuracyWeight"),
                 money_col("PriceWeight"), money_col("QualityWeight")], no_match="IG")
    flow.derived_column("Derive Component Scores", [
        ("OnTimePercent",
         "OrderCount == 0 ? (DT_NUMERIC,18,2)0 : OnTimeCount * 100 / OrderCount",
         money_col("OnTimePercent")),
        ("AccuracyPercent",
         "OrderCount == 0 ? (DT_NUMERIC,18,2)0 : QuantityAccurateCount * 100 / OrderCount",
         money_col("AccuracyPercent")),
        ("PricePercent",
         "OrderCount == 0 ? (DT_NUMERIC,18,2)0 : PriceAdherentCount * 100 / OrderCount",
         money_col("PricePercent")),
        ("QualityPercent",
         "OrderCount == 0 ? (DT_NUMERIC,18,2)0 : "
         "(OrderCount - QualityRejectCount) * 100 / OrderCount", money_col("QualityPercent")),
    ])
    flow.derived_column("Derive Weighted Score", [
        ("SupplierScore",
         "(OnTimePercent * ISNULL(OnTimeWeight) ? 0.4 : OnTimeWeight) + "
         "(AccuracyPercent * (ISNULL(AccuracyWeight) ? 0.2 : AccuracyWeight)) + "
         "(PricePercent * (ISNULL(PriceWeight) ? 0.2 : PriceWeight)) + "
         "(QualityPercent * (ISNULL(QualityWeight) ? 0.2 : QualityWeight))",
         money_col("SupplierScore")),
        ("ScoreBandCode",
         'OrderCount < @[$Package::MinimumOrdersForScore] ? (DT_WSTR,8)"NODATA" : '
         '(OnTimePercent >= 95 ? (DT_WSTR,8)"A" : (OnTimePercent >= 85 ? (DT_WSTR,8)"B" : '
         '(OnTimePercent >= 70 ? (DT_WSTR,8)"C" : (DT_WSTR,8)"D")))',
         str_col("ScoreBandCode", 8)),
    ])
    flow.row_count("Count Scored Suppliers", "User::RowsInserted")
    flow.oledb_destination("Aggregate Supplier Performance", CONN_DW,
                           "[Aggregate].[Supplier Performance]", batch_size=20000)
    score = pkg.add(DataFlowTask(flow))

    insufficient = pkg.add(ExecuteSql(
        "Count Unscored Suppliers",
        CONN_STAGING,
        "SELECT COUNT(*) AS InsufficientData FROM work.SupplierScorecard WHERE OrderCount < ?;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::MinimumOrdersForScore", 0, "LONG")],
        result_bindings=[("0", "User::InsufficientDataCount")],
    ))
    counts = pkg.add(log_row_count("Aggregate.Supplier Performance"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, build)
    pkg.link(build, score)
    pkg.link(score, insufficient, value="Completion")
    pkg.link(insufficient, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# PRC_Load_ContractCompliance
# ---------------------------------------------------------------------------


def build_prc_load_contractcompliance():
    pkg = new_package(
        "PRC_Load_ContractCompliance",
        "Detects off-contract spend and contract leakage. Every purchase line is tested against "
        "the contract that should have covered it: no contract at all, a contract that had expired "
        "on the order date, a price above the contracted price, or an order placed with a "
        "non-preferred supplier while a preferred contract existed for the same category.",
        source_system="ORAERP",
        connections=(CONN_DW,),
        extra_variables=[("LeakageAmount", 0, "decimal"), ("ExpiredContractCount", 0, "int")],
    )
    pkg.add_parameter("ComplianceWindowDays", 30, dtype="int",
                      description="Rolling window of orders assessed for compliance.")
    pkg.add_parameter("PriceLeakageTolerance", 2, dtype="int",
                      description="Percent above contract price treated as compliant rounding.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.ContractCompliance"))
    evaluate = pkg.add(ExecuteSql(
        "Evaluate Contract Compliance",
        CONN_STAGING,
        "INSERT INTO work.ContractCompliance\n"
        "    (PurchaseOrderNumber, LineNumber, SupplierId, CategoryCode, OrderDate, OrderedAmount,\n"
        "     ContractNumber, ContractPricePerOuter, ComplianceStatusCode, LeakageAmount)\n"
        "SELECT  pol.PurchaseOrderNumber\n"
        ",       pol.LineNumber\n"
        ",       poh.SupplierId\n"
        ",       pol.CategoryCode\n"
        ",       poh.OrderDate\n"
        ",       pol.OrderedOuters * pol.ExpectedUnitPricePerOuter\n"
        ",       vc.ContractNumber\n"
        ",       vc.ContractPricePerOuter\n"
        ",       CASE\n"
        "            WHEN vc.ContractNumber IS NULL\n"
        "                 AND EXISTS (SELECT 1 FROM stg.VendorContract AS alt\n"
        "                             WHERE alt.CategoryCode = pol.CategoryCode\n"
        "                               AND alt.IsPreferred = 1\n"
        "                               AND poh.OrderDate BETWEEN alt.ContractStartDate\n"
        "                                   AND ISNULL(alt.ContractEndDate, '9999-12-31'))\n"
        "                THEN N'NON_PREFERRED'\n"
        "            WHEN vc.ContractNumber IS NULL THEN N'NO_CONTRACT'\n"
        "            WHEN poh.OrderDate > ISNULL(vc.ContractEndDate, '9999-12-31')\n"
        "                THEN N'EXPIRED_CONTRACT'\n"
        "            WHEN pol.ExpectedUnitPricePerOuter >\n"
        "                 vc.ContractPricePerOuter * (1 + (? / 100.0))\n"
        "                THEN N'PRICE_LEAKAGE'\n"
        "            ELSE N'COMPLIANT'\n"
        "        END\n"
        ",       CASE WHEN vc.ContractPricePerOuter IS NULL THEN 0\n"
        "             ELSE CASE WHEN pol.ExpectedUnitPricePerOuter > vc.ContractPricePerOuter\n"
        "                       THEN (pol.ExpectedUnitPricePerOuter - vc.ContractPricePerOuter)\n"
        "                            * pol.OrderedOuters\n"
        "                       ELSE 0 END\n"
        "        END\n"
        "FROM    stg.PurchaseOrderLine AS pol\n"
        "INNER JOIN stg.PurchaseOrderHeader AS poh\n"
        "        ON  poh.PurchaseOrderNumber = pol.PurchaseOrderNumber\n"
        "LEFT OUTER JOIN stg.VendorContract AS vc\n"
        "        ON  vc.SupplierId = poh.SupplierId\n"
        "        AND vc.CategoryCode = pol.CategoryCode\n"
        "WHERE   poh.OrderDate >= DATEADD(DAY, -?, CAST(SYSDATETIME() AS date));",
        parameter_bindings=[
            ("$Package::PriceLeakageTolerance", 0, "LONG"),
            ("$Package::ComplianceWindowDays", 1, "LONG"),
        ],
    ))
    leakage = pkg.add(ExecuteSql(
        "Measure Leakage",
        CONN_STAGING,
        "SELECT ISNULL(SUM(LeakageAmount), 0) AS Leakage, "
        "       SUM(CASE WHEN ComplianceStatusCode = N'EXPIRED_CONTRACT' THEN 1 ELSE 0 END) "
        "           AS ExpiredContracts "
        "FROM work.ContractCompliance;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::LeakageAmount"), ("1", "User::ExpiredContractCount")],
    ))
    publish = pkg.add(ExecuteSql(
        "Publish Compliance To Scorecard",
        CONN_DW,
        "UPDATE sp\n"
        "SET    sp.[Off Contract Spend Amount] = c.OffContractAmount\n"
        ",      sp.[Leakage Amount] = c.LeakageAmount\n"
        ",      sp.[Compliance Percent] = CASE WHEN c.TotalAmount = 0 THEN 100\n"
        "            ELSE (c.TotalAmount - c.OffContractAmount) * 100 / c.TotalAmount END\n"
        "FROM   Aggregate.[Supplier Performance] AS sp\n"
        "INNER JOIN (SELECT SupplierId,\n"
        "                   SUM(OrderedAmount) AS TotalAmount,\n"
        "                   SUM(CASE WHEN ComplianceStatusCode <> N'COMPLIANT'\n"
        "                            THEN OrderedAmount ELSE 0 END) AS OffContractAmount,\n"
        "                   SUM(LeakageAmount) AS LeakageAmount\n"
        "            FROM   work.ContractCompliance\n"
        "            GROUP BY SupplierId) AS c\n"
        "        ON c.SupplierId = sp.[WWI Supplier ID];",
    ))
    raise_rows = pkg.add(ExecuteSql(
        "Raise Non Compliant Lines",
        CONN_STAGING,
        "INSERT INTO etl.RejectedRecord "
        "    (BatchId, ObjectName, RejectStage, RejectReasonCode, RejectReasonDescription, "
        "     SourceKey, RejectedAtUtc, IsReprocessed) "
        "SELECT ?, N'stg.VendorContract', N'Fact', ComplianceStatusCode, "
        "       N'Purchase line failed contract compliance', "
        "       CONCAT(PurchaseOrderNumber, N'|', CAST(LineNumber AS nvarchar(10))), "
        "       SYSUTCDATETIME(), 0 "
        "FROM   work.ContractCompliance WHERE ComplianceStatusCode <> N'COMPLIANT';",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("Aggregate.Supplier Performance"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, evaluate)
    pkg.link(evaluate, leakage)
    pkg.link(leakage, publish)
    pkg.link(publish, raise_rows, expression="@[User::LeakageAmount] > 0")
    pkg.link(publish, counts, expression="@[User::LeakageAmount] == 0")
    pkg.link(raise_rows, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# PRC_Export_SupplierStatement
# ---------------------------------------------------------------------------


def build_prc_export_supplierstatement():
    pkg = new_package(
        "PRC_Export_SupplierStatement",
        "Produces the monthly supplier statement extract from the supplier transaction fact: "
        "opening balance, invoices, credits, payments and closing balance per supplier. Suppliers "
        "on self-billing terms are excluded, and EU suppliers receive a VAT summary block that the "
        "NA and APAC statements do not carry.",
        source_system="ORAERP",
        connections=(CONN_DW, CONN_FILES),
        extra_variables=[("StatementCount", 0, "int"), ("StatementFileName", "supplier_statement.csv", "string")],
    )
    pkg.add_parameter("StatementPeriod", "1900-01", dtype="string",
                      description="Statement period, YYYY-MM.")
    pkg.add_parameter("ExcludeSelfBilling", "True", dtype="bool",
                      description="Self-billing suppliers do not receive a statement.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.SupplierStatementLine"))
    build = pkg.add(ExecuteSql(
        "Build Statement Lines",
        CONN_DW,
        "INSERT INTO work.SupplierStatementLine\n"
        "    (SupplierId, SupplierName, RegionCode, StatementPeriod, TransactionTypeCode,\n"
        "     TransactionDate, TransactionReference, TransactionAmount, CurrencyCode, VatAmount)\n"
        "SELECT  s.[WWI Supplier ID]\n"
        ",       s.[Supplier Name]\n"
        ",       s.[Region Code]\n"
        ",       ?\n"
        ",       t.[Transaction Type Code]\n"
        ",       t.[Transaction Date]\n"
        ",       t.[Transaction Reference]\n"
        ",       t.[Transaction Amount]\n"
        ",       t.[Currency Code]\n"
        ",       CASE WHEN s.[Region Code] = N'EU' THEN t.[Tax Amount] ELSE NULL END\n"
        "FROM    Fact.[Supplier Transaction] AS t\n"
        "INNER JOIN Dimension.Supplier AS s ON s.[Supplier Key] = t.[Supplier Key]\n"
        "WHERE   CONVERT(char(7), t.[Transaction Date], 126) = ?\n"
        "AND     (? = 0 OR ISNULL(s.[Is Self Billing], 0) = 0);",
        parameter_bindings=[
            ("$Package::StatementPeriod", 0, "NVARCHAR"),
            ("$Package::StatementPeriod", 1, "NVARCHAR"),
            ("$Package::ExcludeSelfBilling", 2, "BYTE"),
        ],
    ))
    balances = pkg.add(ExecuteSql(
        "Compute Statement Balances",
        CONN_STAGING,
        "UPDATE l\n"
        "SET    l.RunningBalance = b.RunningBalance\n"
        "FROM   work.SupplierStatementLine AS l\n"
        "INNER JOIN (SELECT SupplierStatementLineId,\n"
        "                   SUM(TransactionAmount) OVER (PARTITION BY SupplierId\n"
        "                                                ORDER BY TransactionDate,\n"
        "                                                         SupplierStatementLineId\n"
        "                                                ROWS UNBOUNDED PRECEDING) AS RunningBalance\n"
        "            FROM   work.SupplierStatementLine) AS b\n"
        "        ON b.SupplierStatementLineId = l.SupplierStatementLineId;",
    ))
    name_file = pkg.add(Expression(
        "Build Statement File Name",
        '@[User::StatementFileName] = "supplier_statement_" + '
        '@[$Package::StatementPeriod] + ".csv"',
    ))

    columns = [
        int_col("SupplierId"), str_col("SupplierName", 100), str_col("RegionCode", 4),
        str_col("StatementPeriod", 7), str_col("TransactionTypeCode", 10),
        date_col("TransactionDate"), str_col("TransactionReference", 30),
        money_col("TransactionAmount"), str_col("CurrencyCode", 3), money_col("VatAmount"),
        money_col("RunningBalance"),
    ]
    flow = DataFlow("Write Supplier Statement")
    flow.oledb_source(
        "work SupplierStatementLine", CONN_STAGING,
        "SELECT SupplierId, SupplierName, RegionCode, StatementPeriod, TransactionTypeCode, "
        "TransactionDate, TransactionReference, TransactionAmount, CurrencyCode, VatAmount, "
        "RunningBalance FROM work.SupplierStatementLine "
        "ORDER BY SupplierId, TransactionDate;", columns, timeout=900)
    flow.derived_column("Format Statement Row", [
        ("StatementLineText",
         'RIGHT("0000000000" + (DT_WSTR,10)SupplierId, 10) + '
         '(DT_WSTR,10)TransactionTypeCode + (DT_WSTR,30)TransactionReference',
         str_col("StatementLineText", 60)),
        ("IncludesVatBlock", 'RegionCode == "EU" ? (DT_BOOL)1 : (DT_BOOL)0', bool_col("IncludesVatBlock")),
    ])
    flow.row_count("Count Statement Rows", "User::RowsRead")
    flow.oledb_destination("work SupplierStatementArchive", CONN_STAGING,
                           "[work].[SupplierStatementArchive]", batch_size=20000)
    export = pkg.add(DataFlowTask(flow))

    count_statements = pkg.add(ExecuteSql(
        "Count Statements",
        CONN_STAGING,
        "SELECT COUNT(DISTINCT SupplierId) AS Statements FROM work.SupplierStatementLine;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::StatementCount")],
    ))
    counts = pkg.add(log_row_count("file:supplier_statement.csv"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, build)
    pkg.link(build, balances)
    pkg.link(balances, name_file)
    pkg.link(name_file, export)
    pkg.link(export, count_statements)
    pkg.link(count_statements, counts)
    pkg.link(counts, done)
    return pkg


BUILDERS = [
    build_prc_load_purchasespend,
    build_prc_load_receiptmatching,
    build_prc_load_supplierscorecard,
    build_prc_load_contractcompliance,
    build_prc_export_supplierstatement,
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
