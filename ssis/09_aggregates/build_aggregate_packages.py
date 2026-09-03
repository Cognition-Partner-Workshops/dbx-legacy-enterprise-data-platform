#!/usr/bin/env python3
"""Emit the WWI_Aggregates SSIS project (ssis/09_aggregates).

Thirteen summary refreshes. Each one takes its refresh window from package
parameters that the master workflow supplies, clears only that window from the
target table, rebuilds it from the star schema and reconciles the rebuilt total
back to the underlying fact through etl.usp_AssertRowCountReconciliation.

The daily summaries are re-runnable for a single date, the month-end summaries
for a single accounting period, and AGG_Publish_ReportingLayer refreshes the
Report.* layer the business reads with the dynamic SQL the reporting team wrote
in 2011 and nobody has dared replace.

Run from the repository root:

    python3 ssis/09_aggregates/build_aggregate_packages.py
"""

from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
OUT_DIR = os.path.join(REPO_ROOT, "ssis", "09_aggregates")
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import (  # noqa: E402
    CONN_DW,
    CONN_STAGING,
    exec_proc,
    log_package_start,
    log_package_success,
    log_row_count,
    new_package,
    truncate,
)
from ssisgen import (  # noqa: E402
    Column,
    Container,
    DataFlow,
    DataFlowTask,
    ExecuteSql,
    Expression,
    bigint_col,
    date_col,
    int_col,
    money_col,
    str_col,
)

PROJECT_NAME = "WWI_Aggregates"
PROJECT_CONNECTIONS = ("WWI_Staging_DB", "WWI_DW_Destination_DB")

DEC = lambda name, precision=18, scale=2: Column(name, "numeric", precision=precision, scale=scale)  # noqa: E731

DAILY_WINDOW_PARAMETERS = [
    ("RefreshFromDate", "1900-01-01", "string", "First date of the refresh window."),
    ("RefreshToDate", "1900-01-01", "string", "Last date of the refresh window, inclusive."),
]

PERIOD_WINDOW_PARAMETERS = [
    ("AccountingPeriodCode", "1900-01", "string", "Accounting period (yyyy-MM) to rebuild."),
    ("RebuildPriorPeriods", 0, "int", "Additional closed periods to rebuild behind the current one."),
]


def _agg_package(name, description, extra_variables=None, extra_parameters=()):
    pkg = new_package(
        name,
        description,
        source_system="WWIDW",
        connections=(CONN_STAGING, CONN_DW),
        extra_variables=[
            ("RowsAggregated", 0, "int"),
            ("RowsDeletedForWindow", 0, "int"),
            ("ReconciliationVariance", 0, "int"),
        ] + list(extra_variables or []),
    )
    for pname, pvalue, ptype, pdesc in extra_parameters:
        pkg.add_parameter(pname, pvalue, dtype=ptype, description=pdesc)
    return pkg


def _log_rejects(object_name, name="Log Rejected Records"):
    return ExecuteSql(
        name,
        CONN_STAGING,
        "EXEC etl.usp_LogRejectedRecord @PackageExecutionId = ?, @ObjectName = N'%s', "
        "@RejectReason = N'Aggregate row failed the summary quality gate', @RejectRowCount = ?;" % object_name,
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("User::RowsRejected", 1, "LONG"),
        ],
        is_stored_procedure=True,
    )


def _reconcile(object_name, name="Assert Reconciliation"):
    return ExecuteSql(
        name,
        CONN_STAGING,
        "EXEC etl.usp_AssertRowCountReconciliation @PackageExecutionId = ?, @ObjectName = N'%s', "
        "@ExpectedRowCount = ?, @ActualRowCount = ?;" % object_name,
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("User::RowsRead", 1, "LONG"),
            ("User::RowsInserted", 2, "LONG"),
        ],
        is_stored_procedure=True,
    )


def _init_daily_window():
    return Expression(
        "Init Refresh Window",
        '@[User::WatermarkFrom] = (DT_DBTIMESTAMP)@[$Package::RefreshFromDate]',
    )


def _init_period_window():
    return Expression(
        "Init Refresh Period",
        '@[User::WatermarkFrom] = (DT_DBTIMESTAMP)(@[$Package::AccountingPeriodCode] + "-01")',
    )


def _delete_daily_window(table, date_key_column, name="Delete Refresh Window"):
    return ExecuteSql(
        name,
        CONN_DW,
        "DELETE a FROM %s AS a "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = a.[%s] "
        "WHERE d.[Date] >= CAST(? AS date) AND d.[Date] <= CAST(? AS date);" % (table, date_key_column),
        parameter_bindings=[
            ("$Package::RefreshFromDate", 0, "NVARCHAR"),
            ("$Package::RefreshToDate", 1, "NVARCHAR"),
        ],
    )


def _delete_period_window(table, name="Delete Period Rows"):
    return ExecuteSql(
        name,
        CONN_DW,
        "DELETE FROM %s WHERE [Accounting Period Code] >= "
        "FORMAT(DATEADD(MONTH, -1 * ?, CAST(? + '-01' AS date)), 'yyyy-MM') "
        "AND [Accounting Period Code] <= ?;" % table,
        parameter_bindings=[
            ("$Package::RebuildPriorPeriods", 0, "LONG"),
            ("$Package::AccountingPeriodCode", 1, "NVARCHAR"),
            ("$Package::AccountingPeriodCode", 2, "NVARCHAR"),
        ],
    )


def _write(pkg):
    return pkg.write(os.path.join(OUT_DIR, pkg.name + ".dtsx"))


# ---------------------------------------------------------------------------
# Daily summaries
# ---------------------------------------------------------------------------


def build_agg_refresh_daily_sales_summary():
    pkg = _agg_package(
        "AGG_Refresh_DailySalesSummary",
        "Rebuilds Aggregate.Daily Sales Summary for the requested date window at "
        "date/region/channel grain. Only the window is deleted and rebuilt, so a late correction "
        "to a prior day can be republished without touching the rest of the table.",
        extra_variables=[("RowsSuppressed", 0, "int")],
        extra_parameters=DAILY_WINDOW_PARAMETERS,
    )
    columns = [
        int_col("InvoiceDateKey"),
        str_col("RegionCode", 6),
        str_col("SalesChannelCode", 12),
        int_col("CustomerCount"),
        int_col("InvoiceLineCount"),
        money_col("GrossAmount"),
        money_col("DiscountAmount"),
        money_col("NetAmount"),
        money_col("TaxAmount"),
        money_col("MarginAmount"),
    ]
    flow = DataFlow("Rebuild Daily Sales Summary", "Date / region / channel sales rollup")
    flow.oledb_source(
        "Fact Sale Window",
        CONN_DW,
        "SELECT f.[Invoice Date Key] AS InvoiceDateKey, c.[Region Code] AS RegionCode, "
        "ISNULL(f.[Sales Channel Code], N'DIRECT') AS SalesChannelCode, "
        "COUNT(DISTINCT f.[Customer Key]) AS CustomerCount, COUNT_BIG(*) AS InvoiceLineCount, "
        "SUM(f.[Gross Amount]) AS GrossAmount, SUM(f.[Discount Amount]) AS DiscountAmount, "
        "SUM(f.[Net Amount]) AS NetAmount, SUM(f.[Tax Amount]) AS TaxAmount, "
        "SUM(f.[Margin Amount]) AS MarginAmount "
        "FROM [Fact].[Sale] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = f.[Invoice Date Key] "
        "INNER JOIN Dimension.Customer AS c ON c.[Customer Key] = f.[Customer Key] "
        "WHERE d.[Date] >= CAST(? AS date) AND d.[Date] <= CAST(? AS date) AND f.[Is Reversal] = 0 "
        "GROUP BY f.[Invoice Date Key], c.[Region Code], ISNULL(f.[Sales Channel Code], N'DIRECT');",
        columns,
        timeout=3600,
    )
    flow.derived_column(
        "Derive Summary Ratios",
        [
            ("MarginPercent",
             "NetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)MarginAmount / (DT_NUMERIC,9,4)NetAmount) * 100",
             DEC("MarginPercent", 9, 4)),
            ("AverageLineValue",
             "InvoiceLineCount == 0 ? (DT_NUMERIC,18,2)0 : "
             "((DT_NUMERIC,18,2)NetAmount / (DT_NUMERIC,18,2)InvoiceLineCount)",
             money_col("AverageLineValue")),
            ("TaxRegimeCode",
             'RegionCode == "NA" ? "SALESTAX" : RegionCode == "EU" ? "VAT" : "GST"',
             str_col("TaxRegimeCode", 10)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Summary Rows",
        [
            ("Suppressed Small Cells", "CustomerCount < 3"),
            ("Loss Making Days", "MarginAmount < 0"),
        ],
        default_output="Publishable Rows",
    )
    flow.branch_destination("Insert Publishable Rows", CONN_DW, "[Aggregate].[Daily Sales Summary]",
                            "Route Summary Rows", "Publishable Rows")
    flow.branch_destination("Insert Loss Making Days", CONN_DW, "[Aggregate].[Daily Sales Summary]",
                            "Route Summary Rows", "Loss Making Days")
    flow.branch_destination("Reject Suppressed Cells", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Summary Rows", "Suppressed Small Cells")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Fact Sale Window")

    init = pkg.add(_init_daily_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_daily_window("[Aggregate].[Daily Sales Summary]", "Invoice Date Key"))
    build = pkg.add(DataFlowTask(flow))
    refresh = pkg.add(
        exec_proc(
            "Refresh Derived Sales Measures",
            "EXEC Integration.RefreshAggregateDailySales @RefreshFromDate = ?, @RefreshToDate = ?, "
            "@LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::RefreshFromDate", 0, "NVARCHAR"),
                ("$Package::RefreshToDate", 1, "NVARCHAR"),
                ("User::PackageExecutionId", 2, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Daily Sales Summary"))
    rejects = pkg.add(_log_rejects("Aggregate.Daily Sales Summary"))
    counts = pkg.add(log_row_count("Aggregate.Daily Sales Summary"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_daily_inventory_health():
    pkg = _agg_package(
        "AGG_Refresh_DailyInventoryHealth",
        "Rebuilds Aggregate.Daily Inventory Health for the requested window from the daily "
        "inventory snapshot: cover bands, stock-out counts, aged-stock percentage and the "
        "obsolescence provision per warehouse.",
        extra_variables=[("StockOutRows", 0, "int")],
        extra_parameters=DAILY_WINDOW_PARAMETERS + [
            ("StockOutThreshold", 0, "int", "Quantity at or below which an item counts as out of stock."),
        ],
    )
    columns = [
        int_col("PositionDateKey"),
        str_col("WarehouseCode", 12),
        str_col("ProductCategoryCode", 12),
        int_col("SkuCount"),
        int_col("StockOutSkuCount"),
        int_col("BelowReorderSkuCount"),
        money_col("StockValueAmount"),
        money_col("ProvisionAmount"),
        DEC("AverageDaysOfCover", 9, 2),
    ]
    flow = DataFlow("Rebuild Inventory Health", "Warehouse / category inventory health")
    flow.oledb_source(
        "Fact Daily Inventory Snapshot Window",
        CONN_DW,
        "SELECT f.[Position Date Key] AS PositionDateKey, f.[Warehouse Code] AS WarehouseCode, "
        "si.[Product Category Code] AS ProductCategoryCode, COUNT_BIG(*) AS SkuCount, "
        "SUM(CASE WHEN f.[Quantity On Hand] <= ? THEN 1 ELSE 0 END) AS StockOutSkuCount, "
        "SUM(CASE WHEN f.[Cover Band Code] IN (N'TIGHT', N'CRITICAL') THEN 1 ELSE 0 END) "
        "    AS BelowReorderSkuCount, "
        "SUM(f.[Net Stock Value Amount]) AS StockValueAmount, "
        "SUM(f.[Obsolescence Provision Amount]) AS ProvisionAmount, "
        "AVG(CAST(f.[Days Of Cover] AS decimal(9,2))) AS AverageDaysOfCover "
        "FROM [Fact].[Daily Inventory Snapshot] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = f.[Position Date Key] "
        "INNER JOIN [Dimension].[Stock Item] AS si ON si.[Stock Item Key] = f.[Stock Item Key] "
        "WHERE d.[Date] >= CAST(? AS date) AND d.[Date] <= CAST(? AS date) "
        "GROUP BY f.[Position Date Key], f.[Warehouse Code], si.[Product Category Code];",
        columns,
        timeout=3600,
    )
    flow.derived_column(
        "Derive Health Indicators",
        [
            ("StockOutPercent",
             "SkuCount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)StockOutSkuCount / (DT_NUMERIC,9,4)SkuCount) * 100",
             DEC("StockOutPercent", 9, 4)),
            ("ProvisionPercent",
             "StockValueAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)ProvisionAmount / (DT_NUMERIC,9,4)StockValueAmount) * 100",
             DEC("ProvisionPercent", 9, 4)),
            ("HealthRatingCode",
             "((DT_NUMERIC,9,4)StockOutSkuCount) > 0 && AverageDaysOfCover < 7 ? \"RED\" : "
             "AverageDaysOfCover < 14 ? \"AMBER\" : AverageDaysOfCover > 120 ? \"OVERSTOCK\" : \"GREEN\"",
             str_col("HealthRatingCode", 10)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Health Rows",
        [
            ("Empty Warehouses", "SkuCount == 0"),
            ("Red Rated", 'HealthRatingCode == "RED"'),
        ],
        default_output="Standard Rows",
    )
    flow.branch_destination("Insert Standard Rows", CONN_DW, "[Aggregate].[Daily Inventory Health]",
                            "Route Health Rows", "Standard Rows")
    flow.branch_destination("Insert Red Rated Rows", CONN_DW, "[Aggregate].[Daily Inventory Health]",
                            "Route Health Rows", "Red Rated")
    flow.branch_destination("Reject Empty Warehouses", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Health Rows", "Empty Warehouses")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Fact Daily Inventory Snapshot Window")

    init = pkg.add(_init_daily_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_daily_window("[Aggregate].[Daily Inventory Health]", "Position Date Key"))
    build = pkg.add(DataFlowTask(flow))
    refresh = pkg.add(
        exec_proc(
            "Refresh Inventory Health Bands",
            "EXEC Integration.RefreshAggregateInventoryHealth @RefreshFromDate = ?, @RefreshToDate = ?, "
            "@LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::RefreshFromDate", 0, "NVARCHAR"),
                ("$Package::RefreshToDate", 1, "NVARCHAR"),
                ("User::PackageExecutionId", 2, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Daily Inventory Health"))
    rejects = pkg.add(_log_rejects("Aggregate.Daily Inventory Health"))
    counts = pkg.add(log_row_count("Aggregate.Daily Inventory Health"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_delivery_performance_summary():
    pkg = _agg_package(
        "AGG_Refresh_DeliveryPerformanceSummary",
        "Rebuilds Aggregate.Delivery Performance Summary per carrier, lane and week from the "
        "shipment accumulating snapshot. Shipments still in flight are excluded so the on-time "
        "percentage is not distorted by open rows.",
        extra_variables=[("InFlightExcluded", 0, "int")],
        extra_parameters=DAILY_WINDOW_PARAMETERS + [
            ("OnTimeGraceHours", 12, "int", "Grace period before a delivery is treated as late."),
        ],
    )
    columns = [
        int_col("DeliveredDateKey"),
        str_col("CarrierCode", 8),
        str_col("OriginCountryIsoCode", 2),
        str_col("DestinationCountryIsoCode", 2),
        int_col("ShipmentCount"),
        int_col("OnTimeShipmentCount"),
        DEC("AverageTransitDays", 9, 2),
        DEC("AverageCustomsHoldDays", 9, 2),
        money_col("FreightChargeAmount"),
        DEC("TotalWeightKg", 14, 3),
    ]
    flow = DataFlow("Rebuild Delivery Performance", "Carrier / lane delivery performance")
    flow.oledb_source(
        "Fact Shipment Window",
        CONN_DW,
        "SELECT f.[Delivered Date Key] AS DeliveredDateKey, f.[Carrier Code] AS CarrierCode, "
        "f.[Origin Country ISO Code] AS OriginCountryIsoCode, "
        "f.[Destination Country ISO Code] AS DestinationCountryIsoCode, "
        "COUNT_BIG(*) AS ShipmentCount, "
        "SUM(CASE WHEN f.[Days Late] <= 0 THEN 1 ELSE 0 END) AS OnTimeShipmentCount, "
        "AVG(CAST(f.[Dispatch To Delivery Days] AS decimal(9,2))) AS AverageTransitDays, "
        "AVG(CAST(f.[Customs Hold Days] AS decimal(9,2))) AS AverageCustomsHoldDays, "
        "SUM(f.[Freight Charge Amount]) AS FreightChargeAmount, "
        "SUM(f.[Shipment Weight Kg]) AS TotalWeightKg "
        "FROM [Fact].[Shipment] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = f.[Delivered Date Key] "
        "WHERE d.[Date] >= CAST(? AS date) AND d.[Date] <= CAST(? AS date) "
        "  AND f.[Milestone Status Code] = N'DELIVERED' "
        "GROUP BY f.[Delivered Date Key], f.[Carrier Code], f.[Origin Country ISO Code], "
        "         f.[Destination Country ISO Code];",
        columns,
        timeout=1800,
    )
    flow.derived_column(
        "Derive Performance Ratios",
        [
            ("OnTimePercent",
             "ShipmentCount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)OnTimeShipmentCount / (DT_NUMERIC,9,4)ShipmentCount) * 100",
             DEC("OnTimePercent", 9, 4)),
            ("FreightPerKg",
             "TotalWeightKg == 0 ? (DT_NUMERIC,18,2)0 : "
             "FreightChargeAmount / (DT_NUMERIC,18,2)TotalWeightKg",
             money_col("FreightPerKg")),
            ("IsCrossBorderLane", "OriginCountryIsoCode != DestinationCountryIsoCode",
             Column("IsCrossBorderLane", "bool")),
            ("ServiceLevelBandCode",
             "ShipmentCount == 0 ? \"NODATA\" : "
             "(((DT_NUMERIC,9,4)OnTimeShipmentCount / (DT_NUMERIC,9,4)ShipmentCount) * 100) >= 98 "
             "? \"PLATINUM\" : "
             "(((DT_NUMERIC,9,4)OnTimeShipmentCount / (DT_NUMERIC,9,4)ShipmentCount) * 100) >= 95 "
             "? \"GOLD\" : "
             "(((DT_NUMERIC,9,4)OnTimeShipmentCount / (DT_NUMERIC,9,4)ShipmentCount) * 100) >= 90 "
             "? \"SILVER\" : \"REVIEW\"",
             str_col("ServiceLevelBandCode", 10)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Performance Rows",
        [
            ("Thin Sample", "ShipmentCount < 5"),
            ("Cross Border Lanes", "IsCrossBorderLane"),
        ],
        default_output="Domestic Lanes",
    )
    flow.branch_destination("Insert Domestic Lanes", CONN_DW, "[Aggregate].[Delivery Performance Summary]",
                            "Route Performance Rows", "Domestic Lanes")
    flow.branch_destination("Insert Cross Border Lanes", CONN_DW, "[Aggregate].[Delivery Performance Summary]",
                            "Route Performance Rows", "Cross Border Lanes")
    flow.branch_destination("Reject Thin Sample", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Performance Rows", "Thin Sample")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedShipment]",
                            "Fact Shipment Window")

    init = pkg.add(_init_daily_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_daily_window("[Aggregate].[Delivery Performance Summary]", "Delivered Date Key"))
    build = pkg.add(DataFlowTask(flow))
    refresh = pkg.add(
        exec_proc(
            "Refresh Carrier Scorecards",
            "EXEC Integration.RefreshAggregateDeliveryPerformance @RefreshFromDate = ?, @RefreshToDate = ?, "
            "@OnTimeGraceHours = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::RefreshFromDate", 0, "NVARCHAR"),
                ("$Package::RefreshToDate", 1, "NVARCHAR"),
                ("$Package::OnTimeGraceHours", 2, "LONG"),
                ("User::PackageExecutionId", 3, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Delivery Performance Summary"))
    rejects = pkg.add(_log_rejects("Aggregate.Delivery Performance Summary"))
    counts = pkg.add(log_row_count("Aggregate.Delivery Performance Summary"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Month-end summaries
# ---------------------------------------------------------------------------


def build_agg_refresh_monthly_sales_summary():
    pkg = _agg_package(
        "AGG_Refresh_MonthlySalesSummary",
        "Rebuilds Aggregate.Monthly Sales Summary for the closing period and, optionally, a number "
        "of prior periods. Fiscal periods differ by region - NA runs to a 4-4-5 calendar, EU to the "
        "calendar month and APAC to an April-March year - so the period label comes from the date "
        "dimension rather than being derived here.",
        extra_variables=[("PeriodsRebuilt", 0, "int")],
        extra_parameters=PERIOD_WINDOW_PARAMETERS,
    )
    columns = [
        str_col("AccountingPeriodCode", 7),
        str_col("RegionCode", 6),
        str_col("FiscalPeriodLabel", 12),
        int_col("CustomerCount"),
        int_col("InvoiceCount"),
        money_col("NetAmount"),
        money_col("TaxAmount"),
        money_col("MarginAmount"),
        money_col("NetAmountReporting"),
    ]
    flow = DataFlow("Rebuild Monthly Sales Summary", "Period / region sales rollup")
    flow.oledb_source(
        "Fact Sale Period",
        CONN_DW,
        "SELECT d.[Accounting Period Code] AS AccountingPeriodCode, c.[Region Code] AS RegionCode, "
        "d.[Fiscal Period Label] AS FiscalPeriodLabel, COUNT(DISTINCT f.[Customer Key]) AS CustomerCount, "
        "COUNT(DISTINCT f.[Invoice Number]) AS InvoiceCount, SUM(f.[Net Amount]) AS NetAmount, "
        "SUM(f.[Tax Amount]) AS TaxAmount, SUM(f.[Margin Amount]) AS MarginAmount, "
        "SUM(f.[Net Amount Reporting]) AS NetAmountReporting "
        "FROM [Fact].[Sale] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = f.[Invoice Date Key] "
        "INNER JOIN Dimension.Customer AS c ON c.[Customer Key] = f.[Customer Key] "
        "WHERE d.[Accounting Period Code] >= FORMAT(DATEADD(MONTH, -1 * ?, CAST(? + '-01' AS date)), 'yyyy-MM') "
        "  AND d.[Accounting Period Code] <= ? "
        "GROUP BY d.[Accounting Period Code], c.[Region Code], d.[Fiscal Period Label];",
        columns,
        timeout=3600,
    )
    flow.derived_column(
        "Derive Period Measures",
        [
            ("MarginPercent",
             "NetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)MarginAmount / (DT_NUMERIC,9,4)NetAmount) * 100",
             DEC("MarginPercent", 9, 4)),
            ("AverageInvoiceValue",
             "InvoiceCount == 0 ? (DT_NUMERIC,18,2)0 : "
             "((DT_NUMERIC,18,2)NetAmount / (DT_NUMERIC,18,2)InvoiceCount)",
             money_col("AverageInvoiceValue")),
            ("EffectiveTaxPercent",
             "NetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)TaxAmount / (DT_NUMERIC,9,4)NetAmount) * 100",
             DEC("EffectiveTaxPercent", 9, 4)),
            ("FiscalCalendarCode",
             'RegionCode == "NA" ? "445" : RegionCode == "EU" ? "CALENDAR" : "APRMAR"',
             str_col("FiscalCalendarCode", 10)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Period Rows",
        [
            ("Empty Periods", "NetAmount == 0"),
            ("Loss Making Periods", "MarginAmount < 0"),
        ],
        default_output="Standard Rows",
    )
    flow.branch_destination("Insert Standard Rows", CONN_DW, "[Aggregate].[Monthly Sales Summary]",
                            "Route Period Rows", "Standard Rows")
    flow.branch_destination("Insert Loss Making Periods", CONN_DW, "[Aggregate].[Monthly Sales Summary]",
                            "Route Period Rows", "Loss Making Periods")
    flow.branch_destination("Reject Empty Periods", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Period Rows", "Empty Periods")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Fact Sale Period")

    init = pkg.add(_init_period_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_period_window("[Aggregate].[Monthly Sales Summary]"))
    build = pkg.add(DataFlowTask(flow))
    refresh = pkg.add(
        exec_proc(
            "Refresh Monthly Sales Derivations",
            "EXEC Integration.RefreshAggregateMonthlySales @AccountingPeriodCode = ?, "
            "@RebuildPriorPeriods = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("$Package::RebuildPriorPeriods", 1, "LONG"),
                ("User::PackageExecutionId", 2, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Monthly Sales Summary"))
    rejects = pkg.add(_log_rejects("Aggregate.Monthly Sales Summary"))
    counts = pkg.add(log_row_count("Aggregate.Monthly Sales Summary"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_monthly_margin_analysis():
    pkg = _agg_package(
        "AGG_Refresh_MonthlyMarginAnalysis",
        "Rebuilds Aggregate.Monthly Margin Analysis at period/category/channel grain. Standard cost "
        "and actual cost are both carried because manufacturing still reports on standard, and the "
        "purchase price variance is apportioned across the categories that consumed the stock.",
        extra_variables=[("VarianceRowsPosted", 0, "int")],
        extra_parameters=PERIOD_WINDOW_PARAMETERS + [
            ("ApportionPurchaseVariance", "True", "bool", "Apportion PPV across consuming categories."),
        ],
    )
    columns = [
        str_col("AccountingPeriodCode", 7),
        str_col("ProductCategoryCode", 12),
        str_col("SalesChannelCode", 12),
        money_col("NetAmount"),
        money_col("StandardCostAmount"),
        money_col("ActualCostAmount"),
        money_col("MarginAmount"),
        int_col("QuantitySold"),
    ]
    flow = DataFlow("Rebuild Margin Analysis", "Period / category margin bridge")
    flow.oledb_source(
        "Fact Sale Margin Period",
        CONN_DW,
        "SELECT d.[Accounting Period Code] AS AccountingPeriodCode, "
        "si.[Product Category Code] AS ProductCategoryCode, "
        "ISNULL(f.[Sales Channel Code], N'DIRECT') AS SalesChannelCode, "
        "SUM(f.[Net Amount]) AS NetAmount, SUM(f.[Standard Cost Amount]) AS StandardCostAmount, "
        "SUM(f.[Total Cost Amount]) AS ActualCostAmount, SUM(f.[Margin Amount]) AS MarginAmount, "
        "SUM(f.[Quantity]) AS QuantitySold "
        "FROM [Fact].[Sale] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = f.[Invoice Date Key] "
        "INNER JOIN [Dimension].[Stock Item] AS si ON si.[Stock Item Key] = f.[Stock Item Key] "
        "WHERE d.[Accounting Period Code] >= FORMAT(DATEADD(MONTH, -1 * ?, CAST(? + '-01' AS date)), 'yyyy-MM') "
        "  AND d.[Accounting Period Code] <= ? AND f.[Is Reversal] = 0 "
        "GROUP BY d.[Accounting Period Code], si.[Product Category Code], "
        "         ISNULL(f.[Sales Channel Code], N'DIRECT');",
        columns,
        timeout=3600,
    )
    flow.derived_column(
        "Derive Margin Bridge",
        [
            ("StandardMarginAmount", "NetAmount - StandardCostAmount", money_col("StandardMarginAmount")),
            ("CostVarianceAmount", "StandardCostAmount - ActualCostAmount", money_col("CostVarianceAmount")),
            ("GrossMarginPercent",
             "NetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)MarginAmount / (DT_NUMERIC,9,4)NetAmount) * 100",
             DEC("GrossMarginPercent", 9, 4)),
            ("StandardMarginPercent",
             "NetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "(((DT_NUMERIC,9,4)NetAmount - (DT_NUMERIC,9,4)StandardCostAmount) / "
             "(DT_NUMERIC,9,4)NetAmount) * 100",
             DEC("StandardMarginPercent", 9, 4)),
            ("UnitMarginAmount",
             "QuantitySold == 0 ? (DT_NUMERIC,18,2)0 : "
             "((DT_NUMERIC,18,2)MarginAmount / (DT_NUMERIC,18,2)QuantitySold)",
             money_col("UnitMarginAmount")),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Margin Rows",
        [
            ("Missing Standard Cost", "StandardCostAmount == 0"),
            ("Negative Margin", "MarginAmount < 0"),
        ],
        default_output="Standard Rows",
    )
    flow.branch_destination("Insert Standard Rows", CONN_DW, "[Aggregate].[Monthly Margin Analysis]",
                            "Route Margin Rows", "Standard Rows")
    flow.branch_destination("Insert Negative Margin Rows", CONN_DW, "[Aggregate].[Monthly Margin Analysis]",
                            "Route Margin Rows", "Negative Margin")
    flow.branch_destination("Reject Missing Standard Cost", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Margin Rows", "Missing Standard Cost")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Fact Sale Margin Period")

    init = pkg.add(_init_period_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_period_window("[Aggregate].[Monthly Margin Analysis]"))
    build = pkg.add(DataFlowTask(flow))
    variance = pkg.add(
        exec_proc(
            "Apportion Purchase Price Variance",
            "EXEC Integration.RefreshAggregateMarginAnalysis @AccountingPeriodCode = ?, "
            "@ApportionVariance = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Monthly Margin Analysis"))
    rejects = pkg.add(_log_rejects("Aggregate.Monthly Margin Analysis"))
    counts = pkg.add(log_row_count("Aggregate.Monthly Margin Analysis"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, variance, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_customer_360():
    pkg = _agg_package(
        "AGG_Refresh_Customer360",
        "Full rebuild of Aggregate.Customer 360 - one row per customer with lifetime value, "
        "recency, AR exposure, loyalty balance and web engagement. EU customers who have exercised "
        "erasure are published with their behavioural measures only, per the 2018 retention policy.",
        extra_variables=[("ErasedCustomersMasked", 0, "int")],
        extra_parameters=[
            ("AsAtDate", "1900-01-01", "string", "Position date the profile is built as at."),
            ("IncludeInactiveCustomers", 0, "int", "Include customers with no activity in 24 months."),
        ],
    )
    columns = [
        int_col("CustomerKey"),
        str_col("RegionCode", 6),
        str_col("CustomerSegmentCode", 12),
        date_col("FirstOrderDate"),
        date_col("LastOrderDate"),
        int_col("LifetimeOrderCount"),
        money_col("LifetimeNetAmount"),
        money_col("LifetimeMarginAmount"),
        money_col("OpenArBalance"),
        int_col("LoyaltyPointBalance"),
        int_col("WebSessionCount"),
        Column("IsErased", "bool"),
    ]
    flow = DataFlow("Rebuild Customer 360", "One row per customer profile")
    flow.oledb_source(
        "Customer Profile Source",
        CONN_DW,
        "SELECT c.[Customer Key] AS CustomerKey, c.[Region Code] AS RegionCode, "
        "c.[Customer Segment Code] AS CustomerSegmentCode, "
        "MIN(d.[Date]) AS FirstOrderDate, MAX(d.[Date]) AS LastOrderDate, "
        "COUNT(DISTINCT f.[Invoice Number]) AS LifetimeOrderCount, "
        "SUM(f.[Net Amount]) AS LifetimeNetAmount, SUM(f.[Margin Amount]) AS LifetimeMarginAmount, "
        "ISNULL(MAX(ar.[Open Balance]), 0) AS OpenArBalance, "
        "ISNULL(MAX(lp.[Point Balance]), 0) AS LoyaltyPointBalance, "
        "ISNULL(MAX(ws.[Session Count]), 0) AS WebSessionCount, "
        "MAX(CAST(c.[Is Erased] AS int)) AS IsErased "
        "FROM Dimension.Customer AS c "
        "LEFT JOIN [Fact].[Sale] AS f ON f.[Customer Key] = c.[Customer Key] "
        "LEFT JOIN Dimension.Date AS d ON d.[Date Key] = f.[Invoice Date Key] "
        "LEFT JOIN [Aggregate].[Customer AR Position] AS ar ON ar.[Customer Key] = c.[Customer Key] "
        "LEFT JOIN [Aggregate].[Customer Loyalty Position] AS lp ON lp.[Customer Key] = c.[Customer Key] "
        "LEFT JOIN [Aggregate].[Customer Web Position] AS ws ON ws.[Customer Key] = c.[Customer Key] "
        "WHERE c.[Is Current Row] = 1 "
        "GROUP BY c.[Customer Key], c.[Region Code], c.[Customer Segment Code];",
        columns,
        timeout=7200,
    )
    flow.derived_column(
        "Derive Profile Measures",
        [
            ("RecencyDays", 'DATEDIFF("Day", LastOrderDate, (DT_DBDATE)@[$Package::AsAtDate])',
             int_col("RecencyDays")),
            ("TenureDays", 'DATEDIFF("Day", FirstOrderDate, (DT_DBDATE)@[$Package::AsAtDate])',
             int_col("TenureDays")),
            ("AverageOrderValue",
             "LifetimeOrderCount == 0 ? (DT_NUMERIC,18,2)0 : "
             "((DT_NUMERIC,18,2)LifetimeNetAmount / (DT_NUMERIC,18,2)LifetimeOrderCount)",
             money_col("AverageOrderValue")),
            ("LifetimeMarginPercent",
             "LifetimeNetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)LifetimeMarginAmount / (DT_NUMERIC,9,4)LifetimeNetAmount) * 100",
             DEC("LifetimeMarginPercent", 9, 4)),
            ("IsInactive",
             'DATEDIFF("Day", LastOrderDate, (DT_DBDATE)@[$Package::AsAtDate]) > 730',
             Column("IsInactive", "bool")),
            # Erased EU customers keep their behavioural aggregates but lose the
            # identifying segment attribution.
            ("PublishedSegmentCode", 'IsErased ? "REDACTED" : CustomerSegmentCode',
             str_col("PublishedSegmentCode", 12)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Profiles",
        [
            ("Erased Customers", "IsErased"),
            ("Inactive Customers", "IsInactive && @[$Package::IncludeInactiveCustomers] == 0"),
            ("No Purchase History", "LifetimeOrderCount == 0"),
        ],
        default_output="Active Customers",
    )
    flow.branch_destination("Insert Active Customers", CONN_DW, "[Aggregate].[Customer 360]",
                            "Route Profiles", "Active Customers")
    flow.branch_destination("Insert Erased Customers", CONN_DW, "[Aggregate].[Customer 360]",
                            "Route Profiles", "Erased Customers")
    flow.branch_destination("Insert No Purchase History", CONN_DW, "[Aggregate].[Customer 360]",
                            "Route Profiles", "No Purchase History")
    flow.branch_destination("Reject Inactive Customers", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Profiles", "Inactive Customers")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Customer Profile Source")

    init = pkg.add(
        Expression("Init As At Date", '@[User::WatermarkTo] = (DT_DBTIMESTAMP)@[$Package::AsAtDate]')
    )
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(truncate("[Aggregate].[Customer 360]", connection=CONN_DW,
                             name="Truncate Customer 360"))
    build = pkg.add(DataFlowTask(flow))
    refresh = pkg.add(
        exec_proc(
            "Score Customer Profiles",
            "EXEC Integration.RefreshAggregateCustomer360 @AsAtDate = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::AsAtDate", 0, "NVARCHAR"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Customer 360"))
    rejects = pkg.add(_log_rejects("Aggregate.Customer 360"))
    counts = pkg.add(log_row_count("Aggregate.Customer 360"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_customer_rolling_12_month():
    pkg = _agg_package(
        "AGG_Refresh_CustomerRolling12Month",
        "Rebuilds Aggregate.Customer Rolling 12 Month: a trailing-twelve-month window per customer "
        "and period, recalculated for the requested period and the eleven behind it because a "
        "restatement in any of them moves the whole rolling series.",
        extra_variables=[("PeriodsRecalculated", 0, "int")],
        extra_parameters=PERIOD_WINDOW_PARAMETERS + [
            ("RollingMonths", 12, "int", "Length of the rolling window."),
        ],
    )
    columns = [
        int_col("CustomerKey"),
        str_col("AccountingPeriodCode", 7),
        str_col("RegionCode", 6),
        money_col("RollingNetAmount"),
        money_col("RollingMarginAmount"),
        int_col("RollingOrderCount"),
        int_col("RollingReturnCount"),
        money_col("RollingReturnAmount"),
        money_col("PriorRollingNetAmount"),
    ]
    flow = DataFlow("Rebuild Rolling 12 Month", "Trailing twelve month customer window")
    flow.oledb_source(
        "Rolling Window Source",
        CONN_DW,
        "SELECT f.[Customer Key] AS CustomerKey, p.[Accounting Period Code] AS AccountingPeriodCode, "
        "c.[Region Code] AS RegionCode, SUM(f.[Net Amount]) AS RollingNetAmount, "
        "SUM(f.[Margin Amount]) AS RollingMarginAmount, "
        "COUNT(DISTINCT f.[Invoice Number]) AS RollingOrderCount, "
        "ISNULL(SUM(r.[Return Line Count]), 0) AS RollingReturnCount, "
        "ISNULL(SUM(r.[Return Net Amount]), 0) AS RollingReturnAmount, "
        "ISNULL(MAX(prior.[Rolling Net Amount]), 0) AS PriorRollingNetAmount "
        "FROM [Aggregate].[Period Calendar] AS p "
        "INNER JOIN Dimension.Date AS d "
        "        ON d.[Date] BETWEEN DATEADD(MONTH, -1 * ?, p.[Period End Date]) AND p.[Period End Date] "
        "INNER JOIN [Fact].[Sale] AS f ON f.[Invoice Date Key] = d.[Date Key] "
        "INNER JOIN Dimension.Customer AS c ON c.[Customer Key] = f.[Customer Key] "
        "LEFT JOIN [Aggregate].[Customer Return Position] AS r "
        "       ON r.[Customer Key] = f.[Customer Key] "
        "      AND r.[Accounting Period Code] = p.[Accounting Period Code] "
        "LEFT JOIN [Aggregate].[Customer Rolling 12 Month] AS prior "
        "       ON prior.[Customer Key] = f.[Customer Key] "
        "      AND prior.[Accounting Period Code] = FORMAT(DATEADD(MONTH, -1, p.[Period End Date]), 'yyyy-MM') "
        "WHERE p.[Accounting Period Code] <= ? "
        "  AND p.[Accounting Period Code] >= FORMAT(DATEADD(MONTH, -11, CAST(? + '-01' AS date)), 'yyyy-MM') "
        "GROUP BY f.[Customer Key], p.[Accounting Period Code], c.[Region Code];",
        columns,
        timeout=7200,
    )
    flow.derived_column(
        "Derive Rolling Trends",
        [
            ("NetReturnRatePercent",
             "RollingNetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)RollingReturnAmount / (DT_NUMERIC,9,4)RollingNetAmount) * 100",
             DEC("NetReturnRatePercent", 9, 4)),
            ("GrowthPercent",
             "PriorRollingNetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "(((DT_NUMERIC,9,4)RollingNetAmount - (DT_NUMERIC,9,4)PriorRollingNetAmount) / "
             "(DT_NUMERIC,9,4)PriorRollingNetAmount) * 100",
             DEC("GrowthPercent", 9, 4)),
            ("TrendCode",
             "PriorRollingNetAmount == 0 ? \"NEW\" : "
             "RollingNetAmount > PriorRollingNetAmount * 1.1 ? \"GROWING\" : "
             "RollingNetAmount < PriorRollingNetAmount * 0.9 ? \"DECLINING\" : \"STABLE\"",
             str_col("TrendCode", 10)),
            ("AverageMonthlyNetAmount",
             "((DT_NUMERIC,18,2)RollingNetAmount / (DT_NUMERIC,18,2)12)",
             money_col("AverageMonthlyNetAmount")),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Rolling Rows",
        [
            ("Churn Risk", 'TrendCode == "DECLINING" && NetReturnRatePercent > 10'),
            ("New Customers", 'TrendCode == "NEW"'),
        ],
        default_output="Established Customers",
    )
    flow.branch_destination("Insert Established Customers", CONN_DW,
                            "[Aggregate].[Customer Rolling 12 Month]",
                            "Route Rolling Rows", "Established Customers")
    flow.branch_destination("Insert New Customers", CONN_DW, "[Aggregate].[Customer Rolling 12 Month]",
                            "Route Rolling Rows", "New Customers")
    flow.branch_destination("Insert Churn Risk", CONN_DW, "[Aggregate].[Customer Rolling 12 Month]",
                            "Route Rolling Rows", "Churn Risk")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Rolling Window Source")

    init = pkg.add(_init_period_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(
        ExecuteSql(
            "Delete Rolling Window Periods",
            CONN_DW,
            "DELETE FROM [Aggregate].[Customer Rolling 12 Month] "
            "WHERE [Accounting Period Code] >= FORMAT(DATEADD(MONTH, -11, CAST(? + '-01' AS date)), 'yyyy-MM') "
            "  AND [Accounting Period Code] <= ?;",
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("$Package::AccountingPeriodCode", 1, "NVARCHAR"),
            ],
        )
    )
    build = pkg.add(DataFlowTask(flow))
    refresh = pkg.add(
        exec_proc(
            "Refresh Rolling Customer Scores",
            "EXEC Integration.RefreshAggregateCustomer360 @RollingRefresh = 1, @AccountingPeriodCode = ?, "
            "@RollingMonths = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("$Package::RollingMonths", 1, "LONG"),
                ("User::PackageExecutionId", 2, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Customer Rolling 12 Month"))
    rejects = pkg.add(_log_rejects("Aggregate.Customer Rolling 12 Month"))
    counts = pkg.add(log_row_count("Aggregate.Customer Rolling 12 Month"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_product_performance():
    pkg = _agg_package(
        "AGG_Refresh_ProductPerformance",
        "Rebuilds Aggregate.Product Performance for the period: sell-through, stock turn, return "
        "rate and an ABC classification recalculated from the period's revenue ranking.",
        extra_variables=[("ProductsClassified", 0, "int")],
        extra_parameters=PERIOD_WINDOW_PARAMETERS + [
            ("AbcThresholdA", 80, "int", "Cumulative revenue percentage that defines class A."),
            ("AbcThresholdB", 95, "int", "Cumulative revenue percentage that defines class B."),
        ],
    )
    columns = [
        int_col("StockItemKey"),
        str_col("AccountingPeriodCode", 7),
        str_col("ProductCategoryCode", 12),
        int_col("QuantitySold"),
        money_col("NetAmount"),
        money_col("MarginAmount"),
        int_col("QuantityReturned"),
        DEC("AverageStockOnHand", 18, 2),
        int_col("StockOutDayCount"),
    ]
    flow = DataFlow("Rebuild Product Performance", "Period product performance")
    flow.oledb_source(
        "Product Performance Source",
        CONN_DW,
        "SELECT f.[Stock Item Key] AS StockItemKey, d.[Accounting Period Code] AS AccountingPeriodCode, "
        "si.[Product Category Code] AS ProductCategoryCode, SUM(f.[Quantity]) AS QuantitySold, "
        "SUM(f.[Net Amount]) AS NetAmount, SUM(f.[Margin Amount]) AS MarginAmount, "
        "ISNULL(SUM(r.[Quantity Returned]), 0) AS QuantityReturned, "
        "ISNULL(AVG(inv.[Quantity On Hand]), 0) AS AverageStockOnHand, "
        "ISNULL(SUM(CASE WHEN inv.[Quantity On Hand] = 0 THEN 1 ELSE 0 END), 0) AS StockOutDayCount "
        "FROM [Fact].[Sale] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = f.[Invoice Date Key] "
        "INNER JOIN [Dimension].[Stock Item] AS si ON si.[Stock Item Key] = f.[Stock Item Key] "
        "LEFT JOIN [Fact].[Return] AS r ON r.[Stock Item Key] = f.[Stock Item Key] "
        "LEFT JOIN [Fact].[Daily Inventory Snapshot] AS inv "
        "       ON inv.[Stock Item Key] = f.[Stock Item Key] AND inv.[Position Date Key] = d.[Date Key] "
        "WHERE d.[Accounting Period Code] = ? "
        "GROUP BY f.[Stock Item Key], d.[Accounting Period Code], si.[Product Category Code];",
        columns,
        timeout=7200,
    )
    flow.derived_column(
        "Derive Product Ratios",
        [
            ("ReturnRatePercent",
             "QuantitySold == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)QuantityReturned / (DT_NUMERIC,9,4)QuantitySold) * 100",
             DEC("ReturnRatePercent", 9, 4)),
            ("StockTurnRatio",
             "AverageStockOnHand == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)QuantitySold / (DT_NUMERIC,9,4)AverageStockOnHand)",
             DEC("StockTurnRatio", 9, 4)),
            ("MarginPercent",
             "NetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)MarginAmount / (DT_NUMERIC,9,4)NetAmount) * 100",
             DEC("MarginPercent", 9, 4)),
            ("AvailabilityPercent",
             "((DT_NUMERIC,9,4)30 - (DT_NUMERIC,9,4)StockOutDayCount) / (DT_NUMERIC,9,4)30 * 100",
             DEC("AvailabilityPercent", 9, 4)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Product Rows",
        [
            ("Non Moving", "QuantitySold == 0"),
            ("High Return Rate", "ReturnRatePercent > 15"),
        ],
        default_output="Standard Rows",
    )
    flow.branch_destination("Insert Standard Rows", CONN_DW, "[Aggregate].[Product Performance]",
                            "Route Product Rows", "Standard Rows")
    flow.branch_destination("Insert High Return Rate", CONN_DW, "[Aggregate].[Product Performance]",
                            "Route Product Rows", "High Return Rate")
    flow.branch_destination("Reject Non Moving", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Product Rows", "Non Moving")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Product Performance Source")

    init = pkg.add(_init_period_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_period_window("[Aggregate].[Product Performance]"))
    build = pkg.add(DataFlowTask(flow))
    classify = pkg.add(
        ExecuteSql(
            "Recalculate ABC Classification",
            CONN_DW,
            "WITH ranked AS ("
            "  SELECT [Product Performance Key], "
            "         SUM([Net Amount]) OVER (PARTITION BY [Accounting Period Code] "
            "                                 ORDER BY [Net Amount] DESC "
            "                                 ROWS UNBOUNDED PRECEDING) AS RunningNet, "
            "         SUM([Net Amount]) OVER (PARTITION BY [Accounting Period Code]) AS PeriodNet "
            "  FROM [Aggregate].[Product Performance] WHERE [Accounting Period Code] = ?) "
            "UPDATE p SET p.[ABC Class Code] = "
            "  CASE WHEN r.PeriodNet = 0 THEN N'C' "
            "       WHEN (r.RunningNet / r.PeriodNet) * 100 <= ? THEN N'A' "
            "       WHEN (r.RunningNet / r.PeriodNet) * 100 <= ? THEN N'B' ELSE N'C' END "
            "FROM [Aggregate].[Product Performance] AS p "
            "INNER JOIN ranked AS r ON r.[Product Performance Key] = p.[Product Performance Key];",
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("$Package::AbcThresholdA", 1, "LONG"),
                ("$Package::AbcThresholdB", 2, "LONG"),
            ],
        )
    )
    refresh = pkg.add(
        exec_proc(
            "Refresh Product Performance Derivations",
            "EXEC Integration.RefreshAggregateProductPerformance @AccountingPeriodCode = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Product Performance"))
    rejects = pkg.add(_log_rejects("Aggregate.Product Performance"))
    counts = pkg.add(log_row_count("Aggregate.Product Performance"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, classify, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_supplier_performance():
    pkg = _agg_package(
        "AGG_Refresh_SupplierPerformance",
        "Rebuilds Aggregate.Supplier Performance for the period from purchase receipts and supplier "
        "payments: on-time-in-full, price variance, quality rejections and days-payable-outstanding "
        "per supplier.",
        extra_variables=[("SuppliersScored", 0, "int")],
        extra_parameters=PERIOD_WINDOW_PARAMETERS,
    )
    columns = [
        int_col("SupplierKey"),
        str_col("AccountingPeriodCode", 7),
        str_col("SupplierRegionCode", 6),
        int_col("ReceiptCount"),
        int_col("OnTimeReceiptCount"),
        int_col("InFullReceiptCount"),
        money_col("ReceivedCostAmount"),
        money_col("PriceVarianceAmount"),
        int_col("QualityRejectionCount"),
        DEC("AverageDaysToPay", 9, 2),
    ]
    flow = DataFlow("Rebuild Supplier Performance", "Supplier scorecard per period")
    flow.oledb_source(
        "Supplier Performance Source",
        CONN_DW,
        "SELECT f.[Supplier Key] AS SupplierKey, d.[Accounting Period Code] AS AccountingPeriodCode, "
        "s.[Region Code] AS SupplierRegionCode, COUNT_BIG(*) AS ReceiptCount, "
        "SUM(CASE WHEN f.[Order To Receipt Days] <= s.[Agreed Lead Time Days] THEN 1 ELSE 0 END) "
        "    AS OnTimeReceiptCount, "
        "SUM(CASE WHEN f.[Quantity Variance] >= 0 THEN 1 ELSE 0 END) AS InFullReceiptCount, "
        "SUM(f.[Received Cost Amount]) AS ReceivedCostAmount, "
        "SUM(f.[Price Variance Amount]) AS PriceVarianceAmount, "
        "ISNULL(SUM(q.[Rejection Count]), 0) AS QualityRejectionCount, "
        "ISNULL(AVG(CAST(sp.[Days To Settle] AS decimal(9,2))), 0) AS AverageDaysToPay "
        "FROM [Fact].[Purchase Receipt] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = f.[Goods Received Date Key] "
        "INNER JOIN [Dimension].[Supplier] AS s ON s.[Supplier Key] = f.[Supplier Key] "
        "LEFT JOIN [Aggregate].[Supplier Quality Position] AS q "
        "       ON q.[Supplier Key] = f.[Supplier Key] "
        "      AND q.[Accounting Period Code] = d.[Accounting Period Code] "
        "LEFT JOIN [Fact].[Supplier Payment] AS sp ON sp.[Supplier Key] = f.[Supplier Key] "
        "WHERE d.[Accounting Period Code] = ? AND s.[Is Current Row] = 1 "
        "GROUP BY f.[Supplier Key], d.[Accounting Period Code], s.[Region Code];",
        columns,
        timeout=3600,
    )
    flow.derived_column(
        "Derive Supplier Scores",
        [
            ("OnTimePercent",
             "ReceiptCount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)OnTimeReceiptCount / (DT_NUMERIC,9,4)ReceiptCount) * 100",
             DEC("OnTimePercent", 9, 4)),
            ("OtifPercent",
             "ReceiptCount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)InFullReceiptCount / (DT_NUMERIC,9,4)ReceiptCount) * 100",
             DEC("OtifPercent", 9, 4)),
            ("PriceVariancePercent",
             "ReceivedCostAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)PriceVarianceAmount / (DT_NUMERIC,9,4)ReceivedCostAmount) * 100",
             DEC("PriceVariancePercent", 9, 4)),
            ("QualityRejectionPercent",
             "ReceiptCount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)QualityRejectionCount / (DT_NUMERIC,9,4)ReceiptCount) * 100",
             DEC("QualityRejectionPercent", 9, 4)),
            ("SupplierRatingCode",
             "ReceiptCount == 0 ? \"NODATA\" : "
             "(((DT_NUMERIC,9,4)OnTimeReceiptCount / (DT_NUMERIC,9,4)ReceiptCount) * 100) >= 95 && "
             "QualityRejectionCount == 0 ? \"PREFERRED\" : "
             "(((DT_NUMERIC,9,4)OnTimeReceiptCount / (DT_NUMERIC,9,4)ReceiptCount) * 100) >= 85 "
             "? \"APPROVED\" : \"REVIEW\"",
             str_col("SupplierRatingCode", 12)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Supplier Rows",
        [
            ("No Receipts", "ReceiptCount == 0"),
            ("Under Review", 'SupplierRatingCode == "REVIEW"'),
        ],
        default_output="Standard Rows",
    )
    flow.branch_destination("Insert Standard Rows", CONN_DW, "[Aggregate].[Supplier Performance]",
                            "Route Supplier Rows", "Standard Rows")
    flow.branch_destination("Insert Under Review", CONN_DW, "[Aggregate].[Supplier Performance]",
                            "Route Supplier Rows", "Under Review")
    flow.branch_destination("Reject No Receipts", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Supplier Rows", "No Receipts")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Supplier Performance Source")

    init = pkg.add(_init_period_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_period_window("[Aggregate].[Supplier Performance]"))
    build = pkg.add(DataFlowTask(flow))
    refresh = pkg.add(
        exec_proc(
            "Refresh Supplier Scorecards",
            "EXEC Integration.RefreshAggregateSupplierPerformance @AccountingPeriodCode = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Supplier Performance"))
    rejects = pkg.add(_log_rejects("Aggregate.Supplier Performance"))
    counts = pkg.add(log_row_count("Aggregate.Supplier Performance"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_regional_sales_performance():
    pkg = _agg_package(
        "AGG_Refresh_RegionalSalesPerformance",
        "Rebuilds Aggregate.Regional Sales Performance per region, territory and period. The three "
        "regions are summarised in a sequence container with their own tax and currency treatment, "
        "because the 2009 attempt to merge them into one query produced numbers finance refused to "
        "sign off.",
        extra_variables=[("RegionsProcessed", 0, "int")],
        extra_parameters=PERIOD_WINDOW_PARAMETERS + [
            ("ReportingCurrency", "USD", "string", "Group reporting currency."),
        ],
    )
    columns = [
        str_col("AccountingPeriodCode", 7),
        str_col("RegionCode", 6),
        int_col("SalesTerritoryKey"),
        int_col("SalespersonCount"),
        money_col("NetAmount"),
        money_col("TaxAmount"),
        money_col("MarginAmount"),
        money_col("NetAmountReporting"),
        money_col("QuotaAmount"),
    ]
    flow = DataFlow("Rebuild Regional Sales Performance", "Region / territory performance")
    flow.oledb_source(
        "Regional Sales Source",
        CONN_DW,
        "SELECT d.[Accounting Period Code] AS AccountingPeriodCode, c.[Region Code] AS RegionCode, "
        "t.[Sales Territory Key] AS SalesTerritoryKey, "
        "COUNT(DISTINCT f.[Salesperson Key]) AS SalespersonCount, SUM(f.[Net Amount]) AS NetAmount, "
        "SUM(f.[Tax Amount]) AS TaxAmount, SUM(f.[Margin Amount]) AS MarginAmount, "
        "SUM(f.[Net Amount Reporting]) AS NetAmountReporting, "
        "ISNULL(MAX(q.[Quota Amount]), 0) AS QuotaAmount "
        "FROM [Fact].[Sale] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = f.[Invoice Date Key] "
        "INNER JOIN Dimension.Customer AS c ON c.[Customer Key] = f.[Customer Key] "
        "INNER JOIN [Dimension].[Sales Territory] AS t ON t.[Sales Territory Key] = f.[Sales Territory Key] "
        "LEFT JOIN [Aggregate].[Territory Quota] AS q "
        "       ON q.[Sales Territory Key] = t.[Sales Territory Key] "
        "      AND q.[Accounting Period Code] = d.[Accounting Period Code] "
        "WHERE d.[Accounting Period Code] = ? "
        "GROUP BY d.[Accounting Period Code], c.[Region Code], t.[Sales Territory Key];",
        columns,
        timeout=3600,
    )
    flow.derived_column(
        "Derive Regional Measures",
        [
            ("QuotaAttainmentPercent",
             "QuotaAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)NetAmount / (DT_NUMERIC,9,4)QuotaAmount) * 100",
             DEC("QuotaAttainmentPercent", 9, 4)),
            ("NetPerSalesperson",
             "SalespersonCount == 0 ? (DT_NUMERIC,18,2)0 : "
             "((DT_NUMERIC,18,2)NetAmount / (DT_NUMERIC,18,2)SalespersonCount)",
             money_col("NetPerSalesperson")),
            ("TaxRegimeCode",
             'RegionCode == "NA" ? "SALESTAX" : RegionCode == "EU" ? "VAT" : "GST"',
             str_col("TaxRegimeCode", 10)),
            ("LocalCurrencyCode",
             'RegionCode == "NA" ? "USD" : RegionCode == "EU" ? "EUR" : "SGD"',
             str_col("LocalCurrencyCode", 3)),
            ("MarginPercent",
             "NetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)MarginAmount / (DT_NUMERIC,9,4)NetAmount) * 100",
             DEC("MarginPercent", 9, 4)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Regional Rows",
        [
            ("NA Territories", 'RegionCode == "NA"'),
            ("EU Territories", 'RegionCode == "EU"'),
            ("APAC Territories", 'RegionCode == "APAC"'),
        ],
        default_output="Unassigned Territories",
    )
    flow.branch_destination("Insert NA Territories", CONN_DW, "[Aggregate].[Regional Sales Performance]",
                            "Route Regional Rows", "NA Territories")
    flow.branch_destination("Insert EU Territories", CONN_DW, "[Aggregate].[Regional Sales Performance]",
                            "Route Regional Rows", "EU Territories")
    flow.branch_destination("Insert APAC Territories", CONN_DW, "[Aggregate].[Regional Sales Performance]",
                            "Route Regional Rows", "APAC Territories")
    flow.branch_destination("Reject Unassigned Territories", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Regional Rows", "Unassigned Territories")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Regional Sales Source")

    init = pkg.add(_init_period_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_period_window("[Aggregate].[Regional Sales Performance]"))
    build = pkg.add(DataFlowTask(flow))

    regional = Container("Apply Regional Adjustments", kind="sequence",
                         description="Each region has its own post-aggregation adjustment")
    na_adj = regional.add(
        ExecuteSql(
            "Apply NA State Tax Restatement",
            CONN_DW,
            "UPDATE [Aggregate].[Regional Sales Performance] "
            "SET [Net Amount Excluding Tax] = [Net Amount], "
            "    [Tax Basis Code] = N'POSTDISCOUNT' "
            "WHERE [Region Code] = N'NA' AND [Accounting Period Code] = ?;",
            parameter_bindings=[("$Package::AccountingPeriodCode", 0, "NVARCHAR")],
        )
    )
    eu_adj = regional.add(
        ExecuteSql(
            "Apply EU Reverse Charge Restatement",
            CONN_DW,
            "UPDATE [Aggregate].[Regional Sales Performance] "
            "SET [Net Amount Excluding Tax] = [Net Amount], "
            "    [Reverse Charge Amount] = ISNULL([Reverse Charge Amount], 0), "
            "    [Tax Basis Code] = N'NETVAT' "
            "WHERE [Region Code] = N'EU' AND [Accounting Period Code] = ?;",
            parameter_bindings=[("$Package::AccountingPeriodCode", 0, "NVARCHAR")],
        )
    )
    apac_adj = regional.add(
        ExecuteSql(
            "Apply APAC GST Extraction",
            CONN_DW,
            "UPDATE [Aggregate].[Regional Sales Performance] "
            "SET [Net Amount Excluding Tax] = [Net Amount] - [Tax Amount], "
            "    [Tax Basis Code] = N'GROSSGST' "
            "WHERE [Region Code] = N'APAC' AND [Accounting Period Code] = ?;",
            parameter_bindings=[("$Package::AccountingPeriodCode", 0, "NVARCHAR")],
        )
    )
    regional.chain(na_adj, eu_adj, apac_adj)
    adjustments = pkg.add(regional)

    refresh = pkg.add(
        exec_proc(
            "Refresh Regional Rankings",
            "EXEC Integration.RefreshAggregateRegionalSales @AccountingPeriodCode = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Regional Sales Performance"))
    rejects = pkg.add(_log_rejects("Aggregate.Regional Sales Performance"))
    counts = pkg.add(log_row_count("Aggregate.Regional Sales Performance"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, adjustments, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_finance_close_summary():
    pkg = _agg_package(
        "AGG_Refresh_FinanceCloseSummary",
        "Rebuilds Aggregate.Finance Close Summary for the closing period from GL postings and the "
        "AR/AP sub-ledgers, and asserts that the sub-ledger control totals tie back to the ledger "
        "before the period is marked publishable.",
        extra_variables=[("ControlTotalVariance", 0, "int")],
        extra_parameters=PERIOD_WINDOW_PARAMETERS + [
            ("MaterialityAmount", 100, "int", "Variance in whole currency units tolerated at close."),
        ],
    )
    columns = [
        str_col("AccountingPeriodCode", 7),
        str_col("LegalEntityCode", 8),
        str_col("AccountTypeCode", 8),
        money_col("DebitAmount"),
        money_col("CreditAmount"),
        money_col("SubLedgerAmount"),
        int_col("JournalCount"),
        int_col("ManualJournalCount"),
    ]
    flow = DataFlow("Rebuild Finance Close Summary", "Ledger versus sub-ledger control totals")
    flow.oledb_source(
        "GL Close Source",
        CONN_DW,
        "SELECT gl.[Accounting Period Code] AS AccountingPeriodCode, "
        "gl.[Legal Entity Code] AS LegalEntityCode, a.[Account Type Code] AS AccountTypeCode, "
        "SUM(gl.[Debit Amount]) AS DebitAmount, SUM(gl.[Credit Amount]) AS CreditAmount, "
        "ISNULL(MAX(sl.[Sub Ledger Amount]), 0) AS SubLedgerAmount, "
        "COUNT(DISTINCT gl.[Journal Batch Number]) AS JournalCount, "
        "SUM(CASE WHEN gl.[Is Manual Journal] = 1 THEN 1 ELSE 0 END) AS ManualJournalCount "
        "FROM [Fact].[GL Posting] AS gl "
        "INNER JOIN [Dimension].[GL Account] AS a ON a.[GL Account Key] = gl.[GL Account Key] "
        "LEFT JOIN [Aggregate].[Sub Ledger Control Total] AS sl "
        "       ON sl.[Accounting Period Code] = gl.[Accounting Period Code] "
        "      AND sl.[Account Type Code] = a.[Account Type Code] "
        "      AND sl.[Legal Entity Code] = gl.[Legal Entity Code] "
        "WHERE gl.[Accounting Period Code] = ? "
        "GROUP BY gl.[Accounting Period Code], gl.[Legal Entity Code], a.[Account Type Code];",
        columns,
        timeout=3600,
    )
    flow.derived_column(
        "Derive Close Controls",
        [
            ("NetMovementAmount", "DebitAmount - CreditAmount", money_col("NetMovementAmount")),
            ("ControlVarianceAmount", "(DebitAmount - CreditAmount) - SubLedgerAmount",
             money_col("ControlVarianceAmount")),
            ("IsBalanced", "ABS(DebitAmount - CreditAmount - SubLedgerAmount) <= "
                           "(DT_NUMERIC,18,2)@[$Package::MaterialityAmount]",
             Column("IsBalanced", "bool")),
            ("ManualJournalPercent",
             "JournalCount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)ManualJournalCount / (DT_NUMERIC,9,4)JournalCount) * 100",
             DEC("ManualJournalPercent", 9, 4)),
            ("CloseStatusCode",
             "ABS(DebitAmount - CreditAmount - SubLedgerAmount) <= "
             "(DT_NUMERIC,18,2)@[$Package::MaterialityAmount] ? \"TIED\" : \"VARIANCE\"",
             str_col("CloseStatusCode", 10)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Close Rows",
        [
            ("Control Variances", "!IsBalanced"),
            ("High Manual Journal Share", "ManualJournalPercent > 25"),
        ],
        default_output="Tied Rows",
    )
    flow.branch_destination("Insert Tied Rows", CONN_DW, "[Aggregate].[Finance Close Summary]",
                            "Route Close Rows", "Tied Rows")
    flow.branch_destination("Insert High Manual Share", CONN_DW, "[Aggregate].[Finance Close Summary]",
                            "Route Close Rows", "High Manual Journal Share")
    flow.branch_destination("Reject Control Variances", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Close Rows", "Control Variances")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "GL Close Source")

    init = pkg.add(_init_period_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_period_window("[Aggregate].[Finance Close Summary]"))
    build = pkg.add(DataFlowTask(flow))
    refresh = pkg.add(
        exec_proc(
            "Refresh Finance Close Positions",
            "EXEC Integration.RefreshAggregateFinanceClose @AccountingPeriodCode = ?, "
            "@MaterialityAmount = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("$Package::MaterialityAmount", 1, "LONG"),
                ("User::PackageExecutionId", 2, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Finance Close Summary"))
    rejects = pkg.add(_log_rejects("Aggregate.Finance Close Summary"))
    counts = pkg.add(log_row_count("Aggregate.Finance Close Summary"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


def build_agg_refresh_promotion_effectiveness():
    pkg = _agg_package(
        "AGG_Refresh_PromotionEffectiveness",
        "Rebuilds Aggregate.Promotion Effectiveness per promotion and period: incremental volume "
        "against the pre-promotion baseline, discount cost, margin dilution and redemption rate, "
        "with the regional discount tax treatment carried through.",
        extra_variables=[("PromotionsScored", 0, "int")],
        extra_parameters=PERIOD_WINDOW_PARAMETERS + [
            ("BaselineWeeks", 8, "int", "Weeks of pre-promotion trading used as the baseline."),
        ],
    )
    columns = [
        int_col("PromotionKey"),
        str_col("AccountingPeriodCode", 7),
        str_col("RegionCode", 6),
        int_col("PromotedQuantity"),
        money_col("PromotedNetAmount"),
        money_col("DiscountAmount"),
        money_col("PromotedMarginAmount"),
        DEC("BaselineWeeklyQuantity", 18, 2),
        int_col("RedemptionCount"),
        int_col("EligibleCustomerCount"),
    ]
    flow = DataFlow("Rebuild Promotion Effectiveness", "Promotion uplift and dilution")
    flow.oledb_source(
        "Promotion Source",
        CONN_DW,
        "SELECT f.[Promotion Key] AS PromotionKey, d.[Accounting Period Code] AS AccountingPeriodCode, "
        "c.[Region Code] AS RegionCode, SUM(f.[Quantity]) AS PromotedQuantity, "
        "SUM(f.[Net Amount]) AS PromotedNetAmount, SUM(f.[Discount Amount]) AS DiscountAmount, "
        "SUM(f.[Margin Amount]) AS PromotedMarginAmount, "
        "ISNULL(MAX(b.[Baseline Weekly Quantity]), 0) AS BaselineWeeklyQuantity, "
        "COUNT(DISTINCT f.[Invoice Number]) AS RedemptionCount, "
        "ISNULL(MAX(e.[Eligible Customer Count]), 0) AS EligibleCustomerCount "
        "FROM [Fact].[Sale] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date Key] = f.[Invoice Date Key] "
        "INNER JOIN Dimension.Customer AS c ON c.[Customer Key] = f.[Customer Key] "
        "LEFT JOIN [Aggregate].[Promotion Baseline] AS b ON b.[Promotion Key] = f.[Promotion Key] "
        "LEFT JOIN [Aggregate].[Promotion Eligibility] AS e ON e.[Promotion Key] = f.[Promotion Key] "
        "WHERE d.[Accounting Period Code] = ? AND f.[Promotion Key] IS NOT NULL AND f.[Promotion Key] <> 0 "
        "GROUP BY f.[Promotion Key], d.[Accounting Period Code], c.[Region Code];",
        columns,
        timeout=3600,
    )
    flow.derived_column(
        "Derive Uplift Measures",
        [
            ("BaselineQuantity",
             "BaselineWeeklyQuantity * (DT_NUMERIC,18,2)4.33", DEC("BaselineQuantity", 18, 2)),
            ("IncrementalQuantity",
             "(DT_NUMERIC,18,2)PromotedQuantity - (BaselineWeeklyQuantity * (DT_NUMERIC,18,2)4.33)",
             DEC("IncrementalQuantity", 18, 2)),
            ("UpliftPercent",
             "BaselineWeeklyQuantity == 0 ? (DT_NUMERIC,9,4)0 : "
             "(((DT_NUMERIC,9,4)PromotedQuantity - ((DT_NUMERIC,9,4)BaselineWeeklyQuantity * 4.33)) / "
             "((DT_NUMERIC,9,4)BaselineWeeklyQuantity * 4.33)) * 100",
             DEC("UpliftPercent", 9, 4)),
            ("RedemptionRatePercent",
             "EligibleCustomerCount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)RedemptionCount / (DT_NUMERIC,9,4)EligibleCustomerCount) * 100",
             DEC("RedemptionRatePercent", 9, 4)),
            ("MarginDilutionPercent",
             "PromotedNetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)DiscountAmount / (DT_NUMERIC,9,4)PromotedNetAmount) * 100",
             DEC("MarginDilutionPercent", 9, 4)),
            # The discount changes the taxable base in NA and the EU but not in
            # APAC, where GST was charged on the pre-discount gross.
            ("DiscountTaxBasisCode",
             'RegionCode == "APAC" ? "GROSS" : "NET"',
             str_col("DiscountTaxBasisCode", 8)),
            ("RefreshedByLineageKey", "@[User::PackageExecutionId]", bigint_col("RefreshedByLineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Promotion Rows",
        [
            ("No Baseline", "BaselineWeeklyQuantity == 0"),
            ("Value Destroying", "PromotedMarginAmount < 0"),
        ],
        default_output="Standard Rows",
    )
    flow.branch_destination("Insert Standard Rows", CONN_DW, "[Aggregate].[Promotion Effectiveness]",
                            "Route Promotion Rows", "Standard Rows")
    flow.branch_destination("Insert Value Destroying", CONN_DW, "[Aggregate].[Promotion Effectiveness]",
                            "Route Promotion Rows", "Value Destroying")
    flow.branch_destination("Reject No Baseline", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Promotion Rows", "No Baseline")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Promotion Source")

    init = pkg.add(_init_period_window())
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(_delete_period_window("[Aggregate].[Promotion Effectiveness]"))
    build = pkg.add(DataFlowTask(flow))
    refresh = pkg.add(
        exec_proc(
            "Refresh Promotion Scores",
            "EXEC Integration.RefreshAggregatePromotionEffectiveness @AccountingPeriodCode = ?, "
            "@BaselineWeeks = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::AccountingPeriodCode", 0, "NVARCHAR"),
                ("$Package::BaselineWeeks", 1, "LONG"),
                ("User::PackageExecutionId", 2, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Aggregate.Promotion Effectiveness"))
    rejects = pkg.add(_log_rejects("Aggregate.Promotion Effectiveness"))
    counts = pkg.add(log_row_count("Aggregate.Promotion Effectiveness"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, build, refresh, reconcile, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Publication
# ---------------------------------------------------------------------------


def build_agg_publish_reporting_layer():
    pkg = _agg_package(
        "AGG_Publish_ReportingLayer",
        "Publishes the Report.* layer from the Aggregate.* tables. The publication list lives in "
        "etl.Configuration and is expanded into dynamic SQL at run time, which is how the reporting "
        "team has added tables since 2011 without changing the package.",
        extra_variables=[
            ("PublishSql", "", "string"),
            ("ObjectsPublished", 0, "int"),
            ("StalenessHours", 0, "int"),
        ],
        extra_parameters=[
            ("PublicationGroupCode", "DAILY", "string",
             "Publication group in etl.Configuration to publish (DAILY, MONTHEND or ADHOC)."),
            ("MaxStalenessHours", 26, "int", "Refuse to publish an aggregate older than this."),
            ("FailOnStaleSource", "True", "bool", "Abort the publication when a source is stale."),
        ],
    )

    init = pkg.add(
        Expression(
            "Init Publication Window",
            '@[User::WatermarkTo] = (DT_DBTIMESTAMP)GETDATE()',
        )
    )
    start = pkg.add(log_package_start(pkg))
    read_config = pkg.add(
        ExecuteSql(
            "Read Publication List",
            CONN_STAGING,
            "SELECT etl.ufn_GetConfigurationValue(N'ReportingPublicationList', ?) AS PublicationList;",
            result_type="ResultSetType_SingleRow",
            parameter_bindings=[("$Package::PublicationGroupCode", 0, "NVARCHAR")],
            result_bindings=[("0", "User::PublishSql")],
        )
    )
    staleness = pkg.add(
        ExecuteSql(
            "Check Aggregate Staleness",
            CONN_STAGING,
            "SELECT ISNULL(MAX(DATEDIFF(HOUR, w.WatermarkValue, GETDATE())), 0) AS StalenessHours "
            "FROM etl.Watermark AS w "
            "WHERE w.ObjectName LIKE N'Aggregate.%';",
            result_type="ResultSetType_SingleRow",
            result_bindings=[("0", "User::StalenessHours")],
        )
    )

    publish = Container("Publish Reporting Objects", kind="sequence",
                        description="Swap each Report object over from its Aggregate source")
    build_sql = publish.add(
        ExecuteSql(
            "Build Publication Statements",
            CONN_DW,
            "DECLARE @sql nvarchar(max) = N''; "
            "SELECT @sql = @sql + N'TRUNCATE TABLE ' + QUOTENAME(c.TargetSchemaName) + N'.' "
            "     + QUOTENAME(c.TargetObjectName) + N'; INSERT INTO ' + QUOTENAME(c.TargetSchemaName) "
            "     + N'.' + QUOTENAME(c.TargetObjectName) + N' SELECT * FROM ' "
            "     + QUOTENAME(c.SourceSchemaName) + N'.' + QUOTENAME(c.SourceObjectName) + N'; ' "
            "FROM [Integration].[ReportingPublication] AS c "
            "WHERE c.PublicationGroupCode = ? AND c.IsEnabled = 1 "
            "ORDER BY c.PublishSequence; "
            "EXEC sp_executesql @sql;",
            parameter_bindings=[("$Package::PublicationGroupCode", 0, "NVARCHAR")],
            timeout=7200,
        )
    )
    rebuild_indexes = publish.add(
        ExecuteSql(
            "Rebuild Reporting Indexes",
            CONN_DW,
            "DECLARE @sql nvarchar(max) = N''; "
            "SELECT @sql = @sql + N'ALTER INDEX ALL ON ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) "
            "     + N' REBUILD; ' "
            "FROM sys.tables AS t INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE s.name = N'Report'; "
            "EXEC sp_executesql @sql;",
            timeout=7200,
        )
    )
    stamp = publish.add(
        ExecuteSql(
            "Stamp Publication Metadata",
            CONN_DW,
            "UPDATE [Integration].[ReportingPublication] "
            "SET LastPublishedAt = GETDATE(), LastPublishedByExecutionId = ? "
            "WHERE PublicationGroupCode = ? AND IsEnabled = 1;",
            parameter_bindings=[
                ("User::PackageExecutionId", 0, "LONG"),
                ("$Package::PublicationGroupCode", 1, "NVARCHAR"),
            ],
        )
    )
    publish.chain(build_sql, rebuild_indexes, stamp)
    publish_all = pkg.add(publish)

    quarantine = pkg.add(
        ExecuteSql(
            "Quarantine Stale Publications",
            CONN_STAGING,
            "INSERT INTO err.RejectedConstraintViolation "
            "    (ObjectName, BusinessKey, RejectReason, RejectedAt, PackageExecutionId) "
            "SELECT w.ObjectName, w.ObjectName, N'Aggregate is stale at publication time', GETDATE(), ? "
            "FROM etl.Watermark AS w "
            "WHERE w.ObjectName LIKE N'Aggregate.%' "
            "  AND DATEDIFF(HOUR, w.WatermarkValue, GETDATE()) > ?;",
            parameter_bindings=[
                ("User::PackageExecutionId", 0, "LONG"),
                ("$Package::MaxStalenessHours", 1, "LONG"),
            ],
        )
    )
    proc = pkg.add(
        exec_proc(
            "Publish Reporting Layer",
            "EXEC Integration.PublishReportingLayer @PublicationGroupCode = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::PublicationGroupCode", 0, "NVARCHAR"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    reconcile = pkg.add(_reconcile("Report.*"))
    rejects = pkg.add(_log_rejects("Report.*"))
    counts = pkg.add(log_row_count("Report.*"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, read_config, staleness, publish_all, quarantine, proc, reconcile, rejects,
              counts, done)
    return _write(pkg)


BUILDERS = [
    build_agg_refresh_daily_sales_summary,
    build_agg_refresh_monthly_sales_summary,
    build_agg_refresh_daily_inventory_health,
    build_agg_refresh_monthly_margin_analysis,
    build_agg_refresh_customer_360,
    build_agg_refresh_customer_rolling_12_month,
    build_agg_refresh_product_performance,
    build_agg_refresh_supplier_performance,
    build_agg_refresh_regional_sales_performance,
    build_agg_refresh_finance_close_summary,
    build_agg_refresh_promotion_effectiveness,
    build_agg_refresh_delivery_performance_summary,
    build_agg_publish_reporting_layer,
]


def main():
    if not os.path.isdir(OUT_DIR):
        os.makedirs(OUT_DIR)
    written = [builder() for builder in BUILDERS]
    package_names = sorted(os.path.basename(path)[: -len(".dtsx")] for path in written)
    written.extend(project.write_project(OUT_DIR, PROJECT_NAME, package_names, PROJECT_CONNECTIONS))
    for path in written:
        print(os.path.relpath(path, REPO_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
