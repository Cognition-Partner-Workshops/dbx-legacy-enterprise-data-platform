#!/usr/bin/env python3
"""Spec module for the SQL Server OLTP extract packages (ssis/02_sqlserver_extract).

Twenty-two packages pull the WideWorldImporters OLTP database - the original
Sales/Warehouse/Application schemas plus the Shipping, Returns, Ecommerce and
Integration schemas the estate grew later - into the ``raw`` schema of the
staging database.

Load patterns implemented here:

* numeric-key incrementals driven by the max identity landed so far, with the
  source maximum read up front so a mid-run insert is not skipped,
* a timestamp-watermark incremental over the temporal ValidFrom column with a
  lookback window,
* a bounded date-window extract that is re-runnable for a given window,
* full truncate-and-load refreshes for the small Application/Sales references,
* source-side filtering and joined queries against the vw_* extract views,
* delete detection through SQL Server change tracking (CHANGETABLE) for the
  two OLTP objects where it is enabled.

Incremental source queries carry ``?`` placeholders bound, in order, to the
watermark variables that etl.usp_GetWatermark populates.

Run:  python3 ssis/02_sqlserver_extract/generate_sqlserver_extracts.py
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from ssisgen import (  # noqa: E402
    Column,
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
from patterns import (  # noqa: E402
    CONN_OLTP,
    CONN_STAGING,
    exec_proc,
    get_watermark,
    log_package_start,
    log_package_success,
    log_row_count,
    new_package,
    set_watermark,
    truncate,
)

PROJECT_NAME = "WWI_Extract_SqlServer"
PROJECT_CONNECTIONS = ["WWI_Source_DB", "WWI_Staging_DB"]

SRC_OLTP = "WWI_OLTP"
SRC_WEB = "WWI_WEB"


def num_col(name, precision=18, scale=4):
    return Column(name, "numeric", precision=precision, scale=scale)


def bit_col(name):
    return Column(name, "bool")


def init_variables(assignments):
    return Expression("Init Batch Variables", assignments)


def audit_derivations(source_system):
    return [
        ("SourceSystemCode", '"%s"' % source_system, str_col("SourceSystemCode", 20)),
        ("ExtractedAtUtc", "GETUTCDATE()", date_col("ExtractedAtUtc")),
        ("PackageExecutionId", "@[User::PackageExecutionId]", bigint_col("PackageExecutionId")),
    ]


def read_max_key(name, table, key_column):
    """Read the current source maximum for a numeric-key incremental."""
    return ExecuteSql(
        name,
        CONN_OLTP,
        "SELECT ISNULL(MAX(%s), 0) AS MaxKey FROM %s WITH (NOLOCK);" % (key_column, table),
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::WatermarkTo")],
        timeout=300,
    )


# ---------------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------------


def ext_sql_orders():
    """Numeric-key incremental over Sales.Orders with change-tracking delete
    detection and a split for back-ordered lines."""
    pkg = new_package(
        "EXT_SQL_Orders",
        "Numeric-key incremental order header extract from Sales.Orders, denormalised "
        "with the salesperson, contact person and buying group. A second pass reads "
        "SQL Server change tracking for deletes because order cancellation removes the "
        "header row rather than flagging it.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("RowsDeleted", 0, "int"), ("ChangeTrackingVersion", 0, "long")],
    )

    cols = [
        int_col("OrderID"),
        int_col("CustomerID"),
        int_col("SalespersonPersonID"),
        int_col("PickedByPersonID"),
        int_col("ContactPersonID"),
        int_col("BackorderOrderID"),
        date_col("OrderDate"),
        date_col("ExpectedDeliveryDate"),
        str_col("CustomerPurchaseOrderNumber", 20),
        bit_col("IsUndersupplyBackordered"),
        str_col("Comments", 400),
        str_col("DeliveryInstructions", 400),
        int_col("SalesTerritoryID"),
        str_col("SalesChannelCode", 10),
        date_col("PickingCompletedWhen"),
        date_col("LastEditedWhen"),
        int_col("LastEditedBy"),
    ]

    sql = """SELECT  o.OrderID,
        o.CustomerID,
        o.SalespersonPersonID,
        o.PickedByPersonID,
        o.ContactPersonID,
        o.BackorderOrderID,
        o.OrderDate,
        o.ExpectedDeliveryDate,
        o.CustomerPurchaseOrderNumber,
        o.IsUndersupplyBackordered,
        o.Comments,
        o.DeliveryInstructions,
        st.SalesTerritoryID,
        ISNULL(sc.SalesChannelCode, N'DIRECT')  AS SalesChannelCode,
        o.PickingCompletedWhen,
        o.LastEditedWhen,
        o.LastEditedBy
FROM    Sales.Orders AS o WITH (NOLOCK)
        LEFT OUTER JOIN Sales.SalesTerritories AS st WITH (NOLOCK)
            ON st.SalesTerritoryID = o.SalesTerritoryID
        LEFT OUTER JOIN Sales.SalesChannels AS sc WITH (NOLOCK)
            ON sc.SalesChannelID = o.SalesChannelID
WHERE   o.OrderID > ?
  AND   o.OrderID <= ?
ORDER BY o.OrderID;"""

    df = DataFlow("Extract Orders")
    df.oledb_source("OLTP Sales.Orders", CONN_OLTP, sql, cols, timeout=3600)
    df.derived_column(
        "Derive Order Flags",
        [
            (
                "BackorderFlag",
                'ISNULL(BackorderOrderID) ? "N" : "Y"',
                str_col("BackorderFlag", 1),
            ),
            (
                "PickCycleHours",
                'ISNULL(PickingCompletedWhen) ? -1 : DATEDIFF("hh", OrderDate, PickingCompletedWhen)',
                int_col("PickCycleHours"),
            ),
            ("DeleteFlag", '"N"', str_col("DeleteFlag", 1)),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Orders Read", "User::RowsRead")
    df.oledb_destination(
        "raw SqlOrder",
        CONN_STAGING,
        "raw.SqlOrder",
        batch_size=100000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP Sales.Orders",
    )

    delete_cols = [int_col("OrderID"), str_col("ChangeOperation", 1), bigint_col("ChangeVersion")]
    delete_sql = """SELECT  ct.OrderID,
        ct.SYS_CHANGE_OPERATION      AS ChangeOperation,
        ct.SYS_CHANGE_VERSION        AS ChangeVersion
FROM    CHANGETABLE(CHANGES Sales.Orders, ?) AS ct
WHERE   ct.SYS_CHANGE_OPERATION = 'D';"""

    deletes = DataFlow("Detect Deleted Orders")
    deletes.oledb_source("OLTP Order Change Tracking", CONN_OLTP, delete_sql, delete_cols, timeout=600)
    deletes.derived_column(
        "Flag Deleted Orders",
        audit_derivations(SRC_OLTP) + [("DeleteFlag", '"Y"', str_col("DeleteFlag", 1))],
    )
    deletes.row_count("Count Deleted Orders", "User::RowsDeleted")
    deletes.oledb_destination(
        "raw SqlOrder Deletes",
        CONN_STAGING,
        "raw.SqlOrder",
        batch_size=5000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsDeleted] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Sales.Orders"))
    read_ct = pkg.add(
        ExecuteSql(
            "Read Change Tracking Version",
            CONN_OLTP,
            "SELECT ISNULL(LastSyncVersion, 0) AS LastSyncVersion "
            "FROM Integration.ChangeTrackingWatermark WITH (NOLOCK) WHERE ObjectName = N'Sales.Orders';",
            result_type="ResultSetType_SingleRow",
            result_bindings=[("0", "User::ChangeTrackingVersion")],
        )
    )
    read_max = pkg.add(read_max_key("Read Source Max OrderID", "Sales.Orders", "OrderID"))
    extract = pkg.add(DataFlowTask(df))
    delete_pass = pkg.add(DataFlowTask(deletes))
    setwm = pkg.add(set_watermark("Sales.Orders"))
    rows = pkg.add(log_row_count("raw.SqlOrder"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, read_ct, read_max, extract, delete_pass, setwm, rows, done)
    return pkg


def ext_sql_order_lines():
    """Numeric-key incremental over the order line extract view with a discount
    join and a wide commit size."""
    pkg = new_package(
        "EXT_SQL_OrderLines",
        "Numeric-key incremental order line extract over Sales.vw_OrderLineExtract, "
        "joined to Sales.OrderDiscounts so the applied promotion and discount amount "
        "land with the line. Runs with a large commit size because it is the widest "
        "extract in the nightly batch.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("DiscountedLineCount", 0, "int")],
    )

    cols = [
        int_col("OrderLineID"),
        int_col("OrderID"),
        int_col("StockItemID"),
        str_col("Description", 100),
        str_col("PackageTypeName", 50),
        int_col("Quantity"),
        money_col("UnitPrice"),
        num_col("TaxRate", 18, 3),
        money_col("ExtendedPrice"),
        money_col("LineDiscountAmount"),
        str_col("PromotionCode", 20),
        int_col("PickedQuantity"),
        date_col("PickingCompletedWhen"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  ol.OrderLineID,
        ol.OrderID,
        ol.StockItemID,
        ol.Description,
        ol.PackageTypeName,
        ol.Quantity,
        ol.UnitPrice,
        ol.TaxRate,
        CAST(ol.Quantity * ol.UnitPrice * (1.0 + ol.TaxRate / 100.0) AS decimal(18,2)) AS ExtendedPrice,
        ISNULL(od.DiscountAmount, 0.00)                                                AS LineDiscountAmount,
        od.PromotionCode,
        ol.PickedQuantity,
        ol.PickingCompletedWhen,
        ol.LastEditedWhen
FROM    Sales.vw_OrderLineExtract AS ol WITH (NOLOCK)
        LEFT OUTER JOIN Sales.OrderDiscounts AS od WITH (NOLOCK)
            ON od.OrderLineID = ol.OrderLineID
           AND od.IsVoided = 0
WHERE   ol.OrderLineID > ?
  AND   ol.OrderLineID <= ?
ORDER BY ol.OrderLineID;"""

    df = DataFlow("Extract Order Lines")
    df.oledb_source("OLTP vw_OrderLineExtract", CONN_OLTP, sql, cols, timeout=7200)
    df.derived_column(
        "Derive Line Economics",
        [
            (
                "NetLineAmount",
                "ExtendedPrice - LineDiscountAmount",
                money_col("NetLineAmount"),
            ),
            (
                "ShortPickFlag",
                'PickedQuantity < Quantity ? "Y" : "N"',
                str_col("ShortPickFlag", 1),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Lines Read", "User::RowsRead")
    df.oledb_destination(
        "raw SqlOrderLine",
        CONN_STAGING,
        "raw.SqlOrderLine",
        batch_size=250000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP vw_OrderLineExtract",
    )

    init = pkg.add(init_variables("@[User::DiscountedLineCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Sales.OrderLines"))
    read_max = pkg.add(read_max_key("Read Source Max OrderLineID", "Sales.OrderLines", "OrderLineID"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Sales.OrderLines"))
    rows = pkg.add(log_row_count("raw.SqlOrderLine"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, read_max, extract, setwm, rows, done)
    return pkg


def ext_sql_invoices():
    """Numeric-key incremental over the invoice extract view with the three
    regional tax treatments derived from the delivery city."""
    pkg = new_package(
        "EXT_SQL_Invoices",
        "Numeric-key incremental invoice header extract over Sales.vw_InvoiceExtract. "
        "The tax treatment is resolved from the delivery geography - sales tax for NA, "
        "VAT with the customer registration number for EU, GST for APAC - because the "
        "OLTP schema stores one TaxRate column for all three.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("CreditNoteCount", 0, "int")],
    )

    cols = [
        int_col("InvoiceID"),
        int_col("CustomerID"),
        int_col("BillToCustomerID"),
        int_col("OrderID"),
        int_col("DeliveryMethodID"),
        int_col("ContactPersonID"),
        int_col("SalespersonPersonID"),
        date_col("InvoiceDate"),
        str_col("CustomerPurchaseOrderNumber", 20),
        bit_col("IsCreditNote"),
        str_col("CreditNoteReason", 200),
        money_col("TotalExcludingTax"),
        money_col("TotalTaxAmount"),
        money_col("TotalIncludingTax"),
        str_col("RegionCode", 8),
        str_col("TaxTreatmentCode", 8),
        str_col("CustomerTaxRegistrationNumber", 30),
        date_col("ConfirmedDeliveryTime"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  i.InvoiceID,
        i.CustomerID,
        i.BillToCustomerID,
        i.OrderID,
        i.DeliveryMethodID,
        i.ContactPersonID,
        i.SalespersonPersonID,
        i.InvoiceDate,
        i.CustomerPurchaseOrderNumber,
        i.IsCreditNote,
        i.CreditNoteReason,
        i.TotalExcludingTax,
        i.TotalTaxAmount,
        i.TotalIncludingTax,
        i.RegionCode,
        CASE i.RegionCode
            WHEN N'NA'   THEN N'SALESTAX'
            WHEN N'EU'   THEN N'VAT'
            WHEN N'APAC' THEN N'GST'
            ELSE N'NONE'
        END                                     AS TaxTreatmentCode,
        CASE WHEN i.RegionCode = N'EU' THEN i.CustomerTaxRegistrationNumber ELSE NULL END
                                                AS CustomerTaxRegistrationNumber,
        i.ConfirmedDeliveryTime,
        i.LastEditedWhen
FROM    Sales.vw_InvoiceExtract AS i WITH (NOLOCK)
WHERE   i.InvoiceID > ?
  AND   i.InvoiceID <= ?
ORDER BY i.InvoiceID;"""

    df = DataFlow("Extract Invoices")
    df.oledb_source("OLTP vw_InvoiceExtract", CONN_OLTP, sql, cols, timeout=3600)
    df.derived_column(
        "Derive Tax Attributes",
        [
            (
                "EffectiveTaxRate",
                "TotalExcludingTax == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(TotalTaxAmount / TotalExcludingTax)",
                num_col("EffectiveTaxRate", 9, 4),
            ),
            (
                "SignedTotalIncludingTax",
                "IsCreditNote ? -TotalIncludingTax : TotalIncludingTax",
                money_col("SignedTotalIncludingTax"),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Invoices Read", "User::RowsRead")
    df.conditional_split(
        "Split Credit Notes",
        [("Invoices", "IsCreditNote == FALSE")],
        default_output="Credit Notes",
    )
    df.oledb_destination(
        "raw SqlInvoice",
        CONN_STAGING,
        "raw.SqlInvoice",
        batch_size=100000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "raw SqlInvoice Credit Notes",
        CONN_STAGING,
        "raw.SqlInvoice",
        from_component="Split Credit Notes",
        from_output="Credit Notes",
    )

    init = pkg.add(init_variables("@[User::CreditNoteCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Sales.Invoices"))
    read_max = pkg.add(read_max_key("Read Source Max InvoiceID", "Sales.Invoices", "InvoiceID"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Sales.Invoices"))
    rows = pkg.add(log_row_count("raw.SqlInvoice"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, read_max, extract, setwm, rows, done)
    return pkg


def ext_sql_invoice_lines():
    """Numeric-key incremental invoice lines with a profitability calculation."""
    pkg = new_package(
        "EXT_SQL_InvoiceLines",
        "Numeric-key incremental invoice line extract joined to Warehouse.StockItems "
        "for the last cost price, so gross margin can be computed in staging without a "
        "second pass over the OLTP database.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        int_col("InvoiceLineID"),
        int_col("InvoiceID"),
        int_col("StockItemID"),
        str_col("Description", 100),
        int_col("PackageTypeID"),
        int_col("Quantity"),
        money_col("UnitPrice"),
        num_col("TaxRate", 18, 3),
        money_col("TaxAmount"),
        money_col("LineProfit"),
        money_col("ExtendedPrice"),
        money_col("LastCostPrice"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  il.InvoiceLineID,
        il.InvoiceID,
        il.StockItemID,
        il.Description,
        il.PackageTypeID,
        il.Quantity,
        il.UnitPrice,
        il.TaxRate,
        il.TaxAmount,
        il.LineProfit,
        il.ExtendedPrice,
        si.LastCostPrice,
        il.LastEditedWhen
FROM    Sales.InvoiceLines AS il WITH (NOLOCK)
        INNER JOIN Warehouse.StockItems AS si WITH (NOLOCK)
            ON si.StockItemID = il.StockItemID
WHERE   il.InvoiceLineID > ?
  AND   il.InvoiceLineID <= ?
ORDER BY il.InvoiceLineID;"""

    df = DataFlow("Extract Invoice Lines")
    df.oledb_source("OLTP Sales.InvoiceLines", CONN_OLTP, sql, cols, timeout=7200)
    df.derived_column(
        "Derive Margin",
        [
            (
                "GrossMarginPct",
                "ExtendedPrice == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(LineProfit / ExtendedPrice)",
                num_col("GrossMarginPct", 9, 4),
            ),
            (
                "NegativeMarginFlag",
                'LineProfit < 0 ? "Y" : "N"',
                str_col("NegativeMarginFlag", 1),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Invoice Lines", "User::RowsRead")
    df.oledb_destination(
        "raw SqlInvoiceLine",
        CONN_STAGING,
        "raw.SqlInvoiceLine",
        batch_size=250000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP Sales.InvoiceLines",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Sales.InvoiceLines"))
    read_max = pkg.add(read_max_key("Read Source Max InvoiceLineID", "Sales.InvoiceLines", "InvoiceLineID"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Sales.InvoiceLines"))
    rows = pkg.add(log_row_count("raw.SqlInvoiceLine"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, read_max, extract, setwm, rows, done)
    return pkg


def ext_sql_promotions():
    """Full reload of promotions with their redemption counts aggregated."""
    pkg = new_package(
        "EXT_SQL_Promotions",
        "Full reload of Sales.Promotions with promotion lines and redemption counts "
        "aggregated in the source query. Regional promotions differ in mechanic "
        "(percentage off in NA, VAT-inclusive price points in EU, bundle quantities in "
        "APAC) so the mechanic code is carried through untranslated.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        int_col("PromotionID"),
        str_col("PromotionCode", 20),
        str_col("PromotionName", 100),
        str_col("PromotionMechanicCode", 10),
        str_col("RegionCode", 8),
        num_col("DiscountPercentage", 9, 4),
        money_col("DiscountAmount"),
        int_col("BundleQuantity"),
        int_col("PromotionLineCount"),
        int_col("RedemptionCount"),
        money_col("RedeemedValue"),
        date_col("StartDate"),
        date_col("EndDate"),
        bit_col("IsActive"),
    ]

    sql = """SELECT  p.PromotionID,
        p.PromotionCode,
        p.PromotionName,
        p.PromotionMechanicCode,
        p.RegionCode,
        p.DiscountPercentage,
        p.DiscountAmount,
        p.BundleQuantity,
        ISNULL(pl.PromotionLineCount, 0)    AS PromotionLineCount,
        ISNULL(pr.RedemptionCount, 0)       AS RedemptionCount,
        ISNULL(pr.RedeemedValue, 0.00)      AS RedeemedValue,
        p.StartDate,
        p.EndDate,
        CASE WHEN SYSDATETIME() BETWEEN p.StartDate AND p.EndDate THEN 1 ELSE 0 END AS IsActive
FROM    Sales.Promotions AS p WITH (NOLOCK)
        LEFT OUTER JOIN (
            SELECT PromotionID, COUNT(*) AS PromotionLineCount
            FROM   Sales.PromotionLines WITH (NOLOCK)
            GROUP BY PromotionID
        ) AS pl ON pl.PromotionID = p.PromotionID
        LEFT OUTER JOIN (
            SELECT PromotionID, COUNT(*) AS RedemptionCount, SUM(RedeemedValue) AS RedeemedValue
            FROM   Sales.PromotionRedemptions WITH (NOLOCK)
            GROUP BY PromotionID
        ) AS pr ON pr.PromotionID = p.PromotionID;"""

    df = DataFlow("Load Promotions")
    df.oledb_source("OLTP Sales.Promotions", CONN_OLTP, sql, cols, timeout=900)
    df.derived_column(
        "Derive Promotion Attributes",
        [
            ("RecordKind", '"PROMOTION"', str_col("RecordKind", 12)),
            (
                "RedemptionRatePct",
                "PromotionLineCount == 0 ? (DT_NUMERIC,9,4)0 : "
                "(DT_NUMERIC,9,4)((DT_NUMERIC,18,4)RedemptionCount / PromotionLineCount)",
                num_col("RedemptionRatePct", 9, 4),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Promotions", "User::RowsRead")
    df.oledb_destination(
        "raw SqlOrder Promotions",
        CONN_STAGING,
        "raw.SqlOrder",
        batch_size=5000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Delete Promotion Rows",
            CONN_STAGING,
            "DELETE FROM raw.SqlOrder WHERE RecordKind = N'PROMOTION';",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.SqlOrder"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, rows, done)
    return pkg


def ext_sql_sales_territories():
    """Full reload of sales territories, quotas and commission plans."""
    pkg = new_package(
        "EXT_SQL_SalesTerritories",
        "Full reload of Sales.SalesTerritories joined to the current quota and "
        "commission plan. Quota periods follow the regional fiscal calendar, so the "
        "period code is taken from the quota row rather than derived.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        int_col("SalesTerritoryID"),
        str_col("SalesTerritoryCode", 10),
        str_col("SalesTerritoryName", 80),
        str_col("RegionCode", 8),
        int_col("ParentTerritoryID"),
        int_col("ManagerPersonID"),
        str_col("FiscalPeriodCode", 10),
        money_col("QuotaAmount"),
        str_col("QuotaCurrencyCode", 3),
        str_col("CommissionPlanCode", 10),
        num_col("CommissionRate", 9, 4),
        bit_col("IsActive"),
        date_col("ValidFrom"),
    ]

    sql = """SELECT  t.SalesTerritoryID,
        t.SalesTerritoryCode,
        t.SalesTerritoryName,
        t.RegionCode,
        t.ParentTerritoryID,
        t.ManagerPersonID,
        q.FiscalPeriodCode,
        q.QuotaAmount,
        q.QuotaCurrencyCode,
        cp.CommissionPlanCode,
        cp.CommissionRate,
        t.IsActive,
        t.ValidFrom
FROM    Sales.SalesTerritories AS t WITH (NOLOCK)
        OUTER APPLY (
            SELECT TOP (1) sq.FiscalPeriodCode, sq.QuotaAmount, sq.QuotaCurrencyCode
            FROM   Sales.SalesQuotas AS sq WITH (NOLOCK)
            WHERE  sq.SalesTerritoryID = t.SalesTerritoryID
            ORDER BY sq.PeriodStartDate DESC
        ) AS q
        LEFT OUTER JOIN Sales.CommissionPlans AS cp WITH (NOLOCK)
            ON cp.CommissionPlanID = t.CommissionPlanID;"""

    df = DataFlow("Load Sales Territories")
    df.oledb_source("OLTP Sales.SalesTerritories", CONN_OLTP, sql, cols, timeout=600)
    df.derived_column(
        "Derive Territory Attributes",
        [
            ("RecordKind", '"TERRITORY"', str_col("RecordKind", 12)),
            (
                "FiscalCalendarCode",
                'RegionCode == "NA" ? "445" : (RegionCode == "EU" ? "CAL" : "APR_MAR")',
                str_col("FiscalCalendarCode", 8),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Territories", "User::RowsRead")
    df.oledb_destination(
        "raw SqlOrder Territories",
        CONN_STAGING,
        "raw.SqlOrder",
        batch_size=2000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Delete Territory Rows",
            CONN_STAGING,
            "DELETE FROM raw.SqlOrder WHERE RecordKind = N'TERRITORY';",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.SqlOrder"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, rows, done)
    return pkg


def ext_sql_customer_segments():
    """Full reload of current customer segment assignments."""
    pkg = new_package(
        "EXT_SQL_CustomerSegments",
        "Full reload of the current customer segment assignment from "
        "Sales.vw_CustomerSegmentCurrent. Consent and retention differ by region: EU "
        "assignments older than the retention window are landed without the scoring "
        "attributes that drove them.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("SuppressedSegmentCount", 0, "int")],
    )

    cols = [
        int_col("CustomerSegmentAssignmentID"),
        int_col("CustomerID"),
        str_col("SegmentCode", 10),
        str_col("SegmentName", 80),
        str_col("RegionCode", 8),
        num_col("SegmentScore", 9, 4),
        str_col("ScoringModelCode", 12),
        str_col("ConsentStatusCode", 8),
        date_col("AssignedDate"),
        date_col("ValidFrom"),
        date_col("ValidTo"),
    ]

    sql = """SELECT  a.CustomerSegmentAssignmentID,
        a.CustomerID,
        s.SegmentCode,
        s.SegmentName,
        a.RegionCode,
        CASE WHEN a.RegionCode = N'EU' AND a.AssignedDate < DATEADD(month, -24, SYSDATETIME())
             THEN NULL ELSE a.SegmentScore END      AS SegmentScore,
        CASE WHEN a.RegionCode = N'EU' AND a.AssignedDate < DATEADD(month, -24, SYSDATETIME())
             THEN NULL ELSE a.ScoringModelCode END  AS ScoringModelCode,
        a.ConsentStatusCode,
        a.AssignedDate,
        a.ValidFrom,
        a.ValidTo
FROM    Sales.vw_CustomerSegmentCurrent AS a WITH (NOLOCK)
        INNER JOIN Sales.CustomerSegments AS s WITH (NOLOCK)
            ON s.CustomerSegmentID = a.CustomerSegmentID
WHERE   a.ValidTo > SYSDATETIME();"""

    df = DataFlow("Load Customer Segments")
    df.oledb_source("OLTP vw_CustomerSegmentCurrent", CONN_OLTP, sql, cols, timeout=900)
    df.derived_column(
        "Derive Consent Handling",
        [
            ("RecordKind", '"SEGMENT"', str_col("RecordKind", 12)),
            (
                "MarketableFlag",
                'RegionCode == "EU" ? (ConsentStatusCode == "OPTIN" ? "Y" : "N") : '
                '(ConsentStatusCode == "OPTOUT" ? "N" : "Y")',
                str_col("MarketableFlag", 1),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Segment Rows", "User::RowsRead")
    df.oledb_destination(
        "raw SqlOrder Segments",
        CONN_STAGING,
        "raw.SqlOrder",
        batch_size=25000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedCustomer",
        CONN_STAGING,
        "err.RejectedCustomer",
        from_component="OLTP vw_CustomerSegmentCurrent",
    )

    init = pkg.add(init_variables("@[User::SuppressedSegmentCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Delete Segment Rows",
            CONN_STAGING,
            "DELETE FROM raw.SqlOrder WHERE RecordKind = N'SEGMENT';",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.SqlOrder"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# Warehouse
# ---------------------------------------------------------------------------


def ext_sql_stock_items():
    """Timestamp-watermark incremental over the temporal ValidFrom column with a
    lookback window, plus change-tracking delete detection."""
    pkg = new_package(
        "EXT_SQL_StockItems",
        "Timestamp-watermark incremental over Warehouse.StockItems using the temporal "
        "ValidFrom column with a lookback window for late-arriving edits, joined to the "
        "stock holding and replenishment rule. Deletions are picked up from change "
        "tracking.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("LookbackMinutes", 240, "int"), ("RowsDeleted", 0, "int")],
    )

    cols = [
        int_col("StockItemID"),
        str_col("StockItemName", 100),
        int_col("SupplierID"),
        int_col("ColorID"),
        int_col("UnitPackageID"),
        int_col("OuterPackageID"),
        str_col("Brand", 50),
        str_col("Size", 20),
        int_col("LeadTimeDays"),
        int_col("QuantityPerOuter"),
        bit_col("IsChillerStock"),
        str_col("Barcode", 50),
        money_col("TaxRate"),
        money_col("UnitPrice"),
        money_col("RecommendedRetailPrice"),
        money_col("TypicalWeightPerUnit"),
        int_col("QuantityOnHand"),
        int_col("ReorderLevel"),
        int_col("TargetStockLevel"),
        str_col("ReplenishmentRuleCode", 10),
        date_col("ValidFrom"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  si.StockItemID,
        si.StockItemName,
        si.SupplierID,
        si.ColorID,
        si.UnitPackageID,
        si.OuterPackageID,
        si.Brand,
        si.Size,
        si.LeadTimeDays,
        si.QuantityPerOuter,
        si.IsChillerStock,
        si.Barcode,
        si.TaxRate,
        si.UnitPrice,
        si.RecommendedRetailPrice,
        si.TypicalWeightPerUnit,
        sh.QuantityOnHand,
        sh.ReorderLevel,
        sh.TargetStockLevel,
        rr.ReplenishmentRuleCode,
        si.ValidFrom,
        si.LastEditedWhen
FROM    Warehouse.StockItems AS si WITH (NOLOCK)
        LEFT OUTER JOIN Warehouse.StockItemHoldings AS sh WITH (NOLOCK)
            ON sh.StockItemID = si.StockItemID
        LEFT OUTER JOIN Warehouse.ReplenishmentRules AS rr WITH (NOLOCK)
            ON rr.StockItemID = si.StockItemID
           AND rr.IsCurrent = 1
WHERE   si.ValidFrom >= DATEADD(minute, -240, CAST(? AS datetime2(7)))
  AND   si.ValidFrom <  CAST(? AS datetime2(7));"""

    df = DataFlow("Extract Stock Items")
    df.oledb_source("OLTP Warehouse.StockItems", CONN_OLTP, sql, cols, timeout=2400)
    df.derived_column(
        "Derive Stock Flags",
        [
            (
                "BelowReorderFlag",
                'QuantityOnHand < ReorderLevel ? "Y" : "N"',
                str_col("BelowReorderFlag", 1),
            ),
            (
                "HandlingClass",
                'IsChillerStock ? "CHILL" : "AMB"',
                str_col("HandlingClass", 5),
            ),
            ("DeleteFlag", '"N"', str_col("DeleteFlag", 1)),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Stock Items", "User::RowsRead")
    df.oledb_destination(
        "raw SqlStockItem",
        CONN_STAGING,
        "raw.SqlStockItem",
        batch_size=50000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedProduct",
        CONN_STAGING,
        "err.RejectedProduct",
        from_component="OLTP Warehouse.StockItems",
    )

    delete_cols = [int_col("StockItemID"), str_col("ChangeOperation", 1), bigint_col("ChangeVersion")]
    delete_sql = """SELECT  ct.StockItemID,
        ct.SYS_CHANGE_OPERATION  AS ChangeOperation,
        ct.SYS_CHANGE_VERSION    AS ChangeVersion
FROM    CHANGETABLE(CHANGES Warehouse.StockItems, ?) AS ct
WHERE   ct.SYS_CHANGE_OPERATION = 'D';"""

    deletes = DataFlow("Detect Deleted Stock Items")
    deletes.oledb_source("OLTP Stock Item Change Tracking", CONN_OLTP, delete_sql, delete_cols, timeout=600)
    deletes.derived_column(
        "Flag Deleted Stock Items",
        audit_derivations(SRC_OLTP) + [("DeleteFlag", '"Y"', str_col("DeleteFlag", 1))],
    )
    deletes.row_count("Count Deleted Stock Items", "User::RowsDeleted")
    deletes.oledb_destination(
        "raw SqlStockItem Deletes",
        CONN_STAGING,
        "raw.SqlStockItem",
        batch_size=2000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsDeleted] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Warehouse.StockItems"))
    extract = pkg.add(DataFlowTask(df))
    delete_pass = pkg.add(DataFlowTask(deletes))
    setwm = pkg.add(set_watermark("Warehouse.StockItems"))
    rows = pkg.add(log_row_count("raw.SqlStockItem"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, delete_pass, setwm, rows, done)
    return pkg


def ext_sql_stock_movements():
    """Numeric-key incremental over stock item transactions, the highest-volume
    extract in the estate."""
    pkg = new_package(
        "EXT_SQL_StockMovements",
        "Numeric-key incremental over Warehouse.StockItemTransactions via "
        "Warehouse.vw_StockMovementExtract, carrying the bin and site so the movement "
        "can be attributed to a location. Transaction types are split into receipts, "
        "issues and adjustments on the way in.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("AdjustmentRowCount", 0, "int")],
    )

    cols = [
        bigint_col("StockItemTransactionID"),
        int_col("StockItemID"),
        int_col("TransactionTypeID"),
        str_col("TransactionTypeName", 50),
        int_col("CustomerID"),
        int_col("SupplierID"),
        int_col("InvoiceID"),
        int_col("PurchaseOrderID"),
        num_col("Quantity", 18, 3),
        str_col("WarehouseSiteCode", 8),
        str_col("BinCode", 12),
        str_col("MovementDirectionCode", 3),
        date_col("TransactionOccurredWhen"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  m.StockItemTransactionID,
        m.StockItemID,
        m.TransactionTypeID,
        tt.TransactionTypeName,
        m.CustomerID,
        m.SupplierID,
        m.InvoiceID,
        m.PurchaseOrderID,
        m.Quantity,
        ws.WarehouseSiteCode,
        b.BinCode,
        CASE WHEN m.Quantity >= 0 THEN N'IN' ELSE N'OUT' END AS MovementDirectionCode,
        m.TransactionOccurredWhen,
        m.LastEditedWhen
FROM    Warehouse.vw_StockMovementExtract AS m WITH (NOLOCK)
        INNER JOIN Application.TransactionTypes AS tt WITH (NOLOCK)
            ON tt.TransactionTypeID = m.TransactionTypeID
        LEFT OUTER JOIN Warehouse.Bins AS b WITH (NOLOCK)
            ON b.BinID = m.BinID
        LEFT OUTER JOIN Warehouse.WarehouseSites AS ws WITH (NOLOCK)
            ON ws.WarehouseSiteID = b.WarehouseSiteID
WHERE   m.StockItemTransactionID > ?
  AND   m.StockItemTransactionID <= ?
ORDER BY m.StockItemTransactionID;"""

    df = DataFlow("Extract Stock Movements")
    df.oledb_source("OLTP vw_StockMovementExtract", CONN_OLTP, sql, cols, timeout=7200)
    df.derived_column(
        "Derive Movement Attributes",
        [
            (
                "AbsoluteQuantity",
                "ABS(Quantity)",
                num_col("AbsoluteQuantity", 18, 3),
            ),
            (
                "MovementClass",
                'ISNULL(InvoiceID) && ISNULL(PurchaseOrderID) ? "ADJ" : '
                '(ISNULL(InvoiceID) ? "RCPT" : "ISSUE")',
                str_col("MovementClass", 5),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Movements", "User::RowsRead")
    df.conditional_split(
        "Split Adjustments",
        [("Movements", 'MovementClass != "ADJ"')],
        default_output="Adjustments",
    )
    df.oledb_destination(
        "raw SqlStockMovement",
        CONN_STAGING,
        "raw.SqlStockMovement",
        batch_size=250000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "raw SqlStockMovement Adjustments",
        CONN_STAGING,
        "raw.SqlStockMovement",
        from_component="Split Adjustments",
        from_output="Adjustments",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP vw_StockMovementExtract",
    )

    init = pkg.add(init_variables("@[User::AdjustmentRowCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Warehouse.StockItemTransactions"))
    read_max = pkg.add(
        read_max_key(
            "Read Source Max TransactionID",
            "Warehouse.StockItemTransactions",
            "StockItemTransactionID",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Warehouse.StockItemTransactions"))
    rows = pkg.add(log_row_count("raw.SqlStockMovement"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, read_max, extract, setwm, rows, done)
    return pkg


def ext_sql_stock_transfers():
    """Numeric-key incremental over inter-site transfer lines."""
    pkg = new_package(
        "EXT_SQL_StockTransfers",
        "Numeric-key incremental over Warehouse.StockTransferLines joined to the "
        "transfer header and both site records, so in-transit stock between sites can "
        "be reconciled. In-transit lines older than the tolerance are flagged rather "
        "than rejected.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("InTransitToleranceDays", 14, "int")],
    )

    cols = [
        bigint_col("StockTransferLineID"),
        int_col("StockTransferID"),
        str_col("TransferReference", 20),
        int_col("StockItemID"),
        str_col("FromSiteCode", 8),
        str_col("ToSiteCode", 8),
        num_col("TransferQuantity", 18, 3),
        num_col("ReceivedQuantity", 18, 3),
        str_col("TransferStatusCode", 6),
        date_col("DispatchedWhen"),
        date_col("ReceivedWhen"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  tl.StockTransferLineID,
        tl.StockTransferID,
        t.TransferReference,
        tl.StockItemID,
        fs.WarehouseSiteCode    AS FromSiteCode,
        ts.WarehouseSiteCode    AS ToSiteCode,
        tl.TransferQuantity,
        tl.ReceivedQuantity,
        t.TransferStatusCode,
        t.DispatchedWhen,
        tl.ReceivedWhen,
        tl.LastEditedWhen
FROM    Warehouse.StockTransferLines AS tl WITH (NOLOCK)
        INNER JOIN Warehouse.StockTransfers AS t WITH (NOLOCK)
            ON t.StockTransferID = tl.StockTransferID
        INNER JOIN Warehouse.WarehouseSites AS fs WITH (NOLOCK)
            ON fs.WarehouseSiteID = t.FromWarehouseSiteID
        INNER JOIN Warehouse.WarehouseSites AS ts WITH (NOLOCK)
            ON ts.WarehouseSiteID = t.ToWarehouseSiteID
WHERE   tl.StockTransferLineID > ?
  AND   t.TransferStatusCode <> N'CANC'
ORDER BY tl.StockTransferLineID;"""

    df = DataFlow("Extract Stock Transfers")
    df.oledb_source("OLTP Warehouse.StockTransferLines", CONN_OLTP, sql, cols, timeout=1800)
    df.derived_column(
        "Derive Transit Metrics",
        [
            (
                "InTransitQuantity",
                "TransferQuantity - ReceivedQuantity",
                num_col("InTransitQuantity", 18, 3),
            ),
            (
                "StaleTransitFlag",
                'ISNULL(ReceivedWhen) && DATEDIFF("dd", DispatchedWhen, GETDATE()) > 14 ? "Y" : "N"',
                str_col("StaleTransitFlag", 1),
            ),
            ("MovementClass", '"XFER"', str_col("MovementClass", 5)),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Transfer Lines", "User::RowsRead")
    df.oledb_destination(
        "raw SqlStockMovement Transfers",
        CONN_STAGING,
        "raw.SqlStockMovement",
        batch_size=50000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP Warehouse.StockTransferLines",
    )

    init = pkg.add(init_variables("@[User::InTransitToleranceDays] = 14"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Warehouse.StockTransferLines"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Warehouse.StockTransferLines"))
    rows = pkg.add(log_row_count("raw.SqlStockMovement"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# Shipping / Returns
# ---------------------------------------------------------------------------


def ext_sql_shipments():
    """Numeric-key incremental over shipment headers with carrier and route."""
    pkg = new_package(
        "EXT_SQL_Shipments",
        "Numeric-key incremental shipment header extract over Shipping.vw_ShipmentExtract, "
        "joined to the carrier, delivery route and freight rate. Customs declarations are "
        "attached for cross-border shipments only, which in practice means EU and APAC.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("CrossBorderCount", 0, "int")],
    )

    cols = [
        int_col("ShipmentHeaderID"),
        str_col("ShipmentReference", 20),
        int_col("InvoiceID"),
        int_col("CustomerID"),
        int_col("CarrierID"),
        str_col("CarrierCode", 10),
        str_col("ServiceLevelCode", 8),
        str_col("DeliveryRouteCode", 10),
        str_col("RegionCode", 8),
        str_col("OriginCountryCode", 3),
        str_col("DestinationCountryCode", 3),
        num_col("ShipmentWeightKg", 18, 3),
        money_col("FreightCharge"),
        str_col("FreightCurrencyCode", 3),
        str_col("CustomsDeclarationNumber", 30),
        str_col("ShipmentStatusCode", 6),
        date_col("DispatchedWhen"),
        date_col("DeliveredWhen"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  s.ShipmentHeaderID,
        s.ShipmentReference,
        s.InvoiceID,
        s.CustomerID,
        s.CarrierID,
        c.CarrierCode,
        s.ServiceLevelCode,
        dr.DeliveryRouteCode,
        s.RegionCode,
        s.OriginCountryCode,
        s.DestinationCountryCode,
        s.ShipmentWeightKg,
        fr.FreightCharge,
        fr.FreightCurrencyCode,
        cd.CustomsDeclarationNumber,
        s.ShipmentStatusCode,
        s.DispatchedWhen,
        s.DeliveredWhen,
        s.LastEditedWhen
FROM    Shipping.vw_ShipmentExtract AS s WITH (NOLOCK)
        INNER JOIN Shipping.Carriers AS c WITH (NOLOCK)
            ON c.CarrierID = s.CarrierID
        LEFT OUTER JOIN Shipping.DeliveryRoutes AS dr WITH (NOLOCK)
            ON dr.DeliveryRouteID = s.DeliveryRouteID
        LEFT OUTER JOIN Shipping.FreightRates AS fr WITH (NOLOCK)
            ON fr.CarrierID = s.CarrierID
           AND fr.ServiceLevelCode = s.ServiceLevelCode
           AND s.DispatchedWhen >= fr.EffectiveFrom
           AND s.DispatchedWhen <  fr.EffectiveTo
        LEFT OUTER JOIN Shipping.CustomsDeclarations AS cd WITH (NOLOCK)
            ON cd.ShipmentHeaderID = s.ShipmentHeaderID
WHERE   s.ShipmentHeaderID > ?
  AND   s.ShipmentHeaderID <= ?
  AND   s.ShipmentStatusCode <> N'VOID'
ORDER BY s.ShipmentHeaderID;"""

    df = DataFlow("Extract Shipments")
    df.oledb_source("OLTP vw_ShipmentExtract", CONN_OLTP, sql, cols, timeout=3600)
    df.derived_column(
        "Derive Delivery Performance",
        [
            (
                "TransitHours",
                'ISNULL(DeliveredWhen) ? -1 : DATEDIFF("hh", DispatchedWhen, DeliveredWhen)',
                int_col("TransitHours"),
            ),
            (
                "CrossBorderFlag",
                'OriginCountryCode == DestinationCountryCode ? "N" : "Y"',
                str_col("CrossBorderFlag", 1),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Shipments", "User::RowsRead")
    df.oledb_destination(
        "raw SqlShipment",
        CONN_STAGING,
        "raw.SqlShipment",
        batch_size=100000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP vw_ShipmentExtract",
    )

    init = pkg.add(init_variables("@[User::CrossBorderCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Shipping.ShipmentHeaders"))
    read_max = pkg.add(read_max_key("Read Source Max ShipmentHeaderID", "Shipping.ShipmentHeaders", "ShipmentHeaderID"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Shipping.ShipmentHeaders"))
    rows = pkg.add(log_row_count("raw.SqlShipment"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, read_max, extract, setwm, rows, done)
    return pkg


def ext_sql_shipment_lines():
    """Numeric-key incremental over shipment lines with packaging detail."""
    pkg = new_package(
        "EXT_SQL_ShipmentLines",
        "Numeric-key incremental shipment line extract joined to the packaging type and "
        "the latest scan event, so partially delivered shipments can be reconciled "
        "against the carrier scan files ingested by ING_FILE_CarrierScan.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        bigint_col("ShipmentLineID"),
        int_col("ShipmentHeaderID"),
        int_col("InvoiceLineID"),
        int_col("StockItemID"),
        int_col("ShippedQuantity"),
        str_col("PackagingTypeCode", 8),
        num_col("PackageWeightKg", 18, 3),
        str_col("TrackingNumber", 40),
        str_col("LastScanStatusCode", 8),
        date_col("LastScanWhen"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  sl.ShipmentLineID,
        sl.ShipmentHeaderID,
        sl.InvoiceLineID,
        sl.StockItemID,
        sl.ShippedQuantity,
        pt.PackagingTypeCode,
        sl.PackageWeightKg,
        sl.TrackingNumber,
        ev.ScanStatusCode       AS LastScanStatusCode,
        ev.ScanOccurredWhen     AS LastScanWhen,
        sl.LastEditedWhen
FROM    Shipping.ShipmentLines AS sl WITH (NOLOCK)
        LEFT OUTER JOIN Shipping.PackagingTypes AS pt WITH (NOLOCK)
            ON pt.PackagingTypeID = sl.PackagingTypeID
        OUTER APPLY (
            SELECT TOP (1) se.ScanStatusCode, se.ScanOccurredWhen
            FROM   Shipping.ShipmentEvents AS se WITH (NOLOCK)
            WHERE  se.ShipmentLineID = sl.ShipmentLineID
            ORDER BY se.ScanOccurredWhen DESC
        ) AS ev
WHERE   sl.ShipmentLineID > ?
  AND   sl.ShipmentLineID <= ?
ORDER BY sl.ShipmentLineID;"""

    df = DataFlow("Extract Shipment Lines")
    df.oledb_source("OLTP Shipping.ShipmentLines", CONN_OLTP, sql, cols, timeout=3600)
    df.derived_column(
        "Derive Scan State",
        [
            (
                "AwaitingScanFlag",
                'ISNULL(LastScanWhen) ? "Y" : "N"',
                str_col("AwaitingScanFlag", 1),
            )
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Shipment Lines", "User::RowsRead")
    df.oledb_destination(
        "raw SqlShipmentLine",
        CONN_STAGING,
        "raw.SqlShipmentLine",
        batch_size=150000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP Shipping.ShipmentLines",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Shipping.ShipmentLines"))
    read_max = pkg.add(read_max_key("Read Source Max ShipmentLineID", "Shipping.ShipmentLines", "ShipmentLineID"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Shipping.ShipmentLines"))
    rows = pkg.add(log_row_count("raw.SqlShipmentLine"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, read_max, extract, setwm, rows, done)
    return pkg


def ext_sql_returns():
    """Numeric-key incremental over return lines with inspection outcome."""
    pkg = new_package(
        "EXT_SQL_Returns",
        "Numeric-key incremental return line extract over Returns.vw_ReturnExtract, "
        "carrying the authorisation, the reason code and the inspection outcome. "
        "Lines still awaiting inspection are landed with a pending disposition rather "
        "than being held back.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("PendingInspectionCount", 0, "int")],
    )

    cols = [
        bigint_col("ReturnLineID"),
        int_col("ReturnAuthorizationID"),
        str_col("ReturnAuthorizationNumber", 20),
        int_col("CustomerID"),
        int_col("InvoiceLineID"),
        int_col("StockItemID"),
        int_col("ReturnedQuantity"),
        str_col("ReturnReasonCode", 8),
        str_col("ReturnReasonDescription", 100),
        str_col("InspectionOutcomeCode", 8),
        str_col("DispositionCode", 8),
        money_col("RefundAmount"),
        str_col("RegionCode", 8),
        date_col("ReturnedWhen"),
        date_col("InspectedWhen"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  rl.ReturnLineID,
        ra.ReturnAuthorizationID,
        ra.ReturnAuthorizationNumber,
        ra.CustomerID,
        rl.InvoiceLineID,
        rl.StockItemID,
        rl.ReturnedQuantity,
        rr.ReturnReasonCode,
        rr.ReturnReasonDescription,
        ISNULL(ri.InspectionOutcomeCode, N'PENDING')    AS InspectionOutcomeCode,
        ISNULL(ri.DispositionCode, N'UNKNOWN')          AS DispositionCode,
        rl.RefundAmount,
        ra.RegionCode,
        ra.ReturnedWhen,
        ri.InspectedWhen,
        rl.LastEditedWhen
FROM    Returns.vw_ReturnExtract AS rl WITH (NOLOCK)
        INNER JOIN Returns.ReturnAuthorizations AS ra WITH (NOLOCK)
            ON ra.ReturnAuthorizationID = rl.ReturnAuthorizationID
        INNER JOIN Returns.ReturnReasons AS rr WITH (NOLOCK)
            ON rr.ReturnReasonID = rl.ReturnReasonID
        LEFT OUTER JOIN Returns.ReturnInspections AS ri WITH (NOLOCK)
            ON ri.ReturnLineID = rl.ReturnLineID
WHERE   rl.ReturnLineID > ?
ORDER BY rl.ReturnLineID;"""

    df = DataFlow("Extract Return Lines")
    df.oledb_source("OLTP vw_ReturnExtract", CONN_OLTP, sql, cols, timeout=1800)
    df.derived_column(
        "Derive Return Attributes",
        [
            (
                "DaysToInspect",
                'ISNULL(InspectedWhen) ? -1 : DATEDIFF("dd", ReturnedWhen, InspectedWhen)',
                int_col("DaysToInspect"),
            ),
            (
                "RestockableFlag",
                'DispositionCode == "RESTOCK" ? "Y" : "N"',
                str_col("RestockableFlag", 1),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Return Lines", "User::RowsRead")
    df.conditional_split(
        "Route Pending Inspections",
        [("Inspected", 'InspectionOutcomeCode != "PENDING"')],
        default_output="Pending",
    )
    df.oledb_destination(
        "raw SqlReturnLine",
        CONN_STAGING,
        "raw.SqlReturnLine",
        batch_size=50000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "raw SqlReturnLine Pending",
        CONN_STAGING,
        "raw.SqlReturnLine",
        from_component="Route Pending Inspections",
        from_output="Pending",
    )

    init = pkg.add(init_variables("@[User::PendingInspectionCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Returns.ReturnLines"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Returns.ReturnLines"))
    rows = pkg.add(log_row_count("raw.SqlReturnLine"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


def ext_sql_credit_notes():
    """Numeric-key incremental over credit note lines with regional tax reversal."""
    pkg = new_package(
        "EXT_SQL_CreditNotes",
        "Numeric-key incremental credit note line extract over "
        "Returns.vw_CreditNoteExtract. The tax reversal is region-specific: NA reverses "
        "state sales tax at the original rate, EU reverses VAT and needs the credit "
        "note reference on the VAT return, APAC reverses GST in the period of issue.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        bigint_col("CreditNoteLineID"),
        int_col("CreditNoteID"),
        str_col("CreditNoteNumber", 20),
        int_col("CustomerID"),
        int_col("InvoiceID"),
        int_col("StockItemID"),
        int_col("CreditedQuantity"),
        money_col("CreditedExcludingTax"),
        money_col("CreditedTaxAmount"),
        money_col("CreditedIncludingTax"),
        str_col("RegionCode", 8),
        str_col("TaxTreatmentCode", 8),
        str_col("TaxReversalBasisCode", 10),
        date_col("CreditNoteDate"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  cl.CreditNoteLineID,
        cn.CreditNoteID,
        cn.CreditNoteNumber,
        cn.CustomerID,
        cn.InvoiceID,
        cl.StockItemID,
        cl.CreditedQuantity,
        cl.CreditedExcludingTax,
        cl.CreditedTaxAmount,
        cl.CreditedIncludingTax,
        cn.RegionCode,
        CASE cn.RegionCode
            WHEN N'NA'   THEN N'SALESTAX'
            WHEN N'EU'   THEN N'VAT'
            WHEN N'APAC' THEN N'GST'
            ELSE N'NONE'
        END                                 AS TaxTreatmentCode,
        CASE cn.RegionCode
            WHEN N'NA'   THEN N'ORIGRATE'
            WHEN N'EU'   THEN N'CREDITREF'
            WHEN N'APAC' THEN N'ISSUEPRD'
            ELSE N'NONE'
        END                                 AS TaxReversalBasisCode,
        cn.CreditNoteDate,
        cl.LastEditedWhen
FROM    Returns.vw_CreditNoteExtract AS cl WITH (NOLOCK)
        INNER JOIN Returns.CreditNotes AS cn WITH (NOLOCK)
            ON cn.CreditNoteID = cl.CreditNoteID
WHERE   cl.CreditNoteLineID > ?
  AND   cn.IsVoided = 0
ORDER BY cl.CreditNoteLineID;"""

    df = DataFlow("Extract Credit Note Lines")
    df.oledb_source("OLTP vw_CreditNoteExtract", CONN_OLTP, sql, cols, timeout=1800)
    df.derived_column(
        "Derive Credit Attributes",
        [
            (
                "SignedCreditAmount",
                "-CreditedIncludingTax",
                money_col("SignedCreditAmount"),
            ),
            (
                "VatReturnRequiredFlag",
                'TaxTreatmentCode == "VAT" ? "Y" : "N"',
                str_col("VatReturnRequiredFlag", 1),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Credit Note Lines", "User::RowsRead")
    df.oledb_destination(
        "raw SqlCreditNote",
        CONN_STAGING,
        "raw.SqlCreditNote",
        batch_size=25000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP vw_CreditNoteExtract",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Returns.CreditNoteLines"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Returns.CreditNoteLines"))
    rows = pkg.add(log_row_count("raw.SqlCreditNote"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# Ecommerce
# ---------------------------------------------------------------------------


def ext_sql_web_sessions():
    """Date-window extract over web sessions, re-runnable for a given window."""
    pkg = new_package(
        "EXT_SQL_WebSessions",
        "Date-window extract over Ecommerce.WebSessions. The window comes from "
        "etl.usp_GetWatermark and the target rows for that window are deleted before "
        "the load, so re-running a window is idempotent. EU sessions without analytics "
        "consent are landed without the device fingerprint and referrer.",
        source_system=SRC_WEB,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("WindowDays", 1, "int"), ("ConsentSuppressedCount", 0, "int")],
    )

    cols = [
        bigint_col("WebSessionID"),
        str_col("SessionGuid", 36),
        int_col("CustomerID"),
        str_col("RegionCode", 8),
        str_col("ChannelCode", 10),
        str_col("DeviceCategoryCode", 10),
        str_col("DeviceFingerprint", 64),
        str_col("ReferrerDomain", 120),
        str_col("LandingPagePath", 200),
        int_col("PageViewCount"),
        int_col("SessionDurationSeconds"),
        bit_col("HasCartActivity"),
        bit_col("HasCheckout"),
        str_col("AnalyticsConsentCode", 8),
        date_col("SessionStartedWhen"),
        date_col("SessionEndedWhen"),
    ]

    sql = """SELECT  ws.WebSessionID,
        ws.SessionGuid,
        ws.CustomerID,
        ws.RegionCode,
        ws.ChannelCode,
        ws.DeviceCategoryCode,
        CASE WHEN ws.RegionCode = N'EU' AND ws.AnalyticsConsentCode <> N'GRANTED'
             THEN NULL ELSE ws.DeviceFingerprint END    AS DeviceFingerprint,
        CASE WHEN ws.RegionCode = N'EU' AND ws.AnalyticsConsentCode <> N'GRANTED'
             THEN NULL ELSE ws.ReferrerDomain END       AS ReferrerDomain,
        ws.LandingPagePath,
        ws.PageViewCount,
        DATEDIFF(second, ws.SessionStartedWhen, ws.SessionEndedWhen) AS SessionDurationSeconds,
        CASE WHEN ch.CartHeaderID IS NULL THEN 0 ELSE 1 END          AS HasCartActivity,
        ws.HasCheckout,
        ws.AnalyticsConsentCode,
        ws.SessionStartedWhen,
        ws.SessionEndedWhen
FROM    Ecommerce.WebSessions AS ws WITH (NOLOCK)
        LEFT OUTER JOIN Ecommerce.CartHeaders AS ch WITH (NOLOCK)
            ON ch.WebSessionID = ws.WebSessionID
WHERE   ws.SessionStartedWhen >= CAST(? AS datetime2(7))
  AND   ws.SessionStartedWhen <  CAST(? AS datetime2(7));"""

    df = DataFlow("Extract Web Sessions")
    df.oledb_source("OLTP Ecommerce.WebSessions", CONN_OLTP, sql, cols, timeout=3600)
    df.derived_column(
        "Derive Session Metrics",
        [
            (
                "BounceFlag",
                'PageViewCount <= 1 ? "Y" : "N"',
                str_col("BounceFlag", 1),
            ),
            (
                "ConversionFlag",
                'HasCheckout ? "Y" : "N"',
                str_col("ConversionFlag", 1),
            ),
        ]
        + audit_derivations(SRC_WEB),
    )
    df.row_count("Count Sessions", "User::RowsRead")
    df.oledb_destination(
        "raw SqlWebSession",
        CONN_STAGING,
        "raw.SqlWebSession",
        batch_size=200000,
        error_disposition="IgnoreFailure",
    )

    init = pkg.add(init_variables("@[User::ConsentSuppressedCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Ecommerce.WebSessions"))
    clear_window = pkg.add(
        ExecuteSql(
            "Clear Session Window",
            CONN_STAGING,
            "DELETE FROM raw.SqlWebSession "
            "WHERE SessionStartedWhen >= CAST(? AS datetime2(7)) AND SessionStartedWhen < CAST(? AS datetime2(7));",
            parameter_bindings=[
                ("User::WatermarkFrom", 0, "NVARCHAR"),
                ("User::WatermarkTo", 1, "NVARCHAR"),
            ],
        )
    )
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Ecommerce.WebSessions"))
    rows = pkg.add(log_row_count("raw.SqlWebSession"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, clear_window, extract, setwm, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# Ledger-style OLTP transactions
# ---------------------------------------------------------------------------


def ext_sql_customer_transactions():
    """Numeric-key incremental over customer ledger transactions."""
    pkg = new_package(
        "EXT_SQL_CustomerTransactions",
        "Numeric-key incremental over Sales.CustomerTransactions - the AR ledger side "
        "of the OLTP database - joined to the payment method and transaction type. "
        "Outstanding balances are carried so AR aging can be rebuilt in staging.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        int_col("CustomerTransactionID"),
        int_col("CustomerID"),
        int_col("TransactionTypeID"),
        str_col("TransactionTypeName", 50),
        int_col("InvoiceID"),
        int_col("PaymentMethodID"),
        str_col("PaymentMethodName", 50),
        date_col("TransactionDate"),
        money_col("AmountExcludingTax"),
        money_col("TaxAmount"),
        money_col("TransactionAmount"),
        money_col("OutstandingBalance"),
        date_col("FinalizationDate"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  ct.CustomerTransactionID,
        ct.CustomerID,
        ct.TransactionTypeID,
        tt.TransactionTypeName,
        ct.InvoiceID,
        ct.PaymentMethodID,
        pm.PaymentMethodName,
        ct.TransactionDate,
        ct.AmountExcludingTax,
        ct.TaxAmount,
        ct.TransactionAmount,
        ct.OutstandingBalance,
        ct.FinalizationDate,
        ct.LastEditedWhen
FROM    Sales.CustomerTransactions AS ct WITH (NOLOCK)
        INNER JOIN Application.TransactionTypes AS tt WITH (NOLOCK)
            ON tt.TransactionTypeID = ct.TransactionTypeID
        LEFT OUTER JOIN Application.PaymentMethods AS pm WITH (NOLOCK)
            ON pm.PaymentMethodID = ct.PaymentMethodID
WHERE   ct.CustomerTransactionID > ?
  AND   ct.CustomerTransactionID <= ?
ORDER BY ct.CustomerTransactionID;"""

    df = DataFlow("Extract Customer Transactions")
    df.oledb_source("OLTP Sales.CustomerTransactions", CONN_OLTP, sql, cols, timeout=3600)
    df.derived_column(
        "Derive AR Attributes",
        [
            ("RecordKind", '"ARTRAN"', str_col("RecordKind", 12)),
            (
                "SettledFlag",
                'ISNULL(FinalizationDate) ? "N" : "Y"',
                str_col("SettledFlag", 1),
            ),
            (
                "DaysOutstanding",
                'ISNULL(FinalizationDate) ? DATEDIFF("dd", TransactionDate, GETDATE()) : '
                'DATEDIFF("dd", TransactionDate, FinalizationDate)',
                int_col("DaysOutstanding"),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count AR Transactions", "User::RowsRead")
    df.oledb_destination(
        "raw SqlInvoice AR Transactions",
        CONN_STAGING,
        "raw.SqlInvoice",
        batch_size=150000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP Sales.CustomerTransactions",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Sales.CustomerTransactions"))
    read_max = pkg.add(
        read_max_key("Read Source Max CustomerTransactionID", "Sales.CustomerTransactions", "CustomerTransactionID")
    )
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Sales.CustomerTransactions"))
    rows = pkg.add(log_row_count("raw.SqlInvoice"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, read_max, extract, setwm, rows, done)
    return pkg


def ext_sql_supplier_transactions():
    """Numeric-key incremental over supplier ledger transactions, reconciled
    against the Oracle AP ledger downstream."""
    pkg = new_package(
        "EXT_SQL_SupplierTransactions",
        "Numeric-key incremental over Purchasing.SupplierTransactions. This ledger "
        "overlaps the Oracle AP ledger for suppliers that were never migrated off the "
        "OLTP system, so the extract carries the supplier reference used by the "
        "downstream duplicate-payment check.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("OverlapSupplierCount", 0, "int")],
    )

    cols = [
        int_col("SupplierTransactionID"),
        int_col("SupplierID"),
        str_col("SupplierReference", 20),
        int_col("TransactionTypeID"),
        str_col("TransactionTypeName", 50),
        int_col("PurchaseOrderID"),
        str_col("SupplierInvoiceNumber", 20),
        date_col("TransactionDate"),
        money_col("AmountExcludingTax"),
        money_col("TaxAmount"),
        money_col("TransactionAmount"),
        money_col("OutstandingBalance"),
        date_col("FinalizationDate"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  st.SupplierTransactionID,
        st.SupplierID,
        s.SupplierReference,
        st.TransactionTypeID,
        tt.TransactionTypeName,
        st.PurchaseOrderID,
        st.SupplierInvoiceNumber,
        st.TransactionDate,
        st.AmountExcludingTax,
        st.TaxAmount,
        st.TransactionAmount,
        st.OutstandingBalance,
        st.FinalizationDate,
        st.LastEditedWhen
FROM    Purchasing.SupplierTransactions AS st WITH (NOLOCK)
        INNER JOIN Purchasing.Suppliers AS s WITH (NOLOCK)
            ON s.SupplierID = st.SupplierID
        INNER JOIN Application.TransactionTypes AS tt WITH (NOLOCK)
            ON tt.TransactionTypeID = st.TransactionTypeID
WHERE   st.SupplierTransactionID > ?
ORDER BY st.SupplierTransactionID;"""

    df = DataFlow("Extract Supplier Transactions")
    df.oledb_source("OLTP Purchasing.SupplierTransactions", CONN_OLTP, sql, cols, timeout=2400)
    df.derived_column(
        "Derive AP Overlap Key",
        [
            ("RecordKind", '"APTRAN"', str_col("RecordKind", 12)),
            (
                "DuplicateCheckKey",
                'UPPER(TRIM(SupplierReference)) + "|" + UPPER(TRIM(SupplierInvoiceNumber))',
                str_col("DuplicateCheckKey", 64),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count AP Transactions", "User::RowsRead")
    df.oledb_destination(
        "raw SqlInvoice AP Transactions",
        CONN_STAGING,
        "raw.SqlInvoice",
        batch_size=75000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP Purchasing.SupplierTransactions",
    )

    init = pkg.add(init_variables("@[User::OverlapSupplierCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Purchasing.SupplierTransactions"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Purchasing.SupplierTransactions"))
    rows = pkg.add(log_row_count("raw.SqlInvoice"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# Application reference data
# ---------------------------------------------------------------------------


def ext_sql_people():
    """Full reload of the people reference with role flags."""
    pkg = new_package(
        "EXT_SQL_People",
        "Full reload of Application.People restricted to the roles the warehouse needs "
        "(salespeople, employees, pickers). Login and photo columns are deliberately "
        "not extracted; the search name is carried for the customer-360 matcher.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        int_col("PersonID"),
        str_col("FullName", 50),
        str_col("PreferredName", 50),
        str_col("SearchName", 101),
        bit_col("IsPermittedToLogon"),
        bit_col("IsEmployee"),
        bit_col("IsSalesperson"),
        str_col("PhoneNumber", 20),
        str_col("EmailAddress", 256),
        str_col("EmployeeCode", 12),
        date_col("ValidFrom"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  p.PersonID,
        p.FullName,
        p.PreferredName,
        p.SearchName,
        p.IsPermittedToLogon,
        p.IsEmployee,
        p.IsSalesperson,
        p.PhoneNumber,
        p.EmailAddress,
        p.EmployeeCode,
        p.ValidFrom,
        p.LastEditedWhen
FROM    Application.People AS p WITH (NOLOCK)
WHERE   p.IsEmployee = 1
   OR   p.IsSalesperson = 1;"""

    df = DataFlow("Load People")
    df.oledb_source("OLTP Application.People", CONN_OLTP, sql, cols, timeout=600)
    df.derived_column(
        "Derive Person Attributes",
        [
            ("RecordKind", '"PERSON"', str_col("RecordKind", 12)),
            (
                "RoleCode",
                'IsSalesperson ? "SALES" : "EMP"',
                str_col("RoleCode", 6),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count People", "User::RowsRead")
    df.oledb_destination(
        "raw SqlOrder People",
        CONN_STAGING,
        "raw.SqlOrder",
        batch_size=5000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Delete Person Rows",
            CONN_STAGING,
            "DELETE FROM raw.SqlOrder WHERE RecordKind = N'PERSON';",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.SqlOrder"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, rows, done)
    return pkg


def ext_sql_cities():
    """Full reload of the city reference into the shared geography table."""
    pkg = new_package(
        "EXT_SQL_Cities",
        "Full truncate-and-load of Application.Cities joined to StateProvinces and "
        "Countries, landing into the shared raw.OracleGeography table so OLTP and ERP "
        "geography resolve through one lookup. Postal formatting follows the country's "
        "regional standard.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        int_col("GeographyKey"),
        int_col("CityID"),
        str_col("CityName", 50),
        str_col("StateProvinceCode", 5),
        str_col("StateProvinceName", 50),
        str_col("CountryCode", 3),
        str_col("CountryName", 60),
        str_col("Continent", 30),
        str_col("Region", 30),
        str_col("Subregion", 30),
        int_col("LatestRecordedPopulation"),
        str_col("SalesTerritory", 50),
    ]

    sql = """SELECT  c.CityID                AS GeographyKey,
        c.CityID,
        c.CityName,
        sp.StateProvinceCode,
        sp.StateProvinceName,
        co.IsoAlpha3Code        AS CountryCode,
        co.CountryName,
        co.Continent,
        co.Region,
        co.Subregion,
        c.LatestRecordedPopulation,
        sp.SalesTerritory
FROM    Application.Cities AS c WITH (NOLOCK)
        INNER JOIN Application.StateProvinces AS sp WITH (NOLOCK)
            ON sp.StateProvinceID = c.StateProvinceID
        INNER JOIN Application.Countries AS co WITH (NOLOCK)
            ON co.CountryID = sp.CountryID;"""

    df = DataFlow("Load Cities")
    df.oledb_source("OLTP Application.Cities", CONN_OLTP, sql, cols, timeout=900)
    df.derived_column(
        "Derive Geography Attributes",
        [
            ("RecordKind", '"OLTPCITY"', str_col("RecordKind", 12)),
            (
                "RegionCode",
                'Continent == "North America" ? "NA" : (Continent == "Europe" ? "EU" : '
                '(Continent == "Asia" || Continent == "Oceania" ? "APAC" : "ROW"))',
                str_col("RegionCode", 8),
            ),
            (
                "PostalFormatCode",
                'Continent == "North America" ? "ZIP5_PLUS4" : (Continent == "Europe" ? "ALPHANUM" : "NUMERIC6")',
                str_col("PostalFormatCode", 12),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Cities", "User::RowsRead")
    df.oledb_destination(
        "raw OracleGeography Cities",
        CONN_STAGING,
        "raw.OracleGeography",
        batch_size=50000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="OLTP Application.Cities",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Delete OLTP City Rows",
            CONN_STAGING,
            "DELETE FROM raw.OracleGeography WHERE RecordKind = N'OLTPCITY';",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.OracleGeography"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, rows, done)
    return pkg


def ext_sql_payment_methods():
    """Small reference refresh with a regional availability map."""
    pkg = new_package(
        "EXT_SQL_PaymentMethods",
        "Reference refresh of Application.PaymentMethods with the regional availability "
        "map - direct debit and SEPA in EU, ACH and cheque in NA, local wallets in APAC "
        "- expressed as separate rows rather than one merged code list.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        int_col("PaymentMethodID"),
        str_col("PaymentMethodName", 50),
        str_col("PaymentMethodCode", 10),
        str_col("RegionCode", 8),
        str_col("SettlementTypeCode", 8),
        int_col("SettlementDays"),
        bit_col("IsActive"),
        date_col("ValidFrom"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  pm.PaymentMethodID,
        pm.PaymentMethodName,
        pm.PaymentMethodCode,
        r.RegionCode,
        pm.SettlementTypeCode,
        pm.SettlementDays,
        pm.IsActive,
        pm.ValidFrom,
        pm.LastEditedWhen
FROM    Application.PaymentMethods AS pm WITH (NOLOCK)
        CROSS APPLY (
            SELECT N'NA'   AS RegionCode WHERE pm.PaymentMethodCode IN (N'ACH', N'CHEQUE', N'CARD')
            UNION ALL
            SELECT N'EU'   AS RegionCode WHERE pm.PaymentMethodCode IN (N'SEPA', N'DD', N'CARD')
            UNION ALL
            SELECT N'APAC' AS RegionCode WHERE pm.PaymentMethodCode IN (N'WALLET', N'BANKXFER', N'CARD')
        ) AS r;"""

    df = DataFlow("Load Payment Methods")
    df.oledb_source("OLTP Application.PaymentMethods", CONN_OLTP, sql, cols, timeout=300)
    df.derived_column(
        "Derive Method Attributes",
        [
            ("RecordKind", '"PAYMETHOD"', str_col("RecordKind", 12)),
            (
                "ImmediateSettlementFlag",
                'SettlementDays == 0 ? "Y" : "N"',
                str_col("ImmediateSettlementFlag", 1),
            ),
        ]
        + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Payment Methods", "User::RowsRead")
    df.oledb_destination(
        "raw SqlInvoice Payment Methods",
        CONN_STAGING,
        "raw.SqlInvoice",
        batch_size=1000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Delete Payment Method Rows",
            CONN_STAGING,
            "DELETE FROM raw.SqlInvoice WHERE RecordKind = N'PAYMETHOD';",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.SqlInvoice"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, rows, done)
    return pkg


def ext_sql_transaction_types():
    """Smallest reference refresh in the extract layer."""
    pkg = new_package(
        "EXT_SQL_TransactionTypes",
        "Reference refresh of Application.TransactionTypes with the GL posting hint "
        "the finance mart needs. Twenty-odd rows, reloaded whole; it runs first in the "
        "nightly batch because several extracts join to it.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
    )

    cols = [
        int_col("TransactionTypeID"),
        str_col("TransactionTypeName", 50),
        str_col("TransactionTypeCode", 10),
        str_col("LedgerSideCode", 2),
        str_col("GlPostingHintCode", 12),
        bit_col("IsReversal"),
        date_col("ValidFrom"),
        date_col("LastEditedWhen"),
    ]

    sql = """SELECT  tt.TransactionTypeID,
        tt.TransactionTypeName,
        tt.TransactionTypeCode,
        CASE WHEN tt.TransactionTypeName LIKE N'%Credit%' THEN N'CR' ELSE N'DR' END AS LedgerSideCode,
        tt.GlPostingHintCode,
        CASE WHEN tt.TransactionTypeName LIKE N'%Reversal%' THEN 1 ELSE 0 END       AS IsReversal,
        tt.ValidFrom,
        tt.LastEditedWhen
FROM    Application.TransactionTypes AS tt WITH (NOLOCK);"""

    df = DataFlow("Load Transaction Types")
    df.oledb_source("OLTP Application.TransactionTypes", CONN_OLTP, sql, cols, timeout=120)
    df.derived_column(
        "Add Reference Audit",
        [("RecordKind", '"TRANTYPE"', str_col("RecordKind", 12))] + audit_derivations(SRC_OLTP),
    )
    df.row_count("Count Transaction Types", "User::RowsRead")
    df.oledb_destination(
        "raw SqlInvoice Transaction Types",
        CONN_STAGING,
        "raw.SqlInvoice",
        batch_size=500,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Delete Transaction Type Rows",
            CONN_STAGING,
            "DELETE FROM raw.SqlInvoice WHERE RecordKind = N'TRANTYPE';",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    audit = pkg.add(
        exec_proc(
            "Log Reference Refresh",
            "EXEC etl.usp_LogRowCount @PackageExecutionId = ?, @ObjectName = N'Application.TransactionTypes', "
            "@SourceRowCount = ?, @TargetRowCount = ?, @RejectRowCount = 0;",
            parameter_bindings=[
                ("User::PackageExecutionId", 0, "LONG"),
                ("User::RowsRead", 1, "LONG"),
                ("User::RowsRead", 2, "LONG"),
            ],
        )
    )
    rows = pkg.add(log_row_count("raw.SqlInvoice"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, audit, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# Loyalty
# ---------------------------------------------------------------------------


def ext_sql_loyalty_ledger():
    """Numeric-key incremental over the loyalty points ledger.

    The ledger is append-only apart from the expiry sweep, which back-fills
    ExpiredWhen on rows already landed, so the extract also re-reads the rows
    that expired since the last run rather than relying on the key watermark
    alone. APAC runs its own expiry calendar, which is why the lookback is a
    parameter rather than a constant.
    """
    pkg = new_package(
        "EXT_SQL_LoyaltyLedger",
        "Numeric-key incremental over Loyalty.LoyaltyPointsLedger into "
        "raw.SqlLoyaltyLedger, plus a lookback re-read of rows the expiry sweep "
        "touched since the previous run. Points expiry rules differ by region, so "
        "the region code travels with every ledger entry.",
        source_system=SRC_OLTP,
        connections=(CONN_OLTP, CONN_STAGING),
        extra_variables=[("ExpiryLookbackDays", 7, "int"), ("SourceMaxLedgerId", 0, "int")],
    )

    cols = [
        str_col("LoyaltyLedgerID", 50),
        str_col("LoyaltyMemberID", 50),
        str_col("CustomerID", 50),
        str_col("ProgramCode", 30),
        str_col("TierCode", 20),
        str_col("EntryTypeCode", 20),
        str_col("PointsDelta", 50),
        str_col("PointsBalanceAfter", 50),
        str_col("SourceInvoiceID", 50),
        str_col("RedemptionReference", 50),
        str_col("EntryWhen", 40),
        str_col("ExpiryDate", 40),
        str_col("RegionCode", 10),
        str_col("LastEditedWhen", 40),
    ]

    sql = """SELECT  CONVERT(nvarchar(50), l.LoyaltyLedgerID)          AS LoyaltyLedgerID,
        CONVERT(nvarchar(50), l.LoyaltyMemberID)          AS LoyaltyMemberID,
        CONVERT(nvarchar(50), m.CustomerID)               AS CustomerID,
        p.ProgramCode,
        m.TierCode,
        l.EntryTypeCode,
        CONVERT(nvarchar(50), l.PointsDelta)              AS PointsDelta,
        CONVERT(nvarchar(50), l.PointsRemaining)          AS PointsBalanceAfter,
        CONVERT(nvarchar(50), l.SourceInvoiceID)          AS SourceInvoiceID,
        l.SourceReference                                 AS RedemptionReference,
        CONVERT(nvarchar(40), l.EntryWhen, 126)           AS EntryWhen,
        CONVERT(nvarchar(40), l.ExpiresOnDate, 23)        AS ExpiryDate,
        m.RegionCode,
        CONVERT(nvarchar(40), ISNULL(l.ExpiredWhen, l.EntryWhen), 126) AS LastEditedWhen
FROM    Loyalty.LoyaltyPointsLedger AS l WITH (NOLOCK)
        INNER JOIN Loyalty.LoyaltyMembers AS m WITH (NOLOCK)
            ON m.LoyaltyMemberID = l.LoyaltyMemberID
        INNER JOIN Loyalty.LoyaltyPrograms AS p WITH (NOLOCK)
            ON p.LoyaltyProgramID = m.LoyaltyProgramID
WHERE   l.LoyaltyLedgerID > CAST(? AS bigint)
   OR   l.ExpiredWhen >= DATEADD(day, -1 * CAST(? AS int), SYSDATETIME());"""

    df = DataFlow("Extract Loyalty Ledger")
    df.oledb_source("OLTP Loyalty.LoyaltyPointsLedger", CONN_OLTP, sql, cols, timeout=3600)
    df.derived_column("Derive Audit Columns", audit_derivations(SRC_OLTP))
    df.row_count("Count Ledger Entries", "User::RowsRead")
    df.oledb_destination(
        "raw SqlLoyaltyLedger",
        CONN_STAGING,
        "raw.SqlLoyaltyLedger",
        batch_size=100000,
    )

    init = pkg.add(init_variables("@[User::RowsRead] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="Loyalty.LoyaltyPointsLedger"))
    clear_lookback = pkg.add(
        ExecuteSql(
            "Clear Reread Window",
            CONN_STAGING,
            "DELETE FROM raw.SqlLoyaltyLedger "
            "WHERE TRY_CONVERT(bigint, LoyaltyLedgerID) IN "
            "      (SELECT TRY_CONVERT(bigint, LoyaltyLedgerID) FROM raw.SqlLoyaltyLedger "
            "       WHERE TRY_CONVERT(datetime2(3), LastEditedWhen) >= "
            "             DATEADD(day, -1 * ?, SYSUTCDATETIME()));",
            parameter_bindings=[("User::ExpiryLookbackDays", 0, "LONG")],
        )
    )
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("Loyalty.LoyaltyPointsLedger"))
    rows = pkg.add(log_row_count("raw.SqlLoyaltyLedger"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, clear_lookback, extract, setwm, rows, done)
    return pkg


PACKAGE_BUILDERS = [
    ext_sql_orders,
    ext_sql_order_lines,
    ext_sql_invoices,
    ext_sql_invoice_lines,
    ext_sql_promotions,
    ext_sql_sales_territories,
    ext_sql_customer_segments,
    ext_sql_stock_items,
    ext_sql_stock_movements,
    ext_sql_stock_transfers,
    ext_sql_shipments,
    ext_sql_shipment_lines,
    ext_sql_returns,
    ext_sql_credit_notes,
    ext_sql_web_sessions,
    ext_sql_loyalty_ledger,
    ext_sql_customer_transactions,
    ext_sql_supplier_transactions,
    ext_sql_people,
    ext_sql_cities,
    ext_sql_payment_methods,
    ext_sql_transaction_types,
]


def main():
    written = []
    names = []
    for builder in PACKAGE_BUILDERS:
        pkg = builder()
        names.append(pkg.name)
        written.append(pkg.write(os.path.join(HERE, pkg.name + ".dtsx")))
    written.extend(project.write_project(HERE, PROJECT_NAME, names, PROJECT_CONNECTIONS))
    for path in written:
        print(os.path.relpath(path, REPO_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
