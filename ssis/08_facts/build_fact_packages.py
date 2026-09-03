#!/usr/bin/env python3
"""Emit the WWI_Facts SSIS project (ssis/08_facts).

Twenty-four fact loads: transaction facts driven by the etl watermark, periodic
snapshots that are re-runnable for a period, two accumulating snapshots
(order-to-cash and procure-to-pay) that update milestone dates and lag measures
in place, plus the deduplication and correction passes finance asked for after
the 2016 restatement.

Every load resolves dimension surrogate keys by lookup, falls back to the
unknown member (key 0) when a lookup misses, and queues the miss so
DIM_Rekey_LateArriving can repoint the row once the dimension catches up.

Run from the repository root:

    python3 ssis/08_facts/build_fact_packages.py
"""

from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
OUT_DIR = os.path.join(REPO_ROOT, "ssis", "08_facts")
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import (  # noqa: E402
    CONN_DW,
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

PROJECT_NAME = "WWI_Facts"
PROJECT_CONNECTIONS = ("WWI_Staging_DB", "WWI_DW_Destination_DB")

DEC = lambda name, precision=18, scale=2: Column(name, "numeric", precision=precision, scale=scale)  # noqa: E731


def _fact_package(name, description, extra_variables=None, extra_parameters=()):
    pkg = new_package(
        name,
        description,
        source_system="SQLSTG",
        connections=(CONN_STAGING, CONN_DW),
        extra_variables=[
            ("RowsUnknownMember", 0, "int"),
            ("RowsHeldForRetry", 0, "int"),
            ("BusinessDateFrom", "1900-01-01", "string"),
            ("BusinessDateTo", "1900-01-01", "string"),
        ] + list(extra_variables or []),
    )
    for pname, pvalue, ptype, pdesc in extra_parameters:
        pkg.add_parameter(pname, pvalue, dtype=ptype, description=pdesc)
    return pkg


def _init_window(name="Init Batch Variables"):
    return Expression(
        name,
        '@[User::BusinessDateFrom] = (DT_WSTR, 10)(DT_DBDATE)@[User::WatermarkFrom]',
    )


def _log_rejects(object_name, name="Log Rejected Records"):
    return ExecuteSql(
        name,
        CONN_STAGING,
        "EXEC etl.usp_LogRejectedRecord @PackageExecutionId = ?, @ObjectName = N'%s', "
        "@RejectReason = N'Routed by the package reject path', @RejectRowCount = ?;" % object_name,
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("User::RowsRejected", 1, "LONG"),
        ],
        is_stored_procedure=True,
    )


def _queue_late_arrivers(dimension_name, source_package, name=None):
    """Push unknown-member hits onto the late-arriving queue for the rekey pass."""
    return ExecuteSql(
        name or "Queue Late Arriving %s" % dimension_name,
        CONN_STAGING,
        "INSERT INTO work.LateArrivingDimensionQueue "
        "    (DimensionName, BusinessKey, PlaceholderKey, SourcePackageName, QueuedAt, RetryCount) "
        "SELECT DISTINCT N'%s', r.BusinessKey, 0, N'%s', GETDATE(), 0 "
        "FROM err.RejectedLookupFailure AS r "
        "WHERE r.DimensionName = N'%s' AND r.PackageExecutionId = ? "
        "  AND NOT EXISTS (SELECT 1 FROM work.LateArrivingDimensionQueue AS q "
        "                  WHERE q.DimensionName = r.DimensionName AND q.BusinessKey = r.BusinessKey "
        "                    AND q.ResolvedAt IS NULL);" % (dimension_name, source_package, dimension_name),
        parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
    )


# --- shared dimension lookups ------------------------------------------------
# The estate has resolved keys this way since the first DW build: a full-cache
# lookup per dimension, no-match redirected, then a derived column that swaps in
# the unknown member so the fact row still lands.


def _lookup_customer(flow, name="Lookup Customer Key", no_match="RD"):
    flow.lookup(
        name,
        CONN_DW,
        "SELECT [Customer Key] AS CustomerKey, [WWI Customer ID] AS CustomerBusinessKey "
        "FROM Dimension.Customer WHERE [Is Current Row] = 1;",
        ["CustomerBusinessKey"],
        [int_col("CustomerKey")],
        no_match=no_match,
    )


def _lookup_stock_item(flow, name="Lookup Stock Item Key", no_match="RD"):
    flow.lookup(
        name,
        CONN_DW,
        "SELECT [Stock Item Key] AS StockItemKey, [WWI Stock Item ID] AS StockItemBusinessKey "
        "FROM [Dimension].[Stock Item] WHERE [Is Current Row] = 1;",
        ["StockItemBusinessKey"],
        [int_col("StockItemKey")],
        no_match=no_match,
    )


def _lookup_supplier(flow, name="Lookup Supplier Key", no_match="RD"):
    flow.lookup(
        name,
        CONN_DW,
        "SELECT [Supplier Key] AS SupplierKey, [WWI Supplier ID] AS SupplierBusinessKey "
        "FROM [Dimension].[Supplier] WHERE [Is Current Row] = 1;",
        ["SupplierBusinessKey"],
        [int_col("SupplierKey")],
        no_match=no_match,
    )


def _lookup_date(flow, join_column, output_column, name):
    """Role-playing date dimension lookup.

    Dimension.Date keys on [Date], so the role key a fact stores is the date
    itself; the lookup exists to reject dates the dimension has not been
    populated for rather than to translate the value.
    """
    flow.lookup(
        name,
        CONN_DW,
        "SELECT [Date] AS %s, [Date] AS %s FROM Dimension.Date;" % (join_column, output_column),
        [join_column],
        [date_col(output_column)],
        no_match="IG",
    )


def _lookup_fx(flow, name="Lookup Effective FX Rate", join_columns=("TransactionCurrency", "FxRateDate")):
    flow.lookup(
        name,
        CONN_STAGING,
        "SELECT r.CurrencyCode AS TransactionCurrency, r.RateDate AS FxRateDate, "
        "r.RateToUsd AS FxRateToUsd, r.RateSourceCode AS FxRateSource "
        "FROM stg.CurrencyRate AS r WHERE r.RateTypeCode = N'CLOSE';",
        list(join_columns),
        [DEC("FxRateToUsd", 18, 8), str_col("FxRateSource", 8)],
        no_match="RD",
    )


def _write(pkg):
    return pkg.write(os.path.join(OUT_DIR, pkg.name + ".dtsx"))


SALE_LINE_COLUMNS = [
    bigint_col("SaleLineBusinessKey"),
    str_col("InvoiceNumber", 20),
    int_col("InvoiceLineNumber"),
    str_col("CustomerBusinessKey", 20),
    bigint_col("StockItemBusinessKey"),
    int_col("SalespersonBusinessKey"),
    str_col("CityBusinessKey", 20),
    date_col("InvoiceDate"),
    date_col("DeliveryDate"),
    date_col("PaymentDueDate"),
    int_col("Quantity"),
    money_col("UnitPrice"),
    DEC("DiscountPercent", 5, 2),
    money_col("UnitCost"),
    money_col("FreightAmount"),
    str_col("TransactionCurrency", 3),
    str_col("PromotionBusinessKey", 20),
    str_col("SourceRowHash", 64),
]


def _sale_line_source(flow, region_code, extra_predicate=""):
    flow.oledb_source(
        "stg SaleLine %s" % region_code,
        CONN_STAGING,
        "SELECT sl.SaleLineBusinessKey, sl.InvoiceNumber, sl.InvoiceLineNumber, sl.CustomerBusinessKey, "
        "sl.StockItemBusinessKey, sl.SalespersonBusinessKey, sl.CityBusinessKey, sl.InvoiceDate, "
        "sl.DeliveryDate, sl.PaymentDueDate, sl.Quantity, sl.UnitPrice, sl.DiscountPercent, sl.UnitCost, "
        "sl.FreightAmount, sl.TransactionCurrency, sl.PromotionBusinessKey, sl.SourceRowHash "
        "FROM stg.SaleLine AS sl "
        "WHERE sl.RegionCode = N'%s' AND sl.LastModifiedAt > ? AND sl.LastModifiedAt <= ? %s "
        "ORDER BY sl.InvoiceDate, sl.InvoiceNumber, sl.InvoiceLineNumber;" % (region_code, extra_predicate),
        list(SALE_LINE_COLUMNS),
        timeout=3600,
    )


# ---------------------------------------------------------------------------
# Sale - three regional transaction fact loads
# ---------------------------------------------------------------------------


def build_fact_na_load_sale():
    pkg = _fact_package(
        "FACT_NA_Load_Sale",
        "Incremental transaction-fact load of Fact.Sale for North America. Sales tax is charged on "
        "the discounted line value at the destination state rate; USD is both transaction and "
        "reporting currency so no FX conversion is applied.",
        extra_variables=[("RowsTaxExempt", 0, "int")],
        extra_parameters=[
            ("RegionCode", "NA", "string", "Region filter applied to stg.SaleLine."),
            ("UseDestinationSourcing", "True", "bool",
             "Destination sourcing for sales tax; origin sourcing is still used by two states."),
        ],
    )
    flow = DataFlow("Load Fact Sale NA", "NA sale lines with sales-tax measures")
    _sale_line_source(flow, "NA")
    _lookup_customer(flow)
    _lookup_stock_item(flow)
    flow.lookup(
        "Lookup City Key",
        CONN_DW,
        "SELECT [City Key] AS CityKey, [WWI City ID] AS CityBusinessKey, "
        "[State Province Code] AS DestinationStateCode FROM Dimension.City WHERE [Is Current Row] = 1;",
        ["CityBusinessKey"],
        [int_col("CityKey"), str_col("DestinationStateCode", 10)],
        no_match="RD",
    )
    flow.lookup(
        "Lookup Salesperson Key",
        CONN_DW,
        "SELECT [Salesperson Key] AS SalespersonKey, [WWI Salesperson ID] AS SalespersonBusinessKey "
        "FROM Dimension.Salesperson WHERE [Is Current Row] = 1;",
        ["SalespersonBusinessKey"],
        [int_col("SalespersonKey")],
        no_match="RD",
    )
    flow.lookup(
        "Lookup State Sales Tax Rate",
        CONN_STAGING,
        "SELECT t.StateProvinceCode AS DestinationStateCode, t.CombinedRatePercent AS SalesTaxRatePercent, "
        "t.IsGroceryExempt FROM stg.TaxRate AS t WHERE t.TaxRegimeCode = N'SALESTAX';",
        ["DestinationStateCode"],
        [DEC("SalesTaxRatePercent", 6, 3), Column("IsGroceryExempt", "bool")],
        no_match="IG",
    )
    _lookup_date(flow, "InvoiceDate", "InvoiceDateKey", "Lookup Invoice Date Key")
    _lookup_date(flow, "DeliveryDate", "DeliveryDateKey", "Lookup Delivery Date Key")
    _lookup_date(flow, "PaymentDueDate", "PaymentDueDateKey", "Lookup Payment Due Date Key")
    flow.derived_column(
        "Calculate NA Measures",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("CityKeyResolved", "ISNULL(CityKey) ? 0 : CityKey", int_col("CityKeyResolved")),
            ("SalespersonKeyResolved", "ISNULL(SalespersonKey) ? 0 : SalespersonKey",
             int_col("SalespersonKeyResolved")),
            ("GrossAmount", "(DT_NUMERIC,18,2)Quantity * UnitPrice", money_col("GrossAmount")),
            ("DiscountAmount", "((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100)",
             money_col("DiscountAmount")),
            ("NetAmount",
             "((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))",
             money_col("NetAmount")),
            # Sales tax is applied after discount and never to exempt grocery lines.
            ("TaxAmount",
             "ISNULL(SalesTaxRatePercent) || IsGroceryExempt ? (DT_NUMERIC,18,2)0 : "
             "((((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))) * "
             "(SalesTaxRatePercent / 100))",
             money_col("TaxAmount")),
            ("TotalCostAmount", "(DT_NUMERIC,18,2)Quantity * UnitCost", money_col("TotalCostAmount")),
            ("MarginAmount",
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))) - "
             "((DT_NUMERIC,18,2)Quantity * UnitCost)",
             money_col("MarginAmount")),
            ("MarginPercent",
             "((DT_NUMERIC,18,2)Quantity * UnitPrice) == 0 ? (DT_NUMERIC,9,4)0 : "
             "((((DT_NUMERIC,18,2)Quantity * UnitPrice) - ((DT_NUMERIC,18,2)Quantity * UnitCost)) / "
             "((DT_NUMERIC,18,2)Quantity * UnitPrice)) * 100",
             DEC("MarginPercent", 9, 4)),
            ("FxRateToUsd", "(DT_NUMERIC,18,8)1", DEC("FxRateToUsd", 18, 8)),
            ("ReportingCurrency", '"USD"', str_col("ReportingCurrency", 3)),
            ("TaxRegimeCode", '"SALESTAX"', str_col("TaxRegimeCode", 10)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Sale Lines",
        [
            ("Missing Customer", "ISNULL(CustomerKey)"),
            ("Missing Stock Item", "ISNULL(StockItemKey)"),
            ("Zero Quantity", "Quantity == 0"),
        ],
        default_output="Loadable Lines",
    )
    flow.branch_destination("Insert Fact Sale", CONN_DW, "[Fact].[Sale]", "Route Sale Lines", "Loadable Lines")
    flow.branch_destination("Hold Missing Customer", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Sale Lines", "Missing Customer")
    flow.branch_destination("Hold Missing Stock Item", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Sale Lines", "Missing Stock Item")
    flow.branch_destination("Reject Zero Quantity", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Route Sale Lines", "Zero Quantity")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]", "stg SaleLine NA")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Sale.NA"))
    load = pkg.add(DataFlowTask(flow))
    queue_cust = pkg.add(_queue_late_arrivers("Customer", "FACT_NA_Load_Sale"))
    queue_item = pkg.add(_queue_late_arrivers("Stock Item", "FACT_NA_Load_Sale"))
    unknown_fallback = pkg.add(
        exec_proc(
            "Load Held Rows Against Unknown Member",
            "EXEC Integration.LoadFactSale @RegionCode = N'NA', @UseUnknownMember = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Sale.NA"))
    rejects = pkg.add(_log_rejects("Fact.Sale"))
    counts = pkg.add(log_row_count("Fact.Sale"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, queue_cust, queue_item, unknown_fallback, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_eu_load_sale():
    pkg = _fact_package(
        "FACT_EU_Load_Sale",
        "Incremental transaction-fact load of Fact.Sale for the EU. VAT is charged on the net line "
        "value at the destination country rate, cross-border B2B lines are reverse-charged at zero, "
        "and every line is converted to the EUR reporting currency at the effective closing rate.",
        extra_variables=[("RowsReverseCharged", 0, "int"), ("RowsMissingFxRate", 0, "int")],
        extra_parameters=[
            ("RegionCode", "EU", "string", "Region filter applied to stg.SaleLine."),
            ("ReportingCurrency", "EUR", "string", "EU management reporting currency."),
        ],
    )
    flow = DataFlow("Load Fact Sale EU", "EU sale lines with VAT and FX conversion")
    _sale_line_source(flow, "EU")
    _lookup_customer(flow)
    _lookup_stock_item(flow)
    flow.lookup(
        "Lookup Ship To Country",
        CONN_DW,
        "SELECT [City Key] AS CityKey, [WWI City ID] AS CityBusinessKey, "
        "[Country ISO Code] AS ShipToCountryIsoCode FROM Dimension.City WHERE [Is Current Row] = 1;",
        ["CityBusinessKey"],
        [int_col("CityKey"), str_col("ShipToCountryIsoCode", 2)],
        no_match="RD",
    )
    flow.lookup(
        "Lookup Customer VAT Status",
        CONN_STAGING,
        "SELECT c.CustomerBusinessKey, c.VatRegistrationNumber AS CustomerVatNumber, "
        "c.CountryIsoCode AS CustomerCountryIsoCode FROM stg.Customer AS c WHERE c.RegionCode = N'EU';",
        ["CustomerBusinessKey"],
        [str_col("CustomerVatNumber", 20), str_col("CustomerCountryIsoCode", 2)],
        no_match="IG",
    )
    flow.lookup(
        "Lookup Country VAT Rate",
        CONN_STAGING,
        "SELECT t.CountryIsoCode AS ShipToCountryIsoCode, t.StandardVatRatePercent, t.ReducedVatRatePercent "
        "FROM stg.TaxRate AS t WHERE t.TaxRegimeCode = N'VAT';",
        ["ShipToCountryIsoCode"],
        [DEC("StandardVatRatePercent", 5, 2), DEC("ReducedVatRatePercent", 5, 2)],
        no_match="IG",
    )
    flow.derived_column(
        "Derive FX Rate Date",
        [("FxRateDate", "(DT_DBDATE)InvoiceDate", date_col("FxRateDate"))],
    )
    _lookup_fx(flow)
    _lookup_date(flow, "InvoiceDate", "InvoiceDateKey", "Lookup Invoice Date Key")
    _lookup_date(flow, "DeliveryDate", "DeliveryDateKey", "Lookup Delivery Date Key")
    flow.derived_column(
        "Calculate EU Measures",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("CityKeyResolved", "ISNULL(CityKey) ? 0 : CityKey", int_col("CityKeyResolved")),
            ("GrossAmount", "(DT_NUMERIC,18,2)Quantity * UnitPrice", money_col("GrossAmount")),
            ("DiscountAmount", "((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100)",
             money_col("DiscountAmount")),
            ("NetAmount",
             "((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))",
             money_col("NetAmount")),
            # Article 138 intra-community supply: the customer accounts for the
            # VAT, so the invoice carries a zero rate and a reverse-charge flag.
            ("IsReverseCharge",
             "!ISNULL(CustomerVatNumber) && LEN(TRIM(CustomerVatNumber)) > 0 && "
             "CustomerCountryIsoCode != ShipToCountryIsoCode",
             Column("IsReverseCharge", "bool")),
            ("VatRateApplied",
             "!ISNULL(CustomerVatNumber) && LEN(TRIM(CustomerVatNumber)) > 0 && "
             "CustomerCountryIsoCode != ShipToCountryIsoCode ? (DT_NUMERIC,5,2)0 : "
             "ISNULL(StandardVatRatePercent) ? (DT_NUMERIC,5,2)0 : StandardVatRatePercent",
             DEC("VatRateApplied", 5, 2)),
            ("TaxAmount",
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))) * "
             "(ISNULL(StandardVatRatePercent) ? (DT_NUMERIC,5,2)0 : StandardVatRatePercent) / 100",
             money_col("TaxAmount")),
            ("TotalCostAmount", "(DT_NUMERIC,18,2)Quantity * UnitCost", money_col("TotalCostAmount")),
            ("MarginAmount",
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))) - "
             "((DT_NUMERIC,18,2)Quantity * UnitCost)",
             money_col("MarginAmount")),
            ("MarginPercent",
             "((DT_NUMERIC,18,2)Quantity * UnitPrice) == 0 ? (DT_NUMERIC,9,4)0 : "
             "((((DT_NUMERIC,18,2)Quantity * UnitPrice) - ((DT_NUMERIC,18,2)Quantity * UnitCost)) / "
             "((DT_NUMERIC,18,2)Quantity * UnitPrice)) * 100",
             DEC("MarginPercent", 9, 4)),
            ("NetAmountReporting",
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))) * "
             "(ISNULL(FxRateToUsd) ? (DT_NUMERIC,18,8)0 : FxRateToUsd)",
             money_col("NetAmountReporting")),
            ("IntrastatCommodityFlag", "ShipToCountryIsoCode != CustomerCountryIsoCode",
             Column("IntrastatCommodityFlag", "bool")),
            ("TaxRegimeCode", '"VAT"', str_col("TaxRegimeCode", 10)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route EU Sale Lines",
        [
            ("Missing FX Rate", "ISNULL(FxRateToUsd)"),
            ("Missing Customer", "ISNULL(CustomerKey)"),
            ("Reverse Charged Lines", "IsReverseCharge"),
        ],
        default_output="Domestic Lines",
    )
    flow.branch_destination("Insert Domestic Lines", CONN_DW, "[Fact].[Sale]",
                            "Route EU Sale Lines", "Domestic Lines")
    flow.branch_destination("Insert Reverse Charged Lines", CONN_DW, "[Fact].[Sale]",
                            "Route EU Sale Lines", "Reverse Charged Lines")
    flow.branch_destination("Hold Lines Without FX Rate", CONN_STAGING, "[work].[CurrencyConversionScratch]",
                            "Route EU Sale Lines", "Missing FX Rate")
    flow.branch_destination("Hold Missing Customer", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route EU Sale Lines", "Missing Customer")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]", "stg SaleLine EU")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Sale.EU"))
    clear = pkg.add(truncate("[work].[CurrencyConversionScratch]", connection=CONN_STAGING,
                             name="Clear FX Scratch Table"))
    load = pkg.add(DataFlowTask(flow))
    retry_fx = pkg.add(
        ExecuteSql(
            "Retry Held Lines With Prior Day Rate",
            CONN_STAGING,
            "UPDATE s SET s.FxRateToUsd = r.RateToUsd, s.FxRateSourceCode = N'PRIORDAY' "
            "FROM work.CurrencyConversionScratch AS s "
            "CROSS APPLY (SELECT TOP (1) cr.RateToUsd FROM stg.CurrencyRate AS cr "
            "             WHERE cr.CurrencyCode = s.TransactionCurrency AND cr.RateTypeCode = N'CLOSE' "
            "               AND cr.RateDate < s.FxRateDate ORDER BY cr.RateDate DESC) AS r "
            "WHERE s.FxRateToUsd IS NULL;",
        )
    )
    load_held = pkg.add(
        exec_proc(
            "Load Retried Lines",
            "EXEC Integration.LoadFactSale @RegionCode = N'EU', @LoadHeldRows = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    queue_cust = pkg.add(_queue_late_arrivers("Customer", "FACT_EU_Load_Sale"))
    mark = pkg.add(set_watermark("Fact.Sale.EU"))
    rejects = pkg.add(_log_rejects("Fact.Sale"))
    counts = pkg.add(log_row_count("Fact.Sale"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, clear, load, retry_fx, load_held, queue_cust, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_apac_load_sale():
    pkg = _fact_package(
        "FACT_APAC_Load_Sale",
        "Incremental transaction-fact load of Fact.Sale for APAC. List prices are GST-inclusive in "
        "AU/NZ/IN so the tax is backed out of the gross, the fiscal period follows the April-March "
        "calendar, and distributor rebate accruals are carried as an extra measure.",
        extra_variables=[("RowsGstInclusive", 0, "int")],
        extra_parameters=[
            ("RegionCode", "APAC", "string", "Region filter applied to stg.SaleLine."),
            ("FiscalYearStartMonth", 4, "int", "APAC fiscal year starts in April."),
        ],
    )
    flow = DataFlow("Load Fact Sale APAC", "APAC sale lines with GST-inclusive pricing")
    _sale_line_source(flow, "APAC")
    _lookup_customer(flow)
    _lookup_stock_item(flow)
    flow.lookup(
        "Lookup Ship To Country",
        CONN_DW,
        "SELECT [City Key] AS CityKey, [WWI City ID] AS CityBusinessKey, "
        "[Country ISO Code] AS ShipToCountryIsoCode FROM Dimension.City WHERE [Is Current Row] = 1;",
        ["CityBusinessKey"],
        [int_col("CityKey"), str_col("ShipToCountryIsoCode", 2)],
        no_match="RD",
    )
    flow.lookup(
        "Lookup GST Rate",
        CONN_STAGING,
        "SELECT t.CountryIsoCode AS ShipToCountryIsoCode, t.GstRatePercent, t.IsPriceInclusive "
        "FROM stg.TaxRate AS t WHERE t.TaxRegimeCode = N'GST';",
        ["ShipToCountryIsoCode"],
        [DEC("GstRatePercent", 5, 2), Column("IsPriceInclusive", "bool")],
        no_match="IG",
    )
    flow.derived_column("Derive FX Rate Date", [("FxRateDate", "(DT_DBDATE)InvoiceDate", date_col("FxRateDate"))])
    _lookup_fx(flow)
    _lookup_date(flow, "InvoiceDate", "InvoiceDateKey", "Lookup Invoice Date Key")
    _lookup_date(flow, "DeliveryDate", "DeliveryDateKey", "Lookup Delivery Date Key")
    flow.derived_column(
        "Calculate APAC Measures",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("CityKeyResolved", "ISNULL(CityKey) ? 0 : CityKey", int_col("CityKeyResolved")),
            ("GstRateApplied", "ISNULL(GstRatePercent) ? (DT_NUMERIC,5,2)0 : GstRatePercent",
             DEC("GstRateApplied", 5, 2)),
            ("GrossAmount", "(DT_NUMERIC,18,2)Quantity * UnitPrice", money_col("GrossAmount")),
            ("DiscountAmount", "((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100)",
             money_col("DiscountAmount")),
            # GST-inclusive pricing: the tax is extracted from the gross rather
            # than added to the net, which is why the arithmetic here differs
            # from the NA and EU packages.
            ("TaxAmount",
             "ISNULL(IsPriceInclusive) || !IsPriceInclusive ? "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * "
             "(ISNULL(GstRatePercent) ? (DT_NUMERIC,5,2)0 : GstRatePercent) / 100) : "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) / "
             "(1 + ((ISNULL(GstRatePercent) ? (DT_NUMERIC,5,2)0 : GstRatePercent) / 100))))",
             money_col("TaxAmount")),
            ("NetAmount",
             "ISNULL(IsPriceInclusive) || !IsPriceInclusive ? "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))) : "
             "((((DT_NUMERIC,18,2)Quantity * UnitPrice) / "
             "(1 + ((ISNULL(GstRatePercent) ? (DT_NUMERIC,5,2)0 : GstRatePercent) / 100))) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100)))",
             money_col("NetAmount")),
            ("TotalCostAmount", "(DT_NUMERIC,18,2)Quantity * UnitCost", money_col("TotalCostAmount")),
            ("MarginAmount",
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))) - "
             "((DT_NUMERIC,18,2)Quantity * UnitCost)",
             money_col("MarginAmount")),
            ("MarginPercent",
             "((DT_NUMERIC,18,2)Quantity * UnitPrice) == 0 ? (DT_NUMERIC,9,4)0 : "
             "((((DT_NUMERIC,18,2)Quantity * UnitPrice) - ((DT_NUMERIC,18,2)Quantity * UnitCost)) / "
             "((DT_NUMERIC,18,2)Quantity * UnitPrice)) * 100",
             DEC("MarginPercent", 9, 4)),
            ("DistributorRebateAccrual",
             "((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DT_NUMERIC,18,2)0.025",
             money_col("DistributorRebateAccrual")),
            ("NetAmountReporting",
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) - "
             "(((DT_NUMERIC,18,2)Quantity * UnitPrice) * (DiscountPercent / 100))) * "
             "(ISNULL(FxRateToUsd) ? (DT_NUMERIC,18,8)0 : FxRateToUsd)",
             money_col("NetAmountReporting")),
            ("FiscalYearLabel",
             'DATEPART("mm", InvoiceDate) >= @[$Package::FiscalYearStartMonth] ? '
             '"FY" + (DT_WSTR,4)(YEAR(InvoiceDate) + 1) : "FY" + (DT_WSTR,4)YEAR(InvoiceDate)',
             str_col("FiscalYearLabel", 6)),
            ("TaxRegimeCode", '"GST"', str_col("TaxRegimeCode", 10)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route APAC Sale Lines",
        [
            ("Missing FX Rate", "ISNULL(FxRateToUsd)"),
            ("Missing Customer", "ISNULL(CustomerKey)"),
            ("GST Inclusive Lines", "!ISNULL(IsPriceInclusive) && IsPriceInclusive"),
        ],
        default_output="GST Exclusive Lines",
    )
    flow.branch_destination("Insert GST Inclusive Lines", CONN_DW, "[Fact].[Sale]",
                            "Route APAC Sale Lines", "GST Inclusive Lines")
    flow.branch_destination("Insert GST Exclusive Lines", CONN_DW, "[Fact].[Sale]",
                            "Route APAC Sale Lines", "GST Exclusive Lines")
    flow.branch_destination("Hold Lines Without FX Rate", CONN_STAGING, "[work].[CurrencyConversionScratch]",
                            "Route APAC Sale Lines", "Missing FX Rate")
    flow.branch_destination("Hold Missing Customer", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route APAC Sale Lines", "Missing Customer")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]", "stg SaleLine APAC")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Sale.APAC"))
    load = pkg.add(DataFlowTask(flow))
    queue_cust = pkg.add(_queue_late_arrivers("Customer", "FACT_APAC_Load_Sale"))
    rebate = pkg.add(
        exec_proc(
            "Post Distributor Rebate Accrual",
            "EXEC Integration.LoadFactSale @RegionCode = N'APAC', @PostRebateAccrual = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Sale.APAC"))
    rejects = pkg.add(_log_rejects("Fact.Sale"))
    counts = pkg.add(log_row_count("Fact.Sale"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, queue_cust, rebate, mark, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Order to cash
# ---------------------------------------------------------------------------


def build_fact_load_order():
    pkg = _fact_package(
        "FACT_Load_Order",
        "Incremental load of Fact.Order at order-line grain. Orders for customers that have not "
        "reached the warehouse yet are held in work.OrderLineEnriched and retried on the next run "
        "rather than being pinned to the unknown member.",
        extra_variables=[("BackorderedLines", 0, "int")],
        extra_parameters=[
            ("HoldRetryLimit", 3, "int", "Runs an order line may be held before it is loaded against the "
                                         "unknown member."),
        ],
    )
    columns = [
        bigint_col("OrderLineBusinessKey"),
        str_col("OrderNumber", 20),
        int_col("OrderLineNumber"),
        str_col("CustomerBusinessKey", 20),
        bigint_col("StockItemBusinessKey"),
        int_col("SalespersonBusinessKey"),
        date_col("OrderDate"),
        date_col("ExpectedDeliveryDate"),
        int_col("QuantityOrdered"),
        int_col("QuantityPicked"),
        money_col("UnitPrice"),
        DEC("DiscountPercent", 5, 2),
        str_col("OrderStatusCode", 8),
        str_col("BackorderNumber", 20),
        str_col("TransactionCurrency", 3),
    ]
    flow = DataFlow("Load Fact Order", "Order lines with early-arriving-fact holding")
    flow.oledb_source(
        "stg Order",
        CONN_STAGING,
        "SELECT o.OrderLineBusinessKey, o.OrderNumber, o.OrderLineNumber, o.CustomerBusinessKey, "
        "o.StockItemBusinessKey, o.SalespersonBusinessKey, o.OrderDate, o.ExpectedDeliveryDate, "
        "o.QuantityOrdered, o.QuantityPicked, o.UnitPrice, o.DiscountPercent, o.OrderStatusCode, "
        "o.BackorderNumber, o.TransactionCurrency "
        "FROM stg.[Order] AS o WHERE o.LastModifiedAt > ? AND o.LastModifiedAt <= ? "
        "ORDER BY o.OrderDate, o.OrderNumber, o.OrderLineNumber;",
        columns,
        timeout=3600,
    )
    _lookup_customer(flow)
    _lookup_stock_item(flow)
    _lookup_date(flow, "OrderDate", "OrderDateKey", "Lookup Order Date Key")
    _lookup_date(flow, "ExpectedDeliveryDate", "ExpectedDeliveryDateKey", "Lookup Expected Delivery Date Key")
    flow.derived_column(
        "Derive Order Measures",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("OrderedGrossAmount", "(DT_NUMERIC,18,2)QuantityOrdered * UnitPrice", money_col("OrderedGrossAmount")),
            ("DiscountAmount", "((DT_NUMERIC,18,2)QuantityOrdered * UnitPrice) * (DiscountPercent / 100)",
             money_col("DiscountAmount")),
            ("OrderedNetAmount",
             "((DT_NUMERIC,18,2)QuantityOrdered * UnitPrice) - "
             "(((DT_NUMERIC,18,2)QuantityOrdered * UnitPrice) * (DiscountPercent / 100))",
             money_col("OrderedNetAmount")),
            ("QuantityOutstanding", "QuantityOrdered - QuantityPicked", int_col("QuantityOutstanding")),
            ("IsBackordered",
             'QuantityPicked < QuantityOrdered && !ISNULL(BackorderNumber) && LEN(TRIM(BackorderNumber)) > 0',
             Column("IsBackordered", "bool")),
            ("FillRatePercent",
             "QuantityOrdered == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)QuantityPicked / (DT_NUMERIC,9,4)QuantityOrdered) * 100",
             DEC("FillRatePercent", 9, 4)),
            # Degenerate dimensions: the order and backorder numbers live on the
            # fact, there has never been an order dimension.
            ("OrderNumberDegenerate", "OrderNumber", str_col("OrderNumberDegenerate", 20)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Order Lines",
        [
            ("Early Arriving Customer", "ISNULL(CustomerKey)"),
            ("Early Arriving Stock Item", "ISNULL(StockItemKey)"),
            ("Cancelled Lines", 'OrderStatusCode == "CANC"'),
            ("Backordered Lines", "IsBackordered"),
        ],
        default_output="Complete Lines",
    )
    flow.branch_destination("Insert Complete Lines", CONN_DW, "[Fact].[Order]",
                            "Route Order Lines", "Complete Lines")
    flow.branch_destination("Insert Backordered Lines", CONN_DW, "[Fact].[Order]",
                            "Route Order Lines", "Backordered Lines")
    flow.branch_destination("Hold Early Arriving Customer", CONN_STAGING, "[work].[OrderLineEnriched]",
                            "Route Order Lines", "Early Arriving Customer")
    flow.branch_destination("Hold Early Arriving Stock Item", CONN_STAGING, "[work].[OrderLineEnriched]",
                            "Route Order Lines", "Early Arriving Stock Item")
    flow.branch_destination("Reject Cancelled Lines", CONN_STAGING, "[err].[RejectedOrderLine]",
                            "Route Order Lines", "Cancelled Lines")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedOrderLine]", "stg Order")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Order"))
    load = pkg.add(DataFlowTask(flow))
    retry = pkg.add(
        ExecuteSql(
            "Age Held Order Lines",
            CONN_STAGING,
            "UPDATE work.OrderLineEnriched SET HoldRetryCount = HoldRetryCount + 1, "
            "LastRetriedAt = GETDATE() WHERE LoadedAt IS NULL;",
        )
    )
    queue = pkg.add(_queue_late_arrivers("Customer", "FACT_Load_Order"))
    release = pkg.add(
        exec_proc(
            "Release Held Lines Past Retry Limit",
            "EXEC Integration.LoadFactOrder @LoadHeldRows = 1, @HoldRetryLimit = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::HoldRetryLimit", 0, "LONG"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    mark = pkg.add(set_watermark("Fact.Order"))
    rejects = pkg.add(_log_rejects("Fact.Order"))
    counts = pkg.add(log_row_count("Fact.Order"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, retry, queue, release, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_payment():
    pkg = _fact_package(
        "FACT_Load_Payment",
        "Incremental load of Fact.Payment (customer receipts). Payments are deduplicated on the "
        "bank reference because the cash-application file is re-sent whenever a bank rejects a "
        "batch, and unapplied cash is carried as its own measure.",
        extra_variables=[("DuplicateReferences", 0, "int"), ("UnappliedCashRows", 0, "int")],
    )
    columns = [
        bigint_col("PaymentBusinessKey"),
        str_col("BankReference", 40),
        str_col("CustomerBusinessKey", 20),
        date_col("PaymentDate"),
        date_col("ValueDate"),
        money_col("PaymentAmount"),
        money_col("AllocatedAmount"),
        str_col("TransactionCurrency", 3),
        str_col("PaymentMethodCode", 8),
        str_col("RemittanceAdviceNumber", 30),
    ]
    flow = DataFlow("Load Fact Payment", "Customer receipts with reference-level deduplication")
    flow.oledb_source(
        "stg Payment",
        CONN_STAGING,
        "SELECT p.PaymentBusinessKey, p.BankReference, p.CustomerBusinessKey, p.PaymentDate, p.ValueDate, "
        "p.PaymentAmount, p.AllocatedAmount, p.TransactionCurrency, p.PaymentMethodCode, "
        "p.RemittanceAdviceNumber "
        "FROM stg.Payment AS p WHERE p.LastModifiedAt > ? AND p.LastModifiedAt <= ?;",
        columns,
        timeout=1800,
    )
    flow.sort("Sort By Bank Reference", ["BankReference", "PaymentDate"], eliminate_duplicates=True)
    _lookup_customer(flow)
    _lookup_date(flow, "PaymentDate", "PaymentDateKey", "Lookup Payment Date Key")
    _lookup_date(flow, "ValueDate", "ValueDateKey", "Lookup Value Date Key")
    flow.derived_column("Derive FX Rate Date", [("FxRateDate", "(DT_DBDATE)ValueDate", date_col("FxRateDate"))])
    _lookup_fx(flow)
    flow.derived_column(
        "Derive Payment Measures",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("UnappliedAmount", "PaymentAmount - AllocatedAmount", money_col("UnappliedAmount")),
            ("IsFullyApplied", "PaymentAmount == AllocatedAmount", Column("IsFullyApplied", "bool")),
            ("PaymentAmountReporting",
             "PaymentAmount * (ISNULL(FxRateToUsd) ? (DT_NUMERIC,18,8)1 : FxRateToUsd)",
             money_col("PaymentAmountReporting")),
            ("SettlementLagDays", 'DATEDIFF("Day", PaymentDate, ValueDate)', int_col("SettlementLagDays")),
            ("PaymentChannelCode",
             'PaymentMethodCode == "ACH" || PaymentMethodCode == "SEPA" ? "ELECTRONIC" : '
             'PaymentMethodCode == "CHQ" ? "PAPER" : PaymentMethodCode == "CARD" ? "CARD" : "OTHER"',
             str_col("PaymentChannelCode", 12)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Payments",
        [
            ("Unmatched Customer", "ISNULL(CustomerKey)"),
            ("Unapplied Cash", "AllocatedAmount == 0"),
            ("Partially Applied", "AllocatedAmount > 0 && AllocatedAmount < PaymentAmount"),
        ],
        default_output="Fully Applied",
    )
    flow.branch_destination("Insert Fully Applied", CONN_DW, "[Fact].[Payment]",
                            "Route Payments", "Fully Applied")
    flow.branch_destination("Insert Partially Applied", CONN_DW, "[Fact].[Payment]",
                            "Route Payments", "Partially Applied")
    flow.branch_destination("Insert Unapplied Cash", CONN_DW, "[Fact].[Payment]",
                            "Route Payments", "Unapplied Cash")
    flow.branch_destination("Hold Unmatched Customer", CONN_STAGING, "[work].[PaymentMatched]",
                            "Route Payments", "Unmatched Customer")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedPayment]", "stg Payment")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Payment"))
    clear = pkg.add(truncate("[work].[PaymentMatched]", connection=CONN_STAGING, name="Clear Payment Match Table"))
    load = pkg.add(DataFlowTask(flow))
    dedupe = pkg.add(
        ExecuteSql(
            "Remove Reprocessed Bank Batches",
            CONN_DW,
            "WITH ranked AS (SELECT [Payment Key], "
            "  ROW_NUMBER() OVER (PARTITION BY [Bank Reference], [Payment Date] "
            "                     ORDER BY [Lineage Key] DESC) AS rn "
            "  FROM [Fact].[Payment] WHERE [Bank Reference] IS NOT NULL) "
            "DELETE FROM [Fact].[Payment] WHERE [Payment Key] IN "
            "(SELECT [Payment Key] FROM ranked WHERE rn > 1);",
        )
    )
    match = pkg.add(
        exec_proc(
            "Match Held Payments By Remittance",
            "EXEC Integration.LoadFactPayment @MatchHeldRows = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Payment"))
    rejects = pkg.add(_log_rejects("Fact.Payment"))
    counts = pkg.add(log_row_count("Fact.Payment"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, clear, load, dedupe, match, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_customer_transaction():
    pkg = _fact_package(
        "FACT_Load_CustomerTransaction",
        "Incremental load of Fact.Customer Transaction (the AR sub-ledger). Carries the outstanding "
        "balance and the aging bucket at load time, with the bucket boundaries differing by region "
        "because EU terms are net-30 and APAC terms are net-60.",
        extra_variables=[("RowsPastDue", 0, "int")],
    )
    columns = [
        bigint_col("CustomerTransactionBusinessKey"),
        str_col("CustomerBusinessKey", 20),
        str_col("TransactionTypeCode", 8),
        str_col("InvoiceNumber", 20),
        date_col("TransactionDate"),
        date_col("DueDate"),
        money_col("TransactionAmount"),
        money_col("OutstandingBalance"),
        str_col("TransactionCurrency", 3),
        str_col("RegionCode", 6),
    ]
    flow = DataFlow("Load Fact Customer Transaction", "AR sub-ledger with regional aging buckets")
    flow.oledb_source(
        "stg CustomerTransaction",
        CONN_STAGING,
        "SELECT ct.CustomerTransactionBusinessKey, ct.CustomerBusinessKey, ct.TransactionTypeCode, "
        "ct.InvoiceNumber, ct.TransactionDate, ct.DueDate, ct.TransactionAmount, ct.OutstandingBalance, "
        "ct.TransactionCurrency, ct.RegionCode "
        "FROM stg.CustomerTransaction AS ct WHERE ct.LastModifiedAt > ? AND ct.LastModifiedAt <= ?;",
        columns,
        timeout=1800,
    )
    _lookup_customer(flow)
    _lookup_date(flow, "TransactionDate", "TransactionDateKey", "Lookup Transaction Date Key")
    _lookup_date(flow, "DueDate", "DueDateKey", "Lookup Due Date Key")
    flow.derived_column(
        "Derive Aging Attributes",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("DaysPastDue", 'DATEDIFF("Day", DueDate, GETDATE())', int_col("DaysPastDue")),
            # The aging ladder differs by region because the standard terms do.
            ("AgingBucketCode",
             'DATEDIFF("Day", DueDate, GETDATE()) <= 0 ? "CURRENT" : '
             'RegionCode == "EU" ? (DATEDIFF("Day", DueDate, GETDATE()) <= 30 ? "1-30" : '
             'DATEDIFF("Day", DueDate, GETDATE()) <= 60 ? "31-60" : "60+") : '
             'RegionCode == "APAC" ? (DATEDIFF("Day", DueDate, GETDATE()) <= 60 ? "1-60" : '
             'DATEDIFF("Day", DueDate, GETDATE()) <= 120 ? "61-120" : "120+") : '
             '(DATEDIFF("Day", DueDate, GETDATE()) <= 30 ? "1-30" : '
             'DATEDIFF("Day", DueDate, GETDATE()) <= 60 ? "31-60" : '
             'DATEDIFF("Day", DueDate, GETDATE()) <= 90 ? "61-90" : "90+")',
             str_col("AgingBucketCode", 10)),
            ("IsDebitTransaction", 'TransactionTypeCode == "INV" || TransactionTypeCode == "DBN"',
             Column("IsDebitTransaction", "bool")),
            ("SignedAmount",
             'TransactionTypeCode == "INV" || TransactionTypeCode == "DBN" ? TransactionAmount '
             ': TransactionAmount * -1',
             money_col("SignedAmount")),
            ("InvoiceNumberDegenerate", "InvoiceNumber", str_col("InvoiceNumberDegenerate", 20)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route AR Transactions",
        [
            ("Unmatched Customer", "ISNULL(CustomerKey)"),
            ("Past Due", 'AgingBucketCode != "CURRENT"'),
        ],
        default_output="Current",
    )
    flow.branch_destination("Insert Current", CONN_DW, "[Fact].[Customer Transaction]",
                            "Route AR Transactions", "Current")
    flow.branch_destination("Insert Past Due", CONN_DW, "[Fact].[Customer Transaction]",
                            "Route AR Transactions", "Past Due")
    flow.branch_destination("Hold Unmatched Customer", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route AR Transactions", "Unmatched Customer")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedPayment]",
                            "stg CustomerTransaction")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Customer Transaction"))
    load = pkg.add(DataFlowTask(flow))
    restate = pkg.add(
        exec_proc(
            "Restate Outstanding Balances",
            "EXEC Integration.LoadFactCustomerTransaction @RestateBalances = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    queue = pkg.add(_queue_late_arrivers("Customer", "FACT_Load_CustomerTransaction"))
    mark = pkg.add(set_watermark("Fact.Customer Transaction"))
    rejects = pkg.add(_log_rejects("Fact.Customer Transaction"))
    counts = pkg.add(log_row_count("Fact.Customer Transaction"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, restate, queue, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_loyalty_points():
    pkg = _fact_package(
        "FACT_Load_LoyaltyPoints",
        "Incremental load of Fact.Loyalty Points. Earn rates, expiry rules and the cash value of a "
        "point differ per region, and expiry rows are generated here rather than by the source "
        "system, which has never modelled them.",
        extra_variables=[("PointsExpiredRows", 0, "int")],
        extra_parameters=[
            ("ExpiryMonthsNa", 24, "int", "NA points expire two years after the earning transaction."),
            ("ExpiryMonthsEu", 36, "int", "EU consumer law forced a three-year expiry."),
        ],
    )
    columns = [
        bigint_col("LoyaltyEventBusinessKey"),
        str_col("CustomerBusinessKey", 20),
        str_col("LoyaltyProgramCode", 8),
        str_col("EventTypeCode", 8),
        date_col("EventDate"),
        int_col("PointsQuantity"),
        money_col("QualifyingSpendAmount"),
        str_col("TransactionCurrency", 3),
        str_col("RegionCode", 6),
        str_col("SourceInvoiceNumber", 20),
    ]
    flow = DataFlow("Load Fact Loyalty Points", "Points earned, redeemed and expired")
    flow.oledb_source(
        "stg LoyaltyPoints",
        CONN_STAGING,
        "SELECT lp.LoyaltyEventBusinessKey, lp.CustomerBusinessKey, lp.LoyaltyProgramCode, lp.EventTypeCode, "
        "lp.EventDate, lp.PointsQuantity, lp.QualifyingSpendAmount, lp.TransactionCurrency, lp.RegionCode, "
        "lp.SourceInvoiceNumber "
        "FROM stg.LoyaltyPoints AS lp WHERE lp.LastModifiedAt > ? AND lp.LastModifiedAt <= ?;",
        columns,
    )
    _lookup_customer(flow)
    _lookup_date(flow, "EventDate", "EventDateKey", "Lookup Event Date Key")
    flow.derived_column(
        "Derive Points Measures",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("SignedPoints",
             'EventTypeCode == "REDEEM" || EventTypeCode == "EXPIRE" ? PointsQuantity * -1 : PointsQuantity',
             int_col("SignedPoints")),
            # A point is worth a different amount in each region, and APAC runs
            # a double-points weekend that the source flags only by program code.
            ("PointCashValue",
             'RegionCode == "NA" ? (DT_NUMERIC,18,4)0.0100 : RegionCode == "EU" ? (DT_NUMERIC,18,4)0.0085 '
             ': (DT_NUMERIC,18,4)0.0060',
             DEC("PointCashValue", 18, 4)),
            ("EarnRatePerCurrencyUnit",
             'RegionCode == "NA" ? (DT_NUMERIC,9,4)1.0 : RegionCode == "EU" ? (DT_NUMERIC,9,4)0.8 '
             ': LoyaltyProgramCode == "DBL" ? (DT_NUMERIC,9,4)2.0 : (DT_NUMERIC,9,4)1.2',
             DEC("EarnRatePerCurrencyUnit", 9, 4)),
            ("LiabilityAmount",
             'PointsQuantity * (RegionCode == "NA" ? (DT_NUMERIC,18,4)0.0100 : '
             'RegionCode == "EU" ? (DT_NUMERIC,18,4)0.0085 : (DT_NUMERIC,18,4)0.0060)',
             money_col("LiabilityAmount")),
            ("ExpiryDate",
             'RegionCode == "EU" ? DATEADD("Month", @[$Package::ExpiryMonthsEu], EventDate) '
             ': DATEADD("Month", @[$Package::ExpiryMonthsNa], EventDate)',
             date_col("ExpiryDate")),
            ("SourceInvoiceDegenerate", "SourceInvoiceNumber", str_col("SourceInvoiceDegenerate", 20)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Loyalty Events",
        [
            ("Unmatched Customer", "ISNULL(CustomerKey)"),
            ("Redemptions", 'EventTypeCode == "REDEEM"'),
            ("Adjustments", 'EventTypeCode == "ADJ" || EventTypeCode == "GOODWILL"'),
        ],
        default_output="Earnings",
    )
    flow.branch_destination("Insert Earnings", CONN_DW, "[Fact].[Loyalty Points]",
                            "Route Loyalty Events", "Earnings")
    flow.branch_destination("Insert Redemptions", CONN_DW, "[Fact].[Loyalty Points]",
                            "Route Loyalty Events", "Redemptions")
    flow.branch_destination("Insert Adjustments", CONN_DW, "[Fact].[Loyalty Points]",
                            "Route Loyalty Events", "Adjustments")
    flow.branch_destination("Hold Unmatched Customer", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Loyalty Events", "Unmatched Customer")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg LoyaltyPoints")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Loyalty Points"))
    load = pkg.add(DataFlowTask(flow))
    expire = pkg.add(
        exec_proc(
            "Generate Expiry Rows",
            "EXEC Integration.LoadFactLoyaltyPoints @GenerateExpiryRows = 1, @ExpiryMonthsNa = ?, "
            "@ExpiryMonthsEu = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::ExpiryMonthsNa", 0, "LONG"),
                ("$Package::ExpiryMonthsEu", 1, "LONG"),
                ("User::PackageExecutionId", 2, "LONG"),
            ],
        )
    )
    mark = pkg.add(set_watermark("Fact.Loyalty Points"))
    rejects = pkg.add(_log_rejects("Fact.Loyalty Points"))
    counts = pkg.add(log_row_count("Fact.Loyalty Points"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, expire, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_web_session():
    pkg = _fact_package(
        "FACT_Load_WebSession",
        "Incremental load of Fact.Web Session from the clickstream extract. Bot traffic is filtered, "
        "sessions are deduplicated on the session identifier, and anonymous visitors are attributed "
        "to the unknown customer member instead of being rejected.",
        extra_variables=[("BotSessionsFiltered", 0, "int"), ("AnonymousSessions", 0, "int")],
        extra_parameters=[
            ("MinimumSessionSeconds", 3, "int", "Sessions shorter than this are treated as bounces."),
        ],
    )
    columns = [
        str_col("SessionBusinessKey", 40),
        str_col("VisitorBusinessKey", 40),
        str_col("CustomerBusinessKey", 20),
        date_col("SessionStartedAt"),
        date_col("SessionEndedAt"),
        int_col("PageViewCount"),
        int_col("CartAddCount"),
        int_col("SessionDurationSeconds"),
        str_col("ChannelCode", 12),
        str_col("DeviceTypeCode", 8),
        str_col("UserAgentFamily", 60),
        str_col("CountryIsoCode", 2),
    ]
    flow = DataFlow("Load Fact Web Session", "Clickstream sessions with bot filtering")
    flow.oledb_source(
        "stg WebSession",
        CONN_STAGING,
        "SELECT ws.SessionBusinessKey, ws.VisitorBusinessKey, ws.CustomerBusinessKey, ws.SessionStartedAt, "
        "ws.SessionEndedAt, ws.PageViewCount, ws.CartAddCount, ws.SessionDurationSeconds, ws.ChannelCode, "
        "ws.DeviceTypeCode, ws.UserAgentFamily, ws.CountryIsoCode "
        "FROM stg.WebSession AS ws WHERE ws.LastModifiedAt > ? AND ws.LastModifiedAt <= ?;",
        columns,
        timeout=3600,
    )
    flow.sort("Deduplicate Sessions", ["SessionBusinessKey"], eliminate_duplicates=True)
    _lookup_customer(flow, no_match="IG")
    flow.derived_column(
        "Derive Session Attributes",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("IsAnonymous", "ISNULL(CustomerKey)", Column("IsAnonymous", "bool")),
            ("IsLikelyBot",
             'FINDSTRING(LOWER(UserAgentFamily),"bot",1) > 0 || '
             'FINDSTRING(LOWER(UserAgentFamily),"spider",1) > 0 || '
             'FINDSTRING(LOWER(UserAgentFamily),"crawler",1) > 0 || PageViewCount > 500',
             Column("IsLikelyBot", "bool")),
            ("IsBounce",
             "PageViewCount <= 1 || SessionDurationSeconds < @[$Package::MinimumSessionSeconds]",
             Column("IsBounce", "bool")),
            ("PagesPerMinute",
             "SessionDurationSeconds == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)PageViewCount / (DT_NUMERIC,9,4)SessionDurationSeconds) * 60",
             DEC("PagesPerMinute", 9, 4)),
            ("SessionIdDegenerate", "SessionBusinessKey", str_col("SessionIdDegenerate", 40)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Sessions",
        [
            ("Bot Traffic", "IsLikelyBot"),
            ("Bounces", "IsBounce"),
            ("Anonymous Sessions", "IsAnonymous"),
        ],
        default_output="Identified Sessions",
    )
    flow.branch_destination("Insert Identified Sessions", CONN_DW, "[Fact].[Web Session]",
                            "Route Sessions", "Identified Sessions")
    flow.branch_destination("Insert Anonymous Sessions", CONN_DW, "[Fact].[Web Session]",
                            "Route Sessions", "Anonymous Sessions")
    flow.branch_destination("Insert Bounces", CONN_DW, "[Fact].[Web Session]", "Route Sessions", "Bounces")
    flow.branch_destination("Discard Bot Traffic", CONN_STAGING, "[err].[RejectedFileRow]",
                            "Route Sessions", "Bot Traffic")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedFileRow]", "stg WebSession")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Web Session"))
    load = pkg.add(DataFlowTask(flow))
    attribute = pkg.add(
        exec_proc(
            "Attribute Sessions To Orders",
            "EXEC Integration.LoadFactWebSession @AttributeOrders = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Web Session"))
    rejects = pkg.add(_log_rejects("Fact.Web Session"))
    counts = pkg.add(log_row_count("Fact.Web Session"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, attribute, mark, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Procure to pay
# ---------------------------------------------------------------------------


def build_fact_load_purchase():
    pkg = _fact_package(
        "FACT_Load_Purchase",
        "Incremental load of Fact.Purchase at purchase-order-line grain. Purchase prices are held "
        "in the supplier's currency and converted at the order-date rate; the landed-cost uplift "
        "differs by supplier region because APAC freight is charged separately.",
        extra_variables=[("RowsAwaitingSupplier", 0, "int")],
    )
    columns = [
        bigint_col("PurchaseOrderLineBusinessKey"),
        str_col("PurchaseOrderNumber", 20),
        int_col("PurchaseOrderLineNumber"),
        str_col("SupplierBusinessKey", 20),
        bigint_col("StockItemBusinessKey"),
        date_col("OrderPlacedDate"),
        date_col("ExpectedReceiptDate"),
        int_col("QuantityOrdered"),
        money_col("UnitCostAmount"),
        money_col("FreightAmount"),
        str_col("TransactionCurrency", 3),
        str_col("SupplierRegionCode", 6),
        str_col("BuyerCode", 8),
    ]
    flow = DataFlow("Load Fact Purchase", "Purchase order lines with landed cost")
    flow.oledb_source(
        "stg Purchase",
        CONN_STAGING,
        "SELECT p.PurchaseOrderLineBusinessKey, p.PurchaseOrderNumber, p.PurchaseOrderLineNumber, "
        "p.SupplierBusinessKey, p.StockItemBusinessKey, p.OrderPlacedDate, p.ExpectedReceiptDate, "
        "p.QuantityOrdered, p.UnitCostAmount, p.FreightAmount, p.TransactionCurrency, "
        "p.SupplierRegionCode, p.BuyerCode "
        "FROM stg.Purchase AS p WHERE p.LastModifiedAt > ? AND p.LastModifiedAt <= ?;",
        columns,
        timeout=1800,
    )
    _lookup_supplier(flow)
    _lookup_stock_item(flow)
    flow.derived_column("Derive FX Rate Date", [("FxRateDate", "(DT_DBDATE)OrderPlacedDate", date_col("FxRateDate"))])
    _lookup_fx(flow)
    _lookup_date(flow, "OrderPlacedDate", "OrderPlacedDateKey", "Lookup Order Placed Date Key")
    _lookup_date(flow, "ExpectedReceiptDate", "ExpectedReceiptDateKey", "Lookup Expected Receipt Date Key")
    flow.derived_column(
        "Derive Purchase Measures",
        [
            ("SupplierKeyResolved", "ISNULL(SupplierKey) ? 0 : SupplierKey", int_col("SupplierKeyResolved")),
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("OrderedCostAmount", "(DT_NUMERIC,18,2)QuantityOrdered * UnitCostAmount",
             money_col("OrderedCostAmount")),
            # APAC suppliers invoice freight separately, so the landed cost has
            # to add it back; NA and EU quote delivered prices.
            ("LandedCostAmount",
             'SupplierRegionCode == "APAC" ? ((DT_NUMERIC,18,2)QuantityOrdered * UnitCostAmount) + FreightAmount '
             ': (DT_NUMERIC,18,2)QuantityOrdered * UnitCostAmount',
             money_col("LandedCostAmount")),
            ("OrderedCostReporting",
             "((DT_NUMERIC,18,2)QuantityOrdered * UnitCostAmount) * "
             "(ISNULL(FxRateToUsd) ? (DT_NUMERIC,18,8)1 : FxRateToUsd)",
             money_col("OrderedCostReporting")),
            ("LeadTimeDays", 'DATEDIFF("Day", OrderPlacedDate, ExpectedReceiptDate)', int_col("LeadTimeDays")),
            ("PurchaseOrderDegenerate", "PurchaseOrderNumber", str_col("PurchaseOrderDegenerate", 20)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Purchase Lines",
        [
            ("Unknown Supplier", "ISNULL(SupplierKey)"),
            ("Unknown Stock Item", "ISNULL(StockItemKey)"),
            ("Long Lead Time", "LeadTimeDays > 60"),
        ],
        default_output="Standard Lines",
    )
    flow.branch_destination("Insert Standard Lines", CONN_DW, "[Fact].[Purchase]",
                            "Route Purchase Lines", "Standard Lines")
    flow.branch_destination("Insert Long Lead Time Lines", CONN_DW, "[Fact].[Purchase]",
                            "Route Purchase Lines", "Long Lead Time")
    flow.branch_destination("Hold Unknown Supplier", CONN_STAGING, "[work].[PurchaseLineEnriched]",
                            "Route Purchase Lines", "Unknown Supplier")
    flow.branch_destination("Hold Unknown Stock Item", CONN_STAGING, "[work].[PurchaseLineEnriched]",
                            "Route Purchase Lines", "Unknown Stock Item")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg Purchase")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Purchase"))
    clear = pkg.add(truncate("[work].[PurchaseLineEnriched]", connection=CONN_STAGING,
                             name="Clear Purchase Work Table"))
    load = pkg.add(DataFlowTask(flow))
    queue = pkg.add(_queue_late_arrivers("Supplier", "FACT_Load_Purchase"))
    held = pkg.add(
        exec_proc(
            "Load Held Purchase Lines",
            "EXEC Integration.LoadFactPurchase @LoadHeldRows = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Purchase"))
    rejects = pkg.add(_log_rejects("Fact.Purchase"))
    counts = pkg.add(log_row_count("Fact.Purchase"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, clear, load, queue, held, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_purchase_receipt():
    pkg = _fact_package(
        "FACT_Load_PurchaseReceipt",
        "Accumulating-snapshot load of Fact.Purchase Receipt for procure-to-pay. Each receipt row "
        "is updated in place as the order moves through raised, received, invoiced and paid, and "
        "the lag measures between those milestones are recalculated on every run.",
        extra_variables=[("MilestoneUpdates", 0, "int"), ("ThreeWayMatchFailures", 0, "int")],
        extra_parameters=[
            ("ReceiptTolerancePercent", 2, "int", "Quantity variance tolerated before a match failure."),
        ],
    )
    columns = [
        bigint_col("ReceiptBusinessKey"),
        str_col("PurchaseOrderNumber", 20),
        int_col("PurchaseOrderLineNumber"),
        str_col("SupplierBusinessKey", 20),
        bigint_col("StockItemBusinessKey"),
        date_col("OrderRaisedDate"),
        date_col("GoodsReceivedDate"),
        date_col("InvoiceReceivedDate"),
        date_col("PaymentSettledDate"),
        int_col("QuantityOrdered"),
        int_col("QuantityReceived"),
        money_col("ReceivedCostAmount"),
        money_col("InvoicedAmount"),
        str_col("TransactionCurrency", 3),
    ]
    flow = DataFlow("Load Fact Purchase Receipt", "Procure-to-pay accumulating snapshot")
    flow.oledb_source(
        "stg PurchaseReceipt",
        CONN_STAGING,
        "SELECT pr.ReceiptBusinessKey, pr.PurchaseOrderNumber, pr.PurchaseOrderLineNumber, "
        "pr.SupplierBusinessKey, pr.StockItemBusinessKey, pr.OrderRaisedDate, pr.GoodsReceivedDate, "
        "pr.InvoiceReceivedDate, pr.PaymentSettledDate, pr.QuantityOrdered, pr.QuantityReceived, "
        "pr.ReceivedCostAmount, pr.InvoicedAmount, pr.TransactionCurrency "
        "FROM stg.PurchaseReceipt AS pr WHERE pr.LastModifiedAt > ? AND pr.LastModifiedAt <= ?;",
        columns,
        timeout=1800,
    )
    _lookup_supplier(flow)
    _lookup_stock_item(flow)
    _lookup_date(flow, "OrderRaisedDate", "OrderRaisedDateKey", "Lookup Order Raised Date Key")
    _lookup_date(flow, "GoodsReceivedDate", "GoodsReceivedDateKey", "Lookup Goods Received Date Key")
    _lookup_date(flow, "InvoiceReceivedDate", "InvoiceReceivedDateKey", "Lookup Invoice Received Date Key")
    _lookup_date(flow, "PaymentSettledDate", "PaymentSettledDateKey", "Lookup Payment Settled Date Key")
    flow.lookup(
        "Lookup Existing Receipt Row",
        CONN_DW,
        "SELECT [Purchase Receipt Key] AS PurchaseReceiptKey, [WWI Receipt ID] AS ReceiptBusinessKey, "
        "[Milestone Status Code] AS ExistingMilestoneStatus "
        "FROM [Fact].[Purchase Receipt];",
        ["ReceiptBusinessKey"],
        [bigint_col("PurchaseReceiptKey"), str_col("ExistingMilestoneStatus", 12)],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Milestone Lags",
        [
            ("SupplierKeyResolved", "ISNULL(SupplierKey) ? 0 : SupplierKey", int_col("SupplierKeyResolved")),
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("MilestoneStatusCode",
             '!ISNULL(PaymentSettledDate) ? "PAID" : !ISNULL(InvoiceReceivedDate) ? "INVOICED" : '
             '!ISNULL(GoodsReceivedDate) ? "RECEIVED" : "RAISED"',
             str_col("MilestoneStatusCode", 12)),
            ("OrderToReceiptDays",
             'ISNULL(GoodsReceivedDate) ? -1 : DATEDIFF("Day", OrderRaisedDate, GoodsReceivedDate)',
             int_col("OrderToReceiptDays")),
            ("ReceiptToInvoiceDays",
             'ISNULL(InvoiceReceivedDate) || ISNULL(GoodsReceivedDate) ? -1 : '
             'DATEDIFF("Day", GoodsReceivedDate, InvoiceReceivedDate)',
             int_col("ReceiptToInvoiceDays")),
            ("InvoiceToPaymentDays",
             'ISNULL(PaymentSettledDate) || ISNULL(InvoiceReceivedDate) ? -1 : '
             'DATEDIFF("Day", InvoiceReceivedDate, PaymentSettledDate)',
             int_col("InvoiceToPaymentDays")),
            ("QuantityVariance", "QuantityReceived - QuantityOrdered", int_col("QuantityVariance")),
            ("QuantityVariancePercent",
             "QuantityOrdered == 0 ? (DT_NUMERIC,9,4)0 : "
             "(((DT_NUMERIC,9,4)QuantityReceived - (DT_NUMERIC,9,4)QuantityOrdered) / "
             "(DT_NUMERIC,9,4)QuantityOrdered) * 100",
             DEC("QuantityVariancePercent", 9, 4)),
            ("PriceVarianceAmount", "InvoicedAmount - ReceivedCostAmount", money_col("PriceVarianceAmount")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Receipt Rows",
        [
            ("Three Way Match Failure",
             "ABS(QuantityVariancePercent) > @[$Package::ReceiptTolerancePercent] || "
             "ABS(PriceVarianceAmount) > 100"),
            ("Milestone Update", "!ISNULL(PurchaseReceiptKey)"),
        ],
        default_output="New Receipt",
    )
    flow.branch_destination("Insert New Receipt", CONN_DW, "[Fact].[Purchase Receipt]",
                            "Route Receipt Rows", "New Receipt")
    flow.branch_destination("Stage Milestone Update", CONN_STAGING, "[work].[PurchaseLineEnriched]",
                            "Route Receipt Rows", "Milestone Update")
    flow.branch_destination("Reject Match Failure", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Receipt Rows", "Three Way Match Failure")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg PurchaseReceipt")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Purchase Receipt"))
    clear = pkg.add(truncate("[work].[PurchaseLineEnriched]", connection=CONN_STAGING,
                             name="Clear Receipt Work Table"))
    load = pkg.add(DataFlowTask(flow))
    milestones = pkg.add(
        ExecuteSql(
            "Update Milestones In Place",
            CONN_DW,
            "UPDATE f "
            "SET f.[Goods Received Date Key] = s.GoodsReceivedDateKey, "
            "    f.[Invoice Received Date Key] = s.InvoiceReceivedDateKey, "
            "    f.[Payment Settled Date Key] = s.PaymentSettledDateKey, "
            "    f.[Milestone Status Code] = s.MilestoneStatusCode, "
            "    f.[Order To Receipt Days] = s.OrderToReceiptDays, "
            "    f.[Receipt To Invoice Days] = s.ReceiptToInvoiceDays, "
            "    f.[Invoice To Payment Days] = s.InvoiceToPaymentDays, "
            "    f.[Quantity Received] = s.QuantityReceived, "
            "    f.[Invoiced Amount] = s.InvoicedAmount, "
            "    f.[Lineage Key] = ? "
            "FROM [Fact].[Purchase Receipt] AS f "
            "INNER JOIN work.PurchaseLineEnriched AS s ON s.ReceiptBusinessKey = f.[WWI Receipt ID];",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    accrual = pkg.add(
        exec_proc(
            "Recalculate GRNI Accrual",
            "EXEC Integration.LoadFactPurchaseReceipt @RecalculateAccrual = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Purchase Receipt"))
    rejects = pkg.add(_log_rejects("Fact.Purchase Receipt"))
    counts = pkg.add(log_row_count("Fact.Purchase Receipt"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, clear, load, milestones, accrual, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_supplier_payment():
    pkg = _fact_package(
        "FACT_Load_SupplierPayment",
        "Incremental load of Fact.Supplier Payment. Early-settlement discounts are recognised, and "
        "the realised FX gain or loss between invoice date and settlement date is calculated from "
        "two effective-dated rate lookups.",
        extra_variables=[("RowsWithFxGain", 0, "int")],
    )
    columns = [
        bigint_col("SupplierPaymentBusinessKey"),
        str_col("PaymentReference", 30),
        str_col("SupplierBusinessKey", 20),
        str_col("SupplierInvoiceNumber", 30),
        date_col("InvoiceDate"),
        date_col("SettlementDate"),
        money_col("InvoiceAmount"),
        money_col("SettledAmount"),
        money_col("SettlementDiscountAmount"),
        str_col("TransactionCurrency", 3),
        str_col("PaymentRunCode", 12),
    ]
    flow = DataFlow("Load Fact Supplier Payment", "AP settlements with realised FX")
    flow.oledb_source(
        "stg SupplierPayment",
        CONN_STAGING,
        "SELECT sp.SupplierPaymentBusinessKey, sp.PaymentReference, sp.SupplierBusinessKey, "
        "sp.SupplierInvoiceNumber, sp.InvoiceDate, sp.SettlementDate, sp.InvoiceAmount, sp.SettledAmount, "
        "sp.SettlementDiscountAmount, sp.TransactionCurrency, sp.PaymentRunCode "
        "FROM stg.SupplierPayment AS sp WHERE sp.LastModifiedAt > ? AND sp.LastModifiedAt <= ?;",
        columns,
    )
    _lookup_supplier(flow)
    flow.derived_column(
        "Derive Rate Dates",
        [
            ("FxRateDate", "(DT_DBDATE)InvoiceDate", date_col("FxRateDate")),
            ("SettlementRateDate", "(DT_DBDATE)SettlementDate", date_col("SettlementRateDate")),
        ],
    )
    _lookup_fx(flow, name="Lookup Invoice Date FX Rate")
    flow.lookup(
        "Lookup Settlement Date FX Rate",
        CONN_STAGING,
        "SELECT r.CurrencyCode AS TransactionCurrency, r.RateDate AS SettlementRateDate, "
        "r.RateToUsd AS SettlementRateToUsd FROM stg.CurrencyRate AS r WHERE r.RateTypeCode = N'CLOSE';",
        ["TransactionCurrency", "SettlementRateDate"],
        [DEC("SettlementRateToUsd", 18, 8)],
        no_match="IG",
    )
    _lookup_date(flow, "SettlementDate", "SettlementDateKey", "Lookup Settlement Date Key")
    flow.derived_column(
        "Derive Settlement Measures",
        [
            ("SupplierKeyResolved", "ISNULL(SupplierKey) ? 0 : SupplierKey", int_col("SupplierKeyResolved")),
            ("DiscountTakenPercent",
             "InvoiceAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)SettlementDiscountAmount / (DT_NUMERIC,9,4)InvoiceAmount) * 100",
             DEC("DiscountTakenPercent", 9, 4)),
            ("DaysToSettle", 'DATEDIFF("Day", InvoiceDate, SettlementDate)', int_col("DaysToSettle")),
            ("InvoiceAmountReporting",
             "InvoiceAmount * (ISNULL(FxRateToUsd) ? (DT_NUMERIC,18,8)1 : FxRateToUsd)",
             money_col("InvoiceAmountReporting")),
            ("SettledAmountReporting",
             "SettledAmount * (ISNULL(SettlementRateToUsd) ? (DT_NUMERIC,18,8)1 : SettlementRateToUsd)",
             money_col("SettledAmountReporting")),
            # Realised FX difference: what the invoice was worth when raised
            # against what it cost to settle.
            ("RealisedFxGainLoss",
             "(InvoiceAmount * (ISNULL(FxRateToUsd) ? (DT_NUMERIC,18,8)1 : FxRateToUsd)) - "
             "(SettledAmount * (ISNULL(SettlementRateToUsd) ? (DT_NUMERIC,18,8)1 : SettlementRateToUsd))",
             money_col("RealisedFxGainLoss")),
            ("PaymentReferenceDegenerate", "PaymentReference", str_col("PaymentReferenceDegenerate", 30)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Supplier Payments",
        [
            ("Unknown Supplier", "ISNULL(SupplierKey)"),
            ("Discount Taken", "SettlementDiscountAmount > 0"),
            ("Foreign Currency", 'TransactionCurrency != "USD"'),
        ],
        default_output="Domestic Settlements",
    )
    flow.branch_destination("Insert Domestic Settlements", CONN_DW, "[Fact].[Supplier Payment]",
                            "Route Supplier Payments", "Domestic Settlements")
    flow.branch_destination("Insert Foreign Currency Settlements", CONN_DW, "[Fact].[Supplier Payment]",
                            "Route Supplier Payments", "Foreign Currency")
    flow.branch_destination("Insert Discounted Settlements", CONN_DW, "[Fact].[Supplier Payment]",
                            "Route Supplier Payments", "Discount Taken")
    flow.branch_destination("Reject Unknown Supplier", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Supplier Payments", "Unknown Supplier")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedPayment]", "stg SupplierPayment")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Supplier Payment"))
    load = pkg.add(DataFlowTask(flow))
    revalue = pkg.add(
        exec_proc(
            "Post FX Revaluation Entries",
            "EXEC Integration.LoadFactSupplierPayment @PostRevaluation = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Supplier Payment"))
    rejects = pkg.add(_log_rejects("Fact.Supplier Payment"))
    counts = pkg.add(log_row_count("Fact.Supplier Payment"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, revalue, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_supplier_transaction():
    pkg = _fact_package(
        "FACT_Load_SupplierTransaction",
        "Incremental load of Fact.Supplier Transaction (the AP sub-ledger) with accrual reversal "
        "handling: the prior period accrual is reversed with a negative row rather than being "
        "deleted, so the audit trail survives.",
        extra_variables=[("AccrualReversalRows", 0, "int")],
    )
    columns = [
        bigint_col("SupplierTransactionBusinessKey"),
        str_col("SupplierBusinessKey", 20),
        str_col("TransactionTypeCode", 8),
        str_col("SupplierInvoiceNumber", 30),
        date_col("TransactionDate"),
        date_col("DueDate"),
        money_col("TransactionAmount"),
        money_col("OutstandingBalance"),
        str_col("TransactionCurrency", 3),
        Column("IsAccrual", "bool"),
        str_col("AccountingPeriodCode", 7),
    ]
    flow = DataFlow("Load Fact Supplier Transaction", "AP sub-ledger with accrual reversals")
    flow.oledb_source(
        "stg SupplierTransaction",
        CONN_STAGING,
        "SELECT st.SupplierTransactionBusinessKey, st.SupplierBusinessKey, st.TransactionTypeCode, "
        "st.SupplierInvoiceNumber, st.TransactionDate, st.DueDate, st.TransactionAmount, "
        "st.OutstandingBalance, st.TransactionCurrency, st.IsAccrual, st.AccountingPeriodCode "
        "FROM stg.SupplierTransaction AS st WHERE st.LastModifiedAt > ? AND st.LastModifiedAt <= ?;",
        columns,
    )
    _lookup_supplier(flow)
    _lookup_date(flow, "TransactionDate", "TransactionDateKey", "Lookup Transaction Date Key")
    _lookup_date(flow, "DueDate", "DueDateKey", "Lookup Due Date Key")
    flow.derived_column(
        "Derive AP Measures",
        [
            ("SupplierKeyResolved", "ISNULL(SupplierKey) ? 0 : SupplierKey", int_col("SupplierKeyResolved")),
            ("SignedAmount",
             'TransactionTypeCode == "CRN" || TransactionTypeCode == "PAY" ? TransactionAmount * -1 '
             ': TransactionAmount',
             money_col("SignedAmount")),
            ("PayablesAgingBucket",
             'DATEDIFF("Day", DueDate, GETDATE()) <= 0 ? "NOTDUE" : '
             'DATEDIFF("Day", DueDate, GETDATE()) <= 30 ? "1-30" : '
             'DATEDIFF("Day", DueDate, GETDATE()) <= 60 ? "31-60" : "60+"',
             str_col("PayablesAgingBucket", 10)),
            ("IsReversalCandidate", 'IsAccrual && TransactionTypeCode == "ACC"',
             Column("IsReversalCandidate", "bool")),
            ("SupplierInvoiceDegenerate", "SupplierInvoiceNumber", str_col("SupplierInvoiceDegenerate", 30)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route AP Transactions",
        [
            ("Unknown Supplier", "ISNULL(SupplierKey)"),
            ("Accruals", "IsAccrual"),
            ("Credit Notes", 'TransactionTypeCode == "CRN"'),
        ],
        default_output="Invoices",
    )
    flow.branch_destination("Insert Invoices", CONN_DW, "[Fact].[Supplier Transaction]",
                            "Route AP Transactions", "Invoices")
    flow.branch_destination("Insert Credit Notes", CONN_DW, "[Fact].[Supplier Transaction]",
                            "Route AP Transactions", "Credit Notes")
    flow.branch_destination("Insert Accruals", CONN_DW, "[Fact].[Supplier Transaction]",
                            "Route AP Transactions", "Accruals")
    flow.branch_destination("Reject Unknown Supplier", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route AP Transactions", "Unknown Supplier")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg SupplierTransaction")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Supplier Transaction"))
    reverse = pkg.add(
        ExecuteSql(
            "Reverse Prior Period Accruals",
            CONN_DW,
            "INSERT INTO [Fact].[Supplier Transaction] "
            "    ([Supplier Key], [Transaction Date Key], [Transaction Type Code], [Transaction Amount], "
            "     [Is Accrual], [Is Reversal], [Reverses Transaction Key], [Lineage Key]) "
            "SELECT f.[Supplier Key], f.[Transaction Date Key], N'ACCREV', f.[Transaction Amount] * -1, "
            "       1, 1, f.[Supplier Transaction Key], ? "
            "FROM [Fact].[Supplier Transaction] AS f "
            "WHERE f.[Is Accrual] = 1 AND f.[Is Reversal] = 0 "
            "  AND f.[Accounting Period Code] < FORMAT(GETDATE(), 'yyyy-MM') "
            "  AND NOT EXISTS (SELECT 1 FROM [Fact].[Supplier Transaction] AS r "
            "                  WHERE r.[Reverses Transaction Key] = f.[Supplier Transaction Key]);",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    load = pkg.add(DataFlowTask(flow))
    mark = pkg.add(set_watermark("Fact.Supplier Transaction"))
    rejects = pkg.add(_log_rejects("Fact.Supplier Transaction"))
    counts = pkg.add(log_row_count("Fact.Supplier Transaction"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, reverse, load, mark, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------


def build_fact_load_movement():
    pkg = _fact_package(
        "FACT_Load_Movement",
        "Incremental load of Fact.Movement. Stock adjustments that are later corrected are handled "
        "by writing a reversal row and then the corrected row, because the movement ledger is never "
        "updated in place.",
        extra_variables=[("ReversalRowsWritten", 0, "int")],
    )
    columns = [
        bigint_col("MovementBusinessKey"),
        bigint_col("StockItemBusinessKey"),
        str_col("MovementTypeCode", 8),
        date_col("MovementDate"),
        int_col("QuantityMoved"),
        money_col("MovementValueAmount"),
        str_col("FromLocationCode", 12),
        str_col("ToLocationCode", 12),
        str_col("ReasonCode", 8),
        bigint_col("ReversesMovementKey"),
    ]
    flow = DataFlow("Load Fact Movement", "Inventory movements with signed quantities")
    flow.oledb_source(
        "stg Movement",
        CONN_STAGING,
        "SELECT m.MovementBusinessKey, m.StockItemBusinessKey, m.MovementTypeCode, m.MovementDate, "
        "m.QuantityMoved, m.MovementValueAmount, m.FromLocationCode, m.ToLocationCode, m.ReasonCode, "
        "m.ReversesMovementKey "
        "FROM stg.Movement AS m WHERE m.LastModifiedAt > ? AND m.LastModifiedAt <= ?;",
        columns,
        timeout=2400,
    )
    _lookup_stock_item(flow)
    _lookup_date(flow, "MovementDate", "MovementDateKey", "Lookup Movement Date Key")
    flow.derived_column(
        "Derive Movement Direction",
        [
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("SignedQuantity",
             'MovementTypeCode == "ISSUE" || MovementTypeCode == "SCRAP" || MovementTypeCode == "SALE" '
             '? QuantityMoved * -1 : QuantityMoved',
             int_col("SignedQuantity")),
            ("SignedValueAmount",
             'MovementTypeCode == "ISSUE" || MovementTypeCode == "SCRAP" || MovementTypeCode == "SALE" '
             '? MovementValueAmount * -1 : MovementValueAmount',
             money_col("SignedValueAmount")),
            ("IsReversal", "!ISNULL(ReversesMovementKey) && ReversesMovementKey > 0",
             Column("IsReversal", "bool")),
            ("IsInterWarehouse",
             "!ISNULL(FromLocationCode) && !ISNULL(ToLocationCode) && FromLocationCode != ToLocationCode",
             Column("IsInterWarehouse", "bool")),
            ("MovementReasonGroup",
             'ReasonCode == "DMG" || ReasonCode == "EXP" ? "WRITEOFF" : '
             'ReasonCode == "CYC" ? "CYCLECOUNT" : ReasonCode == "RET" ? "RETURN" : "OPERATIONAL"',
             str_col("MovementReasonGroup", 12)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Movements",
        [
            ("Unknown Stock Item", "ISNULL(StockItemKey)"),
            ("Reversals", "IsReversal"),
            ("Write Offs", 'MovementReasonGroup == "WRITEOFF"'),
        ],
        default_output="Operational Movements",
    )
    flow.branch_destination("Insert Operational Movements", CONN_DW, "[Fact].[Movement]",
                            "Route Movements", "Operational Movements")
    flow.branch_destination("Insert Write Offs", CONN_DW, "[Fact].[Movement]",
                            "Route Movements", "Write Offs")
    flow.branch_destination("Insert Reversal Rows", CONN_DW, "[Fact].[Movement]",
                            "Route Movements", "Reversals")
    flow.branch_destination("Hold Unknown Stock Item", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Movements", "Unknown Stock Item")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg Movement")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Movement"))
    load = pkg.add(DataFlowTask(flow))
    link = pkg.add(
        ExecuteSql(
            "Link Reversals To Original Movements",
            CONN_DW,
            "UPDATE r SET r.[Reverses Movement Key] = o.[Movement Key], r.[Is Reversal] = 1 "
            "FROM [Fact].[Movement] AS r "
            "INNER JOIN [Fact].[Movement] AS o ON o.[WWI Movement ID] = r.[Reverses WWI Movement ID] "
            "WHERE r.[Reverses WWI Movement ID] IS NOT NULL AND r.[Reverses Movement Key] IS NULL;",
        )
    )
    queue = pkg.add(_queue_late_arrivers("Stock Item", "FACT_Load_Movement"))
    mark = pkg.add(set_watermark("Fact.Movement"))
    rejects = pkg.add(_log_rejects("Fact.Movement"))
    counts = pkg.add(log_row_count("Fact.Movement"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, link, queue, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_stock_holding():
    pkg = _fact_package(
        "FACT_Load_StockHolding",
        "Periodic-snapshot load of Fact.Stock Holding. The snapshot for the requested position date "
        "is deleted and rebuilt, so a failed overnight run can simply be repeated without producing "
        "duplicate positions.",
        extra_variables=[("SnapshotRowsDeleted", 0, "int")],
        extra_parameters=[
            ("SnapshotDate", "1900-01-01", "string",
             "Position date to rebuild; the master package passes yesterday's date."),
        ],
    )
    columns = [
        bigint_col("StockItemBusinessKey"),
        str_col("WarehouseCode", 12),
        date_col("PositionDate"),
        int_col("QuantityOnHand"),
        int_col("QuantityAllocated"),
        int_col("QuantityOnOrder"),
        money_col("UnitCostAmount"),
        int_col("ReorderLevel"),
        int_col("TargetStockLevel"),
        date_col("LastStocktakeDate"),
    ]
    flow = DataFlow("Load Fact Stock Holding", "Daily stock position snapshot")
    flow.oledb_source(
        "stg StockHolding",
        CONN_STAGING,
        "SELECT sh.StockItemBusinessKey, sh.WarehouseCode, sh.PositionDate, sh.QuantityOnHand, "
        "sh.QuantityAllocated, sh.QuantityOnOrder, sh.UnitCostAmount, sh.ReorderLevel, "
        "sh.TargetStockLevel, sh.LastStocktakeDate "
        "FROM stg.StockHolding AS sh WHERE sh.PositionDate = CAST(? AS date);",
        columns,
        timeout=2400,
    )
    _lookup_stock_item(flow)
    _lookup_date(flow, "PositionDate", "PositionDateKey", "Lookup Position Date Key")
    flow.derived_column(
        "Derive Position Measures",
        [
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("QuantityAvailable", "QuantityOnHand - QuantityAllocated", int_col("QuantityAvailable")),
            ("StockValueAmount", "(DT_NUMERIC,18,2)QuantityOnHand * UnitCostAmount",
             money_col("StockValueAmount")),
            ("IsBelowReorderLevel", "(QuantityOnHand - QuantityAllocated) < ReorderLevel",
             Column("IsBelowReorderLevel", "bool")),
            ("CoverRatio",
             "TargetStockLevel == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)QuantityOnHand / (DT_NUMERIC,9,4)TargetStockLevel)",
             DEC("CoverRatio", 9, 4)),
            ("DaysSinceStocktake", 'DATEDIFF("Day", LastStocktakeDate, PositionDate)',
             int_col("DaysSinceStocktake")),
            ("StockStatusCode",
             'QuantityOnHand == 0 ? "OUTOFSTOCK" : (QuantityOnHand - QuantityAllocated) < 0 ? "OVERSOLD" : '
             '(QuantityOnHand - QuantityAllocated) < ReorderLevel ? "REORDER" : "HEALTHY"',
             str_col("StockStatusCode", 12)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Positions",
        [
            ("Unknown Stock Item", "ISNULL(StockItemKey)"),
            ("Oversold Positions", 'StockStatusCode == "OVERSOLD"'),
        ],
        default_output="Normal Positions",
    )
    flow.branch_destination("Insert Normal Positions", CONN_DW, "[Fact].[Stock Holding]",
                            "Route Positions", "Normal Positions")
    flow.branch_destination("Insert Oversold Positions", CONN_DW, "[Fact].[Stock Holding]",
                            "Route Positions", "Oversold Positions")
    flow.branch_destination("Reject Unknown Stock Item", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Positions", "Unknown Stock Item")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg StockHolding")

    init = pkg.add(
        Expression("Init Snapshot Date",
                   '@[User::BusinessDateTo] = @[$Package::SnapshotDate]')
    )
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(
        ExecuteSql(
            "Delete Existing Snapshot For Date",
            CONN_DW,
            "DELETE f FROM [Fact].[Stock Holding] AS f "
            "INNER JOIN Dimension.Date AS d ON d.[Date] = f.[Position Date Key] "
            "WHERE d.[Date] = CAST(? AS date);",
            parameter_bindings=[("$Package::SnapshotDate", 0, "NVARCHAR")],
        )
    )
    load = pkg.add(DataFlowTask(flow))
    health = pkg.add(
        exec_proc(
            "Recalculate Inventory Health Flags",
            "EXEC Integration.LoadFactStockHolding @PositionDate = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::SnapshotDate", 0, "NVARCHAR"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    rejects = pkg.add(_log_rejects("Fact.Stock Holding"))
    counts = pkg.add(log_row_count("Fact.Stock Holding"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, load, health, rejects, counts, done)
    return _write(pkg)


def build_fact_load_daily_inventory_snapshot():
    pkg = _fact_package(
        "FACT_Load_DailyInventorySnapshot",
        "Periodic-snapshot rebuild of Fact.Daily Inventory Snapshot at item/warehouse/day grain. "
        "Re-runnable for any date in the retention window; the aggregation is done in the data flow "
        "so the source ledger is read once.",
        extra_variables=[("WarehousesProcessed", 0, "int")],
        extra_parameters=[
            ("SnapshotDate", "1900-01-01", "string", "Snapshot date to rebuild."),
            ("RetentionDays", 400, "int", "Days of snapshot history retained before pruning."),
        ],
    )
    columns = [
        bigint_col("StockItemBusinessKey"),
        str_col("WarehouseCode", 12),
        date_col("PositionDate"),
        int_col("QuantityOnHand"),
        money_col("StockValueAmount"),
        int_col("DaysOfCover"),
        int_col("QuantityAgedOver90Days"),
        money_col("ObsolescenceProvisionAmount"),
    ]
    flow = DataFlow("Build Daily Inventory Snapshot", "Item/warehouse/day inventory position")
    flow.oledb_source(
        "stg DailyInventorySnapshot",
        CONN_STAGING,
        "SELECT dis.StockItemBusinessKey, dis.WarehouseCode, dis.PositionDate, dis.QuantityOnHand, "
        "dis.StockValueAmount, dis.DaysOfCover, dis.QuantityAgedOver90Days, "
        "dis.ObsolescenceProvisionAmount "
        "FROM stg.DailyInventorySnapshot AS dis WHERE dis.PositionDate = CAST(? AS date);",
        columns,
        timeout=3600,
    )
    _lookup_stock_item(flow)
    _lookup_date(flow, "PositionDate", "PositionDateKey", "Lookup Position Date Key")
    flow.aggregate(
        "Aggregate To Item Warehouse Day",
        ["StockItemBusinessKey", "WarehouseCode", "PositionDate"],
        [
            ("QuantityOnHand", "TotalQuantityOnHand", "sum"),
            ("StockValueAmount", "TotalStockValueAmount", "sum"),
            ("QuantityAgedOver90Days", "TotalQuantityAged", "sum"),
            ("ObsolescenceProvisionAmount", "TotalProvisionAmount", "sum"),
            ("DaysOfCover", "AverageDaysOfCover", "avg"),
        ],
    )
    flow.derived_column(
        "Derive Snapshot Attributes",
        [
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("AgedStockPercent",
             "TotalQuantityOnHand == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)TotalQuantityAged / (DT_NUMERIC,9,4)TotalQuantityOnHand) * 100",
             DEC("AgedStockPercent", 9, 4)),
            ("NetStockValueAmount", "TotalStockValueAmount - TotalProvisionAmount",
             money_col("NetStockValueAmount")),
            ("CoverBandCode",
             'AverageDaysOfCover >= 90 ? "OVERSTOCKED" : AverageDaysOfCover >= 30 ? "COMFORTABLE" : '
             'AverageDaysOfCover >= 7 ? "TIGHT" : "CRITICAL"',
             str_col("CoverBandCode", 12)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Snapshot Rows",
        [
            ("Unknown Stock Item", "ISNULL(StockItemKey)"),
            ("Critical Cover", 'CoverBandCode == "CRITICAL"'),
        ],
        default_output="Standard Rows",
    )
    flow.branch_destination("Insert Standard Rows", CONN_DW, "[Fact].[Daily Inventory Snapshot]",
                            "Route Snapshot Rows", "Standard Rows")
    flow.branch_destination("Insert Critical Cover Rows", CONN_DW, "[Fact].[Daily Inventory Snapshot]",
                            "Route Snapshot Rows", "Critical Cover")
    flow.branch_destination("Reject Unknown Stock Item", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Snapshot Rows", "Unknown Stock Item")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg DailyInventorySnapshot")

    init = pkg.add(Expression("Init Snapshot Date", '@[User::BusinessDateTo] = @[$Package::SnapshotDate]'))
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(
        ExecuteSql(
            "Delete Existing Snapshot For Date",
            CONN_DW,
            "DELETE f FROM [Fact].[Daily Inventory Snapshot] AS f "
            "INNER JOIN Dimension.Date AS d ON d.[Date] = f.[Position Date Key] "
            "WHERE d.[Date] = CAST(? AS date);",
            parameter_bindings=[("$Package::SnapshotDate", 0, "NVARCHAR")],
        )
    )
    load = pkg.add(DataFlowTask(flow))
    prune = pkg.add(
        ExecuteSql(
            "Prune Snapshots Past Retention",
            CONN_DW,
            "DELETE f FROM [Fact].[Daily Inventory Snapshot] AS f "
            "INNER JOIN Dimension.Date AS d ON d.[Date] = f.[Position Date Key] "
            "WHERE d.[Date] < DATEADD(DAY, -1 * ?, CAST(GETDATE() AS date));",
            parameter_bindings=[("$Package::RetentionDays", 0, "LONG")],
        )
    )
    rejects = pkg.add(_log_rejects("Fact.Daily Inventory Snapshot"))
    counts = pkg.add(log_row_count("Fact.Daily Inventory Snapshot"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, load, prune, rejects, counts, done)
    return _write(pkg)


def build_fact_load_daily_sales_snapshot():
    pkg = _fact_package(
        "FACT_Load_DailySalesSnapshot",
        "Periodic-snapshot rebuild of Fact.Daily Sales Snapshot at customer/product/day grain, "
        "aggregated from Fact.Sale so the reporting layer never scans the line-level fact for "
        "daily trends.",
        extra_variables=[("SnapshotRowsBuilt", 0, "int")],
        extra_parameters=[
            ("SnapshotDate", "1900-01-01", "string", "Snapshot date to rebuild."),
        ],
    )
    columns = [
        int_col("CustomerKey"),
        int_col("StockItemKey"),
        int_col("InvoiceDateKey"),   # source-side yyyymmdd key from stg.DailySalesSnapshot
        str_col("RegionCode", 6),
        int_col("Quantity"),
        money_col("GrossAmount"),
        money_col("DiscountAmount"),
        money_col("NetAmount"),
        money_col("TaxAmount"),
        money_col("TotalCostAmount"),
        money_col("MarginAmount"),
        int_col("InvoiceLineCount"),
    ]
    flow = DataFlow("Build Daily Sales Snapshot", "Customer/product/day sales rollup")
    flow.oledb_source(
        "stg DailySalesSnapshot",
        CONN_STAGING,
        "SELECT dss.CustomerKey, dss.StockItemKey, dss.InvoiceDateKey, dss.RegionCode, dss.Quantity, "
        "dss.GrossAmount, dss.DiscountAmount, dss.NetAmount, dss.TaxAmount, dss.TotalCostAmount, "
        "dss.MarginAmount, dss.InvoiceLineCount "
        "FROM stg.DailySalesSnapshot AS dss WHERE dss.SnapshotDate = CAST(? AS date);",
        columns,
        timeout=3600,
    )
    flow.aggregate(
        "Aggregate To Customer Product Day",
        ["CustomerKey", "StockItemKey", "InvoiceDateKey", "RegionCode"],
        [
            ("Quantity", "TotalQuantity", "sum"),
            ("GrossAmount", "TotalGrossAmount", "sum"),
            ("DiscountAmount", "TotalDiscountAmount", "sum"),
            ("NetAmount", "TotalNetAmount", "sum"),
            ("TaxAmount", "TotalTaxAmount", "sum"),
            ("TotalCostAmount", "TotalCostAmount", "sum"),
            ("MarginAmount", "TotalMarginAmount", "sum"),
            ("InvoiceLineCount", "TotalLineCount", "sum"),
        ],
    )
    flow.derived_column(
        "Derive Snapshot Ratios",
        [
            ("EffectiveDiscountPercent",
             "TotalGrossAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)TotalDiscountAmount / (DT_NUMERIC,9,4)TotalGrossAmount) * 100",
             DEC("EffectiveDiscountPercent", 9, 4)),
            ("MarginPercent",
             "TotalNetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)TotalMarginAmount / (DT_NUMERIC,9,4)TotalNetAmount) * 100",
             DEC("MarginPercent", 9, 4)),
            ("AverageLineValue",
             "TotalLineCount == 0 ? (DT_NUMERIC,18,2)0 : "
             "((DT_NUMERIC,18,2)TotalNetAmount / (DT_NUMERIC,18,2)TotalLineCount)",
             money_col("AverageLineValue")),
            ("TaxRegimeCode",
             'RegionCode == "NA" ? "SALESTAX" : RegionCode == "EU" ? "VAT" : "GST"',
             str_col("TaxRegimeCode", 10)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Snapshot Rows",
        [
            ("Negative Margin", "TotalMarginAmount < 0"),
            ("Zero Value Rows", "TotalNetAmount == 0"),
        ],
        default_output="Standard Rows",
    )
    flow.branch_destination("Insert Standard Rows", CONN_DW, "[Fact].[Daily Sales Snapshot]",
                            "Route Snapshot Rows", "Standard Rows")
    flow.branch_destination("Insert Negative Margin Rows", CONN_DW, "[Fact].[Daily Sales Snapshot]",
                            "Route Snapshot Rows", "Negative Margin")
    flow.branch_destination("Reject Zero Value Rows", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Snapshot Rows", "Zero Value Rows")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "stg DailySalesSnapshot")

    init = pkg.add(Expression("Init Snapshot Date", '@[User::BusinessDateTo] = @[$Package::SnapshotDate]'))
    start = pkg.add(log_package_start(pkg))
    purge = pkg.add(
        ExecuteSql(
            "Delete Existing Snapshot For Date",
            CONN_DW,
            "DELETE f FROM [Fact].[Daily Sales Snapshot] AS f "
            "INNER JOIN Dimension.Date AS d ON d.[Date] = f.[Invoice Date Key] "
            "WHERE d.[Date] = CAST(? AS date);",
            parameter_bindings=[("$Package::SnapshotDate", 0, "NVARCHAR")],
        )
    )
    load = pkg.add(DataFlowTask(flow))
    reconcile = pkg.add(
        exec_proc(
            "Reconcile Snapshot To Fact Sale",
            "EXEC Integration.LoadFactDailySalesSnapshot @SnapshotDate = ?, @Reconcile = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::SnapshotDate", 0, "NVARCHAR"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    rejects = pkg.add(_log_rejects("Fact.Daily Sales Snapshot"))
    counts = pkg.add(log_row_count("Fact.Daily Sales Snapshot"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, purge, load, reconcile, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Fulfilment and after-sales
# ---------------------------------------------------------------------------


def build_fact_load_shipment():
    pkg = _fact_package(
        "FACT_Load_Shipment",
        "Accumulating-snapshot load of Fact.Shipment. One row per shipment, updated in place as the "
        "picked, packed, dispatched, customs-cleared and delivered milestones arrive, with the lag "
        "measures between them recalculated each run.",
        extra_variables=[("MilestoneUpdates", 0, "int"), ("CustomsHeldRows", 0, "int")],
    )
    columns = [
        bigint_col("ShipmentBusinessKey"),
        str_col("OrderNumber", 20),
        str_col("CustomerBusinessKey", 20),
        str_col("CarrierCode", 8),
        date_col("PickedAt"),
        date_col("PackedAt"),
        date_col("DispatchedAt"),
        date_col("CustomsClearedAt"),
        date_col("DeliveredAt"),
        date_col("PromisedDeliveryDate"),
        int_col("PackageCount"),
        DEC("ShipmentWeightKg", 12, 3),
        money_col("FreightChargeAmount"),
        str_col("DestinationCountryIsoCode", 2),
    ]
    flow = DataFlow("Load Fact Shipment", "Delivery milestone accumulating snapshot")
    flow.oledb_source(
        "stg Shipment",
        CONN_STAGING,
        "SELECT s.ShipmentBusinessKey, s.OrderNumber, s.CustomerBusinessKey, s.CarrierCode, s.PickedAt, "
        "s.PackedAt, s.DispatchedAt, s.CustomsClearedAt, s.DeliveredAt, s.PromisedDeliveryDate, "
        "s.PackageCount, s.ShipmentWeightKg, s.FreightChargeAmount, s.DestinationCountryIsoCode "
        "FROM stg.Shipment AS s WHERE s.LastModifiedAt > ? AND s.LastModifiedAt <= ?;",
        columns,
        timeout=1800,
    )
    _lookup_customer(flow)
    _lookup_date(flow, "DispatchedAt", "DispatchedDateKey", "Lookup Dispatched Date Key")
    _lookup_date(flow, "DeliveredAt", "DeliveredDateKey", "Lookup Delivered Date Key")
    _lookup_date(flow, "PromisedDeliveryDate", "PromisedDeliveryDateKey", "Lookup Promised Delivery Date Key")
    flow.lookup(
        "Lookup Existing Shipment Row",
        CONN_DW,
        "SELECT [Shipment Key] AS ShipmentKey, [WWI Shipment ID] AS ShipmentBusinessKey, "
        "[Milestone Status Code] AS ExistingMilestoneStatus FROM [Fact].[Shipment];",
        ["ShipmentBusinessKey"],
        [bigint_col("ShipmentKey"), str_col("ExistingMilestoneStatus", 12)],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Delivery Lags",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("MilestoneStatusCode",
             '!ISNULL(DeliveredAt) ? "DELIVERED" : !ISNULL(CustomsClearedAt) ? "CLEARED" : '
             '!ISNULL(DispatchedAt) ? "DISPATCHED" : !ISNULL(PackedAt) ? "PACKED" : "PICKED"',
             str_col("MilestoneStatusCode", 12)),
            ("PickToDispatchHours",
             'ISNULL(DispatchedAt) ? -1 : DATEDIFF("Hour", PickedAt, DispatchedAt)',
             int_col("PickToDispatchHours")),
            ("DispatchToDeliveryDays",
             'ISNULL(DeliveredAt) || ISNULL(DispatchedAt) ? -1 : DATEDIFF("Day", DispatchedAt, DeliveredAt)',
             int_col("DispatchToDeliveryDays")),
            ("CustomsHoldDays",
             'ISNULL(CustomsClearedAt) || ISNULL(DispatchedAt) ? 0 : '
             'DATEDIFF("Day", DispatchedAt, CustomsClearedAt)',
             int_col("CustomsHoldDays")),
            ("DeliveredOnTime",
             "!ISNULL(DeliveredAt) && (DT_DBDATE)DeliveredAt <= (DT_DBDATE)PromisedDeliveryDate",
             Column("DeliveredOnTime", "bool")),
            ("DaysLate",
             'ISNULL(DeliveredAt) ? -1 : DATEDIFF("Day", PromisedDeliveryDate, DeliveredAt)',
             int_col("DaysLate")),
            ("FreightPerKg",
             "ShipmentWeightKg == 0 ? (DT_NUMERIC,18,2)0 : "
             "FreightChargeAmount / (DT_NUMERIC,18,2)ShipmentWeightKg",
             money_col("FreightPerKg")),
            ("OrderNumberDegenerate", "OrderNumber", str_col("OrderNumberDegenerate", 20)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Shipment Rows",
        [
            ("New Shipment", "ISNULL(ShipmentKey)"),
            ("Customs Held", "CustomsHoldDays > 3"),
        ],
        default_output="Milestone Update",
    )
    flow.branch_destination("Insert New Shipment", CONN_DW, "[Fact].[Shipment]",
                            "Route Shipment Rows", "New Shipment")
    flow.branch_destination("Stage Milestone Update", CONN_STAGING, "[work].[OrderLineEnriched]",
                            "Route Shipment Rows", "Milestone Update")
    flow.branch_destination("Stage Customs Held", CONN_STAGING, "[work].[OrderLineEnriched]",
                            "Route Shipment Rows", "Customs Held")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedShipment]", "stg Shipment")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Shipment"))
    clear = pkg.add(truncate("[work].[OrderLineEnriched]", connection=CONN_STAGING,
                             name="Clear Shipment Work Table"))
    load = pkg.add(DataFlowTask(flow))
    milestones = pkg.add(
        ExecuteSql(
            "Update Shipment Milestones In Place",
            CONN_DW,
            "UPDATE f "
            "SET f.[Packed At] = s.PackedAt, f.[Dispatched At] = s.DispatchedAt, "
            "    f.[Customs Cleared At] = s.CustomsClearedAt, f.[Delivered At] = s.DeliveredAt, "
            "    f.[Delivered Date Key] = s.DeliveredDateKey, "
            "    f.[Milestone Status Code] = s.MilestoneStatusCode, "
            "    f.[Pick To Dispatch Hours] = s.PickToDispatchHours, "
            "    f.[Dispatch To Delivery Days] = s.DispatchToDeliveryDays, "
            "    f.[Customs Hold Days] = s.CustomsHoldDays, "
            "    f.[Delivered On Time] = s.DeliveredOnTime, f.[Days Late] = s.DaysLate, "
            "    f.[Lineage Key] = ? "
            "FROM [Fact].[Shipment] AS f "
            "INNER JOIN work.OrderLineEnriched AS s ON s.ShipmentBusinessKey = f.[WWI Shipment ID];",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    carrier = pkg.add(
        exec_proc(
            "Recalculate Carrier Performance",
            "EXEC Integration.LoadFactShipment @RecalculateCarrierPerformance = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Shipment"))
    rejects = pkg.add(_log_rejects("Fact.Shipment"))
    counts = pkg.add(log_row_count("Fact.Shipment"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, clear, load, milestones, carrier, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_order_fulfilment():
    pkg = _fact_package(
        "FACT_Load_OrderFulfilment",
        "Accumulating-snapshot load of Fact.Order Fulfilment - the order-to-cash pipeline. One row "
        "per order, with the ordered, allocated, picked, invoiced and cash-received milestones and "
        "the cycle-time measures between them maintained in place.",
        extra_variables=[("MilestoneUpdates", 0, "int"), ("StalledOrders", 0, "int")],
        extra_parameters=[
            ("StalledOrderDays", 14, "int", "Days without a milestone move before an order is flagged stalled."),
        ],
    )
    columns = [
        str_col("OrderNumber", 20),
        str_col("CustomerBusinessKey", 20),
        date_col("OrderedAt"),
        date_col("AllocatedAt"),
        date_col("PickedAt"),
        date_col("InvoicedAt"),
        date_col("CashReceivedAt"),
        money_col("OrderNetAmount"),
        money_col("InvoicedAmount"),
        money_col("CashReceivedAmount"),
        str_col("TransactionCurrency", 3),
        str_col("RegionCode", 6),
    ]
    flow = DataFlow("Load Fact Order Fulfilment", "Order-to-cash accumulating snapshot")
    flow.oledb_source(
        "stg OrderFulfilment",
        CONN_STAGING,
        "SELECT ofm.OrderNumber, ofm.CustomerBusinessKey, ofm.OrderedAt, ofm.AllocatedAt, ofm.PickedAt, "
        "ofm.InvoicedAt, ofm.CashReceivedAt, ofm.OrderNetAmount, ofm.InvoicedAmount, "
        "ofm.CashReceivedAmount, ofm.TransactionCurrency, ofm.RegionCode "
        "FROM stg.OrderFulfilment AS ofm WHERE ofm.LastModifiedAt > ? AND ofm.LastModifiedAt <= ?;",
        columns,
        timeout=1800,
    )
    _lookup_customer(flow)
    _lookup_date(flow, "OrderedAt", "OrderedDateKey", "Lookup Ordered Date Key")
    _lookup_date(flow, "InvoicedAt", "InvoicedDateKey", "Lookup Invoiced Date Key")
    _lookup_date(flow, "CashReceivedAt", "CashReceivedDateKey", "Lookup Cash Received Date Key")
    flow.lookup(
        "Lookup Existing Fulfilment Row",
        CONN_DW,
        "SELECT [Order Fulfilment Key] AS OrderFulfilmentKey, [Order Number] AS OrderNumber, "
        "[Milestone Status Code] AS ExistingMilestoneStatus FROM [Fact].[Order Fulfilment];",
        ["OrderNumber"],
        [bigint_col("OrderFulfilmentKey"), str_col("ExistingMilestoneStatus", 12)],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Cycle Times",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("MilestoneStatusCode",
             '!ISNULL(CashReceivedAt) ? "CASHED" : !ISNULL(InvoicedAt) ? "INVOICED" : '
             '!ISNULL(PickedAt) ? "PICKED" : !ISNULL(AllocatedAt) ? "ALLOCATED" : "ORDERED"',
             str_col("MilestoneStatusCode", 12)),
            ("OrderToAllocateHours",
             'ISNULL(AllocatedAt) ? -1 : DATEDIFF("Hour", OrderedAt, AllocatedAt)',
             int_col("OrderToAllocateHours")),
            ("OrderToInvoiceDays",
             'ISNULL(InvoicedAt) ? -1 : DATEDIFF("Day", OrderedAt, InvoicedAt)',
             int_col("OrderToInvoiceDays")),
            ("InvoiceToCashDays",
             'ISNULL(CashReceivedAt) || ISNULL(InvoicedAt) ? -1 : '
             'DATEDIFF("Day", InvoicedAt, CashReceivedAt)',
             int_col("InvoiceToCashDays")),
            ("OrderToCashDays",
             'ISNULL(CashReceivedAt) ? -1 : DATEDIFF("Day", OrderedAt, CashReceivedAt)',
             int_col("OrderToCashDays")),
            ("UninvoicedAmount", "OrderNetAmount - InvoicedAmount", money_col("UninvoicedAmount")),
            ("UncollectedAmount", "InvoicedAmount - CashReceivedAmount", money_col("UncollectedAmount")),
            ("IsStalled",
             'ISNULL(CashReceivedAt) && DATEDIFF("Day", OrderedAt, GETDATE()) > @[$Package::StalledOrderDays]',
             Column("IsStalled", "bool")),
            ("OrderNumberDegenerate", "OrderNumber", str_col("OrderNumberDegenerate", 20)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Fulfilment Rows",
        [
            ("New Order", "ISNULL(OrderFulfilmentKey)"),
            ("Stalled Orders", "IsStalled"),
        ],
        default_output="Milestone Update",
    )
    flow.branch_destination("Insert New Order", CONN_DW, "[Fact].[Order Fulfilment]",
                            "Route Fulfilment Rows", "New Order")
    flow.branch_destination("Stage Milestone Update", CONN_STAGING, "[work].[OrderLineEnriched]",
                            "Route Fulfilment Rows", "Milestone Update")
    flow.branch_destination("Stage Stalled Orders", CONN_STAGING, "[work].[OrderLineEnriched]",
                            "Route Fulfilment Rows", "Stalled Orders")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedOrderLine]",
                            "stg OrderFulfilment")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Order Fulfilment"))
    clear = pkg.add(truncate("[work].[OrderLineEnriched]", connection=CONN_STAGING,
                             name="Clear Fulfilment Work Table"))
    load = pkg.add(DataFlowTask(flow))
    milestones = pkg.add(
        ExecuteSql(
            "Update Fulfilment Milestones In Place",
            CONN_DW,
            "UPDATE f "
            "SET f.[Allocated At] = s.AllocatedAt, f.[Picked At] = s.PickedAt, "
            "    f.[Invoiced At] = s.InvoicedAt, f.[Cash Received At] = s.CashReceivedAt, "
            "    f.[Milestone Status Code] = s.MilestoneStatusCode, "
            "    f.[Order To Allocate Hours] = s.OrderToAllocateHours, "
            "    f.[Order To Invoice Days] = s.OrderToInvoiceDays, "
            "    f.[Invoice To Cash Days] = s.InvoiceToCashDays, "
            "    f.[Order To Cash Days] = s.OrderToCashDays, "
            "    f.[Uninvoiced Amount] = s.UninvoicedAmount, "
            "    f.[Uncollected Amount] = s.UncollectedAmount, "
            "    f.[Is Stalled] = s.IsStalled, f.[Lineage Key] = ? "
            "FROM [Fact].[Order Fulfilment] AS f "
            "INNER JOIN work.OrderLineEnriched AS s ON s.OrderNumber = f.[Order Number];",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    escalate = pkg.add(
        exec_proc(
            "Escalate Stalled Orders",
            "EXEC Integration.LoadFactOrderFulfilment @EscalateStalled = 1, @StalledOrderDays = ?, "
            "@LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::StalledOrderDays", 0, "LONG"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    mark = pkg.add(set_watermark("Fact.Order Fulfilment"))
    rejects = pkg.add(_log_rejects("Fact.Order Fulfilment"))
    counts = pkg.add(log_row_count("Fact.Order Fulfilment"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, clear, load, milestones, escalate, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_return():
    pkg = _fact_package(
        "FACT_Load_Return",
        "Incremental load of Fact.Return. Every return writes a reversal row against the original "
        "sale rather than adjusting it, and the restocking-fee rules differ by region because EU "
        "distance-selling rules forbid charging one inside the withdrawal window.",
        extra_variables=[("ReversalRowsWritten", 0, "int")],
        extra_parameters=[
            ("EuWithdrawalDays", 14, "int", "EU distance-selling withdrawal window."),
        ],
    )
    columns = [
        bigint_col("ReturnLineBusinessKey"),
        str_col("ReturnNumber", 20),
        str_col("OriginalInvoiceNumber", 20),
        int_col("OriginalInvoiceLineNumber"),
        str_col("CustomerBusinessKey", 20),
        bigint_col("StockItemBusinessKey"),
        date_col("ReturnDate"),
        date_col("OriginalInvoiceDate"),
        int_col("QuantityReturned"),
        money_col("UnitPrice"),
        money_col("UnitCost"),
        str_col("ReturnReasonCode", 8),
        str_col("RegionCode", 6),
        str_col("TransactionCurrency", 3),
    ]
    flow = DataFlow("Load Fact Return", "Returns as reversal rows against the original sale")
    flow.oledb_source(
        "stg Return",
        CONN_STAGING,
        "SELECT r.ReturnLineBusinessKey, r.ReturnNumber, r.OriginalInvoiceNumber, "
        "r.OriginalInvoiceLineNumber, r.CustomerBusinessKey, r.StockItemBusinessKey, r.ReturnDate, "
        "r.OriginalInvoiceDate, r.QuantityReturned, r.UnitPrice, r.UnitCost, r.ReturnReasonCode, "
        "r.RegionCode, r.TransactionCurrency "
        "FROM stg.[Return] AS r WHERE r.LastModifiedAt > ? AND r.LastModifiedAt <= ?;",
        columns,
    )
    _lookup_customer(flow)
    _lookup_stock_item(flow)
    _lookup_date(flow, "ReturnDate", "ReturnDateKey", "Lookup Return Date Key")
    _lookup_date(flow, "OriginalInvoiceDate", "OriginalInvoiceDateKey", "Lookup Original Invoice Date Key")
    flow.lookup(
        "Lookup Original Sale Row",
        CONN_DW,
        "SELECT [Sale Key] AS OriginalSaleKey, [Invoice Number] AS OriginalInvoiceNumber, "
        "[Invoice Line Number] AS OriginalInvoiceLineNumber FROM [Fact].[Sale];",
        ["OriginalInvoiceNumber", "OriginalInvoiceLineNumber"],
        [bigint_col("OriginalSaleKey")],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Return Measures",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("StockItemKeyResolved", "ISNULL(StockItemKey) ? 0 : StockItemKey", int_col("StockItemKeyResolved")),
            ("DaysSinceInvoice", 'DATEDIFF("Day", OriginalInvoiceDate, ReturnDate)', int_col("DaysSinceInvoice")),
            # Restocking fees: NA charges 15% outside 30 days, EU may not charge
            # inside the statutory withdrawal window, APAC applies a flat fee.
            ("RestockingFeeAmount",
             'RegionCode == "EU" && DATEDIFF("Day", OriginalInvoiceDate, ReturnDate) '
             '<= @[$Package::EuWithdrawalDays] ? (DT_NUMERIC,18,2)0 : '
             'RegionCode == "NA" && DATEDIFF("Day", OriginalInvoiceDate, ReturnDate) > 30 '
             '? ((DT_NUMERIC,18,2)QuantityReturned * UnitPrice) * (DT_NUMERIC,18,2)0.15 : '
             'RegionCode == "APAC" ? (DT_NUMERIC,18,2)5.00 : (DT_NUMERIC,18,2)0',
             money_col("RestockingFeeAmount")),
            ("ReversalGrossAmount", "((DT_NUMERIC,18,2)QuantityReturned * UnitPrice) * -1",
             money_col("ReversalGrossAmount")),
            ("ReversalCostAmount", "((DT_NUMERIC,18,2)QuantityReturned * UnitCost) * -1",
             money_col("ReversalCostAmount")),
            ("ReversalMarginAmount",
             "(((DT_NUMERIC,18,2)QuantityReturned * UnitPrice) - "
             "((DT_NUMERIC,18,2)QuantityReturned * UnitCost)) * -1",
             money_col("ReversalMarginAmount")),
            ("IsSaleable", 'ReturnReasonCode != "DMG" && ReturnReasonCode != "EXP"',
             Column("IsSaleable", "bool")),
            ("ReturnNumberDegenerate", "ReturnNumber", str_col("ReturnNumberDegenerate", 20)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Returns",
        [
            ("Orphan Return", "ISNULL(OriginalSaleKey)"),
            ("Damaged Stock", "!IsSaleable"),
        ],
        default_output="Saleable Returns",
    )
    flow.branch_destination("Insert Saleable Returns", CONN_DW, "[Fact].[Return]",
                            "Route Returns", "Saleable Returns")
    flow.branch_destination("Insert Damaged Returns", CONN_DW, "[Fact].[Return]",
                            "Route Returns", "Damaged Stock")
    flow.branch_destination("Reject Orphan Return", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Returns", "Orphan Return")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]", "stg Return")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Return"))
    load = pkg.add(DataFlowTask(flow))
    reversal = pkg.add(
        exec_proc(
            "Write Sale Reversal Rows",
            "EXEC Integration.LoadFactReturn @WriteSaleReversals = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Return"))
    rejects = pkg.add(_log_rejects("Fact.Return"))
    counts = pkg.add(log_row_count("Fact.Return"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, reversal, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_credit_note():
    pkg = _fact_package(
        "FACT_Load_CreditNote",
        "Incremental load of Fact.Credit Note. Unlike returns, credit notes restate the original "
        "sale in place - finance decided in 2014 that pricing corrections must not create ledger "
        "rows - so the package updates Fact.Sale and records the restatement.",
        extra_variables=[("RestatedSaleRows", 0, "int")],
    )
    columns = [
        bigint_col("CreditNoteLineBusinessKey"),
        str_col("CreditNoteNumber", 20),
        str_col("OriginalInvoiceNumber", 20),
        int_col("OriginalInvoiceLineNumber"),
        str_col("CustomerBusinessKey", 20),
        date_col("CreditNoteDate"),
        money_col("CreditAmount"),
        money_col("TaxCreditAmount"),
        str_col("CreditReasonCode", 8),
        str_col("ApprovedByCode", 12),
        str_col("TransactionCurrency", 3),
        str_col("RegionCode", 6),
    ]
    flow = DataFlow("Load Fact Credit Note", "Credit notes with in-place sale restatement")
    flow.oledb_source(
        "stg CreditNote",
        CONN_STAGING,
        "SELECT cn.CreditNoteLineBusinessKey, cn.CreditNoteNumber, cn.OriginalInvoiceNumber, "
        "cn.OriginalInvoiceLineNumber, cn.CustomerBusinessKey, cn.CreditNoteDate, cn.CreditAmount, "
        "cn.TaxCreditAmount, cn.CreditReasonCode, cn.ApprovedByCode, cn.TransactionCurrency, cn.RegionCode "
        "FROM stg.CreditNote AS cn WHERE cn.LastModifiedAt > ? AND cn.LastModifiedAt <= ?;",
        columns,
    )
    _lookup_customer(flow)
    _lookup_date(flow, "CreditNoteDate", "CreditNoteDateKey", "Lookup Credit Note Date Key")
    flow.lookup(
        "Lookup Original Sale Row",
        CONN_DW,
        "SELECT [Sale Key] AS OriginalSaleKey, [Invoice Number] AS OriginalInvoiceNumber, "
        "[Invoice Line Number] AS OriginalInvoiceLineNumber, [Net Amount] AS OriginalNetAmount "
        "FROM [Fact].[Sale];",
        ["OriginalInvoiceNumber", "OriginalInvoiceLineNumber"],
        [bigint_col("OriginalSaleKey"), money_col("OriginalNetAmount")],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Credit Measures",
        [
            ("CustomerKeyResolved", "ISNULL(CustomerKey) ? 0 : CustomerKey", int_col("CustomerKeyResolved")),
            ("RestatedNetAmount",
             "ISNULL(OriginalNetAmount) ? (DT_NUMERIC,18,2)0 : OriginalNetAmount - CreditAmount",
             money_col("RestatedNetAmount")),
            ("CreditPercentOfOriginal",
             "ISNULL(OriginalNetAmount) || OriginalNetAmount == 0 ? (DT_NUMERIC,9,4)0 : "
             "((DT_NUMERIC,9,4)CreditAmount / (DT_NUMERIC,9,4)OriginalNetAmount) * 100",
             DEC("CreditPercentOfOriginal", 9, 4)),
            ("RequiresSecondApproval",
             "CreditAmount > 10000 || (ISNULL(OriginalNetAmount) ? 0 : CreditAmount) > "
             "(ISNULL(OriginalNetAmount) ? (DT_NUMERIC,18,2)0 : OriginalNetAmount)",
             Column("RequiresSecondApproval", "bool")),
            ("TaxAdjustmentCode",
             'RegionCode == "NA" ? "SALESTAX-ADJ" : RegionCode == "EU" ? "VAT-CREDIT" : "GST-ADJ"',
             str_col("TaxAdjustmentCode", 14)),
            ("CreditNoteDegenerate", "CreditNoteNumber", str_col("CreditNoteDegenerate", 20)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Credit Notes",
        [
            ("Orphan Credit Note", "ISNULL(OriginalSaleKey)"),
            ("Requires Approval", "RequiresSecondApproval"),
        ],
        default_output="Approved Credits",
    )
    flow.branch_destination("Insert Approved Credits", CONN_DW, "[Fact].[Credit Note]",
                            "Route Credit Notes", "Approved Credits")
    flow.branch_destination("Hold For Approval", CONN_STAGING, "[work].[PaymentMatched]",
                            "Route Credit Notes", "Requires Approval")
    flow.branch_destination("Reject Orphan Credit Note", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Credit Notes", "Orphan Credit Note")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]", "stg CreditNote")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Credit Note"))
    load = pkg.add(DataFlowTask(flow))
    restate = pkg.add(
        ExecuteSql(
            "Restate Original Sale Rows",
            CONN_DW,
            "UPDATE s "
            "SET s.[Net Amount] = s.[Net Amount] - c.[Credit Amount], "
            "    s.[Tax Amount] = s.[Tax Amount] - c.[Tax Credit Amount], "
            "    s.[Margin Amount] = (s.[Net Amount] - c.[Credit Amount]) - s.[Total Cost Amount], "
            "    s.[Is Restated] = 1, s.[Restated By Lineage Key] = ? "
            "FROM [Fact].[Sale] AS s "
            "INNER JOIN [Fact].[Credit Note] AS c ON c.[Original Sale Key] = s.[Sale Key] "
            "WHERE c.[Lineage Key] = ?;",
            parameter_bindings=[
                ("User::PackageExecutionId", 0, "LONG"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    audit = pkg.add(
        exec_proc(
            "Record Restatement Audit",
            "EXEC Integration.LoadFactCreditNote @RecordRestatementAudit = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Credit Note"))
    rejects = pkg.add(_log_rejects("Fact.Credit Note"))
    counts = pkg.add(log_row_count("Fact.Credit Note"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, restate, audit, mark, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Finance
# ---------------------------------------------------------------------------


def build_fact_load_transaction():
    pkg = _fact_package(
        "FACT_Load_Transaction",
        "Incremental load of Fact.Transaction, the combined customer/supplier transaction ledger the "
        "1990s finance reports still read. Running balances are recalculated per account after the "
        "load because the source only supplies deltas.",
        extra_variables=[("BalanceRowsUpdated", 0, "int")],
    )
    columns = [
        bigint_col("TransactionBusinessKey"),
        str_col("TransactionTypeCode", 8),
        str_col("PartyBusinessKey", 20),
        str_col("PartyTypeCode", 4),
        date_col("TransactionDate"),
        money_col("AmountExcludingTax"),
        money_col("TaxAmount"),
        money_col("TransactionAmount"),
        str_col("TransactionCurrency", 3),
        str_col("SourceDocumentNumber", 30),
        str_col("AccountingPeriodCode", 7),
    ]
    flow = DataFlow("Load Fact Transaction", "Combined ledger transactions")
    flow.oledb_source(
        "stg Transaction",
        CONN_STAGING,
        "SELECT t.TransactionBusinessKey, t.TransactionTypeCode, t.PartyBusinessKey, t.PartyTypeCode, "
        "t.TransactionDate, t.AmountExcludingTax, t.TaxAmount, t.TransactionAmount, t.TransactionCurrency, "
        "t.SourceDocumentNumber, t.AccountingPeriodCode "
        "FROM stg.[Transaction] AS t WHERE t.LastModifiedAt > ? AND t.LastModifiedAt <= ?;",
        columns,
        timeout=2400,
    )
    flow.lookup(
        "Lookup Transaction Type",
        CONN_DW,
        "SELECT [Transaction Type Key] AS TransactionTypeKey, [WWI Transaction Type ID] AS TransactionTypeCode "
        "FROM [Dimension].[Transaction Type];",
        ["TransactionTypeCode"],
        [int_col("TransactionTypeKey")],
        no_match="RD",
    )
    _lookup_date(flow, "TransactionDate", "TransactionDateKey", "Lookup Transaction Date Key")
    flow.derived_column(
        "Derive Ledger Attributes",
        [
            ("TransactionTypeKeyResolved", "ISNULL(TransactionTypeKey) ? 0 : TransactionTypeKey",
             int_col("TransactionTypeKeyResolved")),
            ("IsCustomerSide", 'PartyTypeCode == "CUST"', Column("IsCustomerSide", "bool")),
            ("TaxInclusiveCheck", "AmountExcludingTax + TaxAmount", money_col("TaxInclusiveCheck")),
            ("BalanceCheckVariance", "TransactionAmount - (AmountExcludingTax + TaxAmount)",
             money_col("BalanceCheckVariance")),
            ("SourceDocumentDegenerate", "SourceDocumentNumber", str_col("SourceDocumentDegenerate", 30)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Ledger Rows",
        [
            ("Unbalanced Rows", "ABS(BalanceCheckVariance) > 0.01"),
            ("Customer Side", "IsCustomerSide"),
            ("Supplier Side", 'PartyTypeCode == "SUPP"'),
        ],
        default_output="Other Party Types",
    )
    flow.branch_destination("Insert Customer Side", CONN_DW, "[Fact].[Transaction]",
                            "Route Ledger Rows", "Customer Side")
    flow.branch_destination("Insert Supplier Side", CONN_DW, "[Fact].[Transaction]",
                            "Route Ledger Rows", "Supplier Side")
    flow.branch_destination("Insert Other Party Types", CONN_DW, "[Fact].[Transaction]",
                            "Route Ledger Rows", "Other Party Types")
    flow.branch_destination("Reject Unbalanced Rows", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Ledger Rows", "Unbalanced Rows")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg Transaction")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.Transaction"))
    load = pkg.add(DataFlowTask(flow))
    balances = pkg.add(
        exec_proc(
            "Recalculate Running Balances",
            "EXEC Integration.LoadFactTransaction @RecalculateBalances = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.Transaction"))
    rejects = pkg.add(_log_rejects("Fact.Transaction"))
    counts = pkg.add(log_row_count("Fact.Transaction"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, load, balances, mark, rejects, counts, done)
    return _write(pkg)


def build_fact_load_gl_posting():
    pkg = _fact_package(
        "FACT_Load_GLPosting",
        "Incremental load of Fact.GL Posting from the Oracle-sourced general ledger extract in "
        "staging. Journals must balance by batch before any row is loaded; an unbalanced batch is "
        "rejected whole, which is how the 2015 close was saved.",
        extra_variables=[("UnbalancedBatches", 0, "int")],
        extra_parameters=[
            ("AllowedRoundingVariance", 1, "int", "Cents of rounding tolerated per journal batch."),
        ],
    )
    columns = [
        bigint_col("GlPostingBusinessKey"),
        str_col("JournalBatchNumber", 24),
        int_col("JournalLineNumber"),
        str_col("GlAccountCode", 20),
        str_col("CostCentreCode", 12),
        str_col("LegalEntityCode", 8),
        date_col("PostingDate"),
        str_col("AccountingPeriodCode", 7),
        money_col("DebitAmount"),
        money_col("CreditAmount"),
        str_col("TransactionCurrency", 3),
        str_col("JournalSourceCode", 8),
    ]
    flow = DataFlow("Load Fact GL Posting", "General ledger postings with batch balancing")
    flow.oledb_source(
        "stg GLPosting",
        CONN_STAGING,
        "SELECT gl.GlPostingBusinessKey, gl.JournalBatchNumber, gl.JournalLineNumber, gl.GlAccountCode, "
        "gl.CostCentreCode, gl.LegalEntityCode, gl.PostingDate, gl.AccountingPeriodCode, gl.DebitAmount, "
        "gl.CreditAmount, gl.TransactionCurrency, gl.JournalSourceCode "
        "FROM stg.GLPosting AS gl WHERE gl.LastModifiedAt > ? AND gl.LastModifiedAt <= ? "
        "ORDER BY gl.JournalBatchNumber, gl.JournalLineNumber;",
        columns,
        timeout=2400,
    )
    flow.lookup(
        "Lookup GL Account Key",
        CONN_DW,
        "SELECT [GL Account Key] AS GlAccountKey, [WWI GL Account ID] AS GlAccountCode, "
        "[Account Type Code] AS AccountTypeCode FROM [Dimension].[GL Account];",
        ["GlAccountCode"],
        [int_col("GlAccountKey"), str_col("AccountTypeCode", 8)],
        no_match="RD",
    )
    _lookup_date(flow, "PostingDate", "PostingDateKey", "Lookup Posting Date Key")
    flow.derived_column("Derive FX Rate Date", [("FxRateDate", "(DT_DBDATE)PostingDate", date_col("FxRateDate"))])
    _lookup_fx(flow)
    flow.derived_column(
        "Derive Posting Measures",
        [
            ("GlAccountKeyResolved", "ISNULL(GlAccountKey) ? 0 : GlAccountKey", int_col("GlAccountKeyResolved")),
            ("SignedAmount", "DebitAmount - CreditAmount", money_col("SignedAmount")),
            ("SignedAmountReporting",
             "(DebitAmount - CreditAmount) * (ISNULL(FxRateToUsd) ? (DT_NUMERIC,18,8)1 : FxRateToUsd)",
             money_col("SignedAmountReporting")),
            ("PostingSideCode", 'DebitAmount > 0 ? "DR" : "CR"', str_col("PostingSideCode", 2)),
            ("IsManualJournal", 'JournalSourceCode == "MANUAL" || JournalSourceCode == "SPREAD"',
             Column("IsManualJournal", "bool")),
            ("JournalBatchDegenerate", "JournalBatchNumber", str_col("JournalBatchDegenerate", 24)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route GL Postings",
        [
            ("Unknown Account", "ISNULL(GlAccountKey)"),
            ("Manual Journals", "IsManualJournal"),
        ],
        default_output="System Journals",
    )
    flow.branch_destination("Insert System Journals", CONN_DW, "[Fact].[GL Posting]",
                            "Route GL Postings", "System Journals")
    flow.branch_destination("Insert Manual Journals", CONN_DW, "[Fact].[GL Posting]",
                            "Route GL Postings", "Manual Journals")
    flow.branch_destination("Reject Unknown Account", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route GL Postings", "Unknown Account")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg GLPosting")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    watermark = pkg.add(get_watermark(object_name="Fact.GL Posting"))
    balance_check = pkg.add(
        ExecuteSql(
            "Quarantine Unbalanced Batches",
            CONN_STAGING,
            "INSERT INTO err.RejectedConstraintViolation "
            "    (ObjectName, BusinessKey, RejectReason, RejectedAt, PackageExecutionId) "
            "SELECT N'stg.GLPosting', gl.JournalBatchNumber, "
            "       N'Journal batch does not balance', GETDATE(), ? "
            "FROM stg.GLPosting AS gl "
            "GROUP BY gl.JournalBatchNumber "
            "HAVING ABS(SUM(gl.DebitAmount) - SUM(gl.CreditAmount)) > (CAST(? AS decimal(18,2)) / 100.0);",
            parameter_bindings=[
                ("User::PackageExecutionId", 0, "LONG"),
                ("$Package::AllowedRoundingVariance", 1, "LONG"),
            ],
        )
    )
    load = pkg.add(DataFlowTask(flow))
    close_check = pkg.add(
        exec_proc(
            "Flag Closed Period Postings",
            "EXEC Integration.LoadFactGLPosting @FlagClosedPeriods = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    mark = pkg.add(set_watermark("Fact.GL Posting"))
    rejects = pkg.add(_log_rejects("Fact.GL Posting"))
    counts = pkg.add(log_row_count("Fact.GL Posting"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, watermark, balance_check, load, close_check, mark, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Housekeeping passes
# ---------------------------------------------------------------------------


def build_fact_dedup_sale():
    pkg = _fact_package(
        "FACT_Dedup_Sale",
        "Deduplication pass over Fact.Sale. The three regional sale loads can each pick up the same "
        "invoice line when a region code is corrected mid-day, so duplicates are detected on the "
        "natural key, the survivor is the highest lineage key, and the losers are archived before "
        "deletion.",
        extra_variables=[("DuplicateGroups", 0, "int"), ("RowsArchived", 0, "int")],
        extra_parameters=[
            ("LookbackDays", 7, "int", "Days of Fact.Sale history examined for duplicates."),
        ],
    )
    columns = [
        bigint_col("SaleKey"),
        str_col("InvoiceNumber", 20),
        int_col("InvoiceLineNumber"),
        date_col("InvoiceDateKey"),
        int_col("CustomerKey"),
        int_col("StockItemKey"),
        money_col("NetAmount"),
        bigint_col("LineageKey"),
    ]
    flow = DataFlow("Detect Duplicate Sale Rows", "Natural-key duplicate detection")
    flow.oledb_source(
        "Fact Sale Recent Window",
        CONN_DW,
        "SELECT f.[Sale Key] AS SaleKey, f.[Invoice Number] AS InvoiceNumber, "
        "f.[Invoice Line Number] AS InvoiceLineNumber, f.[Invoice Date Key] AS InvoiceDateKey, "
        "f.[Customer Key] AS CustomerKey, f.[Stock Item Key] AS StockItemKey, "
        "f.[Net Amount] AS NetAmount, f.[Lineage Key] AS LineageKey "
        "FROM [Fact].[Sale] AS f "
        "INNER JOIN Dimension.Date AS d ON d.[Date] = f.[Invoice Date Key] "
        "WHERE d.[Date] >= DATEADD(DAY, -1 * ?, CAST(GETDATE() AS date)) "
        "ORDER BY f.[Invoice Number], f.[Invoice Line Number], f.[Lineage Key] DESC;",
        columns,
        timeout=3600,
    )
    flow.sort("Sort By Natural Key", ["InvoiceNumber", "InvoiceLineNumber", "LineageKey"])
    flow.aggregate(
        "Count Rows Per Natural Key",
        ["InvoiceNumber", "InvoiceLineNumber"],
        [
            ("SaleKey", "RowCountForKey", "count"),
            ("LineageKey", "SurvivingLineageKey", "max"),
            ("NetAmount", "TotalNetAmount", "sum"),
        ],
    )
    flow.derived_column(
        "Flag Duplicate Groups",
        [
            ("IsDuplicateGroup", "RowCountForKey > 1", Column("IsDuplicateGroup", "bool")),
            ("DuplicateRowCount", "RowCountForKey - 1", int_col("DuplicateRowCount")),
            ("DetectedAt", "GETDATE()", date_col("DetectedAt")),
            ("LineageKeyAudit", "@[User::PackageExecutionId]", bigint_col("LineageKeyAudit")),
        ],
    )
    flow.conditional_split(
        "Route Duplicate Groups",
        [("Duplicate Groups", "IsDuplicateGroup")],
        default_output="Unique Rows",
    )
    flow.branch_destination("Stage Duplicate Groups", CONN_STAGING, "[work].[SaleLineEnriched]",
                            "Route Duplicate Groups", "Duplicate Groups")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Fact Sale Recent Window")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("[work].[SaleLineEnriched]", connection=CONN_STAGING, name="Clear Dedup Work Table"))
    detect = pkg.add(DataFlowTask(flow))
    archive = pkg.add(
        ExecuteSql(
            "Archive Losing Rows",
            CONN_DW,
            "INSERT INTO [Fact].[Sale Duplicate Archive] "
            "    ([Sale Key], [Invoice Number], [Invoice Line Number], [Net Amount], "
            "     [Original Lineage Key], [Archived By Lineage Key], [Archived At]) "
            "SELECT f.[Sale Key], f.[Invoice Number], f.[Invoice Line Number], f.[Net Amount], "
            "       f.[Lineage Key], ?, GETDATE() "
            "FROM [Fact].[Sale] AS f "
            "WHERE EXISTS (SELECT 1 FROM [Fact].[Sale] AS d "
            "              WHERE d.[Invoice Number] = f.[Invoice Number] "
            "                AND d.[Invoice Line Number] = f.[Invoice Line Number] "
            "                AND d.[Lineage Key] > f.[Lineage Key]);",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    remove = pkg.add(
        exec_proc(
            "Delete Superseded Duplicates",
            "EXEC Integration.DeduplicateFactSale @LookbackDays = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::LookbackDays", 0, "LONG"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    rejects = pkg.add(_log_rejects("Fact.Sale"))
    counts = pkg.add(log_row_count("Fact.Sale"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, detect, archive, remove, rejects, counts, done)
    return _write(pkg)


def build_fact_apply_corrections():
    pkg = _fact_package(
        "FACT_Apply_Corrections",
        "Month-end correction pass. Reads work.FactRekeyQueue and applies the two correction "
        "styles the estate uses: reversal-and-replace rows for Fact.Sale, where the audit trail "
        "must show the original, and in-place restatement for Fact.Order and Fact.Payment, where "
        "the operational reports only ever show the latest position.",
        extra_variables=[("ReversalRowsWritten", 0, "int"), ("RestatedRows", 0, "int")],
        extra_parameters=[
            ("CorrectionPeriodCode", "1900-01", "string", "Accounting period the corrections are posted into."),
            ("MaxCorrectionsPerRun", 50000, "int", "Safety valve after the 2016 runaway correction run."),
        ],
    )
    columns = [
        bigint_col("CorrectionQueueId"),
        str_col("TargetFactName", 40),
        bigint_col("TargetFactKey"),
        str_col("CorrectionTypeCode", 12),
        str_col("BusinessKey", 40),
        money_col("CorrectedAmount"),
        int_col("CorrectedDimensionKey"),
        str_col("RaisedByCode", 12),
        date_col("RaisedAt"),
    ]
    flow = DataFlow("Classify Corrections", "Split corrections by target fact and style")
    flow.oledb_source(
        "work FactRekeyQueue",
        CONN_STAGING,
        "SELECT q.CorrectionQueueId, q.TargetFactName, q.TargetFactKey, q.CorrectionTypeCode, "
        "q.BusinessKey, q.CorrectedAmount, q.CorrectedDimensionKey, q.RaisedByCode, q.RaisedAt "
        "FROM work.FactRekeyQueue AS q WHERE q.AppliedAt IS NULL ORDER BY q.RaisedAt;",
        columns,
    )
    flow.derived_column(
        "Derive Correction Attributes",
        [
            ("CorrectionAgeDays", 'DATEDIFF("Day", RaisedAt, GETDATE())', int_col("CorrectionAgeDays")),
            ("CorrectionStyleCode",
             'TargetFactName == "Fact.Sale" ? "REVERSAL" : "RESTATEMENT"',
             str_col("CorrectionStyleCode", 12)),
            ("SignedCorrectionAmount",
             'CorrectionTypeCode == "DECREASE" ? CorrectedAmount * -1 : CorrectedAmount',
             money_col("SignedCorrectionAmount")),
            ("AccountingPeriodCode", "@[$Package::CorrectionPeriodCode]", str_col("AccountingPeriodCode", 7)),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Corrections",
        [
            ("Sale Reversals", 'TargetFactName == "Fact.Sale"'),
            ("Order Restatements", 'TargetFactName == "Fact.Order"'),
            ("Payment Restatements", 'TargetFactName == "Fact.Payment"'),
        ],
        default_output="Unroutable Corrections",
    )
    flow.branch_destination("Stage Sale Reversals", CONN_STAGING, "[work].[SaleLineEnriched]",
                            "Route Corrections", "Sale Reversals")
    flow.branch_destination("Stage Order Restatements", CONN_STAGING, "[work].[OrderLineEnriched]",
                            "Route Corrections", "Order Restatements")
    flow.branch_destination("Stage Payment Restatements", CONN_STAGING, "[work].[PaymentMatched]",
                            "Route Corrections", "Payment Restatements")
    flow.branch_destination("Reject Unroutable Corrections", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Corrections", "Unroutable Corrections")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "work FactRekeyQueue")

    init = pkg.add(_init_window())
    start = pkg.add(log_package_start(pkg))
    guard = pkg.add(
        ExecuteSql(
            "Check Correction Volume",
            CONN_STAGING,
            "SELECT COUNT_BIG(*) AS PendingCorrections FROM work.FactRekeyQueue WHERE AppliedAt IS NULL;",
            result_type="ResultSetType_SingleRow",
            result_bindings=[("0", "User::RowsRead")],
        )
    )
    clear = pkg.add(truncate("[work].[SaleLineEnriched]", connection=CONN_STAGING,
                             name="Clear Correction Work Table"))
    classify = pkg.add(DataFlowTask(flow))

    apply_container = Container("Apply Corrections", kind="sequence",
                                description="Reversal rows first, then in-place restatements")
    reversals = apply_container.add(
        ExecuteSql(
            "Write Sale Reversal And Replacement Rows",
            CONN_DW,
            "INSERT INTO [Fact].[Sale] "
            "    ([Invoice Number], [Invoice Line Number], [Customer Key], [Stock Item Key], "
            "     [Invoice Date Key], [Net Amount], [Tax Amount], [Margin Amount], "
            "     [Is Reversal], [Reverses Sale Key], [Lineage Key]) "
            "SELECT s.[Invoice Number], s.[Invoice Line Number], s.[Customer Key], s.[Stock Item Key], "
            "       s.[Invoice Date Key], s.[Net Amount] * -1, s.[Tax Amount] * -1, "
            "       s.[Margin Amount] * -1, 1, s.[Sale Key], ? "
            "FROM [Fact].[Sale] AS s "
            "INNER JOIN work.SaleLineEnriched AS c ON c.TargetFactKey = s.[Sale Key] "
            "WHERE s.[Is Reversal] = 0;",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    restate_orders = apply_container.add(
        ExecuteSql(
            "Restate Order Rows In Place",
            CONN_DW,
            "UPDATE f SET f.[Ordered Net Amount] = c.CorrectedAmount, "
            "    f.[Customer Key] = COALESCE(NULLIF(c.CorrectedDimensionKey, 0), f.[Customer Key]), "
            "    f.[Is Restated] = 1, f.[Restated By Lineage Key] = ? "
            "FROM [Fact].[Order] AS f "
            "INNER JOIN work.OrderLineEnriched AS c ON c.TargetFactKey = f.[Order Key];",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    restate_payments = apply_container.add(
        ExecuteSql(
            "Restate Payment Rows In Place",
            CONN_DW,
            "UPDATE f SET f.[Payment Amount] = c.CorrectedAmount, "
            "    f.[Unapplied Amount] = c.CorrectedAmount - f.[Allocated Amount], "
            "    f.[Is Restated] = 1, f.[Restated By Lineage Key] = ? "
            "FROM [Fact].[Payment] AS f "
            "INNER JOIN work.PaymentMatched AS c ON c.TargetFactKey = f.[Payment Key];",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    apply_container.chain(reversals, restate_orders, restate_payments)
    apply_all = pkg.add(apply_container)

    post = pkg.add(
        exec_proc(
            "Post Correction Audit Trail",
            "EXEC Integration.ApplyFactCorrections @CorrectionPeriodCode = ?, @MaxCorrections = ?, "
            "@LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::CorrectionPeriodCode", 0, "NVARCHAR"),
                ("$Package::MaxCorrectionsPerRun", 1, "LONG"),
                ("User::PackageExecutionId", 2, "LONG"),
            ],
        )
    )
    close_queue = pkg.add(
        ExecuteSql(
            "Close Applied Corrections",
            CONN_STAGING,
            "UPDATE work.FactRekeyQueue SET AppliedAt = GETDATE(), AppliedByExecutionId = ? "
            "WHERE AppliedAt IS NULL;",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    rejects = pkg.add(_log_rejects("work.FactRekeyQueue"))
    counts = pkg.add(log_row_count("Fact.Sale"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, guard, clear, classify, apply_all, post, close_queue, rejects, counts, done)
    return _write(pkg)


BUILDERS = [
    build_fact_na_load_sale,
    build_fact_eu_load_sale,
    build_fact_apac_load_sale,
    build_fact_load_order,
    build_fact_load_purchase,
    build_fact_load_purchase_receipt,
    build_fact_load_payment,
    build_fact_load_supplier_payment,
    build_fact_load_movement,
    build_fact_load_stock_holding,
    build_fact_load_transaction,
    build_fact_load_customer_transaction,
    build_fact_load_supplier_transaction,
    build_fact_load_shipment,
    build_fact_load_return,
    build_fact_load_credit_note,
    build_fact_load_loyalty_points,
    build_fact_load_web_session,
    build_fact_load_daily_inventory_snapshot,
    build_fact_load_daily_sales_snapshot,
    build_fact_load_order_fulfilment,
    build_fact_load_gl_posting,
    build_fact_dedup_sale,
    build_fact_apply_corrections,
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
