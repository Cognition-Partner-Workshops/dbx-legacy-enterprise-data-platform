"""Spec module for the WWI_DataQuality SSIS project (ssis/05_data_quality).

Emits the ten data-quality packages declared in config/estate-catalog.yaml for
folder 05_data_quality. These packages profile and assert; they do not conform
or reshape data. Each one measures the staged data, writes rule outcomes to
etl.DataQualityResult, routes offending rows to err.* through
etl.usp_LogRejectedRecord and then raises a controlled warning or failure via
the etl control framework so that a quality problem can never pass silently.

Run from the repository root:

    python3 ssis/05_data_quality/build_quality_packages.py

The generated .dtsx, .dtproj, .conmgr and Project.params files are committed
alongside this module. Nothing here connects to a database.
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

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
    CONN_STAGING,
    exec_proc,
    log_package_start,
    log_package_success,
    log_row_count,
    new_package,
)
import project  # noqa: E402

PROJECT_NAME = "WWI_DataQuality"
PROJECT_CONNECTIONS = ["WWI_Staging_DB", "WWI_Source_DB", "WWI_Oracle_ERP"]

ORA = "ORA_ERP"
OLTP = "WWI_OLTP"
FILE = "PARTNER_FILE"

BUILDERS = []


def package(func):
    BUILDERS.append(func)
    return func


def dec_col(name, precision=18, scale=6):
    return Column(name, "numeric", precision=precision, scale=scale)


def bool_col(name):
    return Column(name, "bool")


# ---------------------------------------------------------------------------
# quality-specific control-flow helpers
# ---------------------------------------------------------------------------


def evaluate_rules(rule_group, object_name):
    """Run the configured rule set for one staged object.

    etl.usp_EvaluateDataQualityRules reads etl.DataQualityRule and writes one
    etl.DataQualityResult row per rule, returning the failed-rule count so the
    package can decide between warning and failure.
    """
    return ExecuteSql(
        "Evaluate Rules - %s" % rule_group,
        CONN_STAGING,
        "EXEC etl.usp_EvaluateDataQualityRules @BatchId = ?, @PackageExecutionId = ?, "
        "@RuleGroupCode = N'%s', @ObjectName = N'%s', @FailedRuleCount = ? OUTPUT;"
        % (rule_group, object_name),
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("User::PackageExecutionId", 1, "LONG"),
        ],
        result_bindings=[("0", "User::FailedRuleCount")],
        is_stored_procedure=True,
    )


def assert_reconciliation(name="Assert Row Count Reconciliation", raise_on_failure=1):
    return ExecuteSql(
        name,
        CONN_STAGING,
        "EXEC etl.usp_AssertRowCountReconciliation @BatchId = ?, @RaiseOnFailure = %d, "
        "@FailedObjectCount = ? OUTPUT;" % raise_on_failure,
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::FailedObjectCount")],
        is_stored_procedure=True,
    )


def record_measure(name, object_name, measure_code, sql_expression, threshold_variable):
    """Persist one profiling measure into etl.DataQualityResult.

    The measure itself is a scalar SELECT evaluated on the staging server; the
    result is pushed back into an SSIS variable so the gate task can compare it
    against the configured threshold.
    """
    return ExecuteSql(
        name,
        CONN_STAGING,
        "SELECT @Measure = %s;\n"
        "INSERT INTO etl.DataQualityResult\n"
        "    (BatchId, PackageExecutionId, ObjectName, RuleCode, MeasuredValue, EvaluatedAtUtc)\n"
        "VALUES (?, ?, N'%s', N'%s', @Measure, SYSUTCDATETIME());\n"
        "SELECT @Measure AS MeasuredValue;" % (sql_expression, object_name, measure_code),
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("User::PackageExecutionId", 1, "LONG"),
        ],
        result_bindings=[("0", threshold_variable)],
    )


def register_rejects(err_table, object_name, key_expression, reason_column, stage="Quality"):
    """Row-by-row reject registration through the control framework."""
    sql = (
        "DECLARE @RejectId BIGINT, @Key NVARCHAR(200), @Reason NVARCHAR(50);\n"
        "DECLARE dq_cur CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT r.RejectedRowId, %s, r.%s\n"
        "    FROM %s AS r\n"
        "    WHERE r.BatchId = ? AND r.LoggedToControl = 0;\n"
        "OPEN dq_cur;\n"
        "FETCH NEXT FROM dq_cur INTO @RejectId, @Key, @Reason;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    EXEC etl.usp_LogRejectedRecord @PackageExecutionId = ?, @BatchId = ?, "
        "@SourceSystemCode = ?, @ObjectName = N'%s', @BusinessKey = @Key, "
        "@RejectReasonCode = @Reason, @RejectStage = N'%s';\n"
        "    UPDATE %s SET LoggedToControl = 1 WHERE RejectedRowId = @RejectId;\n"
        "    FETCH NEXT FROM dq_cur INTO @RejectId, @Key, @Reason;\n"
        "END\n"
        "CLOSE dq_cur;\n"
        "DEALLOCATE dq_cur;"
        % (key_expression, reason_column, err_table, object_name, stage, err_table)
    )
    return ExecuteSql(
        "Register Quality Rejects - %s" % object_name,
        CONN_STAGING,
        sql,
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("User::PackageExecutionId", 1, "LONG"),
            ("$Package::BatchId", 2, "LONG"),
            ("$Package::SourceSystemCode", 3, "NVARCHAR"),
        ],
    )


def raise_gate(name, expression_description, severity="Warning"):
    """A controlled warning or failure raised through the control framework.

    The task is wired behind an expression-guarded precedence constraint, so it
    only runs when the screen has actually breached its threshold.
    """
    if severity == "Failure":
        sql = (
            "EXEC etl.usp_LogError @PackageExecutionId = ?, @BatchId = ?, @ErrorCode = 50001, "
            "@ErrorDescription = N'%s', @SourceName = ?;\n"
            "THROW 50001, N'%s', 1;" % (expression_description, expression_description)
        )
    else:
        sql = (
            "EXEC etl.usp_LogError @PackageExecutionId = ?, @BatchId = ?, @ErrorCode = 50000, "
            "@ErrorDescription = N'%s', @SourceName = ?;" % expression_description
        )
    return ExecuteSql(
        name,
        CONN_STAGING,
        sql,
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("$Package::BatchId", 1, "LONG"),
            ("System::PackageName", 2, "NVARCHAR"),
        ],
    )


QUALITY_VARIABLES = [
    ("FailedRuleCount", 0, "int"),
    ("FailedObjectCount", 0, "int"),
    ("MeasuredValue", 0, "decimal"),
    ("SecondaryMeasure", 0, "decimal"),
    ("ThresholdPercent", 0, "decimal"),
    ("QualityScore", 0, "decimal"),
]


def build_quality_package(name, description, source_system, object_name, flows=(),
                          pre_tasks=(), post_tasks=(), gates=(), reject_task=None,
                          extra_variables=(), connections=(CONN_STAGING,)):
    """Assemble a screening package: measure, evaluate, route, then gate."""
    pkg = new_package(name, description, source_system=source_system, connections=connections,
                      extra_variables=QUALITY_VARIABLES + list(extra_variables))
    ordered = [pkg.add(log_package_start(pkg))]
    for task in pre_tasks:
        ordered.append(pkg.add(task))
    for flow in flows:
        ordered.append(pkg.add(DataFlowTask(flow)))
    if reject_task is not None:
        ordered.append(pkg.add(reject_task))
    for task in post_tasks:
        ordered.append(pkg.add(task))
    ordered.append(pkg.add(log_row_count(object_name)))
    success = pkg.add(log_package_success())
    ordered.append(success)
    pkg.chain(*ordered)
    # Gates hang off the row-count task on an expression-guarded constraint so a
    # clean run skips them entirely.
    guard_source = ordered[-2]
    for task, expression in gates:
        added = pkg.add(task)
        pkg.link(guard_source, added, value="Success", expression=expression, logical_and=True)
        pkg.link(added, success, value="Completion", expression=None, logical_and=False)
    return pkg


# ---------------------------------------------------------------------------
# entity screens
# ---------------------------------------------------------------------------


@package
def dq_customer_screen():
    """Completeness, domain and consent screening of stg.Customer."""
    cols = [
        str_col("CustomerCode", 20), str_col("CustomerName", 100), str_col("CountryCode", 3),
        str_col("RegionCode", 4), str_col("CustomerClassCode", 10), str_col("TaxRegistrationNumber", 30),
        str_col("MarketingConsentFlag", 1), int_col("RetentionMonths"), money_col("CreditLimitAmount"),
    ]
    flow = DataFlow("DFT Screen Customer", "Rule evaluation over the conformed customer set")
    flow.oledb_source(
        "STG Customer", CONN_STAGING,
        "SELECT CustomerCode, CustomerName, CountryCode, RegionCode, CustomerClassCode,\n"
        "       TaxRegistrationNumber, MarketingConsentFlag, RetentionMonths, CreditLimitAmount\n"
        "FROM stg.Customer WHERE BatchId = ?;",
        cols, timeout=3600)
    flow.row_count("Count Customers Screened", "User::RowsRead")
    flow.derived_column("Evaluate Customer Rules", [
        ("NameMissingFlag", 'ISNULL(CustomerName) || TRIM(CustomerName) == "" ? "Y" : "N"',
         str_col("NameMissingFlag", 1)),
        ("TaxIdMissingFlag",
         'ISNULL(TaxRegistrationNumber) || LEN(TRIM(TaxRegistrationNumber)) < 6 ? "Y" : "N"',
         str_col("TaxIdMissingFlag", 1)),
        # EU rows must carry an explicit consent decision and a retention period
        # no longer than the 24 month policy; other regions are advisory only.
        ("ConsentBreachFlag",
         'RegionCode == "EU" && (MarketingConsentFlag != "Y" && MarketingConsentFlag != "N") ? "Y" : '
         '(RegionCode == "EU" && RetentionMonths > 24 ? "Y" : "N")', str_col("ConsentBreachFlag", 1)),
        ("CreditImplausibleFlag", 'CreditLimitAmount < 0 || CreditLimitAmount > 100000000 ? "Y" : "N"',
         str_col("CreditImplausibleFlag", 1)),
        ("RejectReasonCode",
         'ISNULL(CustomerName) || TRIM(CustomerName) == "" ? "DQ_CUST_NAME_NULL" : '
         '(RegionCode == "EU" && (MarketingConsentFlag != "Y" && MarketingConsentFlag != "N") '
         '? "DQ_CUST_CONSENT" : (CreditLimitAmount < 0 ? "DQ_CUST_CREDIT_NEG" : "DQ_CUST_COUNTRY"))',
         str_col("RejectReasonCode", 30)),
    ])
    flow.lookup(
        "Lookup Valid Country (Full Cache)", CONN_STAGING,
        "SELECT CountryCode, RegionCode AS ReferenceRegionCode FROM ref.Country WHERE IsActive = 1;",
        ["CountryCode"], [str_col("ReferenceRegionCode", 4)], no_match="RD")
    flow.conditional_split("Route Customer Failures", [
        ("Passes All Rules",
         'NameMissingFlag == "N" && ConsentBreachFlag == "N" && CreditImplausibleFlag == "N" '
         '&& RegionCode == ReferenceRegionCode'),
        ("Completeness Failure", 'NameMissingFlag == "Y" || TaxIdMissingFlag == "Y"'),
        ("Consent Failure", 'ConsentBreachFlag == "Y"'),
    ], default_output="Region Mismatch")
    flow.row_count("Count Customers Passing", "User::RowsInserted")
    flow.oledb_destination("DQ Customer Pass Log", CONN_STAGING, "[etl].[DataQualityResult]",
                           batch_size=10000)
    flow.branch_destination("ERR Customer Completeness", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Route Customer Failures", "Completeness Failure")
    flow.branch_destination("ERR Customer Consent", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Route Customer Failures", "Consent Failure")
    flow.branch_destination("ERR Customer Region Mismatch", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Route Customer Failures", "Region Mismatch")
    flow.reject_destination("ERR Customer Unknown Country", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Lookup Valid Country (Full Cache)", "Lookup No Match Output")
    return build_quality_package(
        "DQ_Customer_Screen",
        "Screen stg.Customer for null required attributes, invalid country codes, EU consent and "
        "retention breaches and implausible credit limits. Failing rows are written to "
        "err.RejectedCustomer with a reason code and the null rate is compared against the "
        "configured tolerance before the batch is allowed to continue.",
        ORA, "stg.Customer", [flow],
        pre_tasks=[record_measure("Measure Customer Null Rate", "stg.Customer", "DQ_CUST_NULL_RATE",
                                  "CAST(100.0 * SUM(CASE WHEN CustomerName IS NULL OR LTRIM(RTRIM(CustomerName)) = '' "
                                  "THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(9,4)) "
                                  "FROM stg.Customer", "User::MeasuredValue")],
        post_tasks=[evaluate_rules("CUSTOMER", "stg.Customer")],
        gates=[(raise_gate("Warn On Customer Null Rate",
                           "Customer name null rate exceeded the configured tolerance.", "Warning"),
                "@[User::MeasuredValue] > 2"),
               (raise_gate("Fail On Customer Rule Breach",
                           "One or more blocking customer quality rules failed.", "Failure"),
                "@[User::FailedRuleCount] > 0")],
        reject_task=register_rejects("[err].[RejectedCustomer]", "stg.Customer", "r.CustomerCode",
                                     "RejectReasonCode"))


@package
def dq_supplier_screen():
    """Duplicate tax identifiers and missing payment terms on stg.Supplier."""
    cols = [
        str_col("SupplierCode", 20), str_col("SupplierName", 100), str_col("TaxIdentifier", 30),
        str_col("PaymentTermsCode", 10), str_col("CountryCode", 3), str_col("RegionCode", 4),
        bool_col("IsActive"),
    ]
    flow = DataFlow("DFT Screen Supplier", "Duplicate detection over the supplier tax identifier")
    flow.oledb_source(
        "STG Supplier", CONN_STAGING,
        "SELECT SupplierCode, SupplierName, TaxIdentifier, PaymentTermsCode, CountryCode,\n"
        "       RegionCode, IsActive\n"
        "FROM stg.Supplier WHERE BatchId = ?;",
        cols, timeout=3600)
    flow.row_count("Count Suppliers Screened", "User::RowsRead")
    flow.derived_column("Normalize Tax Identifier", [
        ("NormalizedTaxId",
         'ISNULL(TaxIdentifier) ? "NONE" : UPPER(REPLACE(REPLACE(TRIM(TaxIdentifier), "-", ""), " ", ""))',
         str_col("NormalizedTaxId", 30)),
        ("TermsMissingFlag", 'ISNULL(PaymentTermsCode) || TRIM(PaymentTermsCode) == "" ? "Y" : "N"',
         str_col("TermsMissingFlag", 1)),
        # EU suppliers must present a VAT-shaped identifier; APAC uses a national
        # business number, NA an EIN. Only the EU shape is machine-checkable here.
        ("TaxShapeInvalidFlag",
         'RegionCode == "EU" && (LEN(NormalizedTaxId) < 8 || SUBSTRING(NormalizedTaxId, 1, 2) != '
         'SUBSTRING(UPPER(TRIM(CountryCode)), 1, 2)) ? "Y" : "N"', str_col("TaxShapeInvalidFlag", 1)),
        ("RejectReasonCode",
         'ISNULL(PaymentTermsCode) || TRIM(PaymentTermsCode) == "" ? "DQ_SUPP_TERMS_NULL" : '
         '"DQ_SUPP_TAXID_DUP"', str_col("RejectReasonCode", 30)),
    ])
    flow.sort("Sort By Normalized Tax Identifier", ["NormalizedTaxId", "SupplierCode"])
    flow.aggregate("Count Suppliers Per Tax Identifier", ["NormalizedTaxId"], [
        ("SupplierCode", "SupplierCount", "Count"),
        ("SupplierCode", "FirstSupplierCode", "Minimum"),
    ])
    flow.conditional_split("Route Supplier Failures", [
        ("Unique Supplier", 'SupplierCount == 1'),
        ("Duplicate Tax Identifier", 'SupplierCount > 1 && NormalizedTaxId != "NONE"'),
    ], default_output="Missing Tax Identifier")
    flow.row_count("Count Suppliers Passing", "User::RowsInserted")
    flow.oledb_destination("DQ Supplier Pass Log", CONN_STAGING, "[etl].[DataQualityResult]",
                           batch_size=10000)
    flow.branch_destination("ERR Supplier Duplicate Tax Id", CONN_STAGING, "[err].[RejectedSupplier]",
                            "Route Supplier Failures", "Duplicate Tax Identifier")
    flow.branch_destination("ERR Supplier Missing Tax Id", CONN_STAGING, "[err].[RejectedSupplier]",
                            "Route Supplier Failures", "Missing Tax Identifier")
    return build_quality_package(
        "DQ_Supplier_Screen",
        "Screen stg.Supplier for duplicate tax identifiers and missing payment terms. Identifiers "
        "are normalised before grouping so that punctuation differences do not hide a duplicate; "
        "EU identifiers are additionally checked for the country-prefixed VAT shape.",
        ORA, "stg.Supplier", [flow],
        pre_tasks=[record_measure("Measure Supplier Duplicate Rate", "stg.Supplier",
                                  "DQ_SUPP_DUP_RATE",
                                  "CAST(100.0 * (COUNT(*) - COUNT(DISTINCT REPLACE(REPLACE("
                                  "ISNULL(TaxIdentifier, N'NONE'), '-', ''), ' ', ''))) / "
                                  "NULLIF(COUNT(*), 0) AS DECIMAL(9,4)) FROM stg.Supplier",
                                  "User::MeasuredValue")],
        post_tasks=[evaluate_rules("SUPPLIER", "stg.Supplier")],
        gates=[(raise_gate("Warn On Supplier Duplicates",
                           "Supplier tax identifier duplicate rate exceeded tolerance.", "Warning"),
                "@[User::MeasuredValue] > 1"),
               (raise_gate("Fail On Supplier Rule Breach",
                           "One or more blocking supplier quality rules failed.", "Failure"),
                "@[User::FailedRuleCount] > 0")],
        reject_task=register_rejects("[err].[RejectedSupplier]", "stg.Supplier", "r.SupplierCode",
                                     "RejectReasonCode"))


@package
def dq_order_line_screen():
    """Customer resolution and quantity/range screening of stg.OrderLine."""
    cols = [
        int_col("OrderLineId"), int_col("OrderId"), int_col("CustomerId"), int_col("StockItemId"),
        int_col("Quantity"), money_col("UnitPriceAmount"), money_col("ExtendedAmount"),
    ]
    flow = DataFlow("DFT Screen Order Line", "Range, outlier and lookup screening of order lines")
    flow.oledb_source(
        "STG Order Line", CONN_STAGING,
        "SELECT l.OrderLineId, l.OrderId, o.CustomerId, l.StockItemId, l.Quantity,\n"
        "       l.UnitPriceAmount, l.ExtendedAmount\n"
        "FROM stg.OrderLine AS l\n"
        "     INNER JOIN stg.[Order] AS o ON o.OrderId = l.OrderId\n"
        "WHERE l.BatchId = ?;",
        cols, timeout=7200)
    flow.row_count("Count Order Lines Screened", "User::RowsRead")
    flow.lookup(
        "Lookup Order Customer (No Cache)", CONN_STAGING,
        "SELECT CustomerId, CustomerCode FROM stg.Customer;",
        ["CustomerId"], [str_col("CustomerCode", 20)], no_match="RD")
    flow.derived_column("Evaluate Order Line Rules", [
        ("QuantityOutOfRangeFlag", 'Quantity <= 0 || Quantity > 10000 ? "Y" : "N"',
         str_col("QuantityOutOfRangeFlag", 1)),
        ("PriceOutlierFlag", 'UnitPriceAmount > 250000 ? "Y" : "N"', str_col("PriceOutlierFlag", 1)),
        ("ExtensionMismatchAmount",
         '(DT_NUMERIC,18,2)ABS(ExtendedAmount - (Quantity * UnitPriceAmount))',
         money_col("ExtensionMismatchAmount")),
        ("RejectReasonCode",
         'Quantity <= 0 || Quantity > 10000 ? "DQ_OL_QTY_RANGE" : '
         '(UnitPriceAmount > 250000 ? "DQ_OL_PRICE_OUTLIER" : "DQ_OL_EXTENSION")',
         str_col("RejectReasonCode", 30)),
    ])
    flow.conditional_split("Route Order Line Failures", [
        ("Passes All Rules",
         'QuantityOutOfRangeFlag == "N" && PriceOutlierFlag == "N" '
         '&& ExtensionMismatchAmount <= (DT_NUMERIC,18,2)0.01'),
        ("Invalid Quantity", 'QuantityOutOfRangeFlag == "Y"'),
        ("Price Outlier", 'PriceOutlierFlag == "Y"'),
    ], default_output="Extension Mismatch")
    flow.row_count("Count Order Lines Passing", "User::RowsInserted")
    flow.oledb_destination("DQ Order Line Pass Log", CONN_STAGING, "[etl].[DataQualityResult]",
                           batch_size=20000)
    flow.branch_destination("ERR Order Line Quantity", CONN_STAGING, "[err].[RejectedOrderLine]",
                            "Route Order Line Failures", "Invalid Quantity")
    flow.branch_destination("ERR Order Line Price Outlier", CONN_STAGING, "[err].[RejectedOrderLine]",
                            "Route Order Line Failures", "Price Outlier")
    flow.branch_destination("ERR Order Line Extension", CONN_STAGING, "[err].[RejectedOrderLine]",
                            "Route Order Line Failures", "Extension Mismatch")
    flow.reject_destination("ERR Order Line Customer Lookup", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Lookup Order Customer (No Cache)", "Lookup No Match Output")
    return build_quality_package(
        "DQ_OrderLine_Screen",
        "Screen stg.OrderLine for failed customer resolution, out-of-range quantities, unit price "
        "outliers and line extensions that disagree with quantity times price. The customer lookup "
        "runs uncached because the staged customer set is rebuilt in the same batch.",
        OLTP, "stg.OrderLine", [flow],
        pre_tasks=[record_measure("Measure Order Line Reject Rate", "stg.OrderLine",
                                  "DQ_OL_REJECT_RATE",
                                  "CAST(100.0 * SUM(CASE WHEN Quantity <= 0 OR Quantity > 10000 "
                                  "THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(9,4)) "
                                  "FROM stg.OrderLine", "User::MeasuredValue")],
        post_tasks=[evaluate_rules("ORDERLINE", "stg.OrderLine")],
        gates=[(raise_gate("Warn On Order Line Reject Rate",
                           "Order line reject rate exceeded the configured tolerance.", "Warning"),
                "@[User::MeasuredValue] > 3"),
               (raise_gate("Fail On Order Line Rule Breach",
                           "One or more blocking order line quality rules failed.", "Failure"),
                "@[User::FailedRuleCount] > 0")],
        reject_task=register_rejects("[err].[RejectedOrderLine]", "stg.OrderLine", "r.BusinessKey",
                                     "RejectReasonCode"))


@package
def dq_invoice_line_screen():
    """Tax and currency screening of stg.SaleLine."""
    cols = [
        int_col("InvoiceLineId"), int_col("InvoiceId"), int_col("StockItemId"), int_col("Quantity"),
        money_col("NetAmount"), money_col("RecomputedTaxAmount"), money_col("TaxVarianceAmount"),
        str_col("SaleCurrencyCode", 3), str_col("TaxRegimeCode", 3), str_col("RegionCode", 4),
    ]
    flow = DataFlow("DFT Screen Invoice Line", "Tax regime consistency and currency domain checks")
    flow.oledb_source(
        "STG Sale Line", CONN_STAGING,
        "SELECT l.InvoiceLineId, l.InvoiceId, l.StockItemId, l.Quantity, l.NetAmount,\n"
        "       l.RecomputedTaxAmount, l.TaxVarianceAmount, s.SaleCurrencyCode, s.TaxRegimeCode,\n"
        "       s.RegionCode\n"
        "FROM stg.SaleLine AS l\n"
        "     INNER JOIN stg.Sale AS s ON s.InvoiceId = l.InvoiceId\n"
        "WHERE l.BatchId = ?;",
        cols, timeout=7200)
    flow.row_count("Count Invoice Lines Screened", "User::RowsRead")
    flow.lookup(
        "Lookup Currency Domain (Full Cache)", CONN_STAGING,
        "SELECT CurrencyCode AS SaleCurrencyCode, CurrencyName, MinorUnitDigits\n"
        "FROM ref.Currency WHERE IsActive = 1;",
        ["SaleCurrencyCode"], [str_col("CurrencyName", 60), int_col("MinorUnitDigits")], no_match="RD")
    flow.derived_column("Evaluate Invoice Line Rules", [
        ("RegimeMismatchFlag",
         '(RegionCode == "EU" && TaxRegimeCode != "VAT") || (RegionCode == "APAC" && TaxRegimeCode != "GST") '
         '|| (RegionCode == "NA" && TaxRegimeCode != "SUT") ? "Y" : "N"', str_col("RegimeMismatchFlag", 1)),
        ("TaxMismatchFlag", 'TaxVarianceAmount > (DT_NUMERIC,18,2)0.02 ? "Y" : "N"',
         str_col("TaxMismatchFlag", 1)),
        ("MinorUnitBreachFlag",
         'MinorUnitDigits == 0 && NetAmount != (DT_NUMERIC,18,2)ROUND(NetAmount, 0) ? "Y" : "N"',
         str_col("MinorUnitBreachFlag", 1)),
        ("RejectReasonCode",
         'TaxVarianceAmount > (DT_NUMERIC,18,2)0.02 ? "DQ_IL_TAX_MISMATCH" : '
         '((RegionCode == "EU" && TaxRegimeCode != "VAT") ? "DQ_IL_REGIME" : "DQ_IL_MINOR_UNIT")',
         str_col("RejectReasonCode", 30)),
    ])
    flow.conditional_split("Route Invoice Line Failures", [
        ("Passes All Rules",
         'TaxMismatchFlag == "N" && RegimeMismatchFlag == "N" && MinorUnitBreachFlag == "N"'),
        ("Tax Mismatch", 'TaxMismatchFlag == "Y"'),
        ("Regime Mismatch", 'RegimeMismatchFlag == "Y"'),
    ], default_output="Minor Unit Breach")
    flow.row_count("Count Invoice Lines Passing", "User::RowsInserted")
    flow.oledb_destination("DQ Invoice Line Pass Log", CONN_STAGING, "[etl].[DataQualityResult]",
                           batch_size=20000)
    flow.branch_destination("ERR Invoice Line Tax", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Route Invoice Line Failures", "Tax Mismatch")
    flow.branch_destination("ERR Invoice Line Regime", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Route Invoice Line Failures", "Regime Mismatch")
    flow.branch_destination("ERR Invoice Line Minor Unit", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Route Invoice Line Failures", "Minor Unit Breach")
    flow.reject_destination("ERR Invoice Line Currency", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Lookup Currency Domain (Full Cache)", "Lookup No Match Output")
    return build_quality_package(
        "DQ_InvoiceLine_Screen",
        "Screen stg.SaleLine for tax variance against the recomputed line tax, currency codes "
        "outside the active reference set, minor-unit breaches on zero-decimal currencies and "
        "invoices whose tax regime disagrees with the billing region.",
        OLTP, "stg.SaleLine", [flow],
        pre_tasks=[record_measure("Measure Invoice Tax Variance", "stg.SaleLine",
                                  "DQ_IL_TAX_VARIANCE",
                                  "CAST(ISNULL(SUM(TaxVarianceAmount), 0) AS DECIMAL(18,4)) "
                                  "FROM stg.SaleLine", "User::MeasuredValue")],
        post_tasks=[evaluate_rules("INVOICELINE", "stg.SaleLine")],
        gates=[(raise_gate("Warn On Aggregate Tax Variance",
                           "Aggregate invoice line tax variance exceeded tolerance.", "Warning"),
                "@[User::MeasuredValue] > 100"),
               (raise_gate("Fail On Invoice Line Rule Breach",
                           "One or more blocking invoice line quality rules failed.", "Failure"),
                "@[User::FailedRuleCount] > 0")],
        reject_task=register_rejects("[err].[RejectedInvoiceLine]", "stg.SaleLine", "r.BusinessKey",
                                     "RejectReasonCode"))


@package
def dq_payment_screen():
    """Orphan and future-dated payment screening of stg.Payment."""
    cols = [
        str_col("PaymentNumber", 30), str_col("SupplierCode", 20), money_col("PaymentAmount"),
        str_col("PaymentCurrencyCode", 3), date_col("PaymentDate"), date_col("ValueDate"),
        str_col("PaymentMethodCode", 12), str_col("MatchTypeCode", 10),
    ]
    flow = DataFlow("DFT Screen Payment", "Invoice coverage and date plausibility of payments")
    flow.oledb_source(
        "STG Payment With Match", CONN_STAGING,
        "SELECT p.PaymentNumber, p.SupplierCode, p.PaymentAmount, p.PaymentCurrencyCode,\n"
        "       p.PaymentDate, p.ValueDate, p.PaymentMethodCode,\n"
        "       ISNULL(m.MatchTypeCode, N'UNMATCHED') AS MatchTypeCode\n"
        "FROM stg.Payment AS p\n"
        "     LEFT OUTER JOIN work.PaymentMatched AS m ON m.PaymentNumber = p.PaymentNumber\n"
        "WHERE p.BatchId = ?;",
        cols, timeout=3600)
    flow.row_count("Count Payments Screened", "User::RowsRead")
    flow.derived_column("Evaluate Payment Rules", [
        ("OrphanFlag", 'MatchTypeCode == "UNMATCHED" ? "Y" : "N"', str_col("OrphanFlag", 1)),
        ("FutureDatedFlag", 'PaymentDate > GETDATE() ? "Y" : "N"', str_col("FutureDatedFlag", 1)),
        ("ValueDateBeforePaymentFlag", 'ValueDate < PaymentDate ? "Y" : "N"',
         str_col("ValueDateBeforePaymentFlag", 1)),
        ("LargePaymentFlag", 'PaymentAmount > 5000000 ? "Y" : "N"', str_col("LargePaymentFlag", 1)),
        ("RejectReasonCode",
         'MatchTypeCode == "UNMATCHED" ? "DQ_PAY_ORPHAN" : (PaymentDate > GETDATE() ? '
         '"DQ_PAY_FUTURE" : "DQ_PAY_VALUE_DATE")', str_col("RejectReasonCode", 30)),
    ])
    flow.aggregate("Summarize Payments By Method", ["PaymentMethodCode", "PaymentCurrencyCode"], [
        ("PaymentAmount", "TotalPaidAmount", "Sum"),
        ("PaymentAmount", "MaxPaymentAmount", "Maximum"),
        ("PaymentNumber", "PaymentCount", "Count"),
    ])
    flow.conditional_split("Route Payment Failures", [
        ("Plausible Method Total", 'PaymentCount > 0 && MaxPaymentAmount <= 5000000'),
    ], default_output="Method Total Outlier")
    flow.row_count("Count Payment Summaries", "User::RowsInserted")
    flow.oledb_destination("DQ Payment Summary Log", CONN_STAGING, "[etl].[DataQualityResult]",
                           batch_size=10000)
    flow.branch_destination("ERR Payment Method Outlier", CONN_STAGING, "[err].[RejectedPayment]",
                            "Route Payment Failures", "Method Total Outlier")
    return build_quality_package(
        "DQ_Payment_Screen",
        "Screen stg.Payment for payments with no matching invoice in work.PaymentMatched, "
        "future-dated payments, value dates preceding the payment date and per-method totals "
        "dominated by a single implausibly large payment.",
        ORA, "stg.Payment", [flow],
        pre_tasks=[record_measure("Measure Orphan Payment Rate", "stg.Payment",
                                  "DQ_PAY_ORPHAN_RATE",
                                  "CAST(100.0 * SUM(CASE WHEN m.PaymentNumber IS NULL THEN 1 ELSE 0 END) "
                                  "/ NULLIF(COUNT(*), 0) AS DECIMAL(9,4)) FROM stg.Payment AS p "
                                  "LEFT OUTER JOIN work.PaymentMatched AS m "
                                  "ON m.PaymentNumber = p.PaymentNumber", "User::MeasuredValue")],
        post_tasks=[evaluate_rules("PAYMENT", "stg.Payment")],
        gates=[(raise_gate("Warn On Orphan Payments",
                           "Orphan payment rate exceeded the configured tolerance.", "Warning"),
                "@[User::MeasuredValue] > 5"),
               (raise_gate("Fail On Payment Rule Breach",
                           "One or more blocking payment quality rules failed.", "Failure"),
                "@[User::FailedRuleCount] > 0")],
        reject_task=register_rejects("[err].[RejectedPayment]", "stg.Payment", "r.PaymentNumber",
                                     "RejectReasonCode"))


@package
def dq_file_screen():
    """Structural screening of the partner sales file landing."""
    cols = [
        bigint_col("FileRowId"), str_col("SourceFileName", 200), int_col("FileLineNumber"),
        str_col("RawLine", 1000), int_col("DelimiterCount"), str_col("SaleDateText", 20),
        str_col("AmountText", 20), str_col("PartnerCode", 10),
    ]
    flow = DataFlow("DFT Screen Partner File", "Delimiter, encoding and parse screening of file rows")
    flow.oledb_source(
        "RAW File Partner Sales", CONN_STAGING,
        "SELECT FileRowId, SourceFileName, FileLineNumber, RawLine,\n"
        "       LEN(RawLine) - LEN(REPLACE(RawLine, N'|', N'')) AS DelimiterCount,\n"
        "       SaleDateText, AmountText, PartnerCode\n"
        "FROM raw.FilePartnerSales WHERE BatchId = ?;",
        cols, timeout=3600)
    flow.row_count("Count File Rows Screened", "User::RowsRead")
    flow.derived_column("Evaluate File Row Rules", [
        # The agreed interface is nine pipe-delimited fields; partners have been
        # known to send eight (dropping currency) or ten (an unescaped pipe).
        ("DelimiterBreachFlag", 'DelimiterCount != 8 ? "Y" : "N"', str_col("DelimiterBreachFlag", 1)),
        ("UnparsableDateFlag",
         'ISNULL(SaleDateText) || LEN(TRIM(SaleDateText)) < 8 || '
         '(FINDSTRING(SaleDateText, "/", 1) == 0 && FINDSTRING(SaleDateText, "-", 1) == 0) ? "Y" : "N"',
         str_col("UnparsableDateFlag", 1)),
        ("UnparsableAmountFlag",
         'ISNULL(AmountText) || LEN(REPLACE(REPLACE(REPLACE(TRIM(AmountText), ",", ""), "$", ""), ".", "")) == 0 '
         '? "Y" : "N"', str_col("UnparsableAmountFlag", 1)),
        ("HighBitFlag", 'FINDSTRING(RawLine, "\\uFFFD", 1) > 0 ? "Y" : "N"', str_col("HighBitFlag", 1)),
        ("RejectReasonCode",
         'DelimiterCount != 8 ? "DQ_FILE_DELIMITER" : (ISNULL(SaleDateText) ? "DQ_FILE_DATE" : '
         '"DQ_FILE_AMOUNT")', str_col("RejectReasonCode", 30)),
    ])
    flow.conditional_split("Route File Failures", [
        ("Well Formed Row",
         'DelimiterBreachFlag == "N" && UnparsableDateFlag == "N" && UnparsableAmountFlag == "N" '
         '&& HighBitFlag == "N"'),
        ("Malformed Delimiters", 'DelimiterBreachFlag == "Y"'),
        ("Unparsable Date", 'UnparsableDateFlag == "Y"'),
    ], default_output="Unparsable Amount")
    flow.row_count("Count File Rows Passing", "User::RowsInserted")
    flow.oledb_destination("DQ File Pass Log", CONN_STAGING, "[etl].[DataQualityResult]", batch_size=20000)
    flow.branch_destination("ERR File Delimiters", CONN_STAGING, "[err].[RejectedFileRow]",
                            "Route File Failures", "Malformed Delimiters")
    flow.branch_destination("ERR File Date", CONN_STAGING, "[err].[RejectedFileRow]",
                            "Route File Failures", "Unparsable Date")
    flow.branch_destination("ERR File Amount", CONN_STAGING, "[err].[RejectedFileRow]",
                            "Route File Failures", "Unparsable Amount")
    return build_quality_package(
        "DQ_File_Screen",
        "Screen raw.FilePartnerSales before it is trusted: rows must carry exactly eight pipe "
        "delimiters, a recognisable date and a parsable amount, and must not contain replacement "
        "characters left behind by a code-page mismatch.",
        FILE, "raw.FilePartnerSales", [flow],
        pre_tasks=[record_measure("Measure File Malformed Rate", "raw.FilePartnerSales",
                                  "DQ_FILE_MALFORMED_RATE",
                                  "CAST(100.0 * SUM(CASE WHEN LEN(RawLine) - LEN(REPLACE(RawLine, N'|', N'')) "
                                  "<> 8 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(9,4)) "
                                  "FROM raw.FilePartnerSales", "User::MeasuredValue")],
        post_tasks=[evaluate_rules("FILEROW", "raw.FilePartnerSales")],
        gates=[(raise_gate("Warn On Malformed File Rows",
                           "Malformed partner file row rate exceeded tolerance.", "Warning"),
                "@[User::MeasuredValue] > 1"),
               (raise_gate("Fail On File Structure Breach",
                           "The partner file failed a blocking structural rule.", "Failure"),
                "@[User::MeasuredValue] > 25 || @[User::FailedRuleCount] > 0")],
        reject_task=register_rejects("[err].[RejectedFileRow]", "raw.FilePartnerSales",
                                     "r.BusinessKey", "RejectReasonCode", stage="Extract"))


@package
def dq_referential_screen():
    """Referential integrity of stg.OrderLine and stg.SaleLine against ref.* and stg.*."""
    order_cols = [
        int_col("OrderLineId"), int_col("StockItemId"), str_col("PackageTypeCode", 10),
        str_col("SourceObjectName", 30),
    ]
    order = DataFlow("DFT Order Line Referential", "Orphan foreign keys on the order line feed")
    order.oledb_source(
        "STG Order Line Keys", CONN_STAGING,
        "SELECT OrderLineId, StockItemId, PackageTypeCode, N'stg.OrderLine' AS SourceObjectName\n"
        "FROM stg.OrderLine WHERE BatchId = ?;",
        order_cols, timeout=7200)
    order.row_count("Count Order Keys Checked", "User::RowsRead")
    order.lookup(
        "Lookup Stock Item Key (Full Cache)", CONN_STAGING,
        "SELECT StockItemId, StockItemName FROM stg.StockItem;",
        ["StockItemId"], [str_col("StockItemName", 100)], no_match="RD")
    order.lookup(
        "Lookup Package Type (Full Cache)", CONN_STAGING,
        "SELECT PackageTypeCode, PackageTypeName FROM ref.PackageType;",
        ["PackageTypeCode"], [str_col("PackageTypeName", 60)], no_match="RD")
    order.derived_column("Tag Order Referential Result", [
        ("RejectReasonCode", '"DQ_REF_ORDERLINE"', str_col("RejectReasonCode", 30)),
    ])
    order.row_count("Count Order Keys Resolved", "User::RowsInserted")
    order.oledb_destination("DQ Order Referential Log", CONN_STAGING, "[etl].[DataQualityResult]",
                            batch_size=20000)
    order.reject_destination("ERR Order Orphan Stock Item", CONN_STAGING,
                             "[err].[RejectedLookupFailure]",
                             "Lookup Stock Item Key (Full Cache)", "Lookup No Match Output")
    order.reject_destination("ERR Order Orphan Package Type", CONN_STAGING,
                             "[err].[RejectedLookupFailure]",
                             "Lookup Package Type (Full Cache)", "Lookup No Match Output")

    sale_cols = [
        int_col("InvoiceLineId"), int_col("StockItemId"), str_col("SaleCurrencyCode", 3),
        str_col("SalesTerritoryCode", 12), str_col("SourceObjectName", 30),
    ]
    sale = DataFlow("DFT Sale Line Referential", "Orphan foreign keys on the invoice line feed")
    sale.oledb_source(
        "STG Sale Line Keys", CONN_STAGING,
        "SELECT l.InvoiceLineId, l.StockItemId, s.SaleCurrencyCode, s.SalesTerritoryCode,\n"
        "       N'stg.SaleLine' AS SourceObjectName\n"
        "FROM stg.SaleLine AS l INNER JOIN stg.Sale AS s ON s.InvoiceId = l.InvoiceId\n"
        "WHERE l.BatchId = ?;",
        sale_cols, timeout=7200)
    sale.lookup(
        "Lookup Sale Currency (Full Cache)", CONN_STAGING,
        "SELECT CurrencyCode AS SaleCurrencyCode, CurrencyName FROM ref.Currency;",
        ["SaleCurrencyCode"], [str_col("CurrencyName", 60)], no_match="RD")
    sale.lookup(
        "Lookup Sales Territory (Partial Cache)", CONN_STAGING,
        "SELECT SalesTerritoryCode, SalesTerritoryName FROM stg.SalesTerritory;",
        ["SalesTerritoryCode"], [str_col("SalesTerritoryName", 60)], no_match="RD")
    sale.derived_column("Tag Sale Referential Result", [
        ("RejectReasonCode", '"DQ_REF_SALELINE"', str_col("RejectReasonCode", 30)),
    ])
    sale.row_count("Count Sale Keys Resolved", "User::RowsUpdated")
    sale.oledb_destination("DQ Sale Referential Log", CONN_STAGING, "[etl].[DataQualityResult]",
                           batch_size=20000)
    sale.reject_destination("ERR Sale Orphan Currency", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Sale Currency (Full Cache)", "Lookup No Match Output")
    sale.reject_destination("ERR Sale Orphan Territory", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Sales Territory (Partial Cache)", "Lookup No Match Output")
    return build_quality_package(
        "DQ_Referential_Screen",
        "Assert referential integrity for the transactional staging feeds: every order line must "
        "resolve to a staged stock item and a reference package type, and every invoice line must "
        "resolve to an active currency and a known sales territory. Orphans are collected in "
        "err.RejectedLookupFailure rather than failing the data flow.",
        OLTP, "stg.OrderLine", [order, sale],
        pre_tasks=[record_measure("Measure Orphan Key Count", "stg.OrderLine", "DQ_REF_ORPHANS",
                                  "CAST(COUNT(*) AS DECIMAL(18,4)) FROM stg.OrderLine AS l "
                                  "LEFT OUTER JOIN stg.StockItem AS s ON s.StockItemId = l.StockItemId "
                                  "WHERE s.StockItemId IS NULL", "User::MeasuredValue")],
        post_tasks=[evaluate_rules("REFERENTIAL", "stg.OrderLine")],
        gates=[(raise_gate("Warn On Orphan Keys",
                           "Orphan foreign keys were detected in the staged transaction feeds.",
                           "Warning"), "@[User::MeasuredValue] > 0"),
               (raise_gate("Fail On Excessive Orphans",
                           "Orphan foreign key count exceeded the blocking threshold.", "Failure"),
                "@[User::MeasuredValue] > 1000")],
        reject_task=register_rejects("[err].[RejectedLookupFailure]", "stg.OrderLine",
                                     "r.BusinessKey", "RejectReasonCode", stage="Referential"))


@package
def dq_rule_engine():
    """Configurable rule evaluation across every staged table."""
    cols = [
        int_col("DataQualityRuleId"), str_col("RuleCode", 30), str_col("RuleGroupCode", 20),
        str_col("ObjectName", 200), str_col("RuleExpression", 1000), str_col("SeverityCode", 10),
        dec_col("ThresholdValue", 18, 4), bool_col("IsActive"),
    ]
    flow = DataFlow("DFT Load Rule Set", "Read and classify the configured rule set")
    flow.oledb_source(
        "ETL DataQualityRule", CONN_STAGING,
        "SELECT DataQualityRuleId, RuleCode, RuleGroupCode, ObjectName, RuleExpression,\n"
        "       SeverityCode, ThresholdValue, IsActive\n"
        "FROM etl.DataQualityRule WHERE IsActive = 1;",
        cols, timeout=1800)
    flow.row_count("Count Rules Loaded", "User::RowsRead")
    flow.derived_column("Classify Rule Severity", [
        ("SeverityCode", 'UPPER(TRIM(ISNULL(SeverityCode) ? "WARN" : SeverityCode))',
         str_col("SeverityCode", 10)),
        ("BlockingFlag", 'UPPER(TRIM(ISNULL(SeverityCode) ? "WARN" : SeverityCode)) == "FAIL" ? "Y" : "N"',
         str_col("BlockingFlag", 1)),
        ("EvaluatedAtUtc", 'GETUTCDATE()', date_col("EvaluatedAtUtc")),
    ])
    flow.aggregate("Summarize Rules By Group", ["RuleGroupCode", "SeverityCode"], [
        ("DataQualityRuleId", "RuleCount", "Count"),
        ("ThresholdValue", "AverageThreshold", "Average"),
    ])
    flow.conditional_split("Route Rule Definitions", [
        ("Usable Rule", 'RuleCount > 0'),
    ], default_output="Empty Rule Group")
    flow.row_count("Count Rule Groups", "User::RowsInserted")
    flow.oledb_destination("ETL DataQualityResult Rule Inventory", CONN_STAGING,
                           "[etl].[DataQualityResult]", batch_size=5000)
    flow.branch_destination("ERR Empty Rule Group", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Rule Definitions", "Empty Rule Group")
    # The engine is deliberately row-by-row: each rule is a fragment of dynamic
    # SQL that the shop's stewards edit in the etl.DataQualityRule table.
    evaluate_each = ExecuteSql(
        "Evaluate Each Configured Rule",
        CONN_STAGING,
        "DECLARE @RuleId INT, @RuleCode NVARCHAR(30), @ObjectName NVARCHAR(200), "
        "@RuleExpression NVARCHAR(1000), @Sql NVARCHAR(MAX), @Measure DECIMAL(18,4);\n"
        "DECLARE rule_cur CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT DataQualityRuleId, RuleCode, ObjectName, RuleExpression\n"
        "    FROM etl.DataQualityRule WHERE IsActive = 1 ORDER BY RuleGroupCode, RuleCode;\n"
        "OPEN rule_cur;\n"
        "FETCH NEXT FROM rule_cur INTO @RuleId, @RuleCode, @ObjectName, @RuleExpression;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    SET @Sql = N'SELECT @Out = CAST(COUNT_BIG(*) AS DECIMAL(18,4)) FROM ' + @ObjectName +\n"
        "               N' WHERE ' + @RuleExpression + N';';\n"
        "    BEGIN TRY\n"
        "        EXEC sp_executesql @Sql, N'@Out DECIMAL(18,4) OUTPUT', @Out = @Measure OUTPUT;\n"
        "    END TRY\n"
        "    BEGIN CATCH\n"
        "        SET @Measure = -1;\n"
        "    END CATCH\n"
        "    INSERT INTO etl.DataQualityResult\n"
        "        (BatchId, PackageExecutionId, ObjectName, RuleCode, MeasuredValue, EvaluatedAtUtc)\n"
        "    VALUES (?, ?, @ObjectName, @RuleCode, @Measure, SYSUTCDATETIME());\n"
        "    FETCH NEXT FROM rule_cur INTO @RuleId, @RuleCode, @ObjectName, @RuleExpression;\n"
        "END\n"
        "CLOSE rule_cur;\n"
        "DEALLOCATE rule_cur;",
        parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                            ("User::PackageExecutionId", 1, "LONG")],
    )
    count_failures = ExecuteSql(
        "Count Failing Rules",
        CONN_STAGING,
        "SELECT @FailedRuleCount = COUNT(*)\n"
        "FROM etl.DataQualityResult AS r\n"
        "     INNER JOIN etl.DataQualityRule AS d ON d.RuleCode = r.RuleCode\n"
        "WHERE r.BatchId = ? AND r.MeasuredValue > d.ThresholdValue;\n"
        "SELECT @FailedRuleCount AS FailedRuleCount;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::FailedRuleCount")],
    )
    return build_quality_package(
        "DQ_Rule_Engine",
        "Evaluate every active rule in etl.DataQualityRule against its target object and record one "
        "etl.DataQualityResult row per rule. Rules are stored as SQL predicate fragments and "
        "executed through sp_executesql one at a time, so a broken rule records a -1 measure "
        "instead of aborting the sweep.",
        OLTP, "etl.DataQualityResult", [flow],
        post_tasks=[evaluate_each, count_failures],
        gates=[(raise_gate("Warn On Failing Rules",
                           "One or more configured data quality rules exceeded their threshold.",
                           "Warning"), "@[User::FailedRuleCount] > 0"),
               (raise_gate("Fail On Blocking Rule Set",
                           "Blocking data quality rules failed for this batch.", "Failure"),
                "@[User::FailedRuleCount] > 10")],
        reject_task=register_rejects("[err].[RejectedConstraintViolation]", "etl.DataQualityResult",
                                     "r.BusinessKey", "RejectReasonCode", stage="RuleEngine"))


@package
def dq_reject_reprocess():
    """Replay err.RejectedLookupFailure rows once the late dimension rows arrive."""
    cols = [
        bigint_col("RejectedRowId"), str_col("ObjectName", 200), str_col("BusinessKey", 200),
        str_col("RejectReasonCode", 30), int_col("RetryCount"), date_col("FirstRejectedAtUtc"),
        str_col("PayloadJson", 4000),
    ]
    flow = DataFlow("DFT Reprocess Rejects", "Re-resolve previously orphaned keys against ref.* and stg.*")
    flow.oledb_source(
        "ERR Rejected Lookup Failure", CONN_STAGING,
        "SELECT RejectedRowId, ObjectName, BusinessKey, RejectReasonCode, RetryCount,\n"
        "       FirstRejectedAtUtc, PayloadJson\n"
        "FROM err.RejectedLookupFailure\n"
        "WHERE Reprocessed = 0 AND RetryCount < 5;",
        cols, timeout=3600)
    flow.row_count("Count Rejects Considered", "User::RowsRead")
    flow.derived_column("Prepare Retry", [
        ("RetryCount", 'ISNULL(RetryCount) ? 1 : RetryCount + 1', int_col("RetryCount")),
        ("AgeDays", 'DATEDIFF("day", FirstRejectedAtUtc, GETUTCDATE())', int_col("AgeDays")),
        ("StockItemId", '(DT_I4)TOKEN(BusinessKey, "|", 2)', int_col("StockItemId")),
        ("AbandonFlag", 'DATEDIFF("day", FirstRejectedAtUtc, GETUTCDATE()) > 30 ? "Y" : "N"',
         str_col("AbandonFlag", 1)),
    ])
    flow.lookup(
        "Re-Lookup Stock Item (No Cache)", CONN_STAGING,
        "SELECT StockItemId, StockItemName FROM stg.StockItem;",
        ["StockItemId"], [str_col("StockItemName", 100)], no_match="RD")
    flow.conditional_split("Route Reprocess Outcome", [
        ("Resolved Now", 'AbandonFlag == "N"'),
    ], default_output="Aged Out")
    flow.row_count("Count Rejects Resolved", "User::RowsInserted")
    flow.oledb_destination("STG OrderLine Replay", CONN_STAGING, "[stg].[OrderLine]", batch_size=10000)
    flow.branch_destination("ERR Reject Aged Out", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Reprocess Outcome", "Aged Out")
    flow.reject_destination("ERR Reject Still Unresolved", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Re-Lookup Stock Item (No Cache)", "Lookup No Match Output")
    close_out = ExecuteSql(
        "Close Reprocessed Rejects",
        CONN_STAGING,
        "UPDATE err.RejectedLookupFailure\n"
        "SET Reprocessed = 1, ReprocessedAtUtc = SYSUTCDATETIME(), ReprocessedByExecutionId = ?\n"
        "WHERE Reprocessed = 0 AND RetryCount < 5\n"
        "  AND EXISTS (SELECT 1 FROM stg.StockItem AS s\n"
        "              WHERE CAST(s.StockItemId AS NVARCHAR(20)) = "
        "PARSENAME(REPLACE(err.RejectedLookupFailure.BusinessKey, N'|', N'.'), 1));",
        parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
    )
    return build_quality_package(
        "DQ_Reject_Reprocess",
        "Replay rejected lookup failures after late-arriving dimension rows land. Each reject gets "
        "at most five retries over thirty days; rows that resolve are replayed into stg.OrderLine "
        "and closed out, rows that age out stay in err.RejectedLookupFailure for stewardship.",
        OLTP, "err.RejectedLookupFailure", [flow],
        post_tasks=[close_out, evaluate_rules("REPROCESS", "err.RejectedLookupFailure")],
        gates=[(raise_gate("Warn On Persistent Rejects",
                           "Rejected rows remain unresolved after the retry window.", "Warning"),
                "@[User::FailedRuleCount] > 0")],
        reject_task=register_rejects("[err].[RejectedLookupFailure]", "err.RejectedLookupFailure",
                                     "r.BusinessKey", "RejectReasonCode", stage="Reprocess"))


@package
def dq_threshold_gate():
    """Control-total reconciliation and the batch-level reject-rate gate."""
    cols = [
        str_col("ObjectName", 200), bigint_col("SourceRowCount"), bigint_col("TargetRowCount"),
        bigint_col("RejectRowCount"), bigint_col("VarianceRowCount"),
    ]
    flow = DataFlow("DFT Reconcile Control Totals",
                    "Compare source, target and reject counts per staged object")
    flow.oledb_source(
        "ETL RowCountAudit", CONN_STAGING,
        "SELECT a.ObjectName, SUM(a.SourceRowCount) AS SourceRowCount,\n"
        "       SUM(a.TargetRowCount) AS TargetRowCount, SUM(a.RejectRowCount) AS RejectRowCount,\n"
        "       SUM(a.VarianceRowCount) AS VarianceRowCount\n"
        "FROM etl.RowCountAudit AS a\n"
        "     INNER JOIN etl.PackageExecution AS e ON e.PackageExecutionId = a.PackageExecutionId\n"
        "WHERE e.BatchId = ?\n"
        "GROUP BY a.ObjectName;",
        cols, timeout=1800)
    flow.row_count("Count Objects Reconciled", "User::RowsRead")
    flow.derived_column("Compute Variance Percent", [
        ("RejectPercent",
         'SourceRowCount == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(100.0 * RejectRowCount / SourceRowCount)',
         dec_col("RejectPercent", 9, 4)),
        ("VariancePercent",
         'SourceRowCount == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(100.0 * VarianceRowCount / SourceRowCount)',
         dec_col("VariancePercent", 9, 4)),
        ("ReconciliationStatusCode",
         'SourceRowCount == 0 ? "EMPTY" : (ABS(VarianceRowCount) == 0 ? "BALANCED" : '
         '(ABS(100.0 * VarianceRowCount / SourceRowCount) <= 0.5 ? "WITHIN_TOL" : "BREACH"))',
         str_col("ReconciliationStatusCode", 12)),
        ("RejectReasonCode", '"DQ_RECON_BREACH"', str_col("RejectReasonCode", 30)),
    ])
    flow.conditional_split("Route Reconciliation Outcome", [
        ("Balanced", 'ReconciliationStatusCode == "BALANCED" || ReconciliationStatusCode == "WITHIN_TOL"'),
        ("Empty Object", 'ReconciliationStatusCode == "EMPTY"'),
    ], default_output="Reconciliation Breach")
    flow.row_count("Count Objects Balanced", "User::RowsInserted")
    flow.oledb_destination("ETL ReconciliationResult", CONN_STAGING, "[etl].[ReconciliationResult]",
                           batch_size=5000)
    flow.branch_destination("ETL ReconciliationResult Empty", CONN_STAGING,
                            "[etl].[ReconciliationResult]",
                            "Route Reconciliation Outcome", "Empty Object")
    flow.branch_destination("ERR Reconciliation Breach", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Route Reconciliation Outcome", "Reconciliation Breach")
    measure_reject_rate = record_measure(
        "Measure Batch Reject Rate", "etl.RowCountAudit", "DQ_BATCH_REJECT_RATE",
        "CAST(100.0 * SUM(ISNULL(a.RejectRowCount, 0)) / NULLIF(SUM(ISNULL(a.SourceRowCount, 0)), 0) "
        "AS DECIMAL(9,4)) FROM etl.RowCountAudit AS a INNER JOIN etl.PackageExecution AS e "
        "ON e.PackageExecutionId = a.PackageExecutionId WHERE e.BatchId = ?",
        "User::MeasuredValue")
    scorecard = ExecuteSql(
        "Publish Quality Scorecard",
        CONN_STAGING,
        "SELECT @Score = CAST(100.0 - ISNULL(AVG(CASE WHEN r.MeasuredValue > d.ThresholdValue "
        "THEN 100.0 ELSE 0.0 END), 0.0) AS DECIMAL(9,4))\n"
        "FROM etl.DataQualityResult AS r\n"
        "     INNER JOIN etl.DataQualityRule AS d ON d.RuleCode = r.RuleCode\n"
        "WHERE r.BatchId = ?;\n"
        "INSERT INTO etl.DataQualityResult\n"
        "    (BatchId, PackageExecutionId, ObjectName, RuleCode, MeasuredValue, EvaluatedAtUtc)\n"
        "VALUES (?, ?, N'BATCH', N'DQ_SCORECARD', @Score, SYSUTCDATETIME());\n"
        "SELECT @Score AS QualityScore;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                            ("$Package::BatchId", 1, "LONG"),
                            ("User::PackageExecutionId", 2, "LONG")],
        result_bindings=[("0", "User::QualityScore")],
    )
    return build_quality_package(
        "DQ_Threshold_Gate",
        "Reconcile control totals for the batch through etl.usp_AssertRowCountReconciliation, "
        "publish a batch quality scorecard into etl.DataQualityResult and fail the batch when the "
        "reject rate exceeds tolerance or the scorecard drops below the acceptable score.",
        OLTP, "etl.ReconciliationResult", [flow],
        pre_tasks=[measure_reject_rate],
        post_tasks=[assert_reconciliation(), scorecard],
        gates=[(raise_gate("Warn On Reject Rate",
                           "Batch reject rate exceeded the configured warning tolerance.", "Warning"),
                "@[User::MeasuredValue] > 2"),
               (raise_gate("Fail On Reject Rate",
                           "Batch reject rate exceeded the blocking tolerance.", "Failure"),
                "@[User::MeasuredValue] > 5"),
               (raise_gate("Fail On Reconciliation Breach",
                           "Control total reconciliation failed for one or more objects.", "Failure"),
                "@[User::FailedObjectCount] > 0"),
               (raise_gate("Fail On Low Quality Score",
                           "The batch quality scorecard fell below the acceptable score.", "Failure"),
                "@[User::QualityScore] < 90")],
        reject_task=register_rejects("[err].[RejectedConstraintViolation]", "etl.RowCountAudit",
                                     "r.BusinessKey", "RejectReasonCode", stage="Reconciliation"))


# ---------------------------------------------------------------------------
# generation
# ---------------------------------------------------------------------------


def build_all():
    return [builder() for builder in BUILDERS]


def main():
    packages = build_all()
    names = []
    for pkg in packages:
        path = os.path.join(HERE, pkg.name + ".dtsx")
        pkg.write(path)
        names.append(pkg.name)
        print("wrote %s" % os.path.relpath(path, REPO_ROOT))
    for path in project.write_project(HERE, PROJECT_NAME, sorted(names), PROJECT_CONNECTIONS):
        print("wrote %s" % os.path.relpath(path, REPO_ROOT))
    print("%d packages" % len(names))


if __name__ == "__main__":
    main()
