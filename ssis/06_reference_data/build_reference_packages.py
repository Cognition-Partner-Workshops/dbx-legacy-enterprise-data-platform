"""Spec module for the WWI_ReferenceData SSIS project (ssis/06_reference_data).

Emits the fourteen reference-data packages declared in config/estate-catalog.yaml
for folder 06_reference_data. The packages no longer conform reference data
inside the data flow: the conforming is done by the ref.usp_Load* procedures in
sqlserver/reference, which the packages call, and the data flows are left with
the job they are actually good at - reading the conformed ref.* layer and
publishing it into the warehouse dimension tables named in the catalog.

The order inside a package is always the same:

    log start -> ref.usp_Load<Object> (one or more) -> publish Dimension.*
              -> ref.usp_ReportUnmappedCodes -> log row counts -> log success

Run from the repository root:

    python3 ssis/06_reference_data/build_reference_packages.py

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
    bigint_col,
    date_col,
    int_col,
    money_col,
    str_col,
)
from patterns import (  # noqa: E402
    CONN_DW,
    CONN_STAGING,
    exec_proc,
    log_package_start,
    log_package_success,
    log_row_count,
    new_package,
)
import project  # noqa: E402

PROJECT_NAME = "WWI_ReferenceData"
PROJECT_CONNECTIONS = ["WWI_Staging_DB", "WWI_DW_Destination_DB", "WWI_Source_DB"]

ORA = "ORA_ERP"
OLTP = "WWI_OLTP"

BUILDERS = []


def package(func):
    BUILDERS.append(func)
    return func


def dec_col(name, precision=18, scale=6):
    return Column(name, "numeric", precision=precision, scale=scale)


def bool_col(name):
    return Column(name, "bool")


REFERENCE_VARIABLES = [
    ("UnmappedCodeCount", 0, "int"),
    ("VersionNumber", 0, "int"),
    ("EffectiveFromDate", "1900-01-01", "string"),
]

#  Every reference load procedure takes the same first two arguments, so the
#  bindings are the same everywhere and are written out once.
BATCH_BINDINGS = [("$Package::BatchId", 0, "LONG"),
                  ("User::PackageExecutionId", 1, "LONG")]


def load_reference(task_name, procedure, extra_arguments=""):
    """Call one of the sqlserver/reference load procedures."""
    return exec_proc(
        task_name,
        "EXEC %s @BatchId = ?, @PackageExecutionId = ?%s;" % (procedure, extra_arguments),
        parameter_bindings=list(BATCH_BINDINGS),
    )


def load_crosswalk(code_domain):
    """Refresh ref.CodeCrosswalk for a single code domain."""
    return load_reference(
        "Load Code Crosswalk - %s" % code_domain,
        "ref.usp_LoadCodeCrosswalk",
        ", @CodeDomainCode = N'%s'" % code_domain,
    )


def report_unmapped(code_domain):
    """Report the codes seen in raw.* that the crosswalk does not cover.

    The procedure returns one row per unmapped code; the package keeps the
    count in a variable so the weekly master package can raise it, and the
    stewards read the detail out of ref.vw_UnmappedSourceCode.
    """
    return ExecuteSql(
        "Report Unmapped Codes - %s" % code_domain,
        CONN_STAGING,
        "DECLARE @Unmapped TABLE (CodeDomainCode NVARCHAR(30), SourceSystemCode NVARCHAR(20),\n"
        "                         SourceObjectName NVARCHAR(200), SourceColumnName NVARCHAR(100),\n"
        "                         SourceCodeValue NVARCHAR(50), OccurrenceCount BIGINT,\n"
        "                         FirstObservedAtUtc DATETIME2(3));\n"
        "INSERT INTO @Unmapped\n"
        "EXEC ref.usp_ReportUnmappedCodes @BatchId = ?, @PackageExecutionId = ?, "
        "@CodeDomainCode = N'%s';\n"
        "SELECT COUNT_BIG(*) AS UnmappedCodeCount FROM @Unmapped;" % code_domain,
        result_type="ResultSetType_SingleRow",
        parameter_bindings=list(BATCH_BINDINGS),
        result_bindings=[("0", "User::UnmappedCodeCount")],
    )


def hash_expression(columns):
    """The house change-hash expression: pipe-delimited, upper-cased, trimmed."""
    return " + \"|\" + ".join('UPPER(TRIM((DT_WSTR,60)%s))' % col for col in columns)


def conformed_code_query(code_domain, code_alias, name_alias):
    """The standard read of one code domain out of the conformed crosswalk.

    A conformed code can be reached from several source codes; the description
    shown downstream is the one flagged as the default for that conformed value,
    falling back to whichever mapping sorts first.
    """
    return (
        "SELECT x.ConformedCodeValue AS %s,\n"
        "       MAX(CASE WHEN x.IsDefaultForConformed = 1 THEN x.SourceCodeDescription END) AS %s,\n"
        "       ISNULL(x.RegionCode, N'ALL') AS RegionCode,\n"
        "       MIN(x.EffectiveFromDate) AS EffectiveFromDate,\n"
        "       COUNT_BIG(*) AS SourceCodeCount\n"
        "FROM ref.CodeCrosswalk AS x\n"
        "WHERE x.CodeDomainCode = N'%s'\n"
        "  AND x.EffectiveToDate IS NULL\n"
        "GROUP BY x.ConformedCodeValue, ISNULL(x.RegionCode, N'ALL');" % (
            code_alias, name_alias, code_domain)
    )


def build_reference_package(name, description, source_system, object_name, flows,
                            pre_tasks=(), post_tasks=(), truncate_tables=(),
                            extra_variables=(),
                            connections=(CONN_STAGING, CONN_DW)):
    """Assemble a reference package around the house control flow."""
    pkg = new_package(name, description, source_system=source_system, connections=connections,
                      extra_variables=REFERENCE_VARIABLES + list(extra_variables))
    ordered = [pkg.add(log_package_start(pkg))]
    for table in truncate_tables:
        ordered.append(pkg.add(truncate_dimension(table)))
    for task in pre_tasks:
        ordered.append(pkg.add(task))
    for flow in flows:
        ordered.append(pkg.add(DataFlowTask(flow)))
    for task in post_tasks:
        ordered.append(pkg.add(task))
    ordered.append(pkg.add(log_row_count(object_name)))
    ordered.append(pkg.add(log_package_success()))
    pkg.chain(*ordered)
    return pkg


def truncate_dimension(table):
    """Full-refresh dimensions are emptied on the warehouse connection."""
    return ExecuteSql("Truncate %s" % table, CONN_DW, "TRUNCATE TABLE %s;" % table)


# ---------------------------------------------------------------------------
# currency, rates and financial code sets
# ---------------------------------------------------------------------------


@package
def ref_load_currency():
    """ref.usp_LoadCurrency + ref.usp_LoadFxRateDaily -> Dimension.Currency."""
    dim_cols = [
        str_col("CurrencyCode", 3), str_col("CurrencyName", 60), str_col("CurrencySymbol", 5),
        int_col("MinorUnitDigits"), str_col("RoundingRuleCode", 20), bool_col("IsReportingCurrency"),
        bool_col("IsEuroLegacy"), date_col("RetiredDate"), dec_col("LatestRateToUsd"),
        date_col("LatestRateDate"),
    ]
    dim = DataFlow("DFT Publish Currency Dimension",
                   "Publish the conformed currency set with its latest USD rate")
    dim.oledb_source(
        "REF Currency Conformed", CONN_STAGING,
        "SELECT c.CurrencyCode, c.CurrencyName, c.CurrencySymbol, c.MinorUnitDigits,\n"
        "       c.RoundingRuleCode, c.IsReportingCurrency, c.IsEuroLegacy, c.RetiredDate,\n"
        "       f.ConversionRate AS LatestRateToUsd, f.RateDate AS LatestRateDate\n"
        "FROM ref.Currency AS c\n"
        "     OUTER APPLY (SELECT TOP (1) r.ConversionRate, r.RateDate\n"
        "                  FROM ref.FxRateDaily AS r\n"
        "                  WHERE r.FromCurrencyCode = c.CurrencyCode\n"
        "                    AND r.ToCurrencyCode = N'USD'\n"
        "                    AND r.RateTypeCode = N'CORPORATE'\n"
        "                  ORDER BY r.RateDate DESC) AS f\n"
        "WHERE c.IsActive = 1;",
        dim_cols, timeout=1800)
    dim.row_count("Count Currencies Read", "User::RowsRead")
    dim.derived_column("Shape Currency Dimension", [
        ("CurrencyCode", 'UPPER(TRIM(CurrencyCode))', str_col("CurrencyCode", 3)),
        ("CurrencyName", 'TRIM(CurrencyName)', str_col("CurrencyName", 60)),
        # A euro legacy currency keeps its fixed rate for restatement and is
        # shown retired rather than dropped.
        ("CurrencyStatusCode",
         'IsEuroLegacy == TRUE ? "LEGACY" : (ISNULL(RetiredDate) ? "ACTIVE" : "RETIRED")',
         str_col("CurrencyStatusCode", 10)),
        ("RateStalenessDays",
         'ISNULL(LatestRateDate) ? 9999 : DATEDIFF("Dd", LatestRateDate, GETDATE())',
         int_col("RateStalenessDays")),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_Currency"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["CurrencyCode", "CurrencyName", "MinorUnitDigits"]),
         str_col("ChangeHash", 200)),
    ])
    dim.conditional_split("Screen Currency", [
        ("Rated Currency", 'RateStalenessDays <= 30'),
        ("Stale Rate", 'RateStalenessDays > 30 && RateStalenessDays < 9999'),
    ], default_output="Unrated Currency")
    dim.row_count("Count Currencies Published", "User::RowsInserted")
    dim.oledb_destination("DW Dimension Currency", CONN_DW, "[Dimension].[Currency]", batch_size=5000)
    dim.branch_destination("ERR Currency Rate Stale", CONN_STAGING,
                           "[err].[RejectedLookupFailure]", "Screen Currency", "Stale Rate")
    dim.branch_destination("ERR Currency Unrated", CONN_STAGING,
                           "[err].[RejectedLookupFailure]", "Screen Currency", "Unrated Currency")
    return build_reference_package(
        "REF_Load_Currency",
        "Conform the currency master and the daily FX rates into ref.Currency and ref.FxRateDaily "
        "through the reference load procedures, then publish the active currencies into "
        "Dimension.Currency with their latest corporate rate to USD. Currencies whose rate is more "
        "than thirty days old are still published but are reported to the control framework.",
        ORA, "ref.Currency", [dim],
        truncate_tables=["[Dimension].[Currency]"],
        pre_tasks=[load_reference("Load Currency Reference", "ref.usp_LoadCurrency"),
                   load_reference("Load Daily FX Rates", "ref.usp_LoadFxRateDaily")],
        post_tasks=[report_unmapped("CURRENCY")])


@package
def ref_load_payment_method():
    """ref.usp_LoadCodeCrosswalk (PAYMENT_METHOD) -> Dimension.Payment Method."""
    cols = [
        str_col("PaymentMethodCode", 12), str_col("PaymentMethodName", 60), str_col("RegionCode", 4),
        date_col("EffectiveFromDate"), bigint_col("SourceCodeCount"),
    ]
    flow = DataFlow("DFT Publish Payment Method Dimension",
                    "Publish the conformed payment method set with its regional settlement type")
    flow.oledb_source(
        "REF PaymentMethod Conformed", CONN_STAGING,
        conformed_code_query("PAYMENT_METHOD", "PaymentMethodCode", "PaymentMethodName"),
        cols, timeout=1800)
    flow.row_count("Count Methods Read", "User::RowsRead")
    flow.derived_column("Conform Payment Method", [
        ("PaymentMethodCode", 'UPPER(TRIM(PaymentMethodCode))', str_col("PaymentMethodCode", 12)),
        ("PaymentMethodName",
         'ISNULL(PaymentMethodName) ? UPPER(TRIM(PaymentMethodCode)) : TRIM(PaymentMethodName)',
         str_col("PaymentMethodName", 60)),
        # NA settles by ACH and cheque, EU by SEPA, APAC largely by local RTGS.
        ("SettlementTypeCode",
         'RegionCode == "EU" ? "SEPA" : (RegionCode == "APAC" ? "RTGS" : "ACH")',
         str_col("SettlementTypeCode", 10)),
        ("ElectronicFlag",
         'UPPER(TRIM(PaymentMethodCode)) == "CHQ" || UPPER(TRIM(PaymentMethodCode)) == "CASH" ? "N" : "Y"',
         str_col("ElectronicFlag", 1)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_PaymentMethod"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["PaymentMethodCode", "PaymentMethodName", "RegionCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Payment Method", [
        ("Mapped Method", 'PaymentMethodCode != "" && PaymentMethodCode != "UNKNOWN"'),
    ], default_output="Unmapped Method")
    flow.row_count("Count Methods Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Payment Method", CONN_DW, "[Dimension].[Payment Method]",
                           batch_size=5000)
    flow.branch_destination("ERR Payment Method Unmapped", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Screen Payment Method", "Unmapped Method")
    return build_reference_package(
        "REF_Load_PaymentMethod",
        "Refresh the PAYMENT_METHOD domain of ref.CodeCrosswalk so both source systems' payment "
        "codes reach the conformed set, then publish the conformed methods into "
        "Dimension.Payment Method, defaulting the settlement type by region (SEPA in the EU, RTGS "
        "in APAC, ACH in NA) and reporting source methods that still have no mapping.",
        ORA, "ref.CodeCrosswalk", [flow],
        truncate_tables=["[Dimension].[Payment Method]"],
        pre_tasks=[load_crosswalk("PAYMENT_METHOD")],
        post_tasks=[report_unmapped("PAYMENT_METHOD")])


@package
def ref_load_transaction_type():
    """ref.usp_LoadUnitOfMeasure + crosswalk (TRANSACTION_TYPE) -> Dimension.Transaction Type."""
    cols = [
        str_col("TransactionTypeCode", 12), str_col("TransactionTypeName", 60),
        str_col("RegionCode", 4), date_col("EffectiveFromDate"), bigint_col("SourceCodeCount"),
    ]
    flow = DataFlow("DFT Publish Transaction Type Dimension",
                    "Classify the conformed transaction types by their ledger effect")
    flow.oledb_source(
        "REF TransactionType Conformed", CONN_STAGING,
        conformed_code_query("TRANSACTION_TYPE", "TransactionTypeCode", "TransactionTypeName"),
        cols, timeout=1800)
    flow.row_count("Count Transaction Types Read", "User::RowsRead")
    flow.derived_column("Conform Transaction Type", [
        ("TransactionTypeCode", 'UPPER(TRIM(TransactionTypeCode))', str_col("TransactionTypeCode", 12)),
        ("TransactionTypeName",
         'ISNULL(TransactionTypeName) ? UPPER(TRIM(TransactionTypeCode)) : TRIM(TransactionTypeName)',
         str_col("TransactionTypeName", 60)),
        ("MovementDirectionCode",
         'UPPER(TRIM(TransactionTypeCode)) == "PURCH" || UPPER(TRIM(TransactionTypeCode)) == "RET" '
         '? "IN" : "OUT"', str_col("MovementDirectionCode", 3)),
        ("MovementSign",
         'UPPER(TRIM(TransactionTypeCode)) == "PURCH" || UPPER(TRIM(TransactionTypeCode)) == "RET" '
         '? 1 : -1', int_col("MovementSign")),
        ("AffectsLedgerFlag",
         'UPPER(TRIM(TransactionTypeCode)) == "SALE" || UPPER(TRIM(TransactionTypeCode)) == "PURCH" '
         '? "Y" : "N"', str_col("AffectsLedgerFlag", 1)),
        ("ReversalAllowedFlag",
         'UPPER(TRIM(TransactionTypeCode)) == "ADJ" || UPPER(TRIM(TransactionTypeCode)) == "RET" ? "Y" : "N"',
         str_col("ReversalAllowedFlag", 1)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_TransactionType"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["TransactionTypeCode", "TransactionTypeName",
                                        "RegionCode"]), str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Transaction Type", [
        ("Mapped Type", 'SourceCodeCount > 0 && TransactionTypeCode != "UNKNOWN"'),
    ], default_output="Unmapped Type")
    flow.row_count("Count Transaction Types Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Transaction Type", CONN_DW, "[Dimension].[Transaction Type]",
                           batch_size=5000)
    flow.branch_destination("ERR Transaction Type Unmapped", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Screen Transaction Type", "Unmapped Type")

    uom_cols = [str_col("UomCode", 10), str_col("UomName", 100), str_col("UomClassCode", 20),
                str_col("BaseUomCode", 10), int_col("DecimalPrecision"),
                dec_col("ConversionFactor", 18, 8)]
    uom = DataFlow("DFT Publish Unit Of Measure",
                   "Publish the conformed units and their standard conversion factors")
    uom.oledb_source(
        "REF UnitOfMeasure Conformed", CONN_STAGING,
        "SELECT u.UomCode, u.UomName, u.UomClassCode, u.BaseUomCode, u.DecimalPrecision,\n"
        "       ISNULL(c.ConversionFactor, CONVERT(DECIMAL(18,8), 1)) AS ConversionFactor\n"
        "FROM ref.UnitOfMeasure AS u\n"
        "     LEFT OUTER JOIN ref.UomConversion AS c\n"
        "       ON c.FromUomCode = u.UomCode AND c.ToUomCode = u.BaseUomCode\n"
        "      AND c.StockItemBusinessKey = N'*'\n"
        "WHERE u.IsActive = 1;",
        uom_cols, timeout=1800)
    uom.derived_column("Shape Unit Of Measure", [
        ("UomCode", 'UPPER(TRIM(UomCode))', str_col("UomCode", 10)),
        ("BaseFactorText", '(DT_WSTR,40)ConversionFactor', str_col("BaseFactorText", 40)),
    ])
    uom.row_count("Count Units Published", "User::RowsUpdated")
    uom.oledb_destination("DW Dimension Unit Of Measure", CONN_DW,
                          "[Dimension].[Transaction Type]", batch_size=5000)
    return build_reference_package(
        "REF_Load_TransactionType",
        "Conform the inventory transaction types and the unit-of-measure set: the crosswalk maps "
        "the OLTP movement type names and the ERP miscellaneous issue codes onto the conformed "
        "types, ref.UnitOfMeasure and ref.UomConversion hold the units and both the standard and "
        "the item-specific conversion factors, and the conformed types are published into "
        "Dimension.Transaction Type with their ledger effect and movement sign.",
        OLTP, "ref.UnitOfMeasure", [flow, uom],
        truncate_tables=["[Dimension].[Transaction Type]"],
        pre_tasks=[load_crosswalk("TRANSACTION_TYPE"),
                   load_reference("Load Unit Of Measure", "ref.usp_LoadUnitOfMeasure"),
                   load_reference("Load UOM Conversions", "ref.usp_LoadUomConversion")],
        post_tasks=[report_unmapped("TRANSACTION_TYPE")])


@package
def ref_load_payment_terms():
    """ref.usp_LoadCodeCrosswalk (PAYMENT_TERMS) -> Dimension.Payment Terms."""
    cols = [
        str_col("PaymentTermsCode", 10), str_col("PaymentTermsName", 60), str_col("RegionCode", 4),
        date_col("EffectiveFromDate"), bigint_col("SourceCodeCount"),
    ]
    flow = DataFlow("DFT Publish Payment Terms Dimension",
                    "Decode the conformed terms codes into day counts and discounts")
    flow.oledb_source(
        "REF PaymentTerms Conformed", CONN_STAGING,
        conformed_code_query("PAYMENT_TERMS", "PaymentTermsCode", "PaymentTermsName"),
        cols, timeout=1800)
    flow.row_count("Count Terms Read", "User::RowsRead")
    flow.derived_column("Conform Payment Terms", [
        ("PaymentTermsCode", 'UPPER(TRIM(PaymentTermsCode))', str_col("PaymentTermsCode", 10)),
        ("PaymentTermsName",
         'ISNULL(PaymentTermsName) ? UPPER(TRIM(PaymentTermsCode)) : TRIM(PaymentTermsName)',
         str_col("PaymentTermsName", 60)),
        # The conformed code carries the day count in its last two characters
        # except for EOM, which is month-end and has never fitted the pattern.
        ("NetDays",
         'UPPER(TRIM(PaymentTermsCode)) == "EOM" ? 30 : '
         '(UPPER(TRIM(PaymentTermsCode)) == "DISC210" ? 30 : '
         '(DT_I4)(RIGHT(UPPER(TRIM(PaymentTermsCode)), 2)))', int_col("NetDays")),
        ("DiscountDays", 'UPPER(TRIM(PaymentTermsCode)) == "DISC210" ? 10 : 0',
         int_col("DiscountDays")),
        ("DiscountPercent",
         'UPPER(TRIM(PaymentTermsCode)) == "DISC210" ? (DT_NUMERIC,9,4)2 : (DT_NUMERIC,9,4)0',
         dec_col("DiscountPercent", 9, 4)),
        # The EU late payment directive caps supplier terms at sixty days; APAC
        # trading houses habitually run ninety.
        ("NetDaysCapped",
         'RegionCode == "EU" && NetDays > 60 ? 60 : (RegionCode == "APAC" && NetDays > 90 ? 90 : NetDays)',
         int_col("NetDaysCapped")),
        ("EarlySettlementFlag", 'DiscountDays > 0 && DiscountPercent > 0 ? "Y" : "N"',
         str_col("EarlySettlementFlag", 1)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_PaymentTerms"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["PaymentTermsCode", "PaymentTermsName", "RegionCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Payment Terms", [
        ("Valid Terms", 'NetDaysCapped > 0 && NetDaysCapped <= 365'),
    ], default_output="Implausible Terms")
    flow.row_count("Count Terms Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Payment Terms", CONN_DW, "[Dimension].[Payment Terms]",
                           batch_size=5000)
    flow.branch_destination("ERR Payment Terms Implausible", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Payment Terms", "Implausible Terms")
    return build_reference_package(
        "REF_Load_PaymentTerms",
        "Refresh the PAYMENT_TERMS domain of ref.CodeCrosswalk and publish the conformed terms "
        "into Dimension.Payment Terms, decoding the net and discount day counts out of the "
        "conformed code and capping net days at the regional legal or customary maximum (60 in "
        "the EU, 90 in APAC).",
        ORA, "ref.CodeCrosswalk", [flow],
        truncate_tables=["[Dimension].[Payment Terms]"],
        pre_tasks=[load_crosswalk("PAYMENT_TERMS")],
        post_tasks=[report_unmapped("PAYMENT_TERMS")])


# ---------------------------------------------------------------------------
# commercial code sets
# ---------------------------------------------------------------------------


@package
def ref_load_return_reason():
    """ref.usp_LoadReasonCode + crosswalk (RETURN) -> Dimension.Return Reason."""
    cols = [
        str_col("ReturnReasonCode", 12), str_col("ReturnReasonName", 100),
        str_col("ReasonGroupCode", 20), bool_col("IsCustomerFault"), bool_col("IsSupplierFault"),
        bool_col("RequiresApproval"), bigint_col("SourceCodeCount"),
    ]
    flow = DataFlow("DFT Publish Return Reason Dimension",
                    "Publish the conformed return reasons with their fault attribution")
    flow.oledb_source(
        "REF ReturnReason Conformed", CONN_STAGING,
        "SELECT r.ConformedReasonCode AS ReturnReasonCode, r.ConformedReasonName AS ReturnReasonName,\n"
        "       r.ReasonGroupCode, r.IsCustomerFault, r.IsSupplierFault, r.RequiresApproval,\n"
        "       (SELECT COUNT_BIG(*) FROM ref.CodeCrosswalk AS x\n"
        "        WHERE x.CodeDomainCode = N'RETURN'\n"
        "          AND x.ConformedCodeValue = r.ConformedReasonCode\n"
        "          AND x.EffectiveToDate IS NULL) AS SourceCodeCount\n"
        "FROM ref.ReasonCode AS r\n"
        "WHERE r.ReasonDomainCode = N'RETURN' AND r.IsActive = 1;",
        cols, timeout=1800)
    flow.row_count("Count Return Reasons Read", "User::RowsRead")
    flow.derived_column("Classify Return Reason", [
        ("ReturnReasonCode", 'UPPER(TRIM(ReturnReasonCode))', str_col("ReturnReasonCode", 12)),
        ("ReasonCategoryCode",
         'ReasonGroupCode == "QUALITY" ? "QUALITY" : (ReasonGroupCode == "FULFILMENT" ? "SERVICE" : '
         '(ReasonGroupCode == "COMMERCIAL" ? "CUSTOMER" : "OTHER"))',
         str_col("ReasonCategoryCode", 10)),
        ("SupplierRecoverableFlag", 'IsSupplierFault == TRUE ? "Y" : "N"',
         str_col("SupplierRecoverableFlag", 1)),
        # EU distance selling means a change of mind is always refundable; NA and
        # APAC apply a restocking fee outside the goodwill window. The reason set
        # is not regional, so the fee is published per region on the dimension.
        ("RestockingFeePercentEu", '(DT_NUMERIC,9,4)0', dec_col("RestockingFeePercentEu", 9, 4)),
        ("RestockingFeePercentNa",
         'UPPER(TRIM(ReturnReasonCode)) == "NOTNEEDED" ? (DT_NUMERIC,9,4)15 : (DT_NUMERIC,9,4)0',
         dec_col("RestockingFeePercentNa", 9, 4)),
        ("RestockingFeePercentApac",
         'UPPER(TRIM(ReturnReasonCode)) == "NOTNEEDED" ? (DT_NUMERIC,9,4)15 : (DT_NUMERIC,9,4)0',
         dec_col("RestockingFeePercentApac", 9, 4)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_ReturnReason"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["ReturnReasonCode", "ReturnReasonName", "ReasonGroupCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Return Reason", [
        ("Mapped Reason", 'SourceCodeCount > 0'),
    ], default_output="Unsourced Reason")
    flow.row_count("Count Return Reasons Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Return Reason", CONN_DW, "[Dimension].[Return Reason]",
                           batch_size=5000)
    flow.branch_destination("ERR Return Reason Unsourced", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Screen Return Reason", "Unsourced Reason")
    return build_reference_package(
        "REF_Load_ReturnReason",
        "Conform the return and credit reason sets into ref.ReasonCode, map the OLTP return "
        "reasons and the ERP RTN-* codes onto them through ref.CodeCrosswalk, and publish the "
        "return reasons into Dimension.Return Reason with their fault attribution and the "
        "regional restocking fee (zero in the EU under distance selling).",
        OLTP, "ref.ReasonCode", [flow],
        truncate_tables=["[Dimension].[Return Reason]"],
        pre_tasks=[load_reference("Load Reason Codes", "ref.usp_LoadReasonCode"),
                   load_crosswalk("RETURN"),
                   load_crosswalk("CREDIT")],
        post_tasks=[report_unmapped("RETURN")])


@package
def ref_load_loyalty_tier():
    """ref.usp_LoadCodeCrosswalk (LOYALTY_TIER) -> Dimension.Loyalty Tier."""
    cols = [
        str_col("LoyaltyTierCode", 8), str_col("TierName", 60), str_col("RegionCode", 4),
        date_col("EffectiveFromDate"), bigint_col("SourceCodeCount"),
    ]
    flow = DataFlow("DFT Publish Loyalty Tier Dimension",
                    "Rank the conformed tiers and apply the regional discount schedule")
    flow.oledb_source(
        "REF LoyaltyTier Conformed", CONN_STAGING,
        conformed_code_query("LOYALTY_TIER", "LoyaltyTierCode", "TierName"),
        cols, timeout=1800)
    flow.row_count("Count Tiers Read", "User::RowsRead")
    flow.derived_column("Conform Loyalty Tier", [
        ("LoyaltyTierCode", 'UPPER(TRIM(LoyaltyTierCode))', str_col("LoyaltyTierCode", 8)),
        ("TierName", 'ISNULL(TierName) ? UPPER(TRIM(LoyaltyTierCode)) : TRIM(TierName)',
         str_col("TierName", 60)),
        ("TierRank",
         'UPPER(TRIM(LoyaltyTierCode)) == "TIER4" ? 1 : (UPPER(TRIM(LoyaltyTierCode)) == "TIER3" ? 2 : '
         '(UPPER(TRIM(LoyaltyTierCode)) == "TIER2" ? 3 : 4))', int_col("TierRank")),
        # The APAC programme runs richer discounts than the legacy NA scheme and
        # the EU scheme is capped by the local promotions rules.
        ("DiscountPercent",
         'RegionCode == "APAC" ? (DT_NUMERIC,9,4)(12 - 2 * (UPPER(TRIM(LoyaltyTierCode)) == "TIER4" ? 1 : '
         '(UPPER(TRIM(LoyaltyTierCode)) == "TIER3" ? 2 : (UPPER(TRIM(LoyaltyTierCode)) == "TIER2" ? 3 : 4)))) '
         ': (RegionCode == "EU" ? (DT_NUMERIC,9,4)5 : (DT_NUMERIC,9,4)(10 - 2 * '
         '(UPPER(TRIM(LoyaltyTierCode)) == "TIER4" ? 1 : (UPPER(TRIM(LoyaltyTierCode)) == "TIER3" ? 2 : '
         '(UPPER(TRIM(LoyaltyTierCode)) == "TIER2" ? 3 : 4)))))', dec_col("DiscountPercent", 9, 4)),
        ("PointsMultiplier",
         'RegionCode == "APAC" ? (DT_NUMERIC,18,2)1.5 : (DT_NUMERIC,18,2)1',
         money_col("PointsMultiplier")),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_LoyaltyTier"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["LoyaltyTierCode", "TierName", "RegionCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Loyalty Tier", [
        ("Mapped Tier", 'SourceCodeCount > 0'),
    ], default_output="Unsourced Tier")
    flow.row_count("Count Tiers Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Loyalty Tier", CONN_DW, "[Dimension].[Loyalty Tier]",
                           batch_size=5000)
    flow.branch_destination("ERR Loyalty Tier Unsourced", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Screen Loyalty Tier", "Unsourced Tier")
    return build_reference_package(
        "REF_Load_LoyaltyTier",
        "Refresh the LOYALTY_TIER domain of ref.CodeCrosswalk, which maps the OLTP tier names onto "
        "the conformed TIER1-TIER4 set, and publish the tiers into Dimension.Loyalty Tier with the "
        "regional discount schedule: richer in APAC, capped at five percent in the EU by local "
        "promotion rules and the legacy schedule in NA.",
        OLTP, "ref.CodeCrosswalk", [flow],
        truncate_tables=["[Dimension].[Loyalty Tier]"],
        pre_tasks=[load_crosswalk("LOYALTY_TIER")],
        post_tasks=[report_unmapped("LOYALTY_TIER")])


@package
def ref_load_sales_channel():
    """ref.usp_LoadCodeCrosswalk (SALES_CHANNEL) -> Dimension.Sales Channel."""
    cols = [
        str_col("ChannelCode", 10), str_col("ChannelName", 60), str_col("RegionCode", 4),
        date_col("EffectiveFromDate"), bigint_col("SourceCodeCount"),
    ]
    flow = DataFlow("DFT Publish Sales Channel Dimension",
                    "Group the conformed channels into direct, digital and partner families")
    flow.oledb_source(
        "REF SalesChannel Conformed", CONN_STAGING,
        conformed_code_query("SALES_CHANNEL", "ChannelCode", "ChannelName"),
        cols, timeout=1800)
    flow.row_count("Count Channels Read", "User::RowsRead")
    flow.derived_column("Conform Sales Channel", [
        ("ChannelCode", 'UPPER(TRIM(ChannelCode))', str_col("ChannelCode", 10)),
        ("ChannelName", 'ISNULL(ChannelName) ? UPPER(TRIM(ChannelCode)) + " channel" : TRIM(ChannelName)',
         str_col("ChannelName", 60)),
        ("ChannelGroupCode",
         'UPPER(TRIM(ChannelCode)) == "ONLINE" ? "DIGITAL" : (UPPER(TRIM(ChannelCode)) == "PARTNER" '
         '? "PARTNER" : "DIRECT")', str_col("ChannelGroupCode", 10)),
        ("DigitalFlag", 'UPPER(TRIM(ChannelCode)) == "ONLINE" ? "Y" : "N"', str_col("DigitalFlag", 1)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_SalesChannel"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["ChannelCode", "ChannelGroupCode", "RegionCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Sales Channel", [
        ("Mapped Channel", 'SourceCodeCount > 0'),
    ], default_output="Unsourced Channel")
    flow.row_count("Count Channels Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Sales Channel", CONN_DW, "[Dimension].[Sales Channel]",
                           batch_size=5000)
    flow.branch_destination("ERR Sales Channel Unsourced", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Screen Sales Channel", "Unsourced Channel")
    return build_reference_package(
        "REF_Load_SalesChannel",
        "Refresh the SALES_CHANNEL domain of ref.CodeCrosswalk - the domain that has to carry a "
        "region, because the OLTP code DIR is the direct sales force in NA and a distributor in "
        "APAC - and publish the conformed channels into Dimension.Sales Channel grouped into "
        "direct, digital and partner families.",
        OLTP, "ref.CodeCrosswalk", [flow],
        truncate_tables=["[Dimension].[Sales Channel]"],
        pre_tasks=[load_crosswalk("SALES_CHANNEL")],
        post_tasks=[report_unmapped("SALES_CHANNEL")])


@package
def ref_load_carrier():
    """ref.usp_LoadCodeCrosswalk (CARRIER) -> Dimension.Carrier."""
    cols = [
        str_col("CarrierCode", 10), str_col("CarrierName", 60), str_col("RegionCode", 4),
        date_col("EffectiveFromDate"), bigint_col("SourceCodeCount"),
    ]
    flow = DataFlow("DFT Publish Carrier Dimension",
                    "Publish the conformed carriers with their regional on-time targets")
    flow.oledb_source(
        "REF Carrier Conformed", CONN_STAGING,
        conformed_code_query("CARRIER", "CarrierCode", "CarrierName"),
        cols, timeout=1800)
    flow.row_count("Count Carriers Read", "User::RowsRead")
    flow.derived_column("Conform Carrier", [
        ("CarrierCode", 'UPPER(TRIM(CarrierCode))', str_col("CarrierCode", 10)),
        ("CarrierName", 'ISNULL(CarrierName) ? UPPER(TRIM(CarrierCode)) : TRIM(CarrierName)',
         str_col("CarrierName", 60)),
        ("CrossBorderFlag", 'RegionCode == "ALL" ? "Y" : "N"', str_col("CrossBorderFlag", 1)),
        ("OnTimeTargetDays",
         'RegionCode == "APAC" ? (DT_NUMERIC,9,2)5 : (RegionCode == "EU" ? (DT_NUMERIC,9,2)3 '
         ': (DT_NUMERIC,9,2)4)', dec_col("OnTimeTargetDays", 9, 2)),
        ("OwnFleetFlag", 'UPPER(TRIM(CarrierCode)) == "OWN" ? "Y" : "N"', str_col("OwnFleetFlag", 1)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_Carrier"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["CarrierCode", "CarrierName", "RegionCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Carrier", [
        ("Mapped Carrier", 'SourceCodeCount > 0'),
    ], default_output="Unsourced Carrier")
    flow.row_count("Count Carriers Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Carrier", CONN_DW, "[Dimension].[Carrier]", batch_size=5000)
    flow.branch_destination("ERR Carrier Unsourced", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Screen Carrier", "Unsourced Carrier")
    return build_reference_package(
        "REF_Load_Carrier",
        "Refresh the CARRIER domain of ref.CodeCrosswalk, which maps the OLTP carrier names and "
        "the ERP's APAC carriers onto the conformed carrier set, and publish them into "
        "Dimension.Carrier holding the regional on-time targets (three days in the EU, four in NA, "
        "five across APAC).",
        OLTP, "ref.CodeCrosswalk", [flow],
        truncate_tables=["[Dimension].[Carrier]"],
        pre_tasks=[load_crosswalk("CARRIER")],
        post_tasks=[report_unmapped("CARRIER")])


@package
def ref_load_warehouse_site():
    """ref.PostalFormatRule + ref.Country -> Dimension.Warehouse Site."""
    cols = [
        int_col("WarehouseSiteId"), str_col("WarehouseSiteCode", 10), str_col("WarehouseSiteName", 100),
        str_col("CountryCode", 2), str_col("RegionCode", 10), str_col("PostalCode", 20),
        str_col("FormatMask", 30), str_col("StripCharacters", 30), bool_col("UpperCaseFlag"),
        int_col("TruncateToLength"), bigint_col("MovementCount"),
    ]
    flow = DataFlow("DFT Publish Warehouse Site Dimension",
                    "Standardise the site postal code by the conformed per-country rule")
    flow.oledb_source(
        "REF Warehouse Site Conformed", CONN_STAGING,
        "SELECT m.WarehouseSiteId, MAX(m.WarehouseSiteCode) AS WarehouseSiteCode,\n"
        "       MAX(m.WarehouseSiteName) AS WarehouseSiteName, c.CountryCode, c.RegionCode,\n"
        "       MAX(m.PostalCode) AS PostalCode, MAX(p.FormatMask) AS FormatMask,\n"
        "       MAX(p.StripCharacters) AS StripCharacters,\n"
        "       MAX(CONVERT(TINYINT, p.UpperCaseFlag)) AS UpperCaseFlag,\n"
        "       MAX(CONVERT(INT, p.TruncateToLength)) AS TruncateToLength,\n"
        "       COUNT_BIG(*) AS MovementCount\n"
        "FROM stg.StockMovement AS m\n"
        "     INNER JOIN ref.Country AS c ON c.CountryCode = LEFT(UPPER(m.CountryCode), 2)\n"
        "     LEFT OUTER JOIN ref.PostalFormatRule AS p\n"
        "       ON p.CountryCode = c.CountryCode AND p.RulePriority = 1\n"
        "GROUP BY m.WarehouseSiteId, c.CountryCode, c.RegionCode;",
        cols, timeout=1800)
    flow.row_count("Count Sites Read", "User::RowsRead")
    flow.derived_column("Standardize Site", [
        ("WarehouseSiteCode", 'UPPER(TRIM(WarehouseSiteCode))', str_col("WarehouseSiteCode", 10)),
        # The conformed rule says what to strip and how long the result is; the
        # package still applies it by hand, exactly as it always has.
        ("PostalCodeStandardized",
         'TruncateToLength > 0 ? LEFT(REPLACE(REPLACE(UPPER(TRIM(ISNULL(PostalCode) ? "" : PostalCode)), '
         '" ", ""), "-", ""), TruncateToLength) : '
         'REPLACE(REPLACE(UPPER(TRIM(ISNULL(PostalCode) ? "" : PostalCode)), " ", ""), "-", "")',
         str_col("PostalCodeStandardized", 20)),
        ("SiteTypeCode",
         'MovementCount > 100000 ? "DC" : (MovementCount > 10000 ? "REGIONAL" : "SATELLITE")',
         str_col("SiteTypeCode", 10)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_WarehouseSite"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["WarehouseSiteCode", "WarehouseSiteName", "CountryCode",
                                        "PostalCode"]), str_col("ChangeHash", 200)),
    ])
    flow.lookup(
        "Lookup Site Region (Full Cache)", CONN_STAGING,
        "SELECT RegionCode, RegionName, AddressRuleSetCode, WeightUomCode FROM ref.Region;",
        ["RegionCode"], [str_col("RegionName", 100), str_col("AddressRuleSetCode", 30),
                         str_col("WeightUomCode", 10)], no_match="RD")
    flow.conditional_split("Screen Warehouse Site", [
        ("Formatted Site", 'LEN(PostalCodeStandardized) > 2'),
    ], default_output="Unformatted Site")
    flow.row_count("Count Sites Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Warehouse Site", CONN_DW, "[Dimension].[Warehouse Site]",
                           batch_size=5000)
    flow.branch_destination("ERR Site Postal Unformatted", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Warehouse Site", "Unformatted Site")
    flow.reject_destination("ERR Site Unknown Region", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Site Region (Full Cache)", "Lookup No Match Output")
    return build_reference_package(
        "REF_Load_WarehouseSite",
        "Refresh the conformed regions and the per-country postal rules, then publish the "
        "warehouse sites into Dimension.Warehouse Site with their postal code standardised by the "
        "rule ref.PostalFormatRule holds for the site's country rather than by a region-wide guess, "
        "and classify sites as distribution centres, regional sites or satellites by volume.",
        OLTP, "ref.PostalFormatRule", [flow],
        truncate_tables=["[Dimension].[Warehouse Site]"],
        pre_tasks=[load_reference("Load Regions", "ref.usp_LoadRegion"),
                   load_reference("Load Postal Format Rules", "ref.usp_LoadPostalFormatRule")],
        post_tasks=[report_unmapped("REGION")])


@package
def ref_load_cost_center():
    """ref.usp_LoadCodeCrosswalk (COST_CENTER) -> Dimension.Cost Center."""
    cols = [
        str_col("CostCenterCode", 12), str_col("CostCenterName", 60), str_col("RegionCode", 4),
        date_col("EffectiveFromDate"), bigint_col("SourceCodeCount"),
    ]
    flow = DataFlow("DFT Publish Cost Center Dimension",
                    "Rebuild the cost centre hierarchy attributes from the conformed codes")
    flow.oledb_source(
        "REF CostCenter Conformed", CONN_STAGING,
        conformed_code_query("COST_CENTER", "CostCenterCode", "CostCenterName"),
        cols, timeout=1800)
    flow.row_count("Count Cost Centers Read", "User::RowsRead")
    flow.derived_column("Conform Cost Center", [
        ("CostCenterCode", 'UPPER(TRIM(CostCenterCode))', str_col("CostCenterCode", 12)),
        ("CostCenterName",
         'ISNULL(CostCenterName) ? UPPER(TRIM(CostCenterCode)) : TRIM(CostCenterName)',
         str_col("CostCenterName", 60)),
        # Everything rolls up to CORP except CORP itself; the Oracle chart of
        # accounts has never had more than two levels that the warehouse uses.
        ("ParentCostCenterCode",
         'UPPER(TRIM(CostCenterCode)) == "CORP" ? "ROOT" : "CORP"',
         str_col("ParentCostCenterCode", 12)),
        ("CompanyCode", '"0001"', str_col("CompanyCode", 6)),
        ("FunctionCode", 'LEFT(UPPER(TRIM(CostCenterCode)), 2)', str_col("FunctionCode", 2)),
        ("SuspenseFlag", 'UPPER(TRIM(CostCenterCode)) == "SUSP" ? "Y" : "N"',
         str_col("SuspenseFlag", 1)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_CostCenter"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["CostCenterCode", "CostCenterName", "RegionCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.sort("Sort Cost Centers By Hierarchy", ["ParentCostCenterCode", "CostCenterCode"])
    flow.conditional_split("Screen Cost Center", [
        ("Mapped Cost Center", 'SourceCodeCount > 0 && CostCenterCode != ParentCostCenterCode'),
        ("Self Referencing", 'CostCenterCode == ParentCostCenterCode'),
    ], default_output="Unsourced Cost Center")
    flow.row_count("Count Cost Centers Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Cost Center", CONN_DW, "[Dimension].[Cost Center]",
                           batch_size=5000)
    flow.branch_destination("ERR Cost Center Self Reference", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Cost Center", "Self Referencing")
    flow.branch_destination("ERR Cost Center Unsourced", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Screen Cost Center", "Unsourced Cost Center")
    return build_reference_package(
        "REF_Load_CostCenter",
        "Refresh the COST_CENTER domain of ref.CodeCrosswalk, which maps the numeric Oracle cost "
        "centres onto the conformed mnemonics, and publish them into Dimension.Cost Center with "
        "the rollup parent and the function segment, rejecting self-referencing rows that would "
        "otherwise loop the hierarchy rollup.",
        ORA, "ref.CodeCrosswalk", [flow],
        truncate_tables=["[Dimension].[Cost Center]"],
        pre_tasks=[load_crosswalk("COST_CENTER")],
        post_tasks=[report_unmapped("COST_CENTER")])


@package
def ref_load_geography():
    """ref.usp_LoadRegion / LoadCountry / LoadTaxJurisdiction -> Dimension.Geography."""
    cols = [
        str_col("CountryCode", 2), str_col("CountryCodeIso3", 3), str_col("CountryName", 100),
        str_col("RegionCode", 10), str_col("SubRegionName", 100), str_col("LocalCurrencyCode", 3),
        bool_col("IsEuMemberState"), date_col("EuExitDate"), str_col("TaxRegimeCode", 20),
        str_col("TaxJurisdictionCode", 30), dec_col("CombinedRatePercent", 9, 4),
        bool_col("ReverseChargeEligible"), str_col("PostalFormatMask", 30),
    ]
    flow = DataFlow("DFT Publish Geography Dimension",
                    "Publish the conformed country set with its regional tax structure")
    flow.oledb_source(
        "REF Geography Conformed", CONN_STAGING,
        "SELECT c.CountryCode, c.CountryCodeIso3, c.CountryName, c.RegionCode, c.SubRegionName,\n"
        "       c.LocalCurrencyCode, c.IsEuMemberState, c.EuExitDate, r.TaxRegimeCode,\n"
        "       j.TaxJurisdictionCode, j.CombinedRatePercent, j.ReverseChargeEligible,\n"
        "       c.PostalFormatMask\n"
        "FROM ref.Country AS c\n"
        "     INNER JOIN ref.Region AS r ON r.RegionCode = c.RegionCode\n"
        "     OUTER APPLY (SELECT TOP (1) t.TaxJurisdictionCode, t.CombinedRatePercent,\n"
        "                         t.ReverseChargeEligible\n"
        "                  FROM ref.TaxJurisdiction AS t\n"
        "                  WHERE t.CountryCode = c.CountryCode\n"
        "                    AND t.StateProvinceCode IS NULL\n"
        "                    AND (t.EffectiveToDate IS NULL\n"
        "                         OR t.EffectiveToDate >= CONVERT(date, GETDATE()))\n"
        "                  ORDER BY t.EffectiveFromDate DESC) AS j\n"
        "WHERE c.IsActive = 1;",
        cols, timeout=3600)
    flow.row_count("Count Geography Rows Read", "User::RowsRead")
    flow.derived_column("Standardize Geography", [
        ("CountryCode", 'UPPER(TRIM(CountryCode))', str_col("CountryCode", 2)),
        ("GeographyCode", 'UPPER(TRIM(CountryCode)) + "|" + UPPER(TRIM(RegionCode))',
         str_col("GeographyCode", 20)),
        ("GeographyName", 'TRIM(CountryName)', str_col("GeographyName", 100)),
        # Three tax regimes, three genuinely different shapes: NA stacks state,
        # county and city rates onto a jurisdiction, the EU carries one country
        # VAT rate that can reverse-charge, APAC carries a flat national GST.
        ("TaxStructureCode",
         'TaxRegimeCode == "VAT" ? "EU_VAT" : (TaxRegimeCode == "GST" ? "APAC_GST" : "NA_SALESTAX")',
         str_col("TaxStructureCode", 12)),
        ("ReverseChargeFlag",
         'TaxRegimeCode == "VAT" && ReverseChargeEligible == TRUE ? "Y" : "N"',
         str_col("ReverseChargeFlag", 1)),
        ("EuStatusCode",
         'IsEuMemberState == TRUE ? "MEMBER" : (ISNULL(EuExitDate) ? "NONMEMBER" : "EXITED")',
         str_col("EuStatusCode", 12)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_Geography"', str_col("LineageKey", 40)),
        ("ChangeHash", hash_expression(["CountryCode", "CountryName", "RegionCode",
                                        "TaxJurisdictionCode"]), str_col("ChangeHash", 200)),
    ])
    flow.lookup(
        "Lookup Country Currency (Full Cache)", CONN_STAGING,
        "SELECT CurrencyCode AS LocalCurrencyCode, CurrencyName, MinorUnitDigits FROM ref.Currency;",
        ["LocalCurrencyCode"], [str_col("CurrencyName", 100), int_col("MinorUnitDigits")],
        no_match="RD")
    flow.conditional_split("Screen Geography", [
        ("Taxed Country", 'LEN(TRIM(ISNULL(TaxJurisdictionCode) ? "" : TaxJurisdictionCode)) > 0'),
    ], default_output="Country Without Jurisdiction")
    flow.row_count("Count Geography Rows Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Geography", CONN_DW, "[Dimension].[Geography]",
                           batch_size=50000)
    flow.branch_destination("ERR Geography Without Jurisdiction", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Screen Geography", "Country Without Jurisdiction")
    flow.reject_destination("ERR Geography Unknown Currency", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Lookup Country Currency (Full Cache)", "Lookup No Match Output")
    return build_reference_package(
        "REF_Load_Geography",
        "Rebuild the conformed geography spine - ref.Region, ref.Country, the effective-dated "
        "ref.TaxJurisdiction and the per-country ref.PostalFormatRule - from the Oracle geography "
        "and tax extracts, then publish the countries into Dimension.Geography carrying which of "
        "the three tax structures applies (NA sales tax stack, EU VAT with reverse charge, APAC "
        "GST) and the country's EU membership history.",
        ORA, "ref.Country", [flow],
        truncate_tables=["[Dimension].[Geography]"],
        pre_tasks=[load_reference("Load Regions", "ref.usp_LoadRegion"),
                   load_reference("Load Countries", "ref.usp_LoadCountry"),
                   load_reference("Load Tax Jurisdictions", "ref.usp_LoadTaxJurisdiction"),
                   load_reference("Load Postal Format Rules", "ref.usp_LoadPostalFormatRule")],
        post_tasks=[report_unmapped("COUNTRY")])


@package
def ref_load_date_dimension():
    """Generate Dimension.Date with the fiscal calendar each conformed region keeps."""
    cols = [
        date_col("CalendarDate"), int_col("CalendarYear"), int_col("CalendarMonth"),
        int_col("CalendarDay"), int_col("DayOfWeekNumber"), str_col("MonthName", 12),
        str_col("DayName", 12), int_col("FiscalYearStartMonthNa"), int_col("FiscalYearStartMonthEu"),
        int_col("FiscalYearStartMonthApac"),
    ]
    flow = DataFlow("DFT Generate Calendar",
                    "Derive each region's fiscal attributes from ref.Region")
    flow.oledb_source(
        "STG Calendar Spine", CONN_STAGING,
        # The spine is generated on the fly; the estate has never held a
        # permanent calendar table in the staging database.
        "WITH DateSpine AS\n"
        "(\n"
        "    SELECT CONVERT(date, N'2005-01-01') AS CalendarDate\n"
        "    UNION ALL\n"
        "    SELECT DATEADD(day, 1, CalendarDate) FROM DateSpine\n"
        "    WHERE CalendarDate < CONVERT(date, N'2035-12-31')\n"
        ")\n"
        "SELECT s.CalendarDate, YEAR(s.CalendarDate) AS CalendarYear,\n"
        "       MONTH(s.CalendarDate) AS CalendarMonth, DAY(s.CalendarDate) AS CalendarDay,\n"
        "       DATEPART(weekday, s.CalendarDate) AS DayOfWeekNumber,\n"
        "       DATENAME(month, s.CalendarDate) AS MonthName,\n"
        "       DATENAME(weekday, s.CalendarDate) AS DayName,\n"
        "       (SELECT FiscalYearStartMonth FROM ref.Region WHERE RegionCode = N'NA')\n"
        "           AS FiscalYearStartMonthNa,\n"
        "       (SELECT FiscalYearStartMonth FROM ref.Region WHERE RegionCode = N'EU')\n"
        "           AS FiscalYearStartMonthEu,\n"
        "       (SELECT FiscalYearStartMonth FROM ref.Region WHERE RegionCode = N'APAC')\n"
        "           AS FiscalYearStartMonthApac\n"
        "FROM DateSpine AS s\n"
        "OPTION (MAXRECURSION 0);",
        cols, timeout=1800)
    flow.row_count("Count Calendar Rows Read", "User::RowsRead")
    flow.derived_column("Derive Fiscal Calendars", [
        # The fiscal year start months come from ref.Region rather than being
        # hard-coded here, so a region that moves its year end moves everywhere.
        ("FiscalYearNa",
         'CalendarMonth >= FiscalYearStartMonthNa ? CalendarYear + 1 : CalendarYear',
         int_col("FiscalYearNa")),
        ("FiscalPeriodNa",
         'CalendarMonth >= FiscalYearStartMonthNa ? CalendarMonth - FiscalYearStartMonthNa + 1 '
         ': CalendarMonth + 12 - FiscalYearStartMonthNa + 1', int_col("FiscalPeriodNa")),
        ("FiscalYearEu",
         'FiscalYearStartMonthEu == 1 ? CalendarYear : (CalendarMonth >= FiscalYearStartMonthEu '
         '? CalendarYear + 1 : CalendarYear)', int_col("FiscalYearEu")),
        ("FiscalPeriodEu",
         'CalendarMonth >= FiscalYearStartMonthEu ? CalendarMonth - FiscalYearStartMonthEu + 1 '
         ': CalendarMonth + 12 - FiscalYearStartMonthEu + 1', int_col("FiscalPeriodEu")),
        ("FiscalYearApac",
         'CalendarMonth >= FiscalYearStartMonthApac ? CalendarYear : CalendarYear - 1',
         int_col("FiscalYearApac")),
        ("FiscalPeriodApac",
         'CalendarMonth >= FiscalYearStartMonthApac ? CalendarMonth - FiscalYearStartMonthApac + 1 '
         ': CalendarMonth + 12 - FiscalYearStartMonthApac + 1', int_col("FiscalPeriodApac")),
        ("CalendarQuarter", '((CalendarMonth - 1) / 3) + 1', int_col("CalendarQuarter")),
        ("WeekendFlag", 'DayOfWeekNumber == 1 || DayOfWeekNumber == 7 ? "Y" : "N"',
         str_col("WeekendFlag", 1)),
        ("WorkingDayFlag", 'DayOfWeekNumber == 1 || DayOfWeekNumber == 7 ? "N" : "Y"',
         str_col("WorkingDayFlag", 1)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_DateDimension"', str_col("LineageKey", 40)),
    ])
    flow.conditional_split("Screen Calendar", [
        ("Valid Date", 'CalendarYear >= 2005 && CalendarYear <= 2035'),
    ], default_output="Out Of Range Date")
    flow.row_count("Count Calendar Rows Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Date", CONN_DW, "[Dimension].[Date]", batch_size=50000)
    flow.branch_destination("ERR Calendar Out Of Range", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Calendar", "Out Of Range Date")
    return build_reference_package(
        "REF_Load_DateDimension",
        "Regenerate Dimension.Date for 2005-2035. Each date carries all three fiscal calendars in "
        "parallel, and the year start month for each of them is read from ref.Region rather than "
        "being written into the package, so the conformed region policy is the only place a "
        "fiscal year end is defined.",
        OLTP, "ref.Region", [flow],
        truncate_tables=["[Dimension].[Date]"],
        pre_tasks=[load_reference("Load Regions", "ref.usp_LoadRegion")])


@package
def ref_load_unknown_members():
    """Seed the unknown members and refresh ref.SourceKeyCrosswalk."""
    cols = [str_col("ReferenceTableName", 60), str_col("UnknownCodeValue", 20),
            str_col("UnknownDescription", 100), str_col("DomainCode", 30)]
    flow = DataFlow("DFT Publish Unknown Members",
                    "One unknown member per conformed domain, taken from the conformed sets")
    flow.oledb_source(
        "REF Unknown Members", CONN_STAGING,
        "SELECT N'ref.StatusCode' AS ReferenceTableName, s.ConformedStatusCode AS UnknownCodeValue,\n"
        "       s.ConformedStatusName AS UnknownDescription, s.StatusDomainCode AS DomainCode\n"
        "FROM ref.StatusCode AS s\n"
        "WHERE s.ConformedStatusCode = N'UNKNOWN'\n"
        "UNION ALL\n"
        "SELECT N'ref.ReasonCode', r.ConformedReasonCode, r.ConformedReasonName, r.ReasonDomainCode\n"
        "FROM ref.ReasonCode AS r\n"
        "WHERE r.ConformedReasonCode = N'UNKNOWN';",
        cols, timeout=600)
    flow.row_count("Count Unknown Members Read", "User::RowsRead")
    flow.derived_column("Shape Unknown Member", [
        ("UnknownMemberKey", '-1', int_col("UnknownMemberKey")),
        ("NotApplicableKey", '-2', int_col("NotApplicableKey")),
        ("IsUnknownMemberFlag", '"Y"', str_col("IsUnknownMemberFlag", 1)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_UnknownMembers"', str_col("LineageKey", 40)),
    ])
    flow.conditional_split("Screen Unknown Member", [
        ("Seedable Domain", 'LEN(TRIM(DomainCode)) > 0'),
    ], default_output="Unnamed Domain")
    flow.row_count("Count Unknown Members Published", "User::RowsInserted")
    flow.oledb_destination("DW Dimension Unknown Member", CONN_DW, "[Dimension].[Unknown Member]",
                           batch_size=1000)
    flow.branch_destination("ERR Unknown Member Unnamed", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Unknown Member", "Unnamed Domain")
    return build_reference_package(
        "REF_Load_UnknownMembers",
        "Make sure every conformed domain has the UNKNOWN member the fact loads route to when a "
        "code cannot be translated, and refresh ref.SourceKeyCrosswalk so an ERP product code and "
        "an OLTP stock item id resolve to the same conformed business key. The unknown members "
        "come from ref.StatusCode and ref.ReasonCode rather than from a separate inventory table.",
        OLTP, "ref.SourceKeyCrosswalk", [flow],
        truncate_tables=["[Dimension].[Unknown Member]"],
        pre_tasks=[load_reference("Load Status Codes", "ref.usp_LoadStatusCode"),
                   load_reference("Load Reason Codes", "ref.usp_LoadReasonCode"),
                   load_reference("Load Source Key Crosswalk", "ref.usp_LoadSourceKeyCrosswalk")])


@package
def ref_load_code_translation():
    """Maintain every ref.CodeCrosswalk domain and report unmapped source codes."""
    cols = [
        str_col("CodeDomainCode", 30), str_col("SourceSystemCode", 20), str_col("SourceCodeValue", 50),
        str_col("ConformedCodeValue", 20), str_col("RegionCode", 10),
        bool_col("IsDefaultForConformed"), date_col("EffectiveFromDate"), date_col("EffectiveToDate"),
    ]
    flow = DataFlow("DFT Publish Code Crosswalk Configuration",
                    "Register the active mapping count per domain in the control configuration")
    flow.oledb_source(
        "REF CodeCrosswalk Active", CONN_STAGING,
        "SELECT x.CodeDomainCode, x.SourceSystemCode, x.SourceCodeValue, x.ConformedCodeValue,\n"
        "       x.RegionCode, x.IsDefaultForConformed, x.EffectiveFromDate, x.EffectiveToDate\n"
        "FROM ref.CodeCrosswalk AS x\n"
        "WHERE x.EffectiveToDate IS NULL;",
        cols, timeout=1800)
    flow.row_count("Count Active Mappings", "User::RowsRead")
    flow.derived_column("Shape Configuration Row", [
        ("ConfigurationKey", '"CodeSetVersion." + UPPER(TRIM(CodeDomainCode))',
         str_col("ConfigurationKey", 60)),
        ("ConfigurationValue", 'UPPER(TRIM(SourceSystemCode)) + "|" + UPPER(TRIM(ConformedCodeValue))',
         str_col("ConfigurationValue", 200)),
        ("LineageKey", '(DT_WSTR,40)"REF_Load_CodeTranslation"', str_col("LineageKey", 40)),
    ])
    flow.aggregate("Summarize Mapping Coverage", ["CodeDomainCode", "SourceSystemCode"], [
        ("SourceCodeValue", "MappingCount", "Count"),
    ])
    flow.row_count("Count Domains Registered", "User::RowsInserted")
    flow.oledb_destination("CTL Configuration", CONN_STAGING, "[etl].[Configuration]",
                           batch_size=5000)

    scan_cols = [str_col("CodeDomainCode", 30), str_col("SourceSystemCode", 20),
                 str_col("SourceCodeValue", 50), bigint_col("TotalOccurrenceCount"),
                 date_col("LastObservedAtUtc")]
    scan = DataFlow("DFT Scan For Unmapped Codes",
                    "Read the steward-facing unmapped list and register the worst offenders")
    scan.oledb_source(
        "REF Unmapped Source Codes", CONN_STAGING,
        "SELECT u.CodeDomainCode, u.SourceSystemCode, u.SourceCodeValue, u.TotalOccurrenceCount,\n"
        "       u.LastObservedAtUtc\n"
        "FROM ref.vw_UnmappedSourceCode AS u;",
        scan_cols, timeout=3600)
    scan.derived_column("Tag Mapping Coverage", [
        ("CoverageStatusCode", '"UNMAPPED"', str_col("CoverageStatusCode", 10)),
        ("SeverityCode", 'TotalOccurrenceCount > 1000 ? "HIGH" : "LOW"', str_col("SeverityCode", 4)),
        ("ReviewedFlag", '"N"', str_col("ReviewedFlag", 1)),
    ])
    scan.conditional_split("Screen Unmapped Code", [
        ("Reportable Code", 'LEN(TRIM(SourceCodeValue)) > 0'),
    ], default_output="Blank Code")
    scan.row_count("Count Unmapped Codes", "User::RowsUpdated")
    scan.oledb_destination("ERR Unmapped Code Report", CONN_STAGING,
                           "[err].[RejectedLookupFailure]", batch_size=20000)
    scan.branch_destination("ERR Unmapped Blank Code", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Unmapped Code", "Blank Code")
    return build_reference_package(
        "REF_Load_CodeTranslation",
        "Refresh every domain of ref.CodeCrosswalk from the steward grid held in "
        "ref.usp_LoadCodeCrosswalk, register the active mapping count per domain in "
        "etl.Configuration, and then sweep the raw extracts for codes that still have no mapping. "
        "The unmapped list is what the data-quality packages read out of "
        "ref.vw_UnmappedSourceCode.",
        ORA, "ref.CodeCrosswalk", [flow, scan],
        pre_tasks=[load_reference("Load Status Codes", "ref.usp_LoadStatusCode"),
                   load_reference("Load Reason Codes", "ref.usp_LoadReasonCode"),
                   load_reference("Load All Code Crosswalk Domains", "ref.usp_LoadCodeCrosswalk")],
        post_tasks=[exec_proc("Report Unmapped Codes - All Domains",
                              "EXEC ref.usp_ReportUnmappedCodes @BatchId = ?, "
                              "@PackageExecutionId = ?;",
                              parameter_bindings=list(BATCH_BINDINGS))])


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
