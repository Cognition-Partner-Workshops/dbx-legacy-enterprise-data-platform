"""Spec module for the WWI_ReferenceData SSIS project (ssis/06_reference_data).

Emits the fourteen reference-data packages declared in config/estate-catalog.yaml
for folder 06_reference_data. These packages load and version the conformed
ref.* code sets and crosswalks from the staged feeds and publish them into the
warehouse dimension tables, including the effective-dated FX and tax rates, the
region-specific code sets and the code-crosswalk maintenance that reports
unmapped source codes back to the stewards.

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
    truncate,
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


def version_reference_set(ref_table, code_set_name):
    """Close the current version and open a new one for an effective-dated set.

    The estate has never used temporal tables here; versioning is a hand-rolled
    close-and-open of the ValidFrom/ValidTo pair inside one transaction.
    """
    sql = (
        "DECLARE @Now DATETIME2(3) = SYSUTCDATETIME();\n"
        "BEGIN TRANSACTION;\n"
        "UPDATE %s\n"
        "SET ValidTo = @Now, IsCurrent = 0\n"
        "WHERE IsCurrent = 1\n"
        "  AND EXISTS (SELECT 1 FROM %s_Incoming AS i\n"
        "              WHERE i.BatchId = ? AND i.CodeValue = %s.CodeValue\n"
        "                AND i.ChangeHash <> %s.ChangeHash);\n"
        "INSERT INTO %s (CodeValue, CodeDescription, RegionCode, ChangeHash, ValidFrom, ValidTo, "
        "IsCurrent, VersionNumber, CodeSetName)\n"
        "SELECT i.CodeValue, i.CodeDescription, i.RegionCode, i.ChangeHash, @Now, "
        "CONVERT(DATETIME2(3), N'9999-12-31'), 1,\n"
        "       ISNULL((SELECT MAX(p.VersionNumber) FROM %s AS p WHERE p.CodeValue = i.CodeValue), 0) + 1,\n"
        "       N'%s'\n"
        "FROM %s_Incoming AS i\n"
        "WHERE i.BatchId = ?\n"
        "  AND NOT EXISTS (SELECT 1 FROM %s AS c\n"
        "                  WHERE c.CodeValue = i.CodeValue AND c.IsCurrent = 1\n"
        "                    AND c.ChangeHash = i.ChangeHash);\n"
        "COMMIT TRANSACTION;"
        % (ref_table, ref_table, ref_table, ref_table, ref_table, ref_table, code_set_name,
           ref_table, ref_table)
    )
    return ExecuteSql(
        "Version Reference Set - %s" % code_set_name,
        CONN_STAGING,
        sql,
        parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                            ("$Package::BatchId", 1, "LONG")],
    )


def report_unmapped(code_set_name, source_object):
    """Count and register source codes with no crosswalk entry."""
    sql = (
        "INSERT INTO err.UnmappedCode (BatchId, CodeSetName, SourceSystemCode, SourceCode, "
        "OccurrenceCount, FirstSeenAtUtc)\n"
        "SELECT ?, N'%s', s.SourceSystemCode, s.SourceCode, COUNT_BIG(*), SYSUTCDATETIME()\n"
        "FROM %s AS s\n"
        "WHERE NOT EXISTS (SELECT 1 FROM ref.CodeCrosswalk AS x\n"
        "                  WHERE x.CodeSetName = N'%s' AND x.SourceCode = s.SourceCode\n"
        "                    AND x.SourceSystemCode = s.SourceSystemCode)\n"
        "GROUP BY s.SourceSystemCode, s.SourceCode;\n"
        "SELECT @@ROWCOUNT AS UnmappedCodeCount;" % (code_set_name, source_object, code_set_name)
    )
    return ExecuteSql(
        "Report Unmapped Codes - %s" % code_set_name,
        CONN_STAGING,
        sql,
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
        result_bindings=[("0", "User::UnmappedCodeCount")],
    )


def log_unmapped_rejects(code_set_name):
    """Register each unmapped code with the control framework, one at a time."""
    sql = (
        "DECLARE @Code NVARCHAR(60), @System NVARCHAR(20);\n"
        "DECLARE unmapped_cur CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT u.SourceCode, u.SourceSystemCode FROM err.UnmappedCode AS u\n"
        "    WHERE u.BatchId = ? AND u.CodeSetName = N'%s' AND u.LoggedToControl = 0;\n"
        "OPEN unmapped_cur;\n"
        "FETCH NEXT FROM unmapped_cur INTO @Code, @System;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    EXEC etl.usp_LogRejectedRecord @PackageExecutionId = ?, @BatchId = ?, "
        "@SourceSystemCode = @System, @ObjectName = N'ref.CodeCrosswalk', @BusinessKey = @Code, "
        "@RejectReasonCode = N'REF_UNMAPPED_CODE', @RejectStage = N'Reference';\n"
        "    UPDATE err.UnmappedCode SET LoggedToControl = 1\n"
        "    WHERE BatchId = ? AND CodeSetName = N'%s' AND SourceCode = @Code;\n"
        "    FETCH NEXT FROM unmapped_cur INTO @Code, @System;\n"
        "END\n"
        "CLOSE unmapped_cur;\n"
        "DEALLOCATE unmapped_cur;" % (code_set_name, code_set_name)
    )
    return ExecuteSql(
        "Log Unmapped Codes - %s" % code_set_name,
        CONN_STAGING,
        sql,
        parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                            ("User::PackageExecutionId", 1, "LONG"),
                            ("$Package::BatchId", 2, "LONG"),
                            ("$Package::BatchId", 3, "LONG")],
    )


def hash_expression(columns):
    """The house change-hash expression: pipe-delimited, upper-cased, trimmed."""
    return " + \"|\" + ".join('UPPER(TRIM((DT_WSTR,60)%s))' % col for col in columns)


def build_reference_package(name, description, source_system, object_name, flows,
                            pre_tasks=(), post_tasks=(), truncate_tables=(),
                            extra_variables=(),
                            connections=(CONN_STAGING, CONN_DW)):
    """Assemble a reference package around the house control flow."""
    pkg = new_package(name, description, source_system=source_system, connections=connections,
                      extra_variables=REFERENCE_VARIABLES + list(extra_variables))
    ordered = [pkg.add(log_package_start(pkg))]
    for table in truncate_tables:
        ordered.append(pkg.add(truncate(table)))
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


# ---------------------------------------------------------------------------
# currency, rates and financial code sets
# ---------------------------------------------------------------------------


@package
def ref_load_currency():
    """stg.Currency + stg.FxRate -> ref.Currency, ref.FxRate and Dimension.Currency."""
    cur_cols = [
        str_col("CurrencyCode", 3), str_col("CurrencyName", 60), str_col("CurrencySymbol", 5),
        int_col("MinorUnitDigits"), str_col("RegionCode", 4), bool_col("IsActive"),
    ]
    cur = DataFlow("DFT Load Currency Reference", "Conform the ISO currency set and hash it for versioning")
    cur.oledb_source(
        "STG Currency", CONN_STAGING,
        "SELECT CurrencyCode, CurrencyName, CurrencySymbol, MinorUnitDigits, RegionCode, IsActive\n"
        "FROM stg.Currency;",
        cur_cols, timeout=1800)
    cur.row_count("Count Currencies Read", "User::RowsRead")
    cur.derived_column("Conform Currency", [
        ("CurrencyCode", 'UPPER(TRIM(CurrencyCode))', str_col("CurrencyCode", 3)),
        ("CurrencyName", 'TRIM(CurrencyName)', str_col("CurrencyName", 60)),
        ("MinorUnitDigits", 'ISNULL(MinorUnitDigits) ? 2 : MinorUnitDigits', int_col("MinorUnitDigits")),
        ("CodeValue", 'UPPER(TRIM(CurrencyCode))', str_col("CodeValue", 60)),
        ("CodeDescription", 'TRIM(CurrencyName)', str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["CurrencyCode", "CurrencyName", "MinorUnitDigits"]),
         str_col("ChangeHash", 200)),
    ])
    cur.conditional_split("Screen Currency", [
        ("Valid Currency", 'LEN(CurrencyCode) == 3'),
    ], default_output="Malformed Currency Code")
    cur.row_count("Count Currencies Loaded", "User::RowsInserted")
    cur.oledb_destination("REF Currency Incoming", CONN_STAGING, "[ref].[Currency_Incoming]",
                          batch_size=5000)
    cur.branch_destination("ERR Currency Malformed", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                           "Screen Currency", "Malformed Currency Code")

    fx_cols = [
        str_col("FromCurrencyCode", 3), str_col("ToCurrencyCode", 3), dec_col("ConversionRate"),
        date_col("EffectiveFromDate"), date_col("EffectiveToDate"), str_col("RateSourceCode", 10),
    ]
    fx = DataFlow("DFT Load FX Rates", "Effective-dated FX rates with gap detection")
    fx.oledb_source(
        "STG FX Rate", CONN_STAGING,
        "SELECT FromCurrencyCode, ToCurrencyCode, ConversionRate, EffectiveFromDate,\n"
        "       EffectiveToDate, RateSourceCode\n"
        "FROM stg.FxRate\n"
        "WHERE EffectiveFromDate >= DATEADD(year, -3, CONVERT(date, GETDATE()));",
        fx_cols, timeout=3600)
    fx.sort("Sort FX By Pair And Effective Date",
            ["FromCurrencyCode", "ToCurrencyCode", "EffectiveFromDate"], eliminate_duplicates=True)
    fx.derived_column("Close FX Intervals", [
        ("EffectiveToDate",
         'ISNULL(EffectiveToDate) ? (DT_DBTIMESTAMP)"9999-12-31" : EffectiveToDate',
         date_col("EffectiveToDate")),
        ("InverseRate",
         'ConversionRate == 0 ? (DT_NUMERIC,18,6)0 : (DT_NUMERIC,18,6)(1.0 / ConversionRate)',
         dec_col("InverseRate")),
        ("RateSourceCode", 'UPPER(TRIM(ISNULL(RateSourceCode) ? "ECB" : RateSourceCode))',
         str_col("RateSourceCode", 10)),
        ("CurrencyPairCode",
         'UPPER(TRIM(FromCurrencyCode)) + UPPER(TRIM(ToCurrencyCode))', str_col("CurrencyPairCode", 6)),
    ])
    fx.conditional_split("Screen FX Rate", [
        ("Usable Rate", 'ConversionRate > 0 && EffectiveToDate > EffectiveFromDate'),
        ("Non Positive Rate", 'ConversionRate <= 0'),
    ], default_output="Inverted Interval")
    fx.row_count("Count FX Rows Loaded", "User::RowsUpdated")
    fx.oledb_destination("REF FxRate", CONN_STAGING, "[ref].[FxRate]", batch_size=50000)
    fx.branch_destination("ERR FX Non Positive", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                          "Screen FX Rate", "Non Positive Rate")
    fx.branch_destination("ERR FX Inverted Interval", CONN_STAGING,
                          "[err].[RejectedConstraintViolation]",
                          "Screen FX Rate", "Inverted Interval")

    dim = DataFlow("DFT Publish Currency Dimension", "Publish the current currency version to the DW")
    dim.oledb_source(
        "REF Currency Current", CONN_STAGING,
        "SELECT c.CurrencyCode, c.CurrencyName, c.CurrencySymbol, c.MinorUnitDigits, c.RegionCode,\n"
        "       c.VersionNumber, c.ValidFrom\n"
        "FROM ref.Currency AS c WHERE c.IsCurrent = 1;",
        [str_col("CurrencyCode", 3), str_col("CurrencyName", 60), str_col("CurrencySymbol", 5),
         int_col("MinorUnitDigits"), str_col("RegionCode", 4), int_col("VersionNumber"),
         date_col("ValidFrom")], timeout=1800)
    dim.derived_column("Shape Currency Dimension", [
        ("LineageKey", '(DT_WSTR,40)"REF_Load_Currency"', str_col("LineageKey", 40)),
    ])
    dim.row_count("Count Currency Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Currency", CONN_DW, "[Dimension].[Currency]", batch_size=5000)
    return build_reference_package(
        "REF_Load_Currency",
        "Load the conformed currency code set and the three-year effective-dated FX rate history, "
        "then publish the current currency version into Dimension.Currency. FX intervals are closed "
        "against the next rate for the pair and non-positive or inverted intervals are rejected.",
        ORA, "ref.Currency", [cur, fx, dim],
        truncate_tables=["[ref].[Currency_Incoming]"],
        post_tasks=[version_reference_set("ref.Currency", "CURRENCY")])


@package
def ref_load_payment_method():
    """stg.Payment -> ref.PaymentMethod and Dimension.Payment Method."""
    cols = [
        str_col("PaymentMethodCode", 12), str_col("SourcePaymentMethodCode", 12),
        str_col("PaymentMethodName", 60), str_col("RegionCode", 4), str_col("SettlementTypeCode", 10),
        int_col("UsageCount"),
    ]
    flow = DataFlow("DFT Load Payment Method", "Derive the payment method code set from observed usage")
    flow.oledb_source(
        "STG Payment Method Usage", CONN_STAGING,
        "SELECT p.PaymentMethodCode, p.SourcePaymentMethodCode,\n"
        "       ISNULL(x.TargetDescription, p.PaymentMethodCode) AS PaymentMethodName,\n"
        "       p.RegionCode, ISNULL(x.AttributeValue, N'UNKNOWN') AS SettlementTypeCode,\n"
        "       COUNT_BIG(*) AS UsageCount\n"
        "FROM stg.Payment AS p\n"
        "     LEFT OUTER JOIN ref.CodeCrosswalk AS x\n"
        "       ON x.CodeSetName = N'PAYMENT_METHOD' AND x.SourceCode = p.SourcePaymentMethodCode\n"
        "GROUP BY p.PaymentMethodCode, p.SourcePaymentMethodCode, x.TargetDescription, p.RegionCode,\n"
        "         x.AttributeValue;",
        cols, timeout=1800)
    flow.row_count("Count Methods Read", "User::RowsRead")
    flow.derived_column("Conform Payment Method", [
        ("PaymentMethodCode", 'UPPER(TRIM(PaymentMethodCode))', str_col("PaymentMethodCode", 12)),
        # NA settles by ACH and cheque, EU by SEPA, APAC largely by local RTGS.
        ("SettlementTypeCode",
         'SettlementTypeCode != "UNKNOWN" ? UPPER(TRIM(SettlementTypeCode)) : '
         '(RegionCode == "EU" ? "SEPA" : (RegionCode == "APAC" ? "RTGS" : "ACH"))',
         str_col("SettlementTypeCode", 10)),
        ("ElectronicFlag",
         'UPPER(TRIM(PaymentMethodCode)) == "CHQ" || UPPER(TRIM(PaymentMethodCode)) == "CASH" ? "N" : "Y"',
         str_col("ElectronicFlag", 1)),
        ("CodeValue", 'UPPER(TRIM(PaymentMethodCode))', str_col("CodeValue", 60)),
        ("CodeDescription", 'TRIM(PaymentMethodName)', str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["PaymentMethodCode", "PaymentMethodName", "SettlementTypeCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Payment Method", [
        ("Mapped Method", 'PaymentMethodCode != "" && PaymentMethodCode != "UNKNOWN"'),
    ], default_output="Unmapped Method")
    flow.row_count("Count Methods Loaded", "User::RowsInserted")
    flow.oledb_destination("REF PaymentMethod Incoming", CONN_STAGING,
                           "[ref].[PaymentMethod_Incoming]", batch_size=5000)
    flow.branch_destination("ERR Payment Method Unmapped", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Screen Payment Method", "Unmapped Method")

    dim = DataFlow("DFT Publish Payment Method Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF PaymentMethod Current", CONN_STAGING,
        "SELECT CodeValue AS PaymentMethodCode, CodeDescription AS PaymentMethodName, RegionCode,\n"
        "       VersionNumber, ValidFrom\n"
        "FROM ref.PaymentMethod WHERE IsCurrent = 1;",
        [str_col("PaymentMethodCode", 12), str_col("PaymentMethodName", 60), str_col("RegionCode", 4),
         int_col("VersionNumber"), date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Method Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Payment Method", CONN_DW, "[Dimension].[Payment Method]",
                          batch_size=5000)
    return build_reference_package(
        "REF_Load_PaymentMethod",
        "Rebuild the payment method code set from the methods actually observed in stg.Payment, "
        "defaulting the settlement type by region (SEPA in the EU, RTGS in APAC, ACH in NA) and "
        "reporting source methods that have no crosswalk entry.",
        ORA, "ref.PaymentMethod", [flow, dim],
        truncate_tables=["[ref].[PaymentMethod_Incoming]"],
        post_tasks=[version_reference_set("ref.PaymentMethod", "PAYMENT_METHOD"),
                    report_unmapped("PAYMENT_METHOD", "stg.Payment"),
                    log_unmapped_rejects("PAYMENT_METHOD")])


@package
def ref_load_transaction_type():
    """stg.StockMovement -> ref.TransactionType and Dimension.Transaction Type."""
    cols = [
        str_col("TransactionTypeCode", 12), str_col("TransactionTypeName", 60),
        str_col("MovementDirectionCode", 3), bool_col("AffectsInventory"), bool_col("AffectsLedger"),
        int_col("UsageCount"),
    ]
    flow = DataFlow("DFT Load Transaction Type", "Classify transaction types by their ledger effect")
    flow.oledb_source(
        "STG Transaction Type Usage", CONN_STAGING,
        "SELECT m.TransactionTypeCode,\n"
        "       ISNULL(x.TargetDescription, m.TransactionTypeCode) AS TransactionTypeName,\n"
        "       CASE WHEN SUM(m.SignedQuantity) >= 0 THEN N'IN' ELSE N'OUT' END AS MovementDirectionCode,\n"
        "       CAST(1 AS bit) AS AffectsInventory,\n"
        "       CAST(CASE WHEN m.TransactionTypeCode IN (N'SALE', N'PURCH') THEN 1 ELSE 0 END AS bit)\n"
        "           AS AffectsLedger,\n"
        "       COUNT_BIG(*) AS UsageCount\n"
        "FROM stg.StockMovement AS m\n"
        "     LEFT OUTER JOIN ref.CodeCrosswalk AS x\n"
        "       ON x.CodeSetName = N'TRANSACTION_TYPE' AND x.SourceCode = m.TransactionTypeCode\n"
        "GROUP BY m.TransactionTypeCode, x.TargetDescription;",
        cols, timeout=1800)
    flow.row_count("Count Transaction Types Read", "User::RowsRead")
    flow.derived_column("Conform Transaction Type", [
        ("TransactionTypeCode", 'UPPER(TRIM(TransactionTypeCode))', str_col("TransactionTypeCode", 12)),
        ("MovementSign", 'MovementDirectionCode == "OUT" ? -1 : 1', int_col("MovementSign")),
        ("ReversalAllowedFlag",
         'UPPER(TRIM(TransactionTypeCode)) == "ADJ" || UPPER(TRIM(TransactionTypeCode)) == "RET" ? "Y" : "N"',
         str_col("ReversalAllowedFlag", 1)),
        ("CodeValue", 'UPPER(TRIM(TransactionTypeCode))', str_col("CodeValue", 60)),
        ("CodeDescription", 'TRIM(TransactionTypeName)', str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["TransactionTypeCode", "TransactionTypeName",
                                        "MovementDirectionCode"]), str_col("ChangeHash", 200)),
    ])
    flow.aggregate("Summarize Type Usage", ["TransactionTypeCode", "MovementDirectionCode"], [
        ("UsageCount", "TotalUsageCount", "Sum"),
    ])
    flow.conditional_split("Screen Transaction Type", [
        ("In Use", 'TotalUsageCount > 0'),
    ], default_output="Obsolete Type")
    flow.row_count("Count Transaction Types Loaded", "User::RowsInserted")
    flow.oledb_destination("REF TransactionType Incoming", CONN_STAGING,
                           "[ref].[TransactionType_Incoming]", batch_size=5000)
    flow.branch_destination("ERR Transaction Type Obsolete", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Transaction Type", "Obsolete Type")

    dim = DataFlow("DFT Publish Transaction Type Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF TransactionType Current", CONN_STAGING,
        "SELECT CodeValue AS TransactionTypeCode, CodeDescription AS TransactionTypeName,\n"
        "       VersionNumber, ValidFrom\n"
        "FROM ref.TransactionType WHERE IsCurrent = 1;",
        [str_col("TransactionTypeCode", 12), str_col("TransactionTypeName", 60),
         int_col("VersionNumber"), date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Type Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Transaction Type", CONN_DW, "[Dimension].[Transaction Type]",
                          batch_size=5000)
    return build_reference_package(
        "REF_Load_TransactionType",
        "Rebuild the inventory transaction type code set from observed stock movements, deriving "
        "the movement direction from the net signed quantity and flagging the types that also post "
        "to the general ledger. Types no longer in use are retired rather than deleted.",
        OLTP, "ref.TransactionType", [flow, dim],
        truncate_tables=["[ref].[TransactionType_Incoming]"],
        post_tasks=[version_reference_set("ref.TransactionType", "TRANSACTION_TYPE"),
                    report_unmapped("TRANSACTION_TYPE", "stg.StockMovement"),
                    log_unmapped_rejects("TRANSACTION_TYPE")])


@package
def ref_load_payment_terms():
    """stg.Supplier + stg.ApInvoice -> ref.PaymentTerms and Dimension.Payment Terms."""
    cols = [
        str_col("PaymentTermsCode", 10), str_col("PaymentTermsName", 60), int_col("NetDays"),
        int_col("DiscountDays"), dec_col("DiscountPercent", 9, 4), str_col("RegionCode", 4),
    ]
    flow = DataFlow("DFT Load Payment Terms", "Parse the legacy free-text terms into day counts")
    flow.oledb_source(
        "STG Payment Terms", CONN_STAGING,
        "SELECT DISTINCT s.PaymentTermsCode, ISNULL(t.PaymentTermsName, s.PaymentTermsCode) AS "
        "PaymentTermsName,\n"
        "       ISNULL(t.NetDays, 30) AS NetDays, ISNULL(t.DiscountDays, 0) AS DiscountDays,\n"
        "       ISNULL(t.DiscountPercent, 0) AS DiscountPercent, s.RegionCode\n"
        "FROM stg.Supplier AS s\n"
        "     LEFT OUTER JOIN stg.PaymentTermsRaw AS t ON t.PaymentTermsCode = s.PaymentTermsCode;",
        cols, timeout=1800)
    flow.row_count("Count Terms Read", "User::RowsRead")
    flow.derived_column("Conform Payment Terms", [
        ("PaymentTermsCode", 'UPPER(TRIM(PaymentTermsCode))', str_col("PaymentTermsCode", 10)),
        # The EU late payment directive caps supplier terms at sixty days; APAC
        # trading houses habitually run ninety and NA runs the classic 2/10 net 30.
        ("NetDays",
         'RegionCode == "EU" && NetDays > 60 ? 60 : (RegionCode == "APAC" && NetDays > 90 ? 90 : NetDays)',
         int_col("NetDays")),
        ("DiscountDays", 'DiscountDays > NetDays ? 0 : DiscountDays', int_col("DiscountDays")),
        ("DiscountPercent",
         'DiscountPercent > 10 ? (DT_NUMERIC,9,4)0 : DiscountPercent', dec_col("DiscountPercent", 9, 4)),
        ("EarlySettlementFlag", 'DiscountDays > 0 && DiscountPercent > 0 ? "Y" : "N"',
         str_col("EarlySettlementFlag", 1)),
        ("CodeValue", 'UPPER(TRIM(PaymentTermsCode))', str_col("CodeValue", 60)),
        ("CodeDescription", 'TRIM(PaymentTermsName)', str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["PaymentTermsCode", "NetDays", "DiscountDays",
                                        "DiscountPercent"]), str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Payment Terms", [
        ("Valid Terms", 'NetDays > 0 && NetDays <= 365'),
    ], default_output="Implausible Terms")
    flow.row_count("Count Terms Loaded", "User::RowsInserted")
    flow.oledb_destination("REF PaymentTerms Incoming", CONN_STAGING, "[ref].[PaymentTerms_Incoming]",
                           batch_size=5000)
    flow.branch_destination("ERR Payment Terms Implausible", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Payment Terms", "Implausible Terms")

    dim = DataFlow("DFT Publish Payment Terms Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF PaymentTerms Current", CONN_STAGING,
        "SELECT CodeValue AS PaymentTermsCode, CodeDescription AS PaymentTermsName, RegionCode,\n"
        "       VersionNumber, ValidFrom\n"
        "FROM ref.PaymentTerms WHERE IsCurrent = 1;",
        [str_col("PaymentTermsCode", 10), str_col("PaymentTermsName", 60), str_col("RegionCode", 4),
         int_col("VersionNumber"), date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Terms Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Payment Terms", CONN_DW, "[Dimension].[Payment Terms]",
                          batch_size=5000)
    return build_reference_package(
        "REF_Load_PaymentTerms",
        "Rebuild the payment terms code set from supplier master data, capping net days at the "
        "regional legal or customary maximum (60 in the EU, 90 in APAC) and discarding early "
        "settlement discounts that fall outside the plausible range.",
        ORA, "ref.PaymentTerms", [flow, dim],
        truncate_tables=["[ref].[PaymentTerms_Incoming]"],
        post_tasks=[version_reference_set("ref.PaymentTerms", "PAYMENT_TERMS")])


# ---------------------------------------------------------------------------
# commercial code sets
# ---------------------------------------------------------------------------


@package
def ref_load_return_reason():
    """stg.Return -> ref.ReturnReason and Dimension.Return Reason."""
    cols = [
        str_col("ReturnReasonCode", 12), str_col("ReturnReasonDescription", 80),
        str_col("RegionCode", 4), int_col("OccurrenceCount"),
    ]
    flow = DataFlow("DFT Load Return Reason", "Group return reasons into the conformed reason set")
    flow.oledb_source(
        "STG Return Reason Usage", CONN_STAGING,
        "SELECT r.ReturnReasonCode, MAX(r.ReturnReasonDescription) AS ReturnReasonDescription,\n"
        "       r.RegionCode, COUNT_BIG(*) AS OccurrenceCount\n"
        "FROM stg.[Return] AS r\n"
        "GROUP BY r.ReturnReasonCode, r.RegionCode;",
        cols, timeout=1800)
    flow.row_count("Count Return Reasons Read", "User::RowsRead")
    flow.derived_column("Classify Return Reason", [
        ("ReturnReasonCode", 'UPPER(TRIM(ReturnReasonCode))', str_col("ReturnReasonCode", 12)),
        ("ReasonCategoryCode",
         'UPPER(TRIM(ReturnReasonCode)) == "DAMAGED" || UPPER(TRIM(ReturnReasonCode)) == "FAULTY" '
         '? "QUALITY" : (UPPER(TRIM(ReturnReasonCode)) == "LATE" ? "SERVICE" : '
         '(UPPER(TRIM(ReturnReasonCode)) == "CHANGEDMIND" ? "CUSTOMER" : "OTHER"))',
         str_col("ReasonCategoryCode", 10)),
        ("SupplierRecoverableFlag",
         'UPPER(TRIM(ReturnReasonCode)) == "DAMAGED" || UPPER(TRIM(ReturnReasonCode)) == "FAULTY" '
         '? "Y" : "N"', str_col("SupplierRecoverableFlag", 1)),
        # EU distance selling means a change of mind is always refundable; NA and
        # APAC apply a restocking fee outside the goodwill window.
        ("RestockingFeePercent",
         'RegionCode == "EU" ? (DT_NUMERIC,9,4)0 : (UPPER(TRIM(ReturnReasonCode)) == "CHANGEDMIND" '
         '? (DT_NUMERIC,9,4)15 : (DT_NUMERIC,9,4)0)', dec_col("RestockingFeePercent", 9, 4)),
        ("CodeValue", 'UPPER(TRIM(ReturnReasonCode))', str_col("CodeValue", 60)),
        ("CodeDescription", 'TRIM(ReturnReasonDescription)', str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["ReturnReasonCode", "ReturnReasonDescription", "RegionCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Return Reason", [
        ("Described Reason", 'LEN(TRIM(CodeDescription)) > 0'),
    ], default_output="Undescribed Reason")
    flow.row_count("Count Return Reasons Loaded", "User::RowsInserted")
    flow.oledb_destination("REF ReturnReason Incoming", CONN_STAGING, "[ref].[ReturnReason_Incoming]",
                           batch_size=5000)
    flow.branch_destination("ERR Return Reason Undescribed", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Return Reason", "Undescribed Reason")

    dim = DataFlow("DFT Publish Return Reason Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF ReturnReason Current", CONN_STAGING,
        "SELECT CodeValue AS ReturnReasonCode, CodeDescription AS ReturnReasonName, RegionCode,\n"
        "       VersionNumber, ValidFrom\n"
        "FROM ref.ReturnReason WHERE IsCurrent = 1;",
        [str_col("ReturnReasonCode", 12), str_col("ReturnReasonName", 80), str_col("RegionCode", 4),
         int_col("VersionNumber"), date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Reason Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Return Reason", CONN_DW, "[Dimension].[Return Reason]",
                          batch_size=5000)
    return build_reference_package(
        "REF_Load_ReturnReason",
        "Rebuild the return reason code set from observed returns, grouping reasons into quality, "
        "service, customer and other categories, marking the supplier-recoverable ones and setting "
        "the restocking fee to zero for EU distance-selling returns.",
        OLTP, "ref.ReturnReason", [flow, dim],
        truncate_tables=["[ref].[ReturnReason_Incoming]"],
        post_tasks=[version_reference_set("ref.ReturnReason", "RETURN_REASON"),
                    report_unmapped("RETURN_REASON", "stg.Return")])


@package
def ref_load_loyalty_tier():
    """stg.LoyaltyLedger -> ref.LoyaltyTier and Dimension.Loyalty Tier."""
    cols = [
        str_col("LoyaltyTierCode", 3), str_col("ProgramCode", 10), str_col("RegionCode", 4),
        int_col("MemberCount"), bigint_col("TotalPoints"),
    ]
    flow = DataFlow("DFT Load Loyalty Tier", "Recompute tier thresholds from the member distribution")
    flow.oledb_source(
        "STG Loyalty Tier Distribution", CONN_STAGING,
        "SELECT l.LoyaltyTierCode, l.ProgramCode, l.RegionCode,\n"
        "       COUNT_BIG(DISTINCT l.CustomerId) AS MemberCount,\n"
        "       SUM(CAST(l.NetPointsBalance AS BIGINT)) AS TotalPoints\n"
        "FROM stg.LoyaltyLedger AS l\n"
        "GROUP BY l.LoyaltyTierCode, l.ProgramCode, l.RegionCode;",
        cols, timeout=1800)
    flow.row_count("Count Tiers Read", "User::RowsRead")
    flow.derived_column("Conform Loyalty Tier", [
        ("LoyaltyTierCode", 'UPPER(TRIM(LoyaltyTierCode))', str_col("LoyaltyTierCode", 3)),
        ("TierName",
         'UPPER(TRIM(LoyaltyTierCode)) == "PLT" ? "Platinum" : (UPPER(TRIM(LoyaltyTierCode)) == "GLD" '
         '? "Gold" : (UPPER(TRIM(LoyaltyTierCode)) == "SLV" ? "Silver" : "Bronze"))',
         str_col("TierName", 40)),
        ("TierRank",
         'UPPER(TRIM(LoyaltyTierCode)) == "PLT" ? 1 : (UPPER(TRIM(LoyaltyTierCode)) == "GLD" ? 2 : '
         '(UPPER(TRIM(LoyaltyTierCode)) == "SLV" ? 3 : 4))', int_col("TierRank")),
        # The APAC programme runs richer discounts than the legacy NA scheme and
        # the EU scheme is capped by the local promotions rules.
        ("DiscountPercent",
         'RegionCode == "APAC" ? (DT_NUMERIC,9,4)(12 - 2 * (UPPER(TRIM(LoyaltyTierCode)) == "PLT" ? 1 : '
         '(UPPER(TRIM(LoyaltyTierCode)) == "GLD" ? 2 : (UPPER(TRIM(LoyaltyTierCode)) == "SLV" ? 3 : 4)))) '
         ': (RegionCode == "EU" ? (DT_NUMERIC,9,4)5 : (DT_NUMERIC,9,4)(10 - 2 * '
         '(UPPER(TRIM(LoyaltyTierCode)) == "PLT" ? 1 : (UPPER(TRIM(LoyaltyTierCode)) == "GLD" ? 2 : '
         '(UPPER(TRIM(LoyaltyTierCode)) == "SLV" ? 3 : 4)))))', dec_col("DiscountPercent", 9, 4)),
        ("AveragePointsPerMember",
         'MemberCount == 0 ? (DT_NUMERIC,18,2)0 : (DT_NUMERIC,18,2)(TotalPoints / MemberCount)',
         money_col("AveragePointsPerMember")),
        ("CodeValue", 'UPPER(TRIM(LoyaltyTierCode))', str_col("CodeValue", 60)),
        ("CodeDescription",
         'UPPER(TRIM(LoyaltyTierCode)) == "PLT" ? "Platinum" : (UPPER(TRIM(LoyaltyTierCode)) == "GLD" '
         '? "Gold" : (UPPER(TRIM(LoyaltyTierCode)) == "SLV" ? "Silver" : "Bronze"))',
         str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["LoyaltyTierCode", "ProgramCode", "RegionCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Loyalty Tier", [
        ("Populated Tier", 'MemberCount > 0'),
    ], default_output="Empty Tier")
    flow.row_count("Count Tiers Loaded", "User::RowsInserted")
    flow.oledb_destination("REF LoyaltyTier Incoming", CONN_STAGING, "[ref].[LoyaltyTier_Incoming]",
                           batch_size=5000)
    flow.branch_destination("ERR Loyalty Tier Empty", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Loyalty Tier", "Empty Tier")

    dim = DataFlow("DFT Publish Loyalty Tier Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF LoyaltyTier Current", CONN_STAGING,
        "SELECT CodeValue AS LoyaltyTierCode, CodeDescription AS TierName, RegionCode,\n"
        "       VersionNumber, ValidFrom\n"
        "FROM ref.LoyaltyTier WHERE IsCurrent = 1;",
        [str_col("LoyaltyTierCode", 3), str_col("TierName", 40), str_col("RegionCode", 4),
         int_col("VersionNumber"), date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Tier Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Loyalty Tier", CONN_DW, "[Dimension].[Loyalty Tier]",
                          batch_size=5000)
    return build_reference_package(
        "REF_Load_LoyaltyTier",
        "Rebuild the loyalty tier reference set from the staged member balances, ranking the tiers "
        "and applying the regional discount schedule: richer in APAC, capped at five percent in the "
        "EU by local promotion rules and the legacy schedule in NA.",
        OLTP, "ref.LoyaltyTier", [flow, dim],
        truncate_tables=["[ref].[LoyaltyTier_Incoming]"],
        post_tasks=[version_reference_set("ref.LoyaltyTier", "LOYALTY_TIER")])


@package
def ref_load_sales_channel():
    """stg.Order + stg.WebSession + stg.PartnerSale -> ref.SalesChannel."""
    order_cols = [str_col("ChannelCode", 10), str_col("ChannelName", 60), int_col("OrderCount")]
    orders = DataFlow("DFT Channel From Orders", "Direct and telesales channels observed on orders")
    orders.oledb_source(
        "STG Order Channels", CONN_STAGING,
        "SELECT o.ChannelCode, MAX(o.ChannelName) AS ChannelName, COUNT_BIG(*) AS OrderCount\n"
        "FROM stg.[Order] AS o GROUP BY o.ChannelCode;",
        order_cols, timeout=1800)
    orders.row_count("Count Order Channels", "User::RowsRead")
    orders.derived_column("Shape Order Channel", [
        ("ChannelCode", 'UPPER(TRIM(ISNULL(ChannelCode) ? "DIRECT" : ChannelCode))',
         str_col("ChannelCode", 10)),
        ("ChannelGroupCode", '"DIRECT"', str_col("ChannelGroupCode", 10)),
        ("DigitalFlag", '"N"', str_col("DigitalFlag", 1)),
    ])
    orders.union_all("Union Channel Feeds")
    orders.aggregate("Aggregate Channel Usage", ["ChannelCode", "ChannelGroupCode", "DigitalFlag"], [
        ("OrderCount", "TotalUsageCount", "Sum"),
    ])
    orders.derived_column("Conform Sales Channel", [
        ("CodeValue", 'UPPER(TRIM(ChannelCode))', str_col("CodeValue", 60)),
        ("CodeDescription", 'UPPER(TRIM(ChannelCode)) + " channel"', str_col("CodeDescription", 200)),
        ("RegionCode", '"ALL"', str_col("RegionCode", 4)),
        ("ChangeHash", hash_expression(["ChannelCode", "ChannelGroupCode", "DigitalFlag"]),
         str_col("ChangeHash", 200)),
    ])
    orders.conditional_split("Screen Sales Channel", [
        ("Active Channel", 'TotalUsageCount > 0'),
    ], default_output="Dormant Channel")
    orders.row_count("Count Channels Loaded", "User::RowsInserted")
    orders.oledb_destination("REF SalesChannel Incoming", CONN_STAGING,
                             "[ref].[SalesChannel_Incoming]", batch_size=5000)
    orders.branch_destination("ERR Sales Channel Dormant", CONN_STAGING,
                              "[err].[RejectedConstraintViolation]",
                              "Screen Sales Channel", "Dormant Channel")

    web = DataFlow("DFT Channel From Web And Partner", "Digital and partner channels")
    web.oledb_source(
        "STG Web And Partner Channels", CONN_STAGING,
        "SELECT ChannelCode, COUNT_BIG(*) AS SessionCount FROM stg.WebSession GROUP BY ChannelCode\n"
        "UNION ALL\n"
        "SELECT N'PARTNER' AS ChannelCode, COUNT_BIG(*) AS SessionCount FROM stg.PartnerSale;",
        [str_col("ChannelCode", 10), bigint_col("SessionCount")], timeout=1800)
    web.derived_column("Shape Digital Channel", [
        ("ChannelCode", 'UPPER(TRIM(ChannelCode))', str_col("ChannelCode", 10)),
        ("ChannelGroupCode", 'UPPER(TRIM(ChannelCode)) == "PARTNER" ? "PARTNER" : "DIGITAL"',
         str_col("ChannelGroupCode", 10)),
        ("DigitalFlag", 'UPPER(TRIM(ChannelCode)) == "PARTNER" ? "N" : "Y"', str_col("DigitalFlag", 1)),
    ])
    web.row_count("Count Digital Channels", "User::RowsUpdated")
    web.oledb_destination("REF SalesChannel Feed", CONN_STAGING, "[ref].[SalesChannel_Feed]",
                          batch_size=5000)

    dim = DataFlow("DFT Publish Sales Channel Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF SalesChannel Current", CONN_STAGING,
        "SELECT CodeValue AS ChannelCode, CodeDescription AS ChannelName, VersionNumber, ValidFrom\n"
        "FROM ref.SalesChannel WHERE IsCurrent = 1;",
        [str_col("ChannelCode", 10), str_col("ChannelName", 60), int_col("VersionNumber"),
         date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Channel Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Sales Channel", CONN_DW, "[Dimension].[Sales Channel]",
                          batch_size=5000)
    return build_reference_package(
        "REF_Load_SalesChannel",
        "Rebuild the sales channel code set by unioning the channels observed on orders, web "
        "sessions and the partner file feed, grouping them into direct, digital and partner "
        "families and retiring channels with no activity in the window.",
        OLTP, "ref.SalesChannel", [web, orders, dim],
        truncate_tables=["[ref].[SalesChannel_Incoming]", "[ref].[SalesChannel_Feed]"],
        post_tasks=[version_reference_set("ref.SalesChannel", "SALES_CHANNEL")])


@package
def ref_load_carrier():
    """stg.Shipment -> ref.Carrier and Dimension.Carrier."""
    cols = [
        str_col("CarrierCode", 10), str_col("CarrierName", 60), str_col("ServiceLevelCode", 10),
        str_col("RegionCode", 4), int_col("ShipmentCount"), dec_col("AverageTransitDays", 9, 2),
    ]
    flow = DataFlow("DFT Load Carrier", "Carrier service levels derived from observed transit times")
    flow.oledb_source(
        "STG Carrier Usage", CONN_STAGING,
        "SELECT s.CarrierCode, MAX(s.CarrierName) AS CarrierName, s.ServiceLevelCode, s.RegionCode,\n"
        "       COUNT_BIG(*) AS ShipmentCount,\n"
        "       CAST(AVG(CAST(s.TransitDays AS DECIMAL(9,2))) AS DECIMAL(9,2)) AS AverageTransitDays\n"
        "FROM stg.Shipment AS s\n"
        "WHERE s.DeliveredFlag = N'Y'\n"
        "GROUP BY s.CarrierCode, s.ServiceLevelCode, s.RegionCode;",
        cols, timeout=1800)
    flow.row_count("Count Carriers Read", "User::RowsRead")
    flow.derived_column("Conform Carrier", [
        ("CarrierCode", 'UPPER(TRIM(CarrierCode))', str_col("CarrierCode", 10)),
        ("ServiceLevelCode",
         'ISNULL(ServiceLevelCode) || TRIM(ServiceLevelCode) == "" ? '
         '(AverageTransitDays <= 1 ? "NEXTDAY" : (AverageTransitDays <= 3 ? "EXPRESS" : "STANDARD")) '
         ': UPPER(TRIM(ServiceLevelCode))', str_col("ServiceLevelCode", 10)),
        ("CrossBorderFlag", 'RegionCode == "ALL" ? "Y" : "N"', str_col("CrossBorderFlag", 1)),
        ("OnTimeTargetDays",
         'RegionCode == "APAC" ? (DT_NUMERIC,9,2)5 : (RegionCode == "EU" ? (DT_NUMERIC,9,2)3 '
         ': (DT_NUMERIC,9,2)4)', dec_col("OnTimeTargetDays", 9, 2)),
        ("CodeValue", 'UPPER(TRIM(CarrierCode))', str_col("CodeValue", 60)),
        ("CodeDescription", 'TRIM(CarrierName)', str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["CarrierCode", "CarrierName", "ServiceLevelCode", "RegionCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Carrier", [
        ("Performing Carrier", 'AverageTransitDays <= OnTimeTargetDays * 3'),
    ], default_output="Transit Time Outlier")
    flow.row_count("Count Carriers Loaded", "User::RowsInserted")
    flow.oledb_destination("REF Carrier Incoming", CONN_STAGING, "[ref].[Carrier_Incoming]",
                           batch_size=5000)
    flow.branch_destination("ERR Carrier Transit Outlier", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Carrier", "Transit Time Outlier")

    dim = DataFlow("DFT Publish Carrier Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF Carrier Current", CONN_STAGING,
        "SELECT CodeValue AS CarrierCode, CodeDescription AS CarrierName, RegionCode,\n"
        "       VersionNumber, ValidFrom\n"
        "FROM ref.Carrier WHERE IsCurrent = 1;",
        [str_col("CarrierCode", 10), str_col("CarrierName", 60), str_col("RegionCode", 4),
         int_col("VersionNumber"), date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Carrier Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Carrier", CONN_DW, "[Dimension].[Carrier]", batch_size=5000)
    return build_reference_package(
        "REF_Load_Carrier",
        "Rebuild the carrier reference set from delivered shipments, inferring a missing service "
        "level from the observed average transit time and holding the regional on-time targets "
        "(three days in the EU, four in NA, five across APAC).",
        OLTP, "ref.Carrier", [flow, dim],
        truncate_tables=["[ref].[Carrier_Incoming]"],
        post_tasks=[version_reference_set("ref.Carrier", "CARRIER"),
                    report_unmapped("CARRIER", "stg.Shipment")])


@package
def ref_load_warehouse_site():
    """stg.StockMovement + work.InventoryPositionDaily -> ref.WarehouseSite."""
    cols = [
        int_col("WarehouseSiteId"), str_col("WarehouseSiteCode", 10), str_col("WarehouseSiteName", 60),
        str_col("CountryCode", 3), str_col("RegionCode", 4), str_col("PostalCode", 16),
        int_col("MovementCount"),
    ]
    flow = DataFlow("DFT Load Warehouse Site", "Site master with regional postal standardisation")
    flow.oledb_source(
        "STG Warehouse Site Usage", CONN_STAGING,
        "SELECT m.WarehouseSiteId, MAX(m.WarehouseSiteCode) AS WarehouseSiteCode,\n"
        "       MAX(m.WarehouseSiteName) AS WarehouseSiteName, MAX(m.CountryCode) AS CountryCode,\n"
        "       MAX(m.RegionCode) AS RegionCode, MAX(m.PostalCode) AS PostalCode,\n"
        "       COUNT_BIG(*) AS MovementCount\n"
        "FROM stg.StockMovement AS m\n"
        "GROUP BY m.WarehouseSiteId;",
        cols, timeout=1800)
    flow.row_count("Count Sites Read", "User::RowsRead")
    flow.derived_column("Standardize Site", [
        ("WarehouseSiteCode", 'UPPER(TRIM(WarehouseSiteCode))', str_col("WarehouseSiteCode", 10)),
        ("CountryCode", 'UPPER(TRIM(ISNULL(CountryCode) ? "USA" : CountryCode))', str_col("CountryCode", 3)),
        # Each region prints its postal codes differently and the warehouse master
        # was keyed by hand for twenty years.
        ("PostalCodeStandardized",
         'UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode)) == "NA" ? '
         'LEFT(REPLACE(TRIM(ISNULL(PostalCode) ? "00000" : PostalCode), " ", ""), 5) : '
         '(UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode)) == "EU" ? UPPER(TRIM(CountryCode)) + "-" + '
         'REPLACE(TRIM(ISNULL(PostalCode) ? "0000" : PostalCode), " ", "") : '
         'REPLACE(REPLACE(TRIM(ISNULL(PostalCode) ? "0000" : PostalCode), "-", ""), " ", ""))',
         str_col("PostalCodeStandardized", 16)),
        ("SiteTypeCode",
         'MovementCount > 100000 ? "DC" : (MovementCount > 10000 ? "REGIONAL" : "SATELLITE")',
         str_col("SiteTypeCode", 10)),
        ("CodeValue", 'UPPER(TRIM(WarehouseSiteCode))', str_col("CodeValue", 60)),
        ("CodeDescription", 'TRIM(WarehouseSiteName)', str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["WarehouseSiteCode", "WarehouseSiteName", "CountryCode",
                                        "PostalCode"]), str_col("ChangeHash", 200)),
    ])
    flow.lookup(
        "Lookup Site Country (Full Cache)", CONN_STAGING,
        "SELECT CountryCode, CountryName, RegionCode AS ReferenceRegionCode FROM ref.Country;",
        ["CountryCode"], [str_col("CountryName", 60), str_col("ReferenceRegionCode", 4)], no_match="RD")
    flow.row_count("Count Sites Loaded", "User::RowsInserted")
    flow.oledb_destination("REF WarehouseSite Incoming", CONN_STAGING,
                           "[ref].[WarehouseSite_Incoming]", batch_size=5000)
    flow.reject_destination("ERR Site Unknown Country", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Site Country (Full Cache)", "Lookup No Match Output")

    dim = DataFlow("DFT Publish Warehouse Site Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF WarehouseSite Current", CONN_STAGING,
        "SELECT CodeValue AS WarehouseSiteCode, CodeDescription AS WarehouseSiteName, RegionCode,\n"
        "       VersionNumber, ValidFrom\n"
        "FROM ref.WarehouseSite WHERE IsCurrent = 1;",
        [str_col("WarehouseSiteCode", 10), str_col("WarehouseSiteName", 60), str_col("RegionCode", 4),
         int_col("VersionNumber"), date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Site Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Warehouse Site", CONN_DW, "[Dimension].[Warehouse Site]",
                          batch_size=5000)
    return build_reference_package(
        "REF_Load_WarehouseSite",
        "Rebuild the warehouse site reference set from observed stock movements, standardising the "
        "site postal code by regional convention and classifying sites as distribution centres, "
        "regional sites or satellites from their movement volume.",
        OLTP, "ref.WarehouseSite", [flow, dim],
        truncate_tables=["[ref].[WarehouseSite_Incoming]"],
        post_tasks=[version_reference_set("ref.WarehouseSite", "WAREHOUSE_SITE")])


@package
def ref_load_cost_center():
    """stg.CostCenter -> ref.CostCenter with the hierarchy rebuilt row by row."""
    cols = [
        str_col("CostCenterCode", 12), str_col("CostCenterName", 60), str_col("ParentCostCenterCode", 12),
        str_col("CompanyCode", 6), str_col("RegionCode", 4), bool_col("IsActive"),
    ]
    flow = DataFlow("DFT Load Cost Center", "Cost centre master with company and region attribution")
    flow.oledb_source(
        "STG Cost Center", CONN_STAGING,
        "SELECT CostCenterCode, CostCenterName, ParentCostCenterCode, CompanyCode, RegionCode, IsActive\n"
        "FROM stg.CostCenter;",
        cols, timeout=1800)
    flow.row_count("Count Cost Centers Read", "User::RowsRead")
    flow.derived_column("Conform Cost Center", [
        ("CostCenterCode", 'UPPER(TRIM(CostCenterCode))', str_col("CostCenterCode", 12)),
        ("ParentCostCenterCode",
         'ISNULL(ParentCostCenterCode) || TRIM(ParentCostCenterCode) == "" ? "ROOT" '
         ': UPPER(TRIM(ParentCostCenterCode))', str_col("ParentCostCenterCode", 12)),
        ("CompanyCode", 'UPPER(TRIM(ISNULL(CompanyCode) ? "0001" : CompanyCode))', str_col("CompanyCode", 6)),
        # The Oracle chart of accounts encodes the function in the second segment.
        ("FunctionCode",
         'LEN(UPPER(TRIM(CostCenterCode))) >= 4 ? SUBSTRING(UPPER(TRIM(CostCenterCode)), 3, 2) : "XX"',
         str_col("FunctionCode", 2)),
        ("CodeValue", 'UPPER(TRIM(CostCenterCode))', str_col("CodeValue", 60)),
        ("CodeDescription", 'TRIM(CostCenterName)', str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["CostCenterCode", "CostCenterName", "ParentCostCenterCode",
                                        "CompanyCode"]), str_col("ChangeHash", 200)),
    ])
    flow.sort("Sort Cost Centers By Hierarchy", ["ParentCostCenterCode", "CostCenterCode"])
    flow.conditional_split("Screen Cost Center", [
        ("Active Cost Center", 'IsActive == TRUE && CostCenterCode != ParentCostCenterCode'),
        ("Self Referencing", 'CostCenterCode == ParentCostCenterCode'),
    ], default_output="Inactive Cost Center")
    flow.row_count("Count Cost Centers Loaded", "User::RowsInserted")
    flow.oledb_destination("REF CostCenter Incoming", CONN_STAGING, "[ref].[CostCenter_Incoming]",
                           batch_size=5000)
    flow.branch_destination("ERR Cost Center Self Reference", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Cost Center", "Self Referencing")
    flow.branch_destination("REF CostCenter Retired", CONN_STAGING, "[ref].[CostCenter_Incoming]",
                            "Screen Cost Center", "Inactive Cost Center")

    dim = DataFlow("DFT Publish Cost Center Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF CostCenter Current", CONN_STAGING,
        "SELECT CodeValue AS CostCenterCode, CodeDescription AS CostCenterName, RegionCode,\n"
        "       VersionNumber, ValidFrom\n"
        "FROM ref.CostCenter WHERE IsCurrent = 1;",
        [str_col("CostCenterCode", 12), str_col("CostCenterName", 60), str_col("RegionCode", 4),
         int_col("VersionNumber"), date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Cost Center Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Cost Center", CONN_DW, "[Dimension].[Cost Center]",
                          batch_size=5000)
    return build_reference_package(
        "REF_Load_CostCenter",
        "Rebuild the cost centre reference set from the Oracle chart of accounts, rooting orphaned "
        "nodes, decoding the function segment out of the code and rejecting self-referencing "
        "hierarchy rows that would otherwise loop the rollup procedure.",
        ORA, "ref.CostCenter", [flow, dim],
        truncate_tables=["[ref].[CostCenter_Incoming]"],
        post_tasks=[version_reference_set("ref.CostCenter", "COST_CENTER"),
                    exec_proc("Rebuild Cost Center Hierarchy",
                              "EXEC ref.usp_RebuildCostCenterHierarchy @BatchId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])])


@package
def ref_load_geography():
    """stg.Geography + stg.CustomerAddress -> ref.Geography and Dimension.Geography."""
    cols = [
        str_col("CountryCode", 3), str_col("CountryName", 60), str_col("StateProvinceCode", 6),
        str_col("StateProvinceName", 60), str_col("CityName", 60), str_col("PostalCode", 16),
        str_col("RegionCode", 4), str_col("SubRegionName", 60),
    ]
    flow = DataFlow("DFT Load Geography", "Region-specific geography and postal standardisation")
    flow.oledb_source(
        "STG Geography", CONN_STAGING,
        "SELECT g.CountryCode, g.CountryName, g.StateProvinceCode, g.StateProvinceName, g.CityName,\n"
        "       g.PostalCode, g.RegionCode, g.SubRegionName\n"
        "FROM stg.Geography AS g;",
        cols, timeout=3600)
    flow.row_count("Count Geography Rows Read", "User::RowsRead")
    flow.derived_column("Standardize Geography", [
        ("CountryCode", 'UPPER(TRIM(CountryCode))', str_col("CountryCode", 3)),
        ("CityNameStandardized", 'UPPER(TRIM(CityName))', str_col("CityNameStandardized", 60)),
        # NA keeps the five digit ZIP, the EU prefixes the country, APAC strips
        # separators entirely - three different conventions, three code paths.
        ("PostalCodeStandardized",
         'UPPER(TRIM(RegionCode)) == "NA" ? LEFT(REPLACE(TRIM(PostalCode), " ", ""), 5) : '
         '(UPPER(TRIM(RegionCode)) == "EU" ? UPPER(TRIM(CountryCode)) + "-" + '
         'REPLACE(TRIM(PostalCode), " ", "") : REPLACE(REPLACE(TRIM(PostalCode), "-", ""), " ", ""))',
         str_col("PostalCodeStandardized", 16)),
        ("PostalPrefix", 'LEFT(REPLACE(TRIM(PostalCode), " ", ""), 3)', str_col("PostalPrefix", 3)),
        ("GeographyKeyText",
         'UPPER(TRIM(CountryCode)) + "|" + UPPER(TRIM(ISNULL(StateProvinceCode) ? "XX" : StateProvinceCode)) '
         '+ "|" + UPPER(TRIM(CityName))', str_col("GeographyKeyText", 130)),
        ("CodeValue",
         'UPPER(TRIM(CountryCode)) + "|" + UPPER(TRIM(ISNULL(StateProvinceCode) ? "XX" : StateProvinceCode))',
         str_col("CodeValue", 60)),
        ("CodeDescription", 'TRIM(CountryName) + " / " + TRIM(ISNULL(StateProvinceName) ? "" : StateProvinceName)',
         str_col("CodeDescription", 200)),
        ("ChangeHash", hash_expression(["CountryCode", "StateProvinceCode", "CityName", "PostalCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.sort("Sort Geography For Dedup", ["GeographyKeyText", "PostalCodeStandardized"],
              eliminate_duplicates=True)
    flow.lookup(
        "Lookup Country Reference (Full Cache)", CONN_STAGING,
        "SELECT CountryCode, IsoNumericCode, ContinentName FROM ref.Country;",
        ["CountryCode"], [str_col("IsoNumericCode", 3), str_col("ContinentName", 30)], no_match="RD")
    flow.conditional_split("Screen Geography", [
        ("Complete Geography", 'LEN(CityNameStandardized) > 0 && LEN(PostalCodeStandardized) > 2'),
    ], default_output="Incomplete Geography")
    flow.row_count("Count Geography Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("REF Geography Incoming", CONN_STAGING, "[ref].[Geography_Incoming]",
                           batch_size=50000)
    flow.branch_destination("ERR Geography Incomplete", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Geography", "Incomplete Geography")
    flow.reject_destination("ERR Geography Unknown Country", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Lookup Country Reference (Full Cache)", "Lookup No Match Output")

    dim = DataFlow("DFT Publish Geography Dimension", "Publish the current version to the DW")
    dim.oledb_source(
        "REF Geography Current", CONN_STAGING,
        "SELECT CodeValue AS GeographyCode, CodeDescription AS GeographyName, RegionCode,\n"
        "       VersionNumber, ValidFrom\n"
        "FROM ref.Geography WHERE IsCurrent = 1;",
        [str_col("GeographyCode", 60), str_col("GeographyName", 200), str_col("RegionCode", 4),
         int_col("VersionNumber"), date_col("ValidFrom")], timeout=1800)
    dim.row_count("Count Geography Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Geography", CONN_DW, "[Dimension].[Geography]", batch_size=50000)
    return build_reference_package(
        "REF_Load_Geography",
        "Rebuild the conformed geography reference set. Postal codes are standardised three "
        "different ways depending on the region, duplicate country/state/city combinations are "
        "collapsed on sort and rows without a usable city or postal code are rejected.",
        OLTP, "ref.Geography", [flow, dim],
        truncate_tables=["[ref].[Geography_Incoming]"],
        post_tasks=[version_reference_set("ref.Geography", "GEOGRAPHY")])


@package
def ref_load_date_dimension():
    """Generate ref.Calendar and Dimension.Date with the three regional fiscal calendars."""
    cols = [
        date_col("CalendarDate"), int_col("CalendarYear"), int_col("CalendarMonth"),
        int_col("CalendarDay"), int_col("DayOfWeekNumber"), str_col("MonthName", 12),
        str_col("DayName", 12),
    ]
    flow = DataFlow("DFT Generate Calendar", "Derive the regional fiscal attributes for each date")
    flow.oledb_source(
        "STG Calendar Spine", CONN_STAGING,
        "SELECT CalendarDate, YEAR(CalendarDate) AS CalendarYear, MONTH(CalendarDate) AS CalendarMonth,\n"
        "       DAY(CalendarDate) AS CalendarDay, DATEPART(weekday, CalendarDate) AS DayOfWeekNumber,\n"
        "       DATENAME(month, CalendarDate) AS MonthName, DATENAME(weekday, CalendarDate) AS DayName\n"
        "FROM stg.CalendarSpine\n"
        "WHERE CalendarDate BETWEEN CONVERT(date, N'2005-01-01') AND CONVERT(date, N'2035-12-31');",
        cols, timeout=1800)
    flow.row_count("Count Calendar Rows Read", "User::RowsRead")
    flow.derived_column("Derive Fiscal Calendars", [
        # NA runs a July fiscal year, EU the calendar year and APAC an April year.
        ("FiscalYearNa", 'CalendarMonth >= 7 ? CalendarYear + 1 : CalendarYear', int_col("FiscalYearNa")),
        ("FiscalPeriodNa", 'CalendarMonth >= 7 ? CalendarMonth - 6 : CalendarMonth + 6',
         int_col("FiscalPeriodNa")),
        ("FiscalYearEu", 'CalendarYear', int_col("FiscalYearEu")),
        ("FiscalPeriodEu", 'CalendarMonth', int_col("FiscalPeriodEu")),
        ("FiscalYearApac", 'CalendarMonth >= 4 ? CalendarYear : CalendarYear - 1',
         int_col("FiscalYearApac")),
        ("FiscalPeriodApac", 'CalendarMonth >= 4 ? CalendarMonth - 3 : CalendarMonth + 9',
         int_col("FiscalPeriodApac")),
        ("CalendarQuarter", '((CalendarMonth - 1) / 3) + 1', int_col("CalendarQuarter")),
        ("WeekendFlag", 'DayOfWeekNumber == 1 || DayOfWeekNumber == 7 ? "Y" : "N"',
         str_col("WeekendFlag", 1)),
        ("DateKey", '(CalendarYear * 10000) + (CalendarMonth * 100) + CalendarDay', int_col("DateKey")),
        ("IsoWeekText",
         '(DT_WSTR,4)CalendarYear + "-W" + RIGHT("0" + (DT_WSTR,2)((CalendarMonth * 4)), 2)',
         str_col("IsoWeekText", 8)),
    ])
    flow.lookup(
        "Lookup Public Holiday (Full Cache)", CONN_STAGING,
        "SELECT HolidayDate AS CalendarDate, HolidayName, RegionCode AS HolidayRegionCode\n"
        "FROM ref.PublicHoliday;",
        ["CalendarDate"], [str_col("HolidayName", 60), str_col("HolidayRegionCode", 4)], no_match="IG")
    flow.derived_column("Derive Working Day", [
        ("HolidayFlag", 'ISNULL(HolidayName) ? "N" : "Y"', str_col("HolidayFlag", 1)),
        ("WorkingDayFlag", 'WeekendFlag == "N" && ISNULL(HolidayName) ? "Y" : "N"',
         str_col("WorkingDayFlag", 1)),
    ])
    flow.conditional_split("Screen Calendar", [
        ("Valid Date", 'CalendarYear >= 2005 && CalendarYear <= 2035'),
    ], default_output="Out Of Range Date")
    flow.row_count("Count Calendar Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("REF Calendar", CONN_STAGING, "[ref].[Calendar]", batch_size=50000)
    flow.branch_destination("ERR Calendar Out Of Range", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Calendar", "Out Of Range Date")

    dim = DataFlow("DFT Publish Date Dimension", "Publish the generated calendar to the DW")
    dim.oledb_source(
        "REF Calendar For Dimension", CONN_STAGING,
        "SELECT DateKey, CalendarDate, CalendarYear, CalendarMonth, CalendarQuarter, MonthName,\n"
        "       DayName, FiscalYearNa, FiscalPeriodNa, FiscalYearEu, FiscalPeriodEu, FiscalYearApac,\n"
        "       FiscalPeriodApac, WorkingDayFlag\n"
        "FROM ref.Calendar;",
        [int_col("DateKey"), date_col("CalendarDate"), int_col("CalendarYear"), int_col("CalendarMonth"),
         int_col("CalendarQuarter"), str_col("MonthName", 12), str_col("DayName", 12),
         int_col("FiscalYearNa"), int_col("FiscalPeriodNa"), int_col("FiscalYearEu"),
         int_col("FiscalPeriodEu"), int_col("FiscalYearApac"), int_col("FiscalPeriodApac"),
         str_col("WorkingDayFlag", 1)], timeout=1800)
    dim.row_count("Count Date Dimension Rows", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Date", CONN_DW, "[Dimension].[Date]", batch_size=50000)
    return build_reference_package(
        "REF_Load_DateDimension",
        "Regenerate ref.Calendar for 2005-2035 and publish Dimension.Date. Each date carries all "
        "three fiscal calendars in parallel - a July year for NA, the calendar year for the EU and "
        "an April year for APAC - plus the regional public holiday and working day flags.",
        OLTP, "ref.Calendar", [flow, dim],
        truncate_tables=["[ref].[Calendar]"])


@package
def ref_load_unknown_members():
    """Seed the -1 unknown member into every conformed reference set."""
    cols = [str_col("ReferenceTableName", 60), str_col("UnknownCodeValue", 20),
            str_col("UnknownDescription", 60)]
    flow = DataFlow("DFT Seed Unknown Members", "One unknown member per conformed reference table")
    flow.oledb_source(
        "REF Table Inventory", CONN_STAGING,
        "SELECT t.ReferenceTableName, N'-1' AS UnknownCodeValue, N'Unknown' AS UnknownDescription\n"
        "FROM ref.ReferenceTableInventory AS t\n"
        "WHERE t.RequiresUnknownMember = 1;",
        cols, timeout=600)
    flow.row_count("Count Reference Tables", "User::RowsRead")
    flow.derived_column("Shape Unknown Member", [
        ("CodeValue", '"-1"', str_col("CodeValue", 60)),
        ("CodeDescription", '"Unknown"', str_col("CodeDescription", 200)),
        ("RegionCode", '"ALL"', str_col("RegionCode", 4)),
        ("IsUnknownMemberFlag", '"Y"', str_col("IsUnknownMemberFlag", 1)),
        ("ChangeHash", '"UNKNOWN|MEMBER|SEED"', str_col("ChangeHash", 200)),
    ])
    flow.conditional_split("Screen Unknown Member", [
        ("Seedable Table", 'LEN(TRIM(ReferenceTableName)) > 0'),
    ], default_output="Unnamed Table")
    flow.row_count("Count Unknown Members Seeded", "User::RowsInserted")
    flow.oledb_destination("REF Unknown Member", CONN_STAGING, "[ref].[UnknownMember]", batch_size=1000)
    flow.branch_destination("ERR Unknown Member Unnamed", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "Screen Unknown Member", "Unnamed Table")
    # The seeding itself is a row-by-row loop over the inventory because each
    # reference table has its own column list; this has never been generalised.
    seed = ExecuteSql(
        "Seed Unknown Member Rows",
        CONN_STAGING,
        "DECLARE @Table NVARCHAR(60), @Sql NVARCHAR(MAX);\n"
        "DECLARE seed_cur CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT ReferenceTableName FROM ref.ReferenceTableInventory WHERE RequiresUnknownMember = 1;\n"
        "OPEN seed_cur;\n"
        "FETCH NEXT FROM seed_cur INTO @Table;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    SET @Sql = N'IF NOT EXISTS (SELECT 1 FROM ref.' + QUOTENAME(@Table) +\n"
        "               N' WHERE CodeValue = N''-1'') INSERT INTO ref.' + QUOTENAME(@Table) +\n"
        "               N' (CodeValue, CodeDescription, RegionCode, ChangeHash, ValidFrom, ValidTo, "
        "IsCurrent, VersionNumber) VALUES (N''-1'', N''Unknown'', N''ALL'', N''UNKNOWN'', "
        "SYSUTCDATETIME(), CONVERT(DATETIME2(3), N''9999-12-31''), 1, 1);';\n"
        "    EXEC sp_executesql @Sql;\n"
        "    FETCH NEXT FROM seed_cur INTO @Table;\n"
        "END\n"
        "CLOSE seed_cur;\n"
        "DEALLOCATE seed_cur;",
    )

    dim = DataFlow("DFT Publish Unknown Members", "Publish the unknown members into the DW dimensions")
    dim.oledb_source(
        "REF Unknown Member Current", CONN_STAGING,
        "SELECT ReferenceTableName, CodeValue, CodeDescription, RegionCode FROM ref.UnknownMember;",
        [str_col("ReferenceTableName", 60), str_col("CodeValue", 60), str_col("CodeDescription", 200),
         str_col("RegionCode", 4)], timeout=600)
    dim.row_count("Count Unknown Members Published", "User::RowsUpdated")
    dim.oledb_destination("DW Dimension Unknown Member", CONN_DW, "[Dimension].[UnknownMember]",
                          batch_size=1000)
    return build_reference_package(
        "REF_Load_UnknownMembers",
        "Seed the -1 unknown member into every conformed reference table that declares it is "
        "required, so that fact loads can always resolve a dimension key. The seeding loops over "
        "the inventory table and builds the insert with dynamic SQL per reference table.",
        OLTP, "ref.UnknownMember", [flow, dim],
        truncate_tables=["[ref].[UnknownMember]"],
        post_tasks=[seed])


@package
def ref_load_code_translation():
    """Maintain ref.CodeCrosswalk and report unmapped source codes."""
    cols = [
        str_col("CodeSetName", 30), str_col("SourceSystemCode", 10), str_col("SourceCode", 60),
        str_col("TargetCode", 60), str_col("TargetDescription", 200), str_col("AttributeValue", 60),
        date_col("EffectiveFromDate"), bool_col("IsActive"),
    ]
    flow = DataFlow("DFT Maintain Code Crosswalk",
                    "Merge the steward-maintained crosswalk with the codes seen in staging")
    flow.oledb_source(
        "STG Code Crosswalk Feed", CONN_STAGING,
        "SELECT CodeSetName, SourceSystemCode, SourceCode, TargetCode, TargetDescription,\n"
        "       AttributeValue, EffectiveFromDate, IsActive\n"
        "FROM stg.CodeCrosswalkFeed;",
        cols, timeout=1800)
    flow.row_count("Count Crosswalk Rows Read", "User::RowsRead")
    flow.derived_column("Conform Crosswalk", [
        ("CodeSetName", 'UPPER(TRIM(CodeSetName))', str_col("CodeSetName", 30)),
        ("SourceSystemCode", 'UPPER(TRIM(SourceSystemCode))', str_col("SourceSystemCode", 10)),
        ("SourceCode", 'UPPER(TRIM(SourceCode))', str_col("SourceCode", 60)),
        ("TargetCode", 'UPPER(TRIM(ISNULL(TargetCode) ? "-1" : TargetCode))', str_col("TargetCode", 60)),
        ("TargetDescription",
         'ISNULL(TargetDescription) || TRIM(TargetDescription) == "" ? "Unmapped" : TRIM(TargetDescription)',
         str_col("TargetDescription", 200)),
        ("EffectiveFromDate",
         'ISNULL(EffectiveFromDate) ? (DT_DBTIMESTAMP)"1900-01-01" : EffectiveFromDate',
         date_col("EffectiveFromDate")),
        ("ChangeHash", hash_expression(["CodeSetName", "SourceSystemCode", "SourceCode", "TargetCode"]),
         str_col("ChangeHash", 200)),
    ])
    flow.sort("Sort Crosswalk By Set And Source", ["CodeSetName", "SourceSystemCode", "SourceCode"],
              eliminate_duplicates=True)
    flow.conditional_split("Route Crosswalk Rows", [
        ("Mapped Code", 'TargetCode != "-1" && IsActive == TRUE'),
        ("Unmapped Code", 'TargetCode == "-1"'),
    ], default_output="Retired Mapping")
    flow.row_count("Count Crosswalk Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("REF CodeCrosswalk", CONN_STAGING, "[ref].[CodeCrosswalk]", batch_size=20000)
    flow.branch_destination("ERR Crosswalk Unmapped", CONN_STAGING, "[err].[UnmappedCode]",
                            "Route Crosswalk Rows", "Unmapped Code")
    flow.branch_destination("REF CodeCrosswalk Retired", CONN_STAGING, "[ref].[CodeCrosswalkHistory]",
                            "Route Crosswalk Rows", "Retired Mapping")

    scan_cols = [str_col("CodeSetName", 30), str_col("SourceSystemCode", 10), str_col("SourceCode", 60),
                 bigint_col("OccurrenceCount")]
    scan = DataFlow("DFT Scan Staging For Unmapped Codes",
                    "Union the code-bearing staging columns and find what the crosswalk misses")
    scan.oledb_source(
        "STG Observed Codes", CONN_STAGING,
        "SELECT N'PAYMENT_METHOD' AS CodeSetName, N'ORA_ERP' AS SourceSystemCode,\n"
        "       SourcePaymentMethodCode AS SourceCode, COUNT_BIG(*) AS OccurrenceCount\n"
        "FROM stg.Payment GROUP BY SourcePaymentMethodCode\n"
        "UNION ALL\n"
        "SELECT N'RETURN_REASON', N'WWI_OLTP', SourceReturnReasonCode, COUNT_BIG(*)\n"
        "FROM stg.[Return] GROUP BY SourceReturnReasonCode\n"
        "UNION ALL\n"
        "SELECT N'TRANSACTION_TYPE', N'WWI_OLTP', TransactionTypeCode, COUNT_BIG(*)\n"
        "FROM stg.StockMovement GROUP BY TransactionTypeCode;",
        scan_cols, timeout=3600)
    scan.lookup(
        "Lookup Existing Mapping (Partial Cache)", CONN_STAGING,
        "SELECT CodeSetName, SourceSystemCode, SourceCode, TargetCode FROM ref.CodeCrosswalk\n"
        "WHERE IsActive = 1;",
        ["CodeSetName", "SourceSystemCode", "SourceCode"], [str_col("TargetCode", 60)], no_match="RD")
    scan.derived_column("Tag Mapping Coverage", [
        ("CoverageStatusCode", '"MAPPED"', str_col("CoverageStatusCode", 10)),
        ("ReviewedFlag", '"N"', str_col("ReviewedFlag", 1)),
    ])
    scan.row_count("Count Mapped Observations", "User::RowsUpdated")
    scan.oledb_destination("REF CodeCoverage", CONN_STAGING, "[ref].[CodeCoverage]", batch_size=20000)
    scan.reject_destination("ERR Unmapped Observed Code", CONN_STAGING, "[err].[UnmappedCode]",
                            "Lookup Existing Mapping (Partial Cache)", "Lookup No Match Output")
    return build_reference_package(
        "REF_Load_CodeTranslation",
        "Maintain ref.CodeCrosswalk from the steward-maintained feed, retire superseded mappings "
        "into the crosswalk history and then sweep the staged payment, return and stock movement "
        "codes to report anything the crosswalk still does not cover.",
        ORA, "ref.CodeCrosswalk", [flow, scan],
        post_tasks=[exec_proc("Refresh Crosswalk Configuration",
                              "EXEC etl.usp_GetConfiguration @ConfigurationKey = N'CodeSetVersion', "
                              "@SourceSystemCode = ?;",
                              parameter_bindings=[("$Package::SourceSystemCode", 0, "NVARCHAR")]),
                    report_unmapped("CODE_TRANSLATION", "stg.CodeCrosswalkFeed"),
                    log_unmapped_rejects("CODE_TRANSLATION")])


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
