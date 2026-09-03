#!/usr/bin/env python3
"""Emit the WWI_Dimensions SSIS project (ssis/07_dimensions).

Fifteen warehouse dimension loads. Every package reads the staging database
(stg.*) and writes WideWorldImportersDW dimensions, and every one of them is
registered with the etl control framework in the staging database.

The SCD handling here is deliberately hand-rolled - lookups, conditional splits,
derived columns and set-based Execute SQL Tasks - the way the estate has done it
since the 2007 build. The Slowly Changing Dimension wizard component was banned
after the 2011 rebuild because its generated OLE DB Command ran row by row.

Run from the repository root:

    python3 ssis/07_dimensions/build_dimension_packages.py
"""

from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
OUT_DIR = os.path.join(REPO_ROOT, "ssis", "07_dimensions")
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

PROJECT_NAME = "WWI_Dimensions"
PROJECT_CONNECTIONS = ("WWI_Staging_DB", "WWI_DW_Destination_DB")

# Audit columns every dimension row carries. The estate has never used the
# WideWorldImporters [Lineage Key] alone; the ETL team added its own.
AUDIT_COLUMNS = [
    ("LineageKey", '@[User::PackageExecutionId]', bigint_col("LineageKey")),
    ("EtlLoadedAt", "GETDATE()", date_col("EtlLoadedAt")),
]


def _dim_package(name, description, extra_variables=None, extra_parameters=()):
    """A dimension package pre-wired to the control framework."""
    pkg = new_package(
        name,
        description,
        source_system="SQLSTG",
        connections=(CONN_STAGING, CONN_DW),
        extra_variables=[
            ("RowsInferred", 0, "int"),
            ("RowsClosedOut", 0, "int"),
            ("SourceHashMismatch", 0, "int"),
        ] + list(extra_variables or []),
    )
    for pname, pvalue, ptype, pdesc in extra_parameters:
        pkg.add_parameter(pname, pvalue, dtype=ptype, description=pdesc)
    return pkg


def _init_variables(name="Init Batch Variables", expression=None):
    return Expression(
        name,
        expression
        or '@[User::WatermarkTo] = (DT_WSTR, 30)(DT_DBTIMESTAMP) GETDATE()',
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


def _write(pkg):
    return pkg.write(os.path.join(OUT_DIR, pkg.name + ".dtsx"))


# ---------------------------------------------------------------------------
# Customer - one package per region. The three regions were forked in 2009 when
# the EU privacy rules made a single package unmaintainable, and they have
# diverged every year since.
# ---------------------------------------------------------------------------


def build_dim_na_load_customer():
    pkg = _dim_package(
        "DIM_NA_Load_Customer",
        "SCD Type 2 load of Dimension.Customer for the North America region. "
        "Carries state/county sales-tax nexus, ZIP+4 standardisation and CCPA consent state.",
        extra_variables=[("NexusStateCount", 0, "int")],
        extra_parameters=[
            ("RegionCode", "NA", "string", "Region filter applied to stg.Customer."),
            ("DefaultTaxNexus", "US-NONE", "string", "Nexus code used when the customer has no billing state."),
        ],
    )

    source_sql = (
        "SELECT c.CustomerBusinessKey, c.CustomerName, c.BuyingGroupCode, c.CustomerCategoryCode, "
        "c.PrimaryContactName, c.PhoneNumber, c.BillingStateProvince, c.BillingPostalCode, "
        "c.CreditLimitAmount, c.AccountOpenedDate, c.IsOnCreditHold, c.ConsentStatusCode, "
        "c.SourceRowHash, c.SourceSystemCode "
        "FROM stg.Customer AS c "
        "WHERE c.RegionCode = N'NA' AND c.RecordStatusCode <> N'X' "
        "ORDER BY c.CustomerBusinessKey;"
    )
    columns = [
        str_col("CustomerBusinessKey", 20),
        str_col("CustomerName", 100),
        str_col("BuyingGroupCode", 10),
        str_col("CustomerCategoryCode", 10),
        str_col("PrimaryContactName", 100),
        str_col("PhoneNumber", 20),
        str_col("BillingStateProvince", 50),
        str_col("BillingPostalCode", 10),
        money_col("CreditLimitAmount"),
        date_col("AccountOpenedDate"),
        Column("IsOnCreditHold", "bool"),
        str_col("ConsentStatusCode", 10),
        str_col("SourceRowHash", 64),
        str_col("SourceSystemCode", 10),
    ]

    flow = DataFlow("Load Customer NA Versions", "SCD2 detection for North America customers")
    flow.oledb_source("stg Customer NA", CONN_STAGING, source_sql, columns, timeout=1800)
    flow.lookup(
        "Lookup Current Customer Version",
        CONN_DW,
        "SELECT [Customer Key] AS CustomerKey, [WWI Customer ID] AS CustomerBusinessKey, "
        "[Source Row Hash] AS CurrentRowHash, [Row Version] AS CurrentRowVersion, "
        "[Is Inferred Member] AS IsInferredMember "
        "FROM Dimension.Customer WHERE [Is Current Row] = 1;",
        ["CustomerBusinessKey"],
        [
            int_col("CustomerKey"),
            str_col("CurrentRowHash", 64),
            int_col("CurrentRowVersion"),
            Column("IsInferredMember", "bool"),
        ],
        no_match="RD",
    )
    # ZIP+4 is mandatory for the tax engine; anything else is a reject, not a
    # silent default. The 2013 audit finding forced this.
    flow.derived_column(
        "Apply NA Tax And Address Rules",
        [
            (
                "TaxNexusCode",
                'LEN(TRIM(BillingStateProvince)) == 0 ? @[$Package::DefaultTaxNexus] : '
                '"US-" + UPPER(SUBSTRING(TRIM(BillingStateProvince),1,2))',
                str_col("TaxNexusCode", 12),
            ),
            (
                "PostalCodePlusFour",
                'FINDSTRING(BillingPostalCode,"-",1) > 0 ? BillingPostalCode : '
                'LEN(TRIM(BillingPostalCode)) == 9 ? SUBSTRING(BillingPostalCode,1,5) + "-" + '
                'SUBSTRING(BillingPostalCode,6,4) : BillingPostalCode',
                str_col("PostalCodePlusFour", 10),
            ),
            (
                "PostalCodeIsValid",
                'LEN(REPLACE(TRIM(BillingPostalCode),"-","")) == 5 || '
                'LEN(REPLACE(TRIM(BillingPostalCode),"-","")) == 9',
                Column("PostalCodeIsValid", "bool"),
            ),
            (
                "ConsentOptOut",
                'ConsentStatusCode == "CCPA-OUT" || ConsentStatusCode == "DNS"',
                Column("ConsentOptOut", "bool"),
            ),
            (
                "CreditLimitUsd",
                "IsOnCreditHold ? (DT_NUMERIC,18,2)0 : CreditLimitAmount",
                money_col("CreditLimitUsd"),
            ),
            ("ValidFrom", "GETDATE()", date_col("ValidFrom")),
            (
                "ValidTo",
                '(DT_DBTIMESTAMP)"9999-12-31 23:59:59"',
                date_col("ValidTo"),
            ),
            ("IsCurrentRow", "(DT_BOOL)1", Column("IsCurrentRow", "bool")),
            (
                "RowVersion",
                "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1",
                int_col("RowVersion"),
            ),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Customer Change Type",
        [
            ("Rejected Address", "!PostalCodeIsValid"),
            ("New Customer", "ISNULL(CustomerKey)"),
            ("Inferred Member Enrichment", "IsInferredMember"),
            ("Type 2 Version Change", "SourceRowHash != CurrentRowHash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination(
        "Insert New Customer Version",
        CONN_DW,
        "[Dimension].[Customer]",
        "Route Customer Change Type",
        "New Customer",
    )
    flow.branch_destination(
        "Insert Type 2 Version",
        CONN_DW,
        "[Dimension].[Customer]",
        "Route Customer Change Type",
        "Type 2 Version Change",
    )
    flow.branch_destination(
        "Enrich Inferred Customer",
        CONN_STAGING,
        "[work].[CustomerDedup]",
        "Route Customer Change Type",
        "Inferred Member Enrichment",
    )
    flow.branch_destination(
        "Reject Unusable Address",
        CONN_STAGING,
        "[err].[RejectedCustomer]",
        "Route Customer Change Type",
        "Rejected Address",
    )
    flow.reject_destination(
        "Reject Source Errors",
        CONN_STAGING,
        "[err].[RejectedCustomer]",
        "stg Customer NA",
    )

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Customer', @RegionCode = N'NA';",
            connection=CONN_DW,
        )
    )
    stage_clear = pkg.add(truncate("[work].[CustomerDedup]", connection=CONN_STAGING,
                                   name="Clear Customer Work Table"))
    load = pkg.add(DataFlowTask(flow))
    close_out = pkg.add(
        ExecuteSql(
            "Close Out Superseded Versions",
            CONN_DW,
            "UPDATE prior "
            "SET prior.[Valid To] = DATEADD(SECOND, -1, current_row.[Valid From]), "
            "    prior.[Is Current Row] = 0, "
            "    prior.[Closed By Lineage Key] = ? "
            "FROM Dimension.Customer AS prior "
            "INNER JOIN Dimension.Customer AS current_row "
            "    ON current_row.[WWI Customer ID] = prior.[WWI Customer ID] "
            "   AND current_row.[Row Version] = prior.[Row Version] + 1 "
            "WHERE prior.[Is Current Row] = 1 AND prior.[Region Code] = N'NA';",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    enrich = pkg.add(
        exec_proc(
            "Apply Inferred Member Enrichment",
            "EXEC Integration.MigrateStagedCustomerDataV2 @RegionCode = N'NA', "
            "@EnrichInferredOnly = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    rejects = pkg.add(_log_rejects("Dimension.Customer"))
    counts = pkg.add(log_row_count("Dimension.Customer"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, stage_clear, load, close_out, enrich, rejects, counts, done)
    return _write(pkg)


def build_dim_eu_load_customer():
    pkg = _dim_package(
        "DIM_EU_Load_Customer",
        "SCD Type 2 load of Dimension.Customer for the EU region. VAT registration validation, "
        "GDPR erasure pseudonymisation and consent-driven retention are applied in this package only.",
        extra_variables=[("ErasureRequestCount", 0, "int"), ("VatCheckFailures", 0, "int")],
        extra_parameters=[
            ("RegionCode", "EU", "string", "Region filter applied to stg.Customer."),
            ("RetentionMonths", 84, "int", "Months of inactivity after which an EU customer is pseudonymised."),
        ],
    )

    source_sql = (
        "SELECT c.CustomerBusinessKey, c.CustomerName, c.LegalEntityName, c.CountryIsoCode, "
        "c.VatRegistrationNumber, c.CustomerCategoryCode, c.PaymentTermsCode, c.RiskBandCode, "
        "c.BillingPostalCode, c.CreditLimitAmount, c.CreditLimitCurrency, c.LastActivityDate, "
        "c.ErasureRequestedDate, c.ConsentStatusCode, c.SourceRowHash "
        "FROM stg.Customer AS c "
        "WHERE c.RegionCode = N'EU' AND c.RecordStatusCode IN (N'A', N'P') "
        "ORDER BY c.CountryIsoCode, c.CustomerBusinessKey;"
    )
    columns = [
        str_col("CustomerBusinessKey", 20),
        str_col("CustomerName", 100),
        str_col("LegalEntityName", 150),
        str_col("CountryIsoCode", 2),
        str_col("VatRegistrationNumber", 20),
        str_col("CustomerCategoryCode", 10),
        str_col("PaymentTermsCode", 10),
        str_col("RiskBandCode", 4),
        str_col("BillingPostalCode", 12),
        money_col("CreditLimitAmount"),
        str_col("CreditLimitCurrency", 3),
        date_col("LastActivityDate"),
        date_col("ErasureRequestedDate"),
        str_col("ConsentStatusCode", 10),
        str_col("SourceRowHash", 64),
    ]

    flow = DataFlow("Load Customer EU Versions", "SCD2 detection with GDPR handling for EU customers")
    flow.oledb_source("stg Customer EU", CONN_STAGING, source_sql, columns, timeout=1800)
    flow.lookup(
        "Lookup Current Customer Version",
        CONN_DW,
        "SELECT [Customer Key] AS CustomerKey, [WWI Customer ID] AS CustomerBusinessKey, "
        "[Source Row Hash] AS CurrentRowHash, [Row Version] AS CurrentRowVersion, "
        "[Valid From] AS CurrentValidFrom "
        "FROM Dimension.Customer WHERE [Is Current Row] = 1 AND [Region Code] = N'EU';",
        ["CustomerBusinessKey"],
        [
            int_col("CustomerKey"),
            str_col("CurrentRowHash", 64),
            int_col("CurrentRowVersion"),
            date_col("CurrentValidFrom"),
        ],
        no_match="RD",
    )
    flow.lookup(
        "Lookup Country VAT Regime",
        CONN_STAGING,
        "SELECT CountryIsoCode, VatPrefix, StandardVatRatePercent, ReverseChargeApplies, "
        "FiscalYearStartMonth FROM stg.TaxRate WHERE TaxRegimeCode = N'VAT';",
        ["CountryIsoCode"],
        [
            str_col("VatPrefix", 2),
            Column("StandardVatRatePercent", "numeric", precision=5, scale=2),
            Column("ReverseChargeApplies", "bool"),
            int_col("FiscalYearStartMonth"),
        ],
        no_match="RD",
    )
    flow.derived_column(
        "Apply GDPR And VAT Rules",
        [
            (
                "VatNumberIsWellFormed",
                'LEN(TRIM(VatRegistrationNumber)) >= 8 && '
                'UPPER(SUBSTRING(TRIM(VatRegistrationNumber),1,2)) == VatPrefix',
                Column("VatNumberIsWellFormed", "bool"),
            ),
            (
                "IsErasureRequested",
                "!ISNULL(ErasureRequestedDate)",
                Column("IsErasureRequested", "bool"),
            ),
            (
                "RetentionExpired",
                'DATEDIFF("Month", LastActivityDate, GETDATE()) > @[$Package::RetentionMonths]',
                Column("RetentionExpired", "bool"),
            ),
            (
                "CustomerNamePublished",
                '(!ISNULL(ErasureRequestedDate) || ConsentStatusCode == "GDPR-WITHDRAWN") '
                '? "REDACTED-" + RIGHT(CustomerBusinessKey,6) : CustomerName',
                str_col("CustomerNamePublished", 100),
            ),
            (
                "PostalCodeNormalised",
                'CountryIsoCode == "NL" ? UPPER(REPLACE(TRIM(BillingPostalCode)," ","")) : '
                'CountryIsoCode == "PL" ? SUBSTRING(REPLACE(TRIM(BillingPostalCode),"-",""),1,2) + "-" + '
                'SUBSTRING(REPLACE(TRIM(BillingPostalCode),"-",""),3,3) : UPPER(TRIM(BillingPostalCode))',
                str_col("PostalCodeNormalised", 12),
            ),
            (
                "CreditLimitEur",
                'CreditLimitCurrency == "EUR" ? CreditLimitAmount : (DT_NUMERIC,18,2)NULL(DT_NUMERIC,18,2)',
                money_col("CreditLimitEur"),
            ),
            (
                "FiscalYearStartMonth",
                "ISNULL(FiscalYearStartMonth) ? 1 : FiscalYearStartMonth",
                int_col("FiscalYearStartMonthApplied"),
            ),
            ("ValidFrom", "GETDATE()", date_col("ValidFrom")),
            ("ValidTo", '(DT_DBTIMESTAMP)"9999-12-31 23:59:59"', date_col("ValidTo")),
            ("IsCurrentRow", "(DT_BOOL)1", Column("IsCurrentRow", "bool")),
            (
                "RowVersion",
                "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1",
                int_col("RowVersion"),
            ),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route EU Change Type",
        [
            ("Rejected VAT Number", '!VatNumberIsWellFormed && RiskBandCode != "EXEM"'),
            ("Pseudonymise In Place", "IsErasureRequested || RetentionExpired"),
            ("New Customer", "ISNULL(CustomerKey)"),
            ("Type 2 Version Change", "SourceRowHash != CurrentRowHash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination(
        "Insert New Customer Version", CONN_DW, "[Dimension].[Customer]",
        "Route EU Change Type", "New Customer",
    )
    flow.branch_destination(
        "Insert Type 2 Version", CONN_DW, "[Dimension].[Customer]",
        "Route EU Change Type", "Type 2 Version Change",
    )
    # Erasure is a Type 1 overwrite applied to every historical version, so it
    # lands in the work table and is applied set-based below.
    flow.branch_destination(
        "Stage Pseudonymisation", CONN_STAGING, "[work].[CustomerDedup]",
        "Route EU Change Type", "Pseudonymise In Place",
    )
    flow.branch_destination(
        "Reject Invalid VAT Number", CONN_STAGING, "[err].[RejectedCustomer]",
        "Route EU Change Type", "Rejected VAT Number",
    )
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedCustomer]", "stg Customer EU")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Customer', @RegionCode = N'EU';",
            connection=CONN_DW,
        )
    )
    stage_clear = pkg.add(truncate("[work].[CustomerDedup]", connection=CONN_STAGING,
                                   name="Clear Customer Work Table"))
    load = pkg.add(DataFlowTask(flow))
    close_out = pkg.add(
        ExecuteSql(
            "Close Out Superseded Versions",
            CONN_DW,
            "UPDATE prior "
            "SET prior.[Valid To] = DATEADD(SECOND, -1, current_row.[Valid From]), "
            "    prior.[Is Current Row] = 0, prior.[Closed By Lineage Key] = ? "
            "FROM Dimension.Customer AS prior "
            "INNER JOIN Dimension.Customer AS current_row "
            "    ON current_row.[WWI Customer ID] = prior.[WWI Customer ID] "
            "   AND current_row.[Row Version] = prior.[Row Version] + 1 "
            "WHERE prior.[Is Current Row] = 1 AND prior.[Region Code] = N'EU';",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    erasure = pkg.add(
        exec_proc(
            "Apply Erasure And Retention",
            "EXEC Integration.MigrateStagedCustomerDataV2 @RegionCode = N'EU', "
            "@ApplyErasure = 1, @RetentionMonths = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::RetentionMonths", 0, "LONG"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    rejects = pkg.add(_log_rejects("Dimension.Customer"))
    counts = pkg.add(log_row_count("Dimension.Customer"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, stage_clear, load, close_out, erasure, rejects, counts, done)
    return _write(pkg)


def build_dim_apac_load_customer():
    pkg = _dim_package(
        "DIM_APAC_Load_Customer",
        "SCD Type 2 load of Dimension.Customer for APAC. GST/ABN registration, an April-March "
        "fiscal calendar and script-aware name handling differ from the other two regions.",
        extra_variables=[("RomanisationFallbacks", 0, "int")],
        extra_parameters=[
            ("RegionCode", "APAC", "string", "Region filter applied to stg.Customer."),
            ("FiscalYearStartMonth", 4, "int", "APAC fiscal year starts in April."),
        ],
    )

    source_sql = (
        "SELECT c.CustomerBusinessKey, c.CustomerName, c.CustomerNameLocalScript, c.CountryIsoCode, "
        "c.GstRegistrationNumber, c.DistributorTierCode, c.CustomerCategoryCode, c.BillingPostalCode, "
        "c.BillingCityName, c.CreditLimitAmount, c.CreditLimitCurrency, c.ConsentStatusCode, "
        "c.AccountOpenedDate, c.SourceRowHash "
        "FROM stg.Customer AS c "
        "WHERE c.RegionCode = N'APAC' AND c.RecordStatusCode <> N'X' "
        "ORDER BY c.CountryIsoCode, c.CustomerBusinessKey;"
    )
    columns = [
        str_col("CustomerBusinessKey", 20),
        str_col("CustomerName", 100),
        str_col("CustomerNameLocalScript", 200),
        str_col("CountryIsoCode", 2),
        str_col("GstRegistrationNumber", 20),
        str_col("DistributorTierCode", 4),
        str_col("CustomerCategoryCode", 10),
        str_col("BillingPostalCode", 12),
        str_col("BillingCityName", 60),
        money_col("CreditLimitAmount"),
        str_col("CreditLimitCurrency", 3),
        str_col("ConsentStatusCode", 12),
        date_col("AccountOpenedDate"),
        str_col("SourceRowHash", 64),
    ]

    flow = DataFlow("Load Customer APAC Versions", "SCD2 detection for APAC customers")
    flow.oledb_source("stg Customer APAC", CONN_STAGING, source_sql, columns, timeout=1800)
    flow.lookup(
        "Lookup Current Customer Version",
        CONN_DW,
        "SELECT [Customer Key] AS CustomerKey, [WWI Customer ID] AS CustomerBusinessKey, "
        "[Source Row Hash] AS CurrentRowHash, [Row Version] AS CurrentRowVersion "
        "FROM Dimension.Customer WHERE [Is Current Row] = 1 AND [Region Code] = N'APAC';",
        ["CustomerBusinessKey"],
        [int_col("CustomerKey"), str_col("CurrentRowHash", 64), int_col("CurrentRowVersion")],
        no_match="RD",
    )
    flow.derived_column(
        "Apply APAC GST And Script Rules",
        [
            (
                "GstRegistrationClean",
                'REPLACE(REPLACE(TRIM(GstRegistrationNumber)," ",""),"-","")',
                str_col("GstRegistrationClean", 20),
            ),
            (
                "GstRegistrationIsValid",
                'CountryIsoCode == "AU" ? LEN(REPLACE(TRIM(GstRegistrationNumber)," ","")) == 11 : '
                'CountryIsoCode == "IN" ? LEN(TRIM(GstRegistrationNumber)) == 15 : '
                'CountryIsoCode == "SG" ? LEN(TRIM(GstRegistrationNumber)) >= 9 : '
                "LEN(TRIM(GstRegistrationNumber)) > 0",
                Column("GstRegistrationIsValid", "bool"),
            ),
            (
                "CustomerNameRoman",
                'LEN(TRIM(CustomerName)) == 0 ? "UNROMANISED-" + CustomerBusinessKey : UPPER(TRIM(CustomerName))',
                str_col("CustomerNameRoman", 100),
            ),
            # Hong Kong and Macau have no postal code at all; the 2010 build
            # rejected those rows until finance complained.
            (
                "PostalCodeRequired",
                'CountryIsoCode != "HK" && CountryIsoCode != "MO"',
                Column("PostalCodeRequired", "bool"),
            ),
            (
                "FiscalYearLabel",
                'DATEPART("mm", GETDATE()) >= @[$Package::FiscalYearStartMonth] '
                '? "FY" + (DT_WSTR,4)(YEAR(GETDATE()) + 1) : "FY" + (DT_WSTR,4)YEAR(GETDATE())',
                str_col("FiscalYearLabel", 6),
            ),
            (
                "ConsentRegimeCode",
                'CountryIsoCode == "JP" ? "APPI" : CountryIsoCode == "SG" ? "PDPA" : '
                'CountryIsoCode == "AU" ? "APP" : "LOCAL"',
                str_col("ConsentRegimeCode", 8),
            ),
            ("ValidFrom", "GETDATE()", date_col("ValidFrom")),
            ("ValidTo", '(DT_DBTIMESTAMP)"9999-12-31 23:59:59"', date_col("ValidTo")),
            ("IsCurrentRow", "(DT_BOOL)1", Column("IsCurrentRow", "bool")),
            ("RowVersion", "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1", int_col("RowVersion")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route APAC Change Type",
        [
            (
                "Rejected Registration",
                '!GstRegistrationIsValid && DistributorTierCode != "SMB"',
            ),
            (
                "Rejected Address",
                "PostalCodeRequired && LEN(TRIM(BillingPostalCode)) == 0",
            ),
            ("New Customer", "ISNULL(CustomerKey)"),
            ("Type 2 Version Change", "SourceRowHash != CurrentRowHash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Customer Version", CONN_DW, "[Dimension].[Customer]",
                            "Route APAC Change Type", "New Customer")
    flow.branch_destination("Insert Type 2 Version", CONN_DW, "[Dimension].[Customer]",
                            "Route APAC Change Type", "Type 2 Version Change")
    flow.branch_destination("Reject Invalid Registration", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Route APAC Change Type", "Rejected Registration")
    flow.branch_destination("Reject Missing Postal Code", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Route APAC Change Type", "Rejected Address")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedCustomer]", "stg Customer APAC")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Customer', @RegionCode = N'APAC';",
            connection=CONN_DW,
        )
    )
    load = pkg.add(DataFlowTask(flow))
    close_out = pkg.add(
        ExecuteSql(
            "Close Out Superseded Versions",
            CONN_DW,
            "UPDATE prior "
            "SET prior.[Valid To] = DATEADD(SECOND, -1, current_row.[Valid From]), "
            "    prior.[Is Current Row] = 0, prior.[Closed By Lineage Key] = ? "
            "FROM Dimension.Customer AS prior "
            "INNER JOIN Dimension.Customer AS current_row "
            "    ON current_row.[WWI Customer ID] = prior.[WWI Customer ID] "
            "   AND current_row.[Row Version] = prior.[Row Version] + 1 "
            "WHERE prior.[Is Current Row] = 1 AND prior.[Region Code] = N'APAC';",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    tiering = pkg.add(
        exec_proc(
            "Recalculate Distributor Tier",
            "EXEC Integration.MigrateStagedCustomerDataV2 @RegionCode = N'APAC', "
            "@RecalculateDistributorTier = 1, @FiscalYearStartMonth = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::FiscalYearStartMonth", 0, "LONG"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    rejects = pkg.add(_log_rejects("Dimension.Customer"))
    counts = pkg.add(log_row_count("Dimension.Customer"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, load, close_out, tiering, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Type 1 dimensions
# ---------------------------------------------------------------------------


def build_dim_load_customer_category():
    pkg = _dim_package(
        "DIM_Load_CustomerCategory",
        "SCD Type 1 overwrite of Dimension.Customer Category. Small dimension, compared by "
        "checksum so unchanged rows never touch the warehouse.",
    )
    columns = [
        str_col("CustomerCategoryCode", 10),
        str_col("CustomerCategoryName", 60),
        str_col("CategoryGroupCode", 10),
        Column("DiscountEligiblePercent", "numeric", precision=5, scale=2),
        Column("IsActive", "bool"),
        int_col("SourceChecksum"),
    ]
    flow = DataFlow("Load Customer Category", "Type 1 overwrite with checksum comparison")
    flow.oledb_source(
        "stg CustomerCategory",
        CONN_STAGING,
        "SELECT cc.CustomerCategoryCode, cc.CustomerCategoryName, cc.CategoryGroupCode, "
        "cc.DiscountEligiblePercent, cc.IsActive, "
        "BINARY_CHECKSUM(cc.CustomerCategoryName, cc.CategoryGroupCode, cc.DiscountEligiblePercent, "
        "cc.IsActive) AS SourceChecksum "
        "FROM stg.CustomerCategory AS cc;",
        columns,
    )
    flow.lookup(
        "Lookup Existing Category",
        CONN_DW,
        "SELECT [Customer Category Key] AS CustomerCategoryKey, "
        "[WWI Customer Category ID] AS CustomerCategoryCode, "
        "[Source Checksum] AS TargetChecksum FROM [Dimension].[Customer Category];",
        ["CustomerCategoryCode"],
        [int_col("CustomerCategoryKey"), int_col("TargetChecksum")],
        no_match="RD",
    )
    flow.derived_column(
        "Standardise Category Attributes",
        [
            ("CategoryNameUpper", "UPPER(TRIM(CustomerCategoryName))", str_col("CategoryNameUpper", 60)),
            (
                "DiscountBandCode",
                'DiscountEligiblePercent >= 15 ? "D3" : DiscountEligiblePercent >= 5 ? "D2" : "D1"',
                str_col("DiscountBandCode", 2),
            ),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Category Change",
        [
            ("New Category", "ISNULL(CustomerCategoryKey)"),
            ("Changed Category", "SourceChecksum != TargetChecksum"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Category", CONN_DW, "[Dimension].[Customer Category]",
                            "Route Category Change", "New Category")
    flow.branch_destination("Stage Type 1 Overwrite", CONN_STAGING, "[work].[CustomerDedup]",
                            "Route Category Change", "Changed Category")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedCustomer]", "stg CustomerCategory")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Customer Category';",
            connection=CONN_DW,
        )
    )
    clear = pkg.add(truncate("[work].[CustomerDedup]", connection=CONN_STAGING, name="Clear Category Work Table"))
    load = pkg.add(DataFlowTask(flow))
    overwrite = pkg.add(
        exec_proc(
            "Apply Type 1 Overwrite",
            "EXEC Integration.MigrateStagedCustomerCategoryData @LineageKey = ?, @OverwriteOnly = 1;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    counts = pkg.add(log_row_count("Dimension.Customer Category"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, clear, load, overwrite, counts, done)
    return _write(pkg)


def build_dim_load_sales_territory():
    pkg = _dim_package(
        "DIM_Load_SalesTerritory",
        "SCD Type 1 load of Dimension.Sales Territory plus the territory hierarchy rebuild "
        "(territory -> region -> area) that the reporting layer walks.",
        extra_variables=[("HierarchyDepth", 0, "int")],
    )
    columns = [
        str_col("SalesTerritoryCode", 10),
        str_col("SalesTerritoryName", 60),
        str_col("ParentRegionCode", 10),
        str_col("AreaCode", 10),
        str_col("RegionCode", 6),
        str_col("TaxJurisdictionCode", 12),
        Column("IsActive", "bool"),
    ]
    flow = DataFlow("Load Sales Territory", "Type 1 overwrite with hierarchy attributes")
    flow.oledb_source(
        "stg SalesTerritory",
        CONN_STAGING,
        "SELECT t.SalesTerritoryCode, t.SalesTerritoryName, t.ParentRegionCode, t.AreaCode, "
        "t.RegionCode, t.TaxJurisdictionCode, t.IsActive "
        "FROM stg.SalesTerritory AS t ORDER BY t.AreaCode, t.ParentRegionCode, t.SalesTerritoryCode;",
        columns,
    )
    flow.lookup(
        "Lookup Existing Territory",
        CONN_DW,
        "SELECT [Sales Territory Key] AS SalesTerritoryKey, [WWI Territory ID] AS SalesTerritoryCode, "
        "[Territory Path] AS ExistingPath FROM [Dimension].[Sales Territory];",
        ["SalesTerritoryCode"],
        [int_col("SalesTerritoryKey"), str_col("ExistingPath", 120)],
        no_match="RD",
    )
    flow.derived_column(
        "Build Territory Path",
        [
            (
                "TerritoryPath",
                'UPPER(TRIM(AreaCode)) + "/" + UPPER(TRIM(ParentRegionCode)) + "/" + UPPER(TRIM(SalesTerritoryCode))',
                str_col("TerritoryPath", 120),
            ),
            # The tax jurisdiction spelling differs per region: NA carries a
            # state code, EU a country VAT regime, APAC a GST registration area.
            (
                "TaxJurisdictionNormalised",
                'RegionCode == "NA" ? "US-" + RIGHT(TRIM(TaxJurisdictionCode),2) : '
                'RegionCode == "EU" ? "VAT-" + LEFT(TRIM(TaxJurisdictionCode),2) : '
                '"GST-" + UPPER(TRIM(TaxJurisdictionCode))',
                str_col("TaxJurisdictionNormalised", 16),
            ),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Territory Change",
        [
            ("New Territory", "ISNULL(SalesTerritoryKey)"),
            ("Path Changed", "TerritoryPath != ExistingPath"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Territory", CONN_DW, "[Dimension].[Sales Territory]",
                            "Route Territory Change", "New Territory")
    flow.branch_destination("Stage Hierarchy Move", CONN_STAGING, "[work].[ProductCrosswalk]",
                            "Route Territory Change", "Path Changed")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg SalesTerritory")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("[work].[ProductCrosswalk]", connection=CONN_STAGING,
                             name="Clear Hierarchy Work Table"))
    load = pkg.add(DataFlowTask(flow))
    hierarchy = pkg.add(
        exec_proc(
            "Rebuild Territory Hierarchy",
            "EXEC Integration.MigrateStagedSalesTerritoryData @RebuildHierarchy = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    orphans = pkg.add(
        ExecuteSql(
            "Reparent Orphaned Territories",
            CONN_DW,
            "UPDATE t SET t.[Parent Region Code] = N'UNASSIGNED', "
            "t.[Territory Path] = N'UNASSIGNED/' + t.[WWI Territory ID] "
            "FROM [Dimension].[Sales Territory] AS t "
            "WHERE NOT EXISTS (SELECT 1 FROM [Dimension].[Sales Territory] AS p "
            "                  WHERE p.[WWI Territory ID] = t.[Parent Region Code]);",
        )
    )
    counts = pkg.add(log_row_count("Dimension.Sales Territory"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, load, hierarchy, orphans, counts, done)
    return _write(pkg)


def build_dim_load_product_category():
    pkg = _dim_package(
        "DIM_Load_ProductCategory",
        "SCD Type 1 load of Dimension.Product Category with category hierarchy and the "
        "stock-item-to-category bridge maintenance.",
        extra_variables=[("BridgeRowsWritten", 0, "int")],
    )
    columns = [
        str_col("ProductCategoryCode", 10),
        str_col("ProductCategoryName", 80),
        str_col("ParentCategoryCode", 10),
        str_col("MerchandiseGroupCode", 8),
        int_col("HierarchyLevel"),
        Column("IsLeafCategory", "bool"),
    ]
    flow = DataFlow("Load Product Category", "Type 1 overwrite with hierarchy level derivation")
    flow.oledb_source(
        "stg ProductCategory",
        CONN_STAGING,
        "SELECT pc.ProductCategoryCode, pc.ProductCategoryName, pc.ParentCategoryCode, "
        "pc.MerchandiseGroupCode, pc.HierarchyLevel, pc.IsLeafCategory "
        "FROM stg.ProductCategory AS pc ORDER BY pc.HierarchyLevel, pc.ProductCategoryCode;",
        columns,
    )
    flow.lookup(
        "Lookup Parent Category Key",
        CONN_DW,
        "SELECT [Product Category Key] AS ParentCategoryKey, "
        "[WWI Product Category ID] AS ParentCategoryCode FROM [Dimension].[Product Category];",
        ["ParentCategoryCode"],
        [int_col("ParentCategoryKey")],
        no_match="IG",
    )
    flow.derived_column(
        "Derive Category Hierarchy",
        [
            (
                "ParentCategoryKeyResolved",
                "ISNULL(ParentCategoryKey) ? -1 : ParentCategoryKey",
                int_col("ParentCategoryKeyResolved"),
            ),
            (
                "CategoryPath",
                'ISNULL(ParentCategoryCode) || LEN(TRIM(ParentCategoryCode)) == 0 '
                '? UPPER(TRIM(ProductCategoryCode)) '
                ': UPPER(TRIM(ParentCategoryCode)) + ">" + UPPER(TRIM(ProductCategoryCode))',
                str_col("CategoryPath", 120),
            ),
            (
                "ReportingRollupCode",
                'MerchandiseGroupCode == "CHILL" ? "PERISHABLE" : '
                'MerchandiseGroupCode == "TOY" || MerchandiseGroupCode == "NOV" ? "SEASONAL" : "CORE"',
                str_col("ReportingRollupCode", 12),
            ),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Category Level",
        [
            ("Leaf Category", "IsLeafCategory"),
            ("Rollup Category", "!IsLeafCategory && HierarchyLevel > 1"),
        ],
        default_output="Root Category",
    )
    flow.branch_destination("Insert Leaf Category", CONN_DW, "[Dimension].[Product Category]",
                            "Route Category Level", "Leaf Category")
    flow.branch_destination("Insert Rollup Category", CONN_DW, "[Dimension].[Product Category]",
                            "Route Category Level", "Rollup Category")
    flow.branch_destination("Insert Root Category", CONN_DW, "[Dimension].[Product Category]",
                            "Route Category Level", "Root Category")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedProduct]", "stg ProductCategory")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Product Category';",
            connection=CONN_DW,
        )
    )
    load = pkg.add(DataFlowTask(flow))
    overwrite = pkg.add(
        exec_proc(
            "Apply Type 1 Overwrite",
            "EXEC Integration.MigrateStagedProductCategoryData @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    bridge = pkg.add(
        ExecuteSql(
            "Maintain Stock Item Category Bridge",
            CONN_DW,
            "DELETE b FROM [Dimension].[Stock Item Category Bridge] AS b "
            "WHERE NOT EXISTS (SELECT 1 FROM [Dimension].[Product Category] AS c "
            "                  WHERE c.[Product Category Key] = b.[Product Category Key]); "
            "INSERT INTO [Dimension].[Stock Item Category Bridge] "
            "    ([Stock Item Key], [Product Category Key], [Weighting Factor], [Lineage Key]) "
            "SELECT si.[Stock Item Key], c.[Product Category Key], 1.0, ? "
            "FROM [Dimension].[Stock Item] AS si "
            "INNER JOIN [Dimension].[Product Category] AS c "
            "    ON c.[WWI Product Category ID] = si.[Product Category Code] "
            "WHERE si.[Is Current Row] = 1 "
            "  AND NOT EXISTS (SELECT 1 FROM [Dimension].[Stock Item Category Bridge] AS x "
            "                  WHERE x.[Stock Item Key] = si.[Stock Item Key] "
            "                    AND x.[Product Category Key] = c.[Product Category Key]);",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    counts = pkg.add(log_row_count("Dimension.Product Category"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, load, overwrite, bridge, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Supplier side
# ---------------------------------------------------------------------------


def build_dim_load_supplier():
    pkg = _dim_package(
        "DIM_Load_Supplier",
        "Hybrid dimension load: Type 1 overwrite for contact attributes and Type 2 versioning "
        "for commercial attributes (payment terms, rating, ownership) in a single package.",
        extra_variables=[("Type1Updates", 0, "int"), ("Type2Versions", 0, "int")],
    )
    columns = [
        str_col("SupplierBusinessKey", 20),
        str_col("SupplierName", 100),
        str_col("SupplierCategoryCode", 10),
        str_col("PrimaryContactName", 100),
        str_col("PhoneNumber", 20),
        str_col("FaxNumber", 20),
        str_col("WebsiteUrl", 256),
        str_col("PaymentTermsCode", 10),
        str_col("SupplierRatingCode", 4),
        str_col("OwnershipCode", 8),
        str_col("BankAccountCountry", 2),
        Column("IsStrategicSupplier", "bool"),
        str_col("Type1Hash", 64),
        str_col("Type2Hash", 64),
    ]
    flow = DataFlow("Load Supplier Hybrid SCD", "Type 1 and Type 2 attributes split in one flow")
    flow.oledb_source(
        "stg Supplier",
        CONN_STAGING,
        "SELECT s.SupplierBusinessKey, s.SupplierName, s.SupplierCategoryCode, s.PrimaryContactName, "
        "s.PhoneNumber, s.FaxNumber, s.WebsiteUrl, s.PaymentTermsCode, s.SupplierRatingCode, "
        "s.OwnershipCode, s.BankAccountCountry, s.IsStrategicSupplier, "
        "s.Type1AttributeHash AS Type1Hash, s.Type2AttributeHash AS Type2Hash "
        "FROM stg.Supplier AS s WHERE s.RecordStatusCode <> N'X';",
        columns,
        timeout=900,
    )
    flow.lookup(
        "Lookup Current Supplier Version",
        CONN_DW,
        "SELECT [Supplier Key] AS SupplierKey, [WWI Supplier ID] AS SupplierBusinessKey, "
        "[Type 1 Hash] AS CurrentType1Hash, [Type 2 Hash] AS CurrentType2Hash, "
        "[Row Version] AS CurrentRowVersion FROM [Dimension].[Supplier] WHERE [Is Current Row] = 1;",
        ["SupplierBusinessKey"],
        [
            int_col("SupplierKey"),
            str_col("CurrentType1Hash", 64),
            str_col("CurrentType2Hash", 64),
            int_col("CurrentRowVersion"),
        ],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Supplier Attributes",
        [
            (
                "PaymentTermsDays",
                'PaymentTermsCode == "N30" ? 30 : PaymentTermsCode == "N45" ? 45 : '
                'PaymentTermsCode == "N60" ? 60 : PaymentTermsCode == "EOM" ? 31 : 14',
                int_col("PaymentTermsDays"),
            ),
            (
                "SupplierRiskScore",
                'SupplierRatingCode == "AAA" ? 1 : SupplierRatingCode == "AA" ? 2 : '
                'SupplierRatingCode == "A" ? 3 : SupplierRatingCode == "B" ? 5 : 8',
                int_col("SupplierRiskScore"),
            ),
            (
                "IsCrossBorderPayee",
                'BankAccountCountry != "US" && BankAccountCountry != "GB"',
                Column("IsCrossBorderPayee", "bool"),
            ),
            ("ValidFrom", "GETDATE()", date_col("ValidFrom")),
            ("ValidTo", '(DT_DBTIMESTAMP)"9999-12-31 23:59:59"', date_col("ValidTo")),
            ("IsCurrentRow", "(DT_BOOL)1", Column("IsCurrentRow", "bool")),
            ("RowVersion", "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1", int_col("RowVersion")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Split Type 1 And Type 2 Changes",
        [
            ("New Supplier", "ISNULL(SupplierKey)"),
            ("Type 2 Commercial Change", "Type2Hash != CurrentType2Hash"),
            ("Type 1 Contact Change", "Type1Hash != CurrentType1Hash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Supplier", CONN_DW, "[Dimension].[Supplier]",
                            "Split Type 1 And Type 2 Changes", "New Supplier")
    flow.branch_destination("Insert Type 2 Version", CONN_DW, "[Dimension].[Supplier]",
                            "Split Type 1 And Type 2 Changes", "Type 2 Commercial Change")
    flow.branch_destination("Stage Type 1 Overwrite", CONN_STAGING, "[work].[SupplierDedup]",
                            "Split Type 1 And Type 2 Changes", "Type 1 Contact Change")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedSupplier]", "stg Supplier")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Supplier';",
            connection=CONN_DW,
        )
    )
    clear = pkg.add(truncate("[work].[SupplierDedup]", connection=CONN_STAGING, name="Clear Supplier Work Table"))
    load = pkg.add(DataFlowTask(flow))
    close_out = pkg.add(
        ExecuteSql(
            "Close Out Superseded Versions",
            CONN_DW,
            "UPDATE prior SET prior.[Valid To] = DATEADD(SECOND, -1, current_row.[Valid From]), "
            "prior.[Is Current Row] = 0 "
            "FROM [Dimension].[Supplier] AS prior "
            "INNER JOIN [Dimension].[Supplier] AS current_row "
            "    ON current_row.[WWI Supplier ID] = prior.[WWI Supplier ID] "
            "   AND current_row.[Row Version] = prior.[Row Version] + 1 "
            "WHERE prior.[Is Current Row] = 1;",
        )
    )
    type1 = pkg.add(
        exec_proc(
            "Apply Type 1 Overwrite To All Versions",
            "EXEC Integration.MigrateStagedSupplierDataV2 @ApplyType1 = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    rejects = pkg.add(_log_rejects("Dimension.Supplier"))
    counts = pkg.add(log_row_count("Dimension.Supplier"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, clear, load, close_out, type1, rejects, counts, done)
    return _write(pkg)


def build_dim_load_vendor_contract():
    pkg = _dim_package(
        "DIM_Load_VendorContract",
        "SCD Type 2 load of Dimension.Vendor Contract. Contract windows drive the effective "
        "dating, and contracts that lapsed since the last run are closed out explicitly.",
        extra_variables=[("ContractsExpired", 0, "int")],
        extra_parameters=[
            ("RenewalNoticeDays", 90, "int", "Notice window used to flag contracts as up for renewal."),
        ],
    )
    columns = [
        str_col("ContractBusinessKey", 24),
        str_col("SupplierBusinessKey", 20),
        str_col("ContractTypeCode", 8),
        str_col("ContractCurrency", 3),
        money_col("CommittedSpendAmount"),
        Column("RebatePercent", "numeric", precision=5, scale=2),
        date_col("ContractStartDate"),
        date_col("ContractEndDate"),
        str_col("GoverningLawCode", 8),
        str_col("RegionCode", 6),
        str_col("SourceRowHash", 64),
    ]
    flow = DataFlow("Load Vendor Contract Versions", "Contract-window driven SCD2")
    flow.oledb_source(
        "stg VendorContract",
        CONN_STAGING,
        "SELECT vc.ContractBusinessKey, vc.SupplierBusinessKey, vc.ContractTypeCode, vc.ContractCurrency, "
        "vc.CommittedSpendAmount, vc.RebatePercent, vc.ContractStartDate, vc.ContractEndDate, "
        "vc.GoverningLawCode, vc.RegionCode, vc.SourceRowHash "
        "FROM stg.VendorContract AS vc WHERE vc.ContractEndDate >= DATEADD(YEAR, -3, GETDATE());",
        columns,
    )
    flow.lookup(
        "Lookup Supplier Key",
        CONN_DW,
        "SELECT [Supplier Key] AS SupplierKey, [WWI Supplier ID] AS SupplierBusinessKey "
        "FROM [Dimension].[Supplier] WHERE [Is Current Row] = 1;",
        ["SupplierBusinessKey"],
        [int_col("SupplierKey")],
        no_match="RD",
    )
    flow.lookup(
        "Lookup Current Contract Version",
        CONN_DW,
        "SELECT [Vendor Contract Key] AS VendorContractKey, [WWI Contract ID] AS ContractBusinessKey, "
        "[Source Row Hash] AS CurrentRowHash, [Row Version] AS CurrentRowVersion "
        "FROM [Dimension].[Vendor Contract] WHERE [Is Current Row] = 1;",
        ["ContractBusinessKey"],
        [int_col("VendorContractKey"), str_col("CurrentRowHash", 64), int_col("CurrentRowVersion")],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Contract Window Attributes",
        [
            (
                "ContractStatusCode",
                'ContractEndDate < GETDATE() ? "EXPIRED" : '
                'DATEDIFF("Day", GETDATE(), ContractEndDate) <= @[$Package::RenewalNoticeDays] '
                '? "RENEWAL" : ContractStartDate > GETDATE() ? "PENDING" : "ACTIVE"',
                str_col("ContractStatusCode", 10),
            ),
            (
                "CommittedSpendReporting",
                'ContractCurrency == "USD" ? CommittedSpendAmount : (DT_NUMERIC,18,2)NULL(DT_NUMERIC,18,2)',
                money_col("CommittedSpendReporting"),
            ),
            (
                "RebateTierCode",
                'RebatePercent >= 7.5 ? "R4" : RebatePercent >= 5 ? "R3" : RebatePercent >= 2.5 ? "R2" : "R1"',
                str_col("RebateTierCode", 2),
            ),
            (
                "GoverningLawRegion",
                'GoverningLawCode == "NY" || GoverningLawCode == "DE" ? "NA" : '
                'GoverningLawCode == "ENG" || GoverningLawCode == "NL" ? "EU" : "APAC"',
                str_col("GoverningLawRegion", 6),
            ),
            ("ValidFrom", "ContractStartDate", date_col("ValidFrom")),
            ("ValidTo", "ContractEndDate", date_col("ValidTo")),
            ("IsCurrentRow", "ContractEndDate >= GETDATE()", Column("IsCurrentRow", "bool")),
            ("RowVersion", "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1", int_col("RowVersion")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Contract Change",
        [
            ("Orphan Contract", "ISNULL(SupplierKey)"),
            ("New Contract", "ISNULL(VendorContractKey)"),
            ("Amended Contract", "SourceRowHash != CurrentRowHash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Contract", CONN_DW, "[Dimension].[Vendor Contract]",
                            "Route Contract Change", "New Contract")
    flow.branch_destination("Insert Amended Version", CONN_DW, "[Dimension].[Vendor Contract]",
                            "Route Contract Change", "Amended Contract")
    flow.branch_destination("Reject Orphan Contract", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Contract Change", "Orphan Contract")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedSupplier]", "stg VendorContract")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    load = pkg.add(DataFlowTask(flow))
    expire = pkg.add(
        ExecuteSql(
            "Expire Lapsed Contracts",
            CONN_DW,
            "UPDATE [Dimension].[Vendor Contract] "
            "SET [Is Current Row] = 0, [Contract Status Code] = N'EXPIRED', "
            "    [Valid To] = CASE WHEN [Valid To] > GETDATE() THEN GETDATE() ELSE [Valid To] END, "
            "    [Closed By Lineage Key] = ? "
            "WHERE [Is Current Row] = 1 AND [Contract End Date] < CAST(GETDATE() AS date);",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    merge = pkg.add(
        exec_proc(
            "Apply Contract Attribute Overwrites",
            "EXEC Integration.MigrateStagedVendorContractData @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    rejects = pkg.add(_log_rejects("Dimension.Vendor Contract"))
    counts = pkg.add(log_row_count("Dimension.Vendor Contract"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, load, expire, merge, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Product and inventory side
# ---------------------------------------------------------------------------


def build_dim_load_stock_item():
    pkg = _dim_package(
        "DIM_Load_StockItem",
        "Hybrid SCD load of Dimension.Stock Item: marketing attributes overwrite in place, "
        "commercial attributes version, and inferred members created by the fact loads are "
        "enriched here on their first appearance in staging.",
        extra_variables=[("InferredEnriched", 0, "int")],
    )
    columns = [
        int_col("StockItemBusinessKey"),
        str_col("StockItemName", 120),
        str_col("MarketingComments", 400),
        str_col("ProductCategoryCode", 10),
        str_col("SizeCode", 12),
        str_col("BrandCode", 12),
        money_col("UnitPrice"),
        money_col("RecommendedRetailPrice"),
        money_col("StandardUnitCost"),
        int_col("QuantityPerOuter"),
        Column("IsChillerStock", "bool"),
        Column("TaxRate", "numeric", precision=18, scale=3),
        str_col("Type1Hash", 64),
        str_col("Type2Hash", 64),
    ]
    flow = DataFlow("Load Stock Item Hybrid SCD", "Type 1 marketing / Type 2 commercial split")
    flow.oledb_source(
        "stg StockItem",
        CONN_STAGING,
        "SELECT si.StockItemBusinessKey, si.StockItemName, si.MarketingComments, si.ProductCategoryCode, "
        "si.SizeCode, si.BrandCode, si.UnitPrice, si.RecommendedRetailPrice, si.StandardUnitCost, "
        "si.QuantityPerOuter, si.IsChillerStock, si.TaxRate, "
        "si.Type1AttributeHash AS Type1Hash, si.Type2AttributeHash AS Type2Hash "
        "FROM stg.StockItem AS si;",
        columns,
        timeout=1200,
    )
    flow.lookup(
        "Lookup Current Stock Item Version",
        CONN_DW,
        "SELECT [Stock Item Key] AS StockItemKey, [WWI Stock Item ID] AS StockItemBusinessKey, "
        "[Type 1 Hash] AS CurrentType1Hash, [Type 2 Hash] AS CurrentType2Hash, "
        "[Row Version] AS CurrentRowVersion, [Is Inferred Member] AS IsInferredMember "
        "FROM [Dimension].[Stock Item] WHERE [Is Current Row] = 1;",
        ["StockItemBusinessKey"],
        [
            int_col("StockItemKey"),
            str_col("CurrentType1Hash", 64),
            str_col("CurrentType2Hash", 64),
            int_col("CurrentRowVersion"),
            Column("IsInferredMember", "bool"),
        ],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Stock Item Measures",
        [
            (
                "GrossMarginPercent",
                "UnitPrice > 0 ? ((UnitPrice - StandardUnitCost) / UnitPrice) * 100 "
                ": (DT_NUMERIC,18,2)0",
                Column("GrossMarginPercent", "numeric", precision=9, scale=4),
            ),
            (
                "PriceBandCode",
                'UnitPrice >= 100 ? "P5" : UnitPrice >= 50 ? "P4" : UnitPrice >= 20 ? "P3" : '
                'UnitPrice >= 5 ? "P2" : "P1"',
                str_col("PriceBandCode", 2),
            ),
            (
                "HandlingCode",
                'IsChillerStock ? "CHILL" : QuantityPerOuter > 48 ? "PALLET" : "AMBIENT"',
                str_col("HandlingCode", 8),
            ),
            ("ValidFrom", "GETDATE()", date_col("ValidFrom")),
            ("ValidTo", '(DT_DBTIMESTAMP)"9999-12-31 23:59:59"', date_col("ValidTo")),
            ("IsCurrentRow", "(DT_BOOL)1", Column("IsCurrentRow", "bool")),
            ("RowVersion", "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1", int_col("RowVersion")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Stock Item Change",
        [
            ("Enrich Inferred Member", "!ISNULL(StockItemKey) && IsInferredMember"),
            ("New Stock Item", "ISNULL(StockItemKey)"),
            ("Type 2 Commercial Change", "Type2Hash != CurrentType2Hash"),
            ("Type 1 Marketing Change", "Type1Hash != CurrentType1Hash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Stock Item", CONN_DW, "[Dimension].[Stock Item]",
                            "Route Stock Item Change", "New Stock Item")
    flow.branch_destination("Insert Type 2 Version", CONN_DW, "[Dimension].[Stock Item]",
                            "Route Stock Item Change", "Type 2 Commercial Change")
    flow.branch_destination("Stage Type 1 Overwrite", CONN_STAGING, "[work].[ProductCrosswalk]",
                            "Route Stock Item Change", "Type 1 Marketing Change")
    flow.branch_destination("Stage Inferred Enrichment", CONN_STAGING, "[work].[LateArrivingDimensionQueue]",
                            "Route Stock Item Change", "Enrich Inferred Member")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedProduct]", "stg StockItem")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Stock Item';",
            connection=CONN_DW,
        )
    )
    clear = pkg.add(truncate("[work].[ProductCrosswalk]", connection=CONN_STAGING,
                             name="Clear Product Crosswalk"))
    load = pkg.add(DataFlowTask(flow))
    close_out = pkg.add(
        ExecuteSql(
            "Close Out Superseded Versions",
            CONN_DW,
            "UPDATE prior SET prior.[Valid To] = DATEADD(SECOND, -1, current_row.[Valid From]), "
            "prior.[Is Current Row] = 0 "
            "FROM [Dimension].[Stock Item] AS prior "
            "INNER JOIN [Dimension].[Stock Item] AS current_row "
            "    ON current_row.[WWI Stock Item ID] = prior.[WWI Stock Item ID] "
            "   AND current_row.[Row Version] = prior.[Row Version] + 1 "
            "WHERE prior.[Is Current Row] = 1;",
        )
    )
    type1 = pkg.add(
        exec_proc(
            "Apply Marketing Overwrites",
            "EXEC Integration.MigrateStagedStockItemData @ApplyType1 = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    inferred = pkg.add(
        ExecuteSql(
            "Enrich Inferred Stock Items",
            CONN_DW,
            "UPDATE si SET si.[Is Inferred Member] = 0, si.[Stock Item] = q.[Attribute Value], "
            "    si.[Enriched By Lineage Key] = ? "
            "FROM [Dimension].[Stock Item] AS si "
            "INNER JOIN [Integration].[LateArrivingMemberQueue] AS q "
            "    ON q.[Dimension Name] = N'Stock Item' "
            "   AND q.[Business Key] = CAST(si.[WWI Stock Item ID] AS nvarchar(40)) "
            "WHERE si.[Is Inferred Member] = 1;",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    counts = pkg.add(log_row_count("Dimension.Stock Item"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, clear, load, close_out, type1, inferred, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# People
# ---------------------------------------------------------------------------


def build_dim_load_employee():
    pkg = _dim_package(
        "DIM_Load_Employee",
        "SCD Type 2 load of Dimension.Employee including the self-referencing manager "
        "hierarchy and the manager-key re-keying pass that follows the insert.",
        extra_variables=[("ManagerKeysRepaired", 0, "int")],
    )
    columns = [
        int_col("EmployeeBusinessKey"),
        str_col("EmployeeFullName", 100),
        str_col("PreferredName", 60),
        str_col("JobTitleCode", 12),
        int_col("ManagerBusinessKey"),
        str_col("DepartmentCode", 10),
        str_col("EmploymentTypeCode", 6),
        date_col("HireDate"),
        date_col("TerminationDate"),
        Column("IsSalesperson", "bool"),
        str_col("PayrollRegionCode", 6),
        str_col("SourceRowHash", 64),
    ]
    flow = DataFlow("Load Employee Versions", "SCD2 with deferred manager key resolution")
    flow.oledb_source(
        "stg Employee",
        CONN_STAGING,
        "SELECT e.EmployeeBusinessKey, e.EmployeeFullName, e.PreferredName, e.JobTitleCode, "
        "e.ManagerBusinessKey, e.DepartmentCode, e.EmploymentTypeCode, e.HireDate, e.TerminationDate, "
        "e.IsSalesperson, e.PayrollRegionCode, e.SourceRowHash "
        "FROM stg.Employee AS e ORDER BY e.ManagerBusinessKey, e.EmployeeBusinessKey;",
        columns,
    )
    flow.lookup(
        "Lookup Current Employee Version",
        CONN_DW,
        "SELECT [Employee Key] AS EmployeeKey, [WWI Employee ID] AS EmployeeBusinessKey, "
        "[Source Row Hash] AS CurrentRowHash, [Row Version] AS CurrentRowVersion "
        "FROM [Dimension].[Employee] WHERE [Is Current Row] = 1;",
        ["EmployeeBusinessKey"],
        [int_col("EmployeeKey"), str_col("CurrentRowHash", 64), int_col("CurrentRowVersion")],
        no_match="RD",
    )
    flow.lookup(
        "Lookup Manager Key",
        CONN_DW,
        "SELECT [Employee Key] AS ManagerKey, [WWI Employee ID] AS ManagerBusinessKey "
        "FROM [Dimension].[Employee] WHERE [Is Current Row] = 1;",
        ["ManagerBusinessKey"],
        [int_col("ManagerKey")],
        no_match="IG",
    )
    flow.derived_column(
        "Derive Employment Attributes",
        [
            # A manager hired in the same run has no key yet; -1 is repaired by
            # the re-keying statement after the data flow.
            ("ManagerKeyResolved", "ISNULL(ManagerKey) ? -1 : ManagerKey", int_col("ManagerKeyResolved")),
            (
                "EmploymentStatusCode",
                'ISNULL(TerminationDate) ? "ACTIVE" : TerminationDate > GETDATE() ? "NOTICE" : "LEAVER"',
                str_col("EmploymentStatusCode", 8),
            ),
            (
                "TenureYears",
                'DATEDIFF("Day", HireDate, ISNULL(TerminationDate) ? GETDATE() : TerminationDate) / 365',
                int_col("TenureYears"),
            ),
            (
                "PayrollCalendarCode",
                'PayrollRegionCode == "NA" ? "SEMI-MONTHLY" : PayrollRegionCode == "EU" ? "MONTHLY-EOM" '
                ': "MONTHLY-25TH"',
                str_col("PayrollCalendarCode", 16),
            ),
            ("ValidFrom", "GETDATE()", date_col("ValidFrom")),
            ("ValidTo", '(DT_DBTIMESTAMP)"9999-12-31 23:59:59"', date_col("ValidTo")),
            ("IsCurrentRow", "(DT_BOOL)1", Column("IsCurrentRow", "bool")),
            ("RowVersion", "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1", int_col("RowVersion")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Employee Change",
        [
            ("New Employee", "ISNULL(EmployeeKey)"),
            ("Type 2 Change", "SourceRowHash != CurrentRowHash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Employee", CONN_DW, "[Dimension].[Employee]",
                            "Route Employee Change", "New Employee")
    flow.branch_destination("Insert Type 2 Version", CONN_DW, "[Dimension].[Employee]",
                            "Route Employee Change", "Type 2 Change")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg Employee")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Employee';",
            connection=CONN_DW,
        )
    )
    load = pkg.add(DataFlowTask(flow))
    close_out = pkg.add(
        ExecuteSql(
            "Close Out Superseded Versions",
            CONN_DW,
            "UPDATE prior SET prior.[Valid To] = DATEADD(SECOND, -1, current_row.[Valid From]), "
            "prior.[Is Current Row] = 0 "
            "FROM [Dimension].[Employee] AS prior "
            "INNER JOIN [Dimension].[Employee] AS current_row "
            "    ON current_row.[WWI Employee ID] = prior.[WWI Employee ID] "
            "   AND current_row.[Row Version] = prior.[Row Version] + 1 "
            "WHERE prior.[Is Current Row] = 1;",
        )
    )
    rekey = pkg.add(
        ExecuteSql(
            "Repair Manager Keys",
            CONN_DW,
            "UPDATE e SET e.[Manager Key] = m.[Employee Key], e.[Rekeyed By Lineage Key] = ? "
            "FROM [Dimension].[Employee] AS e "
            "INNER JOIN [Dimension].[Employee] AS m "
            "    ON m.[WWI Employee ID] = e.[Manager Employee ID] AND m.[Is Current Row] = 1 "
            "WHERE e.[Manager Key] = -1 AND e.[Is Current Row] = 1;",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    depth = pkg.add(
        ExecuteSql(
            "Rebuild Reporting Depth",
            CONN_DW,
            "WITH chain AS ("
            " SELECT [Employee Key], [Manager Key], 1 AS Depth FROM [Dimension].[Employee] "
            "  WHERE [Manager Key] IN (-1, 0) AND [Is Current Row] = 1 "
            " UNION ALL "
            " SELECT e.[Employee Key], e.[Manager Key], chain.Depth + 1 "
            " FROM [Dimension].[Employee] AS e "
            " INNER JOIN chain ON chain.[Employee Key] = e.[Manager Key] "
            " WHERE e.[Is Current Row] = 1) "
            "UPDATE e SET e.[Reporting Depth] = chain.Depth "
            "FROM [Dimension].[Employee] AS e INNER JOIN chain ON chain.[Employee Key] = e.[Employee Key] "
            "OPTION (MAXRECURSION 32);",
        )
    )
    counts = pkg.add(log_row_count("Dimension.Employee"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, load, close_out, rekey, depth, counts, done)
    return _write(pkg)


def build_dim_load_salesperson():
    pkg = _dim_package(
        "DIM_Load_Salesperson",
        "SCD Type 2 load of Dimension.Salesperson with quota attributes and maintenance of the "
        "many-to-many salesperson-to-territory bridge.",
        extra_variables=[("BridgeRowsWritten", 0, "int")],
        extra_parameters=[
            ("QuotaFiscalYear", "FY2024", "string", "Fiscal year label the quota attributes belong to."),
        ],
    )
    columns = [
        int_col("SalespersonBusinessKey"),
        str_col("SalespersonName", 100),
        str_col("SalesTerritoryCode", 10),
        str_col("SalesChannelCode", 8),
        money_col("AnnualQuotaAmount"),
        str_col("QuotaCurrency", 3),
        Column("CommissionRatePercent", "numeric", precision=5, scale=2),
        str_col("RegionCode", 6),
        date_col("EffectiveFromDate"),
        str_col("SourceRowHash", 64),
    ]
    flow = DataFlow("Load Salesperson Versions", "SCD2 with quota and commission attributes")
    flow.oledb_source(
        "stg Salesperson",
        CONN_STAGING,
        "SELECT sp.SalespersonBusinessKey, sp.SalespersonName, sp.SalesTerritoryCode, sp.SalesChannelCode, "
        "sp.AnnualQuotaAmount, sp.QuotaCurrency, sp.CommissionRatePercent, sp.RegionCode, "
        "sp.EffectiveFromDate, sp.SourceRowHash "
        "FROM stg.Salesperson AS sp WHERE sp.IsActive = 1 OR sp.EffectiveFromDate >= DATEADD(YEAR, -2, GETDATE());",
        columns,
    )
    flow.lookup(
        "Lookup Current Salesperson Version",
        CONN_DW,
        "SELECT [Salesperson Key] AS SalespersonKey, [WWI Salesperson ID] AS SalespersonBusinessKey, "
        "[Source Row Hash] AS CurrentRowHash, [Row Version] AS CurrentRowVersion "
        "FROM [Dimension].[Salesperson] WHERE [Is Current Row] = 1;",
        ["SalespersonBusinessKey"],
        [int_col("SalespersonKey"), str_col("CurrentRowHash", 64), int_col("CurrentRowVersion")],
        no_match="RD",
    )
    flow.lookup(
        "Lookup Territory Key",
        CONN_DW,
        "SELECT [Sales Territory Key] AS SalesTerritoryKey, [WWI Territory ID] AS SalesTerritoryCode "
        "FROM [Dimension].[Sales Territory];",
        ["SalesTerritoryCode"],
        [int_col("SalesTerritoryKey")],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Quota Attributes",
        [
            # Quotas are held in local currency; the reporting quota is only
            # populated for USD until the FX pass runs at month end.
            (
                "AnnualQuotaReporting",
                'QuotaCurrency == "USD" ? AnnualQuotaAmount : (DT_NUMERIC,18,2)NULL(DT_NUMERIC,18,2)',
                money_col("AnnualQuotaReporting"),
            ),
            (
                "QuotaBandCode",
                'AnnualQuotaAmount >= 5000000 ? "Q4" : AnnualQuotaAmount >= 2000000 ? "Q3" : '
                'AnnualQuotaAmount >= 500000 ? "Q2" : "Q1"',
                str_col("QuotaBandCode", 2),
            ),
            (
                "CommissionSchemeCode",
                'RegionCode == "NA" ? "ACCEL-NA" : RegionCode == "EU" ? "FLAT-EU" : "TIERED-APAC"',
                str_col("CommissionSchemeCode", 12),
            ),
            ("ValidFrom", "EffectiveFromDate", date_col("ValidFrom")),
            ("ValidTo", '(DT_DBTIMESTAMP)"9999-12-31 23:59:59"', date_col("ValidTo")),
            ("IsCurrentRow", "(DT_BOOL)1", Column("IsCurrentRow", "bool")),
            ("RowVersion", "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1", int_col("RowVersion")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Salesperson Change",
        [
            ("Unknown Territory", "ISNULL(SalesTerritoryKey)"),
            ("New Salesperson", "ISNULL(SalespersonKey)"),
            ("Type 2 Change", "SourceRowHash != CurrentRowHash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Salesperson", CONN_DW, "[Dimension].[Salesperson]",
                            "Route Salesperson Change", "New Salesperson")
    flow.branch_destination("Insert Type 2 Version", CONN_DW, "[Dimension].[Salesperson]",
                            "Route Salesperson Change", "Type 2 Change")
    flow.branch_destination("Reject Unknown Territory", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Salesperson Change", "Unknown Territory")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg Salesperson")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Salesperson';",
            connection=CONN_DW,
        )
    )
    load = pkg.add(DataFlowTask(flow))
    close_out = pkg.add(
        ExecuteSql(
            "Close Out Superseded Versions",
            CONN_DW,
            "UPDATE prior SET prior.[Valid To] = DATEADD(SECOND, -1, current_row.[Valid From]), "
            "prior.[Is Current Row] = 0 "
            "FROM [Dimension].[Salesperson] AS prior "
            "INNER JOIN [Dimension].[Salesperson] AS current_row "
            "    ON current_row.[WWI Salesperson ID] = prior.[WWI Salesperson ID] "
            "   AND current_row.[Row Version] = prior.[Row Version] + 1 "
            "WHERE prior.[Is Current Row] = 1;",
        )
    )
    bridge = pkg.add(
        ExecuteSql(
            "Maintain Salesperson Territory Bridge",
            CONN_DW,
            "UPDATE b SET b.[Is Current Assignment] = 0, b.[Assignment Ended] = GETDATE() "
            "FROM [Dimension].[Salesperson Territory Bridge] AS b "
            "INNER JOIN [Dimension].[Salesperson] AS sp ON sp.[Salesperson Key] = b.[Salesperson Key] "
            "WHERE b.[Is Current Assignment] = 1 AND sp.[Is Current Row] = 0; "
            "INSERT INTO [Dimension].[Salesperson Territory Bridge] "
            "    ([Salesperson Key], [Sales Territory Key], [Allocation Percent], "
            "     [Is Current Assignment], [Assignment Started], [Lineage Key]) "
            "SELECT sp.[Salesperson Key], t.[Sales Territory Key], 100.0, 1, GETDATE(), ? "
            "FROM [Dimension].[Salesperson] AS sp "
            "INNER JOIN [Dimension].[Sales Territory] AS t "
            "    ON t.[WWI Territory ID] = sp.[Sales Territory Code] "
            "WHERE sp.[Is Current Row] = 1 "
            "  AND NOT EXISTS (SELECT 1 FROM [Dimension].[Salesperson Territory Bridge] AS x "
            "                  WHERE x.[Salesperson Key] = sp.[Salesperson Key] "
            "                    AND x.[Sales Territory Key] = t.[Sales Territory Key] "
            "                    AND x.[Is Current Assignment] = 1);",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    rejects = pkg.add(_log_rejects("Dimension.Salesperson"))
    counts = pkg.add(log_row_count("Dimension.Salesperson"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, load, close_out, bridge, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Geography, segmentation and marketing
# ---------------------------------------------------------------------------


def build_dim_load_city():
    pkg = _dim_package(
        "DIM_Load_City",
        "SCD Type 2 load of Dimension.City. This is the dimension the fact loads most often "
        "outrun, so it also promotes the inferred members the fact packages created.",
        extra_variables=[("InferredPromoted", 0, "int")],
        extra_parameters=[
            ("PopulationChangeThreshold", 5, "int",
             "Percentage population change that triggers a new version rather than an overwrite."),
        ],
    )
    columns = [
        int_col("CityBusinessKey"),
        str_col("CityName", 60),
        str_col("StateProvinceCode", 10),
        str_col("CountryIsoCode", 2),
        str_col("ContinentName", 30),
        str_col("SalesTerritoryCode", 10),
        int_col("LatestRecordedPopulation"),
        str_col("PostalCodePrefix", 6),
        Column("LatitudeDegrees", "numeric", precision=9, scale=6),
        Column("LongitudeDegrees", "numeric", precision=9, scale=6),
        str_col("SourceRowHash", 64),
    ]
    flow = DataFlow("Load City Versions", "SCD2 with population-change thresholding")
    flow.oledb_source(
        "stg City",
        CONN_STAGING,
        "SELECT c.CityBusinessKey, c.CityName, c.StateProvinceCode, c.CountryIsoCode, c.ContinentName, "
        "c.SalesTerritoryCode, c.LatestRecordedPopulation, c.PostalCodePrefix, "
        "c.LatitudeDegrees, c.LongitudeDegrees, c.SourceRowHash "
        "FROM stg.Geography AS c WHERE c.GeographyLevelCode = N'CITY';",
        columns,
        timeout=2400,
    )
    flow.lookup(
        "Lookup Current City Version",
        CONN_DW,
        "SELECT [City Key] AS CityKey, [WWI City ID] AS CityBusinessKey, "
        "[Source Row Hash] AS CurrentRowHash, [Row Version] AS CurrentRowVersion, "
        "[Latest Recorded Population] AS CurrentPopulation, [Is Inferred Member] AS IsInferredMember "
        "FROM [Dimension].[City] WHERE [Is Current Row] = 1;",
        ["CityBusinessKey"],
        [
            int_col("CityKey"),
            str_col("CurrentRowHash", 64),
            int_col("CurrentRowVersion"),
            int_col("CurrentPopulation"),
            Column("IsInferredMember", "bool"),
        ],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Geography Attributes",
        [
            (
                "PopulationChangePercent",
                "ISNULL(CurrentPopulation) || CurrentPopulation == 0 ? 100 : "
                "(ABS(LatestRecordedPopulation - CurrentPopulation) * 100) / CurrentPopulation",
                int_col("PopulationChangePercent"),
            ),
            (
                "PostalStandardCode",
                'CountryIsoCode == "US" ? "ZIP" : CountryIsoCode == "CA" ? "FSA" : '
                'CountryIsoCode == "GB" ? "OUTCODE" : CountryIsoCode == "JP" ? "JIS" : "GENERIC"',
                str_col("PostalStandardCode", 10),
            ),
            (
                "CityRegionCode",
                'ContinentName == "North America" ? "NA" : '
                'ContinentName == "Europe" ? "EU" : '
                'ContinentName == "Asia" || ContinentName == "Oceania" ? "APAC" : "ROW"',
                str_col("CityRegionCode", 6),
            ),
            ("ValidFrom", "GETDATE()", date_col("ValidFrom")),
            ("ValidTo", '(DT_DBTIMESTAMP)"9999-12-31 23:59:59"', date_col("ValidTo")),
            ("IsCurrentRow", "(DT_BOOL)1", Column("IsCurrentRow", "bool")),
            ("RowVersion", "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1", int_col("RowVersion")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route City Change",
        [
            ("Promote Inferred City", "!ISNULL(CityKey) && IsInferredMember"),
            ("New City", "ISNULL(CityKey)"),
            (
                "Type 2 Version Change",
                "SourceRowHash != CurrentRowHash && "
                "PopulationChangePercent >= @[$Package::PopulationChangeThreshold]",
            ),
            ("Minor Correction", "SourceRowHash != CurrentRowHash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New City", CONN_DW, "[Dimension].[City]",
                            "Route City Change", "New City")
    flow.branch_destination("Insert Type 2 Version", CONN_DW, "[Dimension].[City]",
                            "Route City Change", "Type 2 Version Change")
    flow.branch_destination("Stage Minor Correction", CONN_STAGING, "[work].[CustomerAddressStandardized]",
                            "Route City Change", "Minor Correction")
    flow.branch_destination("Stage Inferred Promotion", CONN_STAGING, "[work].[LateArrivingDimensionQueue]",
                            "Route City Change", "Promote Inferred City")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]", "stg City")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'City';",
            connection=CONN_DW,
        )
    )
    clear = pkg.add(truncate("[work].[CustomerAddressStandardized]", connection=CONN_STAGING,
                             name="Clear Address Work Table"))
    load = pkg.add(DataFlowTask(flow))
    close_out = pkg.add(
        ExecuteSql(
            "Close Out Superseded Versions",
            CONN_DW,
            "UPDATE prior SET prior.[Valid To] = DATEADD(SECOND, -1, current_row.[Valid From]), "
            "prior.[Is Current Row] = 0 "
            "FROM [Dimension].[City] AS prior "
            "INNER JOIN [Dimension].[City] AS current_row "
            "    ON current_row.[WWI City ID] = prior.[WWI City ID] "
            "   AND current_row.[Row Version] = prior.[Row Version] + 1 "
            "WHERE prior.[Is Current Row] = 1;",
        )
    )
    promote = pkg.add(
        exec_proc(
            "Promote Inferred Cities",
            "EXEC Integration.MigrateStagedCityData @PromoteInferredMembers = 1, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    counts = pkg.add(log_row_count("Dimension.City"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, clear, load, close_out, promote, counts, done)
    return _write(pkg)


def build_dim_load_customer_segment():
    pkg = _dim_package(
        "DIM_Load_CustomerSegment",
        "SCD Type 2 load of Dimension.Customer Segment. Segment membership is recalculated from "
        "RFM scores every run, so the package versions the segment definition and records the "
        "migration of customers between segments.",
        extra_variables=[("SegmentMigrations", 0, "int")],
        extra_parameters=[
            ("ScoringModelVersion", "RFM-2019.3", "string",
             "Scoring model release the segment boundaries came from."),
        ],
    )
    columns = [
        str_col("SegmentCode", 10),
        str_col("SegmentName", 60),
        int_col("RecencyScoreFloor"),
        int_col("FrequencyScoreFloor"),
        money_col("MonetaryValueFloor"),
        str_col("RegionCode", 6),
        str_col("ChurnRiskBand", 4),
        str_col("SourceRowHash", 64),
    ]
    flow = DataFlow("Load Customer Segment Versions", "SCD2 driven by the RFM scoring model")
    flow.oledb_source(
        "stg CustomerSegment",
        CONN_STAGING,
        "SELECT cs.SegmentCode, cs.SegmentName, cs.RecencyScoreFloor, cs.FrequencyScoreFloor, "
        "cs.MonetaryValueFloor, cs.RegionCode, cs.ChurnRiskBand, cs.SourceRowHash "
        "FROM stg.CustomerSegment AS cs ORDER BY cs.RegionCode, cs.SegmentCode;",
        columns,
    )
    flow.lookup(
        "Lookup Current Segment Version",
        CONN_DW,
        "SELECT [Customer Segment Key] AS CustomerSegmentKey, [WWI Segment ID] AS SegmentCode, "
        "[Source Row Hash] AS CurrentRowHash, [Row Version] AS CurrentRowVersion "
        "FROM [Dimension].[Customer Segment] WHERE [Is Current Row] = 1;",
        ["SegmentCode"],
        [int_col("CustomerSegmentKey"), str_col("CurrentRowHash", 64), int_col("CurrentRowVersion")],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Segment Boundaries",
        [
            (
                "MonetaryFloorReporting",
                'RegionCode == "EU" ? MonetaryValueFloor * (DT_NUMERIC,18,2)1.08 : '
                'RegionCode == "APAC" ? MonetaryValueFloor * (DT_NUMERIC,18,2)0.74 : MonetaryValueFloor',
                money_col("MonetaryFloorReporting"),
            ),
            (
                "SegmentTierCode",
                'RecencyScoreFloor >= 4 && FrequencyScoreFloor >= 4 ? "PLATINUM" : '
                'RecencyScoreFloor >= 3 ? "GOLD" : RecencyScoreFloor >= 2 ? "SILVER" : "BRONZE"',
                str_col("SegmentTierCode", 10),
            ),
            (
                "ChurnWatchFlag",
                'ChurnRiskBand == "HIGH" || ChurnRiskBand == "CRIT"',
                Column("ChurnWatchFlag", "bool"),
            ),
            ("ScoringModelVersion", "@[$Package::ScoringModelVersion]", str_col("ScoringModelVersion", 20)),
            ("ValidFrom", "GETDATE()", date_col("ValidFrom")),
            ("ValidTo", '(DT_DBTIMESTAMP)"9999-12-31 23:59:59"', date_col("ValidTo")),
            ("IsCurrentRow", "(DT_BOOL)1", Column("IsCurrentRow", "bool")),
            ("RowVersion", "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1", int_col("RowVersion")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Segment Change",
        [
            ("New Segment", "ISNULL(CustomerSegmentKey)"),
            ("Redefined Segment", "SourceRowHash != CurrentRowHash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Segment", CONN_DW, "[Dimension].[Customer Segment]",
                            "Route Segment Change", "New Segment")
    flow.branch_destination("Insert Redefined Segment", CONN_DW, "[Dimension].[Customer Segment]",
                            "Route Segment Change", "Redefined Segment")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg CustomerSegment")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    load = pkg.add(DataFlowTask(flow))
    close_out = pkg.add(
        ExecuteSql(
            "Close Out Superseded Versions",
            CONN_DW,
            "UPDATE prior SET prior.[Valid To] = DATEADD(SECOND, -1, current_row.[Valid From]), "
            "prior.[Is Current Row] = 0 "
            "FROM [Dimension].[Customer Segment] AS prior "
            "INNER JOIN [Dimension].[Customer Segment] AS current_row "
            "    ON current_row.[WWI Segment ID] = prior.[WWI Segment ID] "
            "   AND current_row.[Row Version] = prior.[Row Version] + 1 "
            "WHERE prior.[Is Current Row] = 1;",
        )
    )
    reassign = pkg.add(
        exec_proc(
            "Reassign Customers To Segments",
            "EXEC Integration.MigrateStagedCustomerSegmentData @ScoringModelVersion = ?, @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[
                ("$Package::ScoringModelVersion", 0, "NVARCHAR"),
                ("User::PackageExecutionId", 1, "LONG"),
            ],
        )
    )
    counts = pkg.add(log_row_count("Dimension.Customer Segment"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, load, close_out, reassign, counts, done)
    return _write(pkg)


def build_dim_load_promotion():
    pkg = _dim_package(
        "DIM_Load_Promotion",
        "SCD Type 2 load of Dimension.Promotion. Campaign windows overlap, and the discount "
        "interacts with tax differently in each region, so the regional treatment is derived here "
        "rather than in the fact loads.",
        extra_variables=[("OverlappingCampaigns", 0, "int")],
    )
    columns = [
        str_col("PromotionBusinessKey", 20),
        str_col("PromotionName", 100),
        str_col("PromotionTypeCode", 8),
        str_col("RegionCode", 6),
        Column("DiscountPercent", "numeric", precision=5, scale=2),
        money_col("DiscountAmount"),
        str_col("DiscountCurrency", 3),
        date_col("CampaignStartDate"),
        date_col("CampaignEndDate"),
        str_col("FundingSourceCode", 8),
        str_col("SourceRowHash", 64),
    ]
    flow = DataFlow("Load Promotion Versions", "SCD2 over overlapping campaign windows")
    flow.oledb_source(
        "stg Promotion",
        CONN_STAGING,
        "SELECT p.PromotionBusinessKey, p.PromotionName, p.PromotionTypeCode, p.RegionCode, "
        "p.DiscountPercent, p.DiscountAmount, p.DiscountCurrency, p.CampaignStartDate, "
        "p.CampaignEndDate, p.FundingSourceCode, p.SourceRowHash "
        "FROM stg.Promotion AS p WHERE p.CampaignEndDate >= DATEADD(MONTH, -18, GETDATE());",
        columns,
    )
    flow.lookup(
        "Lookup Current Promotion Version",
        CONN_DW,
        "SELECT [Promotion Key] AS PromotionKey, [WWI Promotion ID] AS PromotionBusinessKey, "
        "[Source Row Hash] AS CurrentRowHash, [Row Version] AS CurrentRowVersion "
        "FROM [Dimension].[Promotion] WHERE [Is Current Row] = 1;",
        ["PromotionBusinessKey"],
        [int_col("PromotionKey"), str_col("CurrentRowHash", 64), int_col("CurrentRowVersion")],
        no_match="RD",
    )
    flow.derived_column(
        "Derive Regional Discount Treatment",
        [
            # NA applies sales tax on the discounted price, the EU applies VAT
            # on the net after discount but reports the gross discount, and
            # APAC GST is calculated on the undiscounted price for some states.
            (
                "TaxTreatmentCode",
                'RegionCode == "NA" ? "TAX-AFTER-DISCOUNT" : RegionCode == "EU" ? "VAT-ON-NET" '
                ': "GST-ON-GROSS"',
                str_col("TaxTreatmentCode", 20),
            ),
            (
                "DiscountBasisCode",
                'DiscountPercent > 0 && DiscountAmount > 0 ? "BOTH" : '
                'DiscountPercent > 0 ? "PERCENT" : DiscountAmount > 0 ? "AMOUNT" : "NONE"',
                str_col("DiscountBasisCode", 8),
            ),
            (
                "CampaignDurationDays",
                'DATEDIFF("Day", CampaignStartDate, CampaignEndDate)',
                int_col("CampaignDurationDays"),
            ),
            (
                "IsCoFunded",
                'FundingSourceCode == "SUPP" || FundingSourceCode == "JOINT"',
                Column("IsCoFunded", "bool"),
            ),
            ("ValidFrom", "CampaignStartDate", date_col("ValidFrom")),
            ("ValidTo", "CampaignEndDate", date_col("ValidTo")),
            ("IsCurrentRow", "CampaignEndDate >= GETDATE()", Column("IsCurrentRow", "bool")),
            ("RowVersion", "ISNULL(CurrentRowVersion) ? 1 : CurrentRowVersion + 1", int_col("RowVersion")),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Promotion Change",
        [
            ("Invalid Window", "CampaignEndDate < CampaignStartDate"),
            ("New Promotion", "ISNULL(PromotionKey)"),
            ("Amended Promotion", "SourceRowHash != CurrentRowHash"),
        ],
        default_output="Unchanged",
    )
    flow.branch_destination("Insert New Promotion", CONN_DW, "[Dimension].[Promotion]",
                            "Route Promotion Change", "New Promotion")
    flow.branch_destination("Insert Amended Promotion", CONN_DW, "[Dimension].[Promotion]",
                            "Route Promotion Change", "Amended Promotion")
    flow.branch_destination("Reject Invalid Window", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Route Promotion Change", "Invalid Window")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "stg Promotion")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    unknown = pkg.add(
        exec_proc(
            "Ensure Unknown Member",
            "EXEC Integration.EnsureUnknownMembers @DimensionName = N'Promotion';",
            connection=CONN_DW,
        )
    )
    load = pkg.add(DataFlowTask(flow))
    overlap = pkg.add(
        ExecuteSql(
            "Flag Overlapping Campaigns",
            CONN_DW,
            "UPDATE p SET p.[Has Overlapping Campaign] = 1 "
            "FROM [Dimension].[Promotion] AS p "
            "WHERE EXISTS (SELECT 1 FROM [Dimension].[Promotion] AS o "
            "              WHERE o.[Promotion Key] <> p.[Promotion Key] "
            "                AND o.[Region Code] = p.[Region Code] "
            "                AND o.[Campaign Start Date] <= p.[Campaign End Date] "
            "                AND o.[Campaign End Date] >= p.[Campaign Start Date]);",
        )
    )
    merge = pkg.add(
        exec_proc(
            "Apply Promotion Attribute Overwrites",
            "EXEC Integration.MigrateStagedPromotionData @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    rejects = pkg.add(_log_rejects("Dimension.Promotion"))
    counts = pkg.add(log_row_count("Dimension.Promotion"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, unknown, load, overlap, merge, rejects, counts, done)
    return _write(pkg)


# ---------------------------------------------------------------------------
# Re-keying pass
# ---------------------------------------------------------------------------


def build_dim_rekey_late_arriving():
    pkg = _dim_package(
        "DIM_Rekey_LateArriving",
        "Surrogate-key repair pass. Walks work.LateArrivingDimensionQueue, enriches the inferred "
        "members the fact loads created, then re-points the facts that were loaded against the "
        "unknown member at the newly assigned surrogate keys.",
        extra_variables=[("QueueDepth", 0, "int"), ("FactRowsRekeyed", 0, "int")],
        extra_parameters=[
            ("MaxQueueAgeDays", 30, "int",
             "Queue entries older than this are escalated rather than retried silently."),
        ],
    )

    queue_columns = [
        bigint_col("QueueId"),
        str_col("DimensionName", 40),
        str_col("BusinessKey", 40),
        int_col("PlaceholderKey"),
        str_col("SourcePackageName", 60),
        date_col("QueuedAt"),
        int_col("RetryCount"),
    ]
    flow = DataFlow("Classify Late Arriving Queue", "Split the queue by dimension and age")
    flow.oledb_source(
        "work LateArrivingDimensionQueue",
        CONN_STAGING,
        "SELECT q.QueueId, q.DimensionName, q.BusinessKey, q.PlaceholderKey, q.SourcePackageName, "
        "q.QueuedAt, q.RetryCount "
        "FROM work.LateArrivingDimensionQueue AS q "
        "WHERE q.ResolvedAt IS NULL ORDER BY q.DimensionName, q.QueuedAt;",
        queue_columns,
    )
    flow.derived_column(
        "Derive Queue Age",
        [
            ("QueueAgeDays", 'DATEDIFF("Day", QueuedAt, GETDATE())', int_col("QueueAgeDays")),
            (
                "EscalationCode",
                'DATEDIFF("Day", QueuedAt, GETDATE()) > @[$Package::MaxQueueAgeDays] ? "ESCALATE" : '
                'RetryCount >= 5 ? "MANUAL" : "RETRY"',
                str_col("EscalationCode", 10),
            ),
            ("LineageKey", "@[User::PackageExecutionId]", bigint_col("LineageKey")),
        ],
    )
    flow.conditional_split(
        "Route Queue Entry",
        [
            ("Escalate", 'EscalationCode == "ESCALATE" || EscalationCode == "MANUAL"'),
            ("Customer Members", 'DimensionName == "Customer"'),
            ("Stock Item Members", 'DimensionName == "Stock Item"'),
            ("City Members", 'DimensionName == "City"'),
        ],
        default_output="Other Dimensions",
    )
    flow.branch_destination("Queue Customer Rekey", CONN_STAGING, "[work].[FactRekeyQueue]",
                            "Route Queue Entry", "Customer Members")
    flow.branch_destination("Queue Stock Item Rekey", CONN_STAGING, "[work].[FactRekeyQueue]",
                            "Route Queue Entry", "Stock Item Members")
    flow.branch_destination("Queue City Rekey", CONN_STAGING, "[work].[FactRekeyQueue]",
                            "Route Queue Entry", "City Members")
    flow.branch_destination("Queue Other Rekey", CONN_STAGING, "[work].[FactRekeyQueue]",
                            "Route Queue Entry", "Other Dimensions")
    flow.branch_destination("Escalate Stale Queue Entries", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Route Queue Entry", "Escalate")
    flow.reject_destination("Reject Source Errors", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "work LateArrivingDimensionQueue")

    init = pkg.add(_init_variables())
    start = pkg.add(log_package_start(pkg))
    depth = pkg.add(
        ExecuteSql(
            "Measure Queue Depth",
            CONN_STAGING,
            "SELECT COUNT_BIG(*) AS QueueDepth FROM work.LateArrivingDimensionQueue WHERE ResolvedAt IS NULL;",
            result_type="ResultSetType_SingleRow",
            result_bindings=[("0", "User::QueueDepth")],
        )
    )
    clear = pkg.add(truncate("[work].[FactRekeyQueue]", connection=CONN_STAGING, name="Clear Fact Rekey Queue"))
    classify = pkg.add(DataFlowTask(flow))

    rekey_container = Container("Assign Surrogate Keys", kind="sequence",
                                description="Per-dimension surrogate key assignment")
    assign_keys = rekey_container.add(
        exec_proc(
            "Assign Surrogate Keys To Inferred Members",
            "EXEC Integration.RekeyLateArrivingDimensions @Phase = N'ASSIGN', @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    repoint = rekey_container.add(
        exec_proc(
            "Repoint Facts At Assigned Keys",
            "EXEC Integration.RekeyLateArrivingDimensions @Phase = N'REPOINT', @LineageKey = ?;",
            connection=CONN_DW,
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    verify = rekey_container.add(
        ExecuteSql(
            "Count Remaining Unknown Member Facts",
            CONN_DW,
            "SELECT COUNT_BIG(*) AS RemainingUnknownFacts FROM [Fact].[Sale] WHERE [Customer Key] = 0 "
            "OR [Stock Item Key] = 0 OR [City Key] = 0;",
            result_type="ResultSetType_SingleRow",
            result_bindings=[("0", "User::FactRowsRekeyed")],
        )
    )
    rekey_container.chain(assign_keys, repoint, verify)
    rekey = pkg.add(rekey_container)

    close_queue = pkg.add(
        ExecuteSql(
            "Close Resolved Queue Entries",
            CONN_STAGING,
            "UPDATE q SET q.ResolvedAt = GETDATE(), q.ResolvedByExecutionId = ? "
            "FROM work.LateArrivingDimensionQueue AS q "
            "INNER JOIN work.FactRekeyQueue AS r ON r.QueueId = q.QueueId "
            "WHERE q.ResolvedAt IS NULL AND r.AssignedSurrogateKey IS NOT NULL; "
            "UPDATE work.LateArrivingDimensionQueue SET RetryCount = RetryCount + 1 "
            "WHERE ResolvedAt IS NULL;",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        )
    )
    rejects = pkg.add(_log_rejects("work.LateArrivingDimensionQueue"))
    counts = pkg.add(log_row_count("work.LateArrivingDimensionQueue"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, depth, clear, classify, rekey, close_queue, rejects, counts, done)
    return _write(pkg)


BUILDERS = [
    build_dim_na_load_customer,
    build_dim_eu_load_customer,
    build_dim_apac_load_customer,
    build_dim_load_customer_category,
    build_dim_load_supplier,
    build_dim_load_vendor_contract,
    build_dim_load_stock_item,
    build_dim_load_product_category,
    build_dim_load_employee,
    build_dim_load_salesperson,
    build_dim_load_city,
    build_dim_load_sales_territory,
    build_dim_load_customer_segment,
    build_dim_load_promotion,
    build_dim_rekey_late_arriving,
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
