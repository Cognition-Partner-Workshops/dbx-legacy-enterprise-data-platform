"""Spec module for the WWI_Staging SSIS project (ssis/04_staging).

Emits the twenty-eight staging packages declared in config/estate-catalog.yaml
for folder 04_staging. These packages move raw.* landings into the conformed
stg.* tables and rebuild the work.* scratch sets, integrating with the etl
control framework for batch registration, watermarking, row-count auditing and
reject routing.

Run from the repository root:

    python3 ssis/04_staging/build_staging_packages.py

The generated .dtsx, .dtproj, .conmgr and Project.params files are committed
alongside this module. Nothing here connects to a database; the pipeline column
metadata is declared in the spec exactly as SSIS persists its design-time cache.
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
    get_watermark,
    log_package_start,
    log_package_success,
    log_row_count,
    new_package,
    set_watermark,
    truncate,
)
import project  # noqa: E402

PROJECT_NAME = "WWI_Staging"
PROJECT_CONNECTIONS = ["WWI_Staging_DB", "WWI_Source_DB", "WWI_Oracle_ERP", "WWI_Inbound_Files"]

# Source system codes as seeded in etl.SourceSystem.
ORA = "ORA_ERP"
OLTP = "WWI_OLTP"
FILE = "PARTNER_FILE"

BUILDERS = []


def package(func):
    BUILDERS.append(func)
    return func


# ---------------------------------------------------------------------------
# local helpers
# ---------------------------------------------------------------------------


def bool_col(name):
    return Column(name, "bool")


def dec_col(name, precision=18, scale=6):
    return Column(name, "numeric", precision=precision, scale=scale)


def add_path(flow, start, end):
    """Attach an extra pipeline path (second Union All input, extra branch).

    ssisgen builds one linear chain per data flow; multi-input transforms are
    wired by appending the additional path directly.
    """
    flow._paths.append((start, end))
    return flow


def hash_expression(columns):
    """The house change-hash expression: pipe-delimited, upper-cased, trimmed."""
    parts = []
    for col in columns:
        parts.append('UPPER(TRIM((DT_WSTR,60)[%s]))' % col)
    return " + \"|\" + ".join(parts)


def reject_sweep(err_table, object_name, key_expression, reason_column, stage="Stage"):
    """Row-by-row reject registration.

    The shop has always registered rejects one row at a time through
    etl.usp_LogRejectedRecord; the loop is kept because downstream operational
    reports depend on one control row per rejected business key.
    """
    sql = (
        "DECLARE @RejectId BIGINT, @Key NVARCHAR(200), @Reason NVARCHAR(50);\n"
        "DECLARE reject_cur CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT r.RejectedRowId, %s, r.%s\n"
        "    FROM %s AS r\n"
        "    WHERE r.BatchId = ? AND r.LoggedToControl = 0;\n"
        "OPEN reject_cur;\n"
        "FETCH NEXT FROM reject_cur INTO @RejectId, @Key, @Reason;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    EXEC etl.usp_LogRejectedRecord @PackageExecutionId = ?, @BatchId = ?, "
        "@SourceSystemCode = ?, @ObjectName = N'%s', @BusinessKey = @Key, "
        "@RejectReasonCode = @Reason, @RejectStage = N'%s';\n"
        "    UPDATE %s SET LoggedToControl = 1 WHERE RejectedRowId = @RejectId;\n"
        "    FETCH NEXT FROM reject_cur INTO @RejectId, @Key, @Reason;\n"
        "END\n"
        "CLOSE reject_cur;\n"
        "DEALLOCATE reject_cur;"
        % (key_expression, reason_column, err_table, object_name, stage, err_table)
    )
    return ExecuteSql(
        "Register Rejects - %s" % object_name,
        CONN_STAGING,
        sql,
        parameter_bindings=[
            ("$Package::BatchId", 0, "LONG"),
            ("User::PackageExecutionId", 1, "LONG"),
            ("$Package::BatchId", 2, "LONG"),
            ("$Package::SourceSystemCode", 3, "NVARCHAR"),
        ],
    )


def build_package(name, description, source_system, object_name, flows,
                  extra_variables=None, pre_tasks=(), post_tasks=(), watermark=False,
                  truncate_tables=(), reject_task=None, connections=(CONN_STAGING,)):
    """Assemble a staging package around the house control flow."""
    pkg = new_package(name, description, source_system=source_system,
                      connections=connections, extra_variables=extra_variables or [])
    ordered = [pkg.add(log_package_start(pkg))]
    if watermark:
        ordered.append(pkg.add(get_watermark(object_name=object_name)))
    for table in truncate_tables:
        ordered.append(pkg.add(truncate(table)))
    for task in pre_tasks:
        ordered.append(pkg.add(task))
    for flow in flows:
        ordered.append(pkg.add(DataFlowTask(flow)))
    for task in post_tasks:
        ordered.append(pkg.add(task))
    if reject_task is not None:
        ordered.append(pkg.add(reject_task))
    if watermark:
        ordered.append(pkg.add(set_watermark(object_name)))
    ordered.append(pkg.add(log_row_count(object_name)))
    ordered.append(pkg.add(log_package_success()))
    pkg.chain(*ordered)
    return pkg


# ---------------------------------------------------------------------------
# Master data
# ---------------------------------------------------------------------------


@package
def stg_load_customer():
    """raw.OracleCustomerMaster -> stg.Customer (truncate and reload)."""
    src_cols = [
        str_col("CUST_CODE", 20), str_col("CUST_NAME", 100), str_col("TRADING_NAME", 100),
        str_col("CUST_CLASS_CD", 10), str_col("CREDIT_STATUS_CD", 4), str_col("COUNTRY_CD", 3),
        str_col("REGION_CD", 4), str_col("TAX_REG_NBR", 30), str_col("CONSENT_FLAG", 1),
        money_col("CREDIT_LIMIT_AMT"), str_col("CREDIT_CCY", 3), date_col("CREATED_DT"),
        date_col("LAST_UPD_DT"),
    ]
    flow = DataFlow("DFT Conform Customer", "Cleanse, classify and hash the Oracle customer master")
    flow.oledb_source(
        "RAW Oracle Customer Master", CONN_STAGING,
        "SELECT CUST_CODE, CUST_NAME, TRADING_NAME, CUST_CLASS_CD, CREDIT_STATUS_CD,\n"
        "       COUNTRY_CD, REGION_CD, TAX_REG_NBR, CONSENT_FLAG, CREDIT_LIMIT_AMT,\n"
        "       CREDIT_CCY, CREATED_DT, LAST_UPD_DT\n"
        "FROM raw.OracleCustomerMaster\n"
        "WHERE BatchId = ? AND NULLIF(LTRIM(RTRIM(CUST_CODE)), '') IS NOT NULL;",
        src_cols, timeout=3600)
    flow.row_count("Count Rows Read", "User::RowsRead")
    flow.derived_column("Cleanse Customer Attributes", [
        ("CustomerCode", 'UPPER(TRIM(CUST_CODE))', str_col("CustomerCode", 20)),
        ("CustomerName", 'TRIM(REPLACE(CUST_NAME,"  "," "))', str_col("CustomerName", 100)),
        ("TradingName",
         'TRIM(CUST_NAME) == TRIM(ISNULL(TRADING_NAME) ? "" : TRADING_NAME) ? NULL(DT_WSTR,100) : TRIM(TRADING_NAME)',
         str_col("TradingName", 100)),
        ("CustomerClassCode", 'ISNULL(CUST_CLASS_CD) || TRIM(CUST_CLASS_CD) == "" ? "UNCL" : UPPER(TRIM(CUST_CLASS_CD))',
         str_col("CustomerClassCode", 10)),
        ("CreditStatusCode", 'UPPER(TRIM(ISNULL(CREDIT_STATUS_CD) ? "NEW" : CREDIT_STATUS_CD))',
         str_col("CreditStatusCode", 4)),
        ("CountryCode", 'UPPER(TRIM(COUNTRY_CD))', str_col("CountryCode", 3)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD))', str_col("RegionCode", 4)),
        ("TaxRegistrationNumber", 'UPPER(REPLACE(REPLACE(TRIM(TAX_REG_NBR)," ",""),"-",""))',
         str_col("TaxRegistrationNumber", 30)),
        ("MarketingConsentFlag",
         'UPPER(TRIM(REGION_CD)) == "EU" ? (UPPER(TRIM(ISNULL(CONSENT_FLAG) ? "N" : CONSENT_FLAG)) == "Y" ? "Y" : "N") '
         ': (UPPER(TRIM(ISNULL(CONSENT_FLAG) ? "Y" : CONSENT_FLAG)) == "N" ? "N" : "Y")',
         str_col("MarketingConsentFlag", 1)),
        ("RetentionMonths",
         'UPPER(TRIM(REGION_CD)) == "EU" ? 24 : (UPPER(TRIM(REGION_CD)) == "APAC" ? 60 : 84)',
         int_col("RetentionMonths")),
    ])
    flow.lookup(
        "Lookup Country (Full Cache)", CONN_STAGING,
        "SELECT CountryCode, CountryName, IsoNumeric, DefaultCurrencyCode\n"
        "FROM ref.Country WHERE IsActive = 1;",
        ["CountryCode"],
        [str_col("CountryName", 60), int_col("IsoNumeric"), str_col("DefaultCurrencyCode", 3)],
        no_match="RD")
    flow.derived_column("Derive Customer Hash", [
        ("CreditLimitAmount", 'ISNULL(CREDIT_LIMIT_AMT) ? (DT_NUMERIC,18,2)0 : CREDIT_LIMIT_AMT',
         money_col("CreditLimitAmount")),
        ("CreditCurrencyCode", 'ISNULL(CREDIT_CCY) ? DefaultCurrencyCode : UPPER(TRIM(CREDIT_CCY))',
         str_col("CreditCurrencyCode", 3)),
        ("SourceCreatedDate", 'ISNULL(CREATED_DT) ? (DT_DBTIMESTAMP)"1900-01-01" : CREATED_DT',
         date_col("SourceCreatedDate")),
        ("SourceModifiedDate", 'ISNULL(LAST_UPD_DT) ? CREATED_DT : LAST_UPD_DT', date_col("SourceModifiedDate")),
        ("ChangeHash", hash_expression(["CustomerName", "CustomerClassCode", "CreditStatusCode",
                                        "CountryCode", "TaxRegistrationNumber", "CreditLimitAmount"]),
         str_col("ChangeHash", 128)),
    ])
    flow.conditional_split("Apply Customer Business Rules", [
        ("Valid Customer", '!ISNULL(CustomerName) && TRIM(CustomerName) != "" && LEN(CustomerCode) >= 4'),
        ("Missing Name", 'ISNULL(CustomerName) || TRIM(CustomerName) == ""'),
    ], default_output="Malformed Code")
    flow.row_count("Count Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG Customer", CONN_STAGING, "[stg].[Customer]", batch_size=50000)
    flow.branch_destination("ERR Customer Missing Name", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Apply Customer Business Rules", "Missing Name")
    flow.branch_destination("ERR Customer Malformed Code", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Apply Customer Business Rules", "Malformed Code")
    flow.reject_destination("ERR Customer Unknown Country", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Lookup Country (Full Cache)", "Lookup No Match Output")
    flow.reject_destination("ERR Customer Source Errors", CONN_STAGING, "[err].[RejectedCustomer]",
                            "RAW Oracle Customer Master", "OLE DB Source Error Output")
    return build_package(
        "STG_Load_Customer",
        "Conform raw.OracleCustomerMaster into stg.Customer: trim and case-fold codes, apply the "
        "regional consent and retention rules, resolve the country reference and hash for change "
        "detection. Rows that fail a rule are routed to err.RejectedCustomer.",
        ORA, "stg.Customer", [flow],
        truncate_tables=["[stg].[Customer]"],
        post_tasks=[exec_proc("Normalize Customer Names",
                              "EXEC stg.usp_NormalizeCustomer @BatchId = ?, @PackageExecutionId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                                                  ("User::PackageExecutionId", 1, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedCustomer]", "stg.Customer", "r.CustomerCode",
                                 "RejectReasonCode"))


@package
def stg_load_customer_address():
    """raw.OracleCustomerAddress -> stg.CustomerAddress with per-region standardisation."""
    def region_flow(region, description, derivations, postal_rule):
        cols = [
            str_col("CUST_CODE", 20), str_col("ADDR_TYPE_CD", 4), str_col("ADDR_LINE_1", 120),
            str_col("ADDR_LINE_2", 120), str_col("CITY_NAME", 80), str_col("STATE_PROV_CD", 8),
            str_col("POSTAL_CD", 16), str_col("COUNTRY_CD", 3), str_col("REGION_CD", 4),
            date_col("EFF_FROM_DT"),
        ]
        flow = DataFlow("DFT Standardize %s Address" % region, description)
        flow.oledb_source(
            "RAW %s Address" % region, CONN_STAGING,
            "SELECT CUST_CODE, ADDR_TYPE_CD, ADDR_LINE_1, ADDR_LINE_2, CITY_NAME, STATE_PROV_CD,\n"
            "       POSTAL_CD, COUNTRY_CD, REGION_CD, EFF_FROM_DT\n"
            "FROM raw.OracleCustomerAddress\n"
            "WHERE BatchId = ? AND UPPER(LTRIM(RTRIM(REGION_CD))) = N'%s';" % region,
            cols)
        flow.row_count("Count %s Rows Read" % region, "User::RowsRead")
        flow.derived_column("Standardize %s Address" % region, derivations)
        flow.lookup(
            "Lookup %s Geography (No Cache)" % region, CONN_STAGING,
            "SELECT GeographyKey, CityName, StateProvinceCode, CountryCode\n"
            "FROM stg.Geography WHERE RegionCode = N'%s';" % region,
            ["CityName", "StateProvinceCode"],
            [int_col("GeographyKey")], no_match="RD")
        flow.conditional_split("Validate %s Postal Code" % region, [
            ("Valid Postal", postal_rule),
        ], default_output="Invalid Postal")
        flow.row_count("Count %s Rows Loaded" % region, "User::RowsInserted")
        flow.oledb_destination("STG %s CustomerAddress" % region, CONN_STAGING,
                               "[stg].[CustomerAddress]", batch_size=20000)
        flow.branch_destination("ERR %s Invalid Postal" % region, CONN_STAGING,
                                "[err].[RejectedCustomer]",
                                "Validate %s Postal Code" % region, "Invalid Postal")
        flow.reject_destination("ERR %s Unknown Geography" % region, CONN_STAGING,
                                "[err].[RejectedLookupFailure]",
                                "Lookup %s Geography (No Cache)" % region, "Lookup No Match Output")
        return flow

    common = [
        ("CustomerCode", 'UPPER(TRIM(CUST_CODE))', str_col("CustomerCode", 20)),
        ("AddressTypeCode", 'UPPER(TRIM(ISNULL(ADDR_TYPE_CD) ? "MAIN" : ADDR_TYPE_CD))',
         str_col("AddressTypeCode", 4)),
        ("EffectiveFromDate", 'ISNULL(EFF_FROM_DT) ? (DT_DBTIMESTAMP)"1900-01-01" : EFF_FROM_DT',
         date_col("EffectiveFromDate")),
    ]
    na = region_flow(
        "NA",
        "USPS-style standardisation: upper case, street-suffix abbreviation, five digit ZIP",
        common + [
            ("AddressLine1",
             'UPPER(REPLACE(REPLACE(REPLACE(TRIM(ADDR_LINE_1),"Street","ST"),"Avenue","AVE"),"Suite","STE"))',
             str_col("AddressLine1", 120)),
            ("AddressLine2", 'UPPER(TRIM(ISNULL(ADDR_LINE_2) ? "" : ADDR_LINE_2))', str_col("AddressLine2", 120)),
            ("CityName", 'UPPER(TRIM(CITY_NAME))', str_col("CityName", 80)),
            ("StateProvinceCode", 'UPPER(SUBSTRING(TRIM(STATE_PROV_CD),1,2))', str_col("StateProvinceCode", 8)),
            ("PostalCode", 'SUBSTRING(REPLACE(TRIM(POSTAL_CD),"-",""),1,5)', str_col("PostalCode", 16)),
            ("PostalStandard", '"USPS"', str_col("PostalStandard", 12)),
            ("CountryCode", 'UPPER(TRIM(ISNULL(COUNTRY_CD) ? "USA" : COUNTRY_CD))', str_col("CountryCode", 3)),
            ("RegionCode", '"NA"', str_col("RegionCode", 4)),
        ],
        'LEN(PostalCode) == 5 && ISNUMERIC(PostalCode)')
    eu = region_flow(
        "EU",
        "EU standardisation: mixed-case town names, country-prefixed postal codes, no state code",
        common + [
            ("AddressLine1", 'TRIM(ADDR_LINE_1)', str_col("AddressLine1", 120)),
            ("AddressLine2", 'TRIM(ISNULL(ADDR_LINE_2) ? "" : ADDR_LINE_2)', str_col("AddressLine2", 120)),
            ("CityName", 'TRIM(CITY_NAME)', str_col("CityName", 80)),
            ("StateProvinceCode", '""', str_col("StateProvinceCode", 8)),
            ("PostalCode", 'UPPER(TRIM(COUNTRY_CD)) + "-" + UPPER(REPLACE(TRIM(POSTAL_CD)," ",""))',
             str_col("PostalCode", 16)),
            ("PostalStandard", '"UPU"', str_col("PostalStandard", 12)),
            ("CountryCode", 'UPPER(TRIM(COUNTRY_CD))', str_col("CountryCode", 3)),
            ("RegionCode", '"EU"', str_col("RegionCode", 4)),
        ],
        'LEN(PostalCode) >= 6 && FINDSTRING(PostalCode,"-",1) > 0')
    apac = region_flow(
        "APAC",
        "APAC standardisation: prefecture retained, postal code digits only, romanised city name",
        common + [
            ("AddressLine1", 'TRIM(ADDR_LINE_1)', str_col("AddressLine1", 120)),
            ("AddressLine2", 'TRIM(ISNULL(ADDR_LINE_2) ? "" : ADDR_LINE_2)', str_col("AddressLine2", 120)),
            ("CityName", 'UPPER(TRIM(CITY_NAME))', str_col("CityName", 80)),
            ("StateProvinceCode", 'UPPER(TRIM(ISNULL(STATE_PROV_CD) ? "XX" : STATE_PROV_CD))',
             str_col("StateProvinceCode", 8)),
            ("PostalCode", 'REPLACE(REPLACE(TRIM(POSTAL_CD),"-","")," ","")', str_col("PostalCode", 16)),
            ("PostalStandard", '"APAC-LOCAL"', str_col("PostalStandard", 12)),
            ("CountryCode", 'UPPER(TRIM(COUNTRY_CD))', str_col("CountryCode", 3)),
            ("RegionCode", '"APAC"', str_col("RegionCode", 4)),
        ],
        'LEN(PostalCode) >= 3 && ISNUMERIC(PostalCode)')
    return build_package(
        "STG_Load_CustomerAddress",
        "Standardise raw.OracleCustomerAddress into stg.CustomerAddress. Each region runs its own "
        "data flow because postal standards, state handling and casing rules diverge: NA follows "
        "USPS abbreviation, EU prefixes the country to the postal code, APAC keeps the prefecture.",
        ORA, "stg.CustomerAddress", [na, eu, apac],
        truncate_tables=["[stg].[CustomerAddress]"],
        post_tasks=[exec_proc("Normalize Address Casing",
                              "EXEC stg.usp_NormalizeAddress @BatchId = ?, @RegionCode = NULL;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedLookupFailure]", "stg.CustomerAddress",
                                 "r.CustomerCode", "RejectReasonCode"))


@package
def stg_load_supplier():
    """raw.OracleSupplierMaster -> stg.Supplier with tax-id survivorship."""
    cols = [
        str_col("SUPP_CODE", 20), str_col("SUPP_NAME", 100), str_col("TAX_ID", 30),
        str_col("PAY_TERMS_CD", 10), str_col("SUPP_STATUS_CD", 4), str_col("COUNTRY_CD", 3),
        str_col("REGION_CD", 4), str_col("DEFAULT_CCY", 3), int_col("LEAD_TIME_DAYS"),
        date_col("LAST_UPD_DT"),
    ]
    flow = DataFlow("DFT Conform Supplier", "Dedupe on tax id and resolve payment terms")
    flow.oledb_source(
        "RAW Oracle Supplier Master", CONN_STAGING,
        "SELECT SUPP_CODE, SUPP_NAME, TAX_ID, PAY_TERMS_CD, SUPP_STATUS_CD, COUNTRY_CD,\n"
        "       REGION_CD, DEFAULT_CCY, LEAD_TIME_DAYS, LAST_UPD_DT\n"
        "FROM raw.OracleSupplierMaster\n"
        "WHERE BatchId = ?\n"
        "ORDER BY TAX_ID, LAST_UPD_DT DESC;",
        cols)
    flow.row_count("Count Rows Read", "User::RowsRead")
    flow.derived_column("Cleanse Supplier Attributes", [
        ("SupplierCode", 'UPPER(TRIM(SUPP_CODE))', str_col("SupplierCode", 20)),
        ("SupplierName", 'TRIM(SUPP_NAME)', str_col("SupplierName", 100)),
        ("TaxIdentifier", 'UPPER(REPLACE(REPLACE(TRIM(ISNULL(TAX_ID) ? "" : TAX_ID)," ",""),".",""))',
         str_col("TaxIdentifier", 30)),
        ("SupplierStatusCode", 'UPPER(TRIM(ISNULL(SUPP_STATUS_CD) ? "PEND" : SUPP_STATUS_CD))',
         str_col("SupplierStatusCode", 4)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD))', str_col("RegionCode", 4)),
        ("DefaultCurrencyCode", 'UPPER(TRIM(ISNULL(DEFAULT_CCY) ? "USD" : DEFAULT_CCY))',
         str_col("DefaultCurrencyCode", 3)),
        ("LeadTimeDays", 'ISNULL(LEAD_TIME_DAYS) || LEAD_TIME_DAYS <= 0 ? 14 : LEAD_TIME_DAYS',
         int_col("LeadTimeDays")),
    ])
    flow.sort("Sort By Tax Identifier", ["TaxIdentifier", "SupplierCode"], eliminate_duplicates=False)
    flow.lookup(
        "Lookup Payment Terms (Partial Cache)", CONN_STAGING,
        "SELECT PaymentTermsCode, NetDays, DiscountPercent, DiscountDays\n"
        "FROM stg.PaymentTerms WHERE IsCurrent = 1;",
        ["PaymentTermsCode"],
        [int_col("NetDays"), dec_col("DiscountPercent", 9, 4), int_col("DiscountDays")],
        no_match="RD")
    flow.derived_column("Derive Supplier Hash", [
        ("PaymentTermsCode", 'UPPER(TRIM(ISNULL(PAY_TERMS_CD) ? "NET30" : PAY_TERMS_CD))',
         str_col("PaymentTermsCode", 10)),
        ("WithholdingApplicableFlag",
         'RegionCode == "APAC" && LEN(TaxIdentifier) == 0 ? "Y" : "N"',
         str_col("WithholdingApplicableFlag", 1)),
        ("ChangeHash", hash_expression(["SupplierName", "TaxIdentifier", "PaymentTermsCode",
                                        "SupplierStatusCode", "LeadTimeDays"]),
         str_col("ChangeHash", 128)),
    ])
    flow.conditional_split("Apply Supplier Business Rules", [
        ("Valid Supplier", 'LEN(TaxIdentifier) > 0 || SupplierStatusCode == "PEND"'),
        ("Missing Tax Identifier", 'LEN(TaxIdentifier) == 0'),
    ], default_output="Suspect Supplier")
    flow.row_count("Count Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG Supplier", CONN_STAGING, "[stg].[Supplier]", batch_size=20000)
    flow.branch_destination("ERR Supplier Missing Tax Id", CONN_STAGING, "[err].[RejectedSupplier]",
                            "Apply Supplier Business Rules", "Missing Tax Identifier")
    flow.reject_destination("ERR Supplier Unknown Terms", CONN_STAGING, "[err].[RejectedSupplier]",
                            "Lookup Payment Terms (Partial Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_Supplier",
        "Conform raw.OracleSupplierMaster into stg.Supplier. The feed carries the same supplier "
        "under several codes, so rows are sorted by tax identifier and last-update date and the "
        "survivorship rule in stg.usp_NormalizeSupplier keeps the most recently updated row.",
        ORA, "stg.Supplier", [flow],
        truncate_tables=["[stg].[Supplier]"],
        post_tasks=[exec_proc("Apply Supplier Survivorship",
                              "EXEC stg.usp_NormalizeSupplier @BatchId = ?, @SurvivorshipRule = N'LATEST_UPDATE';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedSupplier]", "stg.Supplier", "r.SupplierCode",
                                 "RejectReasonCode"))


@package
def stg_load_product():
    """raw.OracleProductMaster -> stg.Product with unit-of-measure conversion."""
    cols = [
        str_col("PROD_CODE", 25), str_col("PROD_DESC", 200), str_col("PROD_FAMILY_CD", 10),
        str_col("BASE_UOM_CD", 6), dec_col("PACK_QTY", 18, 4), money_col("LIST_PRICE_AMT"),
        str_col("LIST_PRICE_CCY", 3), dec_col("NET_WEIGHT", 18, 4), str_col("WEIGHT_UOM_CD", 6),
        str_col("HAZMAT_FLG", 1), str_col("DISCONTINUED_FLG", 1), date_col("LAST_UPD_DT"),
    ]
    flow = DataFlow("DFT Conform Product", "Normalise UoM, convert weights and classify the catalogue")
    flow.oledb_source(
        "RAW Oracle Product Master", CONN_STAGING,
        "SELECT PROD_CODE, PROD_DESC, PROD_FAMILY_CD, BASE_UOM_CD, PACK_QTY, LIST_PRICE_AMT,\n"
        "       LIST_PRICE_CCY, NET_WEIGHT, WEIGHT_UOM_CD, HAZMAT_FLG, DISCONTINUED_FLG, LAST_UPD_DT\n"
        "FROM raw.OracleProductMaster WHERE BatchId = ?;",
        cols)
    flow.row_count("Count Rows Read", "User::RowsRead")
    flow.derived_column("Cleanse Product Attributes", [
        ("ProductCode", 'UPPER(TRIM(PROD_CODE))', str_col("ProductCode", 25)),
        ("ProductDescription",
         'TRIM(REPLACE(REPLACE(PROD_DESC,"\\t"," "),"  "," "))', str_col("ProductDescription", 200)),
        ("ProductFamilyCode", 'UPPER(TRIM(ISNULL(PROD_FAMILY_CD) ? "UNCLASS" : PROD_FAMILY_CD))',
         str_col("ProductFamilyCode", 10)),
        ("BaseUomCode", 'UPPER(TRIM(ISNULL(BASE_UOM_CD) ? "EA" : BASE_UOM_CD))', str_col("BaseUomCode", 6)),
        ("PackQuantity", 'ISNULL(PACK_QTY) || PACK_QTY <= 0 ? (DT_NUMERIC,18,4)1 : PACK_QTY',
         dec_col("PackQuantity", 18, 4)),
        ("HazardousFlag", 'UPPER(TRIM(ISNULL(HAZMAT_FLG) ? "N" : HAZMAT_FLG)) == "Y" ? "Y" : "N"',
         str_col("HazardousFlag", 1)),
        ("DiscontinuedFlag", 'UPPER(TRIM(ISNULL(DISCONTINUED_FLG) ? "N" : DISCONTINUED_FLG))',
         str_col("DiscontinuedFlag", 1)),
    ])
    flow.lookup(
        "Lookup UoM Conversion (Full Cache)", CONN_STAGING,
        "SELECT FromUomCode, ToUomCode, ConversionFactor\n"
        "FROM ref.UomConversion WHERE ToUomCode = N'EA' AND IsActive = 1;",
        ["BaseUomCode"],
        [dec_col("ConversionFactor", 18, 6)], no_match="IG")
    flow.lookup(
        "Lookup Weight UoM (No Cache)", CONN_STAGING,
        "SELECT FromUomCode AS WeightUomCode, ConversionFactor AS WeightFactorKg\n"
        "FROM ref.UomConversion WHERE ToUomCode = N'KG';",
        ["WeightUomCode"],
        [dec_col("WeightFactorKg", 18, 6)], no_match="RD")
    flow.derived_column("Convert Units And Price", [
        ("WeightUomCode", 'UPPER(TRIM(ISNULL(WEIGHT_UOM_CD) ? "KG" : WEIGHT_UOM_CD))',
         str_col("WeightUomCode", 6)),
        ("EachesPerPack",
         'ISNULL(ConversionFactor) ? PackQuantity : PackQuantity * ConversionFactor',
         dec_col("EachesPerPack", 18, 4)),
        ("NetWeightKg",
         'ISNULL(NET_WEIGHT) ? (DT_NUMERIC,18,4)0 : (DT_NUMERIC,18,4)(NET_WEIGHT * ISNULL(WeightFactorKg) ? 1 : WeightFactorKg)',
         dec_col("NetWeightKg", 18, 4)),
        ("ListPriceAmount", 'ISNULL(LIST_PRICE_AMT) ? (DT_NUMERIC,18,2)0 : LIST_PRICE_AMT',
         money_col("ListPriceAmount")),
        ("ListPriceCurrencyCode", 'UPPER(TRIM(ISNULL(LIST_PRICE_CCY) ? "USD" : LIST_PRICE_CCY))',
         str_col("ListPriceCurrencyCode", 3)),
        ("ChangeHash", hash_expression(["ProductDescription", "ProductFamilyCode", "BaseUomCode",
                                        "ListPriceAmount", "DiscontinuedFlag"]),
         str_col("ChangeHash", 128)),
    ])
    flow.conditional_split("Apply Product Business Rules", [
        ("Sellable Product", 'DiscontinuedFlag == "N" && ListPriceAmount > 0'),
        ("Discontinued Product", 'DiscontinuedFlag == "Y"'),
    ], default_output="Priceless Product")
    flow.row_count("Count Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG Product", CONN_STAGING, "[stg].[Product]", batch_size=50000)
    flow.branch_destination("STG Product Discontinued", CONN_STAGING, "[stg].[Product]",
                            "Apply Product Business Rules", "Discontinued Product")
    flow.branch_destination("ERR Product No Price", CONN_STAGING, "[err].[RejectedProduct]",
                            "Apply Product Business Rules", "Priceless Product")
    flow.reject_destination("ERR Product Weight Uom", CONN_STAGING, "[err].[RejectedProduct]",
                            "Lookup Weight UoM (No Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_Product",
        "Conform raw.OracleProductMaster into stg.Product. Pack quantities are converted to eaches "
        "and weights to kilograms through ref.UomConversion; products with no list price are held "
        "in err.RejectedProduct rather than defaulted to zero downstream.",
        ORA, "stg.Product", [flow],
        truncate_tables=["[stg].[Product]"],
        reject_task=reject_sweep("[err].[RejectedProduct]", "stg.Product", "r.ProductCode",
                                 "RejectReasonCode"))


@package
def stg_load_geography():
    """raw.OracleGeography -> stg.Geography with coordinate range checks."""
    cols = [
        str_col("GEO_CODE", 16), str_col("CITY_NAME", 80), str_col("STATE_PROV_CD", 8),
        str_col("STATE_PROV_NAME", 80), str_col("COUNTRY_CD", 3), str_col("REGION_CD", 4),
        str_col("SALES_TERR_CD", 10), dec_col("LATITUDE", 9, 6), dec_col("LONGITUDE", 9, 6),
        bigint_col("POPULATION"),
    ]
    flow = DataFlow("DFT Conform Geography", "Validate coordinates and attach the sales territory")
    flow.oledb_source(
        "RAW Oracle Geography", CONN_STAGING,
        "SELECT GEO_CODE, CITY_NAME, STATE_PROV_CD, STATE_PROV_NAME, COUNTRY_CD, REGION_CD,\n"
        "       SALES_TERR_CD, LATITUDE, LONGITUDE, POPULATION\n"
        "FROM raw.OracleGeography WHERE BatchId = ?;",
        cols)
    flow.row_count("Count Rows Read", "User::RowsRead")
    flow.derived_column("Cleanse Geography", [
        ("GeographyCode", 'UPPER(TRIM(GEO_CODE))', str_col("GeographyCode", 16)),
        ("CityName", 'TRIM(REPLACE(CITY_NAME,"  "," "))', str_col("CityName", 80)),
        ("StateProvinceCode", 'UPPER(TRIM(ISNULL(STATE_PROV_CD) ? "" : STATE_PROV_CD))',
         str_col("StateProvinceCode", 8)),
        ("StateProvinceName", 'TRIM(ISNULL(STATE_PROV_NAME) ? "" : STATE_PROV_NAME)',
         str_col("StateProvinceName", 80)),
        ("CountryCode", 'UPPER(TRIM(COUNTRY_CD))', str_col("CountryCode", 3)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD))', str_col("RegionCode", 4)),
        ("Latitude", 'ISNULL(LATITUDE) ? (DT_NUMERIC,9,6)0 : LATITUDE', dec_col("Latitude", 9, 6)),
        ("Longitude", 'ISNULL(LONGITUDE) ? (DT_NUMERIC,9,6)0 : LONGITUDE', dec_col("Longitude", 9, 6)),
        ("PopulationCount", 'ISNULL(POPULATION) ? (DT_I8)0 : POPULATION', bigint_col("PopulationCount")),
    ])
    flow.lookup(
        "Lookup Sales Territory (Full Cache)", CONN_STAGING,
        "SELECT SalesTerritoryCode, SalesTerritoryName, RegionCode AS TerritoryRegionCode\n"
        "FROM stg.SalesTerritory;",
        ["SalesTerritoryCode"],
        [str_col("SalesTerritoryName", 60), str_col("TerritoryRegionCode", 4)], no_match="RD")
    flow.derived_column("Derive Geography Keys", [
        ("SalesTerritoryCode", 'UPPER(TRIM(ISNULL(SALES_TERR_CD) ? "UNASSIGNED" : SALES_TERR_CD))',
         str_col("SalesTerritoryCode", 10)),
        ("CityStateKey", 'UPPER(TRIM(CITY_NAME)) + "|" + UPPER(TRIM(ISNULL(STATE_PROV_CD) ? "" : STATE_PROV_CD))',
         str_col("CityStateKey", 92)),
        ("ChangeHash", hash_expression(["CityName", "StateProvinceCode", "CountryCode",
                                        "SalesTerritoryCode", "PopulationCount"]),
         str_col("ChangeHash", 128)),
    ])
    flow.conditional_split("Validate Coordinates", [
        ("Plausible Coordinates",
         'Latitude >= -90 && Latitude <= 90 && Longitude >= -180 && Longitude <= 180'),
    ], default_output="Out Of Range")
    flow.row_count("Count Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG Geography", CONN_STAGING, "[stg].[Geography]", batch_size=20000)
    flow.branch_destination("ERR Geography Out Of Range", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]", "Validate Coordinates", "Out Of Range")
    flow.reject_destination("ERR Geography Unknown Territory", CONN_STAGING,
                            "[err].[RejectedLookupFailure]", "Lookup Sales Territory (Full Cache)",
                            "Lookup No Match Output")
    return build_package(
        "STG_Load_Geography",
        "Conform raw.OracleGeography into stg.Geography, attach the sales territory and reject "
        "coordinates outside the valid latitude and longitude ranges.",
        ORA, "stg.Geography", [flow],
        truncate_tables=["[stg].[Geography]"],
        post_tasks=[exec_proc("Reload Geography Hierarchy",
                              "EXEC stg.usp_TruncateAndReload_Geography @BatchId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedConstraintViolation]", "stg.Geography",
                                 "r.GeographyCode", "RejectReasonCode"))


@package
def stg_load_currency():
    """raw.OracleCurrency + raw.OracleFxRate -> stg.Currency, stg.FxRate."""
    ccy_cols = [
        str_col("CCY_CODE", 3), str_col("CCY_NAME", 60), int_col("MINOR_UNITS"),
        str_col("REGION_CD", 4), str_col("ACTIVE_FLG", 1),
    ]
    ccy = DataFlow("DFT Conform Currency", "Currency master with minor-unit defaults")
    ccy.oledb_source(
        "RAW Oracle Currency", CONN_STAGING,
        "SELECT CCY_CODE, CCY_NAME, MINOR_UNITS, REGION_CD, ACTIVE_FLG\n"
        "FROM raw.OracleCurrency WHERE BatchId = ?;",
        ccy_cols)
    ccy.row_count("Count Currency Rows Read", "User::RowsRead")
    ccy.derived_column("Cleanse Currency", [
        ("CurrencyCode", 'UPPER(TRIM(CCY_CODE))', str_col("CurrencyCode", 3)),
        ("CurrencyName", 'TRIM(CCY_NAME)', str_col("CurrencyName", 60)),
        ("MinorUnits", 'ISNULL(MINOR_UNITS) ? 2 : MINOR_UNITS', int_col("MinorUnits")),
        ("IsActiveFlag", 'UPPER(TRIM(ISNULL(ACTIVE_FLG) ? "Y" : ACTIVE_FLG))', str_col("IsActiveFlag", 1)),
    ])
    ccy.conditional_split("Validate Currency Code", [
        ("Valid Currency", 'LEN(CurrencyCode) == 3'),
    ], default_output="Invalid Currency")
    ccy.oledb_destination("STG Currency", CONN_STAGING, "[stg].[Currency]", batch_size=5000)
    ccy.branch_destination("ERR Currency Invalid", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                           "Validate Currency Code", "Invalid Currency")

    fx_cols = [
        str_col("FROM_CCY", 3), str_col("TO_CCY", 3), date_col("EFF_FROM_DT"), date_col("EFF_TO_DT"),
        dec_col("RATE", 18, 8), str_col("RATE_TYPE_CD", 8), str_col("SRC_SYSTEM_CD", 10),
    ]
    fx = DataFlow("DFT Conform FX Rate", "Effective-dated FX rates with USD triangulation")
    fx.oledb_source(
        "RAW Oracle FX Rate", CONN_STAGING,
        "SELECT FROM_CCY, TO_CCY, EFF_FROM_DT, EFF_TO_DT, RATE, RATE_TYPE_CD, SRC_SYSTEM_CD\n"
        "FROM raw.OracleFxRate\n"
        "WHERE BatchId = ? AND RATE > 0\n"
        "UNION ALL\n"
        "SELECT FROM_CCY, TO_CCY, EFF_FROM_DT, EFF_TO_DT, RATE, N'OVERRIDE', N'FILE_FX'\n"
        "FROM raw.FileFxOverride WHERE BatchId = ?;",
        fx_cols)
    fx.row_count("Count FX Rows Read", "User::RowsRead")
    fx.derived_column("Normalise FX Rate", [
        ("FromCurrencyCode", 'UPPER(TRIM(FROM_CCY))', str_col("FromCurrencyCode", 3)),
        ("ToCurrencyCode", 'UPPER(TRIM(TO_CCY))', str_col("ToCurrencyCode", 3)),
        ("RateTypeCode", 'UPPER(TRIM(ISNULL(RATE_TYPE_CD) ? "SPOT" : RATE_TYPE_CD))',
         str_col("RateTypeCode", 8)),
        ("EffectiveFromDate", '(DT_DBDATE)EFF_FROM_DT', Column("EffectiveFromDate", "date")),
        ("EffectiveToDate",
         'ISNULL(EFF_TO_DT) ? (DT_DBDATE)"9999-12-31" : (DT_DBDATE)EFF_TO_DT',
         Column("EffectiveToDate", "date")),
        ("ExchangeRate", '(DT_NUMERIC,18,8)RATE', dec_col("ExchangeRate", 18, 8)),
        ("InverseRate", 'RATE == 0 ? (DT_NUMERIC,18,8)0 : (DT_NUMERIC,18,8)(1 / RATE)',
         dec_col("InverseRate", 18, 8)),
    ])
    fx.sort("Sort FX By Pair And Date", ["FromCurrencyCode", "ToCurrencyCode", "EffectiveFromDate"],
            eliminate_duplicates=True)
    fx.lookup(
        "Lookup USD Cross Rate (Partial Cache)", CONN_STAGING,
        "SELECT CurrencyCode AS ToCurrencyCode, UsdCrossRate\n"
        "FROM stg.FxRate WHERE RateTypeCode = N'SPOT' AND EffectiveToDate = '9999-12-31';",
        ["ToCurrencyCode"], [dec_col("UsdCrossRate", 18, 8)], no_match="IG")
    fx.derived_column("Triangulate Through USD", [
        ("UsdEquivalentRate",
         'ISNULL(UsdCrossRate) ? ExchangeRate : (DT_NUMERIC,18,8)(ExchangeRate * UsdCrossRate)',
         dec_col("UsdEquivalentRate", 18, 8)),
        ("RateSourceCode", 'UPPER(TRIM(ISNULL(SRC_SYSTEM_CD) ? "ORA_ERP" : SRC_SYSTEM_CD))',
         str_col("RateSourceCode", 10)),
    ])
    fx.row_count("Count FX Rows Loaded", "User::RowsInserted")
    fx.oledb_destination("STG FxRate", CONN_STAGING, "[stg].[FxRate]", batch_size=20000)
    fx.reject_destination("ERR FX Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                          "RAW Oracle FX Rate", "OLE DB Source Error Output")
    return build_package(
        "STG_Load_Currency",
        "Conform the currency master and the effective-dated FX rates. The rate feed unions the "
        "Oracle daily extract with the treasury override file; overrides win because they are "
        "loaded last and the sort de-duplicates on currency pair and effective date.",
        ORA, "stg.FxRate", [ccy, fx],
        truncate_tables=["[stg].[Currency]", "[stg].[FxRate]"],
        post_tasks=[exec_proc("Convert Outstanding Amounts",
                              "EXEC stg.usp_ConvertCurrencyAmounts @BatchId = ?, @RateTypeCode = N'SPOT';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])])


@package
def stg_load_tax_and_terms():
    """raw.OracleTaxRate + raw.OraclePaymentTerms -> stg.TaxRate, stg.PaymentTerms."""
    tax_cols = [
        str_col("TAX_CODE", 12), str_col("TAX_TYPE_CD", 8), str_col("COUNTRY_CD", 3),
        str_col("REGION_CD", 4), str_col("JURISDICTION_CD", 12), dec_col("RATE_PCT", 9, 4),
        date_col("EFF_FROM_DT"), date_col("EFF_TO_DT"), str_col("RECOVERABLE_FLG", 1),
    ]
    tax = DataFlow("DFT Conform Tax Rate", "Split sales tax, VAT and GST into their regional shapes")
    tax.oledb_source(
        "RAW Oracle Tax Rate", CONN_STAGING,
        "SELECT TAX_CODE, TAX_TYPE_CD, COUNTRY_CD, REGION_CD, JURISDICTION_CD, RATE_PCT,\n"
        "       EFF_FROM_DT, EFF_TO_DT, RECOVERABLE_FLG\n"
        "FROM raw.OracleTaxRate WHERE BatchId = ?;",
        tax_cols)
    tax.row_count("Count Tax Rows Read", "User::RowsRead")
    tax.derived_column("Derive Regional Tax Attributes", [
        ("TaxCode", 'UPPER(TRIM(TAX_CODE))', str_col("TaxCode", 12)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD))', str_col("RegionCode", 4)),
        ("TaxTypeCode",
         'UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD)) == "EU" ? "VAT" '
         ': (UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD)) == "APAC" ? "GST" : "SALESTAX")',
         str_col("TaxTypeCode", 8)),
        ("JurisdictionCode",
         'UPPER(TRIM(ISNULL(JURISDICTION_CD) ? (ISNULL(COUNTRY_CD) ? "UNKNOWN" : COUNTRY_CD) : JURISDICTION_CD))',
         str_col("JurisdictionCode", 12)),
        ("RatePercent", 'ISNULL(RATE_PCT) ? (DT_NUMERIC,9,4)0 : RATE_PCT', dec_col("RatePercent", 9, 4)),
        ("IsRecoverableFlag",
         'UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD)) == "EU" ? "Y" '
         ': UPPER(TRIM(ISNULL(RECOVERABLE_FLG) ? "N" : RECOVERABLE_FLG))',
         str_col("IsRecoverableFlag", 1)),
        ("EffectiveFromDate", '(DT_DBDATE)EFF_FROM_DT', Column("EffectiveFromDate", "date")),
        ("EffectiveToDate", 'ISNULL(EFF_TO_DT) ? (DT_DBDATE)"9999-12-31" : (DT_DBDATE)EFF_TO_DT',
         Column("EffectiveToDate", "date")),
    ])
    tax.conditional_split("Route Tax By Region", [
        ("NA Sales Tax", 'RegionCode == "NA" && RatePercent >= 0 && RatePercent < 20'),
        ("EU Value Added Tax", 'RegionCode == "EU" && RatePercent > 0 && RatePercent <= 27'),
        ("APAC Goods And Services Tax", 'RegionCode == "APAC" && RatePercent > 0 && RatePercent <= 15'),
    ], default_output="Implausible Rate")
    tax.row_count("Count Tax Rows Loaded", "User::RowsInserted")
    tax.oledb_destination("STG TaxRate NA", CONN_STAGING, "[stg].[TaxRate]", batch_size=5000)
    tax.branch_destination("STG TaxRate EU", CONN_STAGING, "[stg].[TaxRate]",
                           "Route Tax By Region", "EU Value Added Tax")
    tax.branch_destination("STG TaxRate APAC", CONN_STAGING, "[stg].[TaxRate]",
                           "Route Tax By Region", "APAC Goods And Services Tax")
    tax.branch_destination("ERR TaxRate Implausible", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                           "Route Tax By Region", "Implausible Rate")

    terms_cols = [
        str_col("TERMS_CODE", 10), str_col("TERMS_DESC", 80), int_col("NET_DAYS"),
        dec_col("DISC_PCT", 9, 4), int_col("DISC_DAYS"), str_col("REGION_CD", 4),
    ]
    terms = DataFlow("DFT Conform Payment Terms", "Translate legacy terms codes to the conformed set")
    terms.oledb_source(
        "RAW Oracle Payment Terms", CONN_STAGING,
        "SELECT TERMS_CODE, TERMS_DESC, NET_DAYS, DISC_PCT, DISC_DAYS, REGION_CD\n"
        "FROM raw.OraclePaymentTerms WHERE BatchId = ?;",
        terms_cols)
    terms.derived_column("Cleanse Payment Terms", [
        ("PaymentTermsCode", 'UPPER(REPLACE(TRIM(TERMS_CODE)," ",""))', str_col("PaymentTermsCode", 10)),
        ("PaymentTermsDescription", 'TRIM(ISNULL(TERMS_DESC) ? "" : TERMS_DESC)',
         str_col("PaymentTermsDescription", 80)),
        ("NetDays", 'ISNULL(NET_DAYS) || NET_DAYS < 0 ? 30 : NET_DAYS', int_col("NetDays")),
        ("DiscountPercent", 'ISNULL(DISC_PCT) ? (DT_NUMERIC,9,4)0 : DISC_PCT', dec_col("DiscountPercent", 9, 4)),
        ("DiscountDays", 'ISNULL(DISC_DAYS) ? 0 : DISC_DAYS', int_col("DiscountDays")),
        ("RegionCode", 'UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD))', str_col("RegionCode", 4)),
        ("IsCurrent", '(DT_BOOL)1', bool_col("IsCurrent")),
    ])
    terms.lookup(
        "Lookup Terms Crosswalk (Full Cache)", CONN_STAGING,
        "SELECT SourceCode AS PaymentTermsCode, TargetCode AS ConformedTermsCode\n"
        "FROM ref.CodeCrosswalk WHERE CodeSetName = N'PAYMENT_TERMS';",
        ["PaymentTermsCode"], [str_col("ConformedTermsCode", 10)], no_match="RD")
    terms.oledb_destination("STG PaymentTerms", CONN_STAGING, "[stg].[PaymentTerms]", batch_size=5000)
    terms.reject_destination("ERR Terms Unmapped", CONN_STAGING, "[err].[RejectedLookupFailure]",
                             "Lookup Terms Crosswalk (Full Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_TaxAndTerms",
        "Conform tax rates and payment terms. Tax rows are routed by region because the plausible "
        "rate band and the recoverability rule differ: NA sales tax is never recoverable, EU VAT "
        "always is, and APAC GST is capped at fifteen percent.",
        ORA, "stg.TaxRate", [tax, terms],
        truncate_tables=["[stg].[TaxRate]", "[stg].[PaymentTerms]"],
        post_tasks=[exec_proc("Translate Legacy Terms Codes",
                              "EXEC stg.usp_TranslateSourceCodes @BatchId = ?, @CodeSetName = N'PAYMENT_TERMS';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedLookupFailure]", "stg.PaymentTerms",
                                 "r.PaymentTermsCode", "RejectReasonCode"))


@package
def stg_load_cost_center():
    """raw.OracleCostCenter -> stg.CostCenter with hierarchy flattening."""
    cols = [
        str_col("CC_CODE", 12), str_col("CC_NAME", 80), str_col("PARENT_CC_CODE", 12),
        str_col("COMPANY_CD", 6), str_col("REGION_CD", 4), str_col("FUNCTION_CD", 8),
        str_col("ACTIVE_FLG", 1), date_col("VALID_FROM_DT"),
    ]
    flow = DataFlow("DFT Conform Cost Center", "Flatten the cost-centre hierarchy to three levels")
    flow.oledb_source(
        "RAW Oracle Cost Center", CONN_STAGING,
        "SELECT CC_CODE, CC_NAME, PARENT_CC_CODE, COMPANY_CD, REGION_CD, FUNCTION_CD,\n"
        "       ACTIVE_FLG, VALID_FROM_DT\n"
        "FROM raw.OracleCostCenter WHERE BatchId = ?;",
        cols)
    flow.row_count("Count Rows Read", "User::RowsRead")
    flow.derived_column("Cleanse Cost Center", [
        ("CostCenterCode", 'UPPER(REPLACE(TRIM(CC_CODE)," ",""))', str_col("CostCenterCode", 12)),
        ("CostCenterName", 'TRIM(CC_NAME)', str_col("CostCenterName", 80)),
        ("ParentCostCenterCode",
         'ISNULL(PARENT_CC_CODE) || TRIM(PARENT_CC_CODE) == "" ? "ROOT" : UPPER(TRIM(PARENT_CC_CODE))',
         str_col("ParentCostCenterCode", 12)),
        ("CompanyCode", 'UPPER(TRIM(ISNULL(COMPANY_CD) ? "1000" : COMPANY_CD))', str_col("CompanyCode", 6)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD))', str_col("RegionCode", 4)),
        ("FunctionCode", 'UPPER(TRIM(ISNULL(FUNCTION_CD) ? "GEN" : FUNCTION_CD))', str_col("FunctionCode", 8)),
        ("IsActiveFlag", 'UPPER(TRIM(ISNULL(ACTIVE_FLG) ? "Y" : ACTIVE_FLG))', str_col("IsActiveFlag", 1)),
    ])
    flow.lookup(
        "Lookup Parent Cost Center (No Cache)", CONN_STAGING,
        "SELECT CostCenterCode AS ParentCostCenterCode, CostCenterName AS ParentCostCenterName,\n"
        "       FunctionCode AS ParentFunctionCode\n"
        "FROM stg.CostCenter;",
        ["ParentCostCenterCode"],
        [str_col("ParentCostCenterName", 80), str_col("ParentFunctionCode", 8)], no_match="IG")
    flow.derived_column("Derive Hierarchy Path", [
        ("HierarchyPath",
         'CompanyCode + "/" + (ISNULL(ParentCostCenterName) ? "ORPHAN" : ParentCostCenterCode) + "/" + CostCenterCode',
         str_col("HierarchyPath", 40)),
        ("HierarchyLevel", 'ParentCostCenterCode == "ROOT" ? 1 : (ISNULL(ParentCostCenterName) ? 3 : 2)',
         int_col("HierarchyLevel")),
        ("ChangeHash", hash_expression(["CostCenterName", "ParentCostCenterCode", "FunctionCode",
                                        "IsActiveFlag"]), str_col("ChangeHash", 128)),
    ])
    flow.conditional_split("Apply Cost Center Rules", [
        ("Active Cost Center", 'IsActiveFlag == "Y"'),
        ("Closed Cost Center", 'IsActiveFlag == "N" && HierarchyLevel > 1'),
    ], default_output="Orphan Cost Center")
    flow.row_count("Count Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG CostCenter", CONN_STAGING, "[stg].[CostCenter]", batch_size=10000)
    flow.branch_destination("STG CostCenter Closed", CONN_STAGING, "[stg].[CostCenter]",
                            "Apply Cost Center Rules", "Closed Cost Center")
    flow.branch_destination("ERR CostCenter Orphan", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Apply Cost Center Rules", "Orphan Cost Center")
    return build_package(
        "STG_Load_CostCenter",
        "Conform raw.OracleCostCenter into stg.CostCenter. The parent lookup runs uncached because "
        "the hierarchy is loaded in the same pass; orphans keep level three and are reported "
        "through err.RejectedLookupFailure instead of being silently re-parented.",
        ORA, "stg.CostCenter", [flow],
        truncate_tables=["[stg].[CostCenter]"],
        post_tasks=[exec_proc("Translate Cost Center Function Codes",
                              "EXEC stg.usp_TranslateSourceCodes @BatchId = ?, @CodeSetName = N'CC_FUNCTION';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedLookupFailure]", "stg.CostCenter",
                                 "r.CostCenterCode", "RejectReasonCode"))


@package
def stg_load_vendor_contract():
    """raw.OracleVendorContract -> stg.VendorContract with FX conversion and banding."""
    cols = [
        str_col("CONTRACT_NBR", 20), str_col("SUPP_CODE", 20), str_col("CONTRACT_TYPE_CD", 8),
        date_col("START_DT"), date_col("END_DT"), money_col("COMMIT_AMT"), str_col("COMMIT_CCY", 3),
        dec_col("DISC_PCT", 9, 4), str_col("REGION_CD", 4), str_col("STATUS_CD", 4),
    ]
    flow = DataFlow("DFT Conform Vendor Contract", "Convert commitments to USD and band the contract")
    flow.oledb_source(
        "RAW Oracle Vendor Contract", CONN_STAGING,
        "SELECT CONTRACT_NBR, SUPP_CODE, CONTRACT_TYPE_CD, START_DT, END_DT, COMMIT_AMT,\n"
        "       COMMIT_CCY, DISC_PCT, REGION_CD, STATUS_CD\n"
        "FROM raw.OracleVendorContract WHERE BatchId = ?;",
        cols)
    flow.row_count("Count Rows Read", "User::RowsRead")
    flow.derived_column("Cleanse Contract", [
        ("ContractNumber", 'UPPER(TRIM(CONTRACT_NBR))', str_col("ContractNumber", 20)),
        ("SupplierCode", 'UPPER(TRIM(SUPP_CODE))', str_col("SupplierCode", 20)),
        ("ContractTypeCode", 'UPPER(TRIM(ISNULL(CONTRACT_TYPE_CD) ? "STD" : CONTRACT_TYPE_CD))',
         str_col("ContractTypeCode", 8)),
        ("CommitCurrencyCode", 'UPPER(TRIM(ISNULL(COMMIT_CCY) ? "USD" : COMMIT_CCY))',
         str_col("CommitCurrencyCode", 3)),
        ("StartDate", '(DT_DBDATE)START_DT', Column("StartDate", "date")),
        ("EndDate", 'ISNULL(END_DT) ? (DT_DBDATE)"9999-12-31" : (DT_DBDATE)END_DT',
         Column("EndDate", "date")),
        ("CommitAmount", 'ISNULL(COMMIT_AMT) ? (DT_NUMERIC,18,2)0 : COMMIT_AMT', money_col("CommitAmount")),
        ("RegionCode", 'UPPER(TRIM(ISNULL(REGION_CD) ? "NA" : REGION_CD))', str_col("RegionCode", 4)),
    ])
    flow.lookup(
        "Lookup Supplier (Full Cache)", CONN_STAGING,
        "SELECT SupplierCode, SupplierName, DefaultCurrencyCode AS SupplierCurrencyCode\n"
        "FROM stg.Supplier;",
        ["SupplierCode"], [str_col("SupplierName", 100), str_col("SupplierCurrencyCode", 3)],
        no_match="RD")
    flow.lookup(
        "Lookup Contract FX Rate (Partial Cache)", CONN_STAGING,
        "SELECT FromCurrencyCode AS CommitCurrencyCode, ExchangeRate AS ContractFxRate\n"
        "FROM stg.FxRate\n"
        "WHERE ToCurrencyCode = N'USD' AND RateTypeCode = N'CONTRACT';",
        ["CommitCurrencyCode"], [dec_col("ContractFxRate", 18, 8)], no_match="RD")
    flow.derived_column("Convert And Band Contract", [
        ("CommitAmountUsd", '(DT_NUMERIC,18,2)(CommitAmount * ContractFxRate)', money_col("CommitAmountUsd")),
        ("ContractBandCode",
         'CommitAmount * ContractFxRate >= 1000000 ? "STRATEGIC" '
         ': (CommitAmount * ContractFxRate >= 100000 ? "MAJOR" : "TACTICAL")',
         str_col("ContractBandCode", 12)),
        ("DiscountPercent", 'ISNULL(DISC_PCT) ? (DT_NUMERIC,9,4)0 : DISC_PCT', dec_col("DiscountPercent", 9, 4)),
        ("StatusCode", 'UPPER(TRIM(ISNULL(STATUS_CD) ? "DRFT" : STATUS_CD))', str_col("StatusCode", 4)),
    ])
    flow.conditional_split("Validate Contract Dates", [
        ("Valid Window", 'EndDate >= StartDate'),
    ], default_output="Inverted Window")
    flow.row_count("Count Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG VendorContract", CONN_STAGING, "[stg].[VendorContract]", batch_size=10000)
    flow.branch_destination("ERR Contract Inverted Window", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]", "Validate Contract Dates",
                            "Inverted Window")
    flow.reject_destination("ERR Contract Unknown Supplier", CONN_STAGING, "[err].[RejectedSupplier]",
                            "Lookup Supplier (Full Cache)", "Lookup No Match Output")
    flow.reject_destination("ERR Contract Missing Rate", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Contract FX Rate (Partial Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_VendorContract",
        "Conform raw.OracleVendorContract into stg.VendorContract. Commitments are converted to USD "
        "at the contract-type FX rate rather than the spot rate, and the resulting USD value drives "
        "the strategic / major / tactical banding used by procurement reporting.",
        ORA, "stg.VendorContract", [flow],
        truncate_tables=["[stg].[VendorContract]"],
        reject_task=reject_sweep("[err].[RejectedConstraintViolation]", "stg.VendorContract",
                                 "r.ContractNumber", "RejectReasonCode"))


# ---------------------------------------------------------------------------
# Oracle procurement and finance (incremental append against the watermark)
# ---------------------------------------------------------------------------


@package
def stg_load_purchase_order():
    """raw.OraclePurchaseOrderHdr/Line -> stg.PurchaseOrder + stg.PurchaseOrderLine."""
    hdr_cols = [
        str_col("PO_NBR", 20), str_col("VENDOR_CODE", 20), str_col("PO_STATUS_CD", 4),
        str_col("BUY_ORG_CD", 8), str_col("REGION_CD", 4), str_col("PO_CCY", 3),
        money_col("PO_TOTAL_AMT"), date_col("PO_DT"), date_col("PROMISED_DT"),
        date_col("LAST_UPD_DT"),
    ]
    hdr = DataFlow("DFT Purchase Order Header",
                   "Incremental header load with FX conversion to the reporting currency")
    hdr.oledb_source(
        "RAW Oracle PO Header", CONN_STAGING,
        "SELECT PO_NBR, VENDOR_CODE, PO_STATUS_CD, BUY_ORG_CD, REGION_CD, PO_CCY,\n"
        "       PO_TOTAL_AMT, PO_DT, PROMISED_DT, LAST_UPD_DT\n"
        "FROM raw.OraclePurchaseOrderHdr\n"
        "WHERE LAST_UPD_DT > CONVERT(datetime2(3), ?) AND LAST_UPD_DT <= CONVERT(datetime2(3), ?);",
        hdr_cols, timeout=7200)
    hdr.row_count("Count Header Rows Read", "User::RowsRead")
    hdr.derived_column("Standardize PO Header", [
        ("PurchaseOrderNumber", 'UPPER(TRIM(PO_NBR))', str_col("PurchaseOrderNumber", 20)),
        ("SupplierCode", 'UPPER(TRIM(VENDOR_CODE))', str_col("SupplierCode", 20)),
        ("OrderStatusCode",
         'UPPER(TRIM(PO_STATUS_CD)) == "OP" ? "OPEN" : (UPPER(TRIM(PO_STATUS_CD)) == "CL" ? "CLSD" '
         ': (UPPER(TRIM(PO_STATUS_CD)) == "CN" ? "CANC" : "UNKN"))',
         str_col("OrderStatusCode", 4)),
        ("BuyingOrgCode", 'UPPER(TRIM(BUY_ORG_CD))', str_col("BuyingOrgCode", 8)),
        ("RegionCode", 'UPPER(TRIM(REGION_CD))', str_col("RegionCode", 4)),
        ("OrderCurrencyCode", 'UPPER(TRIM(PO_CCY))', str_col("OrderCurrencyCode", 3)),
        ("FxEffectiveDate", '(DT_DBDATE)PO_DT', date_col("FxEffectiveDate")),
    ])
    hdr.lookup(
        "Lookup PO FX Rate (No Cache)", CONN_STAGING,
        "SELECT FromCurrencyCode AS OrderCurrencyCode, RateDate AS FxEffectiveDate,\n"
        "       ConversionRate, ToCurrencyCode\n"
        "FROM stg.FxRate WHERE ToCurrencyCode = N'USD';",
        ["OrderCurrencyCode", "FxEffectiveDate"],
        [dec_col("ConversionRate"), str_col("ToCurrencyCode", 3)],
        no_match="RD")
    hdr.derived_column("Convert PO Amounts", [
        ("OrderTotalAmount", 'ISNULL(PO_TOTAL_AMT) ? (DT_NUMERIC,18,2)0 : PO_TOTAL_AMT',
         money_col("OrderTotalAmount")),
        ("OrderTotalAmountUsd",
         '(DT_NUMERIC,18,2)((ISNULL(PO_TOTAL_AMT) ? (DT_NUMERIC,18,2)0 : PO_TOTAL_AMT) * ConversionRate)',
         money_col("OrderTotalAmountUsd")),
        ("PromisedDate", 'ISNULL(PROMISED_DT) ? PO_DT : PROMISED_DT', date_col("PromisedDate")),
        ("ChangeHash", hash_expression(["PurchaseOrderNumber", "OrderStatusCode",
                                        "OrderTotalAmount", "PromisedDate"]),
         str_col("ChangeHash", 128)),
    ])
    hdr.conditional_split("Screen PO Header", [
        ("Valid Header", 'LEN(PurchaseOrderNumber) > 0 && OrderTotalAmount >= 0 && OrderStatusCode != "UNKN"'),
        ("Negative Total", 'OrderTotalAmount < 0'),
    ], default_output="Unknown Status")
    hdr.row_count("Count Header Rows Loaded", "User::RowsInserted")
    hdr.oledb_destination("STG PurchaseOrder", CONN_STAGING, "[stg].[PurchaseOrder]", batch_size=20000)
    hdr.branch_destination("ERR PO Negative Total", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                           "Screen PO Header", "Negative Total")
    hdr.branch_destination("ERR PO Unknown Status", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                           "Screen PO Header", "Unknown Status")
    hdr.reject_destination("ERR PO Unknown FX Rate", CONN_STAGING, "[err].[RejectedLookupFailure]",
                           "Lookup PO FX Rate (No Cache)", "Lookup No Match Output")

    line_cols = [
        str_col("PO_NBR", 20), int_col("PO_LINE_NBR"), str_col("ITEM_CODE", 30),
        str_col("UOM_CD", 6), dec_col("ORDER_QTY", 18, 4), money_col("UNIT_PRICE_AMT"),
        str_col("TAX_CODE", 10), date_col("NEED_BY_DT"),
    ]
    line = DataFlow("DFT Purchase Order Line",
                    "Line load with unit-of-measure conversion and product crosswalk resolution")
    line.oledb_source(
        "RAW Oracle PO Line", CONN_STAGING,
        "SELECT l.PO_NBR, l.PO_LINE_NBR, l.ITEM_CODE, l.UOM_CD, l.ORDER_QTY,\n"
        "       l.UNIT_PRICE_AMT, l.TAX_CODE, l.NEED_BY_DT\n"
        "FROM raw.OraclePurchaseOrderLine AS l\n"
        "     INNER JOIN raw.OraclePurchaseOrderHdr AS h ON h.PO_NBR = l.PO_NBR\n"
        "WHERE h.LAST_UPD_DT > CONVERT(datetime2(3), ?) AND h.LAST_UPD_DT <= CONVERT(datetime2(3), ?);",
        line_cols, timeout=7200)
    line.derived_column("Standardize PO Line", [
        ("PurchaseOrderNumber", 'UPPER(TRIM(PO_NBR))', str_col("PurchaseOrderNumber", 20)),
        ("LineNumber", 'PO_LINE_NBR', int_col("LineNumber")),
        ("SourceItemCode", 'UPPER(TRIM(ITEM_CODE))', str_col("SourceItemCode", 30)),
        ("SourceUomCode", 'UPPER(TRIM(ISNULL(UOM_CD) ? "EA" : UOM_CD))', str_col("SourceUomCode", 6)),
    ])
    line.lookup(
        "Lookup UoM Conversion (Full Cache)", CONN_STAGING,
        "SELECT FromUomCode AS SourceUomCode, ToUomCode, ConversionFactor\n"
        "FROM ref.UomConversion WHERE ToUomCode = N'EA';",
        ["SourceUomCode"], [str_col("ToUomCode", 6), dec_col("ConversionFactor")], no_match="RD")
    line.lookup(
        "Lookup Product Crosswalk (Partial Cache)", CONN_STAGING,
        "SELECT SourceItemCode, ProductKey, StockItemId\n"
        "FROM work.ProductCrosswalk WHERE SourceSystemCode = N'ORA_ERP';",
        ["SourceItemCode"], [int_col("ProductKey"), int_col("StockItemId")], no_match="RD")
    line.derived_column("Convert PO Line Quantities", [
        ("OrderQuantityBase", '(DT_NUMERIC,18,4)(ORDER_QTY * ConversionFactor)', dec_col("OrderQuantityBase", 18, 4)),
        ("UnitPriceAmount", 'ISNULL(UNIT_PRICE_AMT) ? (DT_NUMERIC,18,2)0 : UNIT_PRICE_AMT',
         money_col("UnitPriceAmount")),
        ("ExtendedAmount", '(DT_NUMERIC,18,2)(ORDER_QTY * (ISNULL(UNIT_PRICE_AMT) ? (DT_NUMERIC,18,2)0 : UNIT_PRICE_AMT))',
         money_col("ExtendedAmount")),
        ("TaxCode", 'UPPER(TRIM(ISNULL(TAX_CODE) ? "NONE" : TAX_CODE))', str_col("TaxCode", 10)),
        ("NeedByDate", 'ISNULL(NEED_BY_DT) ? (DT_DBTIMESTAMP)"1900-01-01" : NEED_BY_DT', date_col("NeedByDate")),
    ])
    line.conditional_split("Screen PO Line", [
        ("Valid Line", 'OrderQuantityBase > 0 && UnitPriceAmount >= 0'),
        ("Zero Quantity", 'OrderQuantityBase <= 0'),
    ], default_output="Negative Price")
    line.row_count("Count Line Rows Loaded", "User::RowsUpdated")
    line.oledb_destination("STG PurchaseOrderLine", CONN_STAGING, "[stg].[PurchaseOrderLine]", batch_size=50000)
    line.branch_destination("ERR PO Line Zero Quantity", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen PO Line", "Zero Quantity")
    line.branch_destination("ERR PO Line Negative Price", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen PO Line", "Negative Price")
    line.reject_destination("ERR PO Line Unknown Item", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Product Crosswalk (Partial Cache)", "Lookup No Match Output")
    line.reject_destination("ERR PO Line Unknown Uom", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup UoM Conversion (Full Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_PurchaseOrder",
        "Incrementally append Oracle purchase orders into stg.PurchaseOrder and "
        "stg.PurchaseOrderLine using the etl watermark. Header amounts are converted with the "
        "effective-dated FX rate; line quantities are converted to the base unit of measure and "
        "resolved through work.ProductCrosswalk.",
        ORA, "stg.PurchaseOrder", [hdr, line], watermark=True,
        post_tasks=[exec_proc("Rebuild Product Crosswalk Gaps",
                              "EXEC work.usp_BuildProductCrosswalk @BatchId = ?, @OnlyMissing = 1;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedLookupFailure]", "stg.PurchaseOrderLine",
                                 "r.BusinessKey", "RejectReasonCode", stage="Transform"))


@package
def stg_load_ap_invoice():
    """raw.OracleApInvoiceHdr/Line -> stg.ApInvoice + stg.ApInvoiceLine with regional tax."""
    hdr_cols = [
        str_col("INVOICE_NBR", 30), str_col("VENDOR_CODE", 20), str_col("REGION_CD", 4),
        str_col("INV_CCY", 3), money_col("GROSS_AMT"), money_col("TAX_AMT"),
        date_col("INVOICE_DT"), date_col("DUE_DT"), str_col("TERMS_CD", 10),
        str_col("HOLD_FLAG", 1),
    ]
    hdr = DataFlow("DFT AP Invoice Header",
                   "Header conformance with region-specific tax semantics (sales tax / VAT / GST)")
    hdr.oledb_source(
        "RAW Oracle AP Invoice Header", CONN_STAGING,
        "SELECT INVOICE_NBR, VENDOR_CODE, REGION_CD, INV_CCY, GROSS_AMT, TAX_AMT,\n"
        "       INVOICE_DT, DUE_DT, TERMS_CD, HOLD_FLAG\n"
        "FROM raw.OracleApInvoiceHdr\n"
        "WHERE INVOICE_DT > CONVERT(datetime2(3), ?) AND INVOICE_DT <= CONVERT(datetime2(3), ?);",
        hdr_cols, timeout=7200)
    hdr.row_count("Count Invoice Rows Read", "User::RowsRead")
    hdr.derived_column("Classify Invoice Tax", [
        ("InvoiceNumber", 'UPPER(TRIM(INVOICE_NBR))', str_col("InvoiceNumber", 30)),
        ("SupplierCode", 'UPPER(TRIM(VENDOR_CODE))', str_col("SupplierCode", 20)),
        ("RegionCode", 'UPPER(TRIM(REGION_CD))', str_col("RegionCode", 4)),
        ("TaxRegimeCode",
         'UPPER(TRIM(REGION_CD)) == "EU" ? "VAT" : (UPPER(TRIM(REGION_CD)) == "APAC" ? "GST" : "SUT")',
         str_col("TaxRegimeCode", 3)),
        ("TaxRecoverableFlag",
         'UPPER(TRIM(REGION_CD)) == "NA" ? "N" : "Y"', str_col("TaxRecoverableFlag", 1)),
        ("GrossAmount", 'ISNULL(GROSS_AMT) ? (DT_NUMERIC,18,2)0 : GROSS_AMT', money_col("GrossAmount")),
        ("TaxAmount", 'ISNULL(TAX_AMT) ? (DT_NUMERIC,18,2)0 : TAX_AMT', money_col("TaxAmount")),
        ("NetAmount",
         '(DT_NUMERIC,18,2)((ISNULL(GROSS_AMT) ? (DT_NUMERIC,18,2)0 : GROSS_AMT) - '
         '(ISNULL(TAX_AMT) ? (DT_NUMERIC,18,2)0 : TAX_AMT))', money_col("NetAmount")),
        ("InvoiceCurrencyCode", 'UPPER(TRIM(INV_CCY))', str_col("InvoiceCurrencyCode", 3)),
        ("PaymentTermsCode", 'UPPER(TRIM(ISNULL(TERMS_CD) ? "NET30" : TERMS_CD))', str_col("PaymentTermsCode", 10)),
        ("OnHoldFlag", 'UPPER(TRIM(ISNULL(HOLD_FLAG) ? "N" : HOLD_FLAG))', str_col("OnHoldFlag", 1)),
        ("FxEffectiveDate", '(DT_DBDATE)INVOICE_DT', date_col("FxEffectiveDate")),
    ])
    hdr.lookup(
        "Lookup Payment Terms (Full Cache)", CONN_STAGING,
        "SELECT PaymentTermsCode, NetDays, DiscountPercent, DiscountDays\n"
        "FROM ref.PaymentTerms WHERE IsActive = 1;",
        ["PaymentTermsCode"],
        [int_col("NetDays"), dec_col("DiscountPercent", 9, 4), int_col("DiscountDays")], no_match="RD")
    hdr.lookup(
        "Lookup Invoice FX Rate (No Cache)", CONN_STAGING,
        "SELECT FromCurrencyCode AS InvoiceCurrencyCode, RateDate AS FxEffectiveDate, ConversionRate\n"
        "FROM stg.FxRate WHERE ToCurrencyCode = N'USD';",
        ["InvoiceCurrencyCode", "FxEffectiveDate"], [dec_col("ConversionRate")], no_match="RD")
    hdr.derived_column("Derive Invoice Reporting Amounts", [
        ("GrossAmountUsd", '(DT_NUMERIC,18,2)(GrossAmount * ConversionRate)', money_col("GrossAmountUsd")),
        ("DueDate",
         'ISNULL(DUE_DT) ? DATEADD("day", NetDays, INVOICE_DT) : DUE_DT', date_col("DueDate")),
        ("DiscountDueDate", 'DATEADD("day", DiscountDays, INVOICE_DT)', date_col("DiscountDueDate")),
        ("ChangeHash", hash_expression(["InvoiceNumber", "SupplierCode", "GrossAmount",
                                        "TaxAmount", "OnHoldFlag"]), str_col("ChangeHash", 128)),
    ])
    hdr.conditional_split("Screen AP Invoice", [
        ("Valid Invoice", 'GrossAmount > 0 && TaxAmount <= GrossAmount'),
        ("Tax Exceeds Gross", 'TaxAmount > GrossAmount'),
    ], default_output="Non Positive Gross")
    hdr.row_count("Count Invoice Rows Loaded", "User::RowsInserted")
    hdr.oledb_destination("STG ApInvoice", CONN_STAGING, "[stg].[ApInvoice]", batch_size=20000)
    hdr.branch_destination("ERR Invoice Tax Exceeds Gross", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                           "Screen AP Invoice", "Tax Exceeds Gross")
    hdr.branch_destination("ERR Invoice Non Positive", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                           "Screen AP Invoice", "Non Positive Gross")
    hdr.reject_destination("ERR Invoice Unknown Terms", CONN_STAGING, "[err].[RejectedLookupFailure]",
                           "Lookup Payment Terms (Full Cache)", "Lookup No Match Output")
    hdr.reject_destination("ERR Invoice Unknown FX", CONN_STAGING, "[err].[RejectedLookupFailure]",
                           "Lookup Invoice FX Rate (No Cache)", "Lookup No Match Output")

    line_cols = [
        str_col("INVOICE_NBR", 30), int_col("INV_LINE_NBR"), str_col("COST_CENTER_CD", 12),
        str_col("GL_ACCOUNT_CD", 20), money_col("LINE_AMT"), str_col("TAX_CODE", 10),
        str_col("PO_NBR", 20),
    ]
    line = DataFlow("DFT AP Invoice Line", "Distribution lines with cost-centre and tax-rate resolution")
    line.oledb_source(
        "RAW Oracle AP Invoice Line", CONN_STAGING,
        "SELECT l.INVOICE_NBR, l.INV_LINE_NBR, l.COST_CENTER_CD, l.GL_ACCOUNT_CD,\n"
        "       l.LINE_AMT, l.TAX_CODE, l.PO_NBR\n"
        "FROM raw.OracleApInvoiceLine AS l\n"
        "     INNER JOIN raw.OracleApInvoiceHdr AS h ON h.INVOICE_NBR = l.INVOICE_NBR\n"
        "WHERE h.INVOICE_DT > CONVERT(datetime2(3), ?) AND h.INVOICE_DT <= CONVERT(datetime2(3), ?);",
        line_cols, timeout=7200)
    line.derived_column("Standardize Invoice Line", [
        ("InvoiceNumber", 'UPPER(TRIM(INVOICE_NBR))', str_col("InvoiceNumber", 30)),
        ("LineNumber", 'INV_LINE_NBR', int_col("LineNumber")),
        ("CostCenterCode", 'UPPER(TRIM(ISNULL(COST_CENTER_CD) ? "UNALLOC" : COST_CENTER_CD))',
         str_col("CostCenterCode", 12)),
        ("GlAccountCode", 'REPLACE(UPPER(TRIM(GL_ACCOUNT_CD)), " ", "")', str_col("GlAccountCode", 20)),
        ("TaxCode", 'UPPER(TRIM(ISNULL(TAX_CODE) ? "NONE" : TAX_CODE))', str_col("TaxCode", 10)),
        ("LineAmount", 'ISNULL(LINE_AMT) ? (DT_NUMERIC,18,2)0 : LINE_AMT', money_col("LineAmount")),
    ])
    line.lookup(
        "Lookup Cost Center (Full Cache)", CONN_STAGING,
        "SELECT CostCenterCode, CostCenterName, OwningRegionCode\n"
        "FROM stg.CostCenter;",
        ["CostCenterCode"], [str_col("CostCenterName", 80), str_col("OwningRegionCode", 4)], no_match="RD")
    line.lookup(
        "Lookup Tax Rate (Partial Cache)", CONN_STAGING,
        "SELECT TaxCode, TaxRatePercent, TaxRegimeCode\n"
        "FROM stg.TaxRate WHERE ValidToDate IS NULL;",
        ["TaxCode"], [dec_col("TaxRatePercent", 9, 4), str_col("LineTaxRegimeCode", 3)], no_match="IG")
    line.derived_column("Derive Line Tax", [
        ("LineTaxAmount",
         'ISNULL(TaxRatePercent) ? (DT_NUMERIC,18,2)0 : (DT_NUMERIC,18,2)(LineAmount * TaxRatePercent / 100)',
         money_col("LineTaxAmount")),
        ("PurchaseOrderNumber", 'ISNULL(PO_NBR) ? NULL(DT_WSTR,20) : UPPER(TRIM(PO_NBR))',
         str_col("PurchaseOrderNumber", 20)),
    ])
    line.row_count("Count Invoice Line Rows", "User::RowsUpdated")
    line.oledb_destination("STG ApInvoiceLine", CONN_STAGING, "[stg].[ApInvoiceLine]", batch_size=50000)
    line.reject_destination("ERR Invoice Line Unknown Cost Center", CONN_STAGING,
                            "[err].[RejectedLookupFailure]",
                            "Lookup Cost Center (Full Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_ApInvoice",
        "Incrementally append Oracle AP invoices and distribution lines. Header tax is classified "
        "into the regional regime (SUT / VAT / GST), reporting amounts are converted with the "
        "effective-dated FX rate, and distribution lines resolve cost centre and tax rate.",
        ORA, "stg.ApInvoice", [hdr, line], watermark=True,
        post_tasks=[exec_proc("Convert Invoice Currency Amounts",
                              "EXEC stg.usp_ConvertCurrencyAmounts @BatchId = ?, @ObjectName = N'stg.ApInvoice';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedInvoiceLine]", "stg.ApInvoice", "r.InvoiceNumber",
                                 "RejectReasonCode"))


@package
def stg_load_payment():
    """raw.OracleApPayment -> stg.Payment with settlement-currency handling."""
    cols = [
        str_col("PAYMENT_NBR", 30), str_col("VENDOR_CODE", 20), str_col("PAY_METHOD_CD", 6),
        str_col("BANK_ACCT_CD", 20), str_col("PAY_CCY", 3), money_col("PAY_AMT"),
        date_col("PAY_DT"), date_col("VALUE_DT"), str_col("PAY_STATUS_CD", 4),
        str_col("REGION_CD", 4),
    ]
    flow = DataFlow("DFT Conform Payment", "Payment conformance, method crosswalk and value-date rules")
    flow.oledb_source(
        "RAW Oracle AP Payment", CONN_STAGING,
        "SELECT PAYMENT_NBR, VENDOR_CODE, PAY_METHOD_CD, BANK_ACCT_CD, PAY_CCY, PAY_AMT,\n"
        "       PAY_DT, VALUE_DT, PAY_STATUS_CD, REGION_CD\n"
        "FROM raw.OracleApPayment\n"
        "WHERE PAY_DT > CONVERT(datetime2(3), ?) AND PAY_DT <= CONVERT(datetime2(3), ?);",
        cols, timeout=3600)
    flow.row_count("Count Payment Rows Read", "User::RowsRead")
    flow.data_conversion("Convert Payment Types", [
        ("PAY_AMT", "PaymentAmount", money_col("PaymentAmount")),
        ("PAY_DT", "PaymentDate", date_col("PaymentDate")),
    ])
    flow.derived_column("Standardize Payment", [
        ("PaymentNumber", 'UPPER(TRIM(PAYMENT_NBR))', str_col("PaymentNumber", 30)),
        ("SupplierCode", 'UPPER(TRIM(VENDOR_CODE))', str_col("SupplierCode", 20)),
        ("SourcePaymentMethodCode", 'UPPER(TRIM(PAY_METHOD_CD))', str_col("SourcePaymentMethodCode", 6)),
        ("BankAccountCode", 'RIGHT(TRIM(BANK_ACCT_CD), 4)', str_col("BankAccountCode", 4)),
        ("PaymentCurrencyCode", 'UPPER(TRIM(PAY_CCY))', str_col("PaymentCurrencyCode", 3)),
        ("PaymentStatusCode",
         'UPPER(TRIM(PAY_STATUS_CD)) == "P" ? "PAID" : (UPPER(TRIM(PAY_STATUS_CD)) == "V" ? "VOID" : "PEND")',
         str_col("PaymentStatusCode", 4)),
        ("RegionCode", 'UPPER(TRIM(REGION_CD))', str_col("RegionCode", 4)),
        ("ValueDate",
         'ISNULL(VALUE_DT) ? (UPPER(TRIM(REGION_CD)) == "EU" ? DATEADD("day", 2, PAY_DT) : '
         'DATEADD("day", 1, PAY_DT)) : VALUE_DT', date_col("ValueDate")),
    ])
    flow.lookup(
        "Lookup Payment Method Crosswalk (Full Cache)", CONN_STAGING,
        "SELECT SourceCode AS SourcePaymentMethodCode, TargetCode AS PaymentMethodCode, Description\n"
        "FROM ref.CodeCrosswalk WHERE CodeSetName = N'PAYMENT_METHOD' AND SourceSystemCode = N'ORA_ERP';",
        ["SourcePaymentMethodCode"],
        [str_col("PaymentMethodCode", 12), str_col("PaymentMethodDescription", 80)], no_match="RD")
    flow.derived_column("Derive Payment Hash", [
        ("ChangeHash", hash_expression(["PaymentNumber", "SupplierCode", "PaymentAmount",
                                        "PaymentStatusCode"]), str_col("ChangeHash", 128)),
    ])
    flow.conditional_split("Screen Payment", [
        ("Valid Payment", 'PaymentAmount > 0 && PaymentDate <= GETDATE()'),
        ("Future Dated", 'PaymentDate > GETDATE()'),
    ], default_output="Non Positive Amount")
    flow.row_count("Count Payment Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG Payment", CONN_STAGING, "[stg].[Payment]", batch_size=25000)
    flow.branch_destination("ERR Payment Future Dated", CONN_STAGING, "[err].[RejectedPayment]",
                            "Screen Payment", "Future Dated")
    flow.branch_destination("ERR Payment Non Positive", CONN_STAGING, "[err].[RejectedPayment]",
                            "Screen Payment", "Non Positive Amount")
    flow.reject_destination("ERR Payment Unknown Method", CONN_STAGING, "[err].[RejectedPayment]",
                            "Lookup Payment Method Crosswalk (Full Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_Payment",
        "Incrementally append Oracle AP payments into stg.Payment. Payment methods are crosswalked "
        "to the conformed code set, value dates default by region (EU settles at T+2, everywhere "
        "else at T+1), and future-dated or non-positive payments are rejected.",
        ORA, "stg.Payment", [flow], watermark=True,
        post_tasks=[exec_proc("Append Incremental Payment",
                              "EXEC stg.usp_AppendIncremental_Payment @BatchId = ?, @PackageExecutionId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                                                  ("User::PackageExecutionId", 1, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedPayment]", "stg.Payment", "r.PaymentNumber",
                                 "RejectReasonCode"))


@package
def stg_load_gl_journal():
    """raw.OracleGlJournalLine -> stg.GlJournalLine with regional fiscal calendars."""
    cols = [
        str_col("JOURNAL_ID", 30), int_col("JOURNAL_LINE_NBR"), str_col("LEDGER_CD", 8),
        str_col("GL_ACCOUNT_CD", 20), str_col("COST_CENTER_CD", 12), str_col("JRNL_CCY", 3),
        money_col("DEBIT_AMT"), money_col("CREDIT_AMT"), date_col("ACCOUNTING_DT"),
        str_col("PERIOD_NAME", 12), str_col("SOURCE_CD", 10), str_col("REGION_CD", 4),
    ]
    flow = DataFlow("DFT Conform GL Journal", "Journal line conformance with fiscal-period derivation")
    flow.oledb_source(
        "RAW Oracle GL Journal Line", CONN_STAGING,
        "SELECT JOURNAL_ID, JOURNAL_LINE_NBR, LEDGER_CD, GL_ACCOUNT_CD, COST_CENTER_CD,\n"
        "       JRNL_CCY, DEBIT_AMT, CREDIT_AMT, ACCOUNTING_DT, PERIOD_NAME, SOURCE_CD, REGION_CD\n"
        "FROM raw.OracleGlJournalLine\n"
        "WHERE ACCOUNTING_DT > CONVERT(datetime2(3), ?) AND ACCOUNTING_DT <= CONVERT(datetime2(3), ?);",
        cols, timeout=7200)
    flow.row_count("Count Journal Rows Read", "User::RowsRead")
    flow.derived_column("Derive Fiscal Attributes", [
        ("JournalId", 'UPPER(TRIM(JOURNAL_ID))', str_col("JournalId", 30)),
        ("JournalLineNumber", 'JOURNAL_LINE_NBR', int_col("JournalLineNumber")),
        ("LedgerCode", 'UPPER(TRIM(LEDGER_CD))', str_col("LedgerCode", 8)),
        ("GlAccountCode", 'REPLACE(UPPER(TRIM(GL_ACCOUNT_CD)), "-", "")', str_col("GlAccountCode", 20)),
        ("CostCenterCode", 'UPPER(TRIM(ISNULL(COST_CENTER_CD) ? "UNALLOC" : COST_CENTER_CD))',
         str_col("CostCenterCode", 12)),
        ("RegionCode", 'UPPER(TRIM(REGION_CD))', str_col("RegionCode", 4)),
        # NA books on a July fiscal year, EU on the calendar year, APAC on an April year.
        ("FiscalYear",
         'UPPER(TRIM(REGION_CD)) == "NA" ? (MONTH(ACCOUNTING_DT) >= 7 ? YEAR(ACCOUNTING_DT) + 1 : YEAR(ACCOUNTING_DT)) '
         ': (UPPER(TRIM(REGION_CD)) == "APAC" ? (MONTH(ACCOUNTING_DT) >= 4 ? YEAR(ACCOUNTING_DT) + 1 : '
         'YEAR(ACCOUNTING_DT)) : YEAR(ACCOUNTING_DT))', int_col("FiscalYear")),
        ("FiscalPeriod",
         'UPPER(TRIM(REGION_CD)) == "NA" ? ((MONTH(ACCOUNTING_DT) + 5) % 12) + 1 '
         ': (UPPER(TRIM(REGION_CD)) == "APAC" ? ((MONTH(ACCOUNTING_DT) + 8) % 12) + 1 : MONTH(ACCOUNTING_DT))',
         int_col("FiscalPeriod")),
        ("DebitAmount", 'ISNULL(DEBIT_AMT) ? (DT_NUMERIC,18,2)0 : DEBIT_AMT', money_col("DebitAmount")),
        ("CreditAmount", 'ISNULL(CREDIT_AMT) ? (DT_NUMERIC,18,2)0 : CREDIT_AMT', money_col("CreditAmount")),
        ("SignedAmount",
         '(DT_NUMERIC,18,2)((ISNULL(DEBIT_AMT) ? (DT_NUMERIC,18,2)0 : DEBIT_AMT) - '
         '(ISNULL(CREDIT_AMT) ? (DT_NUMERIC,18,2)0 : CREDIT_AMT))', money_col("SignedAmount")),
        ("JournalCurrencyCode", 'UPPER(TRIM(JRNL_CCY))', str_col("JournalCurrencyCode", 3)),
        ("SourceCode", 'UPPER(TRIM(ISNULL(SOURCE_CD) ? "MANUAL" : SOURCE_CD))', str_col("SourceCode", 10)),
    ])
    flow.lookup(
        "Lookup GL Account (Full Cache)", CONN_STAGING,
        "SELECT AccountCode AS GlAccountCode, AccountTypeCode, IsIntercompany\n"
        "FROM ref.GlAccount;",
        ["GlAccountCode"], [str_col("AccountTypeCode", 4), bool_col("IsIntercompany")], no_match="RD")
    flow.conditional_split("Screen Journal Line", [
        ("Valid Line", '(DebitAmount > 0 && CreditAmount == 0) || (CreditAmount > 0 && DebitAmount == 0)'),
        ("Both Sides Populated", 'DebitAmount > 0 && CreditAmount > 0'),
    ], default_output="Zero Value Line")
    flow.row_count("Count Journal Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG GlJournalLine", CONN_STAGING, "[stg].[GlJournalLine]", batch_size=100000)
    flow.branch_destination("ERR Journal Both Sides", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen Journal Line", "Both Sides Populated")
    flow.branch_destination("ERR Journal Zero Value", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen Journal Line", "Zero Value Line")
    flow.reject_destination("ERR Journal Unknown Account", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup GL Account (Full Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_GlJournal",
        "Incrementally append Oracle GL journal lines. Fiscal year and period are derived from the "
        "regional calendar (NA July year-end, EU calendar year, APAC April year-end) and the "
        "debit/credit pair is collapsed into a signed amount for downstream aggregation.",
        ORA, "stg.GlJournalLine", [flow], watermark=True,
        post_tasks=[exec_proc("Convert Journal Currency Amounts",
                              "EXEC stg.usp_ConvertCurrencyAmounts @BatchId = ?, "
                              "@ObjectName = N'stg.GlJournalLine';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedConstraintViolation]", "stg.GlJournalLine",
                                 "r.JournalId", "RejectReasonCode"))


# ---------------------------------------------------------------------------
# OLTP sales and inventory
# ---------------------------------------------------------------------------


@package
def stg_load_order():
    """raw.SqlOrder / raw.SqlOrderLine -> stg.Order + stg.OrderLine."""
    hdr_cols = [
        int_col("OrderID"), int_col("CustomerID"), int_col("SalespersonPersonID"),
        date_col("OrderDate"), date_col("ExpectedDeliveryDate"), str_col("CustomerPurchaseOrderNumber", 20),
        bool_col("IsUndersupplyBackordered"), str_col("Comments", 400), date_col("LastEditedWhen"),
    ]
    hdr = DataFlow("DFT Order Header", "Order header conformance and free-text scrubbing")
    hdr.oledb_source(
        "RAW SQL Order", CONN_STAGING,
        "SELECT OrderID, CustomerID, SalespersonPersonID, OrderDate, ExpectedDeliveryDate,\n"
        "       CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, LastEditedWhen\n"
        "FROM raw.SqlOrder\n"
        "WHERE LastEditedWhen > CONVERT(datetime2(3), ?) AND LastEditedWhen <= CONVERT(datetime2(3), ?);",
        hdr_cols, timeout=3600)
    hdr.row_count("Count Order Rows Read", "User::RowsRead")
    hdr.derived_column("Clean Order Header", [
        ("OrderId", 'OrderID', int_col("OrderId")),
        ("CustomerId", 'CustomerID', int_col("CustomerId")),
        ("SalespersonId", 'ISNULL(SalespersonPersonID) ? -1 : SalespersonPersonID', int_col("SalespersonId")),
        ("CustomerPoNumber",
         'ISNULL(CustomerPurchaseOrderNumber) ? "" : UPPER(TRIM(CustomerPurchaseOrderNumber))',
         str_col("CustomerPoNumber", 20)),
        ("BackorderFlag", 'IsUndersupplyBackordered ? "Y" : "N"', str_col("BackorderFlag", 1)),
        ("OrderComments",
         'ISNULL(Comments) ? NULL(DT_WSTR,400) : LEFT(TRIM(REPLACE(REPLACE(Comments, "\\r", " "), "\\n", " ")), 400)',
         str_col("OrderComments", 400)),
        ("ExpectedDeliveryDate",
         'ISNULL(ExpectedDeliveryDate) ? DATEADD("day", 3, OrderDate) : ExpectedDeliveryDate',
         date_col("ExpectedDeliveryDate")),
        ("ChangeHash", hash_expression(["OrderId", "CustomerId", "BackorderFlag",
                                        "ExpectedDeliveryDate"]), str_col("ChangeHash", 128)),
    ])
    hdr.lookup(
        "Lookup Customer (Partial Cache)", CONN_STAGING,
        "SELECT CustomerId, CustomerCode, RegionCode FROM stg.Customer;",
        ["CustomerId"], [str_col("CustomerCode", 20), str_col("RegionCode", 4)], no_match="RD")
    hdr.row_count("Count Order Rows Loaded", "User::RowsInserted")
    hdr.oledb_destination("STG Order", CONN_STAGING, "[stg].[Order]", batch_size=50000)
    hdr.reject_destination("ERR Order Unknown Customer", CONN_STAGING, "[err].[RejectedLookupFailure]",
                           "Lookup Customer (Partial Cache)", "Lookup No Match Output")

    line_cols = [
        int_col("OrderLineID"), int_col("OrderID"), int_col("StockItemID"),
        str_col("Description", 100), int_col("Quantity"), money_col("UnitPrice"),
        dec_col("TaxRate", 9, 3), str_col("PackageTypeCode", 10), date_col("PickingCompletedWhen"),
    ]
    line = DataFlow("DFT Order Line", "Line pricing, tax extension and picking state")
    line.oledb_source(
        "RAW SQL Order Line", CONN_STAGING,
        "SELECT l.OrderLineID, l.OrderID, l.StockItemID, l.Description, l.Quantity, l.UnitPrice,\n"
        "       l.TaxRate, l.PackageTypeCode, l.PickingCompletedWhen\n"
        "FROM raw.SqlOrderLine AS l\n"
        "     INNER JOIN raw.SqlOrder AS o ON o.OrderID = l.OrderID\n"
        "WHERE o.LastEditedWhen > CONVERT(datetime2(3), ?) AND o.LastEditedWhen <= CONVERT(datetime2(3), ?);",
        line_cols, timeout=3600)
    line.derived_column("Extend Order Line", [
        ("OrderLineId", 'OrderLineID', int_col("OrderLineId")),
        ("OrderId", 'OrderID', int_col("OrderId")),
        ("StockItemId", 'StockItemID', int_col("StockItemId")),
        ("LineDescription", 'TRIM(Description)', str_col("LineDescription", 100)),
        ("Quantity", 'Quantity', int_col("Quantity")),
        ("UnitPriceAmount", 'ISNULL(UnitPrice) ? (DT_NUMERIC,18,2)0 : UnitPrice', money_col("UnitPriceAmount")),
        ("ExtendedAmount", '(DT_NUMERIC,18,2)(Quantity * (ISNULL(UnitPrice) ? (DT_NUMERIC,18,2)0 : UnitPrice))',
         money_col("ExtendedAmount")),
        ("LineTaxAmount",
         '(DT_NUMERIC,18,2)(Quantity * (ISNULL(UnitPrice) ? (DT_NUMERIC,18,2)0 : UnitPrice) * '
         '(ISNULL(TaxRate) ? (DT_NUMERIC,9,3)0 : TaxRate) / 100)', money_col("LineTaxAmount")),
        ("PickedFlag", 'ISNULL(PickingCompletedWhen) ? "N" : "Y"', str_col("PickedFlag", 1)),
        ("PackageTypeCode", 'UPPER(TRIM(ISNULL(PackageTypeCode) ? "EACH" : PackageTypeCode))',
         str_col("PackageTypeCode", 10)),
    ])
    line.conditional_split("Screen Order Line", [
        ("Valid Line", 'Quantity > 0 && UnitPriceAmount >= 0'),
        ("Invalid Quantity", 'Quantity <= 0'),
    ], default_output="Negative Price")
    line.row_count("Count Order Line Rows", "User::RowsUpdated")
    line.oledb_destination("STG OrderLine", CONN_STAGING, "[stg].[OrderLine]", batch_size=100000)
    line.branch_destination("ERR Order Line Quantity", CONN_STAGING, "[err].[RejectedOrderLine]",
                            "Screen Order Line", "Invalid Quantity")
    line.branch_destination("ERR Order Line Price", CONN_STAGING, "[err].[RejectedOrderLine]",
                            "Screen Order Line", "Negative Price")
    line.reject_destination("ERR Order Line Source Errors", CONN_STAGING, "[err].[RejectedOrderLine]",
                            "RAW SQL Order Line", "OLE DB Source Error Output")
    return build_package(
        "STG_Load_Order",
        "Incrementally append OLTP orders and order lines into stg.Order and stg.OrderLine. "
        "Free-text comments are scrubbed of line breaks, delivery dates default to order date + 3 "
        "days and line tax is extended from the OLTP tax rate.",
        OLTP, "stg.Order", [hdr, line], watermark=True,
        post_tasks=[exec_proc("Append Incremental Order Line",
                              "EXEC stg.usp_AppendIncremental_OrderLine @BatchId = ?, "
                              "@PackageExecutionId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                                                  ("User::PackageExecutionId", 1, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedOrderLine]", "stg.OrderLine", "r.BusinessKey",
                                 "RejectReasonCode"))


@package
def stg_load_sale():
    """raw.SqlInvoice / raw.SqlInvoiceLine -> stg.Sale + stg.SaleLine."""
    hdr_cols = [
        int_col("InvoiceID"), int_col("CustomerID"), int_col("OrderID"), date_col("InvoiceDate"),
        str_col("DeliveryMethodCode", 20), str_col("BillToRegionCode", 4), str_col("CurrencyCode", 3),
        date_col("ConfirmedDeliveryTime"), date_col("LastEditedWhen"),
    ]
    hdr = DataFlow("DFT Sale Header", "Invoice header conformance with regional tax regime tagging")
    hdr.oledb_source(
        "RAW SQL Invoice", CONN_STAGING,
        "SELECT InvoiceID, CustomerID, OrderID, InvoiceDate, DeliveryMethodCode, BillToRegionCode,\n"
        "       CurrencyCode, ConfirmedDeliveryTime, LastEditedWhen\n"
        "FROM raw.SqlInvoice\n"
        "WHERE LastEditedWhen > CONVERT(datetime2(3), ?) AND LastEditedWhen <= CONVERT(datetime2(3), ?);",
        hdr_cols, timeout=3600)
    hdr.row_count("Count Sale Rows Read", "User::RowsRead")
    hdr.derived_column("Tag Sale Region", [
        ("InvoiceId", 'InvoiceID', int_col("InvoiceId")),
        ("CustomerId", 'CustomerID', int_col("CustomerId")),
        ("OrderId", 'ISNULL(OrderID) ? -1 : OrderID', int_col("OrderId")),
        ("RegionCode", 'UPPER(TRIM(ISNULL(BillToRegionCode) ? "NA" : BillToRegionCode))',
         str_col("RegionCode", 4)),
        ("TaxRegimeCode",
         'UPPER(TRIM(ISNULL(BillToRegionCode) ? "NA" : BillToRegionCode)) == "EU" ? "VAT" : '
         '(UPPER(TRIM(ISNULL(BillToRegionCode) ? "NA" : BillToRegionCode)) == "APAC" ? "GST" : "SUT")',
         str_col("TaxRegimeCode", 3)),
        ("DeliveryMethodCode", 'UPPER(TRIM(ISNULL(DeliveryMethodCode) ? "UNKNOWN" : DeliveryMethodCode))',
         str_col("DeliveryMethodCode", 20)),
        ("SaleCurrencyCode", 'UPPER(TRIM(ISNULL(CurrencyCode) ? "USD" : CurrencyCode))',
         str_col("SaleCurrencyCode", 3)),
        ("DeliveryConfirmedFlag", 'ISNULL(ConfirmedDeliveryTime) ? "N" : "Y"',
         str_col("DeliveryConfirmedFlag", 1)),
        ("FxEffectiveDate", '(DT_DBDATE)InvoiceDate', date_col("FxEffectiveDate")),
    ])
    hdr.lookup(
        "Lookup Sale FX Rate (No Cache)", CONN_STAGING,
        "SELECT FromCurrencyCode AS SaleCurrencyCode, RateDate AS FxEffectiveDate, ConversionRate\n"
        "FROM stg.FxRate WHERE ToCurrencyCode = N'USD';",
        ["SaleCurrencyCode", "FxEffectiveDate"], [dec_col("ConversionRate")], no_match="IG")
    hdr.derived_column("Default Missing FX", [
        ("EffectiveConversionRate", 'ISNULL(ConversionRate) ? (DT_NUMERIC,18,6)1 : ConversionRate',
         dec_col("EffectiveConversionRate")),
        ("FxImputedFlag", 'ISNULL(ConversionRate) ? "Y" : "N"', str_col("FxImputedFlag", 1)),
        ("ChangeHash", hash_expression(["InvoiceId", "CustomerId", "DeliveryMethodCode",
                                        "DeliveryConfirmedFlag"]), str_col("ChangeHash", 128)),
    ])
    hdr.row_count("Count Sale Rows Loaded", "User::RowsInserted")
    hdr.oledb_destination("STG Sale", CONN_STAGING, "[stg].[Sale]", batch_size=50000)
    hdr.reject_destination("ERR Sale Source Errors", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                           "RAW SQL Invoice", "OLE DB Source Error Output")

    line_cols = [
        int_col("InvoiceLineID"), int_col("InvoiceID"), int_col("StockItemID"), int_col("Quantity"),
        money_col("UnitPrice"), dec_col("TaxRate", 9, 3), money_col("TaxAmount"),
        money_col("LineProfit"), money_col("ExtendedPrice"),
    ]
    line = DataFlow("DFT Sale Line", "Line tax recomputation and margin screening")
    line.oledb_source(
        "RAW SQL Invoice Line", CONN_STAGING,
        "SELECT l.InvoiceLineID, l.InvoiceID, l.StockItemID, l.Quantity, l.UnitPrice, l.TaxRate,\n"
        "       l.TaxAmount, l.LineProfit, l.ExtendedPrice\n"
        "FROM raw.SqlInvoiceLine AS l\n"
        "     INNER JOIN raw.SqlInvoice AS i ON i.InvoiceID = l.InvoiceID\n"
        "WHERE i.LastEditedWhen > CONVERT(datetime2(3), ?) AND i.LastEditedWhen <= CONVERT(datetime2(3), ?);",
        line_cols, timeout=3600)
    line.derived_column("Recompute Sale Line Tax", [
        ("InvoiceLineId", 'InvoiceLineID', int_col("InvoiceLineId")),
        ("InvoiceId", 'InvoiceID', int_col("InvoiceId")),
        ("StockItemId", 'StockItemID', int_col("StockItemId")),
        ("NetAmount", '(DT_NUMERIC,18,2)(Quantity * UnitPrice)', money_col("NetAmount")),
        ("RecomputedTaxAmount", '(DT_NUMERIC,18,2)(Quantity * UnitPrice * TaxRate / 100)',
         money_col("RecomputedTaxAmount")),
        ("TaxVarianceAmount",
         '(DT_NUMERIC,18,2)(ABS((ISNULL(TaxAmount) ? (DT_NUMERIC,18,2)0 : TaxAmount) - '
         '(Quantity * UnitPrice * TaxRate / 100)))', money_col("TaxVarianceAmount")),
        ("LineProfitAmount", 'ISNULL(LineProfit) ? (DT_NUMERIC,18,2)0 : LineProfit',
         money_col("LineProfitAmount")),
    ])
    line.conditional_split("Screen Sale Line", [
        ("Valid Line", 'TaxVarianceAmount <= (DT_NUMERIC,18,2)0.02 && Quantity != 0'),
        ("Tax Mismatch", 'TaxVarianceAmount > (DT_NUMERIC,18,2)0.02'),
    ], default_output="Zero Quantity")
    line.row_count("Count Sale Line Rows", "User::RowsUpdated")
    line.oledb_destination("STG SaleLine", CONN_STAGING, "[stg].[SaleLine]", batch_size=100000)
    line.branch_destination("ERR Sale Line Tax Mismatch", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Screen Sale Line", "Tax Mismatch")
    line.branch_destination("ERR Sale Line Zero Quantity", CONN_STAGING, "[err].[RejectedInvoiceLine]",
                            "Screen Sale Line", "Zero Quantity")
    return build_package(
        "STG_Load_Sale",
        "Incrementally append OLTP invoices and invoice lines into stg.Sale and stg.SaleLine. "
        "Line tax is recomputed and compared with the source value; a variance above two cents is "
        "rejected as a tax mismatch. Missing FX rates are imputed at parity and flagged.",
        OLTP, "stg.Sale", [hdr, line], watermark=True,
        post_tasks=[exec_proc("Append Incremental Sale Line",
                              "EXEC stg.usp_AppendIncremental_SaleLine @BatchId = ?, "
                              "@PackageExecutionId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                                                  ("User::PackageExecutionId", 1, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedInvoiceLine]", "stg.SaleLine", "r.BusinessKey",
                                 "RejectReasonCode"))


@package
def stg_load_stock_item():
    """raw.SqlStockItem -> stg.StockItem (truncate and reload)."""
    cols = [
        int_col("StockItemID"), str_col("StockItemName", 100), str_col("Brand", 50),
        str_col("Size", 20), str_col("Barcode", 20), int_col("SupplierID"),
        money_col("UnitPrice"), money_col("RecommendedRetailPrice"), dec_col("TaxRate", 9, 3),
        dec_col("TypicalWeightPerUnit", 18, 3), str_col("MarketingComments", 400),
        bool_col("IsChillerStock"),
    ]
    flow = DataFlow("DFT Conform Stock Item", "Item cleansing, weight conversion and price banding")
    flow.oledb_source(
        "RAW SQL Stock Item", CONN_STAGING,
        "SELECT StockItemID, StockItemName, Brand, Size, Barcode, SupplierID, UnitPrice,\n"
        "       RecommendedRetailPrice, TaxRate, TypicalWeightPerUnit, MarketingComments, IsChillerStock\n"
        "FROM raw.SqlStockItem;",
        cols, timeout=1800)
    flow.row_count("Count Stock Item Rows Read", "User::RowsRead")
    flow.derived_column("Cleanse Stock Item", [
        ("StockItemId", 'StockItemID', int_col("StockItemId")),
        ("StockItemName", 'TRIM(StockItemName)', str_col("StockItemName", 100)),
        ("BrandName", 'ISNULL(Brand) ? "UNBRANDED" : UPPER(TRIM(Brand))', str_col("BrandName", 50)),
        ("SizeText", 'ISNULL(Size) ? "N/A" : UPPER(TRIM(Size))', str_col("SizeText", 20)),
        ("Barcode", 'ISNULL(Barcode) ? NULL(DT_WSTR,20) : REPLACE(TRIM(Barcode), " ", "")',
         str_col("Barcode", 20)),
        ("SupplierId", 'ISNULL(SupplierID) ? -1 : SupplierID', int_col("SupplierId")),
        ("UnitPriceAmount", 'ISNULL(UnitPrice) ? (DT_NUMERIC,18,2)0 : UnitPrice', money_col("UnitPriceAmount")),
        # Source weights are kilograms; the conformed model stores grams.
        ("TypicalWeightGrams", '(DT_NUMERIC,18,4)(ISNULL(TypicalWeightPerUnit) ? 0 : TypicalWeightPerUnit * 1000)',
         dec_col("TypicalWeightGrams", 18, 4)),
        ("PriceBandCode",
         'ISNULL(UnitPrice) ? "UNK" : (UnitPrice < 10 ? "LOW" : (UnitPrice < 100 ? "MID" : "HGH"))',
         str_col("PriceBandCode", 3)),
        ("ChillerFlag", 'IsChillerStock ? "Y" : "N"', str_col("ChillerFlag", 1)),
        ("MarketingText",
         'ISNULL(MarketingComments) ? NULL(DT_WSTR,400) : LEFT(TRIM(MarketingComments), 400)',
         str_col("MarketingText", 400)),
    ])
    flow.derived_column("Derive Stock Item Hash", [
        ("ChangeHash", hash_expression(["StockItemName", "BrandName", "UnitPriceAmount",
                                        "PriceBandCode", "ChillerFlag"]), str_col("ChangeHash", 128)),
    ])
    flow.conditional_split("Screen Stock Item", [
        ("Valid Item", 'LEN(StockItemName) > 2 && UnitPriceAmount >= 0'),
        ("Missing Name", 'LEN(StockItemName) <= 2'),
    ], default_output="Negative Price")
    flow.row_count("Count Stock Item Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG StockItem", CONN_STAGING, "[stg].[StockItem]", batch_size=25000)
    flow.branch_destination("ERR Stock Item Missing Name", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen Stock Item", "Missing Name")
    flow.branch_destination("ERR Stock Item Negative Price", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen Stock Item", "Negative Price")
    return build_package(
        "STG_Load_StockItem",
        "Truncate and reload stg.StockItem from the OLTP stock item landing. Weights are converted "
        "from kilograms to the conformed gram unit, prices are banded, and the change hash feeds "
        "the downstream product dimension.",
        OLTP, "stg.StockItem", [flow],
        truncate_tables=["[stg].[StockItem]"],
        post_tasks=[exec_proc("Clean Stock Item Strings",
                              "EXEC stg.usp_CleanStringBatch @BatchId = ?, @ObjectName = N'stg.StockItem';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedConstraintViolation]", "stg.StockItem",
                                 "r.BusinessKey", "RejectReasonCode"))


@package
def stg_load_stock_movement():
    """raw.SqlStockMovement -> stg.StockMovement (high-volume incremental append)."""
    cols = [
        bigint_col("StockItemTransactionID"), int_col("StockItemID"), int_col("CustomerID"),
        int_col("SupplierID"), str_col("TransactionTypeCode", 10), int_col("WarehouseSiteId"),
        dec_col("Quantity", 18, 3), str_col("UomCode", 6), date_col("TransactionOccurredWhen"),
    ]
    flow = DataFlow("DFT Conform Stock Movement", "Movement typing, sign convention and UoM rebasing")
    flow.oledb_source(
        "RAW SQL Stock Movement", CONN_STAGING,
        "SELECT StockItemTransactionID, StockItemID, CustomerID, SupplierID, TransactionTypeCode,\n"
        "       WarehouseSiteId, Quantity, UomCode, TransactionOccurredWhen\n"
        "FROM raw.SqlStockMovement\n"
        "WHERE TransactionOccurredWhen > CONVERT(datetime2(3), ?)\n"
        "  AND TransactionOccurredWhen <= CONVERT(datetime2(3), ?);",
        cols, timeout=7200)
    flow.row_count("Count Movement Rows Read", "User::RowsRead")
    flow.lookup(
        "Lookup Transaction Type (Full Cache)", CONN_STAGING,
        "SELECT TransactionTypeCode, TransactionTypeName, MovementSign\n"
        "FROM ref.TransactionType;",
        ["TransactionTypeCode"], [str_col("TransactionTypeName", 60), int_col("MovementSign")],
        no_match="RD")
    flow.lookup(
        "Lookup Movement Uom (Full Cache)", CONN_STAGING,
        "SELECT FromUomCode AS UomCode, ConversionFactor FROM ref.UomConversion WHERE ToUomCode = N'EA';",
        ["UomCode"], [dec_col("MovementConversionFactor")], no_match="IG")
    flow.derived_column("Apply Movement Sign", [
        ("StockMovementId", 'StockItemTransactionID', bigint_col("StockMovementId")),
        ("StockItemId", 'StockItemID', int_col("StockItemId")),
        ("CounterpartyTypeCode",
         'ISNULL(CustomerID) ? (ISNULL(SupplierID) ? "INTERNAL" : "SUPPLIER") : "CUSTOMER"',
         str_col("CounterpartyTypeCode", 10)),
        ("SignedQuantity",
         '(DT_NUMERIC,18,3)(Quantity * MovementSign * (ISNULL(MovementConversionFactor) ? 1 : '
         'MovementConversionFactor))', dec_col("SignedQuantity", 18, 3)),
        ("MovementDate", '(DT_DBDATE)TransactionOccurredWhen', date_col("MovementDate")),
    ])
    flow.conditional_split("Screen Stock Movement", [
        ("Valid Movement", 'SignedQuantity != 0'),
    ], default_output="Zero Movement")
    flow.row_count("Count Movement Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG StockMovement", CONN_STAGING, "[stg].[StockMovement]", batch_size=200000)
    flow.branch_destination("ERR Movement Zero Quantity", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen Stock Movement", "Zero Movement")
    flow.reject_destination("ERR Movement Unknown Type", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Transaction Type (Full Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_StockMovement",
        "Incrementally append OLTP stock movements. The reference transaction type supplies the "
        "movement sign, quantities are rebased to the each unit of measure and the counterparty "
        "type is inferred from which of customer or supplier is populated.",
        OLTP, "stg.StockMovement", [flow], watermark=True,
        post_tasks=[exec_proc("Append Incremental Stock Movement",
                              "EXEC stg.usp_AppendIncremental_StockMovement @BatchId = ?, "
                              "@PackageExecutionId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                                                  ("User::PackageExecutionId", 1, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedLookupFailure]", "stg.StockMovement",
                                 "r.BusinessKey", "RejectReasonCode", stage="Transform"))


@package
def stg_load_shipment():
    """raw.SqlShipment / raw.SqlShipmentLine -> stg.Shipment + stg.ShipmentLine."""
    hdr_cols = [
        int_col("ShipmentID"), int_col("InvoiceID"), str_col("CarrierCode", 10),
        str_col("TrackingNumber", 40), str_col("DestinationCountryCode", 3),
        str_col("DestinationPostalCode", 12), dec_col("GrossWeightKg", 18, 3),
        date_col("DespatchedWhen"), date_col("DeliveredWhen"),
    ]
    hdr = DataFlow("DFT Shipment Header", "Carrier resolution and transit-time derivation")
    hdr.oledb_source(
        "RAW SQL Shipment", CONN_STAGING,
        "SELECT ShipmentID, InvoiceID, CarrierCode, TrackingNumber, DestinationCountryCode,\n"
        "       DestinationPostalCode, GrossWeightKg, DespatchedWhen, DeliveredWhen\n"
        "FROM raw.SqlShipment\n"
        "WHERE DespatchedWhen > CONVERT(datetime2(3), ?) AND DespatchedWhen <= CONVERT(datetime2(3), ?);",
        hdr_cols, timeout=3600)
    hdr.row_count("Count Shipment Rows Read", "User::RowsRead")
    hdr.derived_column("Standardize Shipment", [
        ("ShipmentId", 'ShipmentID', int_col("ShipmentId")),
        ("InvoiceId", 'InvoiceID', int_col("InvoiceId")),
        ("CarrierCode", 'UPPER(TRIM(ISNULL(CarrierCode) ? "UNKN" : CarrierCode))', str_col("CarrierCode", 10)),
        ("TrackingNumber", 'UPPER(REPLACE(TRIM(ISNULL(TrackingNumber) ? "" : TrackingNumber), " ", ""))',
         str_col("TrackingNumber", 40)),
        ("DestinationCountryCode", 'UPPER(TRIM(DestinationCountryCode))', str_col("DestinationCountryCode", 3)),
        # Postal standardisation differs by destination: NA keeps the five digit ZIP, EU keeps
        # the country prefix, APAC strips separators entirely.
        ("DestinationPostalCode",
         'UPPER(TRIM(DestinationCountryCode)) == "USA" || UPPER(TRIM(DestinationCountryCode)) == "CAN" '
         '? LEFT(REPLACE(TRIM(DestinationPostalCode), " ", ""), 5) '
         ': (UPPER(TRIM(DestinationCountryCode)) == "JPN" || UPPER(TRIM(DestinationCountryCode)) == "AUS" '
         '? REPLACE(REPLACE(TRIM(DestinationPostalCode), "-", ""), " ", "") '
         ': UPPER(TRIM(DestinationCountryCode)) + "-" + REPLACE(TRIM(DestinationPostalCode), " ", ""))',
         str_col("DestinationPostalCode", 16)),
        ("GrossWeightGrams", '(DT_NUMERIC,18,3)(ISNULL(GrossWeightKg) ? 0 : GrossWeightKg * 1000)',
         dec_col("GrossWeightGrams", 18, 3)),
        ("DeliveredFlag", 'ISNULL(DeliveredWhen) ? "N" : "Y"', str_col("DeliveredFlag", 1)),
        ("TransitDays",
         'ISNULL(DeliveredWhen) ? -1 : DATEDIFF("day", DespatchedWhen, DeliveredWhen)', int_col("TransitDays")),
    ])
    hdr.lookup(
        "Lookup Carrier (Full Cache)", CONN_STAGING,
        "SELECT CarrierCode, CarrierName, ServiceLevelCode FROM ref.Carrier WHERE IsActive = 1;",
        ["CarrierCode"], [str_col("CarrierName", 80), str_col("ServiceLevelCode", 8)], no_match="RD")
    hdr.row_count("Count Shipment Rows Loaded", "User::RowsInserted")
    hdr.oledb_destination("STG Shipment", CONN_STAGING, "[stg].[Shipment]", batch_size=50000)
    hdr.reject_destination("ERR Shipment Unknown Carrier", CONN_STAGING, "[err].[RejectedLookupFailure]",
                           "Lookup Carrier (Full Cache)", "Lookup No Match Output")

    line_cols = [
        int_col("ShipmentLineID"), int_col("ShipmentID"), int_col("StockItemID"),
        int_col("QuantityShipped"), dec_col("LineWeightKg", 18, 3),
    ]
    line = DataFlow("DFT Shipment Line", "Line weight rebasing and shipment aggregate rejoin")
    line.oledb_source(
        "RAW SQL Shipment Line", CONN_STAGING,
        "SELECT l.ShipmentLineID, l.ShipmentID, l.StockItemID, l.QuantityShipped, l.LineWeightKg\n"
        "FROM raw.SqlShipmentLine AS l\n"
        "     INNER JOIN raw.SqlShipment AS s ON s.ShipmentID = l.ShipmentID\n"
        "WHERE s.DespatchedWhen > CONVERT(datetime2(3), ?) AND s.DespatchedWhen <= CONVERT(datetime2(3), ?);",
        line_cols, timeout=3600)
    line.derived_column("Rebase Line Weight", [
        ("ShipmentLineId", 'ShipmentLineID', int_col("ShipmentLineId")),
        ("ShipmentId", 'ShipmentID', int_col("ShipmentId")),
        ("StockItemId", 'StockItemID', int_col("StockItemId")),
        ("QuantityShipped", 'QuantityShipped', int_col("QuantityShipped")),
        ("LineWeightGrams", '(DT_NUMERIC,18,3)(ISNULL(LineWeightKg) ? 0 : LineWeightKg * 1000)',
         dec_col("LineWeightGrams", 18, 3)),
    ])
    line.row_count("Count Shipment Line Rows", "User::RowsUpdated")
    line.oledb_destination("STG ShipmentLine", CONN_STAGING, "[stg].[ShipmentLine]", batch_size=100000)
    line.reject_destination("ERR Shipment Line Source Errors", CONN_STAGING,
                            "[err].[RejectedConstraintViolation]",
                            "RAW SQL Shipment Line", "OLE DB Source Error Output")
    return build_package(
        "STG_Load_Shipment",
        "Incrementally append OLTP shipments and shipment lines. Destination postal codes are "
        "standardised per destination country convention, weights are rebased to grams and transit "
        "days are derived where a delivery timestamp exists.",
        OLTP, "stg.Shipment", [hdr, line], watermark=True,
        post_tasks=[exec_proc("Append Incremental Shipment",
                              "EXEC stg.usp_AppendIncremental_Shipment @BatchId = ?, "
                              "@PackageExecutionId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                                                  ("User::PackageExecutionId", 1, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedLookupFailure]", "stg.Shipment", "r.BusinessKey",
                                 "RejectReasonCode", stage="Transform"))


@package
def stg_load_return_and_credit():
    """raw.SqlReturnLine + raw.SqlCreditNote -> stg.Return + stg.CreditNote."""
    ret_cols = [
        int_col("ReturnLineID"), int_col("InvoiceID"), int_col("StockItemID"),
        int_col("QuantityReturned"), str_col("ReturnReasonCode", 10), str_col("RegionCode", 4),
        date_col("ReturnedWhen"),
    ]
    ret = DataFlow("DFT Return Line", "Return reason crosswalk with regional return windows")
    ret.oledb_source(
        "RAW SQL Return Line", CONN_STAGING,
        "SELECT ReturnLineID, InvoiceID, StockItemID, QuantityReturned, ReturnReasonCode,\n"
        "       RegionCode, ReturnedWhen\n"
        "FROM raw.SqlReturnLine\n"
        "WHERE ReturnedWhen > CONVERT(datetime2(3), ?) AND ReturnedWhen <= CONVERT(datetime2(3), ?);",
        ret_cols, timeout=3600)
    ret.row_count("Count Return Rows Read", "User::RowsRead")
    ret.derived_column("Apply Return Window", [
        ("ReturnLineId", 'ReturnLineID', int_col("ReturnLineId")),
        ("InvoiceId", 'InvoiceID', int_col("InvoiceId")),
        ("StockItemId", 'StockItemID', int_col("StockItemId")),
        ("SourceReturnReasonCode", 'UPPER(TRIM(ISNULL(ReturnReasonCode) ? "UNSTATED" : ReturnReasonCode))',
         str_col("SourceReturnReasonCode", 10)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode))', str_col("RegionCode", 4)),
        # EU distance-selling gives fourteen days, APAC seven, NA the legacy thirty day policy.
        ("ReturnWindowDays",
         'UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode)) == "EU" ? 14 : '
         '(UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode)) == "APAC" ? 7 : 30)',
         int_col("ReturnWindowDays")),
        ("QuantityReturned", 'QuantityReturned', int_col("QuantityReturned")),
    ])
    ret.lookup(
        "Lookup Return Reason (Full Cache)", CONN_STAGING,
        "SELECT SourceCode AS SourceReturnReasonCode, TargetCode AS ReturnReasonCode, Description\n"
        "FROM ref.CodeCrosswalk WHERE CodeSetName = N'RETURN_REASON';",
        ["SourceReturnReasonCode"],
        [str_col("ReturnReasonCode", 12), str_col("ReturnReasonDescription", 80)], no_match="RD")
    ret.conditional_split("Screen Return", [
        ("Valid Return", 'QuantityReturned > 0'),
    ], default_output="Non Positive Quantity")
    ret.row_count("Count Return Rows Loaded", "User::RowsInserted")
    ret.oledb_destination("STG Return", CONN_STAGING, "[stg].[Return]", batch_size=50000)
    ret.branch_destination("ERR Return Non Positive", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                           "Screen Return", "Non Positive Quantity")
    ret.reject_destination("ERR Return Unknown Reason", CONN_STAGING, "[err].[RejectedLookupFailure]",
                           "Lookup Return Reason (Full Cache)", "Lookup No Match Output")

    credit_cols = [
        int_col("CreditNoteID"), int_col("InvoiceID"), str_col("CreditReasonCode", 10),
        money_col("CreditAmount"), str_col("CurrencyCode", 3), date_col("IssuedWhen"),
        str_col("ApprovedBy", 40),
    ]
    credit = DataFlow("DFT Credit Note", "Credit note approval banding and unapproved routing")
    credit.oledb_source(
        "RAW SQL Credit Note", CONN_STAGING,
        "SELECT CreditNoteID, InvoiceID, CreditReasonCode, CreditAmount, CurrencyCode,\n"
        "       IssuedWhen, ApprovedBy\n"
        "FROM raw.SqlCreditNote\n"
        "WHERE IssuedWhen > CONVERT(datetime2(3), ?) AND IssuedWhen <= CONVERT(datetime2(3), ?);",
        credit_cols, timeout=3600)
    credit.derived_column("Band Credit Notes", [
        ("CreditNoteId", 'CreditNoteID', int_col("CreditNoteId")),
        ("InvoiceId", 'InvoiceID', int_col("InvoiceId")),
        ("CreditReasonCode", 'UPPER(TRIM(ISNULL(CreditReasonCode) ? "UNSTATED" : CreditReasonCode))',
         str_col("CreditReasonCode", 10)),
        ("CreditAmount", 'ISNULL(CreditAmount) ? (DT_NUMERIC,18,2)0 : CreditAmount', money_col("CreditAmount")),
        ("ApprovalBandCode",
         'ISNULL(CreditAmount) ? "NONE" : (CreditAmount < 500 ? "AUTO" : (CreditAmount < 5000 ? "MGR" : "FIN"))',
         str_col("ApprovalBandCode", 4)),
        ("ApprovedFlag", 'ISNULL(ApprovedBy) || TRIM(ApprovedBy) == "" ? "N" : "Y"', str_col("ApprovedFlag", 1)),
    ])
    credit.conditional_split("Screen Credit Note", [
        ("Approved Credit", 'ApprovedFlag == "Y" && CreditAmount > 0'),
        ("Unapproved Credit", 'ApprovedFlag == "N"'),
    ], default_output="Zero Credit")
    credit.row_count("Count Credit Rows Loaded", "User::RowsUpdated")
    credit.oledb_destination("STG CreditNote", CONN_STAGING, "[stg].[CreditNote]", batch_size=25000)
    credit.branch_destination("ERR Credit Unapproved", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                              "Screen Credit Note", "Unapproved Credit")
    credit.branch_destination("ERR Credit Zero Amount", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                              "Screen Credit Note", "Zero Credit")
    return build_package(
        "STG_Load_ReturnAndCredit",
        "Incrementally append returns and credit notes. Return reasons are crosswalked to the "
        "conformed code set and carry the regional return window (EU 14 days, APAC 7, NA 30); "
        "credit notes are banded by approval authority and unapproved credits are rejected.",
        OLTP, "stg.Return", [ret, credit], watermark=True,
        post_tasks=[exec_proc("Translate Return Source Codes",
                              "EXEC stg.usp_TranslateSourceCodes @BatchId = ?, @ObjectName = N'stg.Return';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedConstraintViolation]", "stg.Return", "r.BusinessKey",
                                 "RejectReasonCode"))


@package
def stg_load_loyalty_ledger():
    """raw.SqlLoyaltyLedger -> stg.LoyaltyLedger with tier banding and expiry rules."""
    cols = [
        bigint_col("LoyaltyEntryID"), int_col("CustomerID"), str_col("EntryTypeCode", 10),
        int_col("PointsDelta"), str_col("ProgramCode", 10), str_col("RegionCode", 4),
        date_col("EntryDate"), date_col("ExpiryDate"),
    ]
    flow = DataFlow("DFT Conform Loyalty Ledger", "Points typing, expiry defaulting and tier attribution")
    flow.oledb_source(
        "RAW SQL Loyalty Ledger", CONN_STAGING,
        "SELECT LoyaltyEntryID, CustomerID, EntryTypeCode, PointsDelta, ProgramCode, RegionCode,\n"
        "       EntryDate, ExpiryDate\n"
        "FROM raw.SqlLoyaltyLedger\n"
        "WHERE EntryDate > CONVERT(datetime2(3), ?) AND EntryDate <= CONVERT(datetime2(3), ?);",
        cols, timeout=3600)
    flow.row_count("Count Loyalty Rows Read", "User::RowsRead")
    flow.derived_column("Type Loyalty Entries", [
        ("LoyaltyEntryId", 'LoyaltyEntryID', bigint_col("LoyaltyEntryId")),
        ("CustomerId", 'CustomerID', int_col("CustomerId")),
        ("EntryTypeCode", 'UPPER(TRIM(ISNULL(EntryTypeCode) ? "ADJ" : EntryTypeCode))',
         str_col("EntryTypeCode", 10)),
        ("ProgramCode", 'UPPER(TRIM(ISNULL(ProgramCode) ? "BASE" : ProgramCode))', str_col("ProgramCode", 10)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode))', str_col("RegionCode", 4)),
        ("PointsDelta", 'ISNULL(PointsDelta) ? 0 : PointsDelta', int_col("PointsDelta")),
        ("PointsEarned", 'ISNULL(PointsDelta) || PointsDelta < 0 ? 0 : PointsDelta', int_col("PointsEarned")),
        ("PointsRedeemed", 'ISNULL(PointsDelta) || PointsDelta > 0 ? 0 : -PointsDelta', int_col("PointsRedeemed")),
        # EU points expire after 12 months for consent-retention reasons, APAC after 18, NA after 24.
        ("ExpiryDate",
         'ISNULL(ExpiryDate) ? (UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode)) == "EU" ? '
         'DATEADD("month", 12, EntryDate) : (UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode)) == "APAC" ? '
         'DATEADD("month", 18, EntryDate) : DATEADD("month", 24, EntryDate))) : ExpiryDate',
         date_col("ExpiryDate")),
    ])
    flow.aggregate("Aggregate Customer Points", ["CustomerId", "ProgramCode", "RegionCode"], [
        ("PointsEarned", "TotalPointsEarned", "Sum"),
        ("PointsRedeemed", "TotalPointsRedeemed", "Sum"),
        ("LoyaltyEntryId", "EntryCount", "Count"),
    ])
    flow.derived_column("Derive Loyalty Tier", [
        ("NetPointsBalance", 'TotalPointsEarned - TotalPointsRedeemed', int_col("NetPointsBalance")),
        ("LoyaltyTierCode",
         '(TotalPointsEarned - TotalPointsRedeemed) >= 50000 ? "PLT" : '
         '((TotalPointsEarned - TotalPointsRedeemed) >= 20000 ? "GLD" : '
         '((TotalPointsEarned - TotalPointsRedeemed) >= 5000 ? "SLV" : "BRZ"))',
         str_col("LoyaltyTierCode", 3)),
    ])
    flow.lookup(
        "Lookup Loyalty Tier (Full Cache)", CONN_STAGING,
        "SELECT LoyaltyTierCode, TierName, DiscountPercent FROM ref.LoyaltyTier WHERE IsActive = 1;",
        ["LoyaltyTierCode"], [str_col("TierName", 40), dec_col("TierDiscountPercent", 9, 4)], no_match="RD")
    flow.row_count("Count Loyalty Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG LoyaltyLedger", CONN_STAGING, "[stg].[LoyaltyLedger]", batch_size=50000)
    flow.reject_destination("ERR Loyalty Unknown Tier", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Loyalty Tier (Full Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_LoyaltyLedger",
        "Incrementally append loyalty ledger entries, splitting the signed points delta into earned "
        "and redeemed measures, defaulting expiry by regional programme rule and aggregating to a "
        "customer/programme balance that drives the tier banding.",
        OLTP, "stg.LoyaltyLedger", [flow], watermark=True,
        post_tasks=[exec_proc("Translate Loyalty Source Codes",
                              "EXEC stg.usp_TranslateSourceCodes @BatchId = ?, "
                              "@ObjectName = N'stg.LoyaltyLedger';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedLookupFailure]", "stg.LoyaltyLedger", "r.BusinessKey",
                                 "RejectReasonCode", stage="Transform"))


@package
def stg_load_web_session():
    """raw.SqlWebSession -> stg.WebSession with consent-aware attribute suppression."""
    cols = [
        str_col("SessionGuid", 36), int_col("CustomerID"), str_col("ChannelCode", 10),
        str_col("DeviceTypeCode", 10), str_col("CountryCode", 3), str_col("UserAgentText", 400),
        str_col("LandingPageUrl", 400), int_col("PageViewCount"), int_col("DurationSeconds"),
        str_col("ConsentFlag", 1), date_col("SessionStartWhen"),
    ]
    flow = DataFlow("DFT Conform Web Session", "Consent-aware cleansing of clickstream sessions")
    flow.oledb_source(
        "RAW SQL Web Session", CONN_STAGING,
        "SELECT SessionGuid, CustomerID, ChannelCode, DeviceTypeCode, CountryCode, UserAgentText,\n"
        "       LandingPageUrl, PageViewCount, DurationSeconds, ConsentFlag, SessionStartWhen\n"
        "FROM raw.SqlWebSession\n"
        "WHERE SessionStartWhen > CONVERT(datetime2(3), ?) AND SessionStartWhen <= CONVERT(datetime2(3), ?);",
        cols, timeout=7200)
    flow.row_count("Count Session Rows Read", "User::RowsRead")
    flow.lookup(
        "Lookup Country Region (Full Cache)", CONN_STAGING,
        "SELECT CountryCode, RegionCode FROM ref.Country;",
        ["CountryCode"], [str_col("RegionCode", 4)], no_match="IG")
    flow.derived_column("Apply Consent Rules", [
        ("SessionGuid", 'UPPER(TRIM(SessionGuid))', str_col("SessionGuid", 36)),
        ("RegionCode", 'ISNULL(RegionCode) ? "NA" : RegionCode', str_col("RegionCode", 4)),
        ("ConsentGivenFlag", 'UPPER(TRIM(ISNULL(ConsentFlag) ? "N" : ConsentFlag)) == "Y" ? "Y" : "N"',
         str_col("ConsentGivenFlag", 1)),
        # Without consent the EU rows keep only anonymous aggregates; other regions keep the
        # customer id but still lose the raw user agent string.
        ("CustomerId",
         '(ISNULL(RegionCode) ? "NA" : RegionCode) == "EU" && '
         'UPPER(TRIM(ISNULL(ConsentFlag) ? "N" : ConsentFlag)) != "Y" ? -1 : (ISNULL(CustomerID) ? -1 : CustomerID)',
         int_col("CustomerId")),
        ("UserAgentFamily",
         'UPPER(TRIM(ISNULL(ConsentFlag) ? "N" : ConsentFlag)) != "Y" ? "SUPPRESSED" : '
         'LEFT(TRIM(ISNULL(UserAgentText) ? "UNKNOWN" : UserAgentText), 40)',
         str_col("UserAgentFamily", 40)),
        ("LandingPagePath",
         'ISNULL(LandingPageUrl) ? "/" : LOWER(LEFT(TOKEN(REPLACE(LandingPageUrl, "://", " "), " ", 2), 200))',
         str_col("LandingPagePath", 200)),
        ("ChannelCode", 'UPPER(TRIM(ISNULL(ChannelCode) ? "WEB" : ChannelCode))', str_col("ChannelCode", 10)),
        ("DeviceTypeCode", 'UPPER(TRIM(ISNULL(DeviceTypeCode) ? "UNKNOWN" : DeviceTypeCode))',
         str_col("DeviceTypeCode", 10)),
        ("PageViewCount", 'ISNULL(PageViewCount) ? 0 : PageViewCount', int_col("PageViewCount")),
        ("DurationSeconds", 'ISNULL(DurationSeconds) || DurationSeconds < 0 ? 0 : DurationSeconds',
         int_col("DurationSeconds")),
        ("BounceFlag", 'ISNULL(PageViewCount) || PageViewCount <= 1 ? "Y" : "N"', str_col("BounceFlag", 1)),
    ])
    flow.conditional_split("Screen Web Session", [
        ("Valid Session", 'LEN(SessionGuid) == 36 && DurationSeconds <= 86400'),
        ("Implausible Duration", 'DurationSeconds > 86400'),
    ], default_output="Malformed Session Key")
    flow.row_count("Count Session Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG WebSession", CONN_STAGING, "[stg].[WebSession]", batch_size=200000)
    flow.branch_destination("ERR Session Duration Outlier", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen Web Session", "Implausible Duration")
    flow.branch_destination("ERR Session Malformed Key", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen Web Session", "Malformed Session Key")
    return build_package(
        "STG_Load_WebSession",
        "Incrementally append clickstream sessions. EU sessions without consent are de-identified "
        "before landing, user agent strings are suppressed for all non-consenting rows, landing "
        "URLs are reduced to a path and sessions longer than a day are rejected as outliers.",
        OLTP, "stg.WebSession", [flow], watermark=True,
        post_tasks=[exec_proc("Clean Web Session Strings",
                              "EXEC stg.usp_CleanStringBatch @BatchId = ?, @ObjectName = N'stg.WebSession';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedConstraintViolation]", "stg.WebSession",
                                 "r.BusinessKey", "RejectReasonCode"))


@package
def stg_load_partner_sale():
    """raw.FilePartnerSales -> stg.PartnerSale (flat file feed, all columns arrive as text)."""
    cols = [
        str_col("PartnerCode", 10), str_col("PartnerOrderRef", 30), str_col("SaleDateText", 20),
        str_col("CustomerRef", 40), str_col("ItemRef", 30), str_col("QuantityText", 20),
        str_col("AmountText", 20), str_col("CurrencyText", 10), str_col("CountryText", 20),
        str_col("SourceFileName", 200),
    ]
    flow = DataFlow("DFT Conform Partner Sales", "Parse and type the partner sales flat file feed")
    flow.oledb_source(
        "RAW File Partner Sales", CONN_STAGING,
        "SELECT PartnerCode, PartnerOrderRef, SaleDateText, CustomerRef, ItemRef, QuantityText,\n"
        "       AmountText, CurrencyText, CountryText, SourceFileName\n"
        "FROM raw.FilePartnerSales\n"
        "WHERE BatchId = ?;",
        cols, timeout=3600)
    flow.row_count("Count Partner Rows Read", "User::RowsRead")
    flow.derived_column("Normalize Partner Text", [
        ("PartnerCode", 'UPPER(TRIM(PartnerCode))', str_col("PartnerCode", 10)),
        ("PartnerOrderRef", 'UPPER(TRIM(PartnerOrderRef))', str_col("PartnerOrderRef", 30)),
        ("CustomerRef", 'UPPER(TRIM(CustomerRef))', str_col("CustomerRef", 40)),
        ("ItemRef", 'UPPER(REPLACE(TRIM(ItemRef), " ", ""))', str_col("ItemRef", 30)),
        # Partners send DD/MM/YYYY, the legacy loader only ever handled YYYY-MM-DD.
        ("SaleDateIso",
         'FINDSTRING(SaleDateText, "/", 1) > 0 ? '
         'RIGHT(TRIM(SaleDateText), 4) + "-" + SUBSTRING(TRIM(SaleDateText), 4, 2) + "-" + '
         'LEFT(TRIM(SaleDateText), 2) : LEFT(TRIM(SaleDateText), 10)',
         str_col("SaleDateIso", 10)),
        ("QuantityText", 'REPLACE(TRIM(QuantityText), ",", "")', str_col("QuantityText", 20)),
        ("AmountText", 'REPLACE(REPLACE(TRIM(AmountText), ",", ""), "$", "")', str_col("AmountText", 20)),
        ("PartnerCurrencyCode", 'UPPER(LEFT(TRIM(ISNULL(CurrencyText) ? "USD" : CurrencyText), 3))',
         str_col("PartnerCurrencyCode", 3)),
        ("CountryName", 'UPPER(TRIM(CountryText))', str_col("CountryName", 20)),
    ])
    flow.data_conversion("Type Partner Measures", [
        ("SaleDateIso", "SaleDate", date_col("SaleDate")),
        ("QuantityText", "Quantity", int_col("Quantity")),
        ("AmountText", "GrossAmount", money_col("GrossAmount")),
    ])
    flow.lookup(
        "Lookup Country By Name (Full Cache)", CONN_STAGING,
        "SELECT UPPER(CountryName) AS CountryName, CountryCode, RegionCode FROM ref.Country;",
        ["CountryName"], [str_col("CountryCode", 3), str_col("RegionCode", 4)], no_match="RD")
    flow.lookup(
        "Lookup Partner Customer Crosswalk (Partial Cache)", CONN_STAGING,
        "SELECT SourceCode AS CustomerRef, TargetCode AS CustomerCode\n"
        "FROM ref.CodeCrosswalk WHERE CodeSetName = N'PARTNER_CUSTOMER';",
        ["CustomerRef"], [str_col("CustomerCode", 20)], no_match="RD")
    flow.conditional_split("Screen Partner Sale", [
        ("Valid Partner Row", 'Quantity > 0 && GrossAmount > 0 && LEN(PartnerOrderRef) > 0'),
        ("Unparsable Amount", 'GrossAmount <= 0'),
    ], default_output="Missing Order Reference")
    flow.row_count("Count Partner Rows Loaded", "User::RowsInserted")
    flow.oledb_destination("STG PartnerSale", CONN_STAGING, "[stg].[PartnerSale]", batch_size=50000)
    flow.branch_destination("ERR Partner Unparsable Amount", CONN_STAGING, "[err].[RejectedFileRow]",
                            "Screen Partner Sale", "Unparsable Amount")
    flow.branch_destination("ERR Partner Missing Reference", CONN_STAGING, "[err].[RejectedFileRow]",
                            "Screen Partner Sale", "Missing Order Reference")
    flow.reject_destination("ERR Partner Unknown Country", CONN_STAGING, "[err].[RejectedFileRow]",
                            "Lookup Country By Name (Full Cache)", "Lookup No Match Output")
    flow.reject_destination("ERR Partner Unknown Customer", CONN_STAGING, "[err].[RejectedFileRow]",
                            "Lookup Partner Customer Crosswalk (Partial Cache)", "Lookup No Match Output")
    flow.reject_destination("ERR Partner Conversion Errors", CONN_STAGING, "[err].[RejectedFileRow]",
                            "Type Partner Measures", "Data Conversion Error Output")
    return build_package(
        "STG_Load_PartnerSale",
        "Load the partner sales file landing into stg.PartnerSale. Every column arrives as text: "
        "dates are re-ordered from the partners' DD/MM/YYYY convention, thousands separators and "
        "currency symbols are stripped, and unresolvable countries or customer references are "
        "rejected to err.RejectedFileRow.",
        FILE, "stg.PartnerSale", [flow], watermark=True,
        connections=(CONN_STAGING, "WWI_Inbound_Files"),
        post_tasks=[exec_proc("Normalize Partner Customer Names",
                              "EXEC stg.usp_NormalizeCustomer @BatchId = ?, @PackageExecutionId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                                                  ("User::PackageExecutionId", 1, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedFileRow]", "stg.PartnerSale", "r.BusinessKey",
                                 "RejectReasonCode", stage="Extract"))


@package
def stg_load_employee():
    """raw.SqlOrder -> stg.Employee + stg.Salesperson (people are only visible via orders)."""
    emp_cols = [
        int_col("SalespersonPersonID"), str_col("FullName", 100), str_col("PreferredName", 50),
        str_col("EmailAddress", 256), str_col("PhoneNumber", 20), str_col("RegionCode", 4),
        date_col("ValidFrom"), date_col("ValidTo"),
    ]
    emp = DataFlow("DFT Conform Employee", "Name splitting and regional contact masking")
    emp.oledb_source(
        "RAW SQL Order People", CONN_STAGING,
        "SELECT DISTINCT o.SalespersonPersonID, p.FullName, p.PreferredName, p.EmailAddress,\n"
        "       p.PhoneNumber, p.RegionCode, p.ValidFrom, p.ValidTo\n"
        "FROM raw.SqlOrder AS o\n"
        "     INNER JOIN raw.SqlPerson AS p ON p.PersonID = o.SalespersonPersonID;",
        emp_cols, timeout=1800)
    emp.row_count("Count Employee Rows Read", "User::RowsRead")
    emp.derived_column("Split Employee Names", [
        ("EmployeeId", 'SalespersonPersonID', int_col("EmployeeId")),
        ("FullName", 'TRIM(FullName)', str_col("FullName", 100)),
        ("GivenName",
         'FINDSTRING(TRIM(FullName), " ", 1) > 0 ? LEFT(TRIM(FullName), FINDSTRING(TRIM(FullName), " ", 1) - 1) '
         ': TRIM(FullName)', str_col("GivenName", 50)),
        ("FamilyName",
         'FINDSTRING(TRIM(FullName), " ", 1) > 0 ? '
         'SUBSTRING(TRIM(FullName), FINDSTRING(TRIM(FullName), " ", 1) + 1, 50) : ""',
         str_col("FamilyName", 50)),
        ("PreferredName", 'ISNULL(PreferredName) ? TRIM(FullName) : TRIM(PreferredName)',
         str_col("PreferredName", 50)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode))', str_col("RegionCode", 4)),
        # EU staff contact details are masked in staging; other regions keep the corporate address.
        ("EmailAddress",
         'UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode)) == "EU" ? "masked@example.invalid" '
         ': LOWER(TRIM(ISNULL(EmailAddress) ? "" : EmailAddress))', str_col("EmailAddress", 256)),
        ("PhoneNumberLast4",
         'ISNULL(PhoneNumber) ? "0000" : RIGHT(REPLACE(REPLACE(TRIM(PhoneNumber), "-", ""), " ", ""), 4)',
         str_col("PhoneNumberLast4", 4)),
        ("IsCurrentFlag", 'ISNULL(ValidTo) || ValidTo > GETDATE() ? "Y" : "N"', str_col("IsCurrentFlag", 1)),
    ])
    emp.sort("Sort Employees For Survivorship", ["EmployeeId", "IsCurrentFlag"],
             eliminate_duplicates=True)
    emp.row_count("Count Employee Rows Loaded", "User::RowsInserted")
    emp.oledb_destination("STG Employee", CONN_STAGING, "[stg].[Employee]", batch_size=10000)
    emp.reject_destination("ERR Employee Source Errors", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                           "RAW SQL Order People", "OLE DB Source Error Output")

    sp_cols = [
        int_col("SalespersonPersonID"), str_col("SalesTerritoryCode", 10), money_col("QuotaAmount"),
        str_col("QuotaCurrencyCode", 3), int_col("QuotaYear"),
    ]
    sp = DataFlow("DFT Conform Salesperson", "Quota conversion and territory attribution")
    sp.oledb_source(
        "RAW SQL Salesperson Quota", CONN_STAGING,
        "SELECT DISTINCT o.SalespersonPersonID, q.SalesTerritoryCode, q.QuotaAmount,\n"
        "       q.QuotaCurrencyCode, q.QuotaYear\n"
        "FROM raw.SqlOrder AS o\n"
        "     INNER JOIN raw.SqlSalespersonQuota AS q ON q.SalespersonPersonID = o.SalespersonPersonID;",
        sp_cols, timeout=1800)
    sp.derived_column("Prepare Quota Conversion", [
        ("EmployeeId", 'SalespersonPersonID', int_col("EmployeeId")),
        ("SalesTerritoryCode", 'UPPER(TRIM(ISNULL(SalesTerritoryCode) ? "UNASSIGNED" : SalesTerritoryCode))',
         str_col("SalesTerritoryCode", 12)),
        ("QuotaCurrencyCode", 'UPPER(TRIM(ISNULL(QuotaCurrencyCode) ? "USD" : QuotaCurrencyCode))',
         str_col("QuotaCurrencyCode", 3)),
        ("QuotaAmount", 'ISNULL(QuotaAmount) ? (DT_NUMERIC,18,2)0 : QuotaAmount', money_col("QuotaAmount")),
    ])
    sp.lookup(
        "Lookup Quota FX (Partial Cache)", CONN_STAGING,
        "SELECT FromCurrencyCode AS QuotaCurrencyCode, AVG(ConversionRate) AS AverageRate\n"
        "FROM stg.FxRate WHERE ToCurrencyCode = N'USD' GROUP BY FromCurrencyCode;",
        ["QuotaCurrencyCode"], [dec_col("AverageRate")], no_match="IG")
    sp.derived_column("Convert Quota", [
        ("QuotaAmountUsd",
         '(DT_NUMERIC,18,2)(QuotaAmount * (ISNULL(AverageRate) ? (DT_NUMERIC,18,6)1 : AverageRate))',
         money_col("QuotaAmountUsd")),
    ])
    sp.row_count("Count Salesperson Rows", "User::RowsUpdated")
    sp.oledb_destination("STG Salesperson", CONN_STAGING, "[stg].[Salesperson]", batch_size=10000)
    return build_package(
        "STG_Load_Employee",
        "Truncate and reload stg.Employee and stg.Salesperson from the people referenced by OLTP "
        "orders. Names are split into given and family parts, EU contact details are masked in "
        "staging, and quotas are converted to USD at the average rate when a daily rate is absent.",
        OLTP, "stg.Employee", [emp, sp],
        truncate_tables=["[stg].[Employee]", "[stg].[Salesperson]"],
        post_tasks=[exec_proc("Normalize Employee Names",
                              "EXEC stg.usp_NormalizeCustomer @BatchId = ?, @PackageExecutionId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                                                  ("User::PackageExecutionId", 1, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedConstraintViolation]", "stg.Employee",
                                 "r.BusinessKey", "RejectReasonCode"))


@package
def stg_load_promotion_and_territory():
    """raw.SqlOrder -> stg.Promotion + stg.SalesTerritory."""
    promo_cols = [
        str_col("PromotionCode", 20), str_col("PromotionName", 100), str_col("PromotionTypeCode", 10),
        dec_col("DiscountPercent", 9, 4), money_col("DiscountAmount"), str_col("RegionCode", 4),
        date_col("ValidFrom"), date_col("ValidTo"),
    ]
    promo = DataFlow("DFT Conform Promotion", "Promotion typing and overlapping-window survivorship")
    promo.oledb_source(
        "RAW SQL Order Promotions", CONN_STAGING,
        "SELECT DISTINCT p.PromotionCode, p.PromotionName, p.PromotionTypeCode, p.DiscountPercent,\n"
        "       p.DiscountAmount, p.RegionCode, p.ValidFrom, p.ValidTo\n"
        "FROM raw.SqlOrder AS o\n"
        "     INNER JOIN raw.SqlPromotion AS p ON p.PromotionCode = o.PromotionCode;",
        promo_cols, timeout=1800)
    promo.row_count("Count Promotion Rows Read", "User::RowsRead")
    promo.derived_column("Type Promotions", [
        ("PromotionCode", 'UPPER(TRIM(PromotionCode))', str_col("PromotionCode", 20)),
        ("PromotionName", 'TRIM(PromotionName)', str_col("PromotionName", 100)),
        ("PromotionTypeCode",
         'ISNULL(DiscountPercent) || DiscountPercent == 0 ? "AMOUNT" : "PERCENT"',
         str_col("PromotionTypeCode", 10)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode))', str_col("RegionCode", 4)),
        ("DiscountPercent", 'ISNULL(DiscountPercent) ? (DT_NUMERIC,9,4)0 : DiscountPercent',
         dec_col("DiscountPercent", 9, 4)),
        ("DiscountAmount", 'ISNULL(DiscountAmount) ? (DT_NUMERIC,18,2)0 : DiscountAmount',
         money_col("DiscountAmount")),
        ("ValidToDate", 'ISNULL(ValidTo) ? (DT_DBTIMESTAMP)"9999-12-31" : ValidTo', date_col("ValidToDate")),
    ])
    promo.sort("Sort Promotions By Validity", ["PromotionCode", "ValidToDate"],
               eliminate_duplicates=True)
    promo.conditional_split("Screen Promotion", [
        ("Valid Promotion", 'DiscountPercent <= 90 && DiscountPercent >= 0'),
    ], default_output="Implausible Discount")
    promo.row_count("Count Promotion Rows Loaded", "User::RowsInserted")
    promo.oledb_destination("STG Promotion", CONN_STAGING, "[stg].[Promotion]", batch_size=10000)
    promo.branch_destination("ERR Promotion Discount Outlier", CONN_STAGING,
                             "[err].[RejectedConstraintViolation]",
                             "Screen Promotion", "Implausible Discount")

    terr_cols = [
        str_col("SalesTerritoryCode", 12), str_col("SalesTerritoryName", 60), str_col("RegionCode", 4),
        str_col("CountryCode", 3), str_col("ParentTerritoryCode", 12),
    ]
    terr = DataFlow("DFT Conform Sales Territory", "Territory hierarchy conformance")
    terr.oledb_source(
        "RAW SQL Order Territories", CONN_STAGING,
        "SELECT DISTINCT t.SalesTerritoryCode, t.SalesTerritoryName, t.RegionCode, t.CountryCode,\n"
        "       t.ParentTerritoryCode\n"
        "FROM raw.SqlOrder AS o\n"
        "     INNER JOIN raw.SqlSalesTerritory AS t ON t.SalesTerritoryCode = o.SalesTerritoryCode;",
        terr_cols, timeout=1800)
    terr.derived_column("Standardize Territory", [
        ("SalesTerritoryCode", 'UPPER(TRIM(SalesTerritoryCode))', str_col("SalesTerritoryCode", 12)),
        ("SalesTerritoryName", 'TRIM(SalesTerritoryName)', str_col("SalesTerritoryName", 60)),
        ("RegionCode", 'UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode))', str_col("RegionCode", 4)),
        ("CountryCode", 'UPPER(TRIM(ISNULL(CountryCode) ? "USA" : CountryCode))', str_col("CountryCode", 3)),
        ("ParentTerritoryCode",
         'ISNULL(ParentTerritoryCode) ? UPPER(TRIM(ISNULL(RegionCode) ? "NA" : RegionCode)) '
         ': UPPER(TRIM(ParentTerritoryCode))', str_col("ParentTerritoryCode", 12)),
    ])
    terr.lookup(
        "Lookup Territory Country (Full Cache)", CONN_STAGING,
        "SELECT CountryCode, CountryName FROM ref.Country;",
        ["CountryCode"], [str_col("CountryName", 60)], no_match="RD")
    terr.row_count("Count Territory Rows", "User::RowsUpdated")
    terr.oledb_destination("STG SalesTerritory", CONN_STAGING, "[stg].[SalesTerritory]", batch_size=10000)
    terr.reject_destination("ERR Territory Unknown Country", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Territory Country (Full Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Load_PromotionAndTerritory",
        "Truncate and reload stg.Promotion and stg.SalesTerritory from the promotion and territory "
        "codes referenced by OLTP orders. Overlapping promotion windows are resolved by keeping the "
        "latest ValidFrom, and territories inherit the region as parent when the hierarchy is broken.",
        OLTP, "stg.Promotion", [promo, terr],
        truncate_tables=["[stg].[Promotion]", "[stg].[SalesTerritory]"],
        post_tasks=[exec_proc("Translate Promotion Source Codes",
                              "EXEC stg.usp_TranslateSourceCodes @BatchId = ?, @ObjectName = N'stg.Promotion';",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedConstraintViolation]", "stg.Promotion",
                                 "r.BusinessKey", "RejectReasonCode"))


# ---------------------------------------------------------------------------
# work.* scratch rebuilds
# ---------------------------------------------------------------------------


@package
def stg_work_product_crosswalk():
    """stg.Product + stg.StockItem -> work.ProductCrosswalk (union of both feeds)."""
    ora_cols = [
        str_col("ProductCode", 30), str_col("ProductName", 100), str_col("Gtin", 14),
        int_col("ProductKey"),
    ]
    flow = DataFlow("DFT Rebuild Product Crosswalk",
                    "Union the Oracle product master and OLTP stock items into one crosswalk")
    flow.oledb_source(
        "STG Product (Oracle)", CONN_STAGING,
        "SELECT ProductCode, ProductName, Gtin, ProductKey FROM stg.Product;",
        ora_cols, timeout=1800)
    flow.row_count("Count Oracle Product Rows", "User::RowsRead")
    flow.derived_column("Shape Oracle Side", [
        ("SourceSystemCode", '"ORA_ERP"', str_col("SourceSystemCode", 10)),
        ("SourceItemCode", 'UPPER(TRIM(ProductCode))', str_col("SourceItemCode", 30)),
        ("MatchKey",
         'ISNULL(Gtin) || TRIM(Gtin) == "" ? UPPER(REPLACE(TRIM(ProductName), " ", "")) : TRIM(Gtin)',
         str_col("MatchKey", 100)),
        ("MatchRuleCode", 'ISNULL(Gtin) || TRIM(Gtin) == "" ? "NAME" : "GTIN"', str_col("MatchRuleCode", 4)),
        ("StockItemId", '-1', int_col("StockItemId")),
    ])
    flow.union_all("Union Product Feeds")
    flow.sort("Sort Crosswalk Candidates", ["MatchKey", "MatchRuleCode", "SourceSystemCode"])
    flow.conditional_split("Survivorship Rule", [
        ("Preferred Match", 'MatchRuleCode == "GTIN"'),
        ("Name Match", 'MatchRuleCode == "NAME" && LEN(MatchKey) >= 8'),
    ], default_output="Unmatchable")
    flow.row_count("Count Crosswalk Rows Written", "User::RowsInserted")
    flow.oledb_destination("WORK ProductCrosswalk", CONN_STAGING, "[work].[ProductCrosswalk]",
                           batch_size=50000)
    flow.branch_destination("WORK ProductCrosswalk Name Matches", CONN_STAGING,
                            "[work].[ProductCrosswalk]", "Survivorship Rule", "Name Match")
    flow.branch_destination("ERR Crosswalk Unmatchable", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Survivorship Rule", "Unmatchable")

    oltp_cols = [
        int_col("StockItemId"), str_col("StockItemName", 100), str_col("Barcode", 20),
        str_col("BrandName", 50),
    ]
    second = DataFlow("DFT Stock Item Crosswalk Feed", "OLTP side of the crosswalk union")
    second.oledb_source(
        "STG StockItem (OLTP)", CONN_STAGING,
        "SELECT StockItemId, StockItemName, Barcode, BrandName FROM stg.StockItem;",
        oltp_cols, timeout=1800)
    second.derived_column("Shape OLTP Side", [
        ("SourceSystemCode", '"WWI_OLTP"', str_col("SourceSystemCode", 10)),
        ("SourceItemCode", '(DT_WSTR,30)StockItemId', str_col("SourceItemCode", 30)),
        ("MatchKey",
         'ISNULL(Barcode) || TRIM(Barcode) == "" ? UPPER(REPLACE(TRIM(StockItemName), " ", "")) : TRIM(Barcode)',
         str_col("MatchKey", 100)),
        ("MatchRuleCode", 'ISNULL(Barcode) || TRIM(Barcode) == "" ? "NAME" : "GTIN"', str_col("MatchRuleCode", 4)),
    ])
    second.row_count("Count OLTP Crosswalk Rows", "User::RowsUpdated")
    second.oledb_destination("WORK ProductCrosswalk Staging Feed", CONN_STAGING,
                             "[work].[ProductCrosswalkFeed]", batch_size=50000)
    return build_package(
        "STG_Work_ProductCrosswalk",
        "Rebuild work.ProductCrosswalk by unioning the Oracle product master with OLTP stock items. "
        "GTIN/barcode matches win over normalised-name matches; candidates that satisfy neither "
        "rule are written to err.RejectedLookupFailure for the stewardship queue.",
        ORA, "work.ProductCrosswalk", [second, flow],
        truncate_tables=["[work].[ProductCrosswalk]", "[work].[ProductCrosswalkFeed]"],
        post_tasks=[exec_proc("Build Product Crosswalk",
                              "EXEC work.usp_BuildProductCrosswalk @BatchId = ?, @OnlyMissing = 0;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedLookupFailure]", "work.ProductCrosswalk",
                                 "r.BusinessKey", "RejectReasonCode", stage="Transform"))


@package
def stg_work_inventory_position():
    """stg.StockMovement -> work.InventoryPositionDaily (aggregate and rejoin)."""
    cols = [
        int_col("StockItemId"), int_col("WarehouseSiteId"), date_col("MovementDate"),
        dec_col("SignedQuantity", 18, 3), str_col("CounterpartyTypeCode", 10),
    ]
    flow = DataFlow("DFT Rebuild Inventory Position",
                    "Aggregate movements to a daily position and rejoin the item attributes")
    flow.oledb_source(
        "STG Stock Movement", CONN_STAGING,
        "SELECT StockItemId, WarehouseSiteId, MovementDate, SignedQuantity, CounterpartyTypeCode\n"
        "FROM stg.StockMovement\n"
        "WHERE MovementDate >= DATEADD(day, -90, CONVERT(date, GETDATE()));",
        cols, timeout=7200)
    flow.row_count("Count Movement Rows Read", "User::RowsRead")
    flow.aggregate("Aggregate Daily Position", ["StockItemId", "WarehouseSiteId", "MovementDate"], [
        ("SignedQuantity", "NetQuantity", "Sum"),
        ("SignedQuantity", "MaxSingleMovement", "Maximum"),
        ("SignedQuantity", "MinSingleMovement", "Minimum"),
        ("StockItemId", "MovementCount", "Count"),
    ])
    flow.sort("Sort Position By Item And Date", ["StockItemId", "WarehouseSiteId", "MovementDate"])
    flow.lookup(
        "Lookup Stock Item Attributes (Full Cache)", CONN_STAGING,
        "SELECT StockItemId, StockItemName, BrandName, PriceBandCode FROM stg.StockItem;",
        ["StockItemId"],
        [str_col("StockItemName", 100), str_col("BrandName", 50), str_col("PriceBandCode", 3)],
        no_match="RD")
    flow.lookup(
        "Lookup Warehouse Site (Full Cache)", CONN_STAGING,
        "SELECT WarehouseSiteId, WarehouseSiteCode, RegionCode FROM ref.WarehouseSite;",
        ["WarehouseSiteId"], [str_col("WarehouseSiteCode", 10), str_col("RegionCode", 4)], no_match="RD")
    flow.derived_column("Classify Position", [
        ("StockPositionCode",
         'NetQuantity < 0 ? "NEGATIVE" : (NetQuantity == 0 ? "ZERO" : "POSITIVE")',
         str_col("StockPositionCode", 8)),
        ("HighChurnFlag", 'MovementCount > 50 ? "Y" : "N"', str_col("HighChurnFlag", 1)),
    ])
    flow.conditional_split("Screen Position", [
        ("Plausible Position", 'NetQuantity > -1000000 && NetQuantity < 1000000'),
    ], default_output="Outlier Position")
    flow.row_count("Count Position Rows Written", "User::RowsInserted")
    flow.oledb_destination("WORK InventoryPositionDaily", CONN_STAGING,
                           "[work].[InventoryPositionDaily]", batch_size=100000)
    flow.branch_destination("ERR Position Outlier", CONN_STAGING, "[err].[RejectedConstraintViolation]",
                            "Screen Position", "Outlier Position")
    flow.reject_destination("ERR Position Unknown Item", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Stock Item Attributes (Full Cache)", "Lookup No Match Output")
    flow.reject_destination("ERR Position Unknown Site", CONN_STAGING, "[err].[RejectedLookupFailure]",
                            "Lookup Warehouse Site (Full Cache)", "Lookup No Match Output")
    return build_package(
        "STG_Work_InventoryPosition",
        "Rebuild work.InventoryPositionDaily over a rolling ninety-day window: movements are summed "
        "per item, site and day, the item and warehouse attributes are rejoined by lookup, and "
        "implausible positions are held in err.RejectedConstraintViolation.",
        OLTP, "work.InventoryPositionDaily", [flow],
        truncate_tables=["[work].[InventoryPositionDaily]"],
        post_tasks=[exec_proc("Build Inventory Position Daily",
                              "EXEC work.usp_BuildInventoryPositionDaily @BatchId = ?, @WindowDays = 90;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedConstraintViolation]", "work.InventoryPositionDaily",
                                 "r.BusinessKey", "RejectReasonCode", stage="Transform"))


@package
def stg_work_payment_match():
    """stg.Payment + stg.ApInvoice -> work.PaymentMatched (merge-join style matching)."""
    cols = [
        str_col("PaymentNumber", 30), str_col("SupplierCode", 20), money_col("PaymentAmount"),
        str_col("PaymentCurrencyCode", 3), date_col("PaymentDate"), str_col("InvoiceNumber", 30),
        money_col("GrossAmount"), date_col("DueDate"),
    ]
    flow = DataFlow("DFT Match Payments To Invoices",
                    "Sorted merge of payments against open invoices with tolerance banding")
    flow.oledb_source(
        "STG Payment Joined To Invoice", CONN_STAGING,
        "SELECT p.PaymentNumber, p.SupplierCode, p.PaymentAmount, p.PaymentCurrencyCode, p.PaymentDate,\n"
        "       i.InvoiceNumber, i.GrossAmount, i.DueDate\n"
        "FROM stg.Payment AS p\n"
        "     LEFT OUTER JOIN stg.ApInvoice AS i\n"
        "       ON i.SupplierCode = p.SupplierCode\n"
        "      AND i.InvoiceCurrencyCode = p.PaymentCurrencyCode\n"
        "ORDER BY p.SupplierCode, p.PaymentDate;",
        cols, timeout=7200)
    flow.row_count("Count Payment Rows Read", "User::RowsRead")
    flow.sort("Sort By Supplier And Amount", ["SupplierCode", "PaymentAmount"])
    flow.derived_column("Score Payment Match", [
        ("AmountVariance",
         'ISNULL(GrossAmount) ? (DT_NUMERIC,18,2)0 : (DT_NUMERIC,18,2)ABS(PaymentAmount - GrossAmount)',
         money_col("AmountVariance")),
        ("MatchTypeCode",
         'ISNULL(InvoiceNumber) ? "UNMATCHED" : (ABS(PaymentAmount - GrossAmount) <= (DT_NUMERIC,18,2)0.01 '
         '? "EXACT" : (ABS(PaymentAmount - GrossAmount) <= GrossAmount * (DT_NUMERIC,18,2)0.02 '
         '? "TOLERANCE" : "VARIANCE"))', str_col("MatchTypeCode", 10)),
        ("DaysLate",
         'ISNULL(DueDate) ? 0 : DATEDIFF("day", DueDate, PaymentDate)', int_col("DaysLate")),
        ("LatePaymentFlag", 'ISNULL(DueDate) ? "N" : (PaymentDate > DueDate ? "Y" : "N")',
         str_col("LatePaymentFlag", 1)),
    ])
    flow.conditional_split("Route Match Outcome", [
        ("Matched", 'MatchTypeCode == "EXACT" || MatchTypeCode == "TOLERANCE"'),
        ("Variance", 'MatchTypeCode == "VARIANCE"'),
    ], default_output="Unmatched")
    flow.row_count("Count Matched Rows", "User::RowsInserted")
    flow.oledb_destination("WORK PaymentMatched", CONN_STAGING, "[work].[PaymentMatched]", batch_size=50000)
    flow.branch_destination("WORK PaymentMatched Variance", CONN_STAGING, "[work].[PaymentMatched]",
                            "Route Match Outcome", "Variance")
    flow.branch_destination("ERR Payment Unmatched", CONN_STAGING, "[err].[RejectedPayment]",
                            "Route Match Outcome", "Unmatched")
    return build_package(
        "STG_Work_PaymentMatch",
        "Rebuild work.PaymentMatched by matching staged payments to AP invoices on supplier and "
        "currency. Exact matches, two-percent tolerance matches and hard variances are banded "
        "separately; payments with no candidate invoice are rejected for the AP exception queue.",
        ORA, "work.PaymentMatched", [flow],
        truncate_tables=["[work].[PaymentMatched]"],
        post_tasks=[exec_proc("Match Payments To Invoices",
                              "EXEC work.usp_MatchPaymentsToInvoices @BatchId = ?, @TolerancePercent = 2;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedPayment]", "work.PaymentMatched", "r.PaymentNumber",
                                 "RejectReasonCode", stage="Transform"))


@package
def stg_work_customer_dedup():
    """stg.Customer + stg.CustomerAddress -> work.CustomerDedup + work.CustomerAddressStandardized."""
    cols = [
        str_col("CustomerCode", 20), str_col("CustomerName", 100), str_col("CountryCode", 3),
        str_col("RegionCode", 4), str_col("TaxRegistrationNumber", 30), date_col("SourceCreatedDate"),
        str_col("PostalCode", 16), str_col("AddressLine1", 100), str_col("CityName", 60),
    ]
    dedup = DataFlow("DFT Deduplicate Customer",
                     "Blocking key, sort survivorship and duplicate routing into work.CustomerDedup")
    dedup.oledb_source(
        "STG Customer With Address", CONN_STAGING,
        "SELECT c.CustomerCode, c.CustomerName, c.CountryCode, c.RegionCode, c.TaxRegistrationNumber,\n"
        "       c.SourceCreatedDate, a.PostalCode, a.AddressLine1, a.CityName\n"
        "FROM stg.Customer AS c\n"
        "     LEFT OUTER JOIN stg.CustomerAddress AS a\n"
        "       ON a.CustomerCode = c.CustomerCode AND a.AddressTypeCode = N'BILL';",
        cols, timeout=7200)
    dedup.row_count("Count Customer Rows Read", "User::RowsRead")
    dedup.derived_column("Build Blocking Key", [
        ("NormalizedName",
         'UPPER(REPLACE(REPLACE(REPLACE(TRIM(CustomerName), " ", ""), ".", ""), ",", ""))',
         str_col("NormalizedName", 100)),
        ("BlockingKey",
         'LEFT(UPPER(REPLACE(TRIM(CustomerName), " ", "")), 8) + "|" + UPPER(TRIM(CountryCode)) + "|" + '
         'LEFT(UPPER(REPLACE(TRIM(ISNULL(PostalCode) ? "00000" : PostalCode), " ", "")), 5)',
         str_col("BlockingKey", 40)),
        ("TaxKey",
         'ISNULL(TaxRegistrationNumber) || TRIM(TaxRegistrationNumber) == "" ? "NONE" '
         ': UPPER(REPLACE(TRIM(TaxRegistrationNumber), "-", ""))', str_col("TaxKey", 30)),
    ])
    dedup.sort("Sort By Blocking Key And Age", ["BlockingKey", "SourceCreatedDate", "CustomerCode"])
    dedup.aggregate("Count Members Per Block", ["BlockingKey"], [
        ("CustomerCode", "BlockMemberCount", "Count"),
        ("CustomerCode", "SurvivingCustomerCode", "Minimum"),
    ])
    dedup.conditional_split("Apply Survivorship", [
        ("Survivor", 'BlockMemberCount == 1'),
        ("Duplicate Group", 'BlockMemberCount > 1'),
    ], default_output="Unresolved Block")
    dedup.row_count("Count Dedup Rows Written", "User::RowsInserted")
    dedup.oledb_destination("WORK CustomerDedup", CONN_STAGING, "[work].[CustomerDedup]", batch_size=50000)
    dedup.branch_destination("WORK CustomerDedup Duplicates", CONN_STAGING, "[work].[CustomerDedup]",
                             "Apply Survivorship", "Duplicate Group")
    dedup.branch_destination("ERR Dedup Unresolved", CONN_STAGING, "[err].[RejectedCustomer]",
                             "Apply Survivorship", "Unresolved Block")

    addr_cols = [
        str_col("CustomerCode", 20), str_col("AddressTypeCode", 4), str_col("AddressLine1", 100),
        str_col("AddressLine2", 100), str_col("CityName", 60), str_col("StateProvinceCode", 6),
        str_col("PostalCode", 16), str_col("CountryCode", 3), str_col("RegionCode", 4),
    ]
    addr = DataFlow("DFT Standardize Customer Address",
                    "Region-specific address standardisation into work.CustomerAddressStandardized")
    addr.oledb_source(
        "STG Customer Address", CONN_STAGING,
        "SELECT CustomerCode, AddressTypeCode, AddressLine1, AddressLine2, CityName,\n"
        "       StateProvinceCode, PostalCode, CountryCode, RegionCode\n"
        "FROM stg.CustomerAddress;",
        addr_cols, timeout=3600)
    addr.derived_column("Standardize By Region", [
        ("AddressLine1Standardized",
         'UPPER(TRIM(RegionCode)) == "NA" ? UPPER(REPLACE(REPLACE(REPLACE(TRIM(AddressLine1), "Street", "ST"), '
         '"Avenue", "AVE"), "Suite", "STE")) : (UPPER(TRIM(RegionCode)) == "EU" ? TRIM(AddressLine1) '
         ': UPPER(TRIM(AddressLine1)))', str_col("AddressLine1Standardized", 100)),
        ("PostalCodeStandardized",
         'UPPER(TRIM(RegionCode)) == "NA" ? LEFT(REPLACE(TRIM(PostalCode), " ", ""), 5) '
         ': (UPPER(TRIM(RegionCode)) == "EU" ? UPPER(TRIM(CountryCode)) + "-" + REPLACE(TRIM(PostalCode), " ", "") '
         ': REPLACE(REPLACE(TRIM(PostalCode), "-", ""), " ", ""))', str_col("PostalCodeStandardized", 16)),
        ("CityNameStandardized", 'UPPER(TRIM(CityName))', str_col("CityNameStandardized", 60)),
        ("AddressQualityCode",
         'ISNULL(AddressLine1) || TRIM(AddressLine1) == "" ? "MISSING" '
         ': (ISNULL(PostalCode) || TRIM(PostalCode) == "" ? "NOPOST" : "OK")',
         str_col("AddressQualityCode", 8)),
    ])
    addr.conditional_split("Screen Address Quality", [
        ("Usable Address", 'AddressQualityCode == "OK"'),
        ("Missing Postal Code", 'AddressQualityCode == "NOPOST"'),
    ], default_output="Missing Street")
    addr.row_count("Count Address Rows Written", "User::RowsUpdated")
    addr.oledb_destination("WORK CustomerAddressStandardized", CONN_STAGING,
                           "[work].[CustomerAddressStandardized]", batch_size=50000)
    addr.branch_destination("ERR Address Missing Postal", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Screen Address Quality", "Missing Postal Code")
    addr.branch_destination("ERR Address Missing Street", CONN_STAGING, "[err].[RejectedCustomer]",
                            "Screen Address Quality", "Missing Street")
    return build_package(
        "STG_Work_CustomerDedup",
        "Rebuild work.CustomerDedup and work.CustomerAddressStandardized. Customers are blocked on "
        "name prefix, country and postal code; within a block the oldest source record survives. "
        "Addresses are standardised by the regional convention before the duplicate review.",
        OLTP, "work.CustomerDedup", [dedup, addr],
        truncate_tables=["[work].[CustomerDedup]", "[work].[CustomerAddressStandardized]"],
        post_tasks=[exec_proc("Deduplicate Customer",
                              "EXEC stg.usp_DeduplicateCustomer @BatchId = ?, @PackageExecutionId = ?;",
                              parameter_bindings=[("$Package::BatchId", 0, "LONG"),
                                                  ("User::PackageExecutionId", 1, "LONG")])],
        reject_task=reject_sweep("[err].[RejectedCustomer]", "work.CustomerDedup", "r.CustomerCode",
                                 "RejectReasonCode", stage="Transform"))


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
