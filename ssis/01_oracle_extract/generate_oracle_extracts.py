#!/usr/bin/env python3
"""Spec module for the Oracle ERP extract packages (ssis/01_oracle_extract).

Twenty-two packages land Oracle ERP master data, procurement, finance and
reference data into the ``raw`` schema of the staging database. Every package
is emitted from this module through tools/ssisgen so the SSIS XML stays
consistent and regenerable.

The extract patterns implemented here are the ones the estate actually runs:

* full truncate-and-load for small reference and hierarchy objects,
* timestamp-watermark incremental with a lookback window for late updates,
* numeric-key incremental driven by a max-id watermark,
* bounded date-window extracts that can be re-run for a given business window,
* source-side filtering that pushes predicates into the Oracle query,
* joined source queries that denormalise several ERP tables in one pass,
* reference refreshes on their own cadence,
* delete detection off WWI_AUDIT.CHANGE_LOG for the objects that record it.

Incremental source queries carry ``?`` placeholders bound, in order, to the
watermark variables populated by etl.usp_GetWatermark. Regional divergence
(sales tax vs VAT vs GST, fiscal calendars, FX, postal standardisation,
consent and retention) is expressed in the source SQL and in the derived
columns, not in a parameter value.

Run:  python3 ssis/01_oracle_extract/generate_oracle_extracts.py
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
    CONN_ORACLE,
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

PROJECT_NAME = "WWI_Extract_Oracle"
PROJECT_CONNECTIONS = ["WWI_Oracle_ERP", "WWI_Staging_DB"]

# Oracle ledgers are registered per region in etl.SourceSystem; a package that
# reads a single global ERP schema uses ORA_ERP, a package whose rows are
# ledger-scoped uses the regional code.
SRC_GLOBAL = "ORA_ERP"
SRC_NA = "ORA_ERP_NA"
SRC_EU = "ORA_ERP_EU"
SRC_AP = "ORA_ERP_AP"


def num_col(name, precision=18, scale=4):
    return Column(name, "numeric", precision=precision, scale=scale)


def init_variables(assignments):
    """Expression task that resets the audit counters at the top of a package."""
    return Expression("Init Batch Variables", assignments)


def audit_columns():
    """Audit trailer columns every raw table carries."""
    return [
        str_col("SourceSystemCode", 20),
        date_col("ExtractedAtUtc"),
        bigint_col("PackageExecutionId"),
    ]


def audit_derivations(source_system):
    return [
        ("SourceSystemCode", '"%s"' % source_system, str_col("SourceSystemCode", 20)),
        ("ExtractedAtUtc", "GETUTCDATE()", date_col("ExtractedAtUtc")),
        ("PackageExecutionId", "@[User::PackageExecutionId]", bigint_col("PackageExecutionId")),
    ]


# ---------------------------------------------------------------------------
# WWI_MDM - master data
# ---------------------------------------------------------------------------


def ext_ora_customer_master():
    """Timestamp incremental over the customer master, joined to classification
    and credit profile, with EU consent suppression and delete detection off
    the ERP change log."""
    pkg = new_package(
        "EXT_ORA_CustomerMaster",
        "Incremental customer master extract (LAST_UPDATE_DT watermark with lookback), "
        "denormalised over CUST_MASTER, CUST_CLASSIFICATION and CUST_CREDIT_PROFILE, "
        "plus a delete-detection pass over WWI_AUDIT.CHANGE_LOG.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("RowsDeleted", 0, "int"), ("LookbackMinutes", 120, "int")],
    )

    cols = [
        Column("CUST_ID", "numeric", precision=12, scale=0),
        str_col("CUST_NBR", 20),
        str_col("CUST_NAME", 200),
        str_col("CUST_NAME_NORM", 200),
        str_col("LEGAL_ENTITY_CD", 10),
        str_col("REGION_CD", 8),
        str_col("COUNTRY_CD", 3),
        str_col("CUST_STATUS_CD", 4),
        str_col("CLASSIFICATION_CD", 10),
        str_col("BUYING_GROUP_CD", 10),
        str_col("PRICE_LIST_CD", 10),
        num_col("CREDIT_LIMIT_AMT", 18, 2),
        str_col("CREDIT_RATING_CD", 4),
        str_col("PAYMENT_TERMS_CD", 10),
        str_col("CURRENCY_CD", 3),
        str_col("TAX_REGISTRATION_NBR", 30),
        str_col("MARKETING_CONSENT_FLG", 1),
        date_col("CONSENT_CAPTURED_DT"),
        date_col("FIRST_ORDER_DT"),
        date_col("LAST_UPDATE_DT"),
        str_col("LAST_UPDATE_USER", 30),
    ]

    # CUST_STATUS_CD is a cryptic four-character legacy code (ACTV/HOLD/DORM/CLSD).
    # EU rows have marketing consent nulled when the consent record has lapsed
    # past the retention window; NA and APAC keep the captured value.
    sql = """SELECT  c.CUST_ID,
        c.CUST_NBR,
        c.CUST_NAME,
        WWI_MDM.FN_NORMALIZE_NAME(c.CUST_NAME)          AS CUST_NAME_NORM,
        c.LEGAL_ENTITY_CD,
        c.REGION_CD,
        c.COUNTRY_CD,
        WWI_MDM.FN_CUSTOMER_STATUS(c.CUST_ID)           AS CUST_STATUS_CD,
        cl.CLASSIFICATION_CD,
        cl.BUYING_GROUP_CD,
        cl.PRICE_LIST_CD,
        cp.CREDIT_LIMIT_AMT,
        cp.CREDIT_RATING_CD,
        cp.PAYMENT_TERMS_CD,
        c.CURRENCY_CD,
        c.TAX_REGISTRATION_NBR,
        CASE
            WHEN c.REGION_CD = 'EU'
                 AND c.CONSENT_CAPTURED_DT < ADD_MONTHS(SYSDATE, -24) THEN NULL
            ELSE c.MARKETING_CONSENT_FLG
        END                                             AS MARKETING_CONSENT_FLG,
        c.CONSENT_CAPTURED_DT,
        c.FIRST_ORDER_DT,
        c.LAST_UPDATE_DT,
        c.LAST_UPDATE_USER
FROM    WWI_MDM.CUST_MASTER c
        LEFT OUTER JOIN WWI_MDM.CUST_CLASSIFICATION cl
            ON cl.CUST_ID = c.CUST_ID
           AND cl.EFFECTIVE_TO_DT IS NULL
        LEFT OUTER JOIN WWI_MDM.CUST_CREDIT_PROFILE cp
            ON cp.CUST_ID = c.CUST_ID
           AND cp.PROFILE_STATUS_CD = 'CURR'
WHERE   c.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   c.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   c.MERGE_TARGET_CUST_ID IS NULL
ORDER BY c.CUST_ID"""

    df = DataFlow("Extract Customer Master", "Incremental customer master with credit and classification.")
    df.oledb_source("ORA CUST_MASTER", CONN_ORACLE, sql, cols, timeout=3600)
    df.derived_column(
        "Add Audit Columns",
        audit_derivations(SRC_GLOBAL) + [("DeleteFlag", '"N"', str_col("DeleteFlag", 1))],
    )
    df.row_count("Count Rows Read", "User::RowsRead")
    df.conditional_split(
        "Route Unusable Customers",
        [("Valid", "!ISNULL(CUST_NBR) && TRIM(CUST_NBR) != \"\" && !ISNULL(CUST_NAME)")],
        default_output="Unusable",
    )
    df.oledb_destination(
        "raw OracleCustomerMaster",
        CONN_STAGING,
        "raw.OracleCustomerMaster",
        batch_size=50000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedCustomer",
        CONN_STAGING,
        "err.RejectedCustomer",
        from_component="Route Unusable Customers",
        from_output="Unusable",
    )
    df.reject_destination(
        "err RejectedCustomer Conversion",
        CONN_STAGING,
        "err.RejectedCustomer",
        from_component="ORA CUST_MASTER",
    )

    # Deletes are never physical in the ERP UI but the merge/purge job does
    # remove rows, so the change log is the only source of truth for them.
    delete_cols = [
        Column("CUST_ID", "numeric", precision=12, scale=0),
        str_col("CUST_NBR", 20),
        date_col("LAST_UPDATE_DT"),
        str_col("LAST_UPDATE_USER", 30),
    ]
    delete_sql = """SELECT  TO_NUMBER(cg.PRIMARY_KEY_VALUE)  AS CUST_ID,
        cg.SECONDARY_KEY_VALUE           AS CUST_NBR,
        cg.CHANGE_DT                     AS LAST_UPDATE_DT,
        cg.CHANGED_BY                    AS LAST_UPDATE_USER
FROM    WWI_AUDIT.CHANGE_LOG cg
WHERE   cg.TABLE_NAME = 'CUST_MASTER'
  AND   cg.OPERATION_CD = 'D'
  AND   cg.CHANGE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   cg.CHANGE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')"""

    deletes = DataFlow("Detect Deleted Customers", "Delete detection from the ERP change log.")
    deletes.oledb_source("ORA CHANGE_LOG Customer Deletes", CONN_ORACLE, delete_sql, delete_cols, timeout=900)
    deletes.derived_column(
        "Flag Deleted Rows",
        audit_derivations(SRC_GLOBAL) + [("DeleteFlag", '"Y"', str_col("DeleteFlag", 1))],
    )
    deletes.row_count("Count Deleted Rows", "User::RowsDeleted")
    deletes.oledb_destination(
        "raw OracleCustomerMaster Deletes",
        CONN_STAGING,
        "raw.OracleCustomerMaster",
        batch_size=5000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_MDM.CUST_MASTER"))
    extract = pkg.add(DataFlowTask(df))
    delete_pass = pkg.add(DataFlowTask(deletes))
    count_inserted = pkg.add(
        ExecuteSql(
            "Capture Insert Count",
            CONN_STAGING,
            "SELECT COUNT_BIG(*) AS RowsInserted FROM raw.OracleCustomerMaster "
            "WHERE PackageExecutionId = ?;",
            result_type="ResultSetType_SingleRow",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
            result_bindings=[("0", "User::RowsInserted")],
        )
    )
    setwm = pkg.add(set_watermark("WWI_MDM.CUST_MASTER"))
    rows = pkg.add(log_row_count("raw.OracleCustomerMaster"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, delete_pass, count_inserted, setwm, rows, done)
    return pkg


def ext_ora_customer_address():
    """Timestamp incremental over current customer addresses with per-region
    postal standardisation and a geography lookup."""
    pkg = new_package(
        "EXT_ORA_CustomerAddress",
        "Incremental customer address extract from V_CUSTOMER_ADDRESS_CURRENT with "
        "region-specific postal standardisation (ZIP+4, UK/DE postcode casing, APAC "
        "prefecture handling) and a geography lookup against raw.OracleGeography.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("UnmatchedGeographyCount", 0, "int")],
    )

    cols = [
        Column("ADDRESS_ID", "numeric", precision=12, scale=0),
        Column("CUST_ID", "numeric", precision=12, scale=0),
        str_col("ADDRESS_TYPE_CD", 4),
        str_col("ADDRESS_LINE_1", 120),
        str_col("ADDRESS_LINE_2", 120),
        str_col("CITY_NAME", 80),
        str_col("STATE_PROVINCE_CD", 8),
        str_col("POSTAL_CD", 16),
        str_col("COUNTRY_CD", 3),
        str_col("REGION_CD", 8),
        str_col("VALIDATION_STATUS_CD", 4),
        date_col("EFFECTIVE_FROM_DT"),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  a.ADDRESS_ID,
        a.CUST_ID,
        a.ADDRESS_TYPE_CD,
        a.ADDRESS_LINE_1,
        a.ADDRESS_LINE_2,
        a.CITY_NAME,
        a.STATE_PROVINCE_CD,
        CASE a.REGION_CD
            WHEN 'NA'   THEN REGEXP_REPLACE(a.POSTAL_CD, '^([0-9]{5})([0-9]{4})$', '\\1-\\2')
            WHEN 'EU'   THEN UPPER(REPLACE(a.POSTAL_CD, ' ', ''))
            WHEN 'APAC' THEN TRIM(a.POSTAL_CD)
            ELSE a.POSTAL_CD
        END                                     AS POSTAL_CD,
        a.COUNTRY_CD,
        a.REGION_CD,
        a.VALIDATION_STATUS_CD,
        a.EFFECTIVE_FROM_DT,
        a.LAST_UPDATE_DT
FROM    WWI_MDM.V_CUSTOMER_ADDRESS_CURRENT a
WHERE   a.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   a.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   a.ADDRESS_TYPE_CD IN ('BILL', 'SHIP', 'STMT')"""

    df = DataFlow("Extract Customer Addresses")
    df.oledb_source("ORA V_CUSTOMER_ADDRESS_CURRENT", CONN_ORACLE, sql, cols, timeout=1800)
    df.derived_column(
        "Standardise Address",
        [
            ("AddressLine1Std", "UPPER(TRIM(ADDRESS_LINE_1))", str_col("AddressLine1Std", 120)),
            ("CityNameStd", "UPPER(TRIM(CITY_NAME))", str_col("CityNameStd", 80)),
            (
                "PostalCdStd",
                'REGION_CD == "EU" ? UPPER(REPLACE(POSTAL_CD, " ", "")) : TRIM(POSTAL_CD)',
                str_col("PostalCdStd", 16),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.lookup(
        "Lookup Geography Key",
        CONN_STAGING,
        "SELECT CountryCode AS COUNTRY_CD, PostalCode AS PostalCdStd, GeographyKey "
        "FROM raw.OracleGeography WITH (NOLOCK);",
        ["COUNTRY_CD", "PostalCdStd"],
        [int_col("GeographyKey")],
        no_match="RD",
    )
    df.row_count("Count Matched Rows", "User::RowsRead")
    df.oledb_destination(
        "raw OracleCustomerAddress",
        CONN_STAGING,
        "raw.OracleCustomerAddress",
        batch_size=20000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedLookupFailure",
        CONN_STAGING,
        "err.RejectedLookupFailure",
        from_component="Lookup Geography Key",
        from_output="Lookup No Match Output",
    )

    init = pkg.add(init_variables("@[User::UnmatchedGeographyCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_MDM.CUST_ADDRESS"))
    extract = pkg.add(DataFlowTask(df))
    rejects = pkg.add(
        exec_proc(
            "Log Unmatched Geography",
            "EXEC etl.usp_LogRejectedRecord @PackageExecutionId = ?, @BatchId = ?, "
            "@SourceSystemCode = ?, @ObjectName = N'WWI_MDM.CUST_ADDRESS', "
            "@RejectReasonCode = N'GEO_NOMATCH', "
            "@RejectReason = N'Postal code did not resolve to a geography key.', "
            "@RejectStage = N'Extract';",
            parameter_bindings=[
                ("User::PackageExecutionId", 0, "LONG"),
                ("$Package::BatchId", 1, "LONG"),
                ("$Package::SourceSystemCode", 2, "NVARCHAR"),
            ],
        )
    )
    setwm = pkg.add(set_watermark("WWI_MDM.CUST_ADDRESS"))
    rows = pkg.add(log_row_count("raw.OracleCustomerAddress"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract)
    pkg.link(extract, rejects, expression="@[User::RowsRejected] > 0")
    pkg.link(extract, setwm, expression="@[User::RowsRejected] == 0")
    pkg.link(rejects, setwm)
    pkg.chain(setwm, rows, done)
    return pkg


def ext_ora_supplier_master():
    """Timestamp incremental over supplier master joined to the masked bank view,
    with source-side filtering that drops long-dormant suppliers."""
    pkg = new_package(
        "EXT_ORA_SupplierMaster",
        "Incremental supplier master extract joined to V_SUPPLIER_BANK_MASKED and "
        "SUPP_CERTIFICATION, filtered source-side to suppliers transacted in the last "
        "seven years (retention rule) and to non-merged parties.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("DormantSupplierCount", 0, "int")],
    )

    cols = [
        Column("SUPP_ID", "numeric", precision=12, scale=0),
        str_col("SUPP_NBR", 20),
        str_col("SUPP_NAME", 200),
        str_col("SUPP_STATUS_CD", 4),
        str_col("SUPP_TYPE_CD", 6),
        str_col("REGION_CD", 8),
        str_col("COUNTRY_CD", 3),
        str_col("CURRENCY_CD", 3),
        str_col("PAYMENT_TERMS_CD", 10),
        str_col("PAYMENT_METHOD_CD", 6),
        str_col("BANK_ACCOUNT_MASKED", 24),
        str_col("BANK_COUNTRY_CD", 3),
        str_col("TAX_ID_MASKED", 20),
        str_col("WITHHOLDING_RULE_CD", 10),
        str_col("DIVERSITY_CLASSIFICATION_CD", 6),
        str_col("QUALITY_CERT_CD", 10),
        date_col("CERT_EXPIRY_DT"),
        date_col("LAST_TRANSACTION_DT"),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  s.SUPP_ID,
        s.SUPP_NBR,
        s.SUPP_NAME,
        s.SUPP_STATUS_CD,
        s.SUPP_TYPE_CD,
        s.REGION_CD,
        s.COUNTRY_CD,
        s.CURRENCY_CD,
        s.PAYMENT_TERMS_CD,
        s.PAYMENT_METHOD_CD,
        b.BANK_ACCOUNT_MASKED,
        b.BANK_COUNTRY_CD,
        s.TAX_ID_MASKED,
        s.WITHHOLDING_RULE_CD,
        s.DIVERSITY_CLASSIFICATION_CD,
        cert.QUALITY_CERT_CD,
        cert.CERT_EXPIRY_DT,
        s.LAST_TRANSACTION_DT,
        s.LAST_UPDATE_DT
FROM    WWI_MDM.SUPP_MASTER s
        LEFT OUTER JOIN WWI_MDM.V_SUPPLIER_BANK_MASKED b
            ON b.SUPP_ID = s.SUPP_ID
           AND b.PRIMARY_FLG = 'Y'
        LEFT OUTER JOIN (
            SELECT SUPP_ID,
                   MAX(QUALITY_CERT_CD) KEEP (DENSE_RANK LAST ORDER BY CERT_EXPIRY_DT) AS QUALITY_CERT_CD,
                   MAX(CERT_EXPIRY_DT)                                                 AS CERT_EXPIRY_DT
            FROM   WWI_MDM.SUPP_CERTIFICATION
            GROUP BY SUPP_ID
        ) cert ON cert.SUPP_ID = s.SUPP_ID
WHERE   s.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   s.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   (s.LAST_TRANSACTION_DT >= ADD_MONTHS(SYSDATE, -84) OR s.SUPP_STATUS_CD = 'ACTV')
  AND   s.MERGE_TARGET_SUPP_ID IS NULL"""

    df = DataFlow("Extract Supplier Master")
    df.oledb_source("ORA SUPP_MASTER", CONN_ORACLE, sql, cols, timeout=2400)
    df.derived_column(
        "Derive Supplier Attributes",
        [
            (
                "CertificationExpiredFlag",
                'ISNULL(CERT_EXPIRY_DT) ? "U" : (CERT_EXPIRY_DT < GETDATE() ? "Y" : "N")',
                str_col("CertificationExpiredFlag", 1),
            ),
            (
                "WithholdingApplies",
                'REGION_CD == "NA" && !ISNULL(WITHHOLDING_RULE_CD) ? "Y" : "N"',
                str_col("WithholdingApplies", 1),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Suppliers Read", "User::RowsRead")
    df.oledb_destination(
        "raw OracleSupplierMaster",
        CONN_STAGING,
        "raw.OracleSupplierMaster",
        batch_size=25000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedSupplier",
        CONN_STAGING,
        "err.RejectedSupplier",
        from_component="ORA SUPP_MASTER",
    )

    init = pkg.add(init_variables("@[User::DormantSupplierCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_MDM.SUPP_MASTER"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("WWI_MDM.SUPP_MASTER"))
    rows = pkg.add(log_row_count("raw.OracleSupplierMaster"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


def ext_ora_product_master():
    """Timestamp incremental over the product master with UOM conversion and a
    delete-detection pass; numeric ERP columns are converted before landing."""
    pkg = new_package(
        "EXT_ORA_ProductMaster",
        "Incremental product master extract joined to PRODUCT_CATEGORY and "
        "PRODUCT_UOM_CONV, converting Oracle NUMBER to the staging decimal scale and "
        "detecting obsoletions recorded in WWI_AUDIT.CHANGE_LOG.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("RowsDeleted", 0, "int")],
    )

    cols = [
        Column("PRODUCT_ID", "numeric", precision=12, scale=0),
        str_col("PRODUCT_CD", 30),
        str_col("PRODUCT_DESC", 200),
        str_col("CATEGORY_CD", 12),
        str_col("CATEGORY_DESC", 100),
        str_col("BRAND_CD", 10),
        str_col("PRODUCT_STATUS_CD", 4),
        str_col("BASE_UOM_CD", 4),
        str_col("SELL_UOM_CD", 4),
        Column("SELL_TO_BASE_FACTOR", "numeric", precision=18, scale=6),
        num_col("STANDARD_COST_AMT", 18, 4),
        num_col("LIST_PRICE_AMT", 18, 4),
        str_col("COST_CURRENCY_CD", 3),
        str_col("HAZMAT_CLASS_CD", 6),
        str_col("CHILLER_FLG", 1),
        Column("SHELF_LIFE_DAYS", "i4"),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  p.PRODUCT_ID,
        p.PRODUCT_CD,
        p.PRODUCT_DESC,
        c.CATEGORY_CD,
        c.CATEGORY_DESC,
        p.BRAND_CD,
        WWI_MDM.FN_PRODUCT_ACTIVE_FLAG(p.PRODUCT_ID)    AS PRODUCT_STATUS_CD,
        p.BASE_UOM_CD,
        u.SELL_UOM_CD,
        u.CONVERSION_FACTOR                             AS SELL_TO_BASE_FACTOR,
        p.STANDARD_COST_AMT,
        p.LIST_PRICE_AMT,
        p.COST_CURRENCY_CD,
        p.HAZMAT_CLASS_CD,
        p.CHILLER_FLG,
        p.SHELF_LIFE_DAYS,
        p.LAST_UPDATE_DT
FROM    WWI_MDM.PRODUCT_MASTER p
        INNER JOIN WWI_MDM.PRODUCT_CATEGORY c
            ON c.CATEGORY_CD = p.CATEGORY_CD
        LEFT OUTER JOIN WWI_MDM.PRODUCT_UOM_CONV u
            ON u.PRODUCT_ID = p.PRODUCT_ID
           AND u.DEFAULT_SELL_FLG = 'Y'
WHERE   p.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   p.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')"""

    df = DataFlow("Extract Product Master")
    df.oledb_source("ORA PRODUCT_MASTER", CONN_ORACLE, sql, cols, timeout=1800)
    df.data_conversion(
        "Convert ERP Numerics",
        [
            ("STANDARD_COST_AMT", "StandardCostAmount", money_col("StandardCostAmount")),
            ("LIST_PRICE_AMT", "ListPriceAmount", money_col("ListPriceAmount")),
            ("SELL_TO_BASE_FACTOR", "SellToBaseFactor", num_col("SellToBaseFactor", 18, 6)),
        ],
    )
    df.derived_column(
        "Derive Handling Flags",
        [
            (
                "HandlingClass",
                'CHILLER_FLG == "Y" ? "CHILL" : (ISNULL(HAZMAT_CLASS_CD) ? "AMB" : "HAZ")',
                str_col("HandlingClass", 5),
            ),
            ("DeleteFlag", '"N"', str_col("DeleteFlag", 1)),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Products Read", "User::RowsRead")
    df.oledb_destination(
        "raw OracleProductMaster",
        CONN_STAGING,
        "raw.OracleProductMaster",
        batch_size=40000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedProduct",
        CONN_STAGING,
        "err.RejectedProduct",
        from_component="ORA PRODUCT_MASTER",
    )

    delete_cols = [
        Column("PRODUCT_ID", "numeric", precision=12, scale=0),
        str_col("PRODUCT_CD", 30),
        date_col("LAST_UPDATE_DT"),
    ]
    delete_sql = """SELECT  TO_NUMBER(cg.PRIMARY_KEY_VALUE) AS PRODUCT_ID,
        cg.SECONDARY_KEY_VALUE          AS PRODUCT_CD,
        cg.CHANGE_DT                    AS LAST_UPDATE_DT
FROM    WWI_AUDIT.CHANGE_LOG cg
WHERE   cg.TABLE_NAME = 'PRODUCT_MASTER'
  AND   cg.OPERATION_CD IN ('D', 'O')
  AND   cg.CHANGE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')"""

    deletes = DataFlow("Detect Obsoleted Products")
    deletes.oledb_source("ORA CHANGE_LOG Product Deletes", CONN_ORACLE, delete_sql, delete_cols, timeout=600)
    deletes.derived_column(
        "Flag Obsoleted Rows",
        audit_derivations(SRC_GLOBAL) + [("DeleteFlag", '"Y"', str_col("DeleteFlag", 1))],
    )
    deletes.row_count("Count Obsoleted Rows", "User::RowsDeleted")
    deletes.oledb_destination(
        "raw OracleProductMaster Deletes",
        CONN_STAGING,
        "raw.OracleProductMaster",
        batch_size=2000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsDeleted] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_MDM.PRODUCT_MASTER"))
    extract = pkg.add(DataFlowTask(df))
    delete_pass = pkg.add(DataFlowTask(deletes))
    setwm = pkg.add(set_watermark("WWI_MDM.PRODUCT_MASTER"))
    rows = pkg.add(log_row_count("raw.OracleProductMaster"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, delete_pass, setwm, rows, done)
    return pkg


def ext_ora_product_hierarchy():
    """Full truncate-and-load of the flattened product hierarchy."""
    pkg = new_package(
        "EXT_ORA_ProductHierarchy",
        "Full truncate-and-load of the flattened product hierarchy. The source query "
        "walks PRODUCT_HIERARCHY with CONNECT BY, so the extract is small but "
        "expensive and runs on the weekly reference cadence.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
    )

    cols = [
        Column("HIERARCHY_NODE_ID", "numeric", precision=12, scale=0),
        Column("PARENT_NODE_ID", "numeric", precision=12, scale=0),
        str_col("HIERARCHY_CD", 12),
        str_col("NODE_CD", 20),
        str_col("NODE_DESC", 120),
        Column("NODE_LEVEL", "i4"),
        str_col("NODE_PATH", 400),
        str_col("LEVEL1_CD", 20),
        str_col("LEVEL2_CD", 20),
        str_col("LEVEL3_CD", 20),
        str_col("LEAF_FLG", 1),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  h.HIERARCHY_NODE_ID,
        h.PARENT_NODE_ID,
        h.HIERARCHY_CD,
        h.NODE_CD,
        h.NODE_DESC,
        LEVEL                                                   AS NODE_LEVEL,
        SYS_CONNECT_BY_PATH(h.NODE_CD, '/')                     AS NODE_PATH,
        REGEXP_SUBSTR(SYS_CONNECT_BY_PATH(h.NODE_CD, '/'), '[^/]+', 1, 1) AS LEVEL1_CD,
        REGEXP_SUBSTR(SYS_CONNECT_BY_PATH(h.NODE_CD, '/'), '[^/]+', 1, 2) AS LEVEL2_CD,
        REGEXP_SUBSTR(SYS_CONNECT_BY_PATH(h.NODE_CD, '/'), '[^/]+', 1, 3) AS LEVEL3_CD,
        CASE WHEN CONNECT_BY_ISLEAF = 1 THEN 'Y' ELSE 'N' END   AS LEAF_FLG,
        h.LAST_UPDATE_DT
FROM    WWI_MDM.PRODUCT_HIERARCHY h
WHERE   h.HIERARCHY_CD = 'MERCH'
START WITH h.PARENT_NODE_ID IS NULL
CONNECT BY PRIOR h.HIERARCHY_NODE_ID = h.PARENT_NODE_ID
ORDER SIBLINGS BY h.NODE_CD"""

    df = DataFlow("Load Product Hierarchy")
    df.oledb_source("ORA PRODUCT_HIERARCHY", CONN_ORACLE, sql, cols, timeout=3600)
    df.derived_column("Add Audit Columns", audit_derivations(SRC_GLOBAL))
    df.row_count("Count Hierarchy Nodes", "User::RowsRead")
    df.oledb_destination(
        "raw OracleProductMaster Hierarchy",
        CONN_STAGING,
        "raw.OracleProductMaster",
        batch_size=10000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Delete Hierarchy Rows",
            CONN_STAGING,
            "DELETE FROM raw.OracleProductMaster WHERE RecordKind = N'HIERARCHY';",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.OracleProductMaster"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# WWI_PROC - procurement
# ---------------------------------------------------------------------------


def ext_ora_purchase_order_hdr():
    """Timestamp incremental with heavy source-side filtering and a split that
    diverts cancelled orders away from the main raw table."""
    pkg = new_package(
        "EXT_ORA_PurchaseOrderHdr",
        "Incremental purchase order header extract from V_PURCHASE_ORDER_EXTRACT. "
        "Status and org predicates are pushed into the Oracle query; cancelled orders "
        "are split out and landed with a cancellation reason for the reconciliation "
        "report.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("CancelledOrderCount", 0, "int")],
    )

    cols = [
        Column("PO_HDR_ID", "numeric", precision=12, scale=0),
        str_col("PO_NBR", 20),
        Column("SUPP_ID", "numeric", precision=12, scale=0),
        str_col("PURCH_ORG_CD", 8),
        str_col("REGION_CD", 8),
        str_col("PO_TYPE_CD", 6),
        str_col("PO_STATUS_CD", 4),
        str_col("APPROVAL_STATUS_CD", 4),
        str_col("BUYER_CD", 12),
        str_col("CURRENCY_CD", 3),
        num_col("PO_TOTAL_AMT", 18, 2),
        num_col("PO_TOTAL_BASE_AMT", 18, 2),
        str_col("INCOTERM_CD", 3),
        str_col("PAYMENT_TERMS_CD", 10),
        str_col("CANCEL_REASON_CD", 6),
        date_col("ORDER_DT"),
        date_col("PROMISED_DT"),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  h.PO_HDR_ID,
        h.PO_NBR,
        h.SUPP_ID,
        h.PURCH_ORG_CD,
        h.REGION_CD,
        h.PO_TYPE_CD,
        h.PO_STATUS_CD,
        h.APPROVAL_STATUS_CD,
        h.BUYER_CD,
        h.CURRENCY_CD,
        h.PO_TOTAL_AMT,
        WWI_FIN.FN_CONVERT_AMOUNT(h.PO_TOTAL_AMT, h.CURRENCY_CD, 'USD', h.ORDER_DT) AS PO_TOTAL_BASE_AMT,
        h.INCOTERM_CD,
        h.PAYMENT_TERMS_CD,
        h.CANCEL_REASON_CD,
        h.ORDER_DT,
        h.PROMISED_DT,
        h.LAST_UPDATE_DT
FROM    WWI_PROC.V_PURCHASE_ORDER_EXTRACT h
WHERE   h.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   h.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   h.PO_STATUS_CD IN ('OPEN', 'PART', 'CLSD', 'CANC')
  AND   h.APPROVAL_STATUS_CD <> 'DRFT'
  AND   h.PURCH_ORG_CD NOT IN ('TEST', 'TRNG')"""

    df = DataFlow("Extract Purchase Orders")
    df.oledb_source("ORA V_PURCHASE_ORDER_EXTRACT", CONN_ORACLE, sql, cols, timeout=2400)
    df.derived_column(
        "Derive Order Attributes",
        [
            (
                "OrderAgeDays",
                "DATEDIFF(\"dd\", ORDER_DT, GETDATE())",
                int_col("OrderAgeDays"),
            ),
            (
                "LateFlag",
                'PROMISED_DT < GETDATE() && PO_STATUS_CD != "CLSD" ? "Y" : "N"',
                str_col("LateFlag", 1),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Orders Read", "User::RowsRead")
    df.conditional_split(
        "Split Cancelled Orders",
        [("Active", 'PO_STATUS_CD != "CANC"')],
        default_output="Cancelled",
    )
    df.oledb_destination(
        "raw OraclePurchaseOrderHdr",
        CONN_STAGING,
        "raw.OraclePurchaseOrderHdr",
        batch_size=30000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "raw OraclePurchaseOrderHdr Cancelled",
        CONN_STAGING,
        "raw.OraclePurchaseOrderHdr",
        from_component="Split Cancelled Orders",
        from_output="Cancelled",
    )

    init = pkg.add(init_variables("@[User::CancelledOrderCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_PROC.PURCHASE_ORDER_HDR"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("WWI_PROC.PURCHASE_ORDER_HDR"))
    rows = pkg.add(log_row_count("raw.OraclePurchaseOrderHdr"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


def ext_ora_purchase_order_line():
    """Numeric-key incremental driven by the max PO_LINE_ID landed so far."""
    pkg = new_package(
        "EXT_ORA_PurchaseOrderLine",
        "Numeric-key incremental purchase order line extract. The watermark is the "
        "highest PO_LINE_ID landed in raw.OraclePurchaseOrderLine; the new maximum is "
        "read from the source before the data flow so a mid-run insert cannot be lost.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("MaxSourceKey", 0, "long")],
    )

    cols = [
        bigint_col("PO_LINE_ID"),
        Column("PO_HDR_ID", "numeric", precision=12, scale=0),
        Column("LINE_NBR", "i4"),
        Column("PRODUCT_ID", "numeric", precision=12, scale=0),
        str_col("PRODUCT_CD", 30),
        str_col("LINE_STATUS_CD", 4),
        num_col("ORDER_QTY", 18, 4),
        num_col("RECEIVED_QTY", 18, 4),
        num_col("OPEN_QTY", 18, 4),
        str_col("UOM_CD", 4),
        num_col("UNIT_PRICE_AMT", 18, 4),
        num_col("EXTENDED_AMT", 18, 2),
        str_col("CURRENCY_CD", 3),
        str_col("COST_CENTER_CD", 10),
        str_col("GL_ACCOUNT_CD", 12),
        date_col("NEED_BY_DT"),
        date_col("CREATED_DT"),
    ]

    sql = """SELECT  l.PO_LINE_ID,
        l.PO_HDR_ID,
        l.LINE_NBR,
        l.PRODUCT_ID,
        l.PRODUCT_CD,
        l.LINE_STATUS_CD,
        l.ORDER_QTY,
        l.RECEIVED_QTY,
        WWI_PROC.FN_PO_OPEN_QTY(l.PO_LINE_ID) AS OPEN_QTY,
        l.UOM_CD,
        l.UNIT_PRICE_AMT,
        l.ORDER_QTY * l.UNIT_PRICE_AMT        AS EXTENDED_AMT,
        l.CURRENCY_CD,
        l.COST_CENTER_CD,
        l.GL_ACCOUNT_CD,
        l.NEED_BY_DT,
        l.CREATED_DT
FROM    WWI_PROC.V_PO_LINE_EXTRACT l
WHERE   l.PO_LINE_ID > ?
  AND   l.PO_LINE_ID <= ?
ORDER BY l.PO_LINE_ID"""

    df = DataFlow("Extract Purchase Order Lines")
    df.oledb_source("ORA V_PO_LINE_EXTRACT", CONN_ORACLE, sql, cols, timeout=3600)
    df.derived_column(
        "Derive Line Metrics",
        [
            (
                "ReceiptCompletePct",
                "ORDER_QTY == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(RECEIVED_QTY / ORDER_QTY)",
                num_col("ReceiptCompletePct", 9, 4),
            )
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Lines Read", "User::RowsRead")
    df.oledb_destination(
        "raw OraclePurchaseOrderLine",
        CONN_STAGING,
        "raw.OraclePurchaseOrderLine",
        batch_size=100000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="ORA V_PO_LINE_EXTRACT",
    )

    init = pkg.add(init_variables("@[User::MaxSourceKey] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_PROC.PURCHASE_ORDER_LINE"))
    read_max = pkg.add(
        ExecuteSql(
            "Read Source Max Key",
            CONN_ORACLE,
            "SELECT NVL(MAX(PO_LINE_ID), 0) AS MAX_KEY FROM WWI_PROC.PURCHASE_ORDER_LINE",
            result_type="ResultSetType_SingleRow",
            result_bindings=[("0", "User::WatermarkTo")],
            timeout=300,
        )
    )
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("WWI_PROC.PURCHASE_ORDER_LINE"))
    rows = pkg.add(log_row_count("raw.OraclePurchaseOrderLine"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, read_max, extract, setwm, rows, done)
    return pkg


def ext_ora_receipt_line():
    """Numeric-key incremental over receipt lines joined to the receipt header."""
    pkg = new_package(
        "EXT_ORA_ReceiptLine",
        "Numeric-key incremental receipt line extract joined to PO_RECEIPT_HDR and "
        "the PO line, carrying the price/quantity variance percentage used by the "
        "supplier scorecard.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("VarianceRowCount", 0, "int")],
    )

    cols = [
        bigint_col("RECEIPT_LINE_ID"),
        Column("RECEIPT_HDR_ID", "numeric", precision=12, scale=0),
        str_col("RECEIPT_NBR", 20),
        bigint_col("PO_LINE_ID"),
        str_col("PO_NBR", 20),
        Column("SUPP_ID", "numeric", precision=12, scale=0),
        Column("PRODUCT_ID", "numeric", precision=12, scale=0),
        str_col("WAREHOUSE_CD", 8),
        num_col("RECEIVED_QTY", 18, 4),
        num_col("ACCEPTED_QTY", 18, 4),
        num_col("REJECTED_QTY", 18, 4),
        str_col("UOM_CD", 4),
        num_col("UNIT_COST_AMT", 18, 4),
        num_col("VARIANCE_PCT", 9, 4),
        str_col("INSPECTION_STATUS_CD", 4),
        str_col("REJECT_REASON_CD", 6),
        date_col("RECEIPT_DT"),
    ]

    sql = """SELECT  rl.RECEIPT_LINE_ID,
        rh.RECEIPT_HDR_ID,
        rh.RECEIPT_NBR,
        rl.PO_LINE_ID,
        ph.PO_NBR,
        rh.SUPP_ID,
        rl.PRODUCT_ID,
        rh.WAREHOUSE_CD,
        rl.RECEIVED_QTY,
        rl.ACCEPTED_QTY,
        rl.RECEIVED_QTY - rl.ACCEPTED_QTY                       AS REJECTED_QTY,
        rl.UOM_CD,
        rl.UNIT_COST_AMT,
        WWI_PROC.FN_RECEIPT_VARIANCE_PCT(rl.RECEIPT_LINE_ID)    AS VARIANCE_PCT,
        rl.INSPECTION_STATUS_CD,
        rl.REJECT_REASON_CD,
        rh.RECEIPT_DT
FROM    WWI_PROC.PO_RECEIPT_LINE rl
        INNER JOIN WWI_PROC.PO_RECEIPT_HDR rh
            ON rh.RECEIPT_HDR_ID = rl.RECEIPT_HDR_ID
        INNER JOIN WWI_PROC.PURCHASE_ORDER_LINE pl
            ON pl.PO_LINE_ID = rl.PO_LINE_ID
        INNER JOIN WWI_PROC.PURCHASE_ORDER_HDR ph
            ON ph.PO_HDR_ID = pl.PO_HDR_ID
WHERE   rl.RECEIPT_LINE_ID > ?
  AND   rh.RECEIPT_STATUS_CD <> 'VOID'
ORDER BY rl.RECEIPT_LINE_ID"""

    df = DataFlow("Extract Receipt Lines")
    df.oledb_source("ORA PO_RECEIPT_LINE", CONN_ORACLE, sql, cols, timeout=3600)
    df.derived_column(
        "Classify Variance",
        [
            (
                "VarianceBand",
                'ABS(VARIANCE_PCT) <= 0.01 ? "OK" : (ABS(VARIANCE_PCT) <= 0.05 ? "WARN" : "EXCP")',
                str_col("VarianceBand", 4),
            )
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Receipt Lines", "User::RowsRead")
    df.conditional_split(
        "Route Inspection Failures",
        [("Accepted", 'INSPECTION_STATUS_CD != "FAIL"')],
        default_output="Failed Inspection",
    )
    df.oledb_destination(
        "raw OracleReceiptLine",
        CONN_STAGING,
        "raw.OracleReceiptLine",
        batch_size=75000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "raw OracleReceiptLine Quarantined",
        CONN_STAGING,
        "raw.OracleReceiptLine",
        from_component="Route Inspection Failures",
        from_output="Failed Inspection",
    )

    init = pkg.add(init_variables("@[User::VarianceRowCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_PROC.PO_RECEIPT_LINE"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("WWI_PROC.PO_RECEIPT_LINE"))
    rows = pkg.add(log_row_count("raw.OracleReceiptLine"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


def ext_ora_vendor_contract():
    """Full reload of vendor contracts with their line-level commitments rolled up."""
    pkg = new_package(
        "EXT_ORA_VendorContract",
        "Full truncate-and-load of vendor contracts with contract line commitments "
        "aggregated in the source query. Contracts are few and change rarely, so the "
        "package reloads the whole set rather than tracking a watermark.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
    )

    cols = [
        Column("CONTRACT_ID", "numeric", precision=12, scale=0),
        str_col("CONTRACT_NBR", 20),
        Column("SUPP_ID", "numeric", precision=12, scale=0),
        str_col("CONTRACT_TYPE_CD", 6),
        str_col("CONTRACT_STATUS_CD", 4),
        str_col("REGION_CD", 8),
        str_col("CURRENCY_CD", 3),
        num_col("COMMITTED_AMT", 18, 2),
        num_col("CONSUMED_AMT", 18, 2),
        Column("LINE_COUNT", "i4"),
        str_col("PRICE_PROTECTION_FLG", 1),
        str_col("AUTO_RENEW_FLG", 1),
        Column("NOTICE_PERIOD_DAYS", "i4"),
        date_col("EFFECTIVE_FROM_DT"),
        date_col("EFFECTIVE_TO_DT"),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  c.CONTRACT_ID,
        c.CONTRACT_NBR,
        c.SUPP_ID,
        c.CONTRACT_TYPE_CD,
        c.CONTRACT_STATUS_CD,
        c.REGION_CD,
        c.CURRENCY_CD,
        c.COMMITTED_AMT,
        NVL(cl.CONSUMED_AMT, 0)  AS CONSUMED_AMT,
        NVL(cl.LINE_COUNT, 0)    AS LINE_COUNT,
        c.PRICE_PROTECTION_FLG,
        c.AUTO_RENEW_FLG,
        c.NOTICE_PERIOD_DAYS,
        c.EFFECTIVE_FROM_DT,
        c.EFFECTIVE_TO_DT,
        c.LAST_UPDATE_DT
FROM    WWI_PROC.VENDOR_CONTRACT c
        LEFT OUTER JOIN (
            SELECT CONTRACT_ID,
                   SUM(CONSUMED_AMT) AS CONSUMED_AMT,
                   COUNT(*)          AS LINE_COUNT
            FROM   WWI_PROC.VENDOR_CONTRACT_LINE
            GROUP BY CONTRACT_ID
        ) cl ON cl.CONTRACT_ID = c.CONTRACT_ID
WHERE   c.CONTRACT_STATUS_CD <> 'DELT'"""

    df = DataFlow("Load Vendor Contracts")
    df.oledb_source("ORA VENDOR_CONTRACT", CONN_ORACLE, sql, cols, timeout=900)
    df.derived_column(
        "Derive Contract Utilisation",
        [
            (
                "UtilisationPct",
                "COMMITTED_AMT == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(CONSUMED_AMT / COMMITTED_AMT)",
                num_col("UtilisationPct", 9, 4),
            ),
            (
                "RenewalDueFlag",
                'AUTO_RENEW_FLG == "N" && DATEDIFF("dd", GETDATE(), EFFECTIVE_TO_DT) <= NOTICE_PERIOD_DAYS ? "Y" : "N"',
                str_col("RenewalDueFlag", 1),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Contracts", "User::RowsRead")
    df.oledb_destination(
        "raw OracleVendorContract",
        CONN_STAGING,
        "raw.OracleVendorContract",
        batch_size=5000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    trunc = pkg.add(truncate("raw.OracleVendorContract"))
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.OracleVendorContract"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, trunc, extract, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# WWI_FIN - finance
# ---------------------------------------------------------------------------


def ext_ora_ap_invoice_hdr():
    """Timestamp incremental with the three regional tax treatments resolved in
    the source query."""
    pkg = new_package(
        "EXT_ORA_ApInvoiceHdr",
        "Incremental AP invoice header extract. Regional tax treatment diverges in "
        "the source query: NA invoices carry state and local sales tax, EU invoices "
        "carry recoverable and non-recoverable VAT with a registration number, APAC "
        "invoices carry GST. Invoices sitting on an unresolved hold are excluded.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("HeldInvoiceCount", 0, "int")],
    )

    cols = [
        Column("AP_INVOICE_ID", "numeric", precision=12, scale=0),
        str_col("INVOICE_NBR", 30),
        Column("SUPP_ID", "numeric", precision=12, scale=0),
        str_col("LEGAL_ENTITY_CD", 10),
        str_col("REGION_CD", 8),
        str_col("INVOICE_TYPE_CD", 6),
        str_col("INVOICE_STATUS_CD", 4),
        str_col("CURRENCY_CD", 3),
        num_col("INVOICE_AMT", 18, 2),
        num_col("TAX_AMT", 18, 2),
        num_col("RECOVERABLE_TAX_AMT", 18, 2),
        num_col("NON_RECOVERABLE_TAX_AMT", 18, 2),
        str_col("TAX_TREATMENT_CD", 8),
        str_col("TAX_REGISTRATION_NBR", 30),
        num_col("BASE_AMT", 18, 2),
        str_col("PAYMENT_TERMS_CD", 10),
        date_col("INVOICE_DT"),
        date_col("DUE_DT"),
        date_col("GL_DATE"),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  i.AP_INVOICE_ID,
        i.INVOICE_NBR,
        i.SUPP_ID,
        i.LEGAL_ENTITY_CD,
        i.REGION_CD,
        i.INVOICE_TYPE_CD,
        i.INVOICE_STATUS_CD,
        i.CURRENCY_CD,
        i.INVOICE_AMT,
        WWI_FIN.FN_TAX_AMOUNT(i.AP_INVOICE_ID)  AS TAX_AMT,
        CASE i.REGION_CD
            WHEN 'EU'   THEN i.RECOVERABLE_VAT_AMT
            ELSE 0
        END                                     AS RECOVERABLE_TAX_AMT,
        CASE i.REGION_CD
            WHEN 'EU'   THEN i.NON_RECOVERABLE_VAT_AMT
            WHEN 'NA'   THEN i.SALES_TAX_AMT
            WHEN 'APAC' THEN i.GST_AMT
            ELSE 0
        END                                     AS NON_RECOVERABLE_TAX_AMT,
        CASE i.REGION_CD
            WHEN 'NA'   THEN 'SALESTAX'
            WHEN 'EU'   THEN 'VAT'
            WHEN 'APAC' THEN 'GST'
            ELSE 'NONE'
        END                                     AS TAX_TREATMENT_CD,
        i.TAX_REGISTRATION_NBR,
        WWI_FIN.FN_CONVERT_AMOUNT(i.INVOICE_AMT, i.CURRENCY_CD, 'USD', i.GL_DATE) AS BASE_AMT,
        i.PAYMENT_TERMS_CD,
        i.INVOICE_DT,
        WWI_FIN.FN_DUE_DATE(i.INVOICE_DT, i.PAYMENT_TERMS_CD) AS DUE_DT,
        i.GL_DATE,
        i.LAST_UPDATE_DT
FROM    WWI_FIN.V_AP_INVOICE_EXTRACT i
WHERE   i.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   i.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   NOT EXISTS (
            SELECT 1
            FROM   WWI_FIN.AP_INVOICE_HOLD h
            WHERE  h.AP_INVOICE_ID = i.AP_INVOICE_ID
              AND  h.RELEASE_DT IS NULL
        )"""

    df = DataFlow("Extract AP Invoice Headers")
    df.oledb_source("ORA V_AP_INVOICE_EXTRACT", CONN_ORACLE, sql, cols, timeout=3600)
    df.derived_column(
        "Derive Tax Split",
        [
            (
                "TaxRatePct",
                "INVOICE_AMT == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(TAX_AMT / INVOICE_AMT)",
                num_col("TaxRatePct", 9, 4),
            ),
            (
                "VatRecoverableFlag",
                'TAX_TREATMENT_CD == "VAT" && RECOVERABLE_TAX_AMT > 0 ? "Y" : "N"',
                str_col("VatRecoverableFlag", 1),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Invoices Read", "User::RowsRead")
    df.oledb_destination(
        "raw OracleApInvoiceHdr",
        CONN_STAGING,
        "raw.OracleApInvoiceHdr",
        batch_size=50000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="ORA V_AP_INVOICE_EXTRACT",
    )

    init = pkg.add(init_variables("@[User::HeldInvoiceCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_FIN.AP_INVOICE_HDR"))
    extract = pkg.add(DataFlowTask(df))
    count_holds = pkg.add(
        ExecuteSql(
            "Count Suppressed Holds",
            CONN_ORACLE,
            "SELECT COUNT(*) AS HELD_COUNT FROM WWI_FIN.AP_INVOICE_HOLD WHERE RELEASE_DT IS NULL",
            result_type="ResultSetType_SingleRow",
            result_bindings=[("0", "User::HeldInvoiceCount")],
            timeout=300,
        )
    )
    setwm = pkg.add(set_watermark("WWI_FIN.AP_INVOICE_HDR"))
    rows = pkg.add(log_row_count("raw.OracleApInvoiceHdr"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, count_holds, setwm, rows, done)
    return pkg


def ext_ora_ap_invoice_line():
    """Numeric-key incremental over invoice distribution lines with a cost-centre
    lookup that fails the row rather than defaulting it."""
    pkg = new_package(
        "EXT_ORA_ApInvoiceLine",
        "Numeric-key incremental AP invoice distribution extract. Cost centres are "
        "looked up against raw.OracleCostCenter and unmatched distributions are sent "
        "to err.RejectedInvoiceLine rather than defaulted to a suspense account.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
    )

    cols = [
        bigint_col("AP_INVOICE_LINE_ID"),
        Column("AP_INVOICE_ID", "numeric", precision=12, scale=0),
        Column("LINE_NBR", "i4"),
        str_col("DISTRIBUTION_TYPE_CD", 6),
        str_col("COST_CENTER_CD", 10),
        str_col("GL_ACCOUNT_CD", 12),
        str_col("PROJECT_CD", 12),
        bigint_col("PO_LINE_ID"),
        num_col("LINE_AMT", 18, 2),
        num_col("LINE_TAX_AMT", 18, 2),
        str_col("CURRENCY_CD", 3),
        str_col("EXPENSE_CATEGORY_CD", 8),
        str_col("ACCRUAL_FLG", 1),
        date_col("GL_DATE"),
    ]

    sql = """SELECT  l.AP_INVOICE_LINE_ID,
        l.AP_INVOICE_ID,
        l.LINE_NBR,
        l.DISTRIBUTION_TYPE_CD,
        l.COST_CENTER_CD,
        l.GL_ACCOUNT_CD,
        l.PROJECT_CD,
        l.PO_LINE_ID,
        l.LINE_AMT,
        l.LINE_TAX_AMT,
        h.CURRENCY_CD,
        l.EXPENSE_CATEGORY_CD,
        l.ACCRUAL_FLG,
        h.GL_DATE
FROM    WWI_FIN.AP_INVOICE_LINE l
        INNER JOIN WWI_FIN.AP_INVOICE_HDR h
            ON h.AP_INVOICE_ID = l.AP_INVOICE_ID
WHERE   l.AP_INVOICE_LINE_ID > ?
  AND   l.AP_INVOICE_LINE_ID <= ?
  AND   h.INVOICE_STATUS_CD <> 'CANC'
ORDER BY l.AP_INVOICE_LINE_ID"""

    df = DataFlow("Extract AP Invoice Lines")
    df.oledb_source("ORA AP_INVOICE_LINE", CONN_ORACLE, sql, cols, timeout=3600)
    df.lookup(
        "Lookup Cost Center",
        CONN_STAGING,
        "SELECT CostCenterCode AS COST_CENTER_CD, CostCenterKey, OwningRegionCode "
        "FROM raw.OracleCostCenter WITH (NOLOCK) WHERE IsActive = 1;",
        ["COST_CENTER_CD"],
        [int_col("CostCenterKey"), str_col("OwningRegionCode", 8)],
        no_match="RD",
    )
    df.derived_column(
        "Derive Accrual Attributes",
        [
            (
                "AccrualReversalFlag",
                'ACCRUAL_FLG == "Y" && LINE_AMT < 0 ? "Y" : "N"',
                str_col("AccrualReversalFlag", 1),
            )
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Distributions", "User::RowsRead")
    df.oledb_destination(
        "raw OracleApInvoiceLine",
        CONN_STAGING,
        "raw.OracleApInvoiceLine",
        batch_size=100000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedInvoiceLine",
        CONN_STAGING,
        "err.RejectedInvoiceLine",
        from_component="Lookup Cost Center",
        from_output="Lookup No Match Output",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_FIN.AP_INVOICE_LINE"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("WWI_FIN.AP_INVOICE_LINE"))
    rows = pkg.add(log_row_count("raw.OracleApInvoiceLine"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


def ext_ora_ap_payment():
    """Timestamp incremental over payments with FX gain/loss carried across."""
    pkg = new_package(
        "EXT_ORA_ApPayment",
        "Incremental AP payment extract from V_AP_PAYMENT_EXTRACT. Realised FX gain "
        "and loss is computed in the source query against the payment-date rate, and "
        "void payments are landed with a reversal flag so downstream nets them off.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("VoidPaymentCount", 0, "int")],
    )

    cols = [
        Column("AP_PAYMENT_ID", "numeric", precision=12, scale=0),
        str_col("PAYMENT_NBR", 30),
        Column("SUPP_ID", "numeric", precision=12, scale=0),
        str_col("PAYMENT_METHOD_CD", 6),
        str_col("PAYMENT_STATUS_CD", 4),
        str_col("BANK_ACCOUNT_CD", 12),
        str_col("CURRENCY_CD", 3),
        num_col("PAYMENT_AMT", 18, 2),
        num_col("PAYMENT_BASE_AMT", 18, 2),
        num_col("FX_RATE", 18, 8),
        num_col("FX_GAIN_LOSS_AMT", 18, 2),
        str_col("REGION_CD", 8),
        str_col("VOID_FLG", 1),
        date_col("PAYMENT_DT"),
        date_col("CLEARED_DT"),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  p.AP_PAYMENT_ID,
        p.PAYMENT_NBR,
        p.SUPP_ID,
        p.PAYMENT_METHOD_CD,
        p.PAYMENT_STATUS_CD,
        p.BANK_ACCOUNT_CD,
        p.CURRENCY_CD,
        p.PAYMENT_AMT,
        WWI_FIN.FN_CONVERT_AMOUNT(p.PAYMENT_AMT, p.CURRENCY_CD, 'USD', p.PAYMENT_DT) AS PAYMENT_BASE_AMT,
        fx.RATE                                              AS FX_RATE,
        WWI_FIN.FN_CONVERT_AMOUNT(p.PAYMENT_AMT, p.CURRENCY_CD, 'USD', p.PAYMENT_DT)
            - WWI_FIN.FN_CONVERT_AMOUNT(p.PAYMENT_AMT, p.CURRENCY_CD, 'USD', p.INVOICE_GL_DATE)
                                                             AS FX_GAIN_LOSS_AMT,
        p.REGION_CD,
        p.VOID_FLG,
        p.PAYMENT_DT,
        p.CLEARED_DT,
        p.LAST_UPDATE_DT
FROM    WWI_FIN.V_AP_PAYMENT_EXTRACT p
        LEFT OUTER JOIN WWI_REF.FX_RATE_DAILY fx
            ON fx.FROM_CURRENCY_CD = p.CURRENCY_CD
           AND fx.TO_CURRENCY_CD = 'USD'
           AND fx.RATE_DT = TRUNC(p.PAYMENT_DT)
           AND fx.RATE_TYPE_CD = 'SPOT'
WHERE   p.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   p.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')"""

    df = DataFlow("Extract AP Payments")
    df.oledb_source("ORA V_AP_PAYMENT_EXTRACT", CONN_ORACLE, sql, cols, timeout=2400)
    df.derived_column(
        "Derive Payment Attributes",
        [
            (
                "ReversalFlag",
                'VOID_FLG == "Y" || PAYMENT_AMT < 0 ? "Y" : "N"',
                str_col("ReversalFlag", 1),
            ),
            (
                "DaysToClear",
                'ISNULL(CLEARED_DT) ? -1 : DATEDIFF("dd", PAYMENT_DT, CLEARED_DT)',
                int_col("DaysToClear"),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Payments Read", "User::RowsRead")
    df.conditional_split(
        "Split Void Payments",
        [("Settled", 'VOID_FLG != "Y"')],
        default_output="Voided",
    )
    df.oledb_destination(
        "raw OracleApPayment",
        CONN_STAGING,
        "raw.OracleApPayment",
        batch_size=25000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedPayment",
        CONN_STAGING,
        "err.RejectedPayment",
        from_component="Split Void Payments",
        from_output="Voided",
    )

    init = pkg.add(init_variables("@[User::VoidPaymentCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_FIN.AP_PAYMENT"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("WWI_FIN.AP_PAYMENT"))
    rows = pkg.add(log_row_count("raw.OracleApPayment"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


def ext_ora_ap_payment_apply():
    """Numeric-key incremental over payment applications (payment-to-invoice)."""
    pkg = new_package(
        "EXT_ORA_ApPaymentApply",
        "Numeric-key incremental extract of payment applications, denormalised over "
        "AP_PAYMENT_APPLY, AP_PAYMENT and AP_INVOICE_HDR so the settlement, the "
        "discount taken and the withheld amount arrive on one row.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
    )

    cols = [
        bigint_col("PAYMENT_APPLY_ID"),
        Column("AP_PAYMENT_ID", "numeric", precision=12, scale=0),
        Column("AP_INVOICE_ID", "numeric", precision=12, scale=0),
        str_col("PAYMENT_NBR", 30),
        str_col("INVOICE_NBR", 30),
        Column("SUPP_ID", "numeric", precision=12, scale=0),
        num_col("APPLIED_AMT", 18, 2),
        num_col("DISCOUNT_TAKEN_AMT", 18, 2),
        num_col("WITHHELD_AMT", 18, 2),
        str_col("WITHHOLDING_RULE_CD", 10),
        str_col("CURRENCY_CD", 3),
        date_col("APPLIED_DT"),
        date_col("INVOICE_DT"),
    ]

    sql = """SELECT  a.PAYMENT_APPLY_ID,
        a.AP_PAYMENT_ID,
        a.AP_INVOICE_ID,
        p.PAYMENT_NBR,
        i.INVOICE_NBR,
        i.SUPP_ID,
        a.APPLIED_AMT,
        a.DISCOUNT_TAKEN_AMT,
        NVL(w.WITHHELD_AMT, 0)  AS WITHHELD_AMT,
        w.WITHHOLDING_RULE_CD,
        p.CURRENCY_CD,
        a.APPLIED_DT,
        i.INVOICE_DT
FROM    WWI_FIN.AP_PAYMENT_APPLY a
        INNER JOIN WWI_FIN.AP_PAYMENT p
            ON p.AP_PAYMENT_ID = a.AP_PAYMENT_ID
        INNER JOIN WWI_FIN.AP_INVOICE_HDR i
            ON i.AP_INVOICE_ID = a.AP_INVOICE_ID
        LEFT OUTER JOIN WWI_FIN.WITHHOLDING_RULE w
            ON w.WITHHOLDING_RULE_CD = i.WITHHOLDING_RULE_CD
WHERE   a.PAYMENT_APPLY_ID > ?
ORDER BY a.PAYMENT_APPLY_ID"""

    df = DataFlow("Extract Payment Applications")
    df.oledb_source("ORA AP_PAYMENT_APPLY", CONN_ORACLE, sql, cols, timeout=1800)
    df.derived_column(
        "Derive Settlement Metrics",
        [
            (
                "SettlementDays",
                'DATEDIFF("dd", INVOICE_DT, APPLIED_DT)',
                int_col("SettlementDays"),
            ),
            (
                "DiscountTakenFlag",
                'DISCOUNT_TAKEN_AMT > 0 ? "Y" : "N"',
                str_col("DiscountTakenFlag", 1),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Applications", "User::RowsRead")
    df.oledb_destination(
        "raw OracleApPayment Applications",
        CONN_STAGING,
        "raw.OracleApPayment",
        batch_size=60000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedPayment",
        CONN_STAGING,
        "err.RejectedPayment",
        from_component="ORA AP_PAYMENT_APPLY",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_FIN.AP_PAYMENT_APPLY"))
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("WWI_FIN.AP_PAYMENT_APPLY"))
    rows = pkg.add(log_row_count("raw.OracleApPayment"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, extract, setwm, rows, done)
    return pkg


def ext_ora_ap_aging():
    """Full snapshot reload of the AP aging buckets."""
    pkg = new_package(
        "EXT_ORA_ApAging",
        "Full reload of the AP aging snapshot. The ERP recomputes the buckets nightly, "
        "so the package clears the current as-of date and reloads it; the aging bucket "
        "boundaries come from the ERP function rather than being reimplemented here.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("SnapshotDate", "1900-01-01", "string")],
    )

    cols = [
        Column("AP_INVOICE_ID", "numeric", precision=12, scale=0),
        str_col("INVOICE_NBR", 30),
        Column("SUPP_ID", "numeric", precision=12, scale=0),
        str_col("REGION_CD", 8),
        str_col("CURRENCY_CD", 3),
        num_col("OUTSTANDING_AMT", 18, 2),
        num_col("OUTSTANDING_BASE_AMT", 18, 2),
        Column("DAYS_PAST_DUE", "i4"),
        str_col("AGING_BUCKET_CD", 12),
        str_col("DISPUTE_FLG", 1),
        date_col("DUE_DT"),
        date_col("SNAPSHOT_DT"),
    ]

    sql = """SELECT  a.AP_INVOICE_ID,
        a.INVOICE_NBR,
        a.SUPP_ID,
        a.REGION_CD,
        a.CURRENCY_CD,
        a.OUTSTANDING_AMT,
        WWI_FIN.FN_CONVERT_AMOUNT(a.OUTSTANDING_AMT, a.CURRENCY_CD, 'USD', a.SNAPSHOT_DT) AS OUTSTANDING_BASE_AMT,
        TRUNC(a.SNAPSHOT_DT) - TRUNC(a.DUE_DT)          AS DAYS_PAST_DUE,
        WWI_FIN.FN_AGING_BUCKET(a.DUE_DT, a.SNAPSHOT_DT) AS AGING_BUCKET_CD,
        a.DISPUTE_FLG,
        a.DUE_DT,
        a.SNAPSHOT_DT
FROM    WWI_FIN.V_AP_AGING_CURRENT a
WHERE   a.OUTSTANDING_AMT <> 0"""

    df = DataFlow("Load AP Aging Snapshot")
    df.oledb_source("ORA V_AP_AGING_CURRENT", CONN_ORACLE, sql, cols, timeout=2400)
    df.derived_column(
        "Add Snapshot Audit",
        [("RecordKind", '"AGING"', str_col("RecordKind", 12))] + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Aging Rows", "User::RowsRead")
    df.oledb_destination(
        "raw OracleApInvoiceHdr Aging",
        CONN_STAGING,
        "raw.OracleApInvoiceHdr",
        batch_size=40000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Clear Current Snapshot",
            CONN_STAGING,
            "DELETE FROM raw.OracleApInvoiceHdr "
            "WHERE RecordKind = N'AGING' AND CAST(ExtractedAtUtc AS date) = CAST(SYSUTCDATETIME() AS date);",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.OracleApInvoiceHdr"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, rows, done)
    return pkg


def ext_ora_gl_journal_line():
    """Date-window extract over a bounded accounting-date range, re-runnable for
    any window, with per-region fiscal calendars."""
    pkg = new_package(
        "EXT_ORA_GlJournalLine",
        "Date-window GL journal line extract. The window comes from "
        "etl.usp_GetWatermark (DateWindow style) and is re-runnable for any bounded "
        "accounting-date range; only periods that the ledger reports as open or "
        "recently closed are pulled, and the fiscal period is resolved per region.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("WindowDays", 7, "int"), ("OpenPeriodCount", 0, "int")],
    )

    cols = [
        bigint_col("GL_JOURNAL_LINE_ID"),
        Column("GL_JOURNAL_HDR_ID", "numeric", precision=12, scale=0),
        str_col("JOURNAL_NBR", 20),
        str_col("JOURNAL_SOURCE_CD", 10),
        str_col("JOURNAL_CATEGORY_CD", 10),
        str_col("LEDGER_CD", 10),
        str_col("REGION_CD", 8),
        str_col("GL_ACCOUNT_CD", 12),
        str_col("COST_CENTER_CD", 10),
        str_col("PRODUCT_LINE_CD", 10),
        num_col("ENTERED_DR_AMT", 18, 2),
        num_col("ENTERED_CR_AMT", 18, 2),
        num_col("ACCOUNTED_DR_AMT", 18, 2),
        num_col("ACCOUNTED_CR_AMT", 18, 2),
        str_col("CURRENCY_CD", 3),
        str_col("FISCAL_PERIOD_CD", 10),
        str_col("PERIOD_STATUS_CD", 4),
        date_col("ACCOUNTING_DT"),
        date_col("POSTED_DT"),
    ]

    # NA closes on a 4-4-5 calendar, EU on calendar months, APAC on an
    # April-March fiscal year; FN_FISCAL_PERIOD encapsulates all three.
    sql = """SELECT  l.GL_JOURNAL_LINE_ID,
        h.GL_JOURNAL_HDR_ID,
        h.JOURNAL_NBR,
        h.JOURNAL_SOURCE_CD,
        h.JOURNAL_CATEGORY_CD,
        h.LEDGER_CD,
        h.REGION_CD,
        l.GL_ACCOUNT_CD,
        l.COST_CENTER_CD,
        l.PRODUCT_LINE_CD,
        l.ENTERED_DR_AMT,
        l.ENTERED_CR_AMT,
        l.ACCOUNTED_DR_AMT,
        l.ACCOUNTED_CR_AMT,
        h.CURRENCY_CD,
        WWI_REF.FN_FISCAL_PERIOD(h.ACCOUNTING_DT, h.REGION_CD) AS FISCAL_PERIOD_CD,
        ps.PERIOD_STATUS_CD,
        h.ACCOUNTING_DT,
        h.POSTED_DT
FROM    WWI_FIN.GL_JOURNAL_LINE l
        INNER JOIN WWI_FIN.GL_JOURNAL_HDR h
            ON h.GL_JOURNAL_HDR_ID = l.GL_JOURNAL_HDR_ID
        INNER JOIN WWI_FIN.GL_PERIOD_STATUS ps
            ON ps.LEDGER_CD = h.LEDGER_CD
           AND ps.FISCAL_PERIOD_CD = WWI_REF.FN_FISCAL_PERIOD(h.ACCOUNTING_DT, h.REGION_CD)
WHERE   h.ACCOUNTING_DT >= TO_DATE(?, 'YYYY-MM-DD')
  AND   h.ACCOUNTING_DT <  TO_DATE(?, 'YYYY-MM-DD')
  AND   h.POSTING_STATUS_CD = 'P'
  AND   ps.PERIOD_STATUS_CD IN ('O', 'C')
ORDER BY h.ACCOUNTING_DT, l.GL_JOURNAL_LINE_ID"""

    df = DataFlow("Extract GL Journal Lines")
    df.oledb_source("ORA GL_JOURNAL_LINE", CONN_ORACLE, sql, cols, timeout=7200)
    df.derived_column(
        "Derive Ledger Attributes",
        [
            (
                "NetAmount",
                "ACCOUNTED_DR_AMT - ACCOUNTED_CR_AMT",
                money_col("NetAmount"),
            ),
            (
                "FiscalCalendarCd",
                'REGION_CD == "NA" ? "445" : (REGION_CD == "EU" ? "CAL" : "APR_MAR")',
                str_col("FiscalCalendarCd", 8),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Journal Lines", "User::RowsRead")
    df.oledb_destination(
        "raw OracleGlJournalLine",
        CONN_STAGING,
        "raw.OracleGlJournalLine",
        batch_size=200000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="ORA GL_JOURNAL_LINE",
    )

    init = pkg.add(init_variables("@[User::OpenPeriodCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_FIN.GL_JOURNAL_LINE"))
    clear_window = pkg.add(
        ExecuteSql(
            "Clear Target Window",
            CONN_STAGING,
            "DELETE FROM raw.OracleGlJournalLine "
            "WHERE AccountingDate >= CAST(? AS date) AND AccountingDate < CAST(? AS date);",
            parameter_bindings=[
                ("User::WatermarkFrom", 0, "NVARCHAR"),
                ("User::WatermarkTo", 1, "NVARCHAR"),
            ],
        )
    )
    extract = pkg.add(DataFlowTask(df))
    setwm = pkg.add(set_watermark("WWI_FIN.GL_JOURNAL_LINE"))
    rows = pkg.add(log_row_count("raw.OracleGlJournalLine"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, clear_window, extract, setwm, rows, done)
    return pkg


def ext_ora_cost_center():
    """Full reload of the cost centre hierarchy."""
    pkg = new_package(
        "EXT_ORA_CostCenter",
        "Full truncate-and-load of the cost centre hierarchy from "
        "V_COST_CENTER_HIERARCHY, including the allocation rule that applies to each "
        "centre. Small dimension, reloaded whole every night.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
    )

    cols = [
        str_col("COST_CENTER_CD", 10),
        str_col("COST_CENTER_NAME", 120),
        str_col("PARENT_COST_CENTER_CD", 10),
        Column("HIERARCHY_LEVEL", "i4"),
        str_col("COMPANY_CD", 6),
        str_col("REGION_CD", 8),
        str_col("MANAGER_EMPLOYEE_CD", 12),
        str_col("FUNCTION_CD", 8),
        str_col("ALLOCATION_RULE_CD", 10),
        num_col("ALLOCATION_PCT", 9, 4),
        str_col("ACTIVE_FLG", 1),
        date_col("EFFECTIVE_FROM_DT"),
        date_col("EFFECTIVE_TO_DT"),
    ]

    sql = """SELECT  cc.COST_CENTER_CD,
        cc.COST_CENTER_NAME,
        cc.PARENT_COST_CENTER_CD,
        cc.HIERARCHY_LEVEL,
        cc.COMPANY_CD,
        cc.REGION_CD,
        cc.MANAGER_EMPLOYEE_CD,
        cc.FUNCTION_CD,
        ar.ALLOCATION_RULE_CD,
        ar.ALLOCATION_PCT,
        cc.ACTIVE_FLG,
        cc.EFFECTIVE_FROM_DT,
        cc.EFFECTIVE_TO_DT
FROM    WWI_FIN.V_COST_CENTER_HIERARCHY cc
        LEFT OUTER JOIN WWI_FIN.COST_ALLOCATION_RULE ar
            ON ar.COST_CENTER_CD = cc.COST_CENTER_CD
           AND SYSDATE BETWEEN ar.EFFECTIVE_FROM_DT AND NVL(ar.EFFECTIVE_TO_DT, DATE '4712-12-31')"""

    df = DataFlow("Load Cost Centers")
    df.oledb_source("ORA V_COST_CENTER_HIERARCHY", CONN_ORACLE, sql, cols, timeout=600)
    df.derived_column(
        "Derive Cost Center Flags",
        [
            ("IsActive", 'ACTIVE_FLG == "Y" ? (DT_I4)1 : (DT_I4)0', int_col("IsActive")),
            ("CostCenterKey", "(DT_I4)0", int_col("CostCenterKey")),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Cost Centers", "User::RowsRead")
    df.oledb_destination(
        "raw OracleCostCenter",
        CONN_STAGING,
        "raw.OracleCostCenter",
        batch_size=5000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    trunc = pkg.add(truncate("raw.OracleCostCenter"))
    extract = pkg.add(DataFlowTask(df))
    key = pkg.add(
        ExecuteSql(
            "Assign Surrogate Keys",
            CONN_STAGING,
            "UPDATE cc SET CostCenterKey = x.NewKey "
            "FROM raw.OracleCostCenter AS cc "
            "CROSS APPLY (SELECT ROW_NUMBER() OVER (ORDER BY cc.CostCenterCode) AS NewKey) AS x;",
        )
    )
    rows = pkg.add(log_row_count("raw.OracleCostCenter"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, trunc, extract, key, rows, done)
    return pkg


def ext_ora_tax_rate():
    """Reference refresh on its own cadence: the three regional tax regimes."""
    pkg = new_package(
        "EXT_ORA_TaxRate",
        "Reference refresh of tax rates on the weekly reference cadence. The source "
        "query unions the three regimes the estate actually runs - NA state/county "
        "sales tax, EU VAT with reduced and zero rates, APAC GST - because they live "
        "in TAX_RATE with different jurisdiction granularity.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("ReferenceCadence", "WEEKLY", "string")],
    )

    cols = [
        str_col("TAX_RATE_CD", 12),
        str_col("TAX_REGIME_CD", 8),
        str_col("JURISDICTION_CD", 12),
        str_col("JURISDICTION_LEVEL_CD", 8),
        str_col("COUNTRY_CD", 3),
        str_col("REGION_CD", 8),
        num_col("RATE_PCT", 9, 5),
        str_col("RATE_TYPE_CD", 8),
        str_col("RECOVERABLE_FLG", 1),
        str_col("REPORTING_CATEGORY_CD", 10),
        date_col("EFFECTIVE_FROM_DT"),
        date_col("EFFECTIVE_TO_DT"),
    ]

    sql = """SELECT  t.TAX_RATE_CD,
        'SALESTAX'                  AS TAX_REGIME_CD,
        j.JURISDICTION_CD,
        j.JURISDICTION_LEVEL_CD,
        j.COUNTRY_CD,
        'NA'                        AS REGION_CD,
        t.RATE_PCT,
        t.RATE_TYPE_CD,
        'N'                         AS RECOVERABLE_FLG,
        t.REPORTING_CATEGORY_CD,
        t.EFFECTIVE_FROM_DT,
        t.EFFECTIVE_TO_DT
FROM    WWI_FIN.TAX_RATE t
        INNER JOIN WWI_FIN.TAX_JURISDICTION j
            ON j.JURISDICTION_CD = t.JURISDICTION_CD
WHERE   j.COUNTRY_CD IN ('USA', 'CAN', 'MEX')
UNION ALL
SELECT  t.TAX_RATE_CD,
        'VAT'                       AS TAX_REGIME_CD,
        j.JURISDICTION_CD,
        'COUNTRY'                   AS JURISDICTION_LEVEL_CD,
        j.COUNTRY_CD,
        'EU'                        AS REGION_CD,
        t.RATE_PCT,
        t.RATE_TYPE_CD,
        t.RECOVERABLE_FLG,
        t.REPORTING_CATEGORY_CD,
        t.EFFECTIVE_FROM_DT,
        t.EFFECTIVE_TO_DT
FROM    WWI_FIN.TAX_RATE t
        INNER JOIN WWI_FIN.TAX_JURISDICTION j
            ON j.JURISDICTION_CD = t.JURISDICTION_CD
WHERE   j.TAX_REGIME_CD = 'VAT'
UNION ALL
SELECT  t.TAX_RATE_CD,
        'GST'                       AS TAX_REGIME_CD,
        j.JURISDICTION_CD,
        'COUNTRY'                   AS JURISDICTION_LEVEL_CD,
        j.COUNTRY_CD,
        'APAC'                      AS REGION_CD,
        t.RATE_PCT,
        t.RATE_TYPE_CD,
        'Y'                         AS RECOVERABLE_FLG,
        t.REPORTING_CATEGORY_CD,
        t.EFFECTIVE_FROM_DT,
        t.EFFECTIVE_TO_DT
FROM    WWI_FIN.TAX_RATE t
        INNER JOIN WWI_FIN.TAX_JURISDICTION j
            ON j.JURISDICTION_CD = t.JURISDICTION_CD
WHERE   j.TAX_REGIME_CD = 'GST'"""

    df = DataFlow("Load Tax Rates")
    df.oledb_source("ORA TAX_RATE", CONN_ORACLE, sql, cols, timeout=600)
    df.derived_column(
        "Derive Rate Attributes",
        [
            (
                "RateBasisPoints",
                "(DT_I4)(RATE_PCT * 10000)",
                int_col("RateBasisPoints"),
            ),
            (
                "CurrentFlag",
                'ISNULL(EFFECTIVE_TO_DT) || EFFECTIVE_TO_DT >= GETDATE() ? "Y" : "N"',
                str_col("CurrentFlag", 1),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Tax Rates", "User::RowsRead")
    df.oledb_destination(
        "raw OracleTaxRate",
        CONN_STAGING,
        "raw.OracleTaxRate",
        batch_size=2000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables('@[User::ReferenceCadence] = "WEEKLY"'))
    start = pkg.add(log_package_start(pkg))
    trunc = pkg.add(truncate("raw.OracleTaxRate"))
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.OracleTaxRate"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, trunc, extract, rows, done)
    return pkg


def ext_ora_payment_terms():
    """Reference refresh of payment terms and their discount ladders."""
    pkg = new_package(
        "EXT_ORA_PaymentTerms",
        "Reference refresh of payment terms from V_PAYMENT_TERMS_EXTRACT, including "
        "the discount ladder (2/10 net 30 style) that the AP matching rules read.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("ReferenceCadence", "DAILY", "string")],
    )

    cols = [
        str_col("PAYMENT_TERMS_CD", 10),
        str_col("PAYMENT_TERMS_DESC", 120),
        Column("NET_DAYS", "i4"),
        Column("DISCOUNT_DAYS", "i4"),
        num_col("DISCOUNT_PCT", 9, 4),
        str_col("DUE_DATE_BASIS_CD", 8),
        str_col("PRORATE_FLG", 1),
        str_col("REGION_CD", 8),
        str_col("ACTIVE_FLG", 1),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  pt.PAYMENT_TERMS_CD,
        pt.PAYMENT_TERMS_DESC,
        pt.NET_DAYS,
        pt.DISCOUNT_DAYS,
        pt.DISCOUNT_PCT,
        pt.DUE_DATE_BASIS_CD,
        pt.PRORATE_FLG,
        pt.REGION_CD,
        pt.ACTIVE_FLG,
        pt.LAST_UPDATE_DT
FROM    WWI_FIN.V_PAYMENT_TERMS_EXTRACT pt
ORDER BY pt.PAYMENT_TERMS_CD"""

    df = DataFlow("Load Payment Terms")
    df.oledb_source("ORA V_PAYMENT_TERMS_EXTRACT", CONN_ORACLE, sql, cols, timeout=300)
    df.derived_column(
        "Derive Terms Attributes",
        [
            (
                "EarlyPayIncentiveFlag",
                'DISCOUNT_PCT > 0 && DISCOUNT_DAYS > 0 ? "Y" : "N"',
                str_col("EarlyPayIncentiveFlag", 1),
            ),
            (
                "EffectiveAnnualisedPct",
                "DISCOUNT_DAYS == NET_DAYS ? (DT_NUMERIC,9,4)0 : "
                "(DT_NUMERIC,9,4)(DISCOUNT_PCT * 365 / (NET_DAYS - DISCOUNT_DAYS))",
                num_col("EffectiveAnnualisedPct", 9, 4),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Payment Terms", "User::RowsRead")
    df.oledb_destination(
        "raw OraclePaymentTerms",
        CONN_STAGING,
        "raw.OraclePaymentTerms",
        batch_size=1000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables('@[User::ReferenceCadence] = "DAILY"'))
    start = pkg.add(log_package_start(pkg))
    trunc = pkg.add(truncate("raw.OraclePaymentTerms"))
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.OraclePaymentTerms"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, trunc, extract, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# WWI_REF - reference data
# ---------------------------------------------------------------------------


def ext_ora_currency():
    """Reference refresh of the currency master."""
    pkg = new_package(
        "EXT_ORA_Currency",
        "Reference refresh of the currency master from V_CURRENCY_EXTRACT. Carries "
        "minor-unit precision and the regional reporting currency each ledger rolls "
        "up to (USD for NA, EUR for EU, SGD for APAC).",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
    )

    cols = [
        str_col("CURRENCY_CD", 3),
        str_col("CURRENCY_NAME", 80),
        str_col("CURRENCY_SYMBOL", 8),
        Column("MINOR_UNIT_DIGITS", "i4"),
        str_col("ISO_NUMERIC_CD", 3),
        str_col("ACTIVE_FLG", 1),
        str_col("REPORTING_CURRENCY_CD", 3),
        str_col("REGION_CD", 8),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  c.CURRENCY_CD,
        c.CURRENCY_NAME,
        c.CURRENCY_SYMBOL,
        c.MINOR_UNIT_DIGITS,
        c.ISO_NUMERIC_CD,
        c.ACTIVE_FLG,
        CASE c.PRIMARY_REGION_CD
            WHEN 'NA'   THEN 'USD'
            WHEN 'EU'   THEN 'EUR'
            WHEN 'APAC' THEN 'SGD'
            ELSE 'USD'
        END                     AS REPORTING_CURRENCY_CD,
        c.PRIMARY_REGION_CD     AS REGION_CD,
        c.LAST_UPDATE_DT
FROM    WWI_REF.V_CURRENCY_EXTRACT c"""

    df = DataFlow("Load Currencies")
    df.oledb_source("ORA V_CURRENCY_EXTRACT", CONN_ORACLE, sql, cols, timeout=300)
    df.derived_column("Add Audit Columns", audit_derivations(SRC_GLOBAL))
    df.row_count("Count Currencies", "User::RowsRead")
    df.oledb_destination(
        "raw OracleCurrency",
        CONN_STAGING,
        "raw.OracleCurrency",
        batch_size=500,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    trunc = pkg.add(truncate("raw.OracleCurrency"))
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.OracleCurrency"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, trunc, extract, rows, done)
    return pkg


def ext_ora_fx_rate_daily():
    """Date-window FX extract that also derives the cross rates the EU and APAC
    ledgers report in."""
    pkg = new_package(
        "EXT_ORA_FxRateDaily",
        "Date-window FX rate extract. Spot, corporate and period-average rates are "
        "pulled for the requested rate-date window; EUR and SGD cross rates are "
        "triangulated through USD because the ERP only publishes USD pairs for the "
        "minor currencies.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("MissingRateCount", 0, "int")],
    )

    cols = [
        date_col("RATE_DT"),
        str_col("FROM_CURRENCY_CD", 3),
        str_col("TO_CURRENCY_CD", 3),
        str_col("RATE_TYPE_CD", 8),
        num_col("RATE", 18, 8),
        num_col("INVERSE_RATE", 18, 8),
        str_col("RATE_SOURCE_CD", 10),
        str_col("REGION_CD", 8),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  fx.RATE_DT,
        fx.FROM_CURRENCY_CD,
        fx.TO_CURRENCY_CD,
        fx.RATE_TYPE_CD,
        fx.RATE,
        CASE WHEN fx.RATE = 0 THEN 0 ELSE 1 / fx.RATE END   AS INVERSE_RATE,
        fx.RATE_SOURCE_CD,
        'GLOBAL'                                            AS REGION_CD,
        fx.LAST_UPDATE_DT
FROM    WWI_REF.FX_RATE_DAILY fx
WHERE   fx.RATE_DT >= TO_DATE(?, 'YYYY-MM-DD')
  AND   fx.RATE_DT <  TO_DATE(?, 'YYYY-MM-DD')
  AND   fx.RATE_TYPE_CD IN ('SPOT', 'CORP', 'AVG')
UNION ALL
SELECT  usd_from.RATE_DT,
        usd_from.FROM_CURRENCY_CD,
        usd_to.FROM_CURRENCY_CD                             AS TO_CURRENCY_CD,
        usd_from.RATE_TYPE_CD,
        usd_from.RATE / NULLIF(usd_to.RATE, 0)              AS RATE,
        usd_to.RATE / NULLIF(usd_from.RATE, 0)              AS INVERSE_RATE,
        'TRIANG'                                            AS RATE_SOURCE_CD,
        CASE usd_to.FROM_CURRENCY_CD
            WHEN 'EUR' THEN 'EU'
            WHEN 'SGD' THEN 'APAC'
            ELSE 'GLOBAL'
        END                                                 AS REGION_CD,
        usd_from.LAST_UPDATE_DT
FROM    WWI_REF.FX_RATE_DAILY usd_from
        INNER JOIN WWI_REF.FX_RATE_DAILY usd_to
            ON usd_to.RATE_DT = usd_from.RATE_DT
           AND usd_to.RATE_TYPE_CD = usd_from.RATE_TYPE_CD
           AND usd_to.TO_CURRENCY_CD = 'USD'
           AND usd_to.FROM_CURRENCY_CD IN ('EUR', 'SGD')
WHERE   usd_from.TO_CURRENCY_CD = 'USD'
  AND   usd_from.RATE_DT >= TO_DATE(?, 'YYYY-MM-DD')
  AND   usd_from.RATE_DT <  TO_DATE(?, 'YYYY-MM-DD')
  AND   usd_from.FROM_CURRENCY_CD <> usd_to.FROM_CURRENCY_CD"""

    df = DataFlow("Extract FX Rates")
    df.oledb_source("ORA FX_RATE_DAILY", CONN_ORACLE, sql, cols, timeout=1200)
    df.derived_column(
        "Derive Rate Key",
        [
            (
                "RatePairCd",
                'FROM_CURRENCY_CD + "/" + TO_CURRENCY_CD',
                str_col("RatePairCd", 8),
            )
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Rates", "User::RowsRead")
    df.oledb_destination(
        "raw OracleFxRate",
        CONN_STAGING,
        "raw.OracleFxRate",
        batch_size=20000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="ORA FX_RATE_DAILY",
    )

    init = pkg.add(init_variables("@[User::MissingRateCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    wm = pkg.add(get_watermark(object_name="WWI_REF.FX_RATE_DAILY"))
    clear_window = pkg.add(
        ExecuteSql(
            "Clear Rate Window",
            CONN_STAGING,
            "DELETE FROM raw.OracleFxRate WHERE RateDate >= CAST(? AS date) AND RateDate < CAST(? AS date);",
            parameter_bindings=[
                ("User::WatermarkFrom", 0, "NVARCHAR"),
                ("User::WatermarkTo", 1, "NVARCHAR"),
            ],
        )
    )
    extract = pkg.add(DataFlowTask(df))
    check = pkg.add(
        ExecuteSql(
            "Count Missing Rate Days",
            CONN_STAGING,
            "SELECT COUNT(*) AS MissingDays FROM etl.Configuration AS c "
            "WHERE c.ConfigurationKey = N'FxMandatoryPairs' "
            "AND NOT EXISTS (SELECT 1 FROM raw.OracleFxRate AS r WHERE r.RatePairCode = c.ConfigurationValue);",
            result_type="ResultSetType_SingleRow",
            result_bindings=[("0", "User::MissingRateCount")],
        )
    )
    setwm = pkg.add(set_watermark("WWI_REF.FX_RATE_DAILY"))
    rows = pkg.add(log_row_count("raw.OracleFxRate"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, wm, clear_window, extract, check, setwm, rows, done)
    return pkg


def ext_ora_geography():
    """Full reload of the geography reference with regional postal standards."""
    pkg = new_package(
        "EXT_ORA_Geography",
        "Full truncate-and-load of the geography reference from V_GEOGRAPHY_EXTRACT. "
        "Postal formatting differs by region (ZIP+4 in NA, outward/inward split in the "
        "UK, prefecture and postal district in APAC) and is normalised here so every "
        "downstream address lookup uses one shape.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
    )

    cols = [
        int_col("GeographyKey"),
        str_col("COUNTRY_CD", 3),
        str_col("COUNTRY_NAME", 80),
        str_col("REGION_CD", 8),
        str_col("SUBREGION_CD", 8),
        str_col("STATE_PROVINCE_CD", 8),
        str_col("STATE_PROVINCE_NAME", 80),
        str_col("CITY_NAME", 80),
        str_col("POSTAL_CD", 16),
        str_col("POSTAL_FORMAT_CD", 12),
        str_col("TIME_ZONE_CD", 40),
        num_col("LATITUDE", 9, 6),
        num_col("LONGITUDE", 9, 6),
    ]

    sql = """SELECT  g.GEOGRAPHY_KEY                    AS GeographyKey,
        g.COUNTRY_CD,
        g.COUNTRY_NAME,
        g.REGION_CD,
        g.SUBREGION_CD,
        g.STATE_PROVINCE_CD,
        g.STATE_PROVINCE_NAME,
        g.CITY_NAME,
        CASE g.REGION_CD
            WHEN 'NA'   THEN REGEXP_REPLACE(g.POSTAL_CD, '^([0-9]{5})([0-9]{4})$', '\\1-\\2')
            WHEN 'EU'   THEN UPPER(REPLACE(g.POSTAL_CD, ' ', ''))
            ELSE UPPER(TRIM(g.POSTAL_CD))
        END                                 AS POSTAL_CD,
        CASE g.REGION_CD
            WHEN 'NA'   THEN 'ZIP5_PLUS4'
            WHEN 'EU'   THEN 'ALPHANUM'
            WHEN 'APAC' THEN 'NUMERIC6'
            ELSE 'FREEFORM'
        END                                 AS POSTAL_FORMAT_CD,
        g.TIME_ZONE_CD,
        g.LATITUDE,
        g.LONGITUDE
FROM    WWI_REF.V_GEOGRAPHY_EXTRACT g
WHERE   g.ACTIVE_FLG = 'Y'"""

    df = DataFlow("Load Geography")
    df.oledb_source("ORA V_GEOGRAPHY_EXTRACT", CONN_ORACLE, sql, cols, timeout=1800)
    df.derived_column(
        "Derive Postal Keys",
        [
            (
                "PostalCode",
                "UPPER(TRIM(POSTAL_CD))",
                str_col("PostalCode", 16),
            ),
            (
                "CountryCode",
                "UPPER(COUNTRY_CD)",
                str_col("CountryCode", 3),
            ),
        ]
        + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Geography Rows", "User::RowsRead")
    df.oledb_destination(
        "raw OracleGeography",
        CONN_STAGING,
        "raw.OracleGeography",
        batch_size=50000,
        error_disposition="RedirectRow",
    )
    df.reject_destination(
        "err RejectedConstraintViolation",
        CONN_STAGING,
        "err.RejectedConstraintViolation",
        from_component="ORA V_GEOGRAPHY_EXTRACT",
    )

    init = pkg.add(init_variables("@[User::RowsRejected] = 0"))
    start = pkg.add(log_package_start(pkg))
    trunc = pkg.add(truncate("raw.OracleGeography"))
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.OracleGeography"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, trunc, extract, rows, done)
    return pkg


def ext_ora_code_translation():
    """Reference refresh of the cross-system code translation table."""
    pkg = new_package(
        "EXT_ORA_CodeTranslation",
        "Reference refresh of WWI_REF.CODE_TRANSLATION - the cryptic legacy code map "
        "(status, reason, incoterm, payment method) that every downstream package "
        "joins to. Region-specific code sets are kept distinct rather than merged.",
        source_system=SRC_GLOBAL,
        connections=(CONN_ORACLE, CONN_STAGING),
        extra_variables=[("ReferenceCadence", "DAILY", "string")],
    )

    cols = [
        str_col("CODE_SET_CD", 20),
        str_col("SOURCE_SYSTEM_CD", 12),
        str_col("SOURCE_CODE", 20),
        str_col("TARGET_CODE", 20),
        str_col("CODE_DESC", 120),
        str_col("REGION_CD", 8),
        Column("SORT_ORDER", "i4"),
        str_col("ACTIVE_FLG", 1),
        date_col("EFFECTIVE_FROM_DT"),
        date_col("LAST_UPDATE_DT"),
    ]

    sql = """SELECT  ct.CODE_SET_CD,
        ct.SOURCE_SYSTEM_CD,
        ct.SOURCE_CODE,
        WWI_REF.FN_TRANSLATE_CODE(ct.CODE_SET_CD, ct.SOURCE_CODE, ct.REGION_CD) AS TARGET_CODE,
        ct.CODE_DESC,
        ct.REGION_CD,
        ct.SORT_ORDER,
        ct.ACTIVE_FLG,
        ct.EFFECTIVE_FROM_DT,
        ct.LAST_UPDATE_DT
FROM    WWI_REF.CODE_TRANSLATION ct
WHERE   ct.CODE_SET_CD IN ('CUST_STATUS', 'PO_STATUS', 'REASON', 'INCOTERM', 'PAY_METHOD')
ORDER BY ct.CODE_SET_CD, ct.REGION_CD, ct.SORT_ORDER"""

    df = DataFlow("Load Code Translations")
    df.oledb_source("ORA CODE_TRANSLATION", CONN_ORACLE, sql, cols, timeout=300)
    df.derived_column(
        "Add Reference Audit",
        [("RecordKind", '"CODEXREF"', str_col("RecordKind", 12))] + audit_derivations(SRC_GLOBAL),
    )
    df.row_count("Count Code Rows", "User::RowsRead")
    df.oledb_destination(
        "raw OracleCustomerMaster CodeXref",
        CONN_STAGING,
        "raw.OracleCustomerMaster",
        batch_size=5000,
        error_disposition="FailComponent",
    )

    init = pkg.add(init_variables('@[User::ReferenceCadence] = "DAILY"'))
    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(
        ExecuteSql(
            "Delete Code Rows",
            CONN_STAGING,
            "DELETE FROM raw.OracleCustomerMaster WHERE RecordKind = N'CODEXREF';",
        )
    )
    extract = pkg.add(DataFlowTask(df))
    rows = pkg.add(log_row_count("raw.OracleCustomerMaster"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, clear, extract, rows, done)
    return pkg


PACKAGE_BUILDERS = [
    ext_ora_customer_master,
    ext_ora_customer_address,
    ext_ora_supplier_master,
    ext_ora_product_master,
    ext_ora_product_hierarchy,
    ext_ora_purchase_order_hdr,
    ext_ora_purchase_order_line,
    ext_ora_receipt_line,
    ext_ora_vendor_contract,
    ext_ora_ap_invoice_hdr,
    ext_ora_ap_invoice_line,
    ext_ora_ap_payment,
    ext_ora_ap_payment_apply,
    ext_ora_ap_aging,
    ext_ora_gl_journal_line,
    ext_ora_cost_center,
    ext_ora_tax_rate,
    ext_ora_payment_terms,
    ext_ora_currency,
    ext_ora_fx_rate_daily,
    ext_ora_geography,
    ext_ora_code_translation,
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
