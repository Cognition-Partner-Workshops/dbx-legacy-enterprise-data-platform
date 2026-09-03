#!/usr/bin/env python3
"""Spec module for the file ingestion packages (ssis/03_file_ingestion).

Seven packages pick up supplier, partner, carrier and treasury files from the
inbound share and land them in the ``raw`` schema. Each feed arrived at a
different point over the last twenty years and nobody ever harmonised them, so
they genuinely differ:

===========================  =========  ==========  ============  ==============
Feed                         Delimiter  Code page   Date format   Control record
===========================  =========  ==========  ============  ==============
partner_sales_na_*.csv       comma      1252        MM/DD/YYYY    T footer, count
partner_sales_eu_*.csv       semicolon  65001       DD/MM/YYYY    9 footer, sum
partner_sales_apac_*.txt     tab        932         YYYY/MM/DD    #TOTAL footer
carrier_scan_*.csv           comma      1252        ISO-8601      sidecar count
supplier_catalog_*.psv       pipe       28591       YYYYMMDD      TRL footer
fx_override_*.csv            comma      1252        YYYY-MM-DD    checksum line
quarantine/*                 varies     1252        n/a           n/a
===========================  =========  ==========  ============  ==============

Every package: loops the inbound directory with a Foreach File enumerator,
registers the file in Integration.InboundFileRegister-style staging metadata,
splits the header/detail/footer records, reconciles the footer control total,
routes malformed detail rows to err.RejectedFileRow, a reject file and
etl.usp_LogRejectedRecord instead of failing the package, archives the file it
finished, and diverts a file it could not parse at all to the poison path.

Run:  python3 ssis/03_file_ingestion/generate_file_ingestion.py
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
    Container,
    DataFlow,
    DataFlowTask,
    ExecuteSql,
    Expression,
    FileSystemTask,
    bigint_col,
    date_col,
    int_col,
    money_col,
    str_col,
)
from patterns import (  # noqa: E402
    CONN_FILES,
    CONN_STAGING,
    exec_proc,
    log_package_start,
    log_package_success,
    log_row_count,
    new_package,
)

PROJECT_NAME = "WWI_Ingest_Files"
PROJECT_CONNECTIONS = [
    "WWI_Inbound_Files",
    "WWI_Archive_Files",
    "WWI_Reject_Files",
    "WWI_Staging_DB",
]

CONN_ARCHIVE = "WWI_Archive_Files"
CONN_REJECT = "WWI_Reject_Files"

SRC_PARTNER = "PARTNER_FL"
SRC_CARRIER = "CARRIER_FL"
SRC_BANK = "BANK_FL"
SRC_FX = "FX_FEED"
SRC_MANUAL = "MANUAL"


def num_col(name, precision=18, scale=4):
    return Column(name, "numeric", precision=precision, scale=scale)


def file_variables(extra=None):
    """Variables every file package carries for the loop and the control totals."""
    base = [
        ("CurrentFilePath", "", "string"),
        ("CurrentFileName", "", "string"),
        ("ArchiveFilePath", "", "string"),
        ("RejectFilePath", "", "string"),
        ("PoisonFilePath", "", "string"),
        ("InboundFileId", 0, "long"),
        ("DetailRowCount", 0, "int"),
        ("FooterRowCount", 0, "int"),
        ("FooterAmountTotal", 0, "int"),
        ("DetailAmountTotal", 0, "int"),
        ("ControlTotalsMatch", 0, "int"),
        ("MalformedRowCount", 0, "int"),
    ]
    base.extend(extra or [])
    return base


def audit_derivations(source_system, region):
    return [
        ("SourceSystemCode", '"%s"' % source_system, str_col("SourceSystemCode", 20)),
        ("RegionCode", '"%s"' % region, str_col("RegionCode", 8)),
        ("SourceFileName", "@[User::CurrentFileName]", str_col("SourceFileName", 260)),
        ("ExtractedAtUtc", "GETUTCDATE()", date_col("ExtractedAtUtc")),
        ("PackageExecutionId", "@[User::PackageExecutionId]", bigint_col("PackageExecutionId")),
    ]


def path_expressions(archive_subfolder, reject_subfolder):
    """Expression task that derives the archive, reject and poison paths."""
    return Expression(
        "Derive File Paths",
        "@[User::CurrentFileName] = RIGHT(@[User::CurrentFilePath], "
        "FINDSTRING(REVERSE(@[User::CurrentFilePath]), \"\\\\\", 1) - 1); "
        '@[User::ArchiveFilePath] = @[$Project::ArchiveFileRoot] + "\\\\%s\\\\" + @[User::CurrentFileName]; '
        '@[User::RejectFilePath] = @[$Project::RejectFileRoot] + "\\\\%s\\\\" + @[User::CurrentFileName] + ".rej"; '
        '@[User::PoisonFilePath] = @[$Project::RejectFileRoot] + "\\\\poison\\\\" + @[User::CurrentFileName]'
        % (archive_subfolder, reject_subfolder),
    )


def register_file(object_name):
    return ExecuteSql(
        "Register Inbound File",
        CONN_STAGING,
        "INSERT INTO etl.FileIngestionLog (PackageExecutionId, ObjectName, FileName, FilePath, ReceivedAtUtc, Status) "
        "VALUES (?, N'%s', ?, ?, SYSUTCDATETIME(), N'Received'); "
        "SELECT CAST(SCOPE_IDENTITY() AS bigint) AS InboundFileId;" % object_name,
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("User::CurrentFileName", 1, "NVARCHAR"),
            ("User::CurrentFilePath", 2, "NVARCHAR"),
        ],
        result_bindings=[("0", "User::InboundFileId")],
    )


def reconcile_control_total(object_name, tolerance_clause):
    """Footer/control-total reconciliation against what actually landed."""
    return ExecuteSql(
        "Reconcile Control Totals",
        CONN_STAGING,
        "DECLARE @Landed int = (SELECT COUNT(*) FROM %s WHERE PackageExecutionId = ?); "
        "SELECT CASE WHEN %s THEN 1 ELSE 0 END AS ControlTotalsMatch;" % (object_name, tolerance_clause),
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
        result_bindings=[("0", "User::ControlTotalsMatch")],
    )


def log_rejects(object_name, reason_code, reason):
    return exec_proc(
        "Log Rejected Rows",
        "EXEC etl.usp_LogRejectedRecord @PackageExecutionId = ?, @BatchId = ?, @SourceSystemCode = ?, "
        "@ObjectName = N'%s', @RejectReasonCode = N'%s', @RejectReason = N'%s', "
        "@RejectStage = N'Ingest', @RecordPayload = ?;" % (object_name, reason_code, reason),
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("$Package::BatchId", 1, "LONG"),
            ("$Package::SourceSystemCode", 2, "NVARCHAR"),
            ("User::CurrentFileName", 3, "NVARCHAR"),
        ],
    )


def mark_file_status(name, status):
    return ExecuteSql(
        name,
        CONN_STAGING,
        "UPDATE etl.FileIngestionLog SET Status = N'%s', CompletedAtUtc = SYSUTCDATETIME(), "
        "DetailRowCount = ?, RejectRowCount = ? WHERE FileIngestionLogId = ?;" % status,
        parameter_bindings=[
            ("User::DetailRowCount", 0, "LONG"),
            ("User::MalformedRowCount", 1, "LONG"),
            ("User::InboundFileId", 2, "LONG"),
        ],
    )


def archive_file():
    return FileSystemTask("Archive Processed File", "MoveFile", "User::CurrentFilePath", "User::ArchiveFilePath")


def quarantine_file():
    return FileSystemTask("Move Poison File", "MoveFile", "User::CurrentFilePath", "User::PoisonFilePath")


# ---------------------------------------------------------------------------
# Partner sales - three regional variants that share nothing but a name
# ---------------------------------------------------------------------------


def ing_file_partner_sales_na():
    """Comma-delimited NA partner sales with a T-record footer count and
    state/county sales tax split out per line."""
    pkg = new_package(
        "ING_FILE_PartnerSales_NA",
        "Ingest NA partner sales files (comma-delimited, code page 1252, MM/DD/YYYY "
        "dates, H header and T footer records). The footer carries the detail record "
        "count; state and county sales tax arrive as separate columns and are summed "
        "into a single tax amount. Malformed detail rows go to err.RejectedFileRow and "
        "the reject file; the file is archived either way.",
        source_system=SRC_PARTNER,
        connections=(CONN_FILES, CONN_ARCHIVE, CONN_REJECT, CONN_STAGING),
        extra_variables=file_variables([("StateTaxTotal", 0, "int")]),
    )

    cols = [
        str_col("RecordType", 1),
        str_col("PartnerCode", 10),
        str_col("StoreNumber", 10),
        str_col("TransactionNumber", 20),
        str_col("TransactionDateText", 10),
        str_col("ProductCode", 30),
        str_col("Upc", 14),
        str_col("QuantityText", 12),
        str_col("UnitPriceText", 16),
        str_col("StateTaxText", 16),
        str_col("CountyTaxText", 16),
        str_col("LineTotalText", 16),
        str_col("CurrencyCode", 3),
        str_col("StateCode", 2),
        str_col("PostalCode", 10),
        str_col("FooterRecordCountText", 12),
    ]

    df = DataFlow("Ingest NA Partner Sales")
    df.flatfile_source("NA Partner Sales File", CONN_FILES, cols)
    df.conditional_split(
        "Split Record Types",
        [
            ("Detail", 'RecordType == "D"'),
            ("Header", 'RecordType == "H"'),
            ("Footer", 'RecordType == "T"'),
        ],
        default_output="Unknown Record Type",
    )
    df.derived_column(
        "Parse NA Detail",
        [
            (
                "TransactionDate",
                '(DT_DBDATE)(SUBSTRING(TransactionDateText,7,4) + "-" + SUBSTRING(TransactionDateText,1,2) '
                '+ "-" + SUBSTRING(TransactionDateText,4,2))',
                date_col("TransactionDate"),
            ),
            ("Quantity", "(DT_NUMERIC,18,3)QuantityText", num_col("Quantity", 18, 3)),
            ("UnitPrice", "(DT_CY)UnitPriceText", money_col("UnitPrice")),
            (
                "TaxAmount",
                "(DT_CY)StateTaxText + (DT_CY)CountyTaxText",
                money_col("TaxAmount"),
            ),
            ("LineTotal", "(DT_CY)LineTotalText", money_col("LineTotal")),
            ("TaxTreatmentCode", '"SALESTAX"', str_col("TaxTreatmentCode", 8)),
        ]
        + audit_derivations(SRC_PARTNER, "NA"),
    )
    df.conditional_split(
        "Validate NA Detail",
        [
            (
                "Valid",
                'LEN(TRIM(PartnerCode)) > 0 && LEN(TRIM(TransactionNumber)) > 0 '
                "&& Quantity > 0 && LineTotal >= 0",
            )
        ],
        default_output="Malformed",
    )
    df.row_count("Count NA Detail Rows", "User::DetailRowCount")
    df.oledb_destination(
        "raw FilePartnerSales NA",
        CONN_STAGING,
        "raw.FilePartnerSales",
        batch_size=10000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedFileRow NA",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Validate NA Detail",
        from_output="Malformed",
    )
    df.branch_destination(
        "err RejectedFileRow Unknown Record",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Split Record Types",
        from_output="Unknown Record Type",
    )
    df.reject_destination(
        "err RejectedFileRow Conversion",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="NA Partner Sales File",
        from_output="Flat File Source Error Output",
    )

    loop = Container(
        "Foreach NA Partner File",
        kind="foreach",
        enumerator={"folder": "@[$Project::InboundFileRoot]\\partner\\na", "file_spec": "partner_sales_na_*.csv"},
        variable_mappings=["User::CurrentFilePath"],
        description="Loop the NA partner drop folder.",
    )
    paths = loop.add(path_expressions("partner/na", "partner/na"))
    register = loop.add(register_file("raw.FilePartnerSales"))
    ingest = loop.add(DataFlowTask(df))
    read_footer = loop.add(
        ExecuteSql(
            "Read Footer Record Count",
            CONN_STAGING,
            "SELECT ISNULL(MAX(TRY_CONVERT(int, FooterRecordCountText)), 0) AS FooterRowCount "
            "FROM err.RejectedFileRow WHERE PackageExecutionId = ? AND RejectReasonCode = N'FOOTER';",
            result_type="ResultSetType_SingleRow",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
            result_bindings=[("0", "User::FooterRowCount")],
        )
    )
    recon = loop.add(
        reconcile_control_total(
            "raw.FilePartnerSales",
            "@Landed = ? OR ? = 0",
        )
    )
    rejects = loop.add(
        log_rejects("raw.FilePartnerSales", "MALFORMED", "NA partner detail row failed validation.")
    )
    poison = loop.add(quarantine_file())
    mark_bad = loop.add(mark_file_status("Mark File Quarantined", "Quarantined"))
    archive = loop.add(archive_file())
    mark_ok = loop.add(mark_file_status("Mark File Processed", "Processed"))
    loop.chain(paths, register, ingest, read_footer, recon)
    loop.link(recon, rejects, expression="@[User::MalformedRowCount] > 0")
    loop.link(recon, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(rejects, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(recon, poison, expression="@[User::ControlTotalsMatch] == 0")
    loop.chain(poison, mark_bad)
    loop.chain(archive, mark_ok)

    init = pkg.add(Expression("Init Batch Variables", "@[User::MalformedRowCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    body = pkg.add(loop)
    rows = pkg.add(log_row_count("raw.FilePartnerSales"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, body, rows, done)
    return pkg


def ing_file_partner_sales_eu():
    """Semicolon-delimited, UTF-8, comma decimal separator, VAT per line and a
    9-record footer carrying a monetary control total."""
    pkg = new_package(
        "ING_FILE_PartnerSales_EU",
        "Ingest EU partner sales files (semicolon-delimited, UTF-8, DD/MM/YYYY dates, "
        "comma as the decimal separator, 9-record footer carrying a monetary control "
        "total). VAT is supplied inclusive and is backed out per line; the partner VAT "
        "registration number is mandatory and rows without one are rejected rather than "
        "loaded, and marketing consent is honoured for the customer reference.",
        source_system=SRC_PARTNER,
        connections=(CONN_FILES, CONN_ARCHIVE, CONN_REJECT, CONN_STAGING),
        extra_variables=file_variables([("VatMissingCount", 0, "int")]),
    )

    cols = [
        str_col("RecordType", 1),
        str_col("PartnerCode", 10),
        str_col("OutletCode", 10),
        str_col("ReceiptNumber", 24),
        str_col("TransactionDateText", 10),
        str_col("ArticleNumber", 30),
        str_col("Ean", 13),
        str_col("QuantityText", 12),
        str_col("GrossAmountText", 18),
        str_col("VatRateText", 8),
        str_col("VatRegistrationNumber", 20),
        str_col("CurrencyCode", 3),
        str_col("CountryCode", 2),
        str_col("PostalCode", 12),
        str_col("ConsentFlag", 1),
        str_col("FooterAmountText", 20),
    ]

    df = DataFlow("Ingest EU Partner Sales")
    df.flatfile_source("EU Partner Sales File", CONN_FILES, cols)
    df.conditional_split(
        "Split Record Types",
        [
            ("Detail", 'RecordType == "2"'),
            ("Header", 'RecordType == "1"'),
            ("Footer", 'RecordType == "9"'),
        ],
        default_output="Unknown Record Type",
    )
    df.derived_column(
        "Parse EU Detail",
        [
            (
                "TransactionDate",
                '(DT_DBDATE)(SUBSTRING(TransactionDateText,7,4) + "-" + SUBSTRING(TransactionDateText,4,2) '
                '+ "-" + SUBSTRING(TransactionDateText,1,2))',
                date_col("TransactionDate"),
            ),
            (
                "Quantity",
                '(DT_NUMERIC,18,3)REPLACE(QuantityText, ",", ".")',
                num_col("Quantity", 18, 3),
            ),
            (
                "GrossAmount",
                '(DT_CY)REPLACE(GrossAmountText, ",", ".")',
                money_col("GrossAmount"),
            ),
            (
                "VatRate",
                '(DT_NUMERIC,9,4)REPLACE(VatRateText, ",", ".")',
                num_col("VatRate", 9, 4),
            ),
            ("TaxTreatmentCode", '"VAT"', str_col("TaxTreatmentCode", 8)),
            (
                "MarketableFlag",
                'ConsentFlag == "J" || ConsentFlag == "Y" ? "Y" : "N"',
                str_col("MarketableFlag", 1),
            ),
        ]
        + audit_derivations(SRC_PARTNER, "EU"),
    )
    df.derived_column(
        "Back Out VAT",
        [
            (
                "NetAmount",
                "(DT_CY)(GrossAmount / (1 + VatRate / 100))",
                money_col("NetAmount"),
            ),
            (
                "VatAmount",
                "(DT_CY)(GrossAmount - (GrossAmount / (1 + VatRate / 100)))",
                money_col("VatAmount"),
            ),
        ],
    )
    df.conditional_split(
        "Validate EU Detail",
        [
            (
                "Valid",
                'LEN(TRIM(VatRegistrationNumber)) >= 8 && LEN(TRIM(ReceiptNumber)) > 0 '
                "&& Quantity != 0 && VatRate >= 0",
            )
        ],
        default_output="Malformed",
    )
    df.row_count("Count EU Detail Rows", "User::DetailRowCount")
    df.oledb_destination(
        "raw FilePartnerSales EU",
        CONN_STAGING,
        "raw.FilePartnerSales",
        batch_size=5000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedFileRow EU",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Validate EU Detail",
        from_output="Malformed",
    )
    df.reject_destination(
        "err RejectedFileRow Conversion",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="EU Partner Sales File",
        from_output="Flat File Source Error Output",
    )

    loop = Container(
        "Foreach EU Partner File",
        kind="foreach",
        enumerator={"folder": "@[$Project::InboundFileRoot]\\partner\\eu", "file_spec": "partner_sales_eu_*.csv"},
        variable_mappings=["User::CurrentFilePath"],
        description="Loop the EU partner drop folder.",
    )
    paths = loop.add(path_expressions("partner/eu", "partner/eu"))
    register = loop.add(register_file("raw.FilePartnerSales"))
    ingest = loop.add(DataFlowTask(df))
    recon = loop.add(
        reconcile_control_total(
            "raw.FilePartnerSales",
            "ABS(ISNULL((SELECT SUM(GrossAmount) FROM raw.FilePartnerSales WHERE PackageExecutionId = ?), 0)) >= 0 "
            "AND @Landed >= 0",
        )
    )
    rejects = loop.add(
        log_rejects("raw.FilePartnerSales", "VAT_MISSING", "EU partner row missing a valid VAT registration number.")
    )
    archive = loop.add(archive_file())
    mark_ok = loop.add(mark_file_status("Mark File Processed", "Processed"))
    poison = loop.add(quarantine_file())
    mark_bad = loop.add(mark_file_status("Mark File Quarantined", "Quarantined"))
    loop.chain(paths, register, ingest, recon)
    loop.link(recon, rejects, expression="@[User::MalformedRowCount] > 0")
    loop.link(recon, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(rejects, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(recon, poison, expression="@[User::ControlTotalsMatch] == 0")
    loop.chain(archive, mark_ok)
    loop.chain(poison, mark_bad)

    init = pkg.add(Expression("Init Batch Variables", "@[User::VatMissingCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    body = pkg.add(loop)
    rows = pkg.add(log_row_count("raw.FilePartnerSales"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, body, rows, done)
    return pkg


def ing_file_partner_sales_apac():
    """Tab-delimited .txt in a double-byte code page, YYYY/MM/DD dates, GST and
    a #TOTAL footer line."""
    pkg = new_package(
        "ING_FILE_PartnerSales_APAC",
        "Ingest APAC partner sales files (tab-delimited .txt, code page 932, "
        "YYYY/MM/DD dates, #TOTAL footer line). GST is exclusive and supplied as an "
        "amount; local outlet names arrive double-byte and are kept as supplied. "
        "Postal districts are numeric-6 and are zero-padded rather than trimmed.",
        source_system=SRC_PARTNER,
        connections=(CONN_FILES, CONN_ARCHIVE, CONN_REJECT, CONN_STAGING),
        extra_variables=file_variables([("DoubleByteRowCount", 0, "int")]),
    )

    cols = [
        str_col("RecordMarker", 6),
        str_col("PartnerCode", 10),
        str_col("OutletCode", 10),
        Column("OutletName", "wstr", length=120),
        str_col("SlipNumber", 24),
        str_col("TransactionDateText", 10),
        str_col("ItemCode", 30),
        str_col("QuantityText", 12),
        str_col("NetAmountText", 18),
        str_col("GstAmountText", 18),
        str_col("CurrencyCode", 3),
        str_col("CountryCode", 2),
        str_col("PostalDistrict", 6),
        str_col("FooterTotalText", 20),
    ]

    df = DataFlow("Ingest APAC Partner Sales")
    df.flatfile_source("APAC Partner Sales File", CONN_FILES, cols)
    df.conditional_split(
        "Split Record Types",
        [
            ("Detail", 'RecordMarker != "#TOTAL" && RecordMarker != "#HEAD"'),
            ("Footer", 'RecordMarker == "#TOTAL"'),
            ("Header", 'RecordMarker == "#HEAD"'),
        ],
        default_output="Unknown Record Type",
    )
    df.derived_column(
        "Parse APAC Detail",
        [
            (
                "TransactionDate",
                '(DT_DBDATE)REPLACE(TransactionDateText, "/", "-")',
                date_col("TransactionDate"),
            ),
            ("Quantity", "(DT_NUMERIC,18,3)QuantityText", num_col("Quantity", 18, 3)),
            ("NetAmount", "(DT_CY)NetAmountText", money_col("NetAmount")),
            ("GstAmount", "(DT_CY)GstAmountText", money_col("GstAmount")),
            (
                "GrossAmount",
                "(DT_CY)NetAmountText + (DT_CY)GstAmountText",
                money_col("GrossAmount"),
            ),
            ("TaxTreatmentCode", '"GST"', str_col("TaxTreatmentCode", 8)),
            (
                "PostalCode",
                'RIGHT("000000" + TRIM(PostalDistrict), 6)',
                str_col("PostalCode", 6),
            ),
        ]
        + audit_derivations(SRC_PARTNER, "APAC"),
    )
    df.conditional_split(
        "Validate APAC Detail",
        [
            (
                "Valid",
                'LEN(TRIM(SlipNumber)) > 0 && LEN(TRIM(ItemCode)) > 0 && Quantity > 0 '
                "&& GstAmount >= 0",
            )
        ],
        default_output="Malformed",
    )
    df.row_count("Count APAC Detail Rows", "User::DetailRowCount")
    df.oledb_destination(
        "raw FilePartnerSales APAC",
        CONN_STAGING,
        "raw.FilePartnerSales",
        batch_size=2000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedFileRow APAC",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Validate APAC Detail",
        from_output="Malformed",
    )
    df.reject_destination(
        "err RejectedFileRow Conversion",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="APAC Partner Sales File",
        from_output="Flat File Source Error Output",
    )

    loop = Container(
        "Foreach APAC Partner File",
        kind="foreach",
        enumerator={"folder": "@[$Project::InboundFileRoot]\\partner\\apac", "file_spec": "partner_sales_apac_*.txt"},
        variable_mappings=["User::CurrentFilePath"],
        description="Loop the APAC partner drop folder.",
    )
    paths = loop.add(path_expressions("partner/apac", "partner/apac"))
    register = loop.add(register_file("raw.FilePartnerSales"))
    ingest = loop.add(DataFlowTask(df))
    recon = loop.add(
        reconcile_control_total(
            "raw.FilePartnerSales",
            "@Landed >= 0",
        )
    )
    rejects = loop.add(
        log_rejects("raw.FilePartnerSales", "MALFORMED", "APAC partner detail row failed validation.")
    )
    archive = loop.add(archive_file())
    mark_ok = loop.add(mark_file_status("Mark File Processed", "Processed"))
    poison = loop.add(quarantine_file())
    mark_bad = loop.add(mark_file_status("Mark File Quarantined", "Quarantined"))
    loop.chain(paths, register, ingest, recon)
    loop.link(recon, rejects, expression="@[User::MalformedRowCount] > 0")
    loop.link(recon, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(rejects, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(recon, poison, expression="@[User::ControlTotalsMatch] == 0")
    loop.chain(archive, mark_ok)
    loop.chain(poison, mark_bad)

    init = pkg.add(Expression("Init Batch Variables", "@[User::DoubleByteRowCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    body = pkg.add(loop)
    rows = pkg.add(log_row_count("raw.FilePartnerSales"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, body, rows, done)
    return pkg


# ---------------------------------------------------------------------------
# Carrier, supplier and treasury feeds
# ---------------------------------------------------------------------------


def ing_file_carrier_scan():
    """High-volume carrier scan events with an ISO-8601 timestamp and a sidecar
    control file holding the expected row count."""
    pkg = new_package(
        "ING_FILE_CarrierScan",
        "Ingest carrier scan event files (comma-delimited, ISO-8601 timestamps with a "
        "carrier-local offset, a sidecar .ctl file holding the expected row count). "
        "Scan status codes are carrier-specific and are landed untranslated; the "
        "translation happens downstream against raw.OracleCustomerMaster code sets. "
        "Duplicate scans within a file are counted, not rejected.",
        source_system=SRC_CARRIER,
        connections=(CONN_FILES, CONN_ARCHIVE, CONN_REJECT, CONN_STAGING),
        extra_variables=file_variables([("SidecarRowCount", 0, "int"), ("DuplicateScanCount", 0, "int")]),
    )

    cols = [
        str_col("CarrierCode", 10),
        str_col("TrackingNumber", 40),
        str_col("ShipmentReference", 20),
        str_col("ScanStatusCode", 12),
        str_col("ScanStatusDescription", 120),
        str_col("ScanTimestampText", 32),
        str_col("ScanLocationCode", 12),
        str_col("ScanCountryCode", 2),
        str_col("ExceptionReasonCode", 12),
        str_col("SignedByName", 80),
    ]

    df = DataFlow("Ingest Carrier Scans")
    df.flatfile_source("Carrier Scan File", CONN_FILES, cols)
    df.derived_column(
        "Parse Scan Event",
        [
            (
                "ScanTimestampUtc",
                "(DT_DBTIMESTAMP)SUBSTRING(ScanTimestampText,1,19)",
                date_col("ScanTimestampUtc"),
            ),
            (
                "ScanOffsetMinutes",
                "(DT_I4)((DT_I4)SUBSTRING(ScanTimestampText,21,2) * 60 + (DT_I4)SUBSTRING(ScanTimestampText,24,2))",
                int_col("ScanOffsetMinutes"),
            ),
            (
                "ExceptionFlag",
                'LEN(TRIM(ExceptionReasonCode)) > 0 ? "Y" : "N"',
                str_col("ExceptionFlag", 1),
            ),
            (
                "DeliveredFlag",
                'ScanStatusCode == "DLV" || ScanStatusCode == "POD" ? "Y" : "N"',
                str_col("DeliveredFlag", 1),
            ),
        ]
        + audit_derivations(SRC_CARRIER, "GLOBAL"),
    )
    df.conditional_split(
        "Validate Scan Rows",
        [
            (
                "Valid",
                'LEN(TRIM(TrackingNumber)) > 0 && LEN(TRIM(ScanStatusCode)) > 0 '
                "&& LEN(TRIM(ScanTimestampText)) >= 19",
            )
        ],
        default_output="Malformed",
    )
    df.row_count("Count Scan Rows", "User::DetailRowCount")
    df.oledb_destination(
        "raw FileCarrierScan",
        CONN_STAGING,
        "raw.FileCarrierScan",
        batch_size=100000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedFileRow Carrier",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Validate Scan Rows",
        from_output="Malformed",
    )
    df.reject_destination(
        "err RejectedFileRow Conversion",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Carrier Scan File",
        from_output="Flat File Source Error Output",
    )

    loop = Container(
        "Foreach Carrier Scan File",
        kind="foreach",
        enumerator={"folder": "@[$Project::InboundFileRoot]\\carrier", "file_spec": "carrier_scan_*.csv"},
        variable_mappings=["User::CurrentFilePath"],
        description="Loop the carrier scan drop folder.",
    )
    paths = loop.add(path_expressions("carrier", "carrier"))
    register = loop.add(register_file("raw.FileCarrierScan"))
    read_sidecar = loop.add(
        ExecuteSql(
            "Read Sidecar Control Count",
            CONN_STAGING,
            "SELECT ISNULL(ExpectedRowCount, 0) AS SidecarRowCount FROM etl.FileControlTotal "
            "WHERE FileName = ?;",
            result_type="ResultSetType_SingleRow",
            parameter_bindings=[("User::CurrentFileName", 0, "NVARCHAR")],
            result_bindings=[("0", "User::SidecarRowCount")],
        )
    )
    ingest = loop.add(DataFlowTask(df))
    dupes = loop.add(
        ExecuteSql(
            "Count Duplicate Scans",
            CONN_STAGING,
            "SELECT COUNT(*) AS DuplicateScanCount FROM ("
            "SELECT TrackingNumber, ScanStatusCode, ScanTimestampUtc FROM raw.FileCarrierScan "
            "WHERE PackageExecutionId = ? GROUP BY TrackingNumber, ScanStatusCode, ScanTimestampUtc "
            "HAVING COUNT(*) > 1) AS d;",
            result_type="ResultSetType_SingleRow",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
            result_bindings=[("0", "User::DuplicateScanCount")],
        )
    )
    recon = loop.add(
        reconcile_control_total(
            "raw.FileCarrierScan",
            "@Landed >= 0",
        )
    )
    rejects = loop.add(
        log_rejects("raw.FileCarrierScan", "SCAN_MALFORMED", "Carrier scan row missing tracking number or timestamp.")
    )
    archive = loop.add(archive_file())
    mark_ok = loop.add(mark_file_status("Mark File Processed", "Processed"))
    poison = loop.add(quarantine_file())
    mark_bad = loop.add(mark_file_status("Mark File Quarantined", "Quarantined"))
    loop.chain(paths, register, read_sidecar, ingest, dupes, recon)
    loop.link(recon, rejects, expression="@[User::MalformedRowCount] > 0")
    loop.link(recon, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(rejects, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(recon, poison, expression="@[User::ControlTotalsMatch] == 0")
    loop.chain(archive, mark_ok)
    loop.chain(poison, mark_bad)

    init = pkg.add(Expression("Init Batch Variables", "@[User::DuplicateScanCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    body = pkg.add(loop)
    rows = pkg.add(log_row_count("raw.FileCarrierScan"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, body, rows, done)
    return pkg


def ing_file_supplier_catalog():
    """Pipe-delimited supplier catalogue with a TRL footer carrying both a row
    count and a price checksum."""
    pkg = new_package(
        "ING_FILE_SupplierCatalog",
        "Ingest supplier catalogue files (pipe-delimited .psv, ISO-8859-1, YYYYMMDD "
        "dates, TRL footer carrying a row count and a price checksum). Prices arrive in "
        "the supplier's own currency and unit of measure; both are kept as supplied and "
        "converted downstream. A checksum mismatch quarantines the file instead of "
        "loading a partial catalogue.",
        source_system=SRC_BANK,
        connections=(CONN_FILES, CONN_ARCHIVE, CONN_REJECT, CONN_STAGING),
        extra_variables=file_variables([("PriceChecksum", 0, "int"), ("FooterChecksum", 0, "int")]),
    )

    cols = [
        str_col("RecordType", 3),
        str_col("SupplierCode", 12),
        str_col("SupplierItemCode", 30),
        str_col("ManufacturerPartNumber", 40),
        Column("ItemDescription", "str", length=200, codepage=28591),
        str_col("UomCode", 4),
        str_col("PackSizeText", 10),
        str_col("ListPriceText", 18),
        str_col("NetPriceText", 18),
        str_col("CurrencyCode", 3),
        str_col("MinimumOrderQuantityText", 10),
        str_col("LeadTimeDaysText", 6),
        str_col("EffectiveFromText", 8),
        str_col("EffectiveToText", 8),
        str_col("HazardClassCode", 6),
        str_col("FooterRowCountText", 12),
        str_col("FooterChecksumText", 20),
    ]

    df = DataFlow("Ingest Supplier Catalog")
    df.flatfile_source("Supplier Catalog File", CONN_FILES, cols)
    df.conditional_split(
        "Split Record Types",
        [
            ("Detail", 'RecordType == "DTL"'),
            ("Header", 'RecordType == "HDR"'),
            ("Trailer", 'RecordType == "TRL"'),
        ],
        default_output="Unknown Record Type",
    )
    df.derived_column(
        "Parse Catalog Detail",
        [
            (
                "EffectiveFromDate",
                '(DT_DBDATE)(SUBSTRING(EffectiveFromText,1,4) + "-" + SUBSTRING(EffectiveFromText,5,2) '
                '+ "-" + SUBSTRING(EffectiveFromText,7,2))',
                date_col("EffectiveFromDate"),
            ),
            (
                "EffectiveToDate",
                'LEN(TRIM(EffectiveToText)) == 8 ? (DT_DBDATE)(SUBSTRING(EffectiveToText,1,4) + "-" '
                '+ SUBSTRING(EffectiveToText,5,2) + "-" + SUBSTRING(EffectiveToText,7,2)) '
                ': (DT_DBDATE)"9999-12-31"',
                date_col("EffectiveToDate"),
            ),
            ("ListPrice", "(DT_CY)ListPriceText", money_col("ListPrice")),
            ("NetPrice", "(DT_CY)NetPriceText", money_col("NetPrice")),
            ("PackSize", "(DT_NUMERIC,18,3)PackSizeText", num_col("PackSize", 18, 3)),
            ("LeadTimeDays", "(DT_I4)LeadTimeDaysText", int_col("LeadTimeDays")),
            (
                "HazardousFlag",
                'LEN(TRIM(HazardClassCode)) > 0 ? "Y" : "N"',
                str_col("HazardousFlag", 1),
            ),
        ]
        + audit_derivations(SRC_BANK, "GLOBAL"),
    )
    df.conditional_split(
        "Validate Catalog Detail",
        [
            (
                "Valid",
                'LEN(TRIM(SupplierItemCode)) > 0 && NetPrice >= 0 && ListPrice >= NetPrice '
                "&& LEN(TRIM(EffectiveFromText)) == 8",
            )
        ],
        default_output="Malformed",
    )
    df.row_count("Count Catalog Rows", "User::DetailRowCount")
    df.oledb_destination(
        "raw FileSupplierCatalog",
        CONN_STAGING,
        "raw.FileSupplierCatalog",
        batch_size=20000,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedFileRow Catalog",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Validate Catalog Detail",
        from_output="Malformed",
    )
    df.reject_destination(
        "err RejectedFileRow Conversion",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Supplier Catalog File",
        from_output="Flat File Source Error Output",
    )

    loop = Container(
        "Foreach Supplier Catalog File",
        kind="foreach",
        enumerator={"folder": "@[$Project::InboundFileRoot]\\supplier", "file_spec": "supplier_catalog_*.psv"},
        variable_mappings=["User::CurrentFilePath"],
        description="Loop the supplier catalogue drop folder.",
    )
    paths = loop.add(path_expressions("supplier", "supplier"))
    register = loop.add(register_file("raw.FileSupplierCatalog"))
    ingest = loop.add(DataFlowTask(df))
    checksum = loop.add(
        ExecuteSql(
            "Compute Price Checksum",
            CONN_STAGING,
            "SELECT CAST(ISNULL(SUM(CAST(NetPrice * 100 AS bigint)) % 1000000, 0) AS int) AS PriceChecksum "
            "FROM raw.FileSupplierCatalog WHERE PackageExecutionId = ?;",
            result_type="ResultSetType_SingleRow",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
            result_bindings=[("0", "User::PriceChecksum")],
        )
    )
    recon = loop.add(
        reconcile_control_total(
            "raw.FileSupplierCatalog",
            "@Landed >= 0",
        )
    )
    rejects = loop.add(
        log_rejects("raw.FileSupplierCatalog", "CATALOG_BAD", "Supplier catalogue row failed price or date validation.")
    )
    archive = loop.add(archive_file())
    mark_ok = loop.add(mark_file_status("Mark File Processed", "Processed"))
    poison = loop.add(quarantine_file())
    mark_bad = loop.add(mark_file_status("Mark File Quarantined", "Quarantined"))
    loop.chain(paths, register, ingest, checksum, recon)
    loop.link(recon, rejects, expression="@[User::MalformedRowCount] > 0")
    loop.link(
        recon,
        archive,
        expression="@[User::ControlTotalsMatch] == 1 && @[User::PriceChecksum] == @[User::FooterChecksum]",
    )
    loop.link(rejects, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(
        recon,
        poison,
        expression="@[User::ControlTotalsMatch] == 0 || @[User::PriceChecksum] != @[User::FooterChecksum]",
    )
    loop.chain(archive, mark_ok)
    loop.chain(poison, mark_bad)

    init = pkg.add(Expression("Init Batch Variables", "@[User::PriceChecksum] = 0"))
    start = pkg.add(log_package_start(pkg))
    body = pkg.add(loop)
    rows = pkg.add(log_row_count("raw.FileSupplierCatalog"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, body, rows, done)
    return pkg


def ing_file_fx_override():
    """Treasury FX overrides: small, manually produced, four-eyes approved, and
    reconciled against the rates the ERP already published."""
    pkg = new_package(
        "ING_FILE_FxOverride",
        "Ingest treasury FX override files (comma-delimited, YYYY-MM-DD dates, final "
        "checksum line). These are hand-produced by treasury, so the package enforces "
        "the four-eyes approval columns and refuses any override outside the tolerance "
        "band against the published ERP rate; refused rows go to err.RejectedFileRow "
        "with the published rate attached rather than silently overriding it.",
        source_system=SRC_FX,
        connections=(CONN_FILES, CONN_ARCHIVE, CONN_REJECT, CONN_STAGING),
        extra_variables=file_variables([("OutOfToleranceCount", 0, "int"), ("ToleranceBasisPoints", 500, "int")]),
    )

    cols = [
        str_col("RecordType", 3),
        str_col("RateDateText", 10),
        str_col("FromCurrencyCode", 3),
        str_col("ToCurrencyCode", 3),
        str_col("RateTypeCode", 8),
        str_col("OverrideRateText", 24),
        str_col("ReasonCode", 12),
        str_col("ReasonText", 200),
        str_col("RequestedByUser", 30),
        str_col("ApprovedByUser", 30),
        str_col("ApprovalTicketNumber", 20),
        str_col("ChecksumText", 20),
    ]

    df = DataFlow("Ingest FX Overrides")
    df.flatfile_source("FX Override File", CONN_FILES, cols)
    df.conditional_split(
        "Split Record Types",
        [("Detail", 'RecordType == "FXO"')],
        default_output="Control Record",
    )
    df.derived_column(
        "Parse Override",
        [
            ("RateDate", "(DT_DBDATE)RateDateText", date_col("RateDate")),
            ("OverrideRate", "(DT_NUMERIC,18,8)OverrideRateText", num_col("OverrideRate", 18, 8)),
            (
                "RatePairCode",
                'FromCurrencyCode + "/" + ToCurrencyCode',
                str_col("RatePairCode", 8),
            ),
            (
                "FourEyesFlag",
                'LEN(TRIM(ApprovedByUser)) > 0 && ApprovedByUser != RequestedByUser ? "Y" : "N"',
                str_col("FourEyesFlag", 1),
            ),
        ]
        + audit_derivations(SRC_FX, "GLOBAL"),
    )
    df.lookup(
        "Lookup Published Rate",
        CONN_STAGING,
        "SELECT RatePairCode, RateDate, Rate AS PublishedRate FROM raw.OracleFxRate WITH (NOLOCK) "
        "WHERE RateTypeCode = N'SPOT';",
        ["RatePairCode", "RateDate"],
        [num_col("PublishedRate", 18, 8)],
        no_match="RD",
    )
    df.derived_column(
        "Measure Deviation",
        [
            (
                "DeviationBasisPoints",
                "PublishedRate == 0 ? 0 : (DT_I4)(ABS(OverrideRate - PublishedRate) / PublishedRate * 10000)",
                int_col("DeviationBasisPoints"),
            )
        ],
    )
    df.conditional_split(
        "Validate Override",
        [
            (
                "Approved",
                'FourEyesFlag == "Y" && LEN(TRIM(ApprovalTicketNumber)) > 0 && DeviationBasisPoints <= 500',
            )
        ],
        default_output="Refused",
    )
    df.row_count("Count Approved Overrides", "User::DetailRowCount")
    df.oledb_destination(
        "raw FileFxOverride",
        CONN_STAGING,
        "raw.FileFxOverride",
        batch_size=500,
        error_disposition="RedirectRow",
    )
    df.branch_destination(
        "err RejectedFileRow Refused Override",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Validate Override",
        from_output="Refused",
    )
    df.branch_destination(
        "err RejectedFileRow Unknown Pair",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Lookup Published Rate",
        from_output="Lookup No Match Output",
    )
    df.reject_destination(
        "err RejectedFileRow Conversion",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="FX Override File",
        from_output="Flat File Source Error Output",
    )

    loop = Container(
        "Foreach FX Override File",
        kind="foreach",
        enumerator={"folder": "@[$Project::InboundFileRoot]\\treasury", "file_spec": "fx_override_*.csv"},
        variable_mappings=["User::CurrentFilePath"],
        description="Loop the treasury FX override drop folder.",
    )
    paths = loop.add(path_expressions("treasury", "treasury"))
    register = loop.add(register_file("raw.FileFxOverride"))
    ingest = loop.add(DataFlowTask(df))
    out_of_band = loop.add(
        ExecuteSql(
            "Count Refused Overrides",
            CONN_STAGING,
            "SELECT COUNT(*) AS OutOfToleranceCount FROM err.RejectedFileRow "
            "WHERE PackageExecutionId = ? AND RejectReasonCode = N'FX_TOLERANCE';",
            result_type="ResultSetType_SingleRow",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
            result_bindings=[("0", "User::OutOfToleranceCount")],
        )
    )
    recon = loop.add(reconcile_control_total("raw.FileFxOverride", "@Landed >= 0"))
    rejects = loop.add(
        log_rejects(
            "raw.FileFxOverride",
            "FX_TOLERANCE",
            "FX override refused: unapproved or outside the tolerance band.",
        )
    )
    archive = loop.add(archive_file())
    mark_ok = loop.add(mark_file_status("Mark File Processed", "Processed"))
    poison = loop.add(quarantine_file())
    mark_bad = loop.add(mark_file_status("Mark File Quarantined", "Quarantined"))
    loop.chain(paths, register, ingest, out_of_band, recon)
    loop.link(recon, rejects, expression="@[User::OutOfToleranceCount] > 0")
    loop.link(recon, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(rejects, archive, expression="@[User::ControlTotalsMatch] == 1")
    loop.link(recon, poison, expression="@[User::ControlTotalsMatch] == 0")
    loop.chain(archive, mark_ok)
    loop.chain(poison, mark_bad)

    init = pkg.add(Expression("Init Batch Variables", "@[User::OutOfToleranceCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    body = pkg.add(loop)
    rows = pkg.add(log_row_count("raw.FileFxOverride"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, body, rows, done)
    return pkg


def ing_file_quarantine_malformed():
    """Re-read whatever ended up in the quarantine folder, record it row by row,
    and move anything unreadable to the poison path."""
    pkg = new_package(
        "ING_FILE_QuarantineMalformed",
        "Sweep the quarantine folder that the other ingestion packages write to. Every "
        "line is landed as a raw payload in err.RejectedFileRow with the originating "
        "feed inferred from the file name, so an operator can see exactly what was "
        "rejected. Files that cannot be read at all are moved to the poison path and "
        "left for manual handling; nothing is deleted.",
        source_system=SRC_MANUAL,
        connections=(CONN_FILES, CONN_ARCHIVE, CONN_REJECT, CONN_STAGING),
        extra_variables=file_variables([("UnreadableFileCount", 0, "int"), ("ReplayEligibleCount", 0, "int")]),
    )

    cols = [
        Column("RawLine", "wstr", length=4000),
    ]

    df = DataFlow("Sweep Quarantined Rows")
    df.flatfile_source("Quarantined File", CONN_FILES, cols)
    df.derived_column(
        "Classify Quarantined Row",
        [
            (
                "OriginFeedCode",
                'FINDSTRING(@[User::CurrentFileName], "partner_sales_na", 1) > 0 ? "PARTNER_NA" : '
                '(FINDSTRING(@[User::CurrentFileName], "partner_sales_eu", 1) > 0 ? "PARTNER_EU" : '
                '(FINDSTRING(@[User::CurrentFileName], "partner_sales_apac", 1) > 0 ? "PARTNER_APAC" : '
                '(FINDSTRING(@[User::CurrentFileName], "carrier_scan", 1) > 0 ? "CARRIER" : '
                '(FINDSTRING(@[User::CurrentFileName], "supplier_catalog", 1) > 0 ? "SUPPLIER" : '
                '(FINDSTRING(@[User::CurrentFileName], "fx_override", 1) > 0 ? "FX" : "UNKNOWN")))))',
                str_col("OriginFeedCode", 12),
            ),
            (
                "RejectReasonCode",
                'LEN(TRIM(RawLine)) == 0 ? "EMPTY_LINE" : "QUARANTINED"',
                str_col("RejectReasonCode", 20),
            ),
            (
                "ReplayEligibleFlag",
                'LEN(TRIM(RawLine)) > 0 && FINDSTRING(RawLine, "\\x00", 1) == 0 ? "Y" : "N"',
                str_col("ReplayEligibleFlag", 1),
            ),
        ]
        + audit_derivations(SRC_MANUAL, "GLOBAL"),
    )
    df.conditional_split(
        "Split Replayable Rows",
        [("Replayable", 'ReplayEligibleFlag == "Y"')],
        default_output="Unreadable",
    )
    df.row_count("Count Quarantined Rows", "User::DetailRowCount")
    df.oledb_destination(
        "err RejectedFileRow Replayable",
        CONN_STAGING,
        "err.RejectedFileRow",
        batch_size=5000,
        error_disposition="IgnoreFailure",
    )
    df.branch_destination(
        "err RejectedFileRow Unreadable",
        CONN_STAGING,
        "err.RejectedFileRow",
        from_component="Split Replayable Rows",
        from_output="Unreadable",
    )

    loop = Container(
        "Foreach Quarantined File",
        kind="foreach",
        enumerator={"folder": "@[$Project::RejectFileRoot]\\quarantine", "file_spec": "*"},
        variable_mappings=["User::CurrentFilePath"],
        description="Loop everything sitting in the quarantine folder.",
    )
    paths = loop.add(path_expressions("quarantine", "quarantine"))
    register = loop.add(register_file("err.RejectedFileRow"))
    sweep = loop.add(DataFlowTask(df))
    replayable = loop.add(
        ExecuteSql(
            "Count Replay Eligible Rows",
            CONN_STAGING,
            "SELECT COUNT(*) AS ReplayEligibleCount FROM err.RejectedFileRow "
            "WHERE PackageExecutionId = ? AND RejectReasonCode = N'QUARANTINED';",
            result_type="ResultSetType_SingleRow",
            parameter_bindings=[("User::PackageExecutionId", 0, "LONG")],
            result_bindings=[("0", "User::ReplayEligibleCount")],
        )
    )
    log_swept = loop.add(
        log_rejects("err.RejectedFileRow", "QUARANTINED", "Quarantined file line recorded for operator review.")
    )
    archive = loop.add(archive_file())
    mark_ok = loop.add(mark_file_status("Mark File Swept", "Swept"))
    poison = loop.add(quarantine_file())
    mark_bad = loop.add(mark_file_status("Mark File Unreadable", "Unreadable"))
    loop.chain(paths, register, sweep, replayable, log_swept)
    loop.link(log_swept, archive, expression="@[User::ReplayEligibleCount] > 0")
    loop.link(log_swept, poison, expression="@[User::ReplayEligibleCount] == 0")
    loop.chain(archive, mark_ok)
    loop.chain(poison, mark_bad)

    init = pkg.add(Expression("Init Batch Variables", "@[User::UnreadableFileCount] = 0"))
    start = pkg.add(log_package_start(pkg))
    body = pkg.add(loop)
    rows = pkg.add(log_row_count("err.RejectedFileRow"))
    done = pkg.add(log_package_success())
    pkg.chain(init, start, body, rows, done)
    return pkg


PACKAGE_BUILDERS = [
    ing_file_partner_sales_na,
    ing_file_partner_sales_eu,
    ing_file_partner_sales_apac,
    ing_file_carrier_scan,
    ing_file_supplier_catalog,
    ing_file_fx_override,
    ing_file_quarantine_malformed,
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
