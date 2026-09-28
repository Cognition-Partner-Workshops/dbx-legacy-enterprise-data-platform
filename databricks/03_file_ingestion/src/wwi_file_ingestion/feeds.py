"""Feed specifications for the seven ING_FILE_* packages.

Everything here is derived from two legacy sources that must stay in step:

* ``config/landing-zone.yaml`` - path, file pattern, encoding, delimiter and
  header flag of each inbound feed (the generator loads it into FEED_SPECS).
* ``ssis/03_file_ingestion/generate_file_ingestion.py`` - the flat-file
  connection manager columns and the audit / source-system constants.

``tests/test_feeds.py`` asserts the constants below against the YAML so the
bundle cannot silently drift from the landing-zone contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

PROJECT_NAME = "WWI_Ingest_Files"

SRC_PARTNER = "PARTNER_FL"
SRC_CARRIER = "CARRIER_FL"
SRC_BANK = "BANK_FL"
SRC_FX = "FX_FEED"
SRC_MANUAL = "MANUAL"

# landing-zone.yaml encoding -> Python codec (the SSIS code page is in brackets)
PYTHON_CODECS = {
    "windows-1252": "cp1252",  # 1252
    "utf-8": "utf-8",  # 65001
    "iso-8859-1": "latin-1",  # 28591
    "shift_jis": "shift_jis",  # 932
}

VOLUME_INBOUND = "inbound"
VOLUME_ARCHIVE = "archive"
VOLUME_QUARANTINE = "quarantine"
VOLUME_CHECKPOINTS = "checkpoints"
POISON_SUBFOLDER = "poison"
DUPLICATE_SUBFOLDER = "duplicate"

ERR_REJECTED_FILE_ROW = "err_rejected_file_row"
LEGACY_ERR_REJECTED_FILE_ROW = "err.RejectedFileRow"
FILE_INGESTION_LOG = "file_ingestion_log"
FILE_CONTROL_TOTAL = "file_control_total"


@dataclass(frozen=True)
class FeedSpec:
    packageName: str
    feedCode: str
    landingPath: str  # relative to the landing root, as in landing-zone.yaml
    filePattern: str  # landing-zone.yaml pattern
    fileGlob: str  # generator file_spec
    encoding: str  # landing-zone.yaml encoding name
    delimiter: str
    header: bool
    columns: Tuple[str, ...]
    legacyObjectName: str  # raw.X / err.X the package wrote to
    bronzeSchema: str
    bronzeTable: str
    sourceSystemCode: str
    regionCode: str
    archiveSubfolder: str
    rejectSubfolder: str
    stepName: str
    rejectReasonCode: str  # code used by the package's "Log Rejected Rows" task
    rejectReason: str
    textQualifier: str = '"'  # DTS:TextQualifier on every WWI_Inbound flat-file connection manager
    dateFormatAssumed: Optional[str] = None
    decimalSeparator: str = "."
    extraColumns: Tuple[str, ...] = field(default_factory=tuple)

    def bronzeObjectName(self) -> str:
        """ObjectName used in the etl.* control tables: the Delta name of the legacy raw.X / err.X target."""
        return "%s.%s" % (self.bronzeSchema, self.bronzeTable)

    @property
    def expectedColumnCount(self) -> int:
        return len(self.columns)

    @property
    def pythonCodec(self) -> str:
        return PYTHON_CODECS[self.encoding]

    @property
    def volumeFolder(self) -> str:
        """Sub-path under the inbound / quarantine volume (landingPath without its root)."""
        parts = self.landingPath.split("/")
        return "/".join(parts[1:])

    @property
    def sourceVolume(self) -> str:
        return VOLUME_QUARANTINE if self.landingPath.split("/")[0] == "quarantine" else VOLUME_INBOUND


# Tests point this at a temp directory; on Databricks it is always the UC Volume root.
VOLUME_ROOT_TEMPLATE = "/Volumes/{catalog}/bronze"


def volumeRoot(catalog: str) -> str:
    return VOLUME_ROOT_TEMPLATE.format(catalog=catalog)


def volumePath(catalog: str, volume: str, *parts: str) -> str:
    """/Volumes/<catalog>/bronze/<volume>/<parts...> - the UC Volume replacement for the file shares."""
    tail = "/".join(p.strip("/") for p in parts if p)
    base = "%s/%s" % (volumeRoot(catalog), volume)
    return base + ("/" + tail if tail else "")


PARTNER_SALES_NA = FeedSpec(
    packageName="ING_FILE_PartnerSales_NA",
    feedCode="partner_sales_na",
    landingPath="inbound/partner/na",
    filePattern="partner_sales_na_{yyyyMMdd}_{seq3}.csv",
    fileGlob="partner_sales_na_*.csv",
    encoding="windows-1252",
    delimiter=",",
    header=True,
    columns=(
        "RecordType", "PartnerCode", "StoreNumber", "TransactionNumber", "TransactionDateText",
        "ProductCode", "Upc", "QuantityText", "UnitPriceText", "StateTaxText", "CountyTaxText",
        "LineTotalText", "CurrencyCode", "StateCode", "PostalCode", "FooterRecordCountText",
    ),
    legacyObjectName="raw.FilePartnerSales",
    bronzeSchema="bronze",
    bronzeTable="raw_file_partner_sales",
    sourceSystemCode=SRC_PARTNER,
    regionCode="NA",
    archiveSubfolder="partner/na",
    rejectSubfolder="partner/na",
    stepName="Partner Drops",
    rejectReasonCode="MALFORMED",
    rejectReason="NA partner detail row failed validation.",
    dateFormatAssumed="MM/DD/YYYY",
)

PARTNER_SALES_EU = FeedSpec(
    packageName="ING_FILE_PartnerSales_EU",
    feedCode="partner_sales_eu",
    landingPath="inbound/partner/eu",
    filePattern="partner_sales_eu_{yyyyMMdd}_{seq3}.csv",
    fileGlob="partner_sales_eu_*.csv",
    encoding="utf-8",
    delimiter=",",
    header=True,
    columns=(
        "RecordType", "PartnerCode", "OutletCode", "ReceiptNumber", "TransactionDateText",
        "ArticleNumber", "Ean", "QuantityText", "GrossAmountText", "VatRateText",
        "VatRegistrationNumber", "CurrencyCode", "CountryCode", "PostalCode", "ConsentFlag",
        "FooterAmountText",
    ),
    legacyObjectName="raw.FilePartnerSales",
    bronzeSchema="bronze",
    bronzeTable="raw_file_partner_sales",
    sourceSystemCode=SRC_PARTNER,
    regionCode="EU",
    archiveSubfolder="partner/eu",
    rejectSubfolder="partner/eu",
    stepName="Partner Drops",
    rejectReasonCode="VAT_MISSING",
    rejectReason="EU partner row missing a valid VAT registration number.",
    dateFormatAssumed="DD/MM/YYYY",
    decimalSeparator=",",
)

PARTNER_SALES_APAC = FeedSpec(
    packageName="ING_FILE_PartnerSales_APAC",
    feedCode="partner_sales_apac",
    landingPath="inbound/partner/apac",
    filePattern="partner_sales_apac_{yyyyMMdd}_{seq3}.txt",
    fileGlob="partner_sales_apac_*.txt",
    encoding="iso-8859-1",
    delimiter="\t",
    header=False,
    columns=(
        "RecordMarker", "PartnerCode", "OutletCode", "OutletName", "SlipNumber",
        "TransactionDateText", "ItemCode", "QuantityText", "NetAmountText", "GstAmountText",
        "CurrencyCode", "CountryCode", "PostalDistrict", "FooterTotalText",
    ),
    legacyObjectName="raw.FilePartnerSales",
    bronzeSchema="bronze",
    bronzeTable="raw_file_partner_sales",
    sourceSystemCode=SRC_PARTNER,
    regionCode="APAC",
    archiveSubfolder="partner/apac",
    rejectSubfolder="partner/apac",
    stepName="Partner Drops",
    rejectReasonCode="MALFORMED",
    rejectReason="APAC partner detail row failed validation.",
    dateFormatAssumed="YYYY/MM/DD",
)

CARRIER_SCAN = FeedSpec(
    packageName="ING_FILE_CarrierScan",
    feedCode="carrier_scan",
    landingPath="inbound/carrier",
    filePattern="carrier_scan_{yyyyMMdd}_{seq3}.csv",
    fileGlob="carrier_scan_*.csv",
    encoding="windows-1252",
    delimiter=",",
    header=True,
    columns=(
        "CarrierCode", "TrackingNumber", "ShipmentReference", "ScanStatusCode",
        "ScanStatusDescription", "ScanTimestampText", "ScanLocationCode", "ScanCountryCode",
        "ExceptionReasonCode", "SignedByName",
    ),
    legacyObjectName="raw.FileCarrierScan",
    bronzeSchema="bronze",
    bronzeTable="raw_file_carrier_scan",
    sourceSystemCode=SRC_CARRIER,
    regionCode="GLOBAL",
    archiveSubfolder="carrier",
    rejectSubfolder="carrier",
    stepName="Carrier And Catalog",
    rejectReasonCode="SCAN_MALFORMED",
    rejectReason="Carrier scan row missing tracking number or timestamp.",
    dateFormatAssumed="ISO-8601",
)

SUPPLIER_CATALOG = FeedSpec(
    packageName="ING_FILE_SupplierCatalog",
    feedCode="supplier_catalog",
    landingPath="inbound/supplier",
    filePattern="supplier_catalog_{yyyyMMdd}_{seq3}.psv",
    fileGlob="supplier_catalog_*.psv",
    encoding="iso-8859-1",
    delimiter="|",
    header=True,
    columns=(
        "RecordType", "SupplierCode", "SupplierItemCode", "ManufacturerPartNumber",
        "ItemDescription", "UomCode", "PackSizeText", "ListPriceText", "NetPriceText",
        "CurrencyCode", "MinimumOrderQuantityText", "LeadTimeDaysText", "EffectiveFromText",
        "EffectiveToText", "HazardClassCode", "FooterRowCountText", "FooterChecksumText",
    ),
    legacyObjectName="raw.FileSupplierCatalog",
    bronzeSchema="bronze",
    bronzeTable="raw_file_supplier_catalog",
    sourceSystemCode=SRC_BANK,
    regionCode="GLOBAL",
    archiveSubfolder="supplier",
    rejectSubfolder="supplier",
    stepName="Carrier And Catalog",
    rejectReasonCode="CATALOG_BAD",
    rejectReason="Supplier catalogue row failed price or date validation.",
    dateFormatAssumed="YYYYMMDD",
)

FX_OVERRIDE = FeedSpec(
    packageName="ING_FILE_FxOverride",
    feedCode="fx_override",
    landingPath="inbound/treasury",
    filePattern="fx_override_{yyyyMMdd}_{seq3}.csv",
    fileGlob="fx_override_*.csv",
    encoding="windows-1252",
    delimiter=",",
    header=True,
    columns=(
        "RecordType", "RateDateText", "FromCurrencyCode", "ToCurrencyCode", "RateTypeCode",
        "OverrideRateText", "ReasonCode", "ReasonText", "RequestedByUser", "ApprovedByUser",
        "ApprovalTicketNumber", "ChecksumText",
    ),
    legacyObjectName="raw.FileFxOverride",
    bronzeSchema="bronze",
    bronzeTable="raw_file_fx_override",
    sourceSystemCode=SRC_FX,
    regionCode="GLOBAL",
    archiveSubfolder="treasury",
    rejectSubfolder="treasury",
    stepName="Partner Drops",
    rejectReasonCode="FX_TOLERANCE",
    rejectReason="FX override refused: unapproved or outside the tolerance band.",
    dateFormatAssumed="YYYY-MM-DD",
)

QUARANTINE_MALFORMED = FeedSpec(
    packageName="ING_FILE_QuarantineMalformed",
    feedCode="quarantine_rejects",
    landingPath="quarantine",
    filePattern="quarantine_rejects_{yyyyMMdd}_{seq3}.dat",
    fileGlob="quarantine_rejects_*.dat",
    encoding="utf-8",
    delimiter="|",
    header=True,
    columns=("RawLine",),
    legacyObjectName=LEGACY_ERR_REJECTED_FILE_ROW,
    bronzeSchema="silver",
    bronzeTable=ERR_REJECTED_FILE_ROW,
    sourceSystemCode=SRC_MANUAL,
    regionCode="GLOBAL",
    archiveSubfolder="quarantine",
    rejectSubfolder="quarantine",
    stepName="Quarantine",
    rejectReasonCode="QUARANTINED",
    rejectReason="Quarantined file line recorded for operator review.",
)

FEEDS: Dict[str, FeedSpec] = {
    spec.packageName: spec
    for spec in (
        PARTNER_SALES_NA, PARTNER_SALES_EU, PARTNER_SALES_APAC, CARRIER_SCAN,
        SUPPLIER_CATALOG, FX_OVERRIDE, QUARANTINE_MALFORMED,
    )
}

PACKAGE_ORDER: List[str] = list(FEEDS)

# ING_FILE_QuarantineMalformed infers the originating feed from the file name.
ORIGIN_FEED_CODES: Tuple[Tuple[str, str], ...] = (
    ("partner_sales_na", "PARTNER_NA"),
    ("partner_sales_eu", "PARTNER_EU"),
    ("partner_sales_apac", "PARTNER_APAC"),
    ("carrier_scan", "CARRIER"),
    ("supplier_catalog", "SUPPLIER"),
    ("fx_override", "FX"),
)
