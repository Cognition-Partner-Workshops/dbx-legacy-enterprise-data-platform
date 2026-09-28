"""Delta schemas for the tables this bundle writes.

Column names keep the legacy PascalCase. The bronze tables carry the columns
the ING_FILE_* data flows actually insert (landed text, derived typed values
and the audit derivations) plus the audit columns mandated by
sqlserver/staging/tables/12_raw_tables_file.sql and the arrival metadata of
the UC Volume file. See docs/migration/03_file_ingestion-package-mapping.md for
the column-level mapping and the known drift against the legacy DDL.
"""

from __future__ import annotations

from typing import List, Tuple

from pyspark.sql import types as T

from . import feeds

Field = Tuple[str, T.DataType]

STRING = T.StringType()
DATE = T.DateType()
TIMESTAMP = T.TimestampType()
LONG = T.LongType()
INT = T.IntegerType()
MONEY = T.DecimalType(19, 4)
QUANTITY = T.DecimalType(18, 3)
RATE = T.DecimalType(18, 8)
VAT_RATE = T.DecimalType(9, 4)

ARRIVAL_FIELDS: List[Field] = [
    ("FileName", STRING),
    ("FilePath", STRING),
    ("FileModifiedAtUtc", TIMESTAMP),
    ("FileSizeBytes", LONG),
    ("ArrivedAtUtc", TIMESTAMP),
]

AUDIT_FIELDS: List[Field] = [
    ("BatchId", LONG),
    ("PackageExecutionId", LONG),
    ("LoadedAtUtc", TIMESTAMP),
    ("ExtractedAtUtc", TIMESTAMP),
    ("SourceSystemCode", STRING),
    ("RegionCode", STRING),
    ("SourceFileName", STRING),
    ("SourceRowNumber", LONG),
    ("RawLine", STRING),
]


def _struct(fields: List[Field]) -> T.StructType:
    return T.StructType([T.StructField(name, dataType, True) for name, dataType in fields])


RAW_FILE_PARTNER_SALES = _struct(
    [
        # landed text (NA / EU / APAC connection managers; a region leaves the other regions' columns NULL)
        ("RecordType", STRING), ("RecordMarker", STRING),
        ("PartnerCode", STRING), ("StoreNumber", STRING), ("OutletCode", STRING), ("OutletName", STRING),
        ("TransactionNumber", STRING), ("ReceiptNumber", STRING), ("SlipNumber", STRING),
        ("TransactionDateText", STRING),
        ("ProductCode", STRING), ("Upc", STRING), ("ArticleNumber", STRING), ("Ean", STRING), ("ItemCode", STRING),
        ("QuantityText", STRING), ("UnitPriceText", STRING), ("StateTaxText", STRING), ("CountyTaxText", STRING),
        ("LineTotalText", STRING), ("GrossAmountText", STRING), ("VatRateText", STRING),
        ("VatRegistrationNumber", STRING), ("NetAmountText", STRING), ("GstAmountText", STRING),
        ("CurrencyCode", STRING), ("CountryCode", STRING), ("StateCode", STRING),
        ("PostalCode", STRING), ("PostalDistrict", STRING), ("ConsentFlag", STRING),
        ("FooterRecordCountText", STRING), ("FooterAmountText", STRING), ("FooterTotalText", STRING),
        # derived
        ("TransactionDate", DATE), ("Quantity", QUANTITY), ("UnitPrice", MONEY), ("TaxAmount", MONEY),
        ("LineTotal", MONEY), ("GrossAmount", MONEY), ("VatRate", VAT_RATE), ("NetAmount", MONEY),
        ("VatAmount", MONEY), ("GstAmount", MONEY), ("TaxTreatmentCode", STRING), ("MarketableFlag", STRING),
    ]
    + AUDIT_FIELDS
    + ARRIVAL_FIELDS
)

RAW_FILE_CARRIER_SCAN = _struct(
    [
        ("CarrierCode", STRING), ("TrackingNumber", STRING), ("ShipmentReference", STRING),
        ("ScanStatusCode", STRING), ("ScanStatusDescription", STRING), ("ScanTimestampText", STRING),
        ("ScanLocationCode", STRING), ("ScanCountryCode", STRING), ("ExceptionReasonCode", STRING),
        ("SignedByName", STRING),
        ("ScanTimestampUtc", TIMESTAMP), ("ScanOffsetMinutes", INT), ("ExceptionFlag", STRING),
        ("DeliveredFlag", STRING),
    ]
    + AUDIT_FIELDS
    + ARRIVAL_FIELDS
)

RAW_FILE_SUPPLIER_CATALOG = _struct(
    [
        ("RecordType", STRING), ("SupplierCode", STRING), ("SupplierItemCode", STRING),
        ("ManufacturerPartNumber", STRING), ("ItemDescription", STRING), ("UomCode", STRING),
        ("PackSizeText", STRING), ("ListPriceText", STRING), ("NetPriceText", STRING), ("CurrencyCode", STRING),
        ("MinimumOrderQuantityText", STRING), ("LeadTimeDaysText", STRING), ("EffectiveFromText", STRING),
        ("EffectiveToText", STRING), ("HazardClassCode", STRING), ("FooterRowCountText", STRING),
        ("FooterChecksumText", STRING),
        ("EffectiveFromDate", DATE), ("EffectiveToDate", DATE), ("ListPrice", MONEY), ("NetPrice", MONEY),
        ("PackSize", QUANTITY), ("LeadTimeDays", INT), ("HazardousFlag", STRING),
    ]
    + AUDIT_FIELDS
    + ARRIVAL_FIELDS
)

RAW_FILE_FX_OVERRIDE = _struct(
    [
        ("RecordType", STRING), ("RateDateText", STRING), ("FromCurrencyCode", STRING), ("ToCurrencyCode", STRING),
        ("RateTypeCode", STRING), ("OverrideRateText", STRING), ("ReasonCode", STRING), ("ReasonText", STRING),
        ("RequestedByUser", STRING), ("ApprovedByUser", STRING), ("ApprovalTicketNumber", STRING),
        ("ChecksumText", STRING),
        ("RateDate", DATE), ("OverrideRate", RATE), ("RatePairCode", STRING), ("FourEyesFlag", STRING),
        ("PublishedRate", RATE), ("DeviationBasisPoints", INT),
    ]
    + AUDIT_FIELDS
    + ARRIVAL_FIELDS
)

# err.RejectedFileRow (RejectId is a Delta identity column created by the DDL below)
ERR_REJECTED_FILE_ROW = _struct(
    [
        ("BatchId", LONG), ("PackageExecutionId", LONG), ("SourceSystemCode", STRING),
        ("SourceFileName", STRING), ("FileFormatVersion", STRING), ("SourceRowNumber", LONG),
        ("RawRowText", STRING), ("ExpectedColumnCount", INT), ("ActualColumnCount", INT),
        ("DelimiterUsed", STRING), ("DecimalSeparatorUsed", STRING), ("DateFormatAssumed", STRING),
        ("RejectReasonCode", STRING), ("RejectReason", STRING), ("RejectStage", STRING),
        ("ReprocessStatusCode", STRING), ("ReprocessAttemptCount", INT), ("ReprocessedInBatchId", LONG),
        ("RejectedAtUtc", TIMESTAMP),
        # migration additions
        ("RegionCode", STRING), ("RecordClass", STRING), ("OriginFeedCode", STRING),
        ("ReplayEligibleFlag", STRING), ("_rescued_data", STRING),
    ]
    + ARRIVAL_FIELDS
)

BRONZE_SCHEMAS = {
    feeds.PARTNER_SALES_NA.bronzeTable: RAW_FILE_PARTNER_SALES,
    feeds.CARRIER_SCAN.bronzeTable: RAW_FILE_CARRIER_SCAN,
    feeds.SUPPLIER_CATALOG.bronzeTable: RAW_FILE_SUPPLIER_CATALOG,
    feeds.FX_OVERRIDE.bronzeTable: RAW_FILE_FX_OVERRIDE,
}


def createTable(spark, fullName: str, schema: T.StructType, identityColumn: str = None, comment: str = "") -> None:
    """CREATE TABLE IF NOT EXISTS through the DeltaTable builder (identity columns work on DBR and OSS Delta 3.3+)."""
    from delta.tables import DeltaTable, IdentityGenerator

    parts = fullName.split(".")
    if len(parts) == 3 and parts[0] == spark.catalog.currentCatalog():
        # the builder's identifier parser only takes catalog-qualified names on DBR
        fullName = ".".join(parts[1:])
    builder = DeltaTable.createIfNotExists(spark).tableName(fullName)
    if identityColumn:
        builder = builder.addColumn(identityColumn, T.LongType(), nullable=False, generatedAlwaysAs=IdentityGenerator())
    for field in schema.fields:
        builder = builder.addColumn(field.name, field.dataType, nullable=True)
    if comment:
        builder = builder.comment(comment)
    builder.execute()


# etl.FileIngestionLog / etl.FileControlTotal (sqlserver/control/07_tables_operations.sql)
FILE_INGESTION_LOG = _struct(
    [
        ("PackageExecutionId", LONG), ("ObjectName", STRING), ("FileName", STRING), ("FilePath", STRING),
        ("ReceivedAtUtc", TIMESTAMP), ("CompletedAtUtc", TIMESTAMP), ("DetailRowCount", LONG),
        ("RejectRowCount", LONG), ("Status", STRING),
        ("FileSizeBytes", LONG), ("FileModifiedAtUtc", TIMESTAMP), ("BatchId", LONG),
    ]
)

FILE_CONTROL_TOTAL = _struct(
    [
        ("FileName", STRING), ("FeedCode", STRING), ("ExpectedRowCount", LONG),
        ("ExpectedAmountTotal", MONEY), ("BusinessDate", DATE), ("ReceivedAtUtc", TIMESTAMP),
    ]
)
