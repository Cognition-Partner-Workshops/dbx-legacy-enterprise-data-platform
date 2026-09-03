/*
    raw.File* landing tables (flat-file feeds)

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 12
    Depends on    : sqlserver/control/01_schemas.sql
    Called by     : the ING_FILE_* ingestion packages

    File feeds carry SourceFileName and SourceRowNumber so a rejected row can be
    traced back to a line in a file that the archive job has since moved. Every
    column lands as text: the partner feeds are produced by spreadsheets and the
    carrier feed by a mainframe that pads numerics with spaces.
*/

IF OBJECT_ID(N'raw.FilePartnerSales', N'U') IS NULL
BEGIN
    CREATE TABLE raw.FilePartnerSales
    (
        PartnerCode             NVARCHAR(30)    NULL,
        PartnerOutletCode       NVARCHAR(30)    NULL,
        ReportingPeriod         NVARCHAR(20)    NULL,   -- YYYYMM for most partners, YYYY-WW for two of them
        TransactionReference    NVARCHAR(60)    NULL,
        TransactionDate         NVARCHAR(40)    NULL,   -- DD/MM/YYYY from EU partners, MM/DD/YYYY from NA
        PartnerProductCode      NVARCHAR(60)    NULL,
        PartnerProductDesc      NVARCHAR(400)   NULL,
        EanBarcode              NVARCHAR(30)    NULL,
        QuantitySold            NVARCHAR(50)    NULL,
        UnitOfMeasure           NVARCHAR(20)    NULL,
        GrossAmount             NVARCHAR(50)    NULL,   -- decimal comma in EU files
        DiscountAmount          NVARCHAR(50)    NULL,
        TaxAmount               NVARCHAR(50)    NULL,
        NetAmount               NVARCHAR(50)    NULL,
        CurrencyCode            NVARCHAR(10)    NULL,
        CustomerReference       NVARCHAR(60)    NULL,
        SalesChannel            NVARCHAR(30)    NULL,
        CountryCode             NVARCHAR(10)    NULL,
        FileFormatVersion       NVARCHAR(10)    NULL,   -- v1 files have no tax columns at all
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawFilePartnerSales_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawFilePartnerSales_Source DEFAULT (N'PARTNER_FL'),
        SourceFileName          NVARCHAR(260)   NULL,
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawFilePartnerSales_Batch
        ON raw.FilePartnerSales (BatchId, SourceFileName, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.FileCarrierScan', N'U') IS NULL
BEGIN
    CREATE TABLE raw.FileCarrierScan
    (
        CarrierCode             NVARCHAR(20)    NULL,
        TrackingNumber          NVARCHAR(60)    NULL,
        ShipmentReference       NVARCHAR(50)    NULL,
        ScanEventCode           NVARCHAR(20)    NULL,   -- PU / IT / OD / DL / EX / RT, carrier-specific
        ScanEventDescription    NVARCHAR(200)   NULL,
        ScanTimestampLocal      NVARCHAR(40)    NULL,   -- no offset; the depot time zone is implied
        ScanTimeZoneAbbrev      NVARCHAR(10)    NULL,
        DepotCode               NVARCHAR(30)    NULL,
        DepotCity               NVARCHAR(100)   NULL,
        DepotCountryCode        NVARCHAR(10)    NULL,
        ExceptionReasonCode     NVARCHAR(20)    NULL,
        SignatoryName           NVARCHAR(200)   NULL,
        PieceCount              NVARCHAR(20)    NULL,
        WeightValue             NVARCHAR(50)    NULL,
        WeightUomCode           NVARCHAR(10)    NULL,   -- KG from EU/APAC depots, LB from NA depots
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawFileCarrierScan_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawFileCarrierScan_Source DEFAULT (N'CARRIER_FL'),
        SourceFileName          NVARCHAR(260)   NULL,
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawFileCarrierScan_Batch
        ON raw.FileCarrierScan (BatchId, SourceFileName, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.FileSupplierCatalog', N'U') IS NULL
BEGIN
    CREATE TABLE raw.FileSupplierCatalog
    (
        SupplierCode            NVARCHAR(30)    NULL,
        CatalogVersion          NVARCHAR(20)    NULL,
        EffectiveFromDate       NVARCHAR(40)    NULL,
        SupplierItemCode        NVARCHAR(60)    NULL,
        SupplierItemDesc        NVARCHAR(400)   NULL,
        ManufacturerPartNumber  NVARCHAR(60)    NULL,
        EanBarcode              NVARCHAR(30)    NULL,
        PackSize                NVARCHAR(30)    NULL,
        PackUom                 NVARCHAR(20)    NULL,
        ListPrice               NVARCHAR(50)    NULL,
        NetPrice                NVARCHAR(50)    NULL,
        PriceCurrency           NVARCHAR(10)    NULL,
        MinimumOrderQuantity    NVARCHAR(30)    NULL,
        LeadTimeDays            NVARCHAR(20)    NULL,
        HazardClass             NVARCHAR(20)    NULL,
        CountryOfOrigin         NVARCHAR(40)    NULL,   -- country names, not codes, in older catalogues
        TariffCode              NVARCHAR(30)    NULL,
        DiscontinuedFlag        NVARCHAR(5)     NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawFileSupplierCatalog_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawFileSupplierCatalog_Source DEFAULT (N'PARTNER_FL'),
        SourceFileName          NVARCHAR(260)   NULL,
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawFileSupplierCatalog_Batch
        ON raw.FileSupplierCatalog (BatchId, SourceFileName, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.FileFxOverride', N'U') IS NULL
BEGIN
    CREATE TABLE raw.FileFxOverride
    (
        RateDate                NVARCHAR(40)    NULL,
        FromCurrencyCode        NVARCHAR(10)    NULL,
        ToCurrencyCode          NVARCHAR(10)    NULL,
        RateTypeCode            NVARCHAR(20)    NULL,   -- SPOT / CORPORATE / MONTH_END
        ConversionRate          NVARCHAR(50)    NULL,
        RateSource              NVARCHAR(40)    NULL,
        OverrideReason          NVARCHAR(400)   NULL,   -- treasury keys this by hand at month end
        RequestedBy             NVARCHAR(100)   NULL,
        ApprovedBy              NVARCHAR(100)   NULL,
        ApprovedWhen            NVARCHAR(40)    NULL,
        AppliesToLedgerCode     NVARCHAR(20)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawFileFxOverride_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawFileFxOverride_Source DEFAULT (N'FX_FEED'),
        SourceFileName          NVARCHAR(260)   NULL,
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawFileFxOverride_Batch
        ON raw.FileFxOverride (BatchId, SourceFileName, SourceRowNumber);
END;
GO
