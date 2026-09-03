/*
    stg.* conformed dimension-source tables

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 23
    Depends on    : 10_raw_tables_oracle.sql, 11_raw_tables_sqlserver.sql,
                    20_stg_tables_master.sql, 22_stg_tables_sales.sql, 50_ref_tables.sql
    Loaded by     : stg.usp_ConformCityForDimension, stg.usp_ConformCustomerCategoryForDimension,
                    stg.usp_ConformCustomerSegmentForDimension,
                    stg.usp_ConformProductCategoryForDimension
    Read by       : DIM_Load_City, DIM_Load_CustomerCategory, DIM_Load_CustomerSegment,
                    DIM_Load_ProductCategory and the Integration.usp_MigrateStaged*Data
                    procedures they call

    These four tables are the shapes the dimension loads were written against.
    The column names are the ones the packages and the Integration procedures
    select by name, so they are not renamed here even where the master staging
    tables spell the same idea differently: stg.Geography holds the country and
    subdivision grain, stg.City holds the city grain the Type 2 city dimension
    versions, and the two are loaded from the same Oracle geography extract.

    Postal handling is deliberately left raw in this layer. The city dimension
    procedure runs three different standardisation algorithms over it (CASS in
    NA, ref.PostalFormatRule patterns in EU, uppercase-and-strip in APAC), so
    staging keeps PostalCodeRaw as the source wrote it and only records which
    regional rule set applies.
*/

IF OBJECT_ID(N'stg.City', N'U') IS NULL
BEGIN
    CREATE TABLE stg.City
    (
        StagingCityId           BIGINT          NOT NULL IDENTITY(1,1),
        CityBusinessKey         NVARCHAR(120)   NOT NULL,   -- <SourceSystemCode>|<country>|<subdivision>|<city>
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        WWICityID               BIGINT          NULL,       -- MDM allocates it; NULL until the city is matched
        CityName                NVARCHAR(200)   NOT NULL,
        LocalScriptCityName     NVARCHAR(200)   NULL,       -- populated for JPN, KOR, CHN and GRC only
        StateProvince           NVARCHAR(100)   NULL,
        CountryCode             NVARCHAR(10)    NULL,       -- ISO3, conformed through ref.Country
        Continent               NVARCHAR(50)    NULL,
        Subregion               NVARCHAR(100)   NULL,
        SalesTerritoryCode      NVARCHAR(20)    NULL,
        LatestRecordedPopulation BIGINT         NULL,       -- part of the Type 2 hash downstream
        PostalCodeRaw           NVARCHAR(30)    NULL,       -- standardised in the dimension load, not here
        PostalRuleSetCode       NVARCHAR(30)    NULL,       -- NA_USPS / EU_COUNTRY / APAC_LOCAL
        CountyName              NVARCHAR(100)   NULL,       -- NA
        CountyFipsCode          NVARCHAR(10)    NULL,       -- NA
        MetropolitanStatisticalArea NVARCHAR(200) NULL,     -- NA
        NutsLevel3Code          NVARCHAR(10)    NULL,       -- EU
        DistrictName            NVARCHAR(100)   NULL,       -- APAC
        PrefectureOrProvince    NVARCHAR(100)   NULL,       -- APAC
        LocalityName            NVARCHAR(100)   NULL,
        TimeZoneName            NVARCHAR(50)    NULL,
        UtcOffsetMinutes        INT             NULL,
        ObservesDaylightSaving  BIT             NULL,
        TaxJurisdictionCode     NVARCHAR(30)    NULL,
        RegionCode              NVARCHAR(10)    NOT NULL,
        SourceChangedOn         DATETIME2(3)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgCity_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCity_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCity PRIMARY KEY CLUSTERED (StagingCityId)
    );

    CREATE UNIQUE INDEX UX_stgCity_BusinessKey ON stg.City (CityBusinessKey, BatchId);
    CREATE INDEX IX_stgCity_Region ON stg.City (RegionCode, CountryCode) INCLUDE (CityName);
END;
GO

IF OBJECT_ID(N'stg.CustomerCategory', N'U') IS NULL
BEGIN
    CREATE TABLE stg.CustomerCategory
    (
        StagingCustomerCategoryId BIGINT        NOT NULL IDENTITY(1,1),
        CustomerCategoryBusinessKey NVARCHAR(100) NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        CustomerCategoryCode    NVARCHAR(20)    NOT NULL,   -- conformed through ref.CodeCrosswalk
        CustomerCategoryName    NVARCHAR(100)   NULL,
        CategoryGroupCode       NVARCHAR(20)    NULL,       -- WHOLESALE / RETAIL / INTERNAL / OTHER
        DiscountEligiblePercent DECIMAL(5,2)    NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_stgCustomerCategory_Active DEFAULT (1),
        SourceCategoryCode      NVARCHAR(30)    NULL,       -- kept for the ERP-to-OLTP reconciliation
        CustomerCount           INT             NULL,
        RegionCode              NVARCHAR(10)    NULL,
        SourceChangedOn         DATETIME2(3)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgCustomerCategory_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCustomerCategory_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCustomerCategory PRIMARY KEY CLUSTERED (StagingCustomerCategoryId)
    );

    CREATE UNIQUE INDEX UX_stgCustomerCategory_Code ON stg.CustomerCategory (CustomerCategoryCode, BatchId);
END;
GO

IF OBJECT_ID(N'stg.ProductCategory', N'U') IS NULL
BEGIN
    CREATE TABLE stg.ProductCategory
    (
        StagingProductCategoryId BIGINT         NOT NULL IDENTITY(1,1),
        ProductCategoryBusinessKey NVARCHAR(100) NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        ProductCategoryCode     NVARCHAR(30)    NOT NULL,
        ProductCategoryName     NVARCHAR(200)   NULL,
        ParentCategoryCode      NVARCHAR(30)    NULL,       -- NULL at level 1
        MerchandiseGroupCode    NVARCHAR(20)    NULL,
        HierarchyLevel          TINYINT         NULL,       -- 1 group, 2 category, 3 subcategory
        IsLeafCategory          BIT             NOT NULL CONSTRAINT DF_stgProductCategory_Leaf DEFAULT (0),
        HierarchyPath           NVARCHAR(200)   NULL,       -- slash separated, built by the load
        ProductCount            INT             NULL,
        TaxClassCode            NVARCHAR(20)    NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_stgProductCategory_Active DEFAULT (1),
        SourceChangedOn         DATETIME2(3)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgProductCategory_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgProductCategory_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgProductCategory PRIMARY KEY CLUSTERED (StagingProductCategoryId)
    );

    CREATE UNIQUE INDEX UX_stgProductCategory_Code ON stg.ProductCategory (ProductCategoryCode, BatchId);
    CREATE INDEX IX_stgProductCategory_Parent ON stg.ProductCategory (ParentCategoryCode, HierarchyLevel);
END;
GO

IF OBJECT_ID(N'stg.CustomerSegment', N'U') IS NULL
BEGIN
    CREATE TABLE stg.CustomerSegment
    (
        StagingCustomerSegmentId BIGINT         NOT NULL IDENTITY(1,1),
        SegmentBusinessKey      NVARCHAR(100)   NOT NULL,   -- <RegionCode>|<SegmentCode>
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        WWICustomerSegmentID    INT             NULL,
        SegmentCode             NVARCHAR(20)    NOT NULL,
        SegmentName             NVARCHAR(100)   NULL,
        SegmentFamilyCode       NVARCHAR(20)    NULL,       -- the '<family>_DEF' member is the fallback
        ScoringModelCode        NVARCHAR(20)    NULL,       -- RFM / BEHAV / VALUE
        ScoringModelVersion     SMALLINT        NULL,
        ScoringFrequencyCode    NVARCHAR(20)    NULL,       -- MONTHLY / QUARTERLY
        MinimumScore            INT             NULL,
        MaximumScore            INT             NULL,
        RecencyScoreFloor       INT             NULL,
        FrequencyScoreFloor     INT             NULL,
        MonetaryValueFloor      DECIMAL(19,4)   NULL,
        RecencyBand             NVARCHAR(20)    NULL,
        FrequencyBand           NVARCHAR(20)    NULL,
        MonetaryBand            NVARCHAR(20)    NULL,
        ChurnRiskBand           NVARCHAR(20)    NULL,
        LifetimeValueBand       NVARCHAR(20)    NULL,
        TargetContactFrequency  SMALLINT        NULL,       -- contacts per quarter
        RequiresProfilingConsent BIT            NOT NULL CONSTRAINT DF_stgCustomerSegment_Consent DEFAULT (0),
        MarketplaceOnly         BIT             NOT NULL CONSTRAINT DF_stgCustomerSegment_Marketplace DEFAULT (0),
        LastScoredOn            DATE            NULL,
        RegionCode              NVARCHAR(10)    NOT NULL,
        SourceChangedOn         DATETIME2(3)    NULL,
        SourceRowHash           BINARY(32)      NULL,       -- selected by DIM_Load_CustomerSegment
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgCustomerSegment_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCustomerSegment_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCustomerSegment PRIMARY KEY CLUSTERED (StagingCustomerSegmentId)
    );

    CREATE UNIQUE INDEX UX_stgCustomerSegment_Code ON stg.CustomerSegment (RegionCode, SegmentCode, BatchId);
    CREATE INDEX IX_stgCustomerSegment_Model ON stg.CustomerSegment (ScoringModelCode, MinimumScore, MaximumScore);
END;
GO

IF OBJECT_ID(N'stg.CustomerSegmentAssignment', N'U') IS NULL
BEGIN
    CREATE TABLE stg.CustomerSegmentAssignment
    (
        StagingSegmentAssignmentId BIGINT       NOT NULL IDENTITY(1,1),
        AssignmentBusinessKey   NVARCHAR(140)   NOT NULL,   -- <CustomerBusinessKey>|<SegmentCode>
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        CustomerBusinessKey     NVARCHAR(100)   NOT NULL,
        WWICustomerID           INT             NULL,       -- the key the dimension update joins on
        SegmentCode             NVARCHAR(20)    NOT NULL,
        ScoringModelCode        NVARCHAR(20)    NULL,
        RecencyScore            INT             NULL,
        FrequencyScore          INT             NULL,
        MonetaryValue           DECIMAL(19,4)   NULL,
        CompositeScore          INT             NULL,
        ProfilingConsentFlag    BIT             NULL,       -- EU opt-in, NA opt-out, APAC per marketplace
        IsSuppressedForConsent  BIT             NOT NULL CONSTRAINT DF_stgSegmentAssignment_Suppressed DEFAULT (0),
        AssignedOn              DATE            NULL,
        RegionCode              NVARCHAR(10)    NOT NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgSegmentAssignment_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSegmentAssignment_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCustomerSegmentAssignment PRIMARY KEY CLUSTERED (StagingSegmentAssignmentId)
    );

    CREATE UNIQUE INDEX UX_stgSegmentAssignment_Key ON stg.CustomerSegmentAssignment (AssignmentBusinessKey, BatchId);
    CREATE INDEX IX_stgSegmentAssignment_Customer ON stg.CustomerSegmentAssignment (WWICustomerID, SegmentCode);
END;
GO
