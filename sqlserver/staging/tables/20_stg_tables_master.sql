/*
    stg.* conformed master-data tables

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 20
    Depends on    : 10_raw_tables_oracle.sql, 11_raw_tables_sqlserver.sql,
                    12_raw_tables_file.sql, 50_ref_tables.sql
    Loaded by     : stg.usp_TruncateAndReload_Customer / _Supplier / _Product /
                    _Geography, stg.usp_NormalizeCustomer, stg.usp_NormalizeSupplier,
                    stg.usp_NormalizeAddress, stg.usp_DeduplicateCustomer
    Read by       : stg.vw_*ReadyForDimension and the DIM_* packages

    Typed, trimmed, code-translated and deduplicated. Every table carries a
    business key (the conformed key the warehouse matches on), a RowHash over the
    type-1 attributes and a ChangeHash over the type-2 attributes, so the
    dimension loads can tell "nothing changed" from "changed in a way that needs a
    new version" without re-reading the warehouse.
*/

IF OBJECT_ID(N'stg.Customer', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Customer
    (
        StagingCustomerId       BIGINT          NOT NULL IDENTITY(1,1),
        CustomerBusinessKey     NVARCHAR(100)   NOT NULL,   -- <SourceSystemCode>|<natural key>
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceCustomerId        NVARCHAR(50)    NOT NULL,
        OltpCustomerId          INT             NULL,       -- resolved through ref.SourceKeyCrosswalk
        ErpCustomerNumber       NVARCHAR(50)    NULL,
        CustomerName            NVARCHAR(200)   NOT NULL,
        CustomerLegalName       NVARCHAR(200)   NULL,
        CustomerNameStandardized NVARCHAR(200)  NULL,       -- casing, punctuation and suffix normalised
        ParentCustomerBusinessKey NVARCHAR(100) NULL,
        BuyingGroupName         NVARCHAR(100)   NULL,
        CustomerCategoryCode    NVARCHAR(20)    NULL,       -- conformed set from ref.CodeCrosswalk
        CustomerStatusCode      NVARCHAR(20)    NOT NULL,
        CreditLimitAmount       DECIMAL(19,4)   NULL,
        CreditLimitCurrencyCode NCHAR(3)        NULL,
        CreditLimitAmountUsd    DECIMAL(19,4)   NULL,
        CreditRatingCode        NVARCHAR(10)    NULL,
        PaymentTermsCode        NVARCHAR(20)    NULL,
        StandardTermsNetDays    SMALLINT        NULL,
        TaxRegistrationNumber   NVARCHAR(50)    NULL,
        TaxRegistrationValidFlag BIT            NULL,       -- format check only, no registry lookup
        RegionCode              NVARCHAR(10)    NOT NULL,
        LedgerCode              NVARCHAR(20)    NULL,
        PrimaryCountryCode      NCHAR(2)        NULL,
        SalespersonBusinessKey  NVARCHAR(100)   NULL,
        MarketingConsentFlag    BIT             NULL,       -- EU: explicit opt-in only; NA: opt-out
        MarketingConsentDate    DATE            NULL,
        RetentionClassCode      NVARCHAR(20)    NULL,       -- drives the purge window per region
        RetentionExpiryDate     DATE            NULL,
        AccountOpenedDate       DATE            NULL,
        LastActivityDate        DATE            NULL,
        SourceCreatedDate       DATETIME2(3)    NULL,
        SourceModifiedDate      DATETIME2(3)    NULL,
        SurvivorshipRuleApplied NVARCHAR(50)    NULL,       -- which rule won the merge; see usp_DeduplicateCustomer
        DuplicateGroupId        BIGINT          NULL,
        IsSurvivorRow           BIT             NOT NULL CONSTRAINT DF_stgCustomer_IsSurvivor DEFAULT (1),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgCustomer_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,       -- type-1 attributes
        ChangeHash              BINARY(32)      NULL,       -- type-2 attributes
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCustomer_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCustomer PRIMARY KEY CLUSTERED (StagingCustomerId)
    );

    CREATE UNIQUE INDEX UX_stgCustomer_BusinessKey ON stg.Customer (CustomerBusinessKey, BatchId);
    CREATE INDEX IX_stgCustomer_Region ON stg.Customer (RegionCode, CustomerStatusCode) INCLUDE (CustomerName);
END;
GO

IF OBJECT_ID(N'stg.CustomerAddress', N'U') IS NULL
BEGIN
    CREATE TABLE stg.CustomerAddress
    (
        StagingCustomerAddressId BIGINT         NOT NULL IDENTITY(1,1),
        AddressBusinessKey      NVARCHAR(120)   NOT NULL,
        CustomerBusinessKey     NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceAddressId         NVARCHAR(50)    NOT NULL,
        AddressUsageCode        NVARCHAR(20)    NOT NULL,   -- BILLTO / SHIPTO / STATEMENT / LEGAL
        AddressLine1            NVARCHAR(200)   NULL,
        AddressLine2            NVARCHAR(200)   NULL,
        DistrictName            NVARCHAR(200)   NULL,       -- APAC line 3 lands here
        CityName                NVARCHAR(100)   NULL,
        CityNameStandardized    NVARCHAR(100)   NULL,
        StateProvinceCode       NVARCHAR(20)    NULL,
        StateProvinceName       NVARCHAR(100)   NULL,
        PostalCodeRaw           NVARCHAR(40)    NULL,
        PostalCodeStandardized  NVARCHAR(20)    NULL,       -- NA ZIP5, EU per-country mask, APAC digits only
        PostalCodeValidFlag     BIT             NULL,
        CountryCode             NCHAR(2)        NULL,
        CountryName             NVARCHAR(100)   NULL,
        RegionCode              NVARCHAR(10)    NULL,
        TaxJurisdictionCode     NVARCHAR(30)    NULL,
        GeographyBusinessKey    NVARCHAR(120)   NULL,
        Latitude                DECIMAL(9,6)    NULL,
        Longitude               DECIMAL(9,6)    NULL,
        GeocodeQualityCode      NVARCHAR(20)    NULL,       -- EXACT / CITY / POSTAL / NONE
        IsPrimaryAddress        BIT             NOT NULL CONSTRAINT DF_stgCustomerAddress_Primary DEFAULT (0),
        ValidFromDate           DATE            NULL,
        ValidToDate             DATE            NULL,
        StandardizationRuleSet  NVARCHAR(30)    NULL,       -- NA_USPS / EU_COUNTRY / APAC_LOCAL
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgCustomerAddress_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCustomerAddress_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCustomerAddress PRIMARY KEY CLUSTERED (StagingCustomerAddressId)
    );

    CREATE INDEX IX_stgCustomerAddress_Customer ON stg.CustomerAddress (CustomerBusinessKey, AddressUsageCode);
END;
GO

IF OBJECT_ID(N'stg.Supplier', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Supplier
    (
        StagingSupplierId       BIGINT          NOT NULL IDENTITY(1,1),
        SupplierBusinessKey     NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceSupplierId        NVARCHAR(50)    NOT NULL,
        OltpSupplierId          INT             NULL,
        ErpSupplierNumber       NVARCHAR(50)    NULL,
        SupplierName            NVARCHAR(200)   NOT NULL,
        SupplierNameStandardized NVARCHAR(200)  NULL,
        SupplierShortName       NVARCHAR(100)   NULL,
        SupplierCategoryCode    NVARCHAR(20)    NULL,
        SupplierStatusCode      NVARCHAR(20)    NOT NULL,
        DunsNumber              NVARCHAR(20)    NULL,
        TaxIdentifier           NVARCHAR(50)    NULL,
        TaxIdentifierTypeCode   NVARCHAR(20)    NULL,       -- EIN / VATIN / GSTIN / ABN
        WithholdingCode         NVARCHAR(20)    NULL,       -- NA 1099 reporting only
        VatRecoveryEligibleFlag BIT             NULL,       -- EU only
        GstRegisteredFlag       BIT             NULL,       -- APAC only
        PaymentTermsCode        NVARCHAR(20)    NULL,
        PaymentMethodCode       NVARCHAR(20)    NULL,       -- conformed: CHECK / ACH / SEPA / WIRE / BPAY
        TransactionCurrencyCode NCHAR(3)        NULL,
        DefaultIncotermCode     NVARCHAR(10)    NULL,
        LeadTimeDays            SMALLINT        NULL,
        MinimumOrderAmount      DECIMAL(19,4)   NULL,
        MinimumOrderAmountUsd   DECIMAL(19,4)   NULL,
        ScorecardRatingCode     NVARCHAR(10)    NULL,
        DiversityClassCode      NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NOT NULL,
        LedgerCode              NVARCHAR(20)    NULL,
        OnHoldFlag              BIT             NOT NULL CONSTRAINT DF_stgSupplier_OnHold DEFAULT (0),
        HoldReasonCode          NVARCHAR(20)    NULL,
        SourceCreatedDate       DATETIME2(3)    NULL,
        SourceModifiedDate      DATETIME2(3)    NULL,
        DuplicateGroupId        BIGINT          NULL,
        IsSurvivorRow           BIT             NOT NULL CONSTRAINT DF_stgSupplier_IsSurvivor DEFAULT (1),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgSupplier_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        ChangeHash              BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSupplier_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgSupplier PRIMARY KEY CLUSTERED (StagingSupplierId)
    );

    CREATE UNIQUE INDEX UX_stgSupplier_BusinessKey ON stg.Supplier (SupplierBusinessKey, BatchId);
END;
GO

IF OBJECT_ID(N'stg.Product', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Product
    (
        StagingProductId        BIGINT          NOT NULL IDENTITY(1,1),
        ProductBusinessKey      NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceProductId         NVARCHAR(50)    NOT NULL,
        ErpProductCode          NVARCHAR(50)    NULL,
        OltpStockItemId         INT             NULL,       -- from work.ProductCrosswalk
        CrosswalkConfidenceCode NVARCHAR(20)    NULL,       -- EXACT / BARCODE / NAME / UNMATCHED
        ProductName             NVARCHAR(200)   NOT NULL,
        ProductDescription      NVARCHAR(1000)  NULL,
        CategoryCode            NVARCHAR(30)    NULL,
        SubCategoryCode         NVARCHAR(30)    NULL,
        BrandName               NVARCHAR(100)   NULL,
        BaseUomCode             NVARCHAR(10)    NULL,       -- conformed through ref.UnitOfMeasure
        SellUomCode             NVARCHAR(10)    NULL,
        UomConversionFactor     DECIMAL(18,6)   NULL,
        StandardCostAmount      DECIMAL(19,4)   NULL,
        StandardCostCurrencyCode NCHAR(3)       NULL,
        StandardCostAmountUsd   DECIMAL(19,4)   NULL,
        ListPriceAmount         DECIMAL(19,4)   NULL,
        RecommendedRetailAmount DECIMAL(19,4)   NULL,
        TaxClassCode            NVARCHAR(20)    NULL,
        HazmatClassCode         NVARCHAR(20)    NULL,
        TemperatureClassCode    NVARCHAR(20)    NULL,
        IsChillerStock          BIT             NULL,
        ShelfLifeDays           SMALLINT        NULL,
        CountryOfOriginCode     NCHAR(2)        NULL,
        HsTariffCode            NVARCHAR(20)    NULL,
        PrimarySupplierBusinessKey NVARCHAR(100) NULL,
        LifecycleStatusCode     NVARCHAR(20)    NULL,
        DiscontinuedDate        DATE            NULL,
        Barcode                 NVARCHAR(30)    NULL,
        TypicalWeightPerUnitKg  DECIMAL(18,3)   NULL,       -- LB source values converted here
        SourceModifiedDate      DATETIME2(3)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgProduct_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        ChangeHash              BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgProduct_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgProduct PRIMARY KEY CLUSTERED (StagingProductId)
    );

    CREATE UNIQUE INDEX UX_stgProduct_BusinessKey ON stg.Product (ProductBusinessKey, BatchId);
    CREATE INDEX IX_stgProduct_Barcode ON stg.Product (Barcode) WHERE Barcode IS NOT NULL;
END;
GO

IF OBJECT_ID(N'stg.Geography', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Geography
    (
        StagingGeographyId      BIGINT          NOT NULL IDENTITY(1,1),
        GeographyBusinessKey    NVARCHAR(120)   NOT NULL,   -- country|state|city|postal
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        CountryCode             NCHAR(2)        NOT NULL,
        CountryCodeIso3         NCHAR(3)        NULL,
        CountryName             NVARCHAR(100)   NOT NULL,
        RegionCode              NVARCHAR(10)    NOT NULL,
        SubRegionName           NVARCHAR(100)   NULL,
        StateProvinceCode       NVARCHAR(20)    NULL,
        StateProvinceName       NVARCHAR(100)   NULL,
        CityName                NVARCHAR(100)   NULL,
        PostalCode              NVARCHAR(20)    NULL,
        PostalFormatMask        NVARCHAR(30)    NULL,
        TimeZoneName            NVARCHAR(50)    NULL,
        LocalCurrencyCode       NCHAR(3)        NULL,
        TaxJurisdictionCode     NVARCHAR(30)    NULL,
        TaxRegimeCode           NVARCHAR(20)    NULL,       -- SALESTAX / VAT / GST
        Population              BIGINT          NULL,
        Latitude                DECIMAL(9,6)    NULL,
        Longitude               DECIMAL(9,6)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgGeography_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgGeography_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgGeography PRIMARY KEY CLUSTERED (StagingGeographyId)
    );

    CREATE UNIQUE INDEX UX_stgGeography_BusinessKey ON stg.Geography (GeographyBusinessKey, BatchId);
END;
GO

IF OBJECT_ID(N'stg.CostCenter', N'U') IS NULL
BEGIN
    CREATE TABLE stg.CostCenter
    (
        StagingCostCenterId     BIGINT          NOT NULL IDENTITY(1,1),
        CostCenterBusinessKey   NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        CostCenterCode          NVARCHAR(30)    NOT NULL,
        CostCenterName          NVARCHAR(200)   NULL,
        ParentCostCenterCode    NVARCHAR(30)    NULL,
        HierarchyLevel          SMALLINT        NULL,
        HierarchyPath           NVARCHAR(400)   NULL,       -- built by the recursive CTE in the load
        CompanyCode             NVARCHAR(20)    NULL,
        LedgerCode              NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        FunctionalAreaCode      NVARCHAR(20)    NULL,
        ManagerEmployeeKey      NVARCHAR(100)   NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_stgCostCenter_IsActive DEFAULT (1),
        EffectiveFromDate       DATE            NULL,
        EffectiveToDate         DATE            NULL,
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCostCenter_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCostCenter PRIMARY KEY CLUSTERED (StagingCostCenterId)
    );
END;
GO

IF OBJECT_ID(N'stg.Employee', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Employee
    (
        StagingEmployeeId       BIGINT          NOT NULL IDENTITY(1,1),
        EmployeeBusinessKey     NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceEmployeeId        NVARCHAR(50)    NOT NULL,
        EmployeeFullName        NVARCHAR(200)   NOT NULL,
        PreferredName           NVARCHAR(100)   NULL,
        JobTitle                NVARCHAR(100)   NULL,
        DepartmentName          NVARCHAR(100)   NULL,
        CostCenterCode          NVARCHAR(30)    NULL,
        ManagerEmployeeKey      NVARCHAR(100)   NULL,
        WorkEmailAddress        NVARCHAR(200)   NULL,       -- masked for EU rows past retention
        IsSalesperson           BIT             NOT NULL CONSTRAINT DF_stgEmployee_IsSalesperson DEFAULT (0),
        IsWarehouseStaff        BIT             NOT NULL CONSTRAINT DF_stgEmployee_IsWarehouse DEFAULT (0),
        HireDate                DATE            NULL,
        TerminationDate         DATE            NULL,
        RegionCode              NVARCHAR(10)    NULL,
        RetentionMaskedFlag     BIT             NOT NULL CONSTRAINT DF_stgEmployee_Masked DEFAULT (0),
        RowHash                 BINARY(32)      NULL,
        ChangeHash              BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgEmployee_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgEmployee PRIMARY KEY CLUSTERED (StagingEmployeeId)
    );
END;
GO

IF OBJECT_ID(N'stg.Salesperson', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Salesperson
    (
        StagingSalespersonId    BIGINT          NOT NULL IDENTITY(1,1),
        SalespersonBusinessKey  NVARCHAR(100)   NOT NULL,
        EmployeeBusinessKey     NVARCHAR(100)   NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SalespersonName         NVARCHAR(200)   NOT NULL,
        SalesTerritoryCode      NVARCHAR(20)    NULL,
        CommissionPlanCode      NVARCHAR(20)    NULL,
        CommissionRatePercent   DECIMAL(9,4)    NULL,       -- plan rate, region-specific caps applied
        QuotaAmount             DECIMAL(19,4)   NULL,
        QuotaCurrencyCode       NCHAR(3)        NULL,
        QuotaAmountUsd          DECIMAL(19,4)   NULL,
        QuotaPeriodCode         NVARCHAR(20)    NULL,       -- NA calendar quarters, EU fiscal, APAC Apr-Mar
        EffectiveFromDate       DATE            NULL,
        EffectiveToDate         DATE            NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_stgSalesperson_IsActive DEFAULT (1),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSalesperson_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgSalesperson PRIMARY KEY CLUSTERED (StagingSalespersonId)
    );
END;
GO

IF OBJECT_ID(N'stg.SalesTerritory', N'U') IS NULL
BEGIN
    CREATE TABLE stg.SalesTerritory
    (
        StagingSalesTerritoryId BIGINT          NOT NULL IDENTITY(1,1),
        SalesTerritoryBusinessKey NVARCHAR(100) NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SalesTerritoryCode      NVARCHAR(20)    NOT NULL,
        SalesTerritoryName      NVARCHAR(100)   NOT NULL,
        RegionCode              NVARCHAR(10)    NOT NULL,
        CountryCodeList         NVARCHAR(400)   NULL,       -- comma separated; EU territories span countries
        ReportingCurrencyCode   NCHAR(3)        NULL,
        FiscalCalendarCode      NVARCHAR(20)    NULL,       -- NA_CAL / EU_APR / APAC_JUL
        TerritoryManagerKey     NVARCHAR(100)   NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_stgSalesTerritory_IsActive DEFAULT (1),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSalesTerritory_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgSalesTerritory PRIMARY KEY CLUSTERED (StagingSalesTerritoryId)
    );
END;
GO

IF OBJECT_ID(N'stg.Promotion', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Promotion
    (
        StagingPromotionId      BIGINT          NOT NULL IDENTITY(1,1),
        PromotionBusinessKey    NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        PromotionCode           NVARCHAR(30)    NOT NULL,
        PromotionName           NVARCHAR(200)   NULL,
        PromotionTypeCode       NVARCHAR(20)    NULL,       -- PCTOFF / AMTOFF / BOGO / BUNDLE / FREIGHT
        DiscountPercent         DECIMAL(9,4)    NULL,
        DiscountAmount          DECIMAL(19,4)   NULL,
        DiscountCurrencyCode    NCHAR(3)        NULL,
        AppliesToCategoryCode   NVARCHAR(30)    NULL,
        AppliesToChannelCode    NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        TaxTreatmentCode        NVARCHAR(20)    NULL,       -- NA discount pre-tax, EU discount post-VAT base
        StartDate               DATE            NULL,
        EndDate                 DATE            NULL,
        BudgetAmountUsd         DECIMAL(19,4)   NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_stgPromotion_IsActive DEFAULT (1),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPromotion_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgPromotion PRIMARY KEY CLUSTERED (StagingPromotionId)
    );
END;
GO

IF OBJECT_ID(N'stg.StockItem', N'U') IS NULL
BEGIN
    CREATE TABLE stg.StockItem
    (
        StagingStockItemId      BIGINT          NOT NULL IDENTITY(1,1),
        StockItemBusinessKey    NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        OltpStockItemId         INT             NOT NULL,
        StockItemName           NVARCHAR(200)   NOT NULL,
        ProductBusinessKey      NVARCHAR(100)   NULL,       -- ERP side of the crosswalk, may be NULL
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        BrandName               NVARCHAR(100)   NULL,
        SizeText                NVARCHAR(50)    NULL,
        UnitPackageCode         NVARCHAR(30)    NULL,
        OuterPackageCode        NVARCHAR(30)    NULL,
        QuantityPerOuter        INT             NULL,
        LeadTimeDays            SMALLINT        NULL,
        IsChillerStock          BIT             NOT NULL CONSTRAINT DF_stgStockItem_Chiller DEFAULT (0),
        Barcode                 NVARCHAR(30)    NULL,
        TaxRatePercent          DECIMAL(9,4)    NULL,
        UnitPriceAmount         DECIMAL(19,4)   NULL,
        RecommendedRetailAmount DECIMAL(19,4)   NULL,
        TypicalWeightPerUnitKg  DECIMAL(18,3)   NULL,
        MarketingTagList        NVARCHAR(400)   NULL,       -- flattened from the CustomFields JSON
        SourceModifiedDate      DATETIME2(3)    NULL,
        RowHash                 BINARY(32)      NULL,
        ChangeHash              BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgStockItem_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgStockItem PRIMARY KEY CLUSTERED (StagingStockItemId)
    );

    CREATE UNIQUE INDEX UX_stgStockItem_BusinessKey ON stg.StockItem (StockItemBusinessKey, BatchId);
END;
GO
