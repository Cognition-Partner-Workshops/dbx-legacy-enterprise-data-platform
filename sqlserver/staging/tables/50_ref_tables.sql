/*
    ref.* conformed reference and crosswalk tables

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 50
    Depends on    : sqlserver/control/01_schemas.sql (the ref schema)
    Read by       : stg.usp_TranslateSourceCodes, stg.usp_ConvertCurrencyAmounts,
                    stg.usp_NormalizeAddress, stg.ufn_SourceSystemKey

    Two kinds of table live here. The conformed lists (Country, Currency,
    UnitOfMeasure, StatusCode, ReasonCode, TaxJurisdiction, Region) are the single
    agreed set the warehouse dimensions are built from. The crosswalks
    (CodeCrosswalk, SourceKeyCrosswalk, UomConversion) map each source system's
    own codes onto that set; they are maintained by hand by the data stewards and
    reloaded from spreadsheets, which is why they carry a maintainer and a note.

    Ownership of the conformed value is deliberately not the same as ownership of
    the source value: Oracle owns supplier and GL codes, the OLTP database owns
    stock and delivery codes, and the partner feeds own nothing but still need a
    crosswalk row for every code they have ever sent.
*/

IF OBJECT_ID(N'ref.Region', N'U') IS NULL
BEGIN
    CREATE TABLE ref.Region
    (
        RegionCode              NVARCHAR(10)    NOT NULL,
        RegionName              NVARCHAR(100)   NOT NULL,
        TaxRegimeCode           NVARCHAR(20)    NOT NULL,   -- SALESTAX / VAT / GST
        DefaultCurrencyCode     NCHAR(3)        NOT NULL,
        FiscalCalendarCode      NVARCHAR(20)    NOT NULL,   -- NA_CAL / EU_APR / APAC_JUL
        FiscalYearStartMonth    TINYINT         NOT NULL,
        AddressRuleSetCode      NVARCHAR(30)    NOT NULL,   -- NA_USPS / EU_COUNTRY / APAC_LOCAL
        WeightUomCode           NVARCHAR(10)    NOT NULL,   -- LB in NA source data, KG everywhere else
        ConsentModelCode        NVARCHAR(20)    NOT NULL,   -- OPT_OUT / OPT_IN
        DefaultRetentionMonths  SMALLINT        NOT NULL,
        DateFormatHint          NVARCHAR(20)    NOT NULL,   -- MM/DD/YYYY / DD/MM/YYYY / YYYY-MM-DD
        DecimalSeparator        NVARCHAR(2)     NOT NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_refRegion_IsActive DEFAULT (1),
        CONSTRAINT PK_refRegion PRIMARY KEY CLUSTERED (RegionCode)
    );
END;
GO

IF OBJECT_ID(N'ref.Country', N'U') IS NULL
BEGIN
    CREATE TABLE ref.Country
    (
        CountryCode             NCHAR(2)        NOT NULL,
        CountryCodeIso3         NCHAR(3)        NULL,
        CountryName             NVARCHAR(100)   NOT NULL,
        RegionCode              NVARCHAR(10)    NOT NULL,
        SubRegionName           NVARCHAR(100)   NULL,
        LocalCurrencyCode       NCHAR(3)        NULL,
        PostalFormatMask        NVARCHAR(30)    NULL,       -- e.g. 99999, A9A 9A9, 999-9999
        PostalCodeRequiredFlag  BIT             NOT NULL CONSTRAINT DF_refCountry_PostalRequired DEFAULT (1),
        StateProvinceRequiredFlag BIT           NOT NULL CONSTRAINT DF_refCountry_StateRequired DEFAULT (0),
        AddressLineOrderCode    NVARCHAR(20)    NULL,       -- WESTERN / EASTERN (largest unit first)
        VatRegistrationMask     NVARCHAR(40)    NULL,
        IsEuMemberState         BIT             NOT NULL CONSTRAINT DF_refCountry_Eu DEFAULT (0),
        EuAccessionDate         DATE            NULL,
        EuExitDate              DATE            NULL,       -- kept so historical VAT treatment still works
        IsActive                BIT             NOT NULL CONSTRAINT DF_refCountry_IsActive DEFAULT (1),
        CONSTRAINT PK_refCountry PRIMARY KEY CLUSTERED (CountryCode)
    );
END;
GO

IF OBJECT_ID(N'ref.Currency', N'U') IS NULL
BEGIN
    CREATE TABLE ref.Currency
    (
        CurrencyCode            NCHAR(3)        NOT NULL,
        CurrencyName            NVARCHAR(100)   NOT NULL,
        CurrencySymbol          NVARCHAR(10)    NULL,
        MinorUnitDigits         TINYINT         NOT NULL CONSTRAINT DF_refCurrency_MinorUnits DEFAULT (2),
        RoundingRuleCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_refCurrency_Rounding DEFAULT (N'HALF_UP'),
        IsReportingCurrency     BIT             NOT NULL CONSTRAINT DF_refCurrency_Reporting DEFAULT (0),
        IsEuroLegacy            BIT             NOT NULL CONSTRAINT DF_refCurrency_EuroLegacy DEFAULT (0),
        EuroFixedRate           DECIMAL(19,8)   NULL,
        RetiredDate             DATE            NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_refCurrency_IsActive DEFAULT (1),
        CONSTRAINT PK_refCurrency PRIMARY KEY CLUSTERED (CurrencyCode)
    );
END;
GO

IF OBJECT_ID(N'ref.FxRateDaily', N'U') IS NULL
BEGIN
    CREATE TABLE ref.FxRateDaily
    (
        FromCurrencyCode        NCHAR(3)        NOT NULL,
        ToCurrencyCode          NCHAR(3)        NOT NULL,
        RateDate                DATE            NOT NULL,
        RateTypeCode            NVARCHAR(20)    NOT NULL,   -- SPOT / CORPORATE / AVERAGE / PERIOD_END
        ConversionRate          DECIMAL(19,8)   NOT NULL,
        RateSourceCode          NVARCHAR(20)    NULL,
        IsTreasuryOverride      BIT             NOT NULL CONSTRAINT DF_refFxRateDaily_Override DEFAULT (0),
        EffectiveFromUtc        DATETIME2(3)    NULL,
        EffectiveToUtc          DATETIME2(3)    NULL,       -- the overrides are effective-dated intraday
        LoadedFromBatchId       BIGINT          NULL,
        CONSTRAINT PK_refFxRateDaily PRIMARY KEY CLUSTERED
            (FromCurrencyCode, ToCurrencyCode, RateDate, RateTypeCode)
    );
END;
GO

IF OBJECT_ID(N'ref.UnitOfMeasure', N'U') IS NULL
BEGIN
    CREATE TABLE ref.UnitOfMeasure
    (
        UomCode                 NVARCHAR(10)    NOT NULL,
        UomName                 NVARCHAR(100)   NOT NULL,
        UomClassCode            NVARCHAR(20)    NOT NULL,   -- WEIGHT / VOLUME / COUNT / LENGTH
        BaseUomCode             NVARCHAR(10)    NOT NULL,
        IsBaseUom               BIT             NOT NULL CONSTRAINT DF_refUnitOfMeasure_IsBase DEFAULT (0),
        DecimalPrecision        TINYINT         NOT NULL CONSTRAINT DF_refUnitOfMeasure_Precision DEFAULT (4),
        IsActive                BIT             NOT NULL CONSTRAINT DF_refUnitOfMeasure_IsActive DEFAULT (1),
        CONSTRAINT PK_refUnitOfMeasure PRIMARY KEY CLUSTERED (UomCode)
    );
END;
GO

IF OBJECT_ID(N'ref.UomConversion', N'U') IS NULL
BEGIN
    CREATE TABLE ref.UomConversion
    (
        FromUomCode             NVARCHAR(10)    NOT NULL,
        ToUomCode               NVARCHAR(10)    NOT NULL,
        StockItemBusinessKey    NVARCHAR(100)   NOT NULL CONSTRAINT DF_refUomConversion_Item DEFAULT (N'*'),
        ConversionFactor        DECIMAL(18,8)   NOT NULL,
        IsItemSpecific          BIT             NOT NULL CONSTRAINT DF_refUomConversion_ItemSpecific DEFAULT (0),
        EffectiveFromDate       DATE            NULL,
        MaintainedByName        NVARCHAR(100)   NULL,
        MaintenanceNote         NVARCHAR(400)   NULL,
        CONSTRAINT PK_refUomConversion PRIMARY KEY CLUSTERED (FromUomCode, ToUomCode, StockItemBusinessKey)
    );
END;
GO

IF OBJECT_ID(N'ref.StatusCode', N'U') IS NULL
BEGIN
    CREATE TABLE ref.StatusCode
    (
        StatusDomainCode        NVARCHAR(30)    NOT NULL,   -- ORDER / INVOICE / SHIPMENT / SUPPLIER / PO
        ConformedStatusCode     NVARCHAR(20)    NOT NULL,
        ConformedStatusName     NVARCHAR(100)   NOT NULL,
        StatusGroupCode         NVARCHAR(20)    NULL,       -- OPEN / CLOSED / CANCELLED / EXCEPTION
        SortOrder               SMALLINT        NULL,
        IsTerminalStatus        BIT             NOT NULL CONSTRAINT DF_refStatusCode_Terminal DEFAULT (0),
        IsActive                BIT             NOT NULL CONSTRAINT DF_refStatusCode_IsActive DEFAULT (1),
        CONSTRAINT PK_refStatusCode PRIMARY KEY CLUSTERED (StatusDomainCode, ConformedStatusCode)
    );
END;
GO

IF OBJECT_ID(N'ref.ReasonCode', N'U') IS NULL
BEGIN
    CREATE TABLE ref.ReasonCode
    (
        ReasonDomainCode        NVARCHAR(30)    NOT NULL,   -- RETURN / CREDIT / HOLD / ADJUSTMENT / REJECT
        ConformedReasonCode     NVARCHAR(20)    NOT NULL,
        ConformedReasonName     NVARCHAR(100)   NOT NULL,
        ReasonGroupCode         NVARCHAR(20)    NULL,
        IsCustomerFault         BIT             NULL,
        IsSupplierFault         BIT             NULL,
        RequiresApproval        BIT             NOT NULL CONSTRAINT DF_refReasonCode_Approval DEFAULT (0),
        IsActive                BIT             NOT NULL CONSTRAINT DF_refReasonCode_IsActive DEFAULT (1),
        CONSTRAINT PK_refReasonCode PRIMARY KEY CLUSTERED (ReasonDomainCode, ConformedReasonCode)
    );
END;
GO

IF OBJECT_ID(N'ref.TaxJurisdiction', N'U') IS NULL
BEGIN
    CREATE TABLE ref.TaxJurisdiction
    (
        TaxJurisdictionCode     NVARCHAR(30)    NOT NULL,
        TaxJurisdictionName     NVARCHAR(200)   NOT NULL,
        TaxRegimeCode           NVARCHAR(20)    NOT NULL,
        CountryCode             NCHAR(2)        NOT NULL,
        StateProvinceCode       NVARCHAR(20)    NULL,
        CountyOrDistrictName    NVARCHAR(100)   NULL,
        CityName                NVARCHAR(100)   NULL,
        PostalCodeLow           NVARCHAR(20)    NULL,       -- NA jurisdictions are resolved by ZIP range
        PostalCodeHigh          NVARCHAR(20)    NULL,
        CombinedRatePercent     DECIMAL(9,4)    NULL,
        StateRatePercent        DECIMAL(9,4)    NULL,
        CountyRatePercent       DECIMAL(9,4)    NULL,
        CityRatePercent         DECIMAL(9,4)    NULL,
        SpecialDistrictRatePercent DECIMAL(9,4) NULL,
        ReverseChargeEligible   BIT             NOT NULL CONSTRAINT DF_refTaxJurisdiction_ReverseCharge DEFAULT (0),
        RegistrationRequiredFlag BIT            NOT NULL CONSTRAINT DF_refTaxJurisdiction_Registration DEFAULT (0),
        EffectiveFromDate       DATE            NULL,
        EffectiveToDate         DATE            NULL,
        CONSTRAINT PK_refTaxJurisdiction PRIMARY KEY CLUSTERED (TaxJurisdictionCode)
    );

    CREATE INDEX IX_refTaxJurisdiction_Postal ON ref.TaxJurisdiction (CountryCode, PostalCodeLow, PostalCodeHigh);
END;
GO

IF OBJECT_ID(N'ref.CodeCrosswalk', N'U') IS NULL
BEGIN
    CREATE TABLE ref.CodeCrosswalk
    (
        CrosswalkId             BIGINT          NOT NULL IDENTITY(1,1),
        CodeDomainCode          NVARCHAR(30)    NOT NULL,   -- matches ref.StatusCode / ref.ReasonCode domains
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceCodeValue         NVARCHAR(50)    NOT NULL,
        SourceCodeDescription   NVARCHAR(200)   NULL,
        ConformedCodeValue      NVARCHAR(20)    NOT NULL,
        RegionCode              NVARCHAR(10)    NULL,       -- some codes only make sense in one region
        IsDefaultForConformed   BIT             NOT NULL CONSTRAINT DF_refCodeCrosswalk_Default DEFAULT (0),
        EffectiveFromDate       DATE            NOT NULL CONSTRAINT DF_refCodeCrosswalk_EffFrom DEFAULT ('1900-01-01'),
        EffectiveToDate         DATE            NULL,
        MaintainedByName        NVARCHAR(100)   NULL,
        MaintenanceNote         NVARCHAR(400)   NULL,
        CONSTRAINT PK_refCodeCrosswalk PRIMARY KEY CLUSTERED (CrosswalkId)
    );

    CREATE UNIQUE INDEX UX_refCodeCrosswalk_Source
        ON ref.CodeCrosswalk (CodeDomainCode, SourceSystemCode, SourceCodeValue, EffectiveFromDate);
END;
GO

IF OBJECT_ID(N'ref.SourceKeyCrosswalk', N'U') IS NULL
BEGIN
    CREATE TABLE ref.SourceKeyCrosswalk
    (
        CrosswalkId             BIGINT          NOT NULL IDENTITY(1,1),
        EntityName              NVARCHAR(50)    NOT NULL,   -- Customer / Supplier / Product / Geography
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceKeyValue          NVARCHAR(100)   NOT NULL,
        ConformedBusinessKey    NVARCHAR(140)   NOT NULL,
        MatchMethodCode         NVARCHAR(20)    NOT NULL,   -- LOADED / MANUAL / DEDUP_SURVIVOR
        SupersededByBusinessKey NVARCHAR(140)   NULL,       -- set when a dedup merge retires a key
        IsActive                BIT             NOT NULL CONSTRAINT DF_refSourceKeyCrosswalk_IsActive DEFAULT (1),
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_refSourceKeyCrosswalk_Created DEFAULT (SYSUTCDATETIME()),
        MaintainedByName        NVARCHAR(100)   NULL,
        CONSTRAINT PK_refSourceKeyCrosswalk PRIMARY KEY CLUSTERED (CrosswalkId)
    );

    CREATE UNIQUE INDEX UX_refSourceKeyCrosswalk_Source
        ON ref.SourceKeyCrosswalk (EntityName, SourceSystemCode, SourceKeyValue);
    CREATE INDEX IX_refSourceKeyCrosswalk_Conformed ON ref.SourceKeyCrosswalk (ConformedBusinessKey);
END;
GO

IF OBJECT_ID(N'ref.PostalFormatRule', N'U') IS NULL
BEGIN
    CREATE TABLE ref.PostalFormatRule
    (
        RuleId                  INT             NOT NULL IDENTITY(1,1),
        CountryCode             NCHAR(2)        NOT NULL,
        RuleSetCode             NVARCHAR(30)    NOT NULL,
        RulePriority            SMALLINT        NOT NULL,
        StripCharacters         NVARCHAR(30)    NULL,       -- characters removed before the mask is applied
        UpperCaseFlag           BIT             NOT NULL CONSTRAINT DF_refPostalFormatRule_Upper DEFAULT (1),
        FormatMask              NVARCHAR(30)    NULL,
        MinimumLength           TINYINT         NULL,
        MaximumLength           TINYINT         NULL,
        TruncateToLength        TINYINT         NULL,       -- NA keeps ZIP5 and drops the +4
        InsertSeparatorAt       TINYINT         NULL,
        SeparatorCharacter      NVARCHAR(2)     NULL,
        RuleNote                NVARCHAR(400)   NULL,
        CONSTRAINT PK_refPostalFormatRule PRIMARY KEY CLUSTERED (RuleId)
    );

    CREATE INDEX IX_refPostalFormatRule_Country ON ref.PostalFormatRule (CountryCode, RulePriority);
END;
GO
