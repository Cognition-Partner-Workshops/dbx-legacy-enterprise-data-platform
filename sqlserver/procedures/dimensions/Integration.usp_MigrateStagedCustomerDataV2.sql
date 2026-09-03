/*
    Object        : [Integration].[usp_MigrateStagedCustomerDataV2]
    Deploy target : WideWorldImportersDW
    Deploy order  : after every table in sqlserver/warehouse/dimensions
    Depends on    : stg.Customer, stg.CustomerAddress, work.CustomerDedup (staging database,
                    reached through the STAGING synonyms declared by the staging package),
                    Dimension.Customer, Dimension.Customer Category, Dimension.Buying Group,
                    Dimension.Customer Demographic, Dimension.City,
                    Integration.DimensionKeyRegistry, Integration.DimensionLoadAudit,
                    etl.usp_LogPackageStart / LogRowCount / LogError / LogRejectedRecord /
                    LogPackageEnd / usp_GetWatermark / usp_SetWatermark
    Called by     : DIM_NA_Load_Customer, DIM_EU_Load_Customer, DIM_APAC_Load_Customer

    V2 because the original MigrateStagedCustomerData (2004, Type 1 only) is still
    deployed and still called by the quarterly reload job. Do not delete it.

    Hybrid SCD:
        Type 2 attributes  - trading name, bill-to, category, buying group,
                             postal code, city, tax registration, credit limit band
        Type 1 attributes  - contact details, consent block, phone, website,
                             standard discount; overwritten on EVERY version of the
                             customer, not just the current one, because a consent
                             withdrawal must apply to history.

    Same-day changes: [Effective From] is a datetime2 and [Effective From Date] is
    the date part. A second change on the same day closes the first version at the
    new change's timestamp and increments [Effective Sequence], so two versions can
    share a date. The reports that group by [Effective From Date] pick the highest
    [Effective Sequence] for the day.

    Regional divergence - @RegionCode drives genuinely different logic, not a filter:
      NA    : jurisdiction code assembled from state + county + city, exemption
              certificates expiring inside the window force the exempt flag off,
              consent defaults to implied opt-out, retention 7 years.
      EU    : VAT number normalised and its country prefix checked against the
              country code, reverse charge derived for cross-border B2B, no consent
              means marketing and profiling flags are forced to 0, erasure requests
              pseudonymise the name columns in place across all versions,
              retention 6 years.
      APAC  : GST treatment from the business-number type, local-script name kept,
              per-channel consent, retention 5 years, and the postal code is left
              null rather than fabricated where the country has no postcode.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedCustomerDataV2]
    @BatchId                BIGINT,
    @RegionCode             NVARCHAR(10),
    @PackageName            NVARCHAR(200) = N'DIM_Load_Customer',
    @SourceSystemCode       NVARCHAR(20)  = N'ORA_MDM',
    @ReloadFullHistory      BIT           = 0,
    @LineageKey             INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @LoadStartedAt      DATETIME2(7) = SYSDATETIME();
    DECLARE @HighDate           DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');
    DECLARE @WatermarkFrom      NVARCHAR(50);
    DECLARE @WatermarkTo        NVARCHAR(50);
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @RejectRowCount     BIGINT = 0;
    DECLARE @Type1UpdateCount   BIGINT = 0;
    DECLARE @Type2CloseCount    BIGINT = 0;
    DECLARE @Type2InsertCount   BIGINT = 0;
    DECLARE @NewMemberCount     BIGINT = 0;
    DECLARE @EnrichedCount      BIGINT = 0;
    DECLARE @SameDayCount       BIGINT = 0;
    DECLARE @RetentionYears     SMALLINT;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DimensionLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF @RegionCode NOT IN (N'NA', N'EU', N'APAC')
            THROW 51001, N'usp_MigrateStagedCustomerDataV2: @RegionCode must be NA, EU or APAC.', 1;

        SET @RetentionYears =
            CASE @RegionCode
                WHEN N'NA'   THEN 7
                WHEN N'EU'   THEN 6
                WHEN N'APAC' THEN 5
            END;

        EXEC [etl].[usp_GetWatermark]
             @SourceSystemCode  = @SourceSystemCode,
             @ObjectName        = N'Dimension.Customer',
             @ReloadFullHistory = @ReloadFullHistory,
             @WatermarkFrom     = @WatermarkFrom OUTPUT,
             @WatermarkTo       = @WatermarkTo OUTPUT;

        /*
            Work table. The staging synonyms resolve to WideWorldImporters_Staging;
            they are created by the staging deployment, which is why nothing here
            three-part-names the staging database.
        */
        IF OBJECT_ID(N'tempdb..#CustomerSource') IS NOT NULL
            DROP TABLE #CustomerSource;

        CREATE TABLE #CustomerSource
        (
            [WWI Customer ID]           INT             NOT NULL,
            [Source Customer Reference] NVARCHAR(50)    NULL,
            [Customer]                  NVARCHAR(100)   NULL,
            [Bill To Customer]          NVARCHAR(100)   NULL,
            [Category]                  NVARCHAR(50)    NULL,
            [Buying Group]              NVARCHAR(50)    NULL,
            [Primary Contact]           NVARCHAR(50)    NULL,
            [Postal Code]               NVARCHAR(10)    NULL,
            [Country Code]              NVARCHAR(3)     NULL,
            [State Province Code]       NVARCHAR(5)     NULL,
            [County Name]               NVARCHAR(60)    NULL,
            [City Name]                 NVARCHAR(60)    NULL,
            [Tax Registration]          NVARCHAR(20)    NULL,
            [Business Number Type]      NVARCHAR(10)    NULL,
            [Local Script Name]         NVARCHAR(200)   NULL,
            [Credit Limit Amount]       DECIMAL(18, 2)  NULL,
            [Credit Limit Currency Code] NVARCHAR(3)    NULL,
            [Payment Terms Code]        NVARCHAR(10)    NULL,
            [Account Status Code]       NVARCHAR(10)    NULL,
            [Account Opened Date]       DATE            NULL,
            [Standard Discount Percentage] DECIMAL(9,4) NULL,
            [Phone Number Raw]          NVARCHAR(40)    NULL,
            [Website URL]               NVARCHAR(256)   NULL,
            [Consent Basis Code]        NVARCHAR(20)    NULL,
            [Marketing Consent Flag]    BIT             NULL,
            [Profiling Consent Flag]    BIT             NULL,
            [Consent Captured On]       DATETIME2(7)    NULL,
            [Consent Source Code]       NVARCHAR(20)    NULL,
            [Erasure Requested On]      DATETIME2(7)    NULL,
            [Source Changed On]         DATETIME2(7)    NULL,
            [Reject Reason Code]        NVARCHAR(50)    NULL,
            [Reject Reason]             NVARCHAR(500)   NULL
        );

        INSERT INTO #CustomerSource
            ([WWI Customer ID], [Source Customer Reference], [Customer], [Bill To Customer],
             [Category], [Buying Group], [Primary Contact], [Postal Code], [Country Code],
             [State Province Code], [County Name], [City Name], [Tax Registration],
             [Business Number Type], [Local Script Name], [Credit Limit Amount],
             [Credit Limit Currency Code], [Payment Terms Code], [Account Status Code],
             [Account Opened Date], [Standard Discount Percentage], [Phone Number Raw],
             [Website URL], [Consent Basis Code], [Marketing Consent Flag],
             [Profiling Consent Flag], [Consent Captured On], [Consent Source Code],
             [Erasure Requested On], [Source Changed On])
        SELECT
              c.[WWICustomerID]
            , c.[SourceCustomerReference]
            , LTRIM(RTRIM(c.[CustomerName]))
            , LTRIM(RTRIM(ISNULL(c.[BillToCustomerName], c.[CustomerName])))
            , c.[CategoryCode]
            , c.[BuyingGroupCode]
            , c.[PrimaryContactName]
            , a.[PostalCode]
            , a.[CountryCode]
            , a.[StateProvinceCode]
            , a.[CountyName]
            , a.[CityName]
            , c.[TaxRegistrationNumber]
            , c.[BusinessNumberType]
            , c.[LocalScriptName]
            , c.[CreditLimitAmount]
            , c.[CreditLimitCurrencyCode]
            , c.[PaymentTermsCode]
            , c.[AccountStatusCode]
            , c.[AccountOpenedDate]
            , c.[StandardDiscountPercentage]
            , c.[PhoneNumber]
            , c.[WebsiteUrl]
            , c.[ConsentBasisCode]
            , c.[MarketingConsentFlag]
            , c.[ProfilingConsentFlag]
            , c.[ConsentCapturedOn]
            , c.[ConsentSourceCode]
            , c.[ErasureRequestedOn]
            , c.[SourceChangedOn]
        FROM [stg].[Customer] AS c
        LEFT OUTER JOIN [stg].[CustomerAddress] AS a
            ON  a.[WWICustomerID] = c.[WWICustomerID]
            AND a.[AddressTypeCode] = N'BILL'
        WHERE c.[RegionCode] = @RegionCode
          AND (@ReloadFullHistory = 1
               OR c.[SourceChangedOn] > CONVERT(DATETIME2(7), @WatermarkFrom));

        SET @SourceRowCount = @@ROWCOUNT;

        /* ---------------------------------------------------------------
           Data quality screens. Rejected rows never reach the dimension;
           they are routed to etl.RejectedRecord one at a time so the reject
           carries its own reason. The cursor is deliberate - the reject
           procedure is row-scoped and always has been.
           --------------------------------------------------------------- */
        UPDATE #CustomerSource
        SET [Reject Reason Code] = N'MISSING_NAME',
            [Reject Reason]      = N'Customer name is null or blank in the source extract.'
        WHERE NULLIF(LTRIM(RTRIM([Customer])), N'') IS NULL;

        UPDATE #CustomerSource
        SET [Reject Reason Code] = N'MISSING_COUNTRY',
            [Reject Reason]      = N'Billing address has no country code; region assignment is impossible.'
        WHERE [Reject Reason Code] IS NULL
          AND [Country Code] IS NULL;

        IF @RegionCode = N'EU'
            UPDATE #CustomerSource
            SET [Reject Reason Code] = N'VAT_COUNTRY_MISMATCH',
                [Reject Reason]      = N'VAT registration prefix does not match the billing country.'
            WHERE [Reject Reason Code] IS NULL
              AND [Tax Registration] IS NOT NULL
              AND LEFT([Tax Registration], 2) <> LEFT([Country Code], 2);

        IF @RegionCode = N'APAC'
            UPDATE #CustomerSource
            SET [Reject Reason Code] = N'UNKNOWN_BUSINESS_NUMBER_TYPE',
                [Reject Reason]      = N'Business number type is not one of ABN, GSTIN, UEN, BRN.'
            WHERE [Reject Reason Code] IS NULL
              AND [Tax Registration] IS NOT NULL
              AND ISNULL([Business Number Type], N'') NOT IN (N'ABN', N'GSTIN', N'UEN', N'BRN');

        DECLARE @RejectCustomerId   INT;
        DECLARE @RejectReasonCode   NVARCHAR(50);
        DECLARE @RejectReason       NVARCHAR(500);
        DECLARE @RejectPayload      NVARCHAR(MAX);

        DECLARE CustomerRejectCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT [WWI Customer ID], [Reject Reason Code], [Reject Reason],
                   CONCAT(N'Customer=', [Customer], N'; Country=', [Country Code],
                          N'; TaxReg=', [Tax Registration])
            FROM #CustomerSource
            WHERE [Reject Reason Code] IS NOT NULL;

        OPEN CustomerRejectCursor;
        FETCH NEXT FROM CustomerRejectCursor
            INTO @RejectCustomerId, @RejectReasonCode, @RejectReason, @RejectPayload;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC [etl].[usp_LogRejectedRecord]
                 @PackageExecutionId = @PackageExecutionId,
                 @BatchId            = @BatchId,
                 @SourceSystemCode   = @SourceSystemCode,
                 @ObjectName         = N'Dimension.Customer',
                 @BusinessKey        = @RejectCustomerId,
                 @RejectReasonCode   = @RejectReasonCode,
                 @RejectReason       = @RejectReason,
                 @RejectStage        = N'Dimension',
                 @RecordPayload      = @RejectPayload;

            SET @RejectRowCount = @RejectRowCount + 1;

            FETCH NEXT FROM CustomerRejectCursor
                INTO @RejectCustomerId, @RejectReasonCode, @RejectReason, @RejectPayload;
        END;

        CLOSE CustomerRejectCursor;
        DEALLOCATE CustomerRejectCursor;

        DELETE FROM #CustomerSource WHERE [Reject Reason Code] IS NOT NULL;

        /* ---------------------------------------------------------------
           Regional conditioning. Three separate blocks; they were three
           separate procedures until 2013 and the logic has not been merged,
           only moved.
           --------------------------------------------------------------- */
        IF OBJECT_ID(N'tempdb..#CustomerConditioned') IS NOT NULL
            DROP TABLE #CustomerConditioned;

        CREATE TABLE #CustomerConditioned
        (
            [WWI Customer ID]               INT             NOT NULL PRIMARY KEY,
            [Customer]                      NVARCHAR(100)   NULL,
            [Bill To Customer]              NVARCHAR(100)   NULL,
            [Category]                      NVARCHAR(50)    NULL,
            [Buying Group]                  NVARCHAR(50)    NULL,
            [Primary Contact]               NVARCHAR(50)    NULL,
            [Postal Code]                   NVARCHAR(10)    NULL,
            [Country Code]                  NVARCHAR(3)     NULL,
            [Source Customer Reference]     NVARCHAR(50)    NULL,
            [Customer Category Key]         INT             NULL,
            [Buying Group Key]              INT             NULL,
            [Delivery City Key]             INT             NULL,
            [Postal City Key]               INT             NULL,
            [Credit Limit Amount]           DECIMAL(18, 2)  NULL,
            [Credit Limit Currency Code]    NVARCHAR(3)     NULL,
            [Payment Terms Code]            NVARCHAR(10)    NULL,
            [Account Status Code]           NVARCHAR(10)    NULL,
            [Account Opened Date]           DATE            NULL,
            [Standard Discount Percentage]  DECIMAL(9, 4)   NULL,
            [Phone Number Standardized]     NVARCHAR(30)    NULL,
            [Website URL]                   NVARCHAR(256)   NULL,
            [Sales Tax Jurisdiction Code]   NVARCHAR(15)    NULL,
            [Sales Tax Exempt Flag]         BIT             NULL,
            [ZIP Plus Four]                 NVARCHAR(10)    NULL,
            [State Province Code]           NVARCHAR(5)     NULL,
            [VAT Registration Number]       NVARCHAR(20)    NULL,
            [VAT Rate Category]             NVARCHAR(10)    NULL,
            [Is Reverse Charge Applicable]  BIT             NULL,
            [EU Member State Code]          NVARCHAR(2)     NULL,
            [Postcode Standardized]         NVARCHAR(12)    NULL,
            [GST Registration Number]       NVARCHAR(20)    NULL,
            [GST Treatment Code]            NVARCHAR(10)    NULL,
            [Business Number Type]          NVARCHAR(10)    NULL,
            [Local Script Name]             NVARCHAR(200)   NULL,
            [Postal Format Code]            NVARCHAR(10)    NULL,
            [Consent Basis Code]            NVARCHAR(20)    NULL,
            [Marketing Consent Flag]        BIT             NULL,
            [Profiling Consent Flag]        BIT             NULL,
            [Consent Captured On]           DATETIME2(7)    NULL,
            [Consent Source Code]           NVARCHAR(20)    NULL,
            [Erasure Requested On]          DATETIME2(7)    NULL,
            [Retention Expiry Date]         DATE            NULL,
            [Is Pseudonymized]              BIT             NULL,
            [Source Changed On]             DATETIME2(7)    NULL,
            [Row Hash Type 2]               VARBINARY(32)   NULL,
            [Row Hash Type 1]               VARBINARY(32)   NULL
        );

        IF @RegionCode = N'NA'
        BEGIN
            INSERT INTO #CustomerConditioned
                ([WWI Customer ID], [Customer], [Bill To Customer], [Category], [Buying Group],
                 [Primary Contact], [Postal Code], [Country Code], [Source Customer Reference],
                 [Credit Limit Amount], [Credit Limit Currency Code], [Payment Terms Code],
                 [Account Status Code], [Account Opened Date], [Standard Discount Percentage],
                 [Phone Number Standardized], [Website URL], [Sales Tax Jurisdiction Code],
                 [Sales Tax Exempt Flag], [ZIP Plus Four], [State Province Code],
                 [Consent Basis Code], [Marketing Consent Flag], [Profiling Consent Flag],
                 [Consent Captured On], [Consent Source Code], [Erasure Requested On],
                 [Retention Expiry Date], [Is Pseudonymized], [Source Changed On])
            SELECT
                  s.[WWI Customer ID]
                , s.[Customer]
                , s.[Bill To Customer]
                , s.[Category]
                , s.[Buying Group]
                , s.[Primary Contact]
                , LEFT(REPLACE(s.[Postal Code], N'-', N''), 5)
                , s.[Country Code]
                , s.[Source Customer Reference]
                , s.[Credit Limit Amount]
                , ISNULL(s.[Credit Limit Currency Code], N'USD')
                , s.[Payment Terms Code]
                , s.[Account Status Code]
                , s.[Account Opened Date]
                , s.[Standard Discount Percentage]
                  -- 1-NPA-NXX-XXXX, digits only, leading 1 stripped
                , RIGHT(N'0000000000',
                        LEN(REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(s.[Phone Number Raw], N''),
                            N'(', N''), N')', N''), N'-', N''), N' ', N'')))
                , s.[Website URL]
                  -- jurisdiction stack: state + county + city, padded, as the tax engine expects
                , CONCAT(ISNULL(s.[State Province Code], N'XX'), N'-',
                         LEFT(ISNULL(s.[County Name], N'UNK'), 3), N'-',
                         LEFT(ISNULL(s.[City Name], N'UNK'), 3))
                , CASE WHEN s.[Account Status Code] = N'X' THEN 0 ELSE NULL END
                , CASE WHEN CHARINDEX(N'-', s.[Postal Code]) > 0 THEN s.[Postal Code] ELSE NULL END
                , s.[State Province Code]
                  -- implied consent unless the source says otherwise
                , ISNULL(s.[Consent Basis Code], N'OPTOUT')
                , ISNULL(s.[Marketing Consent Flag], 1)
                , ISNULL(s.[Profiling Consent Flag], 1)
                , s.[Consent Captured On]
                , ISNULL(s.[Consent Source Code], N'IMPORT')
                , s.[Erasure Requested On]
                , DATEADD(YEAR, @RetentionYears, CONVERT(DATE, ISNULL(s.[Source Changed On], SYSDATETIME())))
                , 0
                , s.[Source Changed On]
            FROM #CustomerSource AS s;
        END;

        IF @RegionCode = N'EU'
        BEGIN
            INSERT INTO #CustomerConditioned
                ([WWI Customer ID], [Customer], [Bill To Customer], [Category], [Buying Group],
                 [Primary Contact], [Postal Code], [Country Code], [Source Customer Reference],
                 [Credit Limit Amount], [Credit Limit Currency Code], [Payment Terms Code],
                 [Account Status Code], [Account Opened Date], [Standard Discount Percentage],
                 [Phone Number Standardized], [Website URL], [VAT Registration Number],
                 [VAT Rate Category], [Is Reverse Charge Applicable], [EU Member State Code],
                 [Postcode Standardized], [Consent Basis Code], [Marketing Consent Flag],
                 [Profiling Consent Flag], [Consent Captured On], [Consent Source Code],
                 [Erasure Requested On], [Retention Expiry Date], [Is Pseudonymized],
                 [Source Changed On])
            SELECT
                  s.[WWI Customer ID]
                  -- erasure requests pseudonymise the name at load time
                , CASE WHEN s.[Erasure Requested On] IS NOT NULL
                       THEN CONCAT(N'ERASED-', CONVERT(NVARCHAR(12), s.[WWI Customer ID]))
                       ELSE s.[Customer] END
                , CASE WHEN s.[Erasure Requested On] IS NOT NULL
                       THEN CONCAT(N'ERASED-', CONVERT(NVARCHAR(12), s.[WWI Customer ID]))
                       ELSE s.[Bill To Customer] END
                , s.[Category]
                , s.[Buying Group]
                , CASE WHEN s.[Erasure Requested On] IS NOT NULL THEN N'ERASED' ELSE s.[Primary Contact] END
                , LEFT(UPPER(REPLACE(s.[Postal Code], N' ', N'')), 10)
                , s.[Country Code]
                , s.[Source Customer Reference]
                , s.[Credit Limit Amount]
                , ISNULL(s.[Credit Limit Currency Code], N'EUR')
                , s.[Payment Terms Code]
                , s.[Account Status Code]
                , s.[Account Opened Date]
                , s.[Standard Discount Percentage]
                , CASE WHEN s.[Erasure Requested On] IS NOT NULL THEN NULL
                       ELSE LEFT(REPLACE(REPLACE(ISNULL(s.[Phone Number Raw], N''), N' ', N''), N'-', N''), 30) END
                , s.[Website URL]
                , UPPER(REPLACE(ISNULL(s.[Tax Registration], N''), N' ', N''))
                , CASE WHEN s.[Tax Registration] IS NULL THEN N'EXE' ELSE N'STD' END
                  -- cross-border B2B inside the union reverse charges
                , CASE WHEN s.[Tax Registration] IS NOT NULL
                        AND LEFT(s.[Country Code], 2) <> N'NL' THEN 1 ELSE 0 END
                , LEFT(s.[Country Code], 2)
                , UPPER(REPLACE(s.[Postal Code], N' ', N''))
                  -- no explicit opt-in means no consent, whatever the source claims
                , CASE WHEN s.[Consent Basis Code] = N'OPTIN' THEN N'OPTIN' ELSE N'CONTRACT' END
                , CASE WHEN s.[Consent Basis Code] = N'OPTIN' THEN ISNULL(s.[Marketing Consent Flag], 0) ELSE 0 END
                , CASE WHEN s.[Consent Basis Code] = N'OPTIN' THEN ISNULL(s.[Profiling Consent Flag], 0) ELSE 0 END
                , s.[Consent Captured On]
                , s.[Consent Source Code]
                , s.[Erasure Requested On]
                , DATEADD(YEAR, @RetentionYears, CONVERT(DATE, ISNULL(s.[Source Changed On], SYSDATETIME())))
                , CASE WHEN s.[Erasure Requested On] IS NOT NULL THEN 1 ELSE 0 END
                , s.[Source Changed On]
            FROM #CustomerSource AS s;
        END;

        IF @RegionCode = N'APAC'
        BEGIN
            INSERT INTO #CustomerConditioned
                ([WWI Customer ID], [Customer], [Bill To Customer], [Category], [Buying Group],
                 [Primary Contact], [Postal Code], [Country Code], [Source Customer Reference],
                 [Credit Limit Amount], [Credit Limit Currency Code], [Payment Terms Code],
                 [Account Status Code], [Account Opened Date], [Standard Discount Percentage],
                 [Phone Number Standardized], [Website URL], [GST Registration Number],
                 [GST Treatment Code], [Business Number Type], [Local Script Name],
                 [Postal Format Code], [Consent Basis Code], [Marketing Consent Flag],
                 [Profiling Consent Flag], [Consent Captured On], [Consent Source Code],
                 [Erasure Requested On], [Retention Expiry Date], [Is Pseudonymized],
                 [Source Changed On])
            SELECT
                  s.[WWI Customer ID]
                , s.[Customer]
                , s.[Bill To Customer]
                , s.[Category]
                , s.[Buying Group]
                , s.[Primary Contact]
                  -- several APAC markets have no postcode; leave it null rather than invent one
                , CASE WHEN s.[Country Code] IN (N'HKG', N'ARE', N'PAN') THEN NULL
                       ELSE LEFT(s.[Postal Code], 10) END
                , s.[Country Code]
                , s.[Source Customer Reference]
                , s.[Credit Limit Amount]
                , ISNULL(s.[Credit Limit Currency Code], N'SGD')
                , s.[Payment Terms Code]
                , s.[Account Status Code]
                , s.[Account Opened Date]
                , s.[Standard Discount Percentage]
                , LEFT(CONCAT(N'+', REPLACE(ISNULL(s.[Phone Number Raw], N''), N' ', N'')), 30)
                , s.[Website URL]
                , UPPER(REPLACE(ISNULL(s.[Tax Registration], N''), N' ', N''))
                , CASE
                      WHEN s.[Tax Registration] IS NULL             THEN N'OOS'
                      WHEN s.[Business Number Type] = N'GSTIN'      THEN N'TAX'
                      WHEN s.[Business Number Type] = N'ABN'        THEN N'TAX'
                      WHEN s.[Business Number Type] = N'UEN'        THEN N'TAX'
                      WHEN s.[Business Number Type] = N'BRN'        THEN N'ZRL'
                      ELSE N'EXM'
                  END
                , s.[Business Number Type]
                , s.[Local Script Name]
                , CASE
                      WHEN s.[Country Code] IN (N'SGP', N'IND') THEN N'NUM6'
                      WHEN s.[Country Code] IN (N'AUS', N'NZL') THEN N'NUM4'
                      WHEN s.[Country Code] IN (N'HKG')         THEN N'NONE'
                      ELSE N'ALPHA'
                  END
                  -- consent is per channel here; marketing may be granted while profiling is not
                , ISNULL(s.[Consent Basis Code], N'OPTIN')
                , ISNULL(s.[Marketing Consent Flag], 0)
                , ISNULL(s.[Profiling Consent Flag], 0)
                , s.[Consent Captured On]
                , ISNULL(s.[Consent Source Code], N'WEB')
                , s.[Erasure Requested On]
                , DATEADD(YEAR, @RetentionYears, CONVERT(DATE, ISNULL(s.[Source Changed On], SYSDATETIME())))
                , 0
                , s.[Source Changed On]
            FROM #CustomerSource AS s;
        END;

        /*
            Reference lookups. A miss lands on the unknown member, never on NULL.
            The city lookup was removed in 2019: joining on postal code matched
            several city rows for the shared codes and doubled the customer count,
            so both city keys are set to the unknown member and the delivery city
            is resolved by the sales fact load instead.
        */
        UPDATE cc
        SET cc.[Customer Category Key] = ISNULL(cat.[Customer Category Key], -1),
            cc.[Buying Group Key]      = ISNULL(bg.[Buying Group Key], -1),
            cc.[Postal City Key]       = -1,
            cc.[Delivery City Key]     = -1
        FROM #CustomerConditioned AS cc
        LEFT OUTER JOIN [Dimension].[Customer Category] AS cat
            ON cat.[Category Code] = cc.[Category]
        LEFT OUTER JOIN [Dimension].[Buying Group] AS bg
            ON  bg.[Buying Group Code] = cc.[Buying Group]
            AND bg.[Is Current Row]    = 1;

        /* Change-detection hashes. Type 2 set and Type 1 set are hashed separately. */
        UPDATE #CustomerConditioned
        SET [Row Hash Type 2] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|',
                    ISNULL([Customer], N''), ISNULL([Bill To Customer], N''),
                    ISNULL([Category], N''), ISNULL([Buying Group], N''),
                    ISNULL([Postal Code], N''), ISNULL([Country Code], N''),
                    ISNULL(CONVERT(NVARCHAR(20), [Credit Limit Amount]), N''),
                    ISNULL([Sales Tax Jurisdiction Code], N''),
                    ISNULL([VAT Registration Number], N''),
                    ISNULL([GST Registration Number], N''),
                    ISNULL([GST Treatment Code], N''),
                    ISNULL([Account Status Code], N''))),
            [Row Hash Type 1] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|',
                    ISNULL([Primary Contact], N''), ISNULL([Phone Number Standardized], N''),
                    ISNULL([Website URL], N''), ISNULL([Consent Basis Code], N''),
                    ISNULL(CONVERT(NVARCHAR(1), [Marketing Consent Flag]), N''),
                    ISNULL(CONVERT(NVARCHAR(1), [Profiling Consent Flag]), N''),
                    ISNULL(CONVERT(NVARCHAR(30), [Erasure Requested On], 126), N''),
                    ISNULL(CONVERT(NVARCHAR(20), [Standard Discount Percentage]), N'')));

        /* ---------------------------------------------------------------
           Type 1: overwrite on EVERY version, current or not.
           --------------------------------------------------------------- */
        UPDATE d
        SET d.[Primary Contact]             = cc.[Primary Contact],
            d.[Phone Number Standardized]   = cc.[Phone Number Standardized],
            d.[Website URL]                 = cc.[Website URL],
            d.[Standard Discount Percentage] = cc.[Standard Discount Percentage],
            d.[Consent Basis Code]          = cc.[Consent Basis Code],
            d.[Marketing Consent Flag]      = cc.[Marketing Consent Flag],
            d.[Profiling Consent Flag]      = cc.[Profiling Consent Flag],
            d.[Consent Captured On]         = cc.[Consent Captured On],
            d.[Consent Source Code]         = cc.[Consent Source Code],
            d.[Erasure Requested On]        = cc.[Erasure Requested On],
            d.[Retention Expiry Date]       = cc.[Retention Expiry Date],
            d.[Is Pseudonymized]            = cc.[Is Pseudonymized],
            d.[Row Hash Type 1]             = cc.[Row Hash Type 1],
            d.[Last Load Batch Id]          = @BatchId,
            d.[Last Load Package Execution Id] = @PackageExecutionId
        FROM [Dimension].[Customer] AS d
        INNER JOIN #CustomerConditioned AS cc
            ON cc.[WWI Customer ID] = d.[WWI Customer ID]
        WHERE d.[Customer Key] > 0
          AND (d.[Row Hash Type 1] IS NULL OR d.[Row Hash Type 1] <> cc.[Row Hash Type 1]);

        SET @Type1UpdateCount = @@ROWCOUNT;

        /* EU erasure also rewrites the name columns on historical versions. */
        IF @RegionCode = N'EU'
            UPDATE d
            SET d.[Customer]            = cc.[Customer],
                d.[Bill To Customer]    = cc.[Bill To Customer],
                d.[Is Pseudonymized]    = 1
            FROM [Dimension].[Customer] AS d
            INNER JOIN #CustomerConditioned AS cc
                ON cc.[WWI Customer ID] = d.[WWI Customer ID]
            WHERE d.[Customer Key] > 0
              AND cc.[Erasure Requested On] IS NOT NULL
              AND ISNULL(d.[Is Pseudonymized], 0) = 0;

        /* ---------------------------------------------------------------
           Type 2 close-out. Hand-rolled: close the current row, then insert
           the new version. MERGE is not used here because the 2014 attempt
           to do the close and the insert in one MERGE hit the "cannot update
           the same row twice" error whenever a customer changed twice in one
           extract, which happens every month-end.
           --------------------------------------------------------------- */
        SELECT @SameDayCount = COUNT_BIG(*)
        FROM [Dimension].[Customer] AS d
        INNER JOIN #CustomerConditioned AS cc
            ON  cc.[WWI Customer ID] = d.[WWI Customer ID]
        WHERE d.[Is Current Row] = 1
          AND d.[Customer Key] > 0
          AND d.[Row Hash Type 2] <> cc.[Row Hash Type 2]
          AND d.[Effective From Date] = CONVERT(DATE, ISNULL(cc.[Source Changed On], @LoadStartedAt));

        UPDATE d
        SET d.[Is Current Row]      = 0,
            d.[Effective To]        = ISNULL(cc.[Source Changed On], @LoadStartedAt),
            d.[Valid To]            = ISNULL(cc.[Source Changed On], @LoadStartedAt),
            d.[Last Load Batch Id]  = @BatchId
        FROM [Dimension].[Customer] AS d
        INNER JOIN #CustomerConditioned AS cc
            ON cc.[WWI Customer ID] = d.[WWI Customer ID]
        WHERE d.[Is Current Row] = 1
          AND d.[Customer Key] > 0
          AND ISNULL(d.[Is Inferred Member], 0) = 0
          AND d.[Row Hash Type 2] <> cc.[Row Hash Type 2];

        SET @Type2CloseCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Customer]
            ([WWI Customer ID], [Customer], [Bill To Customer], [Category], [Buying Group],
             [Primary Contact], [Postal Code], [Source System Code], [Source Customer Reference],
             [Region Code], [Country Code], [Customer Category Key], [Buying Group Key],
             [Delivery City Key], [Postal City Key], [Account Opened Date], [Account Status Code],
             [Credit Limit Amount], [Credit Limit Currency Code], [Payment Terms Code],
             [Standard Discount Percentage], [Website URL], [Phone Number Standardized],
             [Sales Tax Jurisdiction Code], [Sales Tax Exempt Flag], [ZIP Plus Four],
             [State Province Code], [VAT Registration Number], [VAT Rate Category],
             [Is Reverse Charge Applicable], [EU Member State Code], [Postcode Standardized],
             [GST Registration Number], [GST Treatment Code], [Business Number Type],
             [Local Script Name], [Postal Format Code], [Consent Basis Code],
             [Marketing Consent Flag], [Profiling Consent Flag], [Consent Captured On],
             [Consent Source Code], [Erasure Requested On], [Retention Expiry Date],
             [Is Pseudonymized], [Effective From], [Effective To], [Effective From Date],
             [Effective Sequence], [Is Current Row], [Version Number], [Row Hash Type 2],
             [Row Hash Type 1], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id], [Last Load Package Execution Id])
        SELECT
              cc.[WWI Customer ID]
            , cc.[Customer]
            , cc.[Bill To Customer]
            , ISNULL(cc.[Category], N'Unknown')
            , ISNULL(cc.[Buying Group], N'Unknown')
            , ISNULL(cc.[Primary Contact], N'Unknown')
            , ISNULL(cc.[Postal Code], N'N/A')
            , @SourceSystemCode
            , cc.[Source Customer Reference]
            , @RegionCode
            , cc.[Country Code]
            , cc.[Customer Category Key]
            , cc.[Buying Group Key]
            , cc.[Delivery City Key]
            , cc.[Postal City Key]
            , cc.[Account Opened Date]
            , cc.[Account Status Code]
            , cc.[Credit Limit Amount]
            , cc.[Credit Limit Currency Code]
            , cc.[Payment Terms Code]
            , cc.[Standard Discount Percentage]
            , cc.[Website URL]
            , cc.[Phone Number Standardized]
            , cc.[Sales Tax Jurisdiction Code]
            , cc.[Sales Tax Exempt Flag]
            , cc.[ZIP Plus Four]
            , cc.[State Province Code]
            , cc.[VAT Registration Number]
            , cc.[VAT Rate Category]
            , cc.[Is Reverse Charge Applicable]
            , cc.[EU Member State Code]
            , cc.[Postcode Standardized]
            , cc.[GST Registration Number]
            , cc.[GST Treatment Code]
            , cc.[Business Number Type]
            , cc.[Local Script Name]
            , cc.[Postal Format Code]
            , cc.[Consent Basis Code]
            , cc.[Marketing Consent Flag]
            , cc.[Profiling Consent Flag]
            , cc.[Consent Captured On]
            , cc.[Consent Source Code]
            , cc.[Erasure Requested On]
            , cc.[Retention Expiry Date]
            , cc.[Is Pseudonymized]
            , ISNULL(cc.[Source Changed On], @LoadStartedAt)
            , @HighDate
            , CONVERT(DATE, ISNULL(cc.[Source Changed On], @LoadStartedAt))
            , ISNULL(prior.[Same Day Count], 0) + 1
            , 1
            , ISNULL(prior.[Max Version], 0) + 1
            , cc.[Row Hash Type 2]
            , cc.[Row Hash Type 1]
            , 0
            , ISNULL(cc.[Source Changed On], @LoadStartedAt)
            , @HighDate
            , @LineageKey
            , @BatchId
            , @PackageExecutionId
        FROM #CustomerConditioned AS cc
        OUTER APPLY
        (
            SELECT
                  MAX(d.[Version Number]) AS [Max Version]
                , SUM(CASE WHEN d.[Effective From Date] = CONVERT(DATE, ISNULL(cc.[Source Changed On], @LoadStartedAt))
                           THEN 1 ELSE 0 END) AS [Same Day Count]
            FROM [Dimension].[Customer] AS d
            WHERE d.[WWI Customer ID] = cc.[WWI Customer ID]
              AND d.[Customer Key] > 0
        ) AS prior
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Customer] AS cur
            WHERE cur.[WWI Customer ID] = cc.[WWI Customer ID]
              AND cur.[Is Current Row]  = 1
              AND cur.[Customer Key]    > 0
              AND cur.[Row Hash Type 2] = cc.[Row Hash Type 2]
              AND ISNULL(cur.[Is Inferred Member], 0) = 0
        );

        SET @Type2InsertCount = @@ROWCOUNT;

        SELECT @NewMemberCount = COUNT_BIG(*)
        FROM [Dimension].[Customer] AS d
        WHERE d.[Last Load Batch Id] = @BatchId
          AND d.[Version Number]     = 1
          AND d.[Customer Key]       > 0;

        /* ---------------------------------------------------------------
           Late-arriving members. A fact load may already have inserted a
           stub; enrich it in place so the surrogate key the fact carries
           stays valid. The stub keeps its key, its version stays 1, and
           [Is Inferred Member] is cleared.
           --------------------------------------------------------------- */
        UPDATE d
        SET d.[Customer]                    = cc.[Customer],
            d.[Bill To Customer]            = cc.[Bill To Customer],
            d.[Category]                    = ISNULL(cc.[Category], d.[Category]),
            d.[Buying Group]                = ISNULL(cc.[Buying Group], d.[Buying Group]),
            d.[Postal Code]                 = ISNULL(cc.[Postal Code], d.[Postal Code]),
            d.[Country Code]                = cc.[Country Code],
            d.[Customer Category Key]       = cc.[Customer Category Key],
            d.[Buying Group Key]            = cc.[Buying Group Key],
            d.[Credit Limit Amount]         = cc.[Credit Limit Amount],
            d.[Sales Tax Jurisdiction Code] = cc.[Sales Tax Jurisdiction Code],
            d.[VAT Registration Number]     = cc.[VAT Registration Number],
            d.[GST Registration Number]     = cc.[GST Registration Number],
            d.[Row Hash Type 2]             = cc.[Row Hash Type 2],
            d.[Row Hash Type 1]             = cc.[Row Hash Type 1],
            d.[Is Inferred Member]          = 0,
            d.[Enriched On]                 = SYSDATETIME(),
            d.[Last Load Batch Id]          = @BatchId
        FROM [Dimension].[Customer] AS d
        INNER JOIN #CustomerConditioned AS cc
            ON cc.[WWI Customer ID] = d.[WWI Customer ID]
        WHERE d.[Is Inferred Member] = 1
          AND d.[Is Current Row]     = 1;

        SET @EnrichedCount = @@ROWCOUNT;

        UPDATE q
        SET q.[Enrichment Status]   = N'Enriched',
            q.[Enriched On]         = SYSDATETIME(),
            q.[Last Attempt Note]   = CONCAT(N'Enriched by batch ', CONVERT(NVARCHAR(20), @BatchId))
        FROM [Integration].[InferredMemberQueue] AS q
        INNER JOIN #CustomerConditioned AS cc
            ON cc.[WWI Customer ID] = TRY_CONVERT(INT, q.[Business Key])
        WHERE q.[Dimension Name] = N'Customer'
          AND q.[Enrichment Status] = N'Pending';

        /* ---------------------------------------------------------------
           Audit, watermark and control logging.
           --------------------------------------------------------------- */
        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [New Member Count], [Inferred Enriched Count],
             [Reject Count], [Same Day Change Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Customer', @RegionCode, @BatchId, @PackageExecutionId,
             @SourceRowCount, @Type1UpdateCount, @Type2CloseCount,
             @Type2InsertCount, @NewMemberCount, @EnrichedCount,
             @RejectRowCount, @SameDayCount, N'HybridSCD', @LoadStartedAt, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Customer',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @Type2InsertCount,
             @UpdateRowCount     = @Type1UpdateCount,
             @RejectRowCount     = @RejectRowCount;

        SELECT @WatermarkTo = CONVERT(NVARCHAR(50), MAX([Source Changed On]), 126)
        FROM #CustomerConditioned;

        IF @WatermarkTo IS NOT NULL
            EXEC [etl].[usp_SetWatermark]
                 @SourceSystemCode = @SourceSystemCode,
                 @ObjectName       = N'Dimension.Customer',
                 @WatermarkTo      = @WatermarkTo,
                 @PackageExecutionId = @PackageExecutionId;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @Type2InsertCount,
             @RowsUpdated        = @Type1UpdateCount,
             @RowsRejected       = @RejectRowCount,
             @WatermarkFrom      = @WatermarkFrom,
             @WatermarkTo        = @WatermarkTo;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        IF CURSOR_STATUS(N'local', N'CustomerRejectCursor') >= 0
        BEGIN
            CLOSE CustomerRejectCursor;
            DEALLOCATE CustomerRejectCursor;
        END;

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @ErrorCode          = 51001,
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Customer',
             @ProcedureName      = N'Integration.usp_MigrateStagedCustomerDataV2',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @SourceRowCount,
             @RowsRejected       = @RejectRowCount;

        THROW;
    END CATCH;
END;
GO
