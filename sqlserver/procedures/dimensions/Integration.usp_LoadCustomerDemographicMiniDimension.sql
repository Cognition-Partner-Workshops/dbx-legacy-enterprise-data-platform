/*
    Object        : [Integration].[usp_LoadCustomerDemographicMiniDimension]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.Customer, Dimension.Customer, Dimension.Customer Demographic,
                    the etl control framework
    Called by     : DIM_Refresh_CustomerDemographic, run after the customer
                    dimension load and before the sale fact load

    The mini dimension that keeps the fast-changing customer profile out of the
    Type 2 customer history. Before it existed (2008) a credit-band change created
    a new customer version and the dimension grew by roughly forty thousand rows a
    quarter for attributes nobody reported on at customer grain.

    Two things happen here:
      1. Every distinct band combination seen in staging is added to the mini
         dimension if it is new. Profiles are never deleted - a fact row points at
         the profile that was current when it was loaded and that must stay
         resolvable.
      2. [Dimension].[Customer].[Customer Demographic Key] is repointed to the
         current profile. That is a Type 1 overwrite on a Type 2 dimension, so the
         historic customer versions end up pointing at the *current* profile too.
         It is wrong and it is known: the correct fix is to carry the demographic
         key on the fact, which the sale fact does and the older order fact does
         not.

    Band definitions diverge by region and were re-cut twice, hence
    [Band Definition Version]:
      NA    credit bands in USD, credit score band from the FICO range
      EU    credit bands in EUR at the rate frozen in 2019, agency letter grades
      APAC  credit bands in USD equivalent, internal 1-5 score, and no profiling
            band at all where profiling consent is absent
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_LoadCustomerDemographicMiniDimension]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10)  = N'GLOBAL',
    @PackageName        NVARCHAR(200) = N'DIM_Refresh_CustomerDemographic',
    @BandVersion        NVARCHAR(10)  = N'V2020',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @RepointedCount     BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'MiniDimension',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#Profile') IS NOT NULL DROP TABLE #Profile;

        SELECT
              c.[WWICustomerID]                                  AS [WWI Customer ID]
            , UPPER(ISNULL(c.[RegionCode], N'NA'))               AS [Region Code]
            , CASE
                  WHEN ISNULL(c.[CreditLimitAmount], 0) <= 0        THEN N'0'
                  WHEN c.[CreditLimitAmount] < 5000                 THEN N'1-5K'
                  WHEN c.[CreditLimitAmount] < 25000                THEN N'5-25K'
                  WHEN c.[CreditLimitAmount] < 100000               THEN N'25-100K'
                  ELSE N'100K+'
              END                                                AS [Credit Limit Band]
            , CASE UPPER(ISNULL(c.[RegionCode], N'NA'))
                  WHEN N'EU' THEN
                      CASE
                          WHEN c.[CreditScore] IS NULL       THEN N'UNRATED'
                          WHEN c.[CreditScore] >= 80         THEN N'A'
                          WHEN c.[CreditScore] >= 60         THEN N'B'
                          WHEN c.[CreditScore] >= 40         THEN N'C'
                          ELSE N'D'
                      END
                  WHEN N'APAC' THEN
                      CASE
                          WHEN c.[CreditScore] IS NULL       THEN N'UNRATED'
                          WHEN c.[CreditScore] >= 80         THEN N'1'
                          WHEN c.[CreditScore] >= 60         THEN N'2'
                          WHEN c.[CreditScore] >= 40         THEN N'3'
                          WHEN c.[CreditScore] >= 20         THEN N'4'
                          ELSE N'5'
                      END
                  ELSE
                      /* NA carries the raw FICO score, banded the way the 2015
                         credit policy defined it. */
                      CASE
                          WHEN c.[CreditScore] IS NULL       THEN N'UNRATED'
                          WHEN c.[CreditScore] >= 740        THEN N'740+'
                          WHEN c.[CreditScore] >= 670        THEN N'670-739'
                          WHEN c.[CreditScore] >= 580        THEN N'580-669'
                          ELSE N'<580'
                      END
              END                                                AS [Credit Score Band]
            , CASE
                  WHEN ISNULL(c.[IsOnCreditHold], 0) = 1             THEN N'WATCH'
                  WHEN ISNULL(c.[DaysPastDue], 0) > 90               THEN N'DEFAULT'
                  WHEN ISNULL(c.[DaysPastDue], 0) > 30               THEN N'HIGH'
                  WHEN ISNULL(c.[CreditLimitAmount], 0) >= 100000    THEN N'LOW'
                  ELSE N'MED'
              END                                                AS [Credit Risk Class]
            , CASE
                  WHEN ISNULL(c.[TrailingTwelveMonthSpend], 0) = 0   THEN N'NONE'
                  WHEN c.[TrailingTwelveMonthSpend] < 10000          THEN N'<10K'
                  WHEN c.[TrailingTwelveMonthSpend] < 50000          THEN N'10-50K'
                  WHEN c.[TrailingTwelveMonthSpend] < 250000         THEN N'50-250K'
                  ELSE N'250K+'
              END                                                AS [Annual Spend Band]
            , CASE
                  WHEN ISNULL(c.[OrdersLast365], 0) >= 52            THEN N'WEEKLY'
                  WHEN c.[OrdersLast365] >= 26                       THEN N'FORTNIGHTLY'
                  WHEN c.[OrdersLast365] >= 12                       THEN N'MONTHLY'
                  WHEN c.[OrdersLast365] >= 4                        THEN N'QUARTERLY'
                  ELSE N'RARE'
              END                                                AS [Order Frequency Band]
            , CASE
                  WHEN ISNULL(c.[OrdersLast365], 0) = 0              THEN N'NONE'
                  WHEN ISNULL(c.[TrailingTwelveMonthSpend], 0)
                       / NULLIF(c.[OrdersLast365], 0) < 250          THEN N'<250'
                  WHEN ISNULL(c.[TrailingTwelveMonthSpend], 0)
                       / NULLIF(c.[OrdersLast365], 0) < 1000         THEN N'250-1K'
                  WHEN ISNULL(c.[TrailingTwelveMonthSpend], 0)
                       / NULLIF(c.[OrdersLast365], 0) < 5000         THEN N'1-5K'
                  ELSE N'5K+'
              END                                                AS [Average Order Value Band]
            , CASE
                  WHEN c.[AccountOpenedDate] IS NULL                              THEN N'UNKNOWN'
                  WHEN DATEDIFF(DAY, c.[AccountOpenedDate], @Now) < 365           THEN N'<1Y'
                  WHEN DATEDIFF(DAY, c.[AccountOpenedDate], @Now) < 365 * 3       THEN N'1-3Y'
                  WHEN DATEDIFF(DAY, c.[AccountOpenedDate], @Now) < 365 * 5       THEN N'3-5Y'
                  WHEN DATEDIFF(DAY, c.[AccountOpenedDate], @Now) < 365 * 10      THEN N'5-10Y'
                  ELSE N'10Y+'
              END                                                AS [Tenure Band]
            , CASE
                  WHEN c.[LastOrderDate] IS NULL                                  THEN N'NEVER'
                  WHEN DATEDIFF(DAY, c.[LastOrderDate], @Now) <= 30               THEN N'0-30D'
                  WHEN DATEDIFF(DAY, c.[LastOrderDate], @Now) <= 90               THEN N'31-90D'
                  WHEN DATEDIFF(DAY, c.[LastOrderDate], @Now) <= 365              THEN N'91-365D'
                  ELSE N'365D+'
              END                                                AS [Recency Band]
            , CASE
                  WHEN ISNULL(c.[DaysPastDue], 0) > 60               THEN N'DELINQUENT'
                  WHEN c.[DaysPastDue] > 30                          THEN N'LATE60'
                  WHEN c.[DaysPastDue] > 0                           THEN N'LATE30'
                  WHEN ISNULL(c.[AveragePaymentDays], 0) < 0         THEN N'EARLY'
                  ELSE N'ONTIME'
              END                                                AS [Payment Behaviour Band]
            , CASE
                  WHEN ISNULL(c.[ReturnRatePercent], 0) = 0          THEN N'0'
                  WHEN c.[ReturnRatePercent] < 2                     THEN N'<2PCT'
                  WHEN c.[ReturnRatePercent] < 5                     THEN N'2-5PCT'
                  WHEN c.[ReturnRatePercent] < 10                    THEN N'5-10PCT'
                  ELSE N'10PCT+'
              END                                                AS [Return Rate Band]
            , UPPER(ISNULL(c.[PreferredChannelCode], N'UNKNOWN'))  AS [Channel Preference Code]   -- added to staging in 2012
            , UPPER(NULLIF(LTRIM(RTRIM(c.[LoyaltyTierCode])), N'')) AS [Loyalty Tier Code]
            /* EU treats an absent consent record as no consent; NA and APAC
               inherited the opposite default from the 2006 CRM migration and it
               has never been restated. */
            , CONVERT(BIT, CASE
                  WHEN c.[MarketingConsentFlag] IS NOT NULL THEN c.[MarketingConsentFlag]
                  WHEN UPPER(ISNULL(c.[RegionCode], N'NA')) = N'EU' THEN 0
                  ELSE 1 END)                                  AS [Marketing Consent Flag]
            , CONVERT(BIT, CASE
                  WHEN c.[ProfilingConsentFlag] IS NOT NULL THEN c.[ProfilingConsentFlag]
                  WHEN UPPER(ISNULL(c.[RegionCode], N'NA')) = N'EU' THEN 0
                  ELSE 1 END)                                  AS [Profiling Consent Flag]
        INTO #Profile
        FROM [stg].[Customer] AS c
        WHERE @RegionCode = N'GLOBAL'
           OR UPPER(ISNULL(c.[RegionCode], N'NA')) = @RegionCode;

        SET @SourceRowCount = @@ROWCOUNT;

        /* APAC without profiling consent gets a coarsened profile - the bands
           that are behavioural rather than contractual are blanked out. This is
           the 2019 privacy review's compromise; EU customers without consent are
           excluded from the mini dimension entirely by the customer load. */
        UPDATE #Profile
        SET [Recency Band]           = N'SUPPRESSED',
            [Order Frequency Band]   = N'SUPPRESSED',
            [Average Order Value Band] = N'SUPPRESSED',
            [Return Rate Band]       = N'SUPPRESSED'
        WHERE [Region Code] = N'APAC'
          AND [Profiling Consent Flag] = 0;

        ALTER TABLE #Profile ADD [Profile Hash] VARBINARY(32) NULL;

        UPDATE #Profile
        SET [Profile Hash] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', [Region Code], [Credit Limit Band], [Credit Score Band],
                          [Credit Risk Class], [Annual Spend Band], [Order Frequency Band],
                          [Average Order Value Band], [Tenure Band], [Recency Band],
                          [Payment Behaviour Band], [Return Rate Band], [Channel Preference Code],
                          ISNULL([Loyalty Tier Code], N'~'),
                          CONVERT(NVARCHAR(1), [Marketing Consent Flag]),
                          CONVERT(NVARCHAR(1), [Profiling Consent Flag]),
                          @BandVersion));

        INSERT INTO [Dimension].[Customer Demographic]
            ([Region Code], [Credit Limit Band], [Credit Score Band], [Credit Risk Class],
             [Annual Spend Band], [Order Frequency Band], [Average Order Value Band],
             [Tenure Band], [Recency Band], [Payment Behaviour Band], [Return Rate Band],
             [Channel Preference Code], [Loyalty Tier Code], [Marketing Consent Flag],
             [Profiling Consent Flag], [Band Definition Version], [Profile Hash],
             [First Seen On], [Last Assigned On], [Assigned Customer Count], [Last Load Batch Id])
        SELECT DISTINCT
              p.[Region Code], p.[Credit Limit Band], p.[Credit Score Band], p.[Credit Risk Class]
            , p.[Annual Spend Band], p.[Order Frequency Band], p.[Average Order Value Band]
            , p.[Tenure Band], p.[Recency Band], p.[Payment Behaviour Band], p.[Return Rate Band]
            , p.[Channel Preference Code], p.[Loyalty Tier Code], p.[Marketing Consent Flag]
            , p.[Profiling Consent Flag], @BandVersion, p.[Profile Hash]
            , @Now, @Now, 0, @BatchId
        FROM #Profile AS p
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Customer Demographic] AS d
                          WHERE d.[Profile Hash] = p.[Profile Hash]);

        SET @InsertedCount = @@ROWCOUNT;

        /* Repoint the current customer rows. Historic versions are deliberately
           left alone here and are wrong for the same reason described above. */
        UPDATE c
        SET c.[Customer Demographic Key] = d.[Customer Demographic Key],
            c.[Last Load Batch Id]       = @BatchId
        FROM [Dimension].[Customer] AS c
        INNER JOIN #Profile AS p
            ON p.[WWI Customer ID] = c.[WWI Customer ID]
        INNER JOIN [Dimension].[Customer Demographic] AS d
            ON d.[Profile Hash] = p.[Profile Hash]
        WHERE c.[Is Current Row] = 1
          AND ISNULL(c.[Customer Demographic Key], -1) <> d.[Customer Demographic Key];

        SET @RepointedCount = @@ROWCOUNT;

        UPDATE d
        SET d.[Assigned Customer Count] = x.[Assigned],
            d.[Last Assigned On]        = @Now,
            d.[Last Load Batch Id]      = @BatchId
        FROM [Dimension].[Customer Demographic] AS d
        INNER JOIN (
            SELECT [Customer Demographic Key], COUNT_BIG(*) AS [Assigned]
            FROM [Dimension].[Customer]
            WHERE [Is Current Row] = 1
              AND [Customer Demographic Key] IS NOT NULL
            GROUP BY [Customer Demographic Key]
        ) AS x
            ON x.[Customer Demographic Key] = d.[Customer Demographic Key];

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [New Member Count],
             [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Customer Demographic', @RegionCode, @BatchId, @PackageExecutionId,
             @SourceRowCount, @RepointedCount, @InsertedCount, N'MiniDimension', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Customer Demographic',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @RepointedCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @RepointedCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Customer Demographic',
             @ProcedureName      = N'Integration.usp_LoadCustomerDemographicMiniDimension',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @SourceRowCount;

        THROW;
    END CATCH;
END;
GO
