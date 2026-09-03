/*
    Object        : [Integration].[usp_MigrateStagedCurrencyData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.Currency, stg.FxRate, Dimension.Currency,
                    the etl control framework
    Called by     : REF_Load_Currency (weekly reference load)

    Type 1 overwrite. Currency is the oldest reference load in the estate: it was
    written against the Oracle GL currency table in 2005, retro-fitted for the
    2002-1999 legacy EMU currencies in 2007 (which is why the fixed conversion
    rate and the superseded-by columns exist at all), and given the restricted
    currency handling in 2013 when the APAC entities started invoicing in CNY.

    The load is a single MERGE. Nothing here is versioned - if the Bank of Japan
    changed the minor unit digits for JPY the warehouse would simply forget the
    old value, and the finance team has been told this twice.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedCurrencyData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_Currency',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_GL',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @HighDate           DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @UpdatedCount       BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'ReferenceLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#CurrencySource') IS NOT NULL
            DROP TABLE #CurrencySource;

        SELECT
              UPPER(LTRIM(RTRIM(c.[CurrencyCode])))     AS [Currency Code]
            , c.[CurrencyNumericCode]                   AS [Currency Numeric Code]
            , c.[CurrencyName]                          AS [Currency Name]
            , c.[CurrencySymbol]                        AS [Currency Symbol]
            , CONVERT(SMALLINT, ISNULL(c.[MinorUnitDigits], 2)) AS [Minor Unit Digits]
            /* Rounding rule is not in the source; it is derived the way the 2005
               author derived it, from the minor unit digits and a hard-coded list. */
            , CASE
                  WHEN UPPER(c.[CurrencyCode]) IN (N'JPY', N'KRW', N'VND', N'IDR') THEN N'NEAREST'
                  WHEN ISNULL(c.[MinorUnitDigits], 2) = 0                          THEN N'UP'
                  WHEN UPPER(c.[CurrencyCode]) = N'CHF'                            THEN N'BANKERS'
                  ELSE N'HALFUP'
              END                                       AS [Rounding Rule Code]
            , CASE
                  WHEN UPPER(c.[CurrencyCode]) = N'CHF' THEN CONVERT(DECIMAL(18, 6), 0.05)
                  WHEN ISNULL(c.[MinorUnitDigits], 2) = 0 THEN CONVERT(DECIMAL(18, 6), 1)
                  ELSE CONVERT(DECIMAL(18, 6), 0.01)
              END                                       AS [Rounding Increment]
            , c.[IsReportingCurrency]                   AS [Is Reporting Currency]
            , c.[IsTransactionalCurrency]               AS [Is Transactional Currency]
            , c.[IsHistorical]                          AS [Is Historical]
            , UPPER(NULLIF(LTRIM(RTRIM(c.[SupersededByCurrencyCode])), N''))
                                                        AS [Superseded By Currency Code]
            , c.[SupersededOn]                          AS [Superseded On]
            , c.[FixedConversionRate]                   AS [Fixed Conversion Rate]
            , ISNULL(c.[DefaultRateTypeCode], N'SPOT')  AS [Default Rate Type Code]
            , ISNULL(c.[RateSourceCode], N'ECB')        AS [Rate Source Code]
            , c.[InverseQuotation]                      AS [Inverse Quotation]
            , ISNULL(c.[DecimalSeparator], N'.')        AS [Decimal Separator]
            , ISNULL(c.[ThousandsSeparator], N',')      AS [Thousands Separator]
            , ISNULL(c.[SymbolPositionCode], N'PREFIX') AS [Symbol Position Code]
            /* Restricted (non-deliverable) currencies settle in a substitute.
               The list is maintained in the source but the 2013 fallback stayed. */
            , CASE
                  WHEN c.[IsRestrictedCurrency] = 1 THEN 1
                  WHEN UPPER(c.[CurrencyCode]) IN (N'CNY', N'INR', N'KRW', N'TWD', N'PHP') THEN 1
                  ELSE 0
              END                                       AS [Is Restricted Currency]
            , CASE
                  WHEN UPPER(c.[CurrencyCode]) IN (N'CNY', N'TWD') THEN N'USD'
                  WHEN UPPER(c.[CurrencyCode]) = N'INR'            THEN N'USD'
                  ELSE NULLIF(UPPER(LTRIM(RTRIM(c.[SettlementSubstituteCode]))), N'')
              END                                       AS [Settlement Substitute Code]
            , CASE WHEN c.[IsHistorical] = 1 THEN 0 ELSE 1 END AS [Is Active]
        INTO #CurrencySource
        FROM [stg].[Currency] AS c
        WHERE LEN(LTRIM(RTRIM(c.[CurrencyCode]))) = 3;

        SET @SourceRowCount = @@ROWCOUNT;

        /* A historical currency with no successor breaks the FX translation of any
           open balance carried in it, so it is reported but still loaded. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Currency',
               s.[Currency Code], N'HIST_CCY_NO_SUCCESSOR',
               N'Historical currency has no superseding currency code; FX translation will fall back to the reporting currency.',
               N'Reference', CONCAT(N'Name=', s.[Currency Name], N'|SupersededOn=',
                                    CONVERT(NVARCHAR(10), s.[Superseded On], 23))
        FROM #CurrencySource AS s
        WHERE s.[Is Historical] = 1
          AND s.[Superseded By Currency Code] IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        /* Rows that are not quoted anywhere in the last 400 days are demoted to
           non-transactional. The FX staging table is the only evidence available. */
        UPDATE s
        SET s.[Is Transactional Currency] = 0
        FROM #CurrencySource AS s
        WHERE s.[Is Transactional Currency] = 1
          AND NOT EXISTS (SELECT 1
                          FROM [stg].[FxRate] AS f
                          WHERE UPPER(f.[FromCurrencyCode]) = s.[Currency Code]
                            AND f.[RateDate] >= DATEADD(DAY, -400, CONVERT(DATE, @Now)));

        MERGE [Dimension].[Currency] WITH (HOLDLOCK) AS tgt
        USING #CurrencySource AS src
            ON tgt.[Currency Code] = src.[Currency Code]
           AND tgt.[Currency Key]  > 0
        WHEN MATCHED AND tgt.[Row Hash Type 1] <> HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(src.[Currency Name], N''),
                          ISNULL(src.[Currency Symbol], N''),
                          ISNULL(CONVERT(NVARCHAR(6), src.[Minor Unit Digits]), N''),
                          ISNULL(src.[Rounding Rule Code], N''),
                          ISNULL(src.[Superseded By Currency Code], N''),
                          ISNULL(CONVERT(NVARCHAR(30), src.[Fixed Conversion Rate]), N''),
                          ISNULL(CONVERT(NVARCHAR(1), src.[Is Restricted Currency]), N''),
                          ISNULL(CONVERT(NVARCHAR(1), src.[Is Active]), N'')))
            THEN UPDATE SET
                  tgt.[Currency Numeric Code]       = src.[Currency Numeric Code]
                , tgt.[Currency Name]               = src.[Currency Name]
                , tgt.[Currency Symbol]             = src.[Currency Symbol]
                , tgt.[Minor Unit Digits]           = src.[Minor Unit Digits]
                , tgt.[Rounding Rule Code]          = src.[Rounding Rule Code]
                , tgt.[Rounding Increment]          = src.[Rounding Increment]
                , tgt.[Is Reporting Currency]       = src.[Is Reporting Currency]
                , tgt.[Is Transactional Currency]   = src.[Is Transactional Currency]
                , tgt.[Is Historical]               = src.[Is Historical]
                , tgt.[Superseded By Currency Code] = src.[Superseded By Currency Code]
                , tgt.[Superseded On]               = src.[Superseded On]
                , tgt.[Fixed Conversion Rate]       = src.[Fixed Conversion Rate]
                , tgt.[Default Rate Type Code]      = src.[Default Rate Type Code]
                , tgt.[Rate Source Code]            = src.[Rate Source Code]
                , tgt.[Inverse Quotation]           = src.[Inverse Quotation]
                , tgt.[Decimal Separator]           = src.[Decimal Separator]
                , tgt.[Thousands Separator]         = src.[Thousands Separator]
                , tgt.[Symbol Position Code]        = src.[Symbol Position Code]
                , tgt.[Is Restricted Currency]      = src.[Is Restricted Currency]
                , tgt.[Settlement Substitute Code]  = src.[Settlement Substitute Code]
                , tgt.[Is Active]                   = src.[Is Active]
                , tgt.[Source System Code]          = @SourceSystemCode
                , tgt.[Row Hash Type 1]             = HASHBYTES(N'SHA2_256',
                      CONCAT_WS(N'|', ISNULL(src.[Currency Name], N''),
                                ISNULL(src.[Currency Symbol], N''),
                                ISNULL(CONVERT(NVARCHAR(6), src.[Minor Unit Digits]), N''),
                                ISNULL(src.[Rounding Rule Code], N''),
                                ISNULL(src.[Superseded By Currency Code], N''),
                                ISNULL(CONVERT(NVARCHAR(30), src.[Fixed Conversion Rate]), N''),
                                ISNULL(CONVERT(NVARCHAR(1), src.[Is Restricted Currency]), N''),
                                ISNULL(CONVERT(NVARCHAR(1), src.[Is Active]), N'')))
                , tgt.[Last Load Batch Id]          = @BatchId
        WHEN NOT MATCHED BY TARGET
            THEN INSERT
                ([Currency Code], [Currency Numeric Code], [Currency Name], [Currency Symbol],
                 [Minor Unit Digits], [Rounding Rule Code], [Rounding Increment],
                 [Is Reporting Currency], [Is Transactional Currency], [Is Historical],
                 [Superseded By Currency Code], [Superseded On], [Fixed Conversion Rate],
                 [Default Rate Type Code], [Rate Source Code], [Inverse Quotation],
                 [Decimal Separator], [Thousands Separator], [Symbol Position Code],
                 [Is Restricted Currency], [Settlement Substitute Code], [Is Active],
                 [Source System Code], [Row Hash Type 1], [Valid From], [Valid To],
                 [Lineage Key], [Last Load Batch Id])
            VALUES
                (src.[Currency Code], src.[Currency Numeric Code], src.[Currency Name],
                 src.[Currency Symbol], src.[Minor Unit Digits], src.[Rounding Rule Code],
                 src.[Rounding Increment], src.[Is Reporting Currency],
                 src.[Is Transactional Currency], src.[Is Historical],
                 src.[Superseded By Currency Code], src.[Superseded On],
                 src.[Fixed Conversion Rate], src.[Default Rate Type Code],
                 src.[Rate Source Code], src.[Inverse Quotation], src.[Decimal Separator],
                 src.[Thousands Separator], src.[Symbol Position Code],
                 src.[Is Restricted Currency], src.[Settlement Substitute Code],
                 src.[Is Active], @SourceSystemCode,
                 HASHBYTES(N'SHA2_256',
                     CONCAT_WS(N'|', ISNULL(src.[Currency Name], N''),
                               ISNULL(src.[Currency Symbol], N''),
                               ISNULL(CONVERT(NVARCHAR(6), src.[Minor Unit Digits]), N''),
                               ISNULL(src.[Rounding Rule Code], N''),
                               ISNULL(src.[Superseded By Currency Code], N''),
                               ISNULL(CONVERT(NVARCHAR(30), src.[Fixed Conversion Rate]), N''),
                               ISNULL(CONVERT(NVARCHAR(1), src.[Is Restricted Currency]), N''),
                               ISNULL(CONVERT(NVARCHAR(1), src.[Is Active]), N''))),
                 @Now, @HighDate, @LineageKey, @BatchId);

        SET @UpdatedCount = (SELECT COUNT_BIG(*)
                             FROM [Dimension].[Currency]
                             WHERE [Last Load Batch Id] = @BatchId
                               AND [Currency Key] > 0);

        SET @InsertedCount = (SELECT COUNT_BIG(*)
                              FROM [Dimension].[Currency]
                              WHERE [Last Load Batch Id] = @BatchId
                                AND [Currency Key] > 0
                                AND [Valid From] = @Now);

        SET @UpdatedCount = @UpdatedCount - @InsertedCount;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Currency', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'Type1Merge', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Currency',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @UpdatedCount,
             @RejectRowCount     = @RejectCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @UpdatedCount,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Currency',
             @ProcedureName      = N'Integration.usp_MigrateStagedCurrencyData',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @SourceRowCount,
             @RowsRejected       = @RejectCount;

        THROW;
    END CATCH;
END;
GO
