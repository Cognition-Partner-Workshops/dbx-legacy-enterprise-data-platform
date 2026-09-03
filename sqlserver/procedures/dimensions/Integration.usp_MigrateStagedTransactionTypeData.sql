/*
    Object        : [Integration].[usp_MigrateStagedTransactionTypeData]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.TransactionType, Dimension.Transaction Type,
                    the etl control framework
    Called by     : REF_Load_TransactionType (weekly reference load)

    Type 1. This one is dynamic-SQL driven, which is a genuine piece of estate
    history rather than a flourish: the source reference table was split per
    ledger in 2012 (AR, AP, GL, INV) and instead of a union view the author
    parameterised the object name and looped the same statement four times. The
    @LedgerList parameter still allows an operator to reload a single ledger
    after a correction, which is how it is normally used.

    Sign conventions are the interesting part. The 1998 order entry system
    recorded credit notes as positive amounts with a separate type code; the
    warehouse standardises on a signed amount, so the sign lives here and every
    fact load multiplies by it. Nothing else in the estate knows this.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedTransactionTypeData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_TransactionType',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_GL',
    @LedgerList         NVARCHAR(200) = N'AR,AP,GL,INV',
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
    DECLARE @Ledger             NVARCHAR(10);
    DECLARE @Sql                NVARCHAR(MAX);
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'ReferenceLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#TransactionTypeSource') IS NOT NULL
            DROP TABLE #TransactionTypeSource;

        CREATE TABLE #TransactionTypeSource
        (
            [Transaction Type Code]      NVARCHAR(10)  NOT NULL,
            [WWI Transaction Type ID]    INT           NULL,
            [Transaction Type]           NVARCHAR(50)  NOT NULL,
            [Transaction Category Code]  NVARCHAR(10)  NULL,
            [Ledger Impact Code]         NVARCHAR(10)  NULL,
            [Amount Sign]                SMALLINT      NULL,
            [Affects Customer Balance]   BIT           NULL,
            [Affects Supplier Balance]   BIT           NULL,
            [Affects Inventory Value]    BIT           NULL,
            [Is Reversal Type]           BIT           NULL,
            [Reversal Of Type Code]      NVARCHAR(10)  NULL,
            [Debit GL Account Code]      NVARCHAR(20)  NULL,
            [Credit GL Account Code]     NVARCHAR(20)  NULL,
            [Tax Point Rule Code]        NVARCHAR(15)  NULL,
            [Requires Tax Analysis]      BIT           NULL,
            [Region Code]                NVARCHAR(10)  NULL,
            [Legacy Source Code]         NVARCHAR(4)   NULL,
            [Is Active]                  BIT           NULL
        );

        DECLARE LedgerCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT LTRIM(RTRIM([value]))
            FROM STRING_SPLIT(@LedgerList, N',')
            WHERE NULLIF(LTRIM(RTRIM([value])), N'') IS NOT NULL;

        OPEN LedgerCursor;
        FETCH NEXT FROM LedgerCursor INTO @Ledger;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            /* The per-ledger reference tables are ref.TransactionTypeAR and so on.
               The name is assembled, not concatenated from user input at run time. */
            SET @Sql = N'
                INSERT INTO #TransactionTypeSource
                    ([Transaction Type Code], [WWI Transaction Type ID], [Transaction Type],
                     [Transaction Category Code], [Ledger Impact Code], [Amount Sign],
                     [Affects Customer Balance], [Affects Supplier Balance], [Affects Inventory Value],
                     [Is Reversal Type], [Reversal Of Type Code], [Debit GL Account Code],
                     [Credit GL Account Code], [Tax Point Rule Code], [Requires Tax Analysis],
                     [Region Code], [Legacy Source Code], [Is Active])
                SELECT
                      UPPER(LTRIM(RTRIM(t.[TransactionTypeCode])))
                    , t.[WWITransactionTypeID]
                    , t.[TransactionTypeName]
                    , UPPER(ISNULL(t.[TransactionCategoryCode], @LedgerParam))
                    , @LedgerParam
                    , CASE WHEN t.[IsCreditSide] = 1 THEN -1 ELSE 1 END
                    , CASE WHEN @LedgerParam = N''AR''  THEN 1 ELSE 0 END
                    , CASE WHEN @LedgerParam = N''AP''  THEN 1 ELSE 0 END
                    , CASE WHEN @LedgerParam = N''INV'' THEN 1 ELSE 0 END
                    , ISNULL(t.[IsReversalType], 0)
                    , UPPER(NULLIF(LTRIM(RTRIM(t.[ReversalOfTypeCode])), N''''))
                    , t.[DebitGlAccountCode]
                    , t.[CreditGlAccountCode]
                    , t.[TaxPointRuleCode]
                    , ISNULL(t.[RequiresTaxAnalysis], 0)
                    , UPPER(ISNULL(t.[RegionCode], N''GLOBAL''))
                    , LEFT(UPPER(ISNULL(t.[LegacySourceCode], N'''')), 4)
                    , ISNULL(t.[IsActive], 1)
                FROM [ref].[TransactionType' + @Ledger + N'] AS t
                WHERE NULLIF(LTRIM(RTRIM(t.[TransactionTypeCode])), N'''') IS NOT NULL;';

            EXEC sys.sp_executesql @Sql, N'@LedgerParam NVARCHAR(10)', @LedgerParam = @Ledger;

            SET @SourceRowCount = @SourceRowCount + @@ROWCOUNT;

            FETCH NEXT FROM LedgerCursor INTO @Ledger;
        END;

        CLOSE LedgerCursor;
        DEALLOCATE LedgerCursor;

        /* EU tax point differs from the invoice date for intra-community supply,
           so the rule code is forced where the source left it empty. */
        UPDATE #TransactionTypeSource
        SET [Tax Point Rule Code]   = N'SUPPLY_DATE',
            [Requires Tax Analysis] = 1
        WHERE [Region Code] = N'EU'
          AND NULLIF([Tax Point Rule Code], N'') IS NULL
          AND [Ledger Impact Code] IN (N'AR', N'AP');

        /* NA sales tax is computed at invoice, APAC GST at the earlier of
           invoice or payment. Both are recorded so the fact loads can branch. */
        UPDATE #TransactionTypeSource
        SET [Tax Point Rule Code] = N'INVOICE_DATE'
        WHERE [Region Code] = N'NA'
          AND NULLIF([Tax Point Rule Code], N'') IS NULL;

        UPDATE #TransactionTypeSource
        SET [Tax Point Rule Code] = N'EARLIER_INV_PAY'
        WHERE [Region Code] = N'APAC'
          AND NULLIF([Tax Point Rule Code], N'') IS NULL;

        /* A reversal type pointing at a type code that does not exist in this
           load leaves the credit note fact with no original to net against. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Transaction Type',
               s.[Transaction Type Code], N'REVERSAL_TARGET_MISSING',
               N'Reversal type references a transaction type code that is not present in the reference extract.',
               N'Reference', CONCAT(N'ReversalOf=', s.[Reversal Of Type Code],
                                    N'|Ledger=', s.[Ledger Impact Code])
        FROM #TransactionTypeSource AS s
        WHERE s.[Is Reversal Type] = 1
          AND s.[Reversal Of Type Code] IS NOT NULL
          AND NOT EXISTS (SELECT 1
                          FROM #TransactionTypeSource AS o
                          WHERE o.[Transaction Type Code] = s.[Reversal Of Type Code]);

        SET @RejectCount = @@ROWCOUNT;

        UPDATE d
        SET d.[Transaction Type]          = s.[Transaction Type],
            d.[Transaction Category Code] = s.[Transaction Category Code],
            d.[Ledger Impact Code]        = s.[Ledger Impact Code],
            d.[Amount Sign]               = s.[Amount Sign],
            d.[Affects Customer Balance]  = s.[Affects Customer Balance],
            d.[Affects Supplier Balance]  = s.[Affects Supplier Balance],
            d.[Affects Inventory Value]   = s.[Affects Inventory Value],
            d.[Is Reversal Type]          = s.[Is Reversal Type],
            d.[Reversal Of Type Code]     = s.[Reversal Of Type Code],
            d.[Debit GL Account Code]     = s.[Debit GL Account Code],
            d.[Credit GL Account Code]    = s.[Credit GL Account Code],
            d.[Tax Point Rule Code]       = s.[Tax Point Rule Code],
            d.[Requires Tax Analysis]     = s.[Requires Tax Analysis],
            d.[Region Code]               = s.[Region Code],
            d.[Legacy Source Code]        = NULLIF(s.[Legacy Source Code], N''),
            d.[Is Active]                 = s.[Is Active],
            d.[Source System Code]        = @SourceSystemCode,
            d.[Row Hash Type 1]           = HASHBYTES(N'SHA2_256',
                  CONCAT_WS(N'|', ISNULL(s.[Transaction Type], N''),
                            ISNULL(s.[Ledger Impact Code], N''),
                            ISNULL(CONVERT(NVARCHAR(6), s.[Amount Sign]), N''),
                            ISNULL(s.[Debit GL Account Code], N''),
                            ISNULL(s.[Credit GL Account Code], N''),
                            ISNULL(s.[Tax Point Rule Code], N''),
                            ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N''))),
            d.[Last Load Batch Id]        = @BatchId
        FROM [Dimension].[Transaction Type] AS d
        INNER JOIN #TransactionTypeSource AS s
            ON s.[Transaction Type Code] = d.[Transaction Type Code]
        WHERE d.[Transaction Type Key] > 0;

        SET @UpdatedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Transaction Type]
            ([WWI Transaction Type ID], [Transaction Type], [Transaction Type Code],
             [Transaction Category Code], [Ledger Impact Code], [Amount Sign],
             [Affects Customer Balance], [Affects Supplier Balance], [Affects Inventory Value],
             [Is Reversal Type], [Reversal Of Type Code], [Debit GL Account Code],
             [Credit GL Account Code], [Tax Point Rule Code], [Requires Tax Analysis],
             [Region Code], [Legacy Source Code], [Is Active], [Source System Code],
             [Row Hash Type 1], [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              ISNULL(s.[WWI Transaction Type ID], 0)
            , ISNULL(s.[Transaction Type], s.[Transaction Type Code])
            , s.[Transaction Type Code]
            , s.[Transaction Category Code]
            , s.[Ledger Impact Code]
            , s.[Amount Sign]
            , s.[Affects Customer Balance]
            , s.[Affects Supplier Balance]
            , s.[Affects Inventory Value]
            , s.[Is Reversal Type]
            , s.[Reversal Of Type Code]
            , s.[Debit GL Account Code]
            , s.[Credit GL Account Code]
            , s.[Tax Point Rule Code]
            , s.[Requires Tax Analysis]
            , s.[Region Code]
            , NULLIF(s.[Legacy Source Code], N'')
            , s.[Is Active]
            , @SourceSystemCode
            , HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(s.[Transaction Type], N''),
                          ISNULL(s.[Ledger Impact Code], N''),
                          ISNULL(CONVERT(NVARCHAR(6), s.[Amount Sign]), N''),
                          ISNULL(s.[Debit GL Account Code], N''),
                          ISNULL(s.[Credit GL Account Code], N''),
                          ISNULL(s.[Tax Point Rule Code], N''),
                          ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N'')))
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #TransactionTypeSource AS s
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Transaction Type] AS d
                          WHERE d.[Transaction Type Code] = s.[Transaction Type Code]
                            AND d.[Transaction Type Key]  > 0);

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Transaction Type', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'Type1DynamicUnion', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Transaction Type',
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

        IF CURSOR_STATUS(N'local', N'LedgerCursor') >= 0
        BEGIN
            CLOSE LedgerCursor;
            DEALLOCATE LedgerCursor;
        END;

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Transaction Type',
             @ProcedureName      = N'Integration.usp_MigrateStagedTransactionTypeData',
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
