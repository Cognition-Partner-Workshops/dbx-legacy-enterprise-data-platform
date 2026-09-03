/*
    Object        : [Integration].[usp_MigrateStagedPaymentMethodData]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.PaymentMethod, Dimension.Payment Method,
                    the etl control framework
    Called by     : REF_Load_PaymentMethod (weekly reference load)

    Type 1, rebuilt one region at a time inside a cursor. The cursor is not
    necessary and never was; it exists because the 2011 rewrite kept the shape of
    the DTS package it replaced, where each region was a separate data flow. It
    does have one useful side effect - a failure in the EU rebuild leaves NA and
    APAC intact, and the operators have come to rely on that.

    Regional payment instruments are genuinely different reference sets:

      NA   - ACH, cheque, purchasing card and the card networks; chargeback
             windows follow the card scheme rules, no mandate concept.
      EU   - SEPA direct debit and credit transfer with a mandate reference and
             strong customer authentication; settlement in TARGET2 days.
      APAC - domestic real-time schemes and marketplace wallets, local scheme
             names carried verbatim because they do not map to a global code.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedPaymentMethodData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_PaymentMethod',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_FIN',
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
    DECLARE @DeletedCount       BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @RegionCode         NVARCHAR(10);
    DECLARE @RegionRows         BIGINT;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'ReferenceLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#PaymentMethodSource') IS NOT NULL
            DROP TABLE #PaymentMethodSource;

        SELECT
              UPPER(LTRIM(RTRIM(m.[PaymentMethodCode])))  AS [Payment Method Code]
            , m.[WWIPaymentMethodID]                      AS [WWI Payment Method ID]
            , m.[PaymentMethodName]                       AS [Payment Method]
            , UPPER(ISNULL(m.[InstrumentFamilyCode], N'OTHER')) AS [Instrument Family Code]
            , UPPER(ISNULL(m.[RegionCode], N'GLOBAL'))    AS [Region Code]
            , UPPER(m.[SettlementCurrencyCode])           AS [Settlement Currency Code]
            , m.[ClearingSchemeCode]                      AS [Clearing Scheme Code]
            , CONVERT(SMALLINT, m.[SettlementDays])       AS [Settlement Days]
            , ISNULL(m.[IsPrepayment], 0)                 AS [Is Prepayment]
            , ISNULL(m.[IsCardPresentCapable], 0)         AS [Is Card Present Capable]
            , ISNULL(m.[IsOnlineCapable], 0)              AS [Is Online Capable]
            , ISNULL(m.[IsRefundableToSource], 0)         AS [Is Refundable To Source]
            , CONVERT(SMALLINT, NULL)                     AS [Chargeback Window Days]
            , m.[MerchantFeeFixedAmount]                  AS [Merchant Fee Fixed Amount]
            , m.[MerchantFeePercentage]                   AS [Merchant Fee Percentage]
            , CONVERT(BIT, 0)                             AS [Requires Mandate]
            , CONVERT(NVARCHAR(40), NULL)                 AS [Mandate Reference Format]
            , CONVERT(BIT, 0)                             AS [Requires Strong Authentication]
            , CONVERT(NVARCHAR(10), NULL)                 AS [NA Card Network Code]
            , CONVERT(NVARCHAR(60), NULL)                 AS [APAC Local Scheme Name]
            , m.[GlClearingAccountCode]                   AS [GL Clearing Account Code]
            , ISNULL(m.[IsActive], 1)                     AS [Is Active]
            , m.[RetiredOn]                               AS [Retired On]
        INTO #PaymentMethodSource
        FROM [ref].[PaymentMethod] AS m
        WHERE NULLIF(LTRIM(RTRIM(m.[PaymentMethodCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        /* NA card rules. Chargeback windows are scheme rules, not source data. */
        UPDATE s
        SET s.[NA Card Network Code]    = CASE
                                              WHEN s.[Payment Method Code] LIKE N'CARD_%'
                                                  THEN RIGHT(s.[Payment Method Code],
                                                             LEN(s.[Payment Method Code]) - 5)
                                              ELSE NULL
                                          END,
            s.[Chargeback Window Days]  = CASE
                                              WHEN s.[Instrument Family Code] = N'CARD'  THEN 120
                                              WHEN s.[Instrument Family Code] = N'ACH'   THEN 60
                                              WHEN s.[Instrument Family Code] = N'CHECK' THEN 0
                                              ELSE NULL
                                          END,
            s.[Settlement Days]         = ISNULL(s.[Settlement Days],
                                              CASE s.[Instrument Family Code]
                                                  WHEN N'ACH'   THEN 2
                                                  WHEN N'CARD'  THEN 2
                                                  WHEN N'CHECK' THEN 5
                                                  ELSE 3
                                              END)
        FROM #PaymentMethodSource AS s
        WHERE s.[Region Code] = N'NA';

        /* EU. SEPA mandates and PSD2 strong customer authentication. */
        UPDATE s
        SET s.[Requires Mandate]                = CASE WHEN s.[Instrument Family Code] = N'DD' THEN 1 ELSE 0 END,
            s.[Mandate Reference Format]        = CASE
                                                      WHEN s.[Instrument Family Code] = N'DD'
                                                          THEN N'MND-{CC}-{CUST}-{SEQ:0000}'
                                                      ELSE NULL
                                                  END,
            s.[Requires Strong Authentication]  = CASE
                                                      WHEN s.[Is Online Capable] = 1
                                                           AND s.[Instrument Family Code] IN (N'CARD', N'CT') THEN 1
                                                      ELSE 0
                                                  END,
            s.[Chargeback Window Days]          = CASE
                                                      WHEN s.[Instrument Family Code] = N'DD'   THEN 56
                                                      WHEN s.[Instrument Family Code] = N'CARD' THEN 120
                                                      ELSE NULL
                                                  END,
            s.[Settlement Days]                 = ISNULL(s.[Settlement Days], 1),
            s.[Settlement Currency Code]        = ISNULL(s.[Settlement Currency Code], N'EUR')
        FROM #PaymentMethodSource AS s
        WHERE s.[Region Code] = N'EU';

        /* APAC. Local scheme names are kept verbatim; the global family code is
           a best guess made by the 2015 integration and is wrong for wallets. */
        UPDATE s
        SET s.[APAC Local Scheme Name]  = ISNULL(s.[Clearing Scheme Code], s.[Payment Method]),
            s.[Instrument Family Code]  = CASE
                                              WHEN s.[Payment Method Code] LIKE N'WALLET%' THEN N'WALLET'
                                              WHEN s.[Payment Method Code] LIKE N'RTP%'    THEN N'RTP'
                                              ELSE s.[Instrument Family Code]
                                          END,
            s.[Settlement Days]         = ISNULL(s.[Settlement Days],
                                              CASE WHEN s.[Payment Method Code] LIKE N'RTP%' THEN 0 ELSE 2 END),
            s.[Is Refundable To Source] = CASE
                                              WHEN s.[Payment Method Code] LIKE N'WALLET%' THEN 0
                                              ELSE s.[Is Refundable To Source]
                                          END
        FROM #PaymentMethodSource AS s
        WHERE s.[Region Code] = N'APAC';

        /* An online-capable instrument with no clearing scheme cannot be
           reconciled to the bank statement feed. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Payment Method',
               s.[Payment Method Code], N'NO_CLEARING_SCHEME',
               N'Online-capable payment method has no clearing scheme; bank reconciliation will not match.',
               N'Reference', CONCAT(N'Region=', s.[Region Code], N'|Family=', s.[Instrument Family Code])
        FROM #PaymentMethodSource AS s
        WHERE s.[Is Online Capable] = 1
          AND NULLIF(s.[Clearing Scheme Code], N'') IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        DECLARE RegionCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT DISTINCT [Region Code]
            FROM #PaymentMethodSource
            ORDER BY [Region Code];

        OPEN RegionCursor;
        FETCH NEXT FROM RegionCursor INTO @RegionCode;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DELETE d
            FROM [Dimension].[Payment Method] AS d
            INNER JOIN #PaymentMethodSource AS s
                ON s.[Payment Method Code] = d.[Payment Method Code]
            WHERE d.[Payment Method Key] > 0
              AND s.[Region Code] = @RegionCode;

            SET @DeletedCount = @DeletedCount + @@ROWCOUNT;

            INSERT INTO [Dimension].[Payment Method]
                ([WWI Payment Method ID], [Payment Method], [Payment Method Code],
                 [Instrument Family Code], [Region Code], [Settlement Currency Code],
                 [Clearing Scheme Code], [Settlement Days], [Is Prepayment],
                 [Is Card Present Capable], [Is Online Capable], [Is Refundable To Source],
                 [Chargeback Window Days], [Merchant Fee Fixed Amount], [Merchant Fee Percentage],
                 [Requires Mandate], [Mandate Reference Format], [Requires Strong Authentication],
                 [NA Card Network Code], [APAC Local Scheme Name], [GL Clearing Account Code],
                 [Is Active], [Retired On], [Source System Code], [Row Hash Type 1],
                 [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
            SELECT
                  ISNULL(s.[WWI Payment Method ID], 0)
                , ISNULL(s.[Payment Method], s.[Payment Method Code])
                , s.[Payment Method Code]
                , s.[Instrument Family Code]
                , s.[Region Code]
                , s.[Settlement Currency Code]
                , s.[Clearing Scheme Code]
                , s.[Settlement Days]
                , s.[Is Prepayment]
                , s.[Is Card Present Capable]
                , s.[Is Online Capable]
                , s.[Is Refundable To Source]
                , s.[Chargeback Window Days]
                , s.[Merchant Fee Fixed Amount]
                , s.[Merchant Fee Percentage]
                , s.[Requires Mandate]
                , s.[Mandate Reference Format]
                , s.[Requires Strong Authentication]
                , s.[NA Card Network Code]
                , s.[APAC Local Scheme Name]
                , s.[GL Clearing Account Code]
                , s.[Is Active]
                , s.[Retired On]
                , @SourceSystemCode
                , HASHBYTES(N'SHA2_256',
                    CONCAT_WS(N'|', ISNULL(s.[Payment Method], N''),
                              ISNULL(s.[Instrument Family Code], N''),
                              ISNULL(s.[Clearing Scheme Code], N''),
                              ISNULL(CONVERT(NVARCHAR(6), s.[Settlement Days]), N''),
                              ISNULL(CONVERT(NVARCHAR(1), s.[Requires Mandate]), N''),
                              ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N'')))
                , @Now
                , @HighDate
                , @LineageKey
                , @BatchId
            FROM #PaymentMethodSource AS s
            WHERE s.[Region Code] = @RegionCode;

            SET @RegionRows    = @@ROWCOUNT;
            SET @InsertedCount = @InsertedCount + @RegionRows;

            EXEC [etl].[usp_LogRowCount]
                 @PackageExecutionId = @PackageExecutionId,
                 @ObjectName         = N'Dimension.Payment Method',
                 @SourceRowCount     = @RegionRows,
                 @InsertRowCount     = @RegionRows;

            FETCH NEXT FROM RegionCursor INTO @RegionCode;
        END;

        CLOSE RegionCursor;
        DEALLOCATE RegionCursor;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Payment Method', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             0, @DeletedCount, @InsertedCount, @RejectCount, N'FullRebuildByRegion', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsDeleted        = @DeletedCount,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        IF CURSOR_STATUS(N'local', N'RegionCursor') >= 0
        BEGIN
            CLOSE RegionCursor;
            DEALLOCATE RegionCursor;
        END;

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Payment Method',
             @ProcedureName      = N'Integration.usp_MigrateStagedPaymentMethodData',
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
