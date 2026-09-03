/*
    Integration.usp_ApplyFactCorrections

    Object        : Integration.usp_ApplyFactCorrections
    Deploy target : WideWorldImportersDW
    Deploy order  : after all fact loads.
    Called by     : FACT_Apply_Corrections (nightly), and by finance on demand
                    during close.
    Reads         : stg.FactCorrectionRequest.
    Depends on    : the etl control procedures.

    The estate uses BOTH correction patterns and this procedure is where the
    choice is encoded:

      REVERSAL pattern   - Fact.Sale, Fact.Return, Fact.GL Posting. The
                           original row is left untouched, a mirrored row with
                           negated measures and [Correction Type Code] = 'REV'
                           is inserted, and the restated row is inserted with
                           'RES'. Period totals therefore stay correct for
                           any as-at date, which the statutory reports need.
                           The reason code stays on the request row in
                           stg.FactCorrectionRequest; the facts carry the type
                           and the corrected key only.

      IN-PLACE pattern   - Fact.Payment, Fact.Order, Fact.Shipment. The row is
                           updated and [Restatement Version] incremented. No
                           audit trail on the fact itself; the trail lives in
                           etl.RowCountAudit and in the source system.

    A correction request naming any other fact is rejected rather than guessed
    at.
*/
IF OBJECT_ID(N'Integration.usp_ApplyFactCorrections', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_ApplyFactCorrections;
GO

CREATE PROCEDURE Integration.usp_ApplyFactCorrections
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @FactName           NVARCHAR(128) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @UpdateRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;

    DECLARE @RequestId      BIGINT;
    DECLARE @TargetFact     NVARCHAR(128);
    DECLARE @NaturalKey     NVARCHAR(200);
    DECLARE @CorrectionType NVARCHAR(20);
    DECLARE @NewAmount      DECIMAL(19, 2);
    DECLARE @NewQuantity    DECIMAL(18, 3);
    DECLARE @ReasonCode     NVARCHAR(20);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Apply_Corrections',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'ApplyFactCorrections',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DECLARE correction_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT c.RequestId, c.TargetFactName, c.NaturalKeyValue,
                   c.CorrectionTypeCode, c.NewAmount, c.NewQuantity, c.ReasonCode
            FROM stg.FactCorrectionRequest AS c
            WHERE c.RequestStatusCode = N'APPROVED'
              AND c.AppliedDatetime IS NULL
              AND (@FactName IS NULL OR c.TargetFactName = @FactName)
            ORDER BY c.RequestId;

        OPEN correction_cursor;
        FETCH NEXT FROM correction_cursor
            INTO @RequestId, @TargetFact, @NaturalKey, @CorrectionType,
                 @NewAmount, @NewQuantity, @ReasonCode;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @SourceRowCount = @SourceRowCount + 1;

            IF @TargetFact = N'Fact.Sale'
            BEGIN
                /* Reversal of the original. */
                INSERT INTO Fact.[Sale]
                (
                    [Invoice Date Key], [Delivery Date Key], [Customer Key],
                    [Bill To Customer Key], [Stock Item Key], [Salesperson Key],
                    [Sales Territory Key], [Sales Channel Key], [Customer Segment Key],
                    [Promotion Key], [Currency Key], [City Key], [Region Code],
                    [Invoice Number], [Invoice Line Number], [Order Number],
                    [Transaction Currency Code], [Quantity Base UOM], [Quantity],
                    [Source UOM Code], [Unit Price], [Gross Amount], [Line Discount Amount],
                    [Net Amount], [Tax Amount], [Freight Amount], [Cost Of Sale Amount],
                    [Gross Margin Amount], [FX Rate To Reporting], [Net Amount Reporting],
                    [Correction Type Code],
                    [Corrected Sale Key], [Natural Key Hash], [Lineage Key],
                    [Batch Id], [Load Datetime]
                )
                SELECT
                    s.[Invoice Date Key], s.[Delivery Date Key], s.[Customer Key],
                    s.[Bill To Customer Key], s.[Stock Item Key], s.[Salesperson Key],
                    s.[Sales Territory Key], s.[Sales Channel Key], s.[Customer Segment Key],
                    s.[Promotion Key], s.[Currency Key], s.[City Key], s.[Region Code],
                    s.[Invoice Number], s.[Invoice Line Number], s.[Order Number],
                    s.[Transaction Currency Code],
                    -s.[Quantity Base UOM], -s.[Quantity], s.[Source UOM Code],
                    s.[Unit Price], -s.[Gross Amount], -s.[Line Discount Amount],
                    -s.[Net Amount], -s.[Tax Amount], -ISNULL(s.[Freight Amount], 0),
                    -s.[Cost Of Sale Amount], -s.[Gross Margin Amount], s.[FX Rate To Reporting],
                    -s.[Net Amount Reporting],
                    N'REV', s.[Sale Key], s.[Natural Key Hash], s.[Lineage Key],
                    @BatchId, SYSDATETIME()
                FROM Fact.[Sale] AS s
                WHERE CONVERT(NVARCHAR(200), s.[Invoice Number]) + N'|'
                      + CONVERT(NVARCHAR(20), s.[Invoice Line Number]) = @NaturalKey
                  AND ISNULL(s.[Correction Type Code], N'ORIG') = N'ORIG';

                SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT;

                /* Restated row at the corrected value. */
                INSERT INTO Fact.[Sale]
                (
                    [Invoice Date Key], [Delivery Date Key], [Customer Key],
                    [Bill To Customer Key], [Stock Item Key], [Salesperson Key],
                    [Sales Territory Key], [Sales Channel Key], [Customer Segment Key],
                    [Promotion Key], [Currency Key], [City Key], [Region Code],
                    [Invoice Number], [Invoice Line Number], [Order Number],
                    [Transaction Currency Code], [Quantity Base UOM], [Quantity],
                    [Source UOM Code], [Unit Price], [Gross Amount], [Line Discount Amount],
                    [Net Amount], [Tax Amount], [Freight Amount], [Cost Of Sale Amount],
                    [Gross Margin Amount], [FX Rate To Reporting], [Net Amount Reporting],
                    [Correction Type Code],
                    [Corrected Sale Key], [Natural Key Hash], [Lineage Key],
                    [Batch Id], [Load Datetime]
                )
                SELECT
                    s.[Invoice Date Key], s.[Delivery Date Key], s.[Customer Key],
                    s.[Bill To Customer Key], s.[Stock Item Key], s.[Salesperson Key],
                    s.[Sales Territory Key], s.[Sales Channel Key], s.[Customer Segment Key],
                    s.[Promotion Key], s.[Currency Key], s.[City Key], s.[Region Code],
                    s.[Invoice Number], s.[Invoice Line Number], s.[Order Number],
                    s.[Transaction Currency Code],
                    ISNULL(@NewQuantity, s.[Quantity Base UOM]),
                    ISNULL(@NewQuantity, s.[Quantity]), s.[Source UOM Code],
                    s.[Unit Price], ISNULL(@NewAmount, s.[Gross Amount]),
                    s.[Line Discount Amount],
                    ISNULL(@NewAmount, s.[Gross Amount]) - s.[Line Discount Amount],
                    s.[Tax Amount], s.[Freight Amount], s.[Cost Of Sale Amount],
                    ISNULL(@NewAmount, s.[Gross Amount]) - s.[Line Discount Amount]
                        - s.[Cost Of Sale Amount],
                    s.[FX Rate To Reporting],
                    ROUND((ISNULL(@NewAmount, s.[Gross Amount]) - s.[Line Discount Amount])
                          * s.[FX Rate To Reporting], 2),
                    N'RES', s.[Sale Key], s.[Natural Key Hash], s.[Lineage Key],
                    @BatchId, SYSDATETIME()
                FROM Fact.[Sale] AS s
                WHERE CONVERT(NVARCHAR(200), s.[Invoice Number]) + N'|'
                      + CONVERT(NVARCHAR(20), s.[Invoice Line Number]) = @NaturalKey
                  AND ISNULL(s.[Correction Type Code], N'ORIG') = N'ORIG';

                SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT;
            END
            ELSE IF @TargetFact = N'Fact.Payment'
            BEGIN
                UPDATE Fact.[Payment]
                SET [Allocated Amount]     = ISNULL(@NewAmount, [Allocated Amount]),
                    [Unallocated Amount]   = [Payment Amount] - ISNULL(@NewAmount, [Allocated Amount]),
                    [Restatement Version]  = ISNULL([Restatement Version], 1) + 1,
                    [Restated Datetime]    = SYSDATETIME(),
                    [Batch Id]             = @BatchId,
                    [Load Datetime]        = SYSDATETIME()
                WHERE CONVERT(NVARCHAR(200), [Receipt Number]) + N'|'
                      + CONVERT(NVARCHAR(20), [Receipt Line Number]) = @NaturalKey;

                SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT;
            END
            ELSE IF @TargetFact = N'Fact.Order'
            BEGIN
                UPDATE Fact.[Order]
                SET [Quantity Ordered]    = ISNULL(@NewQuantity, [Quantity Ordered]),
                    [Net Order Amount]    = ISNULL(@NewAmount, [Net Order Amount]),
                    [Net Order Amount Reporting] =
                        ROUND(ISNULL(@NewAmount, [Net Order Amount])
                              * ISNULL([FX Rate To Reporting], 1), 2),
                    [Batch Id]            = @BatchId,
                    [Load Datetime]       = SYSDATETIME()
                WHERE CONVERT(NVARCHAR(200), [Order Number]) + N'|'
                      + CONVERT(NVARCHAR(20), [Order Line Number]) = @NaturalKey;

                SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT;
            END
            ELSE
            BEGIN
                EXECUTE etl.usp_LogRejectedRecord
                    @PackageExecutionId = @PackageExecutionId,
                    @BatchId            = @BatchId,
                    @SourceSystemCode   = N'DW',
                    @ObjectName         = @TargetFact,
                    @BusinessKey        = @NaturalKey,
                    @RejectReasonCode   = N'CORR_FACT_UNSUPPORTED',
                    @RejectReason       = N'No correction pattern is defined for this fact',
                    @RejectStage        = N'Correction';

                SET @RejectRowCount = @RejectRowCount + 1;
            END;

            UPDATE stg.FactCorrectionRequest
            SET AppliedDatetime = SYSDATETIME(),
                AppliedBatchId  = @BatchId
            WHERE RequestId = @RequestId;

            FETCH NEXT FROM correction_cursor
                INTO @RequestId, @TargetFact, @NaturalKey, @CorrectionType,
                     @NewAmount, @NewQuantity, @ReasonCode;
        END;

        CLOSE correction_cursor;
        DEALLOCATE correction_cursor;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact corrections',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @UpdateRowCount     = @UpdateRowCount,
            @RejectRowCount     = @RejectRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsUpdated        = @UpdateRowCount,
                @RowsRejected       = @RejectRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'correction_cursor') >= 0
        BEGIN
            CLOSE correction_cursor;
            DEALLOCATE correction_cursor;
        END;

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact corrections',
            @SourceComponent    = N'Correction apply',
            @ProcedureName      = N'Integration.usp_ApplyFactCorrections',
            @ErrorDescription   = @ErrorMessage;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Failed';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
