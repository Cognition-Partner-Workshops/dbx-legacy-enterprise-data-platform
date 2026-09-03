/*
    work.usp_MatchPaymentsToInvoices

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : WRK_MATCH_PAYMENTS (SSIS), after STG_LOAD_PAYMENT
    Reads         : stg.Payment, stg.ApInvoice, ref.Region
    Writes        : work.PaymentMatched, stg.Payment (MatchStatusCode), err.RejectedPayment
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Three-pass AP cash application, unchanged in shape since the 2008 rewrite.

      Pass 1  REMIT_REF   the remittance reference contains the invoice number.
                          Cleanest match; confidence 100.
      Pass 2  EXACT_AMT   one open invoice for the supplier whose open amount
                          equals the payment amount, allowing for the settlement
                          discount if the payment is inside the discount window.
                          Confidence 90.
      Pass 3  RESIDUAL    oldest-first allocation of whatever is left, invoice by
                          invoice, until the payment is exhausted. This is the
                          cursor pass; it is also the one the AP supervisor reads
                          line by line every morning.

    Regional tolerance for calling a residual match "close enough":
        NA    0.02 absolute - bank rounding on ACH files.
        EU    0.01 absolute - SEPA is exact and anything else is a real query.
        APAC  0.50 percent  - the local bank fees are deducted at the far end and
                              arrive as a short payment.
*/

IF OBJECT_ID(N'work.usp_MatchPaymentsToInvoices', N'P') IS NOT NULL
    DROP PROCEDURE work.usp_MatchPaymentsToInvoices;
GO

CREATE PROCEDURE work.usp_MatchPaymentsToInvoices
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'work.PaymentMatched';
    DECLARE @PaymentRows   BIGINT = 0;
    DECLARE @MatchedRows   BIGINT = 0;
    DECLARE @UnmatchedRows BIGINT = 0;

    DECLARE @PaymentKey    NVARCHAR(100);
    DECLARE @SupplierKey   NVARCHAR(100);
    DECLARE @Remaining     DECIMAL(19,4);
    DECLARE @RegionCode    NVARCHAR(10);
    DECLARE @PaymentDate   DATE;
    DECLARE @InvoiceKey    NVARCHAR(100);
    DECLARE @OpenAmount    DECIMAL(19,4);
    DECLARE @ApplyAmount   DECIMAL(19,4);
    DECLARE @Tolerance     DECIMAL(19,4);

    BEGIN TRY
        SELECT @PaymentRows = COUNT_BIG(*)
        FROM stg.Payment AS p
        WHERE p.BatchId = @BatchId;

        DELETE FROM work.PaymentMatched
        WHERE BatchId = @BatchId;

        --  Pass 1: remittance reference carries the invoice number.
        INSERT INTO work.PaymentMatched
        (
            BatchId, PackageExecutionId, PaymentBusinessKey, ApInvoiceBusinessKey, SupplierBusinessKey,
            MatchPassNumber, MatchRuleCode, AppliedAmount, AppliedAmountUsd, DiscountTakenAmount,
            FxDifferenceUsd, ResidualAmount, WithinToleranceFlag, MatchConfidence, IsFinalAllocation
        )
        SELECT
            @BatchId, @PackageExecutionId, p.PaymentBusinessKey, i.ApInvoiceBusinessKey,
            p.SupplierBusinessKey, 1, N'REMIT_REF',
            CASE WHEN p.PaymentAmount > i.OpenAmount THEN i.OpenAmount ELSE p.PaymentAmount END,
            CASE WHEN p.PaymentAmount > i.OpenAmount
                 THEN CONVERT(DECIMAL(19,4), i.OpenAmount * ISNULL(p.TransactionFxRate, 1))
                 ELSE p.PaymentAmountUsd END,
            p.DiscountTakenAmount,
            CONVERT(DECIMAL(19,4),
                ISNULL(p.PaymentAmountUsd, 0)
              - ISNULL(p.PaymentAmount, 0) * ISNULL(i.TransactionFxRate, ISNULL(p.TransactionFxRate, 1))),
            CONVERT(DECIMAL(19,4), i.OpenAmount - ISNULL(p.PaymentAmount, 0)),
            CASE WHEN ABS(i.OpenAmount - ISNULL(p.PaymentAmount, 0)) <= 0.02 THEN 1 ELSE 0 END,
            100.00, 1
        FROM stg.Payment AS p
        INNER JOIN stg.ApInvoice AS i
            ON  i.BatchId             = @BatchId
            AND i.SupplierBusinessKey = p.SupplierBusinessKey
            AND p.RemittanceReference IS NOT NULL
            AND CHARINDEX(i.InvoiceNumber, p.RemittanceReference) > 0
        WHERE p.BatchId    = @BatchId
          AND p.VoidDate IS NULL;

        --  Pass 2: single open invoice at exactly the payment amount.
        INSERT INTO work.PaymentMatched
        (
            BatchId, PackageExecutionId, PaymentBusinessKey, ApInvoiceBusinessKey, SupplierBusinessKey,
            MatchPassNumber, MatchRuleCode, AppliedAmount, AppliedAmountUsd, DiscountTakenAmount,
            ResidualAmount, WithinToleranceFlag, MatchConfidence, IsFinalAllocation
        )
        SELECT
            @BatchId, @PackageExecutionId, p.PaymentBusinessKey, m.ApInvoiceBusinessKey,
            p.SupplierBusinessKey, 2, N'EXACT_AMT', p.PaymentAmount, p.PaymentAmountUsd,
            m.DiscountApplied, 0.00, 1, 90.00, 1
        FROM stg.Payment AS p
        CROSS APPLY
        (
            SELECT TOP (2)
                i.ApInvoiceBusinessKey,
                DiscountApplied =
                    CASE
                        WHEN i.DiscountDueDate IS NOT NULL AND p.PaymentDate <= i.DiscountDueDate
                            THEN CONVERT(DECIMAL(19,4), i.OpenAmount - p.PaymentAmount)
                        ELSE 0.00
                    END
            FROM stg.ApInvoice AS i
            WHERE i.BatchId             = @BatchId
              AND i.SupplierBusinessKey = p.SupplierBusinessKey
              AND i.OpenAmount > 0
              AND
              (
                  i.OpenAmount = p.PaymentAmount
                  OR (i.DiscountDueDate IS NOT NULL
                      AND p.PaymentDate <= i.DiscountDueDate
                      AND i.OpenAmount - p.PaymentAmount BETWEEN 0.01 AND i.OpenAmount * 0.05)
              )
        ) AS m
        WHERE p.BatchId  = @BatchId
          AND p.VoidDate IS NULL
          AND NOT EXISTS
              (
                  SELECT 1 FROM work.PaymentMatched AS w
                  WHERE w.BatchId            = @BatchId
                    AND w.PaymentBusinessKey = p.PaymentBusinessKey
              );

        --  Pass 3: residual allocation, oldest invoice first, one payment at a time.
        DECLARE PaymentCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT
                p.PaymentBusinessKey,
                p.SupplierBusinessKey,
                p.PaymentAmount,
                ISNULL(p.RegionCode, N'NA'),
                p.PaymentDate
            FROM stg.Payment AS p
            WHERE p.BatchId  = @BatchId
              AND p.VoidDate IS NULL
              AND p.PaymentAmount > 0
              AND NOT EXISTS
                  (
                      SELECT 1 FROM work.PaymentMatched AS w
                      WHERE w.BatchId            = @BatchId
                        AND w.PaymentBusinessKey = p.PaymentBusinessKey
                  )
            ORDER BY p.PaymentBusinessKey;

        OPEN PaymentCursor;
        FETCH NEXT FROM PaymentCursor
            INTO @PaymentKey, @SupplierKey, @Remaining, @RegionCode, @PaymentDate;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @Tolerance =
                CASE @RegionCode
                    WHEN N'EU'   THEN 0.01
                    WHEN N'APAC' THEN CONVERT(DECIMAL(19,4), @Remaining * 0.005)
                    ELSE 0.02
                END;

            DECLARE InvoiceCursor CURSOR LOCAL FAST_FORWARD FOR
                SELECT i.ApInvoiceBusinessKey, i.OpenAmount
                FROM stg.ApInvoice AS i
                WHERE i.BatchId             = @BatchId
                  AND i.SupplierBusinessKey = @SupplierKey
                  AND i.OpenAmount > 0
                  AND i.IsOnHold            = 0
                ORDER BY i.InvoiceDate, i.ApInvoiceBusinessKey;

            OPEN InvoiceCursor;
            FETCH NEXT FROM InvoiceCursor INTO @InvoiceKey, @OpenAmount;

            WHILE @@FETCH_STATUS = 0 AND @Remaining > 0
            BEGIN
                SET @ApplyAmount = CASE WHEN @Remaining >= @OpenAmount THEN @OpenAmount ELSE @Remaining END;

                INSERT INTO work.PaymentMatched
                (
                    BatchId, PackageExecutionId, PaymentBusinessKey, ApInvoiceBusinessKey,
                    SupplierBusinessKey, MatchPassNumber, MatchRuleCode, AppliedAmount,
                    ResidualAmount, WithinToleranceFlag, MatchConfidence, IsFinalAllocation
                )
                VALUES
                (
                    @BatchId, @PackageExecutionId, @PaymentKey, @InvoiceKey,
                    @SupplierKey, 3, N'RESIDUAL', @ApplyAmount,
                    @OpenAmount - @ApplyAmount,
                    CASE WHEN ABS(@OpenAmount - @ApplyAmount) <= @Tolerance THEN 1 ELSE 0 END,
                    65.00,
                    CASE WHEN @Remaining - @ApplyAmount <= @Tolerance THEN 1 ELSE 0 END
                );

                SET @Remaining = @Remaining - @ApplyAmount;

                FETCH NEXT FROM InvoiceCursor INTO @InvoiceKey, @OpenAmount;
            END;

            CLOSE InvoiceCursor;
            DEALLOCATE InvoiceCursor;

            --  Anything left over is unapplied cash on the supplier account.
            IF @Remaining > @Tolerance
                INSERT INTO work.PaymentMatched
                (
                    BatchId, PackageExecutionId, PaymentBusinessKey, ApInvoiceBusinessKey,
                    SupplierBusinessKey, MatchPassNumber, MatchRuleCode, AppliedAmount,
                    ResidualAmount, WithinToleranceFlag, MatchConfidence, IsFinalAllocation,
                    UnmatchedReasonCode
                )
                VALUES
                (
                    @BatchId, @PackageExecutionId, @PaymentKey, NULL,
                    @SupplierKey, 3, N'UNAPPLIED', 0.00,
                    @Remaining, 0, 0.00, 1, N'NO_OPEN_INVOICE'
                );

            FETCH NEXT FROM PaymentCursor
                INTO @PaymentKey, @SupplierKey, @Remaining, @RegionCode, @PaymentDate;
        END;

        CLOSE PaymentCursor;
        DEALLOCATE PaymentCursor;

        --  Feed the outcome back onto the payment rows.
        UPDATE p
        SET p.MatchStatusCode = a.MatchStatusCode,
            p.AppliedInvoiceCount = a.AppliedInvoiceCount,
            p.UnappliedAmount     = a.UnappliedAmount
        FROM stg.Payment AS p
        CROSS APPLY
        (
            SELECT
                AppliedInvoiceCount = COUNT(CASE WHEN w.ApInvoiceBusinessKey IS NOT NULL THEN 1 END),
                UnappliedAmount     = SUM(CASE WHEN w.MatchRuleCode = N'UNAPPLIED' THEN w.ResidualAmount ELSE 0 END),
                MatchStatusCode     =
                    CASE
                        WHEN COUNT(w.WorkRowId) = 0                                      THEN N'UNMATCHED'
                        WHEN MAX(CASE WHEN w.MatchRuleCode = N'UNAPPLIED' THEN 1 ELSE 0 END) = 1
                                                                                         THEN N'PARTIAL'
                        WHEN MIN(w.MatchPassNumber) = 1                                  THEN N'MATCHED_REF'
                        WHEN MIN(w.MatchPassNumber) = 2                                  THEN N'MATCHED_AMT'
                        ELSE N'MATCHED_RESIDUAL'
                    END
            FROM work.PaymentMatched AS w
            WHERE w.BatchId            = @BatchId
              AND w.PaymentBusinessKey = p.PaymentBusinessKey
        ) AS a
        WHERE p.BatchId = @BatchId;

        SELECT @MatchedRows = COUNT_BIG(*)
        FROM work.PaymentMatched AS w
        WHERE w.BatchId              = @BatchId
          AND w.ApInvoiceBusinessKey IS NOT NULL;

        INSERT INTO err.RejectedPayment
        (
            BatchId, PackageExecutionId, SourceSystemCode, PaymentBusinessKey, PaymentNumber,
            SupplierReference, PaymentAmountText, CurrencyCode, UnappliedAmount,
            RejectReasonCode, RejectReason, RejectStage
        )
        SELECT
            @BatchId, @PackageExecutionId, p.SourceSystemCode, p.PaymentBusinessKey, p.PaymentNumber,
            LEFT(p.SupplierBusinessKey, 60), CONVERT(NVARCHAR(50), p.PaymentAmount),
            p.TransactionCurrencyCode, p.UnappliedAmount,
            N'UNAPPLIED_CASH',
            N'payment could not be fully applied to open supplier invoices',
            N'Match'
        FROM stg.Payment AS p
        WHERE p.BatchId         = @BatchId
          AND p.MatchStatusCode IN (N'UNMATCHED', N'PARTIAL');

        SET @UnmatchedRows = @@ROWCOUNT;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @PaymentRows,
            @InsertRowCount     = @MatchedRows,
            @RejectRowCount     = @UnmatchedRows;

        IF @UnmatchedRows > 0
            EXEC err.usp_LogRejectedRows
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @RejectTableName    = N'err.RejectedPayment',
                @ObjectName         = N'stg.Payment',
                @BusinessKeyColumn  = N'PaymentBusinessKey',
                @RejectStage        = N'Match';
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'InvoiceCursor') >= 0
        BEGIN
            CLOSE InvoiceCursor;
            DEALLOCATE InvoiceCursor;
        END;

        IF CURSOR_STATUS('local', 'PaymentCursor') >= 0
        BEGIN
            CLOSE PaymentCursor;
            DEALLOCATE PaymentCursor;
        END;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'WRK_MATCH_PAYMENTS',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'work.usp_MatchPaymentsToInvoices';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
