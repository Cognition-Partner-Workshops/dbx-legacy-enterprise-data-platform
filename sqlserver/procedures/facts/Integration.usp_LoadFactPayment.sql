/*
    Integration.usp_LoadFactPayment

    Object        : Integration.usp_LoadFactPayment
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Payment.
    Called by     : FACT_Load_Payment, FIN_Load_Cash_Application.
    Reads         : stg.CashReceipt, stg.CashAllocation, stg.LockboxFile,
                    stg.SepaRemittance.
    Depends on    : the etl control procedures.

    Correction    : Fact.Payment restates IN PLACE. Cash application is
                    re-allocated by the credit control team for weeks after the
                    money arrives, and finance did not want a reversal row per
                    re-allocation, so the row is updated and
                    [Restatement Version] is incremented. This is the opposite
                    of Fact.Sale, which uses reversal rows. Anything comparing
                    the two facts has to allow for it.

    Regional instruments:
      NA   - lockbox files from the bank, one receipt per lockbox item,
             discounts taken at 2/10 net 30 are common.
      EU   - SEPA credit transfers with a structured remittance reference; the
             reference is what allocates the cash, and unmatched references go
             to the unallocated bucket rather than the reject queue.
      APAC - telegraphic transfers net of withholding tax; the withholding is
             carried separately so the invoice can still be closed in full.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactPayment', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactPayment;
GO

CREATE PROCEDURE Integration.usp_LoadFactPayment
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @LoadStartDate      DATE = NULL,
    @LoadEndDate        DATE = NULL
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
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Payment',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactPayment',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'WWI_AR',
            @ObjectName       = N'Fact.Payment',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        SET @LoadStartDate = ISNULL(@LoadStartDate, TRY_CONVERT(DATE, @WatermarkFrom));
        SET @LoadEndDate   = ISNULL(@LoadEndDate, TRY_CONVERT(DATE, @WatermarkTo));
        IF @LoadStartDate IS NULL SET @LoadStartDate = CONVERT(DATE, '2013-01-01');
        IF @LoadEndDate   IS NULL SET @LoadEndDate   = CONVERT(DATE, SYSDATETIME());

        SELECT
            rec.ReceiptNumber,
            alloc.AllocationLineNumber,
            rec.PaymentDate,
            rec.ValueDate,
            alloc.InvoiceDate,
            rec.CustomerBusinessKey,
            rec.BillToBusinessKey,
            rec.PaymentMethodCode,
            rec.CurrencyCode,
            rec.RegionCode,
            rec.CostCenterCode,
            rec.SalesTerritoryCode,
            alloc.InvoiceNumber,
            rec.RemittanceReference,
            lb.LockboxBatchNumber,
            sepa.SepaEndToEndId,
            rec.PaymentAmount,
            alloc.AllocatedAmount,
            ISNULL(alloc.DiscountTakenAmount, 0)  AS DiscountTakenAmount,
            ISNULL(rec.WithholdingTaxAmount, 0)   AS WithholdingTaxAmount,
            ISNULL(rec.BankChargeAmount, 0)       AS BankChargeAmount,
            ISNULL(alloc.WriteOffAmount, 0)       AS WriteOffAmount,
            ISNULL(rec.FxRate, 1.0)               AS FxRate,
            rec.PaymentStatusCode,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256',
                CONCAT(rec.ReceiptNumber, N'|', alloc.AllocationLineNumber))) AS NaturalKeyHash
        INTO #PaymentWork
        FROM stg.CashReceipt AS rec
        INNER JOIN stg.CashAllocation AS alloc
            ON alloc.ReceiptNumber = rec.ReceiptNumber
        LEFT JOIN stg.LockboxFile AS lb
            ON lb.ReceiptNumber = rec.ReceiptNumber
           AND rec.RegionCode = N'NA'
        LEFT JOIN stg.SepaRemittance AS sepa
            ON sepa.RemittanceReference = rec.RemittanceReference
           AND rec.RegionCode = N'EU'
        WHERE rec.PaymentDate >= @LoadStartDate
          AND rec.PaymentDate <= @LoadEndDate;

        SET @SourceRowCount = @@ROWCOUNT;

        /* An allocation for more than the receipt is a source-system bug that
           surfaces two or three times a year. It is rejected outright; the
           unallocated bucket is only for cash we cannot match, not for cash
           that does not add up. */
        DECLARE @BadReceipt NVARCHAR(50);
        DECLARE bad_alloc_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT ReceiptNumber
            FROM #PaymentWork
            GROUP BY ReceiptNumber, PaymentAmount
            HAVING SUM(AllocatedAmount) > PaymentAmount + 0.01;

        OPEN bad_alloc_cursor;
        FETCH NEXT FROM bad_alloc_cursor INTO @BadReceipt;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'WWI_AR',
                @ObjectName         = N'Fact.Payment',
                @BusinessKey        = @BadReceipt,
                @RejectReasonCode   = N'OVER_ALLOCATED',
                @RejectReason       = N'Allocations exceed the receipt amount',
                @RejectStage        = N'Fact';

            SET @RejectRowCount = @RejectRowCount + 1;
            DELETE FROM #PaymentWork WHERE ReceiptNumber = @BadReceipt;

            FETCH NEXT FROM bad_alloc_cursor INTO @BadReceipt;
        END;

        CLOSE bad_alloc_cursor;
        DEALLOCATE bad_alloc_cursor;

        /* In-place restatement of allocations we have already loaded. */
        UPDATE f
        SET [Allocated Amount]        = w.AllocatedAmount,
            [Unallocated Amount]      = w.PaymentAmount - w.AllocatedAmount,
            [Discount Taken Amount]   = w.DiscountTakenAmount,
            [Write Off Amount]        = w.WriteOffAmount,
            [Withholding Tax Amount]  = w.WithholdingTaxAmount,
            [Payment Status Code]     = w.PaymentStatusCode,
            [Allocated Amount Reporting] = ROUND(w.AllocatedAmount * w.FxRate, 2),
            [Restatement Version]     = ISNULL(f.[Restatement Version], 1) + 1,
            [Batch Id]                = @BatchId,
            [Load Datetime]           = SYSDATETIME()
        FROM Fact.[Payment] AS f
        INNER JOIN #PaymentWork AS w
            ON w.NaturalKeyHash = f.[Natural Key Hash]
        WHERE ROUND(f.[Allocated Amount], 2) <> ROUND(w.AllocatedAmount, 2)
           OR ISNULL(f.[Payment Status Code], N'') <> ISNULL(w.PaymentStatusCode, N'');

        SET @UpdateRowCount = @@ROWCOUNT;

        INSERT INTO Fact.[Payment]
        (
            [Payment Date Key], [Value Date Key], [Invoice Date Key], [Customer Key],
            [Bill To Customer Key], [Payment Method Key], [Currency Key],
            [Sales Territory Key], [Cost Center Key], [Region Code], [Receipt Number],
            [Allocation Line Number], [Invoice Number], [Remittance Reference],
            [Lockbox Batch Number], [Sepa End To End Id], [Transaction Currency Code],
            [Payment Amount], [Allocated Amount], [Unallocated Amount],
            [Discount Taken Amount], [Withholding Tax Amount], [Bank Charge Amount],
            [Write Off Amount], [Fx Rate], [Payment Amount Reporting],
            [Allocated Amount Reporting], [Days To Pay], [Payment Status Code],
            [Restatement Version], [Natural Key Hash], [Inferred Member Flag],
            [Lineage Key], [Batch Id], [Load Datetime]
        )
        SELECT
            w.PaymentDate, w.ValueDate, w.InvoiceDate,
            ISNULL(cust.[Customer Key], 0),
            ISNULL(bill.[Customer Key], ISNULL(cust.[Customer Key], 0)),
            ISNULL(pm.[Payment Method Key], 0),
            ISNULL(cur.[Currency Key], 0),
            CASE WHEN w.SalesTerritoryCode IS NULL THEN -1
                 ELSE ISNULL(terr.[Sales Territory Key], 0) END,
            CASE WHEN w.CostCenterCode IS NULL THEN -1 ELSE ISNULL(cc.[Cost Center Key], 0) END,
            w.RegionCode, w.ReceiptNumber, w.AllocationLineNumber, w.InvoiceNumber,
            w.RemittanceReference, w.LockboxBatchNumber, w.SepaEndToEndId, w.CurrencyCode,
            w.PaymentAmount, w.AllocatedAmount, w.PaymentAmount - w.AllocatedAmount,
            w.DiscountTakenAmount, w.WithholdingTaxAmount, w.BankChargeAmount,
            w.WriteOffAmount, w.FxRate,
            ROUND(w.PaymentAmount * w.FxRate, 2),
            ROUND(w.AllocatedAmount * w.FxRate, 2),
            DATEDIFF(DAY, w.InvoiceDate, w.PaymentDate),
            w.PaymentStatusCode,
            1,
            w.NaturalKeyHash,
            CASE WHEN cust.[Customer Key] IS NULL THEN 1 ELSE 0 END,
            0, @BatchId, SYSDATETIME()
        FROM #PaymentWork AS w
        LEFT JOIN Dimension.[Customer] AS cust
            ON cust.[WWI Customer ID] = TRY_CONVERT(INT, w.CustomerBusinessKey)
           AND cust.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Customer] AS bill
            ON bill.[WWI Customer ID] = TRY_CONVERT(INT, w.BillToBusinessKey)
           AND bill.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Payment Method] AS pm
            ON pm.[Payment Method Code] = w.PaymentMethodCode
        LEFT JOIN Dimension.[Currency] AS cur
            ON cur.[Currency Code] = w.CurrencyCode
        LEFT JOIN Dimension.[Sales Territory] AS terr
            ON terr.[Territory Code] = w.SalesTerritoryCode
        LEFT JOIN Dimension.[Cost Center] AS cc
            ON cc.[Cost Center Code] = w.CostCenterCode
           AND cc.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        WHERE NOT EXISTS
        (
            SELECT 1 FROM Fact.[Payment] AS f
            WHERE f.[Natural Key Hash] = w.NaturalKeyHash
        );

        SET @InsertRowCount = @@ROWCOUNT;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Payment',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @UpdateRowCount     = @UpdateRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'WWI_AR',
            @ObjectName         = N'Fact.Payment',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsUpdated        = @UpdateRowCount,
                @RowsRejected       = @RejectRowCount,
                @WatermarkFrom      = @WatermarkFrom,
                @WatermarkTo        = @WatermarkTo;

        DROP TABLE #PaymentWork;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'bad_alloc_cursor') >= 0
        BEGIN
            CLOSE bad_alloc_cursor;
            DEALLOCATE bad_alloc_cursor;
        END;

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Payment',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactPayment',
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
