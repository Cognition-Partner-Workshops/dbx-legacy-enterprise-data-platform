/*
    Integration.usp_LoadFactSupplierPayment

    Object        : Integration.usp_LoadFactSupplierPayment
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Supplier Ledger.
    Called by     : FACT_Load_Supplier_Payment, FIN_Load_Payment_Run.
    Reads         : stg.ApPaymentRun, stg.ApPaymentAllocation,
                    stg.ApInvoiceHeader.
    Depends on    : the etl control procedures.

    Loads by payment run, not by date window: a run is atomic in AP and a
    half-loaded run reconciles to nothing. The runs to load are the ones the
    source has marked CONFIRMED since the watermark, and each run is loaded
    inside its own transaction so a bad run does not lose the good ones.

    Early payment discount and late payment are both measured against the
    invoice due date, which is itself derived from regionally different terms
    (NA net 30 from invoice, EU net 30 from end of month, APAC net 60 from
    statement date).
*/
IF OBJECT_ID(N'Integration.usp_LoadFactSupplierPayment', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactSupplierPayment;
GO

CREATE PROCEDURE Integration.usp_LoadFactSupplierPayment
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);
    DECLARE @PaymentRunId   NVARCHAR(30);
    DECLARE @RunRowCount    BIGINT;

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Supplier_Payment',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactSupplierPayment',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'WWI_AP',
            @ObjectName       = N'Fact.Supplier Payment',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        DECLARE run_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT r.PaymentRunId
            FROM stg.ApPaymentRun AS r
            WHERE r.RunStatusCode = N'CONFIRMED'
              AND r.ConfirmedDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
              AND NOT EXISTS (SELECT 1 FROM Fact.[Supplier Payment] AS f
                              WHERE f.[Payment Run Reference] = r.PaymentRunId)
            ORDER BY r.ConfirmedDatetime;

        OPEN run_cursor;
        FETCH NEXT FROM run_cursor INTO @PaymentRunId;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRANSACTION;

            INSERT INTO Fact.[Supplier Payment]
            (
                [Payment Date Key], [Invoice Date Key], [Due Date Key], [Supplier Key],
                [Payment Method Key], [Payment Terms Key], [Currency Key], [Cost Center Key],
                [Vendor Contract Key], [Region Code], [Payment Run Id], [Payment Reference],
                [Supplier Invoice Number], [Po Number], [Cheque Number], [Bank Account Reference],
                [Sepa End To End Id], [Transaction Currency Code], [Gross Payment Amount],
                [Discount Taken Amount], [Withholding Tax Amount], [Net Payment Amount],
                [Fx Rate], [Net Payment Reporting], [Days Early], [Days Late],
                [Discount Captured Amount], [Discount Lost Amount], [Payment Block Reason Code],
                [Approval Level Code], [Natural Key Hash], [Inferred Member Flag],
                [Lineage Key], [Batch Id], [Load Datetime]
            )
            SELECT
                run.PaymentDate,
                inv.InvoiceDate,
                inv.DueDate,
                ISNULL(sup.[Supplier Key], 0),
                ISNULL(pm.[Payment Method Key], 0),
                ISNULL(terms.[Payment Terms Key], 0),
                ISNULL(cur.[Currency Key], 0),
                CASE WHEN inv.CostCenterCode IS NULL THEN -1
                     ELSE ISNULL(cc.[Cost Center Key], 0) END,
                CASE WHEN inv.ContractReference IS NULL THEN -1
                     ELSE ISNULL(vc.[Vendor Contract Key], 0) END,
                run.RegionCode,
                run.PaymentRunId,
                alloc.PaymentReference,
                alloc.SupplierInvoiceNumber,
                inv.PoNumber,
                CASE WHEN run.RegionCode = N'NA' THEN alloc.ChequeNumber ELSE NULL END,
                alloc.BankAccountReference,
                CASE WHEN run.RegionCode = N'EU' THEN alloc.SepaEndToEndId ELSE NULL END,
                run.CurrencyCode,
                alloc.GrossAmount,
                ISNULL(alloc.DiscountTakenAmount, 0),
                CASE WHEN run.RegionCode = N'APAC' THEN ISNULL(alloc.WithholdingTaxAmount, 0)
                     ELSE 0 END,
                alloc.GrossAmount
                    - ISNULL(alloc.DiscountTakenAmount, 0)
                    - CASE WHEN run.RegionCode = N'APAC' THEN ISNULL(alloc.WithholdingTaxAmount, 0)
                           ELSE 0 END,
                ISNULL(run.FxRate, 1.0),
                ROUND((alloc.GrossAmount - ISNULL(alloc.DiscountTakenAmount, 0))
                      * ISNULL(run.FxRate, 1.0), 2),
                CASE WHEN run.PaymentDate < inv.DueDate
                     THEN DATEDIFF(DAY, run.PaymentDate, inv.DueDate) ELSE 0 END,
                CASE WHEN run.PaymentDate > inv.DueDate
                     THEN DATEDIFF(DAY, inv.DueDate, run.PaymentDate) ELSE 0 END,
                ISNULL(alloc.DiscountTakenAmount, 0),
                CASE WHEN inv.DiscountOfferedAmount > ISNULL(alloc.DiscountTakenAmount, 0)
                     THEN inv.DiscountOfferedAmount - ISNULL(alloc.DiscountTakenAmount, 0)
                     ELSE 0 END,
                inv.PaymentBlockReasonCode,
                run.ApprovalLevelCode,
                CONVERT(VARBINARY(32), HASHBYTES('SHA2_256',
                    CONCAT(run.PaymentRunId, N'|', alloc.PaymentReference, N'|',
                           alloc.SupplierInvoiceNumber))),
                CASE WHEN sup.[Supplier Key] IS NULL THEN 1 ELSE 0 END,
                0, @BatchId, SYSDATETIME()
            FROM stg.ApPaymentRun AS run
            INNER JOIN stg.ApPaymentAllocation AS alloc
                ON alloc.PaymentRunId = run.PaymentRunId
            LEFT JOIN stg.ApInvoiceHeader AS inv
                ON inv.SupplierInvoiceNumber = alloc.SupplierInvoiceNumber
               AND inv.SupplierBusinessKey = alloc.SupplierBusinessKey
            LEFT JOIN Dimension.[Supplier] AS sup
                ON sup.[Supplier Reference] = alloc.SupplierBusinessKey
               AND run.PaymentDate >= sup.[Valid From] AND run.PaymentDate < sup.[Valid To]
            LEFT JOIN Dimension.[Payment Method] AS pm
                ON pm.[Payment Method Code] = run.PaymentMethodCode
            LEFT JOIN Dimension.[Payment Terms] AS terms
                ON terms.[Terms Code] = inv.PaymentTermsCode
            LEFT JOIN Dimension.[Currency] AS cur
                ON cur.[Currency Code] = run.CurrencyCode
            LEFT JOIN Dimension.[Cost Center] AS cc
                ON cc.[Cost Center Code] = inv.CostCenterCode
               AND cc.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
            LEFT JOIN Dimension.[Vendor Contract] AS vc
                ON vc.[Contract Reference] = inv.ContractReference
               AND run.PaymentDate >= vc.[Valid From] AND run.PaymentDate < vc.[Valid To]
            WHERE run.PaymentRunId = @PaymentRunId;

            SET @RunRowCount = @@ROWCOUNT;
            SET @InsertRowCount = @InsertRowCount + @RunRowCount;
            SET @SourceRowCount = @SourceRowCount + @RunRowCount;

            IF @RunRowCount = 0
            BEGIN
                SET @RejectRowCount = @RejectRowCount + 1;

                EXECUTE etl.usp_LogRejectedRecord
                    @PackageExecutionId = @PackageExecutionId,
                    @BatchId            = @BatchId,
                    @SourceSystemCode   = N'WWI_AP',
                    @ObjectName         = N'Fact.Supplier Payment',
                    @BusinessKey        = @PaymentRunId,
                    @RejectReasonCode   = N'EMPTY_PAYMENT_RUN',
                    @RejectReason       = N'Confirmed payment run has no allocation lines',
                    @RejectStage        = N'Fact';
            END;

            COMMIT TRANSACTION;

            FETCH NEXT FROM run_cursor INTO @PaymentRunId;
        END;

        CLOSE run_cursor;
        DEALLOCATE run_cursor;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Supplier Payment',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'WWI_AP',
            @ObjectName         = N'Fact.Supplier Payment',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsRejected       = @RejectRowCount,
                @WatermarkFrom      = @WatermarkFrom,
                @WatermarkTo        = @WatermarkTo;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        IF CURSOR_STATUS('local', 'run_cursor') >= 0
        BEGIN
            CLOSE run_cursor;
            DEALLOCATE run_cursor;
        END;

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Supplier Payment',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactSupplierPayment',
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
