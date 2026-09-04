/*
    Integration.usp_LoadFactOrderFulfilment

    Object        : Integration.usp_LoadFactOrderFulfilment
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Order Fulfilment, Fact.Order, Fact.Shipment,
                    Fact.Sale, Fact.Payment.
    Called by     : FACT_Load_Order_Fulfilment (nightly, after every other fact).
    Reads         : Fact.Order, Fact.Shipment, Fact.Sale, Fact.Payment.
    Depends on    : the etl control procedures.

    Order-to-cash accumulating snapshot: order -> pick -> despatch -> deliver
    -> invoice -> cash. One row per order, created when the order is first
    seen and updated on every run until the cash milestone lands, after which
    the row is frozen ([Cycle Complete Flag] = 1) and skipped, because
    re-opening a closed order-to-cash row broke the DSO trend twice.

    Lags are stored, not derived, so that the milestone lag as at the day the
    order completed is preserved even if a dimension is later restated.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactOrderFulfilment', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactOrderFulfilment;
GO

CREATE PROCEDURE Integration.usp_LoadFactOrderFulfilment
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @ReopenClosedRows   BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @UpdateRowCount BIGINT = 0;

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Order_Fulfilment',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactOrderFulfilment',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        /* New orders. */
        INSERT INTO Fact.[Order Fulfilment]
        (
            [Order Date Key], [Customer Key], [Sales Territory Key], [Sales Channel Key],
            [Salesperson Key], [Warehouse Site Key], [Region Code], [Order Number],
            [Order Line Number], [Order Line Count], [Order Value Reporting],
            [Transaction Currency Code], [Cycle Complete Flag], [Pipeline Status Code],
            [Lineage Key], [Batch Id], [Load Datetime], [Last Milestone Update]
        )
        SELECT
            o.[Order Date Key],
            MAX(o.[Customer Key]),
            MAX(o.[Sales Territory Key]),
            MAX(o.[Sales Channel Key]),
            MAX(o.[Salesperson Key]),
            -1,
            MAX(o.[Region Code]),
            o.[Order Number],
            0,
            COUNT_BIG(*),
            SUM(o.[Net Order Amount]),
            MAX(o.[Transaction Currency Code]),
            0,
            N'ORDERED',
            0, @BatchId, SYSDATETIME(), SYSDATETIME()
        FROM Fact.[Order] AS o
        WHERE NOT EXISTS (SELECT 1 FROM Fact.[Order Fulfilment] AS f
                          WHERE f.[Order Number] = o.[Order Number])
        GROUP BY o.[Order Date Key], o.[Order Number];

        SET @InsertRowCount = @@ROWCOUNT;

        /* Milestone fill-in. Each milestone comes from a different fact, which
           is why this is five OUTER APPLYs rather than one join. */
        UPDATE f
        SET [Pick Date Key]     = COALESCE(f.[Pick Date Key], sh.PickDate),
            [Despatch Date Key] = COALESCE(f.[Despatch Date Key], sh.DespatchDate),
            [Delivery Date Key] = COALESCE(f.[Delivery Date Key], sh.DeliveryDate),
            [Invoice Date Key]  = COALESCE(f.[Invoice Date Key], inv.InvoiceDate),
            [Cash Applied Date Key] = COALESCE(f.[Cash Applied Date Key], cash.PaymentDate),
            [Invoice Number]    = COALESCE(f.[Invoice Number], inv.InvoiceNumber),
            [Despatch Note Number] = COALESCE(f.[Despatch Note Number], sh.ConsignmentNumber),
            [Invoiced Value Reporting] = COALESCE(f.[Invoiced Value Reporting], inv.InvoicedAmount),
            [Cash Applied Reporting] = COALESCE(cash.CashAmount, f.[Cash Applied Reporting]),
            [Order To Pick Lag Days]      = DATEDIFF(DAY, f.[Order Date Key],
                                                 COALESCE(f.[Pick Date Key], sh.PickDate)),
            [Pick To Despatch Lag Days]   = DATEDIFF(DAY, COALESCE(f.[Pick Date Key], sh.PickDate),
                                                 COALESCE(f.[Despatch Date Key], sh.DespatchDate)),
            [Despatch To Delivery Lag Days] = DATEDIFF(DAY, COALESCE(f.[Despatch Date Key], sh.DespatchDate),
                                                 COALESCE(f.[Delivery Date Key], sh.DeliveryDate)),
            [Delivery To Invoice Lag Days]  = DATEDIFF(DAY, COALESCE(f.[Delivery Date Key], sh.DeliveryDate),
                                                 COALESCE(f.[Invoice Date Key], inv.InvoiceDate)),
            [Invoice To Cash Lag Days]      = DATEDIFF(DAY, COALESCE(f.[Invoice Date Key], inv.InvoiceDate),
                                                 COALESCE(f.[Cash Applied Date Key], cash.PaymentDate)),
            [Order To Cash Cycle Days]      = DATEDIFF(DAY, f.[Order Date Key],
                                                 COALESCE(f.[Cash Applied Date Key], cash.PaymentDate)),
            [Pipeline Status Code] = CASE
                                           WHEN COALESCE(f.[Cash Applied Date Key], cash.PaymentDate) IS NOT NULL THEN N'CASH'
                                           WHEN COALESCE(f.[Invoice Date Key], inv.InvoiceDate) IS NOT NULL THEN N'INVOICED'
                                           WHEN COALESCE(f.[Delivery Date Key], sh.DeliveryDate) IS NOT NULL THEN N'DELIVERED'
                                           WHEN COALESCE(f.[Despatch Date Key], sh.DespatchDate) IS NOT NULL THEN N'DESPATCHED'
                                           WHEN COALESCE(f.[Pick Date Key], sh.PickDate) IS NOT NULL THEN N'PICKED'
                                           ELSE N'ORDERED'
                                       END,
            [Cycle Complete Flag] = CASE WHEN COALESCE(f.[Cash Applied Date Key], cash.PaymentDate) IS NOT NULL
                                         THEN 1 ELSE 0 END,
            [Batch Id] = @BatchId,
            [Last Milestone Update] = SYSDATETIME()
        FROM Fact.[Order Fulfilment] AS f
        OUTER APPLY
        (
            SELECT TOP (1)
                   s.[Despatch Note Number] AS ConsignmentNumber,
                   s.[Picked Date Key] AS PickDate,
                   s.[Despatch Date Key] AS DespatchDate,
                   s.[Delivery Confirmed Date Key] AS DeliveryDate
            FROM Fact.[Shipment] AS s
            WHERE s.[Order Number] = f.[Order Number]
            ORDER BY s.[Despatch Date Key]
        ) AS sh
        OUTER APPLY
        (
            SELECT TOP (1) i.[Invoice Number] AS InvoiceNumber,
                   i.[Invoice Date Key] AS InvoiceDate,
                   SUM(i.[Net Amount]) OVER (PARTITION BY i.[Invoice Number]) AS InvoicedAmount
            FROM Fact.[Sale] AS i
            WHERE i.[Order Number] = f.[Order Number]
              AND ISNULL(i.[Correction Type Code], N'ORIG') <> N'REV'
            ORDER BY i.[Invoice Date Key]
        ) AS inv
        OUTER APPLY
        (
            SELECT TOP (1) p.[Payment Date Key] AS PaymentDate,
                   SUM(p.[Allocated Amount]) OVER (PARTITION BY p.[Invoice Number]) AS CashAmount
            FROM Fact.[Payment] AS p
            WHERE p.[Invoice Number] = COALESCE(f.[Invoice Number], N'~none~')
            ORDER BY p.[Payment Date Key] DESC
        ) AS cash
        WHERE (@ReopenClosedRows = 1 OR ISNULL(f.[Cycle Complete Flag], 0) = 0);

        SET @UpdateRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount + @UpdateRowCount;

        /* Orders sitting past the regional escalation threshold with no
           despatch are flagged for the fulfilment exception report. */
        UPDATE Fact.[Order Fulfilment]
        SET [Stalled Flag] = 1,
            [Stalled Reason Code] = CASE
                                        WHEN [Pick Date Key] IS NULL THEN N'NOPICK'
                                        WHEN [Despatch Date Key] IS NULL THEN N'NODESP'
                                        WHEN [Invoice Date Key] IS NULL THEN N'NOINV'
                                        ELSE N'NOCASH'
                                    END
        WHERE ISNULL([Cycle Complete Flag], 0) = 0
          AND DATEDIFF(DAY, [Order Date Key], CONVERT(DATE, SYSDATETIME())) >
              CASE [Region Code] WHEN N'NA' THEN 30 WHEN N'EU' THEN 45 ELSE 60 END;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Order Fulfilment',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @UpdateRowCount     = @UpdateRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsUpdated        = @UpdateRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Order Fulfilment',
            @SourceComponent    = N'Accumulating snapshot update',
            @ProcedureName      = N'Integration.usp_LoadFactOrderFulfilment',
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
