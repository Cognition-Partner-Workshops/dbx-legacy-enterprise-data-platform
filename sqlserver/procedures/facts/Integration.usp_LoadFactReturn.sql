/*
    Integration.usp_LoadFactReturn

    Object        : Integration.usp_LoadFactReturn
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Return, Fact.Sale.
    Called by     : FACT_Load_Return, SLS_Load_Returns.
    Reads         : stg.ReturnAuthorisationLine, stg.ReturnAuthorisation.
    Depends on    : the etl control procedures.

    Quantities and amounts are stored NEGATIVE. Every report that unions
    returns with sales relies on that, and the one report that does not
    (vw_ReturnsRateByCategory) negates them back.

    Statutory return windows differ:
      EU   - 14 day right of withdrawal from delivery, no reason required.
      APAC - jurisdiction specific, 7 to 30 days, held in the return reason
             reference data rather than in code.
      NA   - no statutory window; the 30 day figure is commercial policy and is
             evaluated against the invoice date, not the delivery date.

    The margin reversal uses the cost that was on the original sale line, not
    today's cost, which is why the lookup joins back to Fact.Sale rather than
    to the item dimension.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactReturn', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactReturn;
GO

CREATE PROCEDURE Integration.usp_LoadFactReturn
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
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Return',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactReturn',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'WWI_RMA',
            @ObjectName       = N'Fact.Return',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        SET @LoadStartDate = ISNULL(@LoadStartDate, TRY_CONVERT(DATE, @WatermarkFrom));
        SET @LoadEndDate   = ISNULL(@LoadEndDate, TRY_CONVERT(DATE, @WatermarkTo));
        IF @LoadStartDate IS NULL SET @LoadStartDate = CONVERT(DATE, '2013-01-01');
        IF @LoadEndDate   IS NULL SET @LoadEndDate   = CONVERT(DATE, SYSDATETIME());

        DELETE FROM Fact.[Return]
        WHERE [Return Date Key] >= @LoadStartDate
          AND [Return Date Key] <= @LoadEndDate;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Fact.[Return]
        (
            [Return Date Key], [Original Invoice Date Key], [Credit Note Date Key],
            [Customer Key], [Stock Item Key], [Return Reason Key], [Warehouse Site Key],
            [Sales Territory Key], [Salesperson Key], [Currency Key], [Region Code],
            [Rma Number], [Rma Line Number], [Original Invoice Number], [Credit Note Number],
            [Quantity Returned], [Quantity Restocked], [Quantity Scrapped], [Source UOM Code],
            [Gross Credit Amount], [Restocking Fee Amount], [Tax Credit Amount],
            [Net Credit Amount], [Fx Rate], [Net Credit Reporting],
            [Cost Of Return Amount], [Margin Reversal Amount], [Days Since Invoice],
            [Within Statutory Window Flag], [Statutory Window Days], [Faulty Flag],
            [Disposition Code], [Natural Key Hash], [Inferred Member Flag],
            [Lineage Key], [Batch Id], [Load Datetime]
        )
        SELECT
            hdr.ReturnDate,
            hdr.OriginalInvoiceDate,
            hdr.CreditNoteDate,
            ISNULL(cust.[Customer Key], 0),
            ISNULL(item.[Stock Item Key], 0),
            ISNULL(rr.[Return Reason Key], 0),
            ISNULL(site.[Warehouse Site Key], 0),
            CASE WHEN hdr.SalesTerritoryCode IS NULL THEN -1
                 ELSE ISNULL(terr.[Sales Territory Key], 0) END,
            CASE WHEN hdr.SalespersonCode IS NULL THEN -1
                 ELSE ISNULL(sp.[Salesperson Key], 0) END,
            ISNULL(cur.[Currency Key], 0),
            hdr.RegionCode,
            hdr.RmaNumber,
            lin.RmaLineNumber,
            hdr.OriginalInvoiceNumber,
            hdr.CreditNoteNumber,
            -ABS(lin.QuantityReturned),
            -ABS(ISNULL(lin.QuantityRestocked, 0)),
            -ABS(ISNULL(lin.QuantityScrapped, 0)),
            lin.UomCode,
            -ABS(ROUND(lin.QuantityReturned * lin.UnitPrice, 2)),
            ISNULL(lin.RestockingFeeAmount, 0),
            -ABS(ISNULL(lin.TaxCreditAmount, 0)),
            -ABS(ROUND(lin.QuantityReturned * lin.UnitPrice, 2)) + ISNULL(lin.RestockingFeeAmount, 0),
            ISNULL(hdr.FxRate, 1.0),
            ROUND((-ABS(ROUND(lin.QuantityReturned * lin.UnitPrice, 2))
                   + ISNULL(lin.RestockingFeeAmount, 0)) * ISNULL(hdr.FxRate, 1.0), 2),
            -ABS(ROUND(lin.QuantityReturned * ISNULL(orig.UnitCost, lin.UnitCost), 2)),
            ROUND(ABS(lin.QuantityReturned) * (ISNULL(orig.UnitCost, lin.UnitCost) - lin.UnitPrice), 2),
            DATEDIFF(DAY, hdr.OriginalInvoiceDate, hdr.ReturnDate),
            CASE
                WHEN hdr.RegionCode = N'EU'
                     AND DATEDIFF(DAY, ISNULL(hdr.DeliveryDate, hdr.OriginalInvoiceDate), hdr.ReturnDate) <= 14 THEN 1
                WHEN hdr.RegionCode = N'APAC'
                     AND DATEDIFF(DAY, ISNULL(hdr.DeliveryDate, hdr.OriginalInvoiceDate), hdr.ReturnDate)
                         <= ISNULL(rr.[Statutory Window Days], 7) THEN 1
                WHEN hdr.RegionCode = N'NA'
                     AND DATEDIFF(DAY, hdr.OriginalInvoiceDate, hdr.ReturnDate) <= 30 THEN 1
                ELSE 0
            END,
            CASE
                WHEN hdr.RegionCode = N'EU' THEN 14
                WHEN hdr.RegionCode = N'APAC' THEN ISNULL(rr.[Statutory Window Days], 7)
                ELSE 30
            END,
            CASE WHEN rr.[Faulty Goods Flag] = 1 THEN 1 ELSE 0 END,
            lin.DispositionCode,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256',
                CONCAT(hdr.RmaNumber, N'|', lin.RmaLineNumber))),
            CASE WHEN cust.[Customer Key] IS NULL THEN 1 ELSE 0 END,
            0, @BatchId, SYSDATETIME()
        FROM stg.ReturnAuthorisationLine AS lin
        INNER JOIN stg.ReturnAuthorisation AS hdr
            ON hdr.RmaNumber = lin.RmaNumber
        LEFT JOIN Dimension.[Customer] AS cust
            ON cust.[WWI Customer ID] = TRY_CONVERT(INT, hdr.CustomerBusinessKey)
           AND hdr.ReturnDate >= cust.[Valid From] AND hdr.ReturnDate < cust.[Valid To]
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Code] = lin.StockItemCode
           AND hdr.ReturnDate >= item.[Valid From] AND hdr.ReturnDate < item.[Valid To]
        LEFT JOIN Dimension.[Return Reason] AS rr
            ON rr.[Reason Code] = lin.ReturnReasonCode
        LEFT JOIN Dimension.[Warehouse Site] AS site
            ON site.[Site Code] = hdr.WarehouseSiteCode
        LEFT JOIN Dimension.[Sales Territory] AS terr
            ON terr.[Territory Code] = hdr.SalesTerritoryCode
        LEFT JOIN Dimension.[Salesperson] AS sp
            ON sp.[Salesperson Code] = hdr.SalespersonCode
           AND sp.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Currency] AS cur
            ON cur.[Currency Code] = hdr.CurrencyCode
        OUTER APPLY
        (
            SELECT TOP (1)
                   CASE WHEN s.[Quantity Base UOM] = 0 THEN NULL
                        ELSE s.[Cost Of Sale Amount] / s.[Quantity Base UOM] END AS UnitCost
            FROM Fact.[Sale] AS s
            WHERE s.[Invoice Number] = hdr.OriginalInvoiceNumber
              AND s.[Stock Item Key] = ISNULL(item.[Stock Item Key], 0)
              AND ISNULL(s.[Correction Type Code], N'ORIG') <> N'REV'
            ORDER BY s.[Sale Key] DESC
        ) AS orig
        WHERE hdr.ReturnDate >= @LoadStartDate
          AND hdr.ReturnDate <= @LoadEndDate
          AND hdr.RmaStatusCode IN (N'APPROVED', N'RECEIVED', N'CREDITED');

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Returns whose original invoice cannot be found still load - the
           goods came back - but they are counted so the RMA team can chase. */
        SELECT @RejectRowCount = COUNT_BIG(*)
        FROM Fact.[Return]
        WHERE [Batch Id] = @BatchId AND [Cost Of Return Amount] IS NULL;

        IF @RejectRowCount > 0
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'WWI_RMA',
                @ObjectName         = N'Fact.Return',
                @BusinessKey        = N'(grouped)',
                @RejectReasonCode   = N'RETURN_NO_ORIGINAL',
                @RejectReason       = N'Return has no matching original sale line; cost reversal is null',
                @RejectStage        = N'Fact';

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Return',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @DeleteRowCount     = @DeleteRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'WWI_RMA',
            @ObjectName         = N'Fact.Return',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsDeleted        = @DeleteRowCount,
                @RowsRejected       = @RejectRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Fact.Return',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactReturn',
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
