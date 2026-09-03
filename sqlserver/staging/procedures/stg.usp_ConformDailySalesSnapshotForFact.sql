/*
    stg.usp_ConformDailySalesSnapshotForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_DailySalesSnapshot (SSIS)
    Reads         : stg.SaleLine, stg.Sale, stg.Customer, stg.StockItem, stg.OrderLine
    Writes        : work.SaleLineEnriched, stg.DailySalesSnapshot
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The daily aggregate is built in two steps, as it has been since the nightly
    window stopped fitting: the sale lines are enriched once into
    work.SaleLineEnriched with the margin and the resolved keys, and the
    aggregate is then a straight group-by over that table.

    The three key columns the fact selects (CustomerKey, StockItemKey,
    InvoiceDateKey) are the source-side integer keys, not warehouse surrogates -
    the fact package looks the surrogates up itself. The date key is the usual
    yyyymmdd integer.

    Tax is regional and is carried, not recomputed: NA lines are net of sales
    tax, EU lines are decomposed from a VAT-inclusive gross by the sale line
    load, and APAC lines carry GST rounded at line level. Summing the line tax
    here is therefore the only safe aggregation.
*/

IF OBJECT_ID(N'stg.usp_ConformDailySalesSnapshotForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformDailySalesSnapshotForFact;
GO

CREATE PROCEDURE stg.usp_ConformDailySalesSnapshotForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SnapshotDate       DATE = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.DailySalesSnapshot';
    DECLARE @WorkObject   NVARCHAR(200) = N'work.SaleLineEnriched';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @EnrichedRows BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        IF @SnapshotDate IS NULL
            SELECT @SnapshotDate = MAX(sl.InvoiceDate)
            FROM stg.SaleLine AS sl
            WHERE sl.BatchId = @BatchId;

        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.SaleLine AS sl
        WHERE sl.BatchId     = @BatchId
          AND sl.InvoiceDate = @SnapshotDate;

        DELETE FROM work.SaleLineEnriched
        WHERE BatchId = @BatchId;

        DELETE FROM stg.DailySalesSnapshot
        WHERE BatchId      = @BatchId
          AND SnapshotDate = @SnapshotDate;

        BEGIN TRANSACTION;

        INSERT INTO work.SaleLineEnriched
        (
            BatchId, PackageExecutionId, SaleLineBusinessKey, SaleBusinessKey,
            CustomerBusinessKey, BillToCustomerBusinessKey, StockItemBusinessKey,
            SalespersonBusinessKey, GeographyBusinessKey, InvoiceDate, RegionCode,
            Quantity, NetLineAmount, TaxAmount, GrossLineAmount, NetLineAmountUsd,
            LineProfitAmount, CommissionAmount, MarginPercent, MarginOutlierFlag,
            LookupFailureList, IsReadyForFact
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            sl.SaleLineBusinessKey,
            sl.SaleBusinessKey,
            s.CustomerBusinessKey,
            s.BillToCustomerBusinessKey,
            sl.StockItemBusinessKey,
            s.SalespersonBusinessKey,
            ca.GeographyBusinessKey,
            sl.InvoiceDate,
            sl.RegionCode,
            sl.Quantity,
            sl.NetLineAmount,
            sl.TaxAmount,
            sl.GrossLineAmount,
            sl.NetLineAmountUsd,
            sl.LineProfitAmount,
            sl.CommissionAmount,
            CASE
                WHEN ISNULL(sl.NetLineAmount, 0) = 0 THEN NULL
                ELSE ROUND(100.0 * ISNULL(sl.LineProfitAmount, 0) / sl.NetLineAmount, 4)
            END,
            CASE
                WHEN ISNULL(sl.NetLineAmount, 0) = 0 THEN 0
                WHEN ABS(100.0 * ISNULL(sl.LineProfitAmount, 0) / sl.NetLineAmount) > 90 THEN 1
                ELSE 0
            END,
            CONCAT_WS(N',',
                CASE WHEN s.CustomerBusinessKey IS NULL   THEN N'CUSTOMER' END,
                CASE WHEN sl.StockItemBusinessKey IS NULL THEN N'STOCKITEM' END,
                CASE WHEN ca.GeographyBusinessKey IS NULL THEN N'GEOGRAPHY' END),
            CASE
                WHEN s.CustomerBusinessKey IS NOT NULL
                     AND sl.StockItemBusinessKey IS NOT NULL THEN 1
                ELSE 0
            END
        FROM stg.SaleLine AS sl
        INNER JOIN stg.Sale AS s
            ON  s.SaleBusinessKey = sl.SaleBusinessKey
            AND s.BatchId         = @BatchId
        OUTER APPLY
        (
            SELECT TOP (1) a.GeographyBusinessKey
            FROM stg.CustomerAddress AS a
            WHERE a.CustomerBusinessKey = s.CustomerBusinessKey
              AND a.BatchId             = @BatchId
            ORDER BY a.StagingCustomerAddressId DESC
        ) AS ca
        WHERE sl.BatchId     = @BatchId
          AND sl.InvoiceDate = @SnapshotDate;

        SET @EnrichedRows = @@ROWCOUNT;

        INSERT INTO stg.DailySalesSnapshot
        (
            SnapshotDate, CustomerKey, StockItemKey, InvoiceDateKey, SourceSystemCode,
            RegionCode, Quantity, GrossAmount, DiscountAmount, NetAmount, TaxAmount,
            TotalCostAmount, MarginAmount, MarginPercent, InvoiceLineCount, DqStatusCode,
            RowHash, BatchId, PackageExecutionId
        )
        SELECT
            @SnapshotDate,
            c.OltpCustomerId,
            si.OltpStockItemId,
            CONVERT(INT, FORMAT(sle.InvoiceDate, N'yyyyMMdd')),
            @SourceSystemCode,
            sle.RegionCode,
            SUM(ISNULL(sle.Quantity, 0)),
            SUM(ISNULL(sle.GrossLineAmount, 0)),
            SUM(ISNULL(disc.LineDiscountAmount, 0)),
            SUM(ISNULL(sle.NetLineAmount, 0)),
            SUM(ISNULL(sle.TaxAmount, 0)),
            SUM(ISNULL(sle.NetLineAmount, 0) - ISNULL(sle.LineProfitAmount, 0)),
            SUM(ISNULL(sle.LineProfitAmount, 0)),
            CASE
                WHEN SUM(ISNULL(sle.NetLineAmount, 0)) = 0 THEN NULL
                ELSE ROUND(100.0 * SUM(ISNULL(sle.LineProfitAmount, 0))
                           / SUM(ISNULL(sle.NetLineAmount, 0)), 4)
            END,
            CONVERT(INT, COUNT_BIG(*)),
            CASE
                WHEN MIN(CONVERT(INT, sle.IsReadyForFact)) = 0 THEN N'WARN'
                WHEN MAX(CONVERT(INT, sle.MarginOutlierFlag)) = 1 THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(c.OltpCustomerId, N'|', si.OltpStockItemId, N'|', @SnapshotDate, N'|',
                       SUM(ISNULL(sle.NetLineAmount, 0)), N'|',
                       SUM(ISNULL(sle.Quantity, 0)))),
            @BatchId,
            @PackageExecutionId
        FROM work.SaleLineEnriched AS sle
        INNER JOIN stg.Customer AS c
            ON  c.CustomerBusinessKey = sle.CustomerBusinessKey
            AND c.BatchId             = @BatchId
            AND c.IsSurvivorRow       = 1
        INNER JOIN stg.StockItem AS si
            ON  si.StockItemBusinessKey = sle.StockItemBusinessKey
            AND si.BatchId              = @BatchId
        OUTER APPLY
        (
            SELECT TOP (1) ol.LineDiscountAmount
            FROM stg.OrderLine AS ol
            WHERE ol.BatchId              = @BatchId
              AND ol.StockItemBusinessKey = sle.StockItemBusinessKey
              AND ol.OrderDate           <= sle.InvoiceDate
            ORDER BY ol.OrderDate DESC
        ) AS disc
        WHERE sle.BatchId = @BatchId
        GROUP BY c.OltpCustomerId, si.OltpStockItemId, sle.InvoiceDate, sle.RegionCode;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.DailySalesSnapshot AS dss
        WHERE dss.BatchId      = @BatchId
          AND dss.DqStatusCode = N'FAIL';

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @WorkObject,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @EnrichedRows,
            @InsertRowCount     = @EnrichedRows,
            @RejectRowCount     = 0;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @FailRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'FACT_Load_DailySalesSnapshot',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformDailySalesSnapshotForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
