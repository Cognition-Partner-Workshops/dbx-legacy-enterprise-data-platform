/*
    Integration.usp_LoadFactStockHolding

    Object        : Integration.usp_LoadFactStockHolding
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Stock Holding.Extensions.
    Called by     : FACT_Load_Stock_Holding, INV_Refresh_Stock_Position.
    Reads         : stg.StockPosition.
    Depends on    : the etl control procedures.

    Fact.Stock Holding is current state only - one row per item per site. It is
    rebuilt by loading into a staging copy and switching it in, so that readers
    never see a half-built position. If the staging copy cannot be created the
    procedure falls back to TRUNCATE + INSERT, which is what actually happens
    on the two servers where the tables were never partitioned.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactStockHolding', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactStockHolding;
GO

CREATE PROCEDURE Integration.usp_LoadFactStockHolding
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @UsePartitionSwitch BIT = 1
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @AsAtDate       DATE = CONVERT(DATE, SYSDATETIME());
    DECLARE @Sql            NVARCHAR(MAX);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Stock_Holding',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactStockHolding',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        IF @UsePartitionSwitch = 1
           AND NOT EXISTS (SELECT 1 FROM sys.partition_schemes AS ps
                           INNER JOIN sys.indexes AS i ON i.data_space_id = ps.data_space_id
                           WHERE i.object_id = OBJECT_ID(N'Fact.Stock Holding'))
        BEGIN
            /* Not partitioned on this server - fall back and say so in the log. */
            SET @UsePartitionSwitch = 0;

            EXECUTE etl.usp_LogError
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @ErrorSeverity      = N'Warning',
                @SourceName         = N'Fact.Stock Holding',
                @SourceComponent    = N'Partition switch',
                @ProcedureName      = N'Integration.usp_LoadFactStockHolding',
                @ErrorDescription   = N'Table is not partitioned; falling back to truncate and reload';
        END;

        IF @UsePartitionSwitch = 1
        BEGIN
            IF OBJECT_ID(N'Fact.Stock Holding Switch', N'U') IS NOT NULL
                DROP TABLE Fact.[Stock Holding Switch];

            SET @Sql = N'SELECT * INTO Fact.[Stock Holding Switch] FROM Fact.[Stock Holding] WHERE 1 = 0;';
            EXECUTE sp_executesql @Sql;
        END;

        DECLARE @TargetTable NVARCHAR(200) =
            CASE WHEN @UsePartitionSwitch = 1 THEN N'Fact.[Stock Holding Switch]'
                 ELSE N'Fact.[Stock Holding]' END;

        IF @UsePartitionSwitch = 0
        BEGIN
            SELECT @DeleteRowCount = COUNT_BIG(*) FROM Fact.[Stock Holding];
            TRUNCATE TABLE Fact.[Stock Holding];
        END;

        SET @Sql = N'
        INSERT INTO ' + @TargetTable + N'
        (
            [Stock Item Key], [Warehouse Site Key], [Region Code], [As At Date],
            [Quantity On Hand], [Quantity Allocated], [Quantity Available],
            [Quantity In Transit], [Quantity On Order], [Quantity Quarantined],
            [Unit Cost], [Stock Value], [Stock Value Reporting], [Days Of Cover],
            [Reorder Required Flag], [Last Movement Date], [Last Stocktake Date],
            [Lineage Key], [Batch Id], [Load Datetime]
        )
        SELECT
            ISNULL(item.[Stock Item Key], 0),
            ISNULL(site.[Warehouse Site Key], 0),
            sp.RegionCode,
            @AsAtDate,
            sp.QuantityOnHand,
            ISNULL(sp.QuantityAllocated, 0),
            sp.QuantityOnHand - ISNULL(sp.QuantityAllocated, 0) - ISNULL(sp.QuantityQuarantined, 0),
            ISNULL(sp.QuantityInTransit, 0),
            ISNULL(sp.QuantityOnOrder, 0),
            ISNULL(sp.QuantityQuarantined, 0),
            sp.UnitCost,
            ROUND(sp.QuantityOnHand * sp.UnitCost, 2),
            ROUND(sp.QuantityOnHand * sp.UnitCost * ISNULL(sp.FxRate, 1.0), 2),
            CASE WHEN ISNULL(sp.AverageDailyIssues, 0) = 0 THEN NULL
                 ELSE ROUND(sp.QuantityOnHand / sp.AverageDailyIssues, 2) END,
            CASE WHEN sp.QuantityOnHand - ISNULL(sp.QuantityAllocated, 0) < ISNULL(sp.ReorderLevel, 0)
                 THEN 1 ELSE 0 END,
            sp.LastMovementDate,
            sp.LastStocktakeDate,
            0, @BatchId, SYSDATETIME()
        FROM stg.StockPosition AS sp
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Code] = sp.StockItemCode
           AND item.[Valid To] = CONVERT(DATETIME2(7), ''9999-12-31'')
        LEFT JOIN Dimension.[Warehouse Site] AS site
            ON site.[Site Code] = sp.WarehouseSiteCode;';

        EXECUTE sp_executesql @Sql,
            N'@AsAtDate DATE, @BatchId BIGINT',
            @AsAtDate = @AsAtDate, @BatchId = @BatchId;

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        IF @UsePartitionSwitch = 1
        BEGIN
            SELECT @DeleteRowCount = COUNT_BIG(*) FROM Fact.[Stock Holding];

            BEGIN TRANSACTION;
            TRUNCATE TABLE Fact.[Stock Holding];

            INSERT INTO Fact.[Stock Holding]
            SELECT * FROM Fact.[Stock Holding Switch];

            COMMIT TRANSACTION;

            DROP TABLE Fact.[Stock Holding Switch];
        END;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Stock Holding',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @DeleteRowCount     = @DeleteRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsDeleted        = @DeleteRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Stock Holding',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactStockHolding',
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
