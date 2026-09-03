/*
    Integration.usp_LoadFactDailyInventorySnapshot

    Object        : Integration.usp_LoadFactDailyInventorySnapshot
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Daily Snapshots, Fact.Movement, Fact.Stock Holding.
    Called by     : FACT_Load_Daily_Inventory_Snapshot (04:15 daily).
    Reads         : Fact.Movement, Fact.Stock Holding, stg.StockPosition.
    Depends on    : the etl control procedures.

    Periodic snapshot. One row per item per site per day, derived from the
    warehouse itself rather than from staging - the snapshot has to agree with
    Fact.Movement or the inventory reconciliation fails, and it did for two
    years while this was built from the source extract.

    The snapshot is dense: an item with no movement still gets a row carrying
    the previous day's closing position, which is what makes days-of-cover and
    ageing work. That is also why it is the largest fact in the warehouse.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactDailyInventorySnapshot', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactDailyInventorySnapshot;
GO

CREATE PROCEDURE Integration.usp_LoadFactDailyInventorySnapshot
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SnapshotDate       DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @PriorDate      DATE;

    SET @SnapshotDate = ISNULL(@SnapshotDate, DATEADD(DAY, -1, CONVERT(DATE, SYSDATETIME())));
    SET @PriorDate = DATEADD(DAY, -1, @SnapshotDate);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Daily_Inventory_Snapshot',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactDailyInventorySnapshot',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Fact.[Daily Inventory Snapshot]
        WHERE [Snapshot Date Key] = @SnapshotDate;

        SET @DeleteRowCount = @@ROWCOUNT;

        /* Movement totals for the day, by item and site. */
        SELECT
            m.[Stock Item Key],
            m.[Warehouse Site Key],
            SUM(CASE WHEN m.[Movement Direction] = N'IN' THEN m.[Quantity Base UOM] ELSE 0 END)  AS QtyReceived,
            SUM(CASE WHEN m.[Movement Direction] = N'OUT' THEN -m.[Quantity Base UOM] ELSE 0 END) AS QtyIssued,
            SUM(CASE WHEN m.[Movement Reason Code] IN (N'ADJP', N'ADJN', N'STKT')
                     THEN m.[Quantity Base UOM] ELSE 0 END) AS QtyAdjusted,
            SUM(CASE WHEN m.[Movement Reason Code] = N'SCRP'
                     THEN -m.[Quantity Base UOM] ELSE 0 END) AS QtyScrapped,
            SUM(m.[Quantity Base UOM]) AS QtyNetMovement,
            SUM(m.[Movement Value Reporting])    AS MovementValue
        INTO #DayMovement
        FROM Fact.[Movement] AS m
        WHERE m.[Date Key] = @SnapshotDate
        GROUP BY m.[Stock Item Key], m.[Warehouse Site Key];

        INSERT INTO Fact.[Daily Inventory Snapshot]
        (
            [Snapshot Date Key], [Stock Item Key], [Warehouse Site Key], [Supplier Key],
            [Region Code], [Opening Quantity], [Quantity Received], [Quantity Issued],
            [Quantity Adjusted], [Quantity Scrapped], [Closing Quantity],
            [Quantity Allocated], [Quantity Available], [Quantity Quarantined],
            [Quantity In Transit], [Unit Cost], [Closing Stock Value],
            [Closing Stock Value Reporting], [Average Daily Issues], [Days Of Cover],
            [Reorder Level], [Below Reorder Flag], [Stockout Flag], [Excess Stock Flag],
            [Days Since Last Movement], [Stock Age Bucket Code], [Costing Method Code],
            [Lineage Key], [Batch Id], [Load Datetime]
        )
        SELECT
            @SnapshotDate,
            base.[Stock Item Key],
            base.[Warehouse Site Key],
            ISNULL(base.[Supplier Key], -1),
            base.[Region Code],
            ISNULL(prior.[Quantity On Hand], 0),
            ISNULL(mv.QtyReceived, 0),
            ISNULL(mv.QtyIssued, 0),
            ISNULL(mv.QtyAdjusted, 0),
            ISNULL(mv.QtyScrapped, 0),
            ISNULL(prior.[Quantity On Hand], 0) + ISNULL(mv.QtyNetMovement, 0),
            ISNULL(sh.[Quantity Allocated], 0),
            ISNULL(prior.[Quantity On Hand], 0) + ISNULL(mv.QtyNetMovement, 0)
                - ISNULL(sh.[Quantity Allocated], 0),
            ISNULL(sh.[Quantity Quarantined], 0),
            ISNULL(sh.[Quantity In Transit], 0),
            base.[Last Cost Price],
            ROUND((ISNULL(prior.[Quantity On Hand], 0) + ISNULL(mv.QtyNetMovement, 0))
                  * base.[Last Cost Price], 2),
            ROUND((ISNULL(prior.[Quantity On Hand], 0) + ISNULL(mv.QtyNetMovement, 0))
                  * base.[Last Cost Price] * ISNULL(base.[FX Rate To Reporting], 1.0), 2),
            base.[Average Daily Issues],
            CASE WHEN ISNULL(base.[Average Daily Issues], 0) = 0 THEN NULL
                 ELSE ROUND((ISNULL(prior.[Quantity On Hand], 0) + ISNULL(mv.QtyNetMovement, 0))
                            / base.[Average Daily Issues], 1) END,
            base.[Reorder Level],
            CASE WHEN ISNULL(prior.[Quantity On Hand], 0) + ISNULL(mv.QtyNetMovement, 0)
                      < ISNULL(base.[Reorder Level], 0) THEN 1 ELSE 0 END,
            CASE WHEN ISNULL(prior.[Quantity On Hand], 0) + ISNULL(mv.QtyNetMovement, 0) <= 0
                 THEN 1 ELSE 0 END,
            CASE WHEN ISNULL(base.[Average Daily Issues], 0) > 0
                      AND (ISNULL(prior.[Quantity On Hand], 0) + ISNULL(mv.QtyNetMovement, 0))
                          / base.[Average Daily Issues] > 180 THEN 1 ELSE 0 END,
            DATEDIFF(DAY, base.[Last Movement Date Key], @SnapshotDate),
            CASE
                WHEN DATEDIFF(DAY, base.[Last Movement Date Key], @SnapshotDate) IS NULL THEN N'UNK'
                WHEN DATEDIFF(DAY, base.[Last Movement Date Key], @SnapshotDate) <= 30  THEN N'A030'
                WHEN DATEDIFF(DAY, base.[Last Movement Date Key], @SnapshotDate) <= 90  THEN N'A090'
                WHEN DATEDIFF(DAY, base.[Last Movement Date Key], @SnapshotDate) <= 180 THEN N'A180'
                WHEN DATEDIFF(DAY, base.[Last Movement Date Key], @SnapshotDate) <= 365 THEN N'A365'
                ELSE N'AOLD'
            END,
            CASE base.[Region Code] WHEN N'NA' THEN N'WAVG' WHEN N'EU' THEN N'FIFO' ELSE N'STD' END,
            0, @BatchId, SYSDATETIME()
        FROM Fact.[Stock Holding] AS base
        LEFT JOIN Fact.[Stock Holding] AS sh
            ON sh.[Stock Item Key] = base.[Stock Item Key]
           AND sh.[Warehouse Site Key] = base.[Warehouse Site Key]
        LEFT JOIN Fact.[Daily Inventory Snapshot] AS prior
            ON prior.[Stock Item Key] = base.[Stock Item Key]
           AND prior.[Warehouse Site Key] = base.[Warehouse Site Key]
           AND prior.[Snapshot Date Key] = @PriorDate
        LEFT JOIN #DayMovement AS mv
            ON mv.[Stock Item Key] = base.[Stock Item Key]
           AND mv.[Warehouse Site Key] = base.[Warehouse Site Key];

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Daily Inventory Snapshot',
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

        DROP TABLE #DayMovement;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Fact.Daily Inventory Snapshot',
            @SourceComponent    = N'Snapshot build',
            @ProcedureName      = N'Integration.usp_LoadFactDailyInventorySnapshot',
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
