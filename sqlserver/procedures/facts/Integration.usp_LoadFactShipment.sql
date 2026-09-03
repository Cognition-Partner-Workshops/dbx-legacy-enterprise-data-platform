/*
    Integration.usp_LoadFactShipment

    Object        : Integration.usp_LoadFactShipment
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Shipment.
    Called by     : FACT_Load_Shipment, INV_Load_Carrier_Events (hourly).
    Reads         : stg.Consignment, stg.CarrierTrackingEvent,
                    stg.CustomsClearance.
    Depends on    : the etl control procedures.

    Accumulating snapshot. The row is created at despatch and then updated in
    place as each milestone event arrives; milestone lags are recomputed on
    every update because a late-arriving event can change an earlier one (the
    carrier feeds routinely deliver a "collected" event after the "delivered"
    event).

    Milestones only ever move forward. An event older than the milestone
    already recorded is ignored rather than applied, which is what the
    COALESCE ordering in the UPDATE below is doing.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactShipment', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactShipment;
GO

CREATE PROCEDURE Integration.usp_LoadFactShipment
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
    DECLARE @UpdateRowCount BIGINT = 0;
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Shipment',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactShipment',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'WWI_TMS',
            @ObjectName       = N'Fact.Shipment',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        /* 1. New consignments. */
        INSERT INTO Fact.[Shipment]
        (
            [Despatch Date Key], [Order Date Key], [Promised Delivery Date Key],
            [Customer Key], [Carrier Key], [Warehouse Site Key], [City Key],
            [Sales Territory Key], [Region Code], [Consignment Number], [Order Number],
            [Tracking Reference], [Incoterm Code], [Service Level Code], [Package Count],
            [Total Weight Kg], [Total Volume M3], [Freight Cost Amount],
            [Freight Cost Reporting], [Fx Rate], [Delivery Attempt Count],
            [Shipment Status Code], [Milestones Complete Flag], [Natural Key Hash],
            [Inferred Member Flag], [Lineage Key], [Batch Id], [Load Datetime],
            [Last Milestone Update Datetime]
        )
        SELECT
            c.DespatchDate, c.OrderDate, c.PromisedDeliveryDate,
            ISNULL(cust.[Customer Key], 0),
            ISNULL(car.[Carrier Key], 0),
            ISNULL(site.[Warehouse Site Key], 0),
            ISNULL(city.[City Key], 0),
            CASE WHEN c.SalesTerritoryCode IS NULL THEN -1
                 ELSE ISNULL(terr.[Sales Territory Key], 0) END,
            c.RegionCode, c.ConsignmentNumber, c.OrderNumber, c.TrackingReference,
            c.IncotermCode, c.ServiceLevelCode, c.PackageCount, c.TotalWeightKg,
            c.TotalVolumeM3, c.FreightCostAmount,
            ROUND(c.FreightCostAmount * ISNULL(c.FxRate, 1.0), 2), ISNULL(c.FxRate, 1.0),
            0, N'DESPATCHED', 0,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', c.ConsignmentNumber)),
            CASE WHEN cust.[Customer Key] IS NULL THEN 1 ELSE 0 END,
            0, @BatchId, SYSDATETIME(), SYSDATETIME()
        FROM stg.Consignment AS c
        LEFT JOIN Dimension.[Customer] AS cust
            ON cust.[WWI Customer ID] = TRY_CONVERT(INT, c.CustomerBusinessKey)
           AND cust.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Carrier] AS car
            ON car.[Carrier Code] = c.CarrierCode
        LEFT JOIN Dimension.[Warehouse Site] AS site
            ON site.[Site Code] = c.WarehouseSiteCode
        LEFT JOIN Dimension.[City] AS city
            ON city.[City Code] = c.DeliveryCityCode
           AND city.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Sales Territory] AS terr
            ON terr.[Territory Code] = c.SalesTerritoryCode
        WHERE c.DespatchDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
          AND NOT EXISTS (SELECT 1 FROM Fact.[Shipment] AS f
                          WHERE f.[Despatch Note Number] = c.ConsignmentNumber);

        SET @InsertRowCount = @@ROWCOUNT;

        /* 2. Milestone events. One pivoted row per consignment, then applied. */
        SELECT
            e.ConsignmentNumber,
            MIN(CASE WHEN e.EventTypeCode = N'PICK' THEN e.EventDate END)      AS PickDate,
            MIN(CASE WHEN e.EventTypeCode = N'PACK' THEN e.EventDate END)      AS PackDate,
            MIN(CASE WHEN e.EventTypeCode = N'COLLECT' THEN e.EventDate END)   AS CollectionDate,
            MIN(CASE WHEN e.EventTypeCode = N'ATTEMPT' THEN e.EventDate END)   AS FirstAttemptDate,
            MAX(CASE WHEN e.EventTypeCode = N'DELIVERED' THEN e.EventDate END) AS DeliveryDate,
            SUM(CASE WHEN e.EventTypeCode = N'ATTEMPT' THEN 1 ELSE 0 END)      AS AttemptCount,
            MAX(CASE WHEN e.EventTypeCode = N'DAMAGE' THEN 1 ELSE 0 END)       AS DamagedFlag,
            MAX(CASE WHEN e.EventTypeCode = N'LOST' THEN 1 ELSE 0 END)         AS LostFlag
        INTO #ShipmentEvent
        FROM stg.CarrierTrackingEvent AS e
        WHERE e.EventDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
        GROUP BY e.ConsignmentNumber;

        SET @SourceRowCount = @@ROWCOUNT + @InsertRowCount;

        UPDATE f
        SET [Pick Date Key]              = COALESCE(f.[Picked Date Key], e.PickDate),
            [Pack Date Key]              = COALESCE(f.[Packed Date Key], e.PackDate),
            [Carrier Collection Date Key] = COALESCE(f.[Carrier Collection Date Key], e.CollectionDate),
            [First Delivery Attempt Date Key] = COALESCE(f.[First Delivery Attempt Key], e.FirstAttemptDate),
            [Customs Clearance Date Key] = COALESCE(f.[Customs Cleared Date Key], cc.ClearanceDate),
            [Delivery Confirmed Date Key] = COALESCE(f.[Delivery Confirmed Date Key], e.DeliveryDate),
            [Delivery Attempt Count]     = ISNULL(f.[Delivery Attempt Count], 0) + ISNULL(e.AttemptCount, 0),
            [Damaged Flag]               = CASE WHEN e.DamagedFlag = 1 THEN 1 ELSE f.[Damaged Flag] END,
            [Lost Flag]                  = CASE WHEN e.LostFlag = 1 THEN 1 ELSE f.[Lost In Transit Flag] END,
            [Customs Entry Number]       = COALESCE(f.[Customs Declaration Number], cc.CustomsEntryNumber),
            [Customs Duty Amount]        = COALESCE(f.[Duty And Clearance Amount], cc.DutyAmount),
            [Despatch To Delivery Days]  = DATEDIFF(DAY, f.[Despatch Date Key],
                                                    COALESCE(f.[Delivery Confirmed Date Key], e.DeliveryDate)),
            [Pick To Despatch Days]      = DATEDIFF(DAY, COALESCE(f.[Picked Date Key], e.PickDate),
                                                    f.[Despatch Date Key]),
            [Customs Hold Days]          = DATEDIFF(DAY, COALESCE(f.[Carrier Collection Date Key], e.CollectionDate),
                                                    COALESCE(f.[Customs Cleared Date Key], cc.ClearanceDate)),
            [On Time Delivery Flag]      = CASE
                                               WHEN COALESCE(f.[Delivery Confirmed Date Key], e.DeliveryDate) IS NULL THEN NULL
                                               WHEN COALESCE(f.[Delivery Confirmed Date Key], e.DeliveryDate)
                                                    <= f.[Promised Delivery Date Key] THEN 1
                                               ELSE 0
                                           END,
            [Shipment Status Code]       = CASE
                                               WHEN e.LostFlag = 1 THEN N'LOST'
                                               WHEN COALESCE(f.[Delivery Confirmed Date Key], e.DeliveryDate) IS NOT NULL
                                                    THEN N'DELIVERED'
                                               WHEN e.FirstAttemptDate IS NOT NULL THEN N'ATTEMPTED'
                                               ELSE f.[Shipment Status Code]
                                           END,
            [Milestones Complete Flag]   = CASE
                                               WHEN COALESCE(f.[Delivery Confirmed Date Key], e.DeliveryDate) IS NOT NULL
                                                    AND COALESCE(f.[Picked Date Key], e.PickDate) IS NOT NULL
                                               THEN 1 ELSE 0
                                           END,
            [Batch Id]                   = @BatchId,
            [Last Milestone Update Datetime] = SYSDATETIME()
        FROM Fact.[Shipment] AS f
        INNER JOIN #ShipmentEvent AS e
            ON e.ConsignmentNumber = f.[Despatch Note Number]
        LEFT JOIN stg.CustomsClearance AS cc
            ON cc.ConsignmentNumber = f.[Despatch Note Number];

        SET @UpdateRowCount = @@ROWCOUNT;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Shipment',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @UpdateRowCount     = @UpdateRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'WWI_TMS',
            @ObjectName         = N'Fact.Shipment',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsUpdated        = @UpdateRowCount,
                @WatermarkFrom      = @WatermarkFrom,
                @WatermarkTo        = @WatermarkTo;

        DROP TABLE #ShipmentEvent;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Fact.Shipment',
            @SourceComponent    = N'Accumulating snapshot update',
            @ProcedureName      = N'Integration.usp_LoadFactShipment',
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
