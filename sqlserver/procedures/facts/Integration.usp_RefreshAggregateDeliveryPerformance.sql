/*
    Integration.usp_RefreshAggregateDeliveryPerformance

    Object        : Integration.usp_RefreshAggregateDeliveryPerformance
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Delivery Performance Summary.
    Called by     : AGG_Refresh_Delivery_Performance (weekly, Monday 06:00).
    Reads         : Fact.Shipment.
    Depends on    : the etl control procedures.

    ISO week grain, which is the only weekly grain in the estate and does not
    line up with any of the three fiscal calendars. The carrier SLA reviews are
    weekly, so this is what they get.

    On-time is measured against different promises by region: NA against the
    requested delivery date, EU against the carrier's committed date (the
    contracts are written that way) and APAC against the committed date plus a
    customs allowance, because clearance delays are excluded from carrier SLA
    by agreement.

    Service credits are only calculated where the contract has them, which in
    practice means EU.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateDeliveryPerformance', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateDeliveryPerformance;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateDeliveryPerformance
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @WeeksToRefresh     INT = 6
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @FromDate       DATE;

    SET @FromDate = DATEADD(WEEK, -@WeeksToRefresh, CONVERT(DATE, SYSDATETIME()));

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Delivery_Performance',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateDeliveryPerformance',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Delivery Performance Summary]
        WHERE [Iso Week Start Date] >= @FromDate;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Aggregate.[Delivery Performance Summary]
        (
            [Iso Week Start Date], [Iso Week Number], [Carrier Key], [Warehouse Site Key],
            [Sales Territory Key], [Region Code], [Service Level Code], [Consignment Count],
            [Package Count], [Delivered Count], [On Time Count], [Late Count],
            [Failed Attempt Count], [Damaged Count], [Lost Count], [On Time Percent],
            [First Attempt Success Percent], [Damage Rate Percent], [Average Transit Days],
            [Average Customs Hold Days], [Average Pick To Despatch Days], [Total Weight Kg],
            [Chargeable Weight Kg], [Freight Cost Reporting], [Fuel Surcharge Reporting],
            [Duty And Clearance Reporting], [Cost Per Consignment], [Cost Per Chargeable Kg],
            [Sla Target Percent], [Sla Breach Flag], [Service Credit Reporting],
            [Refresh Batch Id], [Refreshed Datetime]
        )
        SELECT
            DATEADD(DAY, 1 - DATEPART(WEEKDAY, sh.[Despatch Date Key]), sh.[Despatch Date Key]),
            DATEPART(ISO_WEEK, sh.[Despatch Date Key]),
            sh.[Carrier Key],
            sh.[Warehouse Site Key],
            sh.[Sales Territory Key],
            sh.[Region Code],
            ISNULL(sh.[Service Level Code], N'STD'),
            COUNT(DISTINCT sh.[Despatch Note Number]),
            SUM(ISNULL(sh.[Package Count], 1)),
            SUM(CASE WHEN sh.[Delivery Confirmed Date Key] IS NOT NULL THEN 1 ELSE 0 END),
            /* Regional on-time definition. */
            SUM(CASE sh.[Region Code]
                    WHEN N'NA' THEN CASE WHEN sh.[Delivery Confirmed Date Key]
                                              <= sh.[Promised Delivery Date Key]
                                         THEN 1 ELSE 0 END
                    WHEN N'EU' THEN CASE WHEN sh.[Delivery Confirmed Date Key]
                                              <= sh.[Promised Delivery Date Key]
                                         THEN 1 ELSE 0 END
                    ELSE CASE WHEN sh.[Delivery Confirmed Date Key]
                                   <= DATEADD(DAY, ISNULL(sh.[Customs Hold Days], 0),
                                              sh.[Promised Delivery Date Key])
                              THEN 1 ELSE 0 END
                END),
            SUM(CASE WHEN sh.[Delivery Confirmed Date Key] > sh.[Promised Delivery Date Key]
                     THEN 1 ELSE 0 END),
            SUM(ISNULL(sh.[Delivery Attempt Count], 0)),
            SUM(CASE WHEN sh.[Damaged Flag] = 1 THEN 1 ELSE 0 END),
            SUM(CASE WHEN sh.[Shipment Status Code] = N'LOST' THEN 1 ELSE 0 END),
            NULL, NULL, NULL,
            AVG(CONVERT(DECIMAL(18, 2), sh.[Despatch To Delivery Lag Days])),
            AVG(CONVERT(DECIMAL(18, 2), ISNULL(sh.[Customs Hold Days], 0))),
            AVG(CONVERT(DECIMAL(18, 2), sh.[Pick To Despatch Lag Days])),
            SUM(ISNULL(sh.[Total Weight Kg], 0)),
            SUM(ISNULL(sh.[Chargeable Weight Kg], 0)),
            SUM(ISNULL(sh.[Freight Charge Reporting], 0)),
            SUM(ISNULL(sh.[Fuel Surcharge], 0)),
            SUM(ISNULL(sh.[Duty And Clearance Amount], 0)),
            NULL, NULL,
            CASE sh.[Region Code] WHEN N'NA' THEN 96.0
                                  WHEN N'EU' THEN 98.0
                                  ELSE 92.0 END,
            0, 0,
            @BatchId, SYSDATETIME()
        FROM Fact.[Shipment] AS sh
        WHERE sh.[Despatch Date Key] >= @FromDate
        GROUP BY
            DATEADD(DAY, 1 - DATEPART(WEEKDAY, sh.[Despatch Date Key]), sh.[Despatch Date Key]),
            DATEPART(ISO_WEEK, sh.[Despatch Date Key]),
            sh.[Carrier Key], sh.[Warehouse Site Key], sh.[Sales Territory Key],
            sh.[Region Code], sh.[Service Level Code];

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        UPDATE Aggregate.[Delivery Performance Summary]
        SET [On Time Percent] = CASE WHEN [Delivered Count] = 0 THEN NULL
                                   ELSE ROUND(100.0 * [On Time Count] / [Delivered Count], 2) END,
            [First Attempt Success Percent] =
                CASE WHEN [Delivered Count] = 0 THEN NULL
                     ELSE ROUND(100.0 * ([Delivered Count] - [Failed Attempt Count])
                                / [Delivered Count], 2) END,
            [Damage Rate Percent] =
                CASE WHEN [Consignment Count] = 0 THEN NULL
                     ELSE ROUND(100.0 * [Damaged Count] / [Consignment Count], 2) END,
            [Cost Per Consignment] =
                CASE WHEN [Consignment Count] = 0 THEN NULL
                     ELSE ROUND(([Freight Cost Reporting] + [Fuel Surcharge Reporting]
                                 + [Duty And Clearance Reporting]) / [Consignment Count], 2) END,
            [Cost Per Chargeable Kg] =
                CASE WHEN ISNULL([Chargeable Weight Kg], 0) = 0 THEN NULL
                     ELSE ROUND(([Freight Cost Reporting] + [Fuel Surcharge Reporting])
                                / [Chargeable Weight Kg], 4) END
        WHERE [Iso Week Start Date] >= @FromDate;

        UPDATE Aggregate.[Delivery Performance Summary]
        SET [Sla Breach Flag] = CASE WHEN [On Time Percent] < [Sla Target Percent] THEN 1 ELSE 0 END,
            /* Only the EU carrier contracts carry a service credit, at 2% of
               freight per point below target, capped at 10%. */
            [Service Credit Reporting] =
                CASE WHEN [Region Code] = N'EU' AND [On Time Percent] < [Sla Target Percent]
                     THEN ROUND([Freight Cost Reporting]
                                * CASE WHEN ([Sla Target Percent] - [On Time Percent]) * 0.02 > 0.10
                                       THEN 0.10
                                       ELSE ([Sla Target Percent] - [On Time Percent]) * 0.02 END, 2)
                     ELSE 0 END
        WHERE [Iso Week Start Date] >= @FromDate;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Delivery Performance Summary',
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

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Aggregate.Delivery Performance Summary',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateDeliveryPerformance',
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
