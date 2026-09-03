/*
    stg.usp_AppendIncremental_Shipment

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_LOAD_SHIPMENT (SSIS)
    Reads         : raw.SqlShipment, raw.SqlShipmentLine, raw.FileCarrierScan,
                    stg.Sale, stg.Geography, ref.Country, ref.Region, ref.FxRateDaily
    Writes        : stg.Shipment, stg.ShipmentLine, err.RejectedShipment
    Control       : etl.usp_GetWatermark, etl.usp_SetWatermark, etl.usp_LogRowCount,
                    etl.usp_LogError

    Loads the header and the lines in one procedure because the carrier scan file
    is matched on the shipment reference, and matching it twice was the cause of
    the 2017 duplicate-scan incident.

    Regional divergence:
        NA   depot scan weights arrive in pounds and are converted to kilograms.
        EU   intra-EU movements need no customs declaration, so a missing
             declaration reference is not a reject for an EU-to-EU lane.
        APAC every cross-border lane needs a declaration reference; a missing one
             is a hard reject because the shipment cannot clear.

    The cold-chain check on the lines is deliberately kept as a cursor. It was
    written that way in 2012 so the operations team could add a per-carrier
    threshold without a code change, and nobody has revisited it.
*/

IF OBJECT_ID(N'stg.usp_AppendIncremental_Shipment', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_AppendIncremental_Shipment;
GO

CREATE PROCEDURE stg.usp_AppendIncremental_Shipment
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP',
    @ReloadFullHistory  BIT = 0,
    @ColdChainLimitC    DECIMAL(9,2) = 8.00
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'stg.Shipment';
    DECLARE @LineObject    NVARCHAR(200) = N'stg.ShipmentLine';
    DECLARE @WatermarkFrom NVARCHAR(50);
    DECLARE @WatermarkTo   NVARCHAR(50);
    DECLARE @FromUtc       DATETIME2(3);
    DECLARE @SourceRows    BIGINT = 0;
    DECLARE @InsertedRows  BIGINT = 0;
    DECLARE @LineRows      BIGINT = 0;
    DECLARE @RejectedRows  BIGINT = 0;
    DECLARE @MaxEditedWhen DATETIME2(3);
    DECLARE @LbToKgFactor  DECIMAL(18,9) = 0.453592370;

    --  Cursor variables for the cold-chain sweep.
    DECLARE @LineKey        NVARCHAR(120);
    DECLARE @LineTempC      DECIMAL(9,2);
    DECLARE @LineCarrier    NVARCHAR(30);
    DECLARE @BreachCount    INT = 0;

    BEGIN TRY
        EXEC etl.usp_GetWatermark
            @SourceSystemCode  = @SourceSystemCode,
            @ObjectName        = @ObjectName,
            @ReloadFullHistory = @ReloadFullHistory,
            @WatermarkFrom     = @WatermarkFrom OUTPUT,
            @WatermarkTo       = @WatermarkTo   OUTPUT;

        SET @FromUtc = ISNULL(TRY_CONVERT(DATETIME2(3), @WatermarkFrom, 126), CONVERT(DATETIME2(3), '1900-01-01'));

        SELECT
            ShipmentBusinessKey = stg.ufn_SourceSystemKey(h.SourceSystemCode, h.ShipmentID, 1),
            SourceShipmentId    = LTRIM(RTRIM(h.ShipmentID)),
            ShipmentReference   = stg.ufn_CleanString(h.ShipmentReference, 1),
            SaleBusinessKey     = stg.ufn_SourceSystemKey(h.SourceSystemCode, h.InvoiceID, 1),
            CustomerBusinessKey = stg.ufn_SourceSystemKey(h.SourceSystemCode, h.CustomerID, 1),
            CarrierCode         = NULLIF(UPPER(LTRIM(RTRIM(h.CarrierCode))), N''),
            ServiceLevelCode    = NULLIF(UPPER(LTRIM(RTRIM(h.ServiceLevelCode))), N''),
            DeliveryRouteCode   = NULLIF(UPPER(LTRIM(RTRIM(h.DeliveryRouteCode))), N''),
            ShipFromWarehouseCode = NULLIF(UPPER(LTRIM(RTRIM(h.ShipFromWarehouseCode))), N''),
            ShipToCountryCode   = LEFT(UPPER(LTRIM(RTRIM(h.ShipToCountryCode))), 2),
            ShippedDateTimeUtc  = CONVERT(DATETIME2(3), stg.ufn_SafeDate(h.ShippedWhen, N'NA')),
            PromisedDeliveryUtc = CONVERT(DATETIME2(3), stg.ufn_SafeDate(h.PromisedDeliveryWhen, N'NA')),
            DeliveredDateTimeUtc = CONVERT(DATETIME2(3), stg.ufn_SafeDate(h.DeliveredWhen, N'NA')),
            RawPostalCode       = h.ShipToPostalCode,
            TotalWeightKg       = CONVERT(DECIMAL(18,3), stg.ufn_SafeDecimal(h.TotalWeightKg, N'.')),
            TotalVolumeM3       = CONVERT(DECIMAL(18,4), stg.ufn_SafeDecimal(h.TotalVolumeM3, N'.')),
            FreightChargeAmount = CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(h.FreightChargeAmount, N'.')),
            FreightCurrencyCode = LEFT(UPPER(LTRIM(RTRIM(h.FreightCurrencyCode))), 3),
            CustomsDeclarationRef = stg.ufn_CleanString(h.CustomsDeclarationRef, 1),
            ShipmentStatusCode  = NULLIF(UPPER(LTRIM(RTRIM(h.ShipmentStatusCode))), N''),
            LastEditedWhenUtc   = CONVERT(DATETIME2(3), stg.ufn_SafeDate(h.LastEditedWhen, N'NA'))
        INTO #IncomingShipment
        FROM raw.SqlShipment AS h
        WHERE h.BatchId = @BatchId
          AND (
                  @ReloadFullHistory = 1
               OR ISNULL(CONVERT(DATETIME2(3), stg.ufn_SafeDate(h.LastEditedWhen, N'NA')),
                         CONVERT(DATETIME2(3), '9999-12-31')) > @FromUtc
              );

        SELECT @SourceRows = COUNT_BIG(*) FROM #IncomingShipment;

        --  One scan row per shipment: the latest scan the carrier file carries.
        SELECT
            ShipmentReference = stg.ufn_CleanString(s.ShipmentReference, 1),
            LastScanEventCode = s.ScanEventCode,
            LastScanUtc       = s.ScanUtc,
            ScanWeightKg      = s.ScanWeightKg
        INTO #LatestScan
        FROM
        (
            SELECT
                c.ShipmentReference,
                c.ScanEventCode,
                ScanUtc = CONVERT(DATETIME2(3), stg.ufn_SafeDate(c.ScanTimestampLocal, N'NA')),
                --  NA depots weigh in pounds; EU and APAC depots already send kilograms.
                ScanWeightKg =
                    CASE
                        WHEN UPPER(LTRIM(RTRIM(c.WeightUomCode))) = N'LB'
                            THEN CONVERT(DECIMAL(18,3), stg.ufn_SafeDecimal(c.WeightValue, N'.') * @LbToKgFactor)
                        ELSE CONVERT(DECIMAL(18,3), stg.ufn_SafeDecimal(c.WeightValue, N'.'))
                    END,
                ScanRank = ROW_NUMBER() OVER
                (
                    PARTITION BY c.ShipmentReference
                    ORDER BY CONVERT(DATETIME2(3), stg.ufn_SafeDate(c.ScanTimestampLocal, N'NA')) DESC,
                             c.SourceRowNumber DESC
                )
            FROM raw.FileCarrierScan AS c
            WHERE c.BatchId = @BatchId
              AND NULLIF(LTRIM(RTRIM(c.ShipmentReference)), N'') IS NOT NULL
        ) AS s
        WHERE s.ScanRank = 1;

        BEGIN TRANSACTION;

        INSERT INTO err.RejectedShipment
        (
            BatchId, PackageExecutionId, SourceSystemCode, ShipmentBusinessKey, ShipmentReference,
            CarrierCode, TrackingNumber, RejectReasonCode, RejectReason, RejectStage,
            ShippedWhenText, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, @SourceSystemCode, i.ShipmentBusinessKey, i.ShipmentReference,
            i.CarrierCode, NULL,
            CASE
                WHEN i.ShippedDateTimeUtc IS NULL THEN N'BAD_DATE'
                WHEN i.CarrierCode IS NULL        THEN N'UNKNOWN_CARRIER'
                ELSE N'MISSING_CUSTOMS_REF'
            END,
            CASE
                WHEN i.ShippedDateTimeUtc IS NULL
                    THEN N'ShippedWhen will not parse to a date'
                WHEN i.CarrierCode IS NULL
                    THEN N'no carrier code on the shipment header'
                ELSE N'cross-border APAC lane with no customs declaration reference'
            END,
            N'Stage',
            CONVERT(NVARCHAR(40), i.ShippedDateTimeUtc, 126),
            CONCAT(N'{"ShipmentID":"', i.SourceShipmentId, N'","ShipmentReference":"', i.ShipmentReference, N'"}')
        FROM #IncomingShipment AS i
        LEFT JOIN ref.Country AS cn
            ON cn.CountryCode = i.ShipToCountryCode
        WHERE i.ShippedDateTimeUtc IS NULL
           OR i.CarrierCode IS NULL
           OR (
                  cn.RegionCode = N'APAC'
              AND i.CustomsDeclarationRef IS NULL
              --  Warehouse codes are prefixed with the depot country, so a lane
              --  whose prefix differs from the ship-to country is cross-border.
              AND LEFT(ISNULL(i.ShipFromWarehouseCode, N'??'), 2) <> i.ShipToCountryCode
              );

        SET @RejectedRows = @@ROWCOUNT;

        INSERT INTO stg.Shipment
        (
            ShipmentBusinessKey, SourceSystemCode, ShipmentReference, SaleBusinessKey,
            CustomerBusinessKey, CarrierCode, ServiceLevelCode, DeliveryRouteCode,
            ShipFromWarehouseCode, ShipToCountryCode, ShipToPostalCodeStandardized,
            ShipToGeographyBusinessKey, ShippedDate, ShippedDateTimeUtc, PromisedDeliveryUtc,
            DeliveredDateTimeUtc, DeliveryLatencyHours, OnTimeDeliveryFlag, TotalWeightKg,
            TotalVolumeM3, FreightChargeAmount, FreightCurrencyCode, FreightChargeAmountUsd,
            CustomsDeclarationRef, CustomsRequiredFlag, ShipmentStatusCode, LastScanEventCode,
            LastScanUtc, RegionCode, DqStatusCode, RowHash, BatchId, PackageExecutionId
        )
        SELECT
            i.ShipmentBusinessKey,
            @SourceSystemCode,
            i.ShipmentReference,
            i.SaleBusinessKey,
            i.CustomerBusinessKey,
            i.CarrierCode,
            i.ServiceLevelCode,
            i.DeliveryRouteCode,
            i.ShipFromWarehouseCode,
            i.ShipToCountryCode,
            stg.ufn_StandardizePostalCode(i.RawPostalCode, i.ShipToCountryCode),
            g.GeographyBusinessKey,
            CONVERT(DATE, i.ShippedDateTimeUtc),
            i.ShippedDateTimeUtc,
            i.PromisedDeliveryUtc,
            COALESCE(i.DeliveredDateTimeUtc,
                     CASE WHEN sc.LastScanEventCode = N'DL' THEN sc.LastScanUtc END),
            CASE
                WHEN i.ShippedDateTimeUtc IS NULL THEN NULL
                ELSE CONVERT(DECIMAL(9,2),
                        DATEDIFF(MINUTE, i.ShippedDateTimeUtc,
                                 COALESCE(i.DeliveredDateTimeUtc, sc.LastScanUtc)) / 60.0)
            END,
            CASE
                WHEN COALESCE(i.DeliveredDateTimeUtc, sc.LastScanUtc) IS NULL
                  OR i.PromisedDeliveryUtc IS NULL THEN NULL
                WHEN COALESCE(i.DeliveredDateTimeUtc, sc.LastScanUtc) <= i.PromisedDeliveryUtc THEN 1
                ELSE 0
            END,
            COALESCE(i.TotalWeightKg, sc.ScanWeightKg),
            i.TotalVolumeM3,
            i.FreightChargeAmount,
            i.FreightCurrencyCode,
            CONVERT(DECIMAL(19,4), i.FreightChargeAmount * ISNULL(fx.ConversionRate, 1)),
            i.CustomsDeclarationRef,
            --  Intra-EU lanes are exempt; everything else that crosses a border is not.
            CASE
                WHEN cn.RegionCode = N'EU' AND ISNULL(wh.WarehouseRegionCode, N'EU') = N'EU' THEN 0
                WHEN i.ShipToCountryCode = wh.WarehouseCountryCode THEN 0
                ELSE 1
            END,
            i.ShipmentStatusCode,
            sc.LastScanEventCode,
            sc.LastScanUtc,
            cn.RegionCode,
            CASE
                WHEN g.GeographyBusinessKey IS NULL THEN N'WARN'
                WHEN i.TotalWeightKg IS NULL AND sc.ScanWeightKg IS NULL THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(i.ShipmentBusinessKey, N'|', i.ShipmentStatusCode, N'|', i.ShippedDateTimeUtc, N'|',
                       i.DeliveredDateTimeUtc, N'|', i.TotalWeightKg, N'|', sc.LastScanEventCode)),
            @BatchId,
            @PackageExecutionId
        FROM #IncomingShipment AS i
        LEFT JOIN ref.Country AS cn
            ON cn.CountryCode = i.ShipToCountryCode
        LEFT JOIN #LatestScan AS sc
            ON sc.ShipmentReference = i.ShipmentReference
        OUTER APPLY
        (
            SELECT TOP (1)
                WarehouseCountryCode = gw.CountryCode,
                WarehouseRegionCode  = gw.RegionCode
            FROM stg.Geography AS gw
            WHERE gw.BatchId = @BatchId
              AND gw.GeographyBusinessKey LIKE CONCAT(N'%|', i.ShipFromWarehouseCode, N'|%')
        ) AS wh
        OUTER APPLY
        (
            SELECT TOP (1) gs.GeographyBusinessKey
            FROM stg.Geography AS gs
            WHERE gs.BatchId     = @BatchId
              AND gs.CountryCode = i.ShipToCountryCode
              AND gs.PostalCode  = stg.ufn_StandardizePostalCode(i.RawPostalCode, i.ShipToCountryCode)
        ) AS g
        OUTER APPLY
        (
            SELECT TOP (1) f.ConversionRate
            FROM ref.FxRateDaily AS f
            WHERE f.FromCurrencyCode = i.FreightCurrencyCode
              AND f.ToCurrencyCode   = N'USD'
              AND f.RateTypeCode     = N'SPOT'
              AND f.RateDate        <= CONVERT(DATE, i.ShippedDateTimeUtc)
            ORDER BY f.RateDate DESC
        ) AS fx
        WHERE i.ShippedDateTimeUtc IS NOT NULL
          AND i.CarrierCode IS NOT NULL;

        SET @InsertedRows = @@ROWCOUNT;

        INSERT INTO stg.ShipmentLine
        (
            ShipmentLineBusinessKey, ShipmentBusinessKey, SaleLineBusinessKey, SourceSystemCode,
            StockItemBusinessKey, PackageTypeCode, ShippedQuantity, WeightKg, SerialNumberCount,
            TemperatureAtLoadC, ColdChainBreachFlag, LineStatusCode, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            CONCAT(stg.ufn_SourceSystemKey(l.SourceSystemCode, l.ShipmentID, 1), N'|', LTRIM(RTRIM(l.ShipmentLineID))),
            stg.ufn_SourceSystemKey(l.SourceSystemCode, l.ShipmentID, 1),
            CASE
                WHEN NULLIF(LTRIM(RTRIM(l.InvoiceLineID)), N'') IS NULL THEN NULL
                ELSE CONCAT(sh.SaleBusinessKey, N'|', LTRIM(RTRIM(l.InvoiceLineID)))
            END,
            @SourceSystemCode,
            stg.ufn_SourceSystemKey(l.SourceSystemCode, l.StockItemID, 1),
            NULLIF(UPPER(LTRIM(RTRIM(l.PackageTypeCode))), N''),
            CONVERT(DECIMAL(18,4), stg.ufn_SafeDecimal(l.ShippedQuantity, N'.')),
            CONVERT(DECIMAL(18,3), stg.ufn_SafeDecimal(l.WeightKg, N'.')),
            CASE
                WHEN NULLIF(LTRIM(RTRIM(l.SerialNumbers)), N'') IS NULL THEN 0
                ELSE LEN(REPLACE(REPLACE(l.SerialNumbers, CHAR(13), N''), CHAR(10), N','))
                     - LEN(REPLACE(REPLACE(REPLACE(l.SerialNumbers, CHAR(13), N''), CHAR(10), N','), N',', N'')) + 1
            END,
            CONVERT(DECIMAL(9,2), stg.ufn_SafeDecimal(l.TemperatureAtLoadC, N'.')),
            0,      -- set by the cold-chain sweep below
            NULLIF(UPPER(LTRIM(RTRIM(l.LineStatusCode))), N''),
            CASE
                WHEN stg.ufn_SafeDecimal(l.ShippedQuantity, N'.') IS NULL THEN N'FAIL'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(l.ShipmentLineID, N'|', l.ShippedQuantity, N'|', l.WeightKg, N'|', l.LineStatusCode)),
            @BatchId,
            @PackageExecutionId
        FROM raw.SqlShipmentLine AS l
        INNER JOIN stg.Shipment AS sh
            ON  sh.ShipmentBusinessKey = stg.ufn_SourceSystemKey(l.SourceSystemCode, l.ShipmentID, 1)
            AND sh.BatchId             = @BatchId
        WHERE l.BatchId = @BatchId;

        SET @LineRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        /*
            Cold-chain sweep. Row-by-row on purpose: the threshold is per carrier
            and the operations team maintains the carrier list in ref.ReasonCode
            rather than in code, so the loop reads the threshold per line.
        */
        DECLARE ColdChainCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT sl.ShipmentLineBusinessKey, sl.TemperatureAtLoadC, sh.CarrierCode
            FROM stg.ShipmentLine AS sl
            INNER JOIN stg.Shipment AS sh
                ON  sh.ShipmentBusinessKey = sl.ShipmentBusinessKey
                AND sh.BatchId             = sl.BatchId
            WHERE sl.BatchId = @BatchId
              AND sl.TemperatureAtLoadC IS NOT NULL;

        OPEN ColdChainCursor;
        FETCH NEXT FROM ColdChainCursor INTO @LineKey, @LineTempC, @LineCarrier;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @CarrierLimitC DECIMAL(9,2) = @ColdChainLimitC;

            --  Carriers the operations team has flagged as strict get the tighter
            --  four-degree limit; the flag lives in the reason-code table because
            --  that is the only reference list they can edit themselves.
            IF EXISTS
            (
                SELECT 1
                FROM ref.ReasonCode AS rc
                WHERE rc.ReasonDomainCode    = N'HOLD'
                  AND rc.ConformedReasonCode = @LineCarrier
                  AND rc.RequiresApproval    = 1
            )
                SET @CarrierLimitC = 4.00;

            IF @LineTempC > ISNULL(@CarrierLimitC, @ColdChainLimitC)
            BEGIN
                UPDATE stg.ShipmentLine
                SET ColdChainBreachFlag = 1,
                    DqStatusCode        = N'WARN'
                WHERE ShipmentLineBusinessKey = @LineKey
                  AND BatchId                 = @BatchId;

                SET @BreachCount = @BreachCount + 1;
            END;

            SET @CarrierLimitC = @ColdChainLimitC;

            FETCH NEXT FROM ColdChainCursor INTO @LineKey, @LineTempC, @LineCarrier;
        END;

        CLOSE ColdChainCursor;
        DEALLOCATE ColdChainCursor;

        SELECT @MaxEditedWhen = MAX(i.LastEditedWhenUtc) FROM #IncomingShipment AS i;

        IF @MaxEditedWhen IS NOT NULL
            SET @WatermarkTo = CONVERT(NVARCHAR(50), @MaxEditedWhen, 126);

        EXEC etl.usp_SetWatermark
            @SourceSystemCode   = @SourceSystemCode,
            @ObjectName         = @ObjectName,
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @RejectedRows;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @LineObject,
            @SourceRowCount     = @LineRows,
            @TargetRowCount     = @LineRows,
            @InsertRowCount     = @LineRows,
            @UpdateRowCount     = @BreachCount;

        IF @RejectedRows > 0
            EXEC err.usp_LogRejectedRows
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @RejectTableName    = N'err.RejectedShipment',
                @ObjectName         = @ObjectName,
                @BusinessKeyColumn  = N'ShipmentBusinessKey';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF CURSOR_STATUS('local', 'ColdChainCursor') >= 0
        BEGIN
            CLOSE ColdChainCursor;
            DEALLOCATE ColdChainCursor;
        END;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_LOAD_SHIPMENT',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_AppendIncremental_Shipment';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
