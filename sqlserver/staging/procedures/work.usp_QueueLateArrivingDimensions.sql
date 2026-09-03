/*
    work.usp_QueueLateArrivingDimensions

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : WRK_QUEUE_LATE_DIMS (SSIS), last step of the staging batch
    Reads         : stg.OrderLine, stg.SaleLine, stg.Shipment, stg.Payment,
                    stg.PurchaseOrderLine, stg.Customer, stg.Supplier,
                    stg.StockItem, stg.Geography
    Writes        : work.LateArrivingDimensionQueue, work.FactRekeyQueue
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Sweeps the staged fact-shaped tables for business keys that no staged
    dimension row covers, and queues them so that the warehouse load can create
    an inferred stub member now and re-key the fact when the real dimension row
    turns up.

    The queue is cumulative across batches: a key that was queued last night and
    is still unresolved has its OccurrenceCount incremented rather than being
    inserted again, which is what the MERGE below is doing. The count is how the
    stewards prioritise - a key seen four hundred times is a broken interface,
    a key seen once is a data-entry slip.

    Anything queued also produces a work.FactRekeyQueue row for the fact that
    needed it, at priority 1 for customer and supplier (finance reports on them
    by name) and priority 5 for everything else.

    Inferred attributes are captured as JSON so the stub member can carry more
    than a business key; the warehouse load reads InferredAttributesJson only
    when it creates the stub, never afterwards.
*/

IF OBJECT_ID(N'work.usp_QueueLateArrivingDimensions', N'P') IS NOT NULL
    DROP PROCEDURE work.usp_QueueLateArrivingDimensions;
GO

CREATE PROCEDURE work.usp_QueueLateArrivingDimensions
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName  NVARCHAR(200) = N'work.LateArrivingDimensionQueue';
    DECLARE @QueuedRows  BIGINT = 0;
    DECLARE @RekeyRows   BIGINT = 0;

    BEGIN TRY
        --  Every missing reference from every fact-shaped staging table, in one
        --  set. New sources get a UNION ALL branch here and nothing else.
        SELECT
            DimensionName,
            MissingBusinessKey,
            SourceSystemCode,
            FirstSeenObjectName,
            FactBusinessKey,
            InferredAttributesJson,
            OccurrenceCount = COUNT_BIG(*) OVER (PARTITION BY DimensionName, MissingBusinessKey)
        INTO #Missing
        FROM
        (
            SELECT
                DimensionName       = N'StockItem',
                MissingBusinessKey  = l.StockItemBusinessKey,
                SourceSystemCode    = l.SourceSystemCode,
                FirstSeenObjectName = N'stg.OrderLine',
                FactBusinessKey     = l.OrderLineBusinessKey,
                InferredAttributesJson = CONCAT(N'{"LineDescription":"', REPLACE(ISNULL(l.LineDescription, N''), N'"', N''''),
                                                N'","PackageTypeCode":"', ISNULL(l.PackageTypeCode, N''), N'"}')
            FROM stg.OrderLine AS l
            WHERE l.BatchId = @BatchId
              AND l.StockItemBusinessKey IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM stg.StockItem AS s
                              WHERE s.BatchId = @BatchId
                                AND s.StockItemBusinessKey = l.StockItemBusinessKey)

            UNION ALL

            SELECT
                N'StockItem', sl.StockItemBusinessKey, sl.SourceSystemCode, N'stg.SaleLine',
                sl.SaleLineBusinessKey,
                CONCAT(N'{"LineDescription":"', REPLACE(ISNULL(sl.LineDescription, N''), N'"', N''''), N'"}')
            FROM stg.SaleLine AS sl
            WHERE sl.BatchId = @BatchId
              AND sl.StockItemBusinessKey IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM stg.StockItem AS s
                              WHERE s.BatchId = @BatchId
                                AND s.StockItemBusinessKey = sl.StockItemBusinessKey)

            UNION ALL

            SELECT
                N'Customer', sh.CustomerBusinessKey, sh.SourceSystemCode, N'stg.Shipment',
                sh.ShipmentBusinessKey,
                CONCAT(N'{"ShipToCountryCode":"', ISNULL(sh.ShipToCountryCode, N''),
                       N'","RegionCode":"', ISNULL(sh.RegionCode, N''), N'"}')
            FROM stg.Shipment AS sh
            WHERE sh.BatchId = @BatchId
              AND sh.CustomerBusinessKey IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM stg.Customer AS c
                              WHERE c.BatchId = @BatchId
                                AND c.CustomerBusinessKey = sh.CustomerBusinessKey)

            UNION ALL

            SELECT
                N'Supplier', p.SupplierBusinessKey, p.SourceSystemCode, N'stg.Payment',
                p.PaymentBusinessKey,
                CONCAT(N'{"TransactionCurrencyCode":"', ISNULL(p.TransactionCurrencyCode, N''),
                       N'","RegionCode":"', ISNULL(p.RegionCode, N''), N'"}')
            FROM stg.Payment AS p
            WHERE p.BatchId = @BatchId
              AND p.SupplierBusinessKey IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM stg.Supplier AS s
                              WHERE s.BatchId = @BatchId
                                AND s.SupplierBusinessKey = p.SupplierBusinessKey)

            UNION ALL

            SELECT
                N'Supplier', po.SupplierBusinessKey, po.SourceSystemCode, N'stg.PurchaseOrder',
                po.PurchaseOrderBusinessKey,
                CONCAT(N'{"PurchaseOrderBusinessKey":"', ISNULL(po.PurchaseOrderBusinessKey, N''), N'"}')
            FROM stg.PurchaseOrder AS po
            WHERE po.BatchId = @BatchId
              AND po.SupplierBusinessKey IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM stg.Supplier AS s
                              WHERE s.BatchId = @BatchId
                                AND s.SupplierBusinessKey = po.SupplierBusinessKey)

            UNION ALL

            SELECT
                N'Geography', sh.ShipToGeographyBusinessKey, sh.SourceSystemCode, N'stg.Shipment',
                sh.ShipmentBusinessKey,
                CONCAT(N'{"ShipToCountryCode":"', ISNULL(sh.ShipToCountryCode, N''),
                       N'","PostalCode":"', ISNULL(sh.ShipToPostalCodeStandardized, N''), N'"}')
            FROM stg.Shipment AS sh
            WHERE sh.BatchId = @BatchId
              AND sh.ShipToGeographyBusinessKey IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM stg.Geography AS g
                              WHERE g.BatchId = @BatchId
                                AND g.GeographyBusinessKey = sh.ShipToGeographyBusinessKey)
        ) AS u;

        MERGE work.LateArrivingDimensionQueue AS tgt
        USING
        (
            SELECT
                m.DimensionName,
                m.MissingBusinessKey,
                SourceSystemCode       = MIN(m.SourceSystemCode),
                FirstSeenObjectName    = MIN(m.FirstSeenObjectName),
                InferredAttributesJson = MIN(m.InferredAttributesJson),
                OccurrenceCount        = COUNT_BIG(*)
            FROM #Missing AS m
            GROUP BY m.DimensionName, m.MissingBusinessKey
        ) AS src
            ON  tgt.DimensionName     = src.DimensionName
            AND tgt.MissingBusinessKey = src.MissingBusinessKey
            AND tgt.ResolvedFlag       = 0
        WHEN MATCHED THEN
            UPDATE SET
                tgt.OccurrenceCount    = tgt.OccurrenceCount + CONVERT(INT, src.OccurrenceCount),
                tgt.BatchId            = @BatchId,
                tgt.PackageExecutionId = @PackageExecutionId
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (BatchId, PackageExecutionId, DimensionName, MissingBusinessKey, SourceSystemCode,
                    FirstSeenObjectName, OccurrenceCount, InferredAttributesJson, StubCreatedFlag, ResolvedFlag)
            VALUES (@BatchId, @PackageExecutionId, src.DimensionName, src.MissingBusinessKey,
                    src.SourceSystemCode, src.FirstSeenObjectName, CONVERT(INT, src.OccurrenceCount),
                    src.InferredAttributesJson, 0, 0);

        SET @QueuedRows = @@ROWCOUNT;

        --  Close out anything that has arrived since it was queued.
        UPDATE q
        SET q.ResolvedFlag   = 1,
            q.ResolvedAtUtc  = SYSUTCDATETIME(),
            q.ResolutionNote = CONCAT(N'dimension row present in batch ', CONVERT(NVARCHAR(20), @BatchId))
        FROM work.LateArrivingDimensionQueue AS q
        WHERE q.ResolvedFlag = 0
          AND
          (
              EXISTS (SELECT 1 FROM stg.Customer AS c
                      WHERE q.DimensionName = N'Customer' AND c.BatchId = @BatchId
                        AND c.CustomerBusinessKey = q.MissingBusinessKey)
           OR EXISTS (SELECT 1 FROM stg.Supplier AS s
                      WHERE q.DimensionName = N'Supplier' AND s.BatchId = @BatchId
                        AND s.SupplierBusinessKey = q.MissingBusinessKey)
           OR EXISTS (SELECT 1 FROM stg.StockItem AS si
                      WHERE q.DimensionName = N'StockItem' AND si.BatchId = @BatchId
                        AND si.StockItemBusinessKey = q.MissingBusinessKey)
           OR EXISTS (SELECT 1 FROM stg.Geography AS g
                      WHERE q.DimensionName = N'Geography' AND g.BatchId = @BatchId
                        AND g.GeographyBusinessKey = q.MissingBusinessKey)
          );

        --  One re-key row per fact that referenced a queued key.
        INSERT INTO work.FactRekeyQueue
        (
            BatchId, PackageExecutionId, FactObjectName, FactBusinessKey, DimensionName,
            CurrentSurrogateKey, RekeyReasonCode, RekeyPriority
        )
        SELECT DISTINCT
            @BatchId, @PackageExecutionId, m.FirstSeenObjectName, m.FactBusinessKey, m.DimensionName,
            -1, N'LATE_DIM',
            CASE WHEN m.DimensionName IN (N'Customer', N'Supplier') THEN 1 ELSE 5 END
        FROM #Missing AS m
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM work.FactRekeyQueue AS r
            WHERE r.FactObjectName  = m.FirstSeenObjectName
              AND r.FactBusinessKey = m.FactBusinessKey
              AND r.DimensionName   = m.DimensionName
              AND r.AppliedFlag     = 0
        );

        SET @RekeyRows = @@ROWCOUNT;

        DECLARE @InsertRowCountValue BIGINT = @QueuedRows + @RekeyRows;
        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @QueuedRows,
            @InsertRowCount     = @InsertRowCountValue;
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'WRK_QUEUE_LATE_DIMS',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'work.usp_QueueLateArrivingDimensions';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
