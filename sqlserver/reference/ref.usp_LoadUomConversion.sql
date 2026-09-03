/*
    ref.usp_LoadUomConversion

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_TransactionType (SSIS)
    Reads         : raw.OracleProductMaster, ref.UnitOfMeasure
    Writes        : ref.UomConversion, err.RejectedConstraintViolation
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Two kinds of row live in ref.UomConversion and they are loaded separately:

      1. the standard factors between units of the same class, which are physics
         and are asserted here against StockItemBusinessKey = '*';
      2. the item-specific factors, which come from the ERP product master
         (UOM_CONVERSION_FACTOR between BASE_UOM_CD and SELL_UOM_CD) and are
         keyed by the stock item, because a case of one product is not the same
         number of eaches as a case of another.

    An item-specific factor never overwrites the standard factor and vice versa;
    the consumers look for the item first and fall back to '*'. A conversion
    between two units of different classes is a data error and is rejected -
    that is how the 2016 "cases to kilograms" incident is prevented from
    happening again.
*/

IF OBJECT_ID(N'ref.usp_LoadUomConversion', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadUomConversion;
GO

CREATE PROCEDURE ref.usp_LoadUomConversion
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP',
    @MaintainedByName   NVARCHAR(100) = N'REF_Load_TransactionType'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'ref.UomConversion';
    DECLARE @SourceRows    BIGINT = 0;
    DECLARE @StandardRows  BIGINT = 0;
    DECLARE @ItemRows      BIGINT = 0;
    DECLARE @UpdatedRows   BIGINT = 0;
    DECLARE @RejectedRows  BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleProductMaster AS p
        WHERE p.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(p.UOM_CONVERSION_FACTOR)), N'') IS NOT NULL;

        BEGIN TRANSACTION;

        --  The standard factors. Both directions are stored so no consumer has
        --  to know which way round the pair was written.
        WITH StandardFactor AS
        (
            SELECT *
            FROM
            (
                VALUES
                    (N'G',   N'KG',  CONVERT(DECIMAL(18,8), 0.001)),
                    (N'KG',  N'G',   CONVERT(DECIMAL(18,8), 1000)),
                    (N'LB',  N'KG',  CONVERT(DECIMAL(18,8), 0.45359237)),
                    (N'KG',  N'LB',  CONVERT(DECIMAL(18,8), 2.20462262)),
                    (N'OZ',  N'KG',  CONVERT(DECIMAL(18,8), 0.02834952)),
                    (N'KG',  N'OZ',  CONVERT(DECIMAL(18,8), 35.27396195)),
                    (N'ML',  N'L',   CONVERT(DECIMAL(18,8), 0.001)),
                    (N'L',   N'ML',  CONVERT(DECIMAL(18,8), 1000)),
                    (N'GAL', N'L',   CONVERT(DECIMAL(18,8), 3.78541178)),
                    (N'L',   N'GAL', CONVERT(DECIMAL(18,8), 0.26417205)),
                    (N'CM',  N'M',   CONVERT(DECIMAL(18,8), 0.01)),
                    (N'M',   N'CM',  CONVERT(DECIMAL(18,8), 100)),
                    (N'IN',  N'M',   CONVERT(DECIMAL(18,8), 0.0254)),
                    (N'M',   N'IN',  CONVERT(DECIMAL(18,8), 39.37007874)),
                    (N'FT',  N'M',   CONVERT(DECIMAL(18,8), 0.3048)),
                    (N'M',   N'FT',  CONVERT(DECIMAL(18,8), 3.2808399)),
                    (N'DOZ', N'EA',  CONVERT(DECIMAL(18,8), 12)),
                    (N'EA',  N'DOZ', CONVERT(DECIMAL(18,8), 0.08333333))
            ) AS v (FromUomCode, ToUomCode, ConversionFactor)
        )
        INSERT INTO ref.UomConversion
        (
            FromUomCode, ToUomCode, StockItemBusinessKey, ConversionFactor, IsItemSpecific,
            EffectiveFromDate, MaintainedByName, MaintenanceNote
        )
        SELECT
            s.FromUomCode, s.ToUomCode, N'*', s.ConversionFactor, 0,
            CONVERT(DATE, N'1900-01-01'), @MaintainedByName,
            N'Standard factor between two units of the same class; not item specific.'
        FROM StandardFactor AS s
        WHERE NOT EXISTS
              (
                  SELECT 1
                  FROM ref.UomConversion AS c
                  WHERE c.FromUomCode = s.FromUomCode
                    AND c.ToUomCode   = s.ToUomCode
                    AND c.StockItemBusinessKey = N'*'
              );

        SET @StandardRows = @@ROWCOUNT;

        SELECT
            StockItemBusinessKey = LEFT(CONCAT(N'ORA|', LTRIM(RTRIM(p.PRODUCT_CD))), 100),
            FromUomCode          = UPPER(LTRIM(RTRIM(p.BASE_UOM_CD))),
            ToUomCode            = UPPER(LTRIM(RTRIM(p.SELL_UOM_CD))),
            ConversionFactor     = CONVERT(DECIMAL(18,8), stg.ufn_SafeDecimal(p.UOM_CONVERSION_FACTOR, N'.')),
            ProductCode          = LTRIM(RTRIM(p.PRODUCT_CD))
        INTO #ItemFactor
        FROM raw.OracleProductMaster AS p
        WHERE p.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(p.BASE_UOM_CD)), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(p.SELL_UOM_CD)), N'') IS NOT NULL
          AND UPPER(LTRIM(RTRIM(p.BASE_UOM_CD))) <> UPPER(LTRIM(RTRIM(p.SELL_UOM_CD)));

        --  Cross-class and non-positive factors never reach the conformed set.
        INSERT INTO err.RejectedConstraintViolation
        (
            BatchId, PackageExecutionId, TargetObjectName, ConstraintName, ConstraintTypeCode,
            ViolatingBusinessKey, ViolatingColumnName, ViolatingValue, RejectReasonCode,
            RejectReason, RejectStage, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'ref.UomConversion', N'CK_refUomConversion_Class', N'CK',
            i.StockItemBusinessKey, N'ConversionFactor',
            ISNULL(CONVERT(NVARCHAR(400), i.ConversionFactor), N''), N'CONSTRAINT',
            CASE
                WHEN i.ConversionFactor IS NULL OR i.ConversionFactor <= 0
                    THEN N'UOM_CONVERSION_FACTOR is missing or not greater than zero'
                WHEN f.UomCode IS NULL OR t.UomCode IS NULL
                    THEN N'conversion references a unit that is not in the conformed unit list'
                ELSE N'conversion crosses unit classes, which is never valid'
            END,
            N'Reference',
            CONCAT(N'{"PRODUCT_CD":"', i.ProductCode, N'","FROM":"', i.FromUomCode,
                   N'","TO":"', i.ToUomCode, N'"}')
        FROM #ItemFactor AS i
        LEFT JOIN ref.UnitOfMeasure AS f
            ON f.UomCode = i.FromUomCode
        LEFT JOIN ref.UnitOfMeasure AS t
            ON t.UomCode = i.ToUomCode
        WHERE i.ConversionFactor IS NULL
           OR i.ConversionFactor <= 0
           OR f.UomCode IS NULL
           OR t.UomCode IS NULL
           OR f.UomClassCode <> t.UomClassCode;

        SET @RejectedRows = @@ROWCOUNT;

        --  Item-specific factors. A restated factor updates in place; the ERP
        --  does not version them and neither has this layer.
        UPDATE c
        SET c.ConversionFactor  = i.ConversionFactor,
            c.MaintainedByName  = @MaintainedByName,
            c.MaintenanceNote   = CONCAT(N'Restated from the ERP product master in batch ', @BatchId, N'.')
        FROM ref.UomConversion AS c
        INNER JOIN
        (
            SELECT
                i.StockItemBusinessKey,
                i.FromUomCode,
                i.ToUomCode,
                ConversionFactor = MAX(i.ConversionFactor)
            FROM #ItemFactor AS i
            INNER JOIN ref.UnitOfMeasure AS f ON f.UomCode = i.FromUomCode
            INNER JOIN ref.UnitOfMeasure AS t ON t.UomCode = i.ToUomCode
            WHERE i.ConversionFactor > 0
              AND f.UomClassCode = t.UomClassCode
            GROUP BY i.StockItemBusinessKey, i.FromUomCode, i.ToUomCode
        ) AS i
            ON  c.StockItemBusinessKey = i.StockItemBusinessKey
            AND c.FromUomCode          = i.FromUomCode
            AND c.ToUomCode            = i.ToUomCode
        WHERE c.IsItemSpecific = 1
          AND c.ConversionFactor <> i.ConversionFactor;

        SET @UpdatedRows = @@ROWCOUNT;

        INSERT INTO ref.UomConversion
        (
            FromUomCode, ToUomCode, StockItemBusinessKey, ConversionFactor, IsItemSpecific,
            EffectiveFromDate, MaintainedByName, MaintenanceNote
        )
        SELECT
            i.FromUomCode,
            i.ToUomCode,
            i.StockItemBusinessKey,
            MAX(i.ConversionFactor),
            1,
            CONVERT(DATE, SYSUTCDATETIME()),
            @MaintainedByName,
            N'Item-specific factor taken from UOM_CONVERSION_FACTOR on the ERP product master.'
        FROM #ItemFactor AS i
        INNER JOIN ref.UnitOfMeasure AS f ON f.UomCode = i.FromUomCode
        INNER JOIN ref.UnitOfMeasure AS t ON t.UomCode = i.ToUomCode
        WHERE i.ConversionFactor > 0
          AND f.UomClassCode = t.UomClassCode
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.UomConversion AS c
                  WHERE c.FromUomCode = i.FromUomCode
                    AND c.ToUomCode   = i.ToUomCode
                    AND c.StockItemBusinessKey = i.StockItemBusinessKey
              )
        GROUP BY i.FromUomCode, i.ToUomCode, i.StockItemBusinessKey;

        SET @ItemRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @StandardRows + @ItemRows,
            @InsertRowCount     = @StandardRows + @ItemRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @RejectedRows;

        DROP TABLE #ItemFactor;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_TransactionType',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_LoadUomConversion';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
