/*
    ref.usp_LoadUnitOfMeasure

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_TransactionType (SSIS)
    Reads         : raw.OracleProductMaster, raw.SqlStockItem, ref.CodeCrosswalk
    Writes        : ref.UnitOfMeasure, err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Both source systems measure stock, and neither publishes a unit list. Oracle
    carries BASE_UOM_CD / SELL_UOM_CD / ORDER_UOM_CD on the product and the
    documents; the OLTP database has no unit column at all and works in packages,
    so its units are inferred as EA and reach the conformed set through
    ref.CodeCrosswalk domain UOM.

    The conformed grid below fixes the class and the base unit per class, which
    is what makes ref.UomConversion loadable: a conversion is only accepted
    between two units of the same class. Regional divergence lives in
    ref.Region.WeightUomCode - NA weighs in pounds, everywhere else in kilograms -
    and is not repeated here; the base weight unit is KG for everybody and the NA
    figures are converted, which is the behaviour the fact loads assume.
*/

IF OBJECT_ID(N'ref.usp_LoadUnitOfMeasure', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadUnitOfMeasure;
GO

CREATE PROCEDURE ref.usp_LoadUnitOfMeasure
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'ref.UnitOfMeasure';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @LookupMisses BIGINT = 0;
    DECLARE @MergeAction TABLE (ActionName NVARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(DISTINCT UPPER(LTRIM(RTRIM(p.BASE_UOM_CD))))
                           + COUNT_BIG(DISTINCT UPPER(LTRIM(RTRIM(p.SELL_UOM_CD))))
        FROM raw.OracleProductMaster AS p
        WHERE p.BatchId = @BatchId;

        BEGIN TRANSACTION;

        WITH ConformedUom AS
        (
            SELECT *
            FROM
            (
                VALUES
                    (N'EA',   N'Each',            N'COUNT',  N'EA', 1, CONVERT(TINYINT, 0)),
                    (N'PKT',  N'Packet',          N'COUNT',  N'EA', 0, CONVERT(TINYINT, 0)),
                    (N'CS',   N'Case',            N'COUNT',  N'EA', 0, CONVERT(TINYINT, 0)),
                    (N'PLT',  N'Pallet',          N'COUNT',  N'EA', 0, CONVERT(TINYINT, 0)),
                    (N'DOZ',  N'Dozen',           N'COUNT',  N'EA', 0, CONVERT(TINYINT, 0)),
                    (N'KG',   N'Kilogram',        N'WEIGHT', N'KG', 1, CONVERT(TINYINT, 3)),
                    (N'G',    N'Gram',            N'WEIGHT', N'KG', 0, CONVERT(TINYINT, 3)),
                    (N'LB',   N'Pound',           N'WEIGHT', N'KG', 0, CONVERT(TINYINT, 4)),
                    (N'OZ',   N'Ounce',           N'WEIGHT', N'KG', 0, CONVERT(TINYINT, 4)),
                    (N'L',    N'Litre',           N'VOLUME', N'L',  1, CONVERT(TINYINT, 3)),
                    (N'ML',   N'Millilitre',      N'VOLUME', N'L',  0, CONVERT(TINYINT, 1)),
                    (N'GAL',  N'US Gallon',       N'VOLUME', N'L',  0, CONVERT(TINYINT, 4)),
                    (N'M',    N'Metre',           N'LENGTH', N'M',  1, CONVERT(TINYINT, 3)),
                    (N'CM',   N'Centimetre',      N'LENGTH', N'M',  0, CONVERT(TINYINT, 1)),
                    (N'IN',   N'Inch',            N'LENGTH', N'M',  0, CONVERT(TINYINT, 4)),
                    (N'FT',   N'Foot',            N'LENGTH', N'M',  0, CONVERT(TINYINT, 4))
            ) AS v (UomCode, UomName, UomClassCode, BaseUomCode, IsBaseUom, DecimalPrecision)
        )
        MERGE ref.UnitOfMeasure AS tgt
        USING ConformedUom AS src
            ON tgt.UomCode = src.UomCode
        WHEN MATCHED THEN
            UPDATE SET
                tgt.UomName          = src.UomName,
                tgt.UomClassCode     = src.UomClassCode,
                tgt.BaseUomCode      = src.BaseUomCode,
                tgt.IsBaseUom        = src.IsBaseUom,
                tgt.DecimalPrecision = src.DecimalPrecision,
                tgt.IsActive         = 1
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (UomCode, UomName, UomClassCode, BaseUomCode, IsBaseUom, DecimalPrecision, IsActive)
            VALUES (src.UomCode, src.UomName, src.UomClassCode, src.BaseUomCode, src.IsBaseUom,
                    src.DecimalPrecision, 1)
        OUTPUT $action INTO @MergeAction (ActionName);

        SELECT
            @InsertedRows = COUNT_BIG(CASE WHEN a.ActionName = N'INSERT' THEN 1 END),
            @UpdatedRows  = COUNT_BIG(CASE WHEN a.ActionName = N'UPDATE' THEN 1 END)
        FROM @MergeAction AS a;

        --  Units the ERP uses that are neither conformed nor crosswalked. The
        --  product still loads; the quantity is left in its source unit and the
        --  unit is put in front of a steward.
        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'raw.OracleProductMaster', MIN(u.ProductCode),
            N'UnitOfMeasure', N'BASE_UOM_CD', u.SourceUom, @SourceSystemCode, N'LOOKUP_MISS',
            N'unit of measure is not in the conformed set and has no ref.CodeCrosswalk row in domain UOM',
            N'Reference', 1, 0, COUNT_BIG(*),
            CONCAT(N'{"UOM_CD":"', u.SourceUom, N'"}')
        FROM
        (
            SELECT
                SourceUom   = UPPER(LTRIM(RTRIM(p.BASE_UOM_CD))),
                ProductCode = LTRIM(RTRIM(p.PRODUCT_CD))
            FROM raw.OracleProductMaster AS p
            WHERE p.BatchId = @BatchId
              AND NULLIF(LTRIM(RTRIM(p.BASE_UOM_CD)), N'') IS NOT NULL

            UNION ALL

            SELECT
                UPPER(LTRIM(RTRIM(p.SELL_UOM_CD))),
                LTRIM(RTRIM(p.PRODUCT_CD))
            FROM raw.OracleProductMaster AS p
            WHERE p.BatchId = @BatchId
              AND NULLIF(LTRIM(RTRIM(p.SELL_UOM_CD)), N'') IS NOT NULL
        ) AS u
        WHERE NOT EXISTS
              (
                  SELECT 1
                  FROM ref.UnitOfMeasure AS m
                  WHERE m.UomCode = u.SourceUom
              )
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.CodeCrosswalk AS x
                  WHERE x.CodeDomainCode   = N'UOM'
                    AND x.SourceSystemCode = @SourceSystemCode
                    AND x.SourceCodeValue  = u.SourceUom
                    AND x.EffectiveToDate IS NULL
              )
        GROUP BY u.SourceUom;

        SET @LookupMisses = @@ROWCOUNT;

        COMMIT TRANSACTION;

        DECLARE @TargetRowCountValue BIGINT = @InsertedRows + @UpdatedRows;
        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @TargetRowCountValue,
            @InsertRowCount     = @InsertedRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @LookupMisses;
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
            @ProcedureName      = N'ref.usp_LoadUnitOfMeasure';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
