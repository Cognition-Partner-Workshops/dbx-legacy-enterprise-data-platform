/*
    ref.usp_LoadSourceKeyCrosswalk

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_UnknownMembers (SSIS)
    Reads         : raw.OracleGeography, raw.OracleProductMaster, raw.SqlStockItem,
                    ref.Country
    Writes        : ref.SourceKeyCrosswalk
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    ref.CodeCrosswalk maps codes; this maps keys. The same physical product has
    an ERP PRODUCT_CD and an OLTP StockItemID, and the only thing joining them is
    the WWI_STOCK_ITEM_ID column somebody added to the ERP product master in
    2012 and has maintained by hand ever since.

    Rows loaded from a source column are marked LOADED. Rows a steward has typed
    in are MANUAL and are never overwritten here. Rows retired by a dedup merge
    keep SupersededByBusinessKey so a fact that still carries the old key can
    still be resolved.
*/

IF OBJECT_ID(N'ref.usp_LoadSourceKeyCrosswalk', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadSourceKeyCrosswalk;
GO

CREATE PROCEDURE ref.usp_LoadSourceKeyCrosswalk
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @MaintainedByName   NVARCHAR(100) = N'REF_Load_UnknownMembers'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'ref.SourceKeyCrosswalk';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleProductMaster AS p
        WHERE p.BatchId = @BatchId;

        BEGIN TRANSACTION;

        --  Product: the ERP code is the conformed key, the OLTP stock item id
        --  is the alias, and the ERP's own hand-maintained column is the join.
        INSERT INTO ref.SourceKeyCrosswalk
        (
            EntityName, SourceSystemCode, SourceKeyValue, ConformedBusinessKey, MatchMethodCode,
            SupersededByBusinessKey, IsActive, MaintainedByName
        )
        SELECT
            N'Product',
            N'ORA_ERP',
            LTRIM(RTRIM(p.PRODUCT_CD)),
            LEFT(CONCAT(N'ORA|', LTRIM(RTRIM(p.PRODUCT_CD))), 140),
            N'LOADED',
            NULL,
            1,
            @MaintainedByName
        FROM raw.OracleProductMaster AS p
        WHERE p.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(p.PRODUCT_CD)), N'') IS NOT NULL
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.SourceKeyCrosswalk AS k
                  WHERE k.EntityName       = N'Product'
                    AND k.SourceSystemCode = N'ORA_ERP'
                    AND k.SourceKeyValue   = LTRIM(RTRIM(p.PRODUCT_CD))
              )
        GROUP BY LTRIM(RTRIM(p.PRODUCT_CD));

        SET @InsertedRows = @@ROWCOUNT;

        INSERT INTO ref.SourceKeyCrosswalk
        (
            EntityName, SourceSystemCode, SourceKeyValue, ConformedBusinessKey, MatchMethodCode,
            SupersededByBusinessKey, IsActive, MaintainedByName
        )
        SELECT
            N'Product',
            N'WWI_OLTP',
            LTRIM(RTRIM(p.WWI_STOCK_ITEM_ID)),
            LEFT(CONCAT(N'ORA|', LTRIM(RTRIM(p.PRODUCT_CD))), 140),
            N'LOADED',
            NULL,
            1,
            @MaintainedByName
        FROM raw.OracleProductMaster AS p
        WHERE p.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(p.WWI_STOCK_ITEM_ID)), N'') IS NOT NULL
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.SourceKeyCrosswalk AS k
                  WHERE k.EntityName       = N'Product'
                    AND k.SourceSystemCode = N'WWI_OLTP'
                    AND k.SourceKeyValue   = LTRIM(RTRIM(p.WWI_STOCK_ITEM_ID))
              )
        GROUP BY LTRIM(RTRIM(p.WWI_STOCK_ITEM_ID)), LTRIM(RTRIM(p.PRODUCT_CD));

        SET @InsertedRows = @InsertedRows + @@ROWCOUNT;

        --  Geography: the conformed key is the same shape the staging load
        --  builds, so the two can be joined without re-deriving it.
        INSERT INTO ref.SourceKeyCrosswalk
        (
            EntityName, SourceSystemCode, SourceKeyValue, ConformedBusinessKey, MatchMethodCode,
            SupersededByBusinessKey, IsActive, MaintainedByName
        )
        SELECT
            N'Geography',
            N'ORA_ERP',
            LTRIM(RTRIM(g.GEOGRAPHY_ID)),
            LEFT(CONCAT(c.CountryCode, N'|', ISNULL(NULLIF(UPPER(LTRIM(RTRIM(g.STATE_PROVINCE_CD))), N''), N'-'),
                        N'|', ISNULL(NULLIF(LTRIM(RTRIM(g.CITY_NAME)), N''), N'-'),
                        N'|', ISNULL(NULLIF(LTRIM(RTRIM(g.POSTAL_CD)), N''), N'-')), 140),
            N'LOADED',
            NULL,
            1,
            @MaintainedByName
        FROM raw.OracleGeography AS g
        INNER JOIN ref.Country AS c
            ON c.CountryCode = LEFT(UPPER(LTRIM(RTRIM(g.COUNTRY_CD))), 2)
        WHERE g.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(g.GEOGRAPHY_ID)), N'') IS NOT NULL
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.SourceKeyCrosswalk AS k
                  WHERE k.EntityName       = N'Geography'
                    AND k.SourceSystemCode = N'ORA_ERP'
                    AND k.SourceKeyValue   = LTRIM(RTRIM(g.GEOGRAPHY_ID))
              );

        SET @InsertedRows = @InsertedRows + @@ROWCOUNT;

        --  A key that has stopped arriving is deactivated rather than deleted;
        --  the facts still reference it. Manual rows are left alone.
        UPDATE k
        SET k.IsActive = 0
        FROM ref.SourceKeyCrosswalk AS k
        WHERE k.EntityName       = N'Product'
          AND k.SourceSystemCode = N'ORA_ERP'
          AND k.MatchMethodCode  = N'LOADED'
          AND k.IsActive = 1
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM raw.OracleProductMaster AS p
                  WHERE p.BatchId = @BatchId
                    AND LTRIM(RTRIM(p.PRODUCT_CD)) = k.SourceKeyValue
              );

        SET @UpdatedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = 0;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_UnknownMembers',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_LoadSourceKeyCrosswalk';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
