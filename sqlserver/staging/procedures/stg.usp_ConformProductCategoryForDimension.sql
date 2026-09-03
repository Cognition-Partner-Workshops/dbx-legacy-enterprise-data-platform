/*
    stg.usp_ConformProductCategoryForDimension

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : DIM_Load_ProductCategory (SSIS)
    Reads         : raw.OracleProductMaster, ref.CodeCrosswalk, stg.Product
    Writes        : stg.ProductCategory
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The ERP holds the merchandise hierarchy on the product row as two columns,
    CATEGORY_CD and SUB_CATEGORY_CD, and nowhere else. This load explodes those
    two columns into the two hierarchy levels the dimension expects and marks
    the level 2 rows as leaves, which is true for every category except the
    handful the merchandising team maintains by hand in the crosswalk.

    Sub-categories whose parent code never appears as a category in the same
    batch are still published, with the parent code left as landed and
    DqStatusCode = WARN; the dimension load hangs them off the unknown member
    rather than dropping the products underneath them.
*/

IF OBJECT_ID(N'stg.usp_ConformProductCategoryForDimension', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformProductCategoryForDimension;
GO

CREATE PROCEDURE stg.usp_ConformProductCategoryForDimension
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.ProductCategory';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @WarnRows     BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleProductMaster AS pm
        WHERE pm.BatchId = @BatchId;

        DELETE FROM stg.ProductCategory
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        WITH CategoryLevel AS
        (
            SELECT
                CategoryCode  = UPPER(LTRIM(RTRIM(pm.CATEGORY_CD))),
                ParentCode    = CONVERT(NVARCHAR(30), NULL),
                HierarchyLevel = CONVERT(TINYINT, 1),
                TaxClassCode  = MAX(UPPER(LTRIM(RTRIM(pm.TAX_CLASS_CD)))),
                ProductCount  = COUNT_BIG(*),
                LastChangedOn = MAX(stg.ufn_SafeDate(pm.LAST_UPDATE_DT, N'NA')),
                ActiveCount   = SUM(CASE WHEN UPPER(LTRIM(RTRIM(pm.LIFECYCLE_STATUS_CD))) IN (N'ACT', N'NPI')
                                         THEN 1 ELSE 0 END)
            FROM raw.OracleProductMaster AS pm
            WHERE pm.BatchId = @BatchId
              AND NULLIF(LTRIM(RTRIM(pm.CATEGORY_CD)), N'') IS NOT NULL
            GROUP BY UPPER(LTRIM(RTRIM(pm.CATEGORY_CD)))

            UNION ALL

            SELECT
                CategoryCode  = UPPER(LTRIM(RTRIM(pm.SUB_CATEGORY_CD))),
                ParentCode    = UPPER(LTRIM(RTRIM(pm.CATEGORY_CD))),
                HierarchyLevel = CONVERT(TINYINT, 2),
                TaxClassCode  = MAX(UPPER(LTRIM(RTRIM(pm.TAX_CLASS_CD)))),
                ProductCount  = COUNT_BIG(*),
                LastChangedOn = MAX(stg.ufn_SafeDate(pm.LAST_UPDATE_DT, N'NA')),
                ActiveCount   = SUM(CASE WHEN UPPER(LTRIM(RTRIM(pm.LIFECYCLE_STATUS_CD))) IN (N'ACT', N'NPI')
                                         THEN 1 ELSE 0 END)
            FROM raw.OracleProductMaster AS pm
            WHERE pm.BatchId = @BatchId
              AND NULLIF(LTRIM(RTRIM(pm.SUB_CATEGORY_CD)), N'') IS NOT NULL
              AND NULLIF(LTRIM(RTRIM(pm.CATEGORY_CD)), N'') IS NOT NULL
            GROUP BY UPPER(LTRIM(RTRIM(pm.SUB_CATEGORY_CD))), UPPER(LTRIM(RTRIM(pm.CATEGORY_CD)))
        ),
        DeduplicatedCategory AS
        (
            SELECT
                cl.*,
                CodeRank = ROW_NUMBER() OVER
                (
                    PARTITION BY cl.CategoryCode
                    ORDER BY     cl.HierarchyLevel, cl.ProductCount DESC, cl.ParentCode
                )
            FROM CategoryLevel AS cl
        )
        INSERT INTO stg.ProductCategory
        (
            ProductCategoryBusinessKey, SourceSystemCode, ProductCategoryCode,
            ProductCategoryName, ParentCategoryCode, MerchandiseGroupCode, HierarchyLevel,
            IsLeafCategory, HierarchyPath, ProductCount, TaxClassCode, IsActive,
            SourceChangedOn, DqStatusCode, RowHash, BatchId, PackageExecutionId
        )
        SELECT
            CONCAT(@SourceSystemCode, N'|', dc.CategoryCode),
            @SourceSystemCode,
            dc.CategoryCode,
            stg.ufn_CleanString(COALESCE(cw.SourceCodeDescription, dc.CategoryCode), 0),
            dc.ParentCode,
            COALESCE(cw.ConformedCodeValue, LEFT(COALESCE(dc.ParentCode, dc.CategoryCode), 20)),
            dc.HierarchyLevel,
            CASE WHEN dc.HierarchyLevel = 2 THEN 1 ELSE 0 END,
            CASE
                WHEN dc.ParentCode IS NULL THEN dc.CategoryCode
                ELSE CONCAT(dc.ParentCode, N'/', dc.CategoryCode)
            END,
            CONVERT(INT, dc.ProductCount),
            dc.TaxClassCode,
            CASE WHEN dc.ActiveCount > 0 THEN 1 ELSE 0 END,
            dc.LastChangedOn,
            CASE
                WHEN dc.HierarchyLevel = 2
                     AND NOT EXISTS (SELECT 1 FROM DeduplicatedCategory AS p
                                     WHERE p.CategoryCode   = dc.ParentCode
                                       AND p.HierarchyLevel = 1) THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(dc.CategoryCode, N'|', dc.ParentCode, N'|', dc.HierarchyLevel, N'|',
                       dc.ProductCount)),
            @BatchId,
            @PackageExecutionId
        FROM DeduplicatedCategory AS dc
        LEFT JOIN ref.CodeCrosswalk AS cw
            ON  cw.CodeDomainCode   = N'PRODUCT_CATEGORY'
            AND cw.SourceSystemCode = @SourceSystemCode
            AND cw.SourceCodeValue  = dc.CategoryCode
        WHERE dc.CodeRank = 1;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @WarnRows = COUNT_BIG(*)
        FROM stg.ProductCategory AS pc
        WHERE pc.BatchId      = @BatchId
          AND pc.DqStatusCode = N'WARN';

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @WarnRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'DIM_Load_ProductCategory',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformProductCategoryForDimension';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
