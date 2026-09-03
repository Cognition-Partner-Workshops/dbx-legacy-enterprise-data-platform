/*
    stg.usp_ConformCustomerCategoryForDimension

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : DIM_Load_CustomerCategory (SSIS)
    Reads         : raw.OracleCustomerMaster, ref.CodeCrosswalk, stg.Customer
    Writes        : stg.CustomerCategory
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    There is no customer category master in either source system. The ERP holds
    the category on the customer row (CUST_TYPE_CD) and the OLTP holds its own
    code set, so the category list is the distinct set of ERP codes conformed
    through ref.CodeCrosswalk, with the customer count carried alongside because
    the stewardship report has asked for it since 2011.

    Codes that no crosswalk row conforms are still published, under their raw
    value and with DqStatusCode = WARN. The dimension needs a member for them or
    the customer rows that use them lose their category on the way through.
*/

IF OBJECT_ID(N'stg.usp_ConformCustomerCategoryForDimension', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformCustomerCategoryForDimension;
GO

CREATE PROCEDURE stg.usp_ConformCustomerCategoryForDimension
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.CustomerCategory';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @WarnRows     BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleCustomerMaster AS cm
        WHERE cm.BatchId = @BatchId;

        DELETE FROM stg.CustomerCategory
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        WITH SourceCategory AS
        (
            SELECT
                SourceCategoryCode = UPPER(LTRIM(RTRIM(cm.CUST_TYPE_CD))),
                RegionCode         = UPPER(LTRIM(RTRIM(cm.REGION_CD))),
                CustomerCount      = COUNT_BIG(*),
                LastChangedOn      = MAX(stg.ufn_SafeDate(cm.LAST_UPDATE_DT, N'NA'))
            FROM raw.OracleCustomerMaster AS cm
            WHERE cm.BatchId = @BatchId
              AND NULLIF(LTRIM(RTRIM(cm.CUST_TYPE_CD)), N'') IS NOT NULL
            GROUP BY UPPER(LTRIM(RTRIM(cm.CUST_TYPE_CD))), UPPER(LTRIM(RTRIM(cm.REGION_CD)))
        ),
        ConformedCategory AS
        (
            SELECT
                sc.SourceCategoryCode,
                sc.RegionCode,
                sc.CustomerCount,
                sc.LastChangedOn,
                ConformedCode = COALESCE(cw.ConformedCodeValue, sc.SourceCategoryCode),
                ConformedName = COALESCE(cw.SourceCodeDescription, sc.SourceCategoryCode),
                IsConformed   = CASE WHEN cw.ConformedCodeValue IS NULL THEN 0 ELSE 1 END,
                CodeRank      = ROW_NUMBER() OVER
                (
                    PARTITION BY COALESCE(cw.ConformedCodeValue, sc.SourceCategoryCode)
                    ORDER BY     sc.CustomerCount DESC, sc.SourceCategoryCode
                )
            FROM SourceCategory AS sc
            LEFT JOIN ref.CodeCrosswalk AS cw
                ON  cw.CodeDomainCode  = N'CUSTOMER_CATEGORY'
                AND cw.SourceSystemCode = @SourceSystemCode
                AND cw.SourceCodeValue  = sc.SourceCategoryCode
                AND (cw.EffectiveToDate IS NULL OR cw.EffectiveToDate >= CONVERT(DATE, SYSUTCDATETIME()))
        )
        INSERT INTO stg.CustomerCategory
        (
            CustomerCategoryBusinessKey, SourceSystemCode, CustomerCategoryCode,
            CustomerCategoryName, CategoryGroupCode, DiscountEligiblePercent, IsActive,
            SourceCategoryCode, CustomerCount, RegionCode, SourceChangedOn, DqStatusCode,
            RowHash, BatchId, PackageExecutionId
        )
        SELECT
            CONCAT(@SourceSystemCode, N'|', cc.ConformedCode),
            @SourceSystemCode,
            cc.ConformedCode,
            stg.ufn_CleanString(cc.ConformedName, 0),
            CASE
                WHEN cc.ConformedCode LIKE N'WHOLE%'   THEN N'WHOLESALE'
                WHEN cc.ConformedCode LIKE N'RETAIL%'  THEN N'RETAIL'
                WHEN cc.ConformedCode LIKE N'DIST%'    THEN N'WHOLESALE'
                WHEN cc.ConformedCode LIKE N'INTERNAL%' THEN N'INTERNAL'
                WHEN cc.ConformedCode LIKE N'AGENT%'   THEN N'AGENT'
                ELSE N'OTHER'
            END,
            -- The standing discount ladder was never held anywhere but the pricing
            -- spreadsheet, so it is reproduced here against the conformed code.
            CASE
                WHEN cc.ConformedCode LIKE N'WHOLE%'   THEN CONVERT(DECIMAL(5,2), 12.50)
                WHEN cc.ConformedCode LIKE N'DIST%'    THEN CONVERT(DECIMAL(5,2), 10.00)
                WHEN cc.ConformedCode LIKE N'AGENT%'   THEN CONVERT(DECIMAL(5,2), 7.50)
                WHEN cc.ConformedCode LIKE N'RETAIL%'  THEN CONVERT(DECIMAL(5,2), 0.00)
                ELSE CONVERT(DECIMAL(5,2), 0.00)
            END,
            CASE WHEN cc.CustomerCount = 0 THEN 0 ELSE 1 END,
            cc.SourceCategoryCode,
            CONVERT(INT, cc.CustomerCount),
            cc.RegionCode,
            cc.LastChangedOn,
            CASE WHEN cc.IsConformed = 0 THEN N'WARN' ELSE N'PASS' END,
            HASHBYTES('SHA2_256',
                CONCAT(cc.ConformedCode, N'|', cc.ConformedName, N'|', cc.CustomerCount)),
            @BatchId,
            @PackageExecutionId
        FROM ConformedCategory AS cc
        WHERE cc.CodeRank = 1;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @WarnRows = COUNT_BIG(*)
        FROM stg.CustomerCategory AS c
        WHERE c.BatchId      = @BatchId
          AND c.DqStatusCode = N'WARN';

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
            @SourceName         = N'DIM_Load_CustomerCategory',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformCustomerCategoryForDimension';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
