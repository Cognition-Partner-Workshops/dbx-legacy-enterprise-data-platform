/*
    Object        : [Integration].[usp_LoadBridgeCustomerBuyingGroup]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.Customer, ref.BuyingGroupMembership, Dimension.Customer,
                    Dimension.Buying Group, Dimension.Customer Buying Group Bridge,
                    the etl control framework
    Called by     : DIM_Rebuild_CustomerBuyingGroupBridge, run after the customer
                    and buying-group dimension loads

    A customer can belong to more than one buying group - a hospital group buying
    consumables through one consortium and capital equipment through another is the
    canonical case - so the customer-to-buying-group relationship is many to many
    and lives here rather than as [Dimension].[Customer].[Buying Group Key]. That
    column still exists and still holds the primary affiliation, because a dozen
    reports were written against it before the bridge arrived in 2011 and none of
    them were migrated.

    Allocation factors must sum to 1.0 per customer per open period. They do not,
    in the source: the contract system lets a category-scoped split be entered
    without touching the others. Rather than reject, the load normalises
    proportionally and records the fact it had to - finance would rather see an
    approximately right rebate accrual than nothing at all. Rows that cannot be
    normalised because every factor is zero are rejected.

    Delete/insert, not MERGE. The bridge is small and the 2011 author did not trust
    MERGE with a composite grain; the memberships that are closed rather than
    reloaded are preserved by only rebuilding the currently-open rows.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_LoadBridgeCustomerBuyingGroup]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10)  = N'GLOBAL',
    @PackageName        NVARCHAR(200) = N'DIM_Rebuild_CustomerBuyingGroupBridge',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_OLTP',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @HighDate           DATE = CONVERT(DATE, N'9999-12-31');
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @ClosedCount        BIGINT = 0;
    DECLARE @NormalisedCount    BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    DECLARE @rCustomerId        INT;
    DECLARE @rGroupCode         NVARCHAR(20);
    DECLARE @rReason            NVARCHAR(500);
    DECLARE @rBusinessKey       NVARCHAR(200);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'Bridge',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#Membership') IS NOT NULL DROP TABLE #Membership;

        CREATE TABLE #Membership
        (
            [WWI Customer ID]           INT             NOT NULL,
            [Buying Group Code]         NVARCHAR(20)    NOT NULL,
            [Membership From]           DATE            NOT NULL,
            [Membership To]             DATE            NOT NULL,
            [Is Primary Affiliation]    BIT             NULL,
            [Allocation Factor]         DECIMAL(9, 6)   NOT NULL,
            [Allocation Basis Code]     NVARCHAR(15)    NULL,
            [Allocation Category Scope] NVARCHAR(40)    NULL,
            [Allocation Reviewed On]    DATE            NULL,
            [Allocation Reviewed By]    NVARCHAR(60)    NULL,
            [Rebate Eligible]           BIT             NULL,
            [Rebate Agreement Reference] NVARCHAR(30)   NULL,
            [Region Code]               NVARCHAR(10)    NULL,
            [Source Membership Reference] NVARCHAR(40)  NULL,
            [Reject Reason]             NVARCHAR(500)   NULL
        );

        INSERT INTO #Membership
            ([WWI Customer ID], [Buying Group Code], [Membership From], [Membership To],
             [Is Primary Affiliation], [Allocation Factor], [Allocation Basis Code],
             [Allocation Category Scope], [Allocation Reviewed On], [Allocation Reviewed By],
             [Rebate Eligible], [Rebate Agreement Reference], [Region Code],
             [Source Membership Reference])
        SELECT
              m.[WWICustomerID]
            , UPPER(LTRIM(RTRIM(m.[BuyingGroupCode])))
            , ISNULL(m.[MembershipFrom], CONVERT(DATE, N'1900-01-01'))
            , ISNULL(m.[MembershipTo], @HighDate)
            , CONVERT(BIT, ISNULL(m.[IsPrimaryAffiliation], 0))
            , CONVERT(DECIMAL(9, 6), ISNULL(m.[AllocationFactor], 0))
            , UPPER(NULLIF(LTRIM(RTRIM(m.[AllocationBasisCode])), N''))
            , NULLIF(LTRIM(RTRIM(m.[AllocationCategoryScope])), N'')
            , m.[AllocationReviewedOn]
            , NULLIF(LTRIM(RTRIM(m.[AllocationReviewedBy])), N'')
            , CONVERT(BIT, ISNULL(m.[RebateEligible], 0))
            , NULLIF(LTRIM(RTRIM(m.[RebateAgreementReference])), N'')
            , UPPER(ISNULL(m.[RegionCode], N'NA'))
            , m.[SourceMembershipReference]
        FROM [ref].[BuyingGroupMembership] AS m
        WHERE ISNULL(m.[MembershipTo], @HighDate) >= CONVERT(DATE, @Now)
          AND (@RegionCode = N'GLOBAL' OR UPPER(ISNULL(m.[RegionCode], N'NA')) = @RegionCode);

        SET @SourceRowCount = @@ROWCOUNT;

        /* The single-membership customers never appear in the membership table -
           the consortium system only knows about consortium members - so the
           bridge is topped up from the customer master with an implicit 1.0 row.
           Without this the bridge under-reports every direct customer. */
        INSERT INTO #Membership
            ([WWI Customer ID], [Buying Group Code], [Membership From], [Membership To],
             [Is Primary Affiliation], [Allocation Factor], [Allocation Basis Code],
             [Region Code], [Source Membership Reference])
        SELECT
              c.[WWICustomerID]
            , UPPER(LTRIM(RTRIM(c.[BuyingGroupCode])))
            , ISNULL(c.[AccountOpenedDate], CONVERT(DATE, N'1900-01-01'))
            , @HighDate
            , 1
            , 1.0
            , N'EQUAL'
            , UPPER(ISNULL(c.[RegionCode], N'NA'))
            , N'(implicit)'
        FROM [stg].[Customer] AS c
        WHERE NULLIF(LTRIM(RTRIM(c.[BuyingGroupCode])), N'') IS NOT NULL
          AND (@RegionCode = N'GLOBAL' OR UPPER(ISNULL(c.[RegionCode], N'NA')) = @RegionCode)
          AND NOT EXISTS (SELECT 1
                          FROM #Membership AS m
                          WHERE m.[WWI Customer ID] = c.[WWICustomerID]);

        /* -------------------------------------------------- screens */
        UPDATE #Membership
        SET [Reject Reason] = N'Allocation factor is negative or greater than one.'
        WHERE [Allocation Factor] < 0 OR [Allocation Factor] > 1;

        UPDATE #Membership
        SET [Reject Reason] = N'Membership period is reversed.'
        WHERE [Reject Reason] IS NULL
          AND [Membership To] < [Membership From];

        UPDATE m
        SET m.[Reject Reason] = N'Every allocation factor for this customer is zero; nothing to normalise.'
        FROM #Membership AS m
        INNER JOIN (
            SELECT [WWI Customer ID]
            FROM #Membership
            WHERE [Reject Reason] IS NULL
            GROUP BY [WWI Customer ID]
            HAVING SUM([Allocation Factor]) = 0
        ) AS z
            ON z.[WWI Customer ID] = m.[WWI Customer ID]
        WHERE m.[Reject Reason] IS NULL;

        UPDATE m
        SET m.[Reject Reason] = N'Buying group code is not present in Dimension.Buying Group.'
        FROM #Membership AS m
        WHERE m.[Reject Reason] IS NULL
          AND NOT EXISTS (SELECT 1
                          FROM [Dimension].[Buying Group] AS g
                          WHERE g.[Buying Group Code] = m.[Buying Group Code]
                            AND g.[Buying Group Key] > 0);

        /* Reject routing is row at a time - etl.usp_LogReject is row-scoped. */
        DECLARE curReject CURSOR LOCAL FAST_FORWARD FOR
            SELECT [WWI Customer ID], [Buying Group Code], [Reject Reason]
            FROM #Membership
            WHERE [Reject Reason] IS NOT NULL;

        OPEN curReject;
        FETCH NEXT FROM curReject INTO @rCustomerId, @rGroupCode, @rReason;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @rBusinessKey = CONCAT(CONVERT(NVARCHAR(20), @rCustomerId), N'|', @rGroupCode);

            EXEC [etl].[usp_LogRejectedRecord]
                 @PackageExecutionId = @PackageExecutionId,
                 @BatchId            = @BatchId,
                 @SourceSystemCode   = @SourceSystemCode,
                 @ObjectName         = N'Dimension.Customer Buying Group Bridge',
                 @BusinessKey        = @rBusinessKey,
                 @RejectReasonCode   = N'BRIDGE_ALLOCATION',
                 @RejectReason       = @rReason,
                 @RejectStage        = N'Dimension';

            SET @RejectCount = @RejectCount + 1;

            FETCH NEXT FROM curReject INTO @rCustomerId, @rGroupCode, @rReason;
        END;

        CLOSE curReject;
        DEALLOCATE curReject;

        DELETE FROM #Membership WHERE [Reject Reason] IS NOT NULL;

        /* -------------------------------------------------- normalisation */
        UPDATE m
        SET m.[Allocation Factor]     = CONVERT(DECIMAL(9, 6), m.[Allocation Factor] / t.[Total]),
            m.[Allocation Basis Code] = ISNULL(m.[Allocation Basis Code], N'MANUAL')
        FROM #Membership AS m
        INNER JOIN (
            SELECT [WWI Customer ID], SUM([Allocation Factor]) AS [Total]
            FROM #Membership
            GROUP BY [WWI Customer ID]
            HAVING ABS(SUM([Allocation Factor]) - 1.0) > 0.000001
        ) AS t
            ON t.[WWI Customer ID] = m.[WWI Customer ID];

        SET @NormalisedCount = @@ROWCOUNT;

        /* Close the open rows that are no longer in the source before rebuilding;
           closed rows are history and are left where they are. */
        UPDATE b
        SET b.[Is Current Membership] = 0,
            b.[Membership To]         = CONVERT(DATE, @Now),
            b.[Last Load Batch Id]    = @BatchId,
            b.[Last Load Package Execution Id] = @PackageExecutionId
        FROM [Dimension].[Customer Buying Group Bridge] AS b
        WHERE b.[Is Current Membership] = 1
          AND (@RegionCode = N'GLOBAL' OR b.[Region Code] = @RegionCode)
          AND NOT EXISTS (SELECT 1
                          FROM #Membership AS m
                          WHERE m.[WWI Customer ID]   = b.[WWI Customer ID]
                            AND m.[Buying Group Code] = b.[Buying Group Code]);

        SET @ClosedCount = @@ROWCOUNT;

        DELETE b
        FROM [Dimension].[Customer Buying Group Bridge] AS b
        INNER JOIN #Membership AS m
            ON  m.[WWI Customer ID]   = b.[WWI Customer ID]
            AND m.[Buying Group Code] = b.[Buying Group Code]
        WHERE b.[Is Current Membership] = 1;

        INSERT INTO [Dimension].[Customer Buying Group Bridge]
            ([WWI Customer ID], [Buying Group Code], [Customer Key], [Buying Group Key],
             [Membership From], [Membership To], [Is Current Membership],
             [Is Primary Affiliation], [Allocation Factor], [Allocation Basis Code],
             [Allocation Category Scope], [Allocation Reviewed On], [Allocation Reviewed By],
             [Rebate Eligible], [Rebate Agreement Reference], [Region Code],
             [Source System Code], [Source Membership Reference], [Last Load Batch Id],
             [Last Load Package Execution Id])
        SELECT
              m.[WWI Customer ID]
            , m.[Buying Group Code]
            , c.[Customer Key]
            , g.[Buying Group Key]
            , m.[Membership From]
            , m.[Membership To]
            , 1
            , m.[Is Primary Affiliation]
            , m.[Allocation Factor]
            , m.[Allocation Basis Code]
            , m.[Allocation Category Scope]
            , m.[Allocation Reviewed On]
            , m.[Allocation Reviewed By]
            /* EU consortium rebates are settled by the consortium, not by us, so
               the member row is not rebate eligible even when the source says it
               is; NA and APAC settle with the member directly. */
            , CASE WHEN m.[Region Code] = N'EU' AND ISNULL(m.[Is Primary Affiliation], 0) = 0
                   THEN 0 ELSE m.[Rebate Eligible] END
            , m.[Rebate Agreement Reference]
            , m.[Region Code]
            , @SourceSystemCode
            , m.[Source Membership Reference]
            , @BatchId
            , @PackageExecutionId
        FROM #Membership AS m
        LEFT OUTER JOIN [Dimension].[Customer] AS c
            ON  c.[WWI Customer ID] = m.[WWI Customer ID]
            AND c.[Is Current Row]  = 1
        LEFT OUTER JOIN [Dimension].[Buying Group] AS g
            ON g.[Buying Group Code] = m.[Buying Group Code];

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [New Member Count],
             [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Customer Buying Group Bridge', @RegionCode, @BatchId, @PackageExecutionId,
             @SourceRowCount, @NormalisedCount + @ClosedCount, @InsertedCount, @RejectCount,
             N'BridgeDeleteInsert', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Customer Buying Group Bridge',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @ClosedCount,
             @RejectRowCount     = @RejectCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @ClosedCount,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        IF CURSOR_STATUS(N'local', N'curReject') >= 0
        BEGIN
            CLOSE curReject;
            DEALLOCATE curReject;
        END;

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Customer Buying Group Bridge',
             @ProcedureName      = N'Integration.usp_LoadBridgeCustomerBuyingGroup',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @SourceRowCount;

        THROW;
    END CATCH;
END;
GO
