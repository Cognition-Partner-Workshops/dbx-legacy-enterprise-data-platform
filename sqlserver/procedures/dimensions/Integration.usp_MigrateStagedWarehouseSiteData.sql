/*
    Object        : [Integration].[usp_MigrateStagedWarehouseSiteData]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.WarehouseSite, Dimension.Warehouse Site, Dimension.City,
                    Dimension.Geography, Dimension.Cost Center,
                    the etl control framework
    Called by     : REF_Load_WarehouseSite

    Type 1 with a deliberate exception: storage capacity is a Type 3 attribute.
    The inventory health reports need the previous capacity and the date it
    changed so that a step change in utilisation can be explained, but nobody
    was willing to version the whole site row for it. So the load keeps the
    prior value, the change date and a change reason in three extra columns and
    overwrites everything else.

    Regional divergence is in the customs treatment. EU sites can be excise
    warehouses (duty suspended until removal), NA sites can hold a bonded
    carrier code, APAC sites are neither and are simply flagged bonded or not.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedWarehouseSiteData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_WarehouseSite',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_WMS',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @Today              DATE         = CONVERT(DATE, SYSDATETIME());
    DECLARE @HighDate           DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @UpdatedCount       BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'ReferenceLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#WarehouseSiteSource') IS NOT NULL DROP TABLE #WarehouseSiteSource;

        SELECT
              UPPER(LTRIM(RTRIM(w.[WarehouseSiteCode])))    AS [Warehouse Site Code]
            , w.[WWIWarehouseSiteID]                        AS [WWI Warehouse Site ID]
            , w.[WarehouseSiteName]                         AS [Warehouse Site]
            , UPPER(ISNULL(w.[SiteTypeCode], N'DC'))        AS [Site Type Code]
            , UPPER(ISNULL(w.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , UPPER(w.[CountryCode])                        AS [Country Code]
            , UPPER(w.[SubdivisionCode])                    AS [Subdivision Code]
            , w.[CityName]                                  AS [City Name]
            , UPPER(w.[CostCenterCode])                     AS [Cost Center Code]
            , UPPER(w.[OperatingCompanyCode])               AS [Operating Company Code]
            , ISNULL(w.[IsThirdPartyOperated], 0)           AS [Is Third Party Operated]
            , w.[OperatorName]                              AS [Operator Name]
            , w.[StorageCapacityPallets]                    AS [Storage Capacity Pallets]
            , w.[PickFaces]                                 AS [Pick Faces]
            , CONVERT(SMALLINT, w.[DockDoors])              AS [Dock Doors]
            , ISNULL(w.[HasChilledStorage], 0)              AS [Has Chilled Storage]
            , ISNULL(w.[HasFrozenStorage], 0)               AS [Has Frozen Storage]
            , ISNULL(w.[HasHazardousStorage], 0)            AS [Has Hazardous Storage]
            , UPPER(ISNULL(w.[AutomationLevelCode], N'MAN')) AS [Automation Level Code]
            , w.[OpenedOn]                                  AS [Opened On]
            , w.[ClosedOn]                                  AS [Closed On]
            , UPPER(ISNULL(w.[OperatingHoursCode], N'5X8')) AS [Operating Hours Code]
            , w.[TimeZoneName]                              AS [Time Zone Name]
            , UPPER(ISNULL(w.[InventoryValuationMethod], N'FIFO')) AS [Inventory Valuation Method]
            , UPPER(ISNULL(w.[CycleCountPolicyCode], N'ABC')) AS [Cycle Count Policy Code]
            , ISNULL(w.[CustomsBondedFlag], 0)              AS [Customs Bonded Flag]
            , w.[ExciseWarehouseNumber]                     AS [Excise Warehouse Number]
            , w.[BondedCarrierCode]                         AS [Bonded Carrier Code]
            , w.[CapacityChangeReasonCode]                  AS [Capacity Change Reason Code]
            , CASE WHEN w.[ClosedOn] IS NOT NULL AND w.[ClosedOn] <= @Today THEN 0 ELSE 1 END AS [Is Active]
        INTO #WarehouseSiteSource
        FROM [ref].[WarehouseSite] AS w
        WHERE NULLIF(LTRIM(RTRIM(w.[WarehouseSiteCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        /* An EU bonded site with no excise warehouse number cannot receive duty
           suspended stock; the WMS lets it happen anyway. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Warehouse Site',
               s.[Warehouse Site Code], N'EU_BONDED_NO_EXCISE_NUMBER',
               N'EU site is flagged customs bonded but carries no excise warehouse number.',
               N'Reference', CONCAT(N'Country=', s.[Country Code], N'|Site=', s.[Warehouse Site])
        FROM #WarehouseSiteSource AS s
        WHERE s.[Region Code] = N'EU'
          AND s.[Customs Bonded Flag] = 1
          AND NULLIF(s.[Excise Warehouse Number], N'') IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        /* Type 3 capacity handling: shift the current value down before the
           overwrite, but only when it actually moved. */
        UPDATE d
        SET d.[Prior Storage Capacity Pallets] = d.[Storage Capacity Pallets],
            d.[Capacity Changed On]            = @Today,
            d.[Capacity Change Reason Code]    = ISNULL(s.[Capacity Change Reason Code], N'UNSTATED')
        FROM [Dimension].[Warehouse Site] AS d
        INNER JOIN #WarehouseSiteSource AS s
            ON s.[Warehouse Site Code] = d.[Warehouse Site Code]
        WHERE d.[Warehouse Site Key] > 0
          AND ISNULL(s.[Storage Capacity Pallets], -1) <> ISNULL(d.[Storage Capacity Pallets], -1);

        UPDATE d
        SET d.[Warehouse Site]             = s.[Warehouse Site],
            d.[Site Type Code]             = s.[Site Type Code],
            d.[Region Code]                = s.[Region Code],
            d.[Geography Key]              = g.[Geography Key],
            d.[City Key]                   = ISNULL(c.[City Key], -1),
            d.[Cost Center Key]            = ISNULL(cc.[Cost Center Key], -1),
            d.[Operating Company Code]     = s.[Operating Company Code],
            d.[Is Third Party Operated]    = s.[Is Third Party Operated],
            d.[Operator Name]              = s.[Operator Name],
            d.[Storage Capacity Pallets]   = s.[Storage Capacity Pallets],
            d.[Pick Faces]                 = s.[Pick Faces],
            d.[Dock Doors]                 = s.[Dock Doors],
            d.[Has Chilled Storage]        = s.[Has Chilled Storage],
            d.[Has Frozen Storage]         = s.[Has Frozen Storage],
            d.[Has Hazardous Storage]      = s.[Has Hazardous Storage],
            d.[Automation Level Code]      = s.[Automation Level Code],
            d.[Opened On]                  = s.[Opened On],
            d.[Closed On]                  = s.[Closed On],
            d.[Is Active]                  = s.[Is Active],
            d.[Operating Hours Code]       = s.[Operating Hours Code],
            d.[Time Zone Name]             = s.[Time Zone Name],
            d.[Inventory Valuation Method] = s.[Inventory Valuation Method],
            d.[Cycle Count Policy Code]    = s.[Cycle Count Policy Code],
            d.[Customs Bonded Flag]        = s.[Customs Bonded Flag],
            d.[EU Excise Warehouse Number] = CASE WHEN s.[Region Code] = N'EU' THEN s.[Excise Warehouse Number] END,
            d.[NA Bonded Carrier Code]     = CASE WHEN s.[Region Code] = N'NA' THEN s.[Bonded Carrier Code] END,
            d.[Source System Code]         = @SourceSystemCode,
            d.[Row Hash Type 1]            = HASHBYTES(N'SHA2_256',
                  CONCAT_WS(N'|', ISNULL(s.[Warehouse Site], N''),
                            ISNULL(s.[Site Type Code], N''),
                            ISNULL(CONVERT(NVARCHAR(12), s.[Storage Capacity Pallets]), N''),
                            ISNULL(s.[Automation Level Code], N''),
                            ISNULL(CONVERT(NVARCHAR(1), s.[Customs Bonded Flag]), N''),
                            ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N''))),
            d.[Last Load Batch Id]         = @BatchId
        FROM [Dimension].[Warehouse Site] AS d
        INNER JOIN #WarehouseSiteSource AS s
            ON s.[Warehouse Site Code] = d.[Warehouse Site Code]
        LEFT OUTER JOIN [Dimension].[Geography] AS g
            ON  g.[Country Code]     = s.[Country Code]
            AND g.[Subdivision Code] = s.[Subdivision Code]
        LEFT OUTER JOIN [Dimension].[City] AS c
            ON  c.[City] = s.[City Name]
            AND c.[Is Current Row] = 1
        LEFT OUTER JOIN [Dimension].[Cost Center] AS cc
            ON  cc.[Cost Center Code] = s.[Cost Center Code]
            AND cc.[Is Current Row]   = 1
        WHERE d.[Warehouse Site Key] > 0;

        SET @UpdatedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Warehouse Site]
            ([WWI Warehouse Site ID], [Warehouse Site Code], [Warehouse Site], [Site Type Code],
             [Region Code], [Geography Key], [City Key], [Cost Center Key],
             [Operating Company Code], [Is Third Party Operated], [Operator Name],
             [Storage Capacity Pallets], [Pick Faces], [Dock Doors], [Has Chilled Storage],
             [Has Frozen Storage], [Has Hazardous Storage], [Automation Level Code],
             [Opened On], [Closed On], [Is Active], [Operating Hours Code], [Time Zone Name],
             [Inventory Valuation Method], [Cycle Count Policy Code], [Customs Bonded Flag],
             [EU Excise Warehouse Number], [NA Bonded Carrier Code], [Source System Code],
             [Row Hash Type 1], [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[WWI Warehouse Site ID]
            , s.[Warehouse Site Code]
            , ISNULL(s.[Warehouse Site], s.[Warehouse Site Code])
            , s.[Site Type Code]
            , s.[Region Code]
            , g.[Geography Key]
            , ISNULL(c.[City Key], -1)
            , ISNULL(cc.[Cost Center Key], -1)
            , s.[Operating Company Code]
            , s.[Is Third Party Operated]
            , s.[Operator Name]
            , s.[Storage Capacity Pallets]
            , s.[Pick Faces]
            , s.[Dock Doors]
            , s.[Has Chilled Storage]
            , s.[Has Frozen Storage]
            , s.[Has Hazardous Storage]
            , s.[Automation Level Code]
            , s.[Opened On]
            , s.[Closed On]
            , s.[Is Active]
            , s.[Operating Hours Code]
            , s.[Time Zone Name]
            , s.[Inventory Valuation Method]
            , s.[Cycle Count Policy Code]
            , s.[Customs Bonded Flag]
            , CASE WHEN s.[Region Code] = N'EU' THEN s.[Excise Warehouse Number] END
            , CASE WHEN s.[Region Code] = N'NA' THEN s.[Bonded Carrier Code] END
            , @SourceSystemCode
            , HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(s.[Warehouse Site], N''),
                          ISNULL(s.[Site Type Code], N''),
                          ISNULL(CONVERT(NVARCHAR(12), s.[Storage Capacity Pallets]), N''),
                          ISNULL(s.[Automation Level Code], N''),
                          ISNULL(CONVERT(NVARCHAR(1), s.[Customs Bonded Flag]), N''),
                          ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N'')))
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #WarehouseSiteSource AS s
        LEFT OUTER JOIN [Dimension].[Geography] AS g
            ON  g.[Country Code]     = s.[Country Code]
            AND g.[Subdivision Code] = s.[Subdivision Code]
        LEFT OUTER JOIN [Dimension].[City] AS c
            ON  c.[City] = s.[City Name]
            AND c.[Is Current Row] = 1
        LEFT OUTER JOIN [Dimension].[Cost Center] AS cc
            ON  cc.[Cost Center Code] = s.[Cost Center Code]
            AND cc.[Is Current Row]   = 1
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Warehouse Site] AS d
                          WHERE d.[Warehouse Site Code] = s.[Warehouse Site Code]
                            AND d.[Warehouse Site Key]  > 0);

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Warehouse Site', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'Type1WithType3', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Warehouse Site',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @UpdatedCount,
             @RejectRowCount     = @RejectCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @UpdatedCount,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Warehouse Site',
             @ProcedureName      = N'Integration.usp_MigrateStagedWarehouseSiteData',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @SourceRowCount,
             @RowsRejected       = @RejectCount;

        THROW;
    END CATCH;
END;
GO
