/*
    Object        : [Integration].[usp_MigrateStagedCarrierData]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.CarrierService, Dimension.Carrier,
                    the etl control framework
    Called by     : REF_Load_Carrier

    Type 1 at carrier-service grain, not carrier grain. Shipping asked for the
    service level on the shipment fact in 2011 and rather than add a service
    dimension the carrier dimension was re-grained, which is why the natural key
    is the carrier code plus the service code and why two rows can share a
    carrier name. The old carrier-grain rows were never removed - they have a
    service code of 'STD' and are still referenced by shipment facts loaded
    before 2011, so the load must not touch rows it does not see in the extract.

    Regional divergence is in the surcharge model and the rating basis:

      NA   - dimensional weight with a divisor, residential surcharge, fuel.
      EU   - actual weight in kilograms, no residential surcharge, customs
             surcharge on non-EU destinations.
      APAC - marketplace-nominated carriers with their own scan event code set
             and no negotiated fuel surcharge.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedCarrierData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_Carrier',
    @SourceSystemCode   NVARCHAR(20)  = N'FILE_TMS',
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
        IF OBJECT_ID(N'tempdb..#CarrierSource') IS NOT NULL DROP TABLE #CarrierSource;

        SELECT
              UPPER(LTRIM(RTRIM(c.[CarrierCode])))          AS [Carrier Code]
            , UPPER(ISNULL(NULLIF(LTRIM(RTRIM(c.[ServiceCode])), N''), N'STD')) AS [Service Code]
            , c.[WWICarrierID]                              AS [WWI Carrier ID]
            , c.[CarrierName]                               AS [Carrier Name]
            , c.[ServiceName]                               AS [Service Name]
            , UPPER(ISNULL(c.[ServiceLevelCode], N'STD'))   AS [Service Level Code]
            , UPPER(ISNULL(c.[ModeCode], N'ROAD'))          AS [Mode Code]
            , UPPER(ISNULL(c.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , c.[CoverageCountryList]                       AS [Coverage Country List]
            , ISNULL(c.[IsInternational], 0)                AS [Is International]
            , ISNULL(c.[IsMarketplaceNominated], 0)         AS [Is Marketplace Nominated]
            , CONVERT(SMALLINT, c.[TransitDaysMinimum])     AS [Transit Days Minimum]
            , CONVERT(SMALLINT, c.[TransitDaysMaximum])     AS [Transit Days Maximum]
            , CONVERT(TIME(0), c.[CutoffLocalTime])         AS [Cutoff Local Time]
            , c.[OnTimeTargetPercentage]                    AS [On Time Target Percentage]
            , c.[MaximumParcelWeightKg]                     AS [Maximum Parcel Weight Kg]
            , ISNULL(c.[HandlesHazardous], 0)               AS [Handles Hazardous]
            , ISNULL(c.[HandlesChilled], 0)                 AS [Handles Chilled]
            , c.[TrackingNumberPattern]                     AS [Tracking Number Pattern]
            , c.[TrackingUrlTemplate]                       AS [Tracking URL Template]
            , c.[ScanEventCodeSet]                          AS [Scan Event Code Set]
            , c.[EdiPartnerIdentifier]                      AS [EDI Partner Identifier]
            , c.[AccountReference]                          AS [Account Reference]
            , c.[ContractEndDate]                           AS [Contract End Date]
            , ISNULL(c.[IsActive], 1)                       AS [Is Active]
            , CONVERT(NVARCHAR(10), NULL)                   AS [Rating Basis Code]
            , CONVERT(BIT, 0)                               AS [Fuel Surcharge Applies]
            , CONVERT(BIT, 0)                               AS [Security Surcharge Applies]
            , CONVERT(BIT, 0)                               AS [Customs Surcharge Applies]
            , CONVERT(BIT, 0)                               AS [Residential Surcharge Applies]
            , CONVERT(INT, NULL)                            AS [Dimensional Weight Divisor]
        INTO #CarrierSource
        FROM [ref].[CarrierService] AS c
        WHERE NULLIF(LTRIM(RTRIM(c.[CarrierCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        UPDATE s
        SET s.[Rating Basis Code]             = N'DIMWT',
            s.[Dimensional Weight Divisor]    = CASE WHEN s.[Mode Code] = N'AIR' THEN 166 ELSE 139 END,
            s.[Fuel Surcharge Applies]        = 1,
            s.[Residential Surcharge Applies] = 1,
            s.[Security Surcharge Applies]    = CASE WHEN s.[Mode Code] = N'AIR' THEN 1 ELSE 0 END,
            s.[Scan Event Code Set]           = ISNULL(s.[Scan Event Code Set], N'EDI214')
        FROM #CarrierSource AS s
        WHERE s.[Region Code] = N'NA';

        UPDATE s
        SET s.[Rating Basis Code]             = N'ACTWT',
            s.[Fuel Surcharge Applies]        = 1,
            s.[Customs Surcharge Applies]     = CASE WHEN s.[Is International] = 1 THEN 1 ELSE 0 END,
            s.[Residential Surcharge Applies] = 0,
            s.[Scan Event Code Set]           = ISNULL(s.[Scan Event Code Set], N'GS1')
        FROM #CarrierSource AS s
        WHERE s.[Region Code] = N'EU';

        UPDATE s
        SET s.[Rating Basis Code]             = CASE WHEN s.[Is Marketplace Nominated] = 1 THEN N'MKTPLACE' ELSE N'ACTWT' END,
            s.[Fuel Surcharge Applies]        = CASE WHEN s.[Is Marketplace Nominated] = 1 THEN 0 ELSE 1 END,
            s.[Customs Surcharge Applies]     = CASE WHEN s.[Is International] = 1 THEN 1 ELSE 0 END,
            s.[Scan Event Code Set]           = ISNULL(s.[Scan Event Code Set], N'LOCAL')
        FROM #CarrierSource AS s
        WHERE s.[Region Code] = N'APAC';

        /* A carrier whose contract expired but is still active will keep taking
           shipments at list rates. Logistics want this list weekly. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Carrier',
               CONCAT(s.[Carrier Code], N'/', s.[Service Code]), N'CARRIER_CONTRACT_EXPIRED',
               N'Carrier service is active but its contract end date has passed; rates are not negotiated.',
               N'Reference', CONCAT(N'ContractEnd=', CONVERT(NVARCHAR(10), s.[Contract End Date], 23))
        FROM #CarrierSource AS s
        WHERE s.[Is Active] = 1
          AND s.[Contract End Date] IS NOT NULL
          AND s.[Contract End Date] < @Today;

        SET @RejectCount = @@ROWCOUNT;

        /* Transit day ranges the wrong way round are corrected in place; the
           TMS file has had them reversed for two carriers since 2014. */
        UPDATE #CarrierSource
        SET [Transit Days Minimum] = [Transit Days Maximum],
            [Transit Days Maximum] = [Transit Days Minimum]
        WHERE [Transit Days Minimum] > [Transit Days Maximum];

        UPDATE d
        SET d.[Carrier Name]                   = s.[Carrier Name],
            d.[Service Name]                   = s.[Service Name],
            d.[Service Level Code]             = s.[Service Level Code],
            d.[Mode Code]                      = s.[Mode Code],
            d.[Region Code]                    = s.[Region Code],
            d.[Coverage Country List]          = s.[Coverage Country List],
            d.[Is International]               = s.[Is International],
            d.[Is Marketplace Nominated]       = s.[Is Marketplace Nominated],
            d.[Transit Days Minimum]           = s.[Transit Days Minimum],
            d.[Transit Days Maximum]           = s.[Transit Days Maximum],
            d.[Cutoff Local Time]              = s.[Cutoff Local Time],
            d.[On Time Target Percentage]      = s.[On Time Target Percentage],
            d.[Rating Basis Code]              = s.[Rating Basis Code],
            d.[Fuel Surcharge Applies]         = s.[Fuel Surcharge Applies],
            d.[Security Surcharge Applies]     = s.[Security Surcharge Applies],
            d.[Customs Surcharge Applies]      = s.[Customs Surcharge Applies],
            d.[Residential Surcharge Applies]  = s.[Residential Surcharge Applies],
            d.[Dimensional Weight Divisor]     = s.[Dimensional Weight Divisor],
            d.[Maximum Parcel Weight Kg]       = s.[Maximum Parcel Weight Kg],
            d.[Handles Hazardous]              = s.[Handles Hazardous],
            d.[Handles Chilled]                = s.[Handles Chilled],
            d.[Tracking Number Pattern]        = s.[Tracking Number Pattern],
            d.[Tracking URL Template]          = s.[Tracking URL Template],
            d.[Scan Event Code Set]            = s.[Scan Event Code Set],
            d.[EDI Partner Identifier]         = s.[EDI Partner Identifier],
            d.[Account Reference]              = s.[Account Reference],
            d.[Contract End Date]              = s.[Contract End Date],
            d.[Is Active]                      = s.[Is Active],
            d.[Source System Code]             = @SourceSystemCode,
            d.[Row Hash Type 1]                = HASHBYTES(N'SHA2_256',
                  CONCAT_WS(N'|', ISNULL(s.[Carrier Name], N''), ISNULL(s.[Service Name], N''),
                            ISNULL(s.[Rating Basis Code], N''),
                            ISNULL(CONVERT(NVARCHAR(6), s.[Transit Days Maximum]), N''),
                            ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N''))),
            d.[Last Load Batch Id]             = @BatchId
        FROM [Dimension].[Carrier] AS d
        INNER JOIN #CarrierSource AS s
            ON  s.[Carrier Code] = d.[Carrier Code]
            AND s.[Service Code] = d.[Service Code]
        WHERE d.[Carrier Key] > 0;

        SET @UpdatedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Carrier]
            ([WWI Carrier ID], [Carrier Code], [Carrier Name], [Service Code], [Service Name],
             [Service Level Code], [Mode Code], [Region Code], [Coverage Country List],
             [Is International], [Is Marketplace Nominated], [Transit Days Minimum],
             [Transit Days Maximum], [Cutoff Local Time], [On Time Target Percentage],
             [Rating Basis Code], [Fuel Surcharge Applies], [Security Surcharge Applies],
             [Customs Surcharge Applies], [Residential Surcharge Applies],
             [Dimensional Weight Divisor], [Maximum Parcel Weight Kg], [Handles Hazardous],
             [Handles Chilled], [Tracking Number Pattern], [Tracking URL Template],
             [Scan Event Code Set], [EDI Partner Identifier], [Account Reference],
             [Is Active], [Contract End Date], [Source System Code], [Row Hash Type 1],
             [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[WWI Carrier ID]
            , s.[Carrier Code]
            , ISNULL(s.[Carrier Name], s.[Carrier Code])
            , s.[Service Code]
            , s.[Service Name]
            , s.[Service Level Code]
            , s.[Mode Code]
            , s.[Region Code]
            , s.[Coverage Country List]
            , s.[Is International]
            , s.[Is Marketplace Nominated]
            , s.[Transit Days Minimum]
            , s.[Transit Days Maximum]
            , s.[Cutoff Local Time]
            , s.[On Time Target Percentage]
            , s.[Rating Basis Code]
            , s.[Fuel Surcharge Applies]
            , s.[Security Surcharge Applies]
            , s.[Customs Surcharge Applies]
            , s.[Residential Surcharge Applies]
            , s.[Dimensional Weight Divisor]
            , s.[Maximum Parcel Weight Kg]
            , s.[Handles Hazardous]
            , s.[Handles Chilled]
            , s.[Tracking Number Pattern]
            , s.[Tracking URL Template]
            , s.[Scan Event Code Set]
            , s.[EDI Partner Identifier]
            , s.[Account Reference]
            , s.[Is Active]
            , s.[Contract End Date]
            , @SourceSystemCode
            , HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(s.[Carrier Name], N''), ISNULL(s.[Service Name], N''),
                          ISNULL(s.[Rating Basis Code], N''),
                          ISNULL(CONVERT(NVARCHAR(6), s.[Transit Days Maximum]), N''),
                          ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N'')))
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #CarrierSource AS s
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Carrier] AS d
                          WHERE d.[Carrier Code] = s.[Carrier Code]
                            AND d.[Service Code] = s.[Service Code]
                            AND d.[Carrier Key]  > 0);

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Carrier', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'Type1UpdateInsert', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Carrier',
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
             @SourceComponent    = N'Dimension.Carrier',
             @ProcedureName      = N'Integration.usp_MigrateStagedCarrierData',
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
