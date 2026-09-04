/*
    Integration.usp_LoadFactLoyaltyPoints

    Object        : Integration.usp_LoadFactLoyaltyPoints
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Loyalty Points.
    Called by     : FACT_Load_Loyalty_Points, CRM_Load_Loyalty.
    Reads         : stg.LoyaltyTransaction, stg.LoyaltyScheme.
    Depends on    : the etl control procedures.

    Point movements are a transaction fact with a running balance carried on
    the row, because the loyalty statement has to show the balance after each
    movement and the source system does not keep it.

    Expiry is generated here, not sourced: the schemes expire points on
    different clocks (NA rolling 24 months from earning, EU 36 months from the
    end of the scheme year, APAC 12 months and forfeited on tier downgrade),
    and the source system only stores the earning date. The generated expiry
    rows carry [Movement Type Code] = 'EXPIRE' and a negative point value.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactLoyaltyPoints', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactLoyaltyPoints;
GO

CREATE PROCEDURE Integration.usp_LoadFactLoyaltyPoints
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @GenerateExpiries   BIT = 1
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @ExpiryRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Loyalty_Points',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactLoyaltyPoints',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'CRM_LOYALTY',
            @ObjectName       = N'Fact.Loyalty Points',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        INSERT INTO Fact.[Loyalty Points]
        (
            [Movement Date Key], [Points Expiry Date Key], [Customer Key], [Loyalty Scheme Key],
            [Loyalty Tier Key], [Sales Channel Key], [Promotion Key], [Region Code],
            [Loyalty Account Number], [Movement Reference], [Invoice Number],
            [Movement Type Code], [Points Delta], [Points Balance After],
            [Redemption Value Amount], [Bonus Multiplier], [Marketing Consent Flag],
            [Natural Key Hash], [Lineage Key], [Batch Id], [Load Datetime]
        )
        SELECT
            t.MovementDate,
            CASE
                WHEN t.MovementTypeCode <> N'EARN' THEN NULL
                WHEN t.RegionCode = N'NA'   THEN DATEADD(MONTH, 24, t.MovementDate)
                WHEN t.RegionCode = N'EU'   THEN DATEADD(MONTH, 36, DATEFROMPARTS(YEAR(t.MovementDate), 12, 31))
                WHEN t.RegionCode = N'APAC' THEN DATEADD(MONTH, 12, t.MovementDate)
            END,
            ISNULL(cust.[Customer Key], 0),
            ISNULL(sch.[Loyalty Scheme Key], 0),
            ISNULL(tier.[Loyalty Tier Key], 0),
            CASE WHEN t.ChannelCode IS NULL THEN -1 ELSE ISNULL(chan.[Sales Channel Key], 0) END,
            CASE WHEN t.PromotionCode IS NULL THEN -1 ELSE ISNULL(promo.[Promotion Key], 0) END,
            t.RegionCode,
            t.LoyaltyCardNumber,
            t.MovementReference,
            t.InvoiceNumber,
            t.MovementTypeCode,
            CASE WHEN t.MovementTypeCode = N'REDEEM' THEN -ABS(t.Points) ELSE t.Points END,
            NULL,
            t.RedemptionValueAmount,
            ISNULL(tier.[Point Multiplier], 1.0),
            ISNULL(cust.[Marketing Consent Flag], 0),
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', t.MovementReference)),
            0, @BatchId, SYSDATETIME()
        FROM stg.LoyaltyTransaction AS t
        LEFT JOIN Dimension.[Customer] AS cust
            ON cust.[WWI Customer ID] = TRY_CONVERT(INT, t.CustomerBusinessKey)
           AND cust.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Loyalty Scheme] AS sch
            ON sch.[Scheme Code] = t.SchemeCode
        LEFT JOIN Dimension.[Loyalty Tier] AS tier
            ON tier.[Tier Code] = t.TierCode
           AND tier.[Scheme Code] = t.SchemeCode
        LEFT JOIN Dimension.[Sales Channel] AS chan
            ON chan.[Channel Code] = t.ChannelCode
        LEFT JOIN Dimension.[Promotion] AS promo
            ON promo.[Promotion Code] = t.PromotionCode
           AND promo.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        WHERE t.MovementDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
          AND NOT EXISTS (SELECT 1 FROM Fact.[Loyalty Points] AS f
                          WHERE f.[Movement Reference] = t.MovementReference);

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        IF @GenerateExpiries = 1
        BEGIN
            INSERT INTO Fact.[Loyalty Points]
            (
                [Movement Date Key], [Customer Key], [Loyalty Scheme Key], [Loyalty Tier Key],
                [Sales Channel Key], [Promotion Key], [Region Code], [Loyalty Account Number],
                [Movement Reference], [Movement Type Code], [Points Delta],
                [Marketing Consent Flag],
                [Natural Key Hash], [Lineage Key], [Batch Id], [Load Datetime]
            )
            SELECT
                f.[Points Expiry Date Key], f.[Customer Key], f.[Loyalty Scheme Key], f.[Loyalty Tier Key],
                -1, -1, f.[Region Code], f.[Loyalty Account Number],
                CONCAT(N'EXP-', f.[Movement Reference]), N'EXPIRE',
                -ABS(f.[Points Delta]), f.[Marketing Consent Flag],
                CONVERT(VARBINARY(32), HASHBYTES('SHA2_256',
                    CONCAT(N'EXP-', f.[Movement Reference]))),
                0, @BatchId, SYSDATETIME()
            FROM Fact.[Loyalty Points] AS f
            WHERE f.[Movement Type Code] = N'EARN'
              AND f.[Points Expiry Date Key] IS NOT NULL
              AND f.[Points Expiry Date Key] <= CONVERT(DATE, SYSDATETIME())
              AND NOT EXISTS (SELECT 1 FROM Fact.[Loyalty Points] AS x
                              WHERE x.[Movement Reference] = CONCAT(N'EXP-', f.[Movement Reference]));

            SET @ExpiryRowCount = @@ROWCOUNT;
        END;

        /* Running balance per card, recomputed for every card touched today.
           A window function would do it in one pass but the balance also has
           to survive the expiry rows inserted above, so it is done after. */
        ;WITH bal AS
        (
            SELECT [Loyalty Points Key],
                   SUM([Points Delta]) OVER (PARTITION BY [Loyalty Account Number]
                                             ORDER BY [Movement Date Key], [Loyalty Points Key]
                                             ROWS UNBOUNDED PRECEDING) AS RunningBalance
            FROM Fact.[Loyalty Points]
            WHERE [Loyalty Account Number] IN
            (
                SELECT DISTINCT [Loyalty Account Number]
                FROM Fact.[Loyalty Points]
                WHERE [Batch Id] = @BatchId
            )
        )
        UPDATE f
        SET [Points Balance After] = bal.RunningBalance
        FROM Fact.[Loyalty Points] AS f
        INNER JOIN bal ON bal.[Loyalty Points Key] = f.[Loyalty Points Key];

        SELECT @RejectRowCount = COUNT_BIG(*)
        FROM Fact.[Loyalty Points]
        WHERE [Batch Id] = @BatchId AND [Customer Key] = 0;

        IF @RejectRowCount > 0
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'CRM_LOYALTY',
                @ObjectName         = N'Fact.Loyalty Points',
                @BusinessKey        = N'(grouped)',
                @RejectReasonCode   = N'CARD_NO_CUSTOMER',
                @RejectReason       = N'Loyalty card is not linked to a known customer',
                @RejectStage        = N'Fact';

        DECLARE @InsertRowCountValue BIGINT = @InsertRowCount + @ExpiryRowCount;
        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Loyalty Points',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCountValue,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'CRM_LOYALTY',
            @ObjectName         = N'Fact.Loyalty Points',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            DECLARE @RowsInsertedValue BIGINT = @InsertRowCount + @ExpiryRowCount;
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @RowsInsertedValue,
                @RowsRejected       = @RejectRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Loyalty Points',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactLoyaltyPoints',
            @ErrorDescription   = @ErrorMessage;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Failed';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
