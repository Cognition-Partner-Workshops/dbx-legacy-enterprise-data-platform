/*
    Integration.usp_LoadFactWebSession

    Object        : Integration.usp_LoadFactWebSession
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Web Session.
    Called by     : FACT_Load_Web_Session, WEB_Load_Clickstream (four times a day).
    Reads         : stg.WebSessionRaw, stg.WebPageEvent, stg.CookieConsent.
    Depends on    : the etl control procedures.

    High volume, insert-only, keyed on the session GUID. Sessions are loaded
    once they are closed; an open session is left in staging and picked up on
    the next run, which is why the filter is on SessionEndDatetime.

    Consent handling is the whole reason this load is not trivial:
      EU   - no analytics cookie consent means the session loads with the
             customer key set to the unknown member and the IP truncated. The
             row still exists so traffic volumes are right.
      APAC - consent is recorded per jurisdiction; where it is absent the row
             is pseudonymised (hashed visitor id) rather than unlinked.
      NA   - opt-out model, so the session is linked unless the visitor has an
             active opt-out record.

    Retention differs too and is stamped on the row as [Purge After Date] so
    the housekeeping job does not have to re-derive it.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactWebSession', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactWebSession;
GO

CREATE PROCEDURE Integration.usp_LoadFactWebSession
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Web_Session',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactWebSession',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'WEB_ANALYTICS',
            @ObjectName       = N'Fact.Web Session',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        SELECT
            e.SessionGuid,
            SUM(CASE WHEN e.EventTypeCode = N'PAGEVIEW' THEN 1 ELSE 0 END) AS PageViewCount,
            SUM(CASE WHEN e.EventTypeCode = N'SEARCH' THEN 1 ELSE 0 END)   AS SearchCount,
            SUM(CASE WHEN e.EventTypeCode = N'ADDCART' THEN 1 ELSE 0 END)  AS AddToCartCount,
            SUM(CASE WHEN e.EventTypeCode = N'REMCART' THEN 1 ELSE 0 END)  AS RemoveFromCartCount,
            MAX(CASE WHEN e.EventTypeCode = N'CHECKOUT' THEN 1 ELSE 0 END) AS CheckoutStartedFlag,
            MAX(CASE WHEN e.EventTypeCode = N'ORDER' THEN 1 ELSE 0 END)    AS OrderPlacedFlag,
            MAX(CASE WHEN e.EventTypeCode = N'ORDER' THEN e.OrderNumber END) AS OrderNumber
        INTO #SessionEvent
        FROM stg.WebPageEvent AS e
        GROUP BY e.SessionGuid;

        INSERT INTO Fact.[Web Session]
        (
            [Session Date Key], [Customer Key], [Sales Channel Key], [Device Type Key],
            [Marketing Campaign Key], [Geography Key], [Region Code], [Session Guid],
            [Visitor Id], [Order Number], [Session Start Datetime], [Session End Datetime],
            [Session Duration Seconds], [Page View Count], [Search Count],
            [Add To Cart Count], [Remove From Cart Count], [Cart Value Amount],
            [Cart Currency Code], [Checkout Started Flag], [Order Placed Flag],
            [Bounce Flag], [Landing Page Path], [Exit Page Path], [Referrer Domain],
            [Utm Source], [Utm Medium], [Utm Campaign], [Ip Address Truncated],
            [Consent Analytics Flag], [Consent Basis Code], [Pseudonymised Flag],
            [Purge After Date], [Natural Key Hash], [Lineage Key], [Batch Id], [Load Datetime]
        )
        SELECT
            CONVERT(DATE, s.SessionStartDatetime),
            CASE
                WHEN s.RegionCode = N'EU'   AND ISNULL(cc.AnalyticsConsentFlag, 0) = 0 THEN 0
                WHEN s.RegionCode = N'APAC' AND ISNULL(cc.AnalyticsConsentFlag, 0) = 0 THEN 0
                WHEN s.RegionCode = N'NA'   AND ISNULL(cc.OptOutFlag, 0) = 1 THEN 0
                ELSE ISNULL(cust.[Customer Key], 0)
            END,
            CASE WHEN s.ChannelCode IS NULL THEN -1 ELSE ISNULL(chan.[Sales Channel Key], 0) END,
            ISNULL(dev.[Device Type Key], 0),
            CASE WHEN s.UtmCampaign IS NULL THEN -1
                 ELSE ISNULL(camp.[Marketing Campaign Key], 0) END,
            CASE WHEN s.CountryCode IS NULL THEN -1 ELSE ISNULL(geo.[Geography Key], 0) END,
            s.RegionCode,
            s.SessionGuid,
            CASE
                WHEN s.RegionCode = N'APAC' AND ISNULL(cc.AnalyticsConsentFlag, 0) = 0
                    THEN CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', s.VisitorId), 2)
                ELSE s.VisitorId
            END,
            ev.OrderNumber,
            s.SessionStartDatetime,
            s.SessionEndDatetime,
            DATEDIFF(SECOND, s.SessionStartDatetime, s.SessionEndDatetime),
            ISNULL(ev.PageViewCount, 0),
            ISNULL(ev.SearchCount, 0),
            ISNULL(ev.AddToCartCount, 0),
            ISNULL(ev.RemoveFromCartCount, 0),
            s.CartValueAmount,
            s.CartCurrencyCode,
            ISNULL(ev.CheckoutStartedFlag, 0),
            ISNULL(ev.OrderPlacedFlag, 0),
            CASE WHEN ISNULL(ev.PageViewCount, 0) <= 1 THEN 1 ELSE 0 END,
            s.LandingPagePath,
            s.ExitPagePath,
            s.ReferrerDomain,
            s.UtmSource, s.UtmMedium, s.UtmCampaign,
            /* Last octet dropped everywhere; EU drops the last two. */
            CASE
                WHEN s.RegionCode = N'EU'
                    THEN PARSENAME(REPLACE(s.IpAddress, '.', '@'), 4) + N'.'
                         + PARSENAME(REPLACE(s.IpAddress, '.', '@'), 3) + N'.0.0'
                ELSE PARSENAME(REPLACE(s.IpAddress, '.', '@'), 4) + N'.'
                     + PARSENAME(REPLACE(s.IpAddress, '.', '@'), 3) + N'.'
                     + PARSENAME(REPLACE(s.IpAddress, '.', '@'), 2) + N'.0'
            END,
            CASE
                WHEN s.RegionCode = N'NA' THEN CASE WHEN ISNULL(cc.OptOutFlag, 0) = 1 THEN 0 ELSE 1 END
                ELSE ISNULL(cc.AnalyticsConsentFlag, 0)
            END,
            CASE
                WHEN s.RegionCode = N'EU'   THEN N'GDPR-CONSENT'
                WHEN s.RegionCode = N'APAC' THEN N'APPI-NOTICE'
                ELSE N'CCPA-OPTOUT'
            END,
            CASE WHEN s.RegionCode = N'APAC' AND ISNULL(cc.AnalyticsConsentFlag, 0) = 0
                 THEN 1 ELSE 0 END,
            CASE
                WHEN s.RegionCode = N'EU'   THEN DATEADD(MONTH, 14, CONVERT(DATE, s.SessionStartDatetime))
                WHEN s.RegionCode = N'APAC' THEN DATEADD(MONTH, 6,  CONVERT(DATE, s.SessionStartDatetime))
                ELSE DATEADD(YEAR, 3, CONVERT(DATE, s.SessionStartDatetime))
            END,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', s.SessionGuid)),
            0, @BatchId, SYSDATETIME()
        FROM stg.WebSessionRaw AS s
        LEFT JOIN #SessionEvent AS ev
            ON ev.SessionGuid = s.SessionGuid
        LEFT JOIN stg.CookieConsent AS cc
            ON cc.VisitorId = s.VisitorId
        LEFT JOIN Dimension.[Customer] AS cust
            ON cust.[WWI Customer ID] = TRY_CONVERT(INT, s.CustomerBusinessKey)
           AND cust.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Sales Channel] AS chan
            ON chan.[Channel Code] = s.ChannelCode
        LEFT JOIN Dimension.[Device Type] AS dev
            ON dev.[Device Type Code] = s.DeviceTypeCode
        LEFT JOIN Dimension.[Marketing Campaign] AS camp
            ON camp.[Campaign Code] = s.UtmCampaign
           AND camp.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Geography] AS geo
            ON geo.[Country Code] = s.CountryCode
        WHERE s.SessionEndDatetime IS NOT NULL
          AND s.SessionEndDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
          AND NOT EXISTS (SELECT 1 FROM Fact.[Web Session] AS f
                          WHERE f.[Session Id] = s.SessionGuid);

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        SELECT @RejectRowCount = COUNT_BIG(*)
        FROM stg.WebSessionRaw AS s
        WHERE s.SessionEndDatetime IS NOT NULL
          AND s.SessionEndDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
          AND (s.SessionGuid IS NULL OR s.SessionStartDatetime IS NULL);

        IF @RejectRowCount > 0
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'WEB_ANALYTICS',
                @ObjectName         = N'Fact.Web Session',
                @BusinessKey        = N'(grouped)',
                @RejectReasonCode   = N'SESSION_MALFORMED',
                @RejectReason       = N'Clickstream session has no GUID or no start time',
                @RejectStage        = N'Fact';

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Web Session',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'WEB_ANALYTICS',
            @ObjectName         = N'Fact.Web Session',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsRejected       = @RejectRowCount;

        DROP TABLE #SessionEvent;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Fact.Web Session',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactWebSession',
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
