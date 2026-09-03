/*
    Ecommerce.vw_WebConversionFunnel

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5130 - after 5120
    Depends on    : Ecommerce.WebSessions, Ecommerce.CartHeaders, Ecommerce.CartLines
    Called by     : web analytics reporting

    Daily funnel by region, device and campaign. Bot-suspected sessions are
    counted separately rather than excluded, because the suspicion flag is set
    by a heuristic that marketing does not trust and finance does.

    Anonymised sessions keep their counts but lose their customer link, so
    the identified-session count falls as retention runs over history.
*/
CREATE VIEW [Ecommerce].[vw_WebConversionFunnel]
AS
SELECT
    CONVERT(DATE, ws.[StartedWhen])                                 AS [SessionDate],
    ws.[RegionCode],
    ws.[DeviceCategory],
    ws.[CampaignCode],
    ws.[ConsentStateCode],
    COUNT(*)                                                        AS [SessionCount],
    SUM(CASE WHEN ws.[IsBotSuspected] = 1 THEN 1 ELSE 0 END)        AS [BotSuspectedSessionCount],
    SUM(CASE WHEN ws.[CustomerID] IS NOT NULL THEN 1 ELSE 0 END)    AS [IdentifiedSessionCount],
    SUM(CASE WHEN ws.[AnonymisedWhen] IS NOT NULL THEN 1 ELSE 0 END) AS [AnonymisedSessionCount],
    SUM(CASE WHEN cart.[CartID] IS NOT NULL THEN 1 ELSE 0 END)      AS [SessionsWithCartCount],
    SUM(ISNULL(cart.[LineCount], 0))                                AS [CartLineCount],
    SUM(CASE WHEN cart.[CheckoutStepReached] >= 2 THEN 1 ELSE 0 END) AS [ReachedCheckoutCount],
    SUM(CASE WHEN cart.[AbandonedWhen] IS NOT NULL THEN 1 ELSE 0 END) AS [AbandonedCartCount],
    SUM(CASE WHEN cart.[ConvertedOrderID] IS NOT NULL THEN 1 ELSE 0 END) AS [ConvertedCount],
    SUM(ISNULL(cart.[MerchandiseSubtotal], 0))                      AS [CartValueTotal],
    SUM(CASE WHEN cart.[ConvertedOrderID] IS NOT NULL
             THEN ISNULL(cart.[MerchandiseSubtotal], 0) ELSE 0 END) AS [ConvertedValueTotal],
    AVG(CONVERT(DECIMAL (12, 2), ws.[DurationSeconds]))             AS [AverageSessionSeconds],
    AVG(CONVERT(DECIMAL (12, 2), ws.[PageViewCount]))               AS [AveragePageViews]
FROM [Ecommerce].[WebSessions] AS ws
    LEFT JOIN [Ecommerce].[CartHeaders] AS cart
        ON cart.[WebSessionID] = ws.[WebSessionID]
GROUP BY
    CONVERT(DATE, ws.[StartedWhen]),
    ws.[RegionCode],
    ws.[DeviceCategory],
    ws.[CampaignCode],
    ws.[ConsentStateCode];
GO
