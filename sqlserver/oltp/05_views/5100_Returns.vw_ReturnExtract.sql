/*
    Returns.vw_ReturnExtract

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5100 - after 5090
    Depends on    : Returns.ReturnAuthorizations, Returns.ReturnLines,
                    Returns.ReturnReasons, Returns.ReturnInspections
    Called by     : SSIS returns extract

    RMA lines with their reason and latest inspection outcome. Where a line
    has been inspected more than once the last inspection wins, including the
    ones flagged IsChallenged, so a challenged disposition can be the one that
    reaches the warehouse.
*/
CREATE VIEW [Returns].[vw_ReturnExtract]
AS
SELECT
    ra.[ReturnAuthorizationID],
    ra.[RmaNumber],
    ra.[CustomerID],
    ra.[RegionCode],
    ra.[OriginalInvoiceID],
    ra.[RequestedWhen],
    ra.[RequestChannel],
    ra.[AuthorizationStatus],
    ra.[AuthorizedWhen],
    ra.[ExpiresOnDate],
    ra.[GoodsReceivedWhen],
    ra.[ReceivedAtSiteID],
    ra.[IsCoolingOffPeriod],
    ra.[CreditCurrencyCode],
    rl.[ReturnLineID],
    rl.[LineNumber],
    rl.[StockItemID],
    rl.[LotNumber],
    rl.[QuantityAuthorized],
    rl.[QuantityReceived],
    rl.[QuantityAccepted],
    rl.[QuantityScrapped],
    rl.[UnitPriceAtSale],
    rl.[TaxRatePercentAtSale],
    rl.[RestockingPercent],
    rl.[GrossCreditAmount],
    rl.[DispositionCode]                                            AS [LineDispositionCode],
    rl.[LineStatus],
    rr.[ReasonCode],
    rr.[ReasonCategory],
    rr.[IsCustomerFault],
    rr.[AllowsResale],
    insp.[InspectionSequence]                                       AS [LatestInspectionSequence],
    insp.[ConditionGrade]                                           AS [LatestConditionGrade],
    insp.[DispositionCode]                                          AS [LatestInspectionDisposition],
    insp.[IsChallenged]                                             AS [IsLatestInspectionChallenged],
    CASE WHEN ISNULL(rl.[QuantityReceived], 0) < rl.[QuantityAuthorized] THEN 1 ELSE 0 END AS [IsShortReceipt],
    rl.[LastEditedWhen]                                             AS [ChangedWhen]
FROM [Returns].[ReturnLines] AS rl
    INNER JOIN [Returns].[ReturnAuthorizations] AS ra
        ON ra.[ReturnAuthorizationID] = rl.[ReturnAuthorizationID]
    INNER JOIN [Returns].[ReturnReasons] AS rr
        ON rr.[ReturnReasonID] = rl.[ReturnReasonID]
    OUTER APPLY
    (
        SELECT TOP (1)
            i.[InspectionSequence],
            i.[ConditionGrade],
            i.[DispositionCode],
            i.[IsChallenged]
        FROM [Returns].[ReturnInspections] AS i
        WHERE i.[ReturnLineID] = rl.[ReturnLineID]
        ORDER BY i.[InspectionSequence] DESC
    ) AS insp;
GO
