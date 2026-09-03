/*
    Sales.vw_CustomerSegmentCurrent

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5030 - after 5020
    Depends on    : Sales.CustomerSegmentAssignments, Sales.CustomerSegments,
                    Sales.Customers
    Called by     : Sales.usp_AssignCustomerSegments, marketing extracts

    Current segment per customer. Assignments are supposed to be closed off
    with ValidToDate, and IsCurrentRow is supposed to agree with that, but the
    two are maintained by different code paths; this view trusts the dates and
    exposes the disagreement rather than hiding it.

    Consent is region-specific: EU rows are suppressed for marketing unless an
    explicit consent timestamp exists, NA rows are eligible unless the
    customer opted out, APAC rows follow the segment's own consent flag.
*/
CREATE VIEW [Sales].[vw_CustomerSegmentCurrent]
AS
SELECT
    asg.[CustomerID],
    c.[CustomerName],
    c.[RegionCode],
    seg.[CustomerSegmentID],
    seg.[SegmentCode],
    seg.[SegmentName],
    seg.[SegmentFamily],
    seg.[PriorityOrder],
    asg.[ValidFromDate],
    asg.[ValidToDate],
    asg.[IsCurrentRow],
    CASE WHEN asg.[ValidToDate] IS NULL AND asg.[IsCurrentRow] = 0 THEN 1
         WHEN asg.[ValidToDate] IS NOT NULL AND asg.[IsCurrentRow] = 1 THEN 1
         ELSE 0 END                         AS [IsCurrencyFlagInconsistent],
    asg.[ScoreValue],
    asg.[AssignedByProcess],
    asg.[ConsentCapturedWhen],
    asg.[RetentionExpiryDate],
    CASE c.[RegionCode]
        WHEN N'EU' THEN CASE WHEN asg.[ConsentCapturedWhen] IS NOT NULL AND ISNULL(c.[MarketingConsentFlag], 0) = 1 THEN 1 ELSE 0 END
        WHEN N'NA' THEN CASE WHEN ISNULL(c.[MarketingConsentFlag], 1) = 0 THEN 0 ELSE 1 END
        ELSE CASE WHEN seg.[ConsentRequired] = 1 AND asg.[ConsentCapturedWhen] IS NULL THEN 0 ELSE 1 END
    END                                     AS [IsMarketingEligible]
FROM [Sales].[CustomerSegmentAssignments] AS asg
    INNER JOIN [Sales].[CustomerSegments] AS seg
        ON seg.[CustomerSegmentID] = asg.[CustomerSegmentID]
    INNER JOIN [Sales].[Customers] AS c
        ON c.[CustomerID] = asg.[CustomerID]
WHERE asg.[ValidToDate] IS NULL;
GO
