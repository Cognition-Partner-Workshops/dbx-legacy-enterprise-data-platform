/*
    Sales.ufn_DiscountPercentForCustomer

    Catalog entry : sqlserver_oltp.functions - Sales.ufn_DiscountPercentForCustomer
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 04_functions / 4010 - after 4000
    Depends on    : Sales.CustomerSegmentAssignments, Sales.CustomerSegments,
                    Loyalty.LoyaltyMembers, Loyalty.LoyaltyTiers
    Called by     : Sales.usp_CalculateOrderDiscounts, quote entry screen

    Resolves the standing discount a customer gets, before promotions. The
    precedence is segment, then loyalty tier, then the customer's own
    negotiated rate, and the three are added together up to a cap of 35 per
    cent - a rule that exists because the 2013 stacking bug gave a distributor
    112 per cent off and the fix was a MIN() rather than a redesign.
*/
CREATE FUNCTION [Sales].[ufn_DiscountPercentForCustomer]
(
    @CustomerID     INT,
    @AsAtDate       DATE
)
RETURNS DECIMAL (5, 2)
AS
BEGIN
    DECLARE @SegmentPercent     DECIMAL (5, 2) = 0;
    DECLARE @TierPercent        DECIMAL (5, 2) = 0;
    DECLARE @NegotiatedPercent  DECIMAL (5, 2) = 0;
    DECLARE @Total              DECIMAL (5, 2);

    -- Segment discount is not held on the segment row; it is a CASE that was
    -- copied out of the 2008 pricing spreadsheet and has been maintained here
    -- ever since.
    SELECT TOP (1) @SegmentPercent = CASE seg.[SegmentFamily]
                                         WHEN N'STRATEGIC' THEN 12.50
                                         WHEN N'KEYACCOUNT' THEN 10.00
                                         WHEN N'GROWTH' THEN 5.00
                                         WHEN N'VALUE' THEN 2.50
                                         ELSE 0
                                     END
    FROM [Sales].[CustomerSegmentAssignments] AS asg
        INNER JOIN [Sales].[CustomerSegments] AS seg
            ON seg.[CustomerSegmentID] = asg.[CustomerSegmentID]
    WHERE asg.[CustomerID] = @CustomerID
        AND asg.[ValidFromDate] <= @AsAtDate
        AND (asg.[ValidToDate] IS NULL OR asg.[ValidToDate] > @AsAtDate)
    ORDER BY seg.[PriorityOrder] ASC, asg.[ValidFromDate] DESC;

    SELECT TOP (1) @TierPercent = ISNULL(tier.[DiscountPercent], 0)
    FROM [Loyalty].[LoyaltyMembers] AS mem
        INNER JOIN [Loyalty].[LoyaltyTiers] AS tier
            ON tier.[LoyaltyTierID] = mem.[CurrentTierID]
    WHERE mem.[CustomerID] = @CustomerID
        AND mem.[MemberStatus] = N'ACTIVE'
    ORDER BY tier.[TierRank] DESC;

    -- The negotiated rate lives in the EAV extension store, typed as text.
    SELECT TOP (1) @NegotiatedPercent = TRY_CONVERT(DECIMAL (5, 2), val.[ValueText])
    FROM [Application].[ExtensionAttributeValues] AS val
        INNER JOIN [Application].[ExtensionAttributeDefinitions] AS def
            ON def.[ExtensionAttributeID] = val.[ExtensionAttributeID]
    WHERE def.[EntityTypeCode] = N'CUSTOMER'
        AND def.[AttributeCode] = N'NEGOTIATED_DISCOUNT_PCT'
        AND val.[EntityKeyValue] = CONVERT(NVARCHAR (60), @CustomerID)
    ORDER BY val.[LastEditedWhen] DESC;

    SET @NegotiatedPercent = ISNULL(@NegotiatedPercent, 0);

    SET @Total = @SegmentPercent + @TierPercent + @NegotiatedPercent;

    IF @Total > 35.00
        SET @Total = 35.00;

    RETURN @Total;
END
GO
