/*
    Sales.ufn_CommissionRate

    Catalog entry : sqlserver_oltp.functions - Sales.ufn_CommissionRate
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 04_functions / 4020 - after 4010
    Depends on    : Sales.CommissionPlans
    Called by     : Sales.usp_RecalculateCommissionAccruals

    Returns the marginal rate for an attainment percentage under a plan. The
    plan holds exactly three bands as fixed columns, so this is three nested
    comparisons; the 2019 proposal for a four-band plan was rejected on the
    grounds that it would need a schema change here.
*/
CREATE FUNCTION [Sales].[ufn_CommissionRate]
(
    @CommissionPlanID   INT,
    @AttainmentPercent  DECIMAL (9, 4)
)
RETURNS DECIMAL (6, 3)
AS
BEGIN
    DECLARE @Rate DECIMAL (6, 3) = 0;

    SELECT @Rate = CASE
                       WHEN @AttainmentPercent IS NULL THEN 0
                       WHEN @AttainmentPercent <= cp.[Band1UpperPercent] THEN cp.[Band1RatePercent]
                       WHEN @AttainmentPercent <= cp.[Band2UpperPercent] THEN cp.[Band2RatePercent]
                       WHEN @AttainmentPercent <= cp.[Band3UpperPercent] THEN cp.[Band3RatePercent]
                       ELSE cp.[Band3RatePercent]
                   END
    FROM [Sales].[CommissionPlans] AS cp
    WHERE cp.[CommissionPlanID] = @CommissionPlanID;

    RETURN ISNULL(@Rate, 0);
END
GO
