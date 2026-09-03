/*
    Sales.vw_SalespersonPerformance

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5040 - after 5030
    Depends on    : Sales.SalesQuotas, Sales.CommissionAccruals,
                    Application.SalesTeamMembers, Application.SalesTeams,
                    Application.People
    Called by     : sales management reporting

    Quota, attainment and accrued commission per salesperson per period. Team
    membership is effective-dated and a person can sit on two teams at once,
    so the team columns come from the primary assignment open at the period
    end; where somebody has two open primary rows - which the unique index
    permits across different teams - the lower team id wins arbitrarily.
*/
CREATE VIEW [Sales].[vw_SalespersonPerformance]
AS
SELECT
    q.[SalesQuotaID],
    q.[SalespersonPersonID],
    per.[FullName]                          AS [SalespersonName],
    q.[SalesTerritoryID],
    q.[FiscalCalendarCode],
    q.[FiscalPeriodLabel],
    q.[PeriodStartDate],
    q.[PeriodEndDate],
    q.[QuotaCurrencyCode],
    q.[QuotaAmount],
    q.[StretchQuotaAmount],
    q.[AttainmentAmount],
    q.[AttainmentPercent],
    q.[AttainmentRefreshedWhen],
    q.[QuotaStatus],
    team.[SalesTeamID],
    team.[TeamCode],
    team.[TeamName],
    team.[RegionCode]                       AS [TeamRegionCode],
    ISNULL(acc.[AccruedCommission], 0)      AS [AccruedCommissionAmount],
    ISNULL(acc.[ClawedBackCommission], 0)   AS [ClawedBackCommissionAmount],
    ISNULL(acc.[AccrualCount], 0)           AS [AccrualCount],
    CASE WHEN q.[AttainmentRefreshedWhen] IS NULL THEN 1
         WHEN q.[AttainmentRefreshedWhen] < DATEADD(DAY, -2, SYSDATETIME()) THEN 1
         ELSE 0 END                         AS [IsAttainmentStale]
FROM [Sales].[SalesQuotas] AS q
    INNER JOIN [Application].[People] AS per
        ON per.[PersonID] = q.[SalespersonPersonID]
    OUTER APPLY
    (
        SELECT TOP (1)
            t.[SalesTeamID],
            t.[TeamCode],
            t.[TeamName],
            t.[RegionCode]
        FROM [Application].[SalesTeamMembers] AS m
            INNER JOIN [Application].[SalesTeams] AS t
                ON t.[SalesTeamID] = m.[SalesTeamID]
        WHERE m.[PersonID] = q.[SalespersonPersonID]
            AND m.[IsPrimaryAssignment] = 1
            AND m.[ValidFrom] <= q.[PeriodEndDate]
            AND (m.[ValidTo] IS NULL OR m.[ValidTo] > q.[PeriodEndDate])
        ORDER BY t.[SalesTeamID] ASC
    ) AS team
    OUTER APPLY
    (
        SELECT
            SUM(CASE WHEN ca.[AccrualStatus] <> N'REVERSED' THEN ca.[CommissionAmount] ELSE 0 END) AS [AccruedCommission],
            SUM(CASE WHEN ca.[AccrualStatus] = N'REVERSED' THEN ca.[CommissionAmount] ELSE 0 END)  AS [ClawedBackCommission],
            COUNT(*)                                                                                AS [AccrualCount]
        FROM [Sales].[CommissionAccruals] AS ca
        WHERE ca.[SalespersonPersonID] = q.[SalespersonPersonID]
            AND ca.[AccrualDate] BETWEEN q.[PeriodStartDate] AND q.[PeriodEndDate]
    ) AS acc;
GO
