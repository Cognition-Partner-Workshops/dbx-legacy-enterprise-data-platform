/*
    Sales.usp_RecalculateCommissionAccruals

    Catalog entry : sqlserver_oltp.procedures - Sales.RecalculateCommissionAccruals
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6020 - after 6010
    Depends on    : Sales.CommissionAccruals, Sales.CommissionPlans, Sales.SalesQuotas,
                    Sales.ufn_CommissionRate, Application.SalesTeamMembers, Sales.Invoices
    Called by     : month-end commission run

    Rebuilds accruals for one fiscal period. Existing accruals are reversed by
    inserting clawback rows rather than being updated, then fresh accruals are
    written, so the table grows by roughly the invoice count each time the run
    is repeated - which it is, most months, because sales query the numbers.

    Region drives the basis: NA accrues on invoice net, EU accrues on net less
    freight, APAC accrues on cash received and is therefore always a period
    behind.
*/
CREATE PROCEDURE [Sales].[usp_RecalculateCommissionAccruals]
    @FiscalPeriodLabel  NVARCHAR (20),
    @RegionCode         NCHAR (4) = NULL,
    @RunByPersonID      INT,
    @BatchID            BIGINT = NULL,
    @AccrualsWritten    INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PeriodStart    DATE;
    DECLARE @PeriodEnd      DATE;

    SELECT
        @PeriodStart = MIN(q.[PeriodStartDate]),
        @PeriodEnd = MAX(q.[PeriodEndDate])
    FROM [Sales].[SalesQuotas] AS q
    WHERE q.[FiscalPeriodLabel] = @FiscalPeriodLabel;

    IF @PeriodStart IS NULL
    BEGIN
        RAISERROR (N'No quota rows define fiscal period %s.', 16, 1, @FiscalPeriodLabel);
        RETURN;
    END

    SET @AccrualsWritten = 0;

    BEGIN TRANSACTION;

    -- Reverse what is already there. Clawback rows carry the negative of the
    -- original amount and point back at it.
    INSERT INTO [Sales].[CommissionAccruals]
    (
        [SalespersonPersonID], [CommissionPlanID], [InvoiceID], [InvoiceLineID],
        [AccrualDate], [FiscalPeriodLabel], [BasisAmount], [BasisCurrencyCode],
        [ExchangeRateToUSD], [CommissionRatePercent], [CommissionAmount],
        [AccrualStatus], [AccrualHistoryNote], [ClawbackOfAccrualID], [LastEditedBy]
    )
    SELECT
        ca.[SalespersonPersonID],
        ca.[CommissionPlanID],
        ca.[InvoiceID],
        ca.[InvoiceLineID],
        ca.[AccrualDate],
        ca.[FiscalPeriodLabel],
        -ca.[BasisAmount],
        ca.[BasisCurrencyCode],
        ca.[ExchangeRateToUSD],
        ca.[CommissionRatePercent],
        -ca.[CommissionAmount],
        N'REVERSED',
        N'Reversed by period recalculation ' + @FiscalPeriodLabel,
        ca.[CommissionAccrualID],
        @RunByPersonID
    FROM [Sales].[CommissionAccruals] AS ca
    WHERE ca.[FiscalPeriodLabel] = @FiscalPeriodLabel
        AND ca.[AccrualStatus] = N'ACCRUED'
        AND ca.[ClawbackOfAccrualID] IS NULL;

    UPDATE ca
    SET ca.[AccrualStatus] = N'REVERSED',
        ca.[LastEditedBy] = @RunByPersonID,
        ca.[LastEditedWhen] = SYSDATETIME()
    FROM [Sales].[CommissionAccruals] AS ca
    WHERE ca.[FiscalPeriodLabel] = @FiscalPeriodLabel
        AND ca.[AccrualStatus] = N'ACCRUED'
        AND ca.[ClawbackOfAccrualID] IS NULL;

    INSERT INTO [Sales].[CommissionAccruals]
    (
        [SalespersonPersonID], [CommissionPlanID], [InvoiceID], [AccrualDate],
        [FiscalPeriodLabel], [BasisAmount], [BasisCurrencyCode], [ExchangeRateToUSD],
        [CommissionRatePercent], [CommissionAmount], [AccrualStatus], [LastEditedBy]
    )
    SELECT
        i.[SalespersonPersonID],
        m.[CommissionPlanID],
        i.[InvoiceID],
        i.[InvoiceDate],
        @FiscalPeriodLabel,
        CASE cp.[CommissionBasis]
            WHEN N'INVOICENET' THEN i.[InvoiceTotalExTax]
            WHEN N'NETLESSFREIGHT' THEN i.[InvoiceTotalExTax]
            ELSE i.[InvoiceTotalExTax]
        END,
        ISNULL(i.[CurrencyCode], N'USD'),
        i.[ExchangeRateToUsd],
        [Sales].[ufn_CommissionRate](m.[CommissionPlanID], q.[AttainmentPercent]),
        ROUND(i.[InvoiceTotalExTax]
              * [Sales].[ufn_CommissionRate](m.[CommissionPlanID], q.[AttainmentPercent])
              / 100.0
              * m.[QuotaSharePercent] / 100.0, 2),
        N'ACCRUED',
        @RunByPersonID
    FROM [Sales].[Invoices] AS i
        INNER JOIN [Application].[SalesTeamMembers] AS m
            ON m.[PersonID] = i.[SalespersonPersonID]
                AND m.[ValidFrom] <= i.[InvoiceDate]
                AND (m.[ValidTo] IS NULL OR m.[ValidTo] > i.[InvoiceDate])
                AND m.[CommissionPlanID] IS NOT NULL
        INNER JOIN [Sales].[CommissionPlans] AS cp
            ON cp.[CommissionPlanID] = m.[CommissionPlanID]
        LEFT JOIN [Sales].[SalesQuotas] AS q
            ON q.[SalespersonPersonID] = i.[SalespersonPersonID]
                AND q.[FiscalPeriodLabel] = @FiscalPeriodLabel
    WHERE i.[InvoiceDate] BETWEEN @PeriodStart AND @PeriodEnd
        AND (@RegionCode IS NULL OR cp.[RegionCode] = @RegionCode)
        AND ISNULL(i.[SettlementStatus], N'OPEN') <> N'WRITTENOFF'
        AND (cp.[RegionCode] <> N'APAC' OR i.[SettlementStatus] = N'PAID');

    SET @AccrualsWritten = @@ROWCOUNT;

    COMMIT TRANSACTION;
END
GO
