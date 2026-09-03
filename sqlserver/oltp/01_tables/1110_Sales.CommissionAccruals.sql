/*
    Sales.CommissionAccruals

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1110 - after Sales.CommissionPlans
    Depends on    : Sales.CommissionPlans, Sales.Invoices, Sales.InvoiceLines,
                    Application.People
    Called by     : Sales.usp_RecalculateCommissionAccruals, payroll interface
                    (Integration.OutboundInterfaceQueue)

    Legacy pattern (overloaded status): AccrualStatus is written by two
    different processes with two different vocabularies that share one column.
    The nightly accrual run writes 'ACCRUED', 'ADJUSTED' and 'REVERSED'; the
    monthly payroll export writes 'EXPORTED', 'PAID' and 'HELD' into the same
    column. A row that has been paid therefore loses the accrual state it had,
    and the reporting team reconstructs it from AccrualHistoryNote.
*/
CREATE TABLE [Sales].[CommissionAccruals] (
    [CommissionAccrualID]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [SalespersonPersonID]   INT             NOT NULL,
    [CommissionPlanID]      INT             NOT NULL,
    [InvoiceID]             INT             NOT NULL,
    [InvoiceLineID]         INT             NULL,
    [AccrualDate]           DATE            NOT NULL,
    [FiscalPeriodLabel]     NVARCHAR (20)   NOT NULL,
    [BasisAmount]           DECIMAL (18, 2) NOT NULL,
    [BasisCurrencyCode]     NCHAR (3)       NOT NULL,
    [ExchangeRateToUSD]     DECIMAL (18, 8) NULL,
    [CommissionRatePercent] DECIMAL (5, 2)  NOT NULL,
    [CommissionAmount]      DECIMAL (18, 2) NOT NULL,
    [CommissionAmountUSD]   AS (CASE WHEN [ExchangeRateToUSD] IS NULL THEN NULL
                                     ELSE CONVERT(DECIMAL (18, 2), [CommissionAmount] * [ExchangeRateToUSD]) END),
    [AccrualStatus]         NVARCHAR (12)   CONSTRAINT [DF_Sales_CommissionAccruals_AccrualStatus] DEFAULT (N'ACCRUED') NOT NULL,
    [AccrualHistoryNote]    NVARCHAR (400)  NULL,
    [ClawbackOfAccrualID]   BIGINT          NULL,
    [PayrollBatchReference] NVARCHAR (30)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_CommissionAccruals_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_CommissionAccruals] PRIMARY KEY CLUSTERED ([CommissionAccrualID] ASC),
    CONSTRAINT [CK_Sales_CommissionAccruals_Status] CHECK ([AccrualStatus] IN (N'ACCRUED', N'ADJUSTED', N'REVERSED', N'PENDINGCASH', N'EXPORTED', N'PAID', N'HELD')),
    CONSTRAINT [CK_Sales_CommissionAccruals_Rate] CHECK ([CommissionRatePercent] BETWEEN 0 AND 50),
    CONSTRAINT [FK_Sales_CommissionAccruals_Plans] FOREIGN KEY ([CommissionPlanID]) REFERENCES [Sales].[CommissionPlans] ([CommissionPlanID]),
    CONSTRAINT [FK_Sales_CommissionAccruals_Invoices] FOREIGN KEY ([InvoiceID]) REFERENCES [Sales].[Invoices] ([InvoiceID]),
    CONSTRAINT [FK_Sales_CommissionAccruals_InvoiceLines] FOREIGN KEY ([InvoiceLineID]) REFERENCES [Sales].[InvoiceLines] ([InvoiceLineID]),
    CONSTRAINT [FK_Sales_CommissionAccruals_Clawback] FOREIGN KEY ([ClawbackOfAccrualID]) REFERENCES [Sales].[CommissionAccruals] ([CommissionAccrualID]),
    CONSTRAINT [FK_Sales_CommissionAccruals_Salesperson] FOREIGN KEY ([SalespersonPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_CommissionAccruals_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_CommissionAccruals_Person_Period]
    ON [Sales].[CommissionAccruals] ([SalespersonPersonID] ASC, [FiscalPeriodLabel] ASC)
    INCLUDE ([CommissionAmount], [AccrualStatus], [InvoiceID]);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_CommissionAccruals_Unexported]
    ON [Sales].[CommissionAccruals] ([AccrualDate] ASC)
    INCLUDE ([SalespersonPersonID], [CommissionAmount])
    WHERE [AccrualStatus] IN (N'ACCRUED', N'ADJUSTED', N'PENDINGCASH');
GO
