/*
    Sales.CustomerCreditHolds

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2190 - after 2180
    Depends on    : Sales.Customers, Application.People
    Called by     : credit control screens, order entry

    History of credit limits and account-level holds. Sales.Customers keeps
    only the current position; this keeps every change, including the
    temporary limit increases that sales agree at quarter end and forget to
    reverse. TemporaryUntilDate exists precisely because of that, and nothing
    enforces it.
*/
CREATE TABLE [Sales].[CustomerCreditHolds] (
    [CustomerCreditHoldID]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [EventTypeCode]         NVARCHAR (12)   NOT NULL,
    [EffectiveFromWhen]     DATETIME2 (7)   CONSTRAINT [DF_Sales_CustomerCreditHolds_EffectiveFromWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [EffectiveToWhen]       DATETIME2 (7)   NULL,
    [PreviousCreditLimit]   DECIMAL (18, 2) NULL,
    [NewCreditLimit]        DECIMAL (18, 2) NULL,
    [CurrencyCode]          NCHAR (3)       NULL,
    [IsOnHold]              BIT             CONSTRAINT [DF_Sales_CustomerCreditHolds_IsOnHold] DEFAULT (0) NOT NULL,
    [HoldReasonCode]        NVARCHAR (10)   NULL,
    [TemporaryUntilDate]    DATE            NULL,
    [RequestedByPersonID]   INT             NULL,
    [ApprovedByPersonID]    INT             NULL,
    [ApprovalNarrative]     NVARCHAR (400)  NULL,
    [SourceSystem]          NVARCHAR (30)   NULL,
    CONSTRAINT [PK_Sales_CustomerCreditHolds] PRIMARY KEY CLUSTERED ([CustomerCreditHoldID] ASC),
    CONSTRAINT [CK_Sales_CustomerCreditHolds_Event] CHECK ([EventTypeCode] IN (N'LIMITSET', N'LIMITTEMP', N'HOLDON', N'HOLDOFF', N'REVIEW')),
    CONSTRAINT [CK_Sales_CustomerCreditHolds_Hold] CHECK ([IsOnHold] = 0 OR [HoldReasonCode] IS NOT NULL),
    CONSTRAINT [FK_Sales_CustomerCreditHolds_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Sales_CustomerCreditHolds_RequestedBy] FOREIGN KEY ([RequestedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_CustomerCreditHolds_ApprovedBy] FOREIGN KEY ([ApprovedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Sales_CustomerCreditHolds_Current]
    ON [Sales].[CustomerCreditHolds] ([CustomerID] ASC)
    WHERE [EffectiveToWhen] IS NULL;
GO
