/*
    Sales.OrderHolds

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2100 - after 2000
    Depends on    : Sales.Orders, Application.People
    Called by     : order release screen, Shipping.usp_CreateShipmentFromOrder

    Holds placed on an order. Several can be live at once (credit, stock,
    export control, fraud review) and every one of them must be released
    before despatch; the despatch procedure counts unreleased rows rather than
    reading a flag, because the flag on Sales.Orders was found to be wrong too
    often. The flag is still maintained by trigger for the order entry screen.
*/
CREATE TABLE [Sales].[OrderHolds] (
    [OrderHoldID]           BIGINT          IDENTITY (1, 1) NOT NULL,
    [OrderID]               INT             NOT NULL,
    [HoldTypeCode]          NVARCHAR (12)   NOT NULL,
    [HoldReasonCode]        NVARCHAR (10)   NOT NULL,
    [HoldNarrative]         NVARCHAR (400)  NULL,
    [PlacedWhen]            DATETIME2 (7)   CONSTRAINT [DF_Sales_OrderHolds_PlacedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [PlacedByPersonID]      INT             NOT NULL,
    [PlacedBySystem]        NVARCHAR (30)   NULL,
    [ReleasedWhen]          DATETIME2 (7)   NULL,
    [ReleasedByPersonID]    INT             NULL,
    [ReleaseNarrative]      NVARCHAR (400)  NULL,
    [AutoReleaseAfterWhen]  DATETIME2 (7)   NULL,
    [EscalationLevel]       TINYINT         CONSTRAINT [DF_Sales_OrderHolds_EscalationLevel] DEFAULT (1) NOT NULL,
    [IsBlockingDespatch]    BIT             CONSTRAINT [DF_Sales_OrderHolds_IsBlockingDespatch] DEFAULT (1) NOT NULL,
    CONSTRAINT [PK_Sales_OrderHolds] PRIMARY KEY CLUSTERED ([OrderHoldID] ASC),
    CONSTRAINT [CK_Sales_OrderHolds_Type] CHECK ([HoldTypeCode] IN (N'CREDIT', N'STOCK', N'FRAUD', N'EXPORT', N'PRICE', N'CUSTOMER', N'QUALITY')),
    CONSTRAINT [CK_Sales_OrderHolds_Release] CHECK ([ReleasedWhen] IS NULL OR [ReleasedWhen] >= [PlacedWhen]),
    CONSTRAINT [FK_Sales_OrderHolds_Orders] FOREIGN KEY ([OrderID]) REFERENCES [Sales].[Orders] ([OrderID]),
    CONSTRAINT [FK_Sales_OrderHolds_PlacedBy] FOREIGN KEY ([PlacedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_OrderHolds_ReleasedBy] FOREIGN KEY ([ReleasedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_OrderHolds_Open]
    ON [Sales].[OrderHolds] ([OrderID] ASC)
    INCLUDE ([HoldTypeCode], [PlacedWhen], [EscalationLevel])
    WHERE [ReleasedWhen] IS NULL;
GO
