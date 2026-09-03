/*
    Returns.ReturnAuthorizations

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1410 - after Returns.ReturnReasons
    Depends on    : Sales.Customers, Sales.Invoices, Shipping.Carriers, Application.People
    Called by     : Returns.ReturnLines, Returns.usp_AuthorizeReturn, Returns.vw_ReturnExtract

    RMA header. AuthorizationStatus is one of this estate's overloaded status
    columns: it carries both the commercial state of the authorisation
    (REQUESTED / APPROVED / DECLINED) and the physical state of the goods
    (AWAITINGGOODS / RECEIVED / CLOSED), because the goods-in screen and the
    customer-service screen were written by different teams five years apart
    and both wrote to the same column.
*/
CREATE TABLE [Returns].[ReturnAuthorizations] (
    [ReturnAuthorizationID] INT             CONSTRAINT [DF_Returns_ReturnAuthorizations_ReturnAuthorizationID] DEFAULT (NEXT VALUE FOR [Sequences].[ReturnAuthorizationID]) NOT NULL,
    [RmaNumber]             NVARCHAR (20)   NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [OriginalInvoiceID]     INT             NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [RequestedWhen]         DATETIME2 (7)   CONSTRAINT [DF_Returns_ReturnAuthorizations_RequestedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [RequestChannel]        NVARCHAR (12)   NOT NULL,
    [AuthorizationStatus]   NVARCHAR (16)   CONSTRAINT [DF_Returns_ReturnAuthorizations_AuthorizationStatus] DEFAULT (N'REQUESTED') NOT NULL,
    [AuthorizedWhen]        DATETIME2 (7)   NULL,
    [AuthorizedByPersonID]  INT             NULL,
    [ExpiresOnDate]         DATE            NULL,
    [ReturnCarrierID]       INT             NULL,
    [ReturnTrackingNumber]  NVARCHAR (40)   NULL,
    [ReceivedAtSiteID]      INT             NULL,
    [GoodsReceivedWhen]     DATETIME2 (7)   NULL,
    [IsCoolingOffPeriod]    BIT             CONSTRAINT [DF_Returns_ReturnAuthorizations_IsCoolingOffPeriod] DEFAULT (0) NOT NULL,
    [CustomerNarrative]     NVARCHAR (600)  NULL,
    [InternalNote]          NVARCHAR (600)  NULL,
    [DeclineReason]         NVARCHAR (200)  NULL,
    [TotalExpectedCredit]   DECIMAL (18, 2) NULL,
    [CreditCurrencyCode]    NCHAR (3)       NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Returns_ReturnAuthorizations_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Returns_ReturnAuthorizations] PRIMARY KEY CLUSTERED ([ReturnAuthorizationID] ASC),
    CONSTRAINT [UQ_Returns_ReturnAuthorizations_Rma] UNIQUE ([RmaNumber]),
    CONSTRAINT [CK_Returns_ReturnAuthorizations_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Returns_ReturnAuthorizations_Channel] CHECK ([RequestChannel] IN (N'CALLCENTRE', N'WEB', N'EDI', N'REP', N'STORE')),
    CONSTRAINT [CK_Returns_ReturnAuthorizations_Status] CHECK ([AuthorizationStatus] IN (N'REQUESTED', N'APPROVED', N'DECLINED', N'AWAITINGGOODS', N'RECEIVED', N'CLOSED', N'EXPIRED')),
    CONSTRAINT [CK_Returns_ReturnAuthorizations_Decline] CHECK ([AuthorizationStatus] <> N'DECLINED' OR [DeclineReason] IS NOT NULL),
    CONSTRAINT [FK_Returns_ReturnAuthorizations_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Returns_ReturnAuthorizations_Invoices] FOREIGN KEY ([OriginalInvoiceID]) REFERENCES [Sales].[Invoices] ([InvoiceID]),
    CONSTRAINT [FK_Returns_ReturnAuthorizations_Carriers] FOREIGN KEY ([ReturnCarrierID]) REFERENCES [Shipping].[Carriers] ([CarrierID]),
    CONSTRAINT [FK_Returns_ReturnAuthorizations_Sites] FOREIGN KEY ([ReceivedAtSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Returns_ReturnAuthorizations_AuthorizedBy] FOREIGN KEY ([AuthorizedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Returns_ReturnAuthorizations_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Returns_ReturnAuthorizations_Customer]
    ON [Returns].[ReturnAuthorizations] ([CustomerID] ASC, [RequestedWhen] DESC)
    INCLUDE ([AuthorizationStatus], [TotalExpectedCredit]);
GO

CREATE NONCLUSTERED INDEX [IX_Returns_ReturnAuthorizations_Outstanding]
    ON [Returns].[ReturnAuthorizations] ([ExpiresOnDate] ASC)
    INCLUDE ([RmaNumber], [CustomerID])
    WHERE [AuthorizationStatus] IN (N'APPROVED', N'AWAITINGGOODS');
GO
