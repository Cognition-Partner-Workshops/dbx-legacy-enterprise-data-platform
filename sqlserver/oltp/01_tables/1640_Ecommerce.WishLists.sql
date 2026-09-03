/*
    Ecommerce.WishLists

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1640 - after Ecommerce.WebSessions
    Depends on    : Sales.Customers, Application.People
    Called by     : Ecommerce.WishListLines

    Saved lists. Business customers use them as reorder templates, which is why
    a list can be marked as a template and shared across the customer's
    contacts by a share token; consumers use them as gift registries with an
    event date. Both behaviours live in one table.
*/
CREATE TABLE [Ecommerce].[WishLists] (
    [WishListID]            INT             IDENTITY (1, 1) NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [OwnerPersonID]         INT             NULL,
    [WishListName]          NVARCHAR (80)   NOT NULL,
    [WishListType]          NVARCHAR (12)   CONSTRAINT [DF_Ecommerce_WishLists_WishListType] DEFAULT (N'PERSONAL') NOT NULL,
    [IsReorderTemplate]     BIT             CONSTRAINT [DF_Ecommerce_WishLists_IsReorderTemplate] DEFAULT (0) NOT NULL,
    [IsShared]              BIT             CONSTRAINT [DF_Ecommerce_WishLists_IsShared] DEFAULT (0) NOT NULL,
    [ShareToken]            NVARCHAR (40)   NULL,
    [EventDate]             DATE            NULL,
    [CreatedWhen]           DATETIME2 (7)   CONSTRAINT [DF_Ecommerce_WishLists_CreatedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [LastViewedWhen]        DATETIME2 (7)   NULL,
    [ItemCount]             SMALLINT        CONSTRAINT [DF_Ecommerce_WishLists_ItemCount] DEFAULT (0) NOT NULL,
    [IsArchived]            BIT             CONSTRAINT [DF_Ecommerce_WishLists_IsArchived] DEFAULT (0) NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Ecommerce_WishLists_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Ecommerce_WishLists] PRIMARY KEY CLUSTERED ([WishListID] ASC),
    CONSTRAINT [UQ_Ecommerce_WishLists_Name] UNIQUE ([CustomerID], [WishListName]),
    CONSTRAINT [CK_Ecommerce_WishLists_Type] CHECK ([WishListType] IN (N'PERSONAL', N'REGISTRY', N'TEMPLATE', N'PROJECT')),
    CONSTRAINT [CK_Ecommerce_WishLists_Share] CHECK ([IsShared] = 0 OR [ShareToken] IS NOT NULL),
    CONSTRAINT [FK_Ecommerce_WishLists_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Ecommerce_WishLists_Application_People] FOREIGN KEY ([OwnerPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO
