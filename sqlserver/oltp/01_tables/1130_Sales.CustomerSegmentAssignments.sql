/*
    Sales.CustomerSegmentAssignments

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1130 - after Sales.CustomerSegments
    Depends on    : Sales.CustomerSegments, Sales.Customers, Application.People
    Called by     : Sales.usp_AssignCustomerSegments, Sales.vw_CustomerSegmentCurrent

    Hand-rolled type 2 history. There is no system-versioning here: the
    assignment procedure closes the previous row by stamping ValidToDate and
    IsCurrentRow = 0 and inserts a new one. The pair is not enforced by a
    constraint, so rows with IsCurrentRow = 1 and a non-null ValidToDate do
    exist and every consumer has its own opinion about which to trust.
*/
CREATE TABLE [Sales].[CustomerSegmentAssignments] (
    [CustomerSegmentAssignmentID] BIGINT        IDENTITY (1, 1) NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [CustomerSegmentID]     INT             NOT NULL,
    [ValidFromDate]         DATE            NOT NULL,
    [ValidToDate]           DATE            NULL,
    [IsCurrentRow]          BIT             CONSTRAINT [DF_Sales_CustomerSegmentAssignments_IsCurrentRow] DEFAULT (1) NOT NULL,
    [AssignmentReason]      NVARCHAR (200)  NULL,
    [ScoreValue]            DECIMAL (9, 4)  NULL,
    [ConsentCapturedWhen]   DATETIME2 (7)   NULL,
    [ConsentSourceCode]     NVARCHAR (16)   NULL,
    [RetentionExpiryDate]   DATE            NULL,
    [AssignedByProcess]     NVARCHAR (40)   NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_CustomerSegmentAssignments_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_CustomerSegmentAssignments] PRIMARY KEY CLUSTERED ([CustomerSegmentAssignmentID] ASC),
    CONSTRAINT [CK_Sales_CustomerSegmentAssignments_Validity] CHECK ([ValidToDate] IS NULL OR [ValidToDate] >= [ValidFromDate]),
    CONSTRAINT [FK_Sales_CustomerSegmentAssignments_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Sales_CustomerSegmentAssignments_Segments] FOREIGN KEY ([CustomerSegmentID]) REFERENCES [Sales].[CustomerSegments] ([CustomerSegmentID]),
    CONSTRAINT [FK_Sales_CustomerSegmentAssignments_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Sales_CustomerSegmentAssignments_Current]
    ON [Sales].[CustomerSegmentAssignments] ([CustomerID] ASC, [CustomerSegmentID] ASC)
    WHERE [IsCurrentRow] = 1;
GO

CREATE NONCLUSTERED INDEX [IX_Sales_CustomerSegmentAssignments_Expiry]
    ON [Sales].[CustomerSegmentAssignments] ([RetentionExpiryDate] ASC)
    INCLUDE ([CustomerID], [CustomerSegmentID])
    WHERE [RetentionExpiryDate] IS NOT NULL;
GO
