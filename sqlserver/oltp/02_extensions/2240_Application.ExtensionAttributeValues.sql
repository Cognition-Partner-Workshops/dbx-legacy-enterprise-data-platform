/*
    Application.ExtensionAttributeValues

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2240 - after 2230
    Depends on    : Application.ExtensionAttributeDefinitions, Application.People
    Called by     : custom field screens, customer 360 extracts

    LEGACY PATTERN - entity-attribute-value, the value half.

    EntityKeyValue is the target row's primary key rendered as text, so there
    is no foreign key to anything and orphans accumulate whenever a customer
    or order is purged. ValueText holds every data type; ValueNumeric and
    ValueDate are opportunistic copies populated by the screen when it can
    parse the input, and reports use them when they are there and cast
    ValueText when they are not.
*/
CREATE TABLE [Application].[ExtensionAttributeValues] (
    [ExtensionAttributeValueID] BIGINT      IDENTITY (1, 1) NOT NULL,
    [ExtensionAttributeID]  INT             NOT NULL,
    [EntityTypeCode]        NVARCHAR (30)   NOT NULL,
    [EntityKeyValue]        NVARCHAR (60)   NOT NULL,
    [ValueSequence]         SMALLINT        CONSTRAINT [DF_Application_ExtensionAttributeValues_ValueSequence] DEFAULT (1) NOT NULL,
    [ValueText]             NVARCHAR (MAX)  NULL,
    [ValueNumeric]          DECIMAL (18, 4) NULL,
    [ValueDate]             DATE            NULL,
    [SourceApplication]     NVARCHAR (30)   NULL,
    [IsOrphanSuspected]     BIT             CONSTRAINT [DF_Application_ExtensionAttributeValues_IsOrphanSuspected] DEFAULT (0) NOT NULL,
    [LastEditedBy]          INT             NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Application_ExtensionAttributeValues_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Application_ExtensionAttributeValues] PRIMARY KEY CLUSTERED ([ExtensionAttributeValueID] ASC),
    CONSTRAINT [UQ_Application_ExtensionAttributeValues_Entity] UNIQUE ([ExtensionAttributeID], [EntityKeyValue], [ValueSequence]),
    CONSTRAINT [FK_Application_ExtensionAttributeValues_Definitions] FOREIGN KEY ([ExtensionAttributeID]) REFERENCES [Application].[ExtensionAttributeDefinitions] ([ExtensionAttributeID]),
    CONSTRAINT [FK_Application_ExtensionAttributeValues_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Application_ExtensionAttributeValues_Entity]
    ON [Application].[ExtensionAttributeValues] ([EntityTypeCode] ASC, [EntityKeyValue] ASC)
    INCLUDE ([ExtensionAttributeID], [ValueText]);
GO

CREATE NONCLUSTERED INDEX [IX_Application_ExtensionAttributeValues_Changed]
    ON [Application].[ExtensionAttributeValues] ([LastEditedWhen] ASC)
    INCLUDE ([EntityTypeCode], [EntityKeyValue]);
GO
