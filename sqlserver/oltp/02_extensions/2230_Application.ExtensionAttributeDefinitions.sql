/*
    Application.ExtensionAttributeDefinitions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2230 - after 2220
    Depends on    : Application.People
    Called by     : Application.ExtensionAttributeValues, custom field screens

    LEGACY PATTERN - entity-attribute-value.

    When the change board froze schema changes in 2012, the "custom fields"
    feature was delivered as a generic attribute store instead. This is the
    definition half: what attributes exist, against which entity, and what
    they are notionally typed as. The typing is advisory only - the value half
    stores everything as text and the application casts on read, so a
    definition changed from DATE to TEXT leaves unparseable history behind.
*/
CREATE TABLE [Application].[ExtensionAttributeDefinitions] (
    [ExtensionAttributeID]  INT             IDENTITY (1, 1) NOT NULL,
    [EntityTypeCode]        NVARCHAR (30)   NOT NULL,
    [AttributeCode]         NVARCHAR (40)   NOT NULL,
    [AttributeName]         NVARCHAR (80)   NOT NULL,
    [DataTypeCode]          NVARCHAR (10)   NOT NULL,
    [AllowedValueList]      NVARCHAR (MAX)  NULL,
    [DefaultValueText]      NVARCHAR (400)  NULL,
    [IsMandatory]           BIT             CONSTRAINT [DF_Application_ExtensionAttributeDefinitions_IsMandatory] DEFAULT (0) NOT NULL,
    [IsMultiValued]         BIT             CONSTRAINT [DF_Application_ExtensionAttributeDefinitions_IsMultiValued] DEFAULT (0) NOT NULL,
    [RegionCode]            NCHAR (4)       NULL,
    [DisplayOrder]          SMALLINT        NULL,
    [OwningDepartment]      NVARCHAR (40)   NULL,
    [IsRetired]             BIT             CONSTRAINT [DF_Application_ExtensionAttributeDefinitions_IsRetired] DEFAULT (0) NOT NULL,
    [CreatedWhen]           DATETIME2 (7)   CONSTRAINT [DF_Application_ExtensionAttributeDefinitions_CreatedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Application_ExtensionAttributeDefinitions_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Application_ExtensionAttributeDefinitions] PRIMARY KEY CLUSTERED ([ExtensionAttributeID] ASC),
    CONSTRAINT [UQ_Application_ExtensionAttributeDefinitions_Code] UNIQUE ([EntityTypeCode], [AttributeCode]),
    CONSTRAINT [CK_Application_ExtensionAttributeDefinitions_Entity] CHECK ([EntityTypeCode] IN (N'CUSTOMER', N'ORDER', N'STOCKITEM', N'SHIPMENT', N'SUPPLIER', N'PERSON')),
    CONSTRAINT [CK_Application_ExtensionAttributeDefinitions_DataType] CHECK ([DataTypeCode] IN (N'TEXT', N'NUMBER', N'DATE', N'BOOLEAN', N'LIST')),
    CONSTRAINT [FK_Application_ExtensionAttributeDefinitions_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
