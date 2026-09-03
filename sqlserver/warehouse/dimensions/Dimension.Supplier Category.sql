/*
    Object        : [Dimension].[Supplier Category]  (SCD Type 1)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.SupplierCategoryKey
    Called by     : Integration.usp_MigrateStagedSupplierDataV2 (the Oracle supplier
                    extract carries the category reference set on the same feed)

    Procurement categories drive approval routing and spend analytics. The code set
    is the UNSPSC segment the 2006 project adopted, overlaid with the older
    two-letter purchasing codes that the AP clerks still key by hand, which is why
    both are present and why [Legacy Purchasing Code] is nullable but indexed.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Supplier Category', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Supplier Category]
    (
        [Supplier Category Key]         INT             CONSTRAINT [DF_Dimension_Supplier_Category_Key] DEFAULT (NEXT VALUE FOR [Sequences].[SupplierCategoryKey]) NOT NULL,
        [WWI Supplier Category ID]      INT             NULL,
        [Supplier Category Code]        NVARCHAR(10)    NOT NULL,
        [Supplier Category]             NVARCHAR(60)    NOT NULL,
        [Spend Category Group]          NVARCHAR(60)    NULL,
        [UNSPSC Segment Code]           NVARCHAR(10)    NULL,
        [Legacy Purchasing Code]        NVARCHAR(2)     NULL,

        [Is Direct Spend]               BIT             NULL,
        [Is Indirect Spend]             BIT             NULL,
        [Is Capital Spend]              BIT             NULL,
        [Requires Contract]             BIT             NULL,
        [Requires Quality Audit]        BIT             NULL,
        [Approval Threshold Amount]     DECIMAL(18, 2)  NULL,
        [Approval Threshold Currency]   NVARCHAR(3)     NULL,
        [Approval Route Code]           NVARCHAR(20)    NULL,
        [Default GL Account Code]       NVARCHAR(20)    NULL,
        [Default Cost Center Code]      NVARCHAR(20)    NULL,

        [NA Sourcing Team Code]         NVARCHAR(10)    NULL,
        [EU Framework Agreement Ref]    NVARCHAR(30)    NULL,
        [APAC Local Content Required]   BIT             NULL,

        [Source System Code]            NVARCHAR(20)    NULL,
        [Is Active]                     BIT             NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Supplier_Category] PRIMARY KEY CLUSTERED ([Supplier Category Key] ASC),
        CONSTRAINT [UQ_Dimension_Supplier_Category_Code] UNIQUE ([Supplier Category Code])
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Supplier_Category_Legacy]
        ON [Dimension].[Supplier Category] ([Legacy Purchasing Code] ASC);
END;
GO
