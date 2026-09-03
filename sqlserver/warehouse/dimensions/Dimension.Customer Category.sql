/*
    Object        : [Dimension].[Customer Category]  (SCD Type 1 - overwrite in place)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.CustomerCategoryKey
    Called by     : Integration.usp_MigrateStagedCustomerCategoryData

    Source is Oracle WWI_MDM.CUST_CLASSIFICATION, which is itself a merge of three
    classification schemes that were never reconciled:

        - the 1998 SIC-derived code    ([Legacy Classification Code], 4 characters)
        - the 2005 internal scheme     ([Category Code], 3 characters)
        - the 2013 regional overlays   ([NA Segment Code], [EU Sector Code],
                                        [APAC Trade Code])

    The load procedure keeps all three. Nothing downstream agrees on which to use:
    the finance cube uses the 1998 code, the sales cube uses the 2005 code and the
    regional dashboards use the overlay for their own region only.

    Type 1 throughout: a reclassification is applied to history, which is why the
    2016 margin restatement happened. The behaviour is deliberate and documented.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Customer Category', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Customer Category]
    (
        [Customer Category Key]         INT             CONSTRAINT [DF_Dimension_Customer_Category_Key] DEFAULT (NEXT VALUE FOR [Sequences].[CustomerCategoryKey]) NOT NULL,
        [WWI Customer Category ID]      INT             NULL,
        [Category Code]                 NVARCHAR(3)     NOT NULL,
        [Customer Category]             NVARCHAR(60)    NOT NULL,
        [Category Group]                NVARCHAR(60)    NULL,
        [Legacy Classification Code]    NVARCHAR(4)     NULL,
        [Legacy Classification Name]    NVARCHAR(60)    NULL,

        [NA Segment Code]               NVARCHAR(10)    NULL,   -- NAICS-derived, maintained in Chicago
        [EU Sector Code]                NVARCHAR(10)    NULL,   -- NACE-derived, maintained in Rotterdam
        [APAC Trade Code]               NVARCHAR(10)    NULL,   -- ANZSIC / local, maintained in Singapore

        [Is Retail]                     BIT             NULL,
        [Is Wholesale]                  BIT             NULL,
        [Is Internal]                   BIT             NULL,
        [Is Government]                 BIT             NULL,
        [Default Credit Limit Amount]   DECIMAL(18, 2)  NULL,
        [Default Payment Terms Code]    NVARCHAR(10)    NULL,
        [Default Discount Percentage]   DECIMAL(9, 4)   NULL,
        [Requires Purchase Order]       BIT             NULL,
        [Tax Exempt By Default]         BIT             NULL,

        [Source System Code]            NVARCHAR(20)    NULL,
        [Source Effective From]         DATE            NULL,
        [Is Active]                     BIT             NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Customer_Category] PRIMARY KEY CLUSTERED ([Customer Category Key] ASC),
        CONSTRAINT [UQ_Dimension_Customer_Category_Code] UNIQUE ([Category Code])
    );
END;
GO
