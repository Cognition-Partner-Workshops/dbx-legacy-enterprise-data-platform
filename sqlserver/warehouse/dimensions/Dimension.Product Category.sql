/*
    Object        : [Dimension].[Product Category]  (SCD Type 1, flattened merchandising levels)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.ProductCategoryKey
    Called by     : Integration.usp_MigrateStagedProductData

    The flattened, fixed-depth view of the merchandising hierarchy: department ->
    class -> subclass -> category. It exists alongside the ragged recursive
    [Dimension].[Product Hierarchy] because the 2009 cube cannot consume a parent
    -child structure and the merchandising team refuse to give up the ragged one.
    Both are loaded from the same Oracle extract and can disagree for a day when
    the extract lands after the cube process; that is a known operational quirk.

    Type 1: recategorisation restates history, which merchandising want and
    finance object to every year.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Product Category', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Product Category]
    (
        [Product Category Key]          INT             CONSTRAINT [DF_Dimension_Product_Category_Key] DEFAULT (NEXT VALUE FOR [Sequences].[ProductCategoryKey]) NOT NULL,
        [WWI Product Category ID]       INT             NULL,
        [Category Code]                 NVARCHAR(20)    NOT NULL,
        [Product Category]              NVARCHAR(80)    NOT NULL,

        [Department Code]               NVARCHAR(10)    NULL,
        [Department Name]               NVARCHAR(60)    NULL,
        [Class Code]                    NVARCHAR(10)    NULL,
        [Class Name]                    NVARCHAR(60)    NULL,
        [Subclass Code]                 NVARCHAR(10)    NULL,
        [Subclass Name]                 NVARCHAR(60)    NULL,
        [Category Path]                 NVARCHAR(300)   NULL,

        [Merchandising Manager]         NVARCHAR(60)    NULL,
        [Buying Team Code]              NVARCHAR(10)    NULL,
        [Target Margin Percentage]      DECIMAL(9, 4)   NULL,
        [Markdown Policy Code]          NVARCHAR(10)    NULL,   -- NONE / SEASONAL / CLEAR / EOL
        [Seasonality Code]              NVARCHAR(10)    NULL,   -- YR / SPR / SUM / AUT / WIN / HOL
        [Is Own Brand]                  BIT             NULL,
        [Is Perishable]                 BIT             NULL,
        [Is Age Restricted]             BIT             NULL,
        [Minimum Age]                   SMALLINT        NULL,

        /* Tax categorisation is per region and does not map one-to-one. */
        [NA Tax Category Code]          NVARCHAR(10)    NULL,   -- TPP / FOOD / RX / SVC / EXM
        [EU VAT Rate Category]          NVARCHAR(10)    NULL,   -- STD / RED / SUP / ZER / EXE
        [APAC GST Category Code]        NVARCHAR(10)    NULL,   -- TAX / ZRL / EXM / OOS
        [Harmonised System Code]        NVARCHAR(10)    NULL,

        [Source System Code]            NVARCHAR(20)    NULL,
        [Is Active]                     BIT             NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Product_Category] PRIMARY KEY CLUSTERED ([Product Category Key] ASC),
        CONSTRAINT [UQ_Dimension_Product_Category_Code] UNIQUE ([Category Code])
    );
END;
GO
