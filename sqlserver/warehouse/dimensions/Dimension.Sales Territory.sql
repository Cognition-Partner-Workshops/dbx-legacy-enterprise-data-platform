/*
    Object        : [Dimension].[Sales Territory]  (SCD Type 1 with an annual realignment stamp)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Region.sql
    Depends on    : Sequences.SalesTerritoryKey, Dimension.Region
    Called by     : Integration.usp_MigrateStagedSalesTerritoryData,
                    Integration.usp_LoadBridgeEmployeeTerritory

    Territories are realigned every January. The dimension is Type 1 - the current
    alignment overwrites - and the historical alignment lives only in
    [Dimension].[Employee Territory Bridge], which is time-bound. This is a known
    inconsistency: territory-level revenue restates when the alignment changes,
    and every year the sales operations team reissue the prior-year numbers.
    [Alignment Year] records which realignment the current row belongs to.

    Territory geography is expressed differently per region because the source
    systems are different: NA by state and ZIP prefix, EU by country and NUTS
    code, APAC by country and city list.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Sales Territory', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Sales Territory]
    (
        [Sales Territory Key]           INT             CONSTRAINT [DF_Dimension_Sales_Territory_Key] DEFAULT (NEXT VALUE FOR [Sequences].[SalesTerritoryKey]) NOT NULL,
        [WWI Sales Territory ID]        INT             NULL,
        [Sales Territory Code]          NVARCHAR(20)    NOT NULL,
        [Sales Territory]               NVARCHAR(60)    NOT NULL,
        [Region Key]                    INT             NULL,
        [Region Code]                   NVARCHAR(10)    NOT NULL,
        [Alignment Year]                SMALLINT        NULL,
        [Parent Territory Code]         NVARCHAR(20)    NULL,
        [Territory Level Code]          NVARCHAR(10)    NULL,   -- AREA / DISTRICT / TERRITORY / MICRO
        [Territory Manager Employee No] NVARCHAR(20)    NULL,
        [Sales Office Code]             NVARCHAR(20)    NULL,
        [Coverage Model Code]           NVARCHAR(10)    NULL,   -- FIELD / INSIDE / PARTNER / HYBRID

        [NA State Province List]        NVARCHAR(400)   NULL,   -- comma-delimited; parsed by the bridge load
        [NA ZIP Prefix List]            NVARCHAR(400)   NULL,
        [EU Country List]               NVARCHAR(400)   NULL,
        [EU NUTS Code List]             NVARCHAR(400)   NULL,
        [APAC Country List]             NVARCHAR(400)   NULL,
        [APAC City List]                NVARCHAR(800)   NULL,

        [Annual Target Amount]          DECIMAL(18, 2)  NULL,
        [Target Currency Code]          NVARCHAR(3)     NULL,
        [Target Fiscal Year]            SMALLINT        NULL,
        [Is Active]                     BIT             NULL,
        [Retired On]                    DATE            NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Sales_Territory] PRIMARY KEY CLUSTERED ([Sales Territory Key] ASC),
        CONSTRAINT [UQ_Dimension_Sales_Territory_Code] UNIQUE ([Sales Territory Code])
    );
END;
GO
