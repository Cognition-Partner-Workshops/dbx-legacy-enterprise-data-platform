/*
    Object        : [Dimension].[Sales Channel]  (SCD Type 1)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Region.sql
    Depends on    : Sequences.SalesChannelKey
    Called by     : Integration.usp_MigrateStagedSalesChannelData

    Order-taking channels. The reference set is regional: NA runs field sales,
    inside sales, EDI and a web store; EU adds two national marketplaces and a
    telesales operation inherited with the 2008 acquisition; APAC is dominated by
    third-party marketplaces and a distributor network that reports sell-through
    weekly rather than per transaction, which is why [Reports Sell Through] and
    [Reporting Lag Days] exist and why APAC channel revenue lands late.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Sales Channel', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Sales Channel]
    (
        [Sales Channel Key]             INT             CONSTRAINT [DF_Dimension_Sales_Channel_Key] DEFAULT (NEXT VALUE FOR [Sequences].[SalesChannelKey]) NOT NULL,
        [WWI Sales Channel ID]          INT             NULL,
        [Sales Channel Code]            NVARCHAR(15)    NOT NULL,
        [Sales Channel]                 NVARCHAR(60)    NOT NULL,
        [Channel Group Code]            NVARCHAR(10)    NULL,   -- DIRECT / INDIRECT / DIGITAL
        [Channel Type Code]             NVARCHAR(10)    NULL,   -- FIELD / INSIDE / WEB / EDI / MKTPL / DIST / TEL
        [Region Code]                   NVARCHAR(10)    NOT NULL,
        [Operating Country Code]        NVARCHAR(3)     NULL,
        [Partner Name]                  NVARCHAR(100)   NULL,
        [Is Own Channel]                BIT             NULL,
        [Is Marketplace]                BIT             NULL,
        [Marketplace Commission Pct]    DECIMAL(9, 4)   NULL,
        [Reports Sell Through]          BIT             NULL,   -- distributor channels report weekly sell-through
        [Reporting Lag Days]            SMALLINT        NULL,
        [Order Capture System Code]     NVARCHAR(20)    NULL,
        [Order Number Prefix]           NVARCHAR(5)     NULL,
        [Default Payment Method Code]   NVARCHAR(15)    NULL,
        [Allows Backorder]              BIT             NULL,
        [Allows Partial Shipment]       BIT             NULL,
        [Returns Policy Code]           NVARCHAR(10)    NULL,   -- 30D / 14D (EU statutory) / 7D / NONE
        [Price List Code]               NVARCHAR(20)    NULL,
        [Attribution Model Code]        NVARCHAR(20)    NULL,   -- LASTCLICK / FIRSTCLICK / NONE
        [Is Active]                     BIT             NULL,
        [Launched On]                   DATE            NULL,
        [Retired On]                    DATE            NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Sales_Channel] PRIMARY KEY CLUSTERED ([Sales Channel Key] ASC),
        CONSTRAINT [UQ_Dimension_Sales_Channel_Code] UNIQUE ([Sales Channel Code], [Region Code])
    );
END;
GO
