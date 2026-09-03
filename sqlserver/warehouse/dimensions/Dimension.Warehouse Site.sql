/*
    Object        : [Dimension].[Warehouse Site]  (SCD Type 1 with a Type 2 shadow on capacity)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Geography.sql
    Depends on    : Sequences.WarehouseSiteKey, Dimension.Geography, Dimension.Cost Center
    Called by     : Integration.usp_MigrateStagedWarehouseSiteData

    Catalogued as SCD1 and loaded as SCD1 for every attribute except the capacity
    block, which the 2015 network-optimisation project needed history for. Rather
    than convert the dimension to Type 2 (which would have rekeyed every inventory
    fact) they added [Capacity History] columns holding the previous value and the
    date it changed - a "Type 3" bolt-on. Only one prior value is retained, so
    anything older is lost, which is documented and accepted.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Warehouse Site', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Warehouse Site]
    (
        [Warehouse Site Key]            INT             CONSTRAINT [DF_Dimension_Warehouse_Site_Key] DEFAULT (NEXT VALUE FOR [Sequences].[WarehouseSiteKey]) NOT NULL,
        [WWI Warehouse Site ID]         INT             NULL,
        [Warehouse Site Code]           NVARCHAR(20)    NOT NULL,
        [Warehouse Site]                NVARCHAR(80)    NOT NULL,
        [Site Type Code]                NVARCHAR(10)    NULL,   -- DC / RDC / XDOCK / 3PL / RETAIL / RETURNS
        [Region Code]                   NVARCHAR(10)    NULL,
        [Geography Key]                 INT             NULL,
        [City Key]                      INT             NULL,
        [Cost Center Key]               INT             NULL,
        [Operating Company Code]        NVARCHAR(10)    NULL,
        [Is Third Party Operated]       BIT             NULL,
        [Operator Name]                 NVARCHAR(100)   NULL,

        [Storage Capacity Pallets]      INT             NULL,
        [Pick Faces]                    INT             NULL,
        [Dock Doors]                    SMALLINT        NULL,
        [Has Chilled Storage]           BIT             NULL,
        [Has Frozen Storage]            BIT             NULL,
        [Has Hazardous Storage]         BIT             NULL,
        [Automation Level Code]         NVARCHAR(10)    NULL,   -- MANUAL / SEMI / AUTO / GTP

        /* Type 3 bolt-on: one prior capacity value and when it changed. */
        [Prior Storage Capacity Pallets] INT            NULL,
        [Capacity Changed On]           DATE            NULL,
        [Capacity Change Reason Code]   NVARCHAR(20)    NULL,

        [Opened On]                     DATE            NULL,
        [Closed On]                     DATE            NULL,
        [Is Active]                     BIT             NULL,
        [Operating Hours Code]          NVARCHAR(10)    NULL,   -- 5x8 / 6x12 / 7x24
        [Time Zone Name]                NVARCHAR(60)    NULL,
        [Inventory Valuation Method]    NVARCHAR(10)    NULL,   -- FIFO (EU/APAC) / STDCOST (NA)
        [Cycle Count Policy Code]       NVARCHAR(10)    NULL,
        [Customs Bonded Flag]           BIT             NULL,   -- APAC free-trade-zone sites
        [EU Excise Warehouse Number]    NVARCHAR(30)    NULL,
        [NA Bonded Carrier Code]        NVARCHAR(20)    NULL,

        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Warehouse_Site] PRIMARY KEY CLUSTERED ([Warehouse Site Key] ASC),
        CONSTRAINT [UQ_Dimension_Warehouse_Site_Code] UNIQUE ([Warehouse Site Code])
    );
END;
GO
