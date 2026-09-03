/*
    Object        : [Dimension].[Order Status Junk]  (junk dimension - low-cardinality flag combinations)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.OrderStatusJunkKey
    Called by     : Integration.usp_LoadJunkDimensionOrderStatus

    Collapses eleven order-level flags that were originally eleven columns on
    Fact.Order into one dimension. The 2013 change that created it was driven by
    row width, not by modelling purity: Fact.Order was over the page-density
    target and the flags were the cheapest thing to remove.

    The dimension is loaded on demand: the fact load looks the combination up and,
    when it does not exist, inserts it. The theoretical Cartesian product is a few
    thousand rows but only a few hundred combinations ever occur, so the table is
    NOT pre-populated with the full cross join - the 2013 attempt to do that
    generated 27,648 rows of which 300 were used, and it was reverted.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Order Status Junk', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Order Status Junk]
    (
        [Order Status Junk Key]         INT             CONSTRAINT [DF_Dimension_Order_Status_Junk_Key] DEFAULT (NEXT VALUE FOR [Sequences].[OrderStatusJunkKey]) NOT NULL,
        [Order Status Code]             NVARCHAR(10)    NOT NULL,   -- NEW / CONF / PICK / PACK / SHIP / INV / CANC / HOLD
        [Order Status Description]      NVARCHAR(40)    NULL,
        [Fulfilment Status Code]        NVARCHAR(10)    NOT NULL,   -- NONE / PART / FULL / OVER
        [Payment Status Code]           NVARCHAR(10)    NOT NULL,   -- UNPAID / AUTH / PART / PAID / REFUND / CHARGEBACK
        [Is Backorder]                  BIT             NOT NULL,
        [Is Undersupply]                BIT             NOT NULL,
        [Is Rush Order]                 BIT             NOT NULL,
        [Is Gift Order]                 BIT             NOT NULL,
        [Is Credit Held]                BIT             NOT NULL,
        [Is Manual Price Override]      BIT             NOT NULL,
        [Is Promotion Applied]          BIT             NOT NULL,
        [Is Tax Exempt]                 BIT             NOT NULL,
        [Is Cross Border]               BIT             NOT NULL,
        [Is Marketplace Order]          BIT             NOT NULL,
        [Is Subscription Order]         BIT             NOT NULL,

        /* Denormalised groupings the reports slice on, derived by the load. */
        [Status Group Code]             NVARCHAR(15)    NULL,   -- OPEN / INPROGRESS / CLOSED / EXCEPTION
        [Exception Flag Count]          SMALLINT        NULL,
        [Requires Attention]            BIT             NULL,
        [Combination Hash]              VARBINARY(32)   NULL,   -- lookup key used by the fact load
        [First Seen On]                 DATETIME2(7)    NULL,
        [First Seen Batch Id]           BIGINT          NULL,
        [Occurrence Count]              BIGINT          NULL,   -- maintained by the fact load, not a true dimension attribute
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Order_Status_Junk] PRIMARY KEY CLUSTERED ([Order Status Junk Key] ASC),
        CONSTRAINT [UQ_Dimension_Order_Status_Junk_Hash] UNIQUE ([Combination Hash])
    );
END;
GO
