/*
    Fact.Procure To Pay

    Object        : [Fact].[Procure To Pay] - accumulating snapshot fact for the
                    procure-to-pay pipeline: requisition -> approval -> PO ->
                    receipt -> supplier invoice -> payment.
    Deploy target : WideWorldImportersDW
    Deploy order  : after [Fact].[Purchase], [Fact].[Purchase Receipt],
                    [Fact].[Supplier Transaction], [Fact].[Supplier Payment].
    Called by     : loaded and updated by
                    Integration.usp_LoadFactPurchaseReceipt (milestone fill) and
                    Integration.usp_LoadFactSupplierPayment (final milestone).
    Grain         : one purchase order line.

    Supporting fact for the procurement pipeline; the catalog names the
    individual transaction facts, this one exists so cycle-time reporting does
    not have to join five of them at query time.

    Approval routing is regional: NA requires a single cost-centre owner
    approval above a USD threshold, EU adds a second approval for any
    cross-border PO, APAC routes through a country finance controller
    regardless of value. That is why [Approval Level Count] varies and why the
    approval lag is not comparable across regions without the level count.
*/
CREATE TABLE [Fact].[Procure To Pay] (
    [Procure To Pay Key]            BIGINT          IDENTITY (1, 1) NOT NULL,
    [Requisition Date Key]          DATE            NOT NULL,
    [Approval Date Key]             DATE            NULL,
    [Purchase Order Date Key]       DATE            NULL,
    [First Receipt Date Key]        DATE            NULL,
    [Final Receipt Date Key]        DATE            NULL,
    [Supplier Invoice Date Key]     DATE            NULL,
    [Match Completed Date Key]      DATE            NULL,
    [Payment Date Key]              DATE            NULL,
    [Supplier Key]                  INT             NOT NULL,
    [Stock Item Key]                INT             NULL,
    [Cost Center Key]               INT             NULL,
    [Vendor Contract Key]           INT             NULL,
    [Warehouse Site Key]            INT             NULL,
    [Buyer Employee Key]            INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Requisition Number]            NVARCHAR (20)   NOT NULL,
    [Requisition Line Number]       INT             NOT NULL,
    [Purchase Order Number]         NVARCHAR (20)   NULL,
    [Purchase Order Line Number]    INT             NULL,
    [Receipt Number]                NVARCHAR (20)   NULL,
    [Supplier Invoice Number]       NVARCHAR (20)   NULL,
    [Payment Run Reference]         NVARCHAR (20)   NULL,
    [Requisitioned Value Reporting] DECIMAL (18, 2) NULL,
    [Ordered Value Reporting]       DECIMAL (18, 2) NULL,
    [Received Value Reporting]      DECIMAL (18, 2) NULL,
    [Invoiced Value Reporting]      DECIMAL (18, 2) NULL,
    [Paid Value Reporting]          DECIMAL (18, 2) NULL,
    [Requisition To Approval Days]  INT             NULL,
    [Approval To Order Days]        INT             NULL,
    [Order To Receipt Days]         INT             NULL,
    [Receipt To Invoice Days]       INT             NULL,
    [Invoice To Payment Days]       INT             NULL,
    [Total Cycle Days]              INT             NULL,
    [Approval Level Count]          TINYINT         NULL,
    [Maverick Spend Flag]           BIT             NULL,
    [Contract Covered Flag]         BIT             NULL,
    [Three Way Match Exception Flag] BIT            NULL,
    [Pipeline Status Code]          NVARCHAR (6)    NULL,
    [Open Milestone Count]          TINYINT         CONSTRAINT [DF_Fact_Procure_To_Pay_Open_Milestone_Count] DEFAULT (7) NOT NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    [Last Milestone Update]         DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Procure_To_Pay] PRIMARY KEY NONCLUSTERED ([Procure To Pay Key] ASC, [Requisition Date Key] ASC) ON [PS_Date] ([Requisition Date Key]),
    CONSTRAINT [FK_Fact_Procure_To_Pay_Requisition_Date_Key_Dimension_Date] FOREIGN KEY ([Requisition Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Procure_To_Pay_Supplier_Key_Dimension_Supplier] FOREIGN KEY ([Supplier Key]) REFERENCES [Dimension].[Supplier] ([Supplier Key])
)
ON [PS_Date] ([Requisition Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Procure_To_Pay_Natural_Key]
    ON [Fact].[Procure To Pay] ([Requisition Number] ASC, [Requisition Line Number] ASC, [Requisition Date Key] ASC)
    ON [PS_Date] ([Requisition Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Procure_To_Pay_Open_Pipeline]
    ON [Fact].[Procure To Pay] ([Open Milestone Count] ASC, [Supplier Key] ASC)
    INCLUDE ([Purchase Order Number], [Supplier Invoice Number], [Pipeline Status Code])
    ON [PS_Date] ([Requisition Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Procure-to-pay accumulating snapshot at purchase order line grain',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Procure To Pay';
GO
