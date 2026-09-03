/*
    Shipping.CustomsDeclarations

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1380 - after Shipping.ShipmentHeaders
    Depends on    : Shipping.ShipmentHeaders, Application.People
    Called by     : Shipping.vw_ShipmentExtract, customs broker interface

    Export declarations. Only cross-border shipments have one. The three
    regions file to different regimes and the column set reflects all three at
    once: EORI and VAT numbers for EU exports, an EEI/ITN reference for NA
    exports, and an exporter code plus GST treatment for APAC. Whichever set is
    not relevant is left null, and no constraint enforces the pairing beyond
    the declaration regime check.
*/
CREATE TABLE [Shipping].[CustomsDeclarations] (
    [CustomsDeclarationID]  INT             IDENTITY (1, 1) NOT NULL,
    [ShipmentID]            INT             NOT NULL,
    [DeclarationRegime]     NVARCHAR (10)   NOT NULL,
    [DeclarationReference]  NVARCHAR (40)   NOT NULL,
    [DeclaredWhen]          DATETIME2 (7)   NOT NULL,
    [ExporterEoriNumber]    NVARCHAR (20)   NULL,
    [ExporterVatNumber]     NVARCHAR (20)   NULL,
    [InternalTransactionNumber] NVARCHAR (30) NULL,
    [ApacExporterCode]      NVARCHAR (20)   NULL,
    [GstTreatmentCode]      NVARCHAR (12)   NULL,
    [IncotermCode]          NCHAR (3)       NOT NULL,
    [CountryOfOriginISO3]   NCHAR (3)       NULL,
    [DestinationCountryISO3] NCHAR (3)      NOT NULL,
    [HarmonisedCodeList]    NVARCHAR (400)  NULL,
    [DeclaredValueAmount]   DECIMAL (18, 2) NOT NULL,
    [DeclaredCurrencyCode]  NCHAR (3)       NOT NULL,
    [DutyAmount]            DECIMAL (18, 2) NULL,
    [BrokerReference]       NVARCHAR (40)   NULL,
    [ClearanceStatus]       NVARCHAR (12)   CONSTRAINT [DF_Shipping_CustomsDeclarations_ClearanceStatus] DEFAULT (N'SUBMITTED') NOT NULL,
    [ClearedWhen]           DATETIME2 (7)   NULL,
    [HoldReasonCode]        NVARCHAR (10)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Shipping_CustomsDeclarations_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Shipping_CustomsDeclarations] PRIMARY KEY CLUSTERED ([CustomsDeclarationID] ASC),
    CONSTRAINT [UQ_Shipping_CustomsDeclarations_Reference] UNIQUE ([DeclarationRegime], [DeclarationReference]),
    CONSTRAINT [CK_Shipping_CustomsDeclarations_Regime] CHECK ([DeclarationRegime] IN (N'EUCDS', N'UKCDS', N'USAES', N'AUEXP', N'JPNACCS', N'SGTRADENET')),
    CONSTRAINT [CK_Shipping_CustomsDeclarations_Status] CHECK ([ClearanceStatus] IN (N'SUBMITTED', N'QUERIED', N'HELD', N'CLEARED', N'REJECTED')),
    CONSTRAINT [CK_Shipping_CustomsDeclarations_Cleared] CHECK ([ClearanceStatus] <> N'CLEARED' OR [ClearedWhen] IS NOT NULL),
    CONSTRAINT [FK_Shipping_CustomsDeclarations_Shipments] FOREIGN KEY ([ShipmentID]) REFERENCES [Shipping].[ShipmentHeaders] ([ShipmentID]),
    CONSTRAINT [FK_Shipping_CustomsDeclarations_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Shipping_CustomsDeclarations_Held]
    ON [Shipping].[CustomsDeclarations] ([DeclaredWhen] ASC)
    INCLUDE ([ShipmentID], [HoldReasonCode])
    WHERE [ClearanceStatus] IN (N'QUERIED', N'HELD');
GO
