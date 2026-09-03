/*
    OLTP extension sequences

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 00_schemas / 0010 - after 0000_schemas.sql
    Depends on    : the shipped [Sequences] schema
    Called by     : DEFAULT constraints on the extension tables

    The sample allocates surrogate keys from sequences in the [Sequences]
    schema. Extension tables follow the same pattern so that key generation
    stays in one place. Start values are offset well above the sample's own
    ranges so a partial restore of the sample data cannot collide with keys
    issued by the extension tables.
*/
IF OBJECT_ID(N'Sequences.ShipmentID', N'SO') IS NULL
    EXEC (N'CREATE SEQUENCE [Sequences].[ShipmentID] AS INT START WITH 100001 INCREMENT BY 1;');
GO

IF OBJECT_ID(N'Sequences.ShipmentLineID', N'SO') IS NULL
    EXEC (N'CREATE SEQUENCE [Sequences].[ShipmentLineID] AS BIGINT START WITH 500001 INCREMENT BY 1;');
GO

IF OBJECT_ID(N'Sequences.ReturnAuthorizationID', N'SO') IS NULL
    EXEC (N'CREATE SEQUENCE [Sequences].[ReturnAuthorizationID] AS INT START WITH 40001 INCREMENT BY 1;');
GO

IF OBJECT_ID(N'Sequences.CreditNoteID', N'SO') IS NULL
    EXEC (N'CREATE SEQUENCE [Sequences].[CreditNoteID] AS INT START WITH 60001 INCREMENT BY 1;');
GO

IF OBJECT_ID(N'Sequences.QuoteID', N'SO') IS NULL
    EXEC (N'CREATE SEQUENCE [Sequences].[QuoteID] AS INT START WITH 200001 INCREMENT BY 1;');
GO

IF OBJECT_ID(N'Sequences.StockMovementID', N'SO') IS NULL
    EXEC (N'CREATE SEQUENCE [Sequences].[StockMovementID] AS BIGINT START WITH 1000001 INCREMENT BY 1;');
GO

IF OBJECT_ID(N'Sequences.PromotionID', N'SO') IS NULL
    EXEC (N'CREATE SEQUENCE [Sequences].[PromotionID] AS INT START WITH 3001 INCREMENT BY 1;');
GO

/*
    Legacy quirk: the loyalty programme was bought in as a package in 2011 and
    its member numbers are allocated in blocks of ten so the call-centre could
    reserve a block per agent. The increment is intentionally not 1.
*/
IF OBJECT_ID(N'Sequences.LoyaltyMemberNumber', N'SO') IS NULL
    EXEC (N'CREATE SEQUENCE [Sequences].[LoyaltyMemberNumber] AS BIGINT START WITH 7000000 INCREMENT BY 10;');
GO

IF OBJECT_ID(N'Sequences.OutboundMessageID', N'SO') IS NULL
    EXEC (N'CREATE SEQUENCE [Sequences].[OutboundMessageID] AS BIGINT START WITH 1 INCREMENT BY 1 CACHE 500;');
GO
