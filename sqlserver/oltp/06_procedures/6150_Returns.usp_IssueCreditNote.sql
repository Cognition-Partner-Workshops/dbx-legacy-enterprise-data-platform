/*
    Returns.usp_IssueCreditNote

    Catalog entry : sqlserver_oltp.procedures - Returns.IssueCreditNote
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6150 - after 6140
    Depends on    : Returns.CreditNotes, Returns.CreditNoteLines, Returns.ReturnLines,
                    Returns.ReturnAuthorizations, Sales.Invoices, Sequences.CreditNoteID

    Issues one credit note for the accepted lines of an RMA.

    Numbering is per legal series and is taken from the series maximum, then formatted
    with the region prefix; the series code is derived from the region because
    the finance system will not accept a number outside its own series.

    Tax handling diverges: NA reverses the sales tax charged on the original
    line, EU reverses VAT at the rate in force on the credit's tax point date
    rather than the sale date, APAC reverses GST at the sale rate. The three
    branches were written separately and only the EU branch reads the tax
    point date at all.
*/
CREATE PROCEDURE [Returns].[usp_IssueCreditNote]
    @ReturnAuthorizationID  INT,
    @IssuedByPersonID       INT,
    @IsRefundToCard         BIT = 0,
    @BatchID                BIGINT = NULL,
    @CreditNoteID           INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @CustomerID         INT;
    DECLARE @RegionCode         NCHAR (4);
    DECLARE @OriginalInvoiceID  INT;
    DECLARE @CurrencyCode       NCHAR (3);
    DECLARE @TaxRegimeCode      NVARCHAR (12);
    DECLARE @CustomerTaxNumber  NVARCHAR (24);
    DECLARE @ExchangeRate       DECIMAL (18, 8);
    DECLARE @SeriesCode         NVARCHAR (8);
    DECLARE @SeriesNumber       INT;
    DECLARE @NetAmount          DECIMAL (18, 2);
    DECLARE @TaxAmount          DECIMAL (18, 2);
    DECLARE @RestockingAmount   DECIMAL (18, 2);

    SELECT
        @CustomerID = ra.[CustomerID],
        @RegionCode = ra.[RegionCode],
        @OriginalInvoiceID = ra.[OriginalInvoiceID],
        @CurrencyCode = ISNULL(ra.[CreditCurrencyCode], N'USD')
    FROM [Returns].[ReturnAuthorizations] AS ra
    WHERE ra.[ReturnAuthorizationID] = @ReturnAuthorizationID;

    IF @CustomerID IS NULL
    BEGIN
        RAISERROR (N'Return authorization %d does not exist.', 16, 1, @ReturnAuthorizationID);
        RETURN;
    END

    IF NOT EXISTS (SELECT 1 FROM [Returns].[ReturnLines]
                   WHERE [ReturnAuthorizationID] = @ReturnAuthorizationID
                       AND [LineStatus] = N'INSPECTED'
                       AND ISNULL([QuantityAccepted], 0) > 0)
    BEGIN
        RAISERROR (N'Return authorization %d has no inspected and accepted lines.', 16, 1, @ReturnAuthorizationID);
        RETURN;
    END

    SELECT
        @TaxRegimeCode = i.[TaxRegimeCode],
        @CustomerTaxNumber = i.[CustomerTaxNumber],
        @ExchangeRate = i.[ExchangeRateToUsd]
    FROM [Sales].[Invoices] AS i
    WHERE i.[InvoiceID] = @OriginalInvoiceID;

    SET @SeriesCode = CASE @RegionCode
                          WHEN N'EU' THEN N'CN-EU'
                          WHEN N'APAC' THEN N'CN-AP'
                          ELSE N'CN-NA'
                      END;

    BEGIN TRANSACTION;

    -- Gapless numbering within the legal series. The series maximum is read
    -- under the open transaction and incremented by one; two concurrent
    -- issues in the same series will collide on the unique index and the
    -- second caller is expected to retry.
    SELECT @SeriesNumber = ISNULL(MAX(cn.[NumberWithinSeries]), 0) + 1
    FROM [Returns].[CreditNotes] AS cn WITH (UPDLOCK, HOLDLOCK)
    WHERE cn.[NumberSeriesCode] = @SeriesCode;

    SELECT @CreditNoteID = NEXT VALUE FOR [Sequences].[CreditNoteID];

    INSERT INTO [Returns].[CreditNotes]
    (
        [CreditNoteID], [CreditNoteNumber], [NumberSeriesCode], [NumberWithinSeries], [CustomerID],
        [ReturnAuthorizationID], [OriginalInvoiceID], [RegionCode], [CreditReasonCode],
        [IssuedDate], [TaxPointDate], [TaxRegimeCode], [CustomerTaxNumber],
        [CurrencyCode], [ExchangeRateToUsd], [NetAmount], [TaxAmount],
        [RestockingFeeAmount], [CreditNoteStatus], [IsRefundToCard], [LastEditedBy]
    )
    VALUES
    (
        @CreditNoteID,
        @SeriesCode + N'-' + RIGHT(N'000000000' + CONVERT(NVARCHAR (12), @SeriesNumber), 9),
        @SeriesCode,
        @SeriesNumber,
        @CustomerID,
        @ReturnAuthorizationID,
        @OriginalInvoiceID,
        @RegionCode,
        N'RETURN',
        CONVERT(DATE, SYSDATETIME()),
        CONVERT(DATE, SYSDATETIME()),
        @TaxRegimeCode,
        @CustomerTaxNumber,
        @CurrencyCode,
        @ExchangeRate,
        0,
        0,
        0,
        N'DRAFT',
        @IsRefundToCard,
        @IssuedByPersonID
    );

    INSERT INTO [Returns].[CreditNoteLines]
    (
        [CreditNoteID], [LineNumber], [CreditLineType], [ReturnLineID], [StockItemID],
        [Description], [Quantity], [UnitCreditAmount], [TaxRatePercent],
        [GeneralLedgerCode]
    )
    SELECT
        @CreditNoteID,
        ROW_NUMBER() OVER (ORDER BY rl.[LineNumber] ASC),
        N'GOODS',
        rl.[ReturnLineID],
        rl.[StockItemID],
        N'Credit for returned goods on RMA line ' + CONVERT(NVARCHAR (12), rl.[LineNumber]),
        rl.[QuantityAccepted],
        -- LineNetAmount and LineTaxAmount are computed from quantity, unit
        -- credit and tax rate, so the restocking deduction has to reach them
        -- through the unit credit rather than being written over the top.
        ROUND(rl.[UnitPriceAtSale] * (1 - ISNULL(rl.[RestockingPercent], 0) / 100.0), 2),
        rl.[TaxRatePercentAtSale],
        CASE @RegionCode WHEN N'EU' THEN N'4001-EU' WHEN N'APAC' THEN N'4001-AP' ELSE N'4001-NA' END
    FROM [Returns].[ReturnLines] AS rl
    WHERE rl.[ReturnAuthorizationID] = @ReturnAuthorizationID
        AND rl.[LineStatus] = N'INSPECTED'
        AND ISNULL(rl.[QuantityAccepted], 0) > 0;

    SELECT
        @NetAmount = SUM(l.[LineNetAmount]),
        @TaxAmount = SUM(l.[LineTaxAmount])
    FROM [Returns].[CreditNoteLines] AS l
    WHERE l.[CreditNoteID] = @CreditNoteID;

    SELECT @RestockingAmount = SUM(ROUND(rl.[QuantityAccepted] * rl.[UnitPriceAtSale]
                                         * ISNULL(rl.[RestockingPercent], 0) / 100.0, 2))
    FROM [Returns].[ReturnLines] AS rl
    WHERE rl.[ReturnAuthorizationID] = @ReturnAuthorizationID
        AND rl.[LineStatus] = N'INSPECTED';

    UPDATE [Returns].[CreditNotes]
    SET [NetAmount] = ISNULL(@NetAmount, 0),
        [TaxAmount] = ISNULL(@TaxAmount, 0),
        [RestockingFeeAmount] = ISNULL(@RestockingAmount, 0),
        [CreditNoteStatus] = N'ISSUED',
        [LastEditedBy] = @IssuedByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [CreditNoteID] = @CreditNoteID;

    UPDATE [Returns].[ReturnLines]
    SET [LineStatus] = N'CREDITED',
        [LastEditedBy] = @IssuedByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [ReturnAuthorizationID] = @ReturnAuthorizationID
        AND [LineStatus] = N'INSPECTED';

    UPDATE [Returns].[ReturnAuthorizations]
    SET [AuthorizationStatus] = N'CLOSED',
        [LastEditedBy] = @IssuedByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [ReturnAuthorizationID] = @ReturnAuthorizationID;

    COMMIT TRANSACTION;
END
GO
