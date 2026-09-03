/*
    Returns.usp_AuthorizeReturn

    Catalog entry : sqlserver_oltp.procedures - Returns.AuthorizeReturn
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6130 - after 6120
    Depends on    : Returns.ReturnAuthorizations, Returns.ReturnLines,
                    Returns.ReturnReasons, Sales.Invoices, Returns.ufn_RestockingFee

    Authorises or declines an RMA against the regional return window.

    The window is the shortest of the reason's own window and the regional
    default: NA 30 days, EU 14 days of statutory cooling off plus 16 days of
    goodwill, APAC 7 days with no goodwill. EU requests inside the statutory
    period are flagged IsCoolingOffPeriod and cannot be declined for lateness,
    only for condition.
*/
CREATE PROCEDURE [Returns].[usp_AuthorizeReturn]
    @ReturnAuthorizationID  INT,
    @AuthorizedByPersonID   INT,
    @OverrideWindow         BIT = 0,
    @BatchID                BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @RegionCode     NCHAR (4);
    DECLARE @InvoiceID      INT;
    DECLARE @RequestedWhen  DATETIME2 (7);
    DECLARE @InvoiceDate    DATE;
    DECLARE @DaysSinceSale  INT;
    DECLARE @RegionWindow   SMALLINT;
    DECLARE @CoolingOffDays SMALLINT;
    DECLARE @Status         NVARCHAR (12);
    DECLARE @ExpectedCredit DECIMAL (18, 2);

    SELECT
        @RegionCode = ra.[RegionCode],
        @InvoiceID = ra.[OriginalInvoiceID],
        @RequestedWhen = ra.[RequestedWhen],
        @Status = ra.[AuthorizationStatus]
    FROM [Returns].[ReturnAuthorizations] AS ra
    WHERE ra.[ReturnAuthorizationID] = @ReturnAuthorizationID;

    IF @RegionCode IS NULL
    BEGIN
        RAISERROR (N'Return authorization %d does not exist.', 16, 1, @ReturnAuthorizationID);
        RETURN;
    END

    IF @Status <> N'REQUESTED'
    BEGIN
        RAISERROR (N'Return authorization %d is not awaiting a decision.', 16, 1, @ReturnAuthorizationID);
        RETURN;
    END

    SELECT @InvoiceDate = i.[InvoiceDate]
    FROM [Sales].[Invoices] AS i
    WHERE i.[InvoiceID] = @InvoiceID;

    SET @DaysSinceSale = DATEDIFF(DAY, ISNULL(@InvoiceDate, CONVERT(DATE, @RequestedWhen)), CONVERT(DATE, @RequestedWhen));

    SELECT
        @RegionWindow = CASE @RegionCode WHEN N'NA' THEN 30 WHEN N'EU' THEN 30 ELSE 7 END,
        @CoolingOffDays = CASE @RegionCode WHEN N'EU' THEN 14 ELSE 0 END;

    BEGIN TRANSACTION;

    UPDATE [Returns].[ReturnAuthorizations]
    SET [IsCoolingOffPeriod] = CASE WHEN @DaysSinceSale <= @CoolingOffDays THEN 1 ELSE 0 END
    WHERE [ReturnAuthorizationID] = @ReturnAuthorizationID;

    IF @OverrideWindow = 0
        AND @DaysSinceSale > @RegionWindow
        AND NOT (@RegionCode = N'EU' AND @DaysSinceSale <= @CoolingOffDays)
    BEGIN
        UPDATE [Returns].[ReturnAuthorizations]
        SET [AuthorizationStatus] = N'DECLINED',
            [DeclineReason] = N'Outside the ' + CONVERT(NVARCHAR (6), @RegionWindow) + N' day return window for region ' + @RegionCode,
            [AuthorizedByPersonID] = @AuthorizedByPersonID,
            [AuthorizedWhen] = SYSDATETIME(),
            [LastEditedBy] = @AuthorizedByPersonID,
            [LastEditedWhen] = SYSDATETIME()
        WHERE [ReturnAuthorizationID] = @ReturnAuthorizationID;

        UPDATE [Returns].[ReturnLines]
        SET [LineStatus] = N'REJECTED',
            [LastEditedBy] = @AuthorizedByPersonID,
            [LastEditedWhen] = SYSDATETIME()
        WHERE [ReturnAuthorizationID] = @ReturnAuthorizationID;

        COMMIT TRANSACTION;
        RETURN;
    END

    -- Lines whose reason is beyond its own window are declined individually;
    -- the RMA as a whole still stands for whatever is left.
    UPDATE rl
    SET rl.[LineStatus] = N'REJECTED',
        rl.[LastEditedBy] = @AuthorizedByPersonID,
        rl.[LastEditedWhen] = SYSDATETIME()
    FROM [Returns].[ReturnLines] AS rl
        INNER JOIN [Returns].[ReturnReasons] AS rr
            ON rr.[ReturnReasonID] = rl.[ReturnReasonID]
    WHERE rl.[ReturnAuthorizationID] = @ReturnAuthorizationID
        AND rr.[ReturnWindowDays] IS NOT NULL
        AND @DaysSinceSale > rr.[ReturnWindowDays]
        AND @OverrideWindow = 0;

    -- The function returns the fee in currency; the line carries the percent it
    -- works out to, which is what the credit note and the customer letter quote.
    UPDATE rl
    SET rl.[RestockingPercent] = CASE WHEN rl.[GrossCreditAmount] > 0
                                      THEN CONVERT(DECIMAL (5, 2), ROUND(fee.[Amount] * 100.0 / rl.[GrossCreditAmount], 2))
                                      ELSE 0 END,
        rl.[LineStatus] = N'AUTHORIZED',
        rl.[LastEditedBy] = @AuthorizedByPersonID,
        rl.[LastEditedWhen] = SYSDATETIME()
    FROM [Returns].[ReturnLines] AS rl
        CROSS APPLY (VALUES ([Returns].[ufn_RestockingFee](rl.[ReturnReasonID], @RegionCode,
                                                           rl.[GrossCreditAmount], @DaysSinceSale))) AS fee ([Amount])
    WHERE rl.[ReturnAuthorizationID] = @ReturnAuthorizationID
        AND rl.[LineStatus] = N'AUTHORIZED';

    SELECT @ExpectedCredit = SUM(rl.[GrossCreditAmount])
    FROM [Returns].[ReturnLines] AS rl
    WHERE rl.[ReturnAuthorizationID] = @ReturnAuthorizationID
        AND rl.[LineStatus] = N'AUTHORIZED';

    UPDATE [Returns].[ReturnAuthorizations]
    SET [AuthorizationStatus] = N'APPROVED',
        [AuthorizedWhen] = SYSDATETIME(),
        [AuthorizedByPersonID] = @AuthorizedByPersonID,
        [ExpiresOnDate] = DATEADD(DAY, CASE @RegionCode WHEN N'APAC' THEN 14 ELSE 30 END, CONVERT(DATE, SYSDATETIME())),
        [TotalExpectedCredit] = ISNULL(@ExpectedCredit, 0),
        [LastEditedBy] = @AuthorizedByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [ReturnAuthorizationID] = @ReturnAuthorizationID;

    COMMIT TRANSACTION;
END
GO
