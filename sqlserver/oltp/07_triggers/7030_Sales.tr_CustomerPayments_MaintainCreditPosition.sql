/*
    Sales.tr_CustomerPayments_MaintainCreditPosition

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 07_triggers / 7030
    Depends on    : Sales.CustomerPayments, Sales.PaymentAllocations,
                    Sales.Customers, Sales.CustomerCreditHolds
    Fires on      : AFTER INSERT, UPDATE on Sales.CustomerPayments

    Refreshes the cached average-days-to-pay on the customer and closes an
    automatic credit hold when an allocated payment lands.

    The hold history is effective-dated: closing a hold means stamping
    EffectiveToWhen on the open row and opening a HOLDOFF row after it. Only
    holds raised by the overnight credit run are closed - a hold entered by a
    credit controller carries SourceSystem CREDITCTL and is left alone,
    because in 2011 a payment run lifted forty of them overnight.
*/
CREATE TRIGGER [Sales].[tr_CustomerPayments_MaintainCreditPosition]
    ON [Sales].[CustomerPayments]
    AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM inserted WHERE [PaymentStatus] IN (N'ALLOCATED', N'PARTALLOCATED'))
        RETURN;

    DECLARE @Paid TABLE ([CustomerID] INT NOT NULL PRIMARY KEY);

    INSERT INTO @Paid ([CustomerID])
    SELECT DISTINCT [CustomerID]
    FROM inserted
    WHERE [PaymentStatus] IN (N'ALLOCATED', N'PARTALLOCATED');

    UPDATE c
    SET c.[AverageDaysToPay] = CONVERT(SMALLINT, ISNULL(paid.[AverageDays], c.[AverageDaysToPay]))
    FROM [Sales].[Customers] AS c
        INNER JOIN @Paid AS p ON p.[CustomerID] = c.[CustomerID]
        OUTER APPLY
        (
            SELECT AVG(CONVERT(DECIMAL (9, 2), DATEDIFF(DAY, inv.[InvoiceDate], pay.[ValueDate]))) AS [AverageDays]
            FROM [Sales].[CustomerPayments] AS pay
                INNER JOIN [Sales].[PaymentAllocations] AS pa
                    ON pa.[CustomerPaymentID] = pay.[CustomerPaymentID]
                INNER JOIN [Sales].[Invoices] AS inv
                    ON inv.[InvoiceID] = pa.[InvoiceID]
            WHERE pay.[CustomerID] = c.[CustomerID]
                AND pay.[PaymentStatus] IN (N'ALLOCATED', N'PARTALLOCATED')
                AND pa.[TargetTypeCode] = N'INVOICE'
                AND pay.[ValueDate] >= DATEADD(MONTH, -12, CONVERT(DATE, SYSDATETIME()))
        ) AS paid;

    DECLARE @Released TABLE ([CustomerID] INT NOT NULL PRIMARY KEY);

    UPDATE h
    SET h.[EffectiveToWhen] = SYSDATETIME()
    OUTPUT [inserted].[CustomerID] INTO @Released ([CustomerID])
    FROM [Sales].[CustomerCreditHolds] AS h
        INNER JOIN @Paid AS p ON p.[CustomerID] = h.[CustomerID]
    WHERE h.[EffectiveToWhen] IS NULL
        AND h.[IsOnHold] = 1
        AND h.[HoldReasonCode] IN (N'OVERDUE', N'LIMIT', N'DSO')
        AND ISNULL(h.[SourceSystem], N'') <> N'CREDITCTL';

    INSERT INTO [Sales].[CustomerCreditHolds]
    (
        [CustomerID], [EventTypeCode], [EffectiveFromWhen], [IsOnHold],
        [HoldReasonCode], [ApprovalNarrative], [SourceSystem]
    )
    SELECT
        r.[CustomerID],
        N'HOLDOFF',
        SYSDATETIME(),
        0,
        NULL,
        N'Automatic release on payment allocation',
        N'AR'
    FROM @Released AS r;

    UPDATE c
    SET c.[CreditHoldReasonCode] = NULL,
        c.[CreditHoldSetWhen] = NULL
    FROM [Sales].[Customers] AS c
        INNER JOIN @Released AS r ON r.[CustomerID] = c.[CustomerID]
    WHERE NOT EXISTS (SELECT 1
                      FROM [Sales].[CustomerCreditHolds] AS h
                      WHERE h.[CustomerID] = c.[CustomerID]
                          AND h.[EffectiveToWhen] IS NULL
                          AND h.[IsOnHold] = 1);
END
GO
