/*
    Sales.usp_AssignCustomerSegments

    Catalog entry : sqlserver_oltp.procedures - Sales.AssignCustomerSegments
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6030 - after 6020
    Depends on    : Sales.CustomerSegments, Sales.CustomerSegmentAssignments,
                    Sales.Customers, Sales.Invoices
    Called by     : nightly segmentation job

    Hand-rolled type 2 maintenance over segment assignments. The segment rule
    is held on the segment row as free text in SegmentRuleText and executed
    with dynamic SQL - the rules were written by marketing analysts and the
    text is trusted, which is a known weakness recorded on the risk register
    rather than fixed.

    Retention and consent differ by region: EU assignments carry an explicit
    expiry derived from the segment's retention months, NA assignments do not
    expire, and APAC assignments expire only where the segment demands consent.
*/
CREATE PROCEDURE [Sales].[usp_AssignCustomerSegments]
    @RegionCode     NCHAR (4) = NULL,
    @AsAtDate       DATE = NULL,
    @RunByPersonID  INT,
    @BatchID        BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsAtDate IS NULL
        SET @AsAtDate = CONVERT(DATE, SYSDATETIME());

    DECLARE @CustomerSegmentID  INT;
    DECLARE @SegmentCode        NVARCHAR (12);
    DECLARE @SegmentRegion      NCHAR (4);
    DECLARE @RuleText           NVARCHAR (600);
    DECLARE @RetentionMonths    SMALLINT;
    DECLARE @ConsentRequired    BIT;
    DECLARE @Sql                NVARCHAR (MAX);

    CREATE TABLE #SegmentMatch
    (
        [CustomerID]            INT             NOT NULL,
        [CustomerSegmentID]     INT             NOT NULL,
        [ScoreValue]            DECIMAL (9, 4)  NULL
    );

    DECLARE SegmentCursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            seg.[CustomerSegmentID],
            seg.[SegmentCode],
            seg.[RegionCode],
            seg.[SegmentRuleText],
            seg.[RetentionMonths],
            seg.[ConsentRequired]
        FROM [Sales].[CustomerSegments] AS seg
        WHERE seg.[IsActive] = 1
            AND (@RegionCode IS NULL OR seg.[RegionCode] = @RegionCode)
        ORDER BY seg.[PriorityOrder] ASC;

    OPEN SegmentCursor;
    FETCH NEXT FROM SegmentCursor
        INTO @CustomerSegmentID, @SegmentCode, @SegmentRegion, @RuleText,
             @RetentionMonths, @ConsentRequired;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @RuleText IS NOT NULL AND LEN(@RuleText) > 0
        BEGIN
            -- Marketing's rule text is spliced straight into the predicate.
            SET @Sql = N'INSERT INTO #SegmentMatch ([CustomerID], [CustomerSegmentID], [ScoreValue])
                         SELECT c.[CustomerID], @SegmentID, NULL
                         FROM [Sales].[Customers] AS c
                         WHERE (' + @RuleText + N')
                             AND (@Region IS NULL OR c.[RegionCode] = @Region);';

            EXEC sp_executesql @Sql,
                 N'@SegmentID INT, @Region NCHAR (4)',
                 @SegmentID = @CustomerSegmentID,
                 @Region = @SegmentRegion;
        END

        BEGIN TRANSACTION;

        -- Close assignments that no longer match.
        UPDATE asg
        SET asg.[ValidToDate] = @AsAtDate,
            asg.[IsCurrentRow] = 0,
            asg.[LastEditedBy] = @RunByPersonID,
            asg.[LastEditedWhen] = SYSDATETIME()
        FROM [Sales].[CustomerSegmentAssignments] AS asg
        WHERE asg.[CustomerSegmentID] = @CustomerSegmentID
            AND asg.[ValidToDate] IS NULL
            AND NOT EXISTS (SELECT 1 FROM #SegmentMatch AS m
                            WHERE m.[CustomerID] = asg.[CustomerID]
                                AND m.[CustomerSegmentID] = asg.[CustomerSegmentID]);

        -- Open assignments that are new.
        INSERT INTO [Sales].[CustomerSegmentAssignments]
        (
            [CustomerID], [CustomerSegmentID], [ValidFromDate], [IsCurrentRow],
            [AssignmentReason], [ScoreValue], [ConsentCapturedWhen],
            [RetentionExpiryDate], [AssignedByProcess], [LastEditedBy]
        )
        SELECT
            m.[CustomerID],
            m.[CustomerSegmentID],
            @AsAtDate,
            1,
            N'Rule match for segment ' + @SegmentCode,
            m.[ScoreValue],
            c.[ConsentCapturedWhen],
            CASE
                WHEN @SegmentRegion = N'EU' THEN DATEADD(MONTH, @RetentionMonths, @AsAtDate)
                WHEN @SegmentRegion = N'APAC' AND @ConsentRequired = 1 THEN DATEADD(MONTH, @RetentionMonths, @AsAtDate)
                ELSE NULL
            END,
            N'usp_AssignCustomerSegments',
            @RunByPersonID
        FROM #SegmentMatch AS m
            INNER JOIN [Sales].[Customers] AS c
                ON c.[CustomerID] = m.[CustomerID]
        WHERE m.[CustomerSegmentID] = @CustomerSegmentID
            AND NOT EXISTS (SELECT 1 FROM [Sales].[CustomerSegmentAssignments] AS a2
                            WHERE a2.[CustomerID] = m.[CustomerID]
                                AND a2.[CustomerSegmentID] = m.[CustomerSegmentID]
                                AND a2.[ValidToDate] IS NULL);

        COMMIT TRANSACTION;

        DELETE FROM #SegmentMatch WHERE [CustomerSegmentID] = @CustomerSegmentID;

        FETCH NEXT FROM SegmentCursor
            INTO @CustomerSegmentID, @SegmentCode, @SegmentRegion, @RuleText,
                 @RetentionMonths, @ConsentRequired;
    END

    CLOSE SegmentCursor;
    DEALLOCATE SegmentCursor;

    DROP TABLE #SegmentMatch;
END
GO
