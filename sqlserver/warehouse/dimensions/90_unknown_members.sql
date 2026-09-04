/*
    Object        : Reserved (unknown / not applicable / invalid / inferred) dimension members
    Deploy target : WideWorldImportersDW
    Deploy order  : 90 (after 00_dimension_schemas_and_sequences.sql,
                    01_dimension_key_registry.sql and every Dimension.*.sql table
                    script, before any fact load)
    Depends on    : every dimension table in sqlserver/warehouse/dimensions,
                    Integration.DimensionKeyRegistry
    Called by     : deployment, and by Integration.usp_EnrichInferredMembers when it
                    finds a dimension with no reserved members (it re-runs this file's
                    logic through the same dynamic block)

    Reserved keys, as documented in 00_dimension_schemas_and_sequences.sql:

        -1  Unknown          source value present but unresolvable
        -2  Not Applicable   the business rule says no member can apply
        -3  Invalid          source value present and rejected by a DQ screen
        -9  Error            the lookup itself failed

    Every fact load points at -1 when a lookup misses and the dimension does not
    accept inferred members, so these rows must exist before any fact is loaded.
    The script is re-runnable: each insert is guarded on the key not existing.

    Two mechanisms are used, which is itself a piece of estate history. The
    dimensions with a wide NOT NULL surface get an explicit INSERT written out by
    hand. The narrow reference dimensions are handled by the dynamic block at the
    end, which was added in 2016 when the twelfth reference dimension arrived and
    the author of this script lost patience.
*/
SET NOCOUNT ON;
GO

DECLARE @ValidFrom  DATETIME2(7) = CONVERT(DATETIME2(7), N'1900-01-01T00:00:00');
DECLARE @ValidTo    DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');

/* ---------------------------------------------------------------- Customer */
IF NOT EXISTS (SELECT 1 FROM [Dimension].[Customer] WHERE [Customer Key] = -1)
    INSERT INTO [Dimension].[Customer]
        ([Customer Key], [WWI Customer ID], [Customer], [Bill To Customer], [Category],
         [Buying Group], [Primary Contact], [Postal Code], [Region Code], [Is Current Row],
         [Version Number], [Effective From], [Effective To], [Is Inferred Member],
         [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-1, -1, N'Unknown', N'Unknown', N'Unknown', N'Unknown', N'Unknown', N'N/A',
         N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, 0, @ValidFrom, @ValidTo, 0);
GO

DECLARE @ValidFrom  DATETIME2(7) = CONVERT(DATETIME2(7), N'1900-01-01T00:00:00');
DECLARE @ValidTo    DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Customer] WHERE [Customer Key] = -2)
    INSERT INTO [Dimension].[Customer]
        ([Customer Key], [WWI Customer ID], [Customer], [Bill To Customer], [Category],
         [Buying Group], [Primary Contact], [Postal Code], [Region Code], [Is Current Row],
         [Version Number], [Effective From], [Effective To], [Is Inferred Member],
         [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-2, -2, N'Not Applicable', N'Not Applicable', N'Not Applicable', N'Not Applicable',
         N'Not Applicable', N'N/A', N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, 0, @ValidFrom, @ValidTo, 0);

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Customer] WHERE [Customer Key] = -3)
    INSERT INTO [Dimension].[Customer]
        ([Customer Key], [WWI Customer ID], [Customer], [Bill To Customer], [Category],
         [Buying Group], [Primary Contact], [Postal Code], [Region Code], [Is Current Row],
         [Version Number], [Effective From], [Effective To], [Is Inferred Member],
         [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-3, -3, N'Invalid', N'Invalid', N'Invalid', N'Invalid', N'Invalid', N'N/A',
         N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, 0, @ValidFrom, @ValidTo, 0);
GO

/* -------------------------------------------------------------- Stock Item */
DECLARE @ValidFrom  DATETIME2(7) = CONVERT(DATETIME2(7), N'1900-01-01T00:00:00');
DECLARE @ValidTo    DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Stock Item] WHERE [Stock Item Key] = -1)
    INSERT INTO [Dimension].[Stock Item]
        ([Stock Item Key], [WWI Stock Item ID], [Stock Item], [Color], [Selling Package],
         [Buying Package], [Brand], [Size], [Lead Time Days], [Quantity Per Outer],
         [Is Chiller Stock], [Tax Rate], [Unit Price], [Typical Weight Per Unit],
         [Listing Region Code], [Is Current Row], [Version Number], [Effective From],
         [Effective To], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-1, -1, N'Unknown', N'Unknown', N'Unknown', N'Unknown', N'Unknown', N'Unknown',
         0, 0, 0, 0, 0, 0, N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, 0, @ValidFrom, @ValidTo, 0);

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Stock Item] WHERE [Stock Item Key] = -2)
    INSERT INTO [Dimension].[Stock Item]
        ([Stock Item Key], [WWI Stock Item ID], [Stock Item], [Color], [Selling Package],
         [Buying Package], [Brand], [Size], [Lead Time Days], [Quantity Per Outer],
         [Is Chiller Stock], [Tax Rate], [Unit Price], [Typical Weight Per Unit],
         [Listing Region Code], [Is Current Row], [Version Number], [Effective From],
         [Effective To], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-2, -2, N'Not Applicable', N'N/A', N'N/A', N'N/A', N'N/A', N'N/A',
         0, 0, 0, 0, 0, 0, N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, 0, @ValidFrom, @ValidTo, 0);
GO

/* ---------------------------------------------------------------- Supplier */
DECLARE @ValidFrom  DATETIME2(7) = CONVERT(DATETIME2(7), N'1900-01-01T00:00:00');
DECLARE @ValidTo    DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Supplier] WHERE [Supplier Key] = -1)
    INSERT INTO [Dimension].[Supplier]
        ([Supplier Key], [WWI Supplier ID], [Supplier], [Category], [Primary Contact],
         [Payment Days], [Postal Code], [Region Code], [Is Current Row], [Version Number],
         [Effective From], [Effective To], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-1, -1, N'Unknown', N'Unknown', N'Unknown', 0, N'N/A', N'GLOBAL', 1, 1,
         @ValidFrom, @ValidTo, 0, @ValidFrom, @ValidTo, 0);

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Supplier] WHERE [Supplier Key] = -2)
    INSERT INTO [Dimension].[Supplier]
        ([Supplier Key], [WWI Supplier ID], [Supplier], [Category], [Primary Contact],
         [Payment Days], [Postal Code], [Region Code], [Is Current Row], [Version Number],
         [Effective From], [Effective To], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-2, -2, N'Not Applicable', N'N/A', N'N/A', 0, N'N/A', N'GLOBAL', 1, 1,
         @ValidFrom, @ValidTo, 0, @ValidFrom, @ValidTo, 0);
GO

/* ---------------------------------------------------------------- Employee */
DECLARE @ValidFrom  DATETIME2(7) = CONVERT(DATETIME2(7), N'1900-01-01T00:00:00');
DECLARE @ValidTo    DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Employee] WHERE [Employee Key] = -1)
    INSERT INTO [Dimension].[Employee]
        ([Employee Key], [WWI Employee ID], [Employee], [Preferred Name], [Is Salesperson],
         [Region Code], [Is Current Row], [Version Number], [Effective From], [Effective To],
         [Organisation Level], [Is Leaf Node], [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-1, -1, N'Unknown', N'Unknown', 0, N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, 0, 1,
         @ValidFrom, @ValidTo, 0);

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Salesperson] WHERE [Salesperson Key] = -1)
    INSERT INTO [Dimension].[Salesperson]
        ([Salesperson Key], [WWI Employee ID], [Salesperson], [Region Code], [Is Current Row],
         [Version Number], [Effective From], [Effective To], [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-1, -1, N'Unknown', N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, @ValidFrom, @ValidTo, 0);

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Salesperson] WHERE [Salesperson Key] = -2)
    INSERT INTO [Dimension].[Salesperson]
        ([Salesperson Key], [WWI Employee ID], [Salesperson], [Region Code], [Is Current Row],
         [Version Number], [Effective From], [Effective To], [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-2, -2, N'Not Applicable', N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, @ValidFrom, @ValidTo, 0);
GO

/* -------------------------------------------------------------------- City */
DECLARE @ValidFrom  DATETIME2(7) = CONVERT(DATETIME2(7), N'1900-01-01T00:00:00');
DECLARE @ValidTo    DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');

IF NOT EXISTS (SELECT 1 FROM [Dimension].[City] WHERE [City Key] = -1)
    INSERT INTO [Dimension].[City]
        ([City Key], [WWI City ID], [City], [State Province], [Country], [Continent],
         [Sales Territory], [Region], [Subregion], [Latest Recorded Population],
         [Region Code], [Is Current Row], [Version Number], [Effective From], [Effective To],
         [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-1, -1, N'Unknown', N'Unknown', N'Unknown', N'Unknown', N'Unknown', N'Unknown',
         N'Unknown', 0, N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, @ValidFrom, @ValidTo, 0);

IF NOT EXISTS (SELECT 1 FROM [Dimension].[City] WHERE [City Key] = -2)
    INSERT INTO [Dimension].[City]
        ([City Key], [WWI City ID], [City], [State Province], [Country], [Continent],
         [Sales Territory], [Region], [Subregion], [Latest Recorded Population],
         [Region Code], [Is Current Row], [Version Number], [Effective From], [Effective To],
         [Valid From], [Valid To], [Lineage Key])
    VALUES
        (-2, -2, N'Not Applicable', N'N/A', N'N/A', N'N/A', N'N/A', N'N/A', N'N/A', 0,
         N'GLOBAL', 1, 1, @ValidFrom, @ValidTo, @ValidFrom, @ValidTo, 0);
GO

/* -------------------------------------------------------------------- Date */
DECLARE @ValidFrom  DATETIME2(7) = CONVERT(DATETIME2(7), N'1900-01-01T00:00:00');

/*
    The date dimension keys on [Date], so its reserved members are sentinel dates
    rather than negative keys: 1900-01-01 unknown, 1900-01-02 not applicable.
*/
IF NOT EXISTS (SELECT 1 FROM [Dimension].[Date] WHERE [Date] = CONVERT(DATE, N'1900-01-01'))
    INSERT INTO [Dimension].[Date]
        ([Date], [Day Number], [Day], [Day of Week], [Day of Week Number],
         [Month], [Short Month], [Calendar Month Number], [Calendar Month Label],
         [Calendar Quarter Number], [Calendar Year], [Calendar Year Label],
         [Fiscal Month Number], [Fiscal Month Label], [Fiscal Year], [Fiscal Year Label],
         [ISO Week Number], [Is Reserved Member], [Reserved Member Description])
    VALUES
        (CONVERT(DATE, @ValidFrom), 0, N'Unknown', N'Unknown', 0,
         N'Unknown', N'UNK', 0, N'Unknown', 0, 1900, N'CY1900',
         0, N'Unknown', 1900, N'FY1900',
         0, 1, N'Unknown date');

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Date] WHERE [Date] = CONVERT(DATE, N'1900-01-02'))
    INSERT INTO [Dimension].[Date]
        ([Date], [Day Number], [Day], [Day of Week], [Day of Week Number],
         [Month], [Short Month], [Calendar Month Number], [Calendar Month Label],
         [Calendar Quarter Number], [Calendar Year], [Calendar Year Label],
         [Fiscal Month Number], [Fiscal Month Label], [Fiscal Year], [Fiscal Year Label],
         [ISO Week Number], [Is Reserved Member], [Reserved Member Description])
    VALUES
        (CONVERT(DATE, N'1900-01-02'), 0, N'N/A', N'N/A', 0,
         N'N/A', N'N/A', 0, N'N/A', 0, 1900, N'CY1900',
         0, N'N/A', 1900, N'FY1900',
         0, 1, N'Not applicable - no date for this role');

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Time] WHERE [Time Key] = -1)
    INSERT INTO [Dimension].[Time] ([Time Key], [Is Reserved Member], [Reserved Member Description])
    VALUES (-1, 1, N'Unknown time');

IF NOT EXISTS (SELECT 1 FROM [Dimension].[Time] WHERE [Time Key] = -2)
    INSERT INTO [Dimension].[Time] ([Time Key], [Is Reserved Member], [Reserved Member Description])
    VALUES (-2, 1, N'Not applicable - date grain fact');
GO

/*
    The 2016 dynamic block. For every registered dimension whose table has a
    narrow NOT NULL surface, build and run an INSERT for the reserved keys. The
    column list is derived from sys.columns: every non-nullable column without a
    default gets a type-appropriate placeholder. This is exactly as fragile as it
    looks, and it is why new dimensions are expected to keep their NOT NULL
    columns to the key, the business code and the name.
*/
DECLARE @TableName      SYSNAME;
DECLARE @KeyColumn      SYSNAME;
DECLARE @ReservedKey    INT;
DECLARE @Description    NVARCHAR(40);
DECLARE @Sql            NVARCHAR(MAX);
DECLARE @ColumnList     NVARCHAR(MAX);
DECLARE @ValueList      NVARCHAR(MAX);

DECLARE ReservedMemberCursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT r.[Dimension Name], r.[Key Column Name], k.ReservedKey, k.[Description]
    FROM [Integration].[DimensionKeyRegistry] AS r
    CROSS JOIN (VALUES (-1, N'Unknown'), (-2, N'Not Applicable')) AS k (ReservedKey, [Description])
    WHERE r.[Dimension Name] NOT IN (N'Customer', N'Stock Item', N'Supplier', N'Employee',
                                     N'Salesperson', N'City', N'Date', N'Time')
      AND r.[SCD Pattern] NOT IN (N'Bridge')
      AND OBJECT_ID(N'Dimension.' + QUOTENAME(r.[Dimension Name]), N'U') IS NOT NULL
    ORDER BY r.[Dimension Name], k.ReservedKey;

OPEN ReservedMemberCursor;
FETCH NEXT FROM ReservedMemberCursor INTO @TableName, @KeyColumn, @ReservedKey, @Description;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ColumnList = QUOTENAME(@KeyColumn);
    SET @ValueList  = CONVERT(NVARCHAR(10), @ReservedKey);

    SELECT
        @ColumnList = @ColumnList + N', ' + QUOTENAME(c.name),
        @ValueList  = @ValueList  + N', ' +
            CASE
                WHEN t.name IN (N'nvarchar', N'nchar', N'varchar', N'char')
                    THEN N'N''' + REPLACE(@Description, N'''', N'''''') + N''''
                WHEN t.name IN (N'date')                       THEN N'''1900-01-01'''
                WHEN t.name IN (N'datetime2', N'datetime')     THEN N'''1900-01-01T00:00:00'''
                WHEN t.name IN (N'time')                       THEN N'''00:00:00'''
                WHEN t.name IN (N'bit')                        THEN N'0'
                ELSE N'0'
            END
    FROM sys.columns AS c
    INNER JOIN sys.types AS t
        ON t.user_type_id = c.user_type_id
    WHERE c.object_id = OBJECT_ID(N'Dimension.' + QUOTENAME(@TableName), N'U')
      AND c.is_nullable = 0
      AND c.is_identity = 0
      AND c.is_computed = 0
      AND c.name <> @KeyColumn
      AND c.default_object_id = 0;

    /*
        [Valid To] must be the high date rather than the 1900 placeholder the
        loop produces, so the two lineage columns are patched afterwards. Yes,
        this means the generated statement writes them twice.
    */
    SET @Sql = N'IF NOT EXISTS (SELECT 1 FROM [Dimension].' + QUOTENAME(@TableName)
             + N' WHERE ' + QUOTENAME(@KeyColumn) + N' = ' + CONVERT(NVARCHAR(10), @ReservedKey) + N')'
             + N' INSERT INTO [Dimension].' + QUOTENAME(@TableName) + N' (' + @ColumnList + N')'
             + N' VALUES (' + @ValueList + N');';

    BEGIN TRY
        EXEC sys.sp_executesql @Sql;

        IF COL_LENGTH(N'Dimension.' + QUOTENAME(@TableName), N'Valid To') IS NOT NULL
        BEGIN
            SET @Sql = N'UPDATE [Dimension].' + QUOTENAME(@TableName)
                     + N' SET [Valid To] = ''9999-12-31T23:59:59.9999999'''
                     + N' WHERE ' + QUOTENAME(@KeyColumn) + N' = ' + CONVERT(NVARCHAR(10), @ReservedKey)
                     + N' AND [Valid To] < ''1901-01-01'';';
            EXEC sys.sp_executesql @Sql;
        END;
    END TRY
    BEGIN CATCH
        EXEC [etl].[usp_LogError]
             @ErrorSeverity     = N'Warning',
             @SourceName        = N'90_unknown_members.sql',
             @ProcedureName     = N'ReservedMemberCursor',
             @ErrorDescription  = @Sql;
    END CATCH;

    FETCH NEXT FROM ReservedMemberCursor INTO @TableName, @KeyColumn, @ReservedKey, @Description;
END;

CLOSE ReservedMemberCursor;
DEALLOCATE ReservedMemberCursor;
GO
