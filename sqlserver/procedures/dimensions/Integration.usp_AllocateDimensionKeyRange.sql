/*
    Object        : [Integration].[usp_AllocateDimensionKeyRange]
    Deploy target : WideWorldImportersDW
    Depends on    : Integration.DimensionKeyRegistry
    Called by     : Integration.usp_InsertInferredMember, the fact loads

    Hands out a contiguous block of surrogate keys for a dimension that is
    registered with the Registry allocation method, and for a Sequence dimension
    pulls the block off the sequence instead so the two mechanisms cannot collide.

    Written in 2014 when the fact loads were round-tripping to a sequence per row
    and the nightly window stopped fitting. The update-with-assignment trick below
    reads and advances the counter in one statement under an update lock, which
    is how this shop has always done counter allocation.

    Reserved keys (-9 .. 0) are never handed out: the registry stores the reserved
    range per dimension and the next key starts above it.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_AllocateDimensionKeyRange]
    @DimensionName      NVARCHAR(100),
    @RequestedCount     INT,
    @FirstKey           INT           OUTPUT,
    @LastKey            INT           OUTPUT,
    @RequestedBy        NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @AllocationMethod   NVARCHAR(20);
    DECLARE @SequenceName       SYSNAME;
    DECLARE @ReservedHigh       INT;
    DECLARE @Sql                NVARCHAR(MAX);

    SET @FirstKey = NULL;
    SET @LastKey  = NULL;

    IF @RequestedCount IS NULL OR @RequestedCount < 1
        SET @RequestedCount = 1;

    SELECT @AllocationMethod = r.[Allocation Method],
           @SequenceName     = r.[Sequence Name],
           @ReservedHigh     = r.[Reserved Key High]
    FROM [Integration].[DimensionKeyRegistry] AS r
    WHERE r.[Dimension Name] = @DimensionName;

    IF @AllocationMethod IS NULL
    BEGIN
        RAISERROR(N'Dimension %s is not present in Integration.DimensionKeyRegistry; no key range can be allocated.',
                  16, 1, @DimensionName);
        RETURN;
    END;

    IF @AllocationMethod = N'Identity'
    BEGIN
        /* Identity dimensions allocate their own keys on insert. Callers still
           ask for a range so that the fact loads can be written one way, so this
           returns a null range rather than failing. */
        RETURN;
    END;

    IF @AllocationMethod = N'Sequence' AND @SequenceName IS NOT NULL
    BEGIN
        DECLARE @RangeStart SQL_VARIANT;

        EXEC sys.sp_sequence_get_range
             @sequence_name     = @SequenceName,
             @range_size        = @RequestedCount,
             @range_first_value = @RangeStart OUTPUT;

        SET @FirstKey = CONVERT(INT, @RangeStart);
        SET @LastKey  = @FirstKey + @RequestedCount - 1;
    END
    ELSE
    BEGIN
        BEGIN TRANSACTION;

        UPDATE [Integration].[DimensionKeyRegistry] WITH (UPDLOCK, ROWLOCK)
        SET @FirstKey        = CASE WHEN [Next Key] <= @ReservedHigh
                                    THEN @ReservedHigh + 1 ELSE [Next Key] END,
            [Next Key]       = CASE WHEN [Next Key] <= @ReservedHigh
                                    THEN @ReservedHigh + 1 ELSE [Next Key] END + @RequestedCount,
            [Last Allocated On] = SYSDATETIME(),
            [Last Allocated By] = ISNULL(@RequestedBy, SUSER_SNAME())
        WHERE [Dimension Name] = @DimensionName;

        SET @LastKey = @FirstKey + @RequestedCount - 1;

        COMMIT TRANSACTION;
    END;

    /* The block size on the registry row is advisory: it is what the operations
       team expect a fact load to take, and a request far larger than it is worth
       a note in the log rather than a failure. */
    IF EXISTS (SELECT 1
               FROM [Integration].[DimensionKeyRegistry]
               WHERE [Dimension Name] = @DimensionName
                 AND @RequestedCount > [Block Size] * 10)
        PRINT CONCAT(N'Key range request of ', @RequestedCount, N' for ', @DimensionName,
                     N' greatly exceeds the registered block size.');
END;
GO
