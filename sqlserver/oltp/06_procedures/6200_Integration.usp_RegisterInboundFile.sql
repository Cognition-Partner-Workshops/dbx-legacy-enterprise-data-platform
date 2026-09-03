/*
    Integration.usp_RegisterInboundFile

    Catalog entry : sqlserver_oltp.procedures - Integration.RegisterInboundFile
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6200 - after 6190
    Depends on    : Integration.InboundFileRegister
    Called by     : file landing job, SSIS inbound packages

    Registers an arriving file and decides whether it is a duplicate. The
    duplicate test is the file hash where one is supplied and the name plus
    business date where it is not; senders that rename their files by
    timestamp therefore never look like duplicates even when the content is
    identical.

    A duplicate is registered anyway, flagged against the original, so the
    audit trail shows what arrived rather than what was processed.
*/
CREATE PROCEDURE [Integration].[usp_RegisterInboundFile]
    @InterfaceCode      NVARCHAR (20),
    @FileName           NVARCHAR (200),
    @FileDirectory      NVARCHAR (300),
    @FileSizeBytes      BIGINT,
    @FileHashText       NVARCHAR (64) = NULL,
    @SenderCode         NVARCHAR (20) = NULL,
    @FileBusinessDate   DATE = NULL,
    @BatchID            BIGINT = NULL,
    @InboundFileID      BIGINT = NULL OUTPUT,
    @IsDuplicate        BIT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @DuplicateOfFileID  BIGINT;
    DECLARE @SequenceInDay      SMALLINT;

    IF @FileBusinessDate IS NULL
        SET @FileBusinessDate = CONVERT(DATE, SYSDATETIME());

    SELECT TOP (1) @DuplicateOfFileID = f.[InboundFileID]
    FROM [Integration].[InboundFileRegister] AS f
    WHERE f.[InterfaceCode] = @InterfaceCode
        AND
        (
            (@FileHashText IS NOT NULL AND f.[FileHashText] = @FileHashText)
            OR (@FileHashText IS NULL AND f.[FileName] = @FileName AND f.[FileBusinessDate] = @FileBusinessDate)
        )
    ORDER BY f.[InboundFileID] ASC;

    SET @IsDuplicate = CASE WHEN @DuplicateOfFileID IS NULL THEN 0 ELSE 1 END;

    SELECT @SequenceInDay = ISNULL(MAX(f.[SequenceNumberInDay]), 0) + 1
    FROM [Integration].[InboundFileRegister] AS f
    WHERE f.[InterfaceCode] = @InterfaceCode
        AND f.[FileBusinessDate] = @FileBusinessDate;

    INSERT INTO [Integration].[InboundFileRegister]
    (
        [InterfaceCode], [FileName], [FileDirectory], [FileSizeBytes], [FileHashText],
        [SenderCode], [ReceivedWhen], [FileBusinessDate], [SequenceNumberInDay],
        [ProcessingStatus], [IsDuplicateOfFileID], [BatchID], [RetryCount]
    )
    VALUES
    (
        @InterfaceCode, @FileName, @FileDirectory, @FileSizeBytes, @FileHashText,
        @SenderCode, SYSDATETIME(), @FileBusinessDate, @SequenceInDay,
        CASE WHEN @IsDuplicate = 1 THEN N'DUPLICATE' ELSE N'REGISTERED' END,
        @DuplicateOfFileID, @BatchID, 0
    );

    SET @InboundFileID = SCOPE_IDENTITY();
END
GO
