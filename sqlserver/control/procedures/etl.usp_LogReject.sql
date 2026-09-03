/*
    Object        : [etl].[usp_LogReject]
    Deploy target : WWI_Staging and WideWorldImportersDW
    Deploy order  : after etl.usp_LogRejectedRecord.sql
    Depends on    : etl.RejectedRecord, etl.usp_LogRejectedRecord
    Called by     : the dimension bridge loads and the older staging procedures
                    that were written against the short signature

    A narrow, positional front end to etl.usp_LogRejectedRecord kept because a
    large amount of loader code calls reject logging with four arguments inline
    inside a cursor and nobody wanted to touch all of it again. New code should
    call etl.usp_LogRejectedRecord directly - it takes the payload, the stage and
    the SSIS error columns, which this wrapper cannot express.
*/

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'etl.usp_LogReject', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_LogReject;
GO

CREATE PROCEDURE etl.usp_LogReject
(
    @ObjectName         NVARCHAR(200),
    @BusinessKey        NVARCHAR(200)   = NULL,
    @RejectReasonCode   NVARCHAR(50)    = N'UNSPECIFIED',
    @RejectReason       NVARCHAR(500)   = NULL,
    @BatchId            BIGINT          = NULL,
    @PackageExecutionId BIGINT          = NULL,
    @RejectStage        NVARCHAR(50)    = N'Stage'
)
AS
BEGIN
    SET NOCOUNT ON;

    EXEC etl.usp_LogRejectedRecord
         @PackageExecutionId = @PackageExecutionId,
         @BatchId            = @BatchId,
         @ObjectName         = @ObjectName,
         @BusinessKey        = @BusinessKey,
         @RejectReasonCode   = @RejectReasonCode,
         @RejectReason       = @RejectReason,
         @RejectStage        = @RejectStage;

    RETURN 0;
END
GO
