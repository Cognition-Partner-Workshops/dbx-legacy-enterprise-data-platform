/*
    ref.vw_UnmappedSourceCode

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the data-quality packages in ssis/12_data_quality,
                    REF_Load_CodeTranslation (SSIS)
    Reads         : err.RejectedLookupFailure, ref.CodeCrosswalk
    Depends on    : ref.usp_ReportUnmappedCodes, which writes the rows

    The steward-facing shape of the unmapped-code list. ref.usp_ReportUnmappedCodes
    writes each unmapped code into err.RejectedLookupFailure under reason code
    REF_UNMAPPED_CODE; this view collapses the per-batch rows into one row per
    domain, source system and code, so a code seen in twelve batches is one line
    on the report rather than twelve.

    A code disappears from the view as soon as a mapping is added, because the
    crosswalk is checked live rather than the reject row being updated - nothing
    in this estate goes back and closes a reject.
*/

IF OBJECT_ID(N'ref.vw_UnmappedSourceCode', N'V') IS NOT NULL
    DROP VIEW ref.vw_UnmappedSourceCode;
GO

CREATE VIEW ref.vw_UnmappedSourceCode
AS
SELECT
    r.CodeDomainCode,
    r.SourceSystemCode,
    r.SourceCodeValue,
    SourceObjectName    = MIN(r.SourceObjectName),
    SourceColumnName    = MIN(r.SourceColumnName),
    ObservationCount    = COUNT_BIG(*),
    TotalOccurrenceCount = SUM(CONVERT(BIGINT, r.OccurrenceCount)),
    FirstObservedAtUtc  = MIN(r.RejectedAtUtc),
    LastObservedAtUtc   = MAX(r.RejectedAtUtc),
    LastBatchId         = MAX(r.BatchId)
FROM
(
    --  The domain is carried in the reject payload written by
    --  ref.usp_ReportUnmappedCodes; it is pulled back out here because
    --  err.RejectedLookupFailure has no domain column of its own.
    SELECT
        CodeDomainCode = SUBSTRING(f.RecordPayload,
                                   CHARINDEX(N'"DOMAIN":"', f.RecordPayload) + 10,
                                   CHARINDEX(N'","CODE":"', f.RecordPayload)
                                       - CHARINDEX(N'"DOMAIN":"', f.RecordPayload) - 10),
        f.SourceSystemCode,
        SourceCodeValue = f.LookupValue,
        f.SourceObjectName,
        SourceColumnName = f.LookupColumnName,
        f.OccurrenceCount,
        f.RejectedAtUtc,
        f.BatchId
    FROM err.RejectedLookupFailure AS f
    WHERE f.RejectReasonCode = N'REF_UNMAPPED_CODE'
      AND CHARINDEX(N'"DOMAIN":"', f.RecordPayload) > 0
      AND CHARINDEX(N'","CODE":"', f.RecordPayload) > CHARINDEX(N'"DOMAIN":"', f.RecordPayload)
) AS r
WHERE NOT EXISTS
      (
          SELECT 1
          FROM ref.CodeCrosswalk AS x
          WHERE x.CodeDomainCode   = r.CodeDomainCode
            AND x.SourceSystemCode = r.SourceSystemCode
            AND x.SourceCodeValue  = r.SourceCodeValue
            AND x.EffectiveToDate IS NULL
      )
GROUP BY
    r.CodeDomainCode,
    r.SourceSystemCode,
    r.SourceCodeValue;
GO
