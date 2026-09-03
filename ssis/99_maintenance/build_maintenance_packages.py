"""Emit the WWI_Maintenance housekeeping packages (ssis/99_maintenance).

Housekeeping the estate has grown over the years: retention purges for the
staging and control tables, index and columnstore maintenance, statistics
refresh, processed-file archival, a configuration assertion and the pre-flight
disk space check the operators added after the 2014 outage.

Run:  python3 ssis/99_maintenance/build_maintenance_packages.py
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import project  # noqa: E402
from patterns import (CONN_DW, CONN_FILES, CONN_STAGING, exec_proc,  # noqa: E402
                      log_package_start, log_package_success, log_row_count,
                      new_package, truncate)
from ssisgen import (Column, Container, DataFlow, DataFlowTask, ExecuteSql,  # noqa: E402
                     Expression, FileSystemTask, date_col, int_col, money_col,
                     str_col)

PROJECT_NAME = "WWI_Maintenance"
CONNECTIONS = ["WWI_Staging_DB", "WWI_DW_Destination_DB", "WWI_Inbound_Files",
               "WWI_Archive_Files"]


def bool_col(name):
    return Column(name, "bool")


# ---------------------------------------------------------------------------
# MNT_Purge_StagingHistory
# ---------------------------------------------------------------------------


def build_mnt_purge_staginghistory():
    pkg = new_package(
        "MNT_Purge_StagingHistory",
        "Purges raw and work staging tables beyond their retention. Retention is per object and "
        "per source system because the finance feeds are kept far longer than the operational "
        "ones for audit, and the file-derived partner feeds are kept until the partner has "
        "confirmed settlement. Deletes are chunked so the staging log does not grow unbounded.",
        connections=(CONN_DW,),
        extra_variables=[("PurgeObjectName", "", "string"), ("PurgeSql", "", "string"),
                         ("PurgedRowCount", 0, "int"), ("RetentionDays", 90, "int")],
    )
    pkg.add_parameter("DefaultRetentionDays", 90, dtype="int",
                      description="Retention applied when the object has no explicit rule.")
    pkg.add_parameter("DeleteChunkRows", 50000, dtype="int",
                      description="Rows deleted per statement so the log stays bounded.")
    pkg.add_parameter("DryRun", "False", dtype="bool",
                      description="When True the purge only reports what it would delete.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.StagingPurgePlan"))
    plan = pkg.add(ExecuteSql(
        "Build Purge Plan",
        CONN_STAGING,
        "INSERT INTO work.StagingPurgePlan\n"
        "    (SchemaName, TableName, LoadDateColumn, RetentionDays, CutoffDate, EstimatedRows)\n"
        "SELECT  t.SchemaName\n"
        ",       t.TableName\n"
        ",       ISNULL(t.LoadDateColumn, N'LoadedAtUtc')\n"
        # Finance keeps seven years for audit, the partner file feeds keep a year
        # until settlement is confirmed, everything else takes the default.
        ",       CASE\n"
        "            WHEN t.SchemaName = N'raw' AND t.TableName LIKE N'Fin%'   THEN 2555\n"
        "            WHEN t.TableName LIKE N'%Partner%'                        THEN 365\n"
        "            WHEN t.SchemaName = N'work'                               THEN 14\n"
        "            ELSE ?\n"
        "        END\n"
        ",       DATEADD(DAY, -1 * CASE\n"
        "            WHEN t.SchemaName = N'raw' AND t.TableName LIKE N'Fin%'   THEN 2555\n"
        "            WHEN t.TableName LIKE N'%Partner%'                        THEN 365\n"
        "            WHEN t.SchemaName = N'work'                               THEN 14\n"
        "            ELSE ? END, CAST(SYSUTCDATETIME() AS date))\n"
        ",       t.ApproximateRowCount\n"
        "FROM    etl.StagingTableRegister AS t\n"
        "WHERE   t.IsPurgeEligible = 1;",
        parameter_bindings=[
            ("$Package::DefaultRetentionDays", 0, "LONG"),
            ("$Package::DefaultRetentionDays", 1, "LONG"),
        ],
    ))

    purge_loop = Container(
        "Purge Each Staging Table",
        description="Row-by-row over the purge plan: legacy dynamic SQL, one table at a time.",
    )
    build_sql = purge_loop.add(ExecuteSql(
        "Build And Run Chunked Deletes",
        CONN_STAGING,
        "DECLARE @SchemaName sysname, @TableName sysname, @DateColumn sysname,\n"
        "        @CutoffDate date, @Sql nvarchar(max), @Chunk int = ?, @Deleted int = 1,\n"
        "        @DryRun bit = ?;\n"
        "DECLARE PurgeCursor CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT SchemaName, TableName, LoadDateColumn, CutoffDate\n"
        "    FROM   work.StagingPurgePlan ORDER BY EstimatedRows DESC;\n"
        "OPEN PurgeCursor;\n"
        "FETCH NEXT FROM PurgeCursor INTO @SchemaName, @TableName, @DateColumn, @CutoffDate;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    SET @Deleted = 1;\n"
        "    WHILE @Deleted > 0\n"
        "    BEGIN\n"
        "        SET @Sql = N'DELETE TOP (' + CAST(@Chunk AS nvarchar(20)) + N') FROM '\n"
        "                 + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName)\n"
        "                 + N' WHERE ' + QUOTENAME(@DateColumn) + N' < @Cutoff;';\n"
        "        IF @DryRun = 1 BREAK;\n"
        "        EXEC sp_executesql @Sql, N'@Cutoff date', @Cutoff = @CutoffDate;\n"
        "        SET @Deleted = @@ROWCOUNT;\n"
        "        INSERT INTO etl.PurgeAudit\n"
        "            (BatchId, SchemaName, TableName, CutoffDate, RowsDeleted, PurgedAtUtc)\n"
        "        VALUES (NULL, @SchemaName, @TableName, @CutoffDate, @Deleted, SYSUTCDATETIME());\n"
        "    END\n"
        "    FETCH NEXT FROM PurgeCursor INTO @SchemaName, @TableName, @DateColumn, @CutoffDate;\n"
        "END\n"
        "CLOSE PurgeCursor; DEALLOCATE PurgeCursor;",
        parameter_bindings=[
            ("$Package::DeleteChunkRows", 0, "LONG"),
            ("$Package::DryRun", 1, "BYTE"),
        ],
    ))
    measure = purge_loop.add(ExecuteSql(
        "Measure Purged Rows",
        CONN_STAGING,
        "SELECT ISNULL(SUM(RowsDeleted), 0) AS RowsDeleted FROM etl.PurgeAudit "
        "WHERE PurgedAtUtc >= DATEADD(HOUR, -6, SYSUTCDATETIME());",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::PurgedRowCount")],
    ))
    purge_loop.link(build_sql, measure)
    purge = pkg.add(purge_loop)

    shrink_note = pkg.add(ExecuteSql(
        "Record Space Reclaimed",
        CONN_STAGING,
        "INSERT INTO etl.MaintenanceLog (TaskName, DetailText, RecordedAtUtc) "
        "SELECT N'MNT_Purge_StagingHistory', "
        "       CONCAT(N'Rows deleted in this run: ', CAST(? AS nvarchar(20))), "
        "       SYSUTCDATETIME();",
        parameter_bindings=[("User::PurgedRowCount", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("work.StagingPurgePlan"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, plan)
    pkg.link(plan, purge)
    pkg.link(purge, shrink_note, value="Completion")
    pkg.link(shrink_note, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# MNT_Purge_ControlHistory
# ---------------------------------------------------------------------------


def build_mnt_purge_controlhistory():
    pkg = new_package(
        "MNT_Purge_ControlHistory",
        "Purges the etl control tables beyond retention, archiving the batch and error history "
        "into the archive tables first so the auditors keep a trail. Rejected records are only "
        "purged once they have been resolved or superseded, and watermark rows are never purged.",
        connections=(CONN_DW,),
        extra_variables=[("ArchivedBatchCount", 0, "int"), ("PurgedErrorCount", 0, "int"),
                         ("PurgedRejectCount", 0, "int")],
    )
    pkg.add_parameter("BatchRetentionDays", 400, dtype="int",
                      description="Batch and step history retention.")
    pkg.add_parameter("ErrorRetentionDays", 180, dtype="int",
                      description="Error log retention.")
    pkg.add_parameter("RejectRetentionDays", 90, dtype="int",
                      description="Resolved reject retention.")

    start = pkg.add(log_package_start(pkg))
    archive_batches = pkg.add(ExecuteSql(
        "Archive Batch History",
        CONN_STAGING,
        "INSERT INTO etl.BatchArchive (BatchId, BatchType, BatchStatus, StartedAtUtc, EndedAtUtc, "
        "                              StepCount, FailedStepCount, ArchivedAtUtc) "
        "SELECT b.BatchId, b.BatchType, b.BatchStatus, b.StartedAtUtc, b.EndedAtUtc, "
        "       (SELECT COUNT(*) FROM etl.BatchStep AS s WHERE s.BatchId = b.BatchId), "
        "       (SELECT COUNT(*) FROM etl.BatchStep AS s WHERE s.BatchId = b.BatchId "
        "        AND s.StepStatus = N'Failed'), "
        "       SYSUTCDATETIME() "
        "FROM   etl.Batch AS b "
        "WHERE  b.EndedAtUtc < DATEADD(DAY, -1 * ?, SYSUTCDATETIME()) "
        "AND    NOT EXISTS (SELECT 1 FROM etl.BatchArchive AS a WHERE a.BatchId = b.BatchId);",
        parameter_bindings=[("$Package::BatchRetentionDays", 0, "LONG")],
    ))
    purge_steps = pkg.add(ExecuteSql(
        "Purge Batch Steps",
        CONN_STAGING,
        "DELETE s FROM etl.BatchStep AS s "
        "INNER JOIN etl.BatchArchive AS a ON a.BatchId = s.BatchId;",
    ))
    purge_batches = pkg.add(ExecuteSql(
        "Purge Batches",
        CONN_STAGING,
        "DELETE b FROM etl.Batch AS b "
        "INNER JOIN etl.BatchArchive AS a ON a.BatchId = b.BatchId "
        "WHERE b.BatchStatus <> N'Running';",
    ))
    purge_errors = pkg.add(ExecuteSql(
        "Purge Error Log",
        CONN_STAGING,
        "DELETE FROM etl.ErrorLog "
        "WHERE LoggedAtUtc < DATEADD(DAY, -1 * ?, SYSUTCDATETIME());",
        parameter_bindings=[("$Package::ErrorRetentionDays", 0, "LONG")],
    ))
    purge_rejects = pkg.add(ExecuteSql(
        "Purge Resolved Rejects",
        CONN_STAGING,
        "DELETE FROM etl.RejectedRecord "
        "WHERE IsReprocessed = 1 "
        "AND   RejectedAtUtc < DATEADD(DAY, -1 * ?, SYSUTCDATETIME());",
        parameter_bindings=[("$Package::RejectRetentionDays", 0, "LONG")],
    ))
    purge_exec = pkg.add(exec_proc(
        "Purge Control History Proc",
        "EXEC etl.usp_PurgeControlHistory @RetentionDays = ?;",
        parameter_bindings=[("$Package::BatchRetentionDays", 0, "LONG")],
    ))
    measure = pkg.add(ExecuteSql(
        "Measure Control Purge",
        CONN_STAGING,
        "SELECT (SELECT COUNT(*) FROM etl.BatchArchive) AS ArchivedBatches, "
        "       (SELECT COUNT(*) FROM etl.ErrorLog) AS RemainingErrors, "
        "       (SELECT COUNT(*) FROM etl.RejectedRecord) AS RemainingRejects;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::ArchivedBatchCount"), ("1", "User::PurgedErrorCount"),
                         ("2", "User::PurgedRejectCount")],
    ))
    counts = pkg.add(log_row_count("etl.Batch"))
    done = pkg.add(log_package_success())

    pkg.link(start, archive_batches)
    pkg.link(archive_batches, purge_steps)
    pkg.link(purge_steps, purge_batches)
    pkg.link(purge_batches, purge_errors)
    pkg.link(purge_errors, purge_rejects)
    pkg.link(purge_rejects, purge_exec)
    pkg.link(purge_exec, measure, value="Completion")
    pkg.link(measure, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# MNT_Rebuild_Indexes
# ---------------------------------------------------------------------------


def build_mnt_rebuild_indexes():
    pkg = new_package(
        "MNT_Rebuild_Indexes",
        "Index and columnstore maintenance for the warehouse. Rowstore indexes are reorganised "
        "between 10 and 30 percent fragmentation and rebuilt above that; columnstore row groups "
        "are rebuilt when the deleted-row ratio or the open row-group count crosses the "
        "thresholds the DBAs settled on. The work is ordered by table size so the largest fact "
        "tables get the maintenance window first.",
        connections=(CONN_DW,),
        extra_variables=[("IndexActionCount", 0, "int"), ("RebuiltCount", 0, "int"),
                         ("ReorganisedCount", 0, "int")],
    )
    pkg.add_parameter("ReorganiseThresholdPercent", 10, dtype="int",
                      description="Fragmentation at which an index is reorganised.")
    pkg.add_parameter("RebuildThresholdPercent", 30, dtype="int",
                      description="Fragmentation at which an index is rebuilt.")
    pkg.add_parameter("MaxDurationMinutes", 180, dtype="int",
                      description="Maintenance window; work stops when it is exhausted.")
    pkg.add_parameter("OnlineRebuild", "False", dtype="bool",
                      description="Enterprise edition only; the DR box runs offline rebuilds.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.IndexMaintenancePlan", connection=CONN_DW))
    survey = pkg.add(ExecuteSql(
        "Survey Index Fragmentation",
        CONN_DW,
        "INSERT INTO work.IndexMaintenancePlan\n"
        "    (SchemaName, TableName, IndexName, IndexTypeCode, FragmentationPercent,\n"
        "     PageCount, PlannedAction)\n"
        "SELECT  SCHEMA_NAME(o.schema_id)\n"
        ",       o.name\n"
        ",       i.name\n"
        ",       CASE WHEN i.type IN (5, 6) THEN N'COLUMNSTORE' ELSE N'ROWSTORE' END\n"
        ",       s.avg_fragmentation_in_percent\n"
        ",       s.page_count\n"
        ",       CASE\n"
        "            WHEN i.type IN (5, 6) THEN N'COLUMNSTORE_REBUILD'\n"
        "            WHEN s.avg_fragmentation_in_percent >= ? THEN N'REBUILD'\n"
        "            WHEN s.avg_fragmentation_in_percent >= ? THEN N'REORGANIZE'\n"
        "            ELSE N'NONE'\n"
        "        END\n"
        "FROM    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, N'LIMITED') AS s\n"
        "INNER JOIN sys.indexes AS i ON i.object_id = s.object_id AND i.index_id = s.index_id\n"
        "INNER JOIN sys.objects AS o ON o.object_id = i.object_id\n"
        "WHERE   o.is_ms_shipped = 0\n"
        "AND     i.name IS NOT NULL\n"
        "AND     s.page_count > 1000;",
        parameter_bindings=[
            ("$Package::RebuildThresholdPercent", 0, "LONG"),
            ("$Package::ReorganiseThresholdPercent", 1, "LONG"),
        ],
        timeout=3600,
    ))
    rowstore = pkg.add(ExecuteSql(
        "Maintain Rowstore Indexes",
        CONN_DW,
        "DECLARE @Schema sysname, @Table sysname, @Index sysname, @Action nvarchar(30),\n"
        "        @Sql nvarchar(max), @Online bit = ?, @Deadline datetime2(0);\n"
        "SET @Deadline = DATEADD(MINUTE, ?, SYSUTCDATETIME());\n"
        "DECLARE IndexCursor CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT SchemaName, TableName, IndexName, PlannedAction\n"
        "    FROM   work.IndexMaintenancePlan\n"
        "    WHERE  IndexTypeCode = N'ROWSTORE' AND PlannedAction <> N'NONE'\n"
        "    ORDER BY PageCount DESC;\n"
        "OPEN IndexCursor;\n"
        "FETCH NEXT FROM IndexCursor INTO @Schema, @Table, @Index, @Action;\n"
        "WHILE @@FETCH_STATUS = 0 AND SYSUTCDATETIME() < @Deadline\n"
        "BEGIN\n"
        "    SET @Sql = N'ALTER INDEX ' + QUOTENAME(@Index) + N' ON '\n"
        "             + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table)\n"
        "             + CASE WHEN @Action = N'REBUILD'\n"
        "                    THEN N' REBUILD WITH (ONLINE = '\n"
        "                         + CASE WHEN @Online = 1 THEN N'ON' ELSE N'OFF' END\n"
        "                         + N', SORT_IN_TEMPDB = ON);'\n"
        "                    ELSE N' REORGANIZE;' END;\n"
        "    BEGIN TRY\n"
        "        EXEC sp_executesql @Sql;\n"
        "        INSERT INTO etl.MaintenanceLog (TaskName, DetailText, RecordedAtUtc)\n"
        "        VALUES (N'MNT_Rebuild_Indexes', @Sql, SYSUTCDATETIME());\n"
        "    END TRY\n"
        "    BEGIN CATCH\n"
        "        INSERT INTO etl.MaintenanceLog (TaskName, DetailText, RecordedAtUtc)\n"
        "        VALUES (N'MNT_Rebuild_Indexes',\n"
        "                CONCAT(N'FAILED: ', @Sql, N' -> ', ERROR_MESSAGE()), SYSUTCDATETIME());\n"
        "    END CATCH\n"
        "    FETCH NEXT FROM IndexCursor INTO @Schema, @Table, @Index, @Action;\n"
        "END\n"
        "CLOSE IndexCursor; DEALLOCATE IndexCursor;",
        parameter_bindings=[
            ("$Package::OnlineRebuild", 0, "BYTE"),
            ("$Package::MaxDurationMinutes", 1, "LONG"),
        ],
        timeout=0,
    ))
    columnstore = pkg.add(exec_proc(
        "Rebuild Columnstore Indexes",
        "EXEC Integration.RebuildColumnstoreIndexes @MaxDurationMinutes = ?;",
        connection=CONN_DW,
        parameter_bindings=[("$Package::MaxDurationMinutes", 0, "LONG")],
    ))
    measure = pkg.add(ExecuteSql(
        "Measure Index Actions",
        CONN_DW,
        "SELECT COUNT(*) AS PlannedActions, "
        "       SUM(CASE WHEN PlannedAction = N'REBUILD' THEN 1 ELSE 0 END) AS Rebuilt, "
        "       SUM(CASE WHEN PlannedAction = N'REORGANIZE' THEN 1 ELSE 0 END) AS Reorganised "
        "FROM   work.IndexMaintenancePlan WHERE PlannedAction <> N'NONE';",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::IndexActionCount"), ("1", "User::RebuiltCount"),
                         ("2", "User::ReorganisedCount")],
    ))
    counts = pkg.add(log_row_count("work.IndexMaintenancePlan"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, survey)
    pkg.link(survey, rowstore)
    pkg.link(rowstore, columnstore, value="Completion")
    pkg.link(columnstore, measure, value="Completion")
    pkg.link(measure, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# MNT_Update_Statistics
# ---------------------------------------------------------------------------


def build_mnt_update_statistics():
    pkg = new_package(
        "MNT_Update_Statistics",
        "Statistics refresh after the nightly load. Statistics on the incrementally loaded fact "
        "tables are sampled, the slowly changing dimensions are updated with a full scan because "
        "the optimiser kept choosing bad plans for the type-2 date predicates, and anything "
        "untouched by the batch is skipped so the window is not wasted.",
        connections=(CONN_DW,),
        extra_variables=[("StatisticsUpdatedCount", 0, "int"), ("FullScanCount", 0, "int")],
    )
    pkg.add_parameter("ModificationThresholdRows", 5000, dtype="int",
                      description="Row modifications before statistics are refreshed.")
    pkg.add_parameter("SamplePercent", 20, dtype="int",
                      description="Sample percentage for the large fact tables.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.StatisticsRefreshPlan", connection=CONN_DW))
    plan = pkg.add(ExecuteSql(
        "Plan Statistics Refresh",
        CONN_DW,
        "INSERT INTO work.StatisticsRefreshPlan\n"
        "    (SchemaName, TableName, StatisticsName, ModifiedRowCount, RefreshMode)\n"
        "SELECT  SCHEMA_NAME(o.schema_id)\n"
        ",       o.name\n"
        ",       st.name\n"
        ",       sp.modification_counter\n"
        # The type-2 dimensions get a full scan; the big facts are sampled.
        ",       CASE WHEN o.name LIKE N'Dim%' THEN N'FULLSCAN' ELSE N'SAMPLE' END\n"
        "FROM    sys.stats AS st\n"
        "INNER JOIN sys.objects AS o ON o.object_id = st.object_id\n"
        "CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) AS sp\n"
        "WHERE   o.is_ms_shipped = 0\n"
        "AND     sp.modification_counter >= ?;",
        parameter_bindings=[("$Package::ModificationThresholdRows", 0, "LONG")],
        timeout=1800,
    ))
    refresh = pkg.add(ExecuteSql(
        "Refresh Statistics",
        CONN_DW,
        "DECLARE @Schema sysname, @Table sysname, @Stat sysname, @Mode nvarchar(20),\n"
        "        @Sample int = ?, @Sql nvarchar(max);\n"
        "DECLARE StatsCursor CURSOR LOCAL FAST_FORWARD FOR\n"
        "    SELECT SchemaName, TableName, StatisticsName, RefreshMode\n"
        "    FROM   work.StatisticsRefreshPlan ORDER BY ModifiedRowCount DESC;\n"
        "OPEN StatsCursor;\n"
        "FETCH NEXT FROM StatsCursor INTO @Schema, @Table, @Stat, @Mode;\n"
        "WHILE @@FETCH_STATUS = 0\n"
        "BEGIN\n"
        "    SET @Sql = N'UPDATE STATISTICS ' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table)\n"
        "             + N' ' + QUOTENAME(@Stat)\n"
        "             + CASE WHEN @Mode = N'FULLSCAN' THEN N' WITH FULLSCAN;'\n"
        "                    ELSE N' WITH SAMPLE ' + CAST(@Sample AS nvarchar(10))\n"
        "                         + N' PERCENT;' END;\n"
        "    EXEC sp_executesql @Sql;\n"
        "    FETCH NEXT FROM StatsCursor INTO @Schema, @Table, @Stat, @Mode;\n"
        "END\n"
        "CLOSE StatsCursor; DEALLOCATE StatsCursor;",
        parameter_bindings=[("$Package::SamplePercent", 0, "LONG")],
        timeout=0,
    ))
    measure = pkg.add(ExecuteSql(
        "Measure Statistics Refresh",
        CONN_DW,
        "SELECT COUNT(*) AS Refreshed, "
        "       SUM(CASE WHEN RefreshMode = N'FULLSCAN' THEN 1 ELSE 0 END) AS FullScans "
        "FROM   work.StatisticsRefreshPlan;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::StatisticsUpdatedCount"), ("1", "User::FullScanCount")],
    ))
    log_it = pkg.add(ExecuteSql(
        "Log Statistics Outcome",
        CONN_STAGING,
        "INSERT INTO etl.MaintenanceLog (TaskName, DetailText, RecordedAtUtc) "
        "SELECT N'MNT_Update_Statistics', "
        "       CONCAT(N'Statistics refreshed: ', CAST(? AS nvarchar(20))), SYSUTCDATETIME();",
        parameter_bindings=[("User::StatisticsUpdatedCount", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("work.StatisticsRefreshPlan"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, plan)
    pkg.link(plan, refresh)
    pkg.link(refresh, measure, value="Completion")
    pkg.link(measure, log_it)
    pkg.link(log_it, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# MNT_Archive_ProcessedFiles
# ---------------------------------------------------------------------------


def build_mnt_archive_processedfiles():
    pkg = new_package(
        "MNT_Archive_ProcessedFiles",
        "Archives inbound files that have been processed successfully. Files are moved to the "
        "archive share into a year/month folder, the register is stamped, and files already "
        "beyond the archive retention are listed for deletion by the storage team - the SSIS "
        "service account has never been granted delete on the archive share.",
        connections=(CONN_FILES,),
        extra_variables=[("ArchiveRoot", "archive", "string"), ("CurrentFile", "", "string"),
                         ("ArchivedFileCount", 0, "int"), ("ExpiredFileCount", 0, "int")],
    )
    pkg.add_parameter("ArchiveRetentionDays", 730, dtype="int",
                      description="Age at which an archived file is listed for deletion.")
    pkg.add_parameter("MinimumFileAgeHours", 6, dtype="int",
                      description="Files must settle before they are moved.")

    start = pkg.add(log_package_start(pkg))
    build_root = pkg.add(Expression(
        "Build Archive Root",
        '@[User::ArchiveRoot] = "archive\\\\" + (DT_WSTR,4)YEAR(GETDATE()) + "\\\\" + '
        'RIGHT("0" + (DT_WSTR,2)MONTH(GETDATE()), 2)',
    ))
    select_files = pkg.add(ExecuteSql(
        "Select Processed Files",
        CONN_STAGING,
        "INSERT INTO work.FileArchiveQueue (FileName, FilePath, FeedCode, ProcessedAtUtc)\n"
        "SELECT  f.FileName, f.FilePath, f.FeedCode, f.ProcessedAtUtc\n"
        "FROM    etl.InboundFileRegister AS f\n"
        "WHERE   f.ProcessingStatus = N'Processed'\n"
        "AND     f.IsArchived = 0\n"
        "AND     f.ProcessedAtUtc < DATEADD(HOUR, -1 * ?, SYSUTCDATETIME());",
        parameter_bindings=[("$Package::MinimumFileAgeHours", 0, "LONG")],
    ))

    move_loop = Container(
        "Archive Each File",
        kind="foreach",
        enumerator={"folder": "%INBOUND_FILE_ROOT%\\processed", "file_spec": "*.*"},
        variable_mappings=["User::CurrentFile"],
        description="Moves each settled processed file into the dated archive folder.",
    )
    move = move_loop.add(FileSystemTask(
        "Move File To Archive",
        operation="MoveFile",
        source_variable="User::CurrentFile",
        destination_variable="User::ArchiveRoot",
    ))
    stamp = move_loop.add(ExecuteSql(
        "Stamp File Archived",
        CONN_STAGING,
        "UPDATE etl.InboundFileRegister "
        "SET    IsArchived = 1, ArchivedAtUtc = SYSUTCDATETIME(), ArchivePath = ? "
        "WHERE  FilePath = ?;",
        parameter_bindings=[("User::ArchiveRoot", 0, "NVARCHAR"),
                            ("User::CurrentFile", 1, "NVARCHAR")],
    ))
    move_loop.link(move, stamp, value="Completion")
    loop = pkg.add(move_loop)

    expired = pkg.add(ExecuteSql(
        "List Expired Archive Files",
        CONN_STAGING,
        "INSERT INTO etl.ArchiveExpiryList (FileName, ArchivePath, ArchivedAtUtc, ListedAtUtc) "
        "SELECT FileName, ArchivePath, ArchivedAtUtc, SYSUTCDATETIME() "
        "FROM   etl.InboundFileRegister "
        "WHERE  IsArchived = 1 "
        "AND    ArchivedAtUtc < DATEADD(DAY, -1 * ?, SYSUTCDATETIME()) "
        "AND    NOT EXISTS (SELECT 1 FROM etl.ArchiveExpiryList AS l "
        "                   WHERE l.ArchivePath = etl.InboundFileRegister.ArchivePath);",
        parameter_bindings=[("$Package::ArchiveRetentionDays", 0, "LONG")],
    ))
    measure = pkg.add(ExecuteSql(
        "Measure Archive Run",
        CONN_STAGING,
        "SELECT (SELECT COUNT(*) FROM work.FileArchiveQueue) AS QueuedFiles, "
        "       (SELECT COUNT(*) FROM etl.ArchiveExpiryList "
        "        WHERE ListedAtUtc >= DATEADD(HOUR, -6, SYSUTCDATETIME())) AS ExpiredFiles;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::ArchivedFileCount"), ("1", "User::ExpiredFileCount")],
    ))
    counts = pkg.add(log_row_count("etl.InboundFileRegister"))
    done = pkg.add(log_package_success())

    pkg.link(start, build_root)
    pkg.link(build_root, select_files)
    pkg.link(select_files, loop)
    pkg.link(loop, expired, value="Completion")
    pkg.link(expired, measure)
    pkg.link(measure, counts)
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# MNT_Validate_Configuration
# ---------------------------------------------------------------------------


def build_mnt_validate_configuration():
    pkg = new_package(
        "MNT_Validate_Configuration",
        "Asserts that every configuration key the estate depends on exists and is plausible "
        "before the nightly batch starts: connection placeholders are populated per environment, "
        "the open finance period matches the calendar, the FX rates for the business date have "
        "landed, and the regional reference code sets are all present. Missing keys are raised "
        "as configuration defects rather than discovered mid-load.",
        connections=(CONN_DW,),
        extra_variables=[("MissingKeyCount", 0, "int"), ("SuspectValueCount", 0, "int"),
                         ("ConfigurationStatus", "UNKNOWN", "string")],
    )
    pkg.add_parameter("EnvironmentCode", "DEV", dtype="string",
                      description="Environment whose configuration is validated.")
    pkg.add_parameter("FailOnMissingKey", "True", dtype="bool",
                      description="Missing mandatory keys fail the package.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.ConfigurationValidation"))
    required = pkg.add(ExecuteSql(
        "Check Required Keys",
        CONN_STAGING,
        "INSERT INTO work.ConfigurationValidation\n"
        "    (ConfigurationKey, EnvironmentCode, CheckTypeCode, CheckStatus, DetailText)\n"
        "SELECT  r.ConfigurationKey\n"
        ",       ?\n"
        ",       N'REQUIRED'\n"
        ",       CASE WHEN c.ConfigurationValue IS NULL THEN N'MISSING'\n"
        "             WHEN LTRIM(RTRIM(c.ConfigurationValue)) = N'' THEN N'EMPTY'\n"
        "             ELSE N'OK' END\n"
        ",       CASE WHEN c.ConfigurationValue IS NULL\n"
        "             THEN N'Key is not defined for this environment' ELSE NULL END\n"
        "FROM    etl.RequiredConfigurationKey AS r\n"
        "LEFT OUTER JOIN etl.Configuration AS c\n"
        "       ON  c.ConfigurationKey = r.ConfigurationKey\n"
        "       AND c.EnvironmentCode = ?\n"
        "WHERE   r.IsMandatory = 1;",
        parameter_bindings=[
            ("$Package::EnvironmentCode", 0, "NVARCHAR"),
            ("$Package::EnvironmentCode", 1, "NVARCHAR"),
        ],
    ))
    placeholders = pkg.add(ExecuteSql(
        "Check Connection Placeholders",
        CONN_STAGING,
        "INSERT INTO work.ConfigurationValidation\n"
        "    (ConfigurationKey, EnvironmentCode, CheckTypeCode, CheckStatus, DetailText)\n"
        "SELECT  k.PlaceholderName\n"
        ",       ?\n"
        ",       N'PLACEHOLDER'\n"
        ",       CASE WHEN c.ConfigurationValue IS NULL THEN N'MISSING' ELSE N'OK' END\n"
        ",       N'Connection placeholder resolved from the environment, never stored inline'\n"
        "FROM    (VALUES (N'ORACLE_HOST'), (N'ORACLE_PORT'), (N'ORACLE_SERVICE'),\n"
        "                (N'ORACLE_USER'), (N'SQLSERVER_HOST'), (N'SQLSERVER_PORT'),\n"
        "                (N'SQLSERVER_USER'), (N'SQLSERVER_OLTP_DB'),\n"
        "                (N'SQLSERVER_STAGING_DB'), (N'SQLSERVER_DW_DB'))\n"
        "            AS k(PlaceholderName)\n"
        "LEFT OUTER JOIN etl.Configuration AS c\n"
        "       ON  c.ConfigurationKey = k.PlaceholderName\n"
        "       AND c.EnvironmentCode = ?;",
        parameter_bindings=[
            ("$Package::EnvironmentCode", 0, "NVARCHAR"),
            ("$Package::EnvironmentCode", 1, "NVARCHAR"),
        ],
    ))
    plausibility = pkg.add(ExecuteSql(
        "Check Value Plausibility",
        CONN_STAGING,
        "INSERT INTO work.ConfigurationValidation\n"
        "    (ConfigurationKey, EnvironmentCode, CheckTypeCode, CheckStatus, DetailText)\n"
        # The three regions each keep their own open period and reference sets, so the
        # plausibility rules differ by region rather than being one global check.
        "SELECT  N'OPEN_PERIOD_' + p.RegionCode\n"
        ",       ?\n"
        ",       N'PLAUSIBILITY'\n"
        ",       CASE WHEN p.OpenPeriodKey IS NULL THEN N'MISSING'\n"
        "             WHEN p.RegionCode = N'APAC' AND p.FiscalCalendarCode <> N'445'\n"
        "                  THEN N'SUSPECT'\n"
        "             WHEN p.RegionCode = N'EU' AND p.VatRegimeCode IS NULL THEN N'SUSPECT'\n"
        "             WHEN p.OpenPeriodKey < CONVERT(int, FORMAT(DATEADD(MONTH, -2,\n"
        "                  SYSUTCDATETIME()), N'yyyyMM')) THEN N'SUSPECT'\n"
        "             ELSE N'OK' END\n"
        ",       CONCAT(N'Region ', p.RegionCode, N' open period ',\n"
        "                CAST(ISNULL(p.OpenPeriodKey, 0) AS nvarchar(10)))\n"
        "FROM    etl.RegionPeriodStatus AS p\n"
        "UNION ALL\n"
        "SELECT  N'FX_RATES_' + r.RegionCode, ?, N'PLAUSIBILITY'\n"
        ",       CASE WHEN r.RateCount = 0 THEN N'MISSING' ELSE N'OK' END\n"
        ",       CONCAT(N'FX rates loaded for the business date: ',\n"
        "                CAST(r.RateCount AS nvarchar(10)))\n"
        "FROM    (SELECT RegionCode, COUNT(*) AS RateCount\n"
        "         FROM   etl.FxRateAvailability\n"
        "         WHERE  RateDate = CAST(SYSUTCDATETIME() AS date)\n"
        "         GROUP BY RegionCode) AS r;",
        parameter_bindings=[
            ("$Package::EnvironmentCode", 0, "NVARCHAR"),
            ("$Package::EnvironmentCode", 1, "NVARCHAR"),
        ],
    ))
    measure = pkg.add(ExecuteSql(
        "Measure Configuration Defects",
        CONN_STAGING,
        "SELECT SUM(CASE WHEN CheckStatus IN (N'MISSING', N'EMPTY') THEN 1 ELSE 0 END) AS Missing, "
        "       SUM(CASE WHEN CheckStatus = N'SUSPECT' THEN 1 ELSE 0 END) AS Suspect, "
        "       CASE WHEN SUM(CASE WHEN CheckStatus IN (N'MISSING', N'EMPTY') "
        "                          THEN 1 ELSE 0 END) > 0 THEN N'DEFECTIVE' "
        "            WHEN SUM(CASE WHEN CheckStatus = N'SUSPECT' THEN 1 ELSE 0 END) > 0 "
        "                 THEN N'SUSPECT' ELSE N'OK' END AS ConfigurationStatus "
        "FROM   work.ConfigurationValidation;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::MissingKeyCount"), ("1", "User::SuspectValueCount"),
                         ("2", "User::ConfigurationStatus")],
    ))
    raise_defect = pkg.add(ExecuteSql(
        "Raise Configuration Defect",
        CONN_STAGING,
        "EXEC etl.usp_LogError @BatchId = ?, @ErrorSeverity = N'Error', @ErrorCode = 50011, "
        "@ErrorDescription = N'Mandatory configuration keys are missing for this environment.', "
        "@SourceName = N'MNT_Validate_Configuration';",
        is_stored_procedure=True,
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    warn = pkg.add(ExecuteSql(
        "Record Suspect Configuration",
        CONN_STAGING,
        "INSERT INTO etl.OperatorNotification "
        "    (BatchId, NotificationTypeCode, Severity, Subject, Body, RaisedAtUtc, IsAcknowledged) "
        "SELECT ?, N'CONFIG_SUSPECT', N'WARNING', N'Configuration values look wrong', "
        "       CONCAT(N'Suspect configuration entries: ', CAST(COUNT(*) AS nvarchar(20))), "
        "       SYSUTCDATETIME(), 0 "
        "FROM   work.ConfigurationValidation WHERE CheckStatus = N'SUSPECT' HAVING COUNT(*) > 0;",
        parameter_bindings=[("$Package::BatchId", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("etl.Configuration"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, required)
    pkg.link(required, placeholders)
    pkg.link(placeholders, plausibility)
    pkg.link(plausibility, measure)
    pkg.link(measure, raise_defect,
             expression="@[User::MissingKeyCount] > 0 && @[$Package::FailOnMissingKey]")
    pkg.link(measure, warn, expression='@[User::ConfigurationStatus] == "SUSPECT"')
    pkg.link(measure, counts,
             expression='@[User::ConfigurationStatus] == "OK"')
    pkg.link(raise_defect, counts, value="Completion")
    pkg.link(warn, counts, value="Completion")
    pkg.link(counts, done)
    return pkg


# ---------------------------------------------------------------------------
# MNT_Check_DiskSpace
# ---------------------------------------------------------------------------


def build_mnt_check_diskspace():
    pkg = new_package(
        "MNT_Check_DiskSpace",
        "Pre-flight environment check before the nightly batch: free space on the data, log and "
        "tempdb volumes, the size of the inbound and archive shares, and the projected growth of "
        "the largest fact loads. The thresholds are the ones the operators set after the batch "
        "filled the log volume, and a shortfall is raised loudly enough to hold the batch.",
        connections=(CONN_DW,),
        extra_variables=[("LowVolumeCount", 0, "int"), ("SmallestFreePercent", 100, "int"),
                         ("PreflightStatus", "UNKNOWN", "string")],
    )
    pkg.add_parameter("MinimumFreePercent", 15, dtype="int",
                      description="Minimum free space on any database volume.")
    pkg.add_parameter("MinimumFreeGbTempdb", 50, dtype="int",
                      description="Minimum free space required for tempdb sorts.")
    pkg.add_parameter("HoldBatchOnShortfall", "True", dtype="bool",
                      description="A shortfall raises a hold rather than letting the batch run.")

    start = pkg.add(log_package_start(pkg))
    clear = pkg.add(truncate("work.VolumeSpaceCheck", connection=CONN_DW))
    survey = pkg.add(ExecuteSql(
        "Survey Volume Space",
        CONN_DW,
        "INSERT INTO work.VolumeSpaceCheck\n"
        "    (VolumeMountPoint, TotalBytes, AvailableBytes, FreePercent, DatabaseName,\n"
        "     FileTypeCode, CheckedAtUtc)\n"
        "SELECT DISTINCT\n"
        "        vs.volume_mount_point\n"
        ",       vs.total_bytes\n"
        ",       vs.available_bytes\n"
        ",       CAST(vs.available_bytes * 100.0 / NULLIF(vs.total_bytes, 0) AS decimal(9, 2))\n"
        ",       DB_NAME(f.database_id)\n"
        ",       CASE WHEN f.type = 1 THEN N'LOG' ELSE N'DATA' END\n"
        ",       SYSUTCDATETIME()\n"
        "FROM    sys.master_files AS f\n"
        "CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs;",
        timeout=300,
    ))
    growth = pkg.add(ExecuteSql(
        "Project Batch Growth",
        CONN_DW,
        "INSERT INTO work.VolumeGrowthProjection\n"
        "    (VolumeMountPoint, ProjectedBytes, BasisDescription, CheckedAtUtc)\n"
        "SELECT  v.VolumeMountPoint\n"
        # Projection is the average of the last ten nightly loads plus a third for
        # the index maintenance that runs behind them.
        ",       CAST(AVG(CAST(g.BytesWritten AS bigint)) * 1.33 AS bigint)\n"
        ",       N'Average of the last ten nightly loads plus index maintenance headroom'\n"
        ",       SYSUTCDATETIME()\n"
        "FROM    work.VolumeSpaceCheck AS v\n"
        "INNER JOIN etl.LoadVolumeHistory AS g ON g.VolumeMountPoint = v.VolumeMountPoint\n"
        "WHERE   g.LoadDate >= DATEADD(DAY, -14, CAST(SYSUTCDATETIME() AS date))\n"
        "GROUP BY v.VolumeMountPoint;",
    ))

    columns = [
        str_col("VolumeMountPoint", 128), int_col("FreePercent"), int_col("AvailableGb"),
        str_col("DatabaseName", 128), str_col("FileTypeCode", 10), int_col("ProjectedGb"),
    ]
    flow = DataFlow("Evaluate Preflight")
    flow.oledb_source(
        "work VolumeSpaceCheck", CONN_DW,
        "SELECT  v.VolumeMountPoint\n"
        ",       CAST(v.FreePercent AS int) AS FreePercent\n"
        ",       CAST(v.AvailableBytes / 1073741824 AS int) AS AvailableGb\n"
        ",       v.DatabaseName\n"
        ",       v.FileTypeCode\n"
        ",       CAST(ISNULL(p.ProjectedBytes, 0) / 1073741824 AS int) AS ProjectedGb\n"
        "FROM    work.VolumeSpaceCheck AS v\n"
        "LEFT OUTER JOIN work.VolumeGrowthProjection AS p\n"
        "       ON p.VolumeMountPoint = v.VolumeMountPoint;", columns, timeout=300)
    flow.derived_column("Evaluate Thresholds", [
        ("HasShortfall",
         "FreePercent < @[$Package::MinimumFreePercent] || AvailableGb < ProjectedGb "
         "? (DT_BOOL)1 : (DT_BOOL)0",
         bool_col("HasShortfall")),
        ("SeverityCode",
         'FreePercent < (@[$Package::MinimumFreePercent] / 2) ? (DT_WSTR,10)"CRITICAL" : '
         '(FreePercent < @[$Package::MinimumFreePercent] ? (DT_WSTR,10)"WARNING" : '
         '(DT_WSTR,10)"OK")',
         str_col("SeverityCode", 10)),
        ("HeadroomGb", "AvailableGb - ProjectedGb", int_col("HeadroomGb")),
    ])
    flow.conditional_split("Split Shortfalls", [
        ("Shortfall", "HasShortfall == (DT_BOOL)1"),
        ("Healthy", "HasShortfall == (DT_BOOL)0"),
    ])
    flow.multicast("Fan Out Shortfalls", ["Result Output", "Shortfall Output"])
    flow.row_count("Count Volumes", "User::RowsRead")
    flow.oledb_destination("etl PreflightResult", CONN_STAGING, "[etl].[PreflightResult]",
                           batch_size=1000)
    flow.branch_destination("work VolumeShortfall", CONN_STAGING, "[work].[VolumeShortfall]",
                            "Fan Out Shortfalls", "Shortfall Output")
    evaluate = pkg.add(DataFlowTask(flow))

    measure = pkg.add(ExecuteSql(
        "Measure Shortfalls",
        CONN_STAGING,
        "SELECT COUNT(*) AS LowVolumes, "
        "       ISNULL(MIN(FreePercent), 100) AS SmallestFreePercent, "
        "       CASE WHEN COUNT(*) = 0 THEN N'OK' "
        "            WHEN MIN(FreePercent) < 5 THEN N'CRITICAL' ELSE N'WARNING' END AS Status "
        "FROM   work.VolumeShortfall;",
        result_type="ResultSetType_SingleRow",
        result_bindings=[("0", "User::LowVolumeCount"), ("1", "User::SmallestFreePercent"),
                         ("2", "User::PreflightStatus")],
    ))
    hold = pkg.add(ExecuteSql(
        "Raise Batch Hold",
        CONN_STAGING,
        "INSERT INTO etl.BatchHold (HoldReasonCode, HoldDetail, RaisedAtUtc, IsCleared) "
        "SELECT N'DISK_SPACE', "
        "       CONCAT(N'Volumes below the free-space threshold: ', CAST(? AS nvarchar(10)), "
        "              N'; smallest free percent: ', CAST(? AS nvarchar(10))), "
        "       SYSUTCDATETIME(), 0;",
        parameter_bindings=[("User::LowVolumeCount", 0, "LONG"),
                            ("User::SmallestFreePercent", 1, "LONG")],
    ))
    tempdb = pkg.add(ExecuteSql(
        "Check Tempdb Headroom",
        CONN_DW,
        "INSERT INTO etl.PreflightResult (CheckName, CheckStatus, DetailText, CheckedAtUtc) "
        "SELECT N'TEMPDB_HEADROOM', "
        "       CASE WHEN SUM(vs.available_bytes) / 1073741824 < ? THEN N'FAILED' ELSE N'OK' END, "
        "       CONCAT(N'Tempdb volume free GB: ', "
        "              CAST(SUM(vs.available_bytes) / 1073741824 AS nvarchar(20))), "
        "       SYSUTCDATETIME() "
        "FROM   sys.master_files AS f "
        "CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs "
        "WHERE  f.database_id = DB_ID(N'tempdb');",
        parameter_bindings=[("$Package::MinimumFreeGbTempdb", 0, "LONG")],
    ))
    counts = pkg.add(log_row_count("work.VolumeSpaceCheck"))
    done = pkg.add(log_package_success())

    pkg.link(start, clear)
    pkg.link(clear, survey)
    pkg.link(survey, growth, value="Completion")
    pkg.link(growth, evaluate)
    pkg.link(evaluate, measure)
    pkg.link(measure, hold,
             expression="@[User::LowVolumeCount] > 0 && @[$Package::HoldBatchOnShortfall]")
    pkg.link(measure, tempdb,
             expression="@[User::LowVolumeCount] == 0 || !@[$Package::HoldBatchOnShortfall]")
    pkg.link(hold, tempdb, value="Completion")
    pkg.link(tempdb, counts, value="Completion")
    pkg.link(counts, done)
    return pkg


BUILDERS = [
    build_mnt_purge_staginghistory,
    build_mnt_purge_controlhistory,
    build_mnt_rebuild_indexes,
    build_mnt_update_statistics,
    build_mnt_archive_processedfiles,
    build_mnt_validate_configuration,
    build_mnt_check_diskspace,
]


def main():
    written = []
    names = []
    for builder in BUILDERS:
        pkg = builder()
        written.append(pkg.write(os.path.join(HERE, pkg.name + ".dtsx")))
        names.append(pkg.name)
    written.extend(project.write_project(HERE, PROJECT_NAME, names, CONNECTIONS))
    for path in written:
        print(os.path.relpath(path, REPO_ROOT))


if __name__ == "__main__":
    main()
