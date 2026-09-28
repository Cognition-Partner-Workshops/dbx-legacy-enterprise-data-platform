"""Shared helpers for the wwi_99_maintenance notebooks (legacy SSIS project WWI_Maintenance).

Pure functions (no Spark) carry the legacy decision rules so they can be unit tested
locally; the Spark-facing functions are thin wrappers over information_schema,
DESCRIBE DETAIL / HISTORY and the etl.* operations tables.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta, timezone

MAINTENANCE_SCHEMAS = ("bronze", "silver", "gold")
TARGET_FILE_BYTES = 128 * 1024 * 1024
LEGACY_PAGE_BYTES = 8 * 1024
LEGACY_MIN_PAGE_COUNT = 1000
DATE_COLUMN_PREFERENCE = ("LoadedAtUtc", "ExtractedAtUtc", "ReceivedAtUtc", "LoadedAt",
                          "BusinessDate", "LoadDate")
LEGACY_SCHEMA_MAP = {
    "raw": ("bronze", "raw_"), "stg": ("silver", "stg_"), "work": ("silver", "work_"),
    "err": ("silver", "err_"), "ref": ("silver", "ref_"), "Integration": ("silver", "int_"),
    "Dimension": ("gold", "dim_"), "Fact": ("gold", "fact_"), "Aggregate": ("gold", "agg_"),
    "Report": ("gold", "rpt_"),
}
LEGACY_CONNECTION_PLACEHOLDERS = (
    "ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER", "SQLSERVER_HOST",
    "SQLSERVER_PORT", "SQLSERVER_USER", "SQLSERVER_OLTP_DB", "SQLSERVER_STAGING_DB",
    "SQLSERVER_DW_DB",
)
DEFAULT_REQUIRED_SECRETS = ("oracle_user", "oracle_password", "sqlserver_user", "sqlserver_password")
PREFLIGHT_STATUS_BY_SEVERITY = {"OK": "OK", "WARNING": "WARNING", "CRITICAL": "FAILED"}


# ---------------------------------------------------------------------------
# generic helpers
# ---------------------------------------------------------------------------


def utcNow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def toBool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes", "y")


def toInt(value, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def widgetOr(dbutils, name: str, default: str) -> str:
    """Read a task/job parameter widget, creating it with the default when absent."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        value = default
    return value if value is not None and value != "" else default


def monthsFromDays(days: int) -> int:
    """Legacy retention parameters are in days; control.purgeControlHistory takes months."""
    return max(1, int(round(days / 30.4375)))


def snakeName(legacyName: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", legacyName.strip())
    cleaned = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", cleaned)
    return re.sub(r"_+", "_", cleaned).strip("_").lower()


def deltaName(legacySchema: str, legacyTable: str) -> tuple[str, str]:
    """raw.FinLedger -> (bronze, raw_fin_ledger); Fact.Sale -> (gold, fact_sale)."""
    schema, prefix = LEGACY_SCHEMA_MAP.get(legacySchema, ("silver", snakeName(legacySchema) + "_"))
    return schema, prefix + snakeName(legacyTable)


def sqlString(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def quoted(fqn: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in fqn.split("."))


# ---------------------------------------------------------------------------
# MNT_Purge_StagingHistory
# ---------------------------------------------------------------------------


def retentionDaysFor(schema: str, table: str, defaultRetentionDays: int) -> int:
    """Legacy purge-plan CASE: finance raw feeds 7 years, partner feeds 1 year, work 14 days."""
    if schema == "bronze" and table.startswith("raw_fin"):
        return 2555
    if "partner" in table:
        return 365
    if schema == "silver" and table.startswith("work_"):
        return 14
    return defaultRetentionDays


def cutoffDate(asOfDate: date, retentionDays: int) -> date:
    return asOfDate - timedelta(days=retentionDays)


def isStagingPurgeCandidate(schema: str, table: str) -> bool:
    if schema == "bronze":
        return table.startswith("raw_")
    if schema == "silver":
        return table.startswith(("stg_", "err_", "work_"))
    return False


def choosePurgeColumn(columns, registeredColumn=None):
    lookup = {c.lower(): c for c in columns}
    if registeredColumn and registeredColumn.lower() in lookup:
        return lookup[registeredColumn.lower()]
    for candidate in DATE_COLUMN_PREFERENCE:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]
    return None


def purgePredicate(catalog: str, dateColumn, hasBatchId: bool, cutoff: date):
    """Age predicate: by load-date column when the table has one, else by BatchId age."""
    if dateColumn:
        return f"`{dateColumn}` < TIMESTAMP'{cutoff.isoformat()} 00:00:00'"
    if hasBatchId:
        return (f"`BatchId` IN (SELECT BatchId FROM {catalog}.etl.batch "
                f"WHERE BusinessDate < DATE'{cutoff.isoformat()}')")
    return None


def buildStagingPurgePlan(spark, catalog: str, defaultRetentionDays: int, asOfDate: date,
                          register=None) -> list:
    """One plan row per purge-eligible bronze/silver staging table.

    `register` is the optional etl.staging_table_register content (list of dicts with
    SchemaName/TableName/LoadDateColumn/RetentionDays/IsPurgeEligible in legacy names).
    Tables discovered from information_schema fall back to the legacy retention CASE.
    """
    registered = {}
    for row in register or []:
        schema, table = deltaName(row["SchemaName"], row["TableName"])
        registered[(schema, table)] = row
    plan = []
    for t in listTables(spark, catalog, ("bronze", "silver")):
        key = (t["schema"], t["table"])
        if not isStagingPurgeCandidate(*key):
            continue
        reg = registered.get(key)
        if reg is not None and not toBool(reg.get("IsPurgeEligible", True)):
            continue
        columns = listColumns(spark, catalog, *key)
        dateColumn = choosePurgeColumn(columns, reg.get("LoadDateColumn") if reg else None)
        retention = (toInt(reg.get("RetentionDays"), 0) if reg else 0) or \
            retentionDaysFor(key[0], key[1], defaultRetentionDays)
        cutoff = cutoffDate(asOfDate, retention)
        hasBatchId = any(c.lower() == "batchid" for c in columns)
        plan.append({
            "SchemaName": key[0], "TableName": key[1], "LoadDateColumn": dateColumn,
            "HasBatchId": hasBatchId, "RetentionDays": retention, "CutoffDate": cutoff,
            "EstimatedRows": toInt(reg.get("ApproximateRowCount"), 0) if reg else 0,
            "Predicate": purgePredicate(catalog, dateColumn, hasBatchId, cutoff),
        })
    plan.sort(key=lambda r: (-r["EstimatedRows"], r["SchemaName"], r["TableName"]))
    return plan


def vacuumStatement(fqn: str, retentionHours) -> str:
    if retentionHours is None:
        return f"VACUUM {quoted(fqn)}"
    return f"VACUUM {quoted(fqn)} RETAIN {int(retentionHours)} HOURS"


def deletedFileRetentionHours(properties: dict, default: int = 168) -> int:
    """Parse delta.deletedFileRetentionDuration ('interval 7 days') into hours."""
    raw = (properties or {}).get("delta.deletedFileRetentionDuration")
    if not raw:
        return default
    m = re.match(r"\s*interval\s+(\d+)\s+(hour|hours|day|days|week|weeks)\s*$", raw.lower())
    if not m:
        return default
    n, unit = int(m.group(1)), m.group(2)
    return n * (1 if unit.startswith("hour") else 24 if unit.startswith("day") else 168)


# ---------------------------------------------------------------------------
# information_schema / Delta metadata
# ---------------------------------------------------------------------------


def listTables(spark, catalog: str, schemas=MAINTENANCE_SCHEMAS) -> list:
    inList = ", ".join(sqlString(s) for s in schemas)
    rows = spark.sql(
        f"SELECT table_schema, table_name, table_type, data_source_format "
        f"FROM {catalog}.information_schema.tables "
        f"WHERE table_schema IN ({inList}) "
        f"ORDER BY table_schema, table_name").collect()
    return [{"schema": r[0], "table": r[1], "tableType": r[2], "dataSourceFormat": r[3]}
            for r in rows]


def listColumns(spark, catalog: str, schema: str, table: str) -> list:
    rows = spark.sql(
        f"SELECT column_name FROM {catalog}.information_schema.columns "
        f"WHERE table_schema = {sqlString(schema)} AND table_name = {sqlString(table)} "
        f"ORDER BY ordinal_position").collect()
    return [r[0] for r in rows]


def isDeltaTable(tableInfo: dict) -> bool:
    return (tableInfo.get("tableType") in ("MANAGED", "EXTERNAL")
            and (tableInfo.get("dataSourceFormat") or "DELTA").upper() == "DELTA")


def deltaTableDetail(spark, fqn: str) -> dict:
    row = spark.sql(f"DESCRIBE DETAIL {quoted(fqn)}").collect()[0].asDict()
    return {
        "numFiles": row.get("numFiles") or 0,
        "sizeInBytes": row.get("sizeInBytes") or 0,
        "clusteringColumns": list(row.get("clusteringColumns") or []),
        "partitionColumns": list(row.get("partitionColumns") or []),
        "properties": dict(row.get("properties") or {}),
    }


def tableHistory(spark, fqn: str) -> list:
    rows = spark.sql(f"DESCRIBE HISTORY {quoted(fqn)}").collect()
    return [{"timestamp": r["timestamp"], "operation": r["operation"],
             "operationMetrics": dict(r["operationMetrics"] or {})} for r in rows]


# ---------------------------------------------------------------------------
# MNT_Rebuild_Indexes -> OPTIMIZE
# ---------------------------------------------------------------------------


def fragmentationPercent(numFiles: int, sizeInBytes: int,
                         targetFileBytes: int = TARGET_FILE_BYTES) -> float:
    """Small-file ratio standing in for avg_fragmentation_in_percent."""
    if not numFiles or numFiles <= 1 or not sizeInBytes:
        return 0.0
    avgFile = sizeInBytes / numFiles
    return round(max(0.0, min(100.0, 100.0 * (1.0 - avgFile / targetFileBytes))), 2)


def zorderColumnsFor(schema: str, table: str, columns) -> list:
    """Per-table ZORDER keys: date keys + BatchId for facts, business keys for dims,
    BatchId/BusinessDate for staging. Empty list -> plain OPTIMIZE (compaction only)."""
    keys = []
    if schema == "gold" and table.startswith(("fact_", "agg_")):
        keys += [c for c in columns if re.search(r"date\s*_?key$|^businessdate$", c, re.I)][:2]
        keys += [c for c in columns if c.lower() == "batchid"]
    elif schema == "gold" and table.startswith("dim_"):
        keys += [c for c in columns if re.match(r"^wwi.*(id|key)$", c, re.I)][:1]
        keys += [c for c in columns if c.lower() in ("validfrom", "valid_from")][:1]
    else:
        keys += [c for c in columns if c.lower() in ("batchid", "businessdate")]
    seen, ordered = set(), []
    for k in keys:
        if k.lower() not in seen:
            seen.add(k.lower())
            ordered.append(k)
    return ordered


def planOptimizeAction(schema: str, table: str, columns, detail: dict,
                       reorganiseThresholdPercent: int, rebuildThresholdPercent: int,
                       minSizeBytes: int = LEGACY_MIN_PAGE_COUNT * LEGACY_PAGE_BYTES) -> dict:
    """REORGANIZE -> OPTIMIZE (bin-pack); REBUILD -> OPTIMIZE ZORDER BY (or plain OPTIMIZE when
    the table is liquid-clustered, which re-clusters incrementally)."""
    fragmentation = fragmentationPercent(detail["numFiles"], detail["sizeInBytes"])
    clustered = bool(detail.get("clusteringColumns"))
    indexType = "LIQUID" if clustered else "ZORDER"
    zorder = [] if clustered else zorderColumnsFor(schema, table, columns)
    if detail["sizeInBytes"] < minSizeBytes:
        action = "NONE"
    elif fragmentation >= rebuildThresholdPercent:
        action = "REBUILD"
    elif fragmentation >= reorganiseThresholdPercent:
        action = "REORGANIZE"
    else:
        action = "NONE"
    return {"IndexTypeCode": indexType, "FragmentationPercent": fragmentation,
            "PlannedAction": action, "ZorderColumns": zorder if action == "REBUILD" else []}


def optimizeStatement(fqn: str, plannedAction: str, zorderColumns) -> str:
    stmt = f"OPTIMIZE {quoted(fqn)}"
    if plannedAction == "REBUILD" and zorderColumns:
        stmt += " ZORDER BY (" + ", ".join("`" + c + "`" for c in zorderColumns) + ")"
    return stmt


def deadlineReached(startedAt: datetime, maxDurationMinutes: int, now=None) -> bool:
    now = now or utcNow()
    return now >= startedAt + timedelta(minutes=maxDurationMinutes)


# ---------------------------------------------------------------------------
# MNT_Update_Statistics -> ANALYZE TABLE
# ---------------------------------------------------------------------------

MODIFICATION_METRICS = ("numOutputRows", "numUpdatedRows", "numDeletedRows", "numCopiedRows",
                        "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted")


def modifiedRowsSince(historyRows, since) -> int:
    total = 0
    for h in historyRows:
        if since is not None and h["timestamp"] <= since:
            continue
        if h["operation"] in ("OPTIMIZE", "VACUUM START", "VACUUM END", "SET TBLPROPERTIES"):
            continue
        for metric in MODIFICATION_METRICS:
            total += toInt(h["operationMetrics"].get(metric), 0)
    return total


def statisticsRefreshMode(schema: str, table: str) -> str:
    """Legacy: Dim% -> FULLSCAN, everything else sampled. Delta has no sampled ANALYZE, so
    SAMPLE = table-level statistics only and FULLSCAN = FOR ALL COLUMNS."""
    return "FULLSCAN" if schema == "gold" and table.startswith("dim_") else "SAMPLE"


def analyzeStatement(fqn: str, refreshMode: str) -> str:
    stmt = f"ANALYZE TABLE {quoted(fqn)} COMPUTE STATISTICS"
    if refreshMode == "FULLSCAN":
        stmt += " FOR ALL COLUMNS"
    return stmt


# ---------------------------------------------------------------------------
# MNT_Archive_ProcessedFiles -> UC Volumes
# ---------------------------------------------------------------------------


def archiveFolder(landingRoot: str, asOf: datetime) -> str:
    """archive\\<yyyy>\\<MM> under the landing root, as the legacy expression built it."""
    return f"{landingRoot.rstrip('/')}/archive/{asOf.year:04d}/{asOf.month:02d}"


def processedFolder(landingRoot: str) -> str:
    return f"{landingRoot.rstrip('/')}/inbound/processed"


def fileIsSettled(modificationTimeMs: int, asOf: datetime, minimumFileAgeHours: int) -> bool:
    modified = datetime.utcfromtimestamp(modificationTimeMs / 1000.0)
    return modified <= asOf - timedelta(hours=minimumFileAgeHours)


# ---------------------------------------------------------------------------
# MNT_Check_DiskSpace -> table / volume size health
# ---------------------------------------------------------------------------


def bytesToGb(numBytes) -> int:
    return int((numBytes or 0) // (1024 ** 3))


def evaluateVolume(usedBytes: int, budgetGb: int, projectedBytes: int, minimumFreePercent: int) -> dict:
    """The legacy derived-column expressions over a storage budget instead of a disk."""
    budgetBytes = budgetGb * (1024 ** 3)
    availableBytes = max(0, budgetBytes - usedBytes)
    freePercent = int(availableBytes * 100 // budgetBytes) if budgetBytes else 0
    availableGb, projectedGb = bytesToGb(availableBytes), bytesToGb(projectedBytes)
    hasShortfall = freePercent < minimumFreePercent or availableGb < projectedGb
    if freePercent < minimumFreePercent / 2:
        severity = "CRITICAL"
    elif freePercent < minimumFreePercent:
        severity = "WARNING"
    else:
        severity = "OK"
    return {"FreePercent": freePercent, "AvailableGb": availableGb, "ProjectedGb": projectedGb,
            "HasShortfall": hasShortfall, "SeverityCode": severity,
            "HeadroomGb": availableGb - projectedGb}


def preflightCheckStatus(severityCode: str) -> str:
    return PREFLIGHT_STATUS_BY_SEVERITY.get(severityCode, "WARNING")


def preflightStatus(lowVolumeCount: int, smallestFreePercent: int) -> str:
    if lowVolumeCount == 0:
        return "OK"
    return "CRITICAL" if smallestFreePercent < 5 else "WARNING"


def projectedGrowthBytes(bytesWrittenHistory) -> int:
    """Average of the recent nightly loads plus a third for the maintenance behind them."""
    values = [int(v) for v in bytesWrittenHistory if v is not None]
    if not values:
        return 0
    return int(sum(values) / len(values) * 1.33)


# ---------------------------------------------------------------------------
# MNT_Validate_Configuration
# ---------------------------------------------------------------------------


def requiredKeyStatus(value) -> str:
    if value is None:
        return "MISSING"
    if str(value).strip() == "":
        return "EMPTY"
    return "OK"


def periodPlausibility(regionCode, fiscalCalendarCode, openPeriodKey, vatRegimeCode, asOf: date) -> str:
    if openPeriodKey is None:
        return "MISSING"
    if regionCode == "APAC" and fiscalCalendarCode != "445":
        return "SUSPECT"
    if regionCode == "EU" and vatRegimeCode is None:
        return "SUSPECT"
    twoMonthsBack = asOf.replace(day=1) - timedelta(days=1)
    twoMonthsBack = twoMonthsBack.replace(day=1) - timedelta(days=1)
    floorKey = int(twoMonthsBack.strftime("%Y%m"))
    if toInt(openPeriodKey, 0) < floorKey:
        return "SUSPECT"
    return "OK"


def fxPlausibility(rateCount) -> str:
    return "MISSING" if toInt(rateCount, 0) == 0 else "OK"


def configurationStatus(missingCount: int, suspectCount: int) -> str:
    if missingCount > 0:
        return "DEFECTIVE"
    if suspectCount > 0:
        return "SUSPECT"
    return "OK"


def countDefects(results) -> tuple:
    missing = sum(1 for r in results if r["CheckStatus"] in ("MISSING", "EMPTY"))
    suspect = sum(1 for r in results if r["CheckStatus"] == "SUSPECT")
    return missing, suspect


def splitCsv(value: str) -> list:
    return [v.strip() for v in (value or "").split(",") if v.strip()]


# ---------------------------------------------------------------------------
# etl.* operations tables (07_tables_operations.sql) and silver.work_* plan tables
# ---------------------------------------------------------------------------

OPS_TABLE_DDL = {
    "etl.maintenance_log": (
        "MaintenanceLogId BIGINT GENERATED ALWAYS AS IDENTITY, TaskName STRING NOT NULL, "
        "DetailText STRING, RecordedAtUtc TIMESTAMP NOT NULL"),
    "etl.purge_audit": (
        "PurgeAuditId BIGINT GENERATED ALWAYS AS IDENTITY, BatchId BIGINT, SchemaName STRING NOT NULL, "
        "TableName STRING NOT NULL, CutoffDate DATE, RowsDeleted BIGINT NOT NULL, PurgedAtUtc TIMESTAMP NOT NULL"),
    "etl.batch_archive": (
        "BatchArchiveId BIGINT GENERATED ALWAYS AS IDENTITY, BatchId BIGINT NOT NULL, BatchType STRING, "
        "BatchStatus STRING, StartedAtUtc TIMESTAMP, EndedAtUtc TIMESTAMP, StepCount INT, "
        "FailedStepCount INT, ArchivedAtUtc TIMESTAMP NOT NULL"),
    "etl.preflight_result": (
        "PreflightResultId BIGINT GENERATED ALWAYS AS IDENTITY, BatchId BIGINT, CheckName STRING NOT NULL, "
        "CheckStatus STRING NOT NULL, DetailText STRING, CheckedAtUtc TIMESTAMP NOT NULL"),
    "etl.batch_hold": (
        "BatchHoldId BIGINT GENERATED ALWAYS AS IDENTITY, HoldReasonCode STRING NOT NULL, HoldDetail STRING, "
        "BatchType STRING, RaisedAtUtc TIMESTAMP NOT NULL, IsCleared BOOLEAN NOT NULL, ClearedBy STRING, "
        "ClearedAtUtc TIMESTAMP"),
    "etl.operator_notification": (
        "OperatorNotificationId BIGINT GENERATED ALWAYS AS IDENTITY, BatchId BIGINT, "
        "NotificationTypeCode STRING NOT NULL, Severity STRING NOT NULL, Subject STRING NOT NULL, Body STRING, "
        "ObjectName STRING, RaisedAtUtc TIMESTAMP NOT NULL, IsAcknowledged BOOLEAN NOT NULL, "
        "AcknowledgedBy STRING, AcknowledgedAtUtc TIMESTAMP"),
    "etl.archive_expiry_list": (
        "ArchiveExpiryListId BIGINT GENERATED ALWAYS AS IDENTITY, FileName STRING NOT NULL, "
        "ArchivePath STRING NOT NULL, ArchivedAtUtc TIMESTAMP, ListedAtUtc TIMESTAMP NOT NULL, "
        "IsDeleted BOOLEAN NOT NULL, DeletedAtUtc TIMESTAMP"),
    "etl.inbound_file_register": (
        "InboundFileId BIGINT GENERATED ALWAYS AS IDENTITY, FeedCode STRING NOT NULL, FileName STRING NOT NULL, "
        "FilePath STRING NOT NULL, FileSizeBytes BIGINT, FileHash STRING, ReceivedAtUtc TIMESTAMP NOT NULL, "
        "StructuralCheckStatus STRING, BadFileCount INT NOT NULL, ProcessingStatus STRING NOT NULL, "
        "ProcessedAtUtc TIMESTAMP, IsQuarantined BOOLEAN NOT NULL, QuarantinedAtUtc TIMESTAMP, "
        "QuarantineReason STRING, IsArchived BOOLEAN NOT NULL, ArchivePath STRING, ArchivedAtUtc TIMESTAMP"),
    "etl.load_volume_history": (
        "LoadVolumeHistoryId BIGINT GENERATED ALWAYS AS IDENTITY, VolumeMountPoint STRING NOT NULL, "
        "LoadDate DATE NOT NULL, BytesWritten BIGINT, FreePercent DECIMAL(9,4)"),
    "etl.staging_table_register": (
        "StagingTableId INT, SchemaName STRING NOT NULL, TableName STRING NOT NULL, LoadDateColumn STRING, "
        "RetentionDays INT, IsPurgeEligible BOOLEAN NOT NULL, ApproximateRowCount BIGINT"),
    "etl.required_configuration_key": (
        "RequiredConfigurationKeyId INT, ConfigurationKey STRING NOT NULL, EnvironmentCode STRING, "
        "IsMandatory BOOLEAN NOT NULL, Description STRING"),
    "etl.region_period_status": (
        "RegionPeriodStatusId INT, RegionCode STRING NOT NULL, FiscalCalendarCode STRING NOT NULL, "
        "OpenPeriodKey STRING NOT NULL, VatRegimeCode STRING, LastClosedPeriodKey STRING, UpdatedAtUtc TIMESTAMP NOT NULL"),
    "etl.fx_rate_availability": (
        "FxRateAvailabilityId BIGINT GENERATED ALWAYS AS IDENTITY, RateDate DATE NOT NULL, RegionCode STRING, "
        "RateCount INT NOT NULL, IsComplete BOOLEAN NOT NULL, CheckedAtUtc TIMESTAMP NOT NULL"),
}

WORK_TABLE_DDL = {
    "silver.work_staging_purge_plan": (
        "SchemaName STRING, TableName STRING, LoadDateColumn STRING, HasBatchId BOOLEAN, RetentionDays INT, "
        "CutoffDate DATE, EstimatedRows BIGINT, Predicate STRING, PlannedAtUtc TIMESTAMP"),
    "silver.work_index_maintenance_plan": (
        "SchemaName STRING, TableName STRING, IndexTypeCode STRING, FragmentationPercent DECIMAL(9,2), "
        "NumFiles BIGINT, SizeInBytes BIGINT, PlannedAction STRING, ZorderColumns STRING, PlannedAtUtc TIMESTAMP"),
    "silver.work_statistics_refresh_plan": (
        "SchemaName STRING, TableName STRING, ModifiedRowCount BIGINT, RefreshMode STRING, PlannedAtUtc TIMESTAMP"),
    "silver.work_file_archive_queue": (
        "FileName STRING, FilePath STRING, FeedCode STRING, ProcessedAtUtc TIMESTAMP, QueuedAtUtc TIMESTAMP"),
    "silver.work_configuration_validation": (
        "ConfigurationKey STRING, EnvironmentCode STRING, CheckTypeCode STRING, CheckStatus STRING, "
        "DetailText STRING, CheckedAtUtc TIMESTAMP"),
    "silver.work_volume_space_check": (
        "VolumeMountPoint STRING, TotalBytes BIGINT, AvailableBytes BIGINT, FreePercent DECIMAL(9,2), "
        "DatabaseName STRING, FileTypeCode STRING, CheckedAtUtc TIMESTAMP"),
    "silver.work_volume_growth_projection": (
        "VolumeMountPoint STRING, ProjectedBytes BIGINT, BasisDescription STRING, CheckedAtUtc TIMESTAMP"),
    "silver.work_volume_shortfall": (
        "VolumeMountPoint STRING, FreePercent INT, AvailableGb INT, DatabaseName STRING, FileTypeCode STRING, "
        "ProjectedGb INT, HasShortfall BOOLEAN, SeverityCode STRING, HeadroomGb INT, CheckedAtUtc TIMESTAMP"),
}


def ensureTable(spark, catalog: str, schemaTable: str, ddl: dict) -> str:
    fqn = f"{catalog}.{schemaTable}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {quoted(fqn)} ({ddl[schemaTable]}) USING DELTA")
    return fqn


def ensureOpsTable(spark, catalog: str, schemaTable: str) -> str:
    return ensureTable(spark, catalog, schemaTable, OPS_TABLE_DDL)


def ensureWorkTable(spark, catalog: str, schemaTable: str, truncate: bool = True) -> str:
    fqn = ensureTable(spark, catalog, schemaTable, WORK_TABLE_DDL)
    if truncate:
        spark.sql(f"TRUNCATE TABLE {quoted(fqn)}")
    return fqn


def tableExists(spark, catalog: str, schema: str, table: str) -> bool:
    return spark.sql(
        f"SELECT 1 FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = {sqlString(schema)} AND table_name = {sqlString(table)}").count() > 0


def insertMaintenanceLog(spark, catalog: str, taskName: str, detailText: str) -> None:
    fqn = ensureOpsTable(spark, catalog, "etl.maintenance_log")
    spark.sql(f"INSERT INTO {quoted(fqn)} (TaskName, DetailText, RecordedAtUtc) "
              f"VALUES ({sqlString(taskName)}, {sqlString(detailText)}, current_timestamp())")


def insertRows(spark, fqn: str, rows, schemaDdl: str) -> int:
    """Append a small python list of dicts through a typed DataFrame (plan / audit rows)."""
    if not rows:
        return 0
    df = spark.createDataFrame(rows, schema=schemaDdl)
    df.write.format("delta").mode("append").saveAsTable(fqn)
    return len(rows)
