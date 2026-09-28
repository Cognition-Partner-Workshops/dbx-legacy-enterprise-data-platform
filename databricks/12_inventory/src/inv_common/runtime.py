"""Databricks-side glue for the INV_* notebooks: parameters, Delta writes, lifecycle.

Everything that needs a Unity Catalog or the etl.* control tables lives here so the
notebooks stay short and `transforms` stays testable without Delta.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from dbx_etl_common import control, params

from . import contracts


def widget(dbutils, name: str, default: str) -> str:
    """Package-level parameter (task base_parameters) with a legacy default."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:  # widget not defined on an interactive run
        return default
    return value if value not in (None, "") else default


def asBool(value: str) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes", "y")


def utcNow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class PackageContext:
    """Mirrors the SSIS package shell: job params + batch + package execution row."""

    def __init__(self, spark: SparkSession, dbutils, packageName: str, batchType: str = "Daily"):
        self.spark = spark
        self.dbutils = dbutils
        self.packageName = packageName
        self.params = params.getJobParams(dbutils)
        self.catalog = self.params["catalog"]
        self.businessDate = self.params["businessDate"]
        self.reloadFullHistory = self.params["reloadFullHistory"]
        self.environmentCode = self.params["environmentCode"]
        self.startedBatch = False
        batchId = int(self.params["batchId"] or 0)
        if batchId == 0:
            # standalone run outside a master job: open a batch of our own, like the SSIS
            # package did when executed directly from SSISDB
            batchId = control.startBatch(
                spark, self.catalog, contracts.BATCH_NAME, batchType=batchType,
                businessDate=self.businessDate, environmentCode=self.environmentCode,
                allowAdoptRunning=True, notes=f"standalone {packageName}",
            )
            self.startedBatch = True
        self.batchId = batchId
        self.packageExecutionId = control.logPackageStart(
            spark, self.catalog, batchId, packageName, projectName=contracts.PROJECT_NAME, stepName=packageName
        )
        self.rowsRead = 0
        self.rowsInserted = 0
        self.rowsUpdated = 0
        self.rowsDeleted = 0
        self.rowsRejected = 0

    def table(self, legacyName: str) -> str:
        return contracts.table(self.catalog, legacyName)

    def read(self, legacyName: str) -> DataFrame:
        return self.spark.table(self.table(legacyName))

    def logRowCount(self, objectName: str, sourceRowCount=None, targetRowCount=None, insertRowCount=None,
                    updateRowCount=None, deleteRowCount=None, rejectRowCount=None) -> None:
        control.logRowCount(
            self.spark, self.catalog, self.packageExecutionId, objectName,
            sourceRowCount=sourceRowCount, targetRowCount=targetRowCount, insertRowCount=insertRowCount,
            updateRowCount=updateRowCount, deleteRowCount=deleteRowCount, rejectRowCount=rejectRowCount,
        )

    def logPackageRowCounts(self, objectName: str) -> None:
        """`Log Row Counts` task: usp_LogRowCount(@SourceRowCount=RowsRead, @TargetRowCount=RowsInserted, @RejectRowCount=RowsRejected)."""
        self.logRowCount(objectName, sourceRowCount=self.rowsRead, targetRowCount=self.rowsInserted,
                         rejectRowCount=self.rowsRejected)

    def logRejectedSet(self, objectName: str, rejectedDf: DataFrame, reasonCode: str,
                       rejectStage: str = "Fact", sourceSystemCode: str = "WWIOLTP") -> int:
        count = control.logRejectedRecordSet(
            self.spark, self.catalog, objectName, rejectedDf, batchId=self.batchId,
            packageExecutionId=self.packageExecutionId, sourceSystemCode=sourceSystemCode,
            rejectStage=rejectStage, rejectReasonCode=reasonCode, businessKeyColumn="BusinessKey",
        )
        self.rowsRejected += int(count or 0)
        return int(count or 0)

    def succeed(self) -> None:
        control.logPackageEnd(
            self.spark, self.catalog, self.packageExecutionId, status="Succeeded",
            rowsRead=self.rowsRead, rowsInserted=self.rowsInserted, rowsUpdated=self.rowsUpdated,
            rowsDeleted=self.rowsDeleted, rowsRejected=self.rowsRejected,
        )
        if self.startedBatch:
            control.endBatch(self.spark, self.catalog, self.batchId)

    def fail(self, exc: BaseException) -> None:
        """OnError event handler: `Log Error` + `Mark Execution Failed`."""
        control.logError(
            self.spark, self.catalog, packageExecutionId=self.packageExecutionId, batchId=self.batchId,
            errorSeverity="Error", sourceName=self.packageName, sourceComponent=type(exc).__name__,
            errorDescription=str(exc)[:4000],
        )
        control.logPackageEnd(
            self.spark, self.catalog, self.packageExecutionId, status="Failed",
            rowsRead=self.rowsRead, rowsInserted=self.rowsInserted, rowsRejected=self.rowsRejected,
        )
        if self.startedBatch:
            control.endBatch(self.spark, self.catalog, self.batchId, forceStatus="Failed")


def runPackage(ctx: PackageContext, body) -> None:
    """Execute the package body with the legacy OnError handler semantics (log, mark failed, re-raise)."""
    try:
        body(ctx)
    except BaseException as exc:  # noqa: BLE001 - mirrors the SSIS OnError handler
        ctx.fail(exc)
        raise
    ctx.succeed()


# --- Delta helpers ---------------------------------------------------------


def ensureTable(spark: SparkSession, fullName: str, columnsDdl: str, partitionBy: str | None = None) -> None:
    partition = f" PARTITIONED BY ({partitionBy})" if partitionBy else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fullName} ({columnsDdl}) USING DELTA{partition}")


def alignToTable(spark: SparkSession, df: DataFrame, fullName: str) -> DataFrame:
    """Project df onto the target's column list (missing columns -> NULL) so writes are schema-stable."""
    from pyspark.sql import functions as F

    target = spark.table(fullName).schema
    cols = []
    for field in target.fields:
        if field.name in df.columns:
            cols.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            cols.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*cols)


def overwriteTable(spark: SparkSession, df: DataFrame, fullName: str) -> int:
    """TRUNCATE + INSERT semantics for work tables."""
    aligned = alignToTable(spark, df, fullName)
    aligned.write.format("delta").mode("overwrite").saveAsTable(fullName)
    return spark.table(fullName).count()


def replaceWhere(spark: SparkSession, df: DataFrame, fullName: str, predicate: str) -> int:
    """DELETE WHERE <predicate> + INSERT as one atomic partition overwrite."""
    aligned = alignToTable(spark, df, fullName)
    aligned.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(fullName)
    return spark.table(fullName).where(predicate).count()


def appendTable(spark: SparkSession, df: DataFrame, fullName: str) -> int:
    aligned = alignToTable(spark, df, fullName)
    aligned.write.format("delta").mode("append").saveAsTable(fullName)
    return aligned.count()


def mergeByKey(spark: SparkSession, df: DataFrame, fullName: str, keyColumns: list[str],
               updateColumns: list[str] | None = None) -> tuple[int, int]:
    """MERGE df into fullName on keyColumns -> (inserted, updated) from the Delta operation metrics."""
    aligned = alignToTable(spark, df, fullName)
    viewName = "_inv_merge_source_" + fullName.split(".")[-1]
    aligned.createOrReplaceTempView(viewName)
    onClause = " AND ".join(f"t.`{c}` = s.`{c}`" for c in keyColumns)
    updateCols = updateColumns or [c for c in aligned.columns if c not in keyColumns]
    setClause = ", ".join(f"t.`{c}` = s.`{c}`" for c in updateCols)
    spark.sql(
        f"MERGE INTO {fullName} AS t USING {viewName} AS s ON {onClause} "
        f"WHEN MATCHED THEN UPDATE SET {setClause} WHEN NOT MATCHED THEN INSERT *"
    )
    metrics = spark.sql(f"DESCRIBE HISTORY {fullName} LIMIT 1").select("operationMetrics").first()[0] or {}
    return int(metrics.get("numTargetRowsInserted", 0)), int(metrics.get("numTargetRowsUpdated", 0))


def ensureProjectTables(spark: SparkSession, catalog: str) -> None:
    """Create the tables this project writes (idempotent). Tables of other sessions are left alone."""
    t = lambda name: contracts.table(catalog, name)  # noqa: E731
    ensureTable(spark, t("Fact.Daily Inventory Snapshot"), contracts.FACT_DAILY_INVENTORY_SNAPSHOT_DDL, "SnapshotDateKey")
    ensureTable(spark, t("Fact.Movement"), contracts.FACT_MOVEMENT_DDL, "DateKey")
    ensureTable(spark, t("Aggregate.Daily Inventory Health"), contracts.AGG_DAILY_INVENTORY_HEALTH_DDL)
    ensureTable(spark, t("work.CycleCountVariance"), contracts.WORK_CYCLE_COUNT_VARIANCE_DDL)
    ensureTable(spark, t("work.ReplenishmentSuggestion"), contracts.WORK_REPLENISHMENT_SUGGESTION_DDL)
    ensureTable(spark, t("work.StockTransferMovement"), contracts.WORK_STOCK_TRANSFER_MOVEMENT_DDL)
    ensureTable(spark, t("work.StockTransferReceiptOnly"), contracts.WORK_STOCK_TRANSFER_MOVEMENT_DDL)
    ensureTable(spark, t("err.InventorySnapshotReject"), contracts.ERR_INVENTORY_SNAPSHOT_REJECT_DDL)
    ensureTable(spark, t("etl.ReconciliationResult"), contracts.RECONCILIATION_RESULT_DDL)
