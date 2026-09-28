"""Delta DDL for the gold dimension tables and the silver work / integration tables this project owns."""

from pyspark.sql import SparkSession

from wwi_dimensions import specs


def dimensionTableName(catalog: str, dimensionSpec: specs.DimensionSpec) -> str:
    return dimensionSpec.fullTableName(catalog)


def ensureDimensionTable(spark: SparkSession, catalog: str, dimensionSpec: specs.DimensionSpec) -> str:
    table = dimensionSpec.fullTableName(catalog)
    cols = ",\n            ".join(f"{name} {dtype}" for name, dtype in dimensionSpec.allColumns)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {cols}
        ) USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = false, delta.autoOptimize.optimizeWrite = true)
        """
    )
    return table


def ensureSalespersonTerritoryBridge(spark: SparkSession, catalog: str) -> str:
    table = f"{catalog}.gold.dim_salesperson_territory_bridge"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            SalespersonKey       INT           NOT NULL,
            SalesTerritoryKey    INT           NOT NULL,
            AllocationPercent    DECIMAL(9,4)  NOT NULL,
            IsCurrentAssignment  BOOLEAN       NOT NULL,
            AssignmentStarted    TIMESTAMP     NOT NULL,
            AssignmentEnded      TIMESTAMP,
            LineageKey           BIGINT
        ) USING DELTA
        """
    )
    return table


def ensureLateArrivingQueue(spark: SparkSession, catalog: str) -> str:
    """work.LateArrivingDimensionQueue -> silver.work_late_arriving_dimension_queue (30_work_tables.sql)."""
    table = f"{catalog}.silver.work_late_arriving_dimension_queue"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            QueueRowId             BIGINT     NOT NULL,
            BatchId                BIGINT     NOT NULL,
            PackageExecutionId     BIGINT,
            DimensionName          STRING     NOT NULL,
            MissingBusinessKey     STRING     NOT NULL,
            SourceSystemCode       STRING,
            FirstSeenObjectName    STRING,
            FirstSeenAtUtc         TIMESTAMP  NOT NULL,
            OccurrenceCount        INT        NOT NULL,
            InferredAttributesJson STRING,
            StubCreatedFlag        BOOLEAN    NOT NULL,
            StubCreatedAtUtc       TIMESTAMP,
            PlaceholderKey         INT,
            RetryCount             INT        NOT NULL,
            ResolvedFlag           BOOLEAN    NOT NULL,
            ResolvedAtUtc          TIMESTAMP,
            ResolvedByExecutionId  BIGINT,
            ResolutionNote         STRING
        ) USING DELTA
        """
    )
    return table


def ensureFactRekeyQueue(spark: SparkSession, catalog: str) -> str:
    """work.FactRekeyQueue -> silver.work_fact_rekey_queue (30_work_tables.sql)."""
    table = f"{catalog}.silver.work_fact_rekey_queue"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            QueueRowId            BIGINT     NOT NULL,
            BatchId               BIGINT     NOT NULL,
            PackageExecutionId    BIGINT,
            FactObjectName        STRING     NOT NULL,
            FactBusinessKey       STRING     NOT NULL,
            DimensionName         STRING     NOT NULL,
            CurrentSurrogateKey   BIGINT,
            CorrectedSurrogateKey BIGINT,
            RekeyReasonCode       STRING     NOT NULL,
            EffectiveDate         DATE,
            RekeyPriority         SMALLINT   NOT NULL,
            AppliedFlag           BOOLEAN    NOT NULL,
            AppliedAtUtc          TIMESTAMP,
            AttemptCount          SMALLINT   NOT NULL,
            LastErrorText         STRING,
            CreatedAtUtc          TIMESTAMP  NOT NULL
        ) USING DELTA
        """
    )
    return table


def ensureInferredMemberQueue(spark: SparkSession, catalog: str) -> str:
    """Integration.InferredMemberQueue -> silver.int_inferred_member_queue."""
    table = f"{catalog}.silver.int_inferred_member_queue"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            DimensionName      STRING     NOT NULL,
            BusinessKey        STRING     NOT NULL,
            SurrogateKey       INT        NOT NULL,
            SourceSystemCode   STRING,
            RegionCode         STRING,
            EnrichmentStatus   STRING     NOT NULL,
            CreatedBatchId     BIGINT,
            CreatedAtUtc       TIMESTAMP  NOT NULL,
            EnrichedBatchId    BIGINT,
            EnrichedAtUtc      TIMESTAMP,
            AttributeValue     STRING
        ) USING DELTA
        """
    )
    return table


def ensureDimensionLoadAudit(spark: SparkSession, catalog: str) -> str:
    """Integration.DimensionLoadAudit -> silver.int_dimension_load_audit."""
    table = f"{catalog}.silver.int_dimension_load_audit"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            BatchId              BIGINT     NOT NULL,
            PackageExecutionId   BIGINT,
            DimensionName        STRING     NOT NULL,
            RegionCode           STRING,
            RowsRead             BIGINT,
            RowsInserted         BIGINT,
            RowsType2Versioned   BIGINT,
            RowsType1Updated     BIGINT,
            RowsClosedOut        BIGINT,
            RowsInferredEnriched BIGINT,
            RowsRejected         BIGINT,
            RowsUnchanged        BIGINT,
            LoadedAtUtc          TIMESTAMP  NOT NULL
        ) USING DELTA
        """
    )
    return table


def tableExists(spark: SparkSession, fullName: str) -> bool:
    try:
        return spark.catalog.tableExists(fullName)
    except Exception:  # pragma: no cover - older Spark builds without 3-part tableExists
        catalog, schema, name = fullName.split(".")
        return spark.sql(f"SHOW TABLES IN {catalog}.{schema} LIKE '{name}'").count() > 0
