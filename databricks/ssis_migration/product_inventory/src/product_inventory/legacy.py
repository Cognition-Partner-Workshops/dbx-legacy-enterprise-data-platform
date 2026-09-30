"""Read-only access to legacy objects owned by other groups (Dimension.Customer,
Dimension.Supplier, Dimension.Transaction Type, Dimension.Date) and to the SSIS
outputs used as the reconciliation baseline.

Objects whose SQL Server names contain spaces cannot be addressed with three-level
federation names, so they are read through `remote_query` pushdown on the
`wwi_legacy_sqlserver` connection.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from product_inventory.config import PipelineConfig


def remoteQuery(spark: SparkSession, cfg: PipelineConfig, database: str, tsql: str) -> DataFrame:
    escaped = tsql.replace("\\", "\\\\").replace("'", "\\'")
    return spark.sql(
        f"SELECT * FROM remote_query('{cfg.sqlServerConnection}', database => '{database}', query => '{escaped}')"
    )


def legacyDwQuery(spark: SparkSession, cfg: PipelineConfig, tsql: str) -> DataFrame:
    return remoteQuery(spark, cfg, cfg.dwDatabase, tsql)


def legacyStagingQuery(spark: SparkSession, cfg: PipelineConfig, tsql: str) -> DataFrame:
    return remoteQuery(spark, cfg, cfg.stagingDatabase, tsql)


def readLegacyCustomerKeys(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return (
        spark.table(cfg.dw("Dimension", "Customer"))
        .where(F.col("`Valid To`") >= F.lit("9999-01-01"))
        .select(F.col("`Customer Key`").cast("long").alias("customer_key"), F.col("`WWI Customer ID`").cast("int").alias("wwi_customer_id"))
    )


def readLegacySupplierKeys(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return (
        spark.table(cfg.dw("Dimension", "Supplier"))
        .where(F.col("`Valid To`") >= F.lit("9999-01-01"))
        .select(F.col("`Supplier Key`").cast("long").alias("supplier_key"), F.col("`WWI Supplier ID`").cast("int").alias("wwi_supplier_id"))
    )


def readLegacyTransactionTypeKeys(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    df = legacyDwQuery(
        spark,
        cfg,
        "SELECT [Transaction Type Key] AS transaction_type_key, [WWI Transaction Type ID] AS wwi_transaction_type_id, "
        "[Transaction Type] AS transaction_type FROM [Dimension].[Transaction Type]",
    )
    # one key per WWI id (the legacy dimension carries a duplicate for 'Contra')
    return (
        df.groupBy("wwi_transaction_type_id")
        .agg(F.min("transaction_type_key").alias("transaction_type_key"))
        .select(F.col("transaction_type_key").cast("long"), F.col("wwi_transaction_type_id").cast("int"))
    )


def readLegacyDateKeys(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return spark.table(cfg.dw("Dimension", "Date")).select(F.col("`Date`").cast("date").alias("date_key"))
