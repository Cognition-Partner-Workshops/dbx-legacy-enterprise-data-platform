"""Surrogate-key lookups against the legacy dimensions (wwi_legacy_dw.Dimension.*).

Two flavours exist in the estate and both are kept:
* asOfLookup  - the WWI Integration.MigrateStaged* rule the populated facts were built with
                (`modified > [Valid From] AND modified <= [Valid To]`, first Valid From wins, else 0);
* currentLookup - the SSIS Lookup component (`[Is Current Row] = 1`, no-match -> 0 / hold).
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_o2c.config import DW_CATALOG, UNKNOWN_KEY

DW_CONNECTION = "wwi_legacy_sqlserver"
DW_DATABASE = "WideWorldImportersDW"

DIMS = {
    "Customer": ("Customer", "Customer Key", "WWI Customer ID"),
    "StockItem": ("Stock Item", "Stock Item Key", "WWI Stock Item ID"),
    "City": ("City", "City Key", "WWI City ID"),
    "Employee": ("Employee", "Employee Key", "WWI Employee ID"),
    "Supplier": ("Supplier", "Supplier Key", "WWI Supplier ID"),
    "TransactionType": ("Transaction Type", "Transaction Type Key", "WWI Transaction Type ID"),
    "PaymentMethod": ("Payment Method", "Payment Method Key", "WWI Payment Method ID"),
}


def dimension(spark: SparkSession, name: str) -> DataFrame:
    table, keyCol, bizCol = DIMS[name]
    if " " in table:  # federation cannot address identifiers with spaces -> read-only remote_query on the same connection
        df = spark.sql(f"SELECT * FROM remote_query('{DW_CONNECTION}', database => '{DW_DATABASE}', query => 'SELECT * FROM Dimension.[{table}]')")
    else:
        df = spark.table(f"{DW_CATALOG}.Dimension.`{table}`")
    cols = [F.col(f"`{keyCol}`").alias("dim_key"), F.col(f"`{bizCol}`").alias("dim_biz"), F.col("`Valid From`").alias("valid_from"), F.col("`Valid To`").alias("valid_to")]
    if "Is Current Row" in df.columns:
        cols.append(F.col("`Is Current Row`").alias("is_current"))
    return df.select(*cols)


def asOfLookup(df: DataFrame, dim: DataFrame, bizCol: str, asOfCol: str, outCol: str) -> DataFrame:
    """Temporal lookup; unresolved -> UNKNOWN_KEY (0). Pure DataFrame logic, unit-tested locally."""
    d = dim.select("dim_key", "dim_biz", "valid_from", "valid_to")
    joined = df.join(
        d,
        (F.col(bizCol) == d.dim_biz) & (F.col(asOfCol) > d.valid_from) & (F.col(asOfCol) <= d.valid_to),
        "left",
    )
    keyCols = [c for c in df.columns]
    # the legacy dimensions carry duplicate SCD2 versions for one interval; the SSIS lookup returns the lowest key
    w = Window.partitionBy(*[F.col(c) for c in keyCols]).orderBy(F.col("valid_from").asc_nulls_last(), F.col("dim_key").asc_nulls_last())
    return (
        joined.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .withColumn(outCol, F.coalesce(F.col("dim_key"), F.lit(UNKNOWN_KEY)).cast("int"))
        .drop("_rn", "dim_key", "dim_biz", "valid_from", "valid_to")
    )


def currentLookup(df: DataFrame, dim: DataFrame, bizCol: str, outCol: str, unknown=UNKNOWN_KEY) -> DataFrame:
    """SSIS Lookup against [Is Current Row] = 1; returns null when unknown is None (caller routes the row)."""
    d = dim.filter(F.col("is_current") == True) if "is_current" in dim.columns else dim  # noqa: E712
    d = d.groupBy(F.col("dim_biz").alias(bizCol)).agg(F.min("dim_key").alias(outCol))
    out = df.join(d, bizCol, "left")
    if unknown is not None:
        out = out.withColumn(outCol, F.coalesce(F.col(outCol), F.lit(unknown)).cast("int"))
    return out


def dateKey(df: DataFrame, dates: DataFrame, dateCol: str, outCol: str) -> DataFrame:
    """Dimension.Date lookup (NoMatchBehavior=1 -> null date key)."""
    d = dates.select(F.col("`Date`").alias(dateCol)).distinct().withColumn(outCol, F.col(dateCol))
    return df.join(d, dateCol, "left")
