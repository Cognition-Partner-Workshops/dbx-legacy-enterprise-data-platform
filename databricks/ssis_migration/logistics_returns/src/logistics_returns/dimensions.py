"""Surrogate-key lookups against the legacy warehouse dimensions (read via federation, never rebuilt here).

Every lookup degrades to an empty frame when the dimension is empty or unreachable on the baseline host, so the
facts fall back to the unknown member exactly like the SSIS Lookup no-match outputs did.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_returns.common import readLegacy
from logistics_returns.config import UNKNOWN_MEMBER_KEY, RunContext

NOT_APPLICABLE_KEY = -1

LOOKUP_SCHEMA = T.StructType(
    [
        T.StructField("natural_key", T.StringType()),
        T.StructField("dim_key", T.IntegerType()),
        T.StructField("dim_region_code", T.StringType()),
    ]
)


@dataclass(frozen=True)
class DimensionSpec:
    table: str
    naturalColumn: str
    keyColumn: str
    regionColumn: str | None = None
    currentOnly: bool = True


CUSTOMER = DimensionSpec("Customer", "WWI Customer ID", "Customer Key", "Region Code")
STOCK_ITEM = DimensionSpec("Stock Item", "WWI Stock Item ID", "Stock Item Key", "Listing Region Code")
CARRIER = DimensionSpec("Carrier", "Carrier Code", "Carrier Key", "Region Code", currentOnly=False)
WAREHOUSE_SITE = DimensionSpec("Warehouse Site", "Warehouse Site Code", "Warehouse Site Key", "Region Code", currentOnly=False)
SALES_TERRITORY = DimensionSpec(
    "Sales Territory", "Sales Territory Code", "Sales Territory Key", "Region Code", currentOnly=False
)
SALESPERSON = DimensionSpec("Salesperson", "WWI Employee ID", "Salesperson Key", "Region Code")
CURRENCY = DimensionSpec("Currency", "Currency Code", "Currency Key", None, currentOnly=False)
RETURN_REASON = DimensionSpec("Return Reason", "Return Reason Code", "Return Reason Key", "Region Code", currentOnly=False)


def dimensionLookup(ctx: RunContext, spec: DimensionSpec) -> DataFrame:
    """``natural_key`` (string) -> ``dim_key`` for the current rows of a legacy dimension; empty on failure."""
    spark = ctx.spark
    try:
        dim = readLegacy(spark, ctx.legacyDw, "Dimension", spec.table)
        if spec.currentOnly and "Is Current Row" in dim.columns:
            dim = dim.where(F.coalesce(F.col("Is Current Row").cast("boolean"), F.lit(True)))
        dim = dim.where(F.col(spec.keyColumn).cast("int") > 0)
        region = F.upper(F.trim(F.col(spec.regionColumn))) if spec.regionColumn else F.lit(None).cast("string")
        return (
            dim.select(
                F.upper(F.trim(F.col(spec.naturalColumn).cast("string"))).alias("natural_key"),
                F.col(spec.keyColumn).cast("int").alias("dim_key"),
                region.alias("dim_region_code"),
            )
            .where(F.col("natural_key").isNotNull())
            .dropDuplicates(["natural_key"])
        )
    except Exception:  # noqa: BLE001 - unreachable / missing dimension == every lookup misses
        return spark.createDataFrame([], LOOKUP_SCHEMA)


def attachKey(
    df: DataFrame,
    lookup: DataFrame,
    naturalColumn: str,
    outColumn: str,
    default: int = UNKNOWN_MEMBER_KEY,
    nullDefault: int | None = None,
) -> DataFrame:
    """Left-join the surrogate key; misses get ``default`` and NULL natural keys get ``nullDefault`` (or default)."""
    ref = lookup.select(F.col("natural_key").alias("_nk"), F.col("dim_key").alias(outColumn))
    natural = F.upper(F.trim(F.col(naturalColumn).cast("string")))
    joined = df.withColumn("_nk", natural).join(ref, "_nk", "left").drop("_nk")
    missing = (
        F.lit(default)
        if nullDefault is None
        else F.when(F.col(naturalColumn).isNull(), F.lit(nullDefault)).otherwise(F.lit(default))
    )
    return joined.withColumn(outColumn, F.coalesce(F.col(outColumn), missing).cast("int"))


def inferredMemberFlag(*keyColumns: str) -> F.Column:
    """True when any dimension lookup fell back to the unknown member."""
    conditions = [F.col(c) == UNKNOWN_MEMBER_KEY for c in keyColumns]
    flag = conditions[0]
    for c in conditions[1:]:
        flag = flag | c
    return flag


def legacyFactSale(ctx: RunContext) -> DataFrame:
    """Fact.Sale invoice-level view used for original-sale lookups (cost reversal, restatement)."""
    try:
        sale = readLegacy(ctx.spark, ctx.legacyDw, "Fact", "Sale")
    except Exception:  # noqa: BLE001
        return ctx.spark.createDataFrame([], legacyFactSaleSchema())
    return sale.groupBy(F.col("WWI Invoice ID").cast("long").alias("original_invoice_id")).agg(
        F.min("Invoice Date Key").cast("date").alias("original_invoice_date_key"),
        F.min("Delivery Date Key").cast("date").alias("original_delivery_date_key"),
        F.max("Invoice Number").alias("original_invoice_number"),
        F.max("Customer Key").cast("int").alias("original_customer_key"),
        F.max("Bill To Customer Key").cast("int").alias("original_bill_to_customer_key"),
        F.max("Salesperson Key").cast("int").alias("original_salesperson_key"),
        F.max("Sales Territory Key").cast("int").alias("original_sales_territory_key"),
        F.sum("Quantity").cast("decimal(18,3)").alias("original_quantity"),
        F.sum("Total Excluding Tax").cast("decimal(19,4)").alias("original_net_amount"),
        F.sum("Tax Amount").cast("decimal(19,4)").alias("original_tax_amount"),
        F.sum(F.coalesce(F.col("Cost Of Sale Amount"), F.col("Total Excluding Tax") - F.col("Profit")))
        .cast("decimal(19,4)")
        .alias("original_cost_amount"),
        F.max("Tax Rate").cast("decimal(9,3)").alias("original_tax_rate"),
    )


def legacyFactSaleSchema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("original_invoice_id", T.LongType()),
            T.StructField("original_invoice_date_key", T.DateType()),
            T.StructField("original_delivery_date_key", T.DateType()),
            T.StructField("original_invoice_number", T.StringType()),
            T.StructField("original_customer_key", T.IntegerType()),
            T.StructField("original_bill_to_customer_key", T.IntegerType()),
            T.StructField("original_salesperson_key", T.IntegerType()),
            T.StructField("original_sales_territory_key", T.IntegerType()),
            T.StructField("original_quantity", T.DecimalType(18, 3)),
            T.StructField("original_net_amount", T.DecimalType(19, 4)),
            T.StructField("original_tax_amount", T.DecimalType(19, 4)),
            T.StructField("original_cost_amount", T.DecimalType(19, 4)),
            T.StructField("original_tax_rate", T.DecimalType(9, 3)),
        ]
    )


def returnReasonWindows(ctx: RunContext) -> DataFrame:
    """(reason code, region) -> statutory window days: Dimension.Return Reason, else OLTP Returns.ReturnReasons."""
    schema = T.StructType(
        [
            T.StructField("reason_code", T.StringType()),
            T.StructField("reason_region_code", T.StringType()),
            T.StructField("reason_window_days", T.IntegerType()),
            T.StructField("reason_is_quality_defect", T.BooleanType()),
        ]
    )
    try:
        dim = readLegacy(ctx.spark, ctx.legacyDw, "Dimension", "Return Reason")
        rows = dim.where(F.col("Return Reason Key").cast("int") > 0).select(
            F.upper(F.trim(F.col("Return Reason Code"))).alias("reason_code"),
            F.upper(F.trim(F.col("Region Code"))).alias("reason_region_code"),
            F.col("Return Window Days").cast("int").alias("reason_window_days"),
            F.col("Is Quality Defect").cast("boolean").alias("reason_is_quality_defect"),
        )
        if rows.limit(1).count() > 0:
            return rows.dropDuplicates(["reason_code", "reason_region_code"])
    except Exception:  # noqa: BLE001
        pass
    try:
        oltp = ctx.spark.table(ctx.legacy(ctx.legacyOltp, "Returns", "ReturnReasons"))
        return oltp.select(
            F.upper(F.trim(F.col("ReasonCode"))).alias("reason_code"),
            F.upper(F.trim(F.col("RegionCode"))).alias("reason_region_code"),
            F.col("ReturnWindowDays").cast("int").alias("reason_window_days"),
            (F.upper(F.col("ReasonCategory")) == "QUALITY").alias("reason_is_quality_defect"),
        ).dropDuplicates(["reason_code", "reason_region_code"])
    except Exception:  # noqa: BLE001
        return ctx.spark.createDataFrame([], schema)
