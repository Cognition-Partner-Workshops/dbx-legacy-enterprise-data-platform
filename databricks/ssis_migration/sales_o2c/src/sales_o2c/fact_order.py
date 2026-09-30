"""08_facts: FACT_Load_Order (incremental_fact, order-line grain, hold-and-retry for early-arriving members).

stg_order_line x stg_order -> lookups -> Fact.Order shape. Lines whose customer / stock item is not yet in the
dimension are parked in work_order_line_enriched (SSIS "Early Arriving" outputs); every run retries the parked
rows, and rows past the retry limit are loaded against the unknown member (key 0) as Integration.LoadFactOrder
@LoadHeldRows = 1 does. Cancelled lines go to err_rejected_order_line.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import RunContext
from sales_o2c.dimensions import asOfLookup, dimension
from sales_o2c.tables import appendRows, deleteWhere, mergeUpsert, tableExists, withAudit
from sales_o2c.watermark import TIMESTAMP, getWatermark, setWatermark

LEGACY_BUSINESS_COLUMNS = [
    "city_key", "customer_key", "stock_item_key", "order_date_key", "picked_date_key", "salesperson_key", "picker_key",
    "wwi_order_id", "wwi_backorder_id", "description", "package", "quantity", "unit_price", "tax_rate",
    "total_excluding_tax", "tax_amount", "total_including_tax",
]


def computeOrderMeasures(df: DataFrame) -> DataFrame:
    """Pure Derived Column rules of FACT_Load_Order (+ the legacy Fact.Order amount rules)."""
    qty = F.col("ordered_quantity").cast("decimal(18,4)")
    picked = F.coalesce(F.col("picked_quantity"), F.lit(0)).cast("decimal(18,4)")
    price = F.col("unit_price_amount").cast("decimal(18,2)")
    rate = F.coalesce(F.col("tax_rate_percent"), F.lit(0)).cast("decimal(18,3)")
    discountPct = F.coalesce(F.col("line_discount_percent"), F.lit(0)).cast("decimal(9,4)")
    gross = (qty * price).cast("decimal(18,2)")
    backorder = F.col("backorder_order_id").cast("string")
    return (
        df.withColumn("ordered_gross_amount", gross)
        .withColumn("discount_amount", (gross * discountPct / 100).cast("decimal(18,2)"))
        .withColumn("ordered_net_amount", (gross - gross * discountPct / 100).cast("decimal(18,2)"))
        .withColumn("quantity_outstanding", (qty - picked).cast("int"))
        .withColumn("is_backordered", (picked < qty) & backorder.isNotNull() & (F.length(F.trim(backorder)) > 0))
        .withColumn("fill_rate_percent", F.when(qty == 0, F.lit(0)).otherwise(picked / qty * 100).cast("decimal(9,4)"))
        .withColumn("total_excluding_tax", F.round(qty * price, 2).cast("decimal(18,2)"))
        .withColumn("tax_amount", F.round(qty * price * rate / 100, 2).cast("decimal(18,2)"))
        .withColumn("total_including_tax", F.round(qty * price * (1 + rate / 100), 2).cast("decimal(18,2)"))
        .withColumn("is_cancelled", F.upper(F.coalesce(F.col("order_status_code"), F.lit(""))).isin("CANC", "CANCELLED") | F.upper(F.coalesce(F.col("line_status_code"), F.lit(""))).isin("CANC", "CANCELLED"))
    )


def splitHeld(df: DataFrame, holdRetryLimit: int):
    """Hold rows with unresolved customer/stock item unless they exhausted the retry budget -> (ready, held)."""
    unresolved = (F.col("customer_key") == 0) | (F.col("stock_item_key") == 0)
    retries = F.coalesce(F.col("hold_retry_count"), F.lit(0))
    held = df.filter(unresolved & (retries < holdRetryLimit))
    ready = df.filter(~unresolved | (retries >= holdRetryLimit))
    return ready, held


def toFactOrderRows(df: DataFrame, ctx: RunContext) -> DataFrame:
    return df.select(
        F.col("order_line_id").alias("wwi_order_line_id"),
        F.col("city_key"), F.col("customer_key"), F.col("stock_item_key"),
        F.col("order_date").alias("order_date_key"),
        F.col("picking_completed_when_utc").cast("date").alias("picked_date_key"),
        F.col("salesperson_key"), F.col("picker_key"),
        F.col("order_id").alias("wwi_order_id"),
        F.col("backorder_order_id").cast("int").alias("wwi_backorder_id"),
        F.col("line_description").alias("description"),
        F.col("package_type_code").alias("package"),
        F.col("ordered_quantity").cast("int").alias("quantity"),
        F.col("unit_price_amount").alias("unit_price"),
        F.col("tax_rate_percent").alias("tax_rate"),
        F.col("total_excluding_tax"), F.col("tax_amount"), F.col("total_including_tax"),
        F.lit(int(ctx.packageExecutionId)).cast("bigint").alias("lineage_key"),
        F.col("expected_delivery_date").alias("expected_delivery_date_key"),
        F.col("ordered_gross_amount"), F.col("discount_amount"), F.col("ordered_net_amount"),
        F.col("picked_quantity").cast("int").alias("quantity_picked"), F.col("quantity_outstanding"),
        F.col("is_backordered"), F.col("fill_rate_percent"), F.col("order_status_code"), F.col("region_code"),
        F.col("customer_id").alias("wwi_customer_id"),
        F.col("last_modified_when"),
    )


def enrichOrderLines(spark: SparkSession, lines: DataFrame) -> DataFrame:
    df = lines.withColumn("last_modified_when", F.greatest(F.col("source_modified_date"), F.coalesce(F.col("header_modified_date"), F.col("source_modified_date"))))
    df = asOfLookup(df, dimension(spark, "Customer"), "customer_id", "last_modified_when", "customer_key")
    df = asOfLookup(df, dimension(spark, "StockItem"), "stock_item_id", "last_modified_when", "stock_item_key")
    df = asOfLookup(df, dimension(spark, "City"), "delivery_city_id", "last_modified_when", "city_key")
    df = asOfLookup(df, dimension(spark, "Employee"), "salesperson_id", "last_modified_when", "salesperson_key")
    df = asOfLookup(df, dimension(spark, "Employee"), "picked_by_person_id", "last_modified_when", "picker_key")
    return df


HOLD_COLUMNS = [
    "order_line_id", "order_line_business_key", "order_business_key", "order_id", "customer_id", "stock_item_id", "salesperson_id",
    "picked_by_person_id", "delivery_city_id", "backorder_order_id", "region_code", "order_date", "expected_delivery_date",
    "ordered_quantity", "picked_quantity", "unit_price_amount", "tax_rate_percent", "line_discount_percent", "line_description",
    "package_type_code", "order_status_code", "line_status_code", "picking_completed_when_utc", "source_modified_date", "header_modified_date",
]


def runFactLoadOrder(spark: SparkSession, ctx: RunContext, holdRetryLimit: int = 3) -> dict:
    wmFrom = getWatermark(spark, ctx, "Fact.Order", TIMESTAMP)
    lines = spark.table(ctx.table("stg_order_line"))
    if "dq_status_code" in lines.columns:
        lines = lines.filter(F.coalesce(F.col("dq_status_code"), F.lit("PASS")) != "FAIL")
    orders = spark.table(ctx.table("stg_order")).filter(F.col("delete_flag") == "N").select(
        "order_business_key", "customer_id", "salesperson_id", "picked_by_person_id", "delivery_city_id", "backorder_order_id",
        "region_code", "order_date", "expected_delivery_date", "order_status_code", F.col("source_modified_date").alias("header_modified_date"),
    )
    lines = lines.join(orders, "order_business_key", "inner")
    if not ctx.reloadFullHistory:
        lines = lines.filter(F.greatest(F.col("source_modified_date"), F.col("header_modified_date")) > F.lit(wmFrom).cast("timestamp"))
    lines = lines.select(*HOLD_COLUMNS).withColumn("hold_retry_count", F.lit(0))

    holdTable = ctx.table("work_order_line_enriched")
    if tableExists(spark, holdTable):
        held = spark.table(holdTable).filter(~F.col("is_ready_for_fact")).select(*HOLD_COLUMNS, (F.col("hold_retry_count") + 1).alias("hold_retry_count"))
        lines = lines.unionByName(held).dropDuplicates(["order_line_id"])

    measured = computeOrderMeasures(enrichOrderLines(spark, lines))
    cancelled = measured.filter(F.col("is_cancelled"))
    candidates = measured.filter(~F.col("is_cancelled"))
    # a full-history reload has no later run to retry into: unresolved members load as the unknown member (key 0) at once
    ready, heldNow = splitHeld(candidates, 0 if ctx.reloadFullHistory else holdRetryLimit)

    rowsInserted = mergeUpsert(spark, ctx.table("gold_fact_order"), withAudit(toFactOrderRows(ready, ctx), ctx), ["wwi_order_line_id"])
    # refresh the hold table: parked rows of this run replace the previous parking state
    deleteWhere(spark, holdTable, "1 = 1")
    heldRows = heldNow.select(*HOLD_COLUMNS, "hold_retry_count", "customer_key", "stock_item_key").withColumn(
        "lookup_failure_list",
        F.concat_ws(",", F.when(F.col("customer_key") == 0, F.lit("Customer")), F.when(F.col("stock_item_key") == 0, F.lit("StockItem"))),
    ).withColumn("is_ready_for_fact", F.lit(False)).withColumn("last_retried_at", F.current_timestamp())
    rowsHeld = appendRows(spark, holdTable, withAudit(heldRows, ctx))
    queue = heldNow.filter(F.col("customer_key") == 0).groupBy(F.col("customer_id").cast("string").alias("missing_business_key")).agg(F.count("*").alias("occurrence_count")).withColumn("dimension_name", F.lit("Customer")).withColumn("first_seen_object_name", F.lit("FACT_Load_Order")).withColumn("first_seen_at_utc", F.current_timestamp()).withColumn("resolved_flag", F.lit(False))
    appendRows(spark, ctx.table("work_late_arriving_dimension_queue"), withAudit(queue, ctx))
    rowsRejected = appendRows(
        spark, ctx.table("err_rejected_order_line"),
        withAudit(cancelled.select(
            F.col("order_business_key"), F.col("order_line_business_key"), F.col("stock_item_id").cast("string").alias("stock_item_reference"),
            F.col("ordered_quantity").cast("string").alias("ordered_quantity_text"), F.col("unit_price_amount").cast("string").alias("unit_price_text"),
            F.lit("CANCELLED_LINE").alias("reject_reason_code"), F.lit("Fact").alias("reject_stage"), F.lit("SKIPPED").alias("reprocess_status_code"), F.current_timestamp().alias("rejected_at_utc"),
        ), ctx),
    )
    maxLm = measured.agg(F.max("last_modified_when")).first()[0]
    if maxLm is not None:
        setWatermark(spark, ctx, "Fact.Order", TIMESTAMP, maxLm.isoformat())
    return {"rowsRead": rowsInserted + rowsHeld + rowsRejected, "rowsInserted": rowsInserted, "rowsRejected": rowsRejected, "rowsHeld": rowsHeld}
