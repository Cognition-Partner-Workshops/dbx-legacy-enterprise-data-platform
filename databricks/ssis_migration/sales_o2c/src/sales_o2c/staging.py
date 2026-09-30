"""04_staging: STG_Load_Order and STG_Load_Sale (incremental_append keyed on LastEditedWhen).

raw_* -> stg_* with the SSIS Derived Column rules, the in-package validity split (rejects to err_*) and the
stg.usp_AppendIncremental_* semantics (rows of the current batch newer than the Timestamp watermark; orphan
lines and null/negative numerics rejected; unknown customers queued to work_late_arriving_dimension_queue).
Append is realised as MERGE on the business key so that a rerun of the same batch is idempotent.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import SOURCE_SYSTEM_CODE, RunContext
from sales_o2c.tables import appendRows, mergeUpsert, withAudit
from sales_o2c.watermark import TIMESTAMP, getWatermark, setWatermark


def regionCodeOf(col):
    """STG_Load_Sale: RegionCode = UPPER(TRIM(ISNULL(BillToRegionCode) ? 'NA' : BillToRegionCode))."""
    return F.upper(F.trim(F.coalesce(col, F.lit("NA"))))


def taxRegimeOf(regionCol):
    return F.when(regionCol == "EU", "VAT").when(regionCol == "APAC", "GST").otherwise("SUT")


def changeHash(*cols):
    return F.md5(F.concat_ws("|", *[F.upper(F.trim(F.coalesce(c.cast("string"), F.lit("")))) for c in cols]))


# ------------------------------------------------------------------ STG_Load_Order (pure rules)
def transformOrder(raw: DataFrame) -> DataFrame:
    comments = F.regexp_replace(F.regexp_replace(F.col("comments"), "\r", " "), "\n", " ")
    return raw.select(
        F.col("order_id").cast("bigint").alias("order_id"),
        F.concat(F.lit(SOURCE_SYSTEM_CODE), F.lit("|"), F.col("order_id")).alias("order_business_key"),
        F.col("customer_id").alias("customer_id"),
        F.coalesce(F.col("salesperson_person_id"), F.lit(-1)).alias("salesperson_id"),
        F.col("picked_by_person_id"),
        F.col("delivery_city_id"),
        F.col("contact_person_id"),
        F.col("backorder_order_id"),
        F.col("order_date"),
        F.col("expected_delivery_date"),
        F.when(F.col("customer_purchase_order_number").isNull(), F.lit("")).otherwise(F.upper(F.trim("customer_purchase_order_number"))).alias("customer_po_number"),
        F.when(F.col("is_undersupply_backordered") == True, "Y").otherwise("N").alias("backorder_flag"),  # noqa: E712
        F.when(F.col("comments").isNull(), F.lit(None).cast("string")).otherwise(F.substring(F.trim(comments), 1, 400)).alias("order_comments"),
        F.col("delivery_instructions").alias("delivery_instructions_text"),
        F.col("sales_channel_code"),
        F.col("sales_territory_code"),
        regionCodeOf(F.col("region_code")).alias("region_code"),
        F.coalesce(F.col("order_status_code"), F.lit("OPEN")).alias("order_status_code"),
        F.upper(F.trim(F.coalesce(F.col("currency_code"), F.lit("USD")))).alias("transaction_currency_code"),
        F.col("picking_completed_when"),
        F.col("last_edited_when").alias("source_modified_date"),
        F.col("delete_flag"),
    ).withColumn(
        "row_hash", changeHash(F.col("order_id"), F.col("customer_id"), F.col("backorder_flag"), F.col("expected_delivery_date"))
    )


def transformOrderLine(raw: DataFrame) -> DataFrame:
    unitPrice = F.col("unit_price").cast("decimal(18,2)")
    qty = F.col("quantity")
    return raw.select(
        F.col("order_line_id").cast("bigint").alias("order_line_id"),
        F.concat(F.lit(SOURCE_SYSTEM_CODE), F.lit("|"), F.col("order_line_id")).alias("order_line_business_key"),
        F.col("order_id").cast("bigint").alias("order_id"),
        F.concat(F.lit(SOURCE_SYSTEM_CODE), F.lit("|"), F.col("order_id")).alias("order_business_key"),
        F.col("stock_item_id"),
        F.trim(F.col("description")).alias("line_description"),
        F.col("package_type_name").alias("package_type_code"),
        qty.alias("ordered_quantity"),
        F.coalesce(F.col("picked_quantity"), F.lit(0)).alias("picked_quantity"),
        unitPrice.alias("unit_price_amount"),
        (qty * unitPrice).cast("decimal(18,2)").alias("extended_amount"),
        (qty * unitPrice * F.coalesce(F.col("tax_rate"), F.lit(0)) / 100).cast("decimal(18,2)").alias("line_tax_amount"),
        F.coalesce(F.col("tax_rate"), F.lit(0)).cast("decimal(18,3)").alias("tax_rate_percent"),
        F.coalesce(F.col("line_discount_amount"), F.lit(0)).cast("decimal(18,2)").alias("line_discount_amount"),
        F.coalesce(F.col("line_discount_percent"), F.lit(0)).cast("decimal(9,4)").alias("line_discount_percent"),
        F.col("promotion_code").alias("promotion_business_key"),
        F.when(F.col("picking_completed_when").isNull(), "N").otherwise("Y").alias("picked_flag"),
        F.col("picking_completed_when").alias("picking_completed_when_utc"),
        F.coalesce(F.col("line_status_code"), F.lit("OPEN")).alias("line_status_code"),
        F.col("last_edited_when").alias("source_modified_date"),
    )


def splitOrderLines(lines: DataFrame, orders: DataFrame):
    """Validity split (Quantity present & >= 0, price present) + orphan check -> (valid, rejected)."""
    orderKeys = orders.select("order_business_key").distinct()
    withOrder = lines.join(orderKeys.withColumn("_has_order", F.lit(True)), "order_business_key", "left")
    reason = (
        F.when(F.col("ordered_quantity").isNull() | F.col("unit_price_amount").isNull(), "BAD_NUMERIC")
        .when(F.col("ordered_quantity") < 0, "NEG_QTY")
        .when(F.col("_has_order").isNull(), "ORPHAN_LINE")
    )
    tagged = withOrder.withColumn("reject_reason_code", reason)
    valid = tagged.filter(F.col("reject_reason_code").isNull()).drop("reject_reason_code", "_has_order")
    rejected = tagged.filter(F.col("reject_reason_code").isNotNull()).drop("_has_order")
    return valid, rejected


def rejectedOrderLineRows(rejected: DataFrame, stage: str) -> DataFrame:
    return rejected.select(
        F.col("order_business_key"),
        F.col("order_line_business_key"),
        F.col("stock_item_id").cast("string").alias("stock_item_reference"),
        F.col("ordered_quantity").cast("string").alias("ordered_quantity_text"),
        F.col("unit_price_amount").cast("string").alias("unit_price_text"),
        F.col("reject_reason_code"),
        F.lit(stage).alias("reject_stage"),
        F.to_json(F.struct(*[c for c in rejected.columns if not c.startswith("_")])).alias("record_payload"),
        F.lit("PENDING").alias("reprocess_status_code"),
        F.current_timestamp().alias("rejected_at_utc"),
    )


def lateArrivingRows(df: DataFrame, dimensionName: str, keyCol: str, firstSeenObject: str) -> DataFrame:
    return (
        df.groupBy(F.col(keyCol).cast("string").alias("missing_business_key"))
        .agg(F.count("*").alias("occurrence_count"))
        .withColumn("dimension_name", F.lit(dimensionName))
        .withColumn("first_seen_object_name", F.lit(firstSeenObject))
        .withColumn("first_seen_at_utc", F.current_timestamp())
        .withColumn("resolved_flag", F.lit(False))
    )


def _incrementalFilter(df: DataFrame, ctx: RunContext, watermarkFrom: str) -> DataFrame:
    """stg.usp_AppendIncremental_*: current batch AND (reload OR LastEditedWhen > @from OR LastEditedWhen null)."""
    if ctx.reloadFullHistory:
        return df
    return df.filter((F.col("last_edited_when") > F.lit(watermarkFrom).cast("timestamp")) | F.col("last_edited_when").isNull())


def runStgLoadOrder(spark: SparkSession, ctx: RunContext) -> dict:
    wmFrom = getWatermark(spark, ctx, "stg.OrderLine", TIMESTAMP)
    rawOrders = _incrementalFilter(spark.table(ctx.table("raw_sql_order")).filter(F.col("delete_flag") == "N"), ctx, wmFrom)
    rawLines = _incrementalFilter(spark.table(ctx.table("raw_sql_order_line")), ctx, wmFrom)
    orders = withAudit(transformOrder(rawOrders), ctx)
    allOrders = spark.table(ctx.table("raw_sql_order")).filter(F.col("delete_flag") == "N").select(
        F.concat(F.lit(SOURCE_SYSTEM_CODE), F.lit("|"), F.col("order_id")).alias("order_business_key")
    )
    valid, rejected = splitOrderLines(transformOrderLine(rawLines), allOrders)
    rowsRead = mergeUpsert(spark, ctx.table("stg_order"), orders, ["order_business_key"])
    rowsInserted = mergeUpsert(spark, ctx.table("stg_order_line"), withAudit(valid, ctx), ["order_line_business_key"])
    rowsRejected = appendRows(spark, ctx.table("err_rejected_order_line"), withAudit(rejectedOrderLineRows(rejected, "Staging"), ctx))
    customers = spark.table("wwi_legacy_dw.Dimension.Customer").select(F.col("`WWI Customer ID`").alias("customer_id")).distinct()
    unknown = orders.join(customers, "customer_id", "left_anti")
    appendRows(spark, ctx.table("work_late_arriving_dimension_queue"), withAudit(lateArrivingRows(unknown, "Customer", "customer_id", "stg.Order"), ctx))
    maxEdited = rawLines.agg(F.max("last_edited_when")).first()[0] or rawOrders.agg(F.max("last_edited_when")).first()[0]
    if maxEdited is not None:
        setWatermark(spark, ctx, "stg.OrderLine", TIMESTAMP, maxEdited.isoformat())
    return {"rowsRead": rowsRead + rowsInserted + rowsRejected, "rowsInserted": rowsRead + rowsInserted, "rowsRejected": rowsRejected}


# ------------------------------------------------------------------ STG_Load_Sale (pure rules)
def transformSale(raw: DataFrame) -> DataFrame:
    region = regionCodeOf(F.col("bill_to_region_code"))
    return raw.select(
        F.col("invoice_id").cast("bigint").alias("invoice_id"),
        F.concat(F.lit(SOURCE_SYSTEM_CODE), F.lit("|"), F.col("invoice_id")).alias("sale_business_key"),
        F.col("customer_id"),
        F.col("bill_to_customer_id"),
        F.col("delivery_city_id"),
        F.coalesce(F.col("order_id"), F.lit(-1)).cast("bigint").alias("order_id"),
        F.col("salesperson_person_id").alias("salesperson_id"),
        F.col("invoice_date"),
        F.coalesce(F.col("is_credit_note"), F.lit(False)).alias("is_credit_note"),
        F.col("credit_note_reason").alias("credit_note_reason_text"),
        F.col("delivery_method_name").alias("delivery_method_code"),
        F.col("delivery_run").alias("delivery_run_code"),
        F.col("run_position"),
        F.col("total_dry_items"),
        F.col("total_chiller_items"),
        region.alias("region_code"),
        taxRegimeOf(region).alias("tax_regime_code"),
        F.upper(F.trim(F.coalesce(F.col("currency_code"), F.lit("USD")))).alias("transaction_currency_code"),
        F.col("invoice_date").alias("fx_effective_date"),
        F.coalesce(F.col("conversion_rate"), F.lit(1)).cast("decimal(18,6)").alias("transaction_fx_rate"),
        F.when(F.col("conversion_rate").isNull(), "Y").otherwise("N").alias("fx_imputed_flag"),
        F.when(F.col("confirmed_delivery_time").isNull(), "N").otherwise("Y").alias("delivery_confirmed_flag"),
        F.col("confirmed_delivery_time").alias("confirmed_delivery_utc"),
        F.col("confirmed_received_by").alias("confirmed_received_by_name"),
        F.col("total_excluding_tax").alias("sale_net_amount"),
        F.col("total_tax_amount").alias("sale_tax_amount"),
        F.col("total_including_tax").alias("sale_gross_amount"),
        F.col("customer_tax_registration_number"),
        F.col("last_edited_when").alias("source_modified_date"),
        F.col("delete_flag"),
    ).withColumn(
        "row_hash", changeHash(F.col("invoice_id"), F.col("customer_id"), F.col("delivery_method_code"), F.col("delivery_confirmed_flag"))
    )


def transformSaleLine(raw: DataFrame) -> DataFrame:
    qty, price, rate = F.col("quantity"), F.col("unit_price"), F.coalesce(F.col("tax_rate"), F.lit(0))
    recomputed = (qty * price * rate / 100).cast("decimal(18,2)")
    return raw.select(
        F.col("invoice_line_id").cast("bigint").alias("invoice_line_id"),
        F.concat(F.lit(SOURCE_SYSTEM_CODE), F.lit("|"), F.col("invoice_line_id")).alias("sale_line_business_key"),
        F.col("invoice_id").cast("bigint").alias("invoice_id"),
        F.concat(F.lit(SOURCE_SYSTEM_CODE), F.lit("|"), F.col("invoice_id")).alias("sale_business_key"),
        F.col("stock_item_id"),
        F.trim(F.col("description")).alias("line_description"),
        F.col("package_type_name").alias("package_type_code"),
        qty.alias("quantity"),
        price.cast("decimal(18,2)").alias("unit_price_amount"),
        (qty * price).cast("decimal(18,2)").alias("net_amount"),
        rate.cast("decimal(18,3)").alias("tax_rate_percent"),
        F.coalesce(F.col("tax_amount"), F.lit(0)).cast("decimal(18,2)").alias("tax_amount"),
        recomputed.alias("recomputed_tax_amount"),
        F.abs(F.coalesce(F.col("tax_amount"), F.lit(0)) - recomputed).cast("decimal(18,2)").alias("tax_variance_amount"),
        F.col("extended_price").cast("decimal(18,2)").alias("gross_line_amount"),
        F.coalesce(F.col("line_profit"), F.lit(0)).cast("decimal(18,2)").alias("line_profit_amount"),
        F.coalesce(F.col("last_cost_price"), F.lit(0)).cast("decimal(18,2)").alias("unit_cost"),
        F.coalesce(F.col("is_chiller_stock"), F.lit(False)).alias("is_chiller_stock"),
        F.col("last_edited_when").alias("source_modified_date"),
    )


def splitSaleLines(lines: DataFrame):
    """STG_Load_Sale conditional split: valid when TaxVarianceAmount <= 0.02 && Quantity != 0."""
    reason = (
        F.when(F.col("quantity").isNull() | (F.col("quantity") == 0), "ZERO_QTY")
        .when(F.col("tax_variance_amount") > 0.02, "TAX_VARIANCE")
    )
    tagged = lines.withColumn("reject_reason_code", reason)
    return tagged.filter(F.col("reject_reason_code").isNull()).drop("reject_reason_code"), tagged.filter(F.col("reject_reason_code").isNotNull())


def rejectedInvoiceLineRows(rejected: DataFrame, stage: str) -> DataFrame:
    return rejected.select(
        F.col("sale_business_key").alias("invoice_business_key"),
        F.col("sale_line_business_key").alias("invoice_line_business_key"),
        F.col("invoice_id").cast("string").alias("invoice_number"),
        F.col("net_amount").cast("string").alias("line_amount_text"),
        F.col("reject_reason_code"),
        F.lit(stage).alias("reject_stage"),
        F.col("recomputed_tax_amount").alias("expected_tax_amount"),
        F.col("tax_amount").alias("actual_tax_amount"),
        F.col("tax_variance_amount").alias("variance_amount"),
        F.to_json(F.struct(*rejected.columns)).alias("record_payload"),
        F.lit("PENDING").alias("reprocess_status_code"),
        F.current_timestamp().alias("rejected_at_utc"),
    )


def runStgLoadSale(spark: SparkSession, ctx: RunContext) -> dict:
    wmFrom = getWatermark(spark, ctx, "stg.SaleLine", TIMESTAMP)
    rawInvoices = _incrementalFilter(spark.table(ctx.table("raw_sql_invoice")).filter(F.col("delete_flag") == "N"), ctx, wmFrom)
    rawLines = _incrementalFilter(spark.table(ctx.table("raw_sql_invoice_line")), ctx, wmFrom)
    sales = withAudit(transformSale(rawInvoices), ctx)
    lines = transformSaleLine(rawLines)
    header = spark.table(ctx.table("raw_sql_invoice")).filter(F.col("delete_flag") == "N").select(
        F.col("invoice_id").cast("bigint").alias("invoice_id"), regionCodeOf(F.col("bill_to_region_code")).alias("region_code"),
        F.col("invoice_date"), F.col("customer_id"), F.col("bill_to_customer_id"), F.col("delivery_city_id"), F.col("salesperson_person_id").alias("salesperson_id"),
        F.col("last_edited_when").alias("header_modified_date"), F.col("customer_tax_registration_number"), F.col("confirmed_delivery_time").alias("confirmed_delivery_utc"),
        F.col("total_dry_items"), F.col("total_chiller_items"), F.col("is_credit_note"),
        F.upper(F.trim(F.coalesce(F.col("currency_code"), F.lit("USD")))).alias("transaction_currency_code"),
        F.coalesce(F.col("conversion_rate"), F.lit(1)).cast("decimal(18,6)").alias("transaction_fx_rate"),
    )
    lines = lines.join(header, "invoice_id", "inner").withColumn("tax_regime_code", taxRegimeOf(F.col("region_code")))
    valid, rejected = splitSaleLines(lines)
    rowsRead = mergeUpsert(spark, ctx.table("stg_sale"), sales, ["sale_business_key"])
    rowsInserted = mergeUpsert(spark, ctx.table("stg_sale_line"), withAudit(valid, ctx), ["sale_line_business_key"])
    rowsRejected = appendRows(spark, ctx.table("err_rejected_invoice_line"), withAudit(rejectedInvoiceLineRows(rejected, "Staging"), ctx))
    maxEdited = rawLines.agg(F.max("last_edited_when")).first()[0] or rawInvoices.agg(F.max("last_edited_when")).first()[0]
    if maxEdited is not None:
        setWatermark(spark, ctx, "stg.SaleLine", TIMESTAMP, maxEdited.isoformat())
    return {"rowsRead": rowsRead + rowsInserted + rowsRejected, "rowsInserted": rowsRead + rowsInserted, "rowsRejected": rowsRejected}
