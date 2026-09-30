"""05_data_quality: DQ_OrderLine_Screen and DQ_InvoiceLine_Screen (quality_screen).

Each screen evaluates the staged rows of the current batch, stamps dq_status_code on the stg table, writes
failures to err_rejected_* (RejectStage = 'Quality'), and records one etl_data_quality_result row per rule.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import DW_CATALOG, RunContext
from sales_o2c.staging import rejectedInvoiceLineRows, rejectedOrderLineRows
from sales_o2c.tables import appendRows, withAudit


# ------------------------------------------------------------------ DQ_OrderLine_Screen rules
def screenOrderLines(lines: DataFrame) -> DataFrame:
    qty, price, ext = F.col("ordered_quantity"), F.col("unit_price_amount"), F.col("extended_amount")
    return (
        lines.withColumn("quantity_out_of_range_flag", F.when((qty <= 0) | (qty > 10000), "Y").otherwise("N"))
        .withColumn("price_outlier_flag", F.when(price > 250000, "Y").otherwise("N"))
        .withColumn("extension_mismatch_amount", F.abs(ext - qty * price).cast("decimal(18,2)"))
        .withColumn(
            "dq_pass",
            (F.col("quantity_out_of_range_flag") == "N") & (F.col("price_outlier_flag") == "N") & (F.col("extension_mismatch_amount") <= 0.01),
        )
        .withColumn(
            "reject_reason_code",
            F.when(F.col("dq_pass"), F.lit(None).cast("string"))
            .when(F.col("quantity_out_of_range_flag") == "Y", "DQ_OL_QTY_RANGE")
            .when(F.col("price_outlier_flag") == "Y", "DQ_OL_PRICE_OUTLIER")
            .otherwise("DQ_OL_EXTENSION"),
        )
    )


# ------------------------------------------------------------------ DQ_InvoiceLine_Screen rules
def screenSaleLines(lines: DataFrame) -> DataFrame:
    region, regime = F.col("region_code"), F.col("tax_regime_code")
    net = F.col("net_amount")
    minor = F.coalesce(F.col("minor_unit_digits"), F.lit(2))
    return (
        lines.withColumn(
            "regime_mismatch_flag",
            F.when(
                ((region == "EU") & (regime != "VAT")) | ((region == "APAC") & (regime != "GST")) | ((region == "NA") & (regime != "SUT")), "Y"
            ).otherwise("N"),
        )
        .withColumn("tax_mismatch_flag", F.when(F.col("tax_variance_amount") > 0.02, "Y").otherwise("N"))
        .withColumn("minor_unit_breach_flag", F.when((minor == 0) & (net != F.round(net, 0)), "Y").otherwise("N"))
        .withColumn(
            "dq_pass",
            (F.col("regime_mismatch_flag") == "N") & (F.col("tax_mismatch_flag") == "N") & (F.col("minor_unit_breach_flag") == "N"),
        )
        .withColumn(
            "reject_reason_code",
            F.when(F.col("dq_pass"), F.lit(None).cast("string"))
            .when(F.col("regime_mismatch_flag") == "Y", "DQ_IL_REGIME")
            .when(F.col("tax_mismatch_flag") == "Y", "DQ_IL_TAX_VARIANCE")
            .otherwise("DQ_IL_MINOR_UNIT"),
        )
    )


def _dqResultRows(spark: SparkSession, ctx: RunContext, objectName: str, screened: DataFrame, flagCols) -> DataFrame:
    total = screened.count()
    rows = []
    for flag in flagCols:
        failed = screened.filter(F.col(flag) == "Y").count()
        rows.append((objectName, flag.upper(), float(failed), 0.0, int(total), "PASS" if failed == 0 else "WARN"))
    return withAudit(
        spark.createDataFrame(rows, "object_name string, rule_code string, measured_value double, threshold_value double, rows_evaluated bigint, result_status string").withColumn("evaluated_at_utc", F.current_timestamp()),
        ctx,
    )


def _stampStatus(spark: SparkSession, table: str, keyCol: str, screened: DataFrame) -> None:
    screened.select(keyCol, F.when(F.col("dq_pass"), "PASS").otherwise("FAIL").alias("new_status")).createOrReplaceTempView("dq_status")
    spark.sql(f"MERGE INTO {table} t USING dq_status s ON t.`{keyCol}` = s.`{keyCol}` WHEN MATCHED THEN UPDATE SET t.dq_status_code = s.new_status")


def runDqOrderLineScreen(spark: SparkSession, ctx: RunContext) -> dict:
    table = ctx.table("stg_order_line")
    if "dq_status_code" not in spark.table(table).columns:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS (dq_status_code string)")
    lines = spark.table(table).filter(F.col("batch_id") == ctx.batchId)
    screened = screenOrderLines(lines)
    _stampStatus(spark, table, "order_line_business_key", screened)
    rejected = screened.filter(~F.col("dq_pass"))
    rowsRejected = appendRows(spark, ctx.table("err_rejected_order_line"), withAudit(rejectedOrderLineRows(rejected, "Quality"), ctx))
    # Customer lookup screen (stg.Order -> Dimension.Customer): unmatched customers -> err_rejected_lookup_failure
    customers = spark.table(f"{DW_CATALOG}.Dimension.Customer").select(F.col("`WWI Customer ID`").alias("customer_id")).distinct()
    orders = spark.table(ctx.table("stg_order")).filter(F.col("batch_id") == ctx.batchId)
    unmatched = orders.join(customers, "customer_id", "left_anti").select(
        F.lit("stg.Order").alias("source_object_name"), F.col("order_business_key").alias("source_business_key"),
        F.lit("Customer").alias("lookup_name"), F.lit("WWI Customer ID").alias("lookup_column_name"),
        F.col("customer_id").cast("string").alias("lookup_value"), F.lit("DQ_LOOKUP_CUSTOMER").alias("reject_reason_code"),
        F.lit("Quality").alias("reject_stage"), F.lit(True).alias("queued_for_late_arrival"), F.current_timestamp().alias("rejected_at_utc"),
    )
    lookupFailures = appendRows(spark, ctx.table("err_rejected_lookup_failure"), withAudit(unmatched, ctx))
    appendRows(spark, ctx.table("etl_data_quality_result"), _dqResultRows(spark, ctx, "stg.OrderLine", screened, ["quantity_out_of_range_flag", "price_outlier_flag"]))
    return {"rowsRead": screened.count(), "rowsRejected": rowsRejected + lookupFailures}


def runDqInvoiceLineScreen(spark: SparkSession, ctx: RunContext) -> dict:
    table = ctx.table("stg_sale_line")
    if "dq_status_code" not in spark.table(table).columns:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS (dq_status_code string)")
    lines = spark.table(table).filter(F.col("batch_id") == ctx.batchId)
    # Minor-unit digits: legacy ref.Currency is empty on the host, Dimension.Currency carries the same attribute.
    currency = (
        spark.table(f"{DW_CATALOG}.Dimension.Currency")
        .select(F.upper(F.col("`Currency Code`")).alias("transaction_currency_code"), F.col("`Minor Unit Digits`").cast("int").alias("minor_unit_digits"))
        .dropDuplicates(["transaction_currency_code"])
    )
    screened = screenSaleLines(lines.join(currency, "transaction_currency_code", "left"))
    _stampStatus(spark, table, "sale_line_business_key", screened)
    rejected = screened.filter(~F.col("dq_pass"))
    rowsRejected = appendRows(spark, ctx.table("err_rejected_invoice_line"), withAudit(rejectedInvoiceLineRows(rejected, "Quality"), ctx))
    appendRows(spark, ctx.table("etl_data_quality_result"), _dqResultRows(spark, ctx, "stg.SaleLine", screened, ["regime_mismatch_flag", "tax_mismatch_flag", "minor_unit_breach_flag"]))
    return {"rowsRead": screened.count(), "rowsRejected": rowsRejected}
