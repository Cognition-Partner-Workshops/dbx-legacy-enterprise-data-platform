"""08_facts FACT_Load_DailySalesSnapshot (snapshot_fact) and 09_aggregates AGG_Refresh_DailySalesSummary.

Both are delete-window-then-rebuild loads over gold_fact_sale (Integration.usp_LoadFactDailySalesSnapshot /
usp_RefreshAggregateDailySales). The legacy default windows are anchored on GETDATE() (yesterday - 3 days,
trailing 10 days); the WWI data ends in 2016, so the window is anchored on MAX(invoice_date_key) of the fact
unless snapshot_date / agg_from_date / agg_to_date are supplied. reload_full_history rebuilds every date.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import DW_CATALOG, RunContext
from sales_o2c.tables import appendRows, replaceWhere, tableExists, withAudit


def _dates(spark: SparkSession) -> DataFrame:
    return spark.table(f"{DW_CATALOG}.Dimension.Date").select(
        F.col("Date").alias("invoice_date_key"), F.col("`Fiscal Year`").alias("fiscal_year"), F.col("`Fiscal Month Number`").alias("fiscal_period"),
        F.col("`ISO Week Number`").alias("fiscal_week"),
    )


def buildDailySalesSnapshot(fact: DataFrame, dates: DataFrame) -> DataFrame:
    """Pure aggregation + snapshot measure rules (effective discount, margin, average line value)."""
    isCredit = F.col("total_including_tax") < 0
    g = (
        fact.drop("fiscal_year", "fiscal_period", "fiscal_week").join(dates, "invoice_date_key", "left")
        .groupBy(F.col("invoice_date_key").alias("snapshot_date_key"), "salesperson_key", "region_code", "fiscal_year", "fiscal_period", "fiscal_week")
        .agg(
            F.countDistinct("wwi_invoice_id").alias("invoice_count"),
            F.count("*").alias("line_count"),
            F.countDistinct("customer_key").alias("distinct_customer_count"),
            F.sum("quantity").alias("quantity_sold"),
            F.sum("gross_amount").alias("gross_sales_amount"),
            F.sum("discount_amount").alias("discount_amount"),
            F.sum("net_amount").alias("net_sales_amount"),
            F.sum("tax_amount").alias("tax_amount"),
            F.sum("total_cost_amount").alias("cost_of_sales_amount"),
            F.sum("margin_amount").alias("gross_margin_amount"),
            F.sum(F.when(isCredit, F.col("net_amount")).otherwise(F.lit(0))).alias("credit_note_amount"),
            F.sum("net_amount_reporting").alias("net_sales_amount_reporting"),
        )
    )
    gross, net, margin, lines = F.col("gross_sales_amount"), F.col("net_sales_amount"), F.col("gross_margin_amount"), F.col("line_count")
    return (
        g.withColumn("effective_discount_percent", F.when(gross == 0, F.lit(0)).otherwise(F.col("discount_amount") / gross * 100).cast("decimal(9,4)"))
        .withColumn("margin_percent", F.when(net == 0, F.lit(None)).otherwise(F.round(margin / net * 100, 2)).cast("decimal(9,4)"))
        .withColumn("average_line_value", F.when(lines == 0, F.lit(0)).otherwise(net / lines).cast("decimal(18,2)"))
        .withColumn("average_order_value", F.when(F.col("invoice_count") == 0, F.lit(0)).otherwise(net / F.col("invoice_count")).cast("decimal(18,2)"))
        .withColumn("snapshot_status_code", F.when(margin < 0, "NEGATIVE_MARGIN").when(net == 0, "ZERO_VALUE").otherwise("OK"))
    )


def _windowBounds(spark: SparkSession, ctx: RunContext, explicitTo: str, explicitFrom: str, lookbackDays: int):
    fact = spark.table(ctx.table("gold_fact_sale"))
    if ctx.reloadFullHistory:
        row = fact.agg(F.min("invoice_date_key"), F.max("invoice_date_key")).first()
        return row[0], row[1]
    toDate = spark.sql(f"SELECT CAST('{explicitTo}' AS date) d").first()[0] if explicitTo else fact.agg(F.max("invoice_date_key")).first()[0]
    if toDate is None:
        return None, None
    fromDate = spark.sql(f"SELECT CAST('{explicitFrom}' AS date) d").first()[0] if explicitFrom else spark.sql(f"SELECT date_sub(CAST('{toDate}' AS date), {lookbackDays}) d").first()[0]
    return fromDate, toDate


def runFactLoadDailySalesSnapshot(spark: SparkSession, ctx: RunContext, rebuildDays: int = 3) -> dict:
    if not tableExists(spark, ctx.table("gold_fact_sale")):
        return {"rowsRead": 0}
    fromDate, toDate = _windowBounds(spark, ctx, ctx.snapshotDate, "", rebuildDays)
    if toDate is None:
        return {"rowsRead": 0}
    fact = spark.table(ctx.table("gold_fact_sale")).filter(F.col("invoice_date_key").between(F.lit(fromDate), F.lit(toDate)))
    snapshot = buildDailySalesSnapshot(fact, _dates(spark))
    good = snapshot.filter(F.col("snapshot_status_code") == "OK").withColumn("lineage_key", F.lit(int(ctx.packageExecutionId)).cast("bigint"))
    bad = snapshot.filter(F.col("snapshot_status_code") != "OK")
    predicate = f"snapshot_date_key BETWEEN '{fromDate}' AND '{toDate}'"
    rowsInserted = replaceWhere(spark, ctx.table("gold_fact_daily_sales_snapshot"), withAudit(good, ctx), predicate)
    rowsRejected = appendRows(
        spark, ctx.table("err_rejected_snapshot_row"),
        withAudit(bad.select(
            F.col("snapshot_date_key"), F.col("salesperson_key"), F.col("region_code"), F.col("net_sales_amount"), F.col("gross_margin_amount"),
            F.col("snapshot_status_code").alias("reject_reason_code"), F.lit("Snapshot").alias("reject_stage"), F.current_timestamp().alias("rejected_at_utc"),
        ), ctx),
    )
    return {"rowsRead": fact.count(), "rowsInserted": rowsInserted, "rowsRejected": rowsRejected, "windowFrom": str(fromDate), "windowTo": str(toDate)}


# ------------------------------------------------------------------ AGG_Refresh_DailySalesSummary
SUPPRESSION_MIN_CUSTOMERS = 3


def buildDailySalesSummary(fact: DataFrame, dates: DataFrame) -> DataFrame:
    """Integration.usp_RefreshAggregateDailySales grouping + SSIS Derived Column ratios; channel defaults to DIRECT."""
    channel = F.coalesce(F.col("sales_channel_code"), F.lit("DIRECT")) if "sales_channel_code" in fact.columns else F.lit("DIRECT")
    isCredit = F.col("total_including_tax") < 0
    g = (
        fact.drop("fiscal_year", "fiscal_period", "fiscal_week").join(dates, "invoice_date_key", "left")
        .withColumn("sales_channel_code", channel)
        .groupBy(F.col("invoice_date_key").alias("sales_date"), "stock_item_key", "region_code", "sales_channel_code", "fiscal_year", "fiscal_period")
        .agg(
            F.countDistinct("wwi_invoice_id").alias("invoice_count"),
            F.count("*").alias("line_count"),
            F.countDistinct("customer_key").alias("distinct_customer_count"),
            F.sum("quantity").alias("quantity_sold_base_uom"),
            F.sum("gross_amount").alias("gross_sales_amount"),
            F.sum("discount_amount").alias("line_discount_amount"),
            F.sum("net_amount").alias("net_sales_amount"),
            F.sum("tax_amount").alias("tax_amount"),
            F.sum("total_cost_amount").alias("cost_of_sales_amount"),
            F.sum("margin_amount").alias("gross_margin_amount"),
            F.sum(F.when(isCredit, F.col("net_amount")).otherwise(F.lit(0))).alias("returns_amount"),
            F.sum("net_amount_reporting").alias("net_sales_amount_reporting"),
            F.count("*").alias("source_row_count"),
        )
    )
    net, margin, lines = F.col("net_sales_amount"), F.col("gross_margin_amount"), F.col("line_count")
    return (
        g.withColumn("margin_percent", F.when(net == 0, F.lit(None)).otherwise(F.round(100.0 * margin / net, 2)).cast("decimal(9,4)"))
        .withColumn("average_line_value", F.when(lines == 0, F.lit(0)).otherwise(net / lines).cast("decimal(18,2)"))
        .withColumn("is_suppressed", F.col("distinct_customer_count") < SUPPRESSION_MIN_CUSTOMERS)
        .withColumn("is_loss_making", margin < 0)
    )


def runAggRefreshDailySalesSummary(spark: SparkSession, ctx: RunContext, lookbackDays: int = 10) -> dict:
    if not tableExists(spark, ctx.table("gold_fact_sale")):
        return {"rowsRead": 0}
    fromDate, toDate = _windowBounds(spark, ctx, ctx.aggToDate, ctx.aggFromDate, lookbackDays)
    if toDate is None:
        return {"rowsRead": 0}
    fact = spark.table(ctx.table("gold_fact_sale")).filter(F.col("invoice_date_key").between(F.lit(fromDate), F.lit(toDate)))
    summary = buildDailySalesSummary(fact, _dates(spark)).withColumn("refresh_batch_id", F.lit(int(ctx.batchId)).cast("bigint")).withColumn("refreshed_datetime", F.current_timestamp())
    kept = summary.filter(~F.col("is_suppressed"))
    predicate = f"sales_date BETWEEN '{fromDate}' AND '{toDate}'"
    rowsInserted = replaceWhere(spark, ctx.table("gold_agg_daily_sales_summary"), withAudit(kept, ctx), predicate)
    suppressed = appendRows(
        spark, ctx.table("err_rejected_summary_cell"),
        withAudit(summary.filter(F.col("is_suppressed")).select("sales_date", "stock_item_key", "region_code", "sales_channel_code", "distinct_customer_count", "net_sales_amount", F.lit("SUPPRESSED_SMALL_CELL").alias("reject_reason_code"), F.current_timestamp().alias("rejected_at_utc")), ctx),
    )
    lossDays = appendRows(
        spark, ctx.table("etl_loss_making_day"),
        withAudit(kept.filter(F.col("is_loss_making")).select("sales_date", "stock_item_key", "region_code", "sales_channel_code", "net_sales_amount", "gross_margin_amount", "margin_percent", F.current_timestamp().alias("logged_at_utc")), ctx),
    )
    return {"rowsRead": fact.count(), "rowsInserted": rowsInserted, "rowsRejected": suppressed, "lossMakingDays": lossDays, "windowFrom": str(fromDate), "windowTo": str(toDate)}
