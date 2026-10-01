"""``fact_return`` - return-line accumulating snapshot (``DeltaTable.merge`` on
``return_line_business_key``); milestones ``return_date_key`` -> ``received_date_key`` ->
``credit_issued_date_key`` are filled in as events arrive, lags recomputed each merge.

Replaces Integration.usp_LoadFactReturn and SSIS FACT_Load_Return.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.gold import rules_adapter
from sales_lakehouse.gold.fact_support import (
    UNKNOWN_KEY,
    daysBetween,
    lookupScd2Key,
    mergeAccumulating,
    money,
    optionalColumn,
    pickColumn,
    readOptional,
    surrogateKey,
    withLoadMetadata,
)
from sales_lakehouse.gold.inputs import readOrEmpty

TABLE = "fact_return"
SOURCE_TABLE = "return"
# silver.return contract (no silver producer ships it yet; an absent table loads as empty)
SILVER_RETURN_SCHEMA = (
    "return_line_business_key string, rma_number string, sale_line_business_key string, customer_business_key string, "
    "stock_item_business_key string, return_reason_code string, return_reason_group_code string, "
    "returned_quantity decimal(18,4), restocked_quantity decimal(18,4), scrapped_quantity decimal(18,4), "
    "inspection_result_code string, restocking_fee_amount decimal(19,4), refund_amount decimal(19,4), "
    "transaction_currency_code string, returned_date date, processed_date date, region_code string, "
    "batch_id bigint, loaded_at_utc timestamp"
)
KEY_COLS = ["return_line_business_key"]
MILESTONES = ["return_date_key", "received_date_key", "credit_issued_date_key"]
COST_MISSING = "COST_MISSING"
DEFAULT_APAC_WINDOW_DAYS = 7


def finalizeReturn(df: DataFrame) -> DataFrame:
    """Lag / window columns derived from the (coalesced) milestones - recomputed on every merge."""
    region = F.col("region_code")
    fromDelivery = daysBetween("_window_start", "return_date_key")
    fromInvoice = daysBetween("original_invoice_date_key", "return_date_key")
    windowDays = (
        F.when(region == "EU", F.lit(14))
        .when(region == "APAC", F.coalesce(F.col("_apac_window_days"), F.lit(DEFAULT_APAC_WINDOW_DAYS)))
        .otherwise(F.lit(30))
        .cast("int")
    )
    # LEGACY QUIRK: statutory windows differ by region - EU 14 days from delivery, APAC per return
    # reason reference (default 7) from delivery, NA 30 days commercial policy from INVOICE date.
    within = (
        F.when((region == "EU") & (fromDelivery <= 14), F.lit(True))
        .when((region == "APAC") & (fromDelivery <= windowDays), F.lit(True))
        .when((region == "NA") & (fromInvoice <= 30), F.lit(True))
        .otherwise(F.lit(False))
    )
    return (
        df.withColumn("days_since_invoice", fromInvoice)
        .withColumn("return_to_receipt_lag_days", daysBetween("return_date_key", "received_date_key"))
        .withColumn("return_to_credit_lag_days", daysBetween("return_date_key", "credit_issued_date_key"))
        .withColumn("receipt_to_credit_lag_days", daysBetween("received_date_key", "credit_issued_date_key"))
        .withColumn("statutory_window_days", windowDays)
        .withColumn("within_statutory_window_flag", within)
        .withColumn("credit_issued_flag", F.col("credit_issued_date_key").isNotNull())
        .withColumn("received_flag", F.col("received_date_key").isNotNull())
    )


def buildFactReturn(
    spark: SparkSession,
    cfg: PipelineConfig,
    returns: DataFrame,
    factSale: DataFrame | None,
    creditNotes: DataFrame | None,
    dimCustomer: DataFrame | None,
    returnReasons: DataFrame | None = None,
) -> DataFrame:
    df = returns.select(
        F.col("return_line_business_key"),
        F.col("rma_number"),
        pickColumn(returns, ["sale_line_business_key", "original_sale_line_business_key"], "string").alias(
            "original_sale_line_business_key"
        ),
        F.col("customer_business_key"),
        F.col("stock_item_business_key"),
        pickColumn(returns, ["return_reason_code"], "string").alias("return_reason_code"),
        pickColumn(returns, ["return_reason_group_code"], "string").alias("return_reason_group_code"),
        F.col("returned_quantity").cast("decimal(18,4)").alias("_qty"),
        pickColumn(returns, ["restocked_quantity"], "decimal(18,4)").alias("_restocked"),
        pickColumn(returns, ["scrapped_quantity"], "decimal(18,4)").alias("_scrapped"),
        pickColumn(returns, ["inspection_result_code", "disposition_code"], "string").alias("disposition_code"),
        pickColumn(returns, ["restocking_fee_amount"], "decimal(19,4)").alias("_restock_fee"),
        pickColumn(returns, ["refund_amount"], "decimal(19,4)").alias("_refund"),
        F.col("transaction_currency_code"),
        F.col("returned_date").cast("date").alias("return_date_key"),
        pickColumn(returns, ["processed_date", "received_date"], "date").alias("received_date_key"),
        F.upper(F.col("region_code")).alias("region_code"),
        F.col("batch_id").alias("lineage_key"),
        F.col("loaded_at_utc").alias("source_loaded_at_utc"),
    )
    if factSale is not None:
        orig = factSale.select(
            F.col("sale_line_business_key").alias("_o_key"),
            F.col("invoice_date_key").alias("original_invoice_date_key"),
            F.col("delivery_date_key").alias("_delivery_date"),
            F.col("invoice_number").alias("original_invoice_number"),
            F.col("invoice_line_number").alias("original_invoice_line_number"),
            F.col("unit_price").alias("_orig_unit_price"),
            (F.col("cost_of_sale_amount") / F.col("quantity")).cast("decimal(19,4)").alias("_orig_unit_cost"),
            F.col("tax_rate").alias("_orig_tax_rate"),
            F.col("salesperson_key"),
            F.col("sales_territory_key"),
            F.col("stock_item_key"),
            F.col("transaction_currency_code").alias("_orig_ccy"),
        ).dropDuplicates(["_o_key"])
        df = df.join(orig, df["original_sale_line_business_key"] == orig["_o_key"], "left").drop("_o_key")
    else:
        df = (
            df.withColumn("original_invoice_date_key", F.lit(None).cast("date"))
            .withColumn("_delivery_date", F.lit(None).cast("date"))
            .withColumn("original_invoice_number", F.lit(None).cast("string"))
            .withColumn("original_invoice_line_number", F.lit(None).cast("int"))
            .withColumn("_orig_unit_price", F.lit(None).cast("decimal(19,4)"))
            .withColumn("_orig_unit_cost", F.lit(None).cast("decimal(19,4)"))
            .withColumn("_orig_tax_rate", F.lit(None).cast("decimal(18,3)"))
            .withColumn("salesperson_key", F.lit(UNKNOWN_KEY).cast("bigint"))
            .withColumn("sales_territory_key", F.lit(UNKNOWN_KEY).cast("bigint"))
            .withColumn("stock_item_key", F.lit(UNKNOWN_KEY).cast("bigint"))
            .withColumn("_orig_ccy", F.lit(None).cast("string"))
        )
    if creditNotes is not None:
        cn = (
            creditNotes.filter(F.col("rma_number").isNotNull())
            .groupBy("rma_number")
            .agg(
                F.min(F.col("credit_note_date").cast("date")).alias("credit_issued_date_key"),
                F.min(pickColumn(creditNotes, ["credit_note_number", "credit_note_business_key"], "string")).alias(
                    "credit_note_number"
                ),
            )
        )
        df = df.join(cn, "rma_number", "left")
    else:
        df = df.withColumn("credit_issued_date_key", F.lit(None).cast("date")).withColumn(
            "credit_note_number", F.lit(None).cast("string")
        )
    if returnReasons is not None and "statutory_window_days" in returnReasons.columns:
        rr = returnReasons.select(
            F.col("return_reason_code").alias("_rr_code"),
            F.col("statutory_window_days").cast("int").alias("_apac_window_days"),
            F.coalesce(optionalColumn(returnReasons, "faulty_goods_flag", "boolean"), F.lit(False)).alias(
                "faulty_goods_flag"
            ),
        ).dropDuplicates(["_rr_code"])
        df = df.join(rr, df["return_reason_code"] == rr["_rr_code"], "left").drop("_rr_code")
    else:
        df = df.withColumn("_apac_window_days", F.lit(None).cast("int")).withColumn(
            "faulty_goods_flag", F.upper(F.coalesce(F.col("return_reason_group_code"), F.lit(""))) == F.lit("FAULTY")
        )

    df = df.withColumn("transaction_currency_code", F.coalesce(F.col("transaction_currency_code"), F.col("_orig_ccy")))
    df = df.withColumn("_window_start", F.coalesce(F.col("_delivery_date"), F.col("original_invoice_date_key")))
    absQty = F.abs(F.col("_qty"))
    gross = F.coalesce(money(absQty * F.col("_orig_unit_price")), F.abs(F.col("_refund")))
    # LEGACY QUIRK: returns are stored with NEGATIVE quantities and amounts so they sum with sales.
    df = (
        df.withColumn("quantity_returned", (-absQty).cast("decimal(18,4)"))
        .withColumn("quantity_restocked", (-F.abs(F.coalesce(F.col("_restocked"), F.lit(0)))).cast("decimal(18,4)"))
        .withColumn("quantity_scrapped", (-F.abs(F.coalesce(F.col("_scrapped"), F.lit(0)))).cast("decimal(18,4)"))
        .withColumn("gross_return_amount", money(-gross))
        .withColumn("restocking_fee_amount", money(F.abs(F.coalesce(F.col("_restock_fee"), F.lit(0)))))
        .withColumn("tax_amount", money(-gross * F.coalesce(F.col("_orig_tax_rate"), F.lit(0)) / 100))
        .withColumn(
            "net_credit_amount",
            money(
                F.coalesce(
                    -F.abs(F.col("_refund")),
                    F.col("gross_return_amount") + F.col("tax_amount") + F.col("restocking_fee_amount"),
                )
            ),
        )
        # LEGACY QUIRK: margin reversal uses the cost on the ORIGINAL sale line, not the current item
        # cost; when the original cost cannot be found the row still loads with a null cost.
        .withColumn("cost_of_returned_goods", money(-absQty * F.col("_orig_unit_cost")))
        .withColumn("margin_reversed", money(absQty * (F.col("_orig_unit_cost") - F.col("_orig_unit_price"))))
        .withColumn(
            "cost_status_code", F.when(F.col("_orig_unit_cost").isNull(), F.lit(COST_MISSING)).otherwise(F.lit("OK"))
        )
    )
    df = rules_adapter.applyFx(
        df,
        spark,
        cfg,
        amountCols=["net_credit_amount"],
        currencyCol="transaction_currency_code",
        dateCol="return_date_key",
        regionCol="region_code",
    )
    df = lookupScd2Key(
        df,
        dimCustomer,
        "customer_business_key",
        "return_date_key",
        "customer_key",
        "customer_business_key",
        "customer_key",
    )
    df = df.withColumn("inferred_member_flag", F.col("customer_key") == F.lit(UNKNOWN_KEY))
    out = df.select(
        surrogateKey("return_line_business_key").alias("return_key"),
        F.col("return_line_business_key"),
        F.col("original_sale_line_business_key"),
        F.col("return_date_key"),
        F.col("received_date_key"),
        F.col("original_invoice_date_key"),
        F.col("credit_issued_date_key"),
        F.col("_window_start"),
        F.col("_apac_window_days"),
        F.col("customer_key"),
        F.col("stock_item_key"),
        F.lit(UNKNOWN_KEY).cast("bigint").alias("return_reason_key"),
        F.lit(UNKNOWN_KEY).cast("bigint").alias("warehouse_site_key"),
        F.col("sales_territory_key"),
        F.col("salesperson_key"),
        F.lit(UNKNOWN_KEY).cast("bigint").alias("currency_key"),
        F.col("region_code"),
        F.col("customer_business_key"),
        F.col("stock_item_business_key"),
        F.col("rma_number"),
        F.lit(None).cast("int").alias("rma_line_number"),
        F.col("original_invoice_number"),
        F.col("original_invoice_line_number"),
        F.col("credit_note_number"),
        F.col("return_reason_code"),
        F.col("return_reason_group_code"),
        F.col("quantity_returned"),
        F.col("quantity_restocked"),
        F.col("quantity_scrapped"),
        F.lit(None).cast("string").alias("source_uom_code"),
        F.col("transaction_currency_code"),
        F.col("gross_return_amount"),
        F.col("restocking_fee_amount"),
        F.col("tax_amount"),
        F.col("net_credit_amount"),
        F.col("fx_rate_to_reporting"),
        F.col("fx_rate_source_code"),
        F.col("net_credit_amount_reporting"),
        F.col("cost_of_returned_goods"),
        F.col("margin_reversed"),
        F.col("cost_status_code"),
        F.col("faulty_goods_flag"),
        F.col("disposition_code"),
        F.col("inferred_member_flag"),
        F.col("lineage_key").cast("bigint"),
        F.col("source_loaded_at_utc"),
    )
    return withLoadMetadata(out, cfg)


def writeFactReturn(spark: SparkSession, cfg: PipelineConfig, df: DataFrame) -> None:
    def finalize(merged: DataFrame) -> DataFrame:
        return (
            finalizeReturn(merged)
            .drop("_window_start", "_apac_window_days")
            .withColumn("last_milestone_update", F.current_timestamp())
        )

    # _window_start / _apac_window_days are inputs to the lag recompute, so they must survive
    # into the table for the next merge; keep them under stable names.
    df = df.withColumnRenamed("_window_start", "window_start_date_key").withColumnRenamed(
        "_apac_window_days", "apac_statutory_window_days"
    )

    def finalizeNamed(merged: DataFrame) -> DataFrame:
        m = merged.withColumn("_window_start", F.col("window_start_date_key")).withColumn(
            "_apac_window_days", F.col("apac_statutory_window_days")
        )
        return finalize(m)

    mergeAccumulating(spark, df, cfg.fqn("gold", TABLE), KEY_COLS, MILESTONES, finalizeNamed)


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    df = buildFactReturn(
        spark,
        cfg,
        returns=readOrEmpty(spark, cfg, "silver", SOURCE_TABLE, SILVER_RETURN_SCHEMA),
        factSale=readOptional(spark, cfg, "gold", "fact_sale"),
        creditNotes=readOptional(spark, cfg, "silver", "credit_note"),
        dimCustomer=readOptional(spark, cfg, "silver", "dim_customer"),
        returnReasons=readOptional(spark, cfg, "silver", "ref_return_reason"),
    )
    writeFactReturn(spark, cfg, df)
