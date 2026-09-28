"""``fact_sale`` - invoice-line grain, incremental MERGE on ``sale_line_business_key``.

Replaces Integration.usp_LoadFactSale + usp_DeduplicateFactSale and the three
SSIS packages FACT_NA_Load_Sale / FACT_EU_Load_Sale / FACT_APAC_Load_Sale
with ONE region-parameterised loader (``regionCodes``); the regional tax /
FX / fiscal differences live in the rule library (see ``rules_adapter``).
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.quality import quarantine
from sales_lakehouse.gold import rules_adapter
from sales_lakehouse.gold.fact_support import (
    UNKNOWN_KEY,
    dedupeLatest,
    lookupCurrentKey,
    lookupScd2Key,
    mergeOnKeys,
    money,
    optionalColumn,
    readOptional,
    surrogateKey,
    withLoadMetadata,
)

TABLE = "fact_sale"
KEY_COLS = ["sale_line_business_key"]
SOURCE_TABLE = "silver.sale_line"


def _sourceLines(lines: DataFrame, heads: DataFrame, orders: DataFrame | None) -> DataFrame:
    h = heads.select(
        F.col("sale_business_key").alias("_h_sale_business_key"),
        F.col("source_invoice_id").cast("int").alias("wwi_invoice_id"),
        F.col("order_business_key").alias("order_number"),
        F.col("customer_business_key"),
        F.coalesce(F.col("bill_to_customer_business_key"), F.col("customer_business_key")).alias(
            "bill_to_customer_business_key"
        ),
        F.col("salesperson_business_key"),
        F.col("invoice_date").alias("_h_invoice_date"),
        optionalColumn(heads, "confirmed_delivery_utc", "timestamp").cast("date").alias("delivery_date_key"),
        optionalColumn(heads, "is_credit_note", "boolean").alias("_is_credit_note"),
        optionalColumn(heads, "total_dry_items", "int").alias("total_dry_items"),
        optionalColumn(heads, "total_chiller_items", "int").alias("total_chiller_items"),
        F.col("transaction_currency_code").alias("_h_currency"),
        F.col("region_code").alias("_h_region"),
    ).dropDuplicates(["_h_sale_business_key"])
    df = lines.join(h, lines["sale_business_key"] == h["_h_sale_business_key"], "left").drop("_h_sale_business_key")
    df = (
        df.withColumn("invoice_date", F.coalesce(F.col("invoice_date"), F.col("_h_invoice_date")).cast("date"))
        .withColumn("region_code", F.upper(F.coalesce(F.col("region_code"), F.col("_h_region"))))
        .withColumn("transaction_currency_code", F.coalesce(F.col("transaction_currency_code"), F.col("_h_currency")))
        .drop("_h_invoice_date", "_h_region", "_h_currency")
    )
    if orders is not None:
        o = orders.select(
            F.col("order_business_key").alias("_o_key"),
            optionalColumn(orders, "sales_channel_code", "string").alias("sales_channel_code"),
            optionalColumn(orders, "sales_territory_code", "string").alias("sales_territory_code"),
        ).dropDuplicates(["_o_key"])
        df = df.join(o, df["order_number"] == o["_o_key"], "left").drop("_o_key")
    else:
        df = df.withColumn("sales_channel_code", F.lit(None).cast("string")).withColumn(
            "sales_territory_code", F.lit(None).cast("string")
        )
    return df


def buildFactSale(
    spark: SparkSession,
    cfg: PipelineConfig,
    lines: DataFrame,
    heads: DataFrame,
    orders: DataFrame | None,
    dimCustomer: DataFrame | None,
    dimSalesperson: DataFrame | None,
    dimSalesChannel: DataFrame | None,
    dimSalesTerritory: DataFrame | None,
    dimStockItem: DataFrame | None,
    regionCodes: Sequence[str] | None = None,
) -> DataFrame:
    df = _sourceLines(lines, heads, orders)
    if regionCodes:
        df = df.filter(F.col("region_code").isin([r.upper() for r in regionCodes]))

    # SSIS FACT_<region>_Load_Sale reject paths: missing customer / stock item / zero quantity.
    df = quarantine(
        spark,
        cfg,
        df,
        "FACT_SALE_NO_CUSTOMER",
        SOURCE_TABLE,
        F.col("customer_business_key").isNull(),
        "Sale line has no customer business key",
    )
    df = quarantine(
        spark,
        cfg,
        df,
        "FACT_SALE_NO_STOCK_ITEM",
        SOURCE_TABLE,
        F.col("stock_item_business_key").isNull(),
        "Sale line has no stock item business key",
    )
    df = quarantine(
        spark,
        cfg,
        df,
        "FACT_SALE_ZERO_QTY",
        SOURCE_TABLE,
        F.col("quantity").isNull() | (F.col("quantity") == 0),
        "Sale line quantity is zero",
    )

    # FACT_Dedup_Sale: survivor = highest lineage (latest load / batch); losers -> quarantine.
    df = dedupeLatest(df, KEY_COLS, [F.col("loaded_at_utc").desc_nulls_last(), F.col("batch_id").desc_nulls_last()])
    df = quarantine(
        spark,
        cfg,
        df,
        "FACT_SALE_DUP",
        SOURCE_TABLE,
        F.col("_rn") > 1,
        "Duplicate sale_line_business_key - superseded by a later-loaded row",
    ).drop("_rn")

    df = (
        df.withColumnRenamed("batch_id", "source_batch_id")
        .withColumnRenamed("loaded_at_utc", "source_loaded_at_utc")
        .withColumn("quantity", F.col("quantity").cast("decimal(18,4)"))
        .withColumn("unit_price", F.col("unit_price_amount").cast("decimal(19,4)"))
        .withColumn("net_amount", money(F.col("net_line_amount")))
        .withColumn("gross_amount", money(F.col("quantity") * F.col("unit_price")))
        .withColumn("line_discount_amount", money(F.col("gross_amount") - F.col("net_amount")))
    )
    # LEGACY QUIRK: a credit-note invoice is loaded as a reversing sale row (Correction Type 'REV'),
    # exactly as usp_LoadFactSale does; nothing is netted at load time.
    df = df.withColumn(
        "correction_type_code", F.when(F.col("_is_credit_note") == F.lit(True), F.lit("REV")).otherwise(F.lit("ORIG"))
    ).drop("_is_credit_note")

    df = rules_adapter.applyTax(df, regionCol="region_code")
    df = rules_adapter.applyFx(
        df,
        spark,
        cfg,
        amountCols=["total_excluding_tax", "tax_amount", "net_amount", "gross_amount"],
        currencyCol="transaction_currency_code",
        dateCol="invoice_date",
        regionCol="region_code",
    )
    df = rules_adapter.resolveFiscalPeriod(df, spark, cfg, dateCol="invoice_date", regionCol="region_code")

    # Profit / cost: silver carries the staged line profit (legacy [Profit]); cost of sale is net - profit.
    df = df.withColumn("profit", optionalColumn(df, "line_profit_amount", "decimal(19,4)"))
    df = df.withColumn("cost_of_sale_amount", money(F.col("net_amount") - F.col("profit")))
    df = df.withColumn("gross_margin_amount", F.col("profit"))

    df = lookupScd2Key(
        df,
        dimCustomer,
        "customer_business_key",
        "invoice_date",
        "customer_key",
        "customer_business_key",
        "customer_key",
    )
    df = lookupScd2Key(
        df,
        dimCustomer,
        "bill_to_customer_business_key",
        "invoice_date",
        "bill_to_customer_key",
        "customer_business_key",
        "customer_key",
    )
    df = lookupCurrentKey(
        df,
        dimSalesperson,
        "salesperson_business_key",
        "salesperson_key",
        "salesperson_business_key",
        "salesperson_key",
        "is_current",
    )
    df = lookupCurrentKey(
        df,
        dimSalesChannel,
        "sales_channel_code",
        "sales_channel_key",
        "sales_channel_code",
        "sales_channel_key",
        "is_current",
    )
    df = lookupCurrentKey(
        df,
        dimSalesTerritory,
        "sales_territory_code",
        "sales_territory_key",
        "sales_territory_code",
        "sales_territory_key",
        "is_current",
    )
    df = lookupCurrentKey(
        df,
        dimStockItem,
        "stock_item_business_key",
        "stock_item_key",
        "stock_item_business_key",
        "stock_item_key",
        "is_current",
    )
    # LEGACY QUIRK: late-arriving customer -> inferred member (key -1, flag set), row is NOT rejected.
    df = df.withColumn("inferred_member_flag", F.col("customer_key") == F.lit(UNKNOWN_KEY))

    unknown = F.lit(UNKNOWN_KEY).cast("bigint")
    out = df.select(
        surrogateKey("sale_line_business_key").alias("sale_key"),
        F.col("sale_line_business_key"),
        F.col("sale_business_key"),
        unknown.alias("city_key"),
        F.col("customer_key"),
        F.col("bill_to_customer_key"),
        F.col("stock_item_key"),
        F.col("invoice_date").alias("invoice_date_key"),
        F.col("delivery_date_key"),
        F.col("salesperson_key"),
        F.col("sales_channel_key"),
        F.col("sales_territory_key"),
        unknown.alias("promotion_key"),
        unknown.alias("customer_segment_key"),
        unknown.alias("currency_key"),
        F.col("wwi_invoice_id"),
        F.col("sale_business_key").alias("invoice_number"),
        F.col("line_number").cast("int").alias("invoice_line_number"),
        F.col("order_number"),
        F.col("customer_business_key"),
        F.col("stock_item_business_key"),
        optionalColumn(df, "product_business_key", "string").alias("product_business_key"),
        F.col("salesperson_business_key"),
        F.col("promotion_business_key"),
        F.col("line_description").alias("description"),
        F.lit(None).cast("string").alias("package"),
        F.col("quantity"),
        optionalColumn(df, "quantity_base_uom", "decimal(18,4)").alias("quantity_base_uom"),
        optionalColumn(df, "uom_code", "string").alias("source_uom_code"),
        F.col("unit_price"),
        F.col("tax_rate"),
        F.col("total_excluding_tax").cast("decimal(19,4)"),
        F.col("tax_amount").cast("decimal(19,4)"),
        F.col("profit"),
        F.col("total_including_tax").cast("decimal(19,4)"),
        F.col("total_dry_items"),
        F.col("total_chiller_items"),
        F.col("transaction_currency_code"),
        F.col("fx_rate_to_reporting"),
        F.col("fx_rate_effective_date"),
        F.col("fx_rate_source_code"),
        F.col("total_excluding_tax_reporting"),
        F.col("tax_amount_reporting"),
        F.col("net_amount_reporting"),
        F.col("region_code"),
        F.col("fiscal_calendar_code"),
        F.col("fiscal_year"),
        F.col("fiscal_period"),
        F.col("fiscal_period_key"),
        F.col("tax_regime_code"),
        F.col("tax_treatment_code"),
        F.col("vat_rate"),
        F.col("vat_reverse_charge_flag"),
        F.lit(None).cast("string").alias("customer_tax_registration"),
        F.col("gst_rate"),
        F.col("gst_free_flag"),
        F.col("line_discount_amount"),
        F.lit(None).cast("decimal(19,4)").alias("freight_amount"),
        F.col("cost_of_sale_amount"),
        F.col("gross_amount"),
        F.col("net_amount"),
        F.col("gross_margin_amount"),
        F.col("correction_type_code"),
        F.lit(None).cast("bigint").alias("corrected_sale_key"),
        F.col("inferred_member_flag"),
        F.col("source_batch_id").cast("bigint").alias("lineage_key"),
        F.col("source_loaded_at_utc"),
    )
    return withLoadMetadata(out, cfg)


def run(spark: SparkSession, cfg: PipelineConfig, regionCodes: Sequence[str] | None = None) -> None:
    df = buildFactSale(
        spark,
        cfg,
        lines=spark.table(cfg.fqn("silver", "sale_line")),
        heads=spark.table(cfg.fqn("silver", "sale")),
        orders=readOptional(spark, cfg, "silver", "order"),
        dimCustomer=readOptional(spark, cfg, "silver", "dim_customer"),
        dimSalesperson=readOptional(spark, cfg, "silver", "dim_salesperson"),
        dimSalesChannel=readOptional(spark, cfg, "silver", "dim_sales_channel"),
        dimSalesTerritory=readOptional(spark, cfg, "silver", "dim_sales_territory"),
        dimStockItem=readOptional(spark, cfg, "silver", "dim_stock_item"),
        regionCodes=regionCodes,
    )
    mergeOnKeys(spark, df, cfg.fqn("gold", TABLE), KEY_COLS)
