"""``fact_sales_margin`` - invoice-line grain, FULL REBUILD every run.

Replaces the [Fact].[Sales Margin] rebuild (Integration.usp_RefreshAggregateMarginAnalysis
delete-by-window) with a whole-table ``overwriteTable``.

# LEGACY QUIRK: this fact is rebuilt, not loaded incrementally. Margin depends on cost,
# cost comes from the ERP (Oracle) and the two arrive on different schedules
# (docs/domain-model/business-domains.md "Sales and order to cash"); an incremental load
# would freeze a COST_MISSING row forever. Rebuilding lets a late cost simply appear.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import overwriteTable
from sales_lakehouse.gold import rules_adapter
from sales_lakehouse.gold.fact_support import (
    money,
    optionalColumn,
    readOptional,
    surrogateKey,
    withLoadMetadata,
)

TABLE = "fact_sales_margin"
COST_MISSING = "COST_MISSING"
COST_OK = "OK"
ERP_COST_TABLE = "oracle_wwi_mdm_product_master"
PRODUCT_COST_REF = "ref_product_cost"

COST_CONTRACT_COLUMNS = (
    "product_business_key",
    "stock_item_business_key",
    "unit_cost",
    "cost_currency_code",
    "effective_from",
    "effective_to",
    "cost_basis_code",
)


def costFromProductMaster(productMaster: DataFrame) -> DataFrame:
    """ERP cost contract from bronze WWI_MDM.PRODUCT_MASTER.

    # LEGACY QUIRK: PRODUCT_MASTER.UNIT_COST_STD is the *standard* cost - the DDL header says
    # the GL uses PRODUCT_COST history, which is not in the checked-in estate. Standard cost
    # is treated as effective for all history (cost_basis_code = 'STD'), matching the legacy
    # APAC "standard cost" basis; the row records which basis produced the cost.
    """
    return productMaster.select(
        F.col("ITEM_NBR").cast("string").alias("product_business_key"),
        F.col("WWI_STOCK_ITEM_ID").cast("string").alias("stock_item_business_key"),
        F.col("UNIT_COST_STD").cast("decimal(19,4)").alias("unit_cost"),
        F.coalesce(F.col("COST_CURR_CD"), F.lit("USD")).alias("cost_currency_code"),
        F.lit("1900-01-01").cast("date").alias("effective_from"),
        F.lit(None).cast("date").alias("effective_to"),
        F.lit("STD").alias("cost_basis_code"),
    ).filter(F.coalesce(F.col("DELETED_FLG"), F.lit("N")) != F.lit("Y"))


def _costAsOf(sale: DataFrame, costs: DataFrame | None) -> DataFrame:
    if costs is None:
        return (
            sale.withColumn("unit_cost", F.lit(None).cast("decimal(19,4)"))
            .withColumn("cost_currency_code", F.lit(None).cast("string"))
            .withColumn("cost_basis_code", F.lit(None).cast("string"))
        )
    c = costs.select(
        F.col("product_business_key").alias("_c_product"),
        F.col("stock_item_business_key").alias("_c_stock"),
        F.col("unit_cost").cast("decimal(19,4)").alias("unit_cost"),
        F.col("cost_currency_code"),
        F.col("effective_from").cast("date").alias("_c_from"),
        F.col("effective_to").cast("date").alias("_c_to"),
        F.col("cost_basis_code"),
    )
    d = F.col("invoice_date_key")
    keyMatch = (F.col("_c_product").isNotNull() & (F.col("_c_product") == F.col("product_business_key"))) | (
        F.col("_c_stock").isNotNull() & (F.col("_c_stock") == F.col("stock_item_business_key"))
    )
    cond = keyMatch & (F.col("_c_from") <= d) & (F.col("_c_to").isNull() | (d < F.col("_c_to")))
    w = Window.partitionBy("sale_line_business_key").orderBy(
        F.col("_c_from").desc_nulls_last(), F.col("_c_product").desc_nulls_last()
    )
    return (
        sale.join(c, cond, "left")
        .withColumn("_c_rn", F.row_number().over(w))
        .filter(F.col("_c_rn") == 1)
        .drop("_c_rn", "_c_product", "_c_stock", "_c_from", "_c_to")
    )


def _adjustments(sale: DataFrame, factReturn: DataFrame | None, factCreditNote: DataFrame | None) -> DataFrame:
    if factReturn is not None and "original_sale_line_business_key" in factReturn.columns:
        r = factReturn.groupBy(F.col("original_sale_line_business_key").alias("sale_line_business_key")).agg(
            F.sum("net_credit_amount_reporting").cast("decimal(19,4)").alias("return_amount")
        )
        sale = sale.join(r, "sale_line_business_key", "left")
    else:
        sale = sale.withColumn("return_amount", F.lit(None).cast("decimal(19,4)"))
    if factCreditNote is not None and "original_sale_business_key" in factCreditNote.columns:
        cn = factCreditNote.groupBy(F.col("original_sale_business_key").alias("sale_business_key")).agg(
            F.sum("credit_including_tax_reporting").cast("decimal(19,4)").alias("_cn_invoice")
        )
        share = F.col("net_amount") / F.sum("net_amount").over(Window.partitionBy("sale_business_key"))
        sale = (
            sale.join(cn, "sale_business_key", "left")
            .withColumn("credit_note_amount", money(F.col("_cn_invoice") * share))
            .drop("_cn_invoice")
        )
    else:
        sale = sale.withColumn("credit_note_amount", F.lit(None).cast("decimal(19,4)"))
    return sale


def buildFactSalesMargin(
    spark: SparkSession,
    cfg: PipelineConfig,
    factSale: DataFrame,
    costs: DataFrame | None,
    factReturn: DataFrame | None = None,
    factCreditNote: DataFrame | None = None,
) -> DataFrame:
    sale = factSale.select(
        "sale_line_business_key",
        "sale_business_key",
        "invoice_date_key",
        "customer_key",
        "stock_item_key",
        "salesperson_key",
        "sales_territory_key",
        "sales_channel_key",
        "promotion_key",
        "customer_segment_key",
        "currency_key",
        "region_code",
        "invoice_number",
        "invoice_line_number",
        "order_number",
        "stock_item_business_key",
        "quantity",
        "gross_amount",
        "line_discount_amount",
        "net_amount",
        "tax_amount",
        "net_amount_reporting",
        "lineage_key",
        F.col("fx_rate_to_reporting").alias("sale_fx_rate_to_reporting"),
        optionalColumn(factSale, "product_business_key", "string").alias("product_business_key"),
        optionalColumn(factSale, "quantity_base_uom", "decimal(18,4)").alias("quantity_base_uom"),
    )
    df = _costAsOf(sale, costs)
    qty = F.coalesce(F.col("quantity_base_uom"), F.col("quantity"))
    df = df.withColumn("cost_amount", money(qty * F.col("unit_cost")))
    df = df.withColumn("_cost_ccy", F.coalesce(F.col("cost_currency_code"), F.lit(cfg.reportingCurrency)))
    df = rules_adapter.applyFx(
        df,
        spark,
        cfg,
        amountCols=["cost_amount"],
        currencyCol="_cost_ccy",
        dateCol="invoice_date_key",
        regionCol="region_code",
    ).withColumnRenamed("fx_rate_to_reporting", "cost_fx_rate_to_reporting")
    df = df.withColumnRenamed("fx_rate_source_code", "cost_fx_rate_source_code").drop(
        "fx_rate_effective_date", "_cost_ccy"
    )
    df = _adjustments(df, factReturn, factCreditNote)

    netUsd = F.col("net_amount_reporting")
    costUsd = F.col("cost_amount_reporting")
    df = (
        df.withColumn("cost_usd", costUsd)
        .withColumn("net_usd", netUsd)
        .withColumn("gross_margin_reporting", money(netUsd - costUsd))
        .withColumn(
            "margin_percent",
            F.when(netUsd != 0, F.round(F.col("gross_margin_reporting") / netUsd * 100, 4)).cast("decimal(9,4)"),
        )
        # Cost-missing rows are kept with null cost and flagged - never dropped.
        .withColumn(
            "margin_status_code", F.when(F.col("unit_cost").isNull(), F.lit(COST_MISSING)).otherwise(F.lit(COST_OK))
        )
        .withColumn("negative_margin_flag", F.coalesce(F.col("gross_margin_reporting") < 0, F.lit(False)))
    )
    out = df.select(
        surrogateKey("sale_line_business_key").alias("sales_margin_key"),
        F.col("sale_line_business_key"),
        F.col("sale_business_key"),
        F.col("invoice_date_key"),
        F.col("customer_key"),
        F.col("stock_item_key"),
        F.lit(-1).cast("bigint").alias("product_category_key"),
        F.col("salesperson_key"),
        F.col("sales_territory_key"),
        F.col("sales_channel_key"),
        F.col("promotion_key"),
        F.col("customer_segment_key"),
        F.col("currency_key"),
        F.col("region_code"),
        F.col("invoice_number"),
        F.col("invoice_line_number"),
        F.col("order_number"),
        F.col("product_business_key"),
        F.col("stock_item_business_key"),
        F.coalesce(F.col("quantity_base_uom"), F.col("quantity")).alias("quantity_base_uom"),
        F.col("gross_amount"),
        F.col("line_discount_amount"),
        F.lit(None).cast("decimal(19,4)").alias("promotion_discount_amount"),
        F.col("net_amount"),
        F.col("tax_amount"),
        F.lit(None).cast("decimal(19,4)").alias("freight_recovered_amount"),
        F.lit(None).cast("decimal(19,4)").alias("freight_cost_amount"),
        F.col("unit_cost"),
        F.col("cost_currency_code"),
        F.col("cost_amount").alias("cost_of_sale_amount"),
        F.col("cost_basis_code"),
        F.col("cost_amount").alias("standard_cost_amount"),
        F.lit(None).cast("decimal(19,4)").alias("purchase_price_variance"),
        F.lit(None).cast("decimal(19,4)").alias("rebate_accrual_amount"),
        F.col("credit_note_amount"),
        F.col("return_amount"),
        F.col("gross_margin_reporting").alias("gross_margin_amount"),
        F.col("gross_margin_reporting").alias("standard_margin_amount"),
        money(
            F.col("gross_margin_reporting")
            + F.coalesce(F.col("return_amount"), F.lit(0))
            + F.coalesce(F.col("credit_note_amount"), F.lit(0))
        ).alias("contribution_margin_amount"),
        F.col("margin_percent"),
        F.col("margin_percent").alias("standard_margin_percent"),
        F.col("sale_fx_rate_to_reporting").alias("fx_rate_to_reporting"),
        F.col("cost_fx_rate_to_reporting"),
        F.col("cost_fx_rate_source_code"),
        F.col("net_amount_reporting"),
        F.col("net_usd"),
        F.col("cost_usd"),
        F.col("gross_margin_reporting"),
        F.col("margin_status_code"),
        F.col("negative_margin_flag"),
        F.lit(False).alias("restated_flag"),
        F.col("lineage_key"),
    )
    return withLoadMetadata(out, cfg)


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    costs = readOptional(spark, cfg, "silver", PRODUCT_COST_REF)
    if costs is None:
        master = readOptional(spark, cfg, "bronze", ERP_COST_TABLE)
        costs = costFromProductMaster(master) if master is not None else None
    df = buildFactSalesMargin(
        spark,
        cfg,
        factSale=spark.table(cfg.fqn("gold", "fact_sale")),
        costs=costs,
        factReturn=readOptional(spark, cfg, "gold", "fact_return"),
        factCreditNote=readOptional(spark, cfg, "gold", "fact_credit_note"),
    )
    overwriteTable(df, cfg.fqn("gold", TABLE))
