"""``fact_order`` - order-line grain, incremental MERGE on ``order_line_business_key``.

Replaces Integration.usp_LoadFactOrder and SSIS FACT_Load_Order. Backorder and
hold flags come from silver ``backorder`` / ``order_hold``; the OLTP
``FulfilmentFlags`` string is exploded into booleans.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
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
    pickColumn,
    readOptional,
    surrogateKey,
    withLoadMetadata,
)
from sales_lakehouse.gold.inputs import readDimSalesperson

TABLE = "fact_order"
KEY_COLS = ["order_line_business_key"]
SOURCE_TABLE = "silver.order_line"


def _flag(tokens: Column, *codes: str) -> Column:
    return F.coalesce(F.arrays_overlap(tokens, F.array(*[F.lit(c) for c in codes])), F.lit(False))


def _withFulfilmentFlags(df: DataFrame) -> DataFrame:
    # LEGACY QUIRK: Sales.Orders.FulfilmentFlags is written in two formats - the order-entry
    # screen writes pipe-delimited single letters ('H|B|S|X') while the line trigger writes
    # comma-delimited words ('BO,ALLOC,SHIP,PROMO'). Both are accepted here.
    raw = optionalColumn(df, "fulfilment_flags", "string")
    tokens = F.split(F.upper(F.coalesce(raw, F.lit(""))), r"[|,]")
    return (
        df.withColumn("flag_hold", _flag(tokens, "H", "HOLD"))
        .withColumn("flag_backorder", _flag(tokens, "B", "BO"))
        .withColumn("flag_split", _flag(tokens, "S", "SPLIT"))
        .withColumn("flag_export", _flag(tokens, "X", "EXPORT"))
        .withColumn("flag_allocated", _flag(tokens, "ALLOC"))
        .withColumn("flag_shipped", _flag(tokens, "SHIP"))
        .withColumn("flag_promotion", _flag(tokens, "PROMO"))
    )


def _lineTax(df: DataFrame) -> Column:
    """Staged line tax, else tax on the net line value at the line's percent rate (Sales.OrderLines carries only TaxRate)."""
    if "tax_amount" in df.columns:
        return F.col("tax_amount").cast("decimal(19,4)")
    net = pickColumn(df, ["net_line_amount", "net_line_amount_local"], "decimal(19,4)")
    return money(net * pickColumn(df, ["tax_rate_percent"], "decimal(18,3)") / F.lit(100))


def _backorderState(backorders: DataFrame | None) -> DataFrame | None:
    if backorders is None:
        return None
    isOpen = optionalColumn(backorders, "closed_when_utc", "timestamp").isNull() & (
        ~F.upper(F.coalesce(optionalColumn(backorders, "backorder_status", "string"), F.lit(""))).isin(
            "CLOSED", "CANCELLED"
        )
    )
    return backorders.groupBy("order_line_business_key").agg(
        F.max(F.when(isOpen, F.lit(True)).otherwise(F.lit(False))).alias("_bo_open"),
        F.max(optionalColumn(backorders, "shortage_reason_code", "string")).alias("_bo_reason"),
        F.max(optionalColumn(backorders, "promised_date", "date")).alias("_bo_promised"),
        F.sum(F.when(isOpen, optionalColumn(backorders, "quantity_short", "decimal(18,4)")).otherwise(F.lit(0)))
        .cast("decimal(18,4)")
        .alias("_bo_short"),
    )


def _holdState(holds: DataFrame | None) -> DataFrame | None:
    if holds is None:
        return None
    active = optionalColumn(holds, "released_when_utc", "timestamp").isNull()
    holdType = F.upper(optionalColumn(holds, "hold_type_code", "string"))
    blocking = F.coalesce(optionalColumn(holds, "is_blocking_despatch", "boolean"), F.lit(False))
    return holds.groupBy("order_business_key").agg(
        F.max(F.when(active, F.lit(True)).otherwise(F.lit(False))).alias("_hold_active"),
        F.max(F.when(active & (holdType == "CREDIT"), F.lit(True)).otherwise(F.lit(False))).alias("_hold_credit"),
        F.max(F.when(active & blocking, F.lit(True)).otherwise(F.lit(False))).alias("_hold_blocking"),
        F.max(F.when(active, optionalColumn(holds, "hold_type_code", "string"))).alias("_hold_type"),
    )


def buildFactOrder(
    spark: SparkSession,
    cfg: PipelineConfig,
    lines: DataFrame,
    heads: DataFrame,
    backorders: DataFrame | None,
    holds: DataFrame | None,
    dimCustomer: DataFrame | None,
    dimSalesperson: DataFrame | None,
    dimSalesChannel: DataFrame | None,
    dimSalesTerritory: DataFrame | None,
    dimStockItem: DataFrame | None,
) -> DataFrame:
    h = heads.select(
        F.col("order_business_key").alias("_h_key"),
        optionalColumn(heads, "source_order_id", "string").cast("int").alias("wwi_order_id"),
        optionalColumn(heads, "backorder_order_business_key", "string").alias("wwi_backorder_id"),
        F.col("customer_business_key").alias("_h_customer_business_key"),
        optionalColumn(heads, "salesperson_business_key", "string").alias("salesperson_business_key"),
        F.col("order_date").alias("_h_order_date"),
        optionalColumn(heads, "expected_delivery_date", "date").alias("requested_delivery_date_key"),
        optionalColumn(heads, "customer_purchase_order_number", "string").alias("customer_purchase_order_number"),
        F.coalesce(optionalColumn(heads, "is_undersupply_backordered", "boolean"), F.lit(False)).alias(
            "is_undersupply_backordered"
        ),
        optionalColumn(heads, "sales_channel_code", "string").alias("sales_channel_code"),
        optionalColumn(heads, "sales_territory_code", "string").alias("sales_territory_code"),
        optionalColumn(heads, "order_status_code", "string").alias("order_status_code"),
        pickColumn(heads, ["transaction_currency_code", "currency_code"], "string").alias("_h_currency"),
        optionalColumn(heads, "region_code", "string").alias("_h_region"),
        optionalColumn(heads, "fulfilment_flags", "string").alias("fulfilment_flags"),
    ).dropDuplicates(["_h_key"])
    df = lines.join(h, lines["order_business_key"] == h["_h_key"], "left").drop("_h_key")
    df = (
        df.withColumn(
            "customer_business_key",
            F.coalesce(optionalColumn(df, "customer_business_key", "string"), F.col("_h_customer_business_key")),
        )
        .withColumn("order_date", F.coalesce(F.col("order_date"), F.col("_h_order_date")).cast("date"))
        .withColumn("region_code", F.upper(F.coalesce(optionalColumn(df, "region_code", "string"), F.col("_h_region"))))
        .withColumn(
            "transaction_currency_code",
            F.coalesce(pickColumn(df, ["transaction_currency_code", "currency_code"], "string"), F.col("_h_currency")),
        )
        .drop("_h_order_date", "_h_region", "_h_currency", "_h_customer_business_key")
    )

    # LEGACY QUIRK: zero-quantity lines are quotation-module artefacts and are rejected, not loaded.
    df = quarantine(
        spark,
        cfg,
        df,
        "FACT_ORDER_ZERO_QTY",
        SOURCE_TABLE,
        F.col("ordered_quantity").isNull() | (F.col("ordered_quantity") == 0),
        "Order line quantity is zero (quotation module artefact)",
    )
    df = dedupeLatest(df, KEY_COLS, [F.col("loaded_at_utc").desc_nulls_last(), F.col("batch_id").desc_nulls_last()])
    df = quarantine(
        spark,
        cfg,
        df,
        "FACT_ORDER_DUP",
        SOURCE_TABLE,
        F.col("_rn") > 1,
        "Duplicate order_line_business_key - superseded by a later-loaded row",
    ).drop("_rn")
    df = df.withColumnRenamed("batch_id", "source_batch_id").withColumnRenamed("loaded_at_utc", "source_loaded_at_utc")

    bo = _backorderState(backorders)
    if bo is not None:
        df = df.join(bo, "order_line_business_key", "left")
    else:
        df = (
            df.withColumn("_bo_open", F.lit(None).cast("boolean"))
            .withColumn("_bo_reason", F.lit(None).cast("string"))
            .withColumn("_bo_promised", F.lit(None).cast("date"))
            .withColumn("_bo_short", F.lit(None).cast("decimal(18,4)"))
        )
    hs = _holdState(holds)
    if hs is not None:
        df = df.join(hs, "order_business_key", "left")
    else:
        df = (
            df.withColumn("_hold_active", F.lit(None).cast("boolean"))
            .withColumn("_hold_credit", F.lit(None).cast("boolean"))
            .withColumn("_hold_blocking", F.lit(None).cast("boolean"))
            .withColumn("_hold_type", F.lit(None).cast("string"))
        )
    df = _withFulfilmentFlags(df)

    qtyOrdered = F.col("ordered_quantity").cast("decimal(18,4)")
    qtyPicked = F.coalesce(F.col("picked_quantity").cast("decimal(18,4)"), F.lit(0).cast("decimal(18,4)"))
    df = (
        df.withColumn("quantity_ordered", qtyOrdered)
        .withColumn("quantity_allocated", qtyPicked)
        # LEGACY QUIRK: the base WWI model has no despatch quantity; SSIS FACT_Load_Order uses
        # QuantityOutstanding = QuantityOrdered - QuantityPicked, so picked == despatched here.
        .withColumn("quantity_despatched", qtyPicked)
        .withColumn("quantity_open", (qtyOrdered - qtyPicked).cast("decimal(18,4)"))
        .withColumn("unit_price", pickColumn(df, ["unit_price_amount", "unit_price_amount_local"], "decimal(19,4)"))
        .withColumn(
            "line_discount_amount",
            money(F.coalesce(pickColumn(df, ["line_discount_amount", "line_discount_amount_local"], "decimal(19,4)"), F.lit(0))),
        )
        .withColumn("gross_order_amount", money(qtyOrdered * F.col("unit_price")))
        .withColumn("net_order_amount", money(qtyOrdered * F.col("unit_price") - F.col("line_discount_amount")))
        .withColumn("open_value", money((qtyOrdered - qtyPicked) * F.col("unit_price")))
        .withColumn("fill_rate_percent", F.round(qtyPicked / qtyOrdered * 100, 4).cast("decimal(9,4)"))
        .withColumn("tax_amount", money(F.coalesce(_lineTax(df), F.lit(0))))
        .withColumn("total_excluding_tax", F.col("net_order_amount"))
        .withColumn("total_including_tax", money(F.col("net_order_amount") + F.col("tax_amount")))
        .withColumn(
            "is_backordered",
            F.coalesce(F.col("_bo_open"), F.lit(False)) | F.col("is_undersupply_backordered") | F.col("flag_backorder"),
        )
        .withColumn("is_on_hold", F.coalesce(F.col("_hold_active"), F.lit(False)) | F.col("flag_hold"))
        .withColumn("is_credit_hold", F.coalesce(F.col("_hold_credit"), F.lit(False)))
        .withColumn("is_despatch_blocked", F.coalesce(F.col("_hold_blocking"), F.lit(False)))
        .withColumn("hold_type_code", F.col("_hold_type"))
        .withColumn("backorder_reason_code", F.col("_bo_reason"))
        .withColumn("backorder_quantity_short", F.col("_bo_short"))
        .withColumn("promised_delivery_date_key", F.col("_bo_promised"))
        .withColumn("is_fully_picked", F.col("quantity_open") <= 0)
        .withColumn("is_open", F.col("quantity_open") > 0)
    )
    df = rules_adapter.applyFx(
        df,
        spark,
        cfg,
        amountCols=["net_order_amount", "open_value"],
        currencyCol="transaction_currency_code",
        dateCol="order_date",
        regionCol="region_code",
    )
    df = rules_adapter.resolveFiscalPeriod(df, spark, cfg, dateCol="order_date", regionCol="region_code")
    df = lookupScd2Key(
        df, dimCustomer, "customer_business_key", "order_date", "customer_key", "customer_business_key", "customer_key"
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
    df = df.withColumn("inferred_member_flag", F.col("customer_key") == F.lit(UNKNOWN_KEY))

    unknown = F.lit(UNKNOWN_KEY).cast("bigint")
    out = df.select(
        surrogateKey("order_line_business_key").alias("order_key"),
        F.col("order_line_business_key"),
        F.col("order_business_key"),
        unknown.alias("city_key"),
        F.col("customer_key"),
        F.col("stock_item_key"),
        F.col("order_date").alias("order_date_key"),
        optionalColumn(df, "picking_completed_when_utc", "timestamp").cast("date").alias("picked_date_key"),
        F.col("requested_delivery_date_key"),
        F.col("promised_delivery_date_key"),
        F.col("salesperson_key"),
        unknown.alias("picker_key"),
        F.col("sales_channel_key"),
        F.col("sales_territory_key"),
        unknown.alias("promotion_key"),
        unknown.alias("customer_segment_key"),
        unknown.alias("currency_key"),
        F.col("wwi_order_id"),
        F.col("wwi_backorder_id"),
        F.col("order_business_key").alias("order_number"),
        F.col("line_number").cast("int").alias("order_line_number"),
        F.col("customer_purchase_order_number"),
        F.col("customer_business_key"),
        F.col("stock_item_business_key"),
        F.col("salesperson_business_key"),
        F.col("order_status_code"),
        optionalColumn(df, "line_status_code", "string").alias("line_status_code"),
        F.col("line_description").alias("description"),
        optionalColumn(df, "package_type_code", "string").alias("package"),
        F.col("quantity_ordered").alias("quantity"),
        F.col("quantity_ordered"),
        F.col("quantity_allocated"),
        F.col("quantity_despatched"),
        F.col("quantity_open"),
        F.col("fill_rate_percent"),
        F.col("unit_price"),
        optionalColumn(df, "tax_rate_percent", "decimal(18,3)").alias("tax_rate"),
        F.col("total_excluding_tax"),
        F.col("tax_amount"),
        F.col("total_including_tax"),
        F.col("gross_order_amount"),
        F.col("line_discount_amount"),
        F.col("net_order_amount"),
        F.col("net_order_amount_reporting"),
        F.col("open_value_reporting"),
        F.col("transaction_currency_code"),
        F.col("fx_rate_to_reporting"),
        F.col("fx_rate_source_code"),
        F.col("region_code"),
        F.col("fiscal_year"),
        F.col("fiscal_period"),
        F.col("fiscal_period_key"),
        F.col("is_backordered"),
        F.col("backorder_reason_code"),
        F.col("backorder_quantity_short"),
        F.col("is_on_hold"),
        F.col("hold_type_code"),
        F.col("is_credit_hold"),
        F.col("is_despatch_blocked"),
        F.col("is_fully_picked"),
        F.col("is_open"),
        F.col("flag_hold"),
        F.col("flag_backorder"),
        F.col("flag_split"),
        F.col("flag_export"),
        F.col("flag_allocated"),
        F.col("flag_shipped"),
        F.col("flag_promotion"),
        F.col("inferred_member_flag"),
        F.col("source_batch_id").cast("bigint").alias("lineage_key"),
        F.col("source_loaded_at_utc"),
    )
    return withLoadMetadata(out, cfg)


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    df = buildFactOrder(
        spark,
        cfg,
        lines=spark.table(cfg.fqn("silver", "order_line")),
        heads=spark.table(cfg.fqn("silver", "order")),
        backorders=readOptional(spark, cfg, "silver", "backorder"),
        holds=readOptional(spark, cfg, "silver", "order_hold"),
        dimCustomer=readOptional(spark, cfg, "silver", "dim_customer"),
        dimSalesperson=readDimSalesperson(spark, cfg),
        dimSalesChannel=readOptional(spark, cfg, "silver", "dim_sales_channel"),
        dimSalesTerritory=readOptional(spark, cfg, "silver", "dim_sales_territory"),
        dimStockItem=readOptional(spark, cfg, "silver", "dim_stock_item"),
    )
    mergeOnKeys(spark, df, cfg.fqn("gold", TABLE), KEY_COLS)
