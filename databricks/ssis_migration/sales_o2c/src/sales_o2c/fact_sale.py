"""08_facts: FACT_NA_Load_Sale, FACT_EU_Load_Sale, FACT_APAC_Load_Sale and FACT_Dedup_Sale.

All three regional packages share one pipeline: stg_sale_line filtered on region_code, dimension lookups,
region-specific tax / FX / margin rules (computeSaleMeasures), MERGE into gold_fact_sale (grain = invoice line,
legacy Fact.Sale column set + regional measures). The regional tax-rate table (stg.TaxRate) and FX table
(stg.FxRate) are empty on the legacy host, so the rate falls back to the rate carried on the invoice line and the
FX rate to the invoice header rate (1.0 when absent) - this is exactly what reproduces the populated Fact.Sale.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_o2c.config import DW_CATALOG, RunContext
from sales_o2c.dimensions import asOfLookup, dimension
from sales_o2c.tables import appendRows, deleteWhere, mergeUpsert, tableExists, withAudit
from sales_o2c.watermark import TIMESTAMP, getWatermark, setWatermark

REGION_RULES = {
    "NA": {"taxRegimeCode": "SALESTAX", "reportingCurrency": "USD"},
    "EU": {"taxRegimeCode": "VAT", "reportingCurrency": "EUR"},
    "APAC": {"taxRegimeCode": "GST", "reportingCurrency": "AUD", "distributorRebatePercent": 2.5, "fiscalYearStartMonth": 7},
}

LEGACY_BUSINESS_COLUMNS = [
    "city_key", "customer_key", "bill_to_customer_key", "stock_item_key", "invoice_date_key", "delivery_date_key",
    "salesperson_key", "wwi_invoice_id", "description", "package", "quantity", "unit_price", "tax_rate",
    "total_excluding_tax", "tax_amount", "profit", "total_including_tax", "total_dry_items", "total_chiller_items",
]


def _col(df: DataFrame, name: str, default):
    return F.col(name) if name in df.columns else F.lit(default)


def computeSaleMeasures(df: DataFrame, region: str) -> DataFrame:
    """Pure regional measure rules (SSIS Derived Column of FACT_<region>_Load_Sale)."""
    rules = REGION_RULES[region]
    qty = F.col("quantity").cast("decimal(18,4)")
    price = F.col("unit_price_amount").cast("decimal(18,2)")
    gross = (qty * price).cast("decimal(18,2)")
    discountPct = F.coalesce(_col(df, "discount_percent", None), F.lit(0)).cast("decimal(9,4)")
    discount = (gross * discountPct / 100).cast("decimal(18,2)")
    net = (gross - discount).cast("decimal(18,2)")
    rate = F.coalesce(_col(df, "regional_tax_rate_percent", None), F.col("tax_rate_percent")).cast("decimal(18,3)")
    unitCost = F.coalesce(_col(df, "unit_cost", 0), F.lit(0)).cast("decimal(18,2)")
    fx = F.coalesce(_col(df, "transaction_fx_rate", 1), F.lit(1)).cast("decimal(18,6)")

    out = (
        df.withColumn("gross_amount", gross)
        .withColumn("discount_amount", discount)
        .withColumn("net_amount", net)
        .withColumn("tax_rate_applied", rate)
    )
    if region == "NA":
        exempt = F.coalesce(_col(df, "grocery_exempt_flag", False), F.lit(False))
        tax = F.when(rate.isNull() | exempt, F.lit(0)).otherwise(net * rate / 100)
        out = out.withColumn("tax_amount", F.round(tax, 2).cast("decimal(18,2)")).withColumn("fx_rate_to_usd", F.lit(1).cast("decimal(18,6)"))
    elif region == "EU":
        vatNumber = F.trim(F.coalesce(_col(df, "customer_tax_registration_number", ""), F.lit("")))
        reverseCharge = (vatNumber != "") & (F.coalesce(_col(df, "customer_country", ""), F.lit("")) != F.coalesce(_col(df, "ship_to_country", ""), F.lit("")))
        vatApplied = F.when(reverseCharge, F.lit(0)).otherwise(F.coalesce(rate, F.lit(0))).cast("decimal(18,3)")
        out = (
            out.withColumn("reverse_charge_flag", F.when(reverseCharge, "Y").otherwise("N"))
            .withColumn("vat_rate_applied", vatApplied)
            .withColumn("tax_amount", F.round(net * vatApplied / 100, 2).cast("decimal(18,2)"))
            .withColumn("intrastat_commodity_flag", F.when(F.coalesce(_col(df, "customer_country", ""), F.lit("")) != F.coalesce(_col(df, "ship_to_country", ""), F.lit("")), "Y").otherwise("N"))
            .withColumn("fx_rate_to_usd", fx)
        )
    else:  # APAC
        inclusive = F.coalesce(_col(df, "gst_inclusive_flag", False), F.lit(False))
        r = F.coalesce(rate, F.lit(0))
        tax = F.when(inclusive, gross - gross / (1 + r / 100)).otherwise(gross * r / 100)
        fyStart = rules["fiscalYearStartMonth"]
        out = (
            out.withColumn("tax_amount", F.round(tax, 2).cast("decimal(18,2)"))
            .withColumn("gst_inclusive_flag", F.when(inclusive, "Y").otherwise("N"))
            .withColumn("distributor_rebate_accrual", F.round(qty * price * rules["distributorRebatePercent"] / 100, 2).cast("decimal(18,2)"))
            .withColumn("fiscal_year", F.when(F.month("invoice_date") >= fyStart, F.year("invoice_date") + 1).otherwise(F.year("invoice_date")))
            .withColumn("fx_rate_to_usd", fx)
        )
    totalCost = (qty * unitCost).cast("decimal(18,2)")
    return (
        out.withColumn("total_cost_amount", totalCost)
        .withColumn("margin_amount", (F.col("net_amount") - totalCost).cast("decimal(18,2)"))
        .withColumn(
            "margin_percent",
            F.when(gross == 0, F.lit(0)).otherwise((gross - qty * unitCost) / gross * 100).cast("decimal(9,4)"),
        )
        .withColumn("net_amount_reporting", F.round(F.col("net_amount") * F.col("fx_rate_to_usd"), 2).cast("decimal(18,2)"))
        .withColumn("reporting_currency", F.lit(rules["reportingCurrency"]))
        .withColumn("tax_regime_code", F.lit(rules["taxRegimeCode"]))
        .withColumn("total_excluding_tax", F.col("net_amount"))
        .withColumn("total_including_tax", (F.col("net_amount") + F.col("tax_amount")).cast("decimal(18,2)"))
    )


def toFactSaleRows(df: DataFrame, ctx: RunContext) -> DataFrame:
    """Project the legacy Fact.Sale shape (+ regional measures) from an enriched, measured line set."""
    extras = [c for c in ["reverse_charge_flag", "vat_rate_applied", "intrastat_commodity_flag", "gst_inclusive_flag", "distributor_rebate_accrual", "fiscal_year"] if c in df.columns]
    return df.select(
        F.col("invoice_line_id").alias("wwi_invoice_line_id"),
        F.col("invoice_line_id").alias("sale_key"),
        F.col("city_key"), F.col("customer_key"), F.col("bill_to_customer_key"), F.col("stock_item_key"),
        F.col("invoice_date").alias("invoice_date_key"),
        F.col("confirmed_delivery_utc").cast("date").alias("delivery_date_key"),
        F.col("salesperson_key"),
        F.col("invoice_id").alias("wwi_invoice_id"),
        F.col("line_description").alias("description"),
        F.col("package_type_code").alias("package"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price_amount").alias("unit_price"),
        F.col("tax_rate_percent").alias("tax_rate"),
        F.col("total_excluding_tax"), F.col("tax_amount"),
        F.col("line_profit_amount").alias("profit"),
        F.col("total_including_tax"),
        F.when(~F.col("is_chiller_stock"), F.col("quantity")).otherwise(F.lit(0)).cast("int").alias("total_dry_items"),
        F.when(F.col("is_chiller_stock"), F.col("quantity")).otherwise(F.lit(0)).cast("int").alias("total_chiller_items"),
        F.lit(int(ctx.packageExecutionId)).cast("bigint").alias("lineage_key"),
        F.col("region_code"), F.col("tax_regime_code"),
        F.col("gross_amount"), F.col("discount_amount"), F.col("net_amount"), F.col("total_cost_amount"),
        F.col("margin_amount"), F.col("margin_percent"), F.col("fx_rate_to_usd"), F.col("reporting_currency"), F.col("net_amount_reporting"),
        F.col("last_modified_when"),
        *[F.col(c) for c in extras],
    )


def enrichSaleLines(spark: SparkSession, lines: DataFrame) -> DataFrame:
    lm = F.greatest(F.col("source_modified_date"), F.coalesce(F.col("header_modified_date"), F.col("source_modified_date")))
    df = lines.withColumn("last_modified_when", lm)
    df = asOfLookup(df, dimension(spark, "Customer"), "customer_id", "last_modified_when", "customer_key")
    df = asOfLookup(df, dimension(spark, "Customer"), "bill_to_customer_id", "last_modified_when", "bill_to_customer_key")
    df = asOfLookup(df, dimension(spark, "StockItem"), "stock_item_id", "last_modified_when", "stock_item_key")
    df = asOfLookup(df, dimension(spark, "City"), "delivery_city_id", "last_modified_when", "city_key")
    df = asOfLookup(df, dimension(spark, "Employee"), "salesperson_id", "last_modified_when", "salesperson_key")
    return df


def _lookupFailures(df: DataFrame, objectName: str) -> DataFrame:
    parts = []
    for keyCol, dimName, bizCol in [("customer_key", "Customer", "customer_id"), ("stock_item_key", "Stock Item", "stock_item_id")]:
        parts.append(
            df.filter(F.col(keyCol) == 0).select(
                F.lit(objectName).alias("source_object_name"), F.col("sale_line_business_key").alias("source_business_key"),
                F.lit(dimName).alias("lookup_name"), F.lit(f"WWI {dimName} ID").alias("lookup_column_name"),
                F.col(bizCol).cast("string").alias("lookup_value"), F.lit("LOOKUP_NO_MATCH").alias("reject_reason_code"),
                F.lit("Fact").alias("reject_stage"), F.lit(True).alias("routed_to_unknown_member"), F.lit(True).alias("queued_for_late_arrival"),
                F.current_timestamp().alias("rejected_at_utc"),
            )
        )
    return parts[0].unionByName(parts[1])


def runFactSaleRegion(spark: SparkSession, ctx: RunContext, region: str) -> dict:
    objectName = f"Fact.Sale.{region}"
    wmFrom = getWatermark(spark, ctx, objectName, TIMESTAMP)
    lines = spark.table(ctx.table("stg_sale_line")).filter(F.col("region_code") == region)
    if "dq_status_code" in lines.columns:
        lines = lines.filter(F.coalesce(F.col("dq_status_code"), F.lit("PASS")) != "FAIL")
    if not ctx.reloadFullHistory:
        lines = lines.filter(F.col("source_modified_date") > F.lit(wmFrom).cast("timestamp"))
    zeroQty = lines.filter(F.col("quantity") == 0)
    lines = lines.filter(F.col("quantity") != 0)
    enriched = computeSaleMeasures(enrichSaleLines(spark, lines), region)
    facts = withAudit(toFactSaleRows(enriched, ctx), ctx)
    rowsInserted = mergeUpsert(spark, ctx.table("gold_fact_sale"), facts, ["wwi_invoice_line_id"])
    rejected = appendRows(spark, ctx.table("err_rejected_lookup_failure"), withAudit(_lookupFailures(enriched, f"stg.SaleLine[{region}]"), ctx))
    rejected += appendRows(
        spark, ctx.table("err_rejected_constraint_violation"),
        withAudit(zeroQty.select(F.lit("Fact.Sale").alias("target_object_name"), F.lit("CK_Quantity_NonZero").alias("constraint_name"), F.col("sale_line_business_key").alias("violating_business_key"), F.lit("ZERO_QUANTITY").alias("reject_reason_code"), F.lit("Fact").alias("reject_stage"), F.current_timestamp().alias("rejected_at_utc")), ctx),
    )
    maxLm = enriched.agg(F.max("last_modified_when")).first()[0]
    if maxLm is not None:
        setWatermark(spark, ctx, objectName, TIMESTAMP, maxLm.isoformat())
    return {"rowsRead": rowsInserted + zeroQty.count(), "rowsInserted": rowsInserted, "rowsRejected": rejected}


# ------------------------------------------------------------------ FACT_Dedup_Sale
def naturalKeyHash(df: DataFrame):
    return F.md5(F.concat_ws("|", F.col("wwi_invoice_id"), F.col("stock_item_key"), F.coalesce(F.col("description"), F.lit("")), F.col("quantity"), F.col("unit_price").cast("string")))


def rankDuplicates(df: DataFrame) -> DataFrame:
    """Survivor = highest lineage_key, then highest sale_key; everything else is a duplicate loser."""
    w = Window.partitionBy("natural_key_hash").orderBy(F.col("lineage_key").desc_nulls_last(), F.col("sale_key").desc())
    return (
        df.withColumn("natural_key_hash", naturalKeyHash(df))
        .withColumn("duplicate_rank", F.row_number().over(w))
        .withColumn("duplicate_count", F.count("*").over(Window.partitionBy("natural_key_hash")))
    )


def runFactDedupSale(spark: SparkSession, ctx: RunContext, lookbackDays: int = 7) -> dict:
    table = ctx.table("gold_fact_sale")
    if not tableExists(spark, table):
        return {"rowsRead": 0}
    fact = spark.table(table)
    maxDate = fact.agg(F.max("invoice_date_key")).first()[0]
    if maxDate is None:
        return {"rowsRead": 0}
    window = fact.filter(F.col("invoice_date_key") >= F.date_sub(F.lit(maxDate), lookbackDays))
    ranked = rankDuplicates(window)
    losers = ranked.filter(F.col("duplicate_rank") > 1)
    archived = appendRows(spark, ctx.table("work_fact_sale_duplicate_archive"), withAudit(losers.withColumn("archive_datetime", F.current_timestamp()), ctx))
    if archived:
        losers.select("wwi_invoice_line_id").createOrReplaceTempView("dedup_losers")
        spark.sql(f"DELETE FROM {table} WHERE wwi_invoice_line_id IN (SELECT wwi_invoice_line_id FROM dedup_losers)")
    duplicates = ranked.filter(F.col("duplicate_count") > 1).count()
    return {"rowsRead": duplicates, "rowsDeleted": archived}


def factSaleForRecon(spark: SparkSession, ctx: RunContext) -> DataFrame:
    return spark.table(ctx.table("gold_fact_sale")) if tableExists(spark, ctx.table("gold_fact_sale")) else None


__all__ = ["runFactSaleRegion", "runFactDedupSale", "computeSaleMeasures", "rankDuplicates", "toFactSaleRows", "LEGACY_BUSINESS_COLUMNS", "DW_CATALOG", "deleteWhere"]
