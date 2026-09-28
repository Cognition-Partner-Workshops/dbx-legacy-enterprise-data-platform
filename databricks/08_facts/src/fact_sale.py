"""Region-agnostic port of Integration.usp_LoadFactSale / FACT_<REGION>_Load_Sale.

The regional notebooks (NA / EU / APAC) supply only the tax rule and the FX
source; everything else - backdating window, natural key hash, structural
validation, SCD2 lookups, inferred customers, stock-item holds, effective FX,
measures, reversal (REV) + restatement (RES) rows and the MERGE - lives here."""

from datetime import date, timedelta
from typing import Callable, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbx_etl_common import control

import fact_common as fc
import fact_rules as rules

FACT_TABLE = "fact_sale"
FACT_OBJECT_NAME = "Fact.Sale"
SOURCE_SYSTEM_CODE = "SQLSTG"
DEFAULT_BACKDATING_DAYS = 5
VALID_REGIONS = (rules.REGION_NA, rules.REGION_EU, rules.REGION_APAC)
CURRENCY_SCRATCH_TABLE = "work_currency_conversion_scratch"

MEASURE_COLUMNS = [
    "quantity", "quantity_base_uom", "gross_amount", "line_discount_amount", "net_amount", "tax_amount",
    "total_excluding_tax", "total_including_tax", "profit", "freight_amount", "cost_of_sale_amount",
    "gross_margin_amount", "net_amount_reporting", "total_excluding_tax_reporting", "tax_amount_reporting",
]
KEY_COLUMNS = ["customer_key", "bill_to_customer_key", "stock_item_key", "salesperson_key", "promotion_key"]
LOOKUP_OUTPUT_COLUMNS = set(KEY_COLUMNS)

# snake_case of Fact.Sale (WWI base columns + Fact.Sale.Extensions.sql), plus package_execution_id for lineage
FACT_COLUMNS = [
    "sale_key", "city_key", "customer_key", "bill_to_customer_key", "stock_item_key", "invoice_date_key",
    "delivery_date_key", "salesperson_key", "wwi_invoice_id", "description", "package", "quantity", "unit_price",
    "tax_rate", "total_excluding_tax", "tax_amount", "profit", "total_including_tax", "lineage_key",
    "transaction_currency_code", "currency_key", "fx_rate_to_reporting", "fx_rate_effective_date", "fx_rate_source_code",
    "total_excluding_tax_reporting", "tax_amount_reporting", "region_code", "sales_territory_key", "fiscal_year",
    "fiscal_period", "tax_regime_code", "vat_rate", "vat_reverse_charge_flag", "customer_tax_registration", "gst_rate",
    "gst_free_flag", "sales_channel_key", "promotion_key", "line_discount_amount", "freight_amount",
    "cost_of_sale_amount", "quantity_base_uom", "source_uom_code", "gross_amount", "net_amount",
    "net_amount_reporting", "gross_margin_amount", "customer_segment_key", "source_row_version", "invoice_number",
    "order_number", "invoice_line_number", "natural_key_hash", "correction_type_code", "corrected_sale_key",
    "inferred_member_flag", "batch_id", "package_execution_id", "load_datetime",
]
CLUSTER_COLUMNS = ["invoice_date_key", "region_code"]


def loadDates(spark: SparkSession, catalog: str, p: dict, regionCode: str):
    wmFrom, wmTo = fc.loadWindow(spark, catalog, SOURCE_SYSTEM_CODE, "Fact.Sale.%s" % regionCode, p)
    backdating = int(fc.configurationValue(spark, catalog, "Fact.Sale.BackDatingDays", p["environmentCode"], str(DEFAULT_BACKDATING_DAYS)))
    businessDate: date = p["businessDate"]
    wmDate = wmFrom.date() if hasattr(wmFrom, "date") else wmFrom
    if p["reloadFullHistory"] or wmDate is None or wmDate < date(1901, 1, 1):
        loadStart = date(1900, 1, 1)
    else:
        loadStart = min(wmDate, businessDate) - timedelta(days=backdating)
    return loadStart, businessDate, wmTo


def readSourceLines(spark: SparkSession, catalog: str, regionCode: str, loadStart: date, loadEnd: date) -> DataFrame:
    """stg.SaleLine x stg.Sale (header), region-filtered, draft invoices excluded."""
    lines = fc.readTable(spark, catalog, "silver", "stg_sale_line").alias("l")
    headers = fc.readTable(spark, catalog, "silver", "stg_sale").alias("h")
    df = lines.join(headers, F.col("l.SaleBusinessKey") == F.col("h.SaleBusinessKey"), "inner")
    return (
        df.where(F.col("l.RegionCode") == regionCode)
        .where(F.coalesce(F.col("h.IsCreditNote"), F.lit(False)) == F.lit(False))
        .where(F.upper(F.coalesce(F.col("h.DqStatusCode"), F.lit(""))) != "DRAFT")
        .where(F.col("l.InvoiceDate").between(F.lit(loadStart), F.lit(loadEnd)))
        .select(
            F.col("l.SaleLineBusinessKey").alias("SaleLineBusinessKey"),
            F.col("h.SaleBusinessKey").alias("SaleBusinessKey"),
            F.coalesce(F.col("h.SourceInvoiceId").cast("string"), F.col("h.SaleBusinessKey").cast("string")).alias("InvoiceNumber"),
            F.col("l.LineNumber").alias("InvoiceLineNumber"),
            F.col("l.RegionCode").alias("RegionCode"),
            F.col("h.SourceSystemCode").alias("SourceSystemCode"),
            F.col("h.CustomerBusinessKey").alias("CustomerBusinessKey"),
            F.coalesce(F.col("h.BillToCustomerBusinessKey"), F.col("h.CustomerBusinessKey")).alias("BillToCustomerBusinessKey"),
            F.col("l.StockItemBusinessKey").alias("StockItemBusinessKey"),
            F.col("h.SalespersonBusinessKey").alias("SalespersonBusinessKey"),
            F.col("l.PromotionBusinessKey").alias("PromotionBusinessKey"),
            F.col("h.OrderBusinessKey").alias("OrderBusinessKey"),
            F.col("l.InvoiceDate").cast("date").alias("InvoiceDate"),
            F.to_date(F.col("h.ConfirmedDeliveryUtc")).alias("DeliveryDate"),
            F.col("l.LineDescription").alias("Description"),
            F.col("l.Quantity").alias("Quantity"),
            F.col("l.QuantityBaseUom").alias("QuantityBaseUom"),
            F.col("l.UomCode").alias("UomCode"),
            F.col("l.UnitPriceAmount").alias("UnitPrice"),
            F.col("l.NetLineAmount").alias("SourceNetAmount"),
            F.col("l.GrossLineAmount").alias("SourceGrossAmount"),
            F.col("l.LineProfitAmount").alias("SourceProfitAmount"),
            F.col("l.TaxRegimeCode").alias("TaxRegimeCode"),
            F.col("l.TaxRatePercent").alias("SourceTaxRate"),
            F.col("l.TaxAmount").alias("SourceTaxAmount"),
            F.coalesce(F.col("l.TransactionCurrencyCode"), F.col("h.TransactionCurrencyCode")).alias("TransactionCurrency"),
            F.coalesce(F.col("l.FxRateDate"), F.col("l.InvoiceDate")).cast("date").alias("FxRateDate"),
            F.col("h.DeliveryMethodCode").alias("SalesChannelCode"),
            F.col("h.FiscalPeriodLabel").alias("FiscalPeriodLabel"),
            F.coalesce(F.unix_timestamp(F.col("h.SourceModifiedDate")), F.unix_timestamp(F.col("l.LoadedAtUtc"))).cast("bigint").alias("SourceRowVersion"),
            F.col("l.LoadedAtUtc").alias("LoadedAtUtc"),
        )
        .withColumn("natural_key_hash", fc.naturalKeyHash(F.col("InvoiceNumber"), F.col("InvoiceLineNumber"), F.col("RegionCode")))
    )


def structuralRejects(df: DataFrame):
    """Structural validation from the procedure: NULL quantity / unit price /
    stock item and an unknown region are logged as FACT_VALIDATION and removed."""
    bad = (
        F.col("Quantity").isNull() | F.col("UnitPrice").isNull() | F.col("StockItemBusinessKey").isNull()
        | ~F.col("RegionCode").isin(*VALID_REGIONS)
    )
    return df.where(~bad), df.where(bad)


def withMeasures(df: DataFrame) -> DataFrame:
    uomFactor = F.when(
        F.col("QuantityBaseUom").isNotNull() & (F.col("Quantity") != 0), F.col("QuantityBaseUom") / F.col("Quantity")
    ).otherwise(F.lit(1)).cast("decimal(18,6)")
    gross = rules.saleGrossAmount(F.col("Quantity"), F.col("UnitPrice"), uomFactor)
    discount = F.when(F.col("SourceNetAmount").isNotNull(), F.greatest(gross - F.col("SourceNetAmount"), F.lit(0))).otherwise(F.lit(0))
    cost = F.when(
        F.col("SourceNetAmount").isNotNull() & F.col("SourceProfitAmount").isNotNull(), F.col("SourceNetAmount") - F.col("SourceProfitAmount")
    ).otherwise(F.lit(0))
    df = (
        df.withColumn("uom_conversion_factor", uomFactor)
        .withColumn("gross_amount", gross)
        .withColumn("line_discount_amount", rules.money(discount))
        .withColumn("cost_of_sale", rules.money(cost))
        .withColumn("unit_cost", F.when(F.col("Quantity") != 0, cost / F.col("Quantity")).otherwise(F.lit(0)).cast("decimal(18,4)"))
    )
    df = df.withColumn("net_amount", rules.saleNetAmount(F.col("gross_amount"), F.col("line_discount_amount")))
    df = df.withColumn("gross_margin", rules.saleMarginAmount(F.col("net_amount"), F.col("cost_of_sale")))
    return df.withColumn("margin_percent", rules.marginPercent(F.col("gross_margin"), F.col("net_amount")))


def withReportingValues(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("net_amount_reporting", rules.reportingAmount(F.col("net_amount"), F.col("FxRateToUsd")))
        .withColumn("total_excluding_tax_reporting", rules.reportingAmount(F.col("net_amount"), F.col("FxRateToUsd")))
        .withColumn("tax_amount_reporting", rules.reportingAmount(F.col("tax_amount"), F.col("FxRateToUsd")))
    )


def lookupDimensions(spark: SparkSession, catalog: str, df: DataFrame) -> DataFrame:
    customer = fc.readDimension(spark, catalog, fc.DIMENSIONS["Customer"])
    stockItem = fc.readDimension(spark, catalog, fc.DIMENSIONS["Stock Item"])
    salesperson = fc.readDimension(spark, catalog, fc.DIMENSIONS["Salesperson"])
    df = fc.lookupDimension(df, customer, fc.DIMENSIONS["Customer"], "CustomerBusinessKey", "customer_key", "InvoiceDate",
                            extraCols={"is_inferred_member": "customer_is_inferred"})
    df = fc.lookupDimension(df, customer, fc.DIMENSIONS["Customer"], "BillToCustomerBusinessKey", "bill_to_customer_key", "InvoiceDate")
    df = fc.lookupDimension(df, stockItem, fc.DIMENSIONS["Stock Item"], "StockItemBusinessKey", "stock_item_key", "InvoiceDate",
                            extraCols={"is_inferred_member": "stock_item_is_inferred"})
    df = fc.lookupDimension(df, salesperson, fc.DIMENSIONS["Salesperson"], "SalespersonBusinessKey", "salesperson_key", "InvoiceDate")
    promoName = fc.tableName(catalog, "gold", fc.DIMENSIONS["Promotion"].table)
    if fc.tableExists(spark, promoName):
        df = fc.lookupDimension(df, spark.table(promoName), fc.DIMENSIONS["Promotion"], "PromotionBusinessKey", "promotion_key")
    else:
        df = df.withColumn("promotion_key", F.when(F.col("PromotionBusinessKey").isNull(), F.lit(fc.NOT_APPLICABLE_KEY)).otherwise(F.lit(fc.UNKNOWN_KEY))).withColumn("promotion_key_miss", F.lit(False))
    return df.withColumn("promotion_key", F.when(F.col("PromotionBusinessKey").isNull(), F.lit(fc.NOT_APPLICABLE_KEY)).otherwise(F.col("promotion_key")))


def toFactRows(df: DataFrame, batchId: int, packageExecutionId: int, fiscalYearStartMonth: int = 1) -> DataFrame:
    df = fc.loadAuditColumns(df, batchId, packageExecutionId)
    inferred = F.coalesce(F.col("customer_is_inferred"), F.lit(False)) | F.coalesce(F.col("stock_item_is_inferred"), F.lit(False))
    return df.select(
        F.lit(None).cast("bigint").alias("sale_key"),
        F.lit(fc.NOT_APPLICABLE_KEY).alias("city_key"),
        F.col("customer_key"), F.col("bill_to_customer_key"), F.col("stock_item_key"),
        F.col("InvoiceDate").alias("invoice_date_key"),
        F.col("DeliveryDate").alias("delivery_date_key"),
        F.col("salesperson_key"),
        F.col("InvoiceNumber").alias("wwi_invoice_id"),
        F.col("Description").alias("description"),
        F.col("UomCode").alias("package"),
        F.col("Quantity").cast("decimal(18,4)").alias("quantity"),
        F.col("UnitPrice").cast("decimal(18,4)").alias("unit_price"),
        F.col("tax_rate").cast("decimal(18,3)").alias("tax_rate"),
        F.col("net_amount").alias("total_excluding_tax"),
        F.col("tax_amount"),
        F.col("gross_margin").alias("profit"),
        rules.money(F.col("net_amount") + F.col("tax_amount")).alias("total_including_tax"),
        F.col("lineage_key"),
        F.col("TransactionCurrency").alias("transaction_currency_code"),
        F.lit(fc.NOT_APPLICABLE_KEY).alias("currency_key"),
        F.col("FxRateToUsd").cast("decimal(19,9)").alias("fx_rate_to_reporting"),
        F.col("FxRateDateUsed").alias("fx_rate_effective_date"),
        F.col("FxRateSource").alias("fx_rate_source_code"),
        F.col("total_excluding_tax_reporting"),
        F.col("tax_amount_reporting"),
        F.col("RegionCode").alias("region_code"),
        F.lit(fc.UNKNOWN_KEY).alias("sales_territory_key"),
        rules.fiscalYear(F.col("InvoiceDate"), fiscalYearStartMonth).alias("fiscal_year"),
        rules.fiscalPeriod(F.col("InvoiceDate"), fiscalYearStartMonth).alias("fiscal_period"),
        F.col("TaxRegimeCode").alias("tax_regime_code"),
        F.col("vat_rate_applied").cast("decimal(18,3)").alias("vat_rate"),
        F.coalesce(F.col("is_reverse_charge"), F.lit(False)).cast("boolean").alias("vat_reverse_charge_flag"),
        F.col("CustomerVatNumber").alias("customer_tax_registration") if "CustomerVatNumber" in df.columns else F.lit(None).cast("string").alias("customer_tax_registration"),
        F.col("gst_rate_applied").cast("decimal(18,3)").alias("gst_rate"),
        F.col("gst_free_flag").cast("boolean").alias("gst_free_flag") if "gst_free_flag" in df.columns else F.lit(None).cast("boolean").alias("gst_free_flag"),
        F.lit(fc.UNKNOWN_KEY).alias("sales_channel_key"),
        F.col("promotion_key"),
        F.col("line_discount_amount"),
        F.lit(0).cast("decimal(18,2)").alias("freight_amount"),
        F.col("cost_of_sale").alias("cost_of_sale_amount"),
        F.coalesce(F.col("QuantityBaseUom"), F.col("Quantity")).cast("decimal(18,4)").alias("quantity_base_uom"),
        F.col("UomCode").alias("source_uom_code"),
        F.col("gross_amount"), F.col("net_amount"),
        F.col("net_amount_reporting"),
        F.col("gross_margin").alias("gross_margin_amount"),
        F.lit(fc.UNKNOWN_KEY).alias("customer_segment_key"),
        F.col("SourceRowVersion").alias("source_row_version"),
        F.col("InvoiceNumber").alias("invoice_number"),
        F.col("OrderBusinessKey").cast("string").alias("order_number"),
        F.col("InvoiceLineNumber").cast("int").alias("invoice_line_number"),
        F.col("natural_key_hash"),
        F.lit(fc.CORRECTION_ORIGINAL).alias("correction_type_code"),
        F.lit(None).cast("bigint").alias("corrected_sale_key"),
        inferred.alias("inferred_member_flag"),
        F.col("batch_id"), F.col("package_execution_id"), F.col("load_datetime"),
    )


def measureHash(prefix: str = ""):
    return F.xxhash64(*[F.coalesce(F.col(prefix + c).cast("string"), F.lit("")) for c in MEASURE_COLUMNS + KEY_COLUMNS])


def mergeWithReversals(spark: SparkSession, catalog: str, incoming: DataFrame, batchId: int, packageExecutionId: int) -> Dict[str, int]:
    """Changed lines get a negated REV row plus the restated RES row keyed by
    the existing Sale Key; unseen natural keys insert as ORIG. Unchanged
    lines are left alone so re-runs of the same window are idempotent."""
    fullName = fc.tableName(catalog, "gold", FACT_TABLE)
    fc.ensureTable(spark, fullName, incoming, clusterCols=CLUSTER_COLUMNS)
    existing = (
        spark.table(fullName)
        .where(F.coalesce(F.col("correction_type_code"), F.lit(fc.CORRECTION_ORIGINAL)).isin(fc.CORRECTION_ORIGINAL, fc.CORRECTION_RESTATEMENT))
        .select("natural_key_hash", F.col("sale_key").alias("existing_sale_key"), measureHash().alias("existing_hash"), *[F.col(c).alias("ex_" + c) for c in MEASURE_COLUMNS])
    )
    joined = incoming.withColumn("incoming_hash", measureHash()).join(existing, "natural_key_hash", "left")
    newRows = joined.where(F.col("existing_sale_key").isNull())
    changed = joined.where(F.col("existing_sale_key").isNotNull() & (F.col("incoming_hash") != F.col("existing_hash")))
    unchanged = joined.where(F.col("existing_sale_key").isNotNull() & (F.col("incoming_hash") == F.col("existing_hash"))).count()

    passthrough = [c for c in FACT_COLUMNS if c not in ("sale_key", "correction_type_code", "corrected_sale_key") and c not in MEASURE_COLUMNS]
    reversal = changed.select(
        *[F.col(c) for c in passthrough],
        *[(F.col("ex_" + c) * -1).cast("decimal(18,4)").alias(c) if c in ("quantity", "quantity_base_uom") else rules.negated(F.col("ex_" + c)).alias(c) for c in MEASURE_COLUMNS],
        F.lit(fc.CORRECTION_REVERSAL).alias("correction_type_code"),
        F.col("existing_sale_key").alias("corrected_sale_key"),
    )
    restated = changed.select(
        *[F.col(c) for c in FACT_COLUMNS if c not in ("sale_key", "correction_type_code", "corrected_sale_key")],
        F.lit(fc.CORRECTION_RESTATEMENT).alias("correction_type_code"),
        F.col("existing_sale_key").alias("corrected_sale_key"),
    ).withColumn("sale_key", F.col("corrected_sale_key"))
    fresh = newRows.select(*[F.col(c) for c in FACT_COLUMNS if c != "sale_key"]).withColumn("sale_key", F.lit(None).cast("bigint"))

    reversals = reversal.count()  # before the write: the plan reads the target table
    keyed = fc.assignSurrogateKeys(spark, fullName, reversal.unionByName(fresh.drop("sale_key")), "sale_key", ["invoice_date_key", "natural_key_hash", "correction_type_code"])
    toWrite = keyed.select(*FACT_COLUMNS).unionByName(restated.select(*FACT_COLUMNS))
    merged = fc.mergeFact(spark, fullName, toWrite, ["sale_key"], clusterCols=CLUSTER_COLUMNS)
    merged["unchanged"] = unchanged
    merged["reversals"] = reversals
    return merged


def loadRegion(
    spark: SparkSession, catalog: str, p: dict, regionCode: str, packageName: str, run, batchId: int,
    regionalTaxRule: Callable[[SparkSession, str, DataFrame], DataFrame],
    fxSourceCode: Optional[str] = None,
    fiscalYearStartMonth: int = 1,
) -> Dict[str, int]:
    batchId = int(batchId)
    packageExecutionId = int(run.packageExecutionId)
    loadStart, loadEnd, wmTo = loadDates(spark, catalog, p, regionCode)

    source = readSourceLines(spark, catalog, regionCode, loadStart, loadEnd)
    rowsRead = source.count()
    valid, invalid = structuralRejects(source)
    rejected = fc.rejectRows(spark, catalog, invalid, FACT_OBJECT_NAME, fc.REJECT_FACT_VALIDATION, batchId, packageExecutionId,
                             businessKeyColumn="SaleLineBusinessKey", sourceSystemCode=SOURCE_SYSTEM_CODE)
    valid = fc.dedupByRowVersion(valid, ["natural_key_hash"], "SourceRowVersion", ["LoadedAtUtc"])

    # Held rows from earlier runs (stock item not yet keyed) rejoin the flow.
    held = fc.readHeldRows(spark, catalog, FACT_OBJECT_NAME, valid.schema, regionCode)
    working = valid.withColumn("fact_load_hold_key", F.lit(None).cast("bigint")).withColumn("retry_count", F.lit(0)).withColumn("max_retry_count", F.lit(fc.retryLimitFor(regionCode))).unionByName(held)
    working = fc.coalesceHeldReplays(working)

    working = lookupDimensions(spark, catalog, working)

    # Missing customers -> inferred members (2013-01-01 .. open end) + late-arriving queue.
    missingCustomers = working.where(F.col("customer_key_miss"))
    fc.queueLateArrivers(spark, catalog, missingCustomers, "Customer", "CustomerBusinessKey", packageName, batchId, packageExecutionId, sourceSystemCol="SourceSystemCode")
    inferred = fc.inferMembers(spark, catalog, fc.DIMENSIONS["Customer"], missingCustomers, "CustomerBusinessKey", batchId, packageExecutionId, SOURCE_SYSTEM_CODE, regionCol="RegionCode")
    if inferred > 0:
        lookupOutputs = [c for c in working.columns if c in LOOKUP_OUTPUT_COLUMNS or c.endswith("_miss") or c.endswith("_is_inferred")]
        working = lookupDimensions(spark, catalog, working.drop(*lookupOutputs))

    # Effective FX (latest on/before the invoice date, region-specific source).
    rates = fc.fxRates(spark, catalog)
    if fxSourceCode:
        sourced = rates.where(F.col("RateSourceCode") == fxSourceCode)
        rates = sourced if sourced.limit(1).count() > 0 else rates
    working = fc.lookupEffectiveFxRate(working, rates, "TransactionCurrency", "FxRateDate", rateDateCol="FxRateDateUsed")
    fxMissing = working.where(F.col("FxRateToUsd_miss"))
    if fxMissing.limit(1).count() > 0:
        scratch = fxMissing.select("SaleLineBusinessKey", "TransactionCurrency", "FxRateDate", "RegionCode").withColumn("batch_id", F.lit(batchId)).withColumn("queued_at", F.current_timestamp())
        fc.appendRows(spark, fc.tableName(catalog, "silver", CURRENCY_SCRATCH_TABLE), scratch)

    # Missing stock items / FX are held (DIM_NOT_KEYED / FX_RATE_MISSING) instead of being loaded as unknown.
    holdable = working.where(F.col("fact_load_hold_key").isNull())
    heldStock = fc.holdRows(spark, catalog, holdable.where(F.col("stock_item_key_miss")), FACT_OBJECT_NAME, "Stock Item", "StockItemBusinessKey",
                            fc.HOLD_REASON_DIM_NOT_KEYED, batchId, packageExecutionId, businessDateCol="InvoiceDate",
                            naturalKeyHashCol="natural_key_hash", naturalKeyCols=("InvoiceNumber", "InvoiceLineNumber", "RegionCode"), sourceSystemCode=SOURCE_SYSTEM_CODE)
    heldFx = fc.holdRows(spark, catalog, holdable.where(~F.col("stock_item_key_miss") & F.col("FxRateToUsd_miss")), FACT_OBJECT_NAME, "Currency", "TransactionCurrency",
                         fc.HOLD_REASON_FX_MISSING, batchId, packageExecutionId, businessDateCol="InvoiceDate",
                         naturalKeyHashCol="natural_key_hash", naturalKeyCols=("InvoiceNumber", "InvoiceLineNumber", "RegionCode"), sourceSystemCode=SOURCE_SYSTEM_CODE)
    loadable, stillHeld = fc.splitHeld(working, ["stock_item_key_miss", "FxRateToUsd_miss"])
    loadable = loadable.where(~(F.col("fact_load_hold_key").isNull() & (F.col("stock_item_key_miss") | F.col("FxRateToUsd_miss"))))

    loadable = withMeasures(loadable)
    loadable = regionalTaxRule(spark, catalog, loadable)
    loadable = loadable.withColumn("FxRateToUsd", F.coalesce(F.col("FxRateToUsd"), F.lit(1)))
    loadable = withReportingValues(loadable)

    factRows = toFactRows(loadable, batchId, packageExecutionId, fiscalYearStartMonth)
    merged = mergeWithReversals(spark, catalog, factRows, batchId, packageExecutionId)

    releasedKeys = loadable.where(F.col("fact_load_hold_key").isNotNull()).select("fact_load_hold_key", F.lit(None).cast("bigint").alias("released_fact_key"))
    retriedKeys = stillHeld.where(F.col("fact_load_hold_key").isNotNull()).select("fact_load_hold_key")
    holds = fc.settleHolds(spark, catalog, releasedKeys, retriedKeys, batchId, packageExecutionId)

    fullName = fc.tableName(catalog, "gold", FACT_TABLE)
    fc.logFactRowCounts(spark, catalog, packageExecutionId, FACT_OBJECT_NAME, rowsRead, fullName, merged, rejected)
    control.logRowCount(spark, catalog, packageExecutionId, "Fact.Fact Load Hold", sourceRowCount=heldStock + heldFx, insertRowCount=heldStock + heldFx, updateRowCount=holds["released"] + holds["retried"])
    control.setWatermark(spark, catalog, SOURCE_SYSTEM_CODE, "Fact.Sale.%s" % regionCode, fc.watermarkValue(wmTo), packageExecutionId=packageExecutionId)
    fc.setRunCounts(run, rowsRead, merged, rejected)
    return {"rowsRead": rowsRead, "rejected": rejected, "held": heldStock + heldFx, "inferredCustomers": inferred, **merged, **holds}
