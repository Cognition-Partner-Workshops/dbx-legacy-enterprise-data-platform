"""Silver sales transactions - workstream 4.

Conforms the bronze copies of the OLTP sales tables into the silver transaction
tables, replacing the legacy ``stg.usp_AppendIncremental_*`` /
``stg.usp_Conform*`` procedures, ``stg.usp_DeduplicateOrderLine``,
``work.usp_MatchPaymentsToInvoices`` and ``work.usp_QueueLateArrivingDimensions``
(see the module docstrings of the sibling modules for line references).

Silver tables written (all ``cfg.fqn("silver", ...)``):
  order, order_line, sale, sale_line, payment, payment_allocation, order_hold,
  order_amendment, backorder, quote, quote_line, late_arriving_dimension_queue

Every table carries ``source_system_code``, ``region_code``, ``*_business_key``,
``dq_status_code`` (PASS/WARN), ``row_hash``, ``batch_id``, ``loaded_at_utc``,
``is_deleted`` and ``deleted_batch_id``. Rows are upserted by business key with
a Delta MERGE (unchanged rows are left alone) and rejects go through
``sales_lakehouse.common.quality.quarantine``. No tax or FX arithmetic happens
here - local-currency amounts, ``currency_code``, ``tax_regime_code``,
``is_reverse_charge`` and ``vat_registration_number`` are carried for the
workstream-5 rules engine.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.quality import quarantine
from sales_lakehouse.common.tables import appendBatch, mergeByKey, softDeleteByKey, tableExists
from sales_lakehouse.silver import late_arriving
from sales_lakehouse.silver.business_keys import DEFAULT_SOURCE_SYSTEM, lineBusinessKey, sourceSystemKey
from sales_lakehouse.silver.dedup import (
    DUP_RULE_CODE,
    EXACT_COPY_RANK_COL,
    flagRekeyDuplicates,
    rankExactCopies,
)
from sales_lakehouse.silver.fulfilment_flags import FLAG_COLUMNS, parseFulfilmentFlags
from sales_lakehouse.silver.payment_matching import (
    ALLOCATION_COLUMNS,
    matchPaymentsToInvoices,
    summarisePayments,
)

MONEY = DecimalType(19, 4)
RATE = DecimalType(19, 8)
QTY = DecimalType(18, 4)

DEFAULT_REGION = "NA"
DEFAULT_CURRENCY = "USD"
UNKNOWN_SALESPERSON_ID = -1
CUSTOMER_PAYMENT_TRANSACTION_TYPE_ID = 3  # Application.TransactionTypes 'Customer Payment Received'

REVERSE_CHARGE_REGIME_CODES = ("RC", "VAT_RC", "REVERSE_CHARGE", "EU_RC")

AUDIT_COLUMNS = ("row_hash", "batch_id", "loaded_at_utc", "is_deleted", "deleted_batch_id")

# bronze table -> (silver table, business key column, soft-delete key column)
DELETION_LOG_TARGETS: dict[tuple[str, str], tuple[str, str]] = {
    ("SALES", "ORDERS"): ("order", "source_order_id"),
    ("SALES", "ORDERLINES"): ("order_line", "source_order_line_id"),
    ("SALES", "INVOICES"): ("sale", "source_invoice_id"),
    ("SALES", "INVOICELINES"): ("sale_line", "source_invoice_line_id"),
    ("SALES", "CUSTOMERTRANSACTIONS"): ("payment", "source_payment_id"),
    ("SALES", "CUSTOMERPAYMENTS"): ("payment", "source_payment_id"),
    ("SALES", "ORDERHOLDS"): ("order_hold", "source_order_hold_id"),
    ("SALES", "ORDERAMENDMENTS"): ("order_amendment", "source_order_amendment_id"),
    ("SALES", "BACKORDERS"): ("backorder", "source_backorder_id"),
    ("SALES", "QUOTEHEADERS"): ("quote", "source_quote_id"),
    ("SALES", "QUOTELINES"): ("quote_line", "source_quote_line_id"),
}


# --------------------------------------------------------------------------- #
# column helpers
# --------------------------------------------------------------------------- #
def optCol(df: DataFrame, name: str, dtype: str = "string") -> Column:
    """``F.col(name)`` when the bronze copy carries the column, typed NULL otherwise.

    The OLTP extension columns are added by ``sqlserver/oltp/02_extensions``;
    an older extract may not carry all of them.
    """
    lowered = {c.lower(): c for c in df.columns}
    if name.lower() in lowered:
        return F.col(f"`{lowered[name.lower()]}`").cast(dtype)
    return F.lit(None).cast(dtype)


def sourceSystemCol(df: DataFrame) -> Column:
    return F.coalesce(optCol(df, "_source_system"), F.lit(DEFAULT_SOURCE_SYSTEM))


def cleanText(col: Column, maxLen: int | None = None) -> Column:
    """``stg.ufn_CleanString``: trim, collapse line breaks to spaces, blank -> NULL."""
    cleaned = F.nullif(F.trim(F.regexp_replace(col, r"[\r\n\t]+", " ")), F.lit(""))
    return F.substring(cleaned, 1, maxLen) if maxLen else cleaned


def upperCode(col: Column) -> Column:
    return F.nullif(F.upper(F.trim(col.cast("string"))), F.lit(""))


def taxRegimeForRegion(regionCol: Column) -> Column:
    # LEGACY QUIRK: ssis/04_staging "Tag Sale Region": EU -> VAT, APAC -> GST, else SUT.
    return F.when(regionCol == "EU", F.lit("VAT")).when(regionCol == "APAC", F.lit("GST")).otherwise(F.lit("SUT"))


def taxTreatmentForRegion(regionCol: Column) -> Column:
    # LEGACY QUIRK: EXT_SQL_Invoices (generate_sqlserver_extracts.py) codes the
    # same three regimes as SALESTAX / VAT / GST and everything else NONE.
    return (
        F.when(regionCol == "NA", F.lit("SALESTAX"))
        .when(regionCol == "EU", F.lit("VAT"))
        .when(regionCol == "APAC", F.lit("GST"))
        .otherwise(F.lit("NONE"))
    )


def vatRegistrationForRegion(regionCol: Column, *candidates: Column) -> Column:
    # LEGACY QUIRK: the invoice extract only carries the registration number for
    # EU rows (CASE WHEN RegionCode = 'EU' THEN CustomerTaxRegistrationNumber ELSE NULL).
    return F.when(regionCol == "EU", F.coalesce(*[cleanText(c, 30) for c in candidates])).otherwise(
        F.lit(None).cast("string")
    )


def reverseChargeFlag(regionCol: Column, vatRegistrationCol: Column, taxRegimeCol: Column) -> Column:
    """EU, VAT-registered counter-party and a reverse-charge regime code.

    The legacy sales flow never derived this (only the AP side had
    ``ref.TaxJurisdiction.ReverseChargeEligible``); this is the documented
    default, listed under open questions in the PR.
    """
    return (
        (regionCol == "EU")
        & vatRegistrationCol.isNotNull()
        & F.upper(F.coalesce(taxRegimeCol, F.lit(""))).isin(*REVERSE_CHARGE_REGIME_CODES)
    )


def rowHash(df: DataFrame, exclude: tuple[str, ...] = AUDIT_COLUMNS) -> Column:
    cols = [F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("")) for c in df.columns if c not in exclude]
    return F.sha2(F.concat_ws("\u001f", *cols), 256)


def withAudit(df: DataFrame, cfg: PipelineConfig) -> DataFrame:
    return (
        df.withColumn("row_hash", rowHash(df))
        .withColumn("batch_id", F.lit(cfg.batchId).cast("bigint"))
        .withColumn("loaded_at_utc", F.current_timestamp())
        .withColumn("is_deleted", F.lit(False))
        .withColumn("deleted_batch_id", F.lit(None).cast("bigint"))
    )


def dqStatus(*warnConditions: Column) -> Column:
    warn = F.lit(False)
    for c in warnConditions:
        warn = warn | F.coalesce(c, F.lit(False))
    return F.when(warn, F.lit("WARN")).otherwise(F.lit("PASS"))


# --------------------------------------------------------------------------- #
# bronze access
# --------------------------------------------------------------------------- #
@dataclass
class BronzeReader:
    spark: SparkSession
    cfg: PipelineConfig

    def read(self, table: str) -> DataFrame:
        return self.spark.table(self.cfg.fqn("bronze", table))

    def readOptional(self, *tables: str) -> DataFrame | None:
        for table in tables:
            fqn = self.cfg.fqn("bronze", table)
            if tableExists(self.spark, fqn):
                return self.spark.table(fqn)
        return None


@dataclass
class DimensionKeys:
    """Business keys present in the bronze dimension sources (for late-arriving checks)."""

    customer: DataFrame
    stockItem: DataFrame
    channel: DataFrame
    salesperson: DataFrame | None  # None when Application.People is not in bronze

    def available(self, spark: SparkSession) -> DataFrame:
        parts = [
            self.customer.select(F.lit("Customer").alias("entity_type"), "business_key"),
            self.stockItem.select(F.lit("StockItem").alias("entity_type"), "business_key"),
            self.channel.select(F.lit("SalesChannel").alias("entity_type"), "business_key"),
        ]
        if self.salesperson is not None:
            parts.append(self.salesperson.select(F.lit("Salesperson").alias("entity_type"), "business_key"))
        out = parts[0]
        for p in parts[1:]:
            out = out.unionByName(p)
        return out


def _keys(df: DataFrame, idCol: str) -> DataFrame:
    return df.select(sourceSystemKey(sourceSystemCol(df), optCol(df, idCol)).alias("business_key")).dropna().distinct()


def loadDimensionKeys(bronze: BronzeReader) -> DimensionKeys:
    people = bronze.readOptional("sqlserver_application_people")
    return DimensionKeys(
        customer=_keys(bronze.read("sqlserver_sales_customers"), "CustomerID"),
        stockItem=_keys(bronze.read("sqlserver_warehouse_stock_items"), "StockItemID"),
        channel=_keys(bronze.read("sqlserver_sales_sales_channels"), "SalesChannelID"),
        salesperson=None if people is None else _keys(people, "PersonID"),
    )


def customerContext(customers: DataFrame, territories: DataFrame) -> DataFrame:
    """Per-customer region / currency / tax attributes for fallbacks
    (Sales.Customers.Extensions + Sales.SalesTerritories)."""
    terr = territoryContext(territories)
    cust = customers.select(
        sourceSystemKey(sourceSystemCol(customers), optCol(customers, "CustomerID")).alias("_cust_key"),
        upperCode(optCol(customers, "RegionCode")).alias("_cust_region"),
        sourceSystemKey(sourceSystemCol(customers), optCol(customers, "SalesTerritoryID")).alias("_cust_terr_key"),
        cleanText(optCol(customers, "TaxRegistrationNumber"), 30).alias("_cust_vat_number"),
        optCol(customers, "DefaultPriceListID", "int").alias("_cust_price_list_id"),
    )
    return cust.join(
        terr.select(
            F.col("_terr_key").alias("_cust_terr_key"),
            F.col("_terr_region").alias("_cust_terr_region"),
            F.col("_terr_currency").alias("_cust_terr_currency"),
            F.col("_terr_tax_regime").alias("_cust_terr_tax_regime"),
            F.col("_terr_code").alias("_cust_terr_code"),
        ),
        "_cust_terr_key",
        "left",
    )


def territoryContext(territories: DataFrame) -> DataFrame:
    return territories.select(
        sourceSystemKey(sourceSystemCol(territories), optCol(territories, "SalesTerritoryID")).alias("_terr_key"),
        upperCode(optCol(territories, "TerritoryCode")).alias("_terr_code"),
        upperCode(optCol(territories, "RegionCode")).alias("_terr_region"),
        upperCode(optCol(territories, "ReportingCurrencyCode")).alias("_terr_currency"),
        upperCode(optCol(territories, "TaxRegimeCode")).alias("_terr_tax_regime"),
    )


def channelContext(channels: DataFrame) -> DataFrame:
    return channels.select(
        sourceSystemKey(sourceSystemCol(channels), optCol(channels, "SalesChannelID")).alias("_chan_key"),
        upperCode(optCol(channels, "ChannelCode")).alias("_chan_code"),
    )


def stockItemContext(stockItems: DataFrame) -> DataFrame:
    return stockItems.select(
        sourceSystemKey(sourceSystemCol(stockItems), optCol(stockItems, "StockItemID")).alias("_stock_key"),
        cleanText(optCol(stockItems, "StockItemName"), 200).alias("_stock_name"),
    )


# --------------------------------------------------------------------------- #
# orders
# --------------------------------------------------------------------------- #
def conformOrders(
    spark: SparkSession,
    cfg: PipelineConfig,
    orders: DataFrame,
    customers: DataFrame,
    territories: DataFrame,
    channels: DataFrame,
    holds: DataFrame | None,
    dims: DimensionKeys,
) -> DataFrame:
    src = sourceSystemCol(orders)
    orderKey = sourceSystemKey(src, optCol(orders, "OrderID"))
    base = orders.select(
        orderKey.alias("order_business_key"),
        src.alias("source_system_code"),
        F.trim(optCol(orders, "OrderID")).alias("source_order_id"),
        sourceSystemKey(src, optCol(orders, "CustomerID")).alias("customer_business_key"),
        # LEGACY QUIRK: ssis/04_staging "Conform Order Header" defaults a missing
        # salesperson to -1 (the unknown member) instead of NULL.
        sourceSystemKey(
            src, F.coalesce(optCol(orders, "SalespersonPersonID", "int"), F.lit(UNKNOWN_SALESPERSON_ID))
        ).alias("salesperson_business_key"),
        sourceSystemKey(src, optCol(orders, "ContactPersonID")).alias("contact_person_key"),
        sourceSystemKey(src, optCol(orders, "PickedByPersonID")).alias("picked_by_person_key"),
        sourceSystemKey(src, optCol(orders, "BackorderOrderID")).alias("backorder_order_business_key"),
        sourceSystemKey(src, optCol(orders, "SalesChannelID")).alias("sales_channel_business_key"),
        sourceSystemKey(src, optCol(orders, "SalesTerritoryID")).alias("sales_territory_business_key"),
        sourceSystemKey(src, optCol(orders, "SourceQuoteID")).alias("source_quote_business_key"),
        optCol(orders, "PriceListID", "int").alias("price_list_id"),
        optCol(orders, "OrderDate", "date").alias("order_date"),
        # LEGACY QUIRK: missing ExpectedDeliveryDate defaults to OrderDate + 3
        # days (ssis/04_staging "Conform Order Header").
        F.coalesce(optCol(orders, "ExpectedDeliveryDate", "date"), F.date_add(optCol(orders, "OrderDate", "date"), 3)).alias(
            "expected_delivery_date"
        ),
        # LEGACY QUIRK: PO number is upper-cased/trimmed and NULL becomes '' (same package).
        F.coalesce(F.upper(F.trim(optCol(orders, "CustomerPurchaseOrderNumber"))), F.lit("")).alias(
            "customer_purchase_order_number"
        ),
        F.coalesce(optCol(orders, "IsUndersupplyBackordered", "boolean"), F.lit(False)).alias("is_undersupply_backordered"),
        F.coalesce(upperCode(optCol(orders, "OrderStatusCode")), F.lit("UNKNOWN")).alias("order_status_code"),
        optCol(orders, "FulfilmentFlags").alias("FulfilmentFlags"),
        upperCode(optCol(orders, "CurrencyCode")).alias("_order_currency"),
        optCol(orders, "ExchangeRateToUsd", "decimal(19,8)").alias("exchange_rate_to_usd"),
        upperCode(optCol(orders, "TaxRegimeCode")).alias("_order_tax_regime"),
        optCol(orders, "IsTaxInclusivePricing", "boolean").alias("is_tax_inclusive_pricing"),
        optCol(orders, "OrderValueExTax", "decimal(19,4)").alias("order_value_ex_tax_local"),
        optCol(orders, "TotalDiscountAmount", "decimal(19,4)").alias("total_discount_amount_local"),
        F.coalesce(optCol(orders, "AmendmentCount", "int"), F.lit(0)).alias("amendment_count"),
        optCol(orders, "CreditHoldAppliedWhen", "timestamp").alias("credit_hold_applied_when_utc"),
        optCol(orders, "WebCartID").alias("web_cart_id"),
        optCol(orders, "PickingCompletedWhen", "timestamp").alias("picking_completed_when_utc"),
        cleanText(optCol(orders, "DeliveryInstructions"), 500).alias("delivery_instructions_text"),
        cleanText(optCol(orders, "Comments"), 1000).alias("order_comments_text"),
        optCol(orders, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
        F.coalesce(optCol(orders, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
    )

    # latest bronze copy per order wins (re-extraction of the same header)
    base = rankExactCopies(base, "order_business_key").filter(F.col(EXACT_COPY_RANK_COL) == 1).drop(EXACT_COPY_RANK_COL)

    flagged = parseFulfilmentFlags(base, "FulfilmentFlags").drop("FulfilmentFlags")

    terr = territoryContext(territories)
    cust = customerContext(customers, territories)
    chan = channelContext(channels)
    joined = (
        flagged.join(terr, flagged.sales_territory_business_key == terr._terr_key, "left")
        .join(cust, flagged.customer_business_key == cust._cust_key, "left")
        .join(chan, flagged.sales_channel_business_key == chan._chan_key, "left")
    )
    # LEGACY QUIRK: region falls back territory -> customer -> customer territory
    # -> 'NA' and currency -> 'USD' (ssis/04_staging "Tag Sale Region"); the
    # fallback taken is tagged so the row is never silently defaulted.
    regionSource = (
        F.when(F.col("_terr_region").isNotNull(), F.lit("ORDER_TERRITORY"))
        .when(F.col("_cust_region").isNotNull(), F.lit("CUSTOMER"))
        .when(F.col("_cust_terr_region").isNotNull(), F.lit("CUSTOMER_TERRITORY"))
        .otherwise(F.lit("DEFAULT_NA"))
    )
    region = F.coalesce(F.col("_terr_region"), F.col("_cust_region"), F.col("_cust_terr_region"), F.lit(DEFAULT_REGION))
    currencySource = (
        F.when(F.col("_order_currency").isNotNull(), F.lit("ORDER"))
        .when(F.col("_terr_currency").isNotNull(), F.lit("ORDER_TERRITORY"))
        .when(F.col("_cust_terr_currency").isNotNull(), F.lit("CUSTOMER_TERRITORY"))
        .otherwise(F.lit("DEFAULT_USD"))
    )
    currency = F.coalesce(F.col("_order_currency"), F.col("_terr_currency"), F.col("_cust_terr_currency"), F.lit(DEFAULT_CURRENCY))
    taxRegime = F.coalesce(F.col("_order_tax_regime"), F.col("_terr_tax_regime"), F.col("_cust_terr_tax_regime"), taxRegimeForRegion(region))
    vatNumber = vatRegistrationForRegion(region, F.col("_cust_vat_number"))

    withRegion = (
        joined.withColumn("region_code", region)
        .withColumn("region_source_code", regionSource)
        .withColumn("currency_code", currency)
        .withColumn("currency_source_code", currencySource)
        .withColumn("tax_regime_code", taxRegime)
        .withColumn("vat_registration_number", vatNumber)
        .withColumn("is_reverse_charge", reverseChargeFlag(region, vatNumber, taxRegime))
        .withColumn("sales_territory_code", F.coalesce(F.col("_terr_code"), F.col("_cust_terr_code")))
        .withColumn("sales_channel_code", F.col("_chan_code"))
        .withColumn("price_list_id", F.coalesce(F.col("price_list_id"), F.col("_cust_price_list_id")))
        .drop(*[c for c in joined.columns if c.startswith("_terr_") or c.startswith("_cust_") or c.startswith("_chan_") or c.startswith("_order_")])
    )

    # Sales.vw_OrderLineExtract: HasOpenHold = open Sales.OrderHolds (ReleasedWhen IS NULL)
    if holds is not None:
        openHolds = (
            holds.filter(optCol(holds, "ReleasedWhen", "timestamp").isNull())
            .groupBy(sourceSystemKey(sourceSystemCol(holds), optCol(holds, "OrderID")).alias("_hold_order_key"))
            .agg(
                F.count(F.lit(1)).cast("int").alias("open_hold_count"),
                F.max(F.coalesce(optCol(holds, "IsBlockingDespatch", "boolean"), F.lit(False))).alias("has_despatch_blocking_hold"),
            )
        )
        withRegion = withRegion.join(openHolds, withRegion.order_business_key == openHolds._hold_order_key, "left").drop("_hold_order_key")
    else:
        withRegion = withRegion.withColumn("open_hold_count", F.lit(None).cast("int")).withColumn(
            "has_despatch_blocking_hold", F.lit(None).cast("boolean")
        )
    withRegion = withRegion.withColumn("open_hold_count", F.coalesce(F.col("open_hold_count"), F.lit(0))).withColumn(
        "has_open_hold", F.col("open_hold_count") > 0
    )

    # late-arriving dimension references (work.usp_QueueLateArrivingDimensions intent)
    withRegion = late_arriving.flagMissingReferences(withRegion, "customer_business_key", dims.customer, "is_late_arriving_customer")
    withRegion = late_arriving.flagMissingReferences(withRegion, "sales_channel_business_key", dims.channel, "is_late_arriving_channel")
    if dims.salesperson is not None:
        withRegion = late_arriving.flagMissingReferences(
            withRegion, "salesperson_business_key", dims.salesperson, "is_late_arriving_salesperson"
        )
    else:
        withRegion = withRegion.withColumn("is_late_arriving_salesperson", F.lit(False))

    # hard rejects (stg.usp_AppendIncremental_Order: no key / no customer / no date)
    passed = quarantine(
        spark, cfg, withRegion, "ORDER_MISSING_KEY", "sqlserver_sales_orders",
        F.col("order_business_key").isNull() | F.col("customer_business_key").isNull() | F.col("order_date").isNull(),
        "OrderID, CustomerID or OrderDate is missing; the header cannot be keyed",
    )

    out = passed.withColumn(
        "dq_status_code",
        dqStatus(
            F.col("fulfilment_flags_malformed"),
            F.col("is_late_arriving_customer"),
            F.col("is_late_arriving_channel"),
            F.col("is_late_arriving_salesperson"),
        ),
    )
    return withAudit(out, cfg)


# --------------------------------------------------------------------------- #
# order lines
# --------------------------------------------------------------------------- #
def conformOrderLines(
    spark: SparkSession,
    cfg: PipelineConfig,
    orderLines: DataFrame,
    orders: DataFrame,
    stockItems: DataFrame,
    dims: DimensionKeys,
) -> DataFrame:
    src = sourceSystemCol(orderLines)
    orderKey = sourceSystemKey(src, optCol(orderLines, "OrderID"))
    base = orderLines.select(
        # stg.usp_AppendIncremental_OrderLine lines 62-64
        lineBusinessKey(orderKey, optCol(orderLines, "OrderLineID")).alias("order_line_business_key"),
        orderKey.alias("order_business_key"),
        src.alias("source_system_code"),
        F.trim(optCol(orderLines, "OrderLineID")).alias("source_order_line_id"),
        optCol(orderLines, "OrderLineID", "int").alias("line_number"),
        sourceSystemKey(src, optCol(orderLines, "StockItemID")).alias("stock_item_business_key"),
        cleanText(optCol(orderLines, "Description"), 200).alias("line_description"),
        # LEGACY QUIRK: PackageTypeID is carried as the package *code* and NULL
        # becomes 'EACH' (proc line 66, ssis "Extend Order Line").
        F.coalesce(upperCode(optCol(orderLines, "PackageTypeID")), F.lit("EACH")).alias("package_type_code"),
        optCol(orderLines, "Quantity", "decimal(18,4)").alias("ordered_quantity"),
        optCol(orderLines, "PickedQuantity", "decimal(18,4)").alias("picked_quantity"),
        optCol(orderLines, "QuantityAllocated", "decimal(18,4)").alias("quantity_allocated"),
        optCol(orderLines, "QuantityShipped", "decimal(18,4)").alias("quantity_shipped"),
        optCol(orderLines, "QuantityBackordered", "decimal(18,4)").alias("quantity_backordered"),
        optCol(orderLines, "UnitPrice", "decimal(19,4)").alias("unit_price_amount_local"),
        optCol(orderLines, "ListUnitPrice", "decimal(19,4)").alias("list_unit_price_local"),
        # proc line 173: ISNULL(LineDiscountAmount, 0)
        F.coalesce(optCol(orderLines, "DiscountAmount", "decimal(19,4)"), F.lit(0)).cast(MONEY).alias("line_discount_amount_local"),
        optCol(orderLines, "DiscountPercent", "decimal(9,4)").alias("line_discount_percent"),
        optCol(orderLines, "LineNetAmount", "decimal(19,4)").alias("_source_line_net"),
        optCol(orderLines, "TaxRate", "decimal(9,4)").alias("tax_rate_percent"),
        optCol(orderLines, "PriceListLineID", "bigint").alias("price_list_line_id"),
        sourceSystemKey(src, optCol(orderLines, "PromotionID")).alias("promotion_business_key"),
        optCol(orderLines, "PickingCompletedWhen", "timestamp").alias("picking_completed_when_utc"),
        F.coalesce(upperCode(optCol(orderLines, "LineStatusCode")), F.lit("UNKNOWN")).alias("line_status_code"),
        optCol(orderLines, "RequestedDeliveryDate", "date").alias("requested_delivery_date"),
        cleanText(optCol(orderLines, "SourceLineReference"), 50).alias("source_line_reference"),
        optCol(orderLines, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
        F.coalesce(optCol(orderLines, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
    )

    # LEGACY QUIRK: usp_DeduplicateOrderLine lines 375-391 - exact re-extraction
    # copies: only the latest staged copy survives. Earlier copies are quarantined
    # rather than deleted so nothing is silently dropped.
    ranked = rankExactCopies(base, "order_line_business_key")
    survivors = quarantine(
        spark, cfg, ranked, DUP_RULE_CODE, "sqlserver_sales_order_lines",
        F.col(EXACT_COPY_RANK_COL) > 1,
        "exact re-extraction copy of an order line already staged; latest copy kept (usp_DeduplicateOrderLine step 1)",
    ).drop(EXACT_COPY_RANK_COL)

    # header context (status for derived line status, region/currency/tax carried down)
    hdr = orders.select(
        F.col("order_business_key").alias("_hdr_key"),
        F.col("order_status_code").alias("_hdr_status"),
        F.col("order_date").alias("_hdr_order_date"),
        F.col("region_code").alias("_hdr_region"),
        F.col("currency_code").alias("_hdr_currency"),
        F.col("tax_regime_code").alias("_hdr_tax_regime"),
        F.col("is_reverse_charge").alias("_hdr_reverse_charge"),
        F.col("vat_registration_number").alias("_hdr_vat"),
        F.col("customer_business_key").alias("_hdr_customer"),
    )
    joined = survivors.join(hdr, survivors.order_business_key == hdr._hdr_key, "left")

    # stg.usp_AppendIncremental_OrderLine lines 100-140: BAD_NUMERIC / NEG_QTY / ORPHAN_HEADER
    joined = quarantine(
        spark, cfg, joined, "OL_BAD_NUMERIC", "sqlserver_sales_order_lines",
        F.col("ordered_quantity").isNull() | F.col("unit_price_amount_local").isNull(),
        "Quantity or UnitPrice will not convert to a decimal",
    )
    joined = quarantine(
        spark, cfg, joined, "OL_NEG_QTY", "sqlserver_sales_order_lines",
        F.col("ordered_quantity") < 0,
        "Quantity is negative; returns belong on stg.Return, not on the order",
    )
    joined = quarantine(
        spark, cfg, joined, "OL_ORPHAN_HEADER", "sqlserver_sales_order_lines",
        F.col("_hdr_key").isNull(),
        "no matching order header in silver.order for this line",
    )

    stock = stockItemContext(stockItems)
    joined = joined.join(stock, joined.stock_item_business_key == stock._stock_key, "left")

    netLine = F.coalesce(
        F.col("_source_line_net"),
        (F.col("ordered_quantity") * F.col("unit_price_amount_local") - F.col("line_discount_amount_local")).cast(MONEY),
    )
    # Sales.vw_OrderLineExtract DerivedLineStatus precedence
    derivedStatus = (
        F.when(F.col("_hdr_status") == "CANCELLED", F.lit("CANCELLED"))
        .when(F.col("line_status_code") == "CANCELLED", F.lit("CANCELLED"))
        .when(F.col("quantity_shipped") >= F.col("ordered_quantity"), F.lit("SHIPPED"))
        .when(F.coalesce(F.col("quantity_backordered"), F.lit(0)) > 0, F.lit("BACKORDER"))
        .when(F.col("picking_completed_when_utc").isNotNull(), F.lit("PICKED"))
        .when(F.coalesce(F.col("quantity_allocated"), F.lit(0)) > 0, F.lit("ALLOCATED"))
        .otherwise(F.lit("OPEN"))
    )
    enriched = (
        joined.withColumn("net_line_amount_local", netLine.cast(MONEY))
        .withColumn("derived_line_status_code", derivedStatus)
        # ssis "Extend Order Line": PickedFlag = ISNULL(PickingCompletedWhen) ? "N" : "Y"
        .withColumn("is_picked", F.col("picking_completed_when_utc").isNotNull())
        .withColumn("outstanding_quantity", (F.col("ordered_quantity") - F.coalesce(F.col("picked_quantity"), F.lit(0))).cast(QTY))
        .withColumn("line_description", F.coalesce(F.col("line_description"), F.col("_stock_name")))
        .withColumn("order_date", F.col("_hdr_order_date"))
        .withColumn("region_code", F.col("_hdr_region"))
        .withColumn("currency_code", F.col("_hdr_currency"))
        .withColumn("tax_regime_code", F.col("_hdr_tax_regime"))
        .withColumn("is_reverse_charge", F.col("_hdr_reverse_charge"))
        .withColumn("vat_registration_number", F.col("_hdr_vat"))
        .withColumn("customer_business_key", F.col("_hdr_customer"))
        .drop("_source_line_net", "_stock_key", "_stock_name", *[c for c in joined.columns if c.startswith("_hdr_")])
    )

    enriched = late_arriving.flagMissingReferences(enriched, "stock_item_business_key", dims.stockItem, "is_late_arriving_stock_item")

    # usp_DeduplicateOrderLine lines 393-467: genuine re-keys stay as WARN rows
    # and the losers are also copied to the reject table (legacy err.RejectedOrderLine
    # RejectReasonCode 'DUPLICATE_LINE').
    deduped = flagRekeyDuplicates(enriched)
    quarantine(
        spark, cfg, deduped.filter(F.col("is_duplicate_loser")), DUP_RULE_CODE, "sqlserver_sales_order_lines",
        F.lit(True),
        "duplicate order line (same order, stock item, quantity, unit price); higher LineNumber wins (usp_DeduplicateOrderLine step 2)",
    )

    out = deduped.withColumn(
        "dq_status_code",
        dqStatus(
            F.col("is_late_arriving_stock_item"),
            F.col("is_duplicate_loser"),
            # proc line 194: a missing stock item key is a WARN, not a reject
            F.col("stock_item_business_key").isNull(),
        ),
    )
    return withAudit(out, cfg)


# --------------------------------------------------------------------------- #
# invoices (silver.sale) and invoice lines (silver.sale_line)
# --------------------------------------------------------------------------- #
def conformSales(
    spark: SparkSession,
    cfg: PipelineConfig,
    invoices: DataFrame,
    invoiceLines: DataFrame,
    customers: DataFrame,
    territories: DataFrame,
    dims: DimensionKeys,
) -> DataFrame:
    src = sourceSystemCol(invoices)
    base = invoices.select(
        sourceSystemKey(src, optCol(invoices, "InvoiceID")).alias("sale_business_key"),
        src.alias("source_system_code"),
        F.trim(optCol(invoices, "InvoiceID")).alias("source_invoice_id"),
        sourceSystemKey(src, optCol(invoices, "OrderID")).alias("order_business_key"),
        sourceSystemKey(src, optCol(invoices, "CustomerID")).alias("customer_business_key"),
        sourceSystemKey(src, F.coalesce(optCol(invoices, "BillToCustomerID"), optCol(invoices, "CustomerID"))).alias(
            "bill_to_customer_business_key"
        ),
        sourceSystemKey(
            src, F.coalesce(optCol(invoices, "SalespersonPersonID", "int"), F.lit(UNKNOWN_SALESPERSON_ID))
        ).alias("salesperson_business_key"),
        sourceSystemKey(src, optCol(invoices, "ContactPersonID")).alias("contact_person_key"),
        sourceSystemKey(src, optCol(invoices, "SalesTerritoryID")).alias("sales_territory_business_key"),
        # LEGACY QUIRK: ssis "Conform Invoice Header" defaults the delivery method to UNKNOWN.
        F.coalesce(upperCode(optCol(invoices, "DeliveryMethodID")), F.lit("UNKNOWN")).alias("delivery_method_code"),
        optCol(invoices, "InvoiceDate", "date").alias("invoice_date"),
        F.coalesce(F.upper(F.trim(optCol(invoices, "CustomerPurchaseOrderNumber"))), F.lit("")).alias(
            "customer_purchase_order_number"
        ),
        F.coalesce(optCol(invoices, "IsCreditNote", "boolean"), F.lit(False)).alias("is_credit_note"),
        cleanText(optCol(invoices, "CreditNoteReason"), 200).alias("credit_note_reason_text"),
        cleanText(optCol(invoices, "DeliveryRun"), 5).alias("delivery_run_code"),
        cleanText(optCol(invoices, "RunPosition"), 5).alias("run_position"),
        optCol(invoices, "TotalDryItems", "int").alias("total_dry_items"),
        optCol(invoices, "TotalChillerItems", "int").alias("total_chiller_items"),
        upperCode(optCol(invoices, "CurrencyCode")).alias("_inv_currency"),
        optCol(invoices, "ExchangeRateToUsd", "decimal(19,8)").alias("exchange_rate_to_usd"),
        upperCode(optCol(invoices, "TaxRegimeCode")).alias("_inv_tax_regime"),
        cleanText(optCol(invoices, "CustomerTaxNumber"), 30).alias("_inv_vat"),
        optCol(invoices, "TaxPointDate", "date").alias("tax_point_date"),
        optCol(invoices, "InvoiceTotalExTax", "decimal(19,4)").alias("_inv_net"),
        optCol(invoices, "InvoiceTaxAmount", "decimal(19,4)").alias("_inv_tax"),
        # LEGACY QUIRK: Sales.Invoices.Extensions documents AmountOutstanding as
        # the OLTP's own running balance, known to drift from the allocation
        # ledger; it is carried, the matcher works from gross - allocations.
        optCol(invoices, "AmountOutstanding", "decimal(19,4)").alias("source_amount_outstanding_local"),
        upperCode(optCol(invoices, "SettlementStatus")).alias("settlement_status_code"),
        optCol(invoices, "PaymentDueDate", "date").alias("_inv_due"),
        F.coalesce(optCol(invoices, "DisputeFlag", "boolean"), F.lit(False)).alias("is_disputed"),
        optCol(invoices, "LoyaltyMemberID", "bigint").alias("loyalty_member_id"),
        optCol(invoices, "LoyaltyPointsAccrued", "int").alias("loyalty_points_accrued"),
        optCol(invoices, "ConfirmedDeliveryTime", "timestamp").alias("confirmed_delivery_utc"),
        cleanText(optCol(invoices, "ConfirmedReceivedBy"), 4000).alias("confirmed_received_by_name"),
        optCol(invoices, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
        F.coalesce(optCol(invoices, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
    )
    base = rankExactCopies(base, "sale_business_key").filter(F.col(EXACT_COPY_RANK_COL) == 1).drop(EXACT_COPY_RANK_COL)

    # line totals as the fallback when the header extension amounts are absent
    lineSrc = sourceSystemCol(invoiceLines)
    lineTotals = invoiceLines.groupBy(sourceSystemKey(lineSrc, optCol(invoiceLines, "InvoiceID")).alias("_lt_key")).agg(
        F.sum(optCol(invoiceLines, "ExtendedPrice", "decimal(19,4)")).cast(MONEY).alias("_lt_gross"),
        F.sum(optCol(invoiceLines, "TaxAmount", "decimal(19,4)")).cast(MONEY).alias("_lt_tax"),
        F.count(F.lit(1)).cast("int").alias("line_count"),
    )

    terr = territoryContext(territories)
    cust = customerContext(customers, territories)
    joined = (
        base.join(terr, base.sales_territory_business_key == terr._terr_key, "left")
        .join(cust, base.bill_to_customer_business_key == cust._cust_key, "left")
        .join(lineTotals, base.sale_business_key == lineTotals._lt_key, "left")
    )
    # LEGACY QUIRK: ssis "Conform Invoice Header" - region from the bill-to
    # geography defaulting to NA, currency defaulting to USD.
    region = F.coalesce(F.col("_terr_region"), F.col("_cust_region"), F.col("_cust_terr_region"), F.lit(DEFAULT_REGION))
    regionSource = (
        F.when(F.col("_terr_region").isNotNull(), F.lit("INVOICE_TERRITORY"))
        .when(F.col("_cust_region").isNotNull(), F.lit("BILL_TO_CUSTOMER"))
        .when(F.col("_cust_terr_region").isNotNull(), F.lit("CUSTOMER_TERRITORY"))
        .otherwise(F.lit("DEFAULT_NA"))
    )
    currency = F.coalesce(F.col("_inv_currency"), F.col("_terr_currency"), F.col("_cust_terr_currency"), F.lit(DEFAULT_CURRENCY))
    currencySource = (
        F.when(F.col("_inv_currency").isNotNull(), F.lit("INVOICE"))
        .when(F.col("_terr_currency").isNotNull(), F.lit("INVOICE_TERRITORY"))
        .when(F.col("_cust_terr_currency").isNotNull(), F.lit("CUSTOMER_TERRITORY"))
        .otherwise(F.lit("DEFAULT_USD"))
    )
    taxRegime = F.coalesce(F.col("_inv_tax_regime"), F.col("_terr_tax_regime"), F.col("_cust_terr_tax_regime"), taxRegimeForRegion(region))
    vatNumber = vatRegistrationForRegion(region, F.col("_inv_vat"), F.col("_cust_vat_number"))
    net = F.coalesce(F.col("_inv_net"), (F.col("_lt_gross") - F.coalesce(F.col("_lt_tax"), F.lit(0))).cast(MONEY))
    tax = F.coalesce(F.col("_inv_tax"), F.col("_lt_tax"))
    gross = F.coalesce((F.col("_inv_net") + F.col("_inv_tax")).cast(MONEY), F.col("_lt_gross"), F.col("_inv_net"))
    # LEGACY QUIRK: stg.usp_ConformCustomerTransactionForFact lines 130-134 -
    # missing due date uses regional terms: EU month-end + 30, APAC + 60, else + 30.
    dueDate = F.coalesce(
        F.col("_inv_due"),
        F.when(region == "EU", F.date_add(F.last_day(F.col("invoice_date")), 30))
        .when(region == "APAC", F.date_add(F.col("invoice_date"), 60))
        .otherwise(F.date_add(F.col("invoice_date"), 30)),
    )

    enriched = (
        joined.withColumn("region_code", region)
        .withColumn("region_source_code", regionSource)
        .withColumn("currency_code", currency)
        .withColumn("currency_source_code", currencySource)
        .withColumn("tax_regime_code", taxRegime)
        .withColumn("tax_treatment_code", taxTreatmentForRegion(region))
        .withColumn("vat_registration_number", vatNumber)
        .withColumn("sale_net_amount_local", net.cast(MONEY))
        .withColumn("sale_tax_amount_local", tax.cast(MONEY))
        .withColumn("sale_gross_amount_local", gross.cast(MONEY))
        .withColumn("payment_due_date", dueDate)
        .withColumn("sales_territory_code", F.coalesce(F.col("_terr_code"), F.col("_cust_terr_code")))
        .withColumn("line_count", F.coalesce(F.col("line_count"), F.lit(0)))
        .withColumn(
            "is_on_hold",
            F.col("is_disputed") | F.coalesce(F.col("settlement_status_code"), F.lit("")).isin("DISPUTED", "ON_HOLD", "HOLD"),
        )
    )
    enriched = enriched.withColumn(
        "is_reverse_charge",
        # stg.usp_AppendIncremental_SaleLine lines 21-22: a reverse-charge line carries zero VAT
        reverseChargeFlag(F.col("region_code"), F.col("vat_registration_number"), F.col("tax_regime_code"))
        | ((F.col("region_code") == "EU") & F.col("vat_registration_number").isNotNull() & (F.coalesce(F.col("sale_tax_amount_local"), F.lit(0)) == 0) & (F.col("sale_net_amount_local") > 0)),
    ).drop(*[c for c in enriched.columns if c.startswith(("_terr_", "_cust_", "_inv_", "_lt_"))])

    enriched = late_arriving.flagMissingReferences(enriched, "customer_business_key", dims.customer, "is_late_arriving_customer")
    if dims.salesperson is not None:
        enriched = late_arriving.flagMissingReferences(enriched, "salesperson_business_key", dims.salesperson, "is_late_arriving_salesperson")
    else:
        enriched = enriched.withColumn("is_late_arriving_salesperson", F.lit(False))

    # stg.usp_AppendIncremental_Sale: an invoice without a key, customer or date is rejected
    passed = quarantine(
        spark, cfg, enriched, "SALE_MISSING_KEY", "sqlserver_sales_invoices",
        F.col("sale_business_key").isNull() | F.col("customer_business_key").isNull() | F.col("invoice_date").isNull(),
        "InvoiceID, CustomerID or InvoiceDate is missing; the invoice cannot be keyed",
    )
    out = passed.withColumn(
        "dq_status_code",
        dqStatus(F.col("is_late_arriving_customer"), F.col("is_late_arriving_salesperson"), F.col("sale_gross_amount_local").isNull()),
    )
    return withAudit(out, cfg)


def conformSaleLines(
    spark: SparkSession,
    cfg: PipelineConfig,
    invoiceLines: DataFrame,
    sales: DataFrame,
    stockItems: DataFrame,
    dims: DimensionKeys,
) -> DataFrame:
    src = sourceSystemCol(invoiceLines)
    saleKey = sourceSystemKey(src, optCol(invoiceLines, "InvoiceID"))
    base = invoiceLines.select(
        lineBusinessKey(saleKey, optCol(invoiceLines, "InvoiceLineID")).alias("sale_line_business_key"),
        saleKey.alias("sale_business_key"),
        src.alias("source_system_code"),
        F.trim(optCol(invoiceLines, "InvoiceLineID")).alias("source_invoice_line_id"),
        optCol(invoiceLines, "InvoiceLineID", "int").alias("line_number"),
        sourceSystemKey(src, optCol(invoiceLines, "StockItemID")).alias("stock_item_business_key"),
        cleanText(optCol(invoiceLines, "Description"), 200).alias("line_description"),
        F.coalesce(upperCode(optCol(invoiceLines, "PackageTypeID")), F.lit("EACH")).alias("package_type_code"),
        optCol(invoiceLines, "Quantity", "decimal(18,4)").alias("quantity"),
        optCol(invoiceLines, "UnitPrice", "decimal(19,4)").alias("unit_price_amount_local"),
        optCol(invoiceLines, "TaxRate", "decimal(9,4)").alias("tax_rate_percent"),
        optCol(invoiceLines, "TaxAmount", "decimal(19,4)").alias("tax_amount_local"),
        optCol(invoiceLines, "LineProfit", "decimal(19,4)").alias("line_profit_amount_local"),
        optCol(invoiceLines, "ExtendedPrice", "decimal(19,4)").alias("gross_line_amount_local"),
        optCol(invoiceLines, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
        F.coalesce(optCol(invoiceLines, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
    )
    ranked = rankExactCopies(base, "sale_line_business_key")
    survivors = quarantine(
        spark, cfg, ranked, "DUP_SALE_LINE", "sqlserver_sales_invoice_lines",
        F.col(EXACT_COPY_RANK_COL) > 1,
        "exact re-extraction copy of an invoice line already staged; latest copy kept",
    ).drop(EXACT_COPY_RANK_COL)

    hdr = sales.select(
        F.col("sale_business_key").alias("_hdr_key"),
        F.col("invoice_date").alias("_hdr_invoice_date"),
        F.col("region_code").alias("_hdr_region"),
        F.col("currency_code").alias("_hdr_currency"),
        F.col("tax_regime_code").alias("_hdr_tax_regime"),
        F.col("is_reverse_charge").alias("_hdr_reverse_charge"),
        F.col("vat_registration_number").alias("_hdr_vat"),
        F.col("customer_business_key").alias("_hdr_customer"),
        F.col("is_credit_note").alias("_hdr_credit_note"),
    )
    joined = survivors.join(hdr, survivors.sale_business_key == hdr._hdr_key, "left")
    # stg.usp_AppendIncremental_SaleLine: BAD_NUMERIC and NO_HEADER rejects
    joined = quarantine(
        spark, cfg, joined, "SL_BAD_NUMERIC", "sqlserver_sales_invoice_lines",
        F.col("quantity").isNull() | F.col("unit_price_amount_local").isNull(),
        "Quantity or UnitPrice will not convert to a decimal",
    )
    joined = quarantine(
        spark, cfg, joined, "SL_ORPHAN_HEADER", "sqlserver_sales_invoice_lines",
        F.col("_hdr_key").isNull(),
        "no matching invoice header in silver.sale for this line",
    )
    stock = stockItemContext(stockItems)
    joined = joined.join(stock, joined.stock_item_business_key == stock._stock_key, "left")
    # stg.usp_AppendIncremental_SaleLine line 182: net = extended price - tax.
    # No tax recomputation / variance check here (workstream 5 rules engine).
    enriched = (
        joined.withColumn(
            "net_line_amount_local",
            (F.coalesce(F.col("gross_line_amount_local"), F.col("quantity") * F.col("unit_price_amount_local")) - F.coalesce(F.col("tax_amount_local"), F.lit(0))).cast(MONEY),
        )
        .withColumn("line_description", F.coalesce(F.col("line_description"), F.col("_stock_name")))
        .withColumn("invoice_date", F.col("_hdr_invoice_date"))
        .withColumn("region_code", F.col("_hdr_region"))
        .withColumn("currency_code", F.col("_hdr_currency"))
        .withColumn("tax_regime_code", F.col("_hdr_tax_regime"))
        .withColumn("is_reverse_charge", F.col("_hdr_reverse_charge"))
        .withColumn("vat_registration_number", F.col("_hdr_vat"))
        .withColumn("customer_business_key", F.col("_hdr_customer"))
        .withColumn("is_credit_note", F.col("_hdr_credit_note"))
        .drop("_stock_key", "_stock_name", *[c for c in joined.columns if c.startswith("_hdr_")])
    )
    enriched = late_arriving.flagMissingReferences(enriched, "stock_item_business_key", dims.stockItem, "is_late_arriving_stock_item")
    out = enriched.withColumn(
        "dq_status_code", dqStatus(F.col("is_late_arriving_stock_item"), F.col("stock_item_business_key").isNull())
    )
    return withAudit(out, cfg)


# --------------------------------------------------------------------------- #
# payments and allocations
# --------------------------------------------------------------------------- #
def normalisePaymentMethod(regionCol: Column, methodCol: Column) -> Column:
    """stg.usp_AppendIncremental_Payment lines 160-172 (regional method codes)."""
    method = F.upper(F.trim(F.coalesce(methodCol.cast("string"), F.lit(""))))
    method = F.regexp_replace(method, r"\s+", "_")
    # LEGACY QUIRK: EU bank transfers collapse to SEPA, APAC to BPAY/WIRE,
    # cheques of any spelling to CHECK, everything unknown to WIRE.
    return (
        F.when(method.isin("CHK", "CHEQUE", "CHECK"), F.lit("CHECK"))
        .when(method.isin("CASH"), F.lit("CASH"))
        .when(method.isin("CARD", "CREDIT_CARD", "DEBIT_CARD"), F.lit("CARD"))
        .when((regionCol == "EU") & method.isin("SEPA", "BACS", "DIRECT_DEBIT", "DIRECTDEBIT", "EFT", "BANKXFER", "BANK_TRANSFER"), F.lit("SEPA"))
        .when((regionCol == "APAC") & (method == "BPAY"), F.lit("BPAY"))
        .when(regionCol == "APAC", F.lit("WIRE"))
        .when(method.isin("ACH"), F.lit("ACH"))
        .when(method.isin("EFT", "BANKXFER", "BANK_TRANSFER", "DIRECT_DEBIT", "WIRE"), F.lit("WIRE"))
        .otherwise(F.lit("WIRE"))
    )


def conformPayments(
    spark: SparkSession,
    cfg: PipelineConfig,
    customerTransactions: DataFrame,
    customerPayments: DataFrame | None,
    customers: DataFrame,
    territories: DataFrame,
    dims: DimensionKeys,
) -> DataFrame:
    """Customer receipts. ``Sales.CustomerPayments`` (extension 2140) is the
    preferred source; without it the AR ledger rows of type 'Customer Payment
    Received' in ``Sales.CustomerTransactions`` are used (EXT_SQL_CustomerTransactions)."""
    if customerPayments is not None:
        src = sourceSystemCol(customerPayments)
        base = customerPayments.select(
            sourceSystemKey(src, optCol(customerPayments, "CustomerPaymentID")).alias("payment_business_key"),
            src.alias("source_system_code"),
            F.trim(optCol(customerPayments, "CustomerPaymentID")).alias("source_payment_id"),
            F.lit("Sales.CustomerPayments").alias("source_object_name"),
            sourceSystemKey(src, optCol(customerPayments, "CustomerID")).alias("customer_business_key"),
            F.lit(None).cast("string").alias("referenced_sale_business_key"),
            cleanText(optCol(customerPayments, "PaymentReference"), 40).alias("payment_reference"),
            cleanText(optCol(customerPayments, "BankStatementRef"), 60).alias("remittance_reference"),
            optCol(customerPayments, "ReceivedWhen", "timestamp").alias("received_when_utc"),
            optCol(customerPayments, "ReceivedWhen", "date").alias("payment_date"),
            optCol(customerPayments, "ValueDate", "date").alias("_value_date"),
            optCol(customerPayments, "PaymentMethodCode").alias("_method"),
            upperCode(optCol(customerPayments, "CurrencyCode")).alias("_pay_currency"),
            optCol(customerPayments, "ExchangeRateToUsd", "decimal(19,8)").alias("exchange_rate_to_usd"),
            optCol(customerPayments, "ReceivedAmount", "decimal(19,4)").alias("payment_amount_local"),
            F.coalesce(optCol(customerPayments, "BankChargeAmount", "decimal(19,4)"), F.lit(0)).cast(MONEY).alias("bank_charge_amount_local"),
            optCol(customerPayments, "AllocatedAmount", "decimal(19,4)").alias("source_allocated_amount_local"),
            optCol(customerPayments, "UnallocatedAmount", "decimal(19,4)").alias("source_unallocated_amount_local"),
            upperCode(optCol(customerPayments, "PaymentStatus")).alias("payment_status_code"),
            upperCode(optCol(customerPayments, "ReversalReasonCode")).alias("reversal_reason_code"),
            optCol(customerPayments, "ReversedWhen", "timestamp").alias("reversed_when_utc"),
            upperCode(optCol(customerPayments, "SourceInterfaceCode")).alias("source_interface_code"),
            optCol(customerPayments, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
            F.coalesce(optCol(customerPayments, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
        ).withColumn(
            "is_void",
            F.col("reversed_when_utc").isNotNull() | F.coalesce(F.col("payment_status_code"), F.lit("")).isin("REVERSED", "VOID", "CANCELLED"),
        )
    else:
        src = sourceSystemCol(customerTransactions)
        typeId = optCol(customerTransactions, "TransactionTypeID", "int")
        typeName = F.upper(optCol(customerTransactions, "TransactionTypeName"))
        receipts = customerTransactions.filter(
            (typeId == CUSTOMER_PAYMENT_TRANSACTION_TYPE_ID) | typeName.like("CUSTOMER PAYMENT%")
        )
        # LEGACY QUIRK: the OLTP AR ledger stores receipts as negative amounts;
        # the payment amount is the absolute value (EXT_SQL_CustomerTransactions).
        amount = F.abs(optCol(receipts, "TransactionAmount", "decimal(19,4)"))
        base = receipts.select(
            sourceSystemKey(src, optCol(receipts, "CustomerTransactionID")).alias("payment_business_key"),
            src.alias("source_system_code"),
            F.trim(optCol(receipts, "CustomerTransactionID")).alias("source_payment_id"),
            F.lit("Sales.CustomerTransactions").alias("source_object_name"),
            sourceSystemKey(src, optCol(receipts, "CustomerID")).alias("customer_business_key"),
            sourceSystemKey(src, optCol(receipts, "InvoiceID")).alias("referenced_sale_business_key"),
            F.lit(None).cast("string").alias("payment_reference"),
            F.lit(None).cast("string").alias("remittance_reference"),
            optCol(receipts, "TransactionDate", "timestamp").alias("received_when_utc"),
            optCol(receipts, "TransactionDate", "date").alias("payment_date"),
            optCol(receipts, "FinalizationDate", "date").alias("_value_date"),
            F.coalesce(optCol(receipts, "PaymentMethodName"), optCol(receipts, "PaymentMethodID")).alias("_method"),
            F.lit(None).cast("string").alias("_pay_currency"),
            F.lit(None).cast("decimal(19,8)").alias("exchange_rate_to_usd"),
            amount.cast(MONEY).alias("payment_amount_local"),
            F.lit(0).cast(MONEY).alias("bank_charge_amount_local"),
            F.lit(None).cast(MONEY).alias("source_allocated_amount_local"),
            F.abs(optCol(receipts, "OutstandingBalance", "decimal(19,4)")).cast(MONEY).alias("source_unallocated_amount_local"),
            # ssis "Conform Payment": FinalizationDate present -> PAID else PEND
            F.when(optCol(receipts, "FinalizationDate", "date").isNotNull(), F.lit("PAID")).otherwise(F.lit("PEND")).alias("payment_status_code"),
            F.lit(None).cast("string").alias("reversal_reason_code"),
            F.lit(None).cast("timestamp").alias("reversed_when_utc"),
            F.lit("AR_LEDGER").alias("source_interface_code"),
            optCol(receipts, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
            F.coalesce(optCol(receipts, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
        ).withColumn("is_void", F.coalesce(F.col("payment_amount_local"), F.lit(0)) == 0)

    base = rankExactCopies(base, "payment_business_key").filter(F.col(EXACT_COPY_RANK_COL) == 1).drop(EXACT_COPY_RANK_COL)

    cust = customerContext(customers, territories)
    joined = base.join(cust, base.customer_business_key == cust._cust_key, "left")
    region = F.coalesce(F.col("_cust_region"), F.col("_cust_terr_region"), F.lit(DEFAULT_REGION))
    currency = F.coalesce(F.col("_pay_currency"), F.col("_cust_terr_currency"), F.lit(DEFAULT_CURRENCY))
    # LEGACY QUIRK: ssis "Conform Payment" - missing value date is payment date
    # + 2 days for EU, + 1 day elsewhere.
    valueDate = F.coalesce(
        F.col("_value_date"),
        F.when(region == "EU", F.date_add(F.col("payment_date"), 2)).otherwise(F.date_add(F.col("payment_date"), 1)),
    )
    enriched = (
        joined.withColumn("region_code", region)
        .withColumn("currency_code", currency)
        .withColumn("value_date", valueDate)
        .withColumn("payment_method_code", normalisePaymentMethod(region, F.col("_method")))
        .drop(*[c for c in joined.columns if c.startswith(("_cust_", "_pay_", "_method", "_value_date"))])
    )
    enriched = late_arriving.flagMissingReferences(enriched, "customer_business_key", dims.customer, "is_late_arriving_customer")
    passed = quarantine(
        spark, cfg, enriched, "PAY_MISSING_KEY", "sqlserver_sales_customer_transactions",
        F.col("payment_business_key").isNull() | F.col("customer_business_key").isNull() | F.col("payment_amount_local").isNull(),
        "payment id, customer or amount is missing; the receipt cannot be keyed",
    )
    return passed


def explicitAllocationRows(paymentAllocations: DataFrame | None, payments: DataFrame) -> DataFrame:
    """Explicit references: Sales.PaymentAllocations INVOICE rows plus the
    payment row's own InvoiceID (the remittance naming the invoice, pass 1)."""
    own = payments.filter(F.col("referenced_sale_business_key").isNotNull()).select(
        "payment_business_key",
        F.col("referenced_sale_business_key").alias("sale_business_key"),
        F.col("payment_amount_local").alias("allocated_amount_local"),
    )
    if paymentAllocations is None:
        return own
    src = sourceSystemCol(paymentAllocations)
    rows = paymentAllocations.filter(
        (F.coalesce(F.upper(optCol(paymentAllocations, "TargetTypeCode")), F.lit("INVOICE")) == "INVOICE")
        & optCol(paymentAllocations, "InvoiceID").isNotNull()
        & optCol(paymentAllocations, "ReversalOfAllocationID").isNull()
    ).select(
        sourceSystemKey(src, optCol(paymentAllocations, "CustomerPaymentID")).alias("payment_business_key"),
        sourceSystemKey(src, optCol(paymentAllocations, "InvoiceID")).alias("sale_business_key"),
        optCol(paymentAllocations, "AllocatedAmount", "decimal(19,4)").alias("allocated_amount_local"),
    )
    return rows.unionByName(own)


def allocatePayments(
    spark: SparkSession,
    cfg: PipelineConfig,
    payments: DataFrame,
    sales: DataFrame,
    paymentAllocations: DataFrame | None,
) -> tuple[DataFrame, DataFrame]:
    """Run the matcher for this batch's payments against open invoices.

    Open amount = gross - allocations already made by *other* batches, so a
    re-run of the same batch reproduces the same allocations.
    """
    allocationFqn = cfg.fqn("silver", "payment_allocation")
    priorAllocations = None
    if tableExists(spark, allocationFqn):
        priorAllocations = (
            spark.table(allocationFqn)
            .filter((F.col("batch_id") != cfg.batchId) & F.col("sale_business_key").isNotNull())
            .groupBy("sale_business_key")
            .agg(F.sum("allocated_amount_local").cast(MONEY).alias("_prior_applied"))
        )
    invoices = sales.filter(~F.col("is_credit_note") & ~F.col("is_deleted")).select(
        "sale_business_key", "customer_business_key", "invoice_date", "sale_gross_amount_local", "is_on_hold"
    )
    if priorAllocations is not None:
        invoices = invoices.join(priorAllocations, "sale_business_key", "left")
    else:
        invoices = invoices.withColumn("_prior_applied", F.lit(None).cast(MONEY))
    invoices = invoices.withColumn(
        "open_amount_local",
        (F.coalesce(F.col("sale_gross_amount_local"), F.lit(0)) - F.coalesce(F.col("_prior_applied"), F.lit(0))).cast(MONEY),
    ).drop("_prior_applied", "sale_gross_amount_local")

    matchInput = payments.select(
        "payment_business_key", "customer_business_key", "payment_date", "payment_amount_local", "region_code", "is_void"
    )
    explicit = explicitAllocationRows(paymentAllocations, payments)
    allocations = matchPaymentsToInvoices(matchInput, invoices, explicit).select(*ALLOCATION_COLUMNS)
    allocations = allocations.withColumn(
        "payment_allocation_business_key",
        F.concat_ws("|", F.col("payment_business_key"), F.coalesce(F.col("sale_business_key"), F.lit("UNAPPLIED")), F.col("allocation_method_code")),
    )
    summary = summarisePayments(payments, allocations)
    out = summary.withColumn(
        "dq_status_code",
        dqStatus(F.col("is_late_arriving_customer"), F.col("match_status_code").isin("UNMATCHED", "PARTIAL")),
    )
    return withAudit(out, cfg), withAudit(allocations, cfg)


# --------------------------------------------------------------------------- #
# holds, amendments, backorders, quotes
# --------------------------------------------------------------------------- #
def _orderContext(orders: DataFrame) -> DataFrame:
    return orders.select(
        F.col("order_business_key").alias("_o_key"),
        F.col("region_code").alias("_o_region"),
        F.col("currency_code").alias("_o_currency"),
        F.col("customer_business_key").alias("_o_customer"),
    )


def _attachOrder(df: DataFrame, orders: DataFrame) -> DataFrame:
    ctx = _orderContext(orders)
    joined = df.join(ctx, df.order_business_key == ctx._o_key, "left")
    return (
        joined.withColumn("region_code", F.coalesce(F.col("_o_region"), F.lit(DEFAULT_REGION)))
        .withColumn("currency_code", F.coalesce(F.col("_o_currency"), F.lit(DEFAULT_CURRENCY)))
        .withColumn("customer_business_key", F.col("_o_customer"))
        .withColumn("is_orphan_order", F.col("_o_key").isNull())
        .drop("_o_key", "_o_region", "_o_currency", "_o_customer")
    )


def conformOrderHolds(cfg: PipelineConfig, holds: DataFrame, orders: DataFrame) -> DataFrame:
    src = sourceSystemCol(holds)
    base = holds.select(
        sourceSystemKey(src, optCol(holds, "OrderHoldID")).alias("order_hold_business_key"),
        sourceSystemKey(src, optCol(holds, "OrderID")).alias("order_business_key"),
        src.alias("source_system_code"),
        F.trim(optCol(holds, "OrderHoldID")).alias("source_order_hold_id"),
        upperCode(optCol(holds, "HoldTypeCode")).alias("hold_type_code"),
        upperCode(optCol(holds, "HoldReasonCode")).alias("hold_reason_code"),
        cleanText(optCol(holds, "HoldNarrative"), 500).alias("hold_narrative_text"),
        optCol(holds, "PlacedWhen", "timestamp").alias("placed_when_utc"),
        sourceSystemKey(src, optCol(holds, "PlacedByPersonID")).alias("placed_by_person_key"),
        upperCode(optCol(holds, "PlacedBySystem")).alias("placed_by_system_code"),
        optCol(holds, "ReleasedWhen", "timestamp").alias("released_when_utc"),
        sourceSystemKey(src, optCol(holds, "ReleasedByPersonID")).alias("released_by_person_key"),
        cleanText(optCol(holds, "ReleaseNarrative"), 500).alias("release_narrative_text"),
        optCol(holds, "AutoReleaseAfterWhen", "timestamp").alias("auto_release_after_when_utc"),
        optCol(holds, "EscalationLevel", "int").alias("escalation_level"),
        F.coalesce(optCol(holds, "IsBlockingDespatch", "boolean"), F.lit(False)).alias("is_blocking_despatch"),
        optCol(holds, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
        F.coalesce(optCol(holds, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
    ).filter(F.col("order_hold_business_key").isNotNull())
    base = rankExactCopies(base, "order_hold_business_key").filter(F.col(EXACT_COPY_RANK_COL) == 1).drop(EXACT_COPY_RANK_COL)
    out = _attachOrder(base, orders).withColumn("is_open", F.col("released_when_utc").isNull())
    return withAudit(out.withColumn("dq_status_code", dqStatus(F.col("is_orphan_order"))), cfg)


def conformOrderAmendments(cfg: PipelineConfig, amendments: DataFrame, orders: DataFrame) -> DataFrame:
    src = sourceSystemCol(amendments)
    base = amendments.select(
        sourceSystemKey(src, optCol(amendments, "OrderAmendmentID")).alias("order_amendment_business_key"),
        sourceSystemKey(src, optCol(amendments, "OrderID")).alias("order_business_key"),
        src.alias("source_system_code"),
        F.trim(optCol(amendments, "OrderAmendmentID")).alias("source_order_amendment_id"),
        optCol(amendments, "AmendmentSequence", "int").alias("amendment_sequence"),
        optCol(amendments, "AmendedWhen", "timestamp").alias("amended_when_utc"),
        sourceSystemKey(src, optCol(amendments, "AmendedByPersonID")).alias("amended_by_person_key"),
        upperCode(optCol(amendments, "AmendmentTypeCode")).alias("amendment_type_code"),
        cleanText(optCol(amendments, "TargetTableName"), 128).alias("target_table_name"),
        cleanText(optCol(amendments, "TargetKeyValue"), 100).alias("target_key_value"),
        cleanText(optCol(amendments, "ChangedColumnName"), 128).alias("changed_column_name"),
        cleanText(optCol(amendments, "OldValueText"), 4000).alias("old_value_text"),
        cleanText(optCol(amendments, "NewValueText"), 4000).alias("new_value_text"),
        upperCode(optCol(amendments, "ReasonCode")).alias("reason_code"),
        cleanText(optCol(amendments, "ReasonNarrative"), 500).alias("reason_narrative_text"),
        F.coalesce(optCol(amendments, "RequiresCustomerApproval", "boolean"), F.lit(False)).alias("requires_customer_approval"),
        optCol(amendments, "CustomerApprovedWhen", "timestamp").alias("customer_approved_when_utc"),
        upperCode(optCol(amendments, "SourceApplication")).alias("source_application_code"),
        optCol(amendments, "AmendedWhen", "timestamp").alias("source_modified_at_utc"),
        F.coalesce(optCol(amendments, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
    ).filter(F.col("order_amendment_business_key").isNotNull())
    base = rankExactCopies(base, "order_amendment_business_key").filter(F.col(EXACT_COPY_RANK_COL) == 1).drop(EXACT_COPY_RANK_COL)
    out = _attachOrder(base, orders)
    return withAudit(out.withColumn("dq_status_code", dqStatus(F.col("is_orphan_order"))), cfg)


def conformBackorders(cfg: PipelineConfig, backorders: DataFrame, orders: DataFrame, dims: DimensionKeys) -> DataFrame:
    src = sourceSystemCol(backorders)
    orderKey = sourceSystemKey(src, optCol(backorders, "OrderID"))
    base = backorders.select(
        sourceSystemKey(src, optCol(backorders, "BackorderID")).alias("backorder_business_key"),
        orderKey.alias("order_business_key"),
        lineBusinessKey(orderKey, optCol(backorders, "OrderLineID")).alias("order_line_business_key"),
        src.alias("source_system_code"),
        F.trim(optCol(backorders, "BackorderID")).alias("source_backorder_id"),
        sourceSystemKey(src, optCol(backorders, "StockItemID")).alias("stock_item_business_key"),
        optCol(backorders, "QuantityShort", "decimal(18,4)").alias("quantity_short"),
        F.coalesce(optCol(backorders, "QuantityReleased", "decimal(18,4)"), F.lit(0)).cast(QTY).alias("quantity_released"),
        optCol(backorders, "RaisedWhen", "timestamp").alias("raised_when_utc"),
        upperCode(optCol(backorders, "ShortageReasonCode")).alias("shortage_reason_code"),
        optCol(backorders, "PromisedDate", "date").alias("promised_date"),
        upperCode(optCol(backorders, "PromiseSource")).alias("promise_source_code"),
        F.coalesce(optCol(backorders, "RepromiseCount", "int"), F.lit(0)).alias("repromise_count"),
        optCol(backorders, "LinkedPurchaseOrderLineID", "bigint").alias("linked_purchase_order_line_id"),
        optCol(backorders, "CustomerNotifiedWhen", "timestamp").alias("customer_notified_when_utc"),
        F.coalesce(upperCode(optCol(backorders, "BackorderStatus")), F.lit("UNKNOWN")).alias("backorder_status_code"),
        optCol(backorders, "ClosedWhen", "timestamp").alias("closed_when_utc"),
        optCol(backorders, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
        F.coalesce(optCol(backorders, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
    ).filter(F.col("backorder_business_key").isNotNull())
    base = rankExactCopies(base, "backorder_business_key").filter(F.col(EXACT_COPY_RANK_COL) == 1).drop(EXACT_COPY_RANK_COL)
    out = _attachOrder(base, orders).withColumn(
        "quantity_outstanding", (F.col("quantity_short") - F.col("quantity_released")).cast(QTY)
    )
    out = late_arriving.flagMissingReferences(out, "stock_item_business_key", dims.stockItem, "is_late_arriving_stock_item")
    return withAudit(out.withColumn("dq_status_code", dqStatus(F.col("is_orphan_order"), F.col("is_late_arriving_stock_item"))), cfg)


def conformQuotes(cfg: PipelineConfig, quotes: DataFrame, customers: DataFrame, territories: DataFrame, dims: DimensionKeys) -> DataFrame:
    src = sourceSystemCol(quotes)
    base = quotes.select(
        sourceSystemKey(src, optCol(quotes, "QuoteID")).alias("quote_business_key"),
        src.alias("source_system_code"),
        F.trim(optCol(quotes, "QuoteID")).alias("source_quote_id"),
        cleanText(optCol(quotes, "QuoteReference"), 24).alias("quote_reference"),
        sourceSystemKey(src, optCol(quotes, "CustomerID")).alias("customer_business_key"),
        sourceSystemKey(src, F.coalesce(optCol(quotes, "SalespersonPersonID", "int"), F.lit(UNKNOWN_SALESPERSON_ID))).alias("salesperson_business_key"),
        sourceSystemKey(src, optCol(quotes, "SalesChannelID")).alias("sales_channel_business_key"),
        optCol(quotes, "PriceListID", "int").alias("price_list_id"),
        optCol(quotes, "QuoteDate", "date").alias("quote_date"),
        optCol(quotes, "ValidUntilDate", "date").alias("valid_until_date"),
        upperCode(optCol(quotes, "CurrencyCode")).alias("_q_currency"),
        optCol(quotes, "ExchangeRateToUSD", "decimal(19,8)").alias("exchange_rate_to_usd"),
        upperCode(optCol(quotes, "TaxTreatment")).alias("tax_treatment_code"),
        F.coalesce(upperCode(optCol(quotes, "QuoteStatus")), F.lit("UNKNOWN")).alias("quote_status_code"),
        optCol(quotes, "RevisionNumber", "int").alias("revision_number"),
        sourceSystemKey(src, optCol(quotes, "SupersedesQuoteID")).alias("supersedes_quote_business_key"),
        sourceSystemKey(src, optCol(quotes, "ConvertedOrderID")).alias("converted_order_business_key"),
        optCol(quotes, "ConvertedWhen", "timestamp").alias("converted_when_utc"),
        upperCode(optCol(quotes, "LostReasonCode")).alias("lost_reason_code"),
        optCol(quotes, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
        F.coalesce(optCol(quotes, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
    ).filter(F.col("quote_business_key").isNotNull())
    base = rankExactCopies(base, "quote_business_key").filter(F.col(EXACT_COPY_RANK_COL) == 1).drop(EXACT_COPY_RANK_COL)
    cust = customerContext(customers, territories)
    joined = base.join(cust, base.customer_business_key == cust._cust_key, "left")
    region = F.coalesce(F.col("_cust_region"), F.col("_cust_terr_region"), F.lit(DEFAULT_REGION))
    out = (
        joined.withColumn("region_code", region)
        .withColumn("currency_code", F.coalesce(F.col("_q_currency"), F.col("_cust_terr_currency"), F.lit(DEFAULT_CURRENCY)))
        .withColumn("tax_regime_code", F.coalesce(F.col("_cust_terr_tax_regime"), taxRegimeForRegion(region)))
        .drop(*[c for c in joined.columns if c.startswith(("_cust_", "_q_"))])
    )
    out = late_arriving.flagMissingReferences(out, "customer_business_key", dims.customer, "is_late_arriving_customer")
    return withAudit(out.withColumn("dq_status_code", dqStatus(F.col("is_late_arriving_customer"))), cfg)


def conformQuoteLines(cfg: PipelineConfig, quoteLines: DataFrame, quotes: DataFrame, dims: DimensionKeys) -> DataFrame:
    src = sourceSystemCol(quoteLines)
    quoteKey = sourceSystemKey(src, optCol(quoteLines, "QuoteID"))
    base = quoteLines.select(
        lineBusinessKey(quoteKey, optCol(quoteLines, "QuoteLineID")).alias("quote_line_business_key"),
        quoteKey.alias("quote_business_key"),
        src.alias("source_system_code"),
        F.trim(optCol(quoteLines, "QuoteLineID")).alias("source_quote_line_id"),
        optCol(quoteLines, "LineNumber", "int").alias("line_number"),
        sourceSystemKey(src, optCol(quoteLines, "StockItemID")).alias("stock_item_business_key"),
        cleanText(optCol(quoteLines, "DescriptionSnapshot"), 200).alias("line_description"),
        optCol(quoteLines, "Quantity", "decimal(18,4)").alias("quantity"),
        optCol(quoteLines, "UnitPrice", "decimal(19,4)").alias("unit_price_amount_local"),
        optCol(quoteLines, "DiscountPercent", "decimal(9,4)").alias("line_discount_percent"),
        optCol(quoteLines, "TaxRatePercent", "decimal(9,4)").alias("tax_rate_percent"),
        optCol(quoteLines, "LineNetAmount", "decimal(19,4)").alias("net_line_amount_local"),
        optCol(quoteLines, "PromisedLeadTimeDays", "int").alias("promised_lead_time_days"),
        F.coalesce(optCol(quoteLines, "IsOptionalLine", "boolean"), F.lit(False)).alias("is_optional_line"),
        F.coalesce(upperCode(optCol(quoteLines, "LineStatus")), F.lit("UNKNOWN")).alias("line_status_code"),
        optCol(quoteLines, "LastEditedWhen", "timestamp").alias("source_modified_at_utc"),
        F.coalesce(optCol(quoteLines, "_load_ts", "timestamp"), F.current_timestamp()).alias("_load_ts"),
    ).filter(F.col("quote_line_business_key").isNotNull())
    base = rankExactCopies(base, "quote_line_business_key").filter(F.col(EXACT_COPY_RANK_COL) == 1).drop(EXACT_COPY_RANK_COL)
    hdr = quotes.select(
        F.col("quote_business_key").alias("_h_key"),
        F.col("region_code").alias("_h_region"),
        F.col("currency_code").alias("_h_currency"),
        F.col("tax_regime_code").alias("_h_tax_regime"),
        F.col("customer_business_key").alias("_h_customer"),
    )
    joined = base.join(hdr, base.quote_business_key == hdr._h_key, "left")
    out = (
        joined.withColumn("region_code", F.coalesce(F.col("_h_region"), F.lit(DEFAULT_REGION)))
        .withColumn("currency_code", F.coalesce(F.col("_h_currency"), F.lit(DEFAULT_CURRENCY)))
        .withColumn("tax_regime_code", F.col("_h_tax_regime"))
        .withColumn("customer_business_key", F.col("_h_customer"))
        .withColumn("is_orphan_quote", F.col("_h_key").isNull())
        .drop(*[c for c in joined.columns if c.startswith("_h_")])
    )
    out = late_arriving.flagMissingReferences(out, "stock_item_business_key", dims.stockItem, "is_late_arriving_stock_item")
    return withAudit(out.withColumn("dq_status_code", dqStatus(F.col("is_orphan_quote"), F.col("is_late_arriving_stock_item"))), cfg)


# --------------------------------------------------------------------------- #
# late-arriving queue and soft deletes
# --------------------------------------------------------------------------- #
def queueLateArrivingDimensions(
    spark: SparkSession, cfg: PipelineConfig, dims: DimensionKeys, flagged: list[tuple[DataFrame, str, str, str, str]]
) -> None:
    """``flagged`` items: (df, entityType, keyCol, flagCol, sourceObjectName)."""
    missing = None
    for df, entityType, keyCol, flagCol, objectName in flagged:
        if flagCol not in df.columns:
            continue
        part = late_arriving.collectMissing(df, entityType, keyCol, flagCol, objectName)
        missing = part if missing is None else missing.unionByName(part)
    if missing is None:
        return
    # the same key seen from several objects counts once per key
    missing = missing.groupBy("entity_type", "business_key").agg(
        F.first("source_system_code", ignorenulls=True).alias("source_system_code"),
        F.first("first_seen_object_name").alias("first_seen_object_name"),
        F.sum("occurrence_count").cast("bigint").alias("occurrence_count"),
    )
    late_arriving.mergeQueue(spark, cfg, missing, dims.available(spark))


def applySoftDeletes(spark: SparkSession, cfg: PipelineConfig, bronze: BronzeReader) -> None:
    """``Sales.OrderDeletionLog`` (2310) and ``Integration.DeletedRowLog`` (2300) -> is_deleted."""
    orderLog = bronze.readOptional("sqlserver_sales_order_deletion_log")
    if orderLog is not None:
        orderIds = orderLog.select(F.trim(optCol(orderLog, "OrderID")).alias("source_order_id")).dropna().distinct()
        softDeleteByKey(spark, cfg.fqn("silver", "order"), orderIds, "source_order_id", cfg.batchId)
        orderKeys = orderLog.select(
            sourceSystemKey(sourceSystemCol(orderLog), optCol(orderLog, "OrderID")).alias("order_business_key")
        ).dropna().distinct()
        # the lines of a deleted header are deleted with it (order cancellation removes the header row)
        for table in ("order_line", "order_hold", "order_amendment", "backorder"):
            softDeleteByKey(spark, cfg.fqn("silver", table), orderKeys, "order_business_key", cfg.batchId)

    genericLog = bronze.readOptional("sqlserver_integration_deleted_row_log")
    if genericLog is None:
        return
    log = genericLog.select(
        F.upper(F.trim(optCol(genericLog, "SourceSchemaName"))).alias("_schema"),
        F.upper(F.trim(optCol(genericLog, "SourceTableName"))).alias("_table"),
        F.trim(optCol(genericLog, "SourceKeyValue")).alias("_key"),
    ).dropna()
    for (schema, table), (silverTable, keyCol) in DELETION_LOG_TARGETS.items():
        keys = log.filter((F.col("_schema") == schema) & (F.col("_table") == table)).select(F.col("_key").alias(keyCol))
        softDeleteByKey(spark, cfg.fqn("silver", silverTable), keys, keyCol, cfg.batchId)


def _write(spark: SparkSession, cfg: PipelineConfig, df: DataFrame, table: str, keyCol: str) -> None:
    mergeByKey(spark, df.drop("_load_ts"), cfg.fqn("silver", table), [keyCol], changeCol="row_hash")


# --------------------------------------------------------------------------- #
# entry point
# --------------------------------------------------------------------------- #
def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    cached: list[DataFrame] = []

    def keep(df: DataFrame) -> DataFrame:
        cached.append(df.cache())
        return cached[-1]

    try:
        _run(spark, cfg, keep)
    finally:
        for df in cached:
            df.unpersist()


def _run(spark: SparkSession, cfg: PipelineConfig, keep: Callable[[DataFrame], DataFrame]) -> None:
    bronze = BronzeReader(spark, cfg)
    customers = bronze.read("sqlserver_sales_customers")
    territories = bronze.read("sqlserver_sales_sales_territories")
    channels = bronze.read("sqlserver_sales_sales_channels")
    stockItems = bronze.read("sqlserver_warehouse_stock_items")
    dims = loadDimensionKeys(bronze)

    ordersRaw = bronze.read("sqlserver_sales_orders")
    orderLinesRaw = bronze.read("sqlserver_sales_order_lines")
    invoicesRaw = bronze.read("sqlserver_sales_invoices")
    invoiceLinesRaw = bronze.read("sqlserver_sales_invoice_lines")
    customerTransactionsRaw = bronze.read("sqlserver_sales_customer_transactions")
    customerPaymentsRaw = bronze.readOptional("sqlserver_sales_customer_payments")
    paymentAllocationsRaw = bronze.readOptional("sqlserver_sales_payment_allocations")
    holdsRaw = bronze.readOptional("sqlserver_sales_order_holds")
    amendmentsRaw = bronze.readOptional("sqlserver_sales_order_amendments")
    backordersRaw = bronze.readOptional("sqlserver_sales_backorders")
    quotesRaw = bronze.readOptional("sqlserver_sales_quote_headers", "sqlserver_sales_quotes")
    quoteLinesRaw = bronze.readOptional("sqlserver_sales_quote_lines")

    orders = keep(conformOrders(spark, cfg, ordersRaw, customers, territories, channels, holdsRaw, dims))
    _write(spark, cfg, orders, "order", "order_business_key")

    # lines are checked against every header known to silver, not only this batch
    knownOrders = spark.table(cfg.fqn("silver", "order")).filter(~F.col("is_deleted"))
    orderLines = keep(conformOrderLines(spark, cfg, orderLinesRaw, knownOrders, stockItems, dims))
    _write(spark, cfg, orderLines, "order_line", "order_line_business_key")

    sales = keep(conformSales(spark, cfg, invoicesRaw, invoiceLinesRaw, customers, territories, dims))
    _write(spark, cfg, sales, "sale", "sale_business_key")
    knownSales = spark.table(cfg.fqn("silver", "sale")).filter(~F.col("is_deleted"))
    saleLines = keep(conformSaleLines(spark, cfg, invoiceLinesRaw, knownSales, stockItems, dims))
    _write(spark, cfg, saleLines, "sale_line", "sale_line_business_key")

    payments = keep(conformPayments(spark, cfg, customerTransactionsRaw, customerPaymentsRaw, customers, territories, dims))
    paymentRows, allocationRows = allocatePayments(spark, cfg, payments, knownSales, paymentAllocationsRaw)
    paymentRows = keep(paymentRows)
    _write(spark, cfg, paymentRows, "payment", "payment_business_key")
    appendBatch(allocationRows, cfg.fqn("silver", "payment_allocation"), "batch_id", cfg.batchId)

    flagged: list[tuple[DataFrame, str, str, str, str]] = [
        (orders, "Customer", "customer_business_key", "is_late_arriving_customer", "silver.order"),
        (orders, "SalesChannel", "sales_channel_business_key", "is_late_arriving_channel", "silver.order"),
        (orders, "Salesperson", "salesperson_business_key", "is_late_arriving_salesperson", "silver.order"),
        (orderLines, "StockItem", "stock_item_business_key", "is_late_arriving_stock_item", "silver.order_line"),
        (sales, "Customer", "customer_business_key", "is_late_arriving_customer", "silver.sale"),
        (sales, "Salesperson", "salesperson_business_key", "is_late_arriving_salesperson", "silver.sale"),
        (saleLines, "StockItem", "stock_item_business_key", "is_late_arriving_stock_item", "silver.sale_line"),
        (paymentRows, "Customer", "customer_business_key", "is_late_arriving_customer", "silver.payment"),
    ]

    if holdsRaw is not None:
        holds = conformOrderHolds(cfg, holdsRaw, knownOrders)
        _write(spark, cfg, holds, "order_hold", "order_hold_business_key")
    if amendmentsRaw is not None:
        amendments = conformOrderAmendments(cfg, amendmentsRaw, knownOrders)
        _write(spark, cfg, amendments, "order_amendment", "order_amendment_business_key")
    if backordersRaw is not None:
        backorders = keep(conformBackorders(cfg, backordersRaw, knownOrders, dims))
        _write(spark, cfg, backorders, "backorder", "backorder_business_key")
        flagged.append((backorders, "StockItem", "stock_item_business_key", "is_late_arriving_stock_item", "silver.backorder"))
    if quotesRaw is not None:
        quotes = keep(conformQuotes(cfg, quotesRaw, customers, territories, dims))
        _write(spark, cfg, quotes, "quote", "quote_business_key")
        flagged.append((quotes, "Customer", "customer_business_key", "is_late_arriving_customer", "silver.quote"))
        if quoteLinesRaw is not None:
            knownQuotes = spark.table(cfg.fqn("silver", "quote")).filter(~F.col("is_deleted"))
            quoteLines = keep(conformQuoteLines(cfg, quoteLinesRaw, knownQuotes, dims))
            _write(spark, cfg, quoteLines, "quote_line", "quote_line_business_key")
            flagged.append((quoteLines, "StockItem", "stock_item_business_key", "is_late_arriving_stock_item", "silver.quote_line"))

    if dims.salesperson is None:
        flagged = [f for f in flagged if f[1] != "Salesperson"]
    queueLateArrivingDimensions(spark, cfg, dims, flagged)
    applySoftDeletes(spark, cfg, bronze)
