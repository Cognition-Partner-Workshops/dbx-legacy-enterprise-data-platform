"""Sales Facts transforms converted from the SQL Server / SSIS estate.

Source of the rules (legacy repo, read-only):
  sqlserver/procedures/facts/Integration.usp_LoadFactSale.sql        (governing spec)
  sqlserver/procedures/facts/Integration.usp_DeduplicateFactSale.sql
  sqlserver/procedures/facts/Integration.usp_ApplyFactCorrections.sql
  sqlserver/staging/procedures/stg.usp_AppendIncremental_SaleLine.sql (tax variance rules)
  ssis/08_facts/build_fact_packages.py                               (FACT_{NA,EU,APAC}_Load_Sale)

The legacy load is stateful (delete-by-window, REV/RES against the previously
loaded fact). Here the fact is a deterministic function of the append-only
bronze history, so every refresh is idempotent and the audit trail is
reconstructed from the versions of each invoice line rather than from the prior
state of the fact table.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

REGION_CODES = ("NA", "EU", "APAC")

UNKNOWN_MEMBER_KEY = 0
NOT_APPLICABLE_KEY = -1

# Reject reasons, worded as in Integration.usp_LoadFactSale.sql so the steward
# queue reads the same after cutover.
REJECT_QUANTITY_NULL = "Quantity is null"
REJECT_UNIT_PRICE_NULL = "Unit price is null"
REJECT_STOCK_ITEM_NULL = "Stock item code is null"
REJECT_REGION_UNMAPPED = "Unmapped region code"
REJECT_TAX_VARIANCE = "Tax amount outside regional tolerance"
REJECT_REVERSE_CHARGE_TAX = "Reverse-charge line carries tax"

# Per-line tax variance tolerance from stg.usp_AppendIncremental_SaleLine.sql.
TAX_TOLERANCE = {"NA": 0.02, "EU": 0.01, "APAC": 0.05}

HOLD_REASON_DIM_NOT_KEYED = "DIM_NOT_KEYED"

CORRECTION_ORIG = "ORIG"
CORRECTION_REV = "REV"
CORRECTION_RES = "RES"


# ---------------------------------------------------------------------------
# Bronze -> Silver: typing and defaults (raw.SqlInvoice / raw.SqlInvoiceLine)
# ---------------------------------------------------------------------------


def _clean(col: str) -> Column:
    return F.nullif(F.trim(F.col(col)), F.lit(""))


def _dec(col: str, precision: int = 19, scale: int = 4) -> Column:
    return F.try_to_number(_clean(col), F.lit("9" * (precision - scale) + "." + "9" * scale)).cast(
        f"decimal({precision},{scale})"
    )


def conform_invoice_header(bronze_invoice: DataFrame) -> DataFrame:
    """raw.SqlInvoice -> typed invoice header (stg.SalesInvoiceHeader equivalent).

    Defaults are the ones Integration.usp_LoadFactSale applies at extraction:
    bill-to falls back to the customer, currency to USD, channel to DIRECT.
    DRAFT invoices never reach the fact.
    """
    return bronze_invoice.select(
        _clean("InvoiceID").alias("invoice_number"),
        _clean("OrderID").alias("order_number"),
        F.to_date(_clean("InvoiceDate")).alias("invoice_date"),
        F.to_date(_clean("ConfirmedDeliveryTime")).alias("delivery_date"),
        _clean("CustomerID").alias("customer_business_key"),
        F.coalesce(_clean("BillToCustomerID"), _clean("CustomerID")).alias("bill_to_business_key"),
        _clean("SalespersonPersonID").alias("salesperson_code"),
        _clean("DeliveryCityCode").alias("city_code"),
        F.coalesce(F.upper(_clean("SalesChannelCode")), F.lit("DIRECT")).alias("channel_code"),
        F.upper(_clean("RegionCode")).alias("region_code"),
        F.coalesce(F.upper(_clean("CurrencyCode")), F.lit("USD")).alias("transaction_currency"),
        F.upper(_clean("TaxRegimeCode")).alias("tax_regime_code"),
        _clean("CustomerVatNumber").alias("customer_vat_number"),
        F.coalesce(F.upper(_clean("InvoiceStatusCode")), F.lit("POSTED")).alias("invoice_status_code"),
        F.col("BatchId").cast("bigint").alias("batch_id"),
        F.col("SourceSystemCode").alias("source_system_code"),
    ).where(F.col("invoice_status_code") != F.lit("DRAFT"))


def conform_invoice_line(bronze_invoice_line: DataFrame) -> DataFrame:
    """raw.SqlInvoiceLine -> typed invoice line (stg.SalesInvoiceLine equivalent).

    Numeric text is parsed with try_to_number so malformed values become NULL
    and are caught by the structural reject rules instead of failing the load
    (stg.ufn_SafeDecimal behaviour).
    """
    return bronze_invoice_line.select(
        _clean("InvoiceID").alias("invoice_number"),
        _clean("InvoiceLineID").try_cast("int").alias("invoice_line_number"),
        _clean("StockItemID").alias("stock_item_code"),
        _clean("PromotionLineID").alias("promotion_code"),
        _dec("Quantity", 18, 4).alias("quantity_source_uom"),
        F.coalesce(F.upper(_clean("UnitOfMeasureCode")), F.lit("EA")).alias("source_uom_code"),
        F.coalesce(_dec("UomConversionFactor", 18, 6), F.lit(1.0).cast("decimal(18,6)")).alias("uom_conversion_factor"),
        _dec("UnitPrice").alias("unit_price"),
        F.coalesce(_dec("LineDiscountAmount"), F.lit(0).cast("decimal(19,4)")).alias("line_discount_amount"),
        F.coalesce(_dec("FreightAllocatedAmount"), F.lit(0).cast("decimal(19,4)")).alias("freight_amount"),
        _dec("UnitCost").alias("unit_cost"),
        F.coalesce(_dec("TaxAmount"), F.lit(0).cast("decimal(19,4)")).alias("source_tax_amount"),
        F.coalesce(_dec("TaxRate", 9, 4), F.lit(0).cast("decimal(9,4)")).alias("tax_rate"),
        F.coalesce(_clean("GstFreeFlag").try_cast("int"), F.lit(0)).alias("gst_free_flag"),
        _clean("SourceRowVersion").try_cast("bigint").alias("source_row_version"),
        F.col("BatchId").cast("bigint").alias("batch_id"),
        F.col("SourceSystemCode").alias("source_system_code"),
    )


def conform_fx_rates(bronze_fx: DataFrame) -> DataFrame:
    """stg.ExchangeRateDaily as text -> typed rates."""
    return bronze_fx.select(
        F.upper(_clean("CurrencyCode")).alias("currency_code"),
        F.upper(_clean("RateSourceCode")).alias("rate_source_code"),
        F.to_date(_clean("RateDate")).alias("rate_date"),
        _dec("RateToReporting", 18, 6).alias("rate_to_reporting"),
    ).where(F.col("rate_to_reporting").isNotNull() & F.col("rate_date").isNotNull())


# ---------------------------------------------------------------------------
# Silver: the #SaleWork equivalent
# ---------------------------------------------------------------------------


def build_sale_work(headers: DataFrame, lines: DataFrame) -> DataFrame:
    """Join lines to headers of the same extract batch and stamp the natural key.

    Natural key = invoice number | line number | region, exactly as the legacy
    HASHBYTES('SHA2_256', ...) so the two hashes agree during parallel run.
    """
    h = headers.alias("h")
    ln = lines.alias("l")
    joined = ln.join(
        h,
        (F.col("l.invoice_number") == F.col("h.invoice_number")) & (F.col("l.batch_id") == F.col("h.batch_id")),
        "inner",
    )
    return joined.select(
        F.col("l.invoice_number"),
        F.col("l.invoice_line_number"),
        F.col("h.order_number"),
        F.col("h.invoice_date"),
        F.col("h.delivery_date"),
        F.col("h.customer_business_key"),
        F.col("h.bill_to_business_key"),
        F.col("l.stock_item_code"),
        F.col("h.salesperson_code"),
        F.col("h.city_code"),
        F.col("h.channel_code"),
        F.col("l.promotion_code"),
        F.col("h.region_code"),
        F.col("h.transaction_currency"),
        F.col("l.quantity_source_uom"),
        F.col("l.source_uom_code"),
        F.col("l.uom_conversion_factor"),
        F.col("l.unit_price"),
        F.col("l.line_discount_amount"),
        F.col("l.freight_amount"),
        F.col("l.unit_cost"),
        F.col("l.source_tax_amount"),
        F.col("l.tax_rate"),
        F.col("h.tax_regime_code"),
        F.col("h.customer_vat_number"),
        F.col("l.gst_free_flag"),
        F.col("l.source_row_version"),
        F.col("l.batch_id"),
        F.col("l.source_system_code"),
    ).withColumn(
        "natural_key_hash",
        F.sha2(
            F.concat_ws(
                "|", F.col("invoice_number"), F.col("invoice_line_number").cast("string"), F.col("region_code")
            ),
            256,
        ),
    )


def _structural_reject_reason() -> Column:
    return (
        F.when(F.col("quantity_source_uom").isNull(), F.lit(REJECT_QUANTITY_NULL))
        .when(F.col("unit_price").isNull(), F.lit(REJECT_UNIT_PRICE_NULL))
        .when(F.col("stock_item_code").isNull(), F.lit(REJECT_STOCK_ITEM_NULL))
        .when(~F.col("region_code").isin(*REGION_CODES) | F.col("region_code").isNull(), F.lit(REJECT_REGION_UNMAPPED))
    )


def _tax_variance_reject_reason() -> Column:
    """Tax variance check from stg.usp_AppendIncremental_SaleLine (finance want a
    reject report, not a silent recalculation). EU reverse-charge lines must
    carry zero VAT."""
    expected = F.col("quantity_source_uom") * F.col("unit_price") * F.col("tax_rate") / F.lit(100.0)
    tolerance = (
        F.when(F.col("region_code") == "NA", F.lit(TAX_TOLERANCE["NA"]))
        .when(F.col("region_code") == "EU", F.lit(TAX_TOLERANCE["EU"]))
        .otherwise(F.lit(TAX_TOLERANCE["APAC"]))
    )
    is_reverse_charge = (
        (F.col("region_code") == "EU")
        & F.col("customer_vat_number").isNotNull()
        & (F.col("tax_regime_code") == "EU_RC")
    )
    return F.when(is_reverse_charge & (F.col("source_tax_amount") != 0), F.lit(REJECT_REVERSE_CHARGE_TAX)).when(
        ~is_reverse_charge & (F.abs(F.col("source_tax_amount") - expected) > tolerance),
        F.lit(REJECT_TAX_VARIANCE),
    )


def with_reject_reason(sale_work: DataFrame) -> DataFrame:
    return sale_work.withColumn("reject_reason", F.coalesce(_structural_reject_reason(), _tax_variance_reject_reason()))


def rejected_invoice_lines(sale_work: DataFrame) -> DataFrame:
    """err.RejectedInvoiceLine / etl.usp_LogRejectedRecord equivalent."""
    return (
        with_reject_reason(sale_work)
        .where(F.col("reject_reason").isNotNull())
        .select(
            "invoice_number",
            "invoice_line_number",
            "region_code",
            "natural_key_hash",
            "reject_reason",
            "source_row_version",
            "batch_id",
            "source_system_code",
        )
    )


def accepted_sale_work(sale_work: DataFrame) -> DataFrame:
    return with_reject_reason(sale_work).where(F.col("reject_reason").isNull()).drop("reject_reason")


def dedup_within_batch(sale_work: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Highest source row version wins inside one extract batch (step 3 of the
    legacy proc: the web channel replays extracts on retry). Returns
    (survivors, dropped) so the dropped rows can be archived the way
    Integration.usp_DeduplicateFactSale archives them."""
    w = Window.partitionBy("natural_key_hash", "batch_id").orderBy(
        F.coalesce(F.col("source_row_version"), F.lit(0)).desc()
    )
    ranked = sale_work.withColumn("_rn", F.row_number().over(w))
    return ranked.where(F.col("_rn") == 1).drop("_rn"), ranked.where(F.col("_rn") > 1).drop("_rn")


# ---------------------------------------------------------------------------
# Silver: dimension keys, hold, FX, tax and measures
# ---------------------------------------------------------------------------


def _effective_lookup(
    work: DataFrame,
    dim: DataFrame,
    work_key: str,
    dim_key: str,
    dim_surrogate: str,
    out_col: str,
    effective_dated: bool = True,
) -> DataFrame:
    d = dim.alias("d")
    cond = F.col(f"w.{work_key}") == F.col(f"d.{dim_key}")
    if effective_dated:
        cond = (
            cond & (F.col("w.invoice_date") >= F.col("d.valid_from")) & (F.col("w.invoice_date") < F.col("d.valid_to"))
        )
    return work.alias("w").join(d, cond, "left").select("w.*", F.col(f"d.{dim_surrogate}").alias(out_col))


def resolve_dimension_keys(
    sale_work: DataFrame,
    dim_customer: DataFrame,
    dim_stock_item: DataFrame,
    dim_salesperson: DataFrame,
    dim_city: DataFrame,
    dim_sales_channel: DataFrame,
    dim_promotion: DataFrame,
) -> DataFrame:
    """Step 4/5 of Integration.usp_LoadFactSale.

    Unknown member (0) for every miss; -1 when the source has no salesperson or
    promotion. Customers are the exception: an early-arriving customer is
    flagged `inferred_member_flag` and keyed 0 here; `inferred_customers()`
    publishes the business keys for the dimension pipeline to create the
    inferred members (the legacy proc inserted into Dimension.Customer directly,
    which a fact pipeline must not do).
    """
    w = _effective_lookup(
        sale_work, dim_customer, "customer_business_key", "customer_business_key", "customer_key", "_cust"
    )
    w = _effective_lookup(w, dim_customer, "bill_to_business_key", "customer_business_key", "customer_key", "_bill")
    w = _effective_lookup(w, dim_stock_item, "stock_item_code", "stock_item_code", "stock_item_key", "_item")
    w = _effective_lookup(w, dim_salesperson, "salesperson_code", "salesperson_code", "salesperson_key", "_sp")
    w = _effective_lookup(w, dim_city, "city_code", "city_code", "city_key", "_city")
    w = _effective_lookup(w, dim_sales_channel, "channel_code", "channel_code", "sales_channel_key", "_chan", False)
    w = _effective_lookup(w, dim_promotion, "promotion_code", "promotion_code", "promotion_key", "_promo")
    return (
        w.withColumn("customer_key", F.coalesce(F.col("_cust"), F.lit(UNKNOWN_MEMBER_KEY)))
        .withColumn("bill_to_customer_key", F.coalesce(F.col("_bill"), F.lit(UNKNOWN_MEMBER_KEY)))
        .withColumn("stock_item_key", F.coalesce(F.col("_item"), F.lit(UNKNOWN_MEMBER_KEY)))
        .withColumn(
            "salesperson_key",
            F.when(F.col("salesperson_code").isNull(), F.lit(NOT_APPLICABLE_KEY)).otherwise(
                F.coalesce(F.col("_sp"), F.lit(UNKNOWN_MEMBER_KEY))
            ),
        )
        .withColumn("city_key", F.coalesce(F.col("_city"), F.lit(UNKNOWN_MEMBER_KEY)))
        .withColumn("sales_channel_key", F.coalesce(F.col("_chan"), F.lit(UNKNOWN_MEMBER_KEY)))
        .withColumn(
            "promotion_key",
            F.when(F.col("promotion_code").isNull(), F.lit(NOT_APPLICABLE_KEY)).otherwise(
                F.coalesce(F.col("_promo"), F.lit(UNKNOWN_MEMBER_KEY))
            ),
        )
        .withColumn(
            "inferred_member_flag",
            (
                (F.col("customer_key") == UNKNOWN_MEMBER_KEY)
                & F.col("customer_business_key").isNotNull()
                & F.col("customer_business_key").try_cast("int").isNotNull()
            ).cast("int"),
        )
        .drop("_cust", "_bill", "_item", "_sp", "_city", "_chan", "_promo")
    )


def inferred_customers(keyed: DataFrame) -> DataFrame:
    """Early-arriving customers the dimension pipeline must create as inferred
    members ('*** INFERRED <key>', valid 2013-01-01 .. 9999-12-31)."""
    return (
        keyed.where(F.col("inferred_member_flag") == 1)
        .groupBy("customer_business_key")
        .agg(F.min("invoice_date").alias("first_seen_invoice_date"), F.max("batch_id").alias("last_seen_batch_id"))
        .withColumn("customer_name", F.concat(F.lit("*** INFERRED "), F.col("customer_business_key")))
    )


def fact_load_hold(keyed: DataFrame) -> DataFrame:
    """Fact.[Fact Load Hold] equivalent: a sale with no product would corrupt
    margin reporting, so it is parked instead of loaded against the unknown
    member."""
    return keyed.where(F.col("stock_item_key") == UNKNOWN_MEMBER_KEY).select(
        F.lit("fact_sale").alias("target_fact_name"),
        F.col("source_system_code"),
        F.col("region_code"),
        F.col("natural_key_hash"),
        F.concat_ws("|", F.col("invoice_number"), F.col("invoice_line_number").cast("string")).alias(
            "natural_key_text"
        ),
        F.col("invoice_date").alias("business_date"),
        F.lit("dim_stock_item").alias("missing_dimension_name"),
        F.col("stock_item_code").alias("missing_business_key"),
        F.lit(HOLD_REASON_DIM_NOT_KEYED).alias("hold_reason_code"),
        F.to_json(F.struct(*[c for c in keyed.columns])).alias("source_payload"),
        F.lit("HELD").alias("hold_status_code"),
        F.col("batch_id"),
    )


def release_held(keyed: DataFrame) -> DataFrame:
    return keyed.where(F.col("stock_item_key") != UNKNOWN_MEMBER_KEY)


def apply_fx(keyed: DataFrame, fx_rates: DataFrame) -> DataFrame:
    """Step 7: last published rate on or before the invoice date. APAC uses the
    APAC_TREASURY feed (a day behind the group feed), everyone else GROUP.
    No rate -> 1.0 dated on the invoice date."""
    fx = fx_rates.select(
        F.col("currency_code").alias("fx_currency_code"),
        F.col("rate_source_code").alias("fx_rate_source_code"),
        F.col("rate_date").alias("fx_rate_date_candidate"),
        F.col("rate_to_reporting"),
    )
    wanted_source = F.when(F.col("region_code") == "APAC", F.lit("APAC_TREASURY")).otherwise(F.lit("GROUP"))
    joined = keyed.withColumn("fx_rate_source_code_wanted", wanted_source).join(
        fx,
        (F.col("transaction_currency") == F.col("fx_currency_code"))
        & (F.col("fx_rate_source_code_wanted") == F.col("fx_rate_source_code"))
        & (F.col("fx_rate_date_candidate") <= F.col("invoice_date")),
        "left",
    )
    w = Window.partitionBy("natural_key_hash", "batch_id", "source_row_version").orderBy(
        F.col("fx_rate_date_candidate").desc_nulls_last()
    )
    return (
        joined.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .withColumn("fx_rate", F.coalesce(F.col("rate_to_reporting"), F.lit(1.0)).cast("decimal(18,6)"))
        .withColumn("fx_rate_date", F.coalesce(F.col("fx_rate_date_candidate"), F.col("invoice_date")))
        .withColumn("fx_rate_source_code", F.col("fx_rate_source_code_wanted"))
        .drop("_rn", "fx_currency_code", "fx_rate_date_candidate", "rate_to_reporting", "fx_rate_source_code_wanted")
    )


def compute_measures(work: DataFrame) -> DataFrame:
    """Step 8: measures as the 2006 Access report computed them. Freight is
    excluded from net but included in the invoice total. Tax is regional:
    NA keeps the source amount (state + county already combined), EU recomputes
    VAT and zeroes reverse-charge lines, APAC recomputes GST unless GST-free."""
    qty_base = F.col("quantity_source_uom") * F.col("uom_conversion_factor")
    gross = F.round(qty_base * F.col("unit_price"), 2)
    cost = F.round(qty_base * F.coalesce(F.col("unit_cost"), F.lit(0)), 2)
    net = gross - F.coalesce(F.col("line_discount_amount"), F.lit(0))
    margin = net - cost
    recomputed_tax = F.round(net * F.coalesce(F.col("tax_rate"), F.lit(0)) / F.lit(100.0), 2)
    tax = (
        F.when(F.col("region_code") == "NA", F.col("source_tax_amount"))
        .when(
            F.col("region_code") == "EU",
            F.when(
                F.col("customer_vat_number").isNotNull() & (F.col("tax_regime_code") == "EU_RC"), F.lit(0)
            ).otherwise(recomputed_tax),
        )
        .otherwise(F.when(F.coalesce(F.col("gst_free_flag"), F.lit(0)) == 1, F.lit(0)).otherwise(recomputed_tax))
    )
    return (
        work.withColumn("quantity_base_uom", qty_base.cast("decimal(18,4)"))
        .withColumn("gross_amount", gross.cast("decimal(18,2)"))
        .withColumn("cost_of_sale_amount", cost.cast("decimal(18,2)"))
        .withColumn("net_amount", net.cast("decimal(18,2)"))
        .withColumn("gross_margin_amount", margin.cast("decimal(18,2)"))
        .withColumn("tax_amount", tax.cast("decimal(18,2)"))
        .withColumn("total_including_tax", (F.col("net_amount") + F.col("tax_amount")).cast("decimal(18,2)"))
        .withColumn(
            "margin_percent",
            F.when(F.col("net_amount") == 0, F.lit(None))
            .otherwise(F.round(F.col("gross_margin_amount") / F.col("net_amount") * 100.0, 4))
            .cast("decimal(9,4)"),
        )
        .withColumn("net_amount_reporting", F.round(F.col("net_amount") * F.col("fx_rate"), 2).cast("decimal(18,2)"))
        .withColumn("tax_amount_reporting", F.round(F.col("tax_amount") * F.col("fx_rate"), 2).cast("decimal(18,2)"))
        .withColumn(
            "gross_margin_reporting", F.round(F.col("gross_margin_amount") * F.col("fx_rate"), 2).cast("decimal(18,2)")
        )
        .withColumn("wwi_invoice_id", F.substring(F.col("invoice_number"), -9, 9).try_cast("int"))
    )


MEASURE_COLUMNS = (
    "quantity_source_uom",
    "quantity_base_uom",
    "gross_amount",
    "line_discount_amount",
    "net_amount",
    "tax_amount",
    "total_including_tax",
    "freight_amount",
    "cost_of_sale_amount",
    "gross_margin_amount",
    "net_amount_reporting",
    "tax_amount_reporting",
    "gross_margin_reporting",
)


# ---------------------------------------------------------------------------
# Gold: fact_sale with the REVERSAL correction pattern
# ---------------------------------------------------------------------------


def _sale_key(batch_col: Column, version_col: Column, correction: Column) -> Column:
    """Deterministic surrogate for [Sale Key] (an IDENTITY in the legacy table).
    Including batch and version keeps pre-2017 rows with no source row version
    from colliding."""
    return F.xxhash64(F.col("natural_key_hash"), batch_col.cast("string"), version_col.cast("string"), correction)


FACT_COMMON_COLUMNS = (
    "invoice_number",
    "invoice_line_number",
    "order_number",
    "wwi_invoice_id",
    "region_code",
    "natural_key_hash",
    "transaction_currency",
    "source_uom_code",
    "tax_regime_code",
    "customer_vat_number",
    "gst_free_flag",
    "source_system_code",
)

# Attributes carried onto a REV row from the version being reversed.
FACT_VERSION_ATTRIBUTES = (
    "city_key",
    "customer_key",
    "bill_to_customer_key",
    "stock_item_key",
    "invoice_date",
    "delivery_date",
    "salesperson_key",
    "sales_channel_key",
    "promotion_key",
    "unit_price",
    "tax_rate",
    "fx_rate",
    "fx_rate_date",
    "fx_rate_source_code",
    "margin_percent",
    "inferred_member_flag",
)


def _version_window() -> Window:
    return Window.partitionBy("natural_key_hash").orderBy(
        F.col("batch_id").asc(), F.coalesce(F.col("source_row_version"), F.lit(0)).asc()
    )


def _counted_versions(sale_lines: DataFrame) -> DataFrame:
    """Versions of a line that produce fact rows: the first one, plus every
    later one whose net amount differs from its predecessor. Comparing with the
    immediate predecessor is enough because a skipped version has, by
    definition, the same net as its own predecessor."""
    w = _version_window()
    versioned = sale_lines.withColumn("_version_no", F.row_number().over(w)).withColumn(
        "_prev_net", F.lag("net_amount").over(w)
    )
    return versioned.where(
        (F.col("_version_no") == 1) | (F.round(F.col("net_amount"), 2) != F.round(F.col("_prev_net"), 2))
    ).drop("_version_no", "_prev_net")


def build_fact_sale(sale_lines: DataFrame) -> DataFrame:
    """Turn the per-batch history of every invoice line into fact rows.

    Legacy rule (Integration.usp_LoadFactSale + usp_ApplyFactCorrections): a
    line whose net amount changes after it was loaded is never updated in
    place. The loaded row stays, a negated copy tagged REV is added, and the new
    values are added tagged RES, so period totals are right for any as-at date.

    `sale_lines` holds one row per (natural key, batch). Ordered by batch then
    source row version, the first version is ORIG; each later version with a
    different net amount yields REV (negation of the previous counted version)
    + RES; a later version with the same net amount is an exact replay and is
    dropped (see `duplicate_archive`). `corrected_sale_key` on REV/RES rows
    points at the row they restate.
    """
    w = _version_window()
    counted = _counted_versions(sale_lines).withColumn("_rank", F.row_number().over(w))
    counted = counted.withColumn("_prev_batch", F.lag("batch_id").over(w)).withColumn(
        "_prev_version", F.lag("source_row_version").over(w)
    )
    for c in (*MEASURE_COLUMNS, *FACT_VERSION_ATTRIBUTES):
        counted = counted.withColumn(f"_prev_{c}", F.lag(c).over(w))

    def rows(src: DataFrame, correction: str, corrected_key: Column, prev_values: bool) -> DataFrame:
        if prev_values:
            attrs = [F.col(f"_prev_{c}").alias(c) for c in FACT_VERSION_ATTRIBUTES]
            measures = [(-F.col(f"_prev_{c}")).cast(src.schema[c].dataType).alias(c) for c in MEASURE_COLUMNS]
        else:
            attrs = [F.col(c) for c in FACT_VERSION_ATTRIBUTES]
            measures = [F.col(c) for c in MEASURE_COLUMNS]
        return src.select(
            _sale_key(F.col("batch_id"), F.col("source_row_version"), F.lit(correction)).alias("sale_key"),
            *FACT_COMMON_COLUMNS,
            *attrs,
            *measures,
            F.col("source_row_version"),
            F.lit(correction).alias("correction_type_code"),
            corrected_key.alias("corrected_sale_key"),
            F.col("batch_id"),
        )

    first = counted.where(F.col("_rank") == 1)
    restated = counted.where(F.col("_rank") > 1)
    prev_key = _sale_key(
        F.col("_prev_batch"),
        F.col("_prev_version"),
        F.when(F.col("_rank") == 2, F.lit(CORRECTION_ORIG)).otherwise(F.lit(CORRECTION_RES)),
    )
    orig = rows(first, CORRECTION_ORIG, F.lit(None).cast("bigint"), prev_values=False)
    rev = rows(restated, CORRECTION_REV, prev_key, prev_values=True)
    res = rows(restated, CORRECTION_RES, prev_key, prev_values=False)
    return orig.unionByName(rev).unionByName(res)


DUPLICATE_ARCHIVE_COLUMNS = (
    "natural_key_hash",
    "invoice_number",
    "invoice_line_number",
    "region_code",
    "invoice_date",
    "source_row_version",
    "batch_id",
    "source_system_code",
)


def duplicate_archive(accepted_sale_work: DataFrame, sale_lines: DataFrame) -> DataFrame:
    """Fact.[Sale Duplicate Archive] equivalent, kept because in 2019 the dedup
    deleted 40,000 good rows and there was no way back.

    Two kinds of duplicate: replays inside one extract batch that lost to a
    higher source row version (legacy step 3), and later batches whose net
    amount did not change (legacy FACT_Dedup_Sale)."""
    _survivors, within_batch = dedup_within_batch(accepted_sale_work)
    within_batch = within_batch.select(*DUPLICATE_ARCHIVE_COLUMNS).withColumn(
        "archive_reason", F.lit("REPLAY_WITHIN_BATCH")
    )
    w = _version_window()
    across_batches = (
        sale_lines.withColumn("_version_no", F.row_number().over(w))
        .withColumn("_prev_net", F.lag("net_amount").over(w))
        .where((F.col("_version_no") > 1) & (F.round(F.col("net_amount"), 2) == F.round(F.col("_prev_net"), 2)))
        .select(*DUPLICATE_ARCHIVE_COLUMNS)
        .withColumn("archive_reason", F.lit("UNCHANGED_ACROSS_BATCHES"))
    )
    return within_batch.unionByName(across_batches)
