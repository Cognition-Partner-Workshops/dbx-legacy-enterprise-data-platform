"""STG_Load_* packages: bronze -> silver conformance (stg.Supplier / stg.PurchaseOrder /
stg.PurchaseOrderLine / stg.VendorContract equivalents)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from procurement import io
from procurement.config import HIGH_DATE, LEGACY_ORACLE, LOW_DATE, SOURCE_SYSTEM_ORACLE, qualified
from procurement.extracts import (
    BRONZE_PO_HDR,
    BRONZE_PO_LINE,
    BRONZE_SUPPLIER,
    BRONZE_VENDOR_CONTRACT,
    convertToUsd,
    lowerColumns,
)

SILVER_SUPPLIER = "silver_supplier"
SILVER_PURCHASE_ORDER = "silver_purchase_order"
SILVER_PURCHASE_ORDER_LINE = "silver_purchase_order_line"
SILVER_VENDOR_CONTRACT = "silver_vendor_contract"

SUPPLIER_STATUS_MAP = {"AC": "ACTV", "ACTV": "ACTV", "BL": "BLCK", "BLCK": "BLCK", "IN": "INAC", "INAC": "INAC", "PEND": "PEND"}
SUPPLIER_TYPE2_COLUMNS = [
    "supplier_name", "supplier_status_code", "payment_terms_code", "payment_method_code",
    "transaction_currency_code", "region_code", "country_code", "preferred_supplier_flag", "lead_time_days",
]
SUPPLIER_TYPE1_COLUMNS = ["tax_identifier", "vat_registration_number", "certification_expired_flag", "withholding_applies", "strategic_tier_code"]


def normalizeTaxId(col):
    return F.when(col.isNull() | (F.length(F.trim(col)) == 0), F.lit("NONE")).otherwise(
        F.upper(F.regexp_replace(F.trim(col), "[- ]", ""))
    )


def hashColumns(cols):
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)


# --------------------------------------------------------------------------------------
# STG_Load_Supplier  (truncate_reload; normalise, default, survivorship, lookup rejects)
# --------------------------------------------------------------------------------------
def transformSupplier(bronzeSupplier: DataFrame, paymentTerms: DataFrame, batchId):
    """Returns (silverSupplier, rejects). Survivorship keeps the latest UPDATED_DT per supplier
    number (stg.usp_DeduplicateSupplier); older duplicates stay with is_survivor_row = false."""
    statusMap = F.create_map(*[F.lit(x) for kv in SUPPLIER_STATUS_MAP.items() for x in kv])
    s = bronzeSupplier.select(
        F.upper(F.trim(F.col("supp_nbr"))).alias("supplier_business_key"),
        F.lit(SOURCE_SYSTEM_ORACLE).alias("source_system_code"),
        F.col("supp_id").cast("long").alias("source_supplier_id"),
        F.upper(F.trim(F.col("supp_name"))).alias("supplier_name"),
        F.coalesce(statusMap[F.upper(F.trim(F.col("supp_status_cd")))], F.lit("PEND")).alias("supplier_status_code"),
        F.upper(F.trim(F.col("approval_status_cd"))).alias("approval_status_code"),
        normalizeTaxId(F.col("tax_id_nbr")).alias("tax_identifier"),
        F.upper(F.trim(F.col("vat_reg_nbr"))).alias("vat_registration_number"),
        F.upper(F.trim(F.coalesce(F.col("payment_terms_cd"), F.lit("NET30")))).alias("payment_terms_code"),
        F.upper(F.trim(F.col("payment_method_cd"))).alias("payment_method_code"),
        F.coalesce(F.upper(F.trim(F.col("default_curr_cd"))), F.lit("USD")).alias("transaction_currency_code"),
        F.coalesce(F.col("lead_time_days").cast("int"), F.lit(14)).alias("lead_time_days"),
        F.coalesce(F.upper(F.trim(F.col("region_cd"))), F.lit("NA")).alias("region_code"),
        F.upper(F.trim(F.col("country_cd"))).alias("country_code"),
        F.coalesce(F.col("preferred_supplier_flg"), F.lit("N")).alias("preferred_supplier_flag"),
        F.col("strategic_tier_cd").alias("strategic_tier_code"),
        F.col("certification_expired_flag"),
        F.col("withholding_applies"),
        F.col("quality_cert_cd").alias("quality_certification_code"),
        F.col("cert_expiry_dt").cast("timestamp").alias("certification_expiry_date"),
        F.col("updated_dt").cast("timestamp").alias("source_modified_date"),
        F.col("package_execution_id"),
    )
    terms = lowerColumns(paymentTerms).select(F.upper(F.trim(F.col("payment_terms_cd"))).alias("payment_terms_code"), F.col("net_days").alias("payment_days")).distinct()
    joined = s.join(terms, "payment_terms_code", "left")
    joined = joined.withColumn(
        "reject_reason_code",
        F.when((F.col("tax_identifier") == "NONE") & (F.col("supplier_status_code") != "PEND"), "MISSING_TAX_ID")
        .when(F.col("payment_days").isNull(), "UNKNOWN_PAYMENT_TERMS")
        .otherwise(F.lit(None).cast("string")),
    )
    rejects = joined.where(F.col("reject_reason_code").isNotNull())
    survivorWindow = Window.partitionBy("supplier_business_key").orderBy(F.col("source_modified_date").desc_nulls_last(), F.col("source_supplier_id").desc())
    good = (
        joined.where(F.col("reject_reason_code").isNull())
        .drop("reject_reason_code")
        .withColumn("is_survivor_row", F.row_number().over(survivorWindow) == 1)
        .withColumn("row_hash", hashColumns(SUPPLIER_TYPE2_COLUMNS + SUPPLIER_TYPE1_COLUMNS))
        .withColumn("change_hash", hashColumns(SUPPLIER_TYPE2_COLUMNS))
        .withColumn("type1_hash", hashColumns(SUPPLIER_TYPE1_COLUMNS))
        .withColumn("dq_status_code", F.lit("PASS"))
        .withColumn("batch_id", F.lit(int(batchId)).cast("long"))
        .withColumn("loaded_at_utc", F.current_timestamp())
    )
    return good, rejects


def runSupplier(spark, batchId):
    packageName = "STG_Load_Supplier"
    bronze = spark.table(qualified(BRONZE_SUPPLIER))
    terms = spark.table(f"{LEGACY_ORACLE}.wwi_fin.payment_terms")
    good, rejects = transformSupplier(bronze, terms, batchId)
    rows = good.count()
    io.writeDelta(good, qualified(SILVER_SUPPLIER))
    rejected = io.appendRejects(spark, rejects, batchId, packageName, "err.RejectedLookup", "reject_reason_code", "supplier_business_key")
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows + rejected, rowsInserted=rows, rowsRejected=rejected)
    return rows


# --------------------------------------------------------------------------------------
# STG_Load_PurchaseOrder  (incremental_append; header + line conformance)
# --------------------------------------------------------------------------------------
def transformPurchaseOrder(bronzeHdr: DataFrame, bronzeLine: DataFrame, productMaster: DataFrame, uomRef: DataFrame, batchId):
    """Returns (silverHeaders, silverLines, lineRejects). Only the header rows of the current
    batch are appended; lines are joined to the latest header version known for their PO."""
    latestHdr = Window.partitionBy("po_id").orderBy(F.col("updated_dt").desc(), F.col("batch_id").desc())
    hdr = bronzeHdr.withColumn("_rn", F.row_number().over(latestHdr)).where("_rn = 1").drop("_rn")
    headers = hdr.select(
        F.col("po_id").cast("long").alias("purchase_order_business_key"),
        F.upper(F.trim(F.col("po_nbr"))).alias("purchase_order_number"),
        F.col("supp_id").cast("long").alias("source_supplier_id"),
        F.upper(F.trim(F.col("po_type_cd"))).alias("purchase_order_type_code"),
        F.coalesce(F.upper(F.trim(F.col("region_cd"))), F.lit("NA")).alias("region_code"),
        F.to_date("order_dt").alias("order_date"),
        F.to_date("promised_dt").alias("promised_date"),
        F.upper(F.trim(F.col("buyer_cd"))).alias("buyer_code"),
        F.col("contract_id").cast("long").alias("contract_business_key"),
        F.upper(F.trim(F.col("po_status_cd"))).alias("order_status_code"),
        F.upper(F.trim(F.col("approval_status_cd"))).alias("approval_status_code"),
        F.coalesce(F.upper(F.trim(F.col("order_curr_cd"))), F.lit("USD")).alias("transaction_currency_code"),
        F.col("fx_rate").cast("decimal(18,8)").alias("fx_rate"),
        F.col("fx_rate_to_usd").cast("decimal(18,8)").alias("fx_rate_to_usd"),
        F.col("subtotal_amt").cast("decimal(19,4)").alias("subtotal_amount"),
        F.col("freight_amt").cast("decimal(19,4)").alias("freight_amount"),
        F.col("tax_amt").cast("decimal(19,4)").alias("tax_amount"),
        F.col("total_amt").cast("decimal(19,4)").alias("order_total_amount"),
        F.col("po_total_base_amt").cast("decimal(19,4)").alias("order_total_amount_usd"),
        F.upper(F.trim(F.col("payment_terms_cd"))).alias("payment_terms_code"),
        F.upper(F.trim(F.col("incoterm_cd"))).alias("incoterm_code"),
        F.col("ship_to_location_cd").alias("ship_to_location_code"),
        F.col("is_cancelled"),
        F.col("late_flag"),
        F.col("updated_dt").cast("timestamp").alias("source_modified_date"),
        F.col("batch_id").alias("source_batch_id"),
    ).withColumn("batch_id", F.lit(int(batchId)).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())

    products = lowerColumns(productMaster).select(F.col("product_id").cast("long").alias("product_id"), F.lit(True).alias("_product_known")).distinct()
    uomCols = [c.lower() for c in uomRef.columns]
    uom = lowerColumns(uomRef).select(
        F.upper(F.trim(F.col("uom_cd"))).alias("uom_code"),
        (F.col("conv_factor") if "conv_factor" in uomCols else F.lit(1.0)).cast("decimal(18,6)").alias("uom_factor"),
        (F.col("base_uom_cd") if "base_uom_cd" in uomCols else F.col("uom_cd")).alias("base_uom_code"),
    ).distinct()
    lines = bronzeLine.where(F.col("batch_id") == int(batchId)).select(
        F.col("po_line_id").cast("long").alias("purchase_order_line_business_key"),
        F.col("po_id").cast("long").alias("purchase_order_business_key"),
        F.col("line_nbr").cast("int").alias("line_number"),
        F.col("product_id").cast("long").alias("product_id"),
        F.upper(F.trim(F.col("supplier_item_cd"))).alias("supplier_item_code"),
        F.col("order_qty").cast("decimal(18,4)").alias("order_quantity"),
        F.upper(F.trim(F.col("uom_cd"))).alias("uom_code"),
        F.coalesce(F.col("unit_price").cast("decimal(19,4)"), F.lit(0).cast("decimal(19,4)")).alias("unit_price_amount"),
        F.coalesce(F.upper(F.trim(F.col("tax_code_cd"))), F.lit("NONE")).alias("tax_code"),
        F.col("tax_amt").cast("decimal(19,4)").alias("tax_amount"),
        F.coalesce(F.col("received_qty"), F.lit(0)).cast("decimal(18,4)").alias("received_quantity"),
        F.coalesce(F.col("billed_qty"), F.lit(0)).cast("decimal(18,4)").alias("billed_quantity"),
        F.coalesce(F.col("cancelled_qty"), F.lit(0)).cast("decimal(18,4)").alias("cancelled_quantity"),
        F.coalesce(F.to_date("need_by_dt"), F.lit(LOW_DATE).cast("date")).alias("need_by_date"),
        F.upper(F.trim(F.col("line_status_cd"))).alias("line_status_code"),
        F.col("cost_center_cd").alias("cost_center_code"),
        F.col("gl_account_cd").alias("gl_account_code"),
        F.col("updated_dt").cast("timestamp").alias("source_modified_date"),
    )
    lines = lines.join(products, "product_id", "left").join(uom, "uom_code", "left")
    lines = (
        lines.withColumn("uom_factor", F.coalesce(F.col("uom_factor"), F.lit(1.0)))
        .withColumn("order_quantity_base_uom", (F.col("order_quantity") * F.col("uom_factor")).cast("decimal(18,4)"))
        .withColumn("extended_amount", (F.col("order_quantity") * F.col("unit_price_amount")).cast("decimal(19,4)"))
        .withColumn(
            "lookup_failure_list",
            F.concat_ws(",",
                        F.when(~F.coalesce(F.col("_product_known"), F.lit(False)), F.lit("PRODUCT")),
                        F.when(F.col("base_uom_code").isNull(), F.lit("UOM"))),
        )
        .withColumn(
            "reject_reason_code",
            F.when(F.col("order_quantity_base_uom").isNull() | (F.col("order_quantity_base_uom") <= 0), "NONPOSITIVE_QUANTITY")
            .when(F.col("unit_price_amount") < 0, "NEGATIVE_PRICE")
            .otherwise(F.lit(None).cast("string")),
        )
        .drop("_product_known")
    )
    rejects = lines.where(F.col("reject_reason_code").isNotNull())
    good = (
        lines.where(F.col("reject_reason_code").isNull())
        .drop("reject_reason_code")
        .withColumn("dq_status_code", F.when(F.col("lookup_failure_list") != "", "WARN").otherwise("PASS"))
        .withColumn("batch_id", F.lit(int(batchId)).cast("long"))
        .withColumn("loaded_at_utc", F.current_timestamp())
    )
    return headers, good, rejects


def runPurchaseOrder(spark, batchId):
    packageName = "STG_Load_PurchaseOrder"
    hdr = spark.table(qualified(BRONZE_PO_HDR))
    line = spark.table(qualified(BRONZE_PO_LINE))
    products = spark.table(f"{LEGACY_ORACLE}.wwi_mdm.product_master")
    uom = spark.table(f"{LEGACY_ORACLE}.wwi_ref.uom_ref")
    headers, lines, rejects = transformPurchaseOrder(hdr, line, products, uom, batchId)
    # headers: full current snapshot of every PO seen (idempotent overwrite); lines: append per batch
    io.writeDelta(headers, qualified(SILVER_PURCHASE_ORDER))
    lineTable = qualified(SILVER_PURCHASE_ORDER_LINE)
    if io.tableExists(spark, lineTable):
        io.replaceWhere(lines, lineTable, f"batch_id = {int(batchId)}")
    else:
        io.writeDelta(lines, lineTable)
    inserted = lines.count()
    rejected = io.appendRejects(spark, rejects, batchId, packageName, "err.RejectedLookup", "reject_reason_code", "purchase_order_line_business_key")
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=inserted + rejected, rowsInserted=inserted, rowsRejected=rejected)
    return inserted


# --------------------------------------------------------------------------------------
# STG_Load_VendorContract  (truncate_reload; USD conversion, banding, rejects)
# --------------------------------------------------------------------------------------
def transformVendorContract(bronzeContract: DataFrame, silverSupplier: DataFrame, fxRates: DataFrame, batchId):
    c = bronzeContract.select(
        F.col("contract_id").cast("long").alias("contract_business_key"),
        F.upper(F.trim(F.col("contract_nbr"))).alias("contract_number"),
        F.col("supp_id").cast("long").alias("source_supplier_id"),
        F.coalesce(F.upper(F.trim(F.col("contract_type_cd"))), F.lit("STD")).alias("contract_type_code"),
        F.coalesce(F.upper(F.trim(F.col("region_cd"))), F.lit("NA")).alias("region_code"),
        F.to_date("start_dt").alias("contract_start_date"),
        F.coalesce(F.to_date("end_dt"), F.lit(HIGH_DATE).cast("date")).alias("contract_end_date"),
        F.col("notice_period_days").cast("int").alias("notice_period_days"),
        F.coalesce(F.col("auto_renew_flg"), F.lit("N")).alias("auto_renew_flag"),
        F.coalesce(F.upper(F.trim(F.col("contract_curr_cd"))), F.lit("USD")).alias("contract_currency_code"),
        F.coalesce(F.col("committed_amt"), F.lit(0)).cast("decimal(19,4)").alias("committed_amount"),
        F.coalesce(F.col("consumed_amt"), F.lit(0)).cast("decimal(19,4)").alias("consumed_amount"),
        F.col("line_count").cast("int").alias("line_count"),
        F.col("rebate_pct").cast("decimal(9,4)").alias("rebate_percent"),
        F.coalesce(F.col("price_protection_flg"), F.lit("N")).alias("price_protection_flag"),
        F.upper(F.trim(F.col("contract_status_cd"))).alias("source_status_code"),
        F.upper(F.trim(F.col("payment_terms_cd"))).alias("payment_terms_code"),
        F.to_date("signed_dt").alias("signed_date"),
        F.col("utilisation_pct"),
        F.col("renewal_due_flag"),
        F.col("updated_dt").cast("timestamp").alias("source_modified_date"),
    )
    suppliers = silverSupplier.where("is_survivor_row").select("source_supplier_id", "supplier_business_key", F.col("transaction_currency_code").alias("supplier_currency_code"))
    c = c.join(suppliers, "source_supplier_id", "left")
    c = convertToUsd(c, fxRates, "committed_amount", "contract_currency_code", "contract_start_date", "committed_amount_usd")
    c = (
        c.withColumn(
            "contract_band_code",
            F.when(F.col("committed_amount_usd") >= 1000000, "STRATEGIC")
            .when(F.col("committed_amount_usd") >= 100000, "MAJOR")
            .otherwise("TACTICAL"),
        )
        .withColumn(
            "reject_reason_code",
            F.when(F.col("contract_end_date") < F.col("contract_start_date"), "INVERTED_DATES")
            .when(F.col("supplier_business_key").isNull(), "UNKNOWN_SUPPLIER")
            .when(F.col("committed_amount_usd").isNull(), "MISSING_FX_RATE")
            .otherwise(F.lit(None).cast("string")),
        )
    )
    rejects = c.where(F.col("reject_reason_code").isNotNull())
    good = (
        c.where(F.col("reject_reason_code").isNull())
        .drop("reject_reason_code")
        .withColumn("row_hash", hashColumns(["contract_number", "supplier_business_key", "contract_type_code", "contract_currency_code",
                                             "committed_amount", "rebate_percent", "contract_start_date", "contract_end_date", "region_code", "source_status_code"]))
        .withColumn("batch_id", F.lit(int(batchId)).cast("long"))
        .withColumn("loaded_at_utc", F.current_timestamp())
    )
    return good, rejects


def runVendorContract(spark, batchId):
    packageName = "STG_Load_VendorContract"
    bronze = spark.table(qualified(BRONZE_VENDOR_CONTRACT))
    suppliers = spark.table(qualified(SILVER_SUPPLIER))
    fx = spark.table(f"{LEGACY_ORACLE}.wwi_ref.fx_rate_daily")
    good, rejects = transformVendorContract(bronze, suppliers, fx, batchId)
    rows = good.count()
    io.writeDelta(good, qualified(SILVER_VENDOR_CONTRACT))
    rejected = io.appendRejects(spark, rejects, batchId, packageName, "err.RejectedLookup", "reject_reason_code", "contract_number")
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows + rejected, rowsInserted=rows, rowsRejected=rejected)
    return rows
