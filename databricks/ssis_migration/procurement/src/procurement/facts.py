"""FACT_Load_Purchase / FACT_Load_PurchaseReceipt / FACT_Load_SupplierTransaction."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from procurement import io
from procurement.config import (
    HIGH_TS,
    LEGACY_DW,
    LEGACY_OLTP,
    LEGACY_ORACLE,
    LOW_TS,
    SOURCE_SYSTEM_OLTP,
    SOURCE_SYSTEM_ORACLE,
    UNKNOWN_KEY,
    qualified,
)
from procurement.dimensions import GOLD_DIM_SUPPLIER, GOLD_DIM_VENDOR_CONTRACT
from procurement.extracts import BRONZE_RECEIPT_LINE, BRONZE_SUPPLIER_TRANSACTION, lowerColumns
from procurement.staging import SILVER_PURCHASE_ORDER, SILVER_PURCHASE_ORDER_LINE, SILVER_SUPPLIER

GOLD_FACT_PURCHASE = "gold_fact_purchase"
SILVER_PURCHASE = "silver_purchase"
GOLD_FACT_PURCHASE_P2P = "gold_fact_purchase_p2p"
GOLD_FACT_PURCHASE_RECEIPT = "gold_fact_purchase_receipt"
SILVER_SUPPLIER_TRANSACTION = "silver_supplier_transaction"
GOLD_FACT_SUPPLIER_TRANSACTION = "gold_fact_supplier_transaction"
LEGACY_PURCHASE_LINEAGE_KEY = 10

PURCHASE_BUSINESS_COLUMNS = [
    "date_key", "supplier_key", "stock_item_key", "wwi_purchase_order_id", "ordered_outers", "ordered_quantity",
    "received_outers", "package", "is_order_finalized",
]


# --------------------------------------------------------------------------------------
# FACT_Load_Purchase - legacy lineage (WWI OLTP purchase order lines -> Fact.Purchase grain)
# --------------------------------------------------------------------------------------
def resolveSupplierKey(df: DataFrame, dimSupplier: DataFrame, idCol="wwi_supplier_id") -> DataFrame:
    """Current dimension row per WWI supplier id (lowest key when several are current);
    unknown member 0 when absent (late arriving supplier)."""
    keys = (
        dimSupplier.where(F.col("valid_to") >= F.lit(HIGH_TS).cast("timestamp"))
        .groupBy(F.col("wwi_supplier_id").alias("_sid"))
        .agg(F.min("supplier_key").alias("supplier_key"))
    )
    return df.join(keys, F.col(idCol) == F.col("_sid"), "left").drop("_sid").withColumn(
        "supplier_key", F.coalesce(F.col("supplier_key"), F.lit(UNKNOWN_KEY)).cast("int")
    )


def resolveStockItemKey(df: DataFrame, dimStockItem: DataFrame, idCol="wwi_stock_item_id", dateCol="date_key") -> DataFrame:
    """Version of the stock item valid on the order date (Valid From <= date < Valid To)."""
    versions = dimStockItem.select(
        F.col("wwi_stock_item_id").alias("_iid"), F.col("stock_item_key").alias("_ik"),
        F.col("valid_from").alias("_vf"), F.col("valid_to").alias("_vt"),
    )
    joined = df.join(
        versions,
        (F.col(idCol) == F.col("_iid")) & (F.col(dateCol).cast("timestamp") >= F.col("_vf")) & (F.col(dateCol).cast("timestamp") < F.col("_vt")),
        "left",
    )
    pick = Window.partitionBy(*df.columns).orderBy(F.col("_vf").desc_nulls_last())
    return (
        joined.withColumn("_rn", F.row_number().over(pick)).where("_rn = 1")
        .withColumn("stock_item_key", F.coalesce(F.col("_ik"), F.lit(UNKNOWN_KEY)).cast("int"))
        .drop("_iid", "_ik", "_vf", "_vt", "_rn")
    )


def buildLegacyPurchaseFact(poHdr: DataFrame, poLine: DataFrame, stockItems: DataFrame, packageTypes: DataFrame,
                            dimSupplier: DataFrame, dimStockItem: DataFrame, watermarkFrom, watermarkTo) -> DataFrame:
    """Integration.GetPurchaseUpdates + MigrateStagedPurchaseData: purchase-order-line grain,
    incremental on LastEditedWhen."""
    h = poHdr.select(F.col("PurchaseOrderID").alias("wwi_purchase_order_id"), F.col("SupplierID").alias("wwi_supplier_id"),
                     F.col("OrderDate").alias("date_key"), F.col("LastEditedWhen").alias("_hdr_edited"))
    ln = poLine.select(F.col("PurchaseOrderLineID").alias("_line_id"), F.col("PurchaseOrderID").alias("wwi_purchase_order_id"),
                      F.col("StockItemID").alias("wwi_stock_item_id"), F.col("OrderedOuters").alias("ordered_outers"),
                      F.col("ReceivedOuters").alias("received_outers"), F.col("PackageTypeID").alias("_pkg"),
                      F.col("IsOrderLineFinalized").alias("is_order_finalized"), F.col("LastEditedWhen").alias("_line_edited"))
    si = stockItems.select(F.col("StockItemID").alias("wwi_stock_item_id"), F.col("QuantityPerOuter").alias("_qpo"))
    pt = packageTypes.select(F.col("PackageTypeID").alias("_pkg"), F.col("PackageTypeName").alias("package"))
    rows = (
        ln.join(h, "wwi_purchase_order_id", "inner").join(si, "wwi_stock_item_id", "inner").join(pt, "_pkg", "inner")
        .withColumn("_edited", F.greatest(F.col("_hdr_edited"), F.col("_line_edited")))
        .where((F.col("_edited") > F.lit(watermarkFrom).cast("timestamp")) & (F.col("_edited") <= F.lit(watermarkTo).cast("timestamp")))
        .withColumn("ordered_quantity", (F.col("ordered_outers") * F.col("_qpo")).cast("int"))
        .drop("_qpo", "_pkg", "_hdr_edited", "_line_edited")
    )
    rows = resolveSupplierKey(rows, dimSupplier)
    rows = resolveStockItemKey(rows, dimStockItem)
    return rows.select(
        F.col("_line_id").alias("wwi_purchase_order_line_id"), "date_key", "supplier_key", "stock_item_key", "wwi_purchase_order_id",
        F.col("ordered_outers").cast("int"), "ordered_quantity", F.col("received_outers").cast("int"), "package",
        F.col("is_order_finalized").cast("boolean"), F.col("_edited").alias("last_modified_when"),
    ).withColumn("lineage_key", F.lit(LEGACY_PURCHASE_LINEAGE_KEY).cast("int"))


def assignPurchaseKeys(fact: DataFrame, startKey) -> DataFrame:
    w = Window.orderBy("wwi_purchase_order_id", "wwi_purchase_order_line_id")
    return fact.withColumn("purchase_key", (F.row_number().over(w) + F.lit(int(startKey))).cast("long"))


def legacyStockItemDimension(spark) -> DataFrame:
    return lowerColumns(
        io.readLegacySql(spark, "SELECT [Stock Item Key] AS stock_item_key, [WWI Stock Item ID] AS wwi_stock_item_id, "
                                "[Valid From] AS valid_from, [Valid To] AS valid_to FROM Dimension.[Stock Item]")
    )


# --------------------------------------------------------------------------------------
# FACT_Load_Purchase - Oracle lineage (stg.usp_ConformPurchaseForFact + Integration.usp_LoadFactPurchase)
# --------------------------------------------------------------------------------------
def conformPurchase(silverPo: DataFrame, silverPoLine: DataFrame, silverSupplier: DataFrame, receipts: DataFrame,
                    apInvoiceLines: DataFrame, batchId) -> DataFrame:
    """stg.Purchase equivalent: PO-line grain enriched with supplier region, apportioned header freight,
    recoverable tax by regional rule, receipted/invoiced quantities and the three-way match state."""
    po = silverPo.select(
        "purchase_order_business_key", "purchase_order_number", "source_supplier_id", "order_date", "promised_date",
        F.col("region_code").alias("po_region_code"), "buyer_code", "contract_business_key", "transaction_currency_code",
        "fx_rate_to_usd", "freight_amount", "order_total_amount", "order_status_code",
        F.col("source_modified_date").alias("header_modified_date"),
    )
    supp = silverSupplier.where("is_survivor_row").select(
        "source_supplier_id", "supplier_business_key", F.col("region_code").alias("supplier_region_code"),
        F.col("vat_registration_number").isNotNull().alias("_vat_eligible"),
    )
    rc = lowerColumns(receipts).groupBy(F.col("po_line_id").alias("purchase_order_line_business_key")).agg(
        F.sum("accepted_qty").alias("receipted_quantity")
    )
    il = lowerColumns(apInvoiceLines).where(F.col("po_line_id").isNotNull()).groupBy(F.col("po_line_id").alias("purchase_order_line_business_key")).agg(
        F.sum("quantity").alias("invoiced_quantity"), F.max("unit_price").alias("invoiced_unit_price")
    )
    line = silverPoLine.where(F.col("batch_id") == int(batchId))
    j = (
        line.join(po, "purchase_order_business_key", "inner")
        .join(supp, "source_supplier_id", "left")
        .join(rc, "purchase_order_line_business_key", "left")
        .join(il, "purchase_order_line_business_key", "left")
    )
    j = (
        j.withColumn("receipted_quantity", F.coalesce(F.col("receipted_quantity"), F.lit(0)).cast("decimal(18,4)"))
        .withColumn("invoiced_quantity", F.coalesce(F.col("invoiced_quantity"), F.lit(0)).cast("decimal(18,4)"))
        .withColumn("supplier_region_code", F.coalesce(F.col("supplier_region_code"), F.col("po_region_code"), F.lit("NA")))
        .withColumn(
            "recoverable_tax_amount",
            F.when((F.col("supplier_region_code") == "EU") & F.col("_vat_eligible"), F.coalesce(F.col("tax_amount"), F.lit(0)))
            .when(F.col("supplier_region_code") == "APAC", F.coalesce(F.col("tax_amount"), F.lit(0)))
            .otherwise(F.lit(0)).cast("decimal(19,4)"),
        )
        .withColumn(
            "three_way_match_status_code",
            F.when(F.col("invoiced_quantity") == 0, "UNMATCHED")
            .when(F.col("receipted_quantity") == 0, "TWO_WAY")
            .when(F.abs(F.col("receipted_quantity") - F.col("invoiced_quantity")) <= 0.001, "MATCHED")
            .otherwise("QTY_VARIANCE"),
        )
        .withColumn(
            "price_variance_percent",
            F.when(F.col("unit_price_amount") == 0, F.lit(None).cast("decimal(9,4)")).otherwise(
                F.round(100.0 * (F.coalesce(F.col("invoiced_unit_price"), F.col("unit_price_amount")) - F.col("unit_price_amount")) / F.col("unit_price_amount"), 4)
            ),
        )
        .withColumn(
            "freight_amount",
            F.when(F.coalesce(F.col("order_total_amount"), F.lit(0)) == 0, F.lit(0)).otherwise(
                F.round(F.coalesce(F.col("freight_amount"), F.lit(0)) * F.coalesce(F.col("extended_amount"), F.lit(0)) / F.col("order_total_amount"), 2)
            ).cast("decimal(19,4)"),
        )
        .withColumn("extended_amount_usd", F.round(F.col("extended_amount") * F.coalesce(F.col("fx_rate_to_usd"), F.lit(1)), 2).cast("decimal(19,4)"))
        .withColumn(
            "dq_status_code",
            F.when(F.col("supplier_business_key").isNull(), "FAIL")
            .when(F.col("order_quantity").isNull(), "FAIL")
            .when(F.abs(F.coalesce(F.col("price_variance_percent"), F.lit(0))) > 10, "WARN")
            .otherwise(F.col("dq_status_code")),
        )
    )
    return j.select(
        "purchase_order_line_business_key", F.lit(SOURCE_SYSTEM_ORACLE).alias("source_system_code"), "purchase_order_number",
        F.col("line_number").alias("purchase_order_line_number"), "supplier_business_key", "source_supplier_id",
        F.col("supplier_item_code").alias("stock_item_business_key"), F.col("order_date").alias("order_placed_date"),
        F.coalesce(F.col("need_by_date"), F.col("promised_date")).alias("expected_receipt_date"),
        F.col("order_quantity").alias("quantity_ordered"), F.col("unit_price_amount").alias("unit_cost_amount"), "freight_amount",
        F.col("transaction_currency_code").alias("transaction_currency"), "fx_rate_to_usd", "extended_amount", "extended_amount_usd",
        "recoverable_tax_amount", "supplier_region_code", "buyer_code", "contract_business_key", "order_status_code",
        "receipted_quantity", "invoiced_quantity", "three_way_match_status_code", "price_variance_percent",
        F.greatest(F.col("source_modified_date"), F.col("header_modified_date")).alias("last_modified_at"), "dq_status_code",
        F.sha2(F.concat_ws("|", F.col("purchase_order_line_business_key"), F.col("order_quantity"), F.col("unit_price_amount"), F.col("order_date"), F.col("three_way_match_status_code")), 256).alias("row_hash"),
        F.lit(int(batchId)).cast("long").alias("batch_id"),
    )


def landedCostAmount(regionCol, extendedCol, freightCol, dutyCol, insuranceCol):
    """Integration.usp_LoadFactPurchase regional landed-cost rule."""
    return (
        F.when(regionCol == "EU", extendedCol + freightCol + dutyCol)
        .when(regionCol == "APAC", extendedCol + freightCol + dutyCol + insuranceCol)
        .otherwise(extendedCol)
    )


def buildPurchaseP2pFact(silverPurchase: DataFrame, dimSupplier: DataFrame, dimVendorContract: DataFrame, batchId) -> DataFrame:
    """Oracle-sourced purchase fact rows (Fact.Purchase extension columns) with dimension keys, landed
    cost, reporting currency and lead time; rows failing DQ are excluded (they stay in silver_purchase
    with dq_status_code FAIL, the SSIS 'hold queue')."""
    supplierKeys = dimSupplier.where("is_current_row").select(F.col("supplier_business_key"), F.col("supplier_key").alias("_sk"))
    contractKeys = dimVendorContract.where("is_current_row").select(
        F.col("source_supplier_id").alias("_csid"), F.col("contract_business_key").alias("_cbk") if "contract_business_key" in dimVendorContract.columns else F.lit(None).alias("_cbk"),
        F.col("vendor_contract_key").alias("_vck"), F.col("contract_number").alias("_cn"),
    ) if "vendor_contract_key" in dimVendorContract.columns else None
    p = silverPurchase.where((F.col("batch_id") == int(batchId)) & (F.col("dq_status_code") != "FAIL"))
    p = p.join(supplierKeys, "supplier_business_key", "left").withColumn("supplier_key", F.coalesce(F.col("_sk"), F.lit(UNKNOWN_KEY)).cast("int")).drop("_sk")
    if contractKeys is not None:
        p = p.join(contractKeys, (F.col("source_supplier_id") == F.col("_csid")) & (F.col("contract_business_key") == F.col("_cbk")), "left")
        p = p.withColumn("vendor_contract_key", F.coalesce(F.col("_vck"), F.lit(UNKNOWN_KEY)).cast("int")).withColumn("contract_number", F.col("_cn")).drop("_csid", "_cbk", "_vck", "_cn")
    else:
        p = p.withColumn("vendor_contract_key", F.lit(UNKNOWN_KEY)).withColumn("contract_number", F.lit(None).cast("string"))
    zero = F.lit(0).cast("decimal(19,4)")
    return p.select(
        "purchase_order_line_business_key", "source_system_code", "purchase_order_number", "purchase_order_line_number",
        F.col("order_placed_date").alias("date_key"), "supplier_key", "vendor_contract_key", "contract_number",
        F.col("stock_item_business_key"), F.col("supplier_region_code").alias("region_code"), "buyer_code",
        F.col("transaction_currency").alias("transaction_currency_code"), F.col("fx_rate_to_usd").alias("fx_rate_to_reporting"),
        F.col("quantity_ordered"), F.col("unit_cost_amount").alias("unit_cost"),
        F.col("extended_amount").alias("extended_cost"), F.col("freight_amount").alias("freight_in_amount"),
        zero.alias("customs_duty_amount"), zero.alias("insurance_amount"), "recoverable_tax_amount",
        landedCostAmount(F.col("supplier_region_code"), F.col("extended_amount"), F.col("freight_amount"), zero, zero).cast("decimal(19,4)").alias("landed_cost_amount"),
        F.round(landedCostAmount(F.col("supplier_region_code"), F.col("extended_amount"), F.col("freight_amount"), zero, zero) * F.coalesce(F.col("fx_rate_to_usd"), F.lit(1)), 2).cast("decimal(19,4)").alias("landed_cost_reporting"),
        F.col("extended_amount_usd").alias("extended_cost_reporting"),
        F.datediff(F.col("expected_receipt_date"), F.col("order_placed_date")).alias("lead_time_days"),
        F.col("expected_receipt_date").alias("expected_receipt_date_key"),
        F.col("receipted_quantity").alias("quantity_received_base_uom"), F.col("invoiced_quantity").alias("quantity_invoiced_base_uom"),
        F.col("three_way_match_status_code").alias("match_status_code"), "order_status_code",
        (F.col("supplier_key") == UNKNOWN_KEY).alias("inferred_member_flag"),
        F.sha2(F.concat_ws("|", F.col("source_system_code"), F.col("purchase_order_line_business_key")), 256).alias("natural_key_hash"),
        F.lit(int(batchId)).cast("long").alias("lineage_key"), F.lit(int(batchId)).cast("long").alias("batch_id"), F.current_timestamp().alias("load_datetime"),
    )


def runFactPurchase(spark, batchId):
    packageName = "FACT_Load_Purchase"
    # --- legacy lineage: WWI OLTP -> gold_fact_purchase (exact Fact.Purchase reproduction)
    objectName = "Purchasing.PurchaseOrderLines"
    wmFrom = io.getWatermark(spark, f"{SOURCE_SYSTEM_OLTP}:FACT", objectName, LOW_TS)
    wmTo = spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS ts").collect()[0]["ts"]
    legacyDimSupplier = lowerColumns(
        spark.table(f"{LEGACY_DW}.Dimension.Supplier").select(F.col("`Supplier Key`").alias("supplier_key"), F.col("`WWI Supplier ID`").alias("wwi_supplier_id"),
                                                               F.col("`Valid From`").alias("valid_from"), F.col("`Valid To`").alias("valid_to"))
    )
    fact = buildLegacyPurchaseFact(
        spark.table(f"{LEGACY_OLTP}.Purchasing.PurchaseOrders"), spark.table(f"{LEGACY_OLTP}.Purchasing.PurchaseOrderLines"),
        spark.table(f"{LEGACY_OLTP}.Warehouse.StockItems"), spark.table(f"{LEGACY_OLTP}.Warehouse.PackageTypes"),
        legacyDimSupplier, legacyStockItemDimension(spark), wmFrom, wmTo,
    ).withColumn("batch_id", F.lit(int(batchId)).cast("long")).withColumn("load_datetime", F.current_timestamp())
    target = qualified(GOLD_FACT_PURCHASE)
    startKey = 0
    if io.tableExists(spark, target):
        startKey = spark.table(target).agg(F.coalesce(F.max("purchase_key"), F.lit(0))).collect()[0][0]
        # replay window: rows already present for the same purchase order lines are replaced
        fact.select("wwi_purchase_order_line_id").createOrReplaceTempView("_fp_keys")
        spark.sql(f"DELETE FROM {target} WHERE wwi_purchase_order_line_id IN (SELECT wwi_purchase_order_line_id FROM _fp_keys)")
        io.appendDelta(assignPurchaseKeys(fact, startKey), target)
    else:
        io.writeDelta(assignPurchaseKeys(fact, startKey), target)
    legacyRows = spark.table(target).count()
    io.setWatermark(spark, f"{SOURCE_SYSTEM_OLTP}:FACT", objectName, wmTo, "timestamp", batchId)

    # --- Oracle lineage: silver PO -> silver_purchase -> gold_fact_purchase_p2p
    purchase = conformPurchase(
        spark.table(qualified(SILVER_PURCHASE_ORDER)), spark.table(qualified(SILVER_PURCHASE_ORDER_LINE)), spark.table(qualified(SILVER_SUPPLIER)),
        spark.table(qualified(BRONZE_RECEIPT_LINE)), spark.table(f"{LEGACY_ORACLE}.wwi_fin.ap_invoice_line"), batchId,
    )
    silverTarget = qualified(SILVER_PURCHASE)
    if io.tableExists(spark, silverTarget):
        io.replaceWhere(purchase, silverTarget, f"batch_id = {int(batchId)}")
    else:
        io.writeDelta(purchase, silverTarget)
    dimVc = spark.table(qualified(GOLD_DIM_VENDOR_CONTRACT))
    p2p = buildPurchaseP2pFact(spark.table(silverTarget), spark.table(qualified(GOLD_DIM_SUPPLIER)), dimVc, batchId)
    p2pTarget = qualified(GOLD_FACT_PURCHASE_P2P)
    if io.tableExists(spark, p2pTarget):
        io.replaceWhere(p2p, p2pTarget, f"batch_id = {int(batchId)}")
    else:
        io.writeDelta(p2p, p2pTarget)
    held = spark.table(silverTarget).where(f"batch_id = {int(batchId)} AND dq_status_code = 'FAIL'").count()
    p2pRows = spark.table(p2pTarget).where(f"batch_id = {int(batchId)}").count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=fact.count(), rowsInserted=p2pRows, rowsRejected=held,
                     watermarkFrom=wmFrom, watermarkTo=wmTo, message=f"legacy_lineage_rows={legacyRows}; oracle_p2p_rows={p2pRows}; held_rows={held}")
    return legacyRows


# --------------------------------------------------------------------------------------
# FACT_Load_PurchaseReceipt
# --------------------------------------------------------------------------------------
def onTimeFlag(regionCol, daysLateCol):
    """Regional grace: NA none, EU 2 days, APAC 3 days (usp_LoadFactPurchaseReceipt)."""
    grace = F.when(regionCol == "EU", 2).when(regionCol == "APAC", 3).otherwise(0)
    return F.when(daysLateCol.isNull(), F.lit(None).cast("boolean")).otherwise(daysLateCol <= grace)


def buildPurchaseReceiptFact(receipts: DataFrame, silverPoLine: DataFrame, silverPo: DataFrame, dimSupplier: DataFrame, keyFrom, batchId) -> DataFrame:
    latestLine = Window.partitionBy("purchase_order_line_business_key").orderBy(F.col("batch_id").desc())
    lines = silverPoLine.withColumn("_rn", F.row_number().over(latestLine)).where("_rn = 1").select(
        F.col("purchase_order_line_business_key").alias("po_line_id"), "purchase_order_business_key", F.col("line_number").alias("purchase_order_line_number"),
        F.col("order_quantity_base_uom").alias("quantity_ordered_base_uom"), F.col("unit_price_amount").alias("po_unit_price"),
        F.col("uom_factor"), F.col("need_by_date"),
    )
    po = silverPo.select("purchase_order_business_key", "purchase_order_number", "source_supplier_id", "order_date", "promised_date",
                         F.col("region_code").alias("po_region_code"), "transaction_currency_code", "fx_rate_to_usd", "contract_business_key")
    supplierKeys = dimSupplier.where("is_current_row").select(F.col("wwi_supplier_id").alias("source_supplier_id"), F.col("supplier_key").alias("_sk"), F.col("region_code").alias("_sregion"))
    r = receipts.drop("po_unit_price", "po_order_qty").where(F.col("receipt_line_id") > int(keyFrom))
    j = r.join(lines, "po_line_id", "left").join(po, "purchase_order_business_key", "left")
    j = j.withColumn("source_supplier_id", F.coalesce(F.col("source_supplier_id"), F.col("supp_id").cast("long")))
    j = j.join(supplierKeys, "source_supplier_id", "left")
    j = (
        j.withColumn("supplier_key", F.coalesce(F.col("_sk"), F.lit(UNKNOWN_KEY)).cast("int"))
        .withColumn("region_code", F.coalesce(F.col("_sregion"), F.col("po_region_code"), F.col("receipt_region_cd"), F.lit("NA")))
        .withColumn("receipt_date_key", F.to_date("receipt_dt"))
        .withColumn("promise_date", F.coalesce(F.col("promised_date"), F.col("need_by_date")))
        .withColumn("days_late_versus_promise", F.datediff(F.col("receipt_date_key"), F.col("promise_date")))
        .withColumn("lead_time_days", F.datediff(F.col("receipt_date_key"), F.col("order_date")))
        .withColumn("quantity_received_base_uom", (F.col("received_qty") * F.coalesce(F.col("uom_factor"), F.lit(1))).cast("decimal(18,4)"))
        .withColumn("quantity_rejected_base_uom", (F.coalesce(F.col("rejected_qty"), F.lit(0)) * F.coalesce(F.col("uom_factor"), F.lit(1))).cast("decimal(18,4)"))
        .withColumn("quantity_on_quality_hold", F.when(F.col("is_quarantined"), F.col("quantity_received_base_uom")).otherwise(F.lit(0)).cast("decimal(18,4)"))
        .withColumn("over_receipt_quantity", F.greatest(F.lit(0), F.col("quantity_received_base_uom") - F.coalesce(F.col("quantity_ordered_base_uom"), F.col("quantity_received_base_uom"))).cast("decimal(18,4)"))
        .withColumn("receipt_value", (F.col("received_qty") * F.col("unit_cost")).cast("decimal(19,4)"))
        .withColumn("receipt_value_reporting", F.round(F.col("received_qty") * F.col("unit_cost") * F.coalesce(F.col("fx_rate_to_usd"), F.lit(1)), 2).cast("decimal(19,4)"))
        .withColumn("price_variance_amount", ((F.col("unit_cost") - F.coalesce(F.col("po_unit_price"), F.col("unit_cost"))) * F.col("received_qty")).cast("decimal(19,4)"))
        .withColumn("on_time_flag", onTimeFlag(F.col("region_code"), F.col("days_late_versus_promise")))
        .withColumn("in_full_flag", F.when(F.col("quantity_ordered_base_uom").isNull(), F.lit(None).cast("boolean")).otherwise(F.col("quantity_received_base_uom") >= F.col("quantity_ordered_base_uom")))
        .withColumn("inferred_member_flag", (F.col("supplier_key") == UNKNOWN_KEY) | F.col("purchase_order_business_key").isNull())
    )
    return j.select(
        F.col("receipt_line_id").cast("long"), F.col("receipt_id").cast("long"), "receipt_date_key", F.col("order_date").alias("purchase_order_date_key"),
        "supplier_key", "source_supplier_id", F.col("contract_business_key"), "region_code", F.col("receipt_nbr").alias("receipt_number"),
        F.col("line_nbr").cast("int").alias("receipt_line_number"), F.coalesce(F.col("purchase_order_number"), F.col("po_nbr")).alias("purchase_order_number"),
        "purchase_order_line_number", F.col("po_line_id").alias("purchase_order_line_business_key"), F.col("warehouse_cd").alias("warehouse_site_code"),
        "quantity_ordered_base_uom", "quantity_received_base_uom", "quantity_rejected_base_uom", "quantity_on_quality_hold",
        F.col("uom_cd").alias("source_uom_code"), F.col("received_qty").cast("decimal(18,4)").alias("quantity_source_uom"), "over_receipt_quantity",
        F.coalesce(F.col("cost_curr_cd"), F.col("transaction_currency_code"), F.lit("USD")).alias("transaction_currency_code"),
        F.col("unit_cost").cast("decimal(19,4)"), "receipt_value", F.col("fx_rate_to_usd").alias("fx_rate_to_reporting"), "receipt_value_reporting",
        "days_late_versus_promise", "lead_time_days", "price_variance_amount", "on_time_flag", "in_full_flag",
        F.col("reject_reason_cd").alias("quality_hold_reason_code"), F.col("inspection_status_cd").alias("inspection_result_code"),
        F.col("variance_band"), F.col("receipt_status_cd").alias("receipt_status_code"), "inferred_member_flag",
        F.sha2(F.concat_ws("|", F.lit(SOURCE_SYSTEM_ORACLE), F.col("receipt_line_id")), 256).alias("natural_key_hash"),
        F.lit(int(batchId)).cast("long").alias("lineage_key"), F.lit(int(batchId)).cast("long").alias("batch_id"), F.current_timestamp().alias("load_datetime"),
    )


def runFactPurchaseReceipt(spark, batchId):
    packageName = "FACT_Load_PurchaseReceipt"
    objectName = "stg.Receipt"
    keyFrom = int(io.getWatermark(spark, f"{SOURCE_SYSTEM_ORACLE}:FACT", objectName, "0"))
    fact = buildPurchaseReceiptFact(
        spark.table(qualified(BRONZE_RECEIPT_LINE)), spark.table(qualified(SILVER_PURCHASE_ORDER_LINE)), spark.table(qualified(SILVER_PURCHASE_ORDER)),
        spark.table(qualified(GOLD_DIM_SUPPLIER)), keyFrom, batchId,
    )
    target = qualified(GOLD_FACT_PURCHASE_RECEIPT)
    if io.tableExists(spark, target):
        io.appendDelta(fact, target)
    else:
        io.writeDelta(fact, target)
    rows = spark.table(target).where(f"batch_id = {int(batchId)}").count()
    keyTo = spark.table(target).agg(F.coalesce(F.max("receipt_line_id"), F.lit(keyFrom))).collect()[0][0]
    io.setWatermark(spark, f"{SOURCE_SYSTEM_ORACLE}:FACT", objectName, int(keyTo), "key", batchId)
    inferred = spark.table(target).where(f"batch_id = {int(batchId)} AND inferred_member_flag").count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows, watermarkFrom=keyFrom, watermarkTo=keyTo,
                     message=f"inferred_member_rows={inferred}")
    return rows


# --------------------------------------------------------------------------------------
# FACT_Load_SupplierTransaction  (AP sub-ledger: invoices + payments, accrual reversals)
# --------------------------------------------------------------------------------------
def conformSupplierTransactions(bronze: DataFrame, batchId) -> DataFrame:
    """stg.SupplierTransaction equivalent from Purchasing.SupplierTransactions."""
    return bronze.select(
        F.col("supplier_transaction_id").cast("long").alias("supplier_transaction_business_key"),
        F.lit(SOURCE_SYSTEM_OLTP).alias("source_system_code"),
        F.col("supplier_id").cast("long").alias("wwi_supplier_id"), "supplier_reference",
        F.when(F.col("transaction_type_name") == "Supplier Invoice", "INV").when(F.col("transaction_type_name") == "Supplier Payment Issued", "PAY").otherwise("OTH").alias("transaction_type_code"),
        "transaction_type_name", "purchase_order_id", "supplier_invoice_number", F.to_date("transaction_date").alias("transaction_date"),
        F.col("amount_excluding_tax").cast("decimal(19,4)"), F.col("tax_amount").cast("decimal(19,4)"), F.col("transaction_amount").cast("decimal(19,4)"),
        F.col("outstanding_balance").cast("decimal(19,4)"), F.to_date("finalization_date").alias("finalization_date"),
        F.lit(False).alias("is_accrual"), F.col("last_edited_when").cast("timestamp").alias("last_modified_at"),
    ).withColumn("batch_id", F.lit(int(batchId)).cast("long"))


def buildSupplierTransactionFact(staged: DataFrame, dimSupplier: DataFrame, watermarkFrom, watermarkTo, batchId) -> DataFrame:
    s = staged.where((F.col("last_modified_at") > F.lit(watermarkFrom).cast("timestamp")) & (F.col("last_modified_at") <= F.lit(watermarkTo).cast("timestamp")))
    keys = dimSupplier.where("is_current_row").groupBy("wwi_supplier_id").agg(F.min("supplier_key").alias("_sk"), F.min("payment_days").alias("_pd"), F.min("region_code").alias("_region"))
    j = s.join(keys, "wwi_supplier_id", "left")
    signed = F.when(F.col("transaction_type_code") == "PAY", -F.abs(F.col("transaction_amount"))).otherwise(F.col("transaction_amount"))
    return j.select(
        "supplier_transaction_business_key", "source_system_code", F.col("transaction_date").alias("transaction_date_key"),
        F.date_add(F.col("transaction_date"), F.coalesce(F.col("_pd"), F.lit(30))).alias("due_date_key"),
        F.coalesce(F.col("_sk"), F.lit(UNKNOWN_KEY)).cast("int").alias("supplier_key"), "wwi_supplier_id", "transaction_type_code",
        F.coalesce(F.col("_region"), F.lit("NA")).alias("region_code"), F.col("supplier_transaction_business_key").alias("wwi_supplier_transaction_id"),
        "supplier_invoice_number", F.col("purchase_order_id").cast("string").alias("purchase_order_number"), F.lit("USD").alias("transaction_currency_code"),
        "amount_excluding_tax", F.col("tax_amount").alias("recoverable_tax_amount"), F.lit(0).cast("decimal(19,4)").alias("non_recoverable_tax_amount"),
        signed.cast("decimal(19,4)").alias("transaction_amount"), "outstanding_balance", F.lit(1.0).cast("decimal(18,8)").alias("fx_rate_to_reporting"),
        signed.cast("decimal(19,4)").alias("transaction_amount_reporting"), F.col("is_accrual").alias("accrual_flag"), F.lit(False).alias("is_reversal"),
        F.lit(None).cast("long").alias("reverses_transaction_key"), F.year("transaction_date").alias("fiscal_year"), F.month("transaction_date").alias("fiscal_period"),
        (F.col("supplier_key") == UNKNOWN_KEY).alias("inferred_member_flag"),
        F.sha2(F.concat_ws("|", F.col("source_system_code"), F.col("supplier_transaction_business_key")), 256).alias("natural_key_hash"),
        F.lit(int(batchId)).cast("long").alias("lineage_key"), F.lit(int(batchId)).cast("long").alias("batch_id"), F.current_timestamp().alias("load_datetime"),
    )


def accrualReversals(fact: DataFrame, batchId) -> DataFrame:
    """Every accrual row without a reversal gets an ACCREV row with the negated amount
    (Post Accrual Reversals Execute SQL task)."""
    reversed_ = fact.where("is_reversal").select(F.col("reverses_transaction_key").alias("supplier_transaction_business_key"))
    open_ = fact.where("accrual_flag AND NOT is_reversal").join(reversed_, "supplier_transaction_business_key", "left_anti")
    return (
        open_.withColumn("reverses_transaction_key", F.col("supplier_transaction_business_key"))
        .withColumn("supplier_transaction_business_key", -F.col("supplier_transaction_business_key"))
        .withColumn("wwi_supplier_transaction_id", F.col("reverses_transaction_key"))
        .withColumn("transaction_type_code", F.lit("ACCREV"))
        .withColumn("transaction_amount", -F.col("transaction_amount"))
        .withColumn("transaction_amount_reporting", -F.col("transaction_amount_reporting"))
        .withColumn("amount_excluding_tax", -F.col("amount_excluding_tax"))
        .withColumn("is_reversal", F.lit(True))
        .withColumn("accrual_flag", F.lit(False))
        .withColumn("natural_key_hash", F.sha2(F.concat_ws("|", F.col("source_system_code"), F.lit("ACCREV"), F.col("reverses_transaction_key")), 256))
        .withColumn("lineage_key", F.lit(int(batchId)).cast("long"))
        .withColumn("batch_id", F.lit(int(batchId)).cast("long"))
    )


def runFactSupplierTransaction(spark, batchId):
    packageName = "FACT_Load_SupplierTransaction"
    objectName = "stg.SupplierTransaction"
    staged = conformSupplierTransactions(spark.table(qualified(BRONZE_SUPPLIER_TRANSACTION)), batchId)
    io.writeDelta(staged, qualified(SILVER_SUPPLIER_TRANSACTION))
    wmFrom = io.getWatermark(spark, f"{SOURCE_SYSTEM_OLTP}:FACT", objectName, LOW_TS)
    wmTo = spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS ts").collect()[0]["ts"]
    fact = buildSupplierTransactionFact(staged, spark.table(qualified(GOLD_DIM_SUPPLIER)), wmFrom, wmTo, batchId)
    target = qualified(GOLD_FACT_SUPPLIER_TRANSACTION)
    if io.tableExists(spark, target):
        fact.select("supplier_transaction_business_key").createOrReplaceTempView("_st_keys")
        spark.sql(f"DELETE FROM {target} WHERE supplier_transaction_business_key IN (SELECT supplier_transaction_business_key FROM _st_keys)")
        io.appendDelta(fact, target)
    else:
        io.writeDelta(fact, target)
    reversals = accrualReversals(spark.table(target), batchId)
    reversalCount = reversals.count()
    if reversalCount:
        io.appendDelta(reversals, target)
    rows = spark.table(target).where(f"batch_id = {int(batchId)}").count()
    io.setWatermark(spark, f"{SOURCE_SYSTEM_OLTP}:FACT", objectName, wmTo, "timestamp", batchId)
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=fact.count(), rowsInserted=rows, watermarkFrom=wmFrom, watermarkTo=wmTo,
                     message=f"accrual_reversals={reversalCount}")
    return rows


