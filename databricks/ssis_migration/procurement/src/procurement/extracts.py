"""EXT_ORA_* / EXT_SQL_* packages: federated extracts landed as bronze Delta tables.

Each package keeps the SSIS control-flow shape: read watermark -> extract -> derive audit
columns -> land -> set watermark -> log row counts. Incremental predicates are pushed to the
foreign catalog. The Oracle host exposes base tables (not the V_*_EXTRACT views of the
generator scripts); the column mapping is documented in README.md."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from procurement import io
from procurement.config import (
    LEGACY_OLTP,
    LEGACY_ORACLE,
    LOW_TS,
    PARAMS,
    SOURCE_SYSTEM_OLTP,
    SOURCE_SYSTEM_ORACLE,
    qualified,
)

BRONZE_SUPPLIER = "bronze_ora_supp_master"
BRONZE_PO_HDR = "bronze_ora_purchase_order_hdr"
BRONZE_PO_LINE = "bronze_ora_purchase_order_line"
BRONZE_RECEIPT_LINE = "bronze_ora_receipt_line"
BRONZE_VENDOR_CONTRACT = "bronze_ora_vendor_contract"
BRONZE_SUPPLIER_TRANSACTION = "bronze_sql_supplier_transaction"


def auditColumns(df: DataFrame, sourceSystem, batchId) -> DataFrame:
    """SSIS audit trailer: SourceSystemCode, ExtractedAtUtc, PackageExecutionId."""
    return (
        df.withColumn("source_system_code", F.lit(sourceSystem))
        .withColumn("extracted_at_utc", F.current_timestamp())
        .withColumn("package_execution_id", F.lit(int(batchId)).cast("long"))
    )


def lowerColumns(df: DataFrame) -> DataFrame:
    return df.select([F.col(f"`{c}`").alias(c.lower()) for c in df.columns])


# --------------------------------------------------------------------------------------
# EXT_ORA_SupplierMaster  (incremental_timestamp)
# --------------------------------------------------------------------------------------
def transformSupplierMaster(suppMaster: DataFrame, suppCertification: DataFrame, watermarkFrom, watermarkTo,
                            asOf=None, dormantMonths=PARAMS["dormantSupplierMonths"]) -> DataFrame:
    """Timestamp window on UPDATED_DT; drops dormant (no PO in `dormantMonths`) non-active
    suppliers and merged/deleted parties; joins the latest certification."""
    asOfExpr = F.lit(asOf).cast("timestamp") if asOf else F.current_timestamp()
    s = lowerColumns(suppMaster)
    cert = (
        lowerColumns(suppCertification)
        .groupBy("supp_id")
        .agg(
            F.max_by("cert_type_cd", F.coalesce(F.col("expiry_dt"), F.lit(LOW_TS).cast("timestamp"))).alias("quality_cert_cd"),
            F.max("expiry_dt").alias("cert_expiry_dt"),
        )
    )
    filtered = s.where(
        (F.col("updated_dt") >= F.lit(watermarkFrom).cast("timestamp"))
        & (F.col("updated_dt") < F.lit(watermarkTo).cast("timestamp"))
        & (
            (F.col("last_po_dt") >= F.add_months(asOfExpr, -dormantMonths))
            | (F.col("supp_status_cd").isin("AC", "ACTV"))
        )
        & (F.coalesce(F.col("deleted_flg"), F.lit("N")) == "N")
    )
    return (
        filtered.join(cert, "supp_id", "left")
        .withColumn(
            "certification_expired_flag",
            F.when(F.col("cert_expiry_dt").isNull(), "U")
            .when(F.col("cert_expiry_dt") < asOfExpr, "Y")
            .otherwise("N"),
        )
        .withColumn(
            "withholding_applies",
            F.when((F.col("region_cd") == "NA") & (F.col("withholding_flg") == "Y"), "Y").otherwise("N"),
        )
    )


def runSupplierMaster(spark, batchId, asOf=None):
    packageName = "EXT_ORA_SupplierMaster"
    objectName = "WWI_MDM.SUPP_MASTER"
    wmFrom = io.getWatermark(spark, SOURCE_SYSTEM_ORACLE, objectName, LOW_TS)
    wmTo = spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS ts").collect()[0]["ts"]
    src = spark.table(f"{LEGACY_ORACLE}.wwi_mdm.supp_master")
    cert = spark.table(f"{LEGACY_ORACLE}.wwi_mdm.supp_certification")
    out = auditColumns(transformSupplierMaster(src, cert, wmFrom, wmTo, asOf), SOURCE_SYSTEM_ORACLE, batchId)
    out = out.withColumn("batch_id", F.lit(int(batchId)).cast("long"))
    rows = out.count()
    target = qualified(BRONZE_SUPPLIER)
    if io.tableExists(spark, target):
        io.replaceWhere(out, target, f"batch_id = {int(batchId)}")
    else:
        io.writeDelta(out, target)
    io.setWatermark(spark, SOURCE_SYSTEM_ORACLE, objectName, wmTo, "timestamp", batchId)
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows,
                     watermarkFrom=wmFrom, watermarkTo=wmTo)
    return rows


# --------------------------------------------------------------------------------------
# EXT_ORA_PurchaseOrderHdr  (incremental_timestamp, cancelled split)
# --------------------------------------------------------------------------------------
def convertToUsd(df: DataFrame, fxRates: DataFrame, amountCol, currencyCol, dateCol, outCol) -> DataFrame:
    """WWI_FIN.FN_CONVERT_AMOUNT equivalent: rates are quoted USD -> currency per day; the
    latest rate on/before the transaction date is used; USD passes through."""
    fx = (
        lowerColumns(fxRates)
        .where((F.col("from_curr_cd") == "USD") & (F.col("superseded_flg") == "N"))
        .select(F.col("to_curr_cd").alias("_fx_curr"), F.to_date("rate_dt").alias("_fx_dt"), F.col("rate").alias("_fx_rate"))
    )
    joined = df.join(
        fx,
        (F.col(currencyCol) == F.col("_fx_curr")) & (F.col("_fx_dt") <= F.to_date(F.col(dateCol))),
        "left",
    )
    keyCols = [c for c in df.columns]
    latest = (
        joined.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(*keyCols).orderBy(F.col("_fx_dt").desc_nulls_last())
            ),
        )
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    return (
        latest.withColumn(
            outCol,
            F.when(F.col(currencyCol) == "USD", F.col(amountCol))
            .when(F.col("_fx_rate").isNotNull(), F.round(F.col(amountCol) / F.col("_fx_rate"), 2))
            .otherwise(F.lit(None).cast("decimal(18,2)")),
        )
        .withColumn("fx_rate_to_usd", F.when(F.col(currencyCol) == "USD", F.lit(1.0)).otherwise(1 / F.col("_fx_rate")))
        .drop("_fx_curr", "_fx_dt", "_fx_rate")
    )


def transformPurchaseOrderHdr(poHdr: DataFrame, fxRates: DataFrame, watermarkFrom, watermarkTo, asOf=None) -> DataFrame:
    asOfExpr = F.lit(asOf).cast("timestamp") if asOf else F.current_timestamp()
    h = lowerColumns(poHdr).where(
        (F.col("updated_dt") >= F.lit(watermarkFrom).cast("timestamp"))
        & (F.col("updated_dt") < F.lit(watermarkTo).cast("timestamp"))
        & F.col("po_status_cd").isin("OPEN", "PART", "CLSD", "CANC")
        & (F.coalesce(F.col("approval_status_cd"), F.lit("")) != "DRFT")
    )
    h = convertToUsd(h, fxRates, "total_amt", "order_curr_cd", "order_dt", "po_total_base_amt")
    return (
        h.withColumn("order_age_days", F.datediff(asOfExpr, F.col("order_dt")))
        .withColumn(
            "late_flag",
            F.when((F.col("promised_dt") < asOfExpr) & (F.col("po_status_cd") != "CLSD"), "Y").otherwise("N"),
        )
        .withColumn("is_cancelled", F.col("po_status_cd") == "CANC")
        .withColumn("cancel_reason_cd", F.when(F.col("po_status_cd") == "CANC", F.lit("CANC")).otherwise(F.lit(None).cast("string")))
    )


def runPurchaseOrderHdr(spark, batchId, asOf=None):
    packageName = "EXT_ORA_PurchaseOrderHdr"
    objectName = "WWI_PROC.PURCHASE_ORDER_HDR"
    wmFrom = io.getWatermark(spark, SOURCE_SYSTEM_ORACLE, objectName, LOW_TS)
    wmTo = spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS ts").collect()[0]["ts"]
    src = spark.table(f"{LEGACY_ORACLE}.wwi_proc.purchase_order_hdr")
    fx = spark.table(f"{LEGACY_ORACLE}.wwi_ref.fx_rate_daily")
    out = auditColumns(transformPurchaseOrderHdr(src, fx, wmFrom, wmTo, asOf), SOURCE_SYSTEM_ORACLE, batchId)
    out = out.withColumn("batch_id", F.lit(int(batchId)).cast("long"))
    rows = out.count()
    cancelled = out.where("is_cancelled").count()
    target = qualified(BRONZE_PO_HDR)
    if io.tableExists(spark, target):
        io.replaceWhere(out, target, f"batch_id = {int(batchId)}")
    else:
        io.writeDelta(out, target)
    io.setWatermark(spark, SOURCE_SYSTEM_ORACLE, objectName, wmTo, "timestamp", batchId)
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows,
                     watermarkFrom=wmFrom, watermarkTo=wmTo, message=f"cancelled_orders={cancelled}")
    return rows


# --------------------------------------------------------------------------------------
# EXT_ORA_PurchaseOrderLine  (incremental_key on PO_LINE_ID, max key read before extract)
# --------------------------------------------------------------------------------------
def transformPurchaseOrderLine(poLine: DataFrame, keyFrom, keyTo) -> DataFrame:
    line = lowerColumns(poLine).where((F.col("po_line_id") > int(keyFrom)) & (F.col("po_line_id") <= int(keyTo)))
    return (
        line.withColumn("open_qty", F.col("order_qty") - F.coalesce(F.col("received_qty"), F.lit(0)) - F.coalesce(F.col("cancelled_qty"), F.lit(0)))
        .withColumn("extended_amt", F.col("order_qty") * F.col("unit_price"))
        .withColumn(
            "receipt_complete_pct",
            F.when(F.col("order_qty") == 0, F.lit(0.0))
            .otherwise(F.coalesce(F.col("received_qty"), F.lit(0)) / F.col("order_qty"))
            .cast("decimal(9,4)"),
        )
    )


def runPurchaseOrderLine(spark, batchId):
    packageName = "EXT_ORA_PurchaseOrderLine"
    objectName = "WWI_PROC.PURCHASE_ORDER_LINE"
    keyFrom = int(io.getWatermark(spark, SOURCE_SYSTEM_ORACLE, objectName, "0"))
    src = spark.table(f"{LEGACY_ORACLE}.wwi_proc.purchase_order_line")
    keyTo = src.agg(F.coalesce(F.max("PO_LINE_ID"), F.lit(0))).collect()[0][0]
    out = auditColumns(transformPurchaseOrderLine(src, keyFrom, keyTo), SOURCE_SYSTEM_ORACLE, batchId)
    out = out.withColumn("batch_id", F.lit(int(batchId)).cast("long"))
    rows = out.count()
    target = qualified(BRONZE_PO_LINE)
    if io.tableExists(spark, target):
        io.appendDelta(out, target)
    else:
        io.writeDelta(out, target)
    io.setWatermark(spark, SOURCE_SYSTEM_ORACLE, objectName, int(keyTo), "key", batchId)
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows,
                     watermarkFrom=keyFrom, watermarkTo=keyTo)
    return rows


# --------------------------------------------------------------------------------------
# EXT_ORA_ReceiptLine  (incremental_key on RECEIPT_LINE_ID, void receipts excluded)
# --------------------------------------------------------------------------------------
def transformReceiptLine(receiptLine: DataFrame, receiptHdr: DataFrame, poLine: DataFrame, poHdr: DataFrame, keyFrom) -> DataFrame:
    rl = lowerColumns(receiptLine).where(F.col("receipt_line_id") > int(keyFrom))
    rh = lowerColumns(receiptHdr).where(F.col("receipt_status_cd") != "VOID").select(
        "receipt_id", "receipt_nbr", "supp_id", "warehouse_cd", "receipt_status_cd", "reversed_flg",
        F.col("region_cd").alias("receipt_region_cd"), F.col("receipt_dt").alias("hdr_receipt_dt"),
    )
    pl = lowerColumns(poLine).select(F.col("po_line_id"), F.col("po_id").alias("pl_po_id"), F.col("unit_price").alias("po_unit_price"),
                                     F.col("order_qty").alias("po_order_qty"))
    ph = lowerColumns(poHdr).select(F.col("po_id").alias("ph_po_id"), "po_nbr")
    joined = (
        rl.join(rh, "receipt_id", "inner")
        .join(pl, "po_line_id", "inner")
        .join(ph, F.col("pl_po_id") == F.col("ph_po_id"), "inner")
        .drop("pl_po_id", "ph_po_id")
    )
    # WWI_PROC.FN_RECEIPT_VARIANCE_PCT: price variance of the receipt unit cost against the PO price
    variance = F.when(
        F.coalesce(F.col("po_unit_price"), F.lit(0)) == 0, F.lit(0.0)
    ).otherwise((F.col("unit_cost") - F.col("po_unit_price")) / F.col("po_unit_price"))
    return (
        joined.withColumn("rejected_qty", F.col("received_qty") - F.col("accepted_qty"))
        .withColumn("variance_pct", F.coalesce(F.col("variance_pct"), variance).cast("decimal(9,4)"))
        .withColumn(
            "variance_band",
            F.when(F.abs(F.col("variance_pct")) <= 0.01, "OK")
            .when(F.abs(F.col("variance_pct")) <= 0.05, "WARN")
            .otherwise("EXCP"),
        )
        .withColumn("inspection_status_cd", F.coalesce(F.col("inspection_result_cd"), F.lit("PASS")))
        .withColumn("is_quarantined", F.col("inspection_status_cd") == "FAIL")
    )


def runReceiptLine(spark, batchId):
    packageName = "EXT_ORA_ReceiptLine"
    objectName = "WWI_PROC.PO_RECEIPT_LINE"
    keyFrom = int(io.getWatermark(spark, SOURCE_SYSTEM_ORACLE, objectName, "0"))
    rl = spark.table(f"{LEGACY_ORACLE}.wwi_proc.po_receipt_line")
    rh = spark.table(f"{LEGACY_ORACLE}.wwi_proc.po_receipt_hdr")
    pl = spark.table(f"{LEGACY_ORACLE}.wwi_proc.purchase_order_line")
    ph = spark.table(f"{LEGACY_ORACLE}.wwi_proc.purchase_order_hdr")
    out = auditColumns(transformReceiptLine(rl, rh, pl, ph, keyFrom), SOURCE_SYSTEM_ORACLE, batchId)
    out = out.withColumn("batch_id", F.lit(int(batchId)).cast("long"))
    rows = out.count()
    keyTo = out.agg(F.max("receipt_line_id")).collect()[0][0]
    target = qualified(BRONZE_RECEIPT_LINE)
    if io.tableExists(spark, target):
        io.appendDelta(out, target)
    else:
        io.writeDelta(out, target)
    if keyTo is not None:
        io.setWatermark(spark, SOURCE_SYSTEM_ORACLE, objectName, int(keyTo), "key", batchId)
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows,
                     watermarkFrom=keyFrom, watermarkTo=keyTo if keyTo is not None else keyFrom)
    return rows


# --------------------------------------------------------------------------------------
# EXT_ORA_VendorContract  (full truncate-and-load with line rollup)
# --------------------------------------------------------------------------------------
def transformVendorContract(vendorContract: DataFrame, vendorContractLine: DataFrame, asOf=None) -> DataFrame:
    asOfExpr = F.lit(asOf).cast("timestamp") if asOf else F.current_timestamp()
    c = lowerColumns(vendorContract).where(F.col("contract_status_cd") != "DELT")
    lineCols = [x.lower() for x in vendorContractLine.columns]
    consumedCol = "consumed_amt" if "consumed_amt" in lineCols else None
    cl = lowerColumns(vendorContractLine).groupBy("contract_id").agg(
        (F.sum(consumedCol) if consumedCol else F.lit(0)).alias("line_consumed_amt"),
        F.count(F.lit(1)).alias("line_count"),
    )
    joined = c.join(cl, "contract_id", "left")
    return (
        joined.withColumn("line_count", F.coalesce(F.col("line_count"), F.lit(0)))
        .withColumn("consumed_amt", F.coalesce(F.col("consumed_amt"), F.col("line_consumed_amt"), F.lit(0)))
        .drop("line_consumed_amt")
        .withColumn(
            "utilisation_pct",
            F.when(F.coalesce(F.col("committed_amt"), F.lit(0)) == 0, F.lit(0.0))
            .otherwise(F.col("consumed_amt") / F.col("committed_amt"))
            .cast("decimal(9,4)"),
        )
        .withColumn(
            "renewal_due_flag",
            F.when(
                (F.col("auto_renew_flg") == "N")
                & (F.datediff(F.col("end_dt"), asOfExpr) <= F.col("notice_period_days")),
                "Y",
            ).otherwise("N"),
        )
    )


def runVendorContract(spark, batchId):
    packageName = "EXT_ORA_VendorContract"
    src = spark.table(f"{LEGACY_ORACLE}.wwi_proc.vendor_contract")
    lines = spark.table(f"{LEGACY_ORACLE}.wwi_proc.vendor_contract_line")
    out = auditColumns(transformVendorContract(src, lines), SOURCE_SYSTEM_ORACLE, batchId)
    out = out.withColumn("batch_id", F.lit(int(batchId)).cast("long"))
    rows = out.count()
    io.writeDelta(out, qualified(BRONZE_VENDOR_CONTRACT))
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows)
    return rows


# --------------------------------------------------------------------------------------
# EXT_SQL_SupplierTransactions  (incremental_key on SupplierTransactionID)
# --------------------------------------------------------------------------------------
def transformSupplierTransactions(transactions: DataFrame, suppliers: DataFrame, transactionTypes: DataFrame, keyFrom) -> DataFrame:
    st = transactions.where(F.col("SupplierTransactionID") > int(keyFrom))
    s = suppliers.select("SupplierID", "SupplierReference")
    tt = transactionTypes.select("TransactionTypeID", "TransactionTypeName")
    joined = st.join(s, "SupplierID", "inner").join(tt, "TransactionTypeID", "inner")
    out = joined.select(
        F.col("SupplierTransactionID").alias("supplier_transaction_id"),
        F.col("SupplierID").alias("supplier_id"),
        F.col("SupplierReference").alias("supplier_reference"),
        F.col("TransactionTypeID").alias("transaction_type_id"),
        F.col("TransactionTypeName").alias("transaction_type_name"),
        F.col("PurchaseOrderID").alias("purchase_order_id"),
        F.col("SupplierInvoiceNumber").alias("supplier_invoice_number"),
        F.col("TransactionDate").alias("transaction_date"),
        F.col("AmountExcludingTax").alias("amount_excluding_tax"),
        F.col("TaxAmount").alias("tax_amount"),
        F.col("TransactionAmount").alias("transaction_amount"),
        F.col("OutstandingBalance").alias("outstanding_balance"),
        F.col("FinalizationDate").alias("finalization_date"),
        F.col("LastEditedWhen").alias("last_edited_when"),
    )
    return out.withColumn("record_kind", F.lit("APTRAN")).withColumn(
        "duplicate_check_key",
        F.concat_ws("|", F.upper(F.trim(F.col("supplier_reference"))), F.upper(F.trim(F.coalesce(F.col("supplier_invoice_number"), F.lit(""))))),
    )


def runSupplierTransactions(spark, batchId):
    packageName = "EXT_SQL_SupplierTransactions"
    objectName = "Purchasing.SupplierTransactions"
    keyFrom = int(io.getWatermark(spark, SOURCE_SYSTEM_OLTP, objectName, "0"))
    st = spark.table(f"{LEGACY_OLTP}.Purchasing.SupplierTransactions")
    s = spark.table(f"{LEGACY_OLTP}.Purchasing.Suppliers")
    tt = spark.table(f"{LEGACY_OLTP}.Application.TransactionTypes")
    out = auditColumns(transformSupplierTransactions(st, s, tt, keyFrom), SOURCE_SYSTEM_OLTP, batchId)
    out = out.withColumn("batch_id", F.lit(int(batchId)).cast("long"))
    rows = out.count()
    keyTo = out.agg(F.max("supplier_transaction_id")).collect()[0][0]
    target = qualified(BRONZE_SUPPLIER_TRANSACTION)
    if io.tableExists(spark, target):
        io.appendDelta(out, target)
    else:
        io.writeDelta(out, target)
    if keyTo is not None:
        io.setWatermark(spark, SOURCE_SYSTEM_OLTP, objectName, int(keyTo), "key", batchId)
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows,
                     watermarkFrom=keyFrom, watermarkTo=keyTo if keyTo is not None else keyFrom)
    return rows
