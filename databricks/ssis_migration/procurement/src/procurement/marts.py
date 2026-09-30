"""PRC_* procurement marts: spend classification, three-way receipt matching, contract compliance,
supplier scorecard and the outbound supplier statement file."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from procurement import io
from procurement.config import LEGACY_ORACLE, PARAMS, UNKNOWN_KEY, qualified, volumePath
from procurement.dimensions import GOLD_DIM_SUPPLIER
from procurement.extracts import lowerColumns
from procurement.facts import GOLD_FACT_PURCHASE_RECEIPT, GOLD_FACT_SUPPLIER_TRANSACTION
from procurement.staging import SILVER_PURCHASE_ORDER, SILVER_PURCHASE_ORDER_LINE, SILVER_SUPPLIER, SILVER_VENDOR_CONTRACT

GOLD_FACT_PURCHASE_SPEND = "gold_fact_purchase_spend"
GOLD_FACT_RECEIPT_MATCHING = "gold_fact_receipt_matching"
GOLD_AGG_CONTRACT_COMPLIANCE = "gold_agg_contract_compliance"
GOLD_AGG_SUPPLIER_SCORECARD = "gold_agg_supplier_scorecard"
GOLD_SUPPLIER_STATEMENT = "gold_supplier_statement"
STATEMENT_FILE_PREFIX = "supplier_statement"
TAIL_SPEND_THRESHOLD_USD = 1000.0


# --------------------------------------------------------------------------------------
# PRC_Load_PurchaseSpend
# --------------------------------------------------------------------------------------
def classifySpend(poLine: DataFrame, poHdr: DataFrame, contracts: DataFrame, suppliers: DataFrame, batchId) -> DataFrame:
    """Spend cube at PO-line grain: CONTRACT when the order references / falls inside an active
    contract of the supplier, MAVERICK when the supplier is not preferred and no contract covers
    the line, TAIL when the USD amount is below the tail threshold, otherwise OFF_CONTRACT."""
    hdr = poHdr.select("purchase_order_business_key", "purchase_order_number", "source_supplier_id", "order_date", "region_code",
                       "buyer_code", "contract_business_key", "transaction_currency_code", "fx_rate_to_usd", "order_status_code")
    supp = suppliers.where("is_survivor_row").select("source_supplier_id", "supplier_business_key", F.col("preferred_supplier_flag").alias("_pref"),
                                                     F.col("strategic_tier_code"))
    c = contracts.where(F.col("dq_status_code") != "FAIL").select(
        F.col("source_supplier_id").alias("_c_sid"), F.col("contract_number").alias("_c_number"), F.col("contract_start_date").alias("_c_start"),
        F.col("contract_end_date").alias("_c_end"), F.col("rebate_percent").alias("_c_rebate"), F.col("committed_amount_usd").alias("_c_committed"),
    )
    j = poLine.join(hdr, "purchase_order_business_key", "inner").where(~F.col("order_status_code").isin("CANC", "DRAFT")).join(supp, "source_supplier_id", "left")
    j = j.join(
        c,
        (F.col("source_supplier_id") == F.col("_c_sid"))
        & (F.col("order_date") >= F.col("_c_start"))
        & (F.col("order_date") <= F.coalesce(F.col("_c_end"), F.lit("9999-12-31").cast("date"))),
        "left",
    )
    # prefer the contract the header references, otherwise the most recent covering contract
    pick = Window.partitionBy("purchase_order_line_business_key").orderBy(
        F.when(F.col("_c_number") == F.col("contract_business_key"), 0).otherwise(1), F.col("_c_start").desc_nulls_last()
    )
    j = j.withColumn("_rn", F.row_number().over(pick)).where("_rn = 1").drop("_rn")
    amountUsd = F.round(F.col("extended_amount") * F.coalesce(F.col("fx_rate_to_usd"), F.lit(1)), 2)
    return j.select(
        "purchase_order_line_business_key", "purchase_order_business_key", "purchase_order_number", F.col("line_number").alias("purchase_order_line_number"),
        "source_supplier_id", "supplier_business_key", F.coalesce(F.col("strategic_tier_code"), F.lit("Other")).alias("supplier_tier_code"),
        F.col("order_date").alias("spend_date"), F.date_format("order_date", "yyyy-MM").alias("calendar_month"), "region_code", "buyer_code",
        F.col("supplier_item_code").alias("stock_item_business_key"), "order_quantity", "unit_price_amount", "transaction_currency_code", "fx_rate_to_usd",
        F.col("extended_amount").alias("spend_amount"), amountUsd.cast("decimal(19,4)").alias("spend_amount_usd"),
        F.col("_c_number").alias("contract_number"), F.col("_c_number").isNotNull().alias("contract_covered_flag"),
        F.when(F.col("_c_number").isNotNull(), amountUsd).otherwise(F.lit(0)).cast("decimal(19,4)").alias("contract_covered_spend_usd"),
        F.when(F.col("_c_number").isNotNull(), F.round(amountUsd * F.coalesce(F.col("_c_rebate"), F.lit(0)) / 100.0, 2)).otherwise(F.lit(0)).cast("decimal(19,4)").alias("expected_rebate_usd"),
        F.when(F.col("_c_number").isNotNull(), "CONTRACT")
        .when(amountUsd < TAIL_SPEND_THRESHOLD_USD, "TAIL")
        .when(F.coalesce(F.col("_pref"), F.lit("N")) != "Y", "MAVERICK")
        .otherwise("OFF_CONTRACT").alias("spend_class_code"),
        F.when(F.col("supplier_business_key").isNull(), "FAIL").otherwise("PASS").alias("dq_status_code"),
        F.lit(int(batchId)).cast("long").alias("batch_id"), F.current_timestamp().alias("load_datetime"),
    )


def runPurchaseSpend(spark, batchId):
    packageName = "PRC_Load_PurchaseSpend"
    out = classifySpend(spark.table(qualified(SILVER_PURCHASE_ORDER_LINE)), spark.table(qualified(SILVER_PURCHASE_ORDER)),
                        spark.table(qualified(SILVER_VENDOR_CONTRACT)), spark.table(qualified(SILVER_SUPPLIER)), batchId)
    io.writeDelta(out, qualified(GOLD_FACT_PURCHASE_SPEND))
    rows = spark.table(qualified(GOLD_FACT_PURCHASE_SPEND)).count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows)
    return rows


# --------------------------------------------------------------------------------------
# PRC_Load_ReceiptMatching  (three-way match receipt <-> PO line <-> AP invoice line)
# --------------------------------------------------------------------------------------
def matchReceipts(receiptFact: DataFrame, apInvoiceLines: DataFrame, apInvoiceHdr: DataFrame, asOf, batchId,
                  qtyTolerancePct=PARAMS["receiptMatchQtyTolerancePct"], priceTolerancePct=PARAMS["receiptMatchPriceTolerancePct"],
                  grniAccrualDays=PARAMS["grniAccrualCutoffDays"]) -> DataFrame:
    """MATCHED when an invoice line references the receipt/PO line and quantity and price are within
    tolerance; QTY_EXCEPTION / PRICE_EXCEPTION otherwise; GRNI (goods received not invoiced) when no
    invoice line exists, flagged for accrual once older than grniAccrualDays."""
    il = lowerColumns(apInvoiceLines).select(
        F.col("invoice_line_id").alias("_il_id"), F.col("invoice_id").alias("_inv_id"), F.col("receipt_line_id").alias("_il_rcpt"),
        F.col("po_line_id").alias("_il_po_line"), F.col("quantity").alias("invoiced_quantity"), F.col("unit_price").alias("invoiced_unit_price"),
        F.col("line_amount").alias("invoiced_amount"),
    )
    ih = lowerColumns(apInvoiceHdr).select(F.col("invoice_id").alias("_inv_id"), F.col("invoice_nbr").alias("supplier_invoice_number"), F.to_date("invoice_dt").alias("invoice_date"))
    il = il.join(ih, "_inv_id", "left")
    byReceipt = il.where(F.col("_il_rcpt").isNotNull()).withColumnRenamed("_il_rcpt", "receipt_line_id").drop("_il_po_line")
    byPoLine = il.where(F.col("_il_rcpt").isNull() & F.col("_il_po_line").isNotNull()).withColumnRenamed("_il_po_line", "purchase_order_line_business_key").drop("_il_rcpt")
    r = receiptFact
    j = r.join(byReceipt, "receipt_line_id", "left")
    # fallback: invoice line that references the PO line (one receipt per PO line assumed for the fallback)
    fb = r.join(byPoLine, "purchase_order_line_business_key", "inner").select("receipt_line_id", *[F.col(c).alias(f"_fb_{c}") for c in byPoLine.columns if c != "purchase_order_line_business_key"])
    j = j.join(fb, "receipt_line_id", "left")
    for c in ["_il_id", "_inv_id", "invoiced_quantity", "invoiced_unit_price", "invoiced_amount", "supplier_invoice_number", "invoice_date"]:
        j = j.withColumn(c, F.coalesce(F.col(c), F.col(f"_fb_{c}"))).drop(f"_fb_{c}")
    qtyVar = F.col("invoiced_quantity") - F.col("quantity_received_base_uom")
    qtyVarPct = F.when(F.col("quantity_received_base_uom") == 0, F.lit(None)).otherwise(F.abs(qtyVar) * 100.0 / F.col("quantity_received_base_uom"))
    priceVarPct = F.when(F.col("unit_cost") == 0, F.lit(None)).otherwise(F.abs(F.col("invoiced_unit_price") - F.col("unit_cost")) * 100.0 / F.col("unit_cost"))
    ageDays = F.datediff(F.lit(asOf).cast("date"), F.col("receipt_date_key"))
    return j.select(
        "receipt_line_id", "receipt_number", "receipt_line_number", "purchase_order_number", "purchase_order_line_business_key", "supplier_key", "source_supplier_id",
        "region_code", "receipt_date_key", "quantity_received_base_uom", "unit_cost", "receipt_value", "receipt_value_reporting",
        F.col("_il_id").alias("invoice_line_id"), "supplier_invoice_number", "invoice_date", "invoiced_quantity", "invoiced_unit_price", "invoiced_amount",
        qtyVar.cast("decimal(18,4)").alias("quantity_variance"), qtyVarPct.cast("decimal(9,4)").alias("quantity_variance_pct"),
        (F.col("invoiced_unit_price") - F.col("unit_cost")).cast("decimal(19,4)").alias("price_variance_amount"), priceVarPct.cast("decimal(9,4)").alias("price_variance_pct"),
        F.when(F.col("_il_id").isNull(), "GRNI")
        .when(qtyVarPct > qtyTolerancePct, "QTY_EXCEPTION")
        .when(priceVarPct > priceTolerancePct, "PRICE_EXCEPTION")
        .otherwise("MATCHED").alias("match_status_code"),
        ageDays.alias("receipt_age_days"),
        (F.col("_il_id").isNull() & (ageDays >= grniAccrualDays)).alias("grni_accrual_flag"),
        F.when(F.col("_il_id").isNull() & (ageDays >= grniAccrualDays), F.col("receipt_value_reporting")).otherwise(F.lit(0)).cast("decimal(19,4)").alias("grni_accrual_amount_reporting"),
        F.lit(asOf).cast("date").alias("as_of_date"), F.lit(int(batchId)).cast("long").alias("batch_id"), F.current_timestamp().alias("load_datetime"),
    )


def runReceiptMatching(spark, batchId, asOf=None):
    packageName = "PRC_Load_ReceiptMatching"
    asOf = asOf or spark.sql("SELECT current_date() AS d").collect()[0]["d"].isoformat()
    out = matchReceipts(spark.table(qualified(GOLD_FACT_PURCHASE_RECEIPT)), spark.table(f"{LEGACY_ORACLE}.wwi_fin.ap_invoice_line"),
                        spark.table(f"{LEGACY_ORACLE}.wwi_fin.ap_invoice_hdr"), asOf, batchId)
    io.writeDelta(out, qualified(GOLD_FACT_RECEIPT_MATCHING))
    rows = spark.table(qualified(GOLD_FACT_RECEIPT_MATCHING)).count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows, message=f"as_of={asOf}")
    return rows


# --------------------------------------------------------------------------------------
# PRC_Load_ContractCompliance
# --------------------------------------------------------------------------------------
def contractCompliance(spend: DataFrame, contracts: DataFrame, asOf, batchId, windowDays=PARAMS["complianceWindowDays"]) -> DataFrame:
    """Per supplier / month: covered vs. uncovered spend, leakage percent and compliance code
    (COMPLIANT >= 90% covered, PARTIAL >= 50%, NON_COMPLIANT otherwise, NO_CONTRACT when the supplier
    has no active contract, EXPIRED_CONTRACT when its contracts all ended before the month)."""
    windowStart = F.date_sub(F.lit(asOf).cast("date"), windowDays)
    s = spend.where(F.col("spend_date") >= windowStart)
    monthly = s.groupBy("source_supplier_id", "supplier_business_key", "calendar_month", "region_code").agg(
        F.count("*").alias("purchase_line_count"),
        F.sum("spend_amount_usd").alias("spend_usd"),
        F.sum("contract_covered_spend_usd").alias("contract_covered_spend_usd"),
        F.sum(F.when(F.col("spend_class_code") == "MAVERICK", F.col("spend_amount_usd")).otherwise(0)).alias("maverick_spend_usd"),
        F.sum(F.when(F.col("spend_class_code") == "TAIL", F.col("spend_amount_usd")).otherwise(0)).alias("tail_spend_usd"),
        F.sum("expected_rebate_usd").alias("expected_rebate_usd"),
        F.max("contract_number").alias("contract_number"),
    )
    c = contracts.where(F.col("dq_status_code") != "FAIL").groupBy("source_supplier_id").agg(
        F.count("*").alias("contract_count"), F.max("contract_end_date").alias("_latest_end"), F.sum("committed_amount_usd").alias("committed_spend_usd"),
    )
    j = monthly.join(c, "source_supplier_id", "left")
    coveredPct = F.when(F.col("spend_usd") == 0, F.lit(0)).otherwise(F.round(F.col("contract_covered_spend_usd") * 100.0 / F.col("spend_usd"), 2))
    monthEnd = F.last_day(F.to_date(F.concat(F.col("calendar_month"), F.lit("-01"))))
    return j.select(
        "source_supplier_id", "supplier_business_key", "calendar_month", "region_code", "contract_number", F.coalesce(F.col("contract_count"), F.lit(0)).alias("contract_count"),
        "purchase_line_count", F.col("spend_usd").cast("decimal(19,4)"), F.col("contract_covered_spend_usd").cast("decimal(19,4)"),
        (F.col("spend_usd") - F.col("contract_covered_spend_usd")).cast("decimal(19,4)").alias("off_contract_spend_usd"),
        F.col("maverick_spend_usd").cast("decimal(19,4)"), F.col("tail_spend_usd").cast("decimal(19,4)"), F.col("expected_rebate_usd").cast("decimal(19,4)"),
        F.col("committed_spend_usd").cast("decimal(19,4)"), coveredPct.cast("decimal(9,2)").alias("contract_coverage_pct"),
        (100 - coveredPct).cast("decimal(9,2)").alias("leakage_pct"),
        F.when(F.coalesce(F.col("contract_count"), F.lit(0)) == 0, "NO_CONTRACT")
        .when(F.col("_latest_end") < F.to_date(F.concat(F.col("calendar_month"), F.lit("-01"))), "EXPIRED_CONTRACT")
        .when(coveredPct >= 90, "COMPLIANT").when(coveredPct >= 50, "PARTIAL").otherwise("NON_COMPLIANT").alias("compliance_code"),
        monthEnd.alias("period_end_date"), F.lit(asOf).cast("date").alias("as_of_date"), F.lit(int(batchId)).cast("long").alias("batch_id"), F.current_timestamp().alias("load_datetime"),
    )


def runContractCompliance(spark, batchId, asOf=None):
    packageName = "PRC_Load_ContractCompliance"
    asOf = asOf or spark.sql("SELECT current_date() AS d").collect()[0]["d"].isoformat()
    out = contractCompliance(spark.table(qualified(GOLD_FACT_PURCHASE_SPEND)), spark.table(qualified(SILVER_VENDOR_CONTRACT)), asOf, batchId)
    io.writeDelta(out, qualified(GOLD_AGG_CONTRACT_COMPLIANCE))
    rows = spark.table(qualified(GOLD_AGG_CONTRACT_COMPLIANCE)).count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows, message=f"as_of={asOf}")
    return rows


# --------------------------------------------------------------------------------------
# PRC_Load_SupplierScorecard
# --------------------------------------------------------------------------------------
REGION_WEIGHTS = {  # delivery, quality, price, invoice accuracy (etl.SupplierScoringWeight is empty on the host -> package defaults)
    "NA": (0.40, 0.25, 0.20, 0.15),
    "EU": (0.35, 0.30, 0.20, 0.15),
    "APAC": (0.30, 0.30, 0.25, 0.15),
}


def supplierScorecard(receiptFact: DataFrame, matching: DataFrame, spend: DataFrame, suppliers: DataFrame, asOf, batchId,
                      minimumOrders=PARAMS["scorecardMinimumOrders"]) -> DataFrame:
    """Weighted 0-100 score per supplier from delivery (OTIF), quality (1 - reject rate), price
    (1 - |price variance|) and invoice accuracy (matched share); suppliers with fewer orders than
    the minimum get INSUFFICIENT_DATA."""
    rec = receiptFact.groupBy("source_supplier_id").agg(
        F.count("*").alias("receipt_count"),
        F.avg(F.when(F.col("on_time_flag") & F.col("in_full_flag"), 1.0).otherwise(0.0)).alias("otif_rate"),
        F.avg(F.when(F.col("on_time_flag"), 1.0).otherwise(0.0)).alias("on_time_rate"),
        F.avg(F.when(F.col("in_full_flag"), 1.0).otherwise(0.0)).alias("in_full_rate"),
        (F.sum("quantity_rejected_base_uom") / F.sum("quantity_received_base_uom")).alias("reject_rate"),
        F.avg(F.when(F.col("unit_cost") == 0, 0.0).otherwise(F.abs(F.col("price_variance_amount")) / (F.col("unit_cost") * F.col("quantity_source_uom")))).alias("price_variance_rate"),
        F.avg("days_late_versus_promise").alias("avg_days_late"), F.avg("lead_time_days").alias("avg_lead_time_days"),
    )
    m = matching.groupBy("source_supplier_id").agg(
        F.avg(F.when(F.col("match_status_code") == "MATCHED", 1.0).when(F.col("match_status_code") == "GRNI", F.lit(None)).otherwise(0.0)).alias("invoice_accuracy_rate"),
        F.sum(F.when(F.col("match_status_code").isin("QTY_EXCEPTION", "PRICE_EXCEPTION"), 1).otherwise(0)).alias("match_exception_count"),
    )
    sp = spend.groupBy("source_supplier_id").agg(F.countDistinct("purchase_order_business_key").alias("purchase_order_count"), F.sum("spend_amount_usd").alias("spend_usd"))
    supp = suppliers.where("is_survivor_row").select("source_supplier_id", "supplier_business_key", "supplier_name", "region_code", "strategic_tier_code")
    j = supp.join(rec, "source_supplier_id", "left").join(m, "source_supplier_id", "left").join(sp, "source_supplier_id", "left")
    weights = F.create_map(*[x for k, w in REGION_WEIGHTS.items() for x in (F.lit(k), F.array(*[F.lit(v) for v in w]))])
    w = F.coalesce(weights[F.col("region_code")], weights[F.lit("NA")])
    delivery = F.coalesce(F.col("otif_rate"), F.lit(0.0))
    quality = F.lit(1.0) - F.coalesce(F.col("reject_rate"), F.lit(0.0))
    price = F.greatest(F.lit(0.0), F.lit(1.0) - F.coalesce(F.col("price_variance_rate"), F.lit(0.0)))
    invoice = F.coalesce(F.col("invoice_accuracy_rate"), F.lit(1.0))
    score = F.round(100.0 * (w[0] * delivery + w[1] * quality + w[2] * price + w[3] * invoice), 2)
    return j.select(
        "source_supplier_id", "supplier_business_key", "supplier_name", F.coalesce(F.col("region_code"), F.lit("NA")).alias("region_code"), "strategic_tier_code",
        F.coalesce(F.col("purchase_order_count"), F.lit(0)).alias("purchase_order_count"), F.coalesce(F.col("receipt_count"), F.lit(0)).alias("receipt_count"),
        F.col("spend_usd").cast("decimal(19,4)"),
        F.round(F.col("on_time_rate") * 100, 2).cast("decimal(9,2)").alias("on_time_pct"), F.round(F.col("in_full_rate") * 100, 2).cast("decimal(9,2)").alias("in_full_pct"),
        F.round(delivery * 100, 2).cast("decimal(9,2)").alias("otif_pct"), F.round(F.coalesce(F.col("reject_rate"), F.lit(0.0)) * 100, 2).cast("decimal(9,2)").alias("quality_reject_pct"),
        F.round(F.coalesce(F.col("price_variance_rate"), F.lit(0.0)) * 100, 2).cast("decimal(9,2)").alias("price_variance_pct"),
        F.round(invoice * 100, 2).cast("decimal(9,2)").alias("invoice_accuracy_pct"), F.coalesce(F.col("match_exception_count"), F.lit(0)).alias("match_exception_count"),
        F.round(F.col("avg_days_late"), 2).cast("decimal(9,2)").alias("avg_days_late"), F.round(F.col("avg_lead_time_days"), 2).cast("decimal(9,2)").alias("avg_lead_time_days"),
        w[0].alias("delivery_weight"), w[1].alias("quality_weight"), w[2].alias("price_weight"), w[3].alias("invoice_weight"),
        F.when(F.coalesce(F.col("purchase_order_count"), F.lit(0)) < minimumOrders, F.lit(None)).otherwise(score).cast("decimal(9,2)").alias("scorecard_score"),
        F.when(F.coalesce(F.col("purchase_order_count"), F.lit(0)) < minimumOrders, "INSUFFICIENT_DATA")
        .when(score >= 90, "A").when(score >= 75, "B").when(score >= 60, "C").otherwise("D").alias("scorecard_rating_code"),
        F.lit(asOf).cast("date").alias("as_of_date"), F.lit(int(batchId)).cast("long").alias("batch_id"), F.current_timestamp().alias("load_datetime"),
    )


def runSupplierScorecard(spark, batchId, asOf=None):
    packageName = "PRC_Load_SupplierScorecard"
    asOf = asOf or spark.sql("SELECT current_date() AS d").collect()[0]["d"].isoformat()
    out = supplierScorecard(spark.table(qualified(GOLD_FACT_PURCHASE_RECEIPT)), spark.table(qualified(GOLD_FACT_RECEIPT_MATCHING)),
                            spark.table(qualified(GOLD_FACT_PURCHASE_SPEND)), spark.table(qualified(SILVER_SUPPLIER)), asOf, batchId)
    io.writeDelta(out, qualified(GOLD_AGG_SUPPLIER_SCORECARD))
    rows = spark.table(qualified(GOLD_AGG_SUPPLIER_SCORECARD)).count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows, message=f"as_of={asOf}")
    return rows


# --------------------------------------------------------------------------------------
# PRC_Export_SupplierStatement
# --------------------------------------------------------------------------------------
def buildSupplierStatement(transactionFact: DataFrame, dimSupplier: DataFrame, statementPeriod, batchId) -> DataFrame:
    """Statement lines for one calendar month (yyyy-MM): opening balance, every transaction with a
    running balance and the closing balance per supplier. Amounts are signed (payments negative)."""
    supp = dimSupplier.where("is_current_row").groupBy("wwi_supplier_id").agg(F.min("supplier_key").alias("_sk"), F.first("supplier_name").alias("supplier_name"), F.first("payment_terms_code").alias("payment_terms_code"))
    t = transactionFact.where(~F.col("is_reversal")).withColumn("_period", F.date_format("transaction_date_key", "yyyy-MM"))
    opening = t.where(F.col("_period") < statementPeriod).groupBy("wwi_supplier_id").agg(F.sum("transaction_amount").alias("opening_balance"))
    inPeriod = t.where(F.col("_period") == statementPeriod)
    w = Window.partitionBy("wwi_supplier_id").orderBy("transaction_date_key", "supplier_transaction_business_key")
    lines = inPeriod.join(opening, "wwi_supplier_id", "left").withColumn("opening_balance", F.coalesce(F.col("opening_balance"), F.lit(0)).cast("decimal(19,4)"))
    lines = lines.withColumn("running_balance", (F.col("opening_balance") + F.sum("transaction_amount").over(w)).cast("decimal(19,4)"))
    lines = lines.withColumn("statement_line_number", F.row_number().over(w))
    closing = Window.partitionBy("wwi_supplier_id")
    lines = lines.withColumn("closing_balance", F.max("running_balance").over(closing.orderBy(F.col("statement_line_number").desc())))
    return lines.join(supp, "wwi_supplier_id", "left").select(
        F.lit(statementPeriod).alias("statement_period"), "wwi_supplier_id", F.coalesce(F.col("_sk"), F.lit(UNKNOWN_KEY)).alias("supplier_key"), "supplier_name", "payment_terms_code",
        "statement_line_number", F.col("transaction_date_key").alias("transaction_date"), "transaction_type_code", "supplier_invoice_number", "purchase_order_number",
        F.col("transaction_currency_code").alias("currency_code"), "opening_balance", "transaction_amount", "running_balance", "closing_balance", F.col("due_date_key").alias("due_date"),
        F.col("supplier_transaction_business_key").alias("transaction_reference"), F.lit(int(batchId)).cast("long").alias("batch_id"), F.current_timestamp().alias("load_datetime"),
    )


def runSupplierStatement(spark, batchId, statementPeriod=None):
    packageName = "PRC_Export_SupplierStatement"
    fact = spark.table(qualified(GOLD_FACT_SUPPLIER_TRANSACTION))
    statementPeriod = statementPeriod or fact.agg(F.max(F.date_format("transaction_date_key", "yyyy-MM"))).collect()[0][0]
    out = buildSupplierStatement(fact, spark.table(qualified(GOLD_DIM_SUPPLIER)), statementPeriod, batchId).cache()
    io.writeDelta(out, qualified(GOLD_SUPPLIER_STATEMENT))
    exportDir = f"{volumePath('exports')}/supplier_statement"
    fileName = f"{STATEMENT_FILE_PREFIX}_{statementPeriod.replace('-', '')}.csv"
    csvCols = [c for c in out.columns if c not in ("batch_id", "load_datetime")]
    pdf = out.select(*csvCols).orderBy("wwi_supplier_id", "statement_line_number").toPandas()
    import os
    os.makedirs(exportDir, exist_ok=True)
    pdf.to_csv(f"{exportDir}/{fileName}", index=False, encoding="utf-8")
    rows = len(pdf)
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows, message=f"file={exportDir}/{fileName}; period={statementPeriod}")
    return rows
