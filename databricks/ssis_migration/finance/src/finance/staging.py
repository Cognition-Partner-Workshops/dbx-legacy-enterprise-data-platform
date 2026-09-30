"""STG_* staging conformance, STG_Work_PaymentMatch and DQ_Payment_Screen."""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from finance import sources
from finance.config import FinanceConfig
from finance.extracts import (
    BRONZE_COST_CENTER,
    BRONZE_GL_JOURNAL_LINE,
    BRONZE_INVOICE_HDR,
    BRONZE_INVOICE_LINE,
    BRONZE_PAYMENT,
    BRONZE_PAYMENT_APPLY,
)
from finance.io import (
    PackageResult,
    appendTable,
    mergeTable,
    overwriteTable,
    readTable,
    tableExists,
    writeRejects,
)
from finance.matching import matchPayments
from finance.rules import (
    ledgerTaxRegime,
    paymentStatus,
    postingSide,
    rowHash,
    signedAmount,
    taxRegime,
    valueDate,
)

SILVER_INVOICE = "silver_ap_invoice"
SILVER_INVOICE_LINE = "silver_ap_invoice_line"
SILVER_GL_JOURNAL_LINE = "silver_gl_journal_line"
SILVER_PAYMENT = "silver_payment"
SILVER_COST_CENTER = "silver_cost_center"
WORK_PAYMENT_MATCHED = "work_payment_matched"
WORK_PAYMENT_UNAPPLIED = "work_payment_unapplied"
ERR_REJECTED_PAYMENT = "err_rejected_payment"
DQ_PAYMENT_RESULT = "dq_payment_screen_result"
UNKNOWN_MEMBER_KEY = 0
LARGE_PAYMENT_THRESHOLD = 5_000_000
MAX_ORPHAN_RATE_PCT = 5.0

# ref.CodeCrosswalk (PAYMENT_METHOD / ORA_ERP) is empty on the legacy host, which would have sent every
# payment to err.RejectedPayment.  The conformed code set is carried here instead (deliberate deviation).
PAYMENT_METHOD_CROSSWALK = {
    "ACH": ("ACH", "Automated clearing house"),
    "CARD": ("CARD", "Corporate card"),
    "CHECK": ("CHK", "Paper check"),
    "LOCAL": ("LOCAL", "Local bank transfer"),
    "SEPA": ("SEPA", "SEPA credit transfer"),
    "WIRE": ("WIRE", "Wire transfer"),
}


def latestPerKey(df: DataFrame, key: str, orderCol: str = "last_upd_dt") -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(orderCol).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def withSupplierKey(df: DataFrame, supplierDim: DataFrame) -> DataFrame:
    """Late-arriving / unknown-member rule: no current dimension row -> UnknownMemberKey (0)."""
    dim = supplierDim.where(F.col("is_current_row") == True).select(  # noqa: E712
        F.col("wwi_supplier_id").alias("_dim_supp_id"), F.col("supplier_key").alias("_dim_key")
    )
    return (
        df.join(dim, F.col("supp_id").cast("long") == F.col("_dim_supp_id").cast("long"), "left")
        .withColumn("supplier_key", F.coalesce(F.col("_dim_key"), F.lit(UNKNOWN_MEMBER_KEY)).cast("int"))
        .withColumn("supplier_key_is_unknown", F.col("_dim_key").isNull())
        .drop("_dim_supp_id", "_dim_key")
    )


# --------------------------------------------------------------------------- STG_Load_ApInvoice
def transformStgInvoices(hdr: DataFrame, supplierDim: DataFrame) -> tuple[DataFrame, DataFrame]:
    base = latestPerKey(hdr, "invoice_id")
    base = withSupplierKey(base, supplierDim)
    isCredit = F.col("invoice_type_cd").isin("CRM", "CREDIT", "DBM")
    enriched = (
        base.withColumn("invoice_number", F.upper(F.trim(F.col("invoice_num"))))
        .withColumn("supplier_code", F.upper(F.trim(F.col("supp_num"))))
        .withColumn("region_code", F.upper(F.trim(F.col("region_cd"))))
        .withColumn("currency_code", F.upper(F.trim(F.col("currency_cd"))))
        .withColumn("invoice_status_code", F.upper(F.trim(F.col("status_cd"))))
        .withColumn("tax_regime_code", taxRegime(F.col("region_cd")))
        .withColumn("invoice_amount", F.col("invoice_amt").cast("decimal(18,5)"))
        .withColumn("tax_amount", F.coalesce(F.col("tax_amt"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn("paid_amount", F.coalesce(F.col("paid_amt"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn(
            "open_amount",
            F.coalesce(F.col("outstanding_amt"), F.col("invoice_amt") - F.col("paid_amt")).cast(
                "decimal(18,5)"
            ),
        )
        .withColumn("discount_due_date", F.col("discount_dt"))
        .withColumn(
            "_reject_reason",
            F.when(F.col("supplier_code").isNull(), "UNKNOWN_SUPPLIER").when(
                (F.col("invoice_amount") <= 0) & ~isCredit, "NON_POSITIVE_AMOUNT"
            ),
        )
        .withColumn(
            "change_hash",
            rowHash(
                F.col("invoice_number"),
                F.col("supplier_code"),
                F.col("invoice_amount"),
                F.col("invoice_status_code"),
                F.col("open_amount"),
            ),
        )
    )
    cols = [
        "invoice_id",
        "invoice_number",
        "supp_id",
        "supplier_code",
        "supplier_key",
        "supplier_key_is_unknown",
        "supp_name",
        "region_code",
        "org_cd",
        "invoice_type_cd",
        "invoice_dt",
        "gl_date",
        "period_cd",
        "currency_code",
        "exchange_rate_num",
        "invoice_amount",
        "net_amt",
        "tax_amount",
        "freight_amt",
        "withholding_amt",
        "paid_amount",
        "open_amount",
        "base_amt_usd",
        "payment_terms_cd",
        "discount_percent",
        "discount_days",
        "due_dt",
        "discount_due_date",
        "days_past_due",
        "aging_bucket_cd",
        "invoice_status_code",
        "match_type_cd",
        "approval_status_cd",
        "tax_regime_code",
        "reverse_charge_flag",
        "tax_reg_num",
        "cancelled_flg",
        "is_on_hold",
        "active_hold_count",
        "hold_codes_txt",
        "payment_apply_count",
        "last_apply_dt",
        "change_hash",
        "last_upd_dt",
    ]
    accepted = enriched.where(F.col("_reject_reason").isNull()).select(*cols)
    rejected = enriched.where(F.col("_reject_reason").isNotNull()).select(*cols, "_reject_reason")
    return accepted, rejected


def transformStgInvoiceLines(lines: DataFrame, invoices: DataFrame) -> DataFrame:
    inv = invoices.select(
        "invoice_id",
        "invoice_number",
        "supplier_code",
        "supplier_key",
        "region_code",
        "tax_regime_code",
        "reverse_charge_flag",
        "tax_reg_num",
    )
    base = latestPerKey(lines, "invoice_line_id").drop(
        "invoice_num", "region_cd", "reverse_charge_flag", "tax_regime_cd"
    )
    out = (
        base.join(inv, "invoice_id", "inner")
        .withColumn("line_amount", F.col("line_amt").cast("decimal(18,5)"))
        .withColumn("tax_amount", F.coalesce(F.col("line_tax_amt"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn("account_code", F.upper(F.trim(F.col("account_cd"))))
        .withColumn("cost_center_code", F.upper(F.trim(F.col("cost_center_cd"))))
        .withColumn(
            "change_hash",
            rowHash(
                F.col("invoice_line_id"),
                F.col("line_amount"),
                F.col("account_code"),
                F.col("cost_center_code"),
                F.col("match_status_cd"),
            ),
        )
    )
    return out.select(
        "invoice_line_id",
        "invoice_id",
        "invoice_number",
        "supp_id",
        "supplier_code",
        "supplier_key",
        "region_code",
        "org_cd",
        "gl_date",
        "line_num",
        "line_type_cd",
        "service_category_cd",
        "po_line_id",
        "product_id",
        "item_desc_txt",
        "quantity",
        "uom_cd",
        "unit_price",
        "line_amount",
        "currency_cd",
        "accounted_amt",
        "account_code",
        "cost_center_code",
        "cost_center_id",
        "project_cd",
        "tax_code_cd",
        "tax_rate_pct",
        "tax_regime_code",
        "tax_amount",
        "recoverable_tax_amt",
        "non_recoverable_tax_amt",
        "price_variance_amt",
        "qty_variance_amt",
        "match_status_cd",
        "accrual_reversal_flag",
        "reverse_charge_flag",
        "tax_reg_num",
        "posted_flg",
        "journal_id",
        "change_hash",
        "last_upd_dt",
    )


def runStgApInvoice(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "STG_Load_ApInvoice"
    hdr = readTable(spark, cfg.table(BRONZE_INVOICE_HDR))
    accepted, rejected = transformStgInvoices(hdr, sources.legacySupplierDimension(spark, cfg))
    accepted = accepted.cache()
    w1 = mergeTable(spark, accepted, cfg.table(SILVER_INVOICE), ["invoice_id"])
    lines = transformStgInvoiceLines(
        readTable(spark, cfg.table(BRONZE_INVOICE_LINE)), readTable(spark, cfg.table(SILVER_INVOICE))
    )
    w2 = mergeTable(spark, lines, cfg.table(SILVER_INVOICE_LINE), ["invoice_line_id"])
    rej = writeRejects(
        spark,
        cfg,
        rejected,
        pkg,
        "STAGE",
        "raw.OracleApInvoiceHdr",
        "invoice_id",
        "_reject_reason",
        batchId=batchId,
    )
    return PackageResult(pkg, hdr.count(), w1 + w2, rej, notes={"invoices": w1, "invoice_lines": w2})


# --------------------------------------------------------------------------- STG_Load_GlJournal
def transformStgGlJournal(lines: DataFrame) -> DataFrame:
    base = latestPerKey(lines, "journal_line_id")
    return (
        base.withColumn("ledger_code", F.upper(F.trim(F.col("ledger_cd"))))
        .withColumn("account_code", F.upper(F.trim(F.col("account_cd"))))
        .withColumn("cost_center_code", F.upper(F.trim(F.col("cost_center_cd"))))
        .withColumn("region_code", F.upper(F.trim(F.col("region_cd"))))
        .withColumn("tax_regime_code", ledgerTaxRegime(F.col("ledger_code")))
        .withColumn("debit_amount", F.coalesce(F.col("debit_amt"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn("credit_amount", F.coalesce(F.col("credit_amt"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn("net_amount", signedAmount(F.col("debit_amt"), F.col("credit_amt")).cast("decimal(18,5)"))
        .withColumn("posting_side", postingSide(F.col("debit_amt"), F.col("credit_amt")))
        .withColumn("accounting_period", F.col("fiscal_period_cd"))
        .withColumn("subledger_source_key", F.concat_ws(":", F.col("src_doc_type_cd"), F.col("src_doc_id")))
        .withColumn(
            "change_hash",
            rowHash(
                F.col("journal_line_id"),
                F.col("debit_amount"),
                F.col("credit_amount"),
                F.col("account_code"),
                F.col("posting_status_cd"),
            ),
        )
        .select(
            "journal_line_id",
            "journal_id",
            "journal_num",
            "ledger_code",
            "journal_source_cd",
            "journal_category_cd",
            "region_code",
            "org_cd",
            "accounting_period",
            "fiscal_year_nbr",
            "fiscal_period_nbr",
            "period_status_cd",
            "period_closed_dt",
            "period_end_dt",
            "gl_date",
            "posting_status_cd",
            "posted_flag",
            "posted_dt",
            "reversal_flag",
            "reversed_journal_id",
            "accrual_flag",
            "line_num",
            "gl_account_id",
            "account_code",
            "account_name",
            "account_type_cd",
            "account_class_cd",
            "normal_balance_cd",
            "reconciliation_flg",
            "cost_center_code",
            "cost_center_id",
            "project_cd",
            "intercompany_cd",
            "currency_cd",
            "debit_amount",
            "credit_amount",
            "net_amount",
            "posting_side",
            "base_debit_amt_usd",
            "base_credit_amt_usd",
            "journal_total_debit_amt",
            "journal_total_credit_amt",
            "journal_line_cnt",
            "tax_regime_code",
            "line_desc",
            "src_doc_type_cd",
            "src_doc_id",
            "subledger_source_key",
            "tax_code_cd",
            "change_hash",
            "last_upd_dt",
        )
    )


def runStgGlJournal(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "STG_Load_GlJournal"
    src = readTable(spark, cfg.table(BRONZE_GL_JOURNAL_LINE))
    out = transformStgGlJournal(src)
    written = mergeTable(spark, out, cfg.table(SILVER_GL_JOURNAL_LINE), ["journal_line_id"])
    return PackageResult(pkg, src.count(), written, 0)


# --------------------------------------------------------------------------- STG_Load_Payment
def transformStgPayments(pay: DataFrame, supplierDim: DataFrame, asOf: date) -> tuple[DataFrame, DataFrame]:
    spark = pay.sparkSession
    xw = spark.createDataFrame(
        [(k, v[0], v[1]) for k, v in PAYMENT_METHOD_CROSSWALK.items()],
        "source_payment_method_code string, payment_method_code string, payment_method_description string",
    )
    base = withSupplierKey(latestPerKey(pay, "payment_id"), supplierDim)
    enriched = (
        base.withColumn("payment_number", F.upper(F.trim(F.col("payment_num"))))
        .withColumn("supplier_code", F.upper(F.trim(F.col("supp_num"))))
        .withColumn("source_payment_method_code", F.upper(F.trim(F.col("payment_method_cd"))))
        .withColumn("bank_account_code", F.substring(F.trim(F.col("bank_account_cd")), -4, 4))
        .withColumn("payment_currency_code", F.upper(F.trim(F.col("currency_cd"))))
        .withColumn("payment_status_code", paymentStatus(F.col("status_cd"), F.col("void_dt")))
        .withColumn("region_code", F.upper(F.trim(F.col("region_cd"))))
        .withColumn("payment_amount", F.col("payment_amt").cast("decimal(18,5)"))
        .withColumn("payment_date", F.col("payment_dt"))
        .withColumn("value_date", valueDate(F.col("cleared_dt"), F.col("payment_dt"), F.col("region_cd")))
        .join(xw, "source_payment_method_code", "left")
        .withColumn(
            "change_hash",
            rowHash(
                F.col("payment_number"),
                F.col("supplier_code"),
                F.col("payment_amount"),
                F.col("payment_status_code"),
            ),
        )
        .withColumn(
            "_reject_reason",
            F.when(F.col("payment_method_code").isNull(), "UNKNOWN_PAYMENT_METHOD")
            .when(F.col("payment_date") > F.lit(asOf), "FUTURE_DATED")
            .when(F.col("payment_amount") <= 0, "NON_POSITIVE_AMOUNT"),
        )
    )
    cols = [
        "payment_id",
        "payment_number",
        "supp_id",
        "supplier_code",
        "supplier_key",
        "supplier_key_is_unknown",
        "supp_name",
        "region_code",
        "org_cd",
        "payment_date",
        "gl_date",
        "period_cd",
        "value_date",
        "cleared_dt",
        "payment_currency_code",
        "payment_amount",
        "fx_rate",
        "accounted_amt",
        "withholding_amt",
        "discount_taken_amt",
        "applied_amt",
        "unapplied_amt",
        "payment_amt_usd",
        "source_payment_method_code",
        "payment_method_code",
        "payment_method_description",
        "settlement_days",
        "bank_account_code",
        "payment_run_id",
        "payment_status_code",
        F.col("status_cd").alias("source_status_code"),
        "void_flag",
        "void_dt",
        "invoice_count",
        "settlement_status_cd",
        "days_to_clear",
        "change_hash",
        "last_upd_dt",
    ]
    accepted = enriched.where(F.col("_reject_reason").isNull()).select(*cols)
    rejected = enriched.where(F.col("_reject_reason").isNotNull()).select(*cols, "_reject_reason")
    return accepted, rejected


def writeRejectedPayments(
    spark: SparkSession,
    cfg: FinanceConfig,
    df: DataFrame,
    package: str,
    stage: str,
    batchId: int,
    detailCol: str | None = None,
) -> int:
    """err.RejectedPayment equivalent (typed, unlike the generic reject table)."""
    out = df.select(
        F.col("payment_id").cast("decimal(12,0)"),
        F.col("payment_number").cast("string"),
        F.col("supplier_code").cast("string"),
        F.col("region_code").cast("string"),
        F.col("payment_amount").cast("decimal(18,5)"),
        F.col("payment_date").cast("date"),
        F.col("value_date").cast("date")
        if "value_date" in df.columns
        else F.lit(None).cast("date").alias("value_date"),
        F.col("payment_method_code").cast("string")
        if "payment_method_code" in df.columns
        else F.lit(None).cast("string").alias("payment_method_code"),
        F.col("_reject_reason").cast("string").alias("reject_reason_code"),
        (F.col(detailCol).cast("string") if detailCol else F.lit(None).cast("string")).alias("reject_detail"),
        F.lit(stage).alias("stage"),
        F.lit(package).alias("package_name"),
        F.lit(batchId).cast("bigint").alias("batch_id"),
        F.current_timestamp().alias("rejected_at"),
    )
    fq = cfg.table(ERR_REJECTED_PAYMENT)
    if tableExists(spark, fq):
        spark.sql(f"DELETE FROM {fq} WHERE package_name = '{package}' AND stage = '{stage}'")
        return appendTable(out, fq)
    return overwriteTable(out, fq)


def runStgPayment(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "STG_Load_Payment"
    src = readTable(spark, cfg.table(BRONZE_PAYMENT))
    accepted, rejected = transformStgPayments(
        src, sources.legacySupplierDimension(spark, cfg), cfg.businessDate
    )
    written = mergeTable(spark, accepted, cfg.table(SILVER_PAYMENT), ["payment_id"])
    rej = writeRejectedPayments(spark, cfg, rejected, pkg, "STAGE", batchId)
    return PackageResult(pkg, src.count(), written, rej)


# --------------------------------------------------------------------------- STG_Load_CostCenter
def transformStgCostCenters(cc: DataFrame) -> DataFrame:
    base = latestPerKey(cc, "cost_center_cd")
    return base.select(
        "cost_center_id",
        F.upper(F.trim(F.col("cost_center_cd"))).alias("cost_center_code"),
        F.trim(F.col("cost_center_name")).alias("cost_center_name"),
        F.upper(F.trim(F.col("parent_cost_center_cd"))).alias("parent_cost_center_code"),
        "parent_cost_center_name",
        F.upper(F.trim(F.col("region_cd"))).alias("region_code"),
        "rollup_anomaly_cd",
        "country_cd",
        "legal_entity_cd",
        F.coalesce(F.col("depth_num"), F.col("hierarchy_level_nbr")).alias("hierarchy_level"),
        "rollup_path",
        "root_cost_center_cd",
        "manager_cd",
        "manager_name_txt",
        "default_gl_account_cd",
        "functional_curr_cd",
        "budget_owner_cd",
        "annual_budget_amt",
        "budget_fy_cd",
        "approval_limit_amt",
        "allocation_basis_cd",
        "site_cd",
        (F.col("active_flag") == "Y").alias("is_active"),
        "eff_from_dt",
        "eff_to_dt",
        "legacy_dept_cd",
        "alloc_rule_count",
        rowHash(
            F.col("cost_center_name"),
            F.col("parent_cost_center_cd"),
            F.col("region_cd"),
            F.col("manager_cd"),
            F.col("default_gl_account_cd"),
            F.col("active_flag"),
            F.col("legal_entity_cd"),
        ).alias("scd_hash"),
        "last_upd_dt",
    )


def runStgCostCenter(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "STG_Load_CostCenter"
    src = readTable(spark, cfg.table(BRONZE_COST_CENTER))
    written = overwriteTable(transformStgCostCenters(src), cfg.table(SILVER_COST_CENTER))
    return PackageResult(pkg, src.count(), written, 0, notes={"load_type": "truncate_reload"})


# --------------------------------------------------------------------------- STG_Work_PaymentMatch
def prepareMatchInputs(
    payments: DataFrame, invoices: DataFrame, applies: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame]:
    p = payments.select(
        "payment_id",
        "payment_number",
        "supp_id",
        F.col("supp_id").alias("supplier_id"),
        "region_code",
        "payment_date",
        "payment_amount",
        "payment_status_code",
    ).drop("supp_id")
    i = invoices.select(
        "invoice_id",
        "invoice_number",
        F.col("supp_id").alias("supplier_id"),
        "open_amount",
        F.col("invoice_dt").alias("invoice_date"),
        F.coalesce(F.col("due_dt"), F.col("invoice_dt")).alias("due_date"),
        "discount_percent",
        "discount_due_date",
        "is_on_hold",
        "invoice_status_code",
    )
    r = applies.where(F.col("reversed_flag") != "Y").select(
        "payment_id", "invoice_id", "apply_seq", F.col("applied_amt").alias("remit_amount")
    )
    return p, i, r


def runPaymentMatch(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "STG_Work_PaymentMatch"
    payments = readTable(spark, cfg.table(SILVER_PAYMENT))
    invoices = readTable(spark, cfg.table(SILVER_INVOICE))
    applies = readTable(spark, cfg.table(BRONZE_PAYMENT_APPLY))
    p, i, r = prepareMatchInputs(payments, invoices, applies)
    matches, unapplied = matchPayments(p, i, r)
    matches = matches.withColumn("batch_id", F.lit(batchId).cast("bigint")).withColumn(
        "matched_at", F.current_timestamp()
    )
    w = overwriteTable(matches, cfg.table(WORK_PAYMENT_MATCHED))
    unappliedOut = unapplied.withColumn("batch_id", F.lit(batchId).cast("bigint")).withColumn(
        "matched_at", F.current_timestamp()
    )
    overwriteTable(unappliedOut, cfg.table(WORK_PAYMENT_UNAPPLIED))
    rejects = (
        unapplied.where(~F.col("within_tolerance"))
        .withColumn("_reject_reason", F.col("reject_reason_code"))
        .withColumn("supplier_code", F.col("supplier_id").cast("string"))
    )
    rej = writeRejectedPayments(spark, cfg, rejects, pkg, "MATCH", batchId, detailCol="remaining_amount")
    byPass = {
        row["match_type_code"]: row["n"]
        for row in matches.groupBy("match_type_code").count().withColumnRenamed("count", "n").collect()
    }
    return PackageResult(
        pkg,
        payments.count(),
        w,
        rej,
        notes={"matches_by_pass": byPass, "writeoffs": unapplied.where(F.col("within_tolerance")).count()},
    )


# --------------------------------------------------------------------------- DQ_Payment_Screen
def screenPayments(payments: DataFrame, matches: DataFrame, asOf: date) -> DataFrame:
    matched = matches.groupBy("payment_id").agg(F.sum("matched_amount").alias("matched_amount"))
    return (
        payments.join(matched, "payment_id", "left")
        .withColumn(
            "orphan_flag",
            F.when(
                F.col("matched_amount").isNull() & (F.col("payment_status_code") != "VOID"), "Y"
            ).otherwise("N"),
        )
        .withColumn("future_dated_flag", F.when(F.col("payment_date") > F.lit(asOf), "Y").otherwise("N"))
        .withColumn(
            "value_date_before_payment_flag",
            F.when(F.col("value_date") < F.col("payment_date"), "Y").otherwise("N"),
        )
        .withColumn(
            "large_payment_flag",
            F.when(F.col("payment_amount") > LARGE_PAYMENT_THRESHOLD, "Y").otherwise("N"),
        )
        .withColumn(
            "reject_reason_code",
            F.when(F.col("orphan_flag") == "Y", "DQ_PAY_ORPHAN")
            .when(F.col("future_dated_flag") == "Y", "DQ_PAY_FUTURE")
            .when(F.col("value_date_before_payment_flag") == "Y", "DQ_PAY_VALUE_DATE")
            .when(F.col("large_payment_flag") == "Y", "DQ_PAY_LARGE"),
        )
    )


def runDqPaymentScreen(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "DQ_Payment_Screen"
    payments = readTable(spark, cfg.table(SILVER_PAYMENT))
    matches = readTable(spark, cfg.table(WORK_PAYMENT_MATCHED))
    screened = screenPayments(payments, matches, cfg.businessDate).cache()
    total = screened.count()
    rejects = screened.where(F.col("reject_reason_code").isNotNull()).withColumn(
        "_reject_reason", F.col("reject_reason_code")
    )
    rej = writeRejectedPayments(spark, cfg, rejects, pkg, "DQ", batchId)
    orphanCount = screened.where(F.col("orphan_flag") == "Y").count()
    orphanRate = round(100.0 * orphanCount / total, 4) if total else 0.0
    metrics = (
        screened.groupBy("payment_method_code", "payment_currency_code")
        .agg(
            F.count("*").alias("payment_count"),
            F.sum("payment_amount").alias("payment_total"),
            F.max("payment_amount").alias("max_payment"),
            F.sum(F.when(F.col("orphan_flag") == "Y", 1).otherwise(0)).alias("orphan_count"),
            F.sum(F.when(F.col("future_dated_flag") == "Y", 1).otherwise(0)).alias("future_dated_count"),
            F.sum(F.when(F.col("value_date_before_payment_flag") == "Y", 1).otherwise(0)).alias(
                "value_date_count"
            ),
        )
        .withColumn("method_group_allowed", F.col("max_payment") <= LARGE_PAYMENT_THRESHOLD)
        .withColumn("orphan_rate_pct", F.lit(orphanRate))
        .withColumn("orphan_rate_breached", F.lit(orphanRate > MAX_ORPHAN_RATE_PCT))
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("screened_at", F.current_timestamp())
    )
    overwriteTable(metrics, cfg.table(DQ_PAYMENT_RESULT))
    status = "WARNING" if orphanRate > MAX_ORPHAN_RATE_PCT else "SUCCEEDED"
    return PackageResult(
        pkg,
        total,
        rej,
        rej,
        status=status,
        notes={"orphan_rate_pct": orphanRate, "orphan_count": orphanCount},
    )
