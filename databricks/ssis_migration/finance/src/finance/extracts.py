"""EXT_ORA_* - the seven Oracle bronze extracts.

Each package has a pure `transform*` function (DataFrames in, DataFrame out) that
ports the WWI_FIN.V_* extract view plus the SSIS data-flow derivations, and a
`run*` function that applies the load semantics (timestamp watermark, key
watermark, date window or full reload) and writes the bronze Delta table.
"""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from finance import sources
from finance.config import FinanceConfig
from finance.fx import convertAmounts
from finance.io import (
    PackageResult,
    deleteInsert,
    getWatermark,
    mergeTable,
    overwriteTable,
    setWatermark,
    writeRejects,
)
from finance.rules import dueDate, fiscalPeriod, oracleAgingBucket, settlementStatus, taxRegime

BRONZE_INVOICE_HDR = "bronze_ora_ap_invoice_hdr"
BRONZE_INVOICE_LINE = "bronze_ora_ap_invoice_line"
BRONZE_PAYMENT = "bronze_ora_ap_payment"
BRONZE_PAYMENT_APPLY = "bronze_ora_ap_payment_apply"
BRONZE_AP_AGING = "bronze_ora_ap_aging"
BRONZE_GL_JOURNAL_LINE = "bronze_ora_gl_journal_line"
BRONZE_COST_CENTER = "bronze_ora_cost_center"


def _meta(df: DataFrame, batchId: int) -> DataFrame:
    return df.withColumn("batch_id", F.lit(batchId).cast("bigint")).withColumn(
        "extracted_at", F.current_timestamp()
    )


def _lastUpd(df: DataFrame) -> DataFrame:
    return df.withColumn("last_upd_dt", F.coalesce(F.col("updated_dt"), F.col("created_dt")))


def withFiscalPeriod(
    df: DataFrame, dateCol: str, regionCol: str, regions: DataFrame, calendar: DataFrame, outCol: str
) -> DataFrame:
    """WWI_REF.FN_FISCAL_PERIOD: region -> fiscal calendar -> calendar row, arithmetic fallback."""
    r = regions.select(
        F.col("region_cd").alias("_r_region"), F.col("fiscal_calendar_cd").alias("_calendar_cd")
    )
    c = calendar.select(
        F.col("calendar_cd").alias("_c_calendar_cd"),
        F.col("calendar_dt").alias("_cal_dt"),
        "calendar_period_cd",
    )
    joined = (
        df.join(r, F.upper(F.col(regionCol)) == F.col("_r_region"), "left")
        .join(
            c,
            (F.col("_calendar_cd") == F.col("_c_calendar_cd"))
            & (F.to_date(F.col(dateCol)) == F.col("_cal_dt")),
            "left",
        )
        .withColumn(
            outCol, fiscalPeriod(F.to_date(F.col(dateCol)), F.col(regionCol), F.col("calendar_period_cd"))
        )
    )
    return joined.drop("_r_region", "_calendar_cd", "_c_calendar_cd", "_cal_dt", "calendar_period_cd")


# --------------------------------------------------------------------------- EXT_ORA_ApInvoiceHdr
def transformInvoiceHeaders(
    hdr: DataFrame,
    holds: DataFrame,
    applies: DataFrame,
    suppliers: DataFrame,
    terms: DataFrame,
    rates: DataFrame,
    currencies: DataFrame,
    regions: DataFrame,
    calendar: DataFrame,
    asOf: date,
) -> DataFrame:
    activeHolds = (
        holds.where((F.coalesce(F.col("released_flg"), F.lit("N")) == "N") & F.col("released_dt").isNull())
        .groupBy("invoice_id")
        .agg(
            F.count("*").alias("active_hold_count"),
            F.concat_ws(",", F.sort_array(F.collect_list("hold_code_cd"))).alias("hold_codes_txt"),
        )
    )
    applied = (
        applies.where(F.coalesce(F.col("reversed_flg"), F.lit("N")) == "N")
        .groupBy("invoice_id")
        .agg(F.count("*").alias("payment_apply_count"), F.max(F.to_date("apply_dt")).alias("last_apply_dt"))
    )
    t = terms.select(
        F.col("payment_terms_cd").alias("_terms_cd"),
        "term_basis_cd",
        "net_days",
        "due_day_of_month_nbr",
        "months_forward_nbr",
        F.col("discount_1_pct").alias("discount_percent"),
        F.col("discount_1_days").alias("discount_days"),
    )
    base = (
        _lastUpd(hdr)
        .where(F.col("invoice_status_cd") != "ENTR")
        .join(suppliers, "supp_id", "inner")
        .join(activeHolds, "invoice_id", "left")
        .join(applied, "invoice_id", "left")
        .join(t, F.col("payment_terms_cd") == F.col("_terms_cd"), "left")
    )
    base = withFiscalPeriod(base, "gl_date", "region_cd", regions, calendar, "derived_period_cd")
    base = base.withColumn("_usd_ccy", F.lit("USD"))
    base = convertAmounts(
        base, rates, currencies, "gross_amt", "invoice_curr_cd", "_usd_ccy", "gl_date", "base_amt_usd"
    )
    computedDue = dueDate(
        F.to_date("invoice_dt"),
        F.col("term_basis_cd"),
        F.col("net_days"),
        F.col("due_day_of_month_nbr"),
        F.col("months_forward_nbr"),
        F.col("region_cd"),
    )
    out = (
        base.withColumn(
            "period_cd",
            F.when(F.col("period_cd").rlike(r"^\d{4}-\d{2}$"), F.col("period_cd")).otherwise(
                F.col("derived_period_cd")
            ),
        )
        .withColumn("due_dt", F.coalesce(F.to_date("due_dt"), computedDue))
        .withColumn("paid_amt", F.coalesce(F.col("paid_amt"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn(
            "outstanding_amt",
            F.coalesce(F.col("balance_amt"), F.col("gross_amt") - F.col("paid_amt")).cast("decimal(18,5)"),
        )
        .withColumn(
            "days_past_due", F.datediff(F.lit(asOf), F.coalesce(F.to_date("due_dt"), F.to_date("invoice_dt")))
        )
        .withColumn("aging_bucket_cd", oracleAgingBucket(F.col("days_past_due"), F.col("region_cd")))
        .withColumn("reverse_charge_flag", F.coalesce(F.col("eu_self_billing_flg"), F.lit("N")))
        .withColumn("tax_reg_num", F.coalesce(F.col("eu_supplier_vat_nbr"), F.col("apac_gst_reg_nbr")))
        .withColumn("tax_regime_cd", taxRegime(F.col("region_cd")))
        .withColumn("active_hold_count", F.coalesce(F.col("active_hold_count"), F.lit(0)))
        .withColumn("payment_apply_count", F.coalesce(F.col("payment_apply_count"), F.lit(0)))
        .withColumn("is_on_hold", F.col("active_hold_count") > 0)
    )
    return out.select(
        "invoice_id",
        F.col("invoice_nbr").alias("invoice_num"),
        "supp_id",
        "supp_num",
        "supp_name",
        "supp_site_cd",
        "po_id",
        "region_cd",
        F.col("legal_entity_cd").alias("org_cd"),
        "invoice_type_cd",
        F.to_date("invoice_dt").alias("invoice_dt"),
        F.to_date("received_dt").alias("received_dt"),
        F.to_date("gl_date").alias("gl_date"),
        "period_cd",
        F.col("invoice_curr_cd").alias("currency_cd"),
        F.col("fx_rate").alias("exchange_rate_num"),
        F.col("gross_amt").alias("invoice_amt"),
        "net_amt",
        "tax_amt",
        "freight_amt",
        F.col("withheld_amt").alias("withholding_amt"),
        "paid_amt",
        "outstanding_amt",
        "base_amt_usd",
        "base_amt_usd_rate",
        "base_amt_usd_rate_missing",
        "payment_terms_cd",
        "term_basis_cd",
        "discount_percent",
        "discount_days",
        "due_dt",
        F.to_date("discount_due_dt").alias("discount_dt"),
        F.col("discount_taken_amt").alias("discount_amt"),
        "days_past_due",
        "aging_bucket_cd",
        F.col("invoice_status_cd").alias("status_cd"),
        F.col("match_status_cd").alias("match_type_cd"),
        "approval_status_cd",
        "reverse_charge_flag",
        "tax_reg_num",
        "tax_regime_cd",
        "eu_supplier_vat_nbr",
        "apac_gst_reg_nbr",
        "na_1099_reportable_flg",
        "cancelled_flg",
        "active_hold_count",
        "hold_codes_txt",
        "is_on_hold",
        "payment_apply_count",
        "last_apply_dt",
        "entered_by_cd",
        F.col("source_sys").alias("src_system_cd"),
        F.to_timestamp("created_dt").alias("created_dt"),
        F.to_timestamp("last_upd_dt").alias("last_upd_dt"),
    )


def runInvoiceHeaders(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "EXT_ORA_ApInvoiceHdr"
    wm = getWatermark(spark, cfg, pkg)
    hdr = _lastUpd(sources.oracleTable(spark, cfg, "wwi_fin", "ap_invoice_hdr")).where(
        F.col("last_upd_dt") > F.lit(wm).cast("timestamp")
    )
    out = transformInvoiceHeaders(
        hdr,
        sources.oracleTable(spark, cfg, "wwi_fin", "ap_invoice_hold"),
        sources.oracleTable(spark, cfg, "wwi_fin", "ap_payment_apply"),
        sources.supplierMaster(spark, cfg),
        sources.paymentTerms(spark, cfg),
        sources.fxRates(spark, cfg),
        sources.currencies(spark, cfg),
        sources.regionRef(spark, cfg),
        sources.calendarFiscal(spark, cfg),
        cfg.businessDate,
    )
    out = _meta(out, batchId).cache()
    rowsRead = hdr.count()
    written = mergeTable(spark, out, cfg.table(BRONZE_INVOICE_HDR), ["invoice_id"])
    newWm = out.agg(F.max("last_upd_dt")).collect()[0][0]
    setWatermark(spark, cfg, pkg, "last_upd_dt", newWm, written)
    return PackageResult(pkg, rowsRead, written, 0, notes={"watermark_in": wm, "watermark_out": str(newWm)})


# --------------------------------------------------------------------------- EXT_ORA_ApInvoiceLine
def transformInvoiceLines(
    lines: DataFrame, hdr: DataFrame, costCenters: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Returns (accepted, rejected).  Lines whose header is cancelled are dropped (view rule);
    lines pointing at an unknown / inactive cost center are rejected (SSIS lookup error output)."""
    h = hdr.select(
        "invoice_id",
        F.col("invoice_nbr").alias("invoice_num"),
        "supp_id",
        "region_cd",
        F.col("legal_entity_cd").alias("org_cd"),
        F.to_date("gl_date").alias("gl_date"),
        F.col("invoice_curr_cd").alias("hdr_currency_cd"),
        F.col("invoice_status_cd").alias("hdr_status_cd"),
        F.coalesce(F.col("eu_self_billing_flg"), F.lit("N")).alias("reverse_charge_flag"),
        F.coalesce(F.col("cancelled_flg"), F.lit("N")).alias("hdr_cancelled_flg"),
    )
    cc = costCenters.where(F.coalesce(F.col("active_flg"), F.lit("Y")) == "Y").select(
        F.col("cost_center_cd").alias("_cc_cd"), F.col("cost_center_id").alias("cost_center_id")
    )
    joined = (
        _lastUpd(lines)
        .join(h, "invoice_id", "inner")
        .where((F.col("hdr_cancelled_flg") != "Y") & (F.col("hdr_status_cd") != "CANC"))
        .join(cc, F.col("cost_center_cd") == F.col("_cc_cd"), "left")
    )
    rate = F.coalesce(F.col("tax_rate_pct"), F.lit(0.0))
    region = F.upper(F.col("region_cd"))
    lineTax = (
        F.when(F.col("line_amt").isNull() | (F.col("line_amt") == 0), F.lit(0.0))
        .when((region == "EU") & (F.col("reverse_charge_flag") == "Y"), F.lit(0.0))
        .when(region == "APAC", F.round(F.col("line_amt") - F.col("line_amt") / (1 + rate / 100), 2))
        .otherwise(F.round(F.col("line_amt") * rate / 100, 2))
    )
    enriched = (
        joined.withColumn("tax_regime_cd", taxRegime(F.col("region_cd")))
        .withColumn("line_tax_amt", lineTax.cast("decimal(18,5)"))
        .withColumn("accrual_reversal_flag", F.coalesce(F.col("accrual_reversed_flg"), F.lit("N")))
        .withColumn("service_category_cd", F.upper(F.col("line_type_cd")))
        .withColumn(
            "_reject_reason",
            F.when(F.col("_cc_cd").isNull() & F.col("cost_center_cd").isNotNull(), "UNKNOWN_COST_CENTER"),
        )
    )
    cols = [
        "invoice_line_id",
        "invoice_id",
        "invoice_num",
        "supp_id",
        "region_cd",
        "org_cd",
        "gl_date",
        F.col("line_nbr").alias("line_num"),
        "line_type_cd",
        "service_category_cd",
        "po_line_id",
        "receipt_line_id",
        "product_id",
        "item_desc_txt",
        "quantity",
        "uom_cd",
        "unit_price",
        "line_amt",
        F.coalesce(F.col("line_curr_cd"), F.col("hdr_currency_cd")).alias("currency_cd"),
        "accounted_amt",
        "account_cd",
        "cost_center_cd",
        "cost_center_id",
        "project_cd",
        "tax_code_cd",
        "tax_rate_pct",
        "tax_regime_cd",
        "line_tax_amt",
        "recoverable_tax_amt",
        "non_recoverable_tax_amt",
        "price_variance_amt",
        "qty_variance_amt",
        "match_status_cd",
        "accrual_reversal_flag",
        "reverse_charge_flag",
        "posted_flg",
        "journal_id",
        F.col("source_sys").alias("src_system_cd"),
        F.to_timestamp("created_dt").alias("created_dt"),
        F.to_timestamp("last_upd_dt").alias("last_upd_dt"),
    ]
    accepted = enriched.where(F.col("_reject_reason").isNull()).select(*cols)
    rejected = enriched.where(F.col("_reject_reason").isNotNull()).select(*cols, "_reject_reason")
    return accepted, rejected


def runInvoiceLines(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "EXT_ORA_ApInvoiceLine"
    wm = getWatermark(spark, cfg, pkg)
    wmKey = 0 if wm == "1900-01-01 00:00:00" else int(float(wm))
    lines = sources.oracleTable(spark, cfg, "wwi_fin", "ap_invoice_line").where(
        F.col("invoice_line_id") > wmKey
    )
    accepted, rejected = transformInvoiceLines(
        lines, sources.oracleTable(spark, cfg, "wwi_fin", "ap_invoice_hdr"), sources.costCenters(spark, cfg)
    )
    accepted = _meta(accepted, batchId).cache()
    rowsRead = lines.count()
    written = mergeTable(spark, accepted, cfg.table(BRONZE_INVOICE_LINE), ["invoice_line_id"])
    rej = writeRejects(
        spark,
        cfg,
        rejected,
        pkg,
        "EXTRACT",
        "WWI_FIN.AP_INVOICE_LINE",
        "invoice_line_id",
        "_reject_reason",
        batchId=batchId,
    )
    newWm = accepted.agg(F.max("invoice_line_id")).collect()[0][0]
    setWatermark(spark, cfg, pkg, "invoice_line_id", newWm, written)
    return PackageResult(pkg, rowsRead, written, rej, notes={"watermark_in": wm, "watermark_out": str(newWm)})


# --------------------------------------------------------------------------- EXT_ORA_ApPayment
def transformPayments(
    pay: DataFrame,
    applies: DataFrame,
    hdr: DataFrame,
    suppliers: DataFrame,
    methods: DataFrame,
    rates: DataFrame,
    currencies: DataFrame,
    regions: DataFrame,
    calendar: DataFrame,
    asOf: date,
) -> tuple[DataFrame, DataFrame]:
    """Returns (accepted, voided).  Void payments go to the reject stream as in the SSIS extract."""
    inv = hdr.select("invoice_id", F.to_date("invoice_dt").alias("_inv_dt"))
    ap = (
        applies.where(F.coalesce(F.col("reversed_flg"), F.lit("N")) == "N")
        .join(inv, "invoice_id", "left")
        .groupBy("payment_id")
        .agg(
            F.sum("applied_amt").alias("applied_amt"),
            F.countDistinct("invoice_id").alias("invoice_count"),
            F.min("_inv_dt").alias("earliest_invoice_dt"),
        )
    )
    pm = methods.groupBy(F.col("payment_method_cd").alias("_pm_cd")).agg(
        F.max("method_name").alias("payment_method_name"), F.max("settlement_days").alias("settlement_days")
    )
    base = (
        _lastUpd(pay)
        .join(suppliers, "supp_id", "inner")
        .join(ap, "payment_id", "left")
        .join(pm, F.col("payment_method_cd") == F.col("_pm_cd"), "left")
        .withColumn("_usd_ccy", F.lit("USD"))
    )
    base = withFiscalPeriod(base, "payment_dt", "region_cd", regions, calendar, "derived_period_cd")
    base = convertAmounts(
        base, rates, currencies, "payment_amt", "payment_curr_cd", "_usd_ccy", "payment_dt", "payment_amt_usd"
    )
    enriched = (
        base.withColumn(
            "period_cd",
            F.when(F.col("period_cd").rlike(r"^\d{4}-\d{2}$"), F.col("period_cd")).otherwise(
                F.col("derived_period_cd")
            ),
        )
        .withColumn("applied_amt", F.coalesce(F.col("applied_amt"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn("unapplied_amt", (F.col("payment_amt") - F.col("applied_amt")).cast("decimal(18,5)"))
        .withColumn("invoice_count", F.coalesce(F.col("invoice_count"), F.lit(0)))
        .withColumn(
            "void_flag",
            F.when(F.col("void_dt").isNotNull() | (F.col("payment_status_cd") == "VOID"), "Y").otherwise("N"),
        )
        .withColumn(
            "settlement_status_cd",
            settlementStatus(
                F.col("void_dt"),
                F.col("region_cd"),
                F.to_date("payment_dt"),
                F.col("cleared_dt"),
                F.lit(asOf),
            ),
        )
        .withColumn("days_to_clear", F.datediff(F.to_date("cleared_dt"), F.to_date("payment_dt")))
        .withColumn("reversal_flag", F.when(F.col("reissue_of_payment_id").isNotNull(), "Y").otherwise("N"))
        .withColumn("_reject_reason", F.when(F.col("void_flag") == "Y", "VOID_PAYMENT"))
    )
    cols = [
        "payment_id",
        F.col("payment_nbr").alias("payment_num"),
        "supp_id",
        "supp_num",
        "supp_name",
        "region_cd",
        F.col("legal_entity_cd").alias("org_cd"),
        F.to_date("payment_dt").alias("payment_dt"),
        F.to_date("gl_date").alias("gl_date"),
        "period_cd",
        F.to_date("cleared_dt").alias("cleared_dt"),
        F.col("payment_curr_cd").alias("currency_cd"),
        "payment_amt",
        "fx_rate",
        "accounted_amt",
        F.col("withheld_amt").alias("withholding_amt"),
        "discount_taken_amt",
        "applied_amt",
        "unapplied_amt",
        "payment_amt_usd",
        "payment_amt_usd_rate",
        "payment_amt_usd_rate_missing",
        "payment_method_cd",
        "payment_method_name",
        "settlement_days",
        F.col("supp_bank_account_id").alias("bank_acct_id"),
        "bank_account_cd",
        F.col("payment_batch_nbr").alias("payment_run_id"),
        "na_check_nbr",
        "na_ach_trace_nbr",
        "eu_sepa_e2e_id",
        "apac_transfer_ref",
        F.col("payment_status_cd").alias("status_cd"),
        "void_flag",
        F.to_date("void_dt").alias("void_dt"),
        "void_reason_cd",
        "reissue_of_payment_id",
        "reversal_flag",
        "invoice_count",
        "earliest_invoice_dt",
        "settlement_status_cd",
        "days_to_clear",
        "posted_flg",
        "journal_id",
        "remittance_email_txt",
        F.col("source_sys").alias("src_system_cd"),
        F.to_timestamp("created_dt").alias("created_dt"),
        F.to_timestamp("last_upd_dt").alias("last_upd_dt"),
    ]
    accepted = enriched.where(F.col("_reject_reason").isNull()).select(*cols)
    rejected = enriched.where(F.col("_reject_reason").isNotNull()).select(*cols, "_reject_reason")
    return accepted, rejected


def runPayments(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "EXT_ORA_ApPayment"
    wm = getWatermark(spark, cfg, pkg)
    pay = _lastUpd(sources.oracleTable(spark, cfg, "wwi_fin", "ap_payment")).where(
        F.col("last_upd_dt") > F.lit(wm).cast("timestamp")
    )
    accepted, rejected = transformPayments(
        pay,
        sources.oracleTable(spark, cfg, "wwi_fin", "ap_payment_apply"),
        sources.oracleTable(spark, cfg, "wwi_fin", "ap_invoice_hdr"),
        sources.supplierMaster(spark, cfg),
        sources.oracleTable(spark, cfg, "wwi_ref", "payment_method_ref"),
        sources.fxRates(spark, cfg),
        sources.currencies(spark, cfg),
        sources.regionRef(spark, cfg),
        sources.calendarFiscal(spark, cfg),
        cfg.businessDate,
    )
    accepted = _meta(accepted, batchId).cache()
    rowsRead = pay.count()
    written = mergeTable(spark, accepted, cfg.table(BRONZE_PAYMENT), ["payment_id"])
    rej = writeRejects(
        spark,
        cfg,
        rejected,
        pkg,
        "EXTRACT",
        "WWI_FIN.AP_PAYMENT",
        "payment_id",
        "_reject_reason",
        batchId=batchId,
    )
    newWm = accepted.agg(F.max("last_upd_dt")).collect()[0][0]
    setWatermark(spark, cfg, pkg, "last_upd_dt", newWm, written)
    return PackageResult(pkg, rowsRead, written, rej, notes={"watermark_in": wm, "watermark_out": str(newWm)})


# --------------------------------------------------------------------------- EXT_ORA_ApPaymentApply
def transformPaymentApplies(applies: DataFrame, pay: DataFrame, hdr: DataFrame) -> DataFrame:
    p = pay.select(
        "payment_id",
        F.col("payment_nbr").alias("payment_num"),
        F.col("supp_id").alias("payment_supp_id"),
        F.col("region_cd").alias("payment_region_cd"),
        F.to_date("payment_dt").alias("payment_dt"),
        F.col("payment_curr_cd").alias("payment_currency_cd"),
        F.col("payment_status_cd").alias("payment_status_cd"),
        F.to_date("void_dt").alias("payment_void_dt"),
    )
    i = hdr.select(
        "invoice_id",
        F.col("invoice_nbr").alias("invoice_num"),
        F.col("supp_id").alias("invoice_supp_id"),
        F.col("region_cd").alias("invoice_region_cd"),
        F.to_date("invoice_dt").alias("invoice_dt"),
        F.to_date("due_dt").alias("due_dt"),
        F.to_date("discount_due_dt").alias("discount_due_dt"),
        F.col("gross_amt").alias("invoice_gross_amt"),
        F.col("invoice_status_cd").alias("invoice_status_cd"),
    )
    out = (
        _lastUpd(applies)
        .join(p, "payment_id", "inner")
        .join(i, "invoice_id", "inner")
        .withColumn("reversed_flag", F.coalesce(F.col("reversed_flg"), F.lit("N")))
        .withColumn(
            "discount_taken_flag",
            F.when(F.coalesce(F.col("discount_amt"), F.lit(0.0)) > 0, "Y").otherwise("N"),
        )
        .withColumn(
            "net_applied_amt",
            (
                F.col("applied_amt")
                - F.coalesce(F.col("discount_amt"), F.lit(0.0))
                - F.coalesce(F.col("withheld_amt"), F.lit(0.0))
            ).cast("decimal(18,5)"),
        )
        .withColumn("settlement_days", F.datediff(F.to_date("apply_dt"), F.col("invoice_dt")))
        .withColumn("days_vs_due", F.datediff(F.to_date("apply_dt"), F.col("due_dt")))
        .withColumn(
            "supplier_mismatch_flag",
            F.when(F.col("payment_supp_id") != F.col("invoice_supp_id"), "Y").otherwise("N"),
        )
    )
    return out.select(
        "apply_id",
        "payment_id",
        "payment_num",
        "invoice_id",
        "invoice_num",
        F.col("payment_supp_id").alias("supp_id"),
        "invoice_supp_id",
        "supplier_mismatch_flag",
        F.col("payment_region_cd").alias("region_cd"),
        F.col("apply_seq_nbr").alias("apply_seq"),
        "applied_amt",
        F.col("applied_curr_cd").alias("currency_cd"),
        "discount_amt",
        "withheld_amt",
        "fx_gain_loss_amt",
        "net_applied_amt",
        "discount_taken_flag",
        F.to_date("apply_dt").alias("apply_dt"),
        F.to_date("gl_date").alias("gl_date"),
        "apply_type_cd",
        "reversed_flag",
        F.to_date("reversal_dt").alias("reversal_dt"),
        "reversal_reason_cd",
        "payment_dt",
        "payment_currency_cd",
        "payment_status_cd",
        "payment_void_dt",
        "invoice_dt",
        "due_dt",
        "discount_due_dt",
        "invoice_gross_amt",
        "invoice_status_cd",
        "settlement_days",
        "days_vs_due",
        "posted_flg",
        "journal_id",
        F.col("source_sys").alias("src_system_cd"),
        F.to_timestamp("created_dt").alias("created_dt"),
        F.to_timestamp("last_upd_dt").alias("last_upd_dt"),
    )


def runPaymentApplies(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "EXT_ORA_ApPaymentApply"
    wm = getWatermark(spark, cfg, pkg)
    wmKey = 0 if wm == "1900-01-01 00:00:00" else int(float(wm))
    applies = sources.oracleTable(spark, cfg, "wwi_fin", "ap_payment_apply").where(F.col("apply_id") > wmKey)
    out = transformPaymentApplies(
        applies,
        sources.oracleTable(spark, cfg, "wwi_fin", "ap_payment"),
        sources.oracleTable(spark, cfg, "wwi_fin", "ap_invoice_hdr"),
    )
    out = _meta(out, batchId).cache()
    rowsRead = applies.count()
    written = mergeTable(spark, out, cfg.table(BRONZE_PAYMENT_APPLY), ["apply_id"])
    newWm = out.agg(F.max("apply_id")).collect()[0][0]
    setWatermark(spark, cfg, pkg, "apply_id", newWm, written)
    return PackageResult(pkg, rowsRead, written, 0, notes={"watermark_in": wm, "watermark_out": str(newWm)})


# --------------------------------------------------------------------------- EXT_ORA_ApAging
def transformApAging(snap: DataFrame, suppliers: DataFrame) -> DataFrame:
    """Full reload of the aging snapshots; `is_current_snapshot` marks V_AP_AGING_CURRENT rows."""
    latest = Window.partitionBy("region_cd")
    out = (
        _lastUpd(snap)
        .join(suppliers, "supp_id", "inner")
        .withColumn("snapshot_dt", F.to_date("snapshot_dt"))
        .withColumn("_max_dt", F.max("snapshot_dt").over(latest))
        .withColumn("is_current_snapshot", F.col("snapshot_dt") == F.col("_max_dt"))
        .withColumn("aging_bucket_cd", oracleAgingBucket(F.col("avg_days_outstanding"), F.col("region_cd")))
        .withColumn("hold_flag", F.when(F.coalesce(F.col("on_hold_amt"), F.lit(0.0)) > 0, "Y").otherwise("N"))
        .withColumn(
            "dispute_flag", F.when(F.coalesce(F.col("disputed_amt"), F.lit(0.0)) > 0, "Y").otherwise("N")
        )
        .withColumn(
            "collection_action_cd",
            F.when(F.coalesce(F.col("disputed_amt"), F.lit(0.0)) > 0, "EXCLUDE")
            .when(F.coalesce(F.col("on_hold_amt"), F.lit(0.0)) > 0, "HOLD")
            .when(F.coalesce(F.col("avg_days_outstanding"), F.lit(0.0)) > 90, "ESCALATE")
            .otherwise("NORMAL"),
        )
        .withColumn(
            "past_due_amt",
            (
                F.coalesce(F.col("bucket_1_amt"), F.lit(0.0))
                + F.coalesce(F.col("bucket_2_amt"), F.lit(0.0))
                + F.coalesce(F.col("bucket_3_amt"), F.lit(0.0))
                + F.coalesce(F.col("bucket_4_amt"), F.lit(0.0))
            ).cast("decimal(18,5)"),
        )
    )
    return out.select(
        "snapshot_id",
        "snapshot_dt",
        "is_current_snapshot",
        "period_cd",
        "supp_id",
        "supp_num",
        "supp_name",
        "region_cd",
        "legal_entity_cd",
        F.col("balance_curr_cd").alias("currency_cd"),
        "open_invoice_cnt",
        F.to_date("oldest_invoice_dt").alias("oldest_invoice_dt"),
        "avg_days_outstanding",
        "current_amt",
        "bucket_1_amt",
        "bucket_2_amt",
        "bucket_3_amt",
        "bucket_4_amt",
        "past_due_amt",
        F.col("total_outstanding_amt").alias("outstanding_amt"),
        F.col("reporting_amt_usd").alias("outstanding_base_amt"),
        "disputed_amt",
        "on_hold_amt",
        "bucket_definition_cd",
        "aging_bucket_cd",
        "hold_flag",
        "dispute_flag",
        "collection_action_cd",
        "fx_rate_used",
        F.to_timestamp("calculated_dt").alias("calculated_dt"),
        F.col("source_sys").alias("src_system_cd"),
        F.to_timestamp("created_dt").alias("created_dt"),
        F.to_timestamp("last_upd_dt").alias("last_upd_dt"),
    )


def runApAging(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "EXT_ORA_ApAging"
    snap = sources.oracleTable(spark, cfg, "wwi_fin", "ap_aging_snapshot")
    out = _meta(transformApAging(snap, sources.supplierMaster(spark, cfg)), batchId)
    written = overwriteTable(out, cfg.table(BRONZE_AP_AGING))
    return PackageResult(pkg, snap.count(), written, 0, notes={"load_type": "full"})


# --------------------------------------------------------------------------- EXT_ORA_GlJournalLine
def transformGlJournalLines(
    lines: DataFrame,
    hdr: DataFrame,
    accounts: DataFrame,
    costCenters: DataFrame,
    periods: DataFrame,
    regions: DataFrame,
    calendar: DataFrame,
    rates: DataFrame,
    currencies: DataFrame,
    windowStart: date,
    windowEnd: date,
) -> DataFrame:
    """Posted journal lines in the accounting-date window, excluding STAT accounts and FUTR periods.

    Deviation: the Oracle period-status table is keyed by ledger codes (NA_USD/EU_EUR/AP_AUD) that do not
    match the journal ledgers (USD_PRI/EUR_PRI/APAC_PRI) so we join on region + derived fiscal period.
    """
    h = hdr.select(
        "journal_id",
        F.col("journal_nbr").alias("journal_num"),
        "ledger_cd",
        "journal_source_cd",
        "journal_category_cd",
        "region_cd",
        F.col("legal_entity_cd").alias("org_cd"),
        F.col("period_cd").alias("hdr_period_cd"),
        F.to_date("accounting_dt").alias("gl_date"),
        "posting_status_cd",
        F.to_date("posted_dt").alias("posted_dt"),
        F.col("reversal_flg").alias("reversal_flag"),
        F.col("reversal_of_journal_id").alias("reversed_journal_id"),
        F.col("accrual_flg").alias("accrual_flag"),
        F.col("journal_curr_cd").alias("journal_currency_cd"),
        F.col("total_debit_amt").alias("journal_total_debit_amt"),
        F.col("total_credit_amt").alias("journal_total_credit_amt"),
        F.col("line_cnt").alias("journal_line_cnt"),
        F.col("created_by").alias("hdr_created_by"),
        F.col("created_dt").alias("hdr_created_dt"),
        F.coalesce(F.col("updated_dt"), F.col("created_dt")).alias("hdr_last_upd_dt"),
    )
    ga = accounts.select(
        F.col("account_cd").alias("_acct_cd"),
        "gl_account_id",
        "account_name",
        "account_type_cd",
        "account_class_cd",
        "normal_balance_cd",
        "reconciliation_flg",
    )
    cc = costCenters.select(
        F.col("cost_center_cd").alias("_cc_cd"),
        "cost_center_id",
        "cost_center_name",
        F.col("region_cd").alias("cost_center_region_cd"),
    )
    ps = periods.select(
        F.col("region_cd").alias("_ps_region"),
        F.col("period_cd").alias("_ps_period"),
        F.col("gl_status_cd").alias("period_status_cd"),
        F.col("ap_status_cd").alias("ap_period_status_cd"),
        F.col("closed_dt").alias("period_closed_dt"),
        F.col("period_end_dt").alias("period_end_dt"),
    )
    base = (
        lines.join(h, "journal_id", "inner")
        .where((F.col("gl_date") >= F.lit(windowStart)) & (F.col("gl_date") <= F.lit(windowEnd)))
        .where(F.col("posting_status_cd") == "POST")
        .join(ga, F.col("account_cd") == F.col("_acct_cd"), "inner")
        .where(F.col("account_type_cd") != "STAT")
        .join(cc, F.col("cost_center_cd") == F.col("_cc_cd"), "left")
    )
    base = withFiscalPeriod(base, "gl_date", "region_cd", regions, calendar, "fiscal_period_cd")
    base = base.join(
        ps,
        (F.upper(F.col("region_cd")) == F.col("_ps_region"))
        & (F.col("fiscal_period_cd") == F.col("_ps_period")),
        "left",
    )
    base = base.where(F.coalesce(F.col("period_status_cd"), F.lit("HIST")) != "FUTR")
    base = base.withColumn("_usd_ccy", F.lit("USD"))
    base = convertAmounts(
        base, rates, currencies, "entered_debit_amt", "entered_curr_cd", "_usd_ccy", "gl_date", "_dr_usd"
    )
    base = convertAmounts(
        base, rates, currencies, "entered_credit_amt", "entered_curr_cd", "_usd_ccy", "gl_date", "_cr_usd"
    )
    debit = F.coalesce(F.col("entered_debit_amt"), F.lit(0.0)).cast("decimal(18,5)")
    credit = F.coalesce(F.col("entered_credit_amt"), F.lit(0.0)).cast("decimal(18,5)")
    out = (
        base.withColumn("debit_amt", debit)
        .withColumn("credit_amt", credit)
        .withColumn("net_amt", (debit - credit).cast("decimal(18,5)"))
        .withColumn(
            "base_debit_amt_usd",
            F.coalesce(F.col("accounted_debit_amt"), F.col("_dr_usd")).cast("decimal(18,5)"),
        )
        .withColumn(
            "base_credit_amt_usd",
            F.coalesce(F.col("accounted_credit_amt"), F.col("_cr_usd")).cast("decimal(18,5)"),
        )
        .withColumn("period_status_cd", F.coalesce(F.col("period_status_cd"), F.lit("HIST")))
        .withColumn("posted_flag", F.when(F.col("posting_status_cd") == "POST", "Y").otherwise("N"))
        .withColumn("fiscal_year_nbr", F.substring(F.col("fiscal_period_cd"), 1, 4).cast("int"))
        .withColumn("fiscal_period_nbr", F.substring(F.col("fiscal_period_cd"), 6, 2).cast("int"))
        .withColumn("_upd", F.coalesce(F.col("updated_dt"), F.col("created_dt")))
    )
    return out.select(
        "journal_line_id",
        "journal_id",
        "journal_num",
        "ledger_cd",
        "journal_source_cd",
        "journal_category_cd",
        "region_cd",
        "org_cd",
        "hdr_period_cd",
        "fiscal_period_cd",
        "fiscal_year_nbr",
        "fiscal_period_nbr",
        "period_status_cd",
        "ap_period_status_cd",
        "period_closed_dt",
        "period_end_dt",
        "gl_date",
        "posting_status_cd",
        "posted_flag",
        "posted_dt",
        "reversal_flag",
        "reversed_journal_id",
        "accrual_flag",
        F.col("line_nbr").alias("line_num"),
        "gl_account_id",
        "account_cd",
        "account_name",
        "account_type_cd",
        "account_class_cd",
        "normal_balance_cd",
        "reconciliation_flg",
        "cost_center_cd",
        "cost_center_id",
        "cost_center_name",
        "project_cd",
        "intercompany_cd",
        F.col("entered_curr_cd").alias("currency_cd"),
        "journal_currency_cd",
        "debit_amt",
        "credit_amt",
        "net_amt",
        "accounted_debit_amt",
        "accounted_credit_amt",
        "base_debit_amt_usd",
        "base_credit_amt_usd",
        "journal_total_debit_amt",
        "journal_total_credit_amt",
        "journal_line_cnt",
        "line_desc",
        F.col("source_doc_type_cd").alias("src_doc_type_cd"),
        F.col("source_doc_id").alias("src_doc_id"),
        "tax_code_cd",
        "reconciled_flg",
        F.col("hdr_created_by").alias("created_by"),
        F.to_timestamp("hdr_created_dt").alias("created_dt"),
        F.greatest(F.to_timestamp("_upd"), F.to_timestamp("hdr_last_upd_dt")).alias("last_upd_dt"),
    )


def runGlJournalLines(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "EXT_ORA_GlJournalLine"
    windowStart = date.fromisoformat(cfg.extra.get("window_start", "1900-01-01"))
    windowEnd = cfg.businessDate
    lines = sources.oracleTable(spark, cfg, "wwi_fin", "gl_journal_line")
    out = transformGlJournalLines(
        lines,
        sources.oracleTable(spark, cfg, "wwi_fin", "gl_journal_hdr"),
        sources.glAccounts(spark, cfg),
        sources.costCenters(spark, cfg),
        sources.periodStatus(spark, cfg),
        sources.regionRef(spark, cfg),
        sources.calendarFiscal(spark, cfg),
        sources.fxRates(spark, cfg),
        sources.currencies(spark, cfg),
        windowStart,
        windowEnd,
    )
    out = _meta(out, batchId)
    written = deleteInsert(
        spark,
        out,
        cfg.table(BRONZE_GL_JOURNAL_LINE),
        f"gl_date >= '{windowStart}' AND gl_date <= '{windowEnd}'",
    )
    return PackageResult(
        pkg, lines.count(), written, 0, notes={"window_start": str(windowStart), "window_end": str(windowEnd)}
    )


# --------------------------------------------------------------------------- EXT_ORA_CostCenter
def buildHierarchy(cc: DataFrame, maxDepth: int = 12) -> DataFrame:
    """Iterative CONNECT BY: depth, '/'-separated rollup path and root for every cost center.
    Orphans (parent code not present) and cycles are left with NULL depth (Oracle NOCYCLE drops them)."""
    nodes = cc.select("cost_center_cd", "parent_cost_center_cd")
    roots = nodes.where(F.col("parent_cost_center_cd").isNull()).select(
        "cost_center_cd",
        F.lit(1).alias("depth_num"),
        F.concat(F.lit("/"), F.col("cost_center_cd")).alias("rollup_path"),
        F.col("cost_center_cd").alias("root_cost_center_cd"),
    )
    result = roots
    frontier = roots
    for _ in range(maxDepth):
        nxt = (
            nodes.alias("n")
            .join(frontier.alias("f"), F.col("n.parent_cost_center_cd") == F.col("f.cost_center_cd"))
            .where(
                ~F.col("f.rollup_path").contains(F.concat(F.lit("/"), F.col("n.cost_center_cd"), F.lit("/")))
            )
            .select(
                F.col("n.cost_center_cd").alias("cost_center_cd"),
                (F.col("f.depth_num") + 1).alias("depth_num"),
                F.concat(F.col("f.rollup_path"), F.lit("/"), F.col("n.cost_center_cd")).alias("rollup_path"),
                F.col("f.root_cost_center_cd").alias("root_cost_center_cd"),
            )
        )
        if nxt.isEmpty():
            break
        result = result.unionByName(nxt)
        frontier = nxt
    return result.dropDuplicates(["cost_center_cd"])


def transformCostCenters(cc: DataFrame, rules: DataFrame) -> DataFrame:
    parent = cc.select(
        F.col("cost_center_cd").alias("_parent_cd"),
        F.col("region_cd").alias("parent_region_cd"),
        F.col("cost_center_name").alias("parent_cost_center_name"),
    )
    ruleCount = (
        rules.where(F.coalesce(F.col("active_flg"), F.lit("N")) == "Y")
        .groupBy(F.col("source_cost_center_cd").alias("_rule_cc"))
        .agg(F.count("*").alias("alloc_rule_count"))
    )
    hier = buildHierarchy(cc)
    out = (
        _lastUpd(cc)
        .join(parent, F.col("parent_cost_center_cd") == F.col("_parent_cd"), "left")
        .join(hier, "cost_center_cd", "left")
        .join(ruleCount, F.col("cost_center_cd") == F.col("_rule_cc"), "left")
        .withColumn(
            "rollup_anomaly_cd",
            F.when(F.col("parent_cost_center_cd").isNull(), "ROOT")
            .when(F.col("_parent_cd").isNull(), "ORPHAN")
            .when(F.col("parent_region_cd") != F.col("region_cd"), "CROSS_REGION")
            .otherwise("NORMAL"),
        )
        .withColumn("alloc_rule_count", F.coalesce(F.col("alloc_rule_count"), F.lit(0)))
        .withColumn("active_flag", F.coalesce(F.col("active_flg"), F.lit("Y")))
    )
    return out.select(
        "cost_center_id",
        "cost_center_cd",
        "cost_center_name",
        "parent_cost_center_cd",
        "parent_cost_center_name",
        "region_cd",
        "parent_region_cd",
        "rollup_anomaly_cd",
        "country_cd",
        "legal_entity_cd",
        "hierarchy_level_nbr",
        "depth_num",
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
        "active_flag",
        F.to_date("opened_dt").alias("eff_from_dt"),
        F.to_date("closed_dt").alias("eff_to_dt"),
        "legacy_dept_cd",
        "alloc_rule_count",
        F.col("source_sys").alias("src_system_cd"),
        F.to_timestamp("created_dt").alias("created_dt"),
        F.to_timestamp("last_upd_dt").alias("last_upd_dt"),
    )


def runCostCenters(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "EXT_ORA_CostCenter"
    cc = sources.costCenters(spark, cfg)
    out = _meta(transformCostCenters(cc, sources.allocationRules(spark, cfg)), batchId)
    written = overwriteTable(out, cfg.table(BRONZE_COST_CENTER))
    return PackageResult(pkg, cc.count(), written, 0, notes={"load_type": "full"})
