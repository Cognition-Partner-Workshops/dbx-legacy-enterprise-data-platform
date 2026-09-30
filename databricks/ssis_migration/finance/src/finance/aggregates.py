"""AGG_Refresh_FinanceCloseSummary: full rebuild of the finance close summary (Master_Month_End)."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from finance.close import (
    FIN_AP_AGING_SUMMARY,
    FIN_COST_ALLOCATION,
    FIN_CURRENCY_REVALUATION,
    FIN_PERIOD_LOCK,
    FIN_RECON_RESULT,
    FIN_WITHHOLDING_TAX,
)
from finance.config import FinanceConfig
from finance.facts import GOLD_FACT_GL_POSTING, GOLD_FACT_PAYMENT, LOCK_SCHEMA
from finance.io import PackageResult, overwriteTable, readTable, readTableOrEmpty
from finance.rules import closeTolerance
from finance.staging import SILVER_INVOICE

GOLD_AGG_FINANCE_CLOSE_SUMMARY = "gold_agg_finance_close_summary"
KEYS = ["region_code", "accounting_period"]


def buildFinanceCloseSummary(
    glPostings: DataFrame,
    invoices: DataFrame,
    payments: DataFrame,
    agingSummary: DataFrame,
    recon: DataFrame,
    locks: DataFrame,
    allocations: DataFrame,
    withholding: DataFrame,
    revaluation: DataFrame,
) -> DataFrame:
    gl = glPostings.groupBy(*KEYS).agg(
        F.countDistinct("journal_id").alias("gl_journal_count"),
        F.count("*").alias("gl_posting_count"),
        F.sum("debit_amount").alias("gl_debit_total"),
        F.sum("credit_amount").alias("gl_credit_total"),
        (F.sum("base_debit_amt_usd") - F.sum("base_credit_amt_usd")).alias("gl_net_usd"),
        F.sum(F.when(~F.col("journal_balanced"), 1).otherwise(0)).alias("gl_unbalanced_lines"),
    )
    inv = (
        invoices.where(~F.col("invoice_status_code").isin("CANC", "VOID", "DRAFT"))
        .withColumnRenamed("period_cd", "accounting_period")
        .groupBy(*KEYS)
        .agg(
            F.count("*").alias("ap_invoice_count"),
            F.sum("base_amt_usd").alias("ap_invoice_amount_usd"),
            F.sum("open_amount").alias("ap_open_amount"),
            F.sum(F.when(F.col("is_on_hold"), 1).otherwise(0)).alias("ap_invoices_on_hold"),
        )
    )
    pay = (
        payments.withColumnRenamed("period_cd", "accounting_period")
        .groupBy(*KEYS)
        .agg(
            F.count("*").alias("payment_count"),
            F.sum("payment_amt_usd").alias("payment_amount_usd"),
            F.sum("unapplied_amount").alias("payment_unapplied_amount"),
            F.sum(F.when(F.col("is_orphan"), 1).otherwise(0)).alias("orphan_payment_count"),
        )
    )
    latestAging = (
        agingSummary.where(F.col("as_of_date") == agingSummary.agg(F.max("as_of_date")).collect()[0][0])
        if agingSummary.limit(1).count()
        else agingSummary
    )
    aging = latestAging.groupBy("region_code").agg(
        F.sum(F.when(F.col("aging_bucket_code") == "CURRENT", F.col("open_amount")).otherwise(0)).alias(
            "aging_current_amount"
        ),
        F.sum(F.when(F.col("aging_bucket_code") == "B030", F.col("open_amount")).otherwise(0)).alias(
            "aging_1_30_amount"
        ),
        F.sum(F.when(F.col("aging_bucket_code") == "B060", F.col("open_amount")).otherwise(0)).alias(
            "aging_31_60_amount"
        ),
        F.sum(F.when(F.col("aging_bucket_code") == "B090", F.col("open_amount")).otherwise(0)).alias(
            "aging_61_90_amount"
        ),
        F.sum(F.when(F.col("aging_bucket_code") == "B090P", F.col("open_amount")).otherwise(0)).alias(
            "aging_over_90_amount"
        ),
        F.max("as_of_date").alias("aging_as_of_date"),
    )
    rec = recon.groupBy(*KEYS).agg(
        F.sum("variance_amount").alias("recon_variance_amount"),
        F.sum(F.when(F.col("recon_status") == "VARIANCE", 1).otherwise(0)).alias("recon_variance_count"),
    )
    lk = locks.groupBy(*KEYS).agg(
        F.max_by("lock_status", "locked_at").alias("period_lock_status"),
        F.max("locked_at").alias("period_locked_at"),
    )
    alloc = (
        allocations.where(F.col("allocation_status") != "UNALLOCATED")
        .withColumnRenamed("region_cd", "region_code")
        .groupBy(*KEYS)
        .agg(
            F.sum("allocated_amount").alias("cost_allocated_amount"),
            F.count("*").alias("cost_allocation_rule_count"),
        )
    )
    wht = withholding.groupBy(*KEYS).agg(
        F.sum("withholding_amount").alias("withholding_amount"),
        F.sum(F.when(F.col("is_withheld"), 1).otherwise(0)).alias("withheld_line_count"),
    )
    reval = (
        revaluation.withColumnRenamed("period_cd", "accounting_period")
        .groupBy(*KEYS)
        .agg(
            F.sum("unrealized_gain_loss").alias("unrealized_fx_gain_loss"),
            F.count("*").alias("revalued_item_count"),
        )
    )
    spine = gl.select(*KEYS).unionByName(inv.select(*KEYS)).unionByName(pay.select(*KEYS)).distinct()
    out = (
        spine.join(gl, KEYS, "left")
        .join(inv, KEYS, "left")
        .join(pay, KEYS, "left")
        .join(aging, "region_code", "left")
        .join(rec, KEYS, "left")
        .join(lk, KEYS, "left")
        .join(alloc, KEYS, "left")
        .join(wht, KEYS, "left")
        .join(reval, KEYS, "left")
    )
    return (
        out.withColumn(
            "gl_out_of_balance_amount",
            F.coalesce(F.col("gl_debit_total"), F.lit(0)) - F.coalesce(F.col("gl_credit_total"), F.lit(0)),
        )
        .withColumn("materiality_threshold", closeTolerance(F.col("region_code")))
        .withColumn(
            "is_within_materiality",
            F.abs(F.coalesce(F.col("recon_variance_amount"), F.lit(0))) <= F.col("materiality_threshold"),
        )
        .withColumn("period_lock_status", F.coalesce(F.col("period_lock_status"), F.lit("OPEN")))
        .withColumn(
            "close_status",
            F.when(F.col("period_lock_status") == "LOCKED", "CLOSED")
            .when(F.col("is_within_materiality"), "READY")
            .otherwise("VARIANCE"),
        )
        .withColumn("refreshed_at", F.current_timestamp())
    )


def runAggFinanceCloseSummary(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "AGG_Refresh_FinanceCloseSummary"
    gl = readTable(spark, cfg.table(GOLD_FACT_GL_POSTING))
    out = buildFinanceCloseSummary(
        gl,
        readTable(spark, cfg.table(SILVER_INVOICE)),
        readTable(spark, cfg.table(GOLD_FACT_PAYMENT)),
        readTableOrEmpty(
            spark,
            cfg.table(FIN_AP_AGING_SUMMARY),
            "as_of_date date, region_code string, aging_bucket_code string, open_amount decimal(18,5)",
        ),
        readTableOrEmpty(
            spark,
            cfg.table(FIN_RECON_RESULT),
            "region_code string, accounting_period string, variance_amount decimal(18,5), recon_status string",
        ),
        readTableOrEmpty(spark, cfg.table(FIN_PERIOD_LOCK), LOCK_SCHEMA),
        readTableOrEmpty(
            spark,
            cfg.table(FIN_COST_ALLOCATION),
            "region_cd string, accounting_period string, allocated_amount double, allocation_status string",
        ),
        readTableOrEmpty(
            spark,
            cfg.table(FIN_WITHHOLDING_TAX),
            "region_code string, accounting_period string, withholding_amount decimal(18,5), is_withheld boolean",
        ),
        readTableOrEmpty(
            spark,
            cfg.table(FIN_CURRENCY_REVALUATION),
            "region_code string, period_cd string, unrealized_gain_loss decimal(18,5)",
        ),
    ).withColumn("batch_id", F.lit(batchId).cast("bigint"))
    written = overwriteTable(out, cfg.table(GOLD_AGG_FINANCE_CLOSE_SUMMARY))
    return PackageResult(pkg, gl.count(), written, 0, notes={"load_type": "aggregate_rebuild"})
