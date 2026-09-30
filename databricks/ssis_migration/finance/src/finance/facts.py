"""REF_Load_CostCenter (SCD2 dimension) and the three incremental facts."""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from finance.config import FinanceConfig
from finance.io import (
    PackageResult,
    mergeTable,
    overwriteTable,
    readTable,
    readTableOrEmpty,
    tableExists,
    writeRejects,
)
from finance.rules import DEFAULT_TOLERANCE, rowHash
from finance.staging import (
    SILVER_COST_CENTER,
    SILVER_GL_JOURNAL_LINE,
    SILVER_INVOICE,
    SILVER_PAYMENT,
    UNKNOWN_MEMBER_KEY,
    WORK_PAYMENT_MATCHED,
)

GOLD_DIM_COST_CENTER = "gold_dim_cost_center"
GOLD_FACT_GL_POSTING = "gold_fact_gl_posting"
GOLD_FACT_PAYMENT = "gold_fact_payment"
GOLD_FACT_SUPPLIER_PAYMENT = "gold_fact_supplier_payment"
FIN_PERIOD_LOCK = "fin_period_lock"
END_OF_TIME = "9999-12-31 00:00:00"

SCD2_ATTRS = [
    "cost_center_name",
    "parent_cost_center_code",
    "region_code",
    "hierarchy_level",
    "rollup_path",
    "root_cost_center_cd",
    "rollup_anomaly_cd",
    "manager_cd",
    "manager_name_txt",
    "default_gl_account_cd",
    "legal_entity_cd",
    "functional_curr_cd",
    "allocation_basis_cd",
    "is_active",
    "eff_from_dt",
    "eff_to_dt",
    "scd_hash",
]
DIM_SCHEMA = (
    "cost_center_key int, cost_center_code string, cost_center_name string, parent_cost_center_code string, "
    "region_code string, hierarchy_level int, rollup_path string, root_cost_center_cd string, rollup_anomaly_cd string, "
    "manager_cd string, manager_name_txt string, default_gl_account_cd string, legal_entity_cd string, "
    "functional_curr_cd string, allocation_basis_cd string, is_active boolean, eff_from_dt date, eff_to_dt date, "
    "scd_hash string, valid_from timestamp, valid_to timestamp, is_current boolean, batch_id bigint"
)


# --------------------------------------------------------------------------- SCD2
def unknownCostCenterRow(spark: SparkSession) -> DataFrame:
    row = {c: None for c in DIM_SCHEMA.replace(",", "").split()[0::2]}
    row.update(
        cost_center_key=UNKNOWN_MEMBER_KEY,
        cost_center_code="UNKNOWN",
        cost_center_name="Unknown",
        region_code="UNK",
        is_active=True,
        valid_from=datetime(1900, 1, 1),
        valid_to=datetime(9999, 12, 31),
        is_current=True,
        batch_id=0,
    )
    return spark.createDataFrame([row], DIM_SCHEMA)


def applyScd2(
    existing: DataFrame, incoming: DataFrame, asOf: datetime, batchId: int, key: str = "cost_center_code"
) -> DataFrame:
    """Type-2 merge: changed hash -> expire current row and insert a new version; new key -> insert;
    unchanged -> keep.  Rows missing from `incoming` are left untouched (SSIS SCD does not infer deletes).
    Returns the full new dimension (unknown member preserved)."""
    inc = incoming.select(key, *SCD2_ATTRS)
    cur = existing.where(F.col("is_current")).select(
        key, F.col("scd_hash").alias("_cur_hash"), F.col("cost_center_key").alias("_cur_key")
    )
    classified = inc.join(cur, key, "left").withColumn(
        "_action",
        F.when(F.col("_cur_hash").isNull(), "INSERT")
        .when(F.col("_cur_hash") != F.col("scd_hash"), "CHANGE")
        .otherwise("KEEP"),
    )
    changedKeys = classified.where(F.col("_action") == "CHANGE").select(key)
    expired = (
        existing.join(changedKeys, key, "left_semi")
        .where(F.col("is_current"))
        .withColumn("valid_to", F.lit(asOf).cast("timestamp"))
        .withColumn("is_current", F.lit(False))
    )
    untouched = existing.join(expired.select(key, "valid_from"), [key, "valid_from"], "left_anti")
    maxKey = existing.agg(F.max("cost_center_key")).collect()[0][0] or 0
    newRows = (
        classified.where(F.col("_action").isin("INSERT", "CHANGE"))
        .withColumn("_rn", F.row_number().over(Window.orderBy(key)))
        .withColumn("cost_center_key", (F.lit(maxKey) + F.col("_rn")).cast("int"))
        .withColumn("valid_from", F.lit(asOf).cast("timestamp"))
        .withColumn("valid_to", F.lit(END_OF_TIME).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .select(*existing.columns)
    )
    return untouched.unionByName(expired).unionByName(newRows)


def runRefCostCenter(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "REF_Load_CostCenter"
    fq = cfg.table(GOLD_DIM_COST_CENTER)
    incoming = readTable(spark, cfg.table(SILVER_COST_CENTER))
    existing = readTable(spark, fq) if tableExists(spark, fq) else unknownCostCenterRow(spark)
    if not existing.where(F.col("cost_center_key") == UNKNOWN_MEMBER_KEY).limit(1).count():
        existing = existing.unionByName(unknownCostCenterRow(spark))
    newDim = applyScd2(existing, incoming, datetime.now(timezone.utc), batchId).cache()
    inserted = newDim.where(F.col("batch_id") == batchId).count()
    written = overwriteTable(newDim, fq)
    return PackageResult(
        pkg, incoming.count(), written, 0, notes={"new_versions": inserted, "load_type": "scd2"}
    )


def currentCostCenterKeys(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return (
        readTable(spark, cfg.table(GOLD_DIM_COST_CENTER))
        .where(F.col("is_current"))
        .select(
            F.col("cost_center_code").alias("_dim_cc_code"), F.col("cost_center_key").alias("_dim_cc_key")
        )
    )


def withCostCenterKey(df: DataFrame, dim: DataFrame, codeCol: str = "cost_center_code") -> DataFrame:
    return (
        df.join(dim, F.col(codeCol) == F.col("_dim_cc_code"), "left")
        .withColumn(
            "cost_center_key", F.coalesce(F.col("_dim_cc_key"), F.lit(UNKNOWN_MEMBER_KEY)).cast("int")
        )
        .drop("_dim_cc_code", "_dim_cc_key")
    )


# --------------------------------------------------------------------------- period lock gate
LOCK_SCHEMA = (
    "ledger_code string, region_code string, accounting_period string, lock_status string, locked_at timestamp, "
    "locked_by_batch_id bigint, variance_count int, note string"
)


def readPeriodLocks(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return readTableOrEmpty(spark, cfg.table(FIN_PERIOD_LOCK), LOCK_SCHEMA)


def closedPeriods(locks: DataFrame) -> DataFrame:
    """(region_code, accounting_period) pairs that are locked by FIN_Close_PeriodLock."""
    return (
        locks.where(F.col("lock_status") == "LOCKED")
        .select(F.col("region_code").alias("_lk_region"), F.col("accounting_period").alias("_lk_period"))
        .distinct()
    )


def isPeriodClosed(sourceStatus: F.Column, lockRegion: F.Column) -> F.Column:
    """A period is not open when Oracle says CLSD/PERM or when our lock table has LOCKED it."""
    return sourceStatus.isin("CLSD", "PERM") | lockRegion.isNotNull()


# --------------------------------------------------------------------------- FACT_Load_GLPosting
def journalBalance(lines: DataFrame, tolerance: float = DEFAULT_TOLERANCE) -> DataFrame:
    """Whole-journal gate: debits == credits (within tolerance) and every line of the journal is present."""
    return (
        lines.groupBy("journal_id")
        .agg(
            F.sum("debit_amount").alias("journal_debit_total"),
            F.sum("credit_amount").alias("journal_credit_total"),
            F.count("*").alias("lines_present"),
            F.max("journal_line_cnt").alias("lines_expected"),
        )
        .withColumn("journal_imbalance", F.abs(F.col("journal_debit_total") - F.col("journal_credit_total")))
        .withColumn(
            "journal_complete",
            F.col("lines_expected").isNull() | (F.col("lines_present") >= F.col("lines_expected")),
        )
        .withColumn("journal_balanced", F.col("journal_imbalance") <= tolerance)
    )


def buildGlPostingFact(
    lines: DataFrame,
    ccDim: DataFrame,
    locks: DataFrame,
    allowUnbalanced: bool = False,
    tolerance: float = DEFAULT_TOLERANCE,
) -> tuple[DataFrame, DataFrame]:
    """Returns (postable, rejected).  Reject reasons: JOURNAL_UNBALANCED, JOURNAL_INCOMPLETE, PERIOD_NOT_OPEN."""
    posted = lines.where(F.col("posted_flag") == "Y")
    bal = journalBalance(posted, tolerance)
    lk = closedPeriods(locks)
    enriched = (
        withCostCenterKey(posted, ccDim)
        .join(bal, "journal_id", "left")
        .join(
            lk,
            (F.col("region_code") == F.col("_lk_region"))
            & (F.col("accounting_period") == F.col("_lk_period")),
            "left",
        )
        .withColumn("period_is_closed", isPeriodClosed(F.col("period_status_cd"), F.col("_lk_region")))
        .withColumn(
            "_reject_reason",
            F.when(F.col("period_is_closed"), "PERIOD_NOT_OPEN")
            .when(~F.col("journal_complete"), "JOURNAL_INCOMPLETE")
            .when(~F.col("journal_balanced") & F.lit(not allowUnbalanced), "JOURNAL_UNBALANCED"),
        )
        .withColumn(
            "change_hash",
            rowHash(
                F.col("debit_amount"),
                F.col("credit_amount"),
                F.col("account_code"),
                F.col("cost_center_code"),
                F.col("posting_status_cd"),
                F.col("accounting_period"),
            ),
        )
        .drop("_lk_region", "_lk_period")
    )
    cols = [
        "journal_line_id",
        "journal_id",
        "journal_num",
        "ledger_code",
        "region_code",
        "org_cd",
        "accounting_period",
        "fiscal_year_nbr",
        "fiscal_period_nbr",
        F.col("gl_date").alias("date_key"),
        "gl_date",
        "posted_dt",
        "journal_source_cd",
        "journal_category_cd",
        "line_num",
        "gl_account_id",
        "account_code",
        "account_name",
        "account_type_cd",
        "account_class_cd",
        "normal_balance_cd",
        "cost_center_code",
        "cost_center_key",
        "project_cd",
        "intercompany_cd",
        "currency_cd",
        "debit_amount",
        "credit_amount",
        "net_amount",
        "posting_side",
        "base_debit_amt_usd",
        "base_credit_amt_usd",
        "tax_regime_code",
        "src_doc_type_cd",
        "src_doc_id",
        "subledger_source_key",
        "reversal_flag",
        "accrual_flag",
        "journal_debit_total",
        "journal_credit_total",
        "journal_imbalance",
        "journal_balanced",
        "period_status_cd",
        "period_is_closed",
        "change_hash",
    ]
    postable = enriched.where(F.col("_reject_reason").isNull()).select(*cols)
    rejected = enriched.where(F.col("_reject_reason").isNotNull()).select(*cols, "_reject_reason")
    return postable, rejected


def runFactGlPosting(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FACT_Load_GLPosting"
    lines = readTable(spark, cfg.table(SILVER_GL_JOURNAL_LINE))
    fq = cfg.table(GOLD_FACT_GL_POSTING)
    if tableExists(spark, fq):
        # incremental: only lines that are new or whose business content changed
        existing = readTable(spark, fq).select("journal_line_id", F.col("change_hash").alias("_old_hash"))
    else:
        existing = spark.createDataFrame([], "journal_line_id decimal(12,0), _old_hash string")
    postable, rejected = buildGlPostingFact(
        lines, currentCostCenterKeys(spark, cfg), readPeriodLocks(spark, cfg), cfg.allowUnbalancedJournals
    )
    delta = (
        postable.join(existing, "journal_line_id", "left")
        .where(F.col("_old_hash").isNull() | (F.col("_old_hash") != F.col("change_hash")))
        .drop("_old_hash")
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("loaded_at", F.current_timestamp())
    )
    written = mergeTable(spark, delta, fq, ["journal_line_id"])
    newRejects = rejected.join(existing, "journal_line_id", "left_anti")
    rej = writeRejects(
        spark,
        cfg,
        newRejects,
        pkg,
        "FACT",
        "stg.GlJournalLine",
        "journal_line_id",
        "_reject_reason",
        batchId=batchId,
    )
    return PackageResult(pkg, lines.count(), written, rej)


# --------------------------------------------------------------------------- FACT_Load_Payment
def buildPaymentFact(payments: DataFrame, matches: DataFrame) -> DataFrame:
    m = matches.groupBy("payment_id").agg(
        F.sum("matched_amount").alias("applied_amount"),
        F.sum("discount_amount").alias("discount_amount"),
        F.countDistinct("invoice_id").alias("matched_invoice_count"),
        F.min("confidence_score").alias("min_confidence_score"),
        F.max("match_pass").alias("max_match_pass"),
    )
    return (
        payments.join(m, "payment_id", "left")
        .withColumn("applied_amount", F.coalesce(F.col("applied_amount"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn("discount_amount", F.coalesce(F.col("discount_amount"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn(
            "unapplied_amount",
            (F.col("payment_amount") - F.col("applied_amount") - F.col("discount_amount")).cast(
                "decimal(18,5)"
            ),
        )
        .withColumn("matched_invoice_count", F.coalesce(F.col("matched_invoice_count"), F.lit(0)))
        .withColumn("is_orphan", F.col("matched_invoice_count") == 0)
        .withColumn(
            "change_hash",
            rowHash(
                F.col("payment_amount"),
                F.col("payment_status_code"),
                F.col("applied_amount"),
                F.col("payment_method_code"),
            ),
        )
        .select(
            "payment_id",
            "payment_number",
            "supplier_key",
            "supp_id",
            "supplier_code",
            "supp_name",
            "region_code",
            "org_cd",
            F.col("payment_date").alias("date_key"),
            "payment_date",
            "value_date",
            "cleared_dt",
            "period_cd",
            "payment_method_code",
            "payment_currency_code",
            "payment_amount",
            "payment_amt_usd",
            "withholding_amt",
            "discount_taken_amt",
            "applied_amount",
            "discount_amount",
            "unapplied_amount",
            "matched_invoice_count",
            "min_confidence_score",
            "max_match_pass",
            "is_orphan",
            "payment_status_code",
            "settlement_status_cd",
            "days_to_clear",
            "bank_account_code",
            "payment_run_id",
            "change_hash",
        )
    )


def runFactPayment(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FACT_Load_Payment"
    payments = readTable(spark, cfg.table(SILVER_PAYMENT))
    fact = buildPaymentFact(payments, readTable(spark, cfg.table(WORK_PAYMENT_MATCHED)))
    fq = cfg.table(GOLD_FACT_PAYMENT)
    existing = (
        readTable(spark, fq).select("payment_id", F.col("change_hash").alias("_old_hash"))
        if tableExists(spark, fq)
        else spark.createDataFrame([], "payment_id decimal(12,0), _old_hash string")
    )
    delta = (
        fact.join(existing, "payment_id", "left")
        .where(F.col("_old_hash").isNull() | (F.col("_old_hash") != F.col("change_hash")))
        .drop("_old_hash")
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("loaded_at", F.current_timestamp())
    )
    written = mergeTable(spark, delta, fq, ["payment_id"])
    return PackageResult(pkg, payments.count(), written, 0)


# --------------------------------------------------------------------------- FACT_Load_SupplierPayment
def buildSupplierPaymentFact(matches: DataFrame, payments: DataFrame, invoices: DataFrame) -> DataFrame:
    p = payments.select(
        "payment_id",
        "supplier_key",
        "supplier_code",
        "supp_name",
        "payment_date",
        "value_date",
        "payment_currency_code",
        "payment_method_code",
        "payment_status_code",
    )
    i = invoices.select(
        "invoice_id",
        "invoice_dt",
        "due_dt",
        "discount_due_date",
        "payment_terms_cd",
        "invoice_amount",
        "currency_code",
        "tax_regime_code",
    )
    return (
        matches.join(p, "payment_id", "inner")
        .join(i, "invoice_id", "inner")
        .withColumn("days_to_pay", F.datediff(F.col("payment_date"), F.col("invoice_dt")))
        .withColumn(
            "days_vs_due", F.datediff(F.col("payment_date"), F.coalesce(F.col("due_dt"), F.col("invoice_dt")))
        )
        .withColumn("paid_on_time", F.col("days_vs_due") <= 0)
        .withColumn("discount_captured", F.col("discount_amount") > 0)
        .withColumn(
            "timeliness_code",
            F.when(F.col("days_vs_due") < -5, "EARLY")
            .when(F.col("days_vs_due") <= 0, "ON_TIME")
            .when(F.col("days_vs_due") <= 30, "LATE")
            .otherwise("VERY_LATE"),
        )
        .withColumn(
            "change_hash",
            rowHash(
                F.col("matched_amount"),
                F.col("discount_amount"),
                F.col("match_type_code"),
                F.col("payment_date"),
            ),
        )
        .select(
            "payment_id",
            "invoice_id",
            "payment_number",
            "invoice_number",
            "supplier_key",
            "supplier_id",
            "supplier_code",
            "supp_name",
            "region_code",
            F.col("payment_date").alias("date_key"),
            "payment_date",
            "value_date",
            "invoice_dt",
            "due_dt",
            "discount_due_date",
            "payment_terms_cd",
            "payment_method_code",
            "payment_currency_code",
            F.col("currency_code").alias("invoice_currency_code"),
            "tax_regime_code",
            "invoice_amount",
            "matched_amount",
            "discount_amount",
            "match_type_code",
            "match_pass",
            "confidence_score",
            "days_to_pay",
            "days_vs_due",
            "paid_on_time",
            "discount_captured",
            "timeliness_code",
            "payment_status_code",
            "change_hash",
        )
    )


def runFactSupplierPayment(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FACT_Load_SupplierPayment"
    matches = readTable(spark, cfg.table(WORK_PAYMENT_MATCHED))
    fact = buildSupplierPaymentFact(
        matches, readTable(spark, cfg.table(SILVER_PAYMENT)), readTable(spark, cfg.table(SILVER_INVOICE))
    )
    fq = cfg.table(GOLD_FACT_SUPPLIER_PAYMENT)
    if tableExists(spark, fq):
        existing = readTable(spark, fq).select(
            "payment_id", "invoice_id", F.col("change_hash").alias("_old_hash")
        )
    else:
        existing = spark.createDataFrame(
            [], "payment_id decimal(12,0), invoice_id decimal(12,0), _old_hash string"
        )
    delta = (
        fact.join(existing, ["payment_id", "invoice_id"], "left")
        .where(F.col("_old_hash").isNull() | (F.col("_old_hash") != F.col("change_hash")))
        .drop("_old_hash")
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("loaded_at", F.current_timestamp())
    )
    written = mergeTable(spark, delta, fq, ["payment_id", "invoice_id"])
    return PackageResult(pkg, matches.count(), written, 0)
