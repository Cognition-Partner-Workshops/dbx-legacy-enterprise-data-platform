"""FIN_* finance-close business rules (Master_Finance_Close)."""

import calendar
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from finance import sources
from finance.config import FinanceConfig
from finance.facts import (
    FIN_PERIOD_LOCK,
    GOLD_FACT_GL_POSTING,
    GOLD_FACT_PAYMENT,
    LOCK_SCHEMA,
    buildGlPostingFact,
    currentCostCenterKeys,
    readPeriodLocks,
)
from finance.io import (
    PackageResult,
    appendTable,
    deleteInsert,
    mergeTable,
    overwriteTable,
    readTable,
    readTableOrEmpty,
    tableExists,
    writeRejects,
)
from finance.rules import (
    agingBucketSort,
    reportableAgingAmount,
    revaluationQuoteCurrency,
    revaluationRateType,
    ssisAgingBucket,
    withholdingAmount,
)
from finance.staging import (
    SILVER_GL_JOURNAL_LINE,
    SILVER_INVOICE,
    SILVER_INVOICE_LINE,
    SILVER_PAYMENT,
    WORK_PAYMENT_MATCHED,
)

FIN_FX_RATE_SNAPSHOT = "fin_fx_rate_snapshot"
FIN_CURRENCY_REVALUATION = "fin_currency_revaluation"
FIN_GL_HELD_LINE = "fin_gl_held_line"
FIN_GL_POSTING_CONTROL = "fin_gl_posting_control"
FIN_AP_AGING_DETAIL = "fin_ap_aging_detail"
FIN_AP_AGING_SUMMARY = "fin_ap_aging_summary"
FIN_COST_ALLOCATION = "fin_cost_allocation"
FIN_COST_ALLOCATION_SUMMARY = "fin_cost_allocation_summary"
REF_WITHHOLDING_TAX_RATE = "ref_withholding_tax_rate"
FIN_WITHHOLDING_TAX = "fin_withholding_tax"
FIN_WITHHOLDING_CERT_QUEUE = "fin_withholding_certificate_queue"
FIN_RECON_RESULT = "fin_recon_result"

LEDGERS = {"NA": "NA_USD", "EU": "EU_EUR", "APAC": "AP_AUD"}

# stg.WithholdingTaxRate does not exist on the legacy host; the SSIS rule set is carried here instead
# (jurisdiction = region, service category from the invoice line type).  Rates are placeholders for the
# missing reference data and are called out in the README as an open question.
WITHHOLDING_RATE_SEED = [
    ("NA", "CONS", 24.0, None, 0.0),
    ("NA", "LEGL", 24.0, None, 0.0),
    ("NA", "MEDI", 24.0, None, 0.0),
    ("NA", "RENT", 24.0, None, 0.0),
    ("EU", "*", 20.0, 10.0, 0.0),
    ("APAC", "*", 47.0, None, 75.0),
]


def periodMonthRange(period: str) -> tuple[date, date]:
    y, m = int(period[:4]), int(period[5:7])
    return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])


# --------------------------------------------------------------------------- FIN_Currency_Revaluation
def buildRateSnapshot(rates: DataFrame, revalDate: date) -> DataFrame:
    """Latest SPOT on/before the revaluation date (CLOSING) and the month average (AVERAGE) per pair,
    with inverse and USD-triangulated fallbacks flagged."""
    spot = rates.where((F.col("rate_type_cd") == "SPOT") & (F.col("rate_dt") <= F.lit(revalDate)))
    monthStart, _ = periodMonthRange(revalDate.isoformat()[:7])
    w = Window.partitionBy("from_curr_cd", "to_curr_cd").orderBy(F.col("rate_dt").desc())
    closing = (
        spot.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .select(
            "from_curr_cd",
            "to_curr_cd",
            F.col("rate").alias("closing_rate"),
            F.col("rate_dt").alias("closing_rate_dt"),
        )
    )
    average = (
        spot.where(F.col("rate_dt") >= F.lit(monthStart))
        .groupBy("from_curr_cd", "to_curr_cd")
        .agg(
            F.avg("rate").cast("decimal(18,8)").alias("average_rate"),
            F.count("*").alias("average_rate_points"),
        )
    )
    direct = closing.join(average, ["from_curr_cd", "to_curr_cd"], "left").withColumn(
        "rate_source", F.lit("DIRECT")
    )
    inverse = direct.select(
        F.col("to_curr_cd").alias("from_curr_cd"),
        F.col("from_curr_cd").alias("to_curr_cd"),
        (F.lit(1.0) / F.col("closing_rate")).cast("decimal(18,8)").alias("closing_rate"),
        "closing_rate_dt",
        (F.lit(1.0) / F.col("average_rate")).cast("decimal(18,8)").alias("average_rate"),
        "average_rate_points",
        F.lit("INVERSE").alias("rate_source"),
    )
    legA = direct.where(F.col("to_curr_cd") == "USD").select(
        F.col("from_curr_cd").alias("_a"),
        F.col("closing_rate").alias("_ca"),
        F.col("average_rate").alias("_aa"),
        F.col("closing_rate_dt").alias("_da"),
    )
    legB = direct.where(F.col("from_curr_cd") == "USD").select(
        F.col("to_curr_cd").alias("_b"),
        F.col("closing_rate").alias("_cb"),
        F.col("average_rate").alias("_ab"),
        F.col("closing_rate_dt").alias("_db"),
    )
    tri = (
        legA.crossJoin(legB)
        .where(F.col("_a") != F.col("_b"))
        .select(
            F.col("_a").alias("from_curr_cd"),
            F.col("_b").alias("to_curr_cd"),
            (F.col("_ca") * F.col("_cb")).cast("decimal(18,8)").alias("closing_rate"),
            F.least("_da", "_db").alias("closing_rate_dt"),
            (F.col("_aa") * F.col("_ab")).cast("decimal(18,8)").alias("average_rate"),
            F.lit(None).cast("long").alias("average_rate_points"),
            F.lit("TRIANGULATED").alias("rate_source"),
        )
    )
    prio = F.when(F.col("rate_source") == "DIRECT", 1).when(F.col("rate_source") == "INVERSE", 2).otherwise(3)
    allRates = direct.unionByName(inverse).unionByName(tri).withColumn("_p", prio)
    best = allRates.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("from_curr_cd", "to_curr_cd").orderBy("_p"))
    ).where(F.col("_rn") == 1)
    return (
        best.drop("_p", "_rn")
        .withColumn("revaluation_date", F.lit(revalDate))
        .withColumn("triangulated_flag", F.col("rate_source") == "TRIANGULATED")
    )


def revalueOpenItems(
    invoices: DataFrame, snapshot: DataFrame, regions: DataFrame, revalDate: date
) -> DataFrame:
    """AP open items are balance-sheet -> CLOSING rate; quote currency EUR (EU) / entity reporting currency
    (APAC) / USD (NA).  Missing rate rows are flagged (FailOnMissingRate decides whether the run stops)."""
    reg = regions.select(
        F.col("region_cd").alias("_region"), F.col("reporting_curr_cd").alias("entity_currency")
    )
    items = (
        invoices.where(
            (F.col("open_amount") > 0) & ~F.col("invoice_status_code").isin("CANC", "VOID", "DRAFT")
        )
        .join(reg, F.col("region_code") == F.col("_region"), "left")
        .withColumn(
            "quote_currency", revaluationQuoteCurrency(F.col("region_code"), F.col("entity_currency"))
        )
        .withColumn("rate_type_code", revaluationRateType(F.lit("LIAB")))
        .where(F.col("currency_code") != F.col("quote_currency"))
    )
    snap = snapshot.select(
        F.col("from_curr_cd").alias("currency_code"),
        F.col("to_curr_cd").alias("quote_currency"),
        "closing_rate",
        "average_rate",
        "rate_source",
        "closing_rate_dt",
    )
    out = (
        items.join(snap, ["currency_code", "quote_currency"], "left")
        .withColumn(
            "rate_used",
            F.when(F.col("rate_type_code") == "AVERAGE", F.col("average_rate")).otherwise(
                F.col("closing_rate")
            ),
        )
        .withColumn("rate_missing", F.col("rate_used").isNull())
        .withColumn("booked_rate", F.coalesce(F.col("exchange_rate_num"), F.lit(1.0)).cast("decimal(18,8)"))
        .withColumn(
            "booked_base_amount",
            F.round(F.col("open_amount") * F.col("booked_rate"), 2).cast("decimal(18,5)"),
        )
        .withColumn(
            "revalued_base_amount",
            F.round(F.col("open_amount") * F.col("rate_used"), 2).cast("decimal(18,5)"),
        )
        .withColumn(
            "unrealized_gain_loss",
            (F.col("revalued_base_amount") - F.col("booked_base_amount")).cast("decimal(18,5)"),
        )
        .withColumn("revaluation_date", F.lit(revalDate))
        .withColumn("item_type", F.lit("AP_OPEN_ITEM"))
    )
    return out.select(
        "item_type",
        "invoice_id",
        "invoice_number",
        "supplier_key",
        "supplier_code",
        "region_code",
        "period_cd",
        "currency_code",
        "quote_currency",
        "rate_type_code",
        "open_amount",
        "booked_rate",
        "booked_base_amount",
        "rate_used",
        "rate_source",
        "closing_rate_dt",
        "rate_missing",
        "revalued_base_amount",
        "unrealized_gain_loss",
        "revaluation_date",
    )


def runCurrencyRevaluation(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FIN_Currency_Revaluation"
    revalDate = cfg.revaluationDate
    snapshot = buildRateSnapshot(sources.fxRates(spark, cfg), revalDate).withColumn(
        "batch_id", F.lit(batchId).cast("bigint")
    )
    deleteInsert(spark, snapshot, cfg.table(FIN_FX_RATE_SNAPSHOT), f"revaluation_date = '{revalDate}'")
    invoices = readTable(spark, cfg.table(SILVER_INVOICE))
    reval = (
        revalueOpenItems(invoices, snapshot, sources.regionRef(spark, cfg), revalDate)
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .cache()
    )
    missing = reval.where(F.col("rate_missing"))
    missingCount = missing.count()
    if missingCount and cfg.failOnMissingRate:
        raise RuntimeError(
            f"{pkg}: {missingCount} open items have no FX rate for {revalDate} and FailOnMissingRate is set"
        )
    written = deleteInsert(
        spark,
        reval.where(~F.col("rate_missing")),
        cfg.table(FIN_CURRENCY_REVALUATION),
        f"revaluation_date = '{revalDate}'",
    )
    rej = writeRejects(
        spark,
        cfg,
        missing.withColumn("_reject_reason", F.lit("FX_RATE_MISSING")),
        pkg,
        "REVALUE",
        "stg.FxRate",
        "invoice_id",
        "_reject_reason",
        batchId=batchId,
    )
    return PackageResult(
        pkg,
        invoices.count(),
        written,
        rej,
        notes={"revaluation_date": str(revalDate), "missing_rates": missingCount},
    )


# --------------------------------------------------------------------------- FIN_Load_GlPostings
def runFinGlPostings(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FIN_Load_GlPostings"
    period = cfg.accountingPeriod
    lines = readTable(spark, cfg.table(SILVER_GL_JOURNAL_LINE)).where(F.col("accounting_period") == period)
    if cfg.ledgerScope != "ALL":
        lines = lines.where(F.col("ledger_code") == cfg.ledgerScope)
    postable, rejected = buildGlPostingFact(
        lines, currentCostCenterKeys(spark, cfg), readPeriodLocks(spark, cfg), cfg.allowUnbalancedJournals
    )
    fq = cfg.table(GOLD_FACT_GL_POSTING)
    existing = (
        readTable(spark, fq).select("journal_line_id", F.col("change_hash").alias("_old_hash"))
        if tableExists(spark, fq)
        else spark.createDataFrame([], "journal_line_id decimal(12,0), _old_hash string")
    )
    delta = (
        postable.join(existing, "journal_line_id", "left")
        .where(F.col("_old_hash").isNull() | (F.col("_old_hash") != F.col("change_hash")))
        .drop("_old_hash")
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("loaded_at", F.current_timestamp())
    )
    written = mergeTable(spark, delta, fq, ["journal_line_id"])
    held = (
        rejected.join(existing, "journal_line_id", "left_anti")
        .withColumnRenamed("_reject_reason", "hold_reason_code")
        .withColumn("accounting_period", F.lit(period))
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("held_at", F.current_timestamp())
    )
    heldCount = deleteInsert(
        spark, held, cfg.table(FIN_GL_HELD_LINE), f"accounting_period = '{period}' AND batch_id = {batchId}"
    )
    control = (
        postable.groupBy("ledger_code", "region_code", "accounting_period")
        .agg(
            F.countDistinct("journal_id").alias("journal_count"),
            F.count("*").alias("line_count"),
            F.sum("debit_amount").alias("debit_total"),
            F.sum("credit_amount").alias("credit_total"),
            F.sum(F.when(~F.col("journal_balanced"), 1).otherwise(0)).alias("unbalanced_lines"),
        )
        .join(
            held.groupBy("ledger_code", "region_code", "accounting_period").agg(
                F.count("*").alias("held_line_count")
            ),
            ["ledger_code", "region_code", "accounting_period"],
            "left",
        )
        .withColumn("held_line_count", F.coalesce(F.col("held_line_count"), F.lit(0)))
        .withColumn("control_balanced", F.abs(F.col("debit_total") - F.col("credit_total")) <= 0.005)
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("posted_at", F.current_timestamp())
    )
    deleteInsert(spark, control, cfg.table(FIN_GL_POSTING_CONTROL), f"accounting_period = '{period}'")
    return PackageResult(
        pkg,
        lines.count(),
        written,
        heldCount,
        notes={"accounting_period": period, "ledger_scope": cfg.ledgerScope},
    )


# --------------------------------------------------------------------------- FIN_Load_ApAging
def buildApAging(
    invoices: DataFrame, lines: DataFrame, asOf: date, includeDisputed: bool = False
) -> tuple[DataFrame, DataFrame]:
    """Open, non-cancelled/void/draft invoices bucketed by days past due with regional reportable amounts."""
    tax = lines.groupBy("invoice_id").agg(
        F.sum("recoverable_tax_amt").alias("recoverable_vat_amount"),
        F.sum(F.when(F.col("tax_regime_code") == "GST", F.col("recoverable_tax_amt"))).alias(
            "gst_input_credit_amount"
        ),
    )
    disputed = F.coalesce(F.col("hold_codes_txt"), F.lit("")).contains("DISP")
    base = (
        invoices.where(
            (F.col("open_amount") > 0) & ~F.col("invoice_status_code").isin("CANC", "VOID", "DRAFT")
        )
        .join(tax, "invoice_id", "left")
        .withColumn("is_disputed", disputed)
        .where(F.lit(includeDisputed) | ~disputed)
        .withColumn("as_of_date", F.lit(asOf))
        .withColumn(
            "days_past_due", F.datediff(F.lit(asOf), F.coalesce(F.col("due_dt"), F.col("invoice_dt")))
        )
        .withColumn("aging_bucket_code", ssisAgingBucket(F.col("days_past_due")))
        .withColumn("aging_bucket_sort", agingBucketSort(F.col("aging_bucket_code")))
        .withColumn(
            "reportable_amount",
            reportableAgingAmount(
                F.col("region_code"),
                F.col("open_amount"),
                F.col("recoverable_vat_amount"),
                F.col("gst_input_credit_amount"),
            ).cast("decimal(18,5)"),
        )
        .withColumn(
            "discount_available_amount",
            F.when(
                F.col("discount_due_date").isNotNull()
                & (F.col("discount_due_date") >= F.lit(asOf))
                & (F.coalesce(F.col("discount_percent"), F.lit(0.0)) > 0),
                F.round(F.col("open_amount") * F.col("discount_percent") / 100, 2),
            )
            .otherwise(F.lit(0.0))
            .cast("decimal(18,5)"),
        )
        .withColumn("_reject_reason", F.when(F.col("supplier_code").isNull(), "UNKNOWN_SUPPLIER"))
    )
    cols = [
        "as_of_date",
        "invoice_id",
        "invoice_number",
        "supplier_key",
        "supplier_code",
        "supp_name",
        "region_code",
        "currency_code",
        "period_cd",
        "invoice_dt",
        "due_dt",
        "days_past_due",
        "aging_bucket_code",
        "aging_bucket_sort",
        "invoice_amount",
        "paid_amount",
        "open_amount",
        "recoverable_vat_amount",
        "gst_input_credit_amount",
        "reportable_amount",
        "discount_percent",
        "discount_due_date",
        "discount_available_amount",
        "is_on_hold",
        "is_disputed",
        "invoice_status_code",
    ]
    return base.where(F.col("_reject_reason").isNull()).select(*cols), base.where(
        F.col("_reject_reason").isNotNull()
    ).select(*cols, "_reject_reason")


def summarizeApAging(detail: DataFrame) -> DataFrame:
    return detail.groupBy(
        "as_of_date", "region_code", "currency_code", "aging_bucket_code", "aging_bucket_sort"
    ).agg(
        F.count("*").alias("invoice_count"),
        F.sum("open_amount").alias("open_amount"),
        F.sum("reportable_amount").alias("reportable_amount"),
        F.sum("discount_available_amount").alias("discount_available_amount"),
        F.sum(F.when(F.col("is_on_hold"), F.col("open_amount")).otherwise(0)).alias("on_hold_amount"),
    )


def runFinApAging(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FIN_Load_ApAging"
    invoices = readTable(spark, cfg.table(SILVER_INVOICE))
    detail, rejected = buildApAging(
        invoices, readTable(spark, cfg.table(SILVER_INVOICE_LINE)), cfg.businessDate, cfg.includeDisputed
    )
    detail = detail.withColumn("batch_id", F.lit(batchId).cast("bigint")).cache()
    written = deleteInsert(
        spark, detail, cfg.table(FIN_AP_AGING_DETAIL), f"as_of_date = '{cfg.businessDate}'"
    )
    deleteInsert(
        spark,
        summarizeApAging(detail).withColumn("batch_id", F.lit(batchId).cast("bigint")),
        cfg.table(FIN_AP_AGING_SUMMARY),
        f"as_of_date = '{cfg.businessDate}'",
    )
    rej = writeRejects(
        spark, cfg, rejected, pkg, "AGING", "stg.ApInvoice", "invoice_id", "_reject_reason", batchId=batchId
    )
    return PackageResult(
        pkg,
        invoices.count(),
        written,
        rej,
        notes={"as_of_date": str(cfg.businessDate), "include_disputed": cfg.includeDisputed},
    )


# --------------------------------------------------------------------------- FIN_Load_CostAllocation
def maskToRegex(mask: str) -> str:
    import re

    return "^" + re.escape(mask or "%").replace("%", ".*").replace("_", ".") + "$"


def runAllocationRules(
    rules: list[dict], pools: dict[str, float], drivers: dict[str, float] | None = None
) -> list[dict]:
    """Cursor-style, RuleSequence-ordered allocation (later rules see what earlier rules moved).

    rules: dicts with rule_set_cd, rule_seq_nbr, alloc_rule_id, region_cd, source_cost_center_cd, target_cost_center_cd,
           allocation_method_cd (PCT/FIXED/EVEN/STEP/DRIVER), allocation_pct, fixed_amt, driver_cd
    pools: opening expense balance per source cost center
    drivers: optional driver values keyed by driver_cd (DRIVER rules without a value are skipped and reported)
    Returns allocation rows plus one UNALLOCATED row per source pool with a residual.
    """
    drivers = drivers or {}
    balances = dict(pools)
    ordered = sorted(rules, key=lambda r: (r["rule_set_cd"], int(r["rule_seq_nbr"]), int(r["alloc_rule_id"])))
    rows: list[dict] = []
    touchedSources: set[str] = set()
    for idx, r in enumerate(ordered):
        src, tgt, method = r["source_cost_center_cd"], r["target_cost_center_cd"], r["allocation_method_cd"]
        available = round(balances.get(src, 0.0), 2)
        touchedSources.add(src)
        status, amount = "ALLOCATED", 0.0
        if method == "PCT":
            amount = available * float(r["allocation_pct"] or 0) / 100.0
        elif method == "FIXED":
            amount = float(r["fixed_amt"] or 0)
            if (r.get("fixed_curr_cd") or "USD") != "USD":
                status = "ALLOCATED_FOREIGN_FIXED"
        elif method == "STEP":
            amount = available
        elif method == "EVEN":
            remainingPeers = [
                x
                for x in ordered[idx:]
                if x["rule_set_cd"] == r["rule_set_cd"]
                and x["allocation_method_cd"] == "EVEN"
                and x["source_cost_center_cd"] == src
            ]
            amount = available / len(remainingPeers)
        elif method == "DRIVER":
            driverValue = drivers.get(r.get("driver_cd") or "")
            if driverValue is None:
                status, amount = "DRIVER_UNAVAILABLE", 0.0
            else:
                siblings = [
                    x
                    for x in ordered
                    if x["rule_set_cd"] == r["rule_set_cd"]
                    and x["allocation_method_cd"] == "DRIVER"
                    and x["source_cost_center_cd"] == src
                ]
                total = sum(drivers.get(x.get("driver_cd") or "", 0.0) for x in siblings) or driverValue
                amount = available * driverValue / total
        else:
            status = "UNKNOWN_METHOD"
        amount = round(amount, 2)
        balances[src] = round(balances.get(src, 0.0) - amount, 2)
        balances[tgt] = round(balances.get(tgt, 0.0) + amount, 2)
        rows.append(
            {
                "rule_set_cd": r["rule_set_cd"],
                "rule_seq_nbr": int(r["rule_seq_nbr"]),
                "alloc_rule_id": int(r["alloc_rule_id"]),
                "execution_order": idx + 1,
                "region_cd": r["region_cd"],
                "source_cost_center_cd": src,
                "target_cost_center_cd": tgt,
                "allocation_method_cd": method,
                "pool_amount_before": available,
                "allocated_amount": amount,
                "pool_amount_after": balances[src],
                "allocation_status": status,
                "reverse_next_period_flg": r.get("reverse_next_period_flg") or "N",
            }
        )
    for src in sorted(touchedSources):
        residual = round(balances.get(src, 0.0), 2)
        if residual and src in pools:
            rows.append(
                {
                    "rule_set_cd": None,
                    "rule_seq_nbr": None,
                    "alloc_rule_id": None,
                    "execution_order": None,
                    "region_cd": None,
                    "source_cost_center_cd": src,
                    "target_cost_center_cd": None,
                    "allocation_method_cd": "RESIDUAL",
                    "pool_amount_before": residual,
                    "allocated_amount": 0.0,
                    "pool_amount_after": residual,
                    "allocation_status": "UNALLOCATED",
                    "reverse_next_period_flg": "N",
                }
            )
    return rows


ALLOC_SCHEMA = (
    "rule_set_cd string, rule_seq_nbr int, alloc_rule_id int, execution_order int, region_cd string, "
    "source_cost_center_cd string, target_cost_center_cd string, allocation_method_cd string, pool_amount_before double, "
    "allocated_amount double, pool_amount_after double, allocation_status string, reverse_next_period_flg string"
)


def runFinCostAllocation(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FIN_Load_CostAllocation"
    period = cfg.accountingPeriod
    asOf = cfg.businessDate
    rulesDf = sources.allocationRules(spark, cfg).where(
        (F.coalesce(F.col("active_flg"), F.lit("N")) == "Y")
        & (F.to_date("effective_from_dt") <= F.lit(asOf))
        & (F.coalesce(F.to_date("effective_to_dt"), F.lit("9999-12-31").cast("date")) >= F.lit(asOf))
    )
    rules = [r.asDict() for r in rulesDf.collect()]
    postings = readTable(spark, cfg.table(GOLD_FACT_GL_POSTING)).where(
        (F.col("accounting_period") == period) & (F.col("account_type_cd") == "EXP")
    )
    pools: dict[str, float] = {}
    for r in rules:
        src = r["source_cost_center_cd"]
        if src in pools:
            continue
        pool = (
            postings.where(
                (F.col("cost_center_code") == src)
                & F.col("account_code").rlike(maskToRegex(r["source_account_mask"]))
            )
            .agg(F.sum("net_amount"))
            .collect()[0][0]
        )
        pools[src] = float(pool or 0.0)
    rows = runAllocationRules(rules, pools)
    out = (
        spark.createDataFrame(rows, ALLOC_SCHEMA)
        .withColumn("accounting_period", F.lit(period))
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("allocated_at", F.current_timestamp())
    )
    written = deleteInsert(spark, out, cfg.table(FIN_COST_ALLOCATION), f"accounting_period = '{period}'")
    summary = (
        out.where(F.col("allocation_status") != "UNALLOCATED")
        .groupBy("accounting_period", "region_cd", "target_cost_center_cd")
        .agg(F.sum("allocated_amount").alias("allocated_in_amount"), F.count("*").alias("rule_count"))
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
    )
    deleteInsert(spark, summary, cfg.table(FIN_COST_ALLOCATION_SUMMARY), f"accounting_period = '{period}'")
    unavailable = sum(1 for r in rows if r["allocation_status"] == "DRIVER_UNAVAILABLE")
    return PackageResult(
        pkg, len(rules), written, unavailable, notes={"pools": pools, "driver_unavailable_rules": unavailable}
    )


# --------------------------------------------------------------------------- FIN_Load_WithholdingTax
def withholdingRateSeed(spark: SparkSession) -> DataFrame:
    return (
        spark.createDataFrame(
            WITHHOLDING_RATE_SEED,
            "jurisdiction_code string, service_category_code string, withholding_rate_percent double, treaty_rate_percent double, withholding_threshold_amount double",
        )
        .withColumn("effective_from", F.lit("1900-01-01").cast("date"))
        .withColumn("effective_to", F.lit(None).cast("date"))
    )


def buildWithholding(
    lines: DataFrame, invoices: DataFrame, rates: DataFrame, matches: DataFrame, period: str
) -> tuple[DataFrame, DataFrame]:
    inv = invoices.select(
        "invoice_id",
        "period_cd",
        "invoice_dt",
        "supp_id",
        F.col("tax_reg_num").alias("supplier_tax_registration_number"),
        "supplier_key",
    )
    pay = matches.groupBy("invoice_id").agg(F.max("payment_id").alias("payment_id"))
    exact = rates.select(
        F.col("jurisdiction_code").alias("_j"),
        F.col("service_category_code").alias("_s"),
        "withholding_rate_percent",
        "treaty_rate_percent",
        "withholding_threshold_amount",
        "effective_from",
        "effective_to",
    )
    wildcard = (
        exact.where(F.col("_s") == "*")
        .drop("_s")
        .select(
            F.col("_j").alias("_jw"),
            F.col("withholding_rate_percent").alias("_wr"),
            F.col("treaty_rate_percent").alias("_wt"),
            F.col("withholding_threshold_amount").alias("_wth"),
        )
    )
    base = (
        lines.drop("supplier_key", "supp_id")
        .join(inv, "invoice_id", "inner")
        .where((F.col("period_cd") == period) & (F.coalesce(F.col("line_type_cd"), F.lit("")) != "FREIGHT"))
        .withColumn("jurisdiction_code", F.col("region_code"))
        .join(
            exact.where(F.col("_s") != "*"),
            (F.col("jurisdiction_code") == F.col("_j")) & (F.col("service_category_cd") == F.col("_s")),
            "left",
        )
        .join(wildcard, F.col("jurisdiction_code") == F.col("_jw"), "left")
        .withColumn(
            "withholding_rate_percent",
            F.coalesce(F.col("withholding_rate_percent"), F.col("_wr"), F.lit(0.0)),
        )
        .withColumn("treaty_rate_percent", F.coalesce(F.col("treaty_rate_percent"), F.col("_wt")))
        .withColumn(
            "withholding_threshold_amount",
            F.coalesce(F.col("withholding_threshold_amount"), F.col("_wth"), F.lit(0.0)),
        )
        .join(pay, "invoice_id", "left")
    )
    out = (
        base.withColumn(
            "withholding_amount",
            F.round(
                withholdingAmount(
                    F.col("region_code"),
                    F.col("line_amount"),
                    F.col("service_category_cd"),
                    F.col("supplier_tax_registration_number"),
                    F.col("withholding_rate_percent"),
                    F.col("treaty_rate_percent"),
                    F.col("withholding_threshold_amount"),
                ),
                2,
            ).cast("decimal(18,5)"),
        )
        .withColumn(
            "net_payable_amount", (F.col("line_amount") - F.col("withholding_amount")).cast("decimal(18,5)")
        )
        .withColumn("is_withheld", F.col("withholding_amount") > 0)
        .withColumn(
            "withholding_certificate_required",
            (F.col("region_code") == "EU") & (F.col("withholding_amount") > 0),
        )
        .withColumn(
            "_reject_reason",
            F.when(
                (F.col("withholding_rate_percent") == 0) & (F.col("withholding_amount") > 0),
                "UNMAPPED_JURISDICTION",
            ),
        )
        .withColumn("accounting_period", F.lit(period))
    )
    cols = [
        "accounting_period",
        "invoice_line_id",
        "invoice_id",
        "invoice_number",
        "payment_id",
        "supp_id",
        "supplier_key",
        "supplier_code",
        "supplier_tax_registration_number",
        "jurisdiction_code",
        "region_code",
        "invoice_dt",
        "line_type_cd",
        "service_category_cd",
        "tax_code_cd",
        "line_amount",
        "withholding_rate_percent",
        "treaty_rate_percent",
        "withholding_threshold_amount",
        "withholding_amount",
        "net_payable_amount",
        "is_withheld",
        "withholding_certificate_required",
    ]
    return out.where(F.col("_reject_reason").isNull()).select(*cols), out.where(
        F.col("_reject_reason").isNotNull()
    ).select(*cols, "_reject_reason")


def runFinWithholdingTax(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FIN_Load_WithholdingTax"
    period = cfg.accountingPeriod
    rateFq = cfg.table(REF_WITHHOLDING_TAX_RATE)
    if not tableExists(spark, rateFq):
        overwriteTable(withholdingRateSeed(spark), rateFq)
    lines = readTable(spark, cfg.table(SILVER_INVOICE_LINE))
    out, rejected = buildWithholding(
        lines,
        readTable(spark, cfg.table(SILVER_INVOICE)),
        readTable(spark, rateFq),
        readTable(spark, cfg.table(WORK_PAYMENT_MATCHED)),
        period,
    )
    out = out.withColumn("batch_id", F.lit(batchId).cast("bigint")).cache()
    written = deleteInsert(spark, out, cfg.table(FIN_WITHHOLDING_TAX), f"accounting_period = '{period}'")
    queue = (
        out.where(F.col("withholding_certificate_required"))
        .groupBy("accounting_period", "supp_id", "supplier_key", "jurisdiction_code")
        .agg(F.sum("withholding_amount").alias("withholding_amount"))
        .withColumn("queued_at", F.current_timestamp())
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
    )
    deleteInsert(spark, queue, cfg.table(FIN_WITHHOLDING_CERT_QUEUE), f"accounting_period = '{period}'")
    # post withholding to the payment fact (Integration.usp_PostWithholdingTax)
    payFq = cfg.table(GOLD_FACT_PAYMENT)
    if tableExists(spark, payFq):
        posted = (
            out.where(F.col("payment_id").isNotNull())
            .groupBy("payment_id")
            .agg(F.sum("withholding_amount").alias("withholding_posted_amount"))
        )
        posted.createOrReplaceTempView("wht_posted")
        spark.sql(
            f"ALTER TABLE {payFq} ADD COLUMNS (withholding_posted_amount DECIMAL(18,5))"
        ) if "withholding_posted_amount" not in readTable(spark, payFq).columns else None
        spark.sql(
            f"MERGE INTO {payFq} t USING wht_posted s ON t.payment_id = s.payment_id WHEN MATCHED THEN UPDATE SET t.withholding_posted_amount = s.withholding_posted_amount"
        )
    rej = writeRejects(
        spark,
        cfg,
        rejected,
        pkg,
        "WITHHOLD",
        "stg.ApInvoiceLine",
        "invoice_line_id",
        "_reject_reason",
        batchId=batchId,
    )
    return PackageResult(
        pkg,
        lines.count(),
        written,
        rej,
        notes={"accounting_period": period, "withheld_lines": out.where(F.col("is_withheld")).count()},
    )


# --------------------------------------------------------------------------- FIN_Reconcile_SubledgerToGl
def reconcileSubledger(
    invoices: DataFrame, payments: DataFrame, glPostings: DataFrame, period: str, tolerance: float
) -> DataFrame:
    """AP subledger movement (invoices booked less payments issued, USD) vs GL AP-sourced liability postings
    per region for the period.  |variance| > tolerance -> VARIANCE (unexplained until a controller explains it)."""
    inv = (
        invoices.where(
            (F.col("period_cd") == period) & ~F.col("invoice_status_code").isin("CANC", "VOID", "DRAFT")
        )
        .groupBy("region_code")
        .agg(
            F.sum("base_amt_usd").alias("subledger_invoice_amount"),
            F.count("*").alias("subledger_invoice_count"),
        )
    )
    pay = (
        payments.where((F.col("period_cd") == period) & (F.col("payment_status_code") != "VOID"))
        .groupBy("region_code")
        .agg(
            F.sum("payment_amt_usd").alias("subledger_payment_amount"),
            F.count("*").alias("subledger_payment_count"),
        )
    )
    gl = (
        glPostings.where(
            (F.col("accounting_period") == period)
            & (F.col("journal_source_cd") == "AP")
            & (F.col("account_type_cd") == "LIAB")
        )
        .groupBy("region_code")
        .agg(
            (F.sum("base_credit_amt_usd") - F.sum("base_debit_amt_usd")).alias("gl_amount"),
            F.count("*").alias("gl_line_count"),
            F.countDistinct("journal_id").alias("gl_journal_count"),
        )
    )
    regions = (
        inv.select("region_code")
        .unionByName(pay.select("region_code"))
        .unionByName(gl.select("region_code"))
        .distinct()
    )
    out = (
        regions.join(inv, "region_code", "left")
        .join(pay, "region_code", "left")
        .join(gl, "region_code", "left")
        .withColumn("accounting_period", F.lit(period))
        .withColumn(
            "ledger_code",
            F.coalesce(*[F.when(F.col("region_code") == k, F.lit(v)) for k, v in LEDGERS.items()]),
        )
        .withColumn("account_class", F.lit("AP_CONTROL"))
        .withColumn(
            "subledger_amount",
            (
                F.coalesce(F.col("subledger_invoice_amount"), F.lit(0.0))
                - F.coalesce(F.col("subledger_payment_amount"), F.lit(0.0))
            ).cast("decimal(18,5)"),
        )
        .withColumn("gl_amount", F.coalesce(F.col("gl_amount"), F.lit(0.0)).cast("decimal(18,5)"))
        .withColumn("variance_amount", (F.col("gl_amount") - F.col("subledger_amount")).cast("decimal(18,5)"))
        .withColumn("tolerance_amount", F.lit(tolerance))
        .withColumn(
            "recon_status",
            F.when(F.abs(F.col("variance_amount")) <= F.col("tolerance_amount"), "MATCHED").otherwise(
                "VARIANCE"
            ),
        )
        .withColumn("is_explained", F.lit(False))
        .withColumn("explanation", F.lit(None).cast("string"))
    )
    return out.select(
        "accounting_period",
        "ledger_code",
        "region_code",
        "account_class",
        "subledger_invoice_count",
        "subledger_invoice_amount",
        "subledger_payment_count",
        "subledger_payment_amount",
        "subledger_amount",
        "gl_journal_count",
        "gl_line_count",
        "gl_amount",
        "variance_amount",
        "tolerance_amount",
        "recon_status",
        "is_explained",
        "explanation",
    )


def configuredTolerance(spark: SparkSession, cfg: FinanceConfig) -> float:
    try:
        rows = (
            sources.legacyConfiguration(spark, cfg)
            .where(F.col("configuration_key") == "ReconAbsoluteTolerance")
            .collect()
        )
        return float(rows[0]["configuration_value"]) if rows else 0.0
    except Exception:
        return 0.0


def runFinReconcile(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FIN_Reconcile_SubledgerToGl"
    period = cfg.accountingPeriod
    tol = configuredTolerance(spark, cfg)
    gl = readTable(spark, cfg.table(GOLD_FACT_GL_POSTING))
    out = reconcileSubledger(
        readTable(spark, cfg.table(SILVER_INVOICE)),
        readTable(spark, cfg.table(SILVER_PAYMENT)),
        gl,
        period,
        tol,
    )
    out = (
        out.withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("reconciled_at", F.current_timestamp())
        .cache()
    )
    written = deleteInsert(spark, out, cfg.table(FIN_RECON_RESULT), f"accounting_period = '{period}'")
    variances = out.where(F.col("recon_status") == "VARIANCE").withColumn(
        "_reject_reason", F.lit("SUBLEDGER_GL_VARIANCE")
    )
    rej = writeRejects(
        spark,
        cfg,
        variances,
        pkg,
        "RECON",
        "Fact.GL Posting",
        "region_code",
        "_reject_reason",
        detailCol="variance_amount",
        batchId=batchId,
    )
    return PackageResult(
        pkg, gl.count(), written, rej, notes={"accounting_period": period, "tolerance": tol, "variances": rej}
    )


# --------------------------------------------------------------------------- FIN_Close_PeriodLock
def apacPeriodEnd(period: str) -> date:
    """APAC fiscal period -> calendar month end (FY starts in April: P1 = April of FY-1 ... P12 = March of FY)."""
    fy, p = int(period[:4]), int(period[5:7])
    month = (p + 2) % 12 + 1
    year = fy if p >= 10 else fy - 1
    return date(year, month, calendar.monthrange(year, month)[1])


def decidePeriodLock(
    period: str,
    ledgerScope: str,
    unexplainedByRegion: dict[str, int],
    asOf: date,
    apacPeriodEndDate: date,
    batchId: int,
    now: datetime,
) -> list[dict]:
    """One decision row per ledger in scope.  Any unexplained variance refuses the close for that ledger;
    APAC (4-4-5 / April FY) may only lock after its fiscal period end has passed."""
    rows = []
    for region, ledger in LEDGERS.items():
        if ledgerScope != "ALL" and ledgerScope not in (ledger, region):
            continue
        variances = int(unexplainedByRegion.get(region, 0))
        if variances > 0:
            status, note = "REFUSED", f"{variances} unexplained subledger-to-GL variance(s) remain"
        elif region == "APAC" and asOf <= apacPeriodEndDate:
            status, note = "DEFERRED", f"APAC fiscal period ends {apacPeriodEndDate}; lock after period end"
        else:
            status, note = "LOCKED", "period closed; postings into this period are blocked"
        rows.append(
            {
                "ledger_code": ledger,
                "region_code": region,
                "accounting_period": period,
                "lock_status": status,
                "locked_at": now,
                "locked_by_batch_id": batchId,
                "variance_count": variances,
                "note": note,
            }
        )
    return rows


def runFinPeriodLock(spark: SparkSession, cfg: FinanceConfig, batchId: int) -> PackageResult:
    pkg = "FIN_Close_PeriodLock"
    period = cfg.accountingPeriod
    recon = readTableOrEmpty(
        spark,
        cfg.table(FIN_RECON_RESULT),
        "accounting_period string, region_code string, recon_status string, is_explained boolean",
    )
    unexplained = {
        r["region_code"]: r["n"]
        for r in recon.where(
            (F.col("accounting_period") == period)
            & (F.col("recon_status") == "VARIANCE")
            & ~F.col("is_explained")
        )
        .groupBy("region_code")
        .agg(F.count("*").alias("n"))
        .collect()
    }
    apacEnd = apacPeriodEnd(period)
    ps = (
        sources.periodStatus(spark, cfg)
        .where((F.col("region_cd") == "APAC") & (F.col("period_cd") == period))
        .select("period_end_dt")
        .collect()
    )
    if ps and ps[0]["period_end_dt"]:
        apacEnd = ps[0]["period_end_dt"]
    rows = decidePeriodLock(
        period, cfg.ledgerScope, unexplained, cfg.businessDate, apacEnd, batchId, datetime.now(timezone.utc)
    )
    out = spark.createDataFrame(rows, LOCK_SCHEMA)
    fq = cfg.table(FIN_PERIOD_LOCK)
    if tableExists(spark, fq):
        # a ledger/period already LOCKED stays locked; new decisions replace older non-locked decisions
        existing = readTable(spark, fq)
        keep = existing.join(
            out.select("ledger_code", "accounting_period"), ["ledger_code", "accounting_period"], "left_anti"
        ).unionByName(
            existing.join(
                out.select("ledger_code", "accounting_period"),
                ["ledger_code", "accounting_period"],
                "left_semi",
            ).where(F.col("lock_status") == "LOCKED")
        )
        alreadyLocked = keep.where(F.col("lock_status") == "LOCKED").select(
            "ledger_code", "accounting_period"
        )
        out = out.join(alreadyLocked, ["ledger_code", "accounting_period"], "left_anti")
        overwriteTable(keep.unionByName(out), fq)
        written = out.count()
    else:
        written = appendTable(out, fq)
    locked = [r["ledger_code"] for r in rows if r["lock_status"] == "LOCKED"]
    refused = [r["ledger_code"] for r in rows if r["lock_status"] != "LOCKED"]
    status = "SUCCEEDED" if not refused else "WARNING"
    return PackageResult(
        pkg,
        len(rows),
        written,
        len(refused),
        status=status,
        notes={"locked": locked, "not_locked": refused, "unexplained_variances": unexplained},
    )
