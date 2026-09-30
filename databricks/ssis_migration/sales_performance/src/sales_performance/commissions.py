"""SLS_{NA,EU,APAC}_Load_Commission: regional commission accruals posted onto the sale fact."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_performance import config, fiscal
from sales_performance.common import mergeInto, readTable, saveTable, withAudit
from sales_performance.sale_line import DATE_TABLE, FACT_SALE_TABLE, FX_TABLE, PLAN_TABLE, QUOTA_TABLE, SALE_LINE_TABLE

ACCRUAL_TABLE = "gold_commission_accrual"
REJECT_TABLE = "gold_commission_reject"
METRIC_TABLE = "gold_package_run_metric"
EXCLUDED_LINE_TYPES = ("SAMPLE", "INTERNAL")
PACKAGES = {"NA": "SLS_NA_Load_Commission", "EU": "SLS_EU_Load_Commission", "APAC": "SLS_APAC_Load_Commission"}

MONEY = "decimal(19,4)"


def assignPlans(lines: DataFrame, plans: DataFrame) -> DataFrame:
    """Join the region's default (field) plan effective on the invoice date.

    The legacy estate assigns plans per salesperson through Application.SalesTeamMembers, which
    is empty, so the region's field plan is the deterministic fallback (see README)."""
    p = plans.filter(F.col("is_default_plan")).select(
        F.col("region_code").alias("plan_region_code"),
        "plan_code",
        "commission_basis",
        "band1_upper_percent",
        "band1_rate_percent",
        "band2_rate_percent",
        "accelerator_percent",
        "effective_from_date",
        "effective_to_date",
        "statutory_cap_amount",
    )
    cond = (
        (lines["region_code"] == p["plan_region_code"])
        & (lines["invoice_date"] >= p["effective_from_date"])
        & (lines["invoice_date"] <= F.coalesce(p["effective_to_date"], F.lit("9999-12-31").cast("date")))
    )
    return lines.join(p, cond, "left").drop("plan_region_code")


def commissionableLines(lines: DataFrame, regionCode: str) -> DataFrame:
    return lines.filter(
        (F.col("region_code") == regionCode)
        & ~F.upper(F.coalesce(F.col("line_type_code"), F.lit(""))).isin(*EXCLUDED_LINE_TYPES)
        & ~F.col("is_reversal")
    )


def _baseColumns(regionCode: str):
    return [
        F.lit(regionCode).alias("region_code"),
        F.lit(PACKAGES[regionCode]).alias("package_name"),
        "sale_key",
        "invoice_number",
        "invoice_line_number",
        "invoice_date",
        "salesperson_key",
        "salesperson_id",
        "customer_key",
        "country_code_iso3",
        "plan_code",
        "commission_basis",
    ]


def calculateNaCommission(lines: DataFrame, plans: DataFrame, quotas: DataFrame = None, houseAccountRatePercent: int = 50):
    """NA: (extended price + tax) in USD, base rate plus accelerator above the plan threshold,
    house accounts paid at half rate, calendar-month periods. Returns (accruals, unplanned)."""
    na = assignPlans(commissionableLines(lines, "NA").filter(F.col("currency_code") == "USD"), plans)
    unplanned = na.filter(F.col("plan_code").isNull())
    planned = na.filter(F.col("plan_code").isNotNull())
    period = fiscal.calendarMonth(F.col("invoice_date"))
    planned = planned.withColumn("commission_period", period).withColumn(
        "commissionable_amount", (F.col("extended_price") + F.coalesce(F.col("tax_amount"), F.lit(0))).cast(MONEY)
    )
    if quotas is not None:
        q = quotas.filter(F.col("region_code") == "NA").select(
            F.col("salesperson_id").alias("q_salesperson_id"), F.col("fiscal_period_label").alias("q_period"), F.col("quota_amount")
        )
        planned = planned.join(
            q, (planned["salesperson_id"] == q["q_salesperson_id"]) & (F.col("fiscal_period_label") == q["q_period"]), "left"
        ).drop("q_salesperson_id", "q_period")
    else:
        planned = planned.withColumn("quota_amount", F.lit(None).cast(MONEY))
    threshold = (F.col("quota_amount") * F.col("band1_upper_percent") / 100).cast(MONEY)
    runWindow = (
        Window.partitionBy("salesperson_key", "commission_period")
        .orderBy("invoice_date", "sale_key")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    cumulative = F.sum("commissionable_amount").over(runWindow)
    overThreshold = F.when(
        threshold.isNotNull(),
        F.greatest(F.lit(0), cumulative - F.greatest(threshold, cumulative - F.col("commissionable_amount"))),
    ).otherwise(F.lit(0))
    acceleratorRate = F.coalesce(F.col("accelerator_percent"), F.lit(0))
    houseFactor = F.when(F.coalesce(F.col("is_house_account"), F.lit(False)), F.lit(houseAccountRatePercent) / 100).otherwise(F.lit(1.0))
    out = (
        planned.withColumn("accelerator_threshold_amount", threshold)
        .withColumn("base_commission_amount", (F.col("commissionable_amount") * F.col("band1_rate_percent") / 100).cast(MONEY))
        .withColumn("accelerator_commission_amount", (overThreshold * acceleratorRate / 100).cast(MONEY))
        .withColumn("house_account_factor", houseFactor.cast("decimal(9,4)"))
        .withColumn(
            "commission_amount",
            ((F.col("base_commission_amount") + F.col("accelerator_commission_amount")) * F.col("house_account_factor")).cast(MONEY),
        )
    )
    accruals = out.select(
        *_baseColumns("NA"),
        "commission_period",
        F.col("commissionable_amount").alias("basis_amount"),
        F.lit("USD").alias("basis_currency_code"),
        F.lit("USD").alias("plan_currency_code"),
        F.lit(1.0).alias("conversion_rate"),
        F.col("commissionable_amount").alias("plan_currency_amount"),
        F.col("band1_rate_percent").alias("base_rate_percent"),
        "base_commission_amount",
        "accelerator_commission_amount",
        F.lit(False).alias("cap_applied"),
        "house_account_factor",
        F.lit(1.0).cast("decimal(9,4)").alias("split_factor"),
        "commission_amount",
        F.lit("ACCRUED").alias("accrual_status"),
        F.lit(False).alias("is_period_boundary_line"),
    )
    return accruals, unplanned


def euNetCommissionable(df: DataFrame):
    gross = F.col("total_including_tax")
    return (
        F.when(F.col("net_amount_reported").isNotNull(), F.col("net_amount_reported"))
        .when(F.coalesce(F.col("vat_rate_percent"), F.lit(0)) > 0, gross / (F.lit(1) + F.col("vat_rate_percent") / 100))
        .otherwise(gross - F.coalesce(F.col("vat_amount"), F.lit(0)))
    )


def calculateEuCommission(
    lines: DataFrame,
    plans: DataFrame,
    fxRates: DataFrame,
    cashBasisCountries=("DEU", "AUT"),
    statutoryCapAmount=None,
    clearedPaymentInvoices: DataFrame = None,
):
    """EU: VAT-exclusive net converted to EUR at the average rate, statutory cap, cash-basis hold."""
    eu = assignPlans(commissionableLines(lines, "EU"), plans)
    unplanned = eu.filter(F.col("plan_code").isNull())
    planned = eu.filter(F.col("plan_code").isNotNull()).withColumn("commission_period", fiscal.calendarMonth(F.col("invoice_date")))
    fx = fxRates.filter((F.col("rate_type_code") == "AVERAGE") & (F.col("quote_currency_code") == "EUR")).select(
        F.col("currency_code").alias("fx_currency_code"), "conversion_rate"
    )
    planned = planned.join(fx, planned["currency_code"] == fx["fx_currency_code"], "left").drop("fx_currency_code")
    planned = planned.withColumn("net_commissionable_amount", euNetCommissionable(planned).cast(MONEY)).withColumn(
        "net_amount_eur", (F.col("net_commissionable_amount") * F.col("conversion_rate")).cast(MONEY)
    )
    cap = F.coalesce(F.col("statutory_cap_amount"), F.lit(statutoryCapAmount).cast(MONEY))
    rawCommission = F.col("net_amount_eur") * F.col("band1_rate_percent") / 100
    planned = planned.withColumn("cap_applied", cap.isNotNull() & (cap > 0) & (rawCommission > cap)).withColumn(
        "commission_amount", F.when(F.col("cap_applied"), cap).otherwise(rawCommission).cast(MONEY)
    )
    isCashBasis = F.col("country_code_iso3").isin(*cashBasisCountries)
    if clearedPaymentInvoices is not None:
        cleared = clearedPaymentInvoices.select(F.col("invoice_number").alias("cleared_invoice_number")).distinct()
        planned = planned.join(cleared, planned["invoice_number"] == cleared["cleared_invoice_number"], "left")
        paymentCleared = F.col("cleared_invoice_number").isNotNull()
    else:
        paymentCleared = F.lit(False)
    planned = planned.withColumn(
        "accrual_status",
        F.when(F.col("conversion_rate").isNull(), "FX_MISSING").when(isCashBasis & ~paymentCleared, "HELD_CASH_BASIS").otherwise("ACCRUED"),
    )
    accruals = planned.select(
        *_baseColumns("EU"),
        "commission_period",
        F.col("net_commissionable_amount").alias("basis_amount"),
        F.col("currency_code").alias("basis_currency_code"),
        F.lit("EUR").alias("plan_currency_code"),
        "conversion_rate",
        F.col("net_amount_eur").alias("plan_currency_amount"),
        F.col("band1_rate_percent").alias("base_rate_percent"),
        F.col("commission_amount").alias("base_commission_amount"),
        F.lit(0).cast(MONEY).alias("accelerator_commission_amount"),
        "cap_applied",
        F.lit(1.0).cast("decimal(9,4)").alias("house_account_factor"),
        F.lit(1.0).cast("decimal(9,4)").alias("split_factor"),
        "commission_amount",
        "accrual_status",
        F.lit(False).alias("is_period_boundary_line"),
    )
    return accruals, unplanned


def calculateApacCommission(
    lines: DataFrame,
    plans: DataFrame,
    fxRates: DataFrame,
    dates: DataFrame = None,
    teamSplitEnabled: bool = False,
    teamSplitPercent: float = 100.0,
):
    """APAC: GST-exclusive amount, 4-4-5 fiscal periods from 1 July, period-average FX to the plan
    currency, optional team split. Returns (accruals, rejectedForMissingFx, unplanned, metrics)."""
    ap = assignPlans(commissionableLines(lines, "APAC"), plans)
    unplanned = ap.filter(F.col("plan_code").isNull())
    planned = ap.filter(F.col("plan_code").isNotNull())
    calendar = F.lit(fiscal.CALENDAR_APAC)
    planned = (
        planned.withColumn("commission_period", fiscal.fiscalPeriodLabel(F.col("invoice_date"), calendar))
        .withColumn("period_start_date", fiscal.fiscalPeriodStart(F.col("invoice_date"), calendar))
        .withColumn("gst_exclusive_amount", (F.col("total_including_tax") - F.coalesce(F.col("gst_amount"), F.lit(0))).cast(MONEY))
    )
    fx = fxRates.filter((F.col("rate_type_code") == "AVERAGE") & (F.col("quote_currency_code") == "AUD")).select(
        F.col("currency_code").alias("fx_currency_code"), "conversion_rate"
    )
    planned = planned.join(fx, planned["currency_code"] == fx["fx_currency_code"], "left").drop("fx_currency_code")
    rejected = planned.filter(F.col("conversion_rate").isNull())
    planned = planned.filter(F.col("conversion_rate").isNotNull())
    splitFactor = F.lit(teamSplitPercent / 100.0 if teamSplitEnabled else 1.0)
    planned = (
        planned.withColumn("plan_currency_amount", (F.col("gst_exclusive_amount") * F.col("conversion_rate")).cast(MONEY))
        .withColumn("split_factor", splitFactor.cast("decimal(9,4)"))
        .withColumn(
            "commission_amount", (F.col("plan_currency_amount") * F.col("band1_rate_percent") / 100 * F.col("split_factor")).cast(MONEY)
        )
        .withColumn("is_period_boundary_line", F.month(F.col("invoice_date")) != F.month(F.col("period_start_date")))
    )
    missingCalendarDays = 0
    if dates is not None:
        missingCalendarDays = (
            planned.select("invoice_date")
            .distinct()
            .join(dates.select(F.col("date").alias("invoice_date")), "invoice_date", "left_anti")
            .count()
        )
    accruals = planned.select(
        *_baseColumns("APAC"),
        "commission_period",
        F.col("gst_exclusive_amount").alias("basis_amount"),
        F.col("currency_code").alias("basis_currency_code"),
        F.lit("AUD").alias("plan_currency_code"),
        "conversion_rate",
        "plan_currency_amount",
        F.col("band1_rate_percent").alias("base_rate_percent"),
        F.col("commission_amount").alias("base_commission_amount"),
        F.lit(0).cast(MONEY).alias("accelerator_commission_amount"),
        F.lit(False).alias("cap_applied"),
        F.lit(1.0).cast("decimal(9,4)").alias("house_account_factor"),
        "split_factor",
        "commission_amount",
        F.lit("ACCRUED").alias("accrual_status"),
        "is_period_boundary_line",
    )
    return accruals, rejected, unplanned, {"missing_calendar_days": missingCalendarDays}


def writeMetrics(spark: SparkSession, packageName: str, metrics: dict, batchId: int):
    rows = [(packageName, k, float(v)) for k, v in metrics.items()]
    df = spark.createDataFrame(rows, "package_name string, metric_name string, metric_value double")
    saveTable(withAudit(df, packageName, batchId), METRIC_TABLE, mode="append")


def postCommissions(spark: SparkSession, accruals: DataFrame, packageName: str, batchId: int):
    """Native equivalent of Integration.usp_PostCommission: stamp the accrual onto gold_fact_sale."""
    posted = (
        accruals.filter(F.col("accrual_status") == "ACCRUED")
        .groupBy("sale_key")
        .agg(
            F.sum("commission_amount").cast(MONEY).alias("commission_amount"),
            F.max("commission_period").alias("commission_period"),
            F.max("plan_code").alias("commission_plan_code"),
        )
        .withColumn("commission_posted_batch_id", F.lit(batchId).cast("bigint"))
    )
    fact = readTable(spark, FACT_SALE_TABLE)
    if "commission_amount" not in fact.columns:
        fact = (
            fact.withColumn("commission_amount", F.lit(None).cast(MONEY))
            .withColumn("commission_period", F.lit(None).cast("string"))
            .withColumn("commission_plan_code", F.lit(None).cast("string"))
            .withColumn("commission_posted_batch_id", F.lit(None).cast("bigint"))
        )
        saveTable(fact, FACT_SALE_TABLE)
    mergeInto(
        spark,
        posted,
        FACT_SALE_TABLE,
        ["sale_key"],
        updateColumns=["commission_amount", "commission_period", "commission_plan_code", "commission_posted_batch_id"],
        insertAll=False,
    )
    return posted.count()


def runRegion(spark: SparkSession, regionCode: str, batchId: int, params: dict = None):
    params = params or {}
    packageName = PACKAGES[regionCode]
    lines = readTable(spark, SALE_LINE_TABLE)
    plans = readTable(spark, PLAN_TABLE)
    fx = readTable(spark, FX_TABLE)
    metrics = {}
    if regionCode == "NA":
        accruals, unplanned = calculateNaCommission(
            lines, plans, readTable(spark, QUOTA_TABLE), int(params.get("houseAccountRatePercent", 50))
        )
        metrics["unplanned_rep_count"] = unplanned.select("salesperson_key").distinct().count()
        rejected = None
    elif regionCode == "EU":
        countries = tuple(c.strip() for c in str(params.get("cashBasisCountries", "DEU,AUT")).split(","))
        accruals, unplanned = calculateEuCommission(lines, plans, fx, countries, params.get("statutoryCapAmount"))
        metrics["unplanned_rep_count"] = unplanned.select("salesperson_key").distinct().count()
        metrics["capped_rep_count"] = accruals.filter(F.col("cap_applied")).select("salesperson_key").distinct().count()
        metrics["held_on_cash_count"] = accruals.filter(F.col("accrual_status") == "HELD_CASH_BASIS").count()
        rejected = None
    else:
        accruals, rejected, unplanned, extra = calculateApacCommission(
            lines,
            plans,
            fx,
            readTable(spark, DATE_TABLE),
            bool(params.get("teamSplitEnabled", False)),
            float(params.get("teamSplitPercent", 100)),
        )
        metrics.update(extra)
        metrics["unplanned_rep_count"] = unplanned.select("salesperson_key").distinct().count()
        metrics["missing_fx_row_count"] = rejected.count()
        metrics["period_boundary_line_count"] = accruals.filter(F.col("is_period_boundary_line")).count()
    accruals = withAudit(accruals, packageName, batchId).cache()
    fullName = config.tableName(ACCRUAL_TABLE)
    if spark.catalog.tableExists(fullName):
        spark.sql(f"DELETE FROM {fullName} WHERE region_code = '{regionCode}'")
        saveTable(accruals, ACCRUAL_TABLE, mode="append")
    else:
        saveTable(accruals, ACCRUAL_TABLE)
    if rejected is not None and rejected.limit(1).count() > 0:
        saveTable(
            withAudit(rejected.withColumn("reject_reason_code", F.lit("FX_MISSING")), packageName, batchId), REJECT_TABLE, mode="append"
        )
    metrics["accrual_row_count"] = accruals.count()
    metrics["posted_sale_count"] = postCommissions(spark, accruals, packageName, batchId)
    writeMetrics(spark, packageName, metrics, batchId)
    return metrics
