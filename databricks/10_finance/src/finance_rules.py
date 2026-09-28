"""Pure DataFrame ports of the WWI_Finance package logic.

Each function is a literal translation of the SQL / SSIS expressions in
ssis/10_finance/build_finance_packages.py and takes DataFrames in the package
column contract (see finance_sources.py). No Delta or control-framework calls
live here so the rules can be unit tested with a local SparkSession.
"""
from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# FIN_Load_ApAging
# ---------------------------------------------------------------------------
AP_EXCLUDED_STATUSES = ("CANC", "VOID", "DRAFT")
DISPUTE_HOLD_CODES = ("DISP", "DISPUTE", "DISPUTED")


def apAgingOpenItems(
    invoices: DataFrame,
    paymentTerms: DataFrame,
    asOfDate: dt.date,
    batchId: int,
    includeDisputed: bool = False,
    reloadFullHistory: bool = False,
) -> DataFrame:
    """AP_AGING_SQL + 'Derive Aging Attributes'."""
    ai = invoices.alias("ai")
    pt = paymentTerms.select("PaymentTermsCode", "DiscountPercent").dropDuplicates(["PaymentTermsCode"]).alias("pt")
    asOf = F.lit(asOfDate)
    daysPastDue = F.datediff(asOf, F.col("ai.DueDate"))
    openAmount = F.col("ai.InvoiceAmount") - F.col("ai.PaidAmount")

    df = ai.join(pt, F.col("pt.PaymentTermsCode") == F.col("ai.PaymentTermsCode"), "left")
    df = df.where(openAmount != 0).where(~F.coalesce(F.col("ai.InvoiceStatusCode"), F.lit("")).isin(*AP_EXCLUDED_STATUSES))
    if not reloadFullHistory:
        df = df.where(F.col("ai.LoadBatchId") == F.lit(batchId))
    if not includeDisputed and "IsOnHold" in invoices.columns:
        disputed = (F.coalesce(F.col("ai.IsOnHold"), F.lit(False)) == F.lit(True)) & F.upper(
            F.coalesce(F.col("ai.HoldReasonCode"), F.lit(""))
        ).isin(*DISPUTE_HOLD_CODES)
        df = df.where(~disputed)

    bucket = (
        F.when(daysPastDue <= 0, "CURRENT")
        .when(daysPastDue <= 30, "B030")
        .when(daysPastDue <= 60, "B060")
        .when(daysPastDue <= 90, "B090")
        .otherwise("B090P")
    )
    reportable = (
        F.when(F.col("ai.RegionCode") == "NA", F.col("ai.InvoiceAmount"))
        .when(F.col("ai.RegionCode") == "EU", F.col("ai.InvoiceAmount") - F.coalesce(F.col("ai.RecoverableVatAmount"), F.lit(0)))
        .when(F.col("ai.RegionCode") == "APAC", F.col("ai.InvoiceAmount") - F.coalesce(F.col("ai.GstInputCreditAmount"), F.lit(0)))
        .otherwise(F.col("ai.InvoiceAmount"))
    )
    discountPct = F.coalesce(F.col("pt.DiscountPercent"), F.lit(0))
    out = df.select(
        F.col("ai.ApInvoiceKey").alias("ApInvoiceKey"),
        F.col("ai.SupplierId").alias("SupplierId"),
        F.col("ai.SupplierSiteCode").alias("SupplierSiteCode"),
        F.col("ai.InvoiceNumber").alias("InvoiceNumber"),
        F.col("ai.InvoiceDate").alias("InvoiceDate"),
        F.col("ai.DueDate").alias("DueDate"),
        F.col("ai.CurrencyCode").alias("CurrencyCode"),
        F.col("ai.LedgerCode").alias("LedgerCode"),
        F.col("ai.RegionCode").alias("RegionCode"),
        F.col("ai.InvoiceAmount").alias("InvoiceAmount"),
        F.col("ai.PaidAmount").alias("PaidAmount"),
        openAmount.alias("OpenAmount"),
        daysPastDue.cast("int").alias("DaysPastDue"),
        bucket.alias("AgingBucketCode"),
        reportable.alias("ReportableAmount"),
        discountPct.alias("EarlyPaymentDiscountPercent"),
    )
    return out.withColumn("IsPastDue", F.col("DaysPastDue") > 0).withColumn(
        "AgingBucketSort",
        F.when(F.col("AgingBucketCode") == "CURRENT", 0)
        .when(F.col("AgingBucketCode") == "B030", 1)
        .when(F.col("AgingBucketCode") == "B060", 2)
        .when(F.col("AgingBucketCode") == "B090", 3)
        .otherwise(4),
    ).withColumn(
        "DiscountAtRisk",
        F.when(F.col("DaysPastDue") > 0, F.lit(0)).otherwise(
            F.col("OpenAmount") * F.col("EarlyPaymentDiscountPercent") / F.lit(100)
        ).cast("decimal(19,4)"),
    ).withColumn("AsOfDate", F.lit(asOfDate))


def lookupSupplierKey(aging: DataFrame, supplierDim: DataFrame) -> tuple[DataFrame, DataFrame]:
    """'Lookup Supplier Key' (no-match -> err.ApAgingReject). supplierDim has SupplierKey, SupplierId."""
    dim = supplierDim.select("SupplierKey", "SupplierId").dropDuplicates(["SupplierId"])
    joined = aging.join(dim, "SupplierId", "left")
    matched = joined.where(F.col("SupplierKey").isNotNull())
    rejected = joined.where(F.col("SupplierKey").isNull()).drop("SupplierKey").withColumn(
        "RejectReasonCode", F.lit("SUPPLIER_NOT_FOUND")
    )
    return matched, rejected


def apAgingSummary(aging: DataFrame) -> DataFrame:
    """Set-based replacement for the Integration.usp_RefreshApAgingSummary cursor
    (one row per LedgerCode/AgingBucketCode, in cursor order)."""
    return (
        aging.groupBy("LedgerCode", "AgingBucketCode")
        .agg(
            F.min("AgingBucketSort").alias("AgingBucketSort"),
            F.count("*").alias("OpenItemCount"),
            F.sum("OpenAmount").alias("OpenAmount"),
            F.sum("ReportableAmount").alias("ReportableAmount"),
            F.sum("DiscountAtRisk").alias("DiscountAtRisk"),
        )
        .orderBy("LedgerCode", "AgingBucketCode")
    )


# ---------------------------------------------------------------------------
# FIN_Load_GlPostings
# ---------------------------------------------------------------------------
def unbalancedJournalCount(journalLines: DataFrame, batchId: int, reloadFullHistory: bool = False) -> int:
    df = journalLines if reloadFullHistory else journalLines.where(F.col("LoadBatchId") == F.lit(batchId))
    return (
        df.groupBy("JournalNumber")
        .agg((F.sum("EnteredDebitAmount") - F.sum("EnteredCreditAmount")).alias("Diff"))
        .where(F.abs(F.col("Diff")) > 0.005)
        .count()
    )


def glPostedLines(
    journalLines: DataFrame,
    openPeriods: DataFrame,
    batchId: int,
    reloadFullHistory: bool = False,
) -> DataFrame:
    """GL_SQL: POSTED lines whose period is the open period for their ledger
    (openPeriods: LedgerCode, AccountingPeriod from etl.configuration
    Finance.OpenPeriod.<LedgerCode>)."""
    gl = journalLines.alias("gl")
    cfg = openPeriods.select(
        F.col("LedgerCode").alias("cfgLedgerCode"), F.col("AccountingPeriod").alias("cfgPeriod")
    ).dropDuplicates()
    df = gl.join(
        cfg,
        (F.col("gl.LedgerCode") == F.col("cfgLedgerCode")) & (F.col("gl.AccountingPeriod") == F.col("cfgPeriod")),
        "inner",
    ).where(F.col("gl.JournalStatusCode") == "POSTED")
    if not reloadFullHistory:
        df = df.where(F.col("gl.LoadBatchId") == F.lit(batchId))
    fdr = F.coalesce(F.col("gl.FunctionalDebitAmount"), F.col("gl.EnteredDebitAmount"))
    fcr = F.coalesce(F.col("gl.FunctionalCreditAmount"), F.col("gl.EnteredCreditAmount"))
    return df.select(
        "gl.GlJournalLineId", "gl.JournalNumber", "gl.JournalLineNumber", "gl.LedgerCode",
        "gl.CostCentreCode", "gl.AccountCode", "gl.PostingDate", "gl.AccountingPeriod",
        "gl.CurrencyCode", "gl.EnteredDebitAmount", "gl.EnteredCreditAmount",
        "gl.SourceSubledgerCode", "gl.SourceDocumentNumber",
        fdr.alias("FunctionalDebitAmount"),
        fcr.alias("FunctionalCreditAmount"),
        F.when(F.col("gl.LedgerCode").like("EU%"), "VAT")
        .when(F.col("gl.LedgerCode").like("APAC%"), "GST")
        .otherwise("SALESTAX")
        .alias("TaxRegimeCode"),
    )


def deriveGlPostingAttributes(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("NetAmount", F.col("FunctionalDebitAmount") - F.col("FunctionalCreditAmount"))
        .withColumn("PostingSide", F.when(F.col("FunctionalDebitAmount") > 0, "DR").otherwise("CR"))
        .withColumn(
            "SubledgerSourceKey",
            F.concat(F.coalesce(F.col("SourceSubledgerCode"), F.lit("")), F.lit("|"), F.coalesce(F.col("SourceDocumentNumber"), F.lit(""))),
        )
    )


def splitHeldLines(df: DataFrame, accountingPeriod: str) -> tuple[DataFrame, DataFrame]:
    """'Split Held Lines': Postable == requested period, Held otherwise."""
    postable = df.where(F.col("AccountingPeriod") == F.lit(accountingPeriod))
    held = df.where(F.col("AccountingPeriod") != F.lit(accountingPeriod)).withColumn(
        "RequestedAccountingPeriod", F.lit(accountingPeriod)
    )
    return postable, held


# ---------------------------------------------------------------------------
# FIN_Reconcile_SubledgerToGl
# ---------------------------------------------------------------------------
RECONCILIATION_NAME = "Subledger to GL"


def buildReconciliationSet(
    payments: DataFrame,
    glPostings: DataFrame,
    accountingPeriod: str,
    tolerance: float,
    batchId: int,
) -> DataFrame:
    """'Build Reconciliation Set' (FULL OUTER JOIN of subledger and ledger totals).
    payments: LedgerCode, AccountingPeriod, ControlAccount, FunctionalAmount
    glPostings: LedgerCode, AccountingPeriod, AccountCode, FunctionalDebitAmount, FunctionalCreditAmount"""
    sub = (
        payments.where(F.col("AccountingPeriod") == F.lit(accountingPeriod))
        .groupBy(F.col("LedgerCode"), F.col("AccountingPeriod"), F.col("ControlAccount").alias("AccountCode"))
        .agg(F.sum("FunctionalAmount").alias("SubledgerAmount"))
        .alias("s")
    )
    led = (
        glPostings.where(F.col("AccountingPeriod") == F.lit(accountingPeriod))
        .groupBy("LedgerCode", "AccountingPeriod", "AccountCode")
        .agg(F.sum(F.col("FunctionalDebitAmount") - F.col("FunctionalCreditAmount")).alias("LedgerAmount"))
        .alias("l")
    )
    joined = sub.join(
        led,
        (F.col("l.LedgerCode") == F.col("s.LedgerCode"))
        & (F.col("l.AccountCode") == F.col("s.AccountCode"))
        & (F.col("l.AccountingPeriod") == F.col("s.AccountingPeriod")),
        "full_outer",
    )
    source = F.coalesce(F.col("s.SubledgerAmount"), F.lit(0)).cast("decimal(19,4)")
    target = F.coalesce(F.col("l.LedgerAmount"), F.lit(0)).cast("decimal(19,4)")
    variance = (source - target).cast("decimal(19,4)")
    return joined.select(
        F.lit(batchId).cast("bigint").alias("BatchId"),
        F.lit(RECONCILIATION_NAME).alias("ReconciliationName"),
        F.lit("Fact.Payment -> Fact.GL Posting").alias("ObjectName"),
        F.coalesce(F.col("s.LedgerCode"), F.col("l.LedgerCode")).alias("LedgerCode"),
        F.coalesce(F.col("s.AccountingPeriod"), F.col("l.AccountingPeriod")).alias("AccountingPeriod"),
        F.coalesce(F.col("s.AccountCode"), F.col("l.AccountCode")).alias("AccountCode"),
        source.alias("SourceAmount"),
        target.alias("TargetAmount"),
        variance.alias("VarianceAmount"),
        F.when(F.abs(variance) <= F.lit(tolerance), "Within tolerance").otherwise("Variance").alias("VarianceStatus"),
        F.lit(None).cast("string").alias("ExplanationCode"),
        F.current_timestamp().alias("EvaluatedAtUtc"),
    ).withColumn("SourceKey", F.concat_ws("|", F.col("LedgerCode"), F.col("AccountCode")))


def applyKnownExplanations(results: DataFrame, knownVariances: DataFrame) -> DataFrame:
    """'Apply Known Explanations': Variance rows whose AccountCode has a
    Finance.KnownVariance.<AccountCode> configuration become Explained.
    knownVariances: AccountCode, ExplanationCode."""
    kv = knownVariances.select(
        F.col("AccountCode").alias("kvAccountCode"), F.col("ExplanationCode").alias("kvExplanationCode")
    ).dropDuplicates(["kvAccountCode"])
    df = results.join(kv, results["AccountCode"] == kv["kvAccountCode"], "left")
    explained = (F.col("VarianceStatus") == "Variance") & F.col("kvExplanationCode").isNotNull()
    return df.withColumns(
        {
            "VarianceStatus": F.when(explained, "Explained").otherwise(F.col("VarianceStatus")),
            "ExplanationCode": F.when(explained, F.col("kvExplanationCode")).otherwise(F.col("ExplanationCode")),
        }
    ).drop("kvAccountCode", "kvExplanationCode")


def unexplainedVariances(results: DataFrame) -> DataFrame:
    return results.where(F.col("VarianceStatus") == "Variance")


# ---------------------------------------------------------------------------
# FIN_Load_CostAllocation
# ---------------------------------------------------------------------------
def activeRules(rules: DataFrame, costCentres: DataFrame, ruleSet: str) -> DataFrame:
    """'Count Allocation Rules' join: rules whose source cost centre exists."""
    cc = costCentres.select(F.col("CostCentreCode").alias("ccCode")).dropDuplicates()
    return (
        rules.where((F.col("RuleSetCode") == F.lit(ruleSet)) & (F.col("IsActive") == F.lit(True)))
        .join(cc, F.col("SourceCostCentreCode") == F.col("ccCode"), "inner")
        .drop("ccCode")
    )


def allocateCosts(
    rules: list[dict],
    targets: DataFrame,
    balances: DataFrame,
    accountingPeriod: str,
    batchId: int,
) -> DataFrame:
    """'Apply Allocation Rules' cursor: rules are applied in RuleSequence order
    and each pass sees the pool including amounts allocated INTO the source cost
    centre by earlier passes (which is why the legacy loop was not set-based).
    rules: list of dicts with AllocationRuleId, RuleSequence, SourceCostCentreCode, DriverCode.
    targets: AllocationRuleId, TargetCostCentreCode, DriverValue.
    balances: CostCentreCode, AccountingPeriod, Amount."""
    spark = targets.sparkSession
    resultSchema = (
        "AllocationRuleId INT, RuleSequence INT, SourceCostCentreCode STRING, TargetCostCentreCode STRING, "
        "DriverCode STRING, DriverValue DECIMAL(19,4), PoolAmount DECIMAL(19,4), AllocatedAmount DECIMAL(19,4), "
        "AccountingPeriod STRING, RuleSetCode STRING, BatchId BIGINT"
    )
    result = spark.createDataFrame([], resultSchema)
    periodBalances = balances.where(F.col("AccountingPeriod") == F.lit(accountingPeriod))
    for rule in sorted(rules, key=lambda r: (int(r["RuleSequence"]), int(r["AllocationRuleId"]))):
        source = rule["SourceCostCentreCode"]
        base = periodBalances.where(F.col("CostCentreCode") == F.lit(source)).agg(F.sum("Amount")).collect()[0][0] or 0
        allocatedIn = result.where(F.col("TargetCostCentreCode") == F.lit(source)).agg(F.sum("AllocatedAmount")).collect()[0][0] or 0
        pool = float(base) + float(allocatedIn)
        ruleTargets = targets.where(F.col("AllocationRuleId") == F.lit(int(rule["AllocationRuleId"])))
        totalDriver = F.sum("DriverValue").over(Window.partitionBy())
        rows = ruleTargets.select(
            F.lit(int(rule["AllocationRuleId"])).cast("int").alias("AllocationRuleId"),
            F.lit(int(rule["RuleSequence"])).cast("int").alias("RuleSequence"),
            F.lit(source).alias("SourceCostCentreCode"),
            F.col("TargetCostCentreCode"),
            F.lit(rule["DriverCode"]).alias("DriverCode"),
            F.col("DriverValue").cast("decimal(19,4)").alias("DriverValue"),
            F.lit(pool).cast("decimal(19,4)").alias("PoolAmount"),
            (F.lit(pool) * (F.col("DriverValue") / F.when(totalDriver == 0, F.lit(None)).otherwise(totalDriver))).cast("decimal(19,4)").alias("AllocatedAmount"),
            F.lit(accountingPeriod).alias("AccountingPeriod"),
            F.lit(rule.get("RuleSetCode")).cast("string").alias("RuleSetCode"),
            F.lit(batchId).cast("bigint").alias("BatchId"),
        )
        result = result.unionByName(rows).localCheckpoint(eager=True) if rows.take(1) else result
    return result


def unallocatedResidual(balances: DataFrame, rules: DataFrame, allocations: DataFrame, accountingPeriod: str, ruleSet: str) -> float:
    """'Measure Unallocated Residual': pool total of rule sources minus everything allocated."""
    srcs = rules.where((F.col("RuleSetCode") == F.lit(ruleSet)) & (F.col("IsActive") == F.lit(True))).select(
        F.col("SourceCostCentreCode").alias("CostCentreCode")
    )
    poolTotal = (
        balances.where(F.col("AccountingPeriod") == F.lit(accountingPeriod))
        .join(srcs, "CostCentreCode", "inner")
        .agg(F.sum("Amount")).collect()[0][0] or 0
    )
    allocated = allocations.where(F.col("AccountingPeriod") == F.lit(accountingPeriod)).agg(F.sum("AllocatedAmount")).collect()[0][0] or 0
    return float(poolTotal) - float(allocated)


def summariseAllocationsByTarget(allocations: DataFrame) -> DataFrame:
    """'Summarise By Target' aggregate: SUM(AllocatedAmount), COUNT DISTINCT rules."""
    return allocations.groupBy("TargetCostCentreCode", "AccountingPeriod").agg(
        F.sum("AllocatedAmount").cast("decimal(19,4)").alias("AllocatedCostAmount"),
        F.countDistinct("AllocationRuleId").alias("AllocationRuleCount"),
    )


# ---------------------------------------------------------------------------
# FIN_Currency_Revaluation
# ---------------------------------------------------------------------------
def closingRates(fxRates: DataFrame, revaluationDate: dt.date) -> DataFrame:
    """FX_SQL + 'Derive Inverse Rate': latest rate date on/before the revaluation date."""
    latest = fxRates.where(F.col("RateDate") <= F.lit(revaluationDate)).agg(F.max("RateDate").alias("maxDate"))
    df = fxRates.join(latest, fxRates["RateDate"] == latest["maxDate"], "inner").drop("maxDate")
    df = df.where(F.col("RateTypeCode").isin("CLOSING", "AVERAGE"))
    return df.select("CurrencyCode", "QuoteCurrencyCode", "RateDate", "RateTypeCode", "ConversionRate", "RateSourceCode").withColumn(
        "InverseRate",
        F.when(F.col("ConversionRate") == 0, F.lit(0)).otherwise(F.lit(1) / F.col("ConversionRate")).cast("decimal(19,8)"),
    ).withColumn("IsTriangulated", F.col("QuoteCurrencyCode") != "USD")


def missingClosingRateCurrencies(openInvoices: DataFrame, rates: DataFrame) -> DataFrame:
    """'Find Missing Rates': open-item currencies with no CLOSING rate."""
    currencies = openInvoices.where((F.col("InvoiceAmount") - F.col("PaidAmount")) != 0).select("CurrencyCode").dropDuplicates()
    closing = rates.where(F.col("RateTypeCode") == "CLOSING").select("CurrencyCode").dropDuplicates()
    return currencies.join(closing, "CurrencyCode", "left_anti")


def quoteCurrencyFor(ledgerCode, entityCurrencyCode):
    return (
        F.when(ledgerCode.like("EU%"), F.lit("EUR"))
        .when(ledgerCode.like("APAC%"), entityCurrencyCode)
        .otherwise(F.lit("USD"))
    )


def revalueOpenItems(payments: DataFrame, rates: DataFrame) -> DataFrame:
    """'Revalue Open Items': returns the revalued columns keyed by PaymentBusinessKey.
    payments: PaymentBusinessKey, LedgerCode, AccountClass, TransactionCurrencyCode,
    EntityCurrencyCode, TransactionAmount, FunctionalAmount, OpenAmount."""
    p = payments.where(F.coalesce(F.col("OpenAmount"), F.lit(0)) != 0).alias("p")
    r = rates.alias("r")
    rateType = F.when(F.col("p.AccountClass") == "PL", "AVERAGE").otherwise("CLOSING")
    joined = p.join(
        r,
        (F.col("r.CurrencyCode") == F.col("p.TransactionCurrencyCode"))
        & (F.col("r.RateTypeCode") == rateType)
        & (F.col("r.QuoteCurrencyCode") == quoteCurrencyFor(F.col("p.LedgerCode"), F.col("p.EntityCurrencyCode"))),
        "inner",
    )
    revalued = (F.col("p.TransactionAmount") * F.col("r.ConversionRate")).cast("decimal(19,4)")
    return joined.select(
        F.col("p.PaymentBusinessKey").alias("PaymentBusinessKey"),
        revalued.alias("RevaluedFunctionalAmount"),
        (revalued - F.col("p.FunctionalAmount")).cast("decimal(19,4)").alias("UnrealisedGainLossAmount"),
        F.col("r.RateDate").alias("RevaluationRateDate"),
        rateType.alias("RevaluationRateType"),
        F.col("r.ConversionRate").alias("FxRateToReporting"),
        F.col("r.RateSourceCode").alias("FxRateSourceCode"),
    )


# ---------------------------------------------------------------------------
# FIN_Load_WithholdingTax
# ---------------------------------------------------------------------------
NA_WITHHOLDING_CATEGORIES = ("CONS", "LEGL", "MEDI", "RENT")


def withholdingLines(
    lines: DataFrame,
    rates: DataFrame,
    batchId: int,
    jurisdictionScope: str = "ALL",
    reloadFullHistory: bool = False,
) -> DataFrame:
    """WHT_SQL + 'Derive Net Payable'.
    rates: JurisdictionCode, ServiceCategoryCode, WithholdingRatePercent, TreatyRatePercent,
    WithholdingThresholdAmount, EffectiveFrom, EffectiveTo."""
    l = lines.alias("l")
    t = rates.alias("t")
    df = l.join(
        t,
        (F.col("t.JurisdictionCode") == F.col("l.JurisdictionCode"))
        & (F.col("t.ServiceCategoryCode") == F.col("l.ServiceCategoryCode"))
        & (F.col("l.InvoiceDate") >= F.col("t.EffectiveFrom"))
        & (F.col("l.InvoiceDate") <= F.coalesce(F.col("t.EffectiveTo"), F.lit(dt.date(9999, 12, 31)))),
        "left",
    ).where(F.coalesce(F.col("l.LineTypeCode"), F.lit("")) != "FREIGHT")
    if not reloadFullHistory:
        df = df.where(F.col("l.LoadBatchId") == F.lit(batchId))
    if jurisdictionScope and jurisdictionScope.upper() != "ALL":
        df = df.where(F.col("l.JurisdictionCode") == F.lit(jurisdictionScope))

    rate = F.coalesce(F.col("t.WithholdingRatePercent"), F.lit(0))
    threshold = F.coalesce(F.col("t.WithholdingThresholdAmount"), F.lit(0))
    amount = F.col("l.LineAmount")
    region = F.col("l.RegionCode")
    hasRegistration = F.length(F.trim(F.coalesce(F.col("l.SupplierTaxRegistrationNumber"), F.lit("")))) > 0
    withholding = (
        F.when((region == "NA") & F.col("l.ServiceCategoryCode").isin(*NA_WITHHOLDING_CATEGORIES), amount * rate / 100)
        .when((region == "EU") & hasRegistration, amount * F.coalesce(F.col("t.TreatyRatePercent"), F.col("t.WithholdingRatePercent")) / 100)
        .when(region == "EU", amount * rate / 100)
        .when((region == "APAC") & (amount >= threshold), amount * rate / 100)
        .otherwise(F.lit(0))
    ).cast("decimal(19,4)")
    out = df.select(
        F.col("l.ApInvoiceLineId").alias("ApInvoiceLineId"),
        F.col("l.ApInvoiceKey").alias("ApInvoiceKey"),
        F.col("l.SupplierId").alias("SupplierId"),
        F.col("l.SupplierTaxRegistrationNumber").alias("SupplierTaxRegistrationNumber"),
        F.col("l.JurisdictionCode").alias("JurisdictionCode"),
        region.alias("RegionCode"),
        amount.alias("LineAmount"),
        F.col("l.TaxCode").alias("TaxCode"),
        F.col("l.ServiceCategoryCode").alias("ServiceCategoryCode"),
        rate.cast("decimal(9,4)").alias("WithholdingRatePercent"),
        threshold.cast("decimal(19,4)").alias("WithholdingThresholdAmount"),
        withholding.alias("WithholdingAmount"),
        *[F.col(f"l.{c}").alias(c) for c in ("LedgerCode", "InvoiceDate") if c in lines.columns],
    )
    return (
        out.withColumn("NetPayableAmount", (F.col("LineAmount") - F.col("WithholdingAmount")).cast("decimal(19,4)"))
        .withColumn("IsWithheld", F.col("WithholdingAmount") > 0)
        .withColumn("WithholdingCertificateRequired", (F.col("RegionCode") == "EU") & (F.col("WithholdingAmount") > 0))
    )


def splitUnmappedJurisdictions(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """'Split Unmapped Jurisdictions'."""
    mapped = df.where((F.col("WithholdingRatePercent") > 0) | (F.col("WithholdingAmount") == 0))
    unmapped = df.where((F.col("WithholdingRatePercent") == 0) & (F.col("WithholdingAmount") > 0)).withColumn(
        "RejectReasonCode", F.lit("JURISDICTION_UNMAPPED")
    )
    return mapped, unmapped


def certificateQueue(withholding: DataFrame, batchId: int) -> DataFrame:
    """'Queue EU Withholding Certificates'."""
    return (
        withholding.where(F.col("WithholdingCertificateRequired") == F.lit(True))
        .groupBy("SupplierId", "JurisdictionCode")
        .agg(F.sum("WithholdingAmount").cast("decimal(19,4)").alias("WithholdingAmount"))
        .withColumn("QueuedAtUtc", F.current_timestamp())
        .withColumn("BatchId", F.lit(batchId).cast("bigint"))
    )
