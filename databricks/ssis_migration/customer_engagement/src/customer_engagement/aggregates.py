"""AGG_Refresh_Customer360 and AGG_Refresh_CustomerRolling12Month: month-end aggregate rebuilds."""

from __future__ import annotations

from datetime import date
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_engagement import tables
from customer_engagement.config import CeConfig
from customer_engagement.customer360 import customerView, saleView

INACTIVE_AFTER_DAYS = 730
ROLLING_MONTHS = 12


def loyaltyPosition(factLoyalty: Optional[DataFrame]) -> Optional[DataFrame]:
    """Aggregate.Customer Loyalty Position stand-in: current signed-point balance per customer key."""
    if factLoyalty is None:
        return None
    return factLoyalty.groupBy(F.col("CustomerKeyResolved").alias("CustomerKey")).agg(F.sum("SignedPoints").cast("int").alias("PointBalance"))


def webPosition(factWeb: Optional[DataFrame], asAt: date) -> Optional[DataFrame]:
    """Aggregate.Customer Web Position stand-in: session count (lifetime and trailing 90 days)."""
    if factWeb is None:
        return None
    return factWeb.groupBy(F.col("CustomerKeyResolved").alias("CustomerKey")).agg(
        F.count("*").cast("int").alias("SessionCount"),
        F.sum(F.when(F.to_date("SessionStartedAt") > F.date_sub(F.lit(asAt), 90), 1).otherwise(0)).cast("int").alias("SessionCount90Day"),
    )


def buildCustomer360(
    customers: DataFrame,
    sales: DataFrame,
    payments: Optional[DataFrame],
    loyalty: Optional[DataFrame],
    web: Optional[DataFrame],
    asAt: date,
    includeInactive: bool,
    lineageKey: int,
) -> Tuple[DataFrame, DataFrame]:
    """Package data flow ('Customer Profile Source' + 'Derive Profile Measures' + 'Route Profiles') followed by
    the Integration.RefreshAggregateCustomer360 scoring pass. Returns (aggregate rows, rejected inactive rows)."""
    current = customers.where(F.col("IsCurrentRow"))
    lifetime = sales.groupBy("CustomerKey").agg(
        F.min("InvoiceDate").alias("FirstOrderDate"),
        F.max("InvoiceDate").alias("LastOrderDate"),
        F.countDistinct("InvoiceNumber").cast("int").alias("LifetimeOrderCount"),
        F.sum("NetAmount").cast("decimal(18,2)").alias("LifetimeNetAmount"),
        F.sum("MarginAmount").cast("decimal(18,2)").alias("LifetimeMarginAmount"),
        F.sum(F.when(F.col("Quantity") < 0, F.abs(F.col("NetAmount"))).otherwise(0)).cast("decimal(18,2)").alias("LifetimeReturnsAmount"),
    )
    base = current.join(lifetime, on="CustomerKey", how="left")
    if payments is not None:
        pay = payments.groupBy(F.col("Customer Key").alias("CustomerKey")).agg(
            F.max("Payment Date Key").alias("LastPaymentDate"),
            F.avg("Days To Pay").cast("decimal(9,2)").alias("AverageDaysToPay"),
            F.sum("Unallocated Amount").cast("decimal(18,2)").alias("OpenArBalance"),
        )
        base = base.join(pay, on="CustomerKey", how="left")
    else:
        base = (
            base.withColumn("LastPaymentDate", F.lit(None).cast("date"))
            .withColumn("AverageDaysToPay", F.lit(None).cast("decimal(9,2)"))
            .withColumn("OpenArBalance", F.lit(None).cast("decimal(18,2)"))
        )
    base = base.join(loyalty, on="CustomerKey", how="left") if loyalty is not None else base.withColumn("PointBalance", F.lit(None).cast("int"))
    if web is not None:
        base = base.join(web, on="CustomerKey", how="left")
    else:
        base = base.withColumn("SessionCount", F.lit(None).cast("int")).withColumn("SessionCount90Day", F.lit(None).cast("int"))

    asAtCol = F.lit(asAt)
    orderCount = F.coalesce(F.col("LifetimeOrderCount"), F.lit(0))
    netAmount = F.coalesce(F.col("LifetimeNetAmount"), F.lit(0)).cast("decimal(18,2)")
    marginAmount = F.coalesce(F.col("LifetimeMarginAmount"), F.lit(0)).cast("decimal(18,2)")
    returnsAmount = F.coalesce(F.col("LifetimeReturnsAmount"), F.lit(0)).cast("decimal(18,2)")
    openAr = F.coalesce(F.col("OpenArBalance"), F.lit(0)).cast("decimal(18,2)")
    isErased = F.col("ErasureRequestedOn").isNotNull()
    region = F.col("RegionCode")
    derived = (
        base.withColumn("IsErased", isErased)
        .withColumn("RecencyDays", F.datediff(asAtCol, F.col("LastOrderDate")))
        .withColumn("TenureDays", F.datediff(asAtCol, F.col("FirstOrderDate")))
        .withColumn("TenureMonths", F.months_between(asAtCol, F.col("FirstOrderDate")).cast("int"))
        .withColumn("LifetimeOrderCount", orderCount)
        .withColumn("LifetimeNetAmount", netAmount)
        .withColumn("LifetimeMarginAmount", marginAmount)
        .withColumn("LifetimeReturnsAmount", returnsAmount)
        .withColumn("OpenArBalance", openAr)
        .withColumn("LoyaltyPointBalance", F.coalesce(F.col("PointBalance"), F.lit(0)))
        .withColumn("WebSessionCount", F.coalesce(F.col("SessionCount"), F.lit(0)))
        .withColumn("WebSessionCount90Day", F.coalesce(F.col("SessionCount90Day"), F.lit(0)))
        .withColumn("AverageOrderValue", F.when(orderCount == 0, F.lit(0)).otherwise(netAmount / orderCount).cast("decimal(18,2)"))
        .withColumn("LifetimeMarginPercent", F.when(netAmount == 0, F.lit(0)).otherwise(marginAmount / netAmount * 100).cast("decimal(9,4)"))
        .withColumn("IsInactive", F.coalesce(F.col("RecencyDays") > INACTIVE_AFTER_DAYS, F.lit(False)))
        .withColumn("PublishedSegmentCode", F.when(isErased, F.lit("REDACTED")).otherwise(F.col("CustomerSegmentKey").cast("string")))
        .withColumn(
            "CreditUtilisationPercent",
            F.when(F.coalesce(F.col("CreditLimitAmount"), F.lit(0)) == 0, F.lit(None)).otherwise(F.round(openAr * 100.0 / F.col("CreditLimitAmount"), 2)).cast("decimal(9,4)"),
        )
        .withColumn(
            "MarketingConsentPublished",
            F.when(region == "APAC", F.coalesce(F.col("MarketingConsentFlag"), F.lit(False)))
            .when(region == "EU", F.coalesce(F.col("MarketingConsentFlag"), F.lit(False)) & ~isErased)
            .otherwise(F.coalesce(F.col("MarketingConsentFlag"), F.lit(True))),
        )
        .withColumn(
            "RetentionExpiryDate",
            F.when(region == "EU", F.add_months(F.coalesce(F.col("LastOrderDate"), asAtCol), 84))
            .when(region == "APAC", F.add_months(F.coalesce(F.col("LastOrderDate"), asAtCol), 36))
            .otherwise(F.add_months(F.coalesce(F.col("LastOrderDate"), asAtCol), 120)),
        )
        .withColumn(
            "RouteCode",
            F.when(isErased, "ERASED_CUSTOMERS")
            .when(F.col("IsInactive") & F.lit(not includeInactive), "INACTIVE_CUSTOMERS")
            .when(orderCount == 0, "NO_PURCHASE_HISTORY")
            .otherwise("ACTIVE_CUSTOMERS"),
        )
        .withColumn("RefreshedByLineageKey", F.lit(lineageKey).cast("bigint"))
    )
    rfm = F.concat(
        F.when(F.col("RecencyDays") <= 30, "3").when(F.col("RecencyDays") <= 120, "2").otherwise("1"),
        F.when(orderCount >= 50, "3").when(orderCount >= 10, "2").otherwise("1"),
        F.when(netAmount >= 250000, "3").when(netAmount >= 25000, "2").otherwise("1"),
    )
    churnScore = F.when(F.col("RecencyDays").isNull(), F.lit(100)).otherwise(
        F.when(F.col("RecencyDays") > 365, 60).when(F.col("RecencyDays") > 180, 40).when(F.col("RecencyDays") > 90, 20).otherwise(0)
        + F.when(F.col("WebSessionCount90Day") == 0, 15).otherwise(0)
        + F.when(F.coalesce(F.col("CreditUtilisationPercent"), F.lit(0)) > 90, 10).otherwise(0)
        + F.when((netAmount > 0) & (returnsAmount > 0.10 * netAmount), 15).otherwise(0)
    )
    scored = (
        derived.withColumn("RfmScore", rfm)
        .withColumn("ChurnRiskScore", churnScore.cast("decimal(9,4)"))
        .withColumn("ChurnRiskBand", F.when(F.col("ChurnRiskScore") >= 70, "HIGH").when(F.col("ChurnRiskScore") >= 40, "MED").otherwise("LOW"))
        .withColumn("CustomerName", F.when(isErased & region.isin("EU", "APAC"), F.lit("REDACTED")).otherwise(F.col("CustomerName")))
        .withColumn("PrimaryContactEmail", F.when(isErased & region.isin("EU", "APAC"), F.lit(None).cast("string")).otherwise(F.col("ContactEmail")))
        .withColumn("AnonymisedFlag", isErased & region.isin("EU", "APAC"))
        .withColumn("AsAtDate", asAtCol)
    )
    selected = scored.select(
        "CustomerKey",
        "CustomerId",
        "RegionCode",
        "CustomerName",
        "PrimaryContactEmail",
        "CustomerSegmentKey",
        "PublishedSegmentCode",
        "AccountManagerEmployeeKey",
        "FirstOrderDate",
        "LastOrderDate",
        "LastPaymentDate",
        "TenureDays",
        "TenureMonths",
        "RecencyDays",
        "LifetimeOrderCount",
        "LifetimeNetAmount",
        "LifetimeMarginAmount",
        "LifetimeReturnsAmount",
        "AverageOrderValue",
        "LifetimeMarginPercent",
        "AverageDaysToPay",
        "OpenArBalance",
        "CreditLimitAmount",
        "CreditUtilisationPercent",
        "LoyaltyPointBalance",
        "WebSessionCount",
        "WebSessionCount90Day",
        "ChurnRiskScore",
        "ChurnRiskBand",
        "RfmScore",
        "MarketingConsentPublished",
        "RetentionExpiryDate",
        "IsErased",
        "IsInactive",
        "AnonymisedFlag",
        "RouteCode",
        "RefreshedByLineageKey",
        "AsAtDate",
    )
    return selected.where(F.col("RouteCode") != "INACTIVE_CUSTOMERS"), selected.where(F.col("RouteCode") == "INACTIVE_CUSTOMERS")


def runAggRefreshCustomer360(spark: SparkSession, cfg: CeConfig, asAt: date, includeInactive: bool = False) -> Tuple[int, int]:
    now = tables.utcNow()
    customers = customerView(spark.table(cfg.dw("dimension", "Customer")), cfg.defaultRegion)
    sales = saleView(spark.table(cfg.dw("fact", "Sale")))
    payments = tables.readTableOrNone(spark, cfg.dw("fact", "Payment"))
    loyalty = loyaltyPosition(tables.readTableOrNone(spark, cfg.table(tables.GOLD_FACT_LOYALTY_POINTS)))
    web = webPosition(tables.readTableOrNone(spark, cfg.table(tables.GOLD_FACT_WEB_SESSION)), asAt)
    rows, rejects = buildCustomer360(customers, sales, payments, loyalty, web, asAt, includeInactive, cfg.batchId)
    rows = rows.withColumn("RefreshedDatetime", F.lit(now).cast("timestamp"))
    tables.overwriteTable(rows, cfg.table(tables.GOLD_AGG_CUSTOMER_360))
    rejected = rejects.count()
    if rejected:
        tables.overwriteTable(
            rejects.withColumn("RejectReasonCode", F.lit("INACTIVE_CUSTOMER")).withColumn("RefreshedDatetime", F.lit(now).cast("timestamp")),
            cfg.table(tables.GOLD_AGG_CUSTOMER_360_REJECTS),
        )
    count = spark.table(cfg.table(tables.GOLD_AGG_CUSTOMER_360)).count()
    tables.logPackageRun(
        spark,
        cfg.table(tables.ETL_PACKAGE_RUN),
        "AGG_Refresh_Customer360",
        cfg.runId,
        count,
        rejected,
        f"as_at={asAt} include_inactive={includeInactive}",
    )
    return count, rejected


def periodCode(dateCol) -> F.Column:  # type: ignore[name-defined,no-untyped-def]
    return F.date_format(dateCol, "yyyy-MM")


def buildCustomerRolling12Month(sales: DataFrame, customers: DataFrame, accountingPeriodCode: str, rollingMonths: int, previous: Optional[DataFrame], lineageKey: int) -> DataFrame:
    """'Rolling Window Source' + 'Derive Rolling Trends' + 'Route Rolling Rows' for the requested period and the
    eleven behind it. Each period's window is the trailing ``rollingMonths`` months ending at the period end."""
    periodEnd = F.last_day(F.to_date(F.lit(accountingPeriodCode + "-01")))
    periods = (
        sales.select(F.explode(F.sequence(F.lit(-11), F.lit(0))).alias("offset"))
        .distinct()
        .select(F.last_day(F.add_months(periodEnd, F.col("offset"))).alias("PeriodEndDate"))
        .withColumn("AccountingPeriodCode", periodCode(F.col("PeriodEndDate")))
        .withColumn("WindowStartDate", F.add_months(F.col("PeriodEndDate"), -rollingMonths))
    )
    cust = customers.where(F.col("IsCurrentRow")).select("CustomerKey", "RegionCode")
    joined = sales.join(cust, on="CustomerKey", how="inner").join(
        periods,
        (F.col("InvoiceDate") >= F.col("WindowStartDate")) & (F.col("InvoiceDate") <= F.col("PeriodEndDate")),
        "inner",
    )
    agg = joined.groupBy("CustomerKey", "AccountingPeriodCode", "PeriodEndDate", "RegionCode").agg(
        F.sum("NetAmount").cast("decimal(18,2)").alias("RollingNetAmount"),
        F.sum("MarginAmount").cast("decimal(18,2)").alias("RollingMarginAmount"),
        F.countDistinct("InvoiceNumber").cast("int").alias("RollingOrderCount"),
        F.sum(F.when(F.col("Quantity") < 0, 1).otherwise(0)).cast("int").alias("RollingReturnCount"),
        F.sum(F.when(F.col("Quantity") < 0, F.abs(F.col("NetAmount"))).otherwise(0)).cast("decimal(18,2)").alias("RollingReturnAmount"),
    )
    priorSeries = agg.select(
        F.col("CustomerKey").alias("PriorCustomerKey"),
        F.col("AccountingPeriodCode").alias("PriorPeriodCode"),
        F.col("RollingNetAmount").alias("PriorRollingNetAmountSeries"),
    )
    withPrior = agg.withColumn("PrevPeriodCode", periodCode(F.add_months(F.col("PeriodEndDate"), -1)))
    withPrior = withPrior.join(
        priorSeries,
        (F.col("PriorCustomerKey") == F.col("CustomerKey")) & (F.col("PriorPeriodCode") == F.col("PrevPeriodCode")),
        "left",
    ).drop("PriorCustomerKey", "PriorPeriodCode")
    if previous is not None:
        prev = previous.select(
            F.col("CustomerKey").alias("PrevCustomerKey"),
            F.col("AccountingPeriodCode").alias("PrevTablePeriodCode"),
            F.col("RollingNetAmount").alias("PrevRollingNet"),
        )
        withPrior = withPrior.join(prev, (F.col("PrevCustomerKey") == F.col("CustomerKey")) & (F.col("PrevTablePeriodCode") == F.col("PrevPeriodCode")), "left").drop(
            "PrevCustomerKey", "PrevTablePeriodCode"
        )
    else:
        withPrior = withPrior.withColumn("PrevRollingNet", F.lit(None).cast("decimal(18,2)"))
    prior = F.coalesce(F.col("PriorRollingNetAmountSeries"), F.col("PrevRollingNet"), F.lit(0)).cast("decimal(18,2)")
    net = F.col("RollingNetAmount")
    derived = (
        withPrior.withColumn("PriorRollingNetAmount", prior)
        .withColumn("NetReturnRatePercent", F.when(net == 0, F.lit(0)).otherwise(F.col("RollingReturnAmount") / net * 100).cast("decimal(9,4)"))
        .withColumn("GrowthPercent", F.when(prior == 0, F.lit(0)).otherwise((net - prior) / prior * 100).cast("decimal(9,4)"))
        .withColumn(
            "TrendCode",
            F.when(prior == 0, "NEW").when(net > prior * 1.1, "GROWING").when(net < prior * 0.9, "DECLINING").otherwise("STABLE"),
        )
        .withColumn("AverageMonthlyNetAmount", (net / 12).cast("decimal(18,2)"))
        .withColumn(
            "RouteCode",
            F.when((F.col("TrendCode") == "DECLINING") & (F.col("NetReturnRatePercent") > 10), "CHURN_RISK")
            .when(F.col("TrendCode") == "NEW", "NEW_CUSTOMERS")
            .otherwise("ESTABLISHED_CUSTOMERS"),
        )
        .withColumn("RollingMonths", F.lit(rollingMonths))
        .withColumn("RefreshedByLineageKey", F.lit(lineageKey).cast("bigint"))
    )
    return derived.drop("PriorRollingNetAmountSeries", "PrevRollingNet", "PrevPeriodCode")


def runAggRefreshCustomerRolling12Month(spark: SparkSession, cfg: CeConfig, asAt: date, rollingMonths: int = ROLLING_MONTHS) -> Tuple[int, int]:
    now = tables.utcNow()
    target = cfg.table(tables.GOLD_AGG_CUSTOMER_ROLLING_12_MONTH)
    accountingPeriodCode = asAt.strftime("%Y-%m")
    customers = customerView(spark.table(cfg.dw("dimension", "Customer")), cfg.defaultRegion)
    sales = saleView(spark.table(cfg.dw("fact", "Sale")))
    previous = tables.readTableOrNone(spark, target)
    rows = buildCustomerRolling12Month(sales, customers, accountingPeriodCode, rollingMonths, previous, cfg.batchId)
    rows = rows.withColumn("RefreshedDatetime", F.lit(now).cast("timestamp"))
    rows.cache()
    rows.count()
    if previous is None:
        tables.overwriteTable(rows, target)
    else:
        lowPeriod = F.date_format(F.add_months(F.to_date(F.lit(accountingPeriodCode + "-01")), -11), "yyyy-MM")
        low = spark.range(1).select(lowPeriod.alias("p")).first()["p"]
        tables.deleteWhere(spark, target, f"AccountingPeriodCode >= '{low}' AND AccountingPeriodCode <= '{accountingPeriodCode}'")
        tables.appendTable(rows, target)
    rows.unpersist()
    count = spark.table(target).where(F.col("RefreshedDatetime") == F.lit(now).cast("timestamp")).count()
    tables.logPackageRun(
        spark,
        cfg.table(tables.ETL_PACKAGE_RUN),
        "AGG_Refresh_CustomerRolling12Month",
        cfg.runId,
        count,
        0,
        f"period={accountingPeriodCode} rolling_months={rollingMonths}",
    )
    return count, 0
