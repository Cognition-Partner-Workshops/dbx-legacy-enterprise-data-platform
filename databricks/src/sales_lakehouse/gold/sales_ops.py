"""Quota attainment and commission - replaces the SSIS ``ssis/11_sales`` packages.

Legacy artifact                       -> gold table / function
SLS_Load_QuotaAttainment              -> agg_quota_attainment   (buildQuotaAttainment)
SLS_NA_Load_Commission                -> agg_commission, region 'NA'   (buildCommission)
SLS_EU_Load_Commission                -> agg_commission, region 'EU'
SLS_APAC_Load_Commission              -> agg_commission, region 'APAC'
SLS_Export_PartnerFeed                -> gold.partner_feed.exportPartnerFeed (called from run)

Inputs (contract in CONVENTIONS.md):
  gold   : fact_sale, fact_order, fact_credit_note, fact_payment, fact_sales_margin
  silver : dim_salesperson (salesperson_key, wwi_person_id), dim_sales_territory
           (sales_territory_key, wwi_sales_territory_id, region_code), dim_sales_channel
           (sales_channel_key, is_commissionable), dim_customer, dim_fiscal_calendar, ref_fx_rate
  bronze : sqlserver_sales_commission_plans, sqlserver_application_sales_team_members
           (plan assignment + QuotaSharePercent), sqlserver_sales_sales_quotas
  optional silver ref_commission_statutory_cap (plan_code, statutory_cap_amount) - the
           legacy stg.CommissionPlan.StatutoryCapAmount has no checked-in source DDL.

Both aggregates are full rebuilds (the legacy packages TRUNCATE work.* and
usp_RecalculateCommissionAccruals reverses + re-accrues the whole period).
"""
from __future__ import annotations

import datetime as dt

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.quality import quarantine
from sales_lakehouse.common.tables import overwriteTable, readTable
from sales_lakehouse.gold import partner_feed
from sales_lakehouse.gold.inputs import (
    LEGACY_REGION_CURRENCY,
    NA_FISCAL_CALENDAR_CODE,
    conformDimSalesperson,
    monthlyAverageFxRate,
    naFiscalPeriodLabel,
    notReversal,
    readDimCustomer,
    readOrEmpty,
)

AGG_QUOTA_ATTAINMENT = "agg_quota_attainment"
AGG_COMMISSION = "agg_commission"
REGIONS = ("NA", "EU", "APAC")

BASIS_INVOICED_MARGIN = "INVOICEDMARGIN"
BASIS_NET_REVENUE = "NETREVENUE"
BASIS_COLLECTED_CASH = "COLLECTEDCASH"

MEASURE_BASIS_BY_REGION: dict[str, str] = {"NA": "INVOICED", "EU": "NET_OF_CREDITS", "APAC": "ORDER_INTAKE"}
HOUSE_ACCOUNT_RATE_PERCENT = 50  # $Package::HouseAccountRatePercent default
STATUTORY_CAP_SCHEMA = "plan_code STRING, statutory_cap_amount DECIMAL(18,2)"


def _refreshColumns(df: DataFrame, batchId: int) -> DataFrame:
    return df.withColumn("refresh_batch_id", F.lit(batchId).cast("bigint")).withColumn(
        "refreshed_datetime", F.current_timestamp()
    )


# --------------------------------------------------------------------------- quota attainment
def _quotaHeader(quota: DataFrame, salesperson: DataFrame, territory: DataFrame) -> DataFrame:
    sp = salesperson.select(F.col("wwi_person_id").alias("SalespersonPersonID"), "salesperson_key")
    terr = territory.select(
        F.col("wwi_sales_territory_id").alias("SalesTerritoryID"), "sales_territory_key", "region_code"
    )
    return (
        quota.join(sp, "SalespersonPersonID", "left")
        .join(terr, "SalesTerritoryID", "left")
        .select(
            "salesperson_key", "sales_territory_key", "region_code",
            F.col("FiscalCalendarCode").alias("fiscal_calendar_code"),
            F.col("FiscalPeriodLabel").alias("fiscal_period_label"),
            F.col("PeriodStartDate").alias("period_start_date"),
            F.col("PeriodEndDate").alias("period_end_date"),
            F.col("QuotaAmount").alias("quota_amount"),
            F.col("QuotaCurrencyCode").alias("quota_currency_code"),
        )
    )


def _actualsInPeriod(quotas: DataFrame, actuals: DataFrame, dateCol: str, amountCol: str, region: str) -> DataFrame:
    """Sum ``amountCol`` of ``actuals`` per quota row whose period contains ``dateCol``."""
    q = quotas.filter(F.col("region_code") == region).alias("q")
    a = actuals.filter(F.col("region_code") == region).alias("a")
    joined = q.join(
        a,
        (F.col("a.salesperson_key") == F.col("q.salesperson_key"))
        & (F.col(f"a.{dateCol}") >= F.col("q.period_start_date"))
        & (F.col(f"a.{dateCol}") <= F.col("q.period_end_date")),
        "left",
    )
    return joined.groupBy(*[F.col(f"q.{c}").alias(c) for c in quotas.columns]).agg(
        F.coalesce(F.sum(F.col(f"a.{amountCol}")), F.lit(0)).cast("decimal(18,2)").alias("actual_amount")
    )


def buildQuotaAttainment(
    quota: DataFrame,
    salesperson: DataFrame,
    territory: DataFrame,
    sale: DataFrame,
    creditNote: DataFrame,
    order: DataFrame,
    batchId: int,
) -> tuple[DataFrame, DataFrame]:
    """SLS_Load_QuotaAttainment: salesperson x fiscal period x region.

    Returns (attainment, unresolvedQuotas).  Bookings per region:
      NA   INVOICED        fact_sale net_amount_reporting (lakehouse contract: net USD).
                           # LEGACY QUIRK (documented deviation): the SSIS package summed
                           # ExtendedPrice + TaxAmount (gross incl. tax); the target contract
                           # fixes NA quota on net USD.  See PR "Open questions".
      EU   NET_OF_CREDITS  fact_sale net_amount_reporting less credit notes raised against
                           those invoices (credit attributed to the invoice's period, as the
                           legacy join on InvoiceNumber does, whatever the credit date).
      APAC ORDER_INTAKE    fact_order net_order_amount_reporting by order date.
    """
    header = _quotaHeader(quota, salesperson, territory)
    unresolved = header.filter(F.col("salesperson_key").isNull() | F.col("sales_territory_key").isNull())
    quotas = header.filter(F.col("salesperson_key").isNotNull() & F.col("sales_territory_key").isNotNull())

    sales = notReversal(sale).select(
        "region_code", "salesperson_key", "invoice_number", "invoice_date_key", "net_amount_reporting"
    )
    na = _actualsInPeriod(quotas, sales, "invoice_date_key", "net_amount_reporting", "NA")

    invoices = sales.select("invoice_number", "invoice_date_key", "salesperson_key", "region_code").distinct()
    credits = creditNote.select(
        F.col("original_invoice_number").alias("invoice_number"),
        (F.col("credit_excluding_tax") * F.coalesce(F.col("fx_rate_to_reporting"), F.lit(1.0))).alias("credit_reporting"),
    ).join(invoices, "invoice_number", "inner")
    euSales = _actualsInPeriod(quotas, sales, "invoice_date_key", "net_amount_reporting", "EU")
    euCredits = _actualsInPeriod(quotas, credits, "invoice_date_key", "credit_reporting", "EU").withColumnRenamed(
        "actual_amount", "credit_amount"
    )
    eu = euSales.join(euCredits, quotas.columns, "left").withColumn(
        "actual_amount", (F.col("actual_amount") - F.coalesce(F.col("credit_amount"), F.lit(0))).cast("decimal(18,2)")
    ).drop("credit_amount")

    orders = order.select("region_code", "salesperson_key", "order_date_key", "net_order_amount_reporting")
    apac = _actualsInPeriod(quotas, orders, "order_date_key", "net_order_amount_reporting", "APAC")

    out = na.unionByName(eu).unionByName(apac)
    basis = F.lit(None).cast("string")
    for region, label in MEASURE_BASIS_BY_REGION.items():
        basis = F.when(F.col("region_code") == region, F.lit(label)).otherwise(basis)
    pct = F.when(F.col("quota_amount") == 0, F.lit(0)).otherwise(
        F.col("actual_amount") * 100 / F.col("quota_amount")
    ).cast("decimal(18,2)")
    out = out.withColumn("measure_basis_code", basis).withColumn("attainment_pct", pct)
    # LEGACY QUIRK: zero quota reports 0% attainment (not NULL) and band NOQUOTA.
    out = out.withColumn(
        "attainment_band_code",
        F.when(F.col("quota_amount") == 0, "NOQUOTA")
        .when(F.col("attainment_pct") >= 120, "OVER120")
        .when(F.col("attainment_pct") >= 100, "AT")
        .when(F.col("attainment_pct") >= 80, "NEAR")
        .otherwise("UNDER"),
    )
    return _refreshColumns(out, batchId), unresolved


# --------------------------------------------------------------------------- commission
def _commissionLines(
    sale: DataFrame, salesMargin: DataFrame, payment: DataFrame, region: str
) -> DataFrame:
    """Union of commission basis lines: one row per (basis, source line).

    Columns: region_code, salesperson_key, sales_channel_key, customer_key, country_code,
    invoice_number, basis_date, basis_amount (transaction currency), currency_code, commission_basis.
    """
    s = notReversal(sale).filter(F.col("region_code") == region)
    invoiceHeader = s.groupBy("invoice_number").agg(
        F.max("salesperson_key").alias("h_salesperson_key"),
        F.max("sales_channel_key").alias("h_sales_channel_key"),
        F.max("customer_key").alias("h_customer_key"),
        F.max("transaction_currency_code").alias("h_currency_code"),
        F.max("invoice_date_key").alias("h_invoice_date_key"),
    )
    country = F.col("bill_to_country_code") if "bill_to_country_code" in s.columns else F.lit(None).cast("string")
    netRevenue = s.select(
        "region_code", "salesperson_key", "sales_channel_key", "customer_key",
        country.alias("country_code"), "invoice_number",
        F.col("invoice_date_key").alias("basis_date"),
        # LEGACY QUIRK (EU): the VAT-exclusive net amount is the basis; NA's legacy gross-incl-tax
        # basis is NOT reproduced because the plan contract is NETREVENUE / INVOICEDMARGIN.
        F.col("net_amount").alias("basis_amount"),
        F.col("transaction_currency_code").alias("currency_code"),
        F.lit(BASIS_NET_REVENUE).alias("commission_basis"),
    )
    m = salesMargin.filter(F.col("region_code") == region).join(invoiceHeader, "invoice_number", "left")
    margin = m.select(
        "region_code",
        F.coalesce(F.col("salesperson_key"), F.col("h_salesperson_key")).alias("salesperson_key"),
        F.coalesce(F.col("sales_channel_key"), F.col("h_sales_channel_key")).alias("sales_channel_key"),
        F.coalesce(F.col("customer_key"), F.col("h_customer_key")).alias("customer_key"),
        F.lit(None).cast("string").alias("country_code"),
        "invoice_number",
        F.col("invoice_date_key").alias("basis_date"),
        F.col("gross_margin_amount").alias("basis_amount"),
        F.col("h_currency_code").alias("currency_code"),
        F.lit(BASIS_INVOICED_MARGIN).alias("commission_basis"),
    )
    # LEGACY QUIRK: cash-basis commission only counts cleared/paid allocations
    # (usp_RecalculateCommissionAccruals: APAC requires SettlementStatus = PAID;
    # EU cash-basis lines are released on PaymentStatusCode = CLEARED).
    p = payment.filter(
        (F.col("region_code") == region) & F.col("payment_status_code").isin("CLEARED", "PAID")
    ).join(invoiceHeader, "invoice_number", "inner")
    cash = p.select(
        "region_code",
        F.col("h_salesperson_key").alias("salesperson_key"),
        F.col("h_sales_channel_key").alias("sales_channel_key"),
        F.coalesce(F.col("customer_key"), F.col("h_customer_key")).alias("customer_key"),
        F.lit(None).cast("string").alias("country_code"),
        "invoice_number",
        F.col("payment_date_key").alias("basis_date"),
        F.col("allocated_amount").alias("basis_amount"),
        F.col("h_currency_code").alias("currency_code"),
        F.lit(BASIS_COLLECTED_CASH).alias("commission_basis"),
    )
    return netRevenue.unionByName(margin).unionByName(cash)


def _withCommissionable(channel: DataFrame) -> DataFrame:
    if "is_commissionable" in channel.columns:
        flag = F.coalesce(F.col("is_commissionable"), F.lit(True))
    else:
        # LEGACY QUIRK: Sales.SalesChannels has no IsCommissionable flag; the nightly
        # commission run treats ChannelStatus = 'PILOT' as non-commissionable.
        flag = F.coalesce(F.col("channel_status") != "PILOT", F.lit(True))
    return channel.select("sales_channel_key", flag.alias("is_commissionable"))


def _planRate(attainment: Column) -> Column:
    """Sales.ufn_CommissionRate: marginal band rate for the attainment percentage.

    # LEGACY QUIRK: the OLTP function returns 0 when attainment is NULL, but the
    # SSIS packages pay the plan base rate regardless of quota - the package wins
    # here, so NULL attainment falls into band 1.  Above band 3 the band-3 rate
    # applies (no fourth band, ever).
    """
    return (
        F.when(attainment.isNull() | (attainment <= F.col("Band1UpperPercent")), F.col("Band1RatePercent"))
        .when(attainment <= F.col("Band2UpperPercent"), F.col("Band2RatePercent"))
        .when(attainment <= F.col("Band3UpperPercent"), F.col("Band3RatePercent"))
        .otherwise(F.coalesce(F.col("Band3RatePercent"), F.col("Band2RatePercent"), F.col("Band1RatePercent")))
    )


def buildCommission(
    region: str,
    sale: DataFrame,
    salesMargin: DataFrame,
    payment: DataFrame,
    channel: DataFrame,
    salesperson: DataFrame,
    customer: DataFrame,
    plan: DataFrame,
    assignment: DataFrame,
    attainment: DataFrame,
    fxRate: DataFrame,
    fiscalCalendar: DataFrame,
    statutoryCap: DataFrame,
    batchId: int,
    teamSplitEnabled: bool = True,
) -> tuple[DataFrame, dict[str, DataFrame]]:
    """SLS_<region>_Load_Commission unified.  Returns (agg_commission rows, rejects by rule).

    Per line: basis amount -> plan currency (EU month-average EUR, APAC period-average
    plan currency, NA USD only) -> banded plan rate x split -> EU statutory cap ->
    NA house-account factor; then summed to salesperson x period x plan.
    """
    if region not in REGIONS:
        raise ValueError(f"unknown region {region!r}")
    planCurrency = LEGACY_REGION_CURRENCY[region]
    lines = _commissionLines(sale, salesMargin, payment, region)

    # Channel exclusion (PILOT) ---------------------------------------------------------------
    lines = lines.join(_withCommissionable(channel), "sales_channel_key", "left")
    # LEGACY QUIRK: PILOT channels are excluded from commission entirely.
    nonCommissionable = lines.filter(F.col("is_commissionable") == False)  # noqa: E712
    lines = lines.filter(F.col("is_commissionable") != False).drop("is_commissionable")  # noqa: E712

    # Plan assignment -----------------------------------------------------------------------
    sp = salesperson.select("salesperson_key", "wwi_person_id")
    lines = lines.join(sp, "salesperson_key", "left")
    assign = assignment.filter(F.col("CommissionPlanID").isNotNull()).select(
        F.col("PersonID").alias("wwi_person_id"), "CommissionPlanID",
        F.col("QuotaSharePercent").alias("quota_share_percent"),
        F.col("ValidFrom").alias("assign_valid_from"), F.col("ValidTo").alias("assign_valid_to"),
    )
    plans = plan.filter(F.col("RegionCode") == region).select(
        "CommissionPlanID", F.col("PlanCode").alias("plan_code"), F.col("CommissionBasis").alias("plan_basis"),
        "Band1UpperPercent", "Band1RatePercent", "Band2UpperPercent", "Band2RatePercent",
        "Band3UpperPercent", "Band3RatePercent", F.col("AcceleratorPercent").alias("accelerator_percent"),
        F.col("EffectiveFromDate").alias("plan_effective_from"), F.col("EffectiveToDate").alias("plan_effective_to"),
    )
    planned = lines.join(assign, "wwi_person_id", "left").join(plans, "CommissionPlanID", "left")
    inWindow = (
        (F.col("basis_date") >= F.col("assign_valid_from"))
        & ((F.col("assign_valid_to").isNull()) | (F.col("basis_date") < F.col("assign_valid_to")))
        & (F.col("basis_date") >= F.col("plan_effective_from"))
        & ((F.col("plan_effective_to").isNull()) | (F.col("basis_date") <= F.col("plan_effective_to")))
    )
    planned = planned.withColumn("has_plan", F.coalesce(F.col("plan_code").isNotNull() & inWindow, F.lit(False)))
    lineKey = ["commission_basis", "invoice_number", "basis_date", "basis_amount", "salesperson_key"]
    # Lines of a basis the rep's plan does not pay on are simply not commissionable
    # under that plan; only reps with no plan at all on the date are rejects.
    matched = planned.filter(F.col("has_plan") & (F.col("plan_basis") == F.col("commission_basis"))).drop("plan_basis", "has_plan")
    covered = planned.filter(F.col("has_plan")).select(*lineKey).distinct()
    noPlan = lines.join(covered, lineKey, "left_anti")

    # Period -------------------------------------------------------------------------------
    if region == "APAC":
        # LEGACY QUIRK: APAC commission periods come from the 4-4-5 calendar, so month-end
        # invoices regularly fall into the next commission period (IsPeriodBoundaryLine).
        cal = fiscalCalendar.filter(F.col("calendar_code") == NA_FISCAL_CALENDAR_CODE).select(
            "fiscal_year", "fiscal_period", "period_start", "period_end"
        )
        matched = matched.join(
            cal, (F.col("basis_date") >= F.col("period_start")) & (F.col("basis_date") <= F.col("period_end")), "left"
        )
        matched = (
            matched.withColumn("commission_period_label", naFiscalPeriodLabel(F.col("fiscal_year"), F.col("fiscal_period")))
            .withColumn("period_start_date", F.col("period_start"))
            .withColumn("period_boundary_line", F.month(F.col("basis_date")) != F.month(F.col("period_end")))
            .drop("fiscal_year", "fiscal_period", "period_start", "period_end")
        )
    else:
        matched = (
            matched.withColumn("commission_period_label", F.date_format(F.col("basis_date"), "yyyy-MM"))
            .withColumn("period_start_date", F.trunc(F.col("basis_date"), "month"))
            .withColumn("period_boundary_line", F.lit(False))
        )
    missingPeriod = matched.filter(F.col("commission_period_label").isNull())
    matched = matched.filter(F.col("commission_period_label").isNotNull())

    # Currency ----------------------------------------------------------------------------
    fx = monthlyAverageFxRate(fxRate, planCurrency)
    matched = matched.join(
        fx,
        (matched["currency_code"] == fx["fx_from_currency_code"])
        & (F.trunc(matched["basis_date"], "month") == fx["fx_rate_month"]),
        "left",
    ).drop("fx_from_currency_code", "fx_rate_month")
    sameCurrency = F.col("currency_code") == planCurrency
    if region == "NA":
        # LEGACY QUIRK: NA pays USD lines only; anything else never enters the run.
        nonUsd = matched.filter(~sameCurrency)
        matched = matched.filter(sameCurrency).withColumn("plan_currency_amount", F.col("basis_amount"))
        missingFx = matched.limit(0)
    elif region == "EU":
        # LEGACY QUIRK: missing / zero month-average rate leaves the amount unconverted (x1.0).
        matched = matched.withColumn(
            "plan_currency_amount",
            F.when(sameCurrency, F.col("basis_amount"))
            .when(F.col("monthly_average_rate").isNull() | (F.col("monthly_average_rate") == 0), F.col("basis_amount"))
            .otherwise(F.round(F.col("basis_amount") * F.col("monthly_average_rate"), 2)),
        )
        nonUsd = matched.limit(0)
        missingFx = matched.limit(0)
    else:
        # LEGACY QUIRK: APAC lines with no period-average rate are rejected (err.CommissionApacReject).
        missingFx = matched.filter(~sameCurrency & F.col("monthly_average_rate").isNull())
        matched = matched.filter(sameCurrency | F.col("monthly_average_rate").isNotNull()).withColumn(
            "plan_currency_amount",
            F.when(sameCurrency, F.col("basis_amount")).otherwise(F.round(F.col("basis_amount") * F.col("monthly_average_rate"), 2)),
        )
        nonUsd = matched.limit(0)

    # Attainment (drives the band) ---------------------------------------------------------------
    att = attainment.filter(F.col("region_code") == region).select(
        F.col("salesperson_key").alias("att_salesperson_key"),
        F.col("period_start_date").alias("att_start"), F.col("period_end_date").alias("att_end"),
        "attainment_pct", "quota_amount",
    )
    matched = matched.join(
        att,
        (F.col("salesperson_key") == F.col("att_salesperson_key"))
        & (F.col("basis_date") >= F.col("att_start")) & (F.col("basis_date") <= F.col("att_end")),
        "left",
    ).drop("att_salesperson_key", "att_start", "att_end")

    # Rate, split, cap, house account --------------------------------------------------------------
    split = F.col("quota_share_percent") / 100 if teamSplitEnabled else F.lit(1.0)
    caps = statutoryCap.select("plan_code", "statutory_cap_amount")
    matched = matched.join(caps, "plan_code", "left").withColumn(
        "statutory_cap_amount", F.coalesce(F.col("statutory_cap_amount"), F.lit(0)).cast("decimal(18,2)")
    )
    if "is_house_account" in customer.columns:
        house = customer.filter(F.col("is_current_row") == True).select("customer_key", "is_house_account")  # noqa: E712
        matched = matched.join(house, "customer_key", "left")
    else:
        matched = matched.withColumn("is_house_account", F.lit(None).cast("boolean"))
    matched = (
        matched.withColumn("commission_rate_pct", _planRate(F.col("attainment_pct")).cast("decimal(5,2)"))
        .withColumn("split_factor", F.coalesce(split, F.lit(1.0)).cast("decimal(9,4)"))
        .withColumn(
            "raw_commission_amount", F.round(F.col("plan_currency_amount") * F.col("commission_rate_pct") / 100 * F.col("split_factor"), 2)
        )
    )
    if region == "EU":
        # LEGACY QUIRK (EU statutory cap): applied per LINE, not per month or per rep:
        #   StatutoryCapAmount > 0 && raw > StatutoryCapAmount ? StatutoryCapAmount : raw
        # The plan's three bands (Band1..3) only ladder the RATE by quota attainment; the
        # package has no separate three-band cap expression.  See PR "Open questions".
        capped = (F.col("statutory_cap_amount") > 0) & (F.col("raw_commission_amount") > F.col("statutory_cap_amount"))
        matched = matched.withColumn("capped_flag", capped).withColumn(
            "line_commission_amount", F.when(capped, F.col("statutory_cap_amount")).otherwise(F.col("raw_commission_amount"))
        )
    else:
        matched = matched.withColumn("capped_flag", F.lit(False)).withColumn("line_commission_amount", F.col("raw_commission_amount"))
    if region == "NA":
        # LEGACY QUIRK: house accounts are paid HouseAccountRatePercent (50%) of the plan rate;
        # an unknown customer (NULL flag) pays in full.
        factor = F.when(F.coalesce(F.col("is_house_account"), F.lit(False)), F.lit(HOUSE_ACCOUNT_RATE_PERCENT / 100)).otherwise(F.lit(1.0))
        matched = matched.withColumn("line_commission_amount", F.round(F.col("line_commission_amount") * factor, 2))

    grp = matched.groupBy(
        "region_code", "salesperson_key", "wwi_person_id", "CommissionPlanID", "plan_code", "commission_basis",
        "commission_period_label", "period_start_date",
    ).agg(
        F.count(F.lit(1)).alias("line_count"),
        F.sum("basis_amount").cast("decimal(18,2)").alias("basis_amount_local"),
        F.sum("plan_currency_amount").cast("decimal(18,2)").alias("basis_amount_plan_currency"),
        F.max("quota_amount").alias("quota_amount"),
        F.max("attainment_pct").alias("attainment_pct"),
        F.max("commission_rate_pct").alias("commission_rate_pct"),
        F.max("split_factor").alias("split_factor"),
        F.max("accelerator_percent").alias("accelerator_percent"),
        F.sum("raw_commission_amount").cast("decimal(18,2)").alias("raw_commission_amount"),
        F.max("statutory_cap_amount").alias("statutory_cap_amount"),
        F.sum(F.when(F.col("capped_flag"), 1).otherwise(0)).alias("capped_line_count"),
        F.sum("line_commission_amount").cast("decimal(18,2)").alias("base_commission_amount"),
        F.sum(F.when(F.col("period_boundary_line"), 1).otherwise(0)).alias("period_boundary_line_count"),
    )
    if region == "NA":
        # LEGACY QUIRK: NA accelerator pays AcceleratorPercent on the period's commissionable
        # amount above quota (the plan "threshold" is the rep's quota for the period).
        excess = F.greatest(F.col("basis_amount_plan_currency") - F.col("quota_amount"), F.lit(0))
        accel = F.when(
            F.col("accelerator_percent").isNotNull() & F.col("quota_amount").isNotNull() & (F.col("quota_amount") > 0),
            F.round(excess * F.col("accelerator_percent") / 100, 2),
        ).otherwise(F.lit(0))
    else:
        accel = F.lit(0)
    grp = grp.withColumn("accelerator_commission_amount", accel.cast("decimal(18,2)")).withColumn(
        "commission_amount", (F.col("base_commission_amount") + F.col("accelerator_commission_amount")).cast("decimal(18,2)")
    ).withColumn("plan_currency_code", F.lit(planCurrency)).withColumnRenamed("CommissionPlanID", "commission_plan_id")
    out = _refreshColumns(grp, batchId).select(
        "region_code", "salesperson_key", "wwi_person_id", "commission_plan_id", "plan_code", "commission_basis",
        "commission_period_label", "period_start_date", "plan_currency_code", "line_count", "basis_amount_local",
        "basis_amount_plan_currency", "quota_amount", "attainment_pct", "commission_rate_pct", "split_factor",
        "raw_commission_amount", "statutory_cap_amount", "capped_line_count", "base_commission_amount",
        "accelerator_commission_amount", "commission_amount", "period_boundary_line_count",
        "refresh_batch_id", "refreshed_datetime",
    )
    rejects = {
        "COMM_NON_COMMISSIONABLE_CHANNEL": nonCommissionable.drop("is_commissionable"),
        "COMM_NO_PLAN": noPlan,
        "COMM_NO_FISCAL_PERIOD": missingPeriod,
        "COMM_NA_NON_USD": nonUsd,
        "COMM_MISSING_FX": missingFx,
    }
    return out, rejects


REJECT_REASONS: dict[str, str] = {
    "COMM_NON_COMMISSIONABLE_CHANNEL": "Channel is not commissionable (PILOT) - excluded from commission",
    "COMM_NO_PLAN": "Salesperson has no commission plan for this basis/date (legacy UnplannedRepCount)",
    "COMM_NO_FISCAL_PERIOD": "No 4-4-5 fiscal period covers the basis date (legacy MissingCalendarCount)",
    "COMM_NA_NON_USD": "NA commission run pays USD lines only",
    "COMM_MISSING_FX": "No period-average FX rate to plan currency (legacy err.CommissionApacReject)",
    "QUOTA_UNRESOLVED_KEY": "Quota salesperson/territory not found in silver dimensions",
}


def _writeRejects(spark: SparkSession, cfg: PipelineConfig, sourceTable: str, rejects: dict[str, DataFrame]) -> None:
    for rule, df in rejects.items():
        quarantine(spark, cfg, df, rule, sourceTable, F.lit(True), REJECT_REASONS[rule])


def run(spark: SparkSession, cfg: PipelineConfig, outDir: str | None = None, asOfDate: dt.date | None = None) -> None:
    sale = readTable(spark, cfg, "gold", "fact_sale")
    order = readTable(spark, cfg, "gold", "fact_order")
    payment = readTable(spark, cfg, "gold", "fact_payment")
    creditNote = readTable(spark, cfg, "gold", "fact_credit_note")
    salesMargin = readTable(spark, cfg, "gold", "fact_sales_margin")
    salesperson = conformDimSalesperson(readTable(spark, cfg, "silver", "dim_salesperson"))
    territory = readTable(spark, cfg, "silver", "dim_sales_territory")
    channel = readTable(spark, cfg, "silver", "dim_sales_channel")
    customer = readDimCustomer(spark, cfg)
    fiscalCalendar = readTable(spark, cfg, "silver", "dim_fiscal_calendar")
    fxRate = readTable(spark, cfg, "silver", "ref_fx_rate")
    quota = readTable(spark, cfg, "bronze", "sqlserver_sales_sales_quotas")
    plan = readTable(spark, cfg, "bronze", "sqlserver_sales_commission_plans")
    assignment = readTable(spark, cfg, "bronze", "sqlserver_application_sales_team_members")
    statutoryCap = readOrEmpty(spark, cfg, "silver", "ref_commission_statutory_cap", STATUTORY_CAP_SCHEMA)

    attainment, unresolved = buildQuotaAttainment(quota, salesperson, territory, sale, creditNote, order, cfg.batchId)
    quarantine(
        spark, cfg, unresolved, "QUOTA_UNRESOLVED_KEY", "sqlserver_sales_sales_quotas", F.lit(True), REJECT_REASONS["QUOTA_UNRESOLVED_KEY"]
    )
    overwriteTable(attainment, cfg.fqn("gold", AGG_QUOTA_ATTAINMENT), ["region_code"])
    attainment = spark.table(cfg.fqn("gold", AGG_QUOTA_ATTAINMENT))

    commission = None
    for region in REGIONS:
        regional, rejects = buildCommission(
            region, sale, salesMargin, payment, channel, salesperson, customer, plan, assignment,
            attainment, fxRate, fiscalCalendar, statutoryCap, cfg.batchId,
        )
        _writeRejects(spark, cfg, f"fact_sale/{region}", rejects)
        commission = regional if commission is None else commission.unionByName(regional)
    if commission is not None:
        overwriteTable(commission, cfg.fqn("gold", AGG_COMMISSION), ["region_code"])

    if outDir is not None:
        for region in REGIONS:
            partner_feed.exportPartnerFeed(spark, cfg, outDir, region, asOfDate=asOfDate)
