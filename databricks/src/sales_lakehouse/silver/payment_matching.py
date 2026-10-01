"""Payment-to-invoice matching - port of ``work.usp_MatchPaymentsToInvoices``.

Legacy: sqlserver/staging/procedures/work.usp_MatchPaymentsToInvoices.sql. The
legacy matcher ran against AP (supplier) invoices; the sales lakehouse applies
the same three-pass precedence to customer receipts against ``silver.sale``:

  pass 1 ``REMIT_REF``  (lines 67-95)   - the remittance / explicit allocation
                                          names the invoice; confidence 100.
  pass 2 ``EXACT_AMT``  (lines 97-138)  - exactly one open invoice of the same
                                          customer at the payment amount; 90.
  pass 3 ``RESIDUAL``   (lines 140-211) - oldest open, non-held invoice first,
                                          one payment at a time; 65.
  ``UNAPPLIED``          (lines 213-227) - cash left above the regional tolerance
                                          is on-account, reason ``NO_OPEN_INVOICE``.

Regional tolerance (lines 166-171): EU 0.01, APAC 0.5% of the payment, else 0.02.
Payment status feedback (lines 236-260): UNMATCHED / PARTIAL / MATCHED_REF /
MATCHED_AMT / MATCHED_RESIDUAL.

Input contracts (all amounts decimal(19,4) in transaction currency):
  payments:   payment_business_key, customer_business_key, payment_date,
              payment_amount_local, region_code, is_void
  invoices:   sale_business_key, customer_business_key, invoice_date,
              open_amount_local, is_on_hold
  explicit:   payment_business_key, sale_business_key, allocated_amount_local
              (Sales.PaymentAllocations INVOICE rows and the payment row's own
              InvoiceID reference)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

MONEY = DecimalType(19, 4)

METHOD_REMIT_REF = "REMIT_REF"
METHOD_EXACT_AMT = "EXACT_AMT"
METHOD_RESIDUAL = "RESIDUAL"
METHOD_UNAPPLIED = "UNAPPLIED"
UNMATCHED_REASON_NO_OPEN_INVOICE = "NO_OPEN_INVOICE"

ALLOCATION_COLUMNS = [
    "payment_business_key",
    "sale_business_key",
    "customer_business_key",
    "match_pass_number",
    "allocation_method_code",
    "allocated_amount_local",
    "residual_amount_local",
    "within_tolerance_flag",
    "match_confidence",
    "is_final_allocation",
    "unmatched_reason_code",
]


def regionalTolerance(regionCol, amountCol):
    # LEGACY QUIRK: APAC tolerance is proportional to the payment, the others are
    # fixed cents (lines 166-171); NULL region behaves as NA (line 146).
    region = F.coalesce(regionCol, F.lit("NA"))
    return (
        F.when(region == "EU", F.lit("0.01"))
        .when(region == "APAC", amountCol * F.lit("0.005"))
        .otherwise(F.lit("0.02"))
        .cast(MONEY)
    )


def _money(col):
    return col.cast(MONEY)


def _passOneRemittance(payments: DataFrame, invoices: DataFrame, explicit: DataFrame) -> DataFrame:
    inv = invoices.select(
        "sale_business_key",
        F.col("customer_business_key").alias("_inv_customer"),
        F.col("open_amount_local").alias("_open"),
    )
    refs = explicit.select("payment_business_key", "sale_business_key").distinct()
    joined = payments.join(refs, "payment_business_key").join(inv, "sale_business_key")
    # Lines 89-93: the reference must name an invoice of the same counter-party.
    joined = joined.filter(F.col("customer_business_key") == F.col("_inv_customer"))
    applied = F.when(F.col("payment_amount_local") > F.col("_open"), F.col("_open")).otherwise(
        F.col("payment_amount_local")
    )
    # LEGACY QUIRK: pass 1 applies min(payment, open) per referenced invoice and
    # does not deplete the payment across several references (lines 77-87).
    return joined.select(
        "payment_business_key",
        "sale_business_key",
        "customer_business_key",
        F.lit(1).alias("match_pass_number"),
        F.lit(METHOD_REMIT_REF).alias("allocation_method_code"),
        _money(applied).alias("allocated_amount_local"),
        _money(F.col("_open") - F.col("payment_amount_local")).alias("residual_amount_local"),
        (F.abs(F.col("_open") - F.col("payment_amount_local")) <= F.lit("0.02").cast(MONEY)).alias(
            "within_tolerance_flag"
        ),
        F.lit(100.0).cast("decimal(5,2)").alias("match_confidence"),
        F.lit(True).alias("is_final_allocation"),
        F.lit(None).cast("string").alias("unmatched_reason_code"),
    )


def _passTwoExactAmount(payments: DataFrame, invoices: DataFrame) -> DataFrame:
    inv = invoices.filter(F.col("open_amount_local") > 0).select(
        "sale_business_key", "customer_business_key", F.col("open_amount_local").alias("_open")
    )
    candidates = payments.join(inv, "customer_business_key").filter(F.col("_open") == F.col("payment_amount_local"))
    counted = candidates.withColumn(
        "_candidate_count", F.count(F.lit(1)).over(Window.partitionBy("payment_business_key"))
    )
    # Legacy header (line 97) documents "single open invoice"; the T-SQL uses
    # TOP (2) which would double-allocate when two invoices tie - we follow the
    # documented intent and let ties fall through to the residual pass.
    single = counted.filter(F.col("_candidate_count") == 1)
    return single.select(
        "payment_business_key",
        "sale_business_key",
        "customer_business_key",
        F.lit(2).alias("match_pass_number"),
        F.lit(METHOD_EXACT_AMT).alias("allocation_method_code"),
        _money(F.col("payment_amount_local")).alias("allocated_amount_local"),
        _money(F.lit(0)).alias("residual_amount_local"),
        F.lit(True).alias("within_tolerance_flag"),
        F.lit(90.0).cast("decimal(5,2)").alias("match_confidence"),
        F.lit(True).alias("is_final_allocation"),
        F.lit(None).cast("string").alias("unmatched_reason_code"),
    )


def _passThreeResidual(payments: DataFrame, invoices: DataFrame) -> DataFrame:
    """Oldest-invoice-first FIFO over the customer's open, non-held invoices.

    The legacy cursor walks one payment at a time (ordered by business key,
    line 158) over invoices ordered by ``InvoiceDate, BusinessKey`` (line 180).
    The equivalent set-based form is the overlap of cumulative payment and
    cumulative invoice intervals per customer.
    """
    payWindow = Window.partitionBy("customer_business_key").orderBy("payment_business_key")
    pays = (
        payments.filter(F.col("payment_amount_local") > 0)
        .withColumn("_tolerance", regionalTolerance(F.col("region_code"), F.col("payment_amount_local")))
        .withColumn("_pay_hi", F.sum("payment_amount_local").over(payWindow).cast(MONEY))
        .withColumn("_pay_lo", (F.col("_pay_hi") - F.col("payment_amount_local")).cast(MONEY))
    )
    invWindow = Window.partitionBy("customer_business_key").orderBy("invoice_date", "sale_business_key")
    invs = (
        invoices.filter((F.col("open_amount_local") > 0) & ~F.coalesce(F.col("is_on_hold"), F.lit(False)))
        .withColumn("_inv_hi", F.sum("open_amount_local").over(invWindow).cast(MONEY))
        .withColumn("_inv_lo", (F.col("_inv_hi") - F.col("open_amount_local")).cast(MONEY))
        .select("customer_business_key", "sale_business_key", "open_amount_local", "_inv_lo", "_inv_hi")
    )
    overlap = F.least(F.col("_pay_hi"), F.col("_inv_hi")) - F.greatest(F.col("_pay_lo"), F.col("_inv_lo"))
    matched = (
        pays.join(invs, "customer_business_key")
        .withColumn("_applied", overlap.cast(MONEY))
        .filter(F.col("_applied") > 0)
    )
    invoiceRemaining = (F.col("_inv_hi") - F.least(F.col("_pay_hi"), F.col("_inv_hi"))).cast(MONEY)
    paymentRemaining = (F.col("_pay_hi") - F.least(F.col("_pay_hi"), F.col("_inv_hi"))).cast(MONEY)
    residualRows = matched.select(
        "payment_business_key",
        "sale_business_key",
        "customer_business_key",
        F.lit(3).alias("match_pass_number"),
        F.lit(METHOD_RESIDUAL).alias("allocation_method_code"),
        F.col("_applied").alias("allocated_amount_local"),
        invoiceRemaining.alias("residual_amount_local"),
        (F.abs(invoiceRemaining) <= F.col("_tolerance")).alias("within_tolerance_flag"),
        F.lit(65.0).cast("decimal(5,2)").alias("match_confidence"),
        (paymentRemaining <= F.col("_tolerance")).alias("is_final_allocation"),
        F.lit(None).cast("string").alias("unmatched_reason_code"),
    )

    # Lines 213-227: whatever is left above tolerance is unapplied cash.
    invoiceTotals = invs.groupBy("customer_business_key").agg(F.max("_inv_hi").alias("_inv_total"))
    leftovers = (
        pays.join(invoiceTotals, "customer_business_key", "left")
        .withColumn("_inv_total", F.coalesce(F.col("_inv_total"), F.lit(0)).cast(MONEY))
        .withColumn("_remaining", (F.col("_pay_hi") - F.least(F.col("_pay_hi"), F.col("_inv_total"))).cast(MONEY))
        .filter(F.col("_remaining") > F.col("_tolerance"))
    )
    unappliedRows = leftovers.select(
        "payment_business_key",
        F.lit(None).cast("string").alias("sale_business_key"),
        "customer_business_key",
        F.lit(3).alias("match_pass_number"),
        F.lit(METHOD_UNAPPLIED).alias("allocation_method_code"),
        _money(F.lit(0)).alias("allocated_amount_local"),
        F.col("_remaining").alias("residual_amount_local"),
        F.lit(False).alias("within_tolerance_flag"),
        F.lit(0.0).cast("decimal(5,2)").alias("match_confidence"),
        F.lit(True).alias("is_final_allocation"),
        F.lit(UNMATCHED_REASON_NO_OPEN_INVOICE).alias("unmatched_reason_code"),
    )
    return residualRows.unionByName(unappliedRows)


def matchPaymentsToInvoices(payments: DataFrame, invoices: DataFrame, explicitAllocations: DataFrame) -> DataFrame:
    """Return allocation rows (see ``ALLOCATION_COLUMNS``) for the given payments."""
    live = payments.filter(~F.coalesce(F.col("is_void"), F.lit(False)))  # line 95: VoidDate IS NULL
    passOne = _passOneRemittance(live, invoices, explicitAllocations)

    matchedKeys = passOne.select("payment_business_key").distinct()
    remainingAfterOne = live.join(matchedKeys, "payment_business_key", "left_anti")
    openAfterOne = _depleteInvoices(invoices, passOne)
    passTwo = _passTwoExactAmount(remainingAfterOne, openAfterOne)

    matchedKeys = passTwo.select("payment_business_key").distinct()
    remainingAfterTwo = remainingAfterOne.join(matchedKeys, "payment_business_key", "left_anti")
    openAfterTwo = _depleteInvoices(openAfterOne, passTwo)
    passThree = _passThreeResidual(remainingAfterTwo, openAfterTwo)

    return passOne.unionByName(passTwo).unionByName(passThree).select(*ALLOCATION_COLUMNS)


def _depleteInvoices(invoices: DataFrame, allocations: DataFrame) -> DataFrame:
    applied = allocations.groupBy("sale_business_key").agg(F.sum("allocated_amount_local").alias("_applied_total"))
    return (
        invoices.join(applied, "sale_business_key", "left")
        .withColumn(
            "open_amount_local",
            (F.col("open_amount_local") - F.coalesce(F.col("_applied_total"), F.lit(0))).cast(MONEY),
        )
        .drop("_applied_total")
    )


def summarisePayments(payments: DataFrame, allocations: DataFrame) -> DataFrame:
    """Lines 236-260 plus the lakehouse over/under-payment flags.

    Returns one row per payment: allocated_amount_local, unallocated_amount_local,
    applied_invoice_count, match_status_code, is_overpayment, is_underpayment.
    """
    agg = allocations.groupBy("payment_business_key").agg(
        F.count(F.lit(1)).alias("_rows"),
        F.sum(F.when(F.col("sale_business_key").isNotNull(), 1).otherwise(0)).alias("applied_invoice_count"),
        F.sum("allocated_amount_local").cast(MONEY).alias("allocated_amount_local"),
        F.sum(F.when(F.col("allocation_method_code") == METHOD_UNAPPLIED, F.col("residual_amount_local")).otherwise(0))
        .cast(MONEY)
        .alias("_unapplied"),
        F.max(F.when(F.col("allocation_method_code") == METHOD_UNAPPLIED, 1).otherwise(0)).alias("_has_unapplied"),
        F.min("match_pass_number").alias("_min_pass"),
        F.max(
            F.when(
                F.col("sale_business_key").isNotNull() & (F.col("residual_amount_local") > 0) & ~F.col("within_tolerance_flag"),
                1,
            ).otherwise(0)
        ).alias("_short_paid"),
    )
    out = payments.join(agg, "payment_business_key", "left")
    status = (
        F.when(F.col("_rows").isNull() | (F.col("_rows") == 0), F.lit("UNMATCHED"))
        .when(F.col("_has_unapplied") == 1, F.lit("PARTIAL"))
        .when(F.col("_min_pass") == 1, F.lit("MATCHED_REF"))
        .when(F.col("_min_pass") == 2, F.lit("MATCHED_AMT"))
        .otherwise(F.lit("MATCHED_RESIDUAL"))
    )
    allocated = F.coalesce(F.col("allocated_amount_local"), F.lit(0)).cast(MONEY)
    return (
        out.withColumn("allocated_amount_local", allocated)
        .withColumn("unallocated_amount_local", (F.col("payment_amount_local") - allocated).cast(MONEY))
        .withColumn("applied_invoice_count", F.coalesce(F.col("applied_invoice_count"), F.lit(0)).cast("int"))
        .withColumn("match_status_code", status)
        .withColumn("is_overpayment", F.col("unallocated_amount_local") > 0)
        .withColumn("is_underpayment", F.coalesce(F.col("_short_paid"), F.lit(0)) == 1)
        .drop("_rows", "_unapplied", "_has_unapplied", "_min_pass", "_short_paid")
    )
