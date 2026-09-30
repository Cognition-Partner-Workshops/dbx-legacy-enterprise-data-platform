"""Payment-to-invoice application (port of work.usp_MatchPaymentsToInvoices).

Three passes, each consuming what the previous one left open:
  1. REMIT_REF  - the remittance names the invoice (we use the ERP's own
                  AP_PAYMENT_APPLY rows as the remittance evidence)   confidence 100
  2. EXACT_AMT  - exactly one open supplier invoice equals the remaining
                  payment (gross, or net of an eligible settlement discount) confidence 90
  3. RESIDUAL   - oldest-first FIFO allocation across the supplier's remaining
                  open invoices                                         confidence 65
Residual cash left after pass 3 is written off when inside the regional tolerance
(NA 0.02, EU 0.01, APAC 0.5% of the remaining payment) and rejected as
UNAPPLIED_CASH otherwise.  Only non-void payments and open, unheld invoices take part.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from finance.rules import residualTolerance

MATCH_COLS = [
    "payment_id",
    "payment_number",
    "supplier_id",
    "invoice_id",
    "invoice_number",
    "match_type_code",
    "match_pass",
    "matched_amount",
    "discount_amount",
    "confidence_score",
    "region_code",
]


def _remaining(payments: DataFrame, matches: DataFrame) -> DataFrame:
    applied = matches.groupBy("payment_id").agg(F.sum("matched_amount").alias("_applied"))
    return (
        payments.join(applied, "payment_id", "left")
        .withColumn("remaining_amount", F.col("payment_amount") - F.coalesce(F.col("_applied"), F.lit(0.0)))
        .drop("_applied")
        .where(F.col("remaining_amount") > 0)
    )


def _openInvoices(invoices: DataFrame, matches: DataFrame) -> DataFrame:
    applied = matches.groupBy("invoice_id").agg(F.sum("matched_amount").alias("_applied"))
    return (
        invoices.join(applied, "invoice_id", "left")
        .withColumn("open_remaining", F.col("open_amount") - F.coalesce(F.col("_applied"), F.lit(0.0)))
        .drop("_applied")
        .where(F.col("open_remaining") > 0)
    )


def _emptyMatches(payments: DataFrame) -> DataFrame:
    return payments.sparkSession.createDataFrame(
        [],
        "payment_id decimal(12,0), payment_number string, supplier_id decimal(12,0), invoice_id decimal(12,0), "
        "invoice_number string, match_type_code string, match_pass int, matched_amount decimal(18,5), "
        "discount_amount decimal(18,5), confidence_score int, region_code string",
    )


def _shape(df: DataFrame, matchType: str, matchPass: int, confidence: int) -> DataFrame:
    return df.select(
        F.col("payment_id").cast("decimal(12,0)"),
        "payment_number",
        F.col("supplier_id").cast("decimal(12,0)"),
        F.col("invoice_id").cast("decimal(12,0)"),
        "invoice_number",
        F.lit(matchType).alias("match_type_code"),
        F.lit(matchPass).alias("match_pass"),
        F.col("matched_amount").cast("decimal(18,5)"),
        F.coalesce(F.col("discount_amount"), F.lit(0.0)).cast("decimal(18,5)").alias("discount_amount"),
        F.lit(confidence).alias("confidence_score"),
        "region_code",
    )


def passRemittance(payments: DataFrame, invoices: DataFrame, remittances: DataFrame) -> DataFrame:
    """Pass 1: remittance (apply rows) names the invoice; allocate min(remit, open, remaining)."""
    p = payments.select(
        "payment_id", "payment_number", "supplier_id", "region_code", "payment_date", "remaining_amount"
    )
    i = invoices.select(
        "invoice_id", "invoice_number", F.col("supplier_id").alias("_inv_supplier"), "open_remaining"
    )
    pairs = (
        remittances.select("payment_id", "invoice_id", "apply_seq", "remit_amount")
        .join(p, "payment_id")
        .join(i, "invoice_id")
        .where(F.col("_inv_supplier") == F.col("supplier_id"))
    )
    wPay = Window.partitionBy("payment_id").orderBy("apply_seq", "invoice_id")
    wInv = Window.partitionBy("invoice_id").orderBy("payment_date", "payment_id")
    alloc = (
        pairs.withColumn(
            "_want",
            F.least(F.coalesce(F.col("remit_amount"), F.col("open_remaining")), F.col("open_remaining")),
        )
        .withColumn(
            "_pay_before",
            F.coalesce(F.sum("_want").over(wPay.rowsBetween(Window.unboundedPreceding, -1)), F.lit(0.0)),
        )
        .withColumn(
            "_pay_alloc",
            F.greatest(F.lit(0.0), F.least(F.col("_want"), F.col("remaining_amount") - F.col("_pay_before"))),
        )
        .withColumn(
            "_inv_before",
            F.coalesce(F.sum("_pay_alloc").over(wInv.rowsBetween(Window.unboundedPreceding, -1)), F.lit(0.0)),
        )
        .withColumn(
            "matched_amount",
            F.greatest(
                F.lit(0.0), F.least(F.col("_pay_alloc"), F.col("open_remaining") - F.col("_inv_before"))
            ),
        )
        .where(F.col("matched_amount") > 0)
        .withColumn("discount_amount", F.lit(0.0))
    )
    return _shape(alloc, "REMIT_REF", 1, 100)


def passExactAmount(payments: DataFrame, invoices: DataFrame) -> DataFrame:
    """Pass 2: exactly one open supplier invoice equals the remaining payment (gross or discounted)."""
    p = payments.select(
        "payment_id", "payment_number", "supplier_id", "region_code", "payment_date", "remaining_amount"
    )
    i = invoices.select(
        "invoice_id",
        "invoice_number",
        "supplier_id",
        "open_remaining",
        "discount_percent",
        "discount_due_date",
    )
    cand = (
        p.join(i, "supplier_id")
        .withColumn(
            "_discount",
            F.when(
                F.col("discount_due_date").isNotNull()
                & (F.col("payment_date") <= F.col("discount_due_date"))
                & (F.coalesce(F.col("discount_percent"), F.lit(0.0)) > 0),
                F.round(F.col("open_remaining") * F.col("discount_percent") / 100, 2),
            ).otherwise(F.lit(0.0)),
        )
        .where(
            (F.abs(F.col("open_remaining") - F.col("remaining_amount")) < 0.005)
            | (F.abs(F.col("open_remaining") - F.col("_discount") - F.col("remaining_amount")) < 0.005)
        )
    )
    single = cand.withColumn("_n", F.count("*").over(Window.partitionBy("payment_id"))).where(
        F.col("_n") == 1
    )
    # an invoice may only be taken by one payment in this pass
    firstPay = single.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("invoice_id").orderBy("payment_date", "payment_id"))
    ).where(F.col("_rn") == 1)
    alloc = firstPay.withColumn(
        "discount_amount",
        F.when(F.abs(F.col("open_remaining") - F.col("remaining_amount")) < 0.005, F.lit(0.0)).otherwise(
            F.col("_discount")
        ),
    ).withColumn("matched_amount", F.col("open_remaining") - F.col("discount_amount"))
    return _shape(alloc, "EXACT_AMT", 2, 90)


def passResidual(payments: DataFrame, invoices: DataFrame) -> DataFrame:
    """Pass 3: FIFO (oldest invoice first, oldest payment first) interval allocation per supplier."""
    wPay = Window.partitionBy("supplier_id").orderBy("payment_date", "payment_id")
    wInv = Window.partitionBy("supplier_id").orderBy("due_date", "invoice_date", "invoice_id")
    p = payments.select(
        "payment_id", "payment_number", "supplier_id", "region_code", "payment_date", "remaining_amount"
    ).withColumn(
        "_p_start",
        F.coalesce(
            F.sum("remaining_amount").over(wPay.rowsBetween(Window.unboundedPreceding, -1)), F.lit(0.0)
        ),
    )
    i = invoices.select(
        "invoice_id", "invoice_number", "supplier_id", "open_remaining", "due_date", "invoice_date"
    ).withColumn(
        "_i_start",
        F.coalesce(F.sum("open_remaining").over(wInv.rowsBetween(Window.unboundedPreceding, -1)), F.lit(0.0)),
    )
    alloc = (
        p.join(i, "supplier_id")
        .withColumn(
            "matched_amount",
            F.least(
                F.col("_p_start") + F.col("remaining_amount"), F.col("_i_start") + F.col("open_remaining")
            )
            - F.greatest(F.col("_p_start"), F.col("_i_start")),
        )
        .where(F.col("matched_amount") > 0)
        .withColumn("discount_amount", F.lit(0.0))
    )
    return _shape(alloc, "RESIDUAL", 3, 65)


def matchPayments(
    payments: DataFrame, invoices: DataFrame, remittances: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Run the three passes.  Returns (matches, unapplied) where `unapplied` carries the
    remaining cash per payment with `within_tolerance` and `reject_reason_code` columns.

    payments:    payment_id, payment_number, supplier_id, region_code, payment_date, payment_amount, payment_status_code
    invoices:    invoice_id, invoice_number, supplier_id, open_amount, invoice_date, due_date,
                 discount_percent, discount_due_date, is_on_hold, invoice_status_code
    remittances: payment_id, invoice_id, apply_seq, remit_amount
    """
    eligiblePayments = payments.where(
        (F.col("payment_status_code") != "VOID") & (F.col("payment_amount") > 0)
    )
    eligibleInvoices = invoices.where(
        (F.col("open_amount") > 0)
        & (F.coalesce(F.col("is_on_hold"), F.lit(False)) == F.lit(False))
        & (~F.col("invoice_status_code").isin("CANC", "VOID", "DRAFT"))
    )
    matches = _emptyMatches(payments)

    m1 = passRemittance(
        _remaining(eligiblePayments, matches), _openInvoices(eligibleInvoices, matches), remittances
    )
    matches = matches.unionByName(m1)
    m2 = passExactAmount(_remaining(eligiblePayments, matches), _openInvoices(eligibleInvoices, matches))
    matches = matches.unionByName(m2)
    m3 = passResidual(_remaining(eligiblePayments, matches), _openInvoices(eligibleInvoices, matches))
    matches = matches.unionByName(m3)

    leftover = _remaining(eligiblePayments, matches)
    unapplied = (
        leftover.withColumn(
            "tolerance_amount", residualTolerance(F.col("region_code"), F.col("remaining_amount"))
        )
        .withColumn("within_tolerance", F.col("remaining_amount") <= F.col("tolerance_amount"))
        .withColumn(
            "reject_reason_code",
            F.when(F.col("within_tolerance"), F.lit("TOLERANCE_WRITEOFF")).otherwise(F.lit("UNAPPLIED_CASH")),
        )
    )
    return matches, unapplied
