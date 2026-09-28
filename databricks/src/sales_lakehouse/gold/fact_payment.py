"""``fact_payment`` - payment-allocation grain, incremental MERGE on
``payment_allocation_business_key`` with in-place restatement.

Replaces Integration.usp_LoadFactPayment and SSIS FACT_Load_Payment.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.quality import quarantine
from sales_lakehouse.gold import rules_adapter
from sales_lakehouse.gold.fact_support import (
    UNKNOWN_KEY,
    daysBetween,
    lookupScd2Key,
    mergeOnKeys,
    money,
    pickColumn,
    readOptional,
    surrogateKey,
    withLoadMetadata,
)

TABLE = "fact_payment"
KEY_COLS = ["payment_allocation_business_key"]
SOURCE_TABLE = "silver.payment"
TOLERANCE = 0.01
UNALLOCATED_SUFFIX = "|UNALLOC"
UNALLOCATED_STATUS = "UNALLOC"


def _receipts(payments: DataFrame) -> DataFrame:
    return payments.select(
        F.col("payment_business_key"),
        pickColumn(payments, ["payment_reference", "receipt_number"], "string").alias("receipt_number"),
        F.col("customer_business_key"),
        F.coalesce(
            pickColumn(payments, ["payment_date"], "date"),
            pickColumn(payments, ["received_when_utc", "received_when"], "timestamp").cast("date"),
        ).alias("payment_date"),
        pickColumn(payments, ["value_date"], "date").alias("value_date"),
        pickColumn(payments, ["payment_method_code"], "string").alias("payment_method_code"),
        pickColumn(payments, ["transaction_currency_code", "currency_code"], "string").alias(
            "transaction_currency_code"
        ),
        money(pickColumn(payments, ["payment_amount", "received_amount"], "decimal(19,4)")).alias("payment_amount"),
        pickColumn(payments, ["exchange_rate_to_usd", "transaction_fx_rate"], "decimal(19,8)").alias("_bank_fx_rate"),
        money(F.coalesce(pickColumn(payments, ["bank_charge_amount"], "decimal(19,4)"), F.lit(0))).alias(
            "bank_charge_amount"
        ),
        money(F.coalesce(pickColumn(payments, ["withholding_tax_amount"], "decimal(19,4)"), F.lit(0))).alias(
            "withholding_tax_amount"
        ),
        money(F.coalesce(pickColumn(payments, ["write_off_amount"], "decimal(19,4)"), F.lit(0))).alias(
            "write_off_amount"
        ),
        pickColumn(payments, ["payment_status_code", "payment_status"], "string").alias("payment_status_code"),
        pickColumn(payments, ["remittance_reference", "bank_statement_ref"], "string").alias("remittance_reference"),
        pickColumn(payments, ["lockbox_batch_reference"], "string").alias("lockbox_batch_reference"),
        pickColumn(payments, ["sepa_end_to_end_id"], "string").alias("sepa_end_to_end_id"),
        F.upper(F.col("region_code")).alias("region_code"),
        F.col("batch_id").alias("source_batch_id"),
        F.col("loaded_at_utc").alias("source_loaded_at_utc"),
    ).dropDuplicates(["payment_business_key"])


def _allocations(allocations: DataFrame) -> DataFrame:
    reversed_ = pickColumn(allocations, ["reversal_of_allocation_business_key", "reversal_of_allocation_id"], "string")
    return allocations.select(
        F.col("payment_allocation_business_key"),
        F.col("payment_business_key"),
        pickColumn(allocations, ["allocated_when_utc", "allocated_when", "allocation_date"], "timestamp")
        .cast("date")
        .alias("allocation_date"),
        pickColumn(allocations, ["target_type_code"], "string").alias("target_type_code"),
        pickColumn(allocations, ["sale_business_key", "invoice_business_key"], "string").alias("sale_business_key"),
        pickColumn(allocations, ["credit_note_business_key"], "string").alias("credit_note_business_key"),
        money(F.col("allocated_amount")).alias("allocated_amount"),
        money(
            F.coalesce(
                pickColumn(allocations, ["settlement_discount_amount", "settlement_discount"], "decimal(19,4)"),
                F.lit(0),
            )
        ).alias("settlement_discount_amount"),
        money(
            F.coalesce(
                pickColumn(allocations, ["exchange_difference_amount", "exchange_difference"], "decimal(19,4)"),
                F.lit(0),
            )
        ).alias("realised_fx_gain_loss"),
        pickColumn(allocations, ["match_method_code"], "string").alias("match_method_code"),
        reversed_.isNotNull().alias("is_reversal"),
    ).dropDuplicates(["payment_allocation_business_key"])


def buildFactPayment(
    spark: SparkSession,
    cfg: PipelineConfig,
    payments: DataFrame,
    allocations: DataFrame,
    sales: DataFrame | None,
    dimCustomer: DataFrame | None,
) -> DataFrame:
    rec = _receipts(payments)
    alloc = _allocations(allocations)
    totals = alloc.groupBy("payment_business_key").agg(
        F.sum("allocated_amount").cast("decimal(19,4)").alias("allocated_total"),
        F.count("*").alias("allocation_count"),
    )
    rec = rec.join(totals, "payment_business_key", "left")
    rec = (
        rec.withColumn("allocated_total", money(F.coalesce(F.col("allocated_total"), F.lit(0))))
        .withColumn("allocation_count", F.coalesce(F.col("allocation_count"), F.lit(0)).cast("int"))
        .withColumn("unallocated_amount", money(F.col("payment_amount") - F.col("allocated_total")))
        .withColumn("is_over_allocated", F.col("allocated_total") > F.col("payment_amount") + F.lit(TOLERANCE))
        .withColumn("is_under_allocated", F.col("unallocated_amount") > F.lit(TOLERANCE))
        .withColumn("is_fully_applied", F.abs(F.col("unallocated_amount")) <= F.lit(TOLERANCE))
        .withColumn("is_unallocated_receipt", F.col("allocation_count") == 0)
    )
    # LEGACY QUIRK: over-allocation is rejected at RECEIPT level (all allocations of the receipt),
    # tolerance 0.01, exactly as usp_LoadFactPayment does.
    rec = quarantine(
        spark,
        cfg,
        rec,
        "FACT_PAYMENT_OVER_ALLOC",
        SOURCE_TABLE,
        F.col("is_over_allocated"),
        "Allocated amount exceeds receipt amount by more than 0.01",
    )

    allocRows = alloc.join(rec, "payment_business_key", "inner").withColumn(
        "receipt_line_number",
        F.row_number().over(
            Window.partitionBy("payment_business_key").orderBy("allocation_date", "payment_allocation_business_key")
        ),
    )
    # LEGACY QUIRK: unmatched / unallocated cash is a valid bucket (EU SEPA unmatched references),
    # carried as its own row per receipt, never a reject.
    unallocRows = (
        rec.filter(F.col("unallocated_amount") > F.lit(TOLERANCE))
        .withColumn(
            "payment_allocation_business_key", F.concat(F.col("payment_business_key"), F.lit(UNALLOCATED_SUFFIX))
        )
        .withColumn("allocation_date", F.col("payment_date"))
        .withColumn("target_type_code", F.lit(UNALLOCATED_STATUS))
        .withColumn("sale_business_key", F.lit(None).cast("string"))
        .withColumn("credit_note_business_key", F.lit(None).cast("string"))
        .withColumn("allocated_amount", F.lit(0).cast("decimal(19,4)"))
        .withColumn("settlement_discount_amount", F.lit(0).cast("decimal(19,4)"))
        .withColumn("realised_fx_gain_loss", F.lit(0).cast("decimal(19,4)"))
        .withColumn("match_method_code", F.lit(None).cast("string"))
        .withColumn("is_reversal", F.lit(False))
        .withColumn("receipt_line_number", F.lit(0))
    )
    df = allocRows.unionByName(unallocRows.select(*allocRows.columns))
    df = df.withColumn(
        "payment_status_code",
        F.when(F.col("receipt_line_number") == 0, F.lit(UNALLOCATED_STATUS)).otherwise(F.col("payment_status_code")),
    )

    if sales is not None:
        s = sales.select(
            F.col("sale_business_key").alias("_s_key"),
            F.col("invoice_date").cast("date").alias("invoice_date_key"),
        ).dropDuplicates(["_s_key"])
        df = df.join(s, df["sale_business_key"] == s["_s_key"], "left").drop("_s_key")
    else:
        df = df.withColumn("invoice_date_key", F.lit(None).cast("date"))

    df = rules_adapter.applyFx(
        df,
        spark,
        cfg,
        amountCols=["allocated_amount", "payment_amount", "unallocated_amount"],
        currencyCol="transaction_currency_code",
        dateCol="payment_date",
        regionCol="region_code",
    )
    # LEGACY QUIRK: the bank's booked rate on the receipt wins over the daily table when present.
    df = df.withColumn(
        "fx_rate_source_code",
        F.when(
            F.col("_bank_fx_rate").isNotNull() & (F.col("transaction_currency_code") != F.lit(cfg.reportingCurrency)),
            F.lit("BANK"),
        ).otherwise(F.col("fx_rate_source_code")),
    ).withColumn(
        "fx_rate_to_reporting",
        F.when(F.col("fx_rate_source_code") == "BANK", F.col("_bank_fx_rate"))
        .otherwise(F.col("fx_rate_to_reporting"))
        .cast("decimal(19,8)"),
    )
    for c in ("allocated_amount", "payment_amount", "unallocated_amount"):
        df = df.withColumn(f"{c}_reporting", money(F.col(c) * F.col("fx_rate_to_reporting")))

    df = lookupScd2Key(
        df,
        dimCustomer,
        "customer_business_key",
        "payment_date",
        "customer_key",
        "customer_business_key",
        "customer_key",
    )
    df = df.withColumn("inferred_member_flag", F.col("customer_key") == F.lit(UNKNOWN_KEY))
    df = df.withColumn("days_to_pay", daysBetween("invoice_date_key", "payment_date"))
    df = df.withColumn("settlement_lag_days", daysBetween("payment_date", "value_date"))

    unknown = F.lit(UNKNOWN_KEY).cast("bigint")
    out = df.select(
        surrogateKey("payment_allocation_business_key").alias("payment_key"),
        F.col("payment_allocation_business_key"),
        F.col("payment_business_key"),
        F.col("sale_business_key"),
        F.col("credit_note_business_key"),
        F.col("payment_date").alias("payment_date_key"),
        F.col("value_date").alias("value_date_key"),
        F.col("allocation_date").alias("allocation_date_key"),
        F.col("invoice_date_key"),
        F.col("customer_key"),
        F.col("customer_key").alias("bill_to_customer_key"),
        unknown.alias("payment_method_key"),
        unknown.alias("currency_key"),
        unknown.alias("sales_territory_key"),
        unknown.alias("cost_center_key"),
        F.col("region_code"),
        F.col("customer_business_key"),
        F.col("receipt_number"),
        F.col("receipt_line_number").cast("int"),
        F.col("sale_business_key").alias("invoice_number"),
        F.col("remittance_reference"),
        F.col("lockbox_batch_reference"),
        F.col("sepa_end_to_end_id"),
        F.col("payment_method_code"),
        F.col("target_type_code"),
        F.col("match_method_code"),
        F.col("transaction_currency_code"),
        F.col("payment_amount"),
        F.col("allocated_amount"),
        F.col("unallocated_amount"),
        F.col("settlement_discount_amount"),
        F.col("withholding_tax_amount"),
        F.col("bank_charge_amount"),
        F.col("write_off_amount"),
        F.col("fx_rate_to_reporting"),
        F.col("fx_rate_source_code"),
        F.col("payment_amount_reporting"),
        F.col("allocated_amount_reporting"),
        F.col("unallocated_amount_reporting"),
        F.col("realised_fx_gain_loss"),
        F.col("days_to_pay"),
        F.lit(None).cast("int").alias("days_beyond_terms"),
        F.col("settlement_lag_days"),
        F.col("payment_status_code"),
        F.col("is_fully_applied"),
        F.col("is_under_allocated"),
        F.col("is_over_allocated"),
        F.col("is_unallocated_receipt"),
        F.col("is_reversal"),
        F.lit(0).cast("int").alias("restatement_version"),
        F.lit(None).cast("timestamp").alias("restated_datetime"),
        F.col("inferred_member_flag"),
        F.col("source_batch_id").cast("bigint").alias("lineage_key"),
        F.col("source_loaded_at_utc"),
    )
    return withLoadMetadata(out, cfg)


def writeFactPayment(spark: SparkSession, cfg: PipelineConfig, df: DataFrame) -> None:
    # LEGACY QUIRK: a changed allocation or status restates the existing row in place (no new
    # row) and bumps [Restatement Version]; an unchanged row is left untouched (idempotent).
    changed = (
        (F.col("t.allocated_amount") != F.col("s.allocated_amount"))
        | (F.col("t.unallocated_amount") != F.col("s.unallocated_amount"))
        | (
            F.coalesce(F.col("t.payment_status_code"), F.lit(""))
            != F.coalesce(F.col("s.payment_status_code"), F.lit(""))
        )
        | (F.col("t.payment_amount") != F.col("s.payment_amount"))
    )
    mergeOnKeys(
        spark,
        df,
        cfg.fqn("gold", TABLE),
        KEY_COLS,
        updateCondition=changed,
        extraSet={
            "restatement_version": F.col("t.restatement_version") + F.lit(1),
            "restated_datetime": F.current_timestamp(),
        },
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    df = buildFactPayment(
        spark,
        cfg,
        payments=spark.table(cfg.fqn("silver", "payment")),
        allocations=spark.table(cfg.fqn("silver", "payment_allocation")),
        sales=readOptional(spark, cfg, "silver", "sale"),
        dimCustomer=readOptional(spark, cfg, "silver", "dim_customer"),
    )
    writeFactPayment(spark, cfg, df)
