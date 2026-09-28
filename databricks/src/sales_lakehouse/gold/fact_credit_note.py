"""``fact_credit_note`` - credit-note accumulating snapshot (``DeltaTable.merge`` on
``credit_note_business_key``); milestones ``original_invoice_date_key`` ->
``credit_note_date_key`` -> ``applied_date_key``; lags recomputed each merge.

Replaces Integration.usp_LoadFactCreditNote and SSIS FACT_Load_CreditNote.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.quality import quarantine
from sales_lakehouse.gold import rules_adapter
from sales_lakehouse.gold.fact_support import (
    UNKNOWN_KEY,
    daysBetween,
    lookupCurrentKey,
    lookupScd2Key,
    mergeAccumulating,
    money,
    pickColumn,
    readOptional,
    surrogateKey,
    withLoadMetadata,
)
from sales_lakehouse.gold.inputs import readDimSalesperson, readOrEmpty

TABLE = "fact_credit_note"
KEY_COLS = ["credit_note_business_key"]
MILESTONES = ["original_invoice_date_key", "credit_note_date_key", "applied_date_key"]
SOURCE_TABLE = "silver.credit_note"
# silver.credit_note contract (no silver producer ships it yet; an absent table loads as empty)
SILVER_CREDIT_NOTE_SCHEMA = (
    "credit_note_business_key string, credit_note_number string, customer_business_key string, "
    "original_sale_business_key string, rma_number string, credit_reason_code string, credit_note_date date, "
    "net_amount decimal(19,4), tax_amount decimal(19,4), gross_amount decimal(19,4), transaction_currency_code string, "
    "applied_to_sale_business_key string, approved_by_name string, credit_status_code string, "
    "vat_credit_note_required_flag boolean, region_code string, batch_id bigint, loaded_at_utc timestamp"
)
UNAPPROVED_THRESHOLD = 1000


def finalizeCreditNote(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("invoice_to_credit_lag_days", daysBetween("original_invoice_date_key", "credit_note_date_key"))
        .withColumn("credit_to_applied_lag_days", daysBetween("credit_note_date_key", "applied_date_key"))
        .withColumn("applied_flag", F.col("applied_date_key").isNotNull())
        .withColumn("last_milestone_update", F.current_timestamp())
    )


def buildFactCreditNote(
    spark: SparkSession,
    cfg: PipelineConfig,
    creditNotes: DataFrame,
    sales: DataFrame | None,
    allocations: DataFrame | None,
    dimCustomer: DataFrame | None,
    dimSalesperson: DataFrame | None,
) -> DataFrame:
    df = creditNotes.select(
        F.col("credit_note_business_key"),
        pickColumn(creditNotes, ["credit_note_number"], "string").alias("credit_note_number"),
        F.col("customer_business_key"),
        pickColumn(creditNotes, ["original_sale_business_key", "sale_business_key"], "string").alias(
            "original_sale_business_key"
        ),
        pickColumn(creditNotes, ["rma_number"], "string").alias("rma_number"),
        pickColumn(creditNotes, ["credit_reason_code"], "string").alias("credit_reason_code"),
        F.col("credit_note_date").cast("date").alias("credit_note_date_key"),
        F.col("net_amount").cast("decimal(19,4)").alias("_net"),
        F.coalesce(pickColumn(creditNotes, ["tax_amount"], "decimal(19,4)"), F.lit(0))
        .cast("decimal(19,4)")
        .alias("_tax"),
        pickColumn(creditNotes, ["gross_amount"], "decimal(19,4)").alias("_gross"),
        F.col("transaction_currency_code"),
        pickColumn(creditNotes, ["applied_to_sale_business_key"], "string").alias("applied_to_sale_business_key"),
        pickColumn(creditNotes, ["approved_by_name", "approved_by"], "string").alias("approved_by_name"),
        pickColumn(creditNotes, ["credit_status_code"], "string").alias("credit_status_code"),
        F.coalesce(pickColumn(creditNotes, ["vat_credit_note_required_flag"], "boolean"), F.lit(False)).alias(
            "vat_credit_note_required_flag"
        ),
        F.upper(F.col("region_code")).alias("region_code"),
        F.col("batch_id").alias("lineage_key"),
        F.col("loaded_at_utc").alias("source_loaded_at_utc"),
    )
    # LEGACY QUIRK: return-backed credit notes (an RMA reference) belong to Fact.Return and are
    # excluded here so a return is not credited twice (usp_LoadFactCreditNote).
    df = df.filter(F.col("rma_number").isNull())
    df = df.withColumn("_gross", F.coalesce(F.col("_gross"), F.col("_net") + F.col("_tax")))
    # LEGACY QUIRK: unapproved credit notes above 1,000 in transaction currency are rejected
    # (audit finding 2019); smaller ones load with approved_flag = false.
    df = quarantine(
        spark,
        cfg,
        df,
        "FACT_CREDIT_NOTE_UNAPPROVED",
        SOURCE_TABLE,
        F.col("approved_by_name").isNull() & (F.abs(F.col("_gross")) > F.lit(UNAPPROVED_THRESHOLD)),
        f"Credit note over {UNAPPROVED_THRESHOLD} without approval",
    )

    if sales is not None:
        s = sales.select(
            F.col("sale_business_key").alias("_s_key"),
            F.col("invoice_date").cast("date").alias("original_invoice_date_key"),
            pickColumn(sales, ["salesperson_business_key"], "string").alias("salesperson_business_key"),
        ).dropDuplicates(["_s_key"])
        df = df.join(s, df["original_sale_business_key"] == s["_s_key"], "left").drop("_s_key")
    else:
        df = df.withColumn("original_invoice_date_key", F.lit(None).cast("date")).withColumn(
            "salesperson_business_key", F.lit(None).cast("string")
        )
    if allocations is not None and "credit_note_business_key" in allocations.columns:
        a = (
            allocations.filter(F.col("credit_note_business_key").isNotNull())
            .groupBy("credit_note_business_key")
            .agg(
                F.min(
                    pickColumn(allocations, ["allocated_when_utc", "allocation_date"], "timestamp").cast("date")
                ).alias("applied_date_key"),
                F.sum(pickColumn(allocations, ["allocated_amount_local", "allocated_amount"], "decimal(19,4)"))
                .cast("decimal(19,4)")
                .alias("applied_amount"),
            )
        )
        df = df.join(a, "credit_note_business_key", "left")
    else:
        df = df.withColumn("applied_date_key", F.lit(None).cast("date")).withColumn(
            "applied_amount", F.lit(None).cast("decimal(19,4)")
        )

    # LEGACY QUIRK: credit values are stored NEGATIVE so they net against Fact.Sale.
    df = (
        df.withColumn("credit_excluding_tax", money(-F.abs(F.col("_net"))))
        .withColumn("credit_tax_amount", money(-F.abs(F.col("_tax"))))
        .withColumn("credit_including_tax", money(-F.abs(F.col("_gross"))))
        .withColumn("approved_flag", F.col("approved_by_name").isNotNull())
        .withColumn("goodwill_flag", F.upper(F.coalesce(F.col("credit_reason_code"), F.lit(""))).isin("GOODWILL", "GW"))
        .withColumn("rebate_flag", F.upper(F.coalesce(F.col("credit_reason_code"), F.lit(""))).isin("REBATE", "RB"))
    )
    df = rules_adapter.applyFx(
        df,
        spark,
        cfg,
        amountCols=["credit_including_tax", "credit_excluding_tax"],
        currencyCol="transaction_currency_code",
        dateCol="credit_note_date_key",
        regionCol="region_code",
    )
    df = rules_adapter.resolveFiscalPeriod(df, spark, cfg, dateCol="credit_note_date_key", regionCol="region_code")
    df = lookupScd2Key(
        df,
        dimCustomer,
        "customer_business_key",
        "credit_note_date_key",
        "customer_key",
        "customer_business_key",
        "customer_key",
    )
    df = lookupCurrentKey(
        df,
        dimSalesperson,
        "salesperson_business_key",
        "salesperson_key",
        "salesperson_business_key",
        "salesperson_key",
        "is_current",
    )
    df = df.withColumn("inferred_member_flag", F.col("customer_key") == F.lit(UNKNOWN_KEY))
    out = df.select(
        surrogateKey("credit_note_business_key").alias("credit_note_key"),
        F.col("credit_note_business_key"),
        F.col("original_sale_business_key"),
        F.col("credit_note_date_key"),
        F.col("original_invoice_date_key"),
        F.col("applied_date_key"),
        F.col("customer_key"),
        F.lit(UNKNOWN_KEY).cast("bigint").alias("stock_item_key"),
        F.col("salesperson_key"),
        F.lit(UNKNOWN_KEY).cast("bigint").alias("sales_territory_key"),
        F.lit(UNKNOWN_KEY).cast("bigint").alias("credit_reason_key"),
        F.lit(UNKNOWN_KEY).cast("bigint").alias("currency_key"),
        F.lit(None).cast("bigint").alias("return_key"),
        F.col("region_code"),
        F.col("customer_business_key"),
        F.col("credit_note_number"),
        F.lit(1).cast("int").alias("credit_note_line_number"),
        F.col("original_sale_business_key").alias("original_invoice_number"),
        F.col("applied_to_sale_business_key"),
        F.col("credit_reason_code"),
        F.col("credit_status_code"),
        F.col("approved_by_name"),
        F.col("approved_flag"),
        F.col("vat_credit_note_required_flag"),
        F.col("goodwill_flag"),
        F.col("rebate_flag"),
        F.col("transaction_currency_code"),
        F.col("credit_excluding_tax"),
        F.col("credit_tax_amount"),
        F.col("credit_including_tax"),
        F.col("applied_amount"),
        F.col("fx_rate_to_reporting"),
        F.col("fx_rate_source_code"),
        F.col("credit_including_tax_reporting"),
        F.col("credit_excluding_tax_reporting"),
        F.col("fiscal_year"),
        F.col("fiscal_period"),
        F.col("fiscal_period_key"),
        F.col("inferred_member_flag"),
        F.col("lineage_key").cast("bigint"),
        F.col("source_loaded_at_utc"),
    )
    return withLoadMetadata(out, cfg)


def writeFactCreditNote(spark: SparkSession, cfg: PipelineConfig, df: DataFrame) -> None:
    mergeAccumulating(spark, df, cfg.fqn("gold", TABLE), KEY_COLS, MILESTONES, finalizeCreditNote)


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    df = buildFactCreditNote(
        spark,
        cfg,
        creditNotes=readOrEmpty(spark, cfg, "silver", "credit_note", SILVER_CREDIT_NOTE_SCHEMA),
        sales=readOptional(spark, cfg, "silver", "sale"),
        allocations=readOptional(spark, cfg, "silver", "payment_allocation"),
        dimCustomer=readOptional(spark, cfg, "silver", "dim_customer"),
        dimSalesperson=readDimSalesperson(spark, cfg),
    )
    writeFactCreditNote(spark, cfg, df)
