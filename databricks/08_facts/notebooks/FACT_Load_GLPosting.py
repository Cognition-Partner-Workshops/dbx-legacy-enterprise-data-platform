# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_GLPosting
# MAGIC Port of `ssis/08_facts/FACT_Load_GLPosting.dtsx` (`build_fact_packages.py::build_fact_load_gl_posting`).
# MAGIC
# MAGIC * `Quarantine Unbalanced Batches` runs BEFORE the load: journal batches whose |sum(debit) - sum(credit)| exceeds `AllowedRoundingVariance` cents (default 1, configuration `Fact.GLPosting.AllowedRoundingVariance`) are rejected (`JOURNAL_BATCH_UNBALANCED`) and excluded from the load.
# MAGIC * Source `silver.stg_gl_posting` (`LastModifiedAt` watermark, posted rows only); natural key `JournalBatchNumber|JournalLineNumber`.
# MAGIC * Lookups: GL account (type-1 on `GlAccountCode`, miss -> -1); cost center dimension is not part of this project's dimension set (-1, see mapping doc).
# MAGIC * `Flag Closed Period Postings`: `late_posting_flag` / `period_status_code = CLOSED` when the accounting period is earlier than the BusinessDate period.
# MAGIC * Target `gold.fact_gl_posting` (liquid-clustered `posting_date_key, region_code`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import Window
from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import fact_common as fc
import fact_load
import fact_rules as rules

# COMMAND ----------

PACKAGE_NAME = "FACT_Load_GLPosting"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.GL Posting"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_gl_posting")))

# COMMAND ----------

REJECT_UNBALANCED = "JOURNAL_BATCH_UNBALANCED"
allowedRoundingVarianceCents = int(fc.configurationValue(spark, catalog, "Fact.GLPosting.AllowedRoundingVariance", p["environmentCode"], "1"))
currentPeriod = p["businessDate"].strftime("%Y-%m")


def quarantineUnbalancedBatches(spark, catalog, batchId, packageExecutionId):
    src = fc.readTable(spark, catalog, "silver", "stg_gl_posting")
    unbalanced = (
        src.groupBy("JournalBatchNumber")
        .agg(F.sum(F.coalesce(F.col("DebitAmount"), F.lit(0))).alias("debits"), F.sum(F.coalesce(F.col("CreditAmount"), F.lit(0))).alias("credits"))
        .where(F.abs(F.col("debits") - F.col("credits")) > F.lit(allowedRoundingVarianceCents) / 100.0)
        .select("JournalBatchNumber", "debits", "credits")
    )
    rejected = fc.rejectRows(spark, catalog, unbalanced, "stg.GLPosting", REJECT_UNBALANCED, batchId, packageExecutionId, businessKeyColumn="JournalBatchNumber", sourceSystemCode="DW", rejectStage="Stage")
    return [r["JournalBatchNumber"] for r in unbalanced.select("JournalBatchNumber").collect()], rejected


unbalancedBatches = []


def glSource(df):
    df = df.where(F.coalesce(F.col("IsPosted"), F.lit(True)))
    if unbalancedBatches:
        df = df.where(~F.col("JournalBatchNumber").isin(*unbalancedBatches))
    return df


SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_gl_posting",
    sourceTable="stg_gl_posting", sourceDateCol="PostingDate", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="GlPostingBusinessKey", naturalKeyCols=["JournalBatchNumber", "JournalLineNumber"],
    surrogateKeyCol="gl_posting_key", dateKeyCol="posting_date_key",
    sourceFilter=glSource,
    validation=lambda df: F.col("PostingDate").isNull() | (F.col("DebitAmount").isNull() & F.col("CreditAmount").isNull()),
    lookups=[fact_load.LookupSpec("GL Account", "GlAccountCode", "gl_account_key")],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    debit, credit = F.coalesce(F.col("DebitAmount"), F.lit(0)), F.coalesce(F.col("CreditAmount"), F.lit(0))
    signed = rules.glSignedAmount(debit, credit)
    period = F.col("AccountingPeriodCode")
    closed = period < F.lit(currentPeriod)
    return df.select(
        F.col("PostingDate").cast("date").alias("posting_date_key"), F.col("PostingDate").cast("date").alias("effective_date_key"),
        "gl_account_key", F.lit(fc.UNKNOWN_KEY).alias("cost_center_key"),
        F.col("RegionCode").alias("region_code"), F.col("LegalEntityCode").alias("legal_entity_code"),
        F.col("JournalBatchNumber").alias("journal_number"), F.col("JournalLineNumber").alias("journal_line_number"),
        F.col("JournalSourceCode").alias("journal_source_code"),
        F.col("GlAccountCode").alias("local_account_code"), F.col("CostCentreCode").alias("cost_center_code"),
        F.col("TransactionCurrency").alias("ledger_currency_code"),
        rules.money(debit).alias("debit_amount"), rules.money(credit).alias("credit_amount"), rules.money(signed).alias("signed_amount"),
        rules.safeDivide(F.col("NetAmountUsd"), signed).cast(rules.RATE).alias("consolidation_rate"),
        rules.money(F.col("NetAmountUsd")).alias("signed_amount_reporting"),
        period.alias("accounting_period_code"),
        F.substring(period, 1, 4).cast("smallint").alias("fiscal_year"), F.substring(period, 6, 2).cast("tinyint").alias("fiscal_period"),
        F.when(closed, F.lit("CLOSED")).otherwise(F.lit("OPEN")).alias("period_status_code"),
        closed.alias("late_posting_flag"),
        rules.isManualJournal(F.col("JournalSourceCode")).alias("manual_journal_flag"),
        F.coalesce(F.col("IsPosted"), F.lit(True)).alias("posted_flag"),
        "natural_key_hash",
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    unbalancedBatches[:], quarantined = quarantineUnbalancedBatches(spark, catalog, batchId, run.packageExecutionId)
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform)
    print(result)
