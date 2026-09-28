# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_CreditNote
# MAGIC Port of `ssis/08_facts/FACT_Load_CreditNote.dtsx` (`build_fact_packages.py::build_fact_load_credit_note`).
# MAGIC
# MAGIC * Source `silver.stg_credit_note` (`LoadedAtUtc` watermark); natural key `CreditNoteBusinessKey`.
# MAGIC * Lookups: customer (SCD2 on credit note date, miss -> inferred member); original sale from `gold.fact_sale` on `OriginalSaleBusinessKey` == `wwi_invoice_id`; orphan credit notes (no original sale) are rejected (`ORPHAN_CREDIT_NOTE`), matching the legacy split.
# MAGIC * Approval rule: credits above 1,000.00 without an approver are rejected (`CREDIT_APPROVAL_REQUIRED`).
# MAGIC * `Restate Original Sale Rows` + `Record Restatement Audit`: after the MERGE the linked `gold.fact_sale` rows get net / tax / margin reduced by the credit and their audit columns stamped with this run; a `Fact.Sale restated by credit note` row count is logged.
# MAGIC * Target `gold.fact_credit_note` (liquid-clustered `credit_note_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_CreditNote"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Credit Note"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_credit_note")))

# COMMAND ----------

REJECT_ORPHAN = "ORPHAN_CREDIT_NOTE"
REJECT_APPROVAL = "CREDIT_APPROVAL_REQUIRED"

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_credit_note",
    sourceTable="stg_credit_note", sourceDateCol="CreditNoteDate", sourceTimestampCol="LoadedAtUtc",
    businessKeyCol="CreditNoteBusinessKey", naturalKeyCols=["CreditNoteBusinessKey"],
    surrogateKeyCol="credit_note_key", dateKeyCol="credit_note_date_key",
    validation=lambda df: F.col("GrossAmount").isNull() | F.col("CreditNoteDate").isNull() | F.col("CustomerBusinessKey").isNull(),
    lookups=[fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", "CreditNoteDate", onMiss=fact_load.ON_MISS_INFER)],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    saleName = naming.table(catalog, "gold", "fact_sale")
    if fc.tableExists(spark, saleName):
        sales = (
            spark.table(saleName)
            .where(F.coalesce(F.col("correction_type_code"), F.lit(fc.CORRECTION_ORIGINAL)) == fc.CORRECTION_ORIGINAL)
            .groupBy(F.col("wwi_invoice_id").cast("string").alias("OriginalSaleBusinessKey"))
            .agg(F.min("sale_key").alias("original_sale_key"), F.min("invoice_date_key").alias("original_invoice_date_key"),
                 F.min("invoice_number").alias("original_invoice_number"), F.first("tax_regime_code").alias("tax_regime_code"),
                 F.first("vat_rate").alias("vat_rate"), F.first("gst_rate").alias("gst_rate"), F.first("tax_rate").alias("sales_tax_rate"))
        )
        df = df.join(sales, df["OriginalSaleBusinessKey"].cast("string") == sales["OriginalSaleBusinessKey"], "left").drop(sales["OriginalSaleBusinessKey"])
    else:
        for c, t in (("original_sale_key", "bigint"), ("original_invoice_date_key", "date"), ("original_invoice_number", "string"), ("tax_regime_code", "string"), ("vat_rate", "decimal(5,2)"), ("gst_rate", "decimal(5,2)"), ("sales_tax_rate", "decimal(5,2)")):
            df = df.withColumn(c, F.lit(None).cast(t))

    orphan = df.where(F.col("original_sale_key").isNull() & F.col("OriginalSaleBusinessKey").isNotNull())
    fc.rejectRows(spark, catalog, orphan, OBJECT_NAME, REJECT_ORPHAN, batchId, run.packageExecutionId, businessKeyColumn="CreditNoteBusinessKey", sourceSystemCode="DW")
    df = df.where(F.col("original_sale_key").isNotNull() | F.col("OriginalSaleBusinessKey").isNull())
    needsApproval = rules.creditRequiresApproval(F.col("GrossAmount"), F.col("ApprovedByName"))
    fc.rejectRows(spark, catalog, df.where(needsApproval), OBJECT_NAME, REJECT_APPROVAL, batchId, run.packageExecutionId, businessKeyColumn="CreditNoteBusinessKey", sourceSystemCode="DW")
    df = df.where(~needsApproval)

    net, tax, gross = F.col("NetAmount"), F.coalesce(F.col("TaxAmount"), F.lit(0)), F.col("GrossAmount")
    return df.select(
        F.col("CreditNoteDate").cast("date").alias("credit_note_date_key"),
        F.col("original_invoice_date_key"),
        "customer_key", F.col("customer_key").alias("bill_to_customer_key"), F.lit(fc.NOT_APPLICABLE_KEY).alias("stock_item_key"),
        "original_sale_key",
        F.col("RegionCode").alias("region_code"),
        F.col("CreditNoteNumber").alias("credit_note_number"), F.lit(1).alias("credit_note_line_number"),
        F.col("original_invoice_number"), F.col("RmaNumber").alias("rma_number"),
        F.col("CreditReasonCode").alias("credit_reason_code"), F.col("ApprovedByName").alias("approved_by_name"), F.col("CreditStatusCode").alias("credit_status_code"),
        F.col("TransactionCurrencyCode").alias("transaction_currency_code"),
        rules.money(net).alias("credit_excluding_tax"), rules.money(tax).alias("tax_credit_amount"), rules.money(gross).alias("credit_including_tax"),
        rules.safeDivide(F.col("NetAmountUsd"), net).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(rules.reportingAmount(gross, rules.safeDivide(F.col("NetAmountUsd"), net))).alias("credit_including_tax_reporting"),
        F.col("tax_regime_code"), F.col("vat_rate"), F.col("gst_rate"), F.col("sales_tax_rate"),
        (F.col("CreditReasonCode") == "GOODWILL").alias("goodwill_flag"),
        (F.col("CreditReasonCode") == "REBATE").alias("rebate_settlement_flag"),
        F.coalesce(F.col("VatCreditNoteRequiredFlag"), F.lit(False)).alias("vat_credit_note_required_flag"),
        rules.fiscalYear(F.col("CreditNoteDate"), 1).alias("fiscal_year"), rules.fiscalPeriod(F.col("CreditNoteDate"), 1).alias("fiscal_period"),
        F.col("customer_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )


def restateOriginalSales(spark, catalog, factRows, merged):
    from delta.tables import DeltaTable

    saleName = naming.table(catalog, "gold", "fact_sale")
    credits = factRows.where(F.col("original_sale_key").isNotNull()).groupBy("original_sale_key").agg(
        F.sum("credit_excluding_tax").alias("credit_amount"), F.sum("tax_credit_amount").alias("tax_credit")
    )
    if not fc.tableExists(spark, saleName) or credits.limit(1).count() == 0:
        return
    versionBefore = fc.tableVersion(spark, saleName)
    DeltaTable.forName(spark, saleName).alias("s").merge(credits.alias("c"), "s.sale_key = c.original_sale_key").whenMatchedUpdate(set={
        "net_amount": "round(s.net_amount - c.credit_amount, 2)",
        "total_excluding_tax": "round(s.total_excluding_tax - c.credit_amount, 2)",
        "tax_amount": "round(s.tax_amount - c.tax_credit, 2)",
        "total_including_tax": "round(s.total_including_tax - c.credit_amount - c.tax_credit, 2)",
        "gross_margin_amount": "round((s.net_amount - c.credit_amount) - coalesce(s.cost_of_sale_amount, 0), 2)",
        "profit": "round((s.net_amount - c.credit_amount) - coalesce(s.cost_of_sale_amount, 0), 2)",
        "net_amount_reporting": "round((s.net_amount - c.credit_amount) * coalesce(s.fx_rate_to_reporting, 1), 2)",
        "batch_id": "cast(%d as bigint)" % batchId,
        "package_execution_id": "cast(%d as bigint)" % run.packageExecutionId,
        "load_datetime": "current_timestamp()",
    }).execute()
    restated = fc.lastOperationMetrics(spark, saleName, versionBefore).get("numTargetRowsUpdated", 0)
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact.Sale restated by credit note", updateRowCount=restated)

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform, afterMerge=restateOriginalSales)
    print(result)
