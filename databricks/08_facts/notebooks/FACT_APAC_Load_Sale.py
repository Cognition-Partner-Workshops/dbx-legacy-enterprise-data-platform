# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_APAC_Load_Sale
# MAGIC Port of `ssis/08_facts/FACT_APAC_Load_Sale.dtsx` (`build_fact_apac_load_sale`) and `Integration.usp_LoadFactSale` for `RegionCode = 'APAC'`.
# MAGIC
# MAGIC Shares the sale pipeline in `src/fact_sale.py`; APAC-specific rules:
# MAGIC * GST rate and inclusive-pricing flag from `silver.stg_tax_rate` (`TaxRegimeCode = 'GST'`, effective-dated by the stock item's
# MAGIC   `apac_gst_treatment_code` / country); GST-free treatment yields a zero rate.
# MAGIC * Inclusive pricing backs GST out of the gross (`gross - gross / (1 + rate/100)`), exclusive pricing adds GST on the source amount.
# MAGIC * FX comes from the APAC treasury source (`Fact.Sale.FxRateSource.APAC`, default `APAC_TSY`); missing rates are held (`FX_RATE_MISSING`, retry budget 21).
# MAGIC * Fiscal year label starts in April; distributor rebate accrual (`Fact.Sale.APAC.DistributorRebatePercent`) is exposed as `apac_rebate_accrual`
# MAGIC   in `silver.int_fact_sale_apac_rebate` because `Fact.Sale` has no column for it (see mapping doc).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import fact_common as fc
import fact_rules as rules
import fact_sale

# COMMAND ----------

PACKAGE_NAME = "FACT_APAC_Load_Sale"
STEP_NAME = "Load Facts"
REGION_CODE = rules.REGION_APAC

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", fact_sale.FACT_TABLE)))

# COMMAND ----------


def apacGstRule(spark, catalog, df):
    stockItem = fc.readDimension(spark, catalog, fc.DIMENSIONS["Stock Item"]).where(F.col("is_current_row"))
    treatment = stockItem.select(
        F.col("stock_item_key"),
        (F.col("apac_gst_treatment_code") if "apac_gst_treatment_code" in stockItem.columns else F.lit("STD")).alias("GstTreatmentCode"),
        (F.col("country_of_origin_code") if "country_of_origin_code" in stockItem.columns else F.lit(None).cast("string")).alias("GstCountryCode"),
    )
    df = df.join(treatment, "stock_item_key", "left")

    gst = fc.readTable(spark, catalog, "silver", "stg_tax_rate").where(F.upper(F.col("TaxRegimeCode")) == "GST").alias("r")
    cond = (
        (F.coalesce(F.col("s.GstTreatmentCode"), F.lit("STD")) == F.coalesce(F.col("r.TaxClassCode"), F.lit("STD")))
        & (F.col("s.InvoiceDate") >= F.col("r.EffectiveFromDate"))
        & (F.col("s.InvoiceDate") < F.coalesce(F.col("r.EffectiveToDate"), F.lit("9999-12-31").cast("date")))
    )
    rated = df.alias("s").join(gst, cond, "left").select(
        *[F.col("s." + c) for c in df.columns],
        F.col("r.RatePercent").alias("GstRatePercent"),
        F.coalesce(F.col("r.IsCompound"), F.lit(False)).alias("IsPriceInclusive"),
    )
    rated = fc.dedupByRowVersion(rated, ["SaleLineBusinessKey"], "GstRatePercent")
    gstFree = F.upper(F.coalesce(F.col("GstTreatmentCode"), F.lit(""))).isin("FREE", "GST_FREE", "EXEMPT")
    rated = rated.withColumn("gst_rate_applied", F.when(gstFree, F.lit(0)).otherwise(F.coalesce(F.col("GstRatePercent"), F.lit(0))).cast("decimal(5,2)"))
    rated = rated.withColumn("is_price_inclusive", F.col("IsPriceInclusive")).withColumn("gst_free_flag", gstFree)
    rated = rated.withColumn("tax_amount", rules.apacGstAmount(F.col("Quantity"), F.col("UnitPrice"), F.col("gst_rate_applied"), F.col("is_price_inclusive")))
    rated = rated.withColumn("net_amount", rules.apacNetOfGst(F.col("net_amount"), F.col("tax_amount"), F.col("is_price_inclusive")))
    rated = rated.withColumn("gross_margin", rules.saleMarginAmount(F.col("net_amount"), F.col("cost_of_sale")))
    rated = rated.withColumn("margin_percent", rules.marginPercent(F.col("gross_margin"), F.col("net_amount")))
    return (
        rated.withColumn("tax_rate", F.col("gst_rate_applied"))
        .withColumn("is_reverse_charge", F.lit(False))
        .withColumn("vat_rate_applied", F.lit(None).cast("decimal(5,2)"))
    )


def writeDistributorRebateAccrual(spark, catalog, batchId, packageExecutionId, rebatePercent):
    """Generator-only measure (no Fact.Sale column): accrue the distributor rebate
    for this batch's APAC lines into a silver integration table."""
    customer = fc.readDimension(spark, catalog, fc.DIMENSIONS["Customer"]).where(F.col("is_current_row"))
    isDistributor = (F.upper(F.col("customer_category_code")) == "DISTRIBUTOR") if "customer_category_code" in customer.columns else F.lit(False)
    distributors = customer.select("customer_key", isDistributor.alias("IsDistributor"))
    sales = spark.table(naming.table(catalog, "gold", fact_sale.FACT_TABLE)).where((F.col("batch_id") == batchId) & (F.col("region_code") == REGION_CODE))
    accrual = sales.join(distributors, "customer_key", "left").select(
        "sale_key", "customer_key", "invoice_date_key", "net_amount",
        rules.apacDistributorRebateAccrual(F.col("net_amount"), F.col("IsDistributor"), F.lit(rebatePercent)).alias("apac_rebate_accrual"),
        F.lit(batchId).cast("bigint").alias("batch_id"), F.lit(packageExecutionId).cast("bigint").alias("package_execution_id"), F.current_timestamp().alias("load_datetime"),
    ).where(F.col("apac_rebate_accrual") != 0)
    return fc.mergeFact(spark, naming.table(catalog, "silver", "int_fact_sale_apac_rebate"), accrual, ["sale_key"])


# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    fxSource = fc.configurationValue(spark, catalog, "Fact.Sale.FxRateSource.APAC", p["environmentCode"], "APAC_TSY")
    result = fact_sale.loadRegion(
        spark, catalog, p, REGION_CODE, PACKAGE_NAME, run, batchId, apacGstRule,
        fxSourceCode=fxSource or None, fiscalYearStartMonth=rules.FISCAL_YEAR_START_MONTH_APAC,
    )
    rebatePercent = float(fc.configurationValue(spark, catalog, "Fact.Sale.APAC.DistributorRebatePercent", p["environmentCode"], "2.5"))
    result["rebateRows"] = writeDistributorRebateAccrual(spark, catalog, batchId, run.packageExecutionId, rebatePercent)
    print(result)
