# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_EU_Load_Sale
# MAGIC Port of `ssis/08_facts/FACT_EU_Load_Sale.dtsx` (`build_fact_eu_load_sale`) and `Integration.usp_LoadFactSale` for `RegionCode = 'EU'`.
# MAGIC
# MAGIC Shares the sale pipeline in `src/fact_sale.py` with NA/APAC; EU-specific rules:
# MAGIC * Effective FX by transaction currency and invoice date (latest closing rate on or before the date, group source);
# MAGIC   lines with no rate are written to `silver.work_currency_conversion_scratch` and held (`FX_RATE_MISSING`, EU retry budget 7).
# MAGIC * Ship-to country (`gold.dim_customer.country_code`), customer VAT number (`tax_registration_number`) and the standard VAT
# MAGIC   rate from `silver.stg_tax_rate` (`TaxRegimeCode = 'VAT'`, effective-dated by country).
# MAGIC * Reverse charge when the customer has a VAT number and the customer country differs from the ship-to country:
# MAGIC   `VatRateApplied = 0`, `IsReverseCharge = 1`; otherwise the configured standard rate; VAT is recomputed on the net amount.

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

PACKAGE_NAME = "FACT_EU_Load_Sale"
STEP_NAME = "Load Facts"
REGION_CODE = rules.REGION_EU

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", fact_sale.FACT_TABLE)))

# COMMAND ----------


def euVatRule(spark, catalog, df):
    """Reverse-charge / domestic VAT exactly as the legacy Derived Column expressions."""
    customer = fc.readDimension(spark, catalog, fc.DIMENSIONS["Customer"]).where(F.col("is_current_row"))
    vatStatus = customer.select(
        F.col("customer_key").alias("customer_key"),
        F.col("country_code").alias("CustomerCountryIsoCode"),
        F.col("tax_registration_number").alias("CustomerVatNumber") if "tax_registration_number" in customer.columns else F.lit(None).cast("string").alias("CustomerVatNumber"),
    )
    shipTo = customer.select(F.col("customer_key").alias("bill_to_customer_key"), F.col("country_code").alias("ShipToCountryIsoCode"))
    df = df.join(vatStatus, "customer_key", "left").join(shipTo, "bill_to_customer_key", "left")
    df = df.withColumn("ShipToCountryIsoCode", F.coalesce(F.col("ShipToCountryIsoCode"), F.col("CustomerCountryIsoCode")))

    taxRates = fc.readTable(spark, catalog, "silver", "stg_tax_rate").where(F.upper(F.col("TaxRegimeCode")) == "VAT")
    rateRows = taxRates.alias("r")
    cond = (
        (F.col("s.ShipToCountryIsoCode") == F.col("r.CountryCode"))
        & (F.col("s.InvoiceDate") >= F.col("r.EffectiveFromDate"))
        & (F.col("s.InvoiceDate") < F.coalesce(F.col("r.EffectiveToDate"), F.lit("9999-12-31").cast("date")))
    )
    rated = df.alias("s").join(rateRows, cond, "left").select(*[F.col("s." + c) for c in df.columns], F.col("r.RatePercent").alias("StandardVatRatePercent"))
    rated = fc.dedupByRowVersion(rated, ["SaleLineBusinessKey"], "StandardVatRatePercent")

    isReverse = rules.euIsReverseCharge(F.col("CustomerVatNumber"), F.col("CustomerCountryIsoCode"), F.col("ShipToCountryIsoCode"))
    rated = rated.withColumn("is_reverse_charge", isReverse)
    rated = rated.withColumn("vat_rate_applied", rules.euVatRateApplied(F.col("is_reverse_charge"), F.col("StandardVatRatePercent")))
    return (
        rated.withColumn("tax_amount", rules.euVatAmount(F.col("net_amount"), F.col("vat_rate_applied")))
        .withColumn("tax_rate", F.col("vat_rate_applied"))
        .withColumn("gst_rate_applied", F.lit(None).cast("decimal(5,2)"))
        .withColumn("is_price_inclusive", F.lit(None).cast("boolean"))
    )


# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    fxSource = fc.configurationValue(spark, catalog, "Fact.Sale.FxRateSource.EU", p["environmentCode"], "")
    result = fact_sale.loadRegion(spark, catalog, p, REGION_CODE, PACKAGE_NAME, run, batchId, euVatRule, fxSourceCode=fxSource or None)
    print(result)
