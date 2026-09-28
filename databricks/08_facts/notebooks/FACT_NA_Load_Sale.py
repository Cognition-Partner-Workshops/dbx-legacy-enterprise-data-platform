# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_NA_Load_Sale
# MAGIC Port of `ssis/08_facts/FACT_NA_Load_Sale.dtsx` (`build_fact_packages.py::build_fact_na_load_sale`)
# MAGIC and `Integration.usp_LoadFactSale` for `RegionCode = 'NA'`.
# MAGIC
# MAGIC * Source: `silver.stg_sale_line` x `silver.stg_sale`, five-day backdating window from the `Fact.Sale.NA` watermark, draft invoices excluded.
# MAGIC * Natural key hash = SHA-256(`InvoiceNumber|InvoiceLineNumber|RegionCode`); latest `SourceRowVersion` wins.
# MAGIC * SCD2 lookups on business key + invoice date (`gold.dim_customer`, `gold.dim_stock_item`, `gold.dim_salesperson`); ordinary miss -> `-1`.
# MAGIC * Missing customers become inferred members and are queued in `silver.work_late_arriving_dimension_queue`;
# MAGIC   missing stock items are parked in `gold.fact_fact_load_hold` (`DIM_NOT_KEYED`, NA retry budget 3).
# MAGIC * NA tax rule: the source sales tax is kept as-is (jurisdiction engine already applied it); no recalculation.
# MAGIC * Target `gold.fact_sale` (liquid-clustered by `invoice_date_key, region_code`): MERGE by `sale_key`, changed lines
# MAGIC   emit a negated `REV` row plus the restated `RES` row, new lines insert as `ORIG`.

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

PACKAGE_NAME = "FACT_NA_Load_Sale"
STEP_NAME = "Load Facts"
REGION_CODE = rules.REGION_NA

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", fact_sale.FACT_TABLE)))

# COMMAND ----------


def naTaxRule(spark, catalog, df):
    """NA: keep source tax, expose the source rate; VAT/GST fields are not applicable."""
    return (
        df.withColumn("tax_amount", rules.naSalesTaxAmount(F.col("SourceTaxAmount")))
        .withColumn("tax_rate", F.coalesce(F.col("SourceTaxRate"), F.lit(0)))
        .withColumn("is_reverse_charge", F.lit(False))
        .withColumn("vat_rate_applied", F.lit(None).cast("decimal(5,2)"))
        .withColumn("gst_rate_applied", F.lit(None).cast("decimal(5,2)"))
        .withColumn("is_price_inclusive", F.lit(None).cast("boolean"))
    )


# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_sale.loadRegion(spark, catalog, p, REGION_CODE, PACKAGE_NAME, run, batchId, naTaxRule)
    print(result)
