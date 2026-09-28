# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_Return
# MAGIC Port of `ssis/08_facts/FACT_Load_Return.dtsx` (`build_fact_packages.py::build_fact_load_return`).
# MAGIC
# MAGIC * Source `silver.stg_return` (`LoadedAtUtc` watermark); natural key `ReturnLineBusinessKey`.
# MAGIC * Lookups: customer and stock item (SCD2 on returned date, miss -> inferred member / -1), return reason (type-1, -1), original sale from `gold.fact_sale` on `SaleLineBusinessKey` == `invoice_number|invoice_line_number` (miss -> NULL, flagged).
# MAGIC * Regional restocking fee: NA free within 30 days then 15 %, EU free within the 14-day withdrawal window, APAC flat 5.00 per unit; source fee kept when supplied.
# MAGIC * `Write Sale Reversal Rows`: after the MERGE, each return linked to an original sale inserts a negated `REV` row into `gold.fact_sale` scaled by returned quantity / sold quantity.
# MAGIC * Target `gold.fact_return` (liquid-clustered `return_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_Return"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Return"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_return")))

# COMMAND ----------

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_return",
    sourceTable="stg_return", sourceDateCol="ReturnedDate", sourceTimestampCol="LoadedAtUtc",
    businessKeyCol="ReturnLineBusinessKey", naturalKeyCols=["ReturnLineBusinessKey"],
    surrogateKeyCol="return_key", dateKeyCol="return_date_key",
    validation=lambda df: F.col("ReturnedQuantity").isNull() | F.col("ReturnedDate").isNull() | F.col("StockItemBusinessKey").isNull(),
    lookups=[
        fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", "ReturnedDate", onMiss=fact_load.ON_MISS_INFER),
        fact_load.LookupSpec("Stock Item", "StockItemBusinessKey", "stock_item_key", "ReturnedDate"),
        fact_load.LookupSpec("Return Reason", "ReturnReasonCode", "return_reason_key"),
    ],
    extraClusterCols=["region_code"],
)


def originalSales(spark, catalog):
    fullName = naming.table(catalog, "gold", "fact_sale")
    if not fc.tableExists(spark, fullName):
        return None
    return (
        spark.table(fullName)
        .where(F.coalesce(F.col("correction_type_code"), F.lit(fc.CORRECTION_ORIGINAL)) == fc.CORRECTION_ORIGINAL)
        .select(
            F.concat_ws("|", F.col("invoice_number").cast("string"), F.col("invoice_line_number").cast("string")).alias("SaleLineBusinessKey"),
            F.col("sale_key").alias("original_sale_key"), F.col("invoice_date_key").alias("original_invoice_date_key"),
            F.col("invoice_number").alias("original_invoice_number"), F.col("invoice_line_number").alias("original_invoice_line_number"),
            F.col("unit_price").alias("original_unit_price"), F.col("quantity").alias("original_quantity"),
            F.col("cost_of_sale_amount").alias("original_cost_of_sale"),
        )
    )


def transform(spark, catalog, df):
    sales = originalSales(spark, catalog)
    if sales is not None:
        df = df.join(sales, "SaleLineBusinessKey", "left")
    else:
        for c, t in (("original_sale_key", "bigint"), ("original_invoice_date_key", "date"), ("original_invoice_number", "string"), ("original_invoice_line_number", "int"), ("original_unit_price", rules.MONEY), ("original_quantity", "decimal(18,4)"), ("original_cost_of_sale", rules.MONEY)):
            df = df.withColumn(c, F.lit(None).cast(t))
    qty = F.col("ReturnedQuantity")
    fee = F.coalesce(F.col("RestockingFeeAmount"), rules.restockingFeeAmount(F.col("RegionCode"), F.col("original_invoice_date_key"), F.col("ReturnedDate"), qty, F.col("original_unit_price")))
    gross = F.coalesce(F.col("RefundAmount") + F.coalesce(F.col("RestockingFeeAmount"), F.lit(0)), qty * F.col("original_unit_price"))
    net = rules.money(gross - fee)
    return df.select(
        F.col("ReturnedDate").cast("date").alias("return_date_key"),
        F.col("original_invoice_date_key"), F.col("ProcessedDate").cast("date").alias("credit_issued_date_key"),
        "customer_key", "stock_item_key", "return_reason_key", "original_sale_key",
        F.col("RegionCode").alias("region_code"),
        F.col("RmaNumber").alias("rma_number"), F.col("ReturnLineBusinessKey").alias("rma_line_number"),
        F.col("original_invoice_number"), F.col("original_invoice_line_number"),
        F.col("ReturnReasonCode").alias("return_reason_code"), F.col("ReturnReasonGroupCode").alias("return_reason_group_code"),
        qty.cast("decimal(18,4)").alias("quantity_returned"),
        F.coalesce(F.col("RestockedQuantity"), F.lit(0)).cast("decimal(18,4)").alias("quantity_restocked"),
        F.coalesce(F.col("ScrappedQuantity"), F.lit(0)).cast("decimal(18,4)").alias("quantity_scrapped"),
        F.col("TransactionCurrency").alias("transaction_currency_code"),
        rules.money(gross).alias("gross_return_amount"), rules.money(fee).alias("restocking_fee_amount"), net.alias("net_credit_amount"),
        rules.safeDivide(F.col("RefundAmountUsd"), F.col("RefundAmount")).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(F.col("RefundAmountUsd")).alias("net_credit_amount_reporting"),
        rules.money(rules.safeDivide(F.col("original_cost_of_sale"), F.col("original_quantity")) * qty).alias("cost_of_returned_goods"),
        F.coalesce(F.col("DaysSinceSale"), F.datediff(F.col("ReturnedDate"), F.col("original_invoice_date_key"))).cast("int").alias("days_since_invoice"),
        (F.col("InspectionResultCode") == "FAULTY").alias("faulty_goods_flag"),
        F.col("InspectionResultCode").alias("disposition_code"),
        F.col("customer_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )


def writeSaleReversals(spark, catalog, factRows, merged):
    import fact_sale

    saleName = naming.table(catalog, "gold", "fact_sale")
    linked = factRows.where(F.col("original_sale_key").isNotNull()).select("original_sale_key", "quantity_returned", "return_key")
    if not fc.tableExists(spark, saleName) or linked.limit(1).count() == 0:
        return
    sale = spark.table(saleName).alias("s").join(linked.alias("r"), F.col("s.sale_key") == F.col("r.original_sale_key"))
    existing = spark.table(saleName).where(F.col("correction_type_code") == fc.CORRECTION_REVERSAL).select(F.col("corrected_sale_key").alias("_done"))
    sale = sale.join(existing, F.col("s.sale_key") == F.col("_done"), "left_anti")
    fraction = F.least(F.lit(1.0), rules.safeDivide(F.col("r.quantity_returned"), F.col("s.quantity")))
    cols = []
    for c in fact_sale.FACT_COLUMNS:
        if c in ("sale_key", "batch_id", "package_execution_id", "load_datetime"):
            continue
        if c == "correction_type_code":
            cols.append(F.lit(fc.CORRECTION_REVERSAL).alias(c))
        elif c == "corrected_sale_key":
            cols.append(F.col("s.sale_key").alias(c))
        elif c in ("quantity", "quantity_base_uom"):
            cols.append((F.col("s." + c) * -1 * fraction).cast("decimal(18,4)").alias(c))
        elif c in fact_sale.MEASURE_COLUMNS:
            cols.append(rules.money(F.col("s." + c) * -1 * fraction).alias(c))
        else:
            cols.append(F.col("s." + c).alias(c))
    rows = fc.loadAuditColumns(sale.select(*cols, F.col("r.return_key").alias("_return_key")), batchId, run.packageExecutionId)
    rows = fc.assignSurrogateKeys(spark, saleName, rows, "sale_key", ["_return_key"]).drop("_return_key")
    inserted = fc.appendRows(spark, saleName, rows.select(*fact_sale.FACT_COLUMNS))
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact.Sale return reversals", insertRowCount=inserted)

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform, afterMerge=writeSaleReversals)
    print(result)
