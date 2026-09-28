# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_Order
# MAGIC Port of `ssis/08_facts/FACT_Load_Order.dtsx` (`build_fact_packages.py::build_fact_load_order`).
# MAGIC
# MAGIC * Source `silver.stg_order_line` x `silver.stg_order` (header) in the `Fact.Order` watermark window (`LoadedAtUtc`); natural key `OrderBusinessKey|LineNumber`.
# MAGIC * Lookups: customer (SCD2 on order date, miss -> inferred member + late-arriving queue), stock item (miss -> `gold.fact_fact_load_hold`, retry budget `HoldRetryLimit`=3 for NA / regional default), salesperson (-1), promotion (-2 when absent).
# MAGIC * Held rows are re-read every run (`LoadHeldRows`), retried and abandoned after the retry budget with a `HOLD_EXPIRED` reject.
# MAGIC * Derived: quantity outstanding, backorder flag, fill-rate %, gross/net/tax amounts, reporting amount from the source FX rate.
# MAGIC * Target `gold.fact_order` (liquid-clustered `order_date_key, region_code`), MERGE on `order_key` (kept stable per natural key).

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

PACKAGE_NAME = "FACT_Load_Order"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Order"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_order")))

# COMMAND ----------

def orderSource(df):
    header = fc.readTable(spark, catalog, "silver", "stg_order").select(
        "OrderBusinessKey", F.col("CustomerBusinessKey").alias("HeaderCustomerBusinessKey"), "SalespersonBusinessKey", "BackorderOrderBusinessKey",
        "ExpectedDeliveryDate", "CustomerPurchaseOrderNumber", "IsUndersupplyBackordered", "SalesChannelCode", "SalesTerritoryCode",
        "OrderStatusCode", F.col("RegionCode").alias("HeaderRegionCode"), F.col("SourceOrderId").alias("OrderNumber"), F.col("PromotionBusinessKey").alias("HeaderPromotionBusinessKey"),
    )
    return (
        df.join(header, "OrderBusinessKey", "left")
        .withColumn("CustomerBusinessKey", F.col("HeaderCustomerBusinessKey"))
        .withColumn("RegionCode", F.coalesce(F.col("HeaderRegionCode"), F.lit(None)))
        .withColumn("PromotionCode", F.coalesce(F.col("PromotionBusinessKey"), F.col("HeaderPromotionBusinessKey")))
        .withColumn("OrderNumber", F.coalesce(F.col("OrderNumber"), F.col("OrderBusinessKey")))
    )


SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_order",
    sourceTable="stg_order_line", sourceDateCol="OrderDate", sourceTimestampCol="LoadedAtUtc",
    businessKeyCol="OrderLineBusinessKey", naturalKeyCols=["OrderBusinessKey", "LineNumber"],
    surrogateKeyCol="order_key", dateKeyCol="order_date_key",
    sourceFilter=orderSource,
    validation=lambda df: F.col("OrderedQuantity").isNull() | F.col("OrderDate").isNull() | F.col("StockItemBusinessKey").isNull(),
    lookups=[
        fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", "OrderDate", onMiss=fact_load.ON_MISS_INFER),
        fact_load.LookupSpec("Stock Item", "StockItemBusinessKey", "stock_item_key", "OrderDate", onMiss=fact_load.ON_MISS_HOLD),
        fact_load.LookupSpec("Salesperson", "SalespersonBusinessKey", "salesperson_key", "OrderDate"),
        fact_load.LookupSpec("Promotion", "PromotionCode", "promotion_key", notApplicableWhenNull=True),
    ],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    qty, picked = F.col("OrderedQuantity"), F.coalesce(F.col("PickedQuantity"), F.lit(0))
    fx = F.when(F.col("NetLineAmount").isNull() | (F.col("NetLineAmount") == 0), F.lit(None)).otherwise(F.col("NetLineAmountUsd") / F.col("NetLineAmount"))
    return df.select(
        F.col("OrderDate").cast("date").alias("order_date_key"),
        F.col("ExpectedDeliveryDate").cast("date").alias("expected_delivery_date_key"),
        "customer_key", "stock_item_key", "salesperson_key", "promotion_key",
        F.col("RegionCode").alias("region_code"),
        F.col("OrderNumber").alias("order_number"), F.col("LineNumber").alias("order_line_number"),
        F.col("CustomerPurchaseOrderNumber").alias("customer_purchase_order_number"),
        F.col("BackorderOrderBusinessKey").alias("backorder_order_number"),
        F.col("SalesChannelCode").alias("sales_channel_code"), F.col("SalesTerritoryCode").alias("sales_territory_code"),
        F.col("PackageTypeCode").alias("package_type_code"),
        F.col("TransactionCurrencyCode").alias("transaction_currency_code"),
        qty.cast("decimal(18,4)").alias("quantity_ordered"), picked.cast("decimal(18,4)").alias("quantity_picked"),
        rules.orderQuantityOutstanding(qty, picked).cast("decimal(18,4)").alias("quantity_outstanding"),
        rules.orderIsBackordered(qty, picked, F.col("BackorderOrderBusinessKey")).alias("backordered_flag"),
        rules.orderFillRatePercent(qty, picked).alias("fill_rate_percent"),
        rules.money(F.col("UnitPriceAmount")).alias("unit_price"),
        rules.money(F.coalesce(F.col("GrossLineAmount"), qty * F.col("UnitPriceAmount"))).alias("gross_order_amount"),
        rules.money(F.coalesce(F.col("LineDiscountAmount"), F.lit(0))).alias("line_discount_amount"),
        rules.money(F.col("NetLineAmount")).alias("net_order_amount"),
        rules.money(F.coalesce(F.col("TaxAmount"), F.lit(0))).alias("tax_amount"),
        F.coalesce(F.col("TaxRatePercent"), F.lit(0)).cast("decimal(5,2)").alias("tax_rate"),
        fx.cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(F.col("NetLineAmountUsd")).alias("net_order_amount_reporting"),
        F.col("OrderStatusCode").alias("order_status_code"), F.col("LineStatusCode").alias("line_status_code"),
        F.col("DqStatusCode").alias("dq_status_code"),
        F.col("customer_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform)
    print(result)
