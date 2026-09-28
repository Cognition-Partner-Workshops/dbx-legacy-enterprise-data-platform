# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_Purchase
# MAGIC Port of `ssis/08_facts/FACT_Load_Purchase.dtsx` (`build_fact_packages.py::build_fact_load_purchase`).
# MAGIC
# MAGIC * Source `silver.stg_purchase` (`LastModifiedAt` watermark); natural key `PurchaseOrderNumber|PurchaseOrderLineNumber`.
# MAGIC * Lookups: supplier (SCD2 on order date, miss -> inferred member + late-arriving queue), stock item (miss -> `gold.fact_fact_load_hold`, `Reload Held Purchase Lines` on the next run).
# MAGIC * Derived: extended amount, landed cost (regional freight uplift), recoverable tax, reporting amount from the source USD figure.
# MAGIC * Region comes from `SupplierRegionCode`; retry budget follows the regional hold limits.
# MAGIC * Target `gold.fact_purchase` (liquid-clustered `order_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_Purchase"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Purchase"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_purchase")))

# COMMAND ----------

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_purchase",
    sourceTable="stg_purchase", sourceDateCol="OrderPlacedDate", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="PurchaseOrderLineBusinessKey", naturalKeyCols=["PurchaseOrderNumber", "PurchaseOrderLineNumber"],
    surrogateKeyCol="purchase_key", dateKeyCol="order_date_key", regionCol="SupplierRegionCode",
    validation=lambda df: F.col("QuantityOrdered").isNull() | F.col("UnitCostAmount").isNull() | F.col("OrderPlacedDate").isNull(),
    lookups=[
        fact_load.LookupSpec("Supplier", "SupplierBusinessKey", "supplier_key", "OrderPlacedDate", onMiss=fact_load.ON_MISS_INFER),
        fact_load.LookupSpec("Stock Item", "StockItemBusinessKey", "stock_item_key", "OrderPlacedDate", onMiss=fact_load.ON_MISS_HOLD),
    ],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    qty, cost = F.col("QuantityOrdered"), F.col("UnitCostAmount")
    extended = rules.money(qty * cost)
    return df.select(
        F.col("OrderPlacedDate").cast("date").alias("order_date_key"),
        F.col("ExpectedReceiptDate").cast("date").alias("expected_receipt_date_key"),
        "supplier_key", "stock_item_key",
        F.col("SupplierRegionCode").alias("region_code"),
        F.col("PurchaseOrderNumber").alias("purchase_order_number"), F.col("PurchaseOrderLineNumber").alias("purchase_order_line_number"),
        F.col("BuyerCode").alias("buyer_code"),
        F.col("TransactionCurrency").alias("transaction_currency_code"),
        qty.cast("decimal(18,4)").alias("quantity_ordered"),
        rules.money(cost).alias("unit_cost"), extended.alias("extended_amount"),
        rules.money(F.coalesce(F.col("FreightAmount"), F.lit(0))).alias("freight_amount"),
        rules.landedCostAmount(qty, cost, F.col("FreightAmount"), F.col("SupplierRegionCode")).alias("landed_cost_amount"),
        rules.money(F.coalesce(F.col("RecoverableTaxAmount"), F.lit(0))).alias("recoverable_tax_amount"),
        rules.safeDivide(F.col("ExtendedAmountUsd"), extended).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(F.col("ExtendedAmountUsd")).alias("extended_amount_reporting"),
        F.col("ThreeWayMatchStatusCode").alias("three_way_match_status_code"),
        F.col("supplier_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform)
    print(result)
