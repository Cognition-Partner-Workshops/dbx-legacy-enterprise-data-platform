# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_Shipment
# MAGIC Port of `ssis/08_facts/FACT_Load_Shipment.dtsx` (`build_fact_packages.py::build_fact_load_shipment`).
# MAGIC
# MAGIC * Source `silver.stg_shipment` (`LoadedAtUtc` watermark); natural key `ShipmentBusinessKey`.
# MAGIC * Lookups: customer (SCD2 on shipped date, miss -> inferred member), carrier and ship-from warehouse site (type-1, -1).
# MAGIC * `Update Shipment Milestones In Place`: existing shipments (matched on `wwi_shipment_id` through the stable surrogate key) only get their milestone / lag / status columns refreshed.
# MAGIC * `Recalculate Carrier Performance`: on-time flag, delivery latency, customs hold days, milestone status (`shipmentMilestoneStatus`).
# MAGIC * Target `gold.fact_shipment` (liquid-clustered `despatch_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_Shipment"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Shipment"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_shipment")))

# COMMAND ----------

MILESTONE_UPDATE_COLS = [
    "delivery_confirmed_date_key", "milestone_status_code", "despatch_to_delivery_lag_days", "delivery_latency_hours",
    "customs_hold_days", "on_time_delivery_flag", "shipment_status_code", "last_scan_event_code", "milestone_complete_flag",
    "lineage_key", "batch_id", "package_execution_id", "load_datetime",
]


def shipmentSource(df):
    delivered = F.when(F.col("DeliveryLatencyHours").isNotNull(), F.col("ShippedDate").cast("timestamp") + F.expr("make_interval(0, 0, 0, 0, DeliveryLatencyHours, 0, 0)"))
    return df.withColumn("DeliveredAt", delivered).withColumn("ShippedDateOnly", F.col("ShippedDate").cast("date"))


SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_shipment",
    sourceTable="stg_shipment", sourceDateCol="ShippedDateOnly", sourceTimestampCol="LoadedAtUtc",
    businessKeyCol="ShipmentBusinessKey", naturalKeyCols=["ShipmentBusinessKey"],
    surrogateKeyCol="shipment_key", dateKeyCol="despatch_date_key",
    sourceFilter=shipmentSource,
    validation=lambda df: F.col("ShippedDate").isNull() | F.col("CustomerBusinessKey").isNull(),
    lookups=[
        fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", "ShippedDateOnly", onMiss=fact_load.ON_MISS_INFER),
        fact_load.LookupSpec("Carrier", "CarrierCode", "carrier_key"),
        fact_load.LookupSpec("Warehouse Site", "ShipFromWarehouseCode", "warehouse_site_key"),
    ],
    updateCols=MILESTONE_UPDATE_COLS,
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    delivered = F.col("DeliveredAt")
    return df.select(
        F.col("ShippedDateOnly").alias("despatch_date_key"),
        delivered.cast("date").alias("delivery_confirmed_date_key"),
        "customer_key", "carrier_key", "warehouse_site_key",
        F.col("RegionCode").alias("region_code"),
        F.col("ShipmentBusinessKey").alias("wwi_shipment_id"), F.col("ShipmentReference").alias("despatch_note_number"),
        F.col("SaleBusinessKey").alias("invoice_number"), F.col("CustomsDeclarationRef").alias("customs_declaration_number"),
        F.col("ServiceLevelCode").alias("service_level_code"), F.col("DeliveryRouteCode").alias("delivery_route_code"),
        F.col("ShipToCountryCode").alias("ship_to_country_code"), F.col("ShipToPostalCodeStandardized").alias("ship_to_postal_code"),
        F.col("TotalWeightKg").cast("decimal(18,3)").alias("total_weight_kg"), F.col("TotalVolumeM3").cast("decimal(18,3)").alias("total_volume_m3"),
        F.col("FreightCurrencyCode").alias("transaction_currency_code"),
        rules.money(F.col("FreightChargeAmount")).alias("freight_charge"),
        rules.safeDivide(F.col("FreightChargeAmountUsd"), F.col("FreightChargeAmount")).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(F.col("FreightChargeAmountUsd")).alias("freight_charge_reporting"),
        F.col("DeliveryLatencyHours").cast("decimal(9,2)").alias("delivery_latency_hours"),
        F.ceil(F.col("DeliveryLatencyHours") / 24).cast("int").alias("despatch_to_delivery_lag_days"),
        F.when(F.col("CustomsRequiredFlag") == True, F.greatest(F.lit(0), F.ceil(F.col("DeliveryLatencyHours") / 24) - 2)).otherwise(F.lit(0)).cast("int").alias("customs_hold_days"),
        F.coalesce(F.col("OnTimeDeliveryFlag"), F.lit(False)).alias("on_time_delivery_flag"),
        F.coalesce(F.col("CustomsRequiredFlag"), F.lit(False)).alias("customs_required_flag"),
        F.col("ShipmentStatusCode").alias("shipment_status_code"), F.col("LastScanEventCode").alias("last_scan_event_code"),
        rules.shipmentMilestoneStatus(F.col("LastScanEventCode"), delivered).alias("milestone_status_code"),
        delivered.isNotNull().alias("milestone_complete_flag"),
        F.col("customer_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform)
    print(result)
