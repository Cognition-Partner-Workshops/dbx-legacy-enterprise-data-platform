# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_LoyaltyPoints
# MAGIC Port of `ssis/08_facts/FACT_Load_LoyaltyPoints.dtsx` (`build_fact_packages.py::build_fact_load_loyalty_points`).
# MAGIC
# MAGIC * Source `silver.stg_loyalty_points` (`LastModifiedAt` watermark); natural key `LoyaltyEventBusinessKey`.
# MAGIC * Lookups: customer (SCD2 on event date, miss -> inferred member), promotion via `RedemptionReference` (-2 when absent), stock item not carried by the source (-2).
# MAGIC * Regional loyalty rules: point cash value NA 1.0c / EU 0.8c / APAC 1.2c, APAC earns x2, expiry NA 24 / EU 36 / APAC 18 months unless the source supplies an expiry date.
# MAGIC * `Expire Points` step: after the MERGE, movements whose expiry date is on/before BusinessDate and still carry liability are written off to `breakage_amount`.
# MAGIC * Target `gold.fact_loyalty_points` (liquid-clustered `movement_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_LoyaltyPoints"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Loyalty Points"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_loyalty_points")))

# COMMAND ----------

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_loyalty_points",
    sourceTable="stg_loyalty_points", sourceDateCol="EventDate", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="LoyaltyEventBusinessKey", naturalKeyCols=["LoyaltyEventBusinessKey"],
    surrogateKeyCol="loyalty_points_key", dateKeyCol="movement_date_key",
    validation=lambda df: F.col("PointsQuantity").isNull() | F.col("EventDate").isNull() | F.col("CustomerBusinessKey").isNull(),
    lookups=[
        fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", "EventDate", onMiss=fact_load.ON_MISS_INFER),
        fact_load.LookupSpec("Promotion", "RedemptionReference", "promotion_key", notApplicableWhenNull=True),
    ],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    points = F.col("PointsQuantity")
    liability = rules.loyaltyPointCashValue(points, F.col("RegionCode"))
    return df.select(
        F.col("EventDate").cast("date").alias("movement_date_key"),
        rules.loyaltyExpiryDate(F.col("EventDate"), F.col("RegionCode"), F.col("ExpiryDate")).alias("points_expiry_date_key"),
        "customer_key", "promotion_key", F.lit(fc.NOT_APPLICABLE_KEY).alias("stock_item_key"),
        F.col("RegionCode").alias("region_code"),
        F.col("LoyaltyMemberId").alias("loyalty_account_number"), F.col("LoyaltyProgramCode").alias("loyalty_scheme_code"),
        F.col("LoyaltyEventBusinessKey").alias("movement_reference"), F.col("EventTypeCode").alias("movement_type_code"),
        F.col("SourceInvoiceNumber").alias("invoice_number"),
        points.cast("int").alias("points_delta"), F.col("PointsBalanceAfter").cast("int").alias("points_balance_after"),
        rules.loyaltyEarnMultiplier(F.col("RegionCode"), F.col("LoyaltyProgramCode")).alias("bonus_multiplier"),
        rules.money(F.col("QualifyingSpendAmount")).alias("qualifying_spend_amount"),
        liability.alias("point_liability_amount"),
        rules.money(F.coalesce(F.col("QualifyingSpendAmountUsd"), F.lit(0))).alias("qualifying_spend_reporting"),
        F.lit(None).cast(rules.MONEY).alias("breakage_amount"),
        F.col("TierCode").alias("tier_at_movement_code"), F.col("ExpiryRuleCode").alias("expiry_rule_code"),
        F.col("customer_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )


def expirePoints(spark, catalog, factRows, merged):
    fullName = naming.table(catalog, "gold", "fact_loyalty_points")
    versionBefore = fc.tableVersion(spark, fullName)
    spark.sql(
        "UPDATE %s SET breakage_amount = point_liability_amount, batch_id = %d, load_datetime = current_timestamp() "
        "WHERE points_expiry_date_key <= DATE'%s' AND breakage_amount IS NULL AND points_delta > 0"
        % (fullName, batchId, p["businessDate"].isoformat())
    )
    expired = fc.lastOperationMetrics(spark, fullName, versionBefore).get("numUpdatedRows", 0)
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact.Loyalty Points expiry", updateRowCount=expired)

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform, afterMerge=expirePoints)
    print(result)
