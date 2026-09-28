# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_Movement
# MAGIC Port of `ssis/08_facts/FACT_Load_Movement.dtsx` (`build_fact_packages.py::build_fact_load_movement`).
# MAGIC
# MAGIC * Source `silver.stg_movement` (`LastModifiedAt` watermark); natural key `MovementBusinessKey`.
# MAGIC * Lookups: stock item (SCD2 on movement date, miss -> inferred member + late-arriving queue), from/to warehouse site (type-1, -1; -2 when the location is absent).
# MAGIC * `Link Reversals To Original Movements`: after the MERGE, rows carrying `reverses_wwi_movement_id` are joined back on `wwi_movement_id` to set `reverses_movement_key` / `is_reversal`.
# MAGIC * Derived: signed quantity and value (ISSUE / SCRAP / SALE negative), reason group, inter-warehouse flag.
# MAGIC * Target `gold.fact_movement` (liquid-clustered `movement_date_key, region_code`).

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

PACKAGE_NAME = "FACT_Load_Movement"
STEP_NAME = "Load Facts"
OBJECT_NAME = "Fact.Movement"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
print("catalog=%s batchId=%s businessDate=%s target=%s" % (catalog, batchId, p["businessDate"], naming.table(catalog, "gold", "fact_movement")))

# COMMAND ----------

SPEC = fact_load.FactLoadSpec(
    packageName=PACKAGE_NAME, objectName=OBJECT_NAME, targetTable="fact_movement",
    sourceTable="stg_movement", sourceDateCol="MovementDate", sourceTimestampCol="LastModifiedAt",
    businessKeyCol="MovementBusinessKey", naturalKeyCols=["MovementBusinessKey"],
    surrogateKeyCol="movement_key", dateKeyCol="movement_date_key",
    validation=lambda df: F.col("QuantityMoved").isNull() | F.col("MovementDate").isNull() | F.col("StockItemBusinessKey").isNull(),
    lookups=[
        fact_load.LookupSpec("Stock Item", "StockItemBusinessKey", "stock_item_key", "MovementDate", onMiss=fact_load.ON_MISS_INFER),
        fact_load.LookupSpec("Warehouse Site", "FromLocationCode", "from_warehouse_site_key", notApplicableWhenNull=True),
        fact_load.LookupSpec("Warehouse Site", "ToLocationCode", "to_warehouse_site_key", notApplicableWhenNull=True),
    ],
    extraClusterCols=["region_code"],
)


def transform(spark, catalog, df):
    qty, value, mtype = F.col("QuantityMoved"), F.col("MovementValueAmount"), F.col("MovementTypeCode")
    return df.select(
        F.col("MovementDate").cast("date").alias("movement_date_key"),
        "stock_item_key", "from_warehouse_site_key", "to_warehouse_site_key",
        F.coalesce(F.col("to_warehouse_site_key"), F.col("from_warehouse_site_key")).alias("warehouse_site_key"),
        F.col("RegionCode").alias("region_code"),
        F.col("MovementBusinessKey").alias("wwi_movement_id"), F.col("ReversesMovementKey").alias("reverses_wwi_movement_id"),
        mtype.alias("movement_type_code"), F.col("ReasonCode").alias("reason_code"),
        rules.movementReasonGroup(F.col("ReasonCode"), mtype).alias("reason_group_code"),
        F.col("FromLocationCode").alias("from_location_code"), F.col("ToLocationCode").alias("to_location_code"),
        rules.isInterWarehouse(F.col("FromLocationCode"), F.col("ToLocationCode")).alias("inter_warehouse_flag"),
        rules.movementSignedQuantity(qty, mtype).cast("decimal(18,4)").alias("quantity"),
        rules.movementSignedValue(value, mtype).alias("movement_value"),
        rules.money(rules.safeDivide(value, qty)).alias("unit_cost"),
        F.lit(False).alias("is_reversal"), F.lit(None).cast("bigint").alias("reverses_movement_key"),
        F.col("stock_item_key_miss").alias("inferred_member_flag"),
        "natural_key_hash",
    )


def linkReversals(spark, catalog, factRows, merged):
    from delta.tables import DeltaTable

    fullName = naming.table(catalog, "gold", "fact_movement")
    fact = spark.table(fullName)
    links = (
        fact.alias("r").where(F.col("r.reverses_wwi_movement_id").isNotNull() & F.col("r.reverses_movement_key").isNull())
        .join(fact.alias("o"), F.col("o.wwi_movement_id") == F.col("r.reverses_wwi_movement_id"))
        .select(F.col("r.movement_key").alias("movement_key"), F.col("o.movement_key").alias("original_key"))
    )
    if links.limit(1).count() == 0:
        return
    versionBefore = fc.tableVersion(spark, fullName)
    DeltaTable.forName(spark, fullName).alias("t").merge(links.alias("s"), "t.movement_key = s.movement_key").whenMatchedUpdate(
        set={"reverses_movement_key": "s.original_key", "is_reversal": "true"}
    ).execute()
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact.Movement reversal links", updateRowCount=fc.lastOperationMetrics(spark, fullName, versionBefore).get("numTargetRowsUpdated", 0))

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    result = fact_load.run(spark, catalog, p, SPEC, run, batchId, transform, afterMerge=linkReversals)
    print(result)
