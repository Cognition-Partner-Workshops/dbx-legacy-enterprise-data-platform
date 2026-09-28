# Databricks notebook source
# MAGIC %md
# MAGIC # SLS_Load_PromotionRedemption
# MAGIC Legacy package `ssis/11_sales/SLS_Load_PromotionRedemption.dtsx`: attribute redemptions to
# MAGIC promotions inside the region's attribution window (NA +30 days, APAC +14, EU none; STRICT mode
# MAGIC uses the promotion window everywhere), summarise into `Aggregate.Promotion Effectiveness`
# MAGIC -> `gold.agg_promotion_effectiveness`, keep spill in `work.PromotionSpill` -> `silver.work_promotion_spill`
# MAGIC and flag over-budget promotions.
# MAGIC
# MAGIC Control flow: Log Package Start -> Truncate work_PromotionRedemption -> Attribute Redemptions
# MAGIC (Classify Attribution / Route Spill / Summarise Promotion) -> Flag Over Budget Promotions ->
# MAGIC Log Row Counts -> Log Package Success.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402

import sales_common as sc  # noqa: E402
import sales_schemas as schemas  # noqa: E402
import sales_promotion as promo  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "SLS_Load_PromotionRedemption"
ctx = sc.resolveContext(spark, dbutils, params, control, PACKAGE_NAME, (("AttributionMode", "REGIONAL"),))
if sc.shouldSkipForRestart(ctx.restartFromStep, PACKAGE_NAME):
    dbutils.notebook.exit("Skipped: RestartFromStep=%s" % ctx.restartFromStep)

attributionMode = (sc.parseOptional(sc.getWidget(dbutils, "AttributionMode", "REGIONAL")) or "REGIONAL").upper()
if attributionMode not in promo.ATTRIBUTION_MODES:
    raise ValueError("AttributionMode must be REGIONAL or STRICT, got %r" % attributionMode)


def t(schema, table):
    return naming.table(ctx.catalog, schema, table)


# COMMAND ----------

summary = {"package": PACKAGE_NAME, "batchId": ctx.batchId, "attributionMode": attributionMode}
with sc.legacyPackageRun(spark, control, ctx, PACKAGE_NAME) as run:
    pid = run.packageExecutionId
    spillName = t("silver", "work_promotion_spill")
    targetName = t("gold", "agg_promotion_effectiveness")
    sc.ensureTable(spark, spillName, schemas.WORK_PROMOTION_SPILL)
    sc.ensureTable(spark, targetName, schemas.AGG_PROMOTION_EFFECTIVENESS, partitionBy=("RegionCode",))

    run.currentTask = "Attribute Redemptions"
    promotions = sc.batchFilter(promo.legacyPromotions(spark.table(t("silver", "stg_promotion"))),
                                "LoadBatchId", ctx.batchId, ctx.reloadFullHistory)
    redemptions = promo.legacyRedemptions(spark.table(t("silver", "stg_promotion_redemption")))
    joined = promo.joinRedemptions(promotions, redemptions)
    classified = promo.classifyAttribution(joined, attributionMode).withColumn("BatchId", F.lit(ctx.batchId).cast("long"))
    attributed, spill = promo.splitSpill(classified)
    attributed = attributed.cache()
    attributedRows = attributed.count()
    run.currentTask = "Route Spill"
    spillRows = sc.overwriteTable(spill, spillName, schemas.WORK_PROMOTION_SPILL)
    run.rowsRead = attributedRows + spillRows
    summary["spillRedemptionCount"] = spillRows

    run.currentTask = "Summarise Promotion"
    summarised = promo.summarisePromotion(attributed)
    run.currentTask = "Flag Over Budget Promotions"
    flagged = promo.flagOverBudget(summarised, promotions)
    target = promo.toPromotionEffectiveness(flagged, ctx.batchId)
    mergeMetrics = sc.mergeInto(spark, target, targetName, schemas.AGG_PROMOTION_EFFECTIVENESS,
                                ("RegionCode", "PromotionId"))
    run.rowsInserted = int(mergeMetrics["num_affected_rows"] if mergeMetrics["num_affected_rows"] is not None
                           else summarised.count())
    attributed.unpersist()

    run.currentTask = "Log Row Counts"
    control.logRowCount(spark, ctx.catalog, pid, "Aggregate.Promotion Effectiveness",
                        sourceRowCount=run.rowsRead, targetRowCount=run.rowsInserted,
                        insertRowCount=mergeMetrics["num_inserted_rows"],
                        updateRowCount=mergeMetrics["num_updated_rows"], rejectRowCount=run.rowsRejected)
    control.logRowCount(spark, ctx.catalog, pid, "work.PromotionSpill",
                        sourceRowCount=run.rowsRead, targetRowCount=spillRows)
    run.currentTask = "Log Package Success"
    summary.update({"status": "Succeeded", "rowsRead": run.rowsRead, "rowsInserted": run.rowsInserted})

print(summary)

# COMMAND ----------

dbutils.notebook.exit(str(summary))
