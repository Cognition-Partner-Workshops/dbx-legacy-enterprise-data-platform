# Databricks notebook source
# MAGIC %md
# MAGIC # C360_Build_LoyaltyOverlay
# MAGIC Migrated from `ssis/14_customer_360/C360_Build_LoyaltyOverlay.dtsx` (project `WWI_Customer360`).
# MAGIC Expires aged loyalty points per region, accrues points for new qualifying invoices, recalculates the regional tier ladders, measures tier movement and publishes `Customer360.LoyaltyOverlay` -> `gold.c360_loyalty_overlay`.
# MAGIC
# MAGIC Control framework calls go through the shared `dbx_etl_common` package (session 00); the
# MAGIC business rules live in `../src/c360_lib` so they can be unit tested with local PySpark.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F  # noqa: E402

from c360_lib import runtime as R  # noqa: E402
from c360_lib import tables as T  # noqa: E402

from c360_lib import loyalty as L  # noqa: E402

PACKAGE_NAME = "C360_Build_LoyaltyOverlay"
STEP_NAME = "Customer 360 Build"

# COMMAND ----------

ctx = R.getJobContext(dbutils)
expiryMonths = {
    "NA": int(R.widget(dbutils, "PointsExpiryMonthsNa", str(L.DEFAULT_EXPIRY_MONTHS["NA"]))),
    "EU": int(R.widget(dbutils, "PointsExpiryMonthsEu", str(L.DEFAULT_EXPIRY_MONTHS["EU"]))),
    "APAC": int(R.widget(dbutils, "PointsExpiryMonthsApac", str(L.DEFAULT_EXPIRY_MONTHS["APAC"]))),
}
catalog = ctx.catalog
asOf = ctx.businessDate

if R.shouldSkipForRestart(ctx, PACKAGE_NAME):
    dbutils.notebook.exit(f"SKIPPED (RestartFromStep={ctx.restartFromStep})")

# COMMAND ----------

with R.packageLifecycle(spark, ctx, PACKAGE_NAME, STEP_NAME) as run:
    factSale = R.readTable(spark, T.table(catalog, "Fact.Sale"))
    dimCustomer = R.readTable(spark, T.table(catalog, "Dimension.Customer"))

    # work.LoyaltyOverlayCurrent = the overlay published by the previous run
    overlayTarget = T.table(catalog, "Customer360.LoyaltyOverlay")
    currentTable = T.table(catalog, "work.LoyaltyOverlayCurrent")
    previousOverlay = R.readTable(spark, overlayTarget, required=False)
    if previousOverlay is not None:
        R.overwriteTable(previousOverlay.select("CustomerId", "RegionCode", "TierCode", "ActivePoints"), currentTable)

    # work.LoyaltyQualifyingSale (feeder rebuilt from Fact.Sale) and the persistent ledger
    qualifying = L.buildLoyaltyQualifyingSale(factSale, dimCustomer, asOf)
    R.overwriteTable(R.stamp(qualifying, ctx), T.table(catalog, "work.LoyaltyQualifyingSale"))
    qualifying = R.readTable(spark, T.table(catalog, "work.LoyaltyQualifyingSale"))

    ledgerTable = T.table(catalog, "work.LoyaltyPointLedger")
    emptyLedger = L.accruePoints(qualifying.limit(0), qualifying.limit(0).select(F.col("InvoiceNumber").alias("SourceReference")))
    ledger = R.readTableOrEmpty(spark, ledgerTable, emptyLedger)
    if ctx.reloadFullHistory:
        ledger = spark.createDataFrame([], emptyLedger.schema)

    # Expire Aged Points, then Accrue Points (NOT EXISTS on SourceReference)
    ledger = L.expireAgedPoints(ledger, asOf, expiryMonths)
    accrued = L.accruePoints(qualifying, ledger)
    ledger = ledger.select(*accrued.columns).unionByName(accrued)
    run.rowsRead = R.overwriteTable(ledger, ledgerTable)
    ledger = R.readTable(spark, ledgerTable)

    # Recalculate Tier Ladders -> work.LoyaltyOverlay
    overlayCurrent = R.readTableOrEmpty(spark, currentTable, ledger.select("CustomerId", "RegionCode").withColumn("TierCode", F.lit("NONE")).withColumn("ActivePoints", F.lit(0).cast("decimal(18,2)")))
    overlay = L.recalculateTierLadders(ledger, overlayCurrent)
    workOverlay = T.table(catalog, "work.LoyaltyOverlay")
    R.overwriteTable(overlay, workOverlay)
    overlay = R.readTable(spark, workOverlay)

    tierChangeCount, expiredPointsAmount = L.measureTierMovement(overlay)
    print(f"TierChangeCount={tierChangeCount} ExpiredPointsAmount={expiredPointsAmount}")

    # Publish Loyalty Overlay (data flow: Derive Tier Movement -> Customer360.LoyaltyOverlay)
    run.rowsInserted = R.overwriteTable(R.stamp(L.deriveTierMovement(overlay), ctx), overlayTarget)

    R.logRowCounts(spark, ctx, run, "Customer360.LoyaltyOverlay")
