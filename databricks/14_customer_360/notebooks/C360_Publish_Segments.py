# Databricks notebook source
# MAGIC %md
# MAGIC # C360_Publish_Segments
# MAGIC Migrated from `ssis/14_customer_360/C360_Publish_Segments.dtsx` (project `WWI_Customer360`).
# MAGIC Snapshots the previous segments, assigns segments from the Customer 360 mart tables (model SEG-2021A), optionally drops suppressed rows, measures migration and publishes `Customer360.CustomerSegment` -> `gold.c360_customer_segment` with the reporting view `gold.rpt_customer_segment` repointed atomically (view / table swap).
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
from c360_lib import segments as S  # noqa: E402

PACKAGE_NAME = "C360_Publish_Segments"
STEP_NAME = "Customer 360 Publish"

# COMMAND ----------

ctx = R.getJobContext(dbutils)
segmentModelVersion = R.widget(dbutils, "SegmentModelVersion", S.DEFAULT_SEGMENT_MODEL_VERSION)
publishSuppressedRows = R.asBool(R.widget(dbutils, "PublishSuppressedRows", "True"))
catalog = ctx.catalog

if R.shouldSkipForRestart(ctx, PACKAGE_NAME):
    dbutils.notebook.exit(f"SKIPPED (RestartFromStep={ctx.restartFromStep})")

# COMMAND ----------

with R.packageLifecycle(spark, ctx, PACKAGE_NAME, STEP_NAME) as run:
    profile = R.readTable(spark, T.table(catalog, "Customer360.CustomerProfile"))
    rollingMetric = R.readTable(spark, T.table(catalog, "Customer360.CustomerRollingMetric"))
    churnFlag = R.readTable(spark, T.table(catalog, "Customer360.CustomerChurnFlag"))
    loyaltyOverlay = R.readTable(spark, T.table(catalog, "Customer360.LoyaltyOverlay"))

    workSegment = T.table(catalog, "work.CustomerSegment")
    previousTable = T.table(catalog, "work.CustomerSegmentPrevious")

    # Snapshot Previous Segments (work.CustomerSegment -> work.CustomerSegmentPrevious)
    assigned = S.assignSegments(profile, rollingMetric, churnFlag, loyaltyOverlay, segmentModelVersion)
    previousSource = R.readTableOrEmpty(spark, workSegment, assigned)
    R.overwriteTable(previousSource.select("CustomerId", "SegmentCode", "AssignedAtUtc"), previousTable)
    previous = R.readTable(spark, previousTable)

    # Truncate work_CustomerSegment + Assign Segments (+ Drop Suppressed Rows when !PublishSuppressedRows)
    assigned = S.dropSuppressedRows(assigned, publishSuppressedRows)
    run.rowsRead = R.overwriteTable(assigned, workSegment)
    assigned = R.readTable(spark, workSegment)

    segmentCount, suppressedCount, migratedCount = S.measureSegmentMigration(assigned, previous)
    print(f"SegmentCount={segmentCount} SuppressedCount={suppressedCount} MigratedCount={migratedCount}")

    # Publish Segments: derive movement, atomically replace the mart table, repoint the view
    published = R.stamp(S.deriveSegmentMovement(assigned, previous), ctx)
    target = T.table(catalog, "Customer360.CustomerSegment")
    run.rowsInserted = R.overwriteTable(published, target)
    R.replaceView(spark, T.table(catalog, "Report.vw_CustomerSegment"), target)

    R.logRowCounts(spark, ctx, run, "Customer360.CustomerSegment")
