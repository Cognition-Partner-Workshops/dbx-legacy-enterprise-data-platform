# Databricks notebook source
# MAGIC %md
# MAGIC # C360_Build_CustomerProfile
# MAGIC Migrated from `ssis/14_customer_360/C360_Build_CustomerProfile.dtsx` (project `WWI_Customer360`).
# MAGIC Standardises addresses, resolves the identity graph, applies regional consent rules, derives survivorship attributes, assigns households and publishes `Customer360.CustomerProfile` -> `gold.c360_customer_profile`.
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

from c360_lib import profile as P  # noqa: E402

PACKAGE_NAME = "C360_Build_CustomerProfile"
STEP_NAME = "Customer 360 Build"

# COMMAND ----------

ctx = R.getJobContext(dbutils)
matchThresholdScore = int(R.widget(dbutils, "MatchThresholdScore", str(P.DEFAULT_MATCH_THRESHOLD)))
rebuildIdentityGraph = R.asBool(R.widget(dbutils, "RebuildIdentityGraph", "False")) or ctx.reloadFullHistory
catalog = ctx.catalog
asOf = ctx.businessDate

if R.shouldSkipForRestart(ctx, PACKAGE_NAME):
    dbutils.notebook.exit(f"SKIPPED (RestartFromStep={ctx.restartFromStep})")

# COMMAND ----------

with R.packageLifecycle(spark, ctx, PACKAGE_NAME, STEP_NAME) as run:
    dimCustomer = R.readTable(spark, T.table(catalog, "Dimension.Customer"))
    factSale = R.readTable(spark, T.table(catalog, "Fact.Sale"))
    factPayment = R.readTable(spark, T.table(catalog, "Fact.Payment"))

    # Feeder work tables (work.CustomerSalesSummary / work.CustomerPaymentSummary /
    # work.CustomerAddressStandardised rows) are rebuilt from the warehouse each run.
    salesSummary = P.buildCustomerSalesSummary(factSale)
    paymentSummary = P.buildCustomerPaymentSummary(factPayment)
    R.overwriteTable(R.stamp(salesSummary, ctx), T.table(catalog, "work.CustomerSalesSummary"))
    R.overwriteTable(R.stamp(paymentSummary, ctx), T.table(catalog, "work.CustomerPaymentSummary"))

    # Truncate work_CustomerProfile happens implicitly: the work table is overwritten below.

    # Standardise Addresses
    addr = P.standardiseAddresses(P.buildAddressStandardisationInput(dimCustomer, asOf))
    addrTable = T.table(catalog, "work.CustomerAddressStandardised")
    R.overwriteTable(R.stamp(addr, ctx), addrTable)
    addr = R.readTable(spark, addrTable)

    # Resolve Identity Graph (MERGE work.CustomerIdentityGraph ... MatchScore >= threshold)
    graphTable = T.table(catalog, "work.CustomerIdentityGraph")
    resolved = P.resolveIdentityGraph(addr, matchThresholdScore)
    if rebuildIdentityGraph or not spark.catalog.tableExists(graphTable):
        R.overwriteTable(resolved, graphTable)
    else:
        resolved.createOrReplaceTempView("c360_identity_graph_source")
        spark.sql(f"""
            MERGE INTO {graphTable} AS target
            USING c360_identity_graph_source AS source
              ON target.CustomerId = source.CustomerId
            WHEN MATCHED THEN UPDATE SET
              target.SurvivingCustomerId = source.SurvivingCustomerId,
              target.MatchScore = source.MatchScore,
              target.ResolvedAtUtc = source.ResolvedAtUtc
            WHEN NOT MATCHED THEN INSERT (CustomerId, SurvivingCustomerId, MatchScore, ResolvedAtUtc)
              VALUES (source.CustomerId, source.SurvivingCustomerId, source.MatchScore, source.ResolvedAtUtc)
        """)
    identityGraph = R.readTable(spark, graphTable)

    # Build Customer Profile (data flow) + Assign Households
    source = P.buildProfileSource(dimCustomer, salesSummary, paymentSummary, asOf)
    run.rowsRead = source.count()
    profileDf = P.assignHouseholds(P.buildCustomerProfile(source, identityGraph, asOf), addr)
    workProfile = T.table(catalog, "work.CustomerProfile")
    run.rowsInserted = R.overwriteTable(R.stamp(profileDf, ctx), workProfile)

    # Publish Customer 360 Profile (Integration.usp_PublishCustomer360Profile is not in the
    # repository; the publish is implemented as a full current-state rebuild of the mart table).
    run.rowsUpdated = R.overwriteTable(spark.table(workProfile), T.table(catalog, "Customer360.CustomerProfile"))

    duplicateClusterCount = P.countDuplicateClusters(identityGraph)
    print(f"ProfileCount={run.rowsInserted} DuplicateClusterCount={duplicateClusterCount}")

    R.logRowCounts(spark, ctx, run, "Customer360.CustomerProfile")
