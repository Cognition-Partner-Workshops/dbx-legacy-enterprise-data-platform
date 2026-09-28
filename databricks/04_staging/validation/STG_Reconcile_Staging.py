# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Reconcile_Staging
# MAGIC Row-count and content-hash reconciliation of every `silver.stg_* / work_* / err_*` table
# MAGIC loaded by the `wwi_04_staging` job against the SQL Server baseline
# MAGIC (`validation/runtime/02_row_count_reconciliation.sql` section 4, extended to the
# MAGIC whole WWI_Staging estate). Baseline figures come from a Delta table
# MAGIC (`baselineTable`, columns `ObjectName, BatchId, BusinessDate, RowCount, RowHash`) or
# MAGIC from the `baselineJson` parameter (a JSON array of the same objects) captured on
# MAGIC SQL Server with the hash query printed at the bottom of this notebook.
# MAGIC Results are written to `etl.row_count_log` through `control.logRowCount`
# MAGIC (SourceRowCount = SQL Server, TargetRowCount = Delta).

# COMMAND ----------

import json
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402

p = params.getJobParams(dbutils)  # noqa: F821


def _widget(name, default):
    try:
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return default


baselineTable = _widget("baselineTable", "")
baselineJson = _widget("baselineJson", "")
failOnMismatch = str(_widget("failOnMismatch", "False")).lower() in ("1", "true", "yes")

# COMMAND ----------

# legacy object -> Delta table (every DEST of the 28 packages)
TARGETS = {
    "stg.ApInvoice": "stg_ap_invoice", "stg.ApInvoiceLine": "stg_ap_invoice_line", "stg.CostCenter": "stg_cost_center",
    "stg.CreditNote": "stg_credit_note", "stg.Currency": "stg_currency", "stg.Customer": "stg_customer",
    "stg.CustomerAddress": "stg_customer_address", "stg.Employee": "stg_employee", "stg.FxRate": "stg_fx_rate",
    "stg.Geography": "stg_geography", "stg.GlJournalLine": "stg_gl_journal_line", "stg.LoyaltyLedger": "stg_loyalty_ledger",
    "stg.Order": "stg_order", "stg.OrderLine": "stg_order_line", "stg.PartnerSale": "stg_partner_sale", "stg.Payment": "stg_payment",
    "stg.PaymentTerms": "stg_payment_terms", "stg.Product": "stg_product", "stg.Promotion": "stg_promotion",
    "stg.PurchaseOrder": "stg_purchase_order", "stg.PurchaseOrderLine": "stg_purchase_order_line", "stg.Return": "stg_return",
    "stg.Sale": "stg_sale", "stg.SaleLine": "stg_sale_line", "stg.SalesTerritory": "stg_sales_territory",
    "stg.Salesperson": "stg_salesperson", "stg.Shipment": "stg_shipment", "stg.ShipmentLine": "stg_shipment_line",
    "stg.StockItem": "stg_stock_item", "stg.StockMovement": "stg_stock_movement", "stg.Supplier": "stg_supplier",
    "stg.TaxRate": "stg_tax_rate", "stg.VendorContract": "stg_vendor_contract", "stg.WebSession": "stg_web_session",
    "work.CustomerDedup": "work_customer_dedup", "work.CustomerAddressStandardized": "work_customer_address_standardized",
    "work.InventoryPositionDaily": "work_inventory_position_daily", "work.PaymentMatched": "work_payment_matched",
    "work.ProductCrosswalk": "work_product_crosswalk", "work.ProductCrosswalkFeed": "work_product_crosswalk_feed",
    "err.RejectedCustomer": "err_rejected_customer", "err.RejectedProduct": "err_rejected_product",
    "err.RejectedSupplier": "err_rejected_supplier", "err.RejectedTransaction": "err_rejected_transaction",
    "err.RejectedFileRow": "err_rejected_file_row", "err.RejectedLookupFailure": "err_rejected_lookup_failure",
    "err.RejectedConstraintViolation": "err_rejected_constraint_violation",
}

# stamps that legitimately differ between the two platforms and are excluded from the hash
VOLATILE_COLUMNS = {"PackageExecutionId", "LoadedAtUtc", "CreatedAtUtc", "UpdatedAtUtc", "LoggedAtUtc", "StgRowId", "WorkRowId", "ErrRowId", "RejectedRowId"}


def rowHash(df):
    """Order-independent content hash: sum of xxhash64 over the pipe-joined business columns in name order."""
    cols = sorted(c for c in df.columns if c not in VOLATILE_COLUMNS)
    joined = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])
    return df.agg(F.coalesce(F.sum(F.xxhash64(joined)), F.lit(0)).alias("RowHash"), F.count(F.lit(1)).alias("RowCount")).first()


def measure(spark, catalog, batchId, businessDate):
    rows = []
    for legacyName, tableName in sorted(TARGETS.items()):
        fqn = naming.table(catalog, "silver", tableName)
        if not spark.catalog.tableExists(fqn):
            rows.append((legacyName, tableName, batchId, businessDate, None, None, None, None, "MISSING_TABLE"))
            continue
        df = spark.table(fqn)
        total = rowHash(df)
        scoped = df
        if "BatchId" in df.columns and batchId:
            scoped = scoped.where(F.col("BatchId") == F.lit(batchId))
        if "BusinessDate" in df.columns and businessDate is not None:
            scoped = scoped.where(F.col("BusinessDate") == F.lit(businessDate))
        batch = rowHash(scoped)
        rows.append((legacyName, tableName, batchId, businessDate, total["RowCount"], total["RowHash"], batch["RowCount"], batch["RowHash"], "MEASURED"))
    return spark.createDataFrame(
        rows,
        "ObjectName string, DeltaTable string, BatchId bigint, BusinessDate date, TotalRowCount bigint, TotalRowHash bigint, BatchRowCount bigint, BatchRowHash bigint, MeasureStatus string",
    )


def loadBaseline(spark):
    if baselineTable:
        return spark.table(baselineTable)
    if baselineJson:
        return spark.createDataFrame(json.loads(baselineJson), "ObjectName string, BatchId bigint, BusinessDate string, RowCount bigint, RowHash bigint").withColumn(
            "BusinessDate", F.col("BusinessDate").cast("date")
        )
    return spark.createDataFrame([], "ObjectName string, BatchId bigint, BusinessDate string, RowCount bigint, RowHash bigint").withColumn("BusinessDate", F.col("BusinessDate").cast("date"))


def compare(measured, baseline):
    b = baseline.select("ObjectName", F.col("RowCount").alias("BaselineRowCount"), F.col("RowHash").alias("BaselineRowHash"))
    out = measured.join(b, "ObjectName", "left")
    status = (
        F.when(F.col("MeasureStatus") == "MISSING_TABLE", F.lit("MISSING_TABLE"))
        .when(F.col("BaselineRowCount").isNull(), F.lit("NO_BASELINE"))
        .when(F.col("BaselineRowCount") != F.col("BatchRowCount"), F.lit("COUNT_MISMATCH"))
        .when(F.col("BaselineRowHash").isNotNull() & (F.col("BaselineRowHash") != F.col("BatchRowHash")), F.lit("HASH_MISMATCH"))
        .otherwise(F.lit("MATCH"))
    )
    return out.withColumn("ReconciliationStatus", status)

# COMMAND ----------

catalog = p["catalog"]
with control.packageRun(spark, catalog, p["batchId"], "STG_Reconcile_Staging", projectName="WWI_Staging", stepName="Stage Reconciliation") as run:  # noqa: F821
    measured = measure(spark, catalog, p["batchId"], p["businessDate"])  # noqa: F821
    result = compare(measured, loadBaseline(spark))  # noqa: F821
    results = result.collect()
    mismatches = 0
    for r in results:
        control.logRowCount(
            spark,  # noqa: F821
            catalog,
            run.packageExecutionId,
            r["ObjectName"],
            sourceRowCount=r["BaselineRowCount"],
            targetRowCount=r["BatchRowCount"],
            rejectRowCount=None,
        )
        if r["ReconciliationStatus"] in ("COUNT_MISMATCH", "HASH_MISMATCH", "MISSING_TABLE"):
            mismatches += 1
    run.rowsRead = len(results)
    run.rowsRejected = mismatches
    result.orderBy("ObjectName").show(100, truncate=False)
    if failOnMismatch and mismatches:
        raise RuntimeError("%d staging objects failed reconciliation" % mismatches)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Capturing the SQL Server baseline
# MAGIC Run per target on the legacy staging database and load the result as `baselineTable` / `baselineJson`
# MAGIC (`RowHash` is optional; when omitted only row counts are compared):
# MAGIC ```sql
# MAGIC SELECT N'stg.Customer' AS ObjectName, @BatchId AS BatchId, @BusinessDate AS BusinessDate,
# MAGIC        COUNT_BIG(*) AS RowCount, NULL AS RowHash
# MAGIC FROM stg.Customer WHERE BatchId = @BatchId;
# MAGIC ```
