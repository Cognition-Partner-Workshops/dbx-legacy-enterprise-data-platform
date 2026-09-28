# Databricks notebook source
# MAGIC %md
# MAGIC # 08_facts reconciliation
# MAGIC Row counts, deterministic hashes and integrity checks for every `gold.fact_*` table loaded by the `wwi_08_facts` job,
# MAGIC compared with the SQL Server baseline. Ports the fact parts of `validation/runtime/02_row_count_reconciliation.sql`,
# MAGIC `03_dimension_fact_integrity.sql` and `04_regional_divergence.sql` to Spark SQL.
# MAGIC
# MAGIC **Baseline input** (captured on SQL Server, one row per legacy object):
# MAGIC `ObjectName` (legacy `Fact.X`), `BusinessDate` (nullable), `BatchId` (nullable), `RowCount`, `RowHash` (nullable; `xxhash64` semantics
# MAGIC cannot be reproduced on SQL Server - when `RowHash` is empty only counts are compared and the Delta hash is recorded for later runs).
# MAGIC Provide either `BaselineTable` (`<catalog>.etl.sqlserver_fact_baseline` by default) or `BaselineJson` (JSON array of the same rows).
# MAGIC
# MAGIC Results: every fact table -> `etl.row_count_log` via `control.logRowCount` (`sourceRowCount` = baseline, `targetRowCount` = Delta), plus a
# MAGIC findings DataFrame displayed at the end. Integrity findings (orphan keys, open holds, regional divergence) are written as
# MAGIC row-count log entries too, so `control.assertRowCountReconciliation` can gate the batch.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbx_etl_common import control, naming, params

import fact_common as fc

# COMMAND ----------

dbutils.widgets.text("BaselineTable", "", "Delta table with SQL Server baseline rows (empty -> <catalog>.etl.sqlserver_fact_baseline)")
dbutils.widgets.text("BaselineJson", "", "JSON array of baseline rows (overrides BaselineTable when set)")
dbutils.widgets.text("LookbackMonths", "3", "Months of Fact.Sale history for the regional-divergence checks")

p = params.getJobParams(dbutils)
catalog = p["catalog"]
businessDate = p["businessDate"]
batchId = fc.resolveBatchId(spark, catalog, p)
baselineTable = dbutils.widgets.get("BaselineTable") or naming.table(catalog, "etl", "sqlserver_fact_baseline")
baselineJson = dbutils.widgets.get("BaselineJson")
lookbackMonths = int(dbutils.widgets.get("LookbackMonths") or "3")

# COMMAND ----------

# legacy Fact.X -> (gold table, date-key column used for per-BusinessDate counts, business-key columns hashed for the row hash)
FACTS = {
    "Fact.Sale": ("fact_sale", "invoice_date_key", ["invoice_number", "invoice_line_number", "region_code", "correction_type_code"]),
    "Fact.Order": ("fact_order", "order_date_key", ["order_number", "order_line_number"]),
    "Fact.Purchase": ("fact_purchase", "order_date_key", ["purchase_order_number", "purchase_order_line_number"]),
    "Fact.Purchase Receipt": ("fact_purchase_receipt", "receipt_date_key", ["receipt_number", "receipt_line_number"]),
    "Fact.Payment": ("fact_payment", "payment_date_key", ["receipt_number", "receipt_line_number"]),
    "Fact.Supplier Payment": ("fact_supplier_payment", "payment_date_key", ["payment_reference"]),
    "Fact.Movement": ("fact_movement", "movement_date_key", ["wwi_movement_id"]),
    "Fact.Stock Holding": ("fact_stock_holding", "as_at_date_key", ["as_at_date_key", "stock_item_key", "warehouse_site_key"]),
    "Fact.Transaction": ("fact_transaction", "transaction_date_key", ["transaction_business_key"]),
    "Fact.Customer Transaction": ("fact_customer_transaction", "transaction_date_key", ["customer_transaction_business_key"]),
    "Fact.Supplier Transaction": ("fact_supplier_transaction", "transaction_date_key", ["supplier_transaction_business_key"]),
    "Fact.Shipment": ("fact_shipment", "despatch_date_key", ["shipment_number", "shipment_line_number"]),
    "Fact.Return": ("fact_return", "return_date_key", ["return_number", "return_line_number"]),
    "Fact.Credit Note": ("fact_credit_note", "credit_note_date_key", ["credit_note_number", "credit_note_line_number"]),
    "Fact.Loyalty Points": ("fact_loyalty_points", "movement_date_key", ["loyalty_movement_business_key"]),
    "Fact.Web Session": ("fact_web_session", "session_start_date_key", ["session_id"]),
    "Fact.Daily Inventory Snapshot": ("fact_daily_inventory_snapshot", "snapshot_date_key", ["snapshot_date_key", "stock_item_key", "warehouse_site_key"]),
    "Fact.Daily Sales Snapshot": ("fact_daily_sales_snapshot", "snapshot_date_key", ["snapshot_date_key", "region_code", "stock_item_key"]),
    "Fact.Order Fulfilment": ("fact_order_fulfilment", "order_date_key", ["order_number", "order_line_number"]),
    "Fact.GL Posting": ("fact_gl_posting", "posting_date_key", ["journal_number", "journal_line_number"]),
    "Fact.Fact Load Hold": ("fact_fact_load_hold", "business_date", ["fact_load_hold_key"]),
    "Fact.Sale Duplicate Archive": ("fact_sale_duplicate_archive", "invoice_date_key", ["sale_key"]),
}

AUDIT_COLUMNS = {"batch_id", "package_execution_id", "load_datetime", "lineage_key", "restated_datetime", "first_held_datetime", "last_retry_datetime", "released_datetime", "archived_datetime"}

# COMMAND ----------

# MAGIC %md ## Baseline

# COMMAND ----------

baselineSchema = T.StructType([
    T.StructField("ObjectName", T.StringType()), T.StructField("BusinessDate", T.StringType()), T.StructField("BatchId", T.LongType()),
    T.StructField("RowCount", T.LongType()), T.StructField("RowHash", T.LongType()),
])
if baselineJson.strip():
    baseline = spark.createDataFrame([json.loads(json.dumps(r)) for r in json.loads(baselineJson)], baselineSchema)
elif fc.tableExists(spark, baselineTable):
    baseline = spark.table(baselineTable).select(*[F.col(c.name).cast(c.dataType).alias(c.name) for c in baselineSchema])
else:
    print("No baseline at %s and no BaselineJson: Delta figures will be logged without a source count." % baselineTable)
    baseline = spark.createDataFrame([], baselineSchema)
baseline = baseline.withColumn("BusinessDate", F.to_date("BusinessDate"))
baselineRows = {(r["ObjectName"], r["BusinessDate"], r["BatchId"]): r for r in baseline.collect()}

# COMMAND ----------

# MAGIC %md ## Row counts and deterministic hashes per fact table

# COMMAND ----------

def rowHash(df: DataFrame) -> "F.Column":
    """Order-independent hash: xxhash64 over the sorted non-audit columns of each row, summed. Same
    expression for every table so a re-run over identical data yields the identical figure."""
    cols = sorted(c for c in df.columns if c not in AUDIT_COLUMNS)
    return F.sum(F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]))


def hashKeys(df: DataFrame, keyCols) -> "F.Column":
    present = ["natural_key_hash"] if "natural_key_hash" in df.columns else [c for c in keyCols if c in df.columns]
    return F.sum(F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in present])) if present else F.lit(None).cast("bigint")


packageExecutionId = control.logPackageStart(spark, catalog, batchId, "FACT_Reconciliation", projectName="WWI_Facts", stepName="Validate Facts")
try:
    findings = []
    for objectName, (table, dateKeyCol, keyCols) in FACTS.items():
        fullName = naming.table(catalog, "gold", table)
        if not fc.tableExists(spark, fullName):
            findings.append((objectName, "TABLE_MISSING", None, None, None, None, "gold table not created yet"))
            continue
        df = spark.table(fullName)
        total = df.agg(F.count(F.lit(1)).alias("n"), rowHash(df).alias("h"), hashKeys(df, keyCols).alias("k")).first()
        base = baselineRows.get((objectName, None, None))
        control.logRowCount(
            spark, catalog, packageExecutionId, objectName,
            sourceRowCount=base["RowCount"] if base else None, targetRowCount=total["n"],
        )
        hashMatch = None if not base or base["RowHash"] is None else (base["RowHash"] == total["h"])
        findings.append((objectName, "TOTAL", None, base["RowCount"] if base else None, total["n"], total["h"], "hash %s" % ("match" if hashMatch else "MISMATCH" if hashMatch is False else "not compared")))

        # per BusinessDate (date key of the fact) and per BatchId - the grain the legacy loads logged in etl.RowCountAudit
        if dateKeyCol in df.columns:
            perDate = df.where(F.col(dateKeyCol) == F.lit(businessDate)).agg(F.count(F.lit(1)).alias("n"), rowHash(df).alias("h")).first()
            base = baselineRows.get((objectName, businessDate, None))
            control.logRowCount(spark, catalog, packageExecutionId, "%s @ %s" % (objectName, businessDate.isoformat()),
                                sourceRowCount=base["RowCount"] if base else None, targetRowCount=perDate["n"])
            findings.append((objectName, "BUSINESS_DATE", businessDate.isoformat(), base["RowCount"] if base else None, perDate["n"], perDate["h"], None))
        if "batch_id" in df.columns:
            perBatch = df.where(F.col("batch_id") == F.lit(int(batchId))).agg(F.count(F.lit(1)).alias("n"), rowHash(df).alias("h")).first()
            base = baselineRows.get((objectName, None, int(batchId)))
            control.logRowCount(spark, catalog, packageExecutionId, "%s @ batch %d" % (objectName, batchId),
                                sourceRowCount=base["RowCount"] if base else None, targetRowCount=perBatch["n"])
            findings.append((objectName, "BATCH_ID", str(batchId), base["RowCount"] if base else None, perBatch["n"], perBatch["h"], None))
except Exception as exc:  # noqa: BLE001 - mirrors the SSIS OnError handler: log, mark Failed, re-raise
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, errorCode="FACT_RECON", sourceName="FACT_Reconciliation", errorDescription=str(exc))
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise

countFindings = spark.createDataFrame(findings, "ObjectName string, Grain string, GrainValue string, BaselineRowCount long, DeltaRowCount long, DeltaRowHash long, Note string")
display(countFindings.withColumn("Variance", F.col("DeltaRowCount") - F.col("BaselineRowCount")).orderBy(F.abs(F.col("Variance")).desc_nulls_last()))

# COMMAND ----------

# MAGIC %md ## 02 - row count reconciliation (etl.row_count_log / etl.package_execution for this batch)

# COMMAND ----------

rowCountLog = naming.table(catalog, "etl", "row_count_log")
packageExecution = naming.table(catalog, "etl", "package_execution")
configuration = naming.table(catalog, "etl", "configuration")
if fc.tableExists(spark, rowCountLog) and fc.tableExists(spark, packageExecution):
    tolerance = 0.5
    if fc.tableExists(spark, configuration):
        tol = spark.sql(
            "SELECT TRY_CAST(ConfigurationValue AS DECIMAL(9,4)) FROM %s WHERE ConfigurationKey = 'RowCountVarianceTolerancePercent' "
            "AND EnvironmentCode IN ('ALL', '%s') ORDER BY CASE WHEN EnvironmentCode = 'ALL' THEN 1 ELSE 0 END LIMIT 1" % (configuration, p["environmentCode"])
        ).first()
        tolerance = float(tol[0]) if tol and tol[0] is not None else tolerance
    # 02 #1 / #2: every hop this batch logged, worst variance first, flagged when outside tolerance
    hops = spark.sql("""
        SELECT pe.PackageName, rc.ObjectName, rc.SourceRowCount, rc.TargetRowCount, rc.InsertRowCount, rc.UpdateRowCount,
               rc.DeleteRowCount, rc.RejectRowCount,
               COALESCE(rc.SourceRowCount, 0) - COALESCE(rc.TargetRowCount, 0) - COALESCE(rc.RejectRowCount, 0) AS VarianceRowCount,
               CASE WHEN COALESCE(rc.SourceRowCount, 0) = 0 THEN NULL
                    ELSE CAST(100.0 * ABS(COALESCE(rc.SourceRowCount, 0) - COALESCE(rc.TargetRowCount, 0) - COALESCE(rc.RejectRowCount, 0)) / rc.SourceRowCount AS DECIMAL(9,4)) END AS VariancePercent
        FROM {rc} rc JOIN {pe} pe ON pe.PackageExecutionId = rc.PackageExecutionId
        WHERE pe.BatchId = {batchId} AND pe.PackageName LIKE 'FACT[_]%'
        ORDER BY ABS(COALESCE(rc.SourceRowCount, 0) - COALESCE(rc.TargetRowCount, 0) - COALESCE(rc.RejectRowCount, 0)) DESC
    """.format(rc=rowCountLog, pe=packageExecution, batchId=int(batchId)))
    display(hops)
    display(hops.where(F.col("VariancePercent") > F.lit(tolerance)).withColumn("TolerancePercent", F.lit(tolerance)))
    # 02 #3: fact packages that succeeded this batch without logging a row count
    display(spark.sql("""
        SELECT pe.PackageName, pe.BatchId, pe.StartedAtUtc, pe.Status, pe.RowsRead, pe.RowsInserted
        FROM {pe} pe
        WHERE pe.BatchId = {batchId} AND pe.Status = 'Succeeded' AND pe.PackageName LIKE 'FACT[_]%'
          AND NOT EXISTS (SELECT 1 FROM {rc} rc WHERE rc.PackageExecutionId = pe.PackageExecutionId)
        ORDER BY pe.PackageName
    """.format(rc=rowCountLog, pe=packageExecution, batchId=int(batchId))))
    # 02 #5: fact rejects never reprocessed and older than 3 days
    rejected = naming.table(catalog, "etl", "rejected_record")
    if fc.tableExists(spark, rejected):
        display(spark.sql("""
            SELECT ObjectName, RejectStage, RejectReasonCode, COUNT(*) AS OutstandingRejects, MIN(LoggedAtUtc) AS OldestRejectAtUtc
            FROM {r} WHERE COALESCE(IsReprocessed, false) = false AND LoggedAtUtc < current_timestamp() - INTERVAL 3 DAYS AND ObjectName LIKE 'Fact.%'
            GROUP BY ObjectName, RejectStage, RejectReasonCode ORDER BY OutstandingRejects DESC
        """.format(r=rejected)))

# COMMAND ----------

# MAGIC %md ## 03 - dimension / fact integrity (fact part)

# COMMAND ----------

def logFinding(name: str, count: int):
    """Integrity findings are logged with a zero source count so a non-zero target shows as a variance in etl.row_count_log."""
    control.logRowCount(spark, catalog, packageExecutionId, name, sourceRowCount=0, targetRowCount=int(count))
    return count


saleName = naming.table(catalog, "gold", "fact_sale")
dimCustomer = naming.table(catalog, "gold", "dim_customer")
if fc.tableExists(spark, dimCustomer):
    # 03 #1 / #2 - SCD2 chain integrity of the customer dimension the sale loads key against
    chain = spark.sql("""
        SELECT wwi_customer_id, COUNT(*) AS TotalVersions,
               SUM(CASE WHEN is_current_row THEN 1 ELSE 0 END) AS CurrentRowFlagCount,
               SUM(CASE WHEN valid_to >= TIMESTAMP'9999-12-31 00:00:00' THEN 1 ELSE 0 END) AS OpenValidToCount
        FROM {d} GROUP BY wwi_customer_id
        HAVING SUM(CASE WHEN is_current_row THEN 1 ELSE 0 END) <> 1 OR SUM(CASE WHEN valid_to >= TIMESTAMP'9999-12-31 00:00:00' THEN 1 ELSE 0 END) <> 1
    """.format(d=dimCustomer))
    overlap = spark.sql("""
        SELECT a.wwi_customer_id, a.customer_key AS EarlierKey, a.valid_from AS EarlierFrom, a.valid_to AS EarlierTo,
               b.customer_key AS LaterKey, b.valid_from AS LaterFrom, b.valid_to AS LaterTo
        FROM {d} a JOIN {d} b ON b.wwi_customer_id = a.wwi_customer_id AND b.customer_key <> a.customer_key
                              AND b.valid_from < a.valid_to AND b.valid_to > a.valid_from
    """.format(d=dimCustomer))
    logFinding("Dimension.Customer SCD2 chain violations", chain.count())
    logFinding("Dimension.Customer SCD2 overlapping windows", overlap.count())
    display(chain)
    display(overlap)

# 03 #3 - unknown-member references (-1) per fact / key
unknownChecks = [
    ("Fact.Sale", "fact_sale", "customer_key"), ("Fact.Sale", "fact_sale", "stock_item_key"),
    ("Fact.Purchase", "fact_purchase", "supplier_key"), ("Fact.GL Posting", "fact_gl_posting", "cost_center_key"),
    ("Fact.Order", "fact_order", "customer_key"), ("Fact.Payment", "fact_payment", "customer_key"),
]
unknownRows = []
for objectName, table, keyCol in unknownChecks:
    fullName = naming.table(catalog, "gold", table)
    if fc.tableExists(spark, fullName) and keyCol in spark.table(fullName).columns:
        n = spark.table(fullName).where(F.col(keyCol) == fc.UNKNOWN_KEY).count()
        unknownRows.append((objectName, keyCol, n))
        control.logRowCount(spark, catalog, packageExecutionId, "%s unknown %s" % (objectName, keyCol), targetRowCount=n)
display(spark.createDataFrame(unknownRows, "FactTable string, DimensionKey string, UnknownMemberRows long"))

# 03 #4 - orphan surrogate keys: always a defect
orphanChecks = [
    ("Fact.Sale", "fact_sale", "customer_key", "dim_customer", "customer_key"),
    ("Fact.Sale", "fact_sale", "stock_item_key", "dim_stock_item", "stock_item_key"),
    ("Fact.Order", "fact_order", "customer_key", "dim_customer", "customer_key"),
    ("Fact.Purchase", "fact_purchase", "supplier_key", "dim_supplier", "supplier_key"),
    ("Fact.Movement", "fact_movement", "stock_item_key", "dim_stock_item", "stock_item_key"),
]
for objectName, table, keyCol, dim, dimKey in orphanChecks:
    factName, dimName = naming.table(catalog, "gold", table), naming.table(catalog, "gold", dim)
    if fc.tableExists(spark, factName) and fc.tableExists(spark, dimName):
        orphans = spark.table(factName).alias("f").join(spark.table(dimName).select(F.col(dimKey).alias("_dk")).alias("d"), F.col("f." + keyCol) == F.col("d._dk"), "left_anti")
        n = logFinding("%s orphan %s" % (objectName, keyCol), orphans.count())
        if n:
            display(orphans.select(keyCol, *[c for c in ("sale_key", "invoice_date_key") if c in orphans.columns]).limit(1000))

# 03 #5 - rows parked in Fact.Fact Load Hold
holdName = naming.table(catalog, "gold", fc.FACT_LOAD_HOLD_TABLE)
if fc.tableExists(spark, holdName):
    holds = spark.sql("""
        SELECT target_fact_name AS TargetFactName, missing_dimension_name AS MissingDimensionName, hold_reason_code AS HoldReasonCode,
               hold_status_code AS HoldStatusCode, COUNT(*) AS HeldRows, MIN(first_held_datetime) AS OldestHeldOn, MAX(retry_count) AS MaxRetries
        FROM {h} WHERE released_datetime IS NULL
        GROUP BY target_fact_name, missing_dimension_name, hold_reason_code, hold_status_code ORDER BY HeldRows DESC
    """.format(h=holdName))
    logFinding("Fact.Fact Load Hold unresolved", spark.table(holdName).where(F.col("released_datetime").isNull()).count())
    display(holds)

# late-arriving queue depth (DIM_Rekey_LateArriving input)
queueName = naming.table(catalog, "silver", fc.LATE_ARRIVING_QUEUE_TABLE)
if fc.tableExists(spark, queueName):
    logFinding("work.Late Arriving Dimension Queue open", spark.table(queueName).where(~F.coalesce(F.col("ResolvedFlag"), F.lit(False))).count())

# COMMAND ----------

# MAGIC %md ## 04 - regional divergence (Fact.Sale)

# COMMAND ----------

if fc.tableExists(spark, saleName):
    recent = "invoice_date_key >= add_months(DATE'%s', -%d)" % (businessDate.isoformat(), lookbackMonths)
    # #1 tax regime by region
    display(spark.sql("""
        SELECT region_code, tax_regime_code, COUNT(*) AS SaleRows,
               SUM(CASE WHEN vat_rate IS NOT NULL THEN 1 ELSE 0 END) AS WithVatRate,
               SUM(CASE WHEN gst_rate IS NOT NULL THEN 1 ELSE 0 END) AS WithGstRate,
               SUM(CASE WHEN vat_reverse_charge_flag THEN 1 ELSE 0 END) AS ReverseCharge,
               SUM(CASE WHEN gst_free_flag THEN 1 ELSE 0 END) AS GstFree
        FROM {s} WHERE {recent} GROUP BY region_code, tax_regime_code ORDER BY region_code, SaleRows DESC
    """.format(s=saleName, recent=recent)))
    # regime / region mismatches are the finding
    expectedRegime = {"NA": "SALESTAX", "EU": "VAT", "APAC": "GST"}
    mismatch = spark.table(saleName).where(F.expr(recent)).where(
        ~F.coalesce(F.col("tax_regime_code"), F.lit("")).startswith(
            F.coalesce(F.create_map(*[F.lit(x) for kv in expectedRegime.items() for x in kv])[F.col("region_code")], F.lit("?"))
        )
    )
    logFinding("Fact.Sale tax regime / region mismatch", mismatch.count())
    # #2 rows with no region
    logFinding("Fact.Sale rows without region", spark.table(saleName).where(F.col("region_code").isNull()).count())
    # #3 EU reverse charge without a customer tax registration
    rc = spark.table(saleName).where((F.col("region_code") == "EU") & F.col("vat_reverse_charge_flag") & (F.trim(F.coalesce(F.col("customer_tax_registration"), F.lit(""))) == ""))
    logFinding("Fact.Sale EU reverse charge without registration", rc.count())
    display(rc.select("sale_key", "customer_key", "invoice_date_key", "tax_regime_code", "customer_tax_registration").orderBy(F.col("invoice_date_key").desc()).limit(1000))
    # #4 APAC GST-inclusive arithmetic closes to the cent (one-cent truncation drift is expected)
    apac = spark.table(saleName).where((F.col("region_code") == "APAC") & F.col("gross_amount").isNotNull()).withColumn(
        "ResidualAmount", F.col("gross_amount") - F.col("total_excluding_tax") - F.col("tax_amount")
    ).where(F.abs(F.col("ResidualAmount")) > 0.01)
    logFinding("Fact.Sale APAC GST residual > 0.01", apac.count())
    display(apac.select("sale_key", "invoice_date_key", "gross_amount", "total_excluding_tax", "tax_amount", "gst_rate", "ResidualAmount").orderBy(F.abs(F.col("ResidualAmount")).desc()).limit(1000))
    # #5 FX coverage
    display(spark.sql("""
        SELECT region_code, transaction_currency_code, fx_rate_source_code, COUNT(*) AS SaleRows,
               SUM(CASE WHEN fx_rate_to_reporting IS NULL THEN 1 ELSE 0 END) AS MissingRate,
               SUM(CASE WHEN fx_rate_to_reporting = 1 THEN 1 ELSE 0 END) AS UnitRate,
               MIN(fx_rate_effective_date) AS EarliestRateDate, MAX(fx_rate_effective_date) AS LatestRateDate
        FROM {s} WHERE {recent} GROUP BY region_code, transaction_currency_code, fx_rate_source_code ORDER BY MissingRate DESC, SaleRows DESC
    """.format(s=saleName, recent=recent)))
    logFinding("Fact.Sale non-USD rows without FX rate", spark.table(saleName).where((F.col("transaction_currency_code") != "USD") & F.col("fx_rate_to_reporting").isNull()).count())
    # #6 fiscal period alignment (demonstrates the regional fiscal calendars, not a defect)
    display(spark.sql("""
        SELECT region_code, fiscal_year, fiscal_period, MIN(invoice_date_key) AS FirstInvoiceDate, MAX(invoice_date_key) AS LastInvoiceDate, COUNT(*) AS SaleRows
        FROM {s} WHERE fiscal_year IS NOT NULL GROUP BY region_code, fiscal_year, fiscal_period ORDER BY region_code, fiscal_year DESC, fiscal_period DESC
    """.format(s=saleName)))
    # correction bookkeeping: every REV / RES row must point at an ORIG sale key and reversals must net to zero per corrected key
    corr = spark.sql("""
        SELECT corrected_sale_key, SUM(CASE WHEN correction_type_code = 'REV' THEN net_amount ELSE 0 END) AS ReversedNet
        FROM {s} WHERE correction_type_code IN ('REV', 'RES') AND corrected_sale_key IS NULL GROUP BY corrected_sale_key
    """.format(s=saleName))
    logFinding("Fact.Sale correction rows without corrected_sale_key", corr.count())
    dupes = spark.table(saleName).where(F.col("correction_type_code") != fc.CORRECTION_REVERSAL).groupBy("natural_key_hash").count().where(F.col("count") > 1)
    logFinding("Fact.Sale duplicate natural keys after FACT_Dedup_Sale", dupes.count())

# COMMAND ----------

# MAGIC %md ## Gate

# COMMAND ----------

failed = control.assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=False)
control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded" if failed == 0 else "SucceededWithWarnings", rowsRead=len(FACTS))
print("row-count reconciliation objects failing for batch %d: %s" % (batchId, failed))
dbutils.notebook.exit(json.dumps({"batchId": batchId, "failedObjectCount": failed, "factTables": len(FACTS)}))
