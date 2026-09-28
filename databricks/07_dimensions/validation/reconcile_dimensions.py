# Databricks notebook source
# MAGIC %md
# MAGIC # 07_dimensions reconciliation
# MAGIC For every `gold.dim_*` table loaded by this bundle: row counts (total / current / per LastLoadBatchId),
# MAGIC a deterministic content hash, the dimension checks ported from `validation/runtime/03_dimension_fact_integrity.sql`
# MAGIC (Type 2 chain integrity, overlapping windows, facts on the unknown member, orphan keys) and the customer-side
# MAGIC regional divergence checks from `validation/runtime/04_regional_divergence.sql`.
# MAGIC
# MAGIC Baseline: the SQL Server figures are supplied either as the Delta table `etl.reconciliation_baseline`
# MAGIC (`ObjectName STRING, MetricName STRING, BaselineValue STRING, BusinessDate DATE`) or as the `BaselineJson`
# MAGIC widget (`{"Dimension.Customer": {"rowCount": 1234, "currentRowCount": 1200, "contentHash": "..."}}`).
# MAGIC Every measured figure is written to `etl.row_count_log` through `control.logRowCount`.

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from dbx_etl_common import control, params, naming

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from wwi_dimensions import specs, tables  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("BaselineJson", "", "Baseline JSON (optional)")
dbutils.widgets.text("BaselineTable", "reconciliation_baseline", "Baseline table in etl schema")
p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
PACKAGE_NAME = "VAL_Reconcile_Dimensions"
loadTimestamp = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

# COMMAND ----------

def loadBaseline():
    baseline = {}
    table = naming.table(catalog, "etl", dbutils.widgets.get("BaselineTable") or "reconciliation_baseline")
    if tables.tableExists(spark, table):
        rows = spark.table(table)
        if "BusinessDate" in rows.columns and businessDate is not None:
            rows = rows.where((F.col("BusinessDate") == F.lit(str(businessDate)).cast("date")) | F.col("BusinessDate").isNull())
        for r in rows.collect():
            baseline.setdefault(r["ObjectName"], {})[r["MetricName"]] = r["BaselineValue"]
    raw = dbutils.widgets.get("BaselineJson")
    if raw:
        for objectName, metrics in json.loads(raw).items():
            baseline.setdefault(objectName, {}).update({k: str(v) for k, v in metrics.items()})
    return baseline


def contentHash(df, columns):
    """Order-independent deterministic hash: sum of xxhash64 over the concatenated (sorted) column list."""
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in sorted(columns)]
    return df.select(F.sum(F.xxhash64(F.concat_ws("\x1f", *cols))).alias("h")).collect()[0]["h"]


def compare(objectName, metricName, measured, baseline):
    expected = baseline.get(objectName, {}).get(metricName)
    status = "NO_BASELINE" if expected is None else ("MATCH" if str(expected) == str(measured) else "MISMATCH")
    return {"ObjectName": objectName, "MetricName": metricName, "Measured": str(measured), "Baseline": expected, "Status": status}

# COMMAND ----------

baseline = loadBaseline()
findings = []
with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName="WWI_Dimensions", stepName="Reconcile Row Counts") as run:
    packageExecutionId = run.packageExecutionId
    for name in sorted(specs.SPECS):
        spec = specs.SPECS[name]
        table = naming.table(catalog, "gold", spec.tableName)
        objectName = f"Dimension.{spec.name}"
        if not tables.tableExists(spark, table):
            findings.append({"ObjectName": objectName, "MetricName": "exists", "Measured": "False", "Baseline": None, "Status": "MISSING_TABLE"})
            continue
        df = spark.table(table)
        real = df.where(F.col(spec.keyColumn) > 0)
        rowCount = real.count()
        currentCount = real.where(F.col("IsCurrentRow") == True).count() if spec.isType2 else rowCount  # noqa: E712
        reservedCount = df.where(F.col(spec.keyColumn) < 0).count()
        batchRows = real.where(F.col("LastLoadBatchId") == int(batchId)).count() if int(batchId) > 0 else None
        hashColumns = [c for c, _ in spec.attributeColumns] + ["IsCurrentRow", "EffectiveFrom", "EffectiveTo"]
        digest = contentHash(real, hashColumns)
        findings += [
            compare(objectName, "rowCount", rowCount, baseline),
            compare(objectName, "currentRowCount", currentCount, baseline),
            compare(objectName, "reservedMemberCount", reservedCount, baseline),
            compare(objectName, "contentHash", digest, baseline),
        ]
        expectedRows = baseline.get(objectName, {}).get("rowCount")
        control.logRowCount(spark, catalog, packageExecutionId, objectName,
                            sourceRowCount=int(expectedRows) if expectedRows is not None else None,
                            targetRowCount=rowCount, insertRowCount=batchRows)
        control.logRowCount(spark, catalog, packageExecutionId, f"{objectName}.Current", targetRowCount=currentCount)
        control.logRowCount(spark, catalog, packageExecutionId, f"{objectName}.Reserved", targetRowCount=reservedCount,
                            sourceRowCount=len(specs.RESERVED_MEMBERS))

        if spec.isType2:
            # 03 / check 1: exactly one current row and one open EffectiveTo per business key
            bk = spec.businessKeyColumn
            chain = (real.groupBy(bk)
                     .agg(F.count("*").alias("TotalVersions"),
                          F.sum(F.when(F.col("IsCurrentRow") == True, 1).otherwise(0)).alias("CurrentRowFlagCount"),  # noqa: E712
                          F.sum(F.when(F.col("EffectiveTo") == F.lit(specs.OPEN_ENDED_TIMESTAMP).cast("timestamp"), 1).otherwise(0)).alias("OpenValidToCount"))
                     .where("CurrentRowFlagCount <> 1 OR OpenValidToCount <> 1"))
            chainBreaks = chain.count()
            # 03 / check 2: overlapping validity windows on the same business key
            a, b = real.alias("a"), real.alias("b")
            overlaps = a.join(b, (F.col(f"a.{bk}") == F.col(f"b.{bk}")) & (F.col(f"a.{spec.keyColumn}") != F.col(f"b.{spec.keyColumn}"))
                              & (F.col("b.EffectiveFrom") < F.col("a.EffectiveTo")) & (F.col("b.EffectiveTo") > F.col("a.EffectiveFrom"))).count()
            findings += [compare(objectName, "type2ChainBreaks", chainBreaks, {objectName: {"type2ChainBreaks": "0"}}),
                         compare(objectName, "overlappingWindows", overlaps, {objectName: {"overlappingWindows": "0"}})]
            control.logRowCount(spark, catalog, packageExecutionId, f"{objectName}.Type2ChainBreaks", targetRowCount=chainBreaks, sourceRowCount=0)
            control.logRowCount(spark, catalog, packageExecutionId, f"{objectName}.OverlappingWindows", targetRowCount=overlaps, sourceRowCount=0)

    # 03 / checks 3 + 4: facts on the unknown member and orphan surrogate keys (only for fact tables that exist yet)
    for dimensionName, factTable, keyColumn in specs.FACT_REKEY_TARGETS:
        fact = naming.table(catalog, "gold", factTable)
        if not tables.tableExists(spark, fact):
            continue
        spec = specs.spec(dimensionName)
        f = spark.table(fact)
        unknownRows = f.where(F.col(keyColumn) == specs.UNKNOWN_KEY).count()
        dim = spark.table(naming.table(catalog, "gold", spec.tableName)).select(F.col(spec.keyColumn).alias("_k"))
        orphanRows = f.join(dim, f[keyColumn] == F.col("_k"), "left_anti").count()
        objectName = f"Fact.{factTable}.{keyColumn}"
        findings += [compare(objectName, "unknownMemberRows", unknownRows, baseline),
                     compare(objectName, "orphanKeyRows", orphanRows, {objectName: {"orphanKeyRows": "0"}})]
        control.logRowCount(spark, catalog, packageExecutionId, f"{objectName}.UnknownMember", targetRowCount=unknownRows)
        control.logRowCount(spark, catalog, packageExecutionId, f"{objectName}.OrphanKeys", targetRowCount=orphanRows, sourceRowCount=0)

    # Late-arriving queue depth (03 / check 5 analogue for the dimension side)
    queue = naming.table(catalog, "silver", "work_late_arriving_dimension_queue")
    if tables.tableExists(spark, queue):
        depth = spark.table(queue).where("ResolvedFlag = false").count()
        findings.append(compare("work.LateArrivingDimensionQueue", "openRows", depth, baseline))
        control.logRowCount(spark, catalog, packageExecutionId, "work.LateArrivingDimensionQueue", targetRowCount=depth)

    # 04 / regional divergence on Dimension.Customer: every region must carry its own tax construct and defaults
    customer = naming.table(catalog, "gold", "dim_customer")
    if tables.tableExists(spark, customer):
        c = spark.table(customer).where("CustomerKey > 0 AND IsCurrentRow = true")
        regional = (c.groupBy("RegionCode").agg(
            F.count("*").alias("Customers"),
            F.sum(F.when(F.col("SalesTaxJurisdictionCode").isNotNull(), 1).otherwise(0)).alias("WithSalesTaxJurisdiction"),
            F.sum(F.when(F.col("VATRegistrationNumber").isNotNull(), 1).otherwise(0)).alias("WithVatRegistration"),
            F.sum(F.when(F.col("IsReverseChargeEligible") == True, 1).otherwise(0)).alias("ReverseChargeEligible"),  # noqa: E712
            F.sum(F.when(F.col("GSTRegistrationNumber").isNotNull(), 1).otherwise(0)).alias("WithGstRegistration"),
            F.sum(F.when(F.col("IsPseudonymized") == True, 1).otherwise(0)).alias("Pseudonymized"),  # noqa: E712
            F.sum(F.when(F.col("RegionCode").isNull(), 1).otherwise(0)).alias("RowsWithoutRegion"),
            F.min("RetentionYears").alias("MinRetentionYears"), F.max("RetentionYears").alias("MaxRetentionYears"),
            F.sum(F.when(F.col("CreditLimitCurrencyCode").isNull(), 1).otherwise(0)).alias("MissingCurrency")))
        expectedRetention = {"NA": 7, "EU": 6, "APAC": 5}
        for r in regional.collect():
            region = r["RegionCode"] or "<none>"
            objectName = f"Dimension.Customer[{region}]"
            findings.append(compare(objectName, "currentRowCount", r["Customers"], baseline))
            # a region whose rows fell through to another region's CASE branch shows up as the wrong tax construct
            wrongConstruct = {
                "NA": r["WithVatRegistration"] + r["WithGstRegistration"] + r["ReverseChargeEligible"],
                "EU": r["WithSalesTaxJurisdiction"] + r["WithGstRegistration"],
                "APAC": r["WithSalesTaxJurisdiction"] + r["WithVatRegistration"] + r["ReverseChargeEligible"],
            }.get(region, r["Customers"])
            findings.append(compare(objectName, "crossRegionTaxConstructRows", wrongConstruct, {objectName: {"crossRegionTaxConstructRows": "0"}}))
            retentionOk = expectedRetention.get(region) in (None, r["MinRetentionYears"]) and expectedRetention.get(region) in (None, r["MaxRetentionYears"])
            findings.append(compare(objectName, "retentionYearsUniform", retentionOk, {objectName: {"retentionYearsUniform": "True"}}))
            findings.append(compare(objectName, "missingCreditCurrency", r["MissingCurrency"], {objectName: {"missingCreditCurrency": "0"}}))
            control.logRowCount(spark, catalog, packageExecutionId, objectName, targetRowCount=r["Customers"], rejectRowCount=wrongConstruct)
        # EU reverse charge without a VAT registration (04 / check 3 on the dimension side)
        euBad = c.where("RegionCode = 'EU' AND IsReverseChargeEligible = true AND (VATRegistrationNumber IS NULL OR trim(VATRegistrationNumber) = '')").count()
        findings.append(compare("Dimension.Customer[EU]", "reverseChargeWithoutVatRegistration", euBad, {"Dimension.Customer[EU]": {"reverseChargeWithoutVatRegistration": "0"}}))
        control.logRowCount(spark, catalog, packageExecutionId, "Dimension.Customer[EU].ReverseChargeNoVat", targetRowCount=euBad, sourceRowCount=0)

    mismatches = [f for f in findings if f["Status"] in ("MISMATCH", "MISSING_TABLE")]
    run.rowsRead = len(findings)
    run.rowsRejected = len(mismatches)

# COMMAND ----------

results = spark.createDataFrame(findings, "ObjectName STRING, MetricName STRING, Measured STRING, Baseline STRING, Status STRING")
display(results.orderBy("Status", "ObjectName", "MetricName"))
if mismatches:
    raise ValueError(f"{len(mismatches)} reconciliation finding(s) do not match the baseline: "
                     + "; ".join(f"{m['ObjectName']}.{m['MetricName']}={m['Measured']} (baseline {m['Baseline']})" for m in mismatches[:20]))
