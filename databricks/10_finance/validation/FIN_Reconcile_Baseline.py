# Databricks notebook source
# MAGIC %md
# MAGIC # FIN_Reconcile_Baseline - finance gold mart vs SQL Server baseline
# MAGIC For every table the WWI_Finance job loads, computes row counts and a deterministic
# MAGIC hash (`xxhash64` over the sorted business columns, summed) per BatchId / AccountingPeriod
# MAGIC and compares them with the SQL Server baseline captured from `validation/runtime/*.sql`.
# MAGIC
# MAGIC Baseline input (either):
# MAGIC * `BaselineTable` - a Delta table `(ObjectName STRING, ScopeKey STRING, RowCount BIGINT, HashValue BIGINT)`; or
# MAGIC * `BaselineJson` - the same rows as a JSON array string.
# MAGIC
# MAGIC Results go to `etl.row_count_log` via `control.logRowCount` (SourceRowCount = SQL Server,
# MAGIC TargetRowCount = Delta) and are printed; `FailOnMismatch=True` raises at the end.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import finance_common as fc

PACKAGE_NAME = "FIN_Reconcile_Baseline"

for name, default in (("BaselineTable", ""), ("BaselineJson", "[]"), ("FailOnMismatch", "False")):
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass

p = params.getJobParams(dbutils)
catalog = p["catalog"]
fin = fc.getFinanceParams(dbutils, p["businessDate"])
batchId, _ = fc.resolveBatchId(spark, catalog, p)
baselineTable = dbutils.widgets.get("BaselineTable").strip()
baselineJson = dbutils.widgets.get("BaselineJson").strip() or "[]"
failOnMismatch = fc.parseBool(dbutils.widgets.get("FailOnMismatch"))

# COMMAND ----------

# legacy object -> (delta table, scope column, hash columns) ; scope = BatchId or AccountingPeriod
TARGETS = {
    "Fact.Payment": ("gold.fact_payment", "BatchId",
                     ["PaymentBusinessKey", "LedgerCode", "AccountingPeriod", "ControlAccount", "FunctionalAmount",
                      "OpenAmount", "WithholdingTaxAmount", "AgingBucketCode", "RevaluedFunctionalAmount"]),
    "Fact.GL Posting": ("gold.fact_gl_posting", "BatchId",
                        ["GlJournalLineId", "LedgerCode", "AccountingPeriod", "AccountCode", "FunctionalDebitAmount",
                         "FunctionalCreditAmount", "NetAmount", "PostingSide", "TaxRegimeCode"]),
    "Aggregate.Finance Close Summary": ("gold.agg_finance_close_summary", "AccountingPeriod",
                                        ["CostCentreCode", "AccountingPeriod", "AllocatedCostAmount", "AllocationRuleCount"]),
    "Aggregate.ApAgingSummary": ("gold.agg_ap_aging_summary", "AccountingPeriod",
                                 ["LedgerCode", "AgingBucketCode", "OpenItemCount", "OpenAmount", "ReportableAmount", "DiscountAtRisk"]),
    "etl.ReconciliationResult": ("etl.reconciliation_result", "BatchId",
                                 ["LedgerCode", "AccountingPeriod", "AccountCode", "SourceAmount", "TargetAmount", "VarianceStatus"]),
    "etl.PeriodLock": ("etl.period_lock", "AccountingPeriod", ["LedgerCode", "AccountingPeriod", "LockStatusCode"]),
    "err.ApAgingReject": ("silver.err_ap_aging_reject", "BatchId", ["ApInvoiceKey", "RejectReasonCode"]),
    "err.WithholdingTaxReject": ("silver.err_withholding_tax_reject", "BatchId", ["ApInvoiceLineId", "RejectReasonCode"]),
}


def scopeValue(scopeColumn: str):
    return batchId if scopeColumn == "BatchId" else fin.accountingPeriod


def deltaFigures(objectName: str):
    schemaTable, scopeColumn, hashColumns = TARGETS[objectName]
    schema, table = schemaTable.split(".")
    fullName = naming.table(catalog, schema, table)
    if not spark.catalog.tableExists(fullName):
        return None, None
    df = spark.table(fullName)
    cols = [c for c in hashColumns if c in df.columns]
    if scopeColumn in df.columns:
        df = df.where(F.col(scopeColumn) == F.lit(scopeValue(scopeColumn)))
    hashed = df.select(F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]).alias("h"))
    agg = hashed.agg(F.count("*").alias("n"), F.sum("h").alias("s")).collect()[0]
    return int(agg["n"]), (int(agg["s"]) if agg["s"] is not None else 0)


def loadBaseline():
    if baselineTable:
        rows = spark.table(baselineTable).collect()
        return {(r["ObjectName"], str(r["ScopeKey"])): (r["RowCount"], r["HashValue"]) for r in rows}
    return {(r["ObjectName"], str(r["ScopeKey"])): (r.get("RowCount"), r.get("HashValue")) for r in json.loads(baselineJson)}


baseline = loadBaseline()

# COMMAND ----------

mismatches = []
with fc.PackageExecution(spark, catalog, batchId, PACKAGE_NAME, "Subledger Tie Out") as run:
    for objectName, (schemaTable, scopeColumn, _cols) in TARGETS.items():
        deltaCount, deltaHash = deltaFigures(objectName)
        key = (objectName, str(scopeValue(scopeColumn)))
        baseCount, baseHash = baseline.get(key, (None, None))
        status = "MISSING_TABLE" if deltaCount is None else ("NO_BASELINE" if baseCount is None else
                 ("MATCH" if (int(baseCount) == deltaCount and (baseHash is None or int(baseHash) == deltaHash)) else "MISMATCH"))
        if status == "MISMATCH":
            mismatches.append((objectName, baseCount, deltaCount, baseHash, deltaHash))
        run.logRowCount(f"Baseline|{objectName}", sourceRowCount=int(baseCount) if baseCount is not None else None,
                        targetRowCount=deltaCount)
        if deltaHash is not None:
            run.logRowCount(f"BaselineHash|{objectName}",
                            sourceRowCount=int(baseHash) if baseHash is not None else None, targetRowCount=deltaHash)
        run.rowsRead += 1
        print(f"{status:14} {objectName:35} scope={key[1]} baseline={baseCount}/{baseHash} delta={deltaCount}/{deltaHash}")

if mismatches and failOnMismatch:
    raise RuntimeError(f"{len(mismatches)} finance target(s) differ from the SQL Server baseline: {mismatches}")
dbutils.notebook.exit(json.dumps({"checked": len(TARGETS), "mismatches": len(mismatches)}))
