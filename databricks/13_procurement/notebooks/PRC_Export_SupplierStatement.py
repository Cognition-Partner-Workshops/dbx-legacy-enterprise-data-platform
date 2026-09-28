# Databricks notebook source
# MAGIC %md
# MAGIC # PRC_Export_SupplierStatement
# MAGIC Migrated from `ssis/13_procurement/PRC_Export_SupplierStatement.dtsx` (WWI_Procurement).
# MAGIC
# MAGIC Builds the monthly supplier statement lines for `StatementPeriod` (yyyy-MM) from
# MAGIC `gold.fact_supplier_transaction` + `gold.dim_supplier`, computes running balances, formats the
# MAGIC legacy fixed-layout `StatementLineText`, replaces the period slice of
# MAGIC `silver.work_supplier_statement_archive` and writes `supplier_statement_<yyyy-MM>.csv` to the
# MAGIC outbound UC Volume (plus a BatchId-suffixed copy in the archive Volume, replacing the legacy
# MAGIC `WWI_Archive_Files` share).

# COMMAND ----------

import os
import shutil
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402

from procurement_lib import PROJECT_NAME  # noqa: E402
from procurement_lib import delta_io  # noqa: E402
from procurement_lib import supplier_statement as st  # noqa: E402
from procurement_lib.common import parseBool  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "PRC_Export_SupplierStatement"
STEP_NAME = "Procurement Mart"

for name, default in (("BatchId", "0"), ("BusinessDate", ""), ("ReloadFullHistory", "False"), ("EnvironmentCode", "DEV"),
                      ("RestartFromStep", ""), ("catalog", ""), ("StatementPeriod", ""), ("ExcludeSelfBilling", "True"),
                      ("OutboundVolumePath", ""), ("ArchiveVolumePath", "")):
    dbutils.widgets.text(name, default)

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
# Legacy default "1900-01" produced an empty statement; an empty parameter now means "the month of BusinessDate".
statementPeriod = dbutils.widgets.get("StatementPeriod").strip() or businessDate.strftime("%Y-%m")
excludeSelfBilling = parseBool(dbutils.widgets.get("ExcludeSelfBilling"))
outboundVolumePath = dbutils.widgets.get("OutboundVolumePath").strip() or "/Volumes/%s/etl/outbound" % catalog
archiveVolumePath = dbutils.widgets.get("ArchiveVolumePath").strip() or "/Volumes/%s/etl/archive" % catalog

factSupplierTransaction = naming.table(catalog, "gold", "fact_supplier_transaction")
dimSupplier = naming.table(catalog, "gold", "dim_supplier")
workSupplierStatementLine = naming.table(catalog, "silver", "work_supplier_statement_line")
workSupplierStatementArchive = naming.table(catalog, "silver", "work_supplier_statement_archive")

# COMMAND ----------


def writeSingleCsv(df, directory, fileName):
    """Write df as one CSV file named fileName inside directory (UC Volume path)."""
    stagingDir = os.path.join(directory, "_tmp_%s_%s" % (fileName, batchId))
    (df.coalesce(1).write.mode("overwrite").option("header", "true").option("dateFormat", "yyyy-MM-dd")
       .option("emptyValue", "").csv(stagingDir))
    part = [f for f in os.listdir(stagingDir) if f.startswith("part-") and f.endswith(".csv")][0]
    target = os.path.join(directory, fileName)
    if os.path.exists(target):
        os.remove(target)
    shutil.move(os.path.join(stagingDir, part), target)
    shutil.rmtree(stagingDir, ignore_errors=True)
    return target


# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName=STEP_NAME) as run:
    # "Build Statement Lines" + "Compute Statement Balances" (work.SupplierStatementLine)
    lines = st.computeRunningBalances(
        st.buildStatementLines(spark.table(factSupplierTransaction), spark.table(dimSupplier), statementPeriod, excludeSelfBilling)
    ).cache()
    delta_io.overwriteTable(lines, workSupplierStatementLine)
    rowsRead = delta_io.countWhere(spark, workSupplierStatementLine)

    # "Build Statement File Name" + data flow "Write Statement File" (Format Statement Row -> archive)
    statementFileName = st.statementFileName(statementPeriod)
    formatted = st.formatStatementRows(lines)
    archiveRows = formatted.withColumn("StatementFileName", F.lit(statementFileName)) \
        .withColumn("BatchId", F.lit(int(batchId)).cast("bigint")).withColumn("LoadedAtUtc", F.current_timestamp())
    if delta_io.tableExists(spark, workSupplierStatementArchive):
        delta_io.replaceWhere(archiveRows, workSupplierStatementArchive, "StatementPeriod = '%s'" % statementPeriod)
    else:
        delta_io.overwriteTable(archiveRows, workSupplierStatementArchive)
    rowsInserted = delta_io.countWhere(spark, workSupplierStatementArchive, "StatementPeriod = '%s'" % statementPeriod)

    os.makedirs(outboundVolumePath, exist_ok=True)
    os.makedirs(archiveVolumePath, exist_ok=True)
    outboundFile = writeSingleCsv(st.orderedStatementRows(formatted), outboundVolumePath, statementFileName)
    archiveFile = os.path.join(archiveVolumePath, statementFileName.replace(".csv", "_batch%s.csv" % batchId))
    shutil.copyfile(outboundFile, archiveFile)

    # "Count Statements"
    statementCount = st.countStatements(lines)
    print("StatementPeriod=%s StatementCount=%s OutboundFile=%s ArchiveFile=%s" % (statementPeriod, statementCount, outboundFile, archiveFile))

    # "Log Row Counts"
    control.logRowCount(spark, catalog, run.packageExecutionId, "file:%s" % statementFileName,
                        sourceRowCount=rowsRead, targetRowCount=rowsInserted, insertRowCount=rowsInserted, rejectRowCount=0)
    run.rowsRead = rowsRead
    run.rowsInserted = rowsInserted
    run.rowsUpdated = 0
    run.rowsRejected = 0
    lines.unpersist()

dbutils.notebook.exit("%s: period=%s statements=%s rows=%s file=%s" % (PACKAGE_NAME, statementPeriod, statementCount, rowsInserted, outboundFile))
