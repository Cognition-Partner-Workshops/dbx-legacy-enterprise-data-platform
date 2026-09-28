# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Load_DateDimension
# MAGIC Migrated from `ssis/06_reference_data/REF_Load_DateDimension.dtsx` (generator `build_reference_packages.py`, project WWI_ReferenceData).
# MAGIC
# MAGIC Generates `gold.dim_date` with Spark `sequence()` / `explode()` between the `DateRangeStart` / `DateRangeEnd` job parameters (legacy recursive CTE 2005-01-01 .. 2035-12-31), three regional fiscal calendars (`ref.Region.FiscalYearStartMonth`), the 1900-01-01 / 1900-01-02 sentinel rows, and the `gold.dim_fiscal_calendar` outrigger (Date x Country).
# MAGIC
# MAGIC Control flow (legacy task -> here): `CTL Log Package Start` -> `control.logPackageStart`; `Truncate Dimension.Date` -> idempotent
# MAGIC Delta MERGE (surrogate keys and reserved members are preserved); pre-tasks (`ref.usp_Load*`) -> `wwi_ref.ref_loads`;
# MAGIC data flow -> `wwi_ref.transforms` + `wwi_ref.delta_io`; error outputs -> `silver.err_*` + `control.logRejectedRecordSet`;
# MAGIC `CTL Log Row Count` -> `control.logRowCount`; `CTL Log Package Success` / `OnError` -> `control.logPackageEnd` / `control.logError`.
# MAGIC
# MAGIC Job parameters: `BatchId`, `BusinessDate`, `ReloadFullHistory`, `EnvironmentCode`, `RestartFromStep`, `catalog`, `DateRangeStart`, `DateRangeEnd`.

# COMMAND ----------

import datetime
import os
import sys


def bundleSourcePath():
    """../src of this bundle (workspace files), so `wwi_ref` imports without a wheel build."""
    try:
        notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return os.path.join("/Workspace", os.path.dirname(os.path.dirname(notebookPath)).lstrip("/"), "src")
    except Exception:
        return os.path.abspath(os.path.join(os.getcwd(), "..", "src"))


if bundleSourcePath() not in sys.path:
    sys.path.insert(0, bundleSourcePath())

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402,F401  (dbx_etl_common wheel: task environment)

from wwi_ref import runtime, ref_loads, transforms, delta_io, schemas  # noqa: E402,F401
from wwi_ref import datecalendar  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "REF_Load_DateDimension"
p = params.getJobParams(dbutils)
print("catalog=%s batchId=%s businessDate=%s environment=%s restartFromStep=%s"
      % (p["catalog"], p["batchId"], p["businessDate"], p["environmentCode"], p["restartFromStep"]))

if runtime.isSkippedByRestart(p, PACKAGE_NAME):
    dbutils.notebook.exit("SKIPPED: RestartFromStep=%s resumes after %s" % (p["restartFromStep"], PACKAGE_NAME))

# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    # -- pre-task: ref.usp_LoadRegion (fiscal calendar start months)
    ref_loads.loadRegion(ctx)

    # -- DFT Generate Calendar: SRC Calendar Spine (recursive CTE) -> Derive Fiscal Periods -> Screen Calendar -> DST Date
    ctx.step("DFT Generate Calendar")
    startDate = runtime.parseDate(runtime.widgetOrDefault(dbutils, "DateRangeStart", None), datetime.date(2005, 1, 1))
    endDate = runtime.parseDate(runtime.widgetOrDefault(dbutils, "DateRangeEnd", None), datetime.date(2035, 12, 31))
    if endDate < startDate:
        raise ValueError("DateRangeEnd %s is before DateRangeStart %s" % (endDate, startDate))
    regions = spark.table(ctx.table("ref.Region"))
    fiscalStartMonths = {row["RegionCode"]: int(row["FiscalYearStartMonth"])
                         for row in regions.select("RegionCode", "FiscalYearStartMonth").collect()}
    dates = datecalendar.buildDateDimension(spark, startDate, endDate, fiscalStartMonths, PACKAGE_NAME, ctx.batchId).cache()
    rowsRead = dates.count()
    inRange = F.col("CalendarYear").between(startDate.year, endDate.year)
    published = dates.where(inRange).unionByName(datecalendar.sentinelDateRows(spark, PACKAGE_NAME, ctx.batchId))
    rejected = ctx.rejectConstraintViolations(
        dates.where(~inRange), "Dimension.Date", "DateKey", "CalendarYear", "CalendarYear",
        "REF_DATE_OUT_OF_RANGE", "calendar year outside the generated range", constraintName="CK_Date_Range",
        payloadColumns=["DateKey", "Date", "CalendarYear"])
    dateTarget = ctx.table("Dimension.Date")
    counts = delta_io.mergeReference(spark, dateTarget, published, ["DateKey"])
    ctx.addCounts("Dimension.Date", sourceRowCount=rowsRead, targetRowCount=spark.table(dateTarget).count(),
                  insertRowCount=counts.inserted, updateRowCount=counts.updated, rejectRowCount=rejected)

    # -- Dimension.Fiscal Calendar outrigger (Dimension.Fiscal Calendar.sql): one row per country and date
    ctx.step("DFT Generate Fiscal Calendar")
    fiscal = datecalendar.buildFiscalCalendar(spark.table(dateTarget).where(~F.col("IsReservedMember")),
                                              spark.table(ctx.table("ref.Country")), regions, ctx.batchId)
    fiscalTarget = ctx.table("Dimension.Fiscal Calendar")
    fiscalCounts = delta_io.mergeReference(spark, fiscalTarget, fiscal, ["CountryCode", "Date"])
    ctx.addCounts("Dimension.Fiscal Calendar", sourceRowCount=None, targetRowCount=spark.table(fiscalTarget).count(),
                  insertRowCount=fiscalCounts.inserted, updateRowCount=fiscalCounts.updated)
    dates.unpersist()

    print(ctx.summary())
