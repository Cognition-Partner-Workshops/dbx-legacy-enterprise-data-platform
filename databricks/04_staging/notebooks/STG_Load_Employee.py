# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Employee
# MAGIC Legacy: `ssis/04_staging/STG_Load_Employee.dtsx` (WWI_Staging). Two Data Flows over raw.SqlOrder: people (name split, EU contact masking, survivorship on EmployeeId) into stg.Employee and salesperson quotas (average-FX conversion) into stg.Salesperson.
# MAGIC
# MAGIC Control framework: `dbx_etl_common` (session 00) via `stg_common.loader.StagingRun`
# MAGIC (`control.packageRun` / `getWatermark` / `setWatermark` / `logRejectedRecordSet` / `logRowCount`).

# COMMAND ----------

import os
import sys

from pyspark.sql import Window  # noqa: F401
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402,F401
from stg_common import expressions as X  # noqa: E402,F401
from stg_common import refs  # noqa: E402,F401
from stg_common import transforms as T  # noqa: E402,F401
from stg_common import work as W  # noqa: E402,F401
from stg_common.loader import PHASE_STAGE_WORK, StagingRun, lookupIgnore, lookupLeft, splitByCondition  # noqa: E402,F401

p = params.getJobParams(dbutils)  # noqa: F821  (batchId, businessDate, reloadFullHistory, environmentCode, restartFromStep, catalog)

# COMMAND ----------

run = StagingRun(spark, dbutils, "STG_Load_Employee", sourceSystemCode="SQL_WWI", objectName="stg.Employee", jobParams=p)

EMPLOYEE_COLUMNS = ["EmployeeId", "FullName", "GivenName", "FamilyName", "PreferredName", "RegionCode", "EmailAddress", "PhoneNumberLast4", "IsCurrentFlag", "ValidFrom", "ValidTo"]
SALESPERSON_COLUMNS = ["EmployeeId", "SalesTerritoryCode", "QuotaYear", "QuotaAmount", "QuotaCurrencyCode", "QuotaAmountUsd"]


def load(run):
    orders = run.bronze("raw_sql_order", currentBatchOnly=False)
    salespeople = orders.select("SalespersonPersonID").where(F.col("SalespersonPersonID").isNotNull()).dropDuplicates()
    # raw.SqlOrder INNER JOIN raw.SqlPerson / raw.SqlSalespersonQuota (unscripted OLTP extracts)
    persons = run.bronze("raw_sql_person", currentBatchOnly=False).select(F.col("PersonID").alias("SalespersonPersonID"), "FullName", "PreferredName", "EmailAddress", "PhoneNumber", "RegionCode", "ValidFrom", "ValidTo")
    people = salespeople.join(persons, "SalespersonPersonID", "inner").dropDuplicates()
    run.countRead(people)
    employees = T.splitEmployeeNames(people)
    # Sort Employees For Survivorship (EmployeeId, IsCurrentFlag) with duplicate removal: current row wins
    survivors = T.survivorshipDedupe(employees, ["EmployeeId"], [F.col("IsCurrentFlag").desc(), F.col("ValidFrom").desc_nulls_last()])
    run.truncateReload(survivors.select(*EMPLOYEE_COLUMNS), "stg_employee")

    quotaRows = run.bronze("raw_sql_salesperson_quota", currentBatchOnly=False).select("SalespersonPersonID", "SalesTerritoryCode", "QuotaAmount", "QuotaCurrencyCode", "QuotaYear")
    quotas = salespeople.join(quotaRows, "SalespersonPersonID", "inner").dropDuplicates()
    run.countRead(quotas)
    prepared = T.prepareQuota(quotas)
    averageRate = (
        run.silver("stg_fx_rate").where(F.col("ToCurrencyCode") == "USD")
        .groupBy(F.col("FromCurrencyCode").alias("QuotaCurrencyCode"))
        .agg(F.avg("ExchangeRate").cast("decimal(18,6)").alias("AverageRate"))
    )
    converted = T.convertQuota(lookupIgnore(prepared, averageRate, ["QuotaCurrencyCode"], ["AverageRate"]))
    run.truncateReload(converted.select(*SALESPERSON_COLUMNS), "stg_salesperson")
    run.logRowCount(objectName="stg.Salesperson")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
