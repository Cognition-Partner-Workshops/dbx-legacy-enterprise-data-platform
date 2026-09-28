# Databricks notebook source
# MAGIC %md
# MAGIC # MNT_Validate_Configuration
# MAGIC Legacy: `ssis/99_maintenance/MNT_Validate_Configuration.dtsx`.
# MAGIC Asserts that every mandatory `etl.required_configuration_key` has a value in `etl.configuration`
# MAGIC for this environment, that the legacy connection placeholders and the required `wwi` secret-scope
# MAGIC keys exist, that the job parameters are populated, and that the regional open periods and FX rates
# MAGIC look plausible. Missing mandatory values are logged through `control.logError` and, with
# MAGIC `FailOnMissingKey`, fail the task so the maintenance job stops before purging anything.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
import maintenance_lib as mnt  # noqa: E402

PACKAGE_NAME = "MNT_Validate_Configuration"
PROJECT_NAME = "WWI_Maintenance"


class ConfigurationDefectError(RuntimeError):
    pass


# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"] or None
environmentCode = p["environmentCode"]
businessDate = p["businessDate"]
failOnMissingKey = mnt.toBool(mnt.widgetOr(dbutils, "FailOnMissingKey", "True"))
requiredSecretScope = mnt.widgetOr(dbutils, "RequiredSecretScope", "wwi")
requiredSecretKeys = mnt.splitCsv(mnt.widgetOr(dbutils, "RequiredSecretKeys", ",".join(mnt.DEFAULT_REQUIRED_SECRETS)))

# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME,
                                             projectName=PROJECT_NAME, stepName="Pre Flight")
try:
    resultTable = mnt.ensureWorkTable(spark, catalog, "silver.work_configuration_validation")
    configTable = naming.table(catalog, "etl", "configuration")
    checkedAt = mnt.utcNow()
    results = []

    def addResult(key, checkType, status, detail=None):
        results.append({"ConfigurationKey": key, "EnvironmentCode": environmentCode, "CheckTypeCode": checkType,
                        "CheckStatus": status, "DetailText": detail, "CheckedAtUtc": checkedAt})

    # 1. Required keys (environment-specific row wins over the 'ALL' row, as usp_GetConfiguration resolves it)
    configValues = {
        r["ConfigurationKey"]: r["ConfigurationValue"]
        for r in spark.sql(
            f"SELECT ConfigurationKey, ConfigurationValue FROM {mnt.quoted(configTable)} "
            f"WHERE EnvironmentCode = 'ALL' UNION ALL "
            f"SELECT ConfigurationKey, ConfigurationValue FROM {mnt.quoted(configTable)} "
            f"WHERE EnvironmentCode = {mnt.sqlString(environmentCode)}").collect()
    }
    if mnt.tableExists(spark, catalog, "etl", "required_configuration_key"):
        requiredKeys = spark.sql(
            f"SELECT ConfigurationKey FROM {mnt.quoted(naming.table(catalog, 'etl', 'required_configuration_key'))} "
            f"WHERE IsMandatory = true AND (EnvironmentCode IS NULL OR EnvironmentCode = {mnt.sqlString(environmentCode)})"
        ).collect()
        for r in requiredKeys:
            status = mnt.requiredKeyStatus(configValues.get(r["ConfigurationKey"]))
            addResult(r["ConfigurationKey"], "REQUIRED", status,
                      "Key is not defined for this environment" if status == "MISSING" else None)
    else:
        addResult("etl.required_configuration_key", "REQUIRED", "MISSING", "Register table does not exist")

    # 2. Connection placeholders (legacy list) resolved from etl.configuration, never stored inline
    for placeholder in mnt.LEGACY_CONNECTION_PLACEHOLDERS:
        addResult(placeholder, "PLACEHOLDER", "MISSING" if placeholder not in configValues else "OK",
                  "Connection placeholder resolved from the environment, never stored inline")

    # 3. Secret-scope keys the JDBC extracts reference as {{secrets/wwi/<key>}}
    try:
        scopeKeys = {s.key for s in dbutils.secrets.list(requiredSecretScope)}
        scopeDetail = f"Secret scope {requiredSecretScope}"
    except Exception as exc:
        scopeKeys, scopeDetail = set(), f"Secret scope {requiredSecretScope} unavailable: {str(exc)[:200]}"
    for key in requiredSecretKeys:
        addResult(f"secrets/{requiredSecretScope}/{key}", "SECRET", "OK" if key in scopeKeys else "MISSING", scopeDetail)

    # 4. Job parameters every notebook in the estate depends on
    for name, value in (("catalog", catalog), ("EnvironmentCode", environmentCode),
                        ("BusinessDate", businessDate), ("BatchId", p["batchId"])):
        addResult(name, "PARAMETER", mnt.requiredKeyStatus(value), "Job parameter")

    # 5. Plausibility: regional open periods and FX-rate availability for the business date
    if mnt.tableExists(spark, catalog, "etl", "region_period_status"):
        for r in spark.table(naming.table(catalog, "etl", "region_period_status")).collect():
            status = mnt.periodPlausibility(r["RegionCode"], r["FiscalCalendarCode"], r["OpenPeriodKey"],
                                            r["VatRegimeCode"], businessDate)
            addResult(f"OPEN_PERIOD_{r['RegionCode']}", "PLAUSIBILITY", status,
                      f"Region {r['RegionCode']} open period {r['OpenPeriodKey'] or 0}")
    if mnt.tableExists(spark, catalog, "etl", "fx_rate_availability"):
        fxRows = spark.sql(
            f"SELECT RegionCode, COUNT(*) AS RateCount FROM {mnt.quoted(naming.table(catalog, 'etl', 'fx_rate_availability'))} "
            f"WHERE RateDate = DATE'{businessDate.isoformat()}' GROUP BY RegionCode").collect()
        for r in fxRows:
            addResult(f"FX_RATES_{r['RegionCode']}", "PLAUSIBILITY", mnt.fxPlausibility(r["RateCount"]),
                      f"FX rates loaded for the business date: {r['RateCount']}")

    mnt.insertRows(spark, resultTable, results, mnt.WORK_TABLE_DDL["silver.work_configuration_validation"])
    missingCount, suspectCount = mnt.countDefects(results)
    configurationStatus = mnt.configurationStatus(missingCount, suspectCount)

    if missingCount > 0:
        control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                         errorSeverity="Error", errorCode=50011, sourceName=PACKAGE_NAME,
                         sourceComponent="Raise Configuration Defect",
                         errorDescription="Mandatory configuration keys are missing for this environment: "
                                          + ", ".join(r["ConfigurationKey"] for r in results
                                                      if r["CheckStatus"] in ("MISSING", "EMPTY"))[:3000])
    if suspectCount > 0:
        notificationTable = mnt.ensureOpsTable(spark, catalog, "etl.operator_notification")
        spark.sql(f"INSERT INTO {mnt.quoted(notificationTable)} "
                  f"(BatchId, NotificationTypeCode, Severity, Subject, Body, RaisedAtUtc, IsAcknowledged) VALUES "
                  f"({batchId if batchId is not None else 'NULL'}, 'CONFIG_SUSPECT', 'WARNING', "
                  f"'Configuration values look wrong', 'Suspect configuration entries: {suspectCount}', "
                  f"current_timestamp(), false)")

    configurationCount = spark.table(configTable).count()
    control.logRowCount(spark, catalog, packageExecutionId, "etl.configuration",
                        sourceRowCount=len(results), targetRowCount=configurationCount, rejectRowCount=missingCount)
    if missingCount > 0 and failOnMissingKey:
        raise ConfigurationDefectError(
            f"{PACKAGE_NAME}: {missingCount} mandatory configuration value(s) missing for {environmentCode}; "
            f"status={configurationStatus}")
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded",
                          rowsRead=len(results), rowsRejected=missingCount)
except Exception as exc:
    if not isinstance(exc, ConfigurationDefectError):
        control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                         errorSeverity="Error", sourceName=PACKAGE_NAME,
                         sourceComponent="Check Required Keys", errorDescription=str(exc)[:4000])
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise
