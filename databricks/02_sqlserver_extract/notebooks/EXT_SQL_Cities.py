# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_SQL_Cities
# MAGIC
# MAGIC Migrated from `ssis/02_sqlserver_extract/EXT_SQL_Cities.dtsx` (project `WWI_Extract_SqlServer`).
# MAGIC
# MAGIC Full truncate-and-load of Application.Cities joined to StateProvinces and Countries, landing into the shared raw.OracleGeography table so OLTP and ERP geography resolve through one lookup. Postal formatting follows the country's regional standard.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Source | `Application.Cities` via Spark JDBC (`com.microsoft.sqlserver.jdbc.SQLServerDriver`) |
# MAGIC | Legacy target | `raw.OracleGeography` |
# MAGIC | Delta target | `${catalog}.bronze.raw_sql_city` |
# MAGIC | Load pattern | full reload (clear RecordKind slice, then insert) |
# MAGIC | Watermark object | n/a (full reload) |
# MAGIC | Delete detection | none |
# MAGIC | Null-key disposition | RedirectRow |
# MAGIC
# MAGIC The package logic (source SQL, column contract, derived columns, conditional splits,
# MAGIC reject handling, watermark and control-framework calls) lives in
# MAGIC `src/wwi_sqlserver_extract`; this notebook only binds parameters and runs it.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402

from wwi_sqlserver_extract import extract, jdbc  # noqa: E402

# COMMAND ----------

# Job parameters (shared naming contract) and JDBC settings from Project.params;
# secrets are read through dbutils.secrets and never appear here.
for widgetName, widgetDefault in extract.JOB_PARAMETER_DEFAULTS.items():
    dbutils.widgets.text(widgetName, widgetDefault)
for widgetName, widgetDefault in jdbc.PARAMETER_DEFAULTS.items():
    dbutils.widgets.text(widgetName, widgetDefault)

# COMMAND ----------

result = extract.runPackage(
    spark,
    dbutils,
    "EXT_SQL_Cities",
    control=control,
    params=params,
    naming=naming,
)
print(json.dumps(json.loads(result.toJson()), indent=2))

# COMMAND ----------

dbutils.notebook.exit(result.toJson())
