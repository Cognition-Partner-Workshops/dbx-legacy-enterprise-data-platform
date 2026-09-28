# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_SQL_CustomerTransactions
# MAGIC
# MAGIC Migrated from `ssis/02_sqlserver_extract/EXT_SQL_CustomerTransactions.dtsx` (project `WWI_Extract_SqlServer`).
# MAGIC
# MAGIC Numeric-key incremental over Sales.CustomerTransactions - the AR ledger side of the OLTP database - joined to the payment method and transaction type. Outstanding balances are carried so AR aging can be rebuilt in staging.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Source | `Sales.CustomerTransactions` via Spark JDBC (`com.microsoft.sqlserver.jdbc.SQLServerDriver`) |
# MAGIC | Legacy target | `raw.SqlInvoice` |
# MAGIC | Delta target | `${catalog}.bronze.raw_sql_customer_transaction` |
# MAGIC | Load pattern | incremental, numeric key bounded by the source MAX at start of run |
# MAGIC | Watermark object | `Sales.CustomerTransactions` |
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
    "EXT_SQL_CustomerTransactions",
    control=control,
    params=params,
    naming=naming,
)
print(json.dumps(json.loads(result.toJson()), indent=2))

# COMMAND ----------

dbutils.notebook.exit(result.toJson())
