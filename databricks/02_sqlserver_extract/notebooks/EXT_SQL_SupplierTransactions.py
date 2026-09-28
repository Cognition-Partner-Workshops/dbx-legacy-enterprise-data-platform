# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_SQL_SupplierTransactions
# MAGIC
# MAGIC Migrated from `ssis/02_sqlserver_extract/EXT_SQL_SupplierTransactions.dtsx` (project `WWI_Extract_SqlServer`).
# MAGIC
# MAGIC Numeric-key incremental over Purchasing.SupplierTransactions. This ledger overlaps the Oracle AP ledger for suppliers that were never migrated off the OLTP system, so the extract carries the supplier reference used by the downstream duplicate-payment check.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Source | `Purchasing.SupplierTransactions` via Spark JDBC (`com.microsoft.sqlserver.jdbc.SQLServerDriver`) |
# MAGIC | Legacy target | `raw.SqlInvoice` |
# MAGIC | Delta target | `${catalog}.bronze.raw_sql_supplier_transaction` |
# MAGIC | Load pattern | incremental, open numeric key (watermark = max key extracted) |
# MAGIC | Watermark object | `Purchasing.SupplierTransactions` |
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
    "EXT_SQL_SupplierTransactions",
    control=control,
    params=params,
    naming=naming,
)
print(json.dumps(json.loads(result.toJson()), indent=2))

# COMMAND ----------

dbutils.notebook.exit(result.toJson())
