# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_CustomerAddress
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_CustomerAddress.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Incremental customer address extract from V_CUSTOMER_ADDRESS_CURRENT with region-specific postal standardisation (ZIP+4, UK/DE postcode casing, APAC prefecture handling) and a geography lookup against raw.OracleGeography.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OracleCustomerAddress` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_customer_address` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_MDM.CUST_ADDRESS` (Timestamp) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract Customer Addresses -> Log Unmatched Geography -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA V_CUSTOMER_ADDRESS_CURRENT` | `WWI_MDM/CUST_ADDRESS.dat` | AddressLine1Std, CityNameStd, PostalCdStd | - | Lookup Geography Key |
# MAGIC
# MAGIC Job parameters: `BatchId`, `BusinessDate`, `ReloadFullHistory`, `EnvironmentCode`, `RestartFromStep`, `catalog`
# MAGIC (read via `dbx_etl_common.params.getJobParams`). Task parameters: `source_mode` (`jdbc` | `files`),
# MAGIC `oracle_host`, `oracle_port`, `oracle_service`, `oracle_user`, `oracle_secret_scope`, `oracle_password_key`,
# MAGIC `extract_volume_path`, `jdbc_fetch_size`, `jdbc_num_partitions`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from oracle_extract import notebook, specs

# COMMAND ----------

spec = specs.getSpec("EXT_ORA_CustomerAddress")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
