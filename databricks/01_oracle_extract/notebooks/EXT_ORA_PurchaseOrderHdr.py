# Databricks notebook source
# MAGIC %md
# MAGIC # EXT_ORA_PurchaseOrderHdr
# MAGIC
# MAGIC Migrated from `ssis/01_oracle_extract/EXT_ORA_PurchaseOrderHdr.dtsx` (project `WWI_Extract_Oracle`).
# MAGIC
# MAGIC Incremental purchase order header extract from V_PURCHASE_ORDER_EXTRACT. Status and org predicates are pushed into the Oracle query; cancelled orders are split out and landed with a cancellation reason for the reconciliation report.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Legacy target | `raw.OraclePurchaseOrderHdr` |
# MAGIC | Bronze target | `${catalog}.bronze.raw_oracle_purchase_order_hdr` |
# MAGIC | Source system | `ORA_ERP` |
# MAGIC | Watermark | `WWI_PROC.PURCHASE_ORDER_HDR` (Timestamp) |
# MAGIC | Load mode | `append` |
# MAGIC | Legacy control flow | Init Batch Variables -> Log Package Start -> Get Watermark -> Extract Purchase Orders -> Set Watermark -> Log Row Counts -> Log Package Success |
# MAGIC
# MAGIC | Data flow source | Extract file (files mode) | Derived columns | Conditional split | Lookup |
# MAGIC |---|---|---|---|---|
# MAGIC | `ORA V_PURCHASE_ORDER_EXTRACT` | `WWI_PROC/PURCHASE_ORDER_HDR.dat` | OrderAgeDays, LateFlag | Split Cancelled Orders | - |
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

spec = specs.getSpec("EXT_ORA_PurchaseOrderHdr")
summary = notebook.runPackage(spark, dbutils, spec)
print(notebook.summaryJson(summary))

# COMMAND ----------

dbutils.notebook.exit(notebook.summaryJson(summary))
