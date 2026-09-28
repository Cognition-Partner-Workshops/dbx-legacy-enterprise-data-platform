# Databricks notebook source
# MAGIC %md
# MAGIC # SLS_APAC_Load_Commission
# MAGIC Legacy package `ssis/11_sales/SLS_APAC_Load_Commission.dtsx` (project WWI_Sales).
# MAGIC Thin entry point that runs the shared, parameterised commission implementation
# MAGIC (`SLS_Load_Commission` / `src/sales_commission_run.py`) with `RegionCode = APAC`.
# MAGIC The job task of the same name calls `SLS_Load_Commission` directly with
# MAGIC `RegionCode=APAC`; this notebook exists for package-for-package traceability and ad-hoc runs.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402

import sales_common as sc  # noqa: E402
import sales_schemas as schemas  # noqa: E402
import sales_commission_run as runner  # noqa: E402

# COMMAND ----------

summary = runner.runCommission(spark, dbutils, regionCode="APAC")
print(summary)

# COMMAND ----------

dbutils.notebook.exit(str(summary))
