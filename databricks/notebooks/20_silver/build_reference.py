# Databricks notebook source
# MAGIC %md
# MAGIC # Silver reference tables: FX rates, tax rates, fiscal calendars
# MAGIC
# MAGIC Rebuilds `ref_fx_rate`, `ref_tax_rate_na`, `ref_tax_rate_eu`, `ref_tax_rate_apac`,
# MAGIC `dim_fiscal_calendar` and `dim_date` from the bronze `oracle_wwi_ref_*` /
# MAGIC `oracle_wwi_fin_*` tables. Full overwrite, safe to re-run.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "", "Unity Catalog catalog (blank = two-level names)")  # noqa: F821
dbutils.widgets.text("mock_data_root", "mock_data/output", "Mock source CSV root")  # noqa: F821

catalog = dbutils.widgets.get("catalog").strip() or None  # noqa: F821
mockDataRoot = dbutils.widgets.get("mock_data_root").strip()  # noqa: F821

srcPath = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src"))
if srcPath not in sys.path:
    sys.path.insert(0, srcPath)

# COMMAND ----------

from sales_lakehouse.common.config import PipelineConfig  # noqa: E402
from sales_lakehouse.common.spark import ensureSchemas  # noqa: E402
from sales_lakehouse.silver import reference  # noqa: E402

cfg = PipelineConfig(catalog=catalog, mockDataRoot=mockDataRoot)
ensureSchemas(spark, cfg)  # noqa: F821
reference.run(spark, cfg)  # noqa: F821

# COMMAND ----------

for table in (
    reference.REF_FX_RATE,
    reference.REF_TAX_RATE_NA,
    reference.REF_TAX_RATE_EU,
    reference.REF_TAX_RATE_APAC,
    reference.DIM_FISCAL_CALENDAR,
    reference.DIM_DATE,
):
    fqn = cfg.fqn("silver", table)
    print(f"{fqn}: {spark.table(fqn).count()} rows")  # noqa: F821
